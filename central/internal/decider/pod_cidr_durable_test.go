package decider

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/state"
)

// fakeArbiter stands in for the durable store. It exists because state's
// persister seam is unexported, so no other package can wire a fake *state.Store
// — see podSlotArbiter. held is the set of slots some OTHER replica owns: a
// reserve for one of those loses, exactly as the pod_slots primary key makes it
// lose.
type fakeArbiter struct {
	bursts   []*state.Burst
	reserved map[int]bool
	held     map[int]bool

	burstsErr   error
	reservedErr error
	reserveErr  error
	noDurable   bool // report "no durable backend" instead of answering

	attempted []int  // slots this allocation tried to reserve, in order
	wonBy     string // burst id recorded on the winning reservation
}

func (f *fakeArbiter) DurableBursts(context.Context) ([]*state.Burst, bool, error) {
	if f.burstsErr != nil {
		return nil, true, f.burstsErr
	}
	if f.noDurable {
		return nil, false, nil
	}
	return f.bursts, true, nil
}

func (f *fakeArbiter) ReservedPodSlots(context.Context, time.Duration) (map[int]bool, bool, error) {
	if f.reservedErr != nil {
		return nil, true, f.reservedErr
	}
	return f.reserved, true, nil
}

func (f *fakeArbiter) ReservePodSlot(_ context.Context, slot int, burstID string, _ time.Duration) (bool, bool, error) {
	f.attempted = append(f.attempted, slot)
	if f.reserveErr != nil {
		return false, true, f.reserveErr
	}
	if f.held[slot] {
		return false, true, nil
	}
	f.wonBy = burstID
	return true, true, nil
}

var errDurable = errors.New("postgres is down")

// TestAllocateDurablePodCIDR pins the multi-replica allocation contract. Every
// case is a networking decision: handing out a /24 that another live burst
// already holds gives the mesh two peers advertising one prefix, which breaks
// routing for both customers rather than merely mis-recording something.
func TestAllocateDurablePodCIDR(t *testing.T) {
	tests := []struct {
		name string
		arb  fakeArbiter

		wantCIDR      string
		wantDurable   bool
		wantErr       error
		wantErrSubstr string
		wantAttempted []int
	}{
		{
			name:          "empty pool hands out the lowest slot",
			wantCIDR:      "10.244.10.0/24",
			wantDurable:   true,
			wantAttempted: []int{10},
		},
		{
			// The burst was created by another central, so it was never in this
			// process's map. Its /24 is still taken.
			name: "a live burst on another replica holds its slot",
			arb: fakeArbiter{bursts: []*state.Burst{
				{ID: "burst_other_replica", PodCIDR: "10.244.10.0/24"},
				{ID: "burst_also_other", PodCIDR: "10.244.11.0/24"},
			}},
			wantCIDR:      "10.244.12.0/24",
			wantDurable:   true,
			wantAttempted: []int{12},
		},
		{
			// Another replica is mid-Plan: it holds the slot but its burst row
			// has not landed yet. This is the window the table exists for.
			name:          "a slot durably reserved by another replica is skipped",
			arb:           fakeArbiter{reserved: map[int]bool{10: true, 11: true}},
			wantCIDR:      "10.244.12.0/24",
			wantDurable:   true,
			wantAttempted: []int{12},
		},
		{
			// The other replica's reservation landed between this one's read
			// and its write. Losing is ordinary; the allocator walks on.
			name:          "losing the reserve race moves to the next slot",
			arb:           fakeArbiter{held: map[int]bool{10: true, 11: true}},
			wantCIDR:      "10.244.12.0/24",
			wantDurable:   true,
			wantAttempted: []int{10, 11, 12},
		},
		{
			// Falling back to the in-memory map here is what hands out a
			// duplicate /24 — the allocator would see an empty pool.
			name:          "a failed burst read fails the allocation",
			arb:           fakeArbiter{burstsErr: errDurable, reserved: map[int]bool{10: true}},
			wantDurable:   true,
			wantErr:       errDurable,
			wantAttempted: nil,
		},
		{
			name: "a failed reservation read fails the allocation",
			arb: fakeArbiter{reservedErr: errDurable, bursts: []*state.Burst{
				{ID: "burst_other_replica", PodCIDR: "10.244.10.0/24"},
			}},
			wantDurable:   true,
			wantErr:       errDurable,
			wantAttempted: nil,
		},
		{
			name:          "a failed reserve fails the allocation",
			arb:           fakeArbiter{reserveErr: errDurable},
			wantDurable:   true,
			wantErr:       errDurable,
			wantAttempted: []int{10},
		},
		{
			// The in-memory / OSS store. The caller stays on its own map.
			name:        "no durable backend defers to the memory path",
			arb:         fakeArbiter{noDurable: true},
			wantDurable: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New(Config{})
			arb := tc.arb
			cidr, durable, err := d.allocateDurablePodCIDR(context.Background(), &arb, "burst_mine")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if durable != tc.wantDurable {
				t.Fatalf("durable = %v, want %v — durable=false means 'no durable backend' and "+
					"sends the caller to memory, which a failed read must never do", durable, tc.wantDurable)
			}
			if cidr != tc.wantCIDR {
				t.Errorf("cidr = %q, want %q", cidr, tc.wantCIDR)
			}
			if len(arb.attempted) != len(tc.wantAttempted) {
				t.Fatalf("reserve attempts = %v, want %v", arb.attempted, tc.wantAttempted)
			}
			for i, slot := range tc.wantAttempted {
				if arb.attempted[i] != slot {
					t.Fatalf("reserve attempts = %v, want %v", arb.attempted, tc.wantAttempted)
				}
			}
			if tc.wantCIDR != "" && arb.wonBy != "burst_mine" {
				t.Errorf("reservation recorded for %q, want burst_mine — the release is keyed "+
					"by burst id, so a slot booked under the wrong one is never freed", arb.wonBy)
			}
		})
	}
}

// TestAllocateDurablePodCIDRExhausted proves the pool-exhausted error survives
// the durable path: 241 slots, all reserved elsewhere, nothing to hand out.
func TestAllocateDurablePodCIDRExhausted(t *testing.T) {
	reserved := make(map[int]bool, totalPodSlots)
	for slot := podSlotMin; slot <= podSlotMax; slot++ {
		reserved[slot] = true
	}
	d := New(Config{})
	arb := &fakeArbiter{reserved: reserved}

	_, durable, err := d.allocateDurablePodCIDR(context.Background(), arb, "burst_mine")
	if !durable {
		t.Fatal("durable = false on an exhausted pool; the caller would fall back to memory and reuse a live /24")
	}
	if err == nil || !strings.Contains(err.Error(), "pod cidr pool exhausted") {
		t.Fatalf("err = %v, want a pool-exhausted error", err)
	}
	if len(arb.attempted) != 0 {
		t.Errorf("reserve attempts = %v, want none — every slot was known taken", arb.attempted)
	}
}

// TestAllocateDurablePodCIDRGauge proves the free-slot gauge still reports pool
// capacity before this burst takes a slot, on the durable path too.
func TestAllocateDurablePodCIDRGauge(t *testing.T) {
	d := New(Config{})
	d.Cost = cost.NewMeter()
	arb := &fakeArbiter{
		bursts:   []*state.Burst{{ID: "burst_other", PodCIDR: "10.244.10.0/24"}},
		reserved: map[int]bool{11: true, 12: true},
	}

	if _, _, err := d.allocateDurablePodCIDR(context.Background(), arb, "burst_mine"); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	d.Cost.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	want := "yscale_podcidr_pool_free 238" // 241 total - 1 live burst - 2 reservations
	if !strings.Contains(rec.Body.String(), want) {
		t.Errorf("gauge missing %q; body:\n%s", want, rec.Body.String())
	}
}

func TestRefreshPodCIDRGaugeDurable(t *testing.T) {
	d := New(Config{})
	d.Cost = cost.NewMeter()
	arb := &fakeArbiter{
		bursts:   []*state.Burst{{ID: "burst_other", PodCIDR: "10.244.10.0/24"}},
		reserved: map[int]bool{11: true, 12: true},
	}

	durable, err := d.refreshPodCIDRGaugeDurable(context.Background(), arb)
	if err != nil {
		t.Fatal(err)
	}
	if !durable {
		t.Fatal("durable refresh reported no durable backend")
	}

	rec := httptest.NewRecorder()
	d.Cost.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), "yscale_podcidr_pool_free 238") {
		t.Errorf("durable refresh gauge wrong; body:\n%s", rec.Body.String())
	}
}

func TestRefreshPodCIDRGaugeInMemoryReservations(t *testing.T) {
	base := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	nowUTC = func() time.Time { return base }
	defer func() { nowUTC = func() time.Time { return time.Now().UTC() } }()

	store := state.New()
	if err := store.PutBurst(&state.Burst{ID: "burst_live", PodCIDR: "10.244.10.0/24"}); err != nil {
		t.Fatal(err)
	}
	d := New(Config{})
	d.store = store
	d.Cost = cost.NewMeter()
	d.reservedSlots[11] = base
	d.reservedSlots[12] = base.Add(-(podReservationTTL + time.Minute))

	if err := d.RefreshPodCIDRGauge(context.Background()); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	d.Cost.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), "yscale_podcidr_pool_free 239") {
		t.Errorf("in-memory refresh gauge wrong; body:\n%s", rec.Body.String())
	}
	if _, ok := d.reservedSlots[12]; ok {
		t.Error("stale in-memory reservation was not pruned")
	}
}

func TestRefreshPodCIDRGaugeDurableReadErrorRetainsGauge(t *testing.T) {
	d := New(Config{})
	d.Cost = cost.NewMeter()
	d.Cost.SetPodCIDRFree(200)
	arb := &fakeArbiter{burstsErr: errDurable}

	if durable, err := d.refreshPodCIDRGaugeDurable(context.Background(), arb); err == nil || !durable || !errors.Is(err, errDurable) {
		t.Fatalf("durable refresh read = (durable=%v, err=%v), want wrapped durable error", durable, err)
	}

	rec := httptest.NewRecorder()
	d.Cost.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), "yscale_podcidr_pool_free 200") {
		t.Errorf("gauge changed after durable read error; body:\n%s", rec.Body.String())
	}
}

// TestAllocatePodCIDRNoDurableBackendUsesMemory pins that a store with no
// durable backend — the in-memory and OSS builds — still allocates from the
// process-local map. The durable path must not change one-process behaviour.
func TestAllocatePodCIDRNoDurableBackendUsesMemory(t *testing.T) {
	store := state.New()
	if err := store.PutBurst(&state.Burst{ID: "b1", PodCIDR: "10.244.10.0/24"}); err != nil {
		t.Fatal(err)
	}
	d := New(Config{})
	d.store = store

	got, err := d.allocatePodCIDR(context.Background(), "burst_mine")
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.244.11.0/24" {
		t.Fatalf("cidr = %q, want 10.244.11.0/24", got)
	}
	if len(d.reservedSlots) != 1 || d.reservedSlots[11].IsZero() {
		t.Errorf("in-memory reservations = %v, want slot 11 held — with no durable backend "+
			"the process map is the only thing bridging allocate->persist", d.reservedSlots)
	}
}
