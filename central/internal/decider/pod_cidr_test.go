package decider

import (
	"context"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// Two allocations without an intervening store write must not hand out the same
// /24 — the reservation map bridges the allocate->persist window.
func TestAllocatePodCIDR_SequentialDistinct(t *testing.T) {
	d := New(Config{})
	a, err := d.allocatePodCIDR(context.Background(), "burst_a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := d.allocatePodCIDR(context.Background(), "burst_b")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("two allocations returned the same CIDR %q", a)
	}
	if a != "10.244.10.0/24" || b != "10.244.11.0/24" {
		t.Fatalf("want 10/11, got %q %q", a, b)
	}
}

// Slots already held by LIVE bursts in the store are skipped — this is what makes
// allocation restart-safe (the old counter reset to 10 and re-handed live /24s).
func TestAllocatePodCIDR_SkipsLiveBursts(t *testing.T) {
	store := state.New()
	store.PutBurst(&state.Burst{ID: "b1", PodCIDR: "10.244.10.0/24"})
	store.PutBurst(&state.Burst{ID: "b2", PodCIDR: "10.244.12.0/24"})
	d := New(Config{})
	d.store = store

	got, err := d.allocatePodCIDR(context.Background(), "burst_new")
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.244.11.0/24" { // 10 and 12 taken, 11 is the lowest free
		t.Fatalf("want 10.244.11.0/24, got %q", got)
	}
}

// A reaped burst leaves ListBursts (ClaimBurst deletes it), so its slot is
// reclaimed — the lowest free slot is handed out again.
func TestAllocatePodCIDR_ReclaimsFreedSlot(t *testing.T) {
	store := state.New()
	store.PutBurst(&state.Burst{ID: "b1", PodCIDR: "10.244.10.0/24"})
	d := New(Config{})
	d.store = store

	first, err := d.allocatePodCIDR(context.Background(), "burst_first") // 10 taken -> 11 (and reserves 11)
	if err != nil {
		t.Fatal(err)
	}
	if first != "10.244.11.0/24" {
		t.Fatalf("want 11, got %q", first)
	}
	store.DeleteBurst("b1") // slot 10 freed
	got, err := d.allocatePodCIDR(context.Background(), "burst_second")
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.244.10.0/24" { // 10 now free and lowest; 11 still reserved
		t.Fatalf("want reclaimed 10.244.10.0/24, got %q", got)
	}
}

// A reservation for a Plan that never stored its burst ages out after the TTL,
// so the slot isn't leaked forever.
func TestAllocatePodCIDR_ReservationExpires(t *testing.T) {
	base := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	now := base
	nowUTC = func() time.Time { return now }
	defer func() { nowUTC = func() time.Time { return time.Now().UTC() } }()

	d := New(Config{}) // no store: only reservations gate reuse
	if got, _ := d.allocatePodCIDR(context.Background(), "burst_1"); got != "10.244.10.0/24" {
		t.Fatalf("first alloc want 10, got %q", got)
	}
	// Within TTL: slot 10 is still reserved, so the next alloc moves on.
	now = base.Add(podReservationTTL - time.Minute)
	if got, _ := d.allocatePodCIDR(context.Background(), "burst_2"); got != "10.244.11.0/24" {
		t.Fatalf("within-TTL alloc want 11, got %q", got)
	}
	// Past TTL: the slot-10 reservation is pruned, so 10 is free again.
	now = base.Add(podReservationTTL + time.Minute)
	if got, _ := d.allocatePodCIDR(context.Background(), "burst_3"); got != "10.244.10.0/24" {
		t.Fatalf("post-TTL alloc want reclaimed 10, got %q", got)
	}
}

func TestPodSlotOf(t *testing.T) {
	cases := []struct {
		in       string
		wantSlot int
		wantOK   bool
	}{
		{"10.244.15.0/24", 15, true},
		{"10.244.0.0/24", 0, true},
		{"10.244.250.0/24", 250, true},
		{"10.43.0.0/24", 0, false},    // wrong pool
		{"10.244.15.1/24", 0, false},  // non-zero host octet
		{"10.244.999.0/24", 0, false}, // slot out of range
		{"garbage", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		slot, ok := podSlotOf(c.in)
		if ok != c.wantOK || (ok && slot != c.wantSlot) {
			t.Errorf("podSlotOf(%q) = (%d,%v), want (%d,%v)", c.in, slot, ok, c.wantSlot, c.wantOK)
		}
	}
}
