package state

import (
	"context"
	"errors"
	"maps"
	"os"
	"sync"
	"testing"
	"time"
)

// podSlotPersister emulates the pod_slots table: one row per slot, so a second
// reserver for a slot someone already holds loses exactly as the PRIMARY KEY
// makes it lose. It carries its own clock so the reservation TTL is testable
// without sleeping, and it records releases so the by-burst-id keying can be
// asserted.
type podSlotPersister struct {
	auditRecorder
	mu   sync.Mutex
	rows map[int]podSlotRow
	now  time.Time

	reserveErr error
	releaseErr error
	listErr    error
	released   []string
}

type podSlotRow struct {
	burstID    string
	reservedAt time.Time
}

func newPodSlotPersister() *podSlotPersister {
	return &podSlotPersister{
		rows: make(map[int]podSlotRow),
		now:  time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC),
	}
}

func (p *podSlotPersister) advance(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.now = p.now.Add(d)
}

func (p *podSlotPersister) upsertCustomer(*Customer) error                        { return nil }
func (p *podSlotPersister) createTenant(*Customer, *TenantMembership, bool) error { return nil }
func (p *podSlotPersister) revokeCustomer(*Customer) error                        { return nil }
func (p *podSlotPersister) finalizeRevokedCustomer(string) error                  { return nil }
func (p *podSlotPersister) deleteCustomerAndMemberships(string) error             { return nil }
func (p *podSlotPersister) upsertAccount(*Account) error                          { return nil }
func (p *podSlotPersister) upsertMembership(*TenantMembership, *AuditEvent) error {
	return nil
}
func (p *podSlotPersister) deleteMembership(string, *AuditEvent) error { return nil }
func (p *podSlotPersister) upsertWorkload(*Workload) error             { return nil }
func (p *podSlotPersister) stampWorkloadPodObservation(context.Context, string, string, string, string, string, time.Time) (int64, error) {
	return 1, nil
}
func (p *podSlotPersister) getWorkloadPodObservation(context.Context, string, string, string, string) (*PodObservation, bool, error) {
	return nil, false, nil
}
func (p *podSlotPersister) workloadByBurst(context.Context, string) (*Workload, error) {
	return nil, nil
}
func (p *podSlotPersister) recordWorkloadCost(context.Context, string, *WorkloadCost) (bool, error) {
	return false, nil
}

func (p *podSlotPersister) upsertBurst(*Burst) error         { return nil }
func (p *podSlotPersister) deleteBurst(string) error         { return nil }
func (p *podSlotPersister) upsertPV(*PersistentVolume) error { return nil }
func (p *podSlotPersister) deletePV(string) error            { return nil }

func (p *podSlotPersister) updateBurstNodePhase(context.Context, BurstNodePhaseUpdate) (*burstNodePhaseResult, error) {
	return nil, nil
}
func (p *podSlotPersister) updateBurstGPUTelemetry(context.Context, BurstGPUTelemetryUpdate) (bool, error) {
	return false, nil
}

func (p *podSlotPersister) burstOwnedBy(context.Context, string, string) (bool, error) {
	return false, nil
}

func (p *podSlotPersister) burstForTenant(context.Context, string, string) (*Burst, bool, error) {
	return nil, false, nil
}

// loadAll is the boot read. These fakes hold no durable set to reload, so an
// empty snapshot is honest — including its tombstones: nothing here ever
// retires an id.
func (p *podSlotPersister) loadAll(context.Context) (*snapshot, error) { return &snapshot{}, nil }

func (p *podSlotPersister) Close() {}

func (p *podSlotPersister) claimBurst(context.Context, string) (*Burst, bool, error) {
	return nil, false, nil
}
func (p *podSlotPersister) recordBurstReapReceipt(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *podSlotPersister) burstReapRecorded(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *podSlotPersister) listBursts(context.Context) ([]*Burst, error) { return nil, nil }

// reservePodSlot mirrors the INSERT ... ON CONFLICT DO UPDATE ... WHERE expired
// statement: a live row blocks, an expired one is taken over, and either way
// only one caller can leave holding the slot.
func (p *podSlotPersister) reservePodSlot(_ context.Context, slot int, burstID string, ttl time.Duration) (bool, error) {
	if p.reserveErr != nil {
		return false, p.reserveErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if row, held := p.rows[slot]; held && p.now.Sub(row.reservedAt) <= ttl {
		return false, nil
	}
	p.rows[slot] = podSlotRow{burstID: burstID, reservedAt: p.now}
	return true, nil
}

func (p *podSlotPersister) releasePodSlot(_ context.Context, burstID string) error {
	if p.releaseErr != nil {
		return p.releaseErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.released = append(p.released, burstID)
	for slot, row := range p.rows {
		if row.burstID == burstID {
			delete(p.rows, slot)
		}
	}
	return nil
}

// The idempotency trio: this fake's subject is elsewhere, so the claim seam is
// stubbed to "nothing was ever claimed".
func (p *podSlotPersister) claimIdempotency(context.Context, *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	return nil, true, nil
}
func (p *podSlotPersister) completeIdempotency(context.Context, string, int, []byte, time.Time) error {
	return nil
}
func (p *podSlotPersister) deleteIdempotency(context.Context, string) error { return nil }

func (p *podSlotPersister) reservedPodSlots(_ context.Context, ttl time.Duration) (map[int]bool, error) {
	if p.listErr != nil {
		return nil, p.listErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	slots := make(map[int]bool)
	for slot, row := range p.rows {
		if p.now.Sub(row.reservedAt) <= ttl {
			slots[slot] = true
		}
	}
	return slots, nil
}

var errPodSlots = errors.New("postgres is down")

const testSlotTTL = 10 * time.Minute

// TestReservedPodSlots pins the read half of the contract the pod-CIDR
// allocator depends on. Each case decides what the allocator believes is free,
// and believing a taken slot is free hands two live bursts the same /24 —
// both advertise the prefix on the mesh and routing breaks for both customers.
func TestReservedPodSlots(t *testing.T) {
	tests := []struct {
		name    string
		durable bool // false = no persister wired (in-memory / OSS store)
		rows    map[int]string
		age     time.Duration // how long before the read the rows were written
		listErr error

		wantOK    bool
		wantSlots map[int]bool // nil = expect none back
	}{
		{
			// One process, so the allocator's own map IS the reservation set.
			// Unchanged behaviour for OSS-local.
			name:   "no durable backend defers to the caller's memory path",
			wantOK: false,
		},
		{
			name:      "reservations another replica made are returned",
			durable:   true,
			rows:      map[int]string{10: "burst_other_replica", 12: "burst_also_other"},
			wantOK:    true,
			wantSlots: map[int]bool{10: true, 12: true},
		},
		{
			// A Plan that died between allocating a /24 and persisting its
			// burst. Nothing releases that row, so counting it forever would
			// take a /24 out of the pool permanently.
			name:      "a reservation older than the TTL is not in use",
			durable:   true,
			rows:      map[int]string{10: "burst_abandoned"},
			age:       testSlotTTL + time.Minute,
			wantOK:    true,
			wantSlots: map[int]bool{},
		},
		{
			name:      "a reservation inside the TTL is still in use",
			durable:   true,
			rows:      map[int]string{10: "burst_in_flight"},
			age:       testSlotTTL - time.Minute,
			wantOK:    true,
			wantSlots: map[int]bool{10: true},
		},
		{
			name:    "read failure is reported, never an empty set",
			durable: true,
			rows:    map[int]string{10: "burst_other_replica"},
			listErr: errPodSlots,
			wantOK:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			if tc.durable {
				p := newPodSlotPersister()
				for slot, id := range tc.rows {
					if _, err := p.reservePodSlot(context.Background(), slot, id, testSlotTTL); err != nil {
						t.Fatalf("seed slot %d: %v", slot, err)
					}
				}
				p.advance(tc.age)
				p.listErr = tc.listErr
				s.persist = p
			}

			slots, ok, err := s.ReservedPodSlots(context.Background(), testSlotTTL)
			if !errors.Is(err, tc.listErr) {
				t.Fatalf("ReservedPodSlots err = %v, want %v", err, tc.listErr)
			}
			if ok != tc.wantOK {
				t.Fatalf("ReservedPodSlots ok = %v, want %v — ok=false means 'no durable backend' "+
					"and sends the caller back to its own map", ok, tc.wantOK)
			}
			if tc.listErr != nil {
				if slots != nil {
					t.Errorf("slots = %v, want nil — a partial set is indistinguishable from "+
						"'nothing is reserved', which is how a /24 gets handed out twice", slots)
				}
				return
			}
			if !maps.Equal(slots, tc.wantSlots) {
				t.Errorf("slots = %v, want %v", slots, tc.wantSlots)
			}
		})
	}
}

// TestReservePodSlot pins the write half. won=true means this caller owns the
// /24, so a wrong true is two bursts on one prefix.
func TestReservePodSlot(t *testing.T) {
	tests := []struct {
		name       string
		durable    bool // false = no persister wired (in-memory / OSS store)
		heldBy     string
		heldAge    time.Duration
		reserveErr error

		wantWon bool
		wantOK  bool
	}{
		{name: "no durable backend cannot arbitrate", wantWon: false, wantOK: false},
		{name: "a free slot is won", durable: true, wantWon: true, wantOK: true},
		{
			// Another replica reserved it between this one's read and this
			// write. Ordinary, not an error: the caller walks to the next slot.
			name: "a slot another replica holds is lost", durable: true,
			heldBy: "burst_other_replica", heldAge: time.Minute,
			wantWon: false, wantOK: true,
		},
		{
			// The row belongs to a Plan that never persisted its burst. Taking
			// it over in the same write is what makes the TTL reclaim anything.
			name: "an expired reservation is taken over", durable: true,
			heldBy: "burst_abandoned", heldAge: testSlotTTL + time.Minute,
			wantWon: true, wantOK: true,
		},
		{
			name: "write failure is reported with a durable backend present", durable: true,
			reserveErr: errPodSlots, wantWon: false, wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			var p *podSlotPersister
			if tc.durable {
				p = newPodSlotPersister()
				if tc.heldBy != "" {
					if _, err := p.reservePodSlot(context.Background(), 10, tc.heldBy, testSlotTTL); err != nil {
						t.Fatalf("seed: %v", err)
					}
					p.advance(tc.heldAge)
				}
				p.reserveErr = tc.reserveErr
				s.persist = p
			}

			won, ok, err := s.ReservePodSlot(context.Background(), 10, "burst_mine", testSlotTTL)
			if !errors.Is(err, tc.reserveErr) {
				t.Fatalf("ReservePodSlot err = %v, want %v", err, tc.reserveErr)
			}
			if won != tc.wantWon {
				t.Fatalf("ReservePodSlot won = %v, want %v", won, tc.wantWon)
			}
			if ok != tc.wantOK {
				t.Fatalf("ReservePodSlot ok = %v, want %v — ok=false means 'no durable backend', "+
					"and must never stand in for a write that failed", ok, tc.wantOK)
			}
		})
	}
}

// TestReleasePodSlot pins that a release names the BURST, not the slot. Slots
// are recycled, so a release keyed by number would free whatever burst happens
// to hold that slot now — handing its /24 out a second time, which is the
// collision the table exists to prevent.
func TestReleasePodSlot(t *testing.T) {
	t.Run("frees only the releasing burst's slot", func(t *testing.T) {
		p := newPodSlotPersister()
		s := New()
		s.persist = p
		ctx := context.Background()

		// burst_old holds slot 10 and is torn down; by then slot 11 has been
		// recycled to burst_new. Only slot 10 may come free.
		if _, err := p.reservePodSlot(ctx, 10, "burst_old", testSlotTTL); err != nil {
			t.Fatal(err)
		}
		if _, err := p.reservePodSlot(ctx, 11, "burst_new", testSlotTTL); err != nil {
			t.Fatal(err)
		}
		if err := s.ReleasePodSlot(ctx, "burst_old"); err != nil {
			t.Fatalf("ReleasePodSlot: %v", err)
		}

		slots, _, err := s.ReservedPodSlots(ctx, testSlotTTL)
		if err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(slots, map[int]bool{11: true}) {
			t.Errorf("reserved slots = %v, want {11} — releasing burst_old must not free "+
				"the slot burst_new is currently holding", slots)
		}
	})

	t.Run("no durable backend is a successful no-op", func(t *testing.T) {
		if err := New().ReleasePodSlot(context.Background(), "burst_x"); err != nil {
			t.Errorf("ReleasePodSlot on the in-memory store = %v, want nil", err)
		}
	})

	t.Run("failure is reported", func(t *testing.T) {
		s := New()
		p := newPodSlotPersister()
		p.releaseErr = errPodSlots
		s.persist = p
		if err := s.ReleasePodSlot(context.Background(), "burst_x"); !errors.Is(err, errPodSlots) {
			t.Errorf("ReleasePodSlot err = %v, want %v", err, errPodSlots)
		}
	})
}

// TestReservePodSlotElectsOneWinner is the anti-duplicate-/24 assertion at the
// store seam: many callers, one slot, one winner. It proves the Store wrapper
// adds no race of its own; that the SQL statement really is atomic is proven
// against a real database in TestPodSlotsPostgres.
func TestReservePodSlotElectsOneWinner(t *testing.T) {
	s := New()
	s.persist = newPodSlotPersister()

	const racers = 16
	var wg sync.WaitGroup
	wins := make([]bool, racers)
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			won, ok, err := s.ReservePodSlot(context.Background(), 10, "burst_racer", testSlotTTL)
			if err != nil || !ok {
				t.Errorf("racer %d: ok=%v err=%v", i, ok, err)
				return
			}
			wins[i] = won
		}()
	}
	wg.Wait()

	n := 0
	for _, won := range wins {
		if won {
			n++
		}
	}
	if n != 1 {
		t.Errorf("winners = %d, want exactly 1 — every extra winner is a second burst "+
			"handed the same /24, and two peers advertising one prefix break routing for both", n)
	}
}

// TestPodSlotsPostgres proves the properties that live in the SQL rather than
// in Go: that the PRIMARY KEY really elects one winner among concurrent
// replicas, that an expired row is taken over rather than wedging its slot, and
// that a release is scoped to its burst.
//
// It needs a real database — the fake above can only assert that the store
// wrapper is faithful to the statement, not that the statement is what we
// think. Gated on YSCALE_TEST_DATABASE_URL like TestPostgresRoundTrip, so
// ordinary CI does NOT cover it; CI should set it against a throwaway Postgres.
func TestPodSlotsPostgres(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	store, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	// Slots well outside the allocator's 10..250 pool, so a shared test
	// database is not disturbed and reruns start clean.
	const slotA, slotB = 9001, 9002
	cleanup := func() {
		for _, id := range []string{"burst_pg_a", "burst_pg_b", "burst_pg_stale", "burst_pg_taker"} {
			_ = store.ReleasePodSlot(ctx, id)
		}
	}
	cleanup()
	defer cleanup()

	t.Run("concurrent reserves elect one winner", func(t *testing.T) {
		const racers = 8
		var wg sync.WaitGroup
		wins := make([]bool, racers)
		for i := range racers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				won, _, err := store.ReservePodSlot(ctx, slotA, "burst_pg_a", testSlotTTL)
				if err != nil {
					t.Errorf("racer %d: %v", i, err)
					return
				}
				wins[i] = won
			}()
		}
		wg.Wait()
		n := 0
		for _, won := range wins {
			if won {
				n++
			}
		}
		if n != 1 {
			t.Errorf("winners = %d, want exactly 1 — two replicas holding one slot is two "+
				"bursts advertising the same /24", n)
		}
	})

	t.Run("an expired reservation is reclaimed, a live one is not", func(t *testing.T) {
		if won, _, err := store.ReservePodSlot(ctx, slotB, "burst_pg_stale", testSlotTTL); err != nil || !won {
			t.Fatalf("seed reserve = won:%v err:%v", won, err)
		}
		// A zero TTL makes the row we just wrote unconditionally expired, which
		// is what a Plan that died before persisting its burst looks like once
		// the real TTL elapses.
		slots, _, err := store.ReservedPodSlots(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		if slots[slotB] {
			t.Error("an expired reservation was counted as in use; nothing else deletes it, " +
				"so that /24 would leave the pool permanently")
		}
		if won, _, err := store.ReservePodSlot(ctx, slotB, "burst_pg_taker", 0); err != nil || !won {
			t.Fatalf("expired slot not taken over: won=%v err=%v", won, err)
		}
		// Still held under the real TTL, so the next allocator sees it taken.
		if won, _, err := store.ReservePodSlot(ctx, slotB, "burst_pg_b", testSlotTTL); err != nil || won {
			t.Fatalf("live reservation was stolen: won=%v err=%v", won, err)
		}
	})

	t.Run("release is scoped to the releasing burst", func(t *testing.T) {
		// slotA belongs to burst_pg_a and slotB to burst_pg_taker by now.
		if err := store.ReleasePodSlot(ctx, "burst_pg_a"); err != nil {
			t.Fatalf("release: %v", err)
		}
		slots, _, err := store.ReservedPodSlots(ctx, testSlotTTL)
		if err != nil {
			t.Fatal(err)
		}
		if slots[slotA] {
			t.Error("released slot still reserved")
		}
		if !slots[slotB] {
			t.Error("releasing one burst freed another burst's slot; that hands a live " +
				"burst's /24 to a second burst")
		}
	})
}
