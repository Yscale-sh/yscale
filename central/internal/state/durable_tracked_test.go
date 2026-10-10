package state

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"
)

// listOnlyPersister serves a fixed durable burst set and can be made to fail,
// so the sweep's fail-closed contract is testable without a real Postgres.
type listOnlyPersister struct {
	auditRecorder
	bursts []*Burst
	err    error
}

func (p *listOnlyPersister) upsertCustomer(*Customer) error                        { return nil }
func (p *listOnlyPersister) createTenant(*Customer, *TenantMembership, bool) error { return nil }
func (p *listOnlyPersister) revokeCustomer(*Customer) error                        { return nil }
func (p *listOnlyPersister) finalizeRevokedCustomer(string) error                  { return nil }
func (p *listOnlyPersister) deleteCustomerAndMemberships(string) error             { return nil }
func (p *listOnlyPersister) upsertAccount(*Account) error                          { return nil }
func (p *listOnlyPersister) upsertMembership(*TenantMembership, *AuditEvent) error {
	return nil
}
func (p *listOnlyPersister) deleteMembership(string, *AuditEvent) error { return nil }
func (p *listOnlyPersister) upsertWorkload(*Workload) error             { return nil }
func (p *listOnlyPersister) stampWorkloadPodObservation(context.Context, string, string, string, string, string, time.Time) (int64, error) {
	return 1, nil
}
func (p *listOnlyPersister) getWorkloadPodObservation(context.Context, string, string, string, string) (*PodObservation, bool, error) {
	return nil, false, nil
}
func (p *listOnlyPersister) workloadByBurst(context.Context, string) (*Workload, error) {
	return nil, nil
}
func (p *listOnlyPersister) recordWorkloadCost(context.Context, string, *WorkloadCost) (bool, error) {
	return false, nil
}

func (p *listOnlyPersister) upsertBurst(*Burst) error         { return nil }
func (p *listOnlyPersister) deleteBurst(string) error         { return nil }
func (p *listOnlyPersister) upsertPV(*PersistentVolume) error { return nil }
func (p *listOnlyPersister) deletePV(string) error            { return nil }

func (p *listOnlyPersister) updateBurstNodePhase(context.Context, BurstNodePhaseUpdate) (*burstNodePhaseResult, error) {
	return nil, nil
}
func (p *listOnlyPersister) updateBurstGPUTelemetry(context.Context, BurstGPUTelemetryUpdate) (bool, error) {
	return false, nil
}

func (p *listOnlyPersister) burstOwnedBy(context.Context, string, string) (bool, error) {
	return false, nil
}

func (p *listOnlyPersister) burstForTenant(context.Context, string, string) (*Burst, bool, error) {
	return nil, false, nil
}

// loadAll is the boot read. These fakes hold no durable set to reload, so an
// empty snapshot is honest — including its tombstones: nothing here ever
// retires an id.
func (p *listOnlyPersister) loadAll(context.Context) (*snapshot, error) { return &snapshot{}, nil }

func (p *listOnlyPersister) Close() {}

func (p *listOnlyPersister) claimBurst(context.Context, string) (*Burst, bool, error) {
	return nil, false, nil
}
func (p *listOnlyPersister) recordBurstReapReceipt(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *listOnlyPersister) burstReapRecorded(context.Context, string, string) (bool, error) {
	return false, nil
}

func (p *listOnlyPersister) reservePodSlot(context.Context, int, string, time.Duration) (bool, error) {
	return false, nil
}
func (p *listOnlyPersister) releasePodSlot(context.Context, string) error { return nil }

// The idempotency trio: this fake's subject is elsewhere, so the claim seam is
// stubbed to "nothing was ever claimed".
func (p *listOnlyPersister) claimIdempotency(context.Context, *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	return nil, true, nil
}
func (p *listOnlyPersister) completeIdempotency(context.Context, string, int, []byte, time.Time) error {
	return nil
}
func (p *listOnlyPersister) deleteIdempotency(context.Context, string) error { return nil }

func (p *listOnlyPersister) reservedPodSlots(context.Context, time.Duration) (map[int]bool, error) {
	return nil, nil
}

func (p *listOnlyPersister) listBursts(context.Context) ([]*Burst, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.bursts, nil
}

// TestDurableBursts pins the contract the reaper watchdog depends on. The
// watchdog reaps whatever in this set has overrun its budget, so a burst
// missing here keeps billing past its deadline until some other replica
// notices — and there may not be another replica.
//
// Deadline is the distinguishing field throughout: the two copies of
// burst_shared differ only in theirs, so a result served from the in-memory map
// is impossible to confuse with one served from the durable backend. It is also
// the field the sweep actually reads.
func TestDurableBursts(t *testing.T) {
	const (
		memDeadline = time.Hour
		pgDeadline  = 30 * time.Minute
	)
	tests := []struct {
		name    string
		durable bool     // false = no persister wired (in-memory / OSS store)
		rows    []*Burst // what the durable backend holds
		listErr error    // injected read failure; also the error the caller must see

		wantOK     bool
		wantBursts map[string]time.Duration // id → Deadline; nil = expect none
	}{
		{
			// One process, so its map IS the burst set and the caller keeps
			// today's ListBursts path. Unchanged behaviour for OSS-local.
			name:   "no durable backend defers to the caller's memory path",
			wantOK: false,
		},
		{
			name:       "durable records win over this replica's memory",
			durable:    true,
			rows:       []*Burst{{ID: "burst_shared", BackendID: "vm-shared", Deadline: pgDeadline}},
			wantOK:     true,
			wantBursts: map[string]time.Duration{"burst_shared": pgDeadline},
		},
		{
			// The multi-replica property: another central created this burst, so
			// it was never in this process's map. Without it here, nothing on
			// this replica would ever enforce its budget.
			name:       "a burst only another replica created is still returned",
			durable:    true,
			rows:       []*Burst{{ID: "burst_other_replica", BackendID: "vm-other", Deadline: pgDeadline}},
			wantOK:     true,
			wantBursts: map[string]time.Duration{"burst_other_replica": pgDeadline},
		},
		{
			name:    "read failure is reported, never a partial set",
			durable: true,
			rows:    []*Burst{{ID: "burst_shared", BackendID: "vm-shared", Deadline: pgDeadline}},
			listErr: errDurableRead,
			wantOK:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			// Seeded in memory only. burst_shared exists durably too in the
			// cases that wire a persister, with a different deadline.
			for _, id := range []string{"burst_mem_only", "burst_shared"} {
				if err := s.PutBurst(&Burst{ID: id, BackendID: "vm-" + id, Deadline: memDeadline}); err != nil {
					t.Fatalf("PutBurst %s: %v", id, err)
				}
			}
			if tc.durable {
				s.persist = &listOnlyPersister{bursts: tc.rows, err: tc.listErr}
			}

			bursts, ok, err := s.DurableBursts(context.Background())
			if !errors.Is(err, tc.listErr) {
				t.Fatalf("DurableBursts err = %v, want %v", err, tc.listErr)
			}
			if ok != tc.wantOK {
				t.Fatalf("DurableBursts ok = %v, want %v — ok=false means 'no durable backend' "+
					"and sends the caller back to memory", ok, tc.wantOK)
			}
			if tc.listErr != nil {
				if bursts != nil {
					t.Errorf("bursts = %+v, want nil — a partial set is worse than none: "+
						"the caller cannot tell it apart from 'nothing is over budget'", bursts)
				}
				return
			}

			got := make(map[string]time.Duration, len(bursts))
			for _, b := range bursts {
				got[b.ID] = b.Deadline
			}
			if !maps.Equal(got, tc.wantBursts) {
				t.Errorf("bursts = %v, want %v", got, tc.wantBursts)
			}
			if !tc.wantOK {
				// ok=false is the signal to read memory instead, so memory must
				// still hold everything this store was given.
				if n := len(s.ListBursts()); n != 2 {
					t.Errorf("ListBursts returned %d bursts, want 2 — ok=false promises "+
						"the caller a usable in-memory set", n)
				}
			}
		})
	}
}

var errDurableRead = errors.New("postgres is down")

// TestDurableBurstBackendIDs pins the contract the orphan sweep depends on.
// The sweep destroys any owned VM absent from this set, so each case here is a
// deletion decision, not a lookup.
func TestDurableBurstBackendIDs(t *testing.T) {
	t.Run("reads durable state, not the in-memory map", func(t *testing.T) {
		s := New()
		// A burst only this process knows about. It must NOT appear: the
		// whole point is that memory is per-replica and durable state is not.
		if err := s.PutBurst(&Burst{ID: "burst_mem", BackendID: "vm-in-memory-only"}); err != nil {
			t.Fatalf("PutBurst: %v", err)
		}
		s.persist = &listOnlyPersister{bursts: []*Burst{
			{ID: "burst_a", BackendID: "vm-a"},
			{ID: "burst_b", BackendID: "vm-b"},
		}}

		ids, ok, err := s.DurableBurstBackendIDs(context.Background())
		if err != nil || !ok {
			t.Fatalf("got ok=%v err=%v, want ok=true err=nil", ok, err)
		}
		if !ids["vm-a"] || !ids["vm-b"] {
			t.Errorf("durable bursts missing from tracked set: %v", ids)
		}
		if ids["vm-in-memory-only"] {
			t.Error("in-memory-only burst leaked into the tracked set — the set must " +
				"come from durable state so another replica's burst is never seen as an orphan")
		}
	})

	t.Run("read failure is reported, never an empty set", func(t *testing.T) {
		s := New()
		s.persist = &listOnlyPersister{err: errors.New("postgres down")}

		ids, ok, err := s.DurableBurstBackendIDs(context.Background())
		if err == nil {
			t.Fatal("want an error when the durable read fails")
		}
		if !ok {
			t.Error("ok must stay true — a durable backend exists, it just could not be read; " +
				"ok=false means 'no backend' and would silently fall back to memory")
		}
		if ids != nil {
			t.Errorf("ids = %v, want nil — a partial set reads as 'everything is an orphan'", ids)
		}
	})

	t.Run("no durable backend falls back to memory", func(t *testing.T) {
		s := New() // nil persister: the in-memory/OSS store
		if err := s.PutBurst(&Burst{ID: "burst_x", BackendID: "vm-x"}); err != nil {
			t.Fatalf("PutBurst: %v", err)
		}
		ids, ok, err := s.DurableBurstBackendIDs(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Error("ok must be false with no persister so the caller uses ListBursts — " +
				"there, memory IS the truth because only one process exists")
		}
		if ids != nil {
			t.Errorf("ids = %v, want nil when there is no durable backend", ids)
		}
	})

	t.Run("bursts with no backend ID are skipped", func(t *testing.T) {
		s := New()
		s.persist = &listOnlyPersister{bursts: []*Burst{
			{ID: "burst_pending", BackendID: ""},
			{ID: "burst_real", BackendID: "vm-real"},
		}}
		ids, _, err := s.DurableBurstBackendIDs(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, present := ids[""]; present {
			t.Error(`empty BackendID must not enter the set — it would match nothing and hide real state`)
		}
		if len(ids) != 1 || !ids["vm-real"] {
			t.Errorf("ids = %v, want exactly {vm-real}", ids)
		}
	})
}
