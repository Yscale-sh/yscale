package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

// claimPersister scripts what the durable backend's DELETE ... RETURNING does.
// row is the record the database would hand back; it is consumed by the first
// winning claim, which is how a real single-statement delete behaves — exactly
// one caller can match a given id.
type claimPersister struct {
	auditRecorder
	row    *Burst
	err    error
	claims []string
}

func (p *claimPersister) upsertCustomer(*Customer) error                        { return nil }
func (p *claimPersister) createTenant(*Customer, *TenantMembership, bool) error { return nil }
func (p *claimPersister) revokeCustomer(*Customer) error                        { return nil }
func (p *claimPersister) finalizeRevokedCustomer(string) error                  { return nil }
func (p *claimPersister) deleteCustomerAndMemberships(string) error             { return nil }
func (p *claimPersister) upsertAccount(*Account) error                          { return nil }
func (p *claimPersister) upsertMembership(*TenantMembership, *AuditEvent) error {
	return nil
}
func (p *claimPersister) deleteMembership(string, *AuditEvent) error { return nil }
func (p *claimPersister) upsertWorkload(*Workload) error             { return nil }
func (p *claimPersister) stampWorkloadPodObservation(context.Context, string, string, string, string, string, time.Time) (int64, error) {
	return 1, nil
}
func (p *claimPersister) getWorkloadPodObservation(context.Context, string, string, string, string) (*PodObservation, bool, error) {
	return nil, false, nil
}
func (p *claimPersister) workloadByBurst(context.Context, string) (*Workload, error) {
	return nil, nil
}
func (p *claimPersister) recordWorkloadCost(context.Context, string, *WorkloadCost) (bool, error) {
	return false, nil
}

func (p *claimPersister) upsertBurst(*Burst) error         { return nil }
func (p *claimPersister) deleteBurst(string) error         { return nil }
func (p *claimPersister) upsertPV(*PersistentVolume) error { return nil }
func (p *claimPersister) deletePV(string) error            { return nil }

func (p *claimPersister) updateBurstNodePhase(context.Context, BurstNodePhaseUpdate) (*burstNodePhaseResult, error) {
	return nil, nil
}
func (p *claimPersister) updateBurstGPUTelemetry(context.Context, BurstGPUTelemetryUpdate) (bool, error) {
	return false, nil
}

func (p *claimPersister) burstOwnedBy(context.Context, string, string) (bool, error) {
	return false, nil
}

func (p *claimPersister) burstForTenant(context.Context, string, string) (*Burst, bool, error) {
	return nil, false, nil
}

// loadAll is the boot read. These fakes hold no durable set to reload, so an
// empty snapshot is honest — including its tombstones: nothing here ever
// retires an id.
func (p *claimPersister) loadAll(context.Context) (*snapshot, error) { return &snapshot{}, nil }

func (p *claimPersister) Close() {}

func (p *claimPersister) listBursts(context.Context) ([]*Burst, error) { return nil, nil }

// The pod-slot trio has its own fake and its own table in pod_slots_test.go;
// these tests are about the claim.
func (p *claimPersister) reservePodSlot(context.Context, int, string, time.Duration) (bool, error) {
	return false, nil
}
func (p *claimPersister) releasePodSlot(context.Context, string) error { return nil }

// The idempotency trio: this fake's subject is elsewhere, so the claim seam is
// stubbed to "nothing was ever claimed".
func (p *claimPersister) claimIdempotency(context.Context, *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	return nil, true, nil
}
func (p *claimPersister) completeIdempotency(context.Context, string, int, []byte, time.Time) error {
	return nil
}
func (p *claimPersister) deleteIdempotency(context.Context, string) error { return nil }

func (p *claimPersister) reservedPodSlots(context.Context, time.Duration) (map[int]bool, error) {
	return nil, nil
}

func (p *claimPersister) claimBurst(_ context.Context, id string) (*Burst, bool, error) {
	p.claims = append(p.claims, id)
	if p.err != nil {
		return nil, false, p.err
	}
	if p.row == nil {
		return nil, false, nil
	}
	b := p.row
	p.row = nil
	return b, true, nil
}
func (p *claimPersister) recordBurstReapReceipt(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *claimPersister) burstReapRecorded(context.Context, string, string) (bool, error) {
	return false, nil
}

var errClaim = errors.New("postgres is down")

// The two records carry different backends so a claim that answers from memory
// is distinguishable from one that answers from the database. That difference
// IS the multi-replica property: every central holds its own copy of a burst,
// so only the durable record can decide who tears the VM down.
const (
	memBackend = "memory-copy"
	pgBackend  = "durable-copy"
)

// TestClaimBurst pins the exactly-once teardown guarantee. Each case is a
// billing decision: won=true means this caller destroys a cloud VM and accrues
// its cost, so a wrong true double-bills the customer and a wrong false leaves
// a VM running.
func TestClaimBurst(t *testing.T) {
	tests := []struct {
		name     string
		durable  bool   // false = no persister wired (in-memory / OSS store)
		row      *Burst // what the database's claim yields, when durable
		claimErr error

		wantWon      bool
		wantErr      error
		wantBackend  string // "" = expect no burst back
		wantInMemory bool   // seeded record survives the claim
	}{
		{
			// One process, so the mutex IS the atomicity. Unchanged behaviour.
			name:        "no persister claims from memory",
			wantWon:     true,
			wantBackend: memBackend,
		},
		{
			name:        "durable claim returns the database's record, not memory's",
			durable:     true,
			row:         &Burst{ID: "b1", CustomerID: "cust_a", Backend: pgBackend, BackendID: "vm-durable"},
			wantWon:     true,
			wantBackend: pgBackend,
		},
		{
			// Another replica already claimed it. The local copy is stale, so it
			// goes too — otherwise admission counts and the watchdog keep seeing
			// a burst that no longer exists anywhere.
			name:    "no row means someone else won",
			durable: true,
			wantWon: false,
		},
		{
			// The DELETE may or may not have committed. Tearing down on a
			// maybe-claim is the double-bill this whole mechanism prevents, so
			// the answer is "not yours" plus the error. The record stays in
			// memory because the outcome is unknown and this replica must keep
			// tracking a burst that may still exist; the retry that makes
			// won=false safe comes from the durable row the claim left behind.
			name:         "claim error is never a win",
			durable:      true,
			claimErr:     errClaim,
			wantWon:      false,
			wantErr:      errClaim,
			wantInMemory: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := emptyStore()
			s.bursts["b1"] = &Burst{ID: "b1", CustomerID: "cust_a", Backend: memBackend, BackendID: "vm-memory"}
			var p *claimPersister
			if tc.durable {
				p = &claimPersister{row: tc.row, err: tc.claimErr}
				s.persist = p
			}

			b, won, err := s.ClaimBurst("b1")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ClaimBurst err = %v, want %v", err, tc.wantErr)
			}
			if won != tc.wantWon {
				t.Fatalf("ClaimBurst won = %v, want %v", won, tc.wantWon)
			}
			if tc.wantBackend == "" {
				if b != nil {
					t.Errorf("got burst %+v, want none", b)
				}
			} else if b == nil {
				t.Fatalf("want a burst back with backend %q, got nil", tc.wantBackend)
			} else if b.Backend != tc.wantBackend {
				t.Errorf("claimed burst backend = %q, want %q — the durable record decides, "+
					"not this replica's map", b.Backend, tc.wantBackend)
			}

			_, gerr := s.GetBurst("b1")
			if tc.wantInMemory && gerr != nil {
				t.Errorf("record dropped from memory on an unknown outcome; the watchdog "+
					"sweeps the in-memory set and would never retry it: %v", gerr)
			}
			if !tc.wantInMemory && gerr != ErrNotFound {
				t.Errorf("burst still in memory after the claim resolved: %v", gerr)
			}
			if p != nil && len(p.claims) != 1 {
				t.Errorf("durable claims = %v, want exactly 1", p.claims)
			}
		})
	}
}

// TestClaimBurstAcrossReplicas covers the case the whole change exists for: a
// central that never saw the burst — because a different replica created it —
// must still be able to claim and tear it down.
func TestClaimBurstAcrossReplicas(t *testing.T) {
	s := emptyStore() // empty map: this replica has never heard of b1
	s.persist = &claimPersister{
		row: &Burst{ID: "b1", CustomerID: "cust_a", Backend: pgBackend, BackendID: "vm-durable"},
	}

	b, won, err := s.ClaimBurst("b1")
	if err != nil || !won {
		t.Fatalf("ClaimBurst = won:%v err:%v, want won:true err:nil", won, err)
	}
	if b == nil || b.BackendID != "vm-durable" {
		t.Fatalf("claim must return the durable record so the VM can be torn down: %+v", b)
	}
}

// TestClaimBurstElectsOneWinner is the anti-double-teardown assertion. Two
// claims for the same burst, one row in the database: the second must lose.
func TestClaimBurstElectsOneWinner(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "in-memory store"
		if durable {
			name = "durable store"
		}
		t.Run(name, func(t *testing.T) {
			s := emptyStore()
			s.bursts["b1"] = &Burst{ID: "b1", CustomerID: "cust_a", Backend: memBackend}
			if durable {
				s.persist = &claimPersister{row: &Burst{ID: "b1", CustomerID: "cust_a", Backend: pgBackend}}
			}

			if _, won, err := s.ClaimBurst("b1"); !won || err != nil {
				t.Fatalf("first claim = won:%v err:%v, want won:true err:nil", won, err)
			}
			b, won, err := s.ClaimBurst("b1")
			if won || err != nil || b != nil {
				t.Fatalf("second claim = b:%+v won:%v err:%v, want nil/false/nil — two winners "+
					"means two teardowns and two charges for one burst", b, won, err)
			}
		})
	}
}
