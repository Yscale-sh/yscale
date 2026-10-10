package state

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// workloadPersister holds the durable workload set — the rows a replica that
// never created the workload can still reach — and records everything written
// back to it.
type workloadPersister struct {
	auditRecorder
	mu           sync.Mutex
	rows         map[string]*Workload // burst id → the durable workload row
	reaps        map[string]string
	lookupErr    error
	upsertErr    error
	costErr      error
	costCtxErr   error
	attentionErr error

	upserts []*Workload
	// costWrites counts recordWorkloadCost calls that reached the row, so a test
	// can tell "the guard refused it" from "nobody asked".
	costWrites      int
	attentionWrites int
}

func (p *workloadPersister) upsertCustomer(*Customer) error                        { return nil }
func (p *workloadPersister) createTenant(*Customer, *TenantMembership, bool) error { return nil }
func (p *workloadPersister) revokeCustomer(*Customer) error                        { return nil }
func (p *workloadPersister) finalizeRevokedCustomer(string) error                  { return nil }
func (p *workloadPersister) deleteCustomerAndMemberships(string) error             { return nil }
func (p *workloadPersister) upsertAccount(*Account) error                          { return nil }
func (p *workloadPersister) upsertMembership(*TenantMembership, *AuditEvent) error {
	return nil
}
func (p *workloadPersister) deleteMembership(string, *AuditEvent) error { return nil }
func (p *workloadPersister) upsertBurst(*Burst) error                   { return nil }
func (p *workloadPersister) deleteBurst(string) error                   { return nil }
func (p *workloadPersister) upsertPV(*PersistentVolume) error           { return nil }
func (p *workloadPersister) deletePV(string) error                      { return nil }

func (p *workloadPersister) updateBurstNodePhase(context.Context, BurstNodePhaseUpdate) (*burstNodePhaseResult, error) {
	return nil, nil
}
func (p *workloadPersister) updateBurstGPUTelemetry(context.Context, BurstGPUTelemetryUpdate) (bool, error) {
	return false, nil
}

func (p *workloadPersister) burstOwnedBy(context.Context, string, string) (bool, error) {
	return false, nil
}

func (p *workloadPersister) burstForTenant(context.Context, string, string) (*Burst, bool, error) {
	return nil, false, nil
}

// loadAll is the boot read. These fakes hold no durable set to reload, so an
// empty snapshot is honest — including its tombstones: nothing here ever
// retires an id.
func (p *workloadPersister) loadAll(context.Context) (*snapshot, error) { return &snapshot{}, nil }

func (p *workloadPersister) Close() {}

func (p *workloadPersister) claimBurst(context.Context, string) (*Burst, bool, error) {
	return nil, false, nil
}
func (p *workloadPersister) listBursts(context.Context) ([]*Burst, error) { return nil, nil }
func (p *workloadPersister) reservePodSlot(context.Context, int, string, time.Duration) (bool, error) {
	return false, nil
}
func (p *workloadPersister) releasePodSlot(context.Context, string) error { return nil }

// The idempotency trio: this fake's subject is elsewhere, so the claim seam is
// stubbed to "nothing was ever claimed".
func (p *workloadPersister) claimIdempotency(context.Context, *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	return nil, true, nil
}
func (p *workloadPersister) completeIdempotency(context.Context, string, int, []byte, time.Time) error {
	return nil
}
func (p *workloadPersister) deleteIdempotency(context.Context, string) error { return nil }

func (p *workloadPersister) reservedPodSlots(context.Context, time.Duration) (map[int]bool, error) {
	return nil, nil
}

// workloadByBurst hands back a COPY, as the real one does: it decodes a row, so
// a caller mutating the result must not be able to reach the stored record.
func (p *workloadPersister) workloadByBurst(_ context.Context, burstID string) (*Workload, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lookupErr != nil {
		return nil, p.lookupErr
	}
	w, ok := p.rows[burstID]
	if !ok {
		return nil, nil // no workload for this burst; nothing to fail
	}
	cp := *w
	return &cp, nil
}

// upsertWorkload mirrors upsertWorkloadStmt: the stored row keeps a Cost the
// incoming document does not carry. A spy that took the document whole would
// pass a store that erases the observation on every later lifecycle write.
func (p *workloadPersister) stampWorkloadPodObservation(context.Context, string, string, string, string, string, time.Time) (int64, error) {
	return 1, nil
}
func (p *workloadPersister) getWorkloadPodObservation(context.Context, string, string, string, string) (*PodObservation, bool, error) {
	return nil, false, nil
}
func (p *workloadPersister) upsertWorkload(w *Workload) error {
	cp := *w
	p.upserts = append(p.upserts, &cp)
	if p.upsertErr != nil {
		return p.upsertErr
	}
	row := cp
	if stored, ok := p.rows[w.BurstID]; ok {
		preserveWorkloadTransition(&row, stored)
		preserveWorkloadBillingManualAttention(&row, stored)
		if row.Cost == nil && stored.Cost != nil {
			observed := *stored.Cost
			row.Cost = &observed
		}
		*stored = row
		return nil
	}
	if w.BurstID != "" {
		if p.rows == nil {
			p.rows = make(map[string]*Workload)
		}
		p.rows[w.BurstID] = &row
	}
	return nil
}

// recordWorkloadCost mirrors recordWorkloadCostStmt: it patches only the Cost
// key, only onto the row the burst backs, and only when that row has none.
func (p *workloadPersister) recordWorkloadCost(ctx context.Context, burstID string, c *WorkloadCost) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.costCtxErr = ctx.Err()
	if p.costErr != nil {
		return false, p.costErr
	}
	w, ok := p.rows[burstID]
	if !ok || w.Cost != nil {
		return false, nil
	}
	observed := *c
	w.Cost = &observed
	p.costWrites++
	return true, nil
}

func (p *workloadPersister) markWorkloadBillingManualAttention(
	_ context.Context,
	customerID string,
	workloadRef string,
	holdID int64,
) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.attentionErr != nil {
		return false, p.attentionErr
	}
	for _, workload := range p.rows {
		if workload.ID != workloadRef || workload.CustomerID != customerID || workload.Billing == nil ||
			workload.Billing.WorkloadRef != workloadRef || workload.Billing.HoldID != holdID {
			continue
		}
		workload.Billing.ManualAttention = true
		p.attentionWrites++
		return true, nil
	}
	return false, nil
}

func (p *workloadPersister) recordBurstReapReceipt(_ context.Context, burstID, customerID string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reaps == nil {
		p.reaps = make(map[string]string)
	}
	if owner, exists := p.reaps[burstID]; exists {
		return owner == customerID, nil
	}
	p.reaps[burstID] = customerID
	return true, nil
}

func (p *workloadPersister) burstReapRecorded(_ context.Context, burstID, customerID string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reaps[burstID] == customerID, nil
}

var errWorkloadRead = errors.New("postgres is down")

const crossReplicaBurst = "burst_other_replica"

// TestFinishWorkloadForBurst pins what a cross-replica reap owes the customer.
// Any replica can now win the claim on any burst, so the replica that destroys
// a node is often not the one holding that workload in memory — and a workload
// nothing marks failed reads "provisioning" for a job whose node is gone.
func TestFinishWorkloadForBurst(t *testing.T) {
	finishedAt := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	alreadyFinished := time.Date(2026, 8, 6, 11, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		durable   bool                 // false = no persister wired (in-memory / OSS store)
		inMemory  *Workload            // seeded into this replica's map
		rows      map[string]*Workload // what the durable backend holds
		lookupErr error
		upsertErr error

		wantApplied bool
		wantErr     error
		wantUpserts int    // durable writes this call made
		wantStatus  string // status of the last durable write; "" = don't check
	}{
		{
			// One process: its map IS the workload set, and the write-through
			// keeps the durable row in step. Unchanged behaviour for OSS-local.
			name:        "no durable backend finishes the workload in memory",
			inMemory:    &Workload{ID: "wl_1", BurstID: crossReplicaBurst, Status: "provisioning"},
			wantApplied: true,
		},
		{
			name: "no durable backend and no workload is not an error",
		},
		{
			// The creating replica still resolves its current durable row; a
			// cached copy alone is not evidence the workload continues to exist.
			name:     "a workload this replica holds is finished in memory",
			durable:  true,
			inMemory: &Workload{ID: "wl_1", BurstID: crossReplicaBurst, Status: "provisioning"},
			rows: map[string]*Workload{
				crossReplicaBurst: {ID: "wl_1", BurstID: crossReplicaBurst, Status: "provisioning"},
			},
			wantApplied: true,
			wantUpserts: 1,
			wantStatus:  "failed",
		},
		{
			// The case this exists for: another central created the burst and
			// the workload, then died or simply lost the claim. Nothing in this
			// process's map mentions either.
			name:    "a workload another replica created is finished durably",
			durable: true,
			rows: map[string]*Workload{
				crossReplicaBurst: {ID: "wl_1", BurstID: crossReplicaBurst, Status: "provisioning"},
			},
			wantApplied: true,
			wantUpserts: 1,
			wantStatus:  "failed",
		},
		{
			// onlyIfUnfinished, evaluated against the durable copy: a workload
			// the customer already completed on another replica must not be
			// rewritten as failed.
			name:    "a workload already finished durably is not clobbered",
			durable: true,
			rows: map[string]*Workload{
				crossReplicaBurst: {ID: "wl_1", BurstID: crossReplicaBurst, Status: "succeeded", FinishedAt: &alreadyFinished},
			},
		},
		{
			// A nodeOnly burst, or a workload row never written. Nothing to do,
			// and nothing wrong.
			name:    "a burst with no workload is not an error",
			durable: true,
		},
		{
			name:      "a failed durable lookup is reported",
			durable:   true,
			rows:      map[string]*Workload{crossReplicaBurst: {ID: "wl_1", BurstID: crossReplicaBurst}},
			lookupErr: errWorkloadRead,
			wantErr:   errWorkloadRead,
		},
		{
			// The write is the whole point of the call, so a caller told
			// "applied" for a row that never landed would log nothing and leave
			// the workload reading as running.
			name:    "a failed durable write is reported",
			durable: true,
			rows: map[string]*Workload{
				crossReplicaBurst: {ID: "wl_1", BurstID: crossReplicaBurst, Status: "provisioning"},
			},
			upsertErr:   errWorkloadRead,
			wantErr:     errWorkloadRead,
			wantUpserts: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := emptyStore()
			if tc.inMemory != nil {
				// Seeded straight into the map: PutWorkload would spend a
				// durable write of its own and skew the counts below.
				s.workloads[tc.inMemory.ID] = tc.inMemory
			}
			var p *workloadPersister
			if tc.durable {
				p = &workloadPersister{rows: tc.rows, lookupErr: tc.lookupErr, upsertErr: tc.upsertErr}
				s.persist = p
			}

			applied, err := s.FinishWorkloadForBurst(context.Background(), crossReplicaBurst, "failed", finishedAt)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("FinishWorkloadForBurst err = %v, want %v", err, tc.wantErr)
			}
			if applied != tc.wantApplied {
				t.Fatalf("FinishWorkloadForBurst applied = %v, want %v", applied, tc.wantApplied)
			}

			if tc.inMemory != nil && tc.wantApplied {
				w, err := s.GetWorkload(tc.inMemory.ID)
				if err != nil || w.Status != "failed" || w.FinishedAt == nil {
					t.Errorf("in-memory workload = %+v (err %v), want failed + FinishedAt set", w, err)
				}
			}
			if p == nil {
				return
			}
			if len(p.upserts) != tc.wantUpserts {
				t.Fatalf("durable writes = %d, want %d", len(p.upserts), tc.wantUpserts)
			}
			if tc.wantStatus == "" {
				return
			}
			got := p.upserts[len(p.upserts)-1]
			if got.Status != tc.wantStatus {
				t.Errorf("durable workload status = %q, want %q", got.Status, tc.wantStatus)
			}
			if got.FinishedAt == nil || !got.FinishedAt.Equal(finishedAt) {
				t.Errorf("durable workload FinishedAt = %v, want %v — without it the "+
					"onlyIfUnfinished guard has nothing to read and a later reap rewrites the row",
					got.FinishedAt, finishedAt)
			}
		})
	}
}

// TestFinishWorkloadForBurstDoesNotReadMemoryForAnotherBurst is the negative of
// the lookup: the durable path must resolve the burst it was asked about. A
// scan that matched anything else would fail an unrelated customer's workload.
func TestFinishWorkloadForBurstDoesNotReadMemoryForAnotherBurst(t *testing.T) {
	s := emptyStore()
	s.workloads["wl_mine"] = &Workload{ID: "wl_mine", BurstID: "burst_mine", Status: "running"}
	p := &workloadPersister{rows: map[string]*Workload{
		crossReplicaBurst: {ID: "wl_theirs", BurstID: crossReplicaBurst, Status: "provisioning"},
	}}
	s.persist = p

	applied, err := s.FinishWorkloadForBurst(context.Background(), crossReplicaBurst, "failed", time.Now().UTC())
	if err != nil || !applied {
		t.Fatalf("FinishWorkloadForBurst = applied:%v err:%v, want true/nil", applied, err)
	}
	if len(p.upserts) != 1 || p.upserts[0].ID != "wl_theirs" {
		t.Fatalf("durable writes = %+v, want exactly wl_theirs", p.upserts)
	}
	w, err := s.GetWorkload("wl_mine")
	if err != nil || w.Status != "running" || w.FinishedAt != nil {
		t.Errorf("unrelated in-memory workload = %+v (err %v), want untouched", w, err)
	}
}

// TestWorkloadByBurstPostgres proves the half of the lookup that lives in SQL
// rather than in Go: the JSONB key really is the Go field name. The fake above
// indexes rows by burst id directly, so it cannot notice a rename of
// Workload.BurstID or a json tag added to it — either of which turns every
// cross-replica reap into a silent "this burst has no workload" and leaves the
// row reading "provisioning" forever, which is the exact bug being fixed.
//
// Needs a real database. Gated on YSCALE_TEST_DATABASE_URL like
// TestPostgresRoundTrip, so ordinary CI does NOT cover it; CI should set it
// against a throwaway Postgres.
func TestWorkloadByBurstPostgres(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	p := &pgPersister{pool: freshSchemaPool(t, dsn, "yscale_test_workload_by_burst", 4)}
	if err := p.ensureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	mine := &Workload{ID: "wl_mine", CustomerID: "cust_a", BurstID: "burst_mine", Status: "provisioning"}
	theirs := &Workload{ID: "wl_theirs", CustomerID: "cust_b", BurstID: "burst_theirs", Status: "running"}
	for _, w := range []*Workload{mine, theirs} {
		if err := p.upsertWorkload(w); err != nil {
			t.Fatalf("upsert %s: %v", w.ID, err)
		}
	}

	got, err := p.workloadByBurst(ctx, "burst_mine")
	if err != nil {
		t.Fatalf("workloadByBurst: %v", err)
	}
	if got == nil {
		t.Fatal("no workload found for a burst that has one — the JSONB key does not match the Go field")
	}
	if got.ID != mine.ID || got.Status != mine.Status {
		t.Errorf("workloadByBurst = %+v, want %+v", got, mine)
	}

	// A nodeOnly burst, or one whose workload row was never written.
	got, err = p.workloadByBurst(ctx, "burst_nobody")
	if err != nil || got != nil {
		t.Errorf("workloadByBurst for an unknown burst = %+v (err %v), want nil/nil — "+
			"a missing workload is nothing to fail, not a failure", got, err)
	}
}
