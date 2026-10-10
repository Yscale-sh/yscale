package state

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// idemPersister is a durable backend for the claim seam alone: it holds rows in
// a map and mirrors the two Postgres statements' semantics — an insert that
// yields the incumbent on conflict, and a complete that only lands while the
// claim is still in progress. Everything else is stubbed; the rows outlive any
// one Store, which is what makes a restart expressible here.
type idemPersister struct {
	mu   sync.Mutex
	rows map[string]*IdempotencyClaim
	// claimErr, when set, is what claimIdempotency returns instead of deciding.
	claimErr error
}

func newIdemPersister() *idemPersister {
	return &idemPersister{rows: make(map[string]*IdempotencyClaim)}
}

func (p *idemPersister) claimIdempotency(_ context.Context, c *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	if p.claimErr != nil {
		return nil, false, p.claimErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, ok := p.rows[c.ID]; ok {
		return existing.clone(), false, nil
	}
	p.rows[c.ID] = c.clone()
	return nil, true, nil
}

func (p *idemPersister) completeIdempotency(_ context.Context, id string, status int, body []byte, at time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.rows[id]
	if !ok || c.State != IdempotencyInProgress {
		return errors.New("state: no in-progress idempotency claim " + id)
	}
	c.State = IdempotencyCompleted
	c.Status = status
	c.Response = append([]byte(nil), body...)
	c.CompletedAt = at
	return nil
}

func (p *idemPersister) deleteIdempotency(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.rows, id)
	return nil
}

func (p *idemPersister) upsertCustomer(*Customer) error                        { return nil }
func (p *idemPersister) createTenant(*Customer, *TenantMembership, bool) error { return nil }
func (p *idemPersister) revokeCustomer(*Customer) error                        { return nil }
func (p *idemPersister) finalizeRevokedCustomer(string) error                  { return nil }
func (p *idemPersister) deleteCustomerAndMemberships(string) error             { return nil }
func (p *idemPersister) upsertAccount(*Account) error                          { return nil }
func (p *idemPersister) upsertMembership(*TenantMembership, *AuditEvent) error { return nil }
func (p *idemPersister) deleteMembership(string, *AuditEvent) error            { return nil }
func (p *idemPersister) upsertCustomerAudited(*Customer, *AuditEvent) error    { return nil }
func (p *idemPersister) submitWorkload(*Workload, *AuditEvent) error           { return nil }
func (p *idemPersister) appendAudit(*AuditEvent) error                         { return nil }
func (p *idemPersister) listAudit(context.Context, string, string, int) ([]*AuditEvent, error) {
	return nil, nil
}
func (p *idemPersister) upsertWorkload(*Workload) error { return nil }
func (p *idemPersister) stampWorkloadPodObservation(context.Context, string, string, string, string, string, time.Time) (int64, error) {
	return 1, nil
}
func (p *idemPersister) getWorkloadPodObservation(context.Context, string, string, string, string) (*PodObservation, bool, error) {
	return nil, false, nil
}
func (p *idemPersister) workloadByBurst(context.Context, string) (*Workload, error) {
	return nil, nil
}
func (p *idemPersister) recordWorkloadCost(context.Context, string, *WorkloadCost) (bool, error) {
	return false, nil
}
func (p *idemPersister) upsertBurst(*Burst) error { return nil }
func (p *idemPersister) deleteBurst(string) error { return nil }
func (p *idemPersister) updateBurstNodePhase(context.Context, BurstNodePhaseUpdate) (*burstNodePhaseResult, error) {
	return nil, nil
}
func (p *idemPersister) updateBurstGPUTelemetry(context.Context, BurstGPUTelemetryUpdate) (bool, error) {
	return false, nil
}
func (p *idemPersister) burstOwnedBy(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *idemPersister) burstForTenant(context.Context, string, string) (*Burst, bool, error) {
	return nil, false, nil
}
func (p *idemPersister) claimBurst(context.Context, string) (*Burst, bool, error) {
	return nil, false, nil
}
func (p *idemPersister) recordBurstReapReceipt(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *idemPersister) burstReapRecorded(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *idemPersister) listBursts(context.Context) ([]*Burst, error) { return nil, nil }
func (p *idemPersister) reservePodSlot(context.Context, int, string, time.Duration) (bool, error) {
	return true, nil
}
func (p *idemPersister) releasePodSlot(context.Context, string) error { return nil }
func (p *idemPersister) reservedPodSlots(context.Context, time.Duration) (map[int]bool, error) {
	return nil, nil
}
func (p *idemPersister) upsertPV(*PersistentVolume) error { return nil }
func (p *idemPersister) deletePV(string) error            { return nil }
func (p *idemPersister) setMeshState(*meshState) error    { return nil }
func (p *idemPersister) Close()                           {}

func testClaim(id, hash string) *IdempotencyClaim {
	return &IdempotencyClaim{
		ID:          id,
		CustomerID:  "cust_1",
		Actor:       HumanActor("acct_1", "cust_1"),
		KeyDigest:   "digest-of-" + id,
		RequestHash: hash,
		WorkloadID:  "wl_" + id,
	}
}

// The claim is the election, and the election has exactly one winner. Both
// store shapes have to agree on that: the in-memory one because it is the whole
// record on a self-hosted central, and the durable one because a second replica
// is the reason the table exists.
func TestClaimIdempotentElectsOneWinner(t *testing.T) {
	for _, tc := range []struct {
		name    string
		durable bool
	}{
		{name: "in-memory store"},
		{name: "durable store", durable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := emptyStore()
			if tc.durable {
				s.persist = newIdemPersister()
			}

			const n = 16
			var (
				mu     sync.Mutex
				won    int
				losses []*IdempotencyClaim
			)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					existing, ok, err := s.ClaimIdempotent(context.Background(), testClaim("idem_1", "hash-a"))
					if err != nil {
						t.Errorf("ClaimIdempotent: %v", err)
						return
					}
					mu.Lock()
					defer mu.Unlock()
					if ok {
						won++
						return
					}
					losses = append(losses, existing)
				}()
			}
			close(start)
			wg.Wait()

			if won != 1 {
				t.Fatalf("winners = %d, want exactly 1 — every extra winner is another paid node", won)
			}
			for _, l := range losses {
				if l == nil {
					t.Fatal("a loser got no claim back; it cannot tell a replay from a conflict")
				}
				if l.State != IdempotencyInProgress || l.WorkloadID != "wl_idem_1" {
					t.Fatalf("loser saw %+v, want the in-progress claim with the winner's workload id", l)
				}
			}
		})
	}
}

// A completed claim must come back with the answer the first caller got, byte
// for byte, from a store that never saw the submission. This is the restart:
// the process that took the claim is gone, and a retry arriving at a fresh one
// has to replay rather than provision.
func TestCompletedClaimReplaysAfterReload(t *testing.T) {
	rows := newIdemPersister()
	ctx := context.Background()

	s := emptyStore()
	s.persist = rows
	if _, won, err := s.ClaimIdempotent(ctx, testClaim("idem_done", "hash-a")); err != nil || !won {
		t.Fatalf("ClaimIdempotent = won %v, err %v; want won", won, err)
	}
	body := []byte(`{"id":"wl_idem_done","status":"provisioning"}` + "\n")
	at := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	if err := s.CompleteIdempotent(ctx, "idem_done", 202, body, at); err != nil {
		t.Fatalf("CompleteIdempotent: %v", err)
	}

	// A brand-new Store over the same durable rows — nothing carried over in
	// memory, which is the whole point.
	reloaded := emptyStore()
	reloaded.persist = rows
	existing, won, err := reloaded.ClaimIdempotent(ctx, testClaim("idem_done", "hash-a"))
	if err != nil {
		t.Fatalf("ClaimIdempotent after reload: %v", err)
	}
	if won {
		t.Fatal("a fresh store re-won a completed claim; the retry would provision a second node")
	}
	if existing.State != IdempotencyCompleted || existing.Status != 202 {
		t.Fatalf("reloaded claim = state %q status %d, want completed/202", existing.State, existing.Status)
	}
	if !bytes.Equal(existing.Response, body) {
		t.Fatalf("replayed body = %q, want the stored answer %q", existing.Response, body)
	}
	if !existing.CompletedAt.Equal(at) {
		t.Errorf("CompletedAt = %v, want %v", existing.CompletedAt, at)
	}
}

// The ambiguous case: a claim whose owner died mid-flight. Nothing here may
// hand it to a new owner — the dead process may have been inside a provider
// call that returned a billing machine — so a reloaded store still reports it
// as in progress, however stale it is.
func TestInProgressClaimIsNeverTakenOverAfterReload(t *testing.T) {
	rows := newIdemPersister()
	ctx := context.Background()

	s := emptyStore()
	s.persist = rows
	claim := testClaim("idem_stuck", "hash-a")
	claim.CreatedAt = time.Now().UTC().Add(-72 * time.Hour) // long dead
	if _, won, err := s.ClaimIdempotent(ctx, claim); err != nil || !won {
		t.Fatalf("ClaimIdempotent = won %v, err %v; want won", won, err)
	}

	reloaded := emptyStore()
	reloaded.persist = rows
	existing, won, err := reloaded.ClaimIdempotent(ctx, testClaim("idem_stuck", "hash-a"))
	if err != nil {
		t.Fatalf("ClaimIdempotent after reload: %v", err)
	}
	if won {
		t.Fatal("a stale in-progress claim was taken over; retrying an ambiguous provider call is the double-bill")
	}
	if existing.State != IdempotencyInProgress {
		t.Fatalf("state = %q, want %q", existing.State, IdempotencyInProgress)
	}
	if existing.WorkloadID != "wl_idem_stuck" {
		t.Errorf("workload id = %q; the waiter needs the original id to poll", existing.WorkloadID)
	}
}

// Releasing is what keeps a refusal that provisioned nothing from spending the
// tenant's key forever.
func TestReleaseIdempotentFreesTheKey(t *testing.T) {
	for _, tc := range []struct {
		name    string
		durable bool
	}{
		{name: "in-memory store"},
		{name: "durable store", durable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := emptyStore()
			if tc.durable {
				s.persist = newIdemPersister()
			}
			ctx := context.Background()
			if _, won, err := s.ClaimIdempotent(ctx, testClaim("idem_free", "hash-a")); err != nil || !won {
				t.Fatalf("first claim = won %v, err %v", won, err)
			}
			if err := s.ReleaseIdempotent(ctx, "idem_free"); err != nil {
				t.Fatalf("ReleaseIdempotent: %v", err)
			}
			if _, won, err := s.ClaimIdempotent(ctx, testClaim("idem_free", "hash-a")); err != nil || !won {
				t.Fatalf("re-claim after release = won %v, err %v; the key stayed spent", won, err)
			}
			// Idempotent: releasing an absent claim is not an error.
			if err := s.ReleaseIdempotent(ctx, "idem_gone"); err != nil {
				t.Errorf("releasing an absent claim: %v", err)
			}
		})
	}
}

// Completing twice must not rewrite a published answer: two retries of one
// submission cannot be handed two different responses.
func TestCompleteIdempotentIsWriteOnce(t *testing.T) {
	for _, tc := range []struct {
		name    string
		durable bool
	}{
		{name: "in-memory store"},
		{name: "durable store", durable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := emptyStore()
			if tc.durable {
				s.persist = newIdemPersister()
			}
			ctx := context.Background()
			if _, won, err := s.ClaimIdempotent(ctx, testClaim("idem_once", "hash-a")); err != nil || !won {
				t.Fatalf("claim = won %v, err %v", won, err)
			}
			if err := s.CompleteIdempotent(ctx, "idem_once", 202, []byte("first"), time.Now().UTC()); err != nil {
				t.Fatalf("first complete: %v", err)
			}
			if err := s.CompleteIdempotent(ctx, "idem_once", 503, []byte("second"), time.Now().UTC()); err == nil {
				t.Fatal("a second complete overwrote a terminal answer")
			}
			existing, _, err := s.ClaimIdempotent(ctx, testClaim("idem_once", "hash-a"))
			if err != nil {
				t.Fatalf("re-read: %v", err)
			}
			if existing.Status != 202 || string(existing.Response) != "first" {
				t.Fatalf("stored answer = %d %q, want the first one", existing.Status, existing.Response)
			}
		})
	}
}

// The store refuses what it could not replay, and refuses a body it could not
// bound. Both are fail-closed: a claim with no workload id has nothing to hand
// a waiter, and an unbounded response turns the claim table into a log sink.
func TestClaimIdempotentRejectsUnusableClaims(t *testing.T) {
	s := emptyStore()
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		claim *IdempotencyClaim
	}{
		{name: "nil"},
		{name: "no id", claim: &IdempotencyClaim{WorkloadID: "wl_1", RequestHash: "h"}},
		{name: "no workload id", claim: &IdempotencyClaim{ID: "idem_1", RequestHash: "h"}},
		{name: "no request hash", claim: &IdempotencyClaim{ID: "idem_1", WorkloadID: "wl_1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := s.ClaimIdempotent(ctx, tc.claim); !errors.Is(err, ErrIdempotencyInvalid) {
				t.Fatalf("err = %v, want ErrIdempotencyInvalid", err)
			}
		})
	}

	if _, won, err := s.ClaimIdempotent(ctx, testClaim("idem_big", "hash-a")); err != nil || !won {
		t.Fatalf("claim = won %v, err %v", won, err)
	}
	oversize := bytes.Repeat([]byte("x"), MaxIdempotencyResponseBytes+1)
	if err := s.CompleteIdempotent(ctx, "idem_big", 202, oversize, time.Now().UTC()); !errors.Is(err, ErrIdempotencyInvalid) {
		t.Fatalf("oversize complete = %v, want ErrIdempotencyInvalid", err)
	}
}

// A claim seam that cannot answer must not be read as "free". The error has to
// reach the caller, because the caller's next move is provisioning.
func TestClaimIdempotentPropagatesBackendFailure(t *testing.T) {
	rows := newIdemPersister()
	rows.claimErr = errors.New("connection refused")
	s := emptyStore()
	s.persist = rows
	if _, won, err := s.ClaimIdempotent(context.Background(), testClaim("idem_err", "hash-a")); err == nil || won {
		t.Fatalf("ClaimIdempotent = won %v, err %v; a failed claim must never report a win", won, err)
	}
}

// The claim seam against a REAL Postgres. The fake above mirrors the statements'
// semantics, but it cannot check the two things that make them work: that the
// insert-then-read pair really does yield exactly one winner across concurrent
// sessions, and that completeIdempotency's merge writes a document the INSERT
// path's own decoder reads back — the response is base64 and the timestamp is
// RFC3339 because that is what encoding/json wrote, and a SQL-side cast would
// produce neither. Gated on YSCALE_TEST_DATABASE_URL like the other integration
// tests here; ordinary CI does NOT cover it.
func TestIdempotencyPostgres(t *testing.T) {
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

	const claimID = "idem_pg_test"
	cleanup := func() { _ = store.ReleaseIdempotent(ctx, claimID) }
	cleanup()
	defer cleanup()

	t.Run("concurrent claims elect one winner", func(t *testing.T) {
		const racers = 8
		var (
			mu   sync.Mutex
			won  int
			lost int
		)
		var wg sync.WaitGroup
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				existing, ok, err := store.ClaimIdempotent(ctx, testClaim(claimID, "hash-a"))
				if err != nil {
					t.Errorf("ClaimIdempotent: %v", err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				if ok {
					won++
					return
				}
				lost++
				if existing == nil || existing.WorkloadID != "wl_"+claimID {
					t.Errorf("loser saw %+v, want the incumbent claim", existing)
				}
			}()
		}
		wg.Wait()
		if won != 1 || lost != racers-1 {
			t.Fatalf("won %d / lost %d of %d, want exactly one winner", won, lost, racers)
		}
	})

	t.Run("the terminal answer survives the JSONB round trip", func(t *testing.T) {
		body := []byte(`{"id":"wl_idem_pg_test","status":"provisioning"}` + "\n")
		at := time.Date(2026, 8, 13, 9, 30, 15, 123456000, time.UTC)
		if err := store.CompleteIdempotent(ctx, claimID, 202, body, at); err != nil {
			t.Fatalf("CompleteIdempotent: %v", err)
		}
		// A store that never saw the submission — the restart.
		reloaded, err := NewPostgres(ctx, dsn)
		if err != nil {
			t.Fatalf("reconnect: %v", err)
		}
		defer reloaded.Close()

		existing, won, err := reloaded.ClaimIdempotent(ctx, testClaim(claimID, "hash-a"))
		if err != nil {
			t.Fatalf("ClaimIdempotent after reload: %v", err)
		}
		if won {
			t.Fatal("a fresh store re-won a completed claim")
		}
		if existing.State != IdempotencyCompleted || existing.Status != 202 {
			t.Fatalf("state %q status %d, want completed/202", existing.State, existing.Status)
		}
		if !bytes.Equal(existing.Response, body) {
			t.Fatalf("response = %q, want %q", existing.Response, body)
		}
		if !existing.CompletedAt.Equal(at) {
			t.Errorf("CompletedAt = %v, want %v", existing.CompletedAt, at)
		}
		// And the fields the insert wrote are still intact underneath the merge.
		if existing.CustomerID != "cust_1" || existing.RequestHash != "hash-a" || existing.WorkloadID != "wl_"+claimID {
			t.Errorf("the completion merge clobbered the claim: %+v", existing)
		}
	})

	t.Run("completing twice is refused", func(t *testing.T) {
		err := store.CompleteIdempotent(ctx, claimID, 503, []byte("second"), time.Now().UTC())
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("second complete = %v, want ErrNotFound", err)
		}
	})

	t.Run("release frees the key", func(t *testing.T) {
		if err := store.ReleaseIdempotent(ctx, claimID); err != nil {
			t.Fatalf("ReleaseIdempotent: %v", err)
		}
		if _, won, err := store.ClaimIdempotent(ctx, testClaim(claimID, "hash-a")); err != nil || !won {
			t.Fatalf("re-claim after release = won %v, err %v", won, err)
		}
	})
}
