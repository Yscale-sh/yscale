//go:build integration

package lifecycletest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/internal/testkit"
	"github.com/yscale-sh/yscale/pkg/backends"
)

const (
	// shortLease is the store's minimum accepted lease. A crashed worker's
	// claim can only come back through real expiry, so these fixtures wait for
	// the database clock rather than rewriting timestamps.
	shortLease = time.Second
	leaseGrace = 1500 * time.Millisecond
)

// TestPostgresCrashConvergence drives real lifecycle.Store transitions and a
// real backends.Backend across simulated process death. Every assertion reads
// committed database state or provider state; there is no in-memory lifecycle
// model anywhere in this file.
func TestPostgresCrashConvergence(t *testing.T) {
	f := newFixture(t)
	ctx := f.ctx

	t.Run("crash after admission commit keeps exactly one claimable create", func(t *testing.T) {
		f.reset(t)
		req := admission("crash_after_admission")
		worker := f.worker(t, testkit.CrashPlan{Checkpoint: testkit.AfterAdmission, Occurrence: 1})

		resp, err := worker.AdmitWorkload(ctx, req)
		if !errors.Is(err, testkit.ErrInjectedCrash) {
			t.Fatalf("admission err = %v, want ErrInjectedCrash", err)
		}
		if resp.WorkloadID != "" || resp.Inserted {
			t.Fatalf("crash returned %+v; the response must be withheld", resp)
		}
		// The admission transaction committed before the process died.
		for table, want := range map[string]int{
			"workloads": 1, "bursts": 1, "operations": 1, "outbox": 1,
		} {
			if got := f.countRows(t, table); got != want {
				t.Fatalf("%s rows = %d, want %d", table, got, want)
			}
		}

		restarted := f.restart(t, worker, testkit.CrashPlan{})
		replay, err := restarted.AdmitWorkload(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if replay.Inserted || replay.WorkloadID != req.WorkloadID || replay.BurstID != req.BurstID {
			t.Fatalf("replay = %+v, want the committed identity without a second insert", replay)
		}
		if got := f.countRows(t, "operations"); got != 1 {
			t.Fatalf("operations = %d, want the replay to add none", got)
		}

		claimed, ok, err := restarted.ClaimProviderCreate(ctx, time.Minute)
		if err != nil || !ok {
			t.Fatalf("claim after restart: ok=%v err=%v", ok, err)
		}
		if claimed.BurstID != req.BurstID || claimed.Attempts != 1 || claimed.LeaseToken == "" {
			t.Fatalf("claim = %+v", claimed)
		}
		if _, ok, err := restarted.ClaimProviderCreate(ctx, time.Minute); err != nil || ok {
			t.Fatalf("second claim ok=%v err=%v, want no duplicate work", ok, err)
		}
	})

	t.Run("crash after create claim stays ambiguous after lease expiry", func(t *testing.T) {
		f.reset(t)
		req := admission("crash_after_create_claim")
		worker := f.worker(t, testkit.CrashPlan{Checkpoint: testkit.AfterProviderCreateClaim, Occurrence: 1})
		if _, err := worker.AdmitWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}

		claimed, ok, err := worker.ClaimProviderCreate(ctx, shortLease)
		if !errors.Is(err, testkit.ErrInjectedCrash) || ok || claimed.LeaseToken != "" {
			t.Fatalf("claim = %+v ok=%v err=%v; the lease token must be lost", claimed, ok, err)
		}
		id, state, attempts, _ := f.createRow(t)
		if state != lifecycle.OperationProcessing || attempts != 1 {
			t.Fatalf("operation state=%s attempts=%d, want a committed held lease", state, attempts)
		}

		restarted := f.restart(t, worker, testkit.CrashPlan{})
		if _, ok, err := restarted.ClaimProviderCreate(ctx, time.Minute); err != nil || ok {
			t.Fatalf("unexpired lease was reclaimed: ok=%v err=%v", ok, err)
		}
		time.Sleep(leaseGrace)

		if _, ok, err := restarted.ClaimProviderCreate(ctx, time.Minute); err != nil || ok {
			t.Fatalf("expired dispatched create was reclaimed: ok=%v err=%v", ok, err)
		}
		storedID, storedState, storedAttempts, resourceID := f.createRow(t)
		if storedID != id || storedState != lifecycle.OperationProcessing || storedAttempts != 1 || resourceID != "" {
			t.Fatalf("stranded operation = id=%d state=%s attempts=%d resource=%q, want %d/processing/1/empty",
				storedID, storedState, storedAttempts, resourceID, id)
		}
	})

	t.Run("ambiguous provider create converges on the provider-assigned identity", func(t *testing.T) {
		f.reset(t)
		req := admission("ambiguous_create")
		const assigned = "linode_51001"
		provider := newProvider(t, testkit.FaultPlan{
			CreateMode:        testkit.CreateAccepted,
			ServerAssignedIDs: []string{assigned},
		})
		worker := f.worker(t, testkit.CrashPlan{Checkpoint: testkit.AfterProviderCreateCall, Occurrence: 1})
		decorated := newDecorator(t, provider, worker.Crash())

		if _, err := worker.AdmitWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}
		claimed, ok, err := worker.ClaimProviderCreate(ctx, shortLease)
		if err != nil || !ok {
			t.Fatalf("claim: ok=%v err=%v", ok, err)
		}
		spec := nodeSpec(req)
		backendID, err := decorated.CreateNode(ctx, spec)
		if !errors.Is(err, testkit.ErrInjectedCrash) || backendID != "" {
			t.Fatalf("create = %q, %v; the identity must be withheld", backendID, err)
		}
		// The provider accepted and holds the resource we are now billed for.
		if got := len(provider.Resources()); got != 1 {
			t.Fatalf("provider resources = %d, want the accepted create preserved", got)
		}
		if _, _, _, resourceID := f.createRow(t); resourceID != "" {
			t.Fatalf("operation recorded provider ID %q without a receipt", resourceID)
		}

		restarted := f.restart(t, worker, testkit.CrashPlan{})
		time.Sleep(leaseGrace)
		if _, ok, err := restarted.ClaimProviderCreate(ctx, time.Minute); err != nil || ok {
			t.Fatalf("ambiguous create was reclaimed for a second provider call: ok=%v err=%v", ok, err)
		}

		class, owned, err := provider.ClassifyCreateAmbiguity(ctx, testkit.OwnershipFromSpec(spec))
		if err != nil {
			t.Fatalf("inventory: %v", err)
		}
		if class != testkit.AmbiguityOne || len(owned) != 1 {
			t.Fatalf("inventory = %s %+v, want exactly one owned resource", class, owned)
		}
		observed := owned[0].ProviderID
		if observed != assigned || observed == spec.BurstID || observed == spec.Name {
			t.Fatalf("observed %q, want the provider-assigned identity", observed)
		}

		if got := len(provider.Resources()); got != 1 {
			t.Fatalf("provider resources = %d, want exactly one", got)
		}
		if got := createCalls(provider); got != 1 {
			t.Fatalf("provider CreateNode calls = %d, want no second create", got)
		}
		storedID, state, attempts, resourceID := f.createRow(t)
		if storedID != claimed.ID || state != lifecycle.OperationProcessing || attempts != 1 || resourceID != "" {
			t.Fatalf("ambiguous operation = id=%d state=%s attempts=%d resource=%q", storedID, state, attempts, resourceID)
		}
		burstState, burstResource := f.burstRow(t, req)
		if burstState != lifecycle.BurstCreateRequested || burstResource != "" {
			t.Fatalf("burst = %s/%q, want create_requested until reconciliation adopts the observed identity", burstState, burstResource)
		}
	})

	t.Run("create success is recorded once when the mark receipt is lost", func(t *testing.T) {
		f.reset(t)
		req := admission("create_mark_lost")
		const assigned = "linode_51002"
		worker := f.worker(t, testkit.CrashPlan{Checkpoint: testkit.AfterProviderCreateMark, Occurrence: 1})
		if _, err := worker.AdmitWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}
		claimed, ok, err := worker.ClaimProviderCreate(ctx, time.Minute)
		if err != nil || !ok {
			t.Fatalf("claim: ok=%v err=%v", ok, err)
		}
		if err := worker.MarkProviderCreateSucceeded(ctx, claimed.ID, claimed.LeaseToken, assigned); !errors.Is(err, testkit.ErrInjectedCrash) {
			t.Fatalf("mark err = %v, want ErrInjectedCrash", err)
		}
		// The mark committed; only its acknowledgement was lost.
		_, state, _, resourceID := f.createRow(t)
		if state != lifecycle.OperationSucceeded || resourceID != assigned {
			t.Fatalf("operation = %s/%q, want a committed success", state, resourceID)
		}

		restarted := f.restart(t, worker, testkit.CrashPlan{})
		if err := restarted.MarkProviderCreateSucceeded(ctx, claimed.ID, claimed.LeaseToken, assigned); !errors.Is(err, lifecycle.ErrLeaseLost) {
			t.Fatalf("replayed mark err = %v, want ErrLeaseLost", err)
		}
		for eventType, want := range map[string]int{
			"provider_create.succeeded": 1,
			"burst.provider_created":    1,
		} {
			if got := f.countEvents(t, eventType); got != want {
				t.Fatalf("%s events = %d, want %d", eventType, got, want)
			}
		}
		burstState, burstResource := f.burstRow(t, req)
		if burstState != lifecycle.BurstProviderCreated || burstResource != assigned {
			t.Fatalf("burst = %s/%q", burstState, burstResource)
		}
		if _, ok, err := restarted.ClaimProviderCreate(ctx, time.Minute); err != nil || ok {
			t.Fatalf("succeeded operation was re-claimed: ok=%v err=%v", ok, err)
		}
	})

	t.Run("crash after delete claim is recovered only by lease expiry", func(t *testing.T) {
		f.reset(t)
		req := admission("crash_after_delete_claim")
		const assigned = "linode_52001"
		worker := f.worker(t, testkit.CrashPlan{Checkpoint: testkit.AfterProviderDeleteClaim, Occurrence: 1})
		f.provision(t, worker, req, assigned)

		requested, err := worker.RequestProviderDelete(ctx, deleteRequest(req))
		if err != nil {
			t.Fatal(err)
		}
		if !requested.Inserted || requested.State != lifecycle.ProviderDeleteQueued {
			t.Fatalf("delete request = %+v", requested)
		}

		record, ok, err := worker.ClaimProviderDelete(ctx, shortLease)
		if !errors.Is(err, testkit.ErrInjectedCrash) || ok || record.LeaseToken != "" {
			t.Fatalf("claim = %+v ok=%v err=%v", record, ok, err)
		}

		restarted := f.restart(t, worker, testkit.CrashPlan{})
		if _, ok, err := restarted.ClaimProviderDelete(ctx, time.Minute); err != nil || ok {
			t.Fatalf("unexpired delete lease was reclaimed: ok=%v err=%v", ok, err)
		}
		time.Sleep(leaseGrace)

		reclaimed, ok, err := restarted.ClaimProviderDelete(ctx, time.Minute)
		if err != nil || !ok {
			t.Fatalf("reclaim after expiry: ok=%v err=%v", ok, err)
		}
		if reclaimed.ID != requested.DeleteID || reclaimed.ProviderResourceID != assigned {
			t.Fatalf("reclaimed = %+v, want delete %d for %q", reclaimed, requested.DeleteID, assigned)
		}
		if reclaimed.Attempts != 1 {
			t.Fatalf("reclaim consumed an attempt: attempts=%d", reclaimed.Attempts)
		}
		if reclaimed.DeletedAt != nil {
			t.Fatal("delete receipt stamped without a provider delete")
		}
		burstState, _ := f.burstRow(t, req)
		if burstState != lifecycle.BurstProviderCreated {
			t.Fatalf("burst = %s, want a live-cost obligation until deletion is proven", burstState)
		}
	})

	t.Run("lost delete receipt terminalizes once after observing provider absence", func(t *testing.T) {
		f.reset(t)
		req := admission("delete_receipt_lost")
		const assigned = "linode_52002"
		provider := newProvider(t, testkit.FaultPlan{
			CreateMode:        testkit.CreateAccepted,
			ServerAssignedIDs: []string{assigned},
		})
		if _, err := provider.CreateNode(ctx, nodeSpec(req)); err != nil {
			t.Fatal(err)
		}

		worker := f.worker(t, testkit.CrashPlan{Checkpoint: testkit.AfterProviderDeleteCall, Occurrence: 1})
		decorated := newDecorator(t, provider, worker.Crash())
		f.provision(t, worker, req, assigned)

		// Command dispatch and workload completion have no durable store API in
		// this package yet, so the harness only marks those positions.
		f.mark(t, worker, testkit.BeforeCommandDispatch, testkit.AfterCommandDispatch,
			testkit.BeforeWorkloadCompletion, testkit.AfterWorkloadCompletion)

		requested, err := worker.RequestProviderDelete(ctx, deleteRequest(req))
		if err != nil {
			t.Fatal(err)
		}
		claimed, ok, err := worker.ClaimProviderDelete(ctx, shortLease)
		if err != nil || !ok {
			t.Fatalf("claim: ok=%v err=%v", ok, err)
		}
		if err := decorated.DeleteNode(ctx, claimed.ProviderResourceID); !errors.Is(err, testkit.ErrInjectedCrash) {
			t.Fatalf("delete err = %v, want ErrInjectedCrash", err)
		}
		// The provider really destroyed it; the receipt was withheld.
		if got := len(provider.Resources()); got != 0 {
			t.Fatalf("provider resources = %d, want 0", got)
		}
		if record := f.getDelete(t, req); record.State != lifecycle.ProviderDeleteDeleting || record.DeletedAt != nil {
			t.Fatalf("delete record = %s deleted_at=%v, want an unproven delete", record.State, record.DeletedAt)
		}

		restarted := f.restart(t, worker, testkit.CrashPlan{})
		recovered := newDecorator(t, provider, restarted.Crash())
		time.Sleep(leaseGrace)
		reclaimed, ok, err := restarted.ClaimProviderDelete(ctx, time.Minute)
		if err != nil || !ok {
			t.Fatalf("reclaim: ok=%v err=%v", ok, err)
		}
		if reclaimed.ID != requested.DeleteID {
			t.Fatalf("reclaimed delete %d, want %d", reclaimed.ID, requested.DeleteID)
		}
		// Replay is idempotent, matching the live providers' already-absent
		// contract. Inventory/status supplies the explicit absence observation.
		if err := recovered.DeleteNode(ctx, reclaimed.ProviderResourceID); err != nil {
			t.Fatalf("replayed delete err = %v, want idempotent success", err)
		}
		if _, err := recovered.GetNodeStatus(ctx, reclaimed.ProviderResourceID); !errors.Is(err, testkit.ErrProviderResourceAbsent) {
			t.Fatalf("status after replay err = %v, want provider absence", err)
		}
		if _, err := restarted.MarkProviderDeleteSucceeded(ctx, reclaimed.ID, reclaimed.LeaseToken); err != nil {
			t.Fatal(err)
		}

		final := f.getDelete(t, req)
		if final.State != lifecycle.ProviderDeleteTerminated || final.DeletedAt == nil {
			t.Fatalf("delete record = %s deleted_at=%v", final.State, final.DeletedAt)
		}
		burstState, _ := f.burstRow(t, req)
		if burstState != lifecycle.BurstTerminated {
			t.Fatalf("burst = %s, want terminated", burstState)
		}
		// Terminalize exactly once, even though the worker replayed.
		if _, err := restarted.MarkProviderDeleteSucceeded(ctx, reclaimed.ID, reclaimed.LeaseToken); !errors.Is(err, lifecycle.ErrLeaseLost) {
			t.Fatalf("replayed terminalization err = %v, want ErrLeaseLost", err)
		}
		for eventType, want := range map[string]int{
			"provider_delete.terminated": 1,
			"burst.terminated":           1,
		} {
			if got := f.countEvents(t, eventType); got != want {
				t.Fatalf("%s events = %d, want %d", eventType, got, want)
			}
		}
		if _, ok, err := restarted.ClaimProviderDelete(ctx, time.Minute); err != nil || ok {
			t.Fatalf("terminated delete was re-claimed: ok=%v err=%v", ok, err)
		}
		f.mark(t, restarted, testkit.BeforeSettlement, testkit.AfterSettlement)
	})

	t.Run("persistent provider delete failures reach durable manual attention", func(t *testing.T) {
		f.reset(t)
		req := admission("delete_manual_attention")
		const assigned = "linode_52003"
		provider := newProvider(t, testkit.FaultPlan{
			CreateMode:        testkit.CreateAccepted,
			ServerAssignedIDs: []string{assigned},
			DeleteAlwaysFails: true,
		})
		if _, err := provider.CreateNode(ctx, nodeSpec(req)); err != nil {
			t.Fatal(err)
		}
		worker := f.worker(t, testkit.CrashPlan{})
		decorated := newDecorator(t, provider, worker.Crash())
		f.provision(t, worker, req, assigned)
		if _, err := worker.RequestProviderDelete(ctx, deleteRequest(req)); err != nil {
			t.Fatal(err)
		}

		const maxAttempts = 2
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			claimed, ok, err := worker.ClaimProviderDelete(ctx, time.Minute)
			if err != nil || !ok {
				t.Fatalf("attempt %d claim: ok=%v err=%v", attempt, ok, err)
			}
			if claimed.Attempts != attempt {
				t.Fatalf("attempt %d recorded attempts=%d", attempt, claimed.Attempts)
			}
			if err := decorated.DeleteNode(ctx, claimed.ProviderResourceID); !errors.Is(err, testkit.ErrDeleteTransient) {
				t.Fatalf("attempt %d delete err = %v", attempt, err)
			}
			if err := worker.MarkProviderDeleteFailed(ctx, claimed.ID, claimed.LeaseToken,
				"provider rejected the delete", time.Now().Add(shortLease), maxAttempts); err != nil {
				t.Fatalf("attempt %d mark failed: %v", attempt, err)
			}
			if attempt < maxAttempts {
				if record := f.getDelete(t, req); record.State != lifecycle.ProviderDeleteRetrying {
					t.Fatalf("attempt %d left state %s, want retrying", attempt, record.State)
				}
				time.Sleep(leaseGrace)
			}
		}

		exhausted := f.getDelete(t, req)
		if exhausted.State != lifecycle.ProviderDeleteManualAttention {
			t.Fatalf("state = %s, want manual_attention", exhausted.State)
		}
		if exhausted.DeletedAt != nil || exhausted.LastError == "" {
			t.Fatalf("record = %+v, want no receipt and a retained error", exhausted)
		}
		// The burst is still a live-cost obligation: nothing was terminalized.
		burstState, burstResource := f.burstRow(t, req)
		if burstState != lifecycle.BurstProviderCreated || burstResource != assigned {
			t.Fatalf("burst = %s/%q, want the untouched provider-created burst", burstState, burstResource)
		}
		if got := len(provider.Resources()); got != 1 {
			t.Fatalf("provider resources = %d, want the undeleted resource", got)
		}
		if got := f.countEvents(t, "burst.terminated"); got != 0 {
			t.Fatalf("burst.terminated events = %d, want 0", got)
		}
		if _, ok, err := worker.ClaimProviderDelete(ctx, time.Minute); err != nil || ok {
			t.Fatalf("manual-attention delete stayed claimable: ok=%v err=%v", ok, err)
		}

		// The store's real operator path is the only way out.
		queued, err := f.store.ListProviderDeleteManualAttention(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(queued) != 1 || queued[0].BurstID != req.BurstID {
			t.Fatalf("manual-attention queue = %+v", queued)
		}
		requeued, err := f.store.RetryProviderDelete(ctx, req.CustomerID, req.ClusterID, req.BurstID, "operator", req.TraceID)
		if err != nil {
			t.Fatal(err)
		}
		if requeued.State != lifecycle.ProviderDeleteQueued || requeued.Generation != exhausted.Generation+1 {
			t.Fatalf("requeued = %s generation=%d, want queued at generation %d",
				requeued.State, requeued.Generation, exhausted.Generation+1)
		}
	})

	t.Run("inventory failure leaves the create explicitly inconclusive", func(t *testing.T) {
		f.reset(t)
		req := admission("inventory_inconclusive")
		provider := newProvider(t, testkit.FaultPlan{
			CreateMode:         testkit.CreateAccepted,
			ServerAssignedIDs:  []string{"linode_53001"},
			InventoryExhausted: true,
		})
		worker := f.worker(t, testkit.CrashPlan{Checkpoint: testkit.AfterProviderCreateCall, Occurrence: 1})
		decorated := newDecorator(t, provider, worker.Crash())

		if _, err := worker.AdmitWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := worker.ClaimProviderCreate(ctx, shortLease); err != nil || !ok {
			t.Fatalf("claim: ok=%v err=%v", ok, err)
		}
		spec := nodeSpec(req)
		if _, err := decorated.CreateNode(ctx, spec); !errors.Is(err, testkit.ErrInjectedCrash) {
			t.Fatalf("create err = %v", err)
		}

		restarted := f.restart(t, worker, testkit.CrashPlan{})
		time.Sleep(leaseGrace)
		if _, ok, err := restarted.ClaimProviderCreate(ctx, time.Minute); err != nil || ok {
			t.Fatalf("create with unreadable inventory was reclaimed: ok=%v err=%v", ok, err)
		}

		class, owned, err := provider.ClassifyCreateAmbiguity(ctx, testkit.OwnershipFromSpec(spec))
		if !errors.Is(err, testkit.ErrInventoryUnavailable) || class != testkit.AmbiguityUnknown || owned != nil {
			t.Fatalf("classification = %s %+v %v, want an explicit inconclusive error", class, owned, err)
		}

		// Inconclusive is not known failure and cannot be made retryable: the
		// provider may hold a machine, so the dispatched operation remains
		// fenced for reconciliation/manual attention.
		_, state, attempts, resourceID := f.createRow(t)
		if state != lifecycle.OperationProcessing {
			t.Fatalf("operation state = %s, want an ambiguous processing operation", state)
		}
		if attempts != 1 || resourceID != "" {
			t.Fatalf("operation attempts=%d provider_resource_id=%q", attempts, resourceID)
		}
		burstState, burstResource := f.burstRow(t, req)
		if burstState != lifecycle.BurstCreateRequested || burstResource != "" {
			t.Fatalf("burst = %s/%q, want an untouched create_requested burst", burstState, burstResource)
		}
		for _, eventType := range []string{"provider_create.dead_letter", "burst.manual_attention", "workload.failed"} {
			if got := f.countEvents(t, eventType); got != 0 {
				t.Fatalf("%s events = %d, want 0 on an unread inventory", eventType, got)
			}
		}
		// The possibly-orphaned resource is still there and still unaccounted for.
		if got := len(provider.Resources()); got != 1 {
			t.Fatalf("provider resources = %d, want the unreconciled resource preserved", got)
		}
	})
}

// fixture owns the real pool and the real store. Restarts rebuild only the
// in-process worker; the database and the provider account persist.
type fixture struct {
	ctx   context.Context
	pool  *pgxpool.Pool
	store *lifecycle.Store
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dsn := os.Getenv("LIFECYCLE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set LIFECYCLE_TEST_DATABASE_URL to an isolated Postgres database")
	}
	if os.Getenv("LIFECYCLE_TEST_ALLOW_DESTRUCTIVE") != "DROP_LIFECYCLE_SCHEMA" {
		t.Fatal("set LIFECYCLE_TEST_ALLOW_DESTRUCTIVE=DROP_LIFECYCLE_SCHEMA for the isolated test database")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var databaseName string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("LIFECYCLE_TEST_EXPECT_DATABASE"); expected == "" || expected != databaseName {
		t.Fatalf("LIFECYCLE_TEST_EXPECT_DATABASE must exactly match current database %q", databaseName)
	}
	if !strings.HasSuffix(databaseName, "_lifecycle_test") {
		t.Fatalf("refusing destructive integration test against non-test database %q", databaseName)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS lifecycle CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS lifecycle CASCADE`); err != nil {
			t.Errorf("cleanup lifecycle schema: %v", err)
		}
		pool.Close()
	})
	if err := lifecycle.EnsureProviderDeleteSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return &fixture{ctx: ctx, pool: pool, store: lifecycle.NewStore(pool)}
}

func (f *fixture) reset(t *testing.T) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `
		TRUNCATE lifecycle.lifecycle_events, lifecycle.provider_deletes, lifecycle.outbox,
		         lifecycle.operations, lifecycle.bursts, lifecycle.workloads
		RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) worker(t *testing.T, plan testkit.CrashPlan) *Store {
	t.Helper()
	controller, err := testkit.NewCrashController(plan)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(f.store, controller)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// restart builds the worker a restarted process would have: a fresh crash
// controller with no surviving in-memory state, over the same real store and
// the same real database.
func (f *fixture) restart(t *testing.T, previous *Store, plan testkit.CrashPlan) *Store {
	t.Helper()
	controller, err := previous.Crash().Restart(plan)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(f.store, controller)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// provision drives the real create path to provider_created.
func (f *fixture) provision(t *testing.T, worker *Store, req lifecycle.AdmissionRequest, providerResourceID string) {
	t.Helper()
	if _, err := worker.AdmitWorkload(f.ctx, req); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := worker.ClaimProviderCreate(f.ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("provision claim: ok=%v err=%v", ok, err)
	}
	if err := worker.MarkProviderCreateSucceeded(f.ctx, claimed.ID, claimed.LeaseToken, providerResourceID); err != nil {
		t.Fatal(err)
	}
}

// mark walks checkpoints that have no durable store API yet.
func (f *fixture) mark(t *testing.T, worker *Store, checkpoints ...testkit.Checkpoint) {
	t.Helper()
	for _, checkpoint := range checkpoints {
		if err := worker.Crash().Enter(checkpoint); err != nil {
			t.Fatalf("%s: %v", checkpoint, err)
		}
	}
}

func (f *fixture) countRows(t *testing.T, table string) int {
	t.Helper()
	var count int
	if err := f.pool.QueryRow(f.ctx, "SELECT count(*) FROM lifecycle."+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (f *fixture) countEvents(t *testing.T, eventType string) int {
	t.Helper()
	var count int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM lifecycle.lifecycle_events WHERE event_type=$1`,
		eventType).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (f *fixture) createRow(t *testing.T) (int64, string, int, string) {
	t.Helper()
	var id int64
	var state, resourceID string
	var attempts int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT id, state, attempts, provider_resource_id
		FROM lifecycle.operations
		WHERE operation_type='provider_create'`).
		Scan(&id, &state, &attempts, &resourceID); err != nil {
		t.Fatal(err)
	}
	return id, state, attempts, resourceID
}

func (f *fixture) burstRow(t *testing.T, req lifecycle.AdmissionRequest) (string, string) {
	t.Helper()
	var state, resourceID string
	if err := f.pool.QueryRow(f.ctx, `
		SELECT state, provider_resource_id FROM lifecycle.bursts
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3`,
		req.CustomerID, req.ClusterID, req.BurstID).Scan(&state, &resourceID); err != nil {
		t.Fatal(err)
	}
	return state, resourceID
}

func (f *fixture) getDelete(t *testing.T, req lifecycle.AdmissionRequest) lifecycle.ProviderDeleteRecord {
	t.Helper()
	record, err := f.store.GetProviderDelete(f.ctx, req.CustomerID, req.ClusterID, req.BurstID)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func newProvider(t *testing.T, plan testkit.FaultPlan) *testkit.FaultBackend {
	t.Helper()
	provider, err := testkit.NewFaultBackend("linode", plan)
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func newDecorator(t *testing.T, inner backends.Backend, controller *testkit.CrashController) *testkit.CrashBackend {
	t.Helper()
	decorated, err := testkit.NewCrashBackend(inner, controller)
	if err != nil {
		t.Fatal(err)
	}
	return decorated
}

func createCalls(provider *testkit.FaultBackend) int {
	calls := 0
	for _, call := range provider.CallHistory() {
		if call.Operation == testkit.OperationCreate {
			calls++
		}
	}
	return calls
}

func admission(suffix string) lifecycle.AdmissionRequest {
	return lifecycle.AdmissionRequest{
		CustomerID:            "cust_" + suffix,
		ClusterID:             "cluster_" + suffix,
		IdempotencyKey:        "kube:cluster_" + suffix + ":uid_" + suffix + ":1",
		CanonicalVersion:      1,
		WorkloadID:            "wl_" + suffix,
		BurstID:               "burst_" + suffix,
		WorkloadSpec:          []byte(fmt.Sprintf(`{"image":"busybox","suffix":%q}`, suffix)),
		BurstSpec:             []byte(fmt.Sprintf(`{"cpu":1,"suffix":%q}`, suffix)),
		Provider:              "linode",
		Region:                "us-east",
		SKU:                   "g6-standard-1",
		ProviderCreatePayload: []byte(fmt.Sprintf(`{"burst_id":"burst_%s"}`, suffix)),
		Actor:                 "crash-harness",
		TraceID:               "trace_" + suffix,
	}
}

func deleteRequest(req lifecycle.AdmissionRequest) lifecycle.ProviderDeleteRequest {
	return lifecycle.ProviderDeleteRequest{
		CustomerID: req.CustomerID,
		ClusterID:  req.ClusterID,
		BurstID:    req.BurstID,
		Reason:     "workload completed",
		Actor:      "crash-harness",
		TraceID:    req.TraceID,
	}
}

func nodeSpec(req lifecycle.AdmissionRequest) *backends.NodeSpec {
	return &backends.NodeSpec{
		Name:       "ys-" + req.BurstID,
		BurstID:    req.BurstID,
		Region:     req.Region,
		Lifecycle:  backends.LifecycleCold,
		ScopeHash:  "scope_" + req.ClusterID,
		ConfigHash: "config_" + req.ClusterID,
		NodeLabels: map[string]string{"customer": req.CustomerID},
	}
}
