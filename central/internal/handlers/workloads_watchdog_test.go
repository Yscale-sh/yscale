package handlers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

type fakeReaper struct {
	mu        sync.Mutex
	teardowns []string
	err       error // when non-nil, Teardown records the attempt then fails
	mesh      []string
	meshErr   error
}

func (f *fakeReaper) Teardown(ctx context.Context, b *state.Burst) error {
	return errors.Join(f.DeleteProviderNode(ctx, b), f.CleanupMesh(ctx, b))
}

func (f *fakeReaper) DeleteProviderNode(_ context.Context, b *state.Burst) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.teardowns = append(f.teardowns, b.ID)
	return f.err
}

func (f *fakeReaper) CleanupMesh(_ context.Context, b *state.Burst) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mesh = append(f.mesh, b.ID)
	return f.meshErr
}

func (f *fakeReaper) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeReaper) setMeshErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.meshErr = err
}

func (f *fakeReaper) meshCleanupCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return countString(f.mesh, id)
}

func (f *fakeReaper) reaped(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.teardowns, id)
}

func (f *fakeReaper) teardownCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return countString(f.teardowns, id)
}

func countString(values []string, want string) int {
	n := 0
	for _, value := range values {
		if value == want {
			n++
		}
	}
	return n
}

// TestSweepExpiredBursts proves the watchdog reaps only over-budget bursts and
// never touches a healthy or unbudgeted one (the nodeOnly-capacity safety
// property) — unless the opt-in global safety net is enabled.
func TestSweepExpiredBursts(t *testing.T) {
	store := state.New()
	reaper := &fakeReaper{}
	h := &Workloads{
		Store:  store,
		Reaper: reaper,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		// Cost intentionally nil — reapBurst must tolerate it.
	}

	old := time.Now().Add(-2 * time.Hour).UTC()
	recent := time.Now().Add(-10 * time.Minute).UTC()

	add := func(id string, createdAt time.Time, deadline time.Duration, maxUSD, hourly float64) {
		store.PutBurst(&state.Burst{
			ID: id, CustomerID: "cust_test", Backend: "linode", BackendID: "x" + id,
			CreatedAt: createdAt, Status: "provisioning", HourlyUSD: hourly,
			Deadline: deadline, MaxUSD: maxUSD,
		})
		store.PutWorkload(&state.Workload{
			ID: "wl_" + id, CustomerID: "cust_test", BurstID: id,
			Status: "provisioning", CreatedAt: createdAt,
		})
	}

	add("deadline_hit", old, time.Hour, 0, 0.62)   // age 2h > 1h deadline → reap
	add("deadline_ok", recent, time.Hour, 0, 0.62) // age 10m < 1h → keep
	add("budget_hit", old, 0, 1.00, 0.62)          // accrued ~$1.24 > $1.00 → reap
	add("budget_ok", recent, 0, 1.00, 0.62)        // accrued ~$0.10 < $1.00 → keep
	add("unbudgeted", old, 0, 0, 0.62)             // no budget, no safety net → keep

	// Safety net disabled (0): only declared budgets are enforced.
	h.sweepExpiredBursts(context.Background(), 0, 0)

	for _, id := range []string{"deadline_hit", "budget_hit"} {
		if !reaper.reaped(id) {
			t.Errorf("%s: expected reaped", id)
		}
		if _, err := store.GetBurst(id); err == nil {
			t.Errorf("%s: burst record should be deleted after reap", id)
		}
		w, err := store.GetWorkload("wl_" + id)
		if err != nil || w.Status != "failed" || w.FinishedAt == nil {
			t.Errorf("%s: workload should be marked failed, got %+v (err %v)", id, w, err)
		}
	}
	for _, id := range []string{"deadline_ok", "budget_ok", "unbudgeted"} {
		if reaper.reaped(id) {
			t.Errorf("%s: should NOT be reaped", id)
		}
		if _, err := store.GetBurst(id); err != nil {
			t.Errorf("%s: burst should still exist, got %v", id, err)
		}
	}

	// Enable the opt-in global safety net (1h): the unbudgeted 2h-old burst now reaps.
	h.sweepExpiredBursts(context.Background(), time.Hour, 0)
	if !reaper.reaped("unbudgeted") {
		t.Error("unbudgeted: expected reaped once the safety net is enabled")
	}
	if _, err := store.GetBurst("unbudgeted"); err == nil {
		t.Error("unbudgeted: burst should be deleted under safety net")
	}
}

// The backstop for a connector that never comes back. nodeOnly capacity is
// ended by a signal only the customer's cluster can send — node removed, or node
// idle — so a dead connector takes the whole teardown path with it and the burst
// bills until something else stops it. This is that something else, and it runs
// through the same claim/reap/finish path every other teardown does.
//
// It measures SILENCE, not age — and specifically the silence of OCCUPANCY
// observations, the claim that some connector can still see whether the node is
// idle. A node that is up and busy under a connector with cluster-wide pod
// visibility is observed continuously and never expires here however long it
// runs, which is the point for capacity behind a Deployment meant to stay up.
//
// Node HEALTH is deliberately not enough. A namespace-scoped connector, and one
// that has lost cluster-wide pod LIST, both go on reporting Ready forever while
// being unable to ever ask for the teardown — so reading health as liveness kept
// the ceiling open for capacity nothing was watching.
//
// And a burst that has NEVER been observed by a connector that never promised to
// is not silent, it is unwatched: the shipped chart is namespaced, so the default
// install was never granted the visibility that makes the claim. Aging one out
// from CreatedAt destroyed live nodes at 6h for a capability they never had, so
// the ceiling does not start for those at all — admission bounds them with a
// deadline instead. Every burst here made no such promise; the ones that did are
// covered by TestSweepExpiredBursts_NodeOnlyPromisedObservationsMeasureFromCreation.
// Explicit budgets are unaffected by any of this.
func TestSweepExpiredBursts_NodeOnlyBackstopMeasuresObservationSilence(t *testing.T) {
	store := state.New()
	reaper := &fakeReaper{}
	h := &Workloads{
		Store:  store,
		Reaper: reaper,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	// healthAgo and observedAgo are separate on purpose: negative means never.
	add := func(id string, age time.Duration, nodeOnly bool, deadline, healthAgo, observedAgo time.Duration) {
		createdAt := time.Now().Add(-age).UTC()
		b := &state.Burst{
			ID: id, CustomerID: "cust_test", Backend: "linode", BackendID: "x" + id,
			CreatedAt: createdAt, Status: "provisioning", HourlyUSD: 0.62,
			NodeOnly: nodeOnly, Deadline: deadline,
		}
		if healthAgo >= 0 {
			at := time.Now().Add(-healthAgo).UTC()
			b.NodePhaseAt = &at
			b.NodePhase = protocol.NodePhaseReady
		}
		if observedAgo >= 0 {
			at := time.Now().Add(-observedAgo).UTC()
			b.OccupancyObservedAt = &at
		}
		store.PutBurst(b)
		store.PutWorkload(&state.Workload{
			ID: "wl_" + id, CustomerID: "cust_test", BurstID: id,
			Status: "provisioning", CreatedAt: createdAt,
		})
	}

	// Up for a day and observed two minutes ago: a node the autoscaler is
	// actively using, which an age cap would destroy.
	add("nodeonly_busy_all_day", 24*time.Hour, true, 0, 2*time.Minute, 2*time.Minute)
	// Observed once, then the connector died. This is the ONLY shape the ceiling
	// acts on: a capability that was demonstrably there and has stopped.
	add("nodeonly_gone_quiet", 24*time.Hour, true, 0, 7*time.Hour, 7*time.Hour)
	// Observed once and then quiet, with a declared deadline longer than the
	// ceiling. The ceiling still wins: the connector that was supposed to end the
	// burst early is the thing presumed dead.
	add("nodeonly_quiet_long_deadline", 24*time.Hour, true, 48*time.Hour, 7*time.Hour, 7*time.Hour)
	// Never observed at all. NOT reaped, and this is the defect being fixed: the
	// shipped chart is namespaced, so the default connector was never granted the
	// pod visibility that makes this claim — and reading the missing claim as
	// silence destroyed live nodes at the ceiling for a feature they never had.
	add("nodeonly_never_observed", 8*time.Hour, true, 0, -1, -1)
	// Same, with node health arriving perfectly freshly. Health is not the claim,
	// and its absence is not one either: neither may start the ceiling.
	add("nodeonly_healthy_but_unwatched", 8*time.Hour, true, 0, time.Minute, -1)
	// Never observed, but with a declared deadline it has passed. The explicit
	// budget is untouched by any of this and still ends the burst — which is what
	// a namespace-scoped install is told to rely on.
	add("nodeonly_never_observed_past_deadline", 8*time.Hour, true, time.Hour, -1, -1)
	// Not nodeOnly; the nodeOnly backstop is not its business.
	add("managed_old", 8*time.Hour, false, 0, -1, -1)

	// Safety net off, nodeOnly silence ceiling at the shipped 6h default.
	h.sweepExpiredBursts(context.Background(), 0, 6*time.Hour)

	if !reaper.reaped("nodeonly_gone_quiet") {
		t.Error("nodeonly_gone_quiet: a nodeOnly burst whose connector stopped observing must be reaped")
	}
	if !reaper.reaped("nodeonly_quiet_long_deadline") {
		t.Error("nodeonly_quiet_long_deadline: a longer declared deadline must not outlive the silence ceiling once observations existed")
	}
	if !reaper.reaped("nodeonly_never_observed_past_deadline") {
		t.Error("nodeonly_never_observed_past_deadline: an explicit declared budget still applies with or without observations")
	}
	for _, id := range []string{
		"nodeonly_busy_all_day",
		"nodeonly_never_observed",
		"nodeonly_healthy_but_unwatched",
		"managed_old",
	} {
		if reaper.reaped(id) {
			t.Errorf("%s: should NOT be reaped", id)
		}
		if _, err := store.GetBurst(id); err != nil {
			t.Errorf("%s: burst should still exist, got %v", id, err)
		}
	}
	// The finish goes through the same durable path, so the customer's workload
	// stops reading "provisioning" for a node that no longer exists.
	wl, err := store.GetWorkload("wl_nodeonly_gone_quiet")
	if err != nil || wl.Status != "failed" || wl.FinishedAt == nil {
		t.Fatalf("workload = %+v err=%v, want a terminal record after the backstop reap", wl, err)
	}
}

// The connector that promised and then died before it could deliver. Admission
// recorded the promise on the burst (OccupancyObservationExpected), so silence
// here is a capability that was there to be lost — and measuring it from
// CreatedAt is the only thing that ends a burst whose connector never survived
// long enough to send its first 5m observation.
//
// It is exactly the case the plain never-observed bursts above must NOT be
// treated as. The difference is the promise, and the promise is what is written
// down.
func TestSweepExpiredBursts_NodeOnlyPromisedObservationsMeasureFromCreation(t *testing.T) {
	store := state.New()
	reaper := &fakeReaper{}
	h := &Workloads{
		Store:  store,
		Reaper: reaper,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	add := func(id string, age time.Duration, expected bool) {
		createdAt := time.Now().Add(-age).UTC()
		store.PutBurst(&state.Burst{
			ID: id, CustomerID: "cust_test", Backend: "linode", BackendID: "x" + id,
			CreatedAt: createdAt, Status: "provisioning", HourlyUSD: 0.62,
			NodeOnly: true, OccupancyObservationExpected: expected,
		})
		store.PutWorkload(&state.Workload{
			ID: "wl_" + id, CustomerID: "cust_test", BurstID: id,
			Status: "provisioning", CreatedAt: createdAt,
		})
	}

	// Promised, never delivered, past the ceiling: the connector died between
	// admission and its first sweep.
	add("promised_never_delivered", 8*time.Hour, true)
	// Promised and still inside the ceiling. A connector that is simply young must
	// not be mistaken for a dead one.
	add("promised_still_fresh", 2*time.Hour, true)
	// Promised nothing. Unchanged: this is the namespaced install, bounded by the
	// deadline admission gave it and never by this ceiling.
	add("promised_nothing", 8*time.Hour, false)

	h.sweepExpiredBursts(context.Background(), 0, 6*time.Hour)

	if !reaper.reaped("promised_never_delivered") {
		t.Error("promised_never_delivered: a connector that promised observations and sent none must not hold capacity open forever")
	}
	for _, id := range []string{"promised_still_fresh", "promised_nothing"} {
		if reaper.reaped(id) {
			t.Errorf("%s: should NOT be reaped", id)
		}
		if _, err := store.GetBurst(id); err != nil {
			t.Errorf("%s: burst should still exist, got %v", id, err)
		}
	}
	wl, err := store.GetWorkload("wl_promised_never_delivered")
	if err != nil || wl.Status != "failed" || wl.FinishedAt == nil {
		t.Fatalf("workload = %+v err=%v, want a terminal record after the backstop reap", wl, err)
	}
}

// Zero disables it, consistently with the safety net beside it and with every
// other duration this deployment reads from the environment. An operator who
// sets YSCALE_NODE_ONLY_MAX_LIFETIME=0 has chosen to let an unobserved nodeOnly
// burst run indefinitely.
func TestSweepExpiredBursts_NodeOnlyMaxLifetimeDisabledByZero(t *testing.T) {
	store := state.New()
	reaper := &fakeReaper{}
	h := &Workloads{
		Store:  store,
		Reaper: reaper,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	createdAt := time.Now().Add(-72 * time.Hour).UTC()
	store.PutBurst(&state.Burst{
		ID: "nodeonly_ancient", CustomerID: "cust_test", Backend: "linode", BackendID: "xancient",
		CreatedAt: createdAt, Status: "provisioning", HourlyUSD: 0.62, NodeOnly: true,
	})

	h.sweepExpiredBursts(context.Background(), 0, 0)

	if reaper.reaped("nodeonly_ancient") {
		t.Error("a disabled nodeOnly cap still reaped")
	}
	if _, err := store.GetBurst("nodeonly_ancient"); err != nil {
		t.Errorf("burst should survive a disabled cap, got %v", err)
	}
}

// A declared budget that is SHORTER still wins: the ceiling only ever ends a
// burst nothing else has already ended.
func TestSweepExpiredBursts_NodeOnlyShorterDeadlineStillWins(t *testing.T) {
	store := state.New()
	reaper := &fakeReaper{}
	h := &Workloads{
		Store:  store,
		Reaper: reaper,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	createdAt := time.Now().Add(-90 * time.Minute).UTC()
	store.PutBurst(&state.Burst{
		ID: "nodeonly_short", CustomerID: "cust_test", Backend: "linode", BackendID: "xshort",
		CreatedAt: createdAt, Status: "provisioning", HourlyUSD: 0.62,
		NodeOnly: true, Deadline: time.Hour,
	})

	h.sweepExpiredBursts(context.Background(), 0, 6*time.Hour)

	if !reaper.reaped("nodeonly_short") {
		t.Error("a nodeOnly burst past its own declared deadline was not reaped")
	}
}

func TestSweepExpiredBursts_DeadlineOverridesSafetyNet(t *testing.T) {
	store := state.New()
	reaper := &fakeReaper{}
	h := &Workloads{
		Store:  store,
		Reaper: reaper,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	burst := &state.Burst{
		ID: "long_deadline", CustomerID: "cust_test", Backend: "linode", BackendID: "xlong_deadline",
		CreatedAt: time.Now().Add(-2 * time.Hour).UTC(), Status: "provisioning", HourlyUSD: 0.62,
		Deadline: 4 * time.Hour,
	}
	store.PutBurst(burst)
	store.PutWorkload(&state.Workload{
		ID: "wl_long_deadline", CustomerID: "cust_test", BurstID: burst.ID,
		Status: "provisioning", CreatedAt: burst.CreatedAt,
	})

	// The 90-minute safety net must not shorten the declared four-hour deadline.
	h.sweepExpiredBursts(context.Background(), 90*time.Minute, 0)
	if reaper.reaped(burst.ID) {
		t.Fatal("burst with a longer declared deadline was reaped by the safety net")
	}
	if _, err := store.GetBurst(burst.ID); err != nil {
		t.Fatalf("burst should survive until its declared deadline: %v", err)
	}

	burst.CreatedAt = time.Now().Add(-4*time.Hour - time.Minute).UTC()
	h.sweepExpiredBursts(context.Background(), 90*time.Minute, 0)
	if !reaper.reaped(burst.ID) {
		t.Fatal("burst should be reaped after its declared deadline")
	}
}

// TestReapBurst_RequeuesOnTeardownFailure pins the fix for the VM-leak bug:
// ClaimBurst removes the record atomically (race-safety), so if Teardown then
// fails — the backend DeleteNode hit a transient cloud error — the record must
// be RE-INSERTED and marked pending, not dropped. Otherwise an unbudgeted burst
// with the safety net disabled is never selected by the watchdog and the cloud
// VM runs (and bills) forever. A later sweep must retry and succeed.
func TestReapBurst_RequeuesOnTeardownFailure(t *testing.T) {
	store := state.New()
	reaper := &fakeReaper{err: errors.New("backend DeleteNode failed (transient)")}
	h := &Workloads{
		Store:  store,
		Reaper: reaper,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		// Cost nil: cost accrual shares the success branch with record
		// deletion, so retaining the record is sufficient to prove cost
		// didn't accrue on the failure path.
	}
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_test", Backend: "linode", BackendID: "node1", CreatedAt: time.Now()})
	store.PutWorkload(&state.Workload{ID: "wl_b1", CustomerID: "cust_test", BurstID: "b1", Status: "running", CreatedAt: time.Now()})

	// First attempt: teardown fails. Must NOT report success, must keep the record.
	if got := h.reapBurst(context.Background(), "b1", "complete"); got != ReapOutcomeRequeued {
		t.Errorf("reapBurst = %v, want ReapOutcomeRequeued: the delete failed and the record went back on the queue", got)
	}
	if _, err := store.GetBurst("b1"); err != nil {
		t.Fatal("burst record must be retained for retry after teardown failure (else the VM is orphaned and bills forever)")
	}
	if pending, err := store.GetBurst("b1"); err != nil || !pending.ReapPending {
		t.Fatalf("re-queued burst = %+v, err=%v; want durable reap-pending marker", pending, err)
	} else if pending.ReapPendingStatus != "failed" {
		t.Fatalf("reap-pending status = %q, want failed", pending.ReapPendingStatus)
	}
	if n := reaper.teardownCount("b1"); n != 1 {
		t.Fatalf("Teardown attempted %d times, want 1", n)
	}

	// Backend recovers. This burst has no deadline or spend cap, and both global
	// watchdog ceilings are disabled: the pending marker alone must select it.
	reaper.setErr(nil)
	h.sweepExpiredBursts(context.Background(), 0, 0)
	if _, err := store.GetBurst("b1"); err == nil {
		t.Error("unbudgeted reap-pending burst survived the watchdog retry")
	}
	if n := reaper.teardownCount("b1"); n != 2 {
		t.Fatalf("Teardown attempted %d times total, want 2 (one failed, one retried)", n)
	}
	if wl, err := store.GetWorkload("wl_b1"); err != nil || wl.Status != "failed" || wl.FinishedAt == nil {
		t.Fatalf("workload after watchdog retry = %+v, err=%v; want terminal failed", wl, err)
	}
}

func TestReapBurst_PendingIdleRetryCancelsWorkload(t *testing.T) {
	store := state.New()
	reaper := &fakeReaper{err: errors.New("provider unavailable")}
	h := &Workloads{Store: store, Reaper: reaper, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	created := time.Now().Add(-time.Hour)
	store.PutBurst(&state.Burst{
		ID: "b_idle", CustomerID: "cust_test", Backend: "linode", BackendID: "node-idle",
		NodeOnly: true, Status: state.BurstStatusRunning, CreatedAt: created,
	})
	store.PutWorkload(&state.Workload{
		ID: "wl_idle", CustomerID: "cust_test", BurstID: "b_idle",
		Status: "running", CreatedAt: created,
	})

	if got := h.reapBurst(context.Background(), "b_idle", nodeIdleReason); got != ReapOutcomeRequeued {
		t.Fatalf("idle reap = %v, want ReapOutcomeRequeued", got)
	}
	pending, err := store.GetBurst("b_idle")
	if err != nil || !pending.ReapPending || pending.ReapPendingStatus != "cancelled" {
		t.Fatalf("pending idle burst = %+v, err=%v; want cancelled retry", pending, err)
	}

	// A second provider failure is entered with the watchdog's generic retry
	// reason. It must not overwrite the original idle outcome with "failed".
	h.sweepExpiredBursts(context.Background(), 0, 0)
	pending, err = store.GetBurst("b_idle")
	if err != nil || pending.ReapPendingStatus != "cancelled" {
		t.Fatalf("idle retry after repeated failure = %+v, err=%v; want cancelled preserved", pending, err)
	}

	reaper.setErr(nil)
	h.sweepExpiredBursts(context.Background(), 0, 0)
	if wl, err := store.GetWorkload("wl_idle"); err != nil || wl.Status != "cancelled" || wl.FinishedAt == nil {
		t.Fatalf("idle workload after watchdog retry = %+v, err=%v; want terminal cancelled", wl, err)
	}
}

// TestReapBurstIdempotent proves two reapers can't double-tear-down one burst.
func TestReapBurstIdempotent(t *testing.T) {
	store := state.New()
	reaper := &fakeReaper{}
	h := &Workloads{Store: store, Reaper: reaper, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	store.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_test", Backend: "flyio", CreatedAt: time.Now()})

	if !h.reapBurst(context.Background(), "b1", "first").Reaped() {
		t.Fatal("first reap should win")
	}
	if got := h.reapBurst(context.Background(), "b1", "second"); got != ReapOutcomeNotOwned {
		t.Fatalf("second reap = %v, want ReapOutcomeNotOwned (burst already claimed)", got)
	}
	if n := len(reaper.teardowns); n != 1 {
		t.Fatalf("Teardown called %d times; want exactly 1", n)
	}
}

func TestSweepExpiredBursts_NodeOnly(t *testing.T) {
	store := state.New()
	reaper := &fakeReaper{}
	h := &Workloads{
		Store:  store,
		Reaper: reaper,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	old := time.Now().Add(-2 * time.Hour).UTC()

	addNodeOnly := func(id string, createdAt time.Time, deadline time.Duration, maxUSD, hourly float64) {
		store.PutBurst(&state.Burst{
			ID: id, CustomerID: "cust_test", Backend: "linode", BackendID: "x" + id,
			CreatedAt: createdAt, Status: "provisioning", HourlyUSD: hourly,
			Deadline: deadline, MaxUSD: maxUSD, NodeOnly: true,
		})
		store.PutWorkload(&state.Workload{
			ID: "wl_" + id, CustomerID: "cust_test", BurstID: id,
			Status: "provisioning", CreatedAt: createdAt,
		})
	}
	addOrdinary := func(id string, createdAt time.Time, deadline time.Duration, maxUSD, hourly float64) {
		store.PutBurst(&state.Burst{
			ID: id, CustomerID: "cust_test", Backend: "linode", BackendID: "x" + id,
			CreatedAt: createdAt, Status: "provisioning", HourlyUSD: hourly,
			Deadline: deadline, MaxUSD: maxUSD, NodeOnly: false,
		})
		store.PutWorkload(&state.Workload{
			ID: "wl_" + id, CustomerID: "cust_test", BurstID: id,
			Status: "provisioning", CreatedAt: createdAt,
		})
	}

	// 1. Unbudgeted nodeOnly (should survive global cap)
	addNodeOnly("nodeonly_unbudgeted", old, 0, 0, 0.62)
	// 2. NodeOnly with explicit Deadline (should be reaped when deadline hit)
	addNodeOnly("nodeonly_deadline_hit", old, time.Hour, 0, 0.62)
	// 3. NodeOnly with explicit MaxUSD (should be reaped when budget hit)
	addNodeOnly("nodeonly_budget_hit", old, 0, 1.00, 0.62)
	// 4. Ordinary unbudgeted burst (should be reaped under global cap)
	addOrdinary("ordinary_unbudgeted", old, 0, 0, 0.62)

	// Run with safety net enabled (1h max lifetime).
	h.sweepExpiredBursts(context.Background(), time.Hour, 0)

	// Verify survivors
	for _, id := range []string{"nodeonly_unbudgeted"} {
		if reaper.reaped(id) {
			t.Errorf("%s: nodeOnly unbudgeted should NOT be reaped by the global cap", id)
		}
		if _, err := store.GetBurst(id); err != nil {
			t.Errorf("%s: burst should still exist, got %v", id, err)
		}
	}

	// Verify reaped
	for _, id := range []string{"nodeonly_deadline_hit", "nodeonly_budget_hit", "ordinary_unbudgeted"} {
		if !reaper.reaped(id) {
			t.Errorf("%s: expected to be reaped", id)
		}
		if _, err := store.GetBurst(id); err == nil {
			t.Errorf("%s: burst should be deleted after reap", id)
		}
	}
}

// TestSweepExpiredBursts_Durable is the multi-replica property the sweep exists
// for: budgets are enforced against DURABLE state, so any central enforces any
// burst's budget, and a durable read that fails skips the pass rather than
// falling back to this replica's map.
//
// It needs two real stores over one database. A fake persister cannot stand in
// from this package — state.Store.persist is unexported and the persister
// interface's methods are too, so only package state can implement or install
// one — so this is gated on YSCALE_TEST_DATABASE_URL like state's round-trip
// test. The accessor half of the contract is unit-tested in
// state.TestDurableBursts.
func TestSweepExpiredBursts_Durable(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()

	// Replica B boots FIRST, so it never hydrates the burst replica A is about
	// to create: B's in-memory map is the empty one a second central really has.
	replicaB, err := state.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("replica B: %v", err)
	}
	defer replicaB.Close()
	replicaA, err := state.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("replica A: %v", err)
	}
	defer replicaA.Close()

	const burstID = "burst_sweep_cross_replica"
	overBudget := &state.Burst{
		ID: burstID, CustomerID: "cust_sweep_cross_replica", Backend: "linode", BackendID: "vm-cross-replica",
		CreatedAt: time.Now().Add(-2 * time.Hour).UTC(), Status: "provisioning",
		HourlyUSD: 0.62, Deadline: time.Hour, // age 2h > 1h deadline → over budget
	}
	if err := replicaA.PutBurst(overBudget); err != nil {
		t.Fatalf("replica A PutBurst: %v", err)
	}
	// Registered last so it runs first: the pools are still open. Reruns against
	// a reused test database must not inherit this row.
	defer func() { _ = replicaB.DeleteBurst(burstID) }()
	if _, err := replicaB.GetBurst(burstID); err == nil {
		t.Fatal("replica B already holds the burst in memory; the test would prove nothing")
	}

	reaper := &fakeReaper{}
	h := &Workloads{Store: replicaB, Reaper: reaper, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	// Safety net off: only the declared deadline may reap, so unrelated rows in
	// a shared test database are left alone.
	h.sweepExpiredBursts(ctx, 0, 0)
	if !reaper.reaped(burstID) {
		t.Fatal("replica B did not enforce a burst replica A created — with the creating " +
			"replica dead or restarted, nothing would ever reap it and it bills past its deadline")
	}
	// The claim removed the durable row, so no other replica reaps it again.
	bursts, ok, err := replicaB.DurableBursts(ctx)
	if err != nil || !ok {
		t.Fatalf("DurableBursts after sweep: ok=%v err=%v", ok, err)
	}
	for _, b := range bursts {
		if b.ID == burstID {
			t.Error("durable burst row survived the reap; another replica would tear the same VM down again")
		}
	}
	// replicaA still holds the record in memory and its workload is unknown to
	// B — bookkeeping this commit deliberately does not chase; only the money
	// path (teardown, exactly once) is enforced across replicas.

	// A durable read that fails must skip the pass entirely. Replica A still
	// holds the (now stale) burst in memory and it is still over budget, so a
	// fallback to ListBursts would put it back on the reap path. Asserted on the
	// log rather than on teardowns because this failure mode — a closed pool —
	// also fails the claim, which would mask a fallback here; a read that times
	// out while writes still work would not.
	var logs bytes.Buffer
	replicaA.Close()
	hA := &Workloads{Store: replicaA, Reaper: reaper, Log: slog.New(slog.NewTextHandler(&logs, nil))}
	hA.sweepExpiredBursts(ctx, 0, 0)
	if !strings.Contains(logs.String(), "budget sweep skipped") {
		t.Errorf("a failed durable read must be reported, not swallowed; log was:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "burst claim failed") {
		t.Errorf("the sweep reached the reap path on a failed durable read — it fell back to "+
			"this replica's map, which is the per-replica blindness the durable read removes; log was:\n%s",
			logs.String())
	}
	if n := reaper.teardownCount(burstID); n != 1 {
		t.Errorf("teardowns = %d, want 1 — the burst was already reaped by replica B", n)
	}
}

// These tests prove full sweep→delete-worker convergence at each readiness
// boundary that can idle-bill after provider creation. The sweep books a
// provider delete; the worker confirms provider absence, retires the burst,
// freezes cost, and writes the receipt. A second sweep+drain produces no
// duplicate work.
func TestPermanentlyUnschedulableDeadlineConvergesThroughAuthoritativeDelete(t *testing.T) {
	assertReadinessDeadlineConverges(t, "unsched", protocol.NodePhaseReady, true)
}

func TestNodeNeverReadyDeadlineConvergesThroughAuthoritativeDelete(t *testing.T) {
	assertReadinessDeadlineConverges(t, "node-not-ready", protocol.NodePhaseJoining, false)
}

func TestGPUPluginNeverHealthyDeadlineConvergesThroughAuthoritativeDelete(t *testing.T) {
	assertReadinessDeadlineConverges(t, "gpu-not-ready", protocol.NodePhaseReady, false)
}

func assertReadinessDeadlineConverges(t *testing.T, testID, nodePhase string, gpuReady bool) {
	t.Helper()

	store := state.New()
	deletes := newFakeProviderDeletes()
	reaper := &fakeReaper{}
	meter := cost.NewMeter()

	burstID := "burst-" + testID
	workloadID := "wl-" + testID
	observedAt := time.Now().Add(-90 * time.Minute).UTC()

	store.PutBurst(&state.Burst{
		ID: burstID, CustomerID: state.DevCustomerID, ClusterID: "cluster-gpu",
		Backend: "linode", BackendID: "linode_" + testID, Region: "us-sea",
		SKU: "g2-gpu-rtx4000a1-s", HourlyUSD: 0.52,
		CreatedAt: time.Now().Add(-2 * time.Hour).UTC(), Status: "provisioning",
		Deadline: time.Hour, NodeName: "ys-" + testID,
		NodePhase: nodePhase, NodePhaseAt: &observedAt,
	})
	workload := &state.Workload{
		ID: workloadID, CustomerID: state.DevCustomerID, ClusterID: "cluster-gpu", BurstID: burstID,
		Status: "provisioning", CreatedAt: time.Now().Add(-2 * time.Hour).UTC(),
		SpecYAML: []byte("image: nvidia/cuda:12.2.0-base-ubuntu22.04\ngpu:\n  kind: rtx4000ada\n  count: 1\n"),
		NodeObservation: &state.NodeObservation{
			NodeName: "ys-" + testID, Phase: nodePhase, ObservedAt: observedAt,
		},
	}
	if gpuReady {
		workload.GPUObservation = &state.GPUObservation{AllocatableAt: observedAt}
	}
	store.PutWorkload(workload)

	h := &Workloads{
		Store:    store,
		Reaper:   reaper,
		Log:      quietLog(),
		Cost:     meter,
		Deletes:  deletes,
		Commands: store,
	}

	// ── Sweep: the deadline-exceeded burst is expired via lifecycle ──
	h.sweepExpiredBursts(context.Background(), 0, 0)

	wl, err := store.GetWorkload(workloadID)
	if err != nil || wl.Status != "failed" || wl.FinishedAt == nil {
		t.Fatalf("workload after sweep = %+v err=%v, want failed with FinishedAt", wl, err)
	}
	if got := deletes.count(); got != 1 {
		t.Fatalf("provider delete bookings = %d, want exactly 1", got)
	}
	deleteRecord := deletes.get(burstID)
	if deleteRecord.State != lifecycle.ProviderDeleteQueued {
		t.Fatalf("delete state = %q, want %q", deleteRecord.State, lifecycle.ProviderDeleteQueued)
	}
	if _, err := store.GetBurst(burstID); err != nil {
		t.Fatal("burst retired before delete worker ran — it must stay live-cost until provider absence is confirmed")
	}
	if wl.Cost != nil {
		t.Fatalf("cost frozen before provider absence confirmed: %+v", wl.Cost)
	}
	if reaper.teardownCount(burstID) != 0 {
		t.Fatal("provider was called from the sweep path instead of the delete worker")
	}
	if recorded, err := store.BurstReapRecorded(context.Background(), burstID, state.DevCustomerID); err != nil || recorded {
		t.Fatalf("teardown receipt before delete worker = recorded=%v err=%v, want absent", recorded, err)
	}

	// ── Drain the real worker: provider absence confirmed ──
	worker := &ProviderDeleteWorker{
		Deletes:  deletes,
		Reaper:   reaper,
		Store:    store,
		Commands: store,
		Cost:     meter,
		Log:      quietLog(),
		Lease:    time.Minute,
		Now:      deletes.currentTime,
	}
	processed, drainErr := worker.Drain(context.Background())
	if drainErr != nil {
		t.Fatalf("worker drain: %v", drainErr)
	}
	if processed != 2 {
		t.Fatalf("worker drained %d items, want 2 (delete + cleanup repair)", processed)
	}

	if _, err := store.GetBurst(burstID); err == nil {
		t.Fatal("burst survived a confirmed provider delete")
	}
	finalDelete := deletes.get(burstID)
	if finalDelete.State != lifecycle.ProviderDeleteTerminated || finalDelete.DeletedAt == nil {
		t.Fatalf("delete = state=%q deleted_at=%v, want terminated with receipt",
			finalDelete.State, finalDelete.DeletedAt)
	}
	if got := deletes.successes[burstID]; got != 1 {
		t.Fatalf("provider success recorded %d times, want exactly 1", got)
	}

	wl, _ = store.GetWorkload(workloadID)
	if wl.NodeObservation == nil || wl.NodeObservation.Phase != nodePhase {
		t.Fatalf("retained node observation = %+v, want phase %q", wl.NodeObservation, nodePhase)
	}
	if got := wl.GPUObservation != nil; got != gpuReady {
		t.Fatalf("retained GPU readiness = %v, want %v", got, gpuReady)
	}
	if wl.PodObservation != nil {
		t.Fatalf("workload unexpectedly scheduled before deadline teardown: %+v", wl.PodObservation)
	}
	if wl.Cost == nil {
		t.Fatal("no cost observation after confirmed provider delete")
	}
	if wl.Cost.Runtime <= 0 {
		t.Fatalf("cost observation has no runtime: %+v", wl.Cost)
	}
	if wl.Cost.Basis != state.WorkloadCostBasisRateRuntimeToProviderDelete {
		t.Fatalf("cost basis = %q, want %q", wl.Cost.Basis, state.WorkloadCostBasisRateRuntimeToProviderDelete)
	}

	receipted, err := store.BurstReapRecorded(context.Background(), burstID, state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	if !receipted {
		t.Fatal("no durable teardown receipt after worker convergence")
	}

	cleanup := deletes.cleanupEvent(finalDelete.ID)
	if cleanup.State != lifecycle.OutboxAcknowledged {
		t.Fatalf("cleanup repair = %q, want %q", cleanup.State, lifecycle.OutboxAcknowledged)
	}

	// ── Repeat sweep + drain: no duplicate work ──
	h.sweepExpiredBursts(context.Background(), 0, 0)
	if got := deletes.count(); got != 1 {
		t.Fatalf("repeat sweep created %d bookings, want 1 (idempotent)", got)
	}
	if further, _ := worker.Drain(context.Background()); further != 0 {
		t.Fatalf("repeat drain found %d more items, want 0", further)
	}
}
