package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/broker"
)

// fakeProviderDeletes models lifecycle.Store's provider-delete transitions
// rather than merely recording calls. Every guard below mirrors a WHERE clause,
// a CHECK or a trigger in central/internal/lifecycle: one operation per burst,
// identity written once and never substituted, transitions fenced by the lease
// token, an absorbing terminal state, and attempts that advance only on a claim
// from queued/retrying. The tests here are about the INTEGRATION, so the state
// machine underneath them has to behave like the real one or they prove nothing.
type fakeProviderDeletes struct {
	mu        sync.Mutex
	now       time.Time
	nextID    int64
	nextLease int64
	byBurst   map[string]*lifecycle.ProviderDeleteRecord

	// outbox models lifecycle.outbox. Cleanup repairs live here and NOWHERE in
	// the delete records, which is the whole point of the split.
	outbox []*lifecycle.OutboxEvent

	requests     int
	claims       int
	reclaims     int
	acknowledged int
	successes    map[string]int

	requestErr      error
	claimErr        error
	successErr      error
	cleanupClaimErr error
}

func newFakeProviderDeletes() *fakeProviderDeletes {
	return &fakeProviderDeletes{
		now:       time.Now(),
		byBurst:   make(map[string]*lifecycle.ProviderDeleteRecord),
		successes: make(map[string]int),
	}
}

func (f *fakeProviderDeletes) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func (f *fakeProviderDeletes) currentTime() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeProviderDeletes) get(burstID string) lifecycle.ProviderDeleteRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.byBurst[burstID]
	if !ok {
		return lifecycle.ProviderDeleteRecord{}
	}
	return *record
}

func (f *fakeProviderDeletes) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.byBurst)
}

func (f *fakeProviderDeletes) RequestProviderDeleteForBooking(_ context.Context, booking lifecycle.ProviderDeleteBooking) (lifecycle.ProviderDeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests++
	if f.requestErr != nil {
		return lifecycle.ProviderDeleteResponse{}, f.requestErr
	}
	if booking.CustomerID == "" || booking.BurstID == "" || booking.Provider == "" || booking.ProviderResourceID == "" {
		return lifecycle.ProviderDeleteResponse{}, lifecycle.ErrInvalidArgument
	}
	if existing, ok := f.byBurst[booking.BurstID]; ok {
		// The stored booking is the one under lock; a replay that names a
		// different resource is refused, not silently ignored.
		if existing.CustomerID != booking.CustomerID ||
			existing.ClusterID != booking.ClusterID ||
			existing.Provider != booking.Provider ||
			existing.Region != booking.Region ||
			existing.CloudAccountID != booking.CloudAccountID ||
			existing.SKU != booking.SKU ||
			existing.ProviderResourceID != booking.ProviderResourceID {
			return lifecycle.ProviderDeleteResponse{}, lifecycle.ErrIdentityConflict
		}
		return lifecycle.ProviderDeleteResponse{
			DeleteID: existing.ID, BurstID: existing.BurstID, State: existing.State,
		}, nil
	}
	f.nextID++
	record := &lifecycle.ProviderDeleteRecord{
		ID: f.nextID, CustomerID: booking.CustomerID, ClusterID: booking.ClusterID,
		WorkloadID: booking.WorkloadID, BurstID: booking.BurstID,
		Provider: booking.Provider, Region: booking.Region, SKU: booking.SKU,
		CloudAccountID:     booking.CloudAccountID,
		ProviderResourceID: booking.ProviderResourceID, Reason: booking.Reason,
		Payload: append([]byte(nil), booking.Payload...), Generation: 1,
		State: lifecycle.ProviderDeleteQueued, NextAttemptAt: f.now,
		RequestedAt: f.now, UpdatedAt: f.now,
	}
	f.byBurst[booking.BurstID] = record
	return lifecycle.ProviderDeleteResponse{
		DeleteID: record.ID, BurstID: record.BurstID, State: record.State, Inserted: true,
	}, nil
}

func (f *fakeProviderDeletes) ClaimProviderDelete(_ context.Context, lease time.Duration) (lifecycle.ProviderDeleteRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return lifecycle.ProviderDeleteRecord{}, false, f.claimErr
	}
	ids := make([]string, 0, len(f.byBurst))
	for id := range f.byBurst {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		record := f.byBurst[id]
		due := (record.State == lifecycle.ProviderDeleteQueued || record.State == lifecycle.ProviderDeleteRetrying) &&
			!record.NextAttemptAt.After(f.now)
		expired := record.State == lifecycle.ProviderDeleteDeleting &&
			record.LockedUntil != nil && !record.LockedUntil.After(f.now)
		if !due && !expired {
			continue
		}
		if due {
			// Attempts advance only on a fresh attempt; reclaiming an expired
			// lease must not burn one, or a crash loop exhausts the budget.
			record.Attempts++
		} else {
			f.reclaims++
		}
		f.claims++
		f.nextLease++
		record.State = lifecycle.ProviderDeleteDeleting
		record.LeaseToken = fmt.Sprintf("lease-%d", f.nextLease)
		locked := f.now.Add(lease)
		record.LockedUntil = &locked
		record.UpdatedAt = f.now

		claimed := *record
		claimed.Payload = append([]byte(nil), record.Payload...)
		return claimed, true, nil
	}
	return lifecycle.ProviderDeleteRecord{}, false, nil
}

func (f *fakeProviderDeletes) leased(id int64, token string) (*lifecycle.ProviderDeleteRecord, error) {
	for _, record := range f.byBurst {
		if record.ID != id {
			continue
		}
		if record.State != lifecycle.ProviderDeleteDeleting || record.LeaseToken != token ||
			record.LockedUntil == nil || !record.LockedUntil.After(f.now) {
			return nil, lifecycle.ErrLeaseLost
		}
		return record, nil
	}
	return nil, lifecycle.ErrNotFound
}

// MarkProviderDeleteSucceeded terminalizes the delete AND enqueues the cleanup
// repair, in one step — the fake's stand-in for one transaction. The repair
// carries the operation's stored payload, never a caller-supplied copy.
func (f *fakeProviderDeletes) MarkProviderDeleteSucceeded(_ context.Context, id int64, leaseToken string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, err := f.leased(id, leaseToken)
	if err != nil {
		return time.Time{}, err
	}
	if f.successErr != nil {
		return time.Time{}, f.successErr
	}
	record.State = lifecycle.ProviderDeleteTerminated
	deletedAt := f.now
	record.DeletedAt = &deletedAt
	record.LeaseToken = ""
	record.LockedUntil = nil
	record.LastError = ""
	record.UpdatedAt = f.now
	f.successes[record.BurstID]++

	key := fmt.Sprintf("provider_delete_cleanup:%d", record.ID)
	for _, event := range f.outbox {
		if event.EventKey == key {
			return deletedAt, nil // UNIQUE (customer_id, cluster_id, event_key)
		}
	}
	f.nextID++
	f.outbox = append(f.outbox, &lifecycle.OutboxEvent{
		ID: f.nextID, CustomerID: record.CustomerID, ClusterID: record.ClusterID,
		AggregateType: "operation", AggregateID: fmt.Sprintf("provider_delete:%d", record.ID),
		EventKey: key, EventType: lifecycle.ProviderDeleteCleanupEventType,
		PayloadVersion: 1, Payload: append([]byte(nil), record.Payload...),
		State: lifecycle.OutboxPending, NextAttemptAt: f.now, CreatedAt: f.now,
	})
	return deletedAt, nil
}

// unrelatedOutboxEvent seeds a publication event, which the cleanup claim must
// never see: a delete worker that claimed one would try to settle a workload
// that was never torn down.
func (f *fakeProviderDeletes) unrelatedOutboxEvent(payload string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	f.outbox = append(f.outbox, &lifecycle.OutboxEvent{
		ID: f.nextID, CustomerID: state.DevCustomerID, ClusterID: "cluster_1",
		AggregateType: "workload", AggregateID: "wl_1",
		EventKey: fmt.Sprintf("workload_admitted:%d", f.nextID), EventType: "workload.admitted",
		PayloadVersion: 1, Payload: []byte(payload),
		State: lifecycle.OutboxPending, NextAttemptAt: f.now,
	})
}

func (f *fakeProviderDeletes) cleanupEvent(deleteID int64) lifecycle.OutboxEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fmt.Sprintf("provider_delete_cleanup:%d", deleteID)
	for _, event := range f.outbox {
		if event.EventKey == key {
			return *event
		}
	}
	return lifecycle.OutboxEvent{}
}

// ClaimProviderDeleteCleanup leases one due CLEANUP event. The event-type
// filter is the contract under test, so the fake enforces it rather than
// assuming the caller only ever enqueued cleanups.
func (f *fakeProviderDeletes) ClaimProviderDeleteCleanup(_ context.Context, lease time.Duration) (lifecycle.OutboxEvent, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cleanupClaimErr != nil {
		return lifecycle.OutboxEvent{}, false, f.cleanupClaimErr
	}
	for _, event := range f.outbox {
		if event.EventType != lifecycle.ProviderDeleteCleanupEventType {
			continue
		}
		due := (event.State == lifecycle.OutboxPending || event.State == lifecycle.OutboxFailed) &&
			!event.NextAttemptAt.After(f.now)
		expired := event.State == lifecycle.OutboxProcessing && !event.LockedUntil.After(f.now)
		if !due && !expired {
			continue
		}
		if due {
			event.Attempts++
		}
		f.nextLease++
		event.State = lifecycle.OutboxProcessing
		event.LeaseToken = fmt.Sprintf("cleanup-lease-%d", f.nextLease)
		event.LockedUntil = f.now.Add(lease)

		claimed := *event
		claimed.Payload = append([]byte(nil), event.Payload...)
		return claimed, true, nil
	}
	return lifecycle.OutboxEvent{}, false, nil
}

func (f *fakeProviderDeletes) leasedEvent(id int64, token string) (*lifecycle.OutboxEvent, error) {
	for _, event := range f.outbox {
		if event.ID != id {
			continue
		}
		if event.State != lifecycle.OutboxProcessing || event.LeaseToken != token ||
			!event.LockedUntil.After(f.now) {
			return nil, lifecycle.ErrLeaseLost
		}
		return event, nil
	}
	return nil, lifecycle.ErrNotFound
}

func (f *fakeProviderDeletes) AcknowledgeOutboxEvent(_ context.Context, id int64, leaseToken string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	event, err := f.leasedEvent(id, leaseToken)
	if err != nil {
		return err
	}
	event.State = lifecycle.OutboxAcknowledged
	event.LeaseToken = ""
	event.LockedUntil = time.Time{}
	f.acknowledged++
	return nil
}

func (f *fakeProviderDeletes) MarkOutboxFailed(_ context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if maxAttempts < 1 || maxAttempts > 100 || !retryAt.After(time.Now()) {
		return lifecycle.ErrInvalidArgument
	}
	event, err := f.leasedEvent(id, leaseToken)
	if err != nil {
		return err
	}
	event.State = lifecycle.OutboxFailed
	if event.Attempts >= maxAttempts {
		event.State = lifecycle.OutboxDeadLetter
	}
	event.NextAttemptAt = retryAt
	event.LeaseToken = ""
	event.LockedUntil = time.Time{}
	return nil
}

func (f *fakeProviderDeletes) MarkProviderDeleteFailed(_ context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if maxAttempts < 1 || maxAttempts > 100 || !retryAt.After(time.Now()) {
		return lifecycle.ErrInvalidArgument
	}
	record, err := f.leased(id, leaseToken)
	if err != nil {
		return err
	}
	record.State = lifecycle.ProviderDeleteRetrying
	if record.Attempts >= maxAttempts {
		record.State = lifecycle.ProviderDeleteManualAttention
	}
	record.NextAttemptAt = retryAt
	record.LastError = safeError
	record.LeaseToken = ""
	record.LockedUntil = nil
	record.UpdatedAt = f.now
	return nil
}

// requeue mirrors lifecycle.Store.RetryProviderDelete: the explicit operator
// path out of manual attention, on a new generation with a fresh attempt budget.
func (f *fakeProviderDeletes) requeue(burstID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.byBurst[burstID]
	if !ok {
		return lifecycle.ErrNotFound
	}
	if record.State != lifecycle.ProviderDeleteManualAttention {
		return lifecycle.ErrInvariantViolation
	}
	record.State = lifecycle.ProviderDeleteQueued
	record.Generation++
	record.Attempts = 0
	record.NextAttemptAt = f.now
	record.LastError = ""
	return nil
}

// deadBroker stands in for Redis being down. Nothing in the lifecycle reap path
// may depend on it, so every method fails.
type deadBroker struct{ publishes int }

func (d *deadBroker) Publish(context.Context, string, []byte) error {
	d.publishes++
	return errors.New("redis unavailable")
}

func (d *deadBroker) Consume(context.Context, string, string, string) (*broker.Message, error) {
	return nil, errors.New("redis unavailable")
}
func (d *deadBroker) Ack(context.Context, string, string, string) error {
	return errors.New("redis unavailable")
}
func (d *deadBroker) Close() error { return nil }

// deleteFixture is one live booked burst on one running workload, aged so a
// reap has a runtime to freeze.
type deleteFixture struct {
	store   *state.Store
	deletes *fakeProviderDeletes
	reaper  *fakeReaper
	meter   *cost.Meter
	wls     *Workloads
	worker  *ProviderDeleteWorker
}

type deadlineReaper struct {
	blockDelete  bool
	blockCleanup bool
}

func (r *deadlineReaper) DeleteProviderNode(ctx context.Context, _ *state.Burst) error {
	if !r.blockDelete {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (r *deadlineReaper) CleanupMesh(ctx context.Context, _ *state.Burst) error {
	if !r.blockCleanup {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func newDeleteFixture(t *testing.T) *deleteFixture {
	t.Helper()
	store := state.New()
	store.PutWorkload(&state.Workload{
		ID: "wl_1", CustomerID: state.DevCustomerID, BurstID: "burst_1", Status: "running",
		CreatedAt: time.Now().Add(-30 * time.Minute).UTC(),
	})
	if err := store.PutBurst(&state.Burst{
		ID: "burst_1", CustomerID: state.DevCustomerID, ClusterID: "cluster_1",
		Backend: "linode", BackendID: "linode_987654", Region: "us-east", SKU: "g6-standard-2",
		PodCIDR: "10.244.7.0/24", HourlyUSD: 2.0,
		CreatedAt: time.Now().Add(-30 * time.Minute).UTC(), Status: "provisioning",
	}); err != nil {
		t.Fatalf("seed burst: %v", err)
	}
	f := &deleteFixture{
		store:   store,
		deletes: newFakeProviderDeletes(),
		reaper:  &fakeReaper{},
		meter:   cost.NewMeter(),
	}
	f.wls = &Workloads{
		Store: store, Reaper: f.reaper, Log: quietLog(), Cost: f.meter, Deletes: f.deletes,
	}
	f.worker = &ProviderDeleteWorker{
		Deletes: f.deletes, Reaper: f.reaper, Store: store, Cost: f.meter, Log: quietLog(),
		Lease: time.Minute, Now: f.deletes.currentTime,
	}
	return f
}

func (f *deleteFixture) burstAlive() bool {
	_, err := f.store.GetBurst("burst_1")
	return err == nil
}

func (f *deleteFixture) reapReceipted(t *testing.T) bool {
	t.Helper()
	recorded, err := f.store.BurstReapRecorded(context.Background(), "burst_1", state.DevCustomerID)
	if err != nil {
		t.Fatalf("BurstReapRecorded: %v", err)
	}
	return recorded
}

// drain runs one full worker pass. The count is every durable item it moved:
// delete operations AND cleanup repairs. A successful teardown is therefore two
// — the provider call that confirmed absence, then the repair that finished the
// ledger behind it.
func (f *deleteFixture) drain(t *testing.T) int {
	t.Helper()
	processed, err := f.worker.Drain(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	return processed
}

// cleanupState is the durable state of the repair a confirmed delete enqueued,
// or "" when there is none.
func (f *deleteFixture) cleanupState(t *testing.T) string {
	t.Helper()
	return f.deletes.cleanupEvent(f.deletes.get("burst_1").ID).State
}

func (f *deleteFixture) makeBilled(t *testing.T) *orderedBilling {
	t.Helper()
	association := &state.WorkloadBilling{
		HoldID: 17, WorkloadRef: "wl_1", ReservedMicroUSD: 2_000_000,
		Currency: "USD", AuthoritativeUsageRequired: true,
	}
	workload, err := f.store.GetWorkload("wl_1")
	if err != nil {
		t.Fatal(err)
	}
	workload.Status = "failed"
	workload.Billing = association
	f.store.PutWorkload(workload)
	burst, err := f.store.GetBurst("burst_1")
	if err != nil {
		t.Fatal(err)
	}
	burst.Billing = association
	if err := f.store.PutBurst(burst); err != nil {
		t.Fatal(err)
	}
	events := []string{}
	ledger := &orderedBilling{events: &events}
	f.worker.Billing = ledger
	return ledger
}

// Complete, Cancel and the watchdog all reach the same reap. Racing them must
// produce ONE delete operation for one paid resource — a second would be a
// second provider call, and the whole state machine exists to make that
// impossible.
func TestRacingReapsCreateExactlyOneDeleteOperation(t *testing.T) {
	f := newDeleteFixture(t)
	reasons := []string{"workload Succeeded", "workload cancelled", "workload deadline exceeded"}

	var wg sync.WaitGroup
	outcomes := make([]ReapOutcome, len(reasons))
	for i, reason := range reasons {
		wg.Add(1)
		go func(i int, reason string) {
			defer wg.Done()
			outcomes[i] = f.wls.reapBurst(context.Background(), "burst_1", reason)
		}(i, reason)
	}
	wg.Wait()

	if got := f.deletes.count(); got != 1 {
		t.Fatalf("%d delete operations exist for one burst; want exactly 1", got)
	}
	for i, outcome := range outcomes {
		if outcome != ReapOutcomeDeletePending {
			t.Errorf("racer %d = %v, want ReapOutcomeDeletePending — every racer sees the one durable delete", i, outcome)
		}
	}
	if f.deletes.requests < len(reasons) {
		t.Errorf("only %d of %d racers reached the store", f.deletes.requests, len(reasons))
	}
	if f.reaper.reaped("burst_1") {
		t.Error("a reap called the provider directly; only the delete worker may")
	}
}

func TestPendingDeleteDoesNotDuplicateWatchdogExpiryAudit(t *testing.T) {
	f := newDeleteFixture(t)
	journal := &failingJournal{Store: f.store}
	f.wls.Journal = journal
	b, err := f.store.GetBurst("burst_1")
	if err != nil {
		t.Fatal(err)
	}
	b.Deadline = time.Minute
	b.CreatedAt = time.Now().Add(-time.Hour).UTC()
	if err := f.store.PutBurst(b); err != nil {
		t.Fatal(err)
	}

	f.wls.sweepExpiredBursts(context.Background(), 0, 0)
	f.wls.sweepExpiredBursts(context.Background(), 0, 0)

	if got := f.deletes.count(); got != 1 {
		t.Fatalf("provider deletes = %d, want one durable operation", got)
	}
	if expired := journal.recorded(state.ActionWorkloadExpired); len(expired) != 1 {
		t.Fatalf("expiry audit rows = %d, want exactly one across repeated watchdog passes", len(expired))
	}
}

// The invariant the removal-first claim broke: until the provider confirms the
// resource is gone, the burst is still a live-cost obligation. Nothing may be
// settled, released, receipted, or reported as finished.
func TestReapRecordsIntentAndChangesNothingElse(t *testing.T) {
	f := newDeleteFixture(t)

	if got := f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded"); got != ReapOutcomeDeletePending {
		t.Fatalf("reapBurst = %v, want ReapOutcomeDeletePending", got)
	}
	if !f.burstAlive() {
		t.Error("the burst was claimed away before its provider resource was deleted")
	}
	if f.reaper.reaped("burst_1") {
		t.Error("the provider was called from the request path")
	}
	if f.reapReceipted(t) {
		t.Error("a teardown receipt was written before the teardown happened")
	}
	if wl, err := f.store.GetWorkload("wl_1"); err != nil {
		t.Fatal(err)
	} else if wl.Cost != nil {
		t.Errorf("cost was frozen onto the workload before the node was deleted: %+v", wl.Cost)
	}
	if record := f.deletes.get("burst_1"); record.State != lifecycle.ProviderDeleteQueued {
		t.Errorf("delete state = %q, want %q", record.State, lifecycle.ProviderDeleteQueued)
	} else if job, err := decodeProviderDeleteJob(record.Payload); err != nil {
		t.Fatal(err)
	} else if job.Cost != nil {
		t.Fatalf("delete intent froze cost before provider confirmation: %+v", job.Cost)
	}
}

// The provider confirms, the delete terminalizes on the spot, the live record
// retires, and the cleanup chain behind it runs exactly once as its own durable
// repair.
func TestDeleteWorkerConfirmsThenTerminalizesThenCleansUp(t *testing.T) {
	f := newDeleteFixture(t)
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	if processed := f.drain(t); processed != 2 {
		t.Fatalf("drained %d items, want 2 (the delete, then its cleanup repair)", processed)
	}
	if got := f.cleanupState(t); got != lifecycle.OutboxAcknowledged {
		t.Fatalf("cleanup repair = %q, want %q", got, lifecycle.OutboxAcknowledged)
	}
	if !f.reaper.reaped("burst_1") {
		t.Fatal("the worker never called the provider")
	}
	record := f.deletes.get("burst_1")
	if record.State != lifecycle.ProviderDeleteTerminated || record.DeletedAt == nil {
		t.Fatalf("delete = %q (deleted_at %v), want terminated with a receipt", record.State, record.DeletedAt)
	}
	if f.burstAlive() {
		t.Error("the live burst record survived a confirmed provider delete")
	}
	if !f.reapReceipted(t) {
		t.Error("no durable teardown receipt was written")
	}
	if wl, err := f.store.GetWorkload("wl_1"); err != nil {
		t.Fatal(err)
	} else if wl.Cost == nil {
		t.Error("the confirmed delete recorded no cost observation")
	} else if wl.Cost.Runtime <= 0 {
		t.Errorf("cost observation has no runtime: %+v", wl.Cost)
	}
	if f.drain(t) != 0 {
		t.Error("a terminated delete was claimable again")
	}
	if got := f.deletes.successes["burst_1"]; got != 1 {
		t.Errorf("provider success recorded %d times, want exactly 1", got)
	}
}

func TestCostAccruesUntilProviderDeletionIsConfirmed(t *testing.T) {
	f := newDeleteFixture(t)
	b, err := f.store.GetBurst("burst_1")
	if err != nil {
		t.Fatal(err)
	}
	f.wls.reapBurst(context.Background(), b.ID, "workload Succeeded")
	intentAt := f.deletes.get(b.ID).RequestedAt
	f.deletes.advance(20 * time.Minute)
	confirmedAt := f.deletes.currentTime()
	f.drain(t)

	wl, err := f.store.GetWorkload("wl_1")
	if err != nil {
		t.Fatal(err)
	}
	if wl.Cost == nil {
		t.Fatal("confirmed provider delete recorded no cost")
	}
	if !wl.Cost.FrozenAt.Equal(confirmedAt) || !wl.Cost.FrozenAt.After(intentAt) {
		t.Fatalf("cost frozen_at=%v, want provider confirmation %v after intent %v",
			wl.Cost.FrozenAt, confirmedAt, intentAt)
	}
	if wl.Cost.Basis != state.WorkloadCostBasisRateRuntimeToProviderDelete {
		t.Fatalf("cost basis=%q, want %q", wl.Cost.Basis, state.WorkloadCostBasisRateRuntimeToProviderDelete)
	}
	wantRuntime := confirmedAt.Sub(b.CreatedAt)
	if wl.Cost.Runtime != wantRuntime {
		t.Fatalf("cost runtime=%s, want full provider lifetime %s", wl.Cost.Runtime, wantRuntime)
	}
	if got := scrapeCostTotal(t, f.meter, b.Backend); math.Abs(got-wl.Cost.EstimatedUSD) > 1e-9 {
		t.Fatalf("meter cost=%v, want durable confirmed-delete cost=%v", got, wl.Cost.EstimatedUSD)
	}
}

// A mesh outage happens after the provider absence hinge. It must leave the
// delete terminal (and therefore no longer live-cost), retry only the durable
// cleanup event, and never call DeleteNode again.
func TestMeshCleanupFailureAfterProviderSuccessRetriesOnlyCleanup(t *testing.T) {
	f := newDeleteFixture(t)
	f.reaper.setMeshErr(errors.New("mesh unavailable"))
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	if processed := f.drain(t); processed != 2 {
		t.Fatalf("drained %d items, want provider delete and failed cleanup", processed)
	}
	if got := f.deletes.get("burst_1"); got.State != lifecycle.ProviderDeleteTerminated || got.DeletedAt == nil {
		t.Fatalf("delete = %+v, want terminal despite mesh failure", got)
	}
	if got := f.cleanupState(t); got != lifecycle.OutboxFailed {
		t.Fatalf("cleanup = %q, want %q", got, lifecycle.OutboxFailed)
	}
	if got := f.reaper.teardownCount("burst_1"); got != 1 {
		t.Fatalf("provider deletes = %d, want 1", got)
	}

	f.reaper.setMeshErr(nil)
	f.deletes.advance(time.Minute)
	if processed := f.drain(t); processed != 1 {
		t.Fatalf("retry drained %d items, want cleanup only", processed)
	}
	if got := f.reaper.teardownCount("burst_1"); got != 1 {
		t.Fatalf("provider delete replayed during cleanup retry: %d calls", got)
	}
	if got := f.reaper.meshCleanupCount("burst_1"); got != 2 {
		t.Fatalf("mesh cleanup calls = %d, want failed attempt plus retry", got)
	}
	if got := f.cleanupState(t); got != lifecycle.OutboxAcknowledged {
		t.Fatalf("cleanup after retry = %q, want %q", got, lifecycle.OutboxAcknowledged)
	}
}

func TestPaidDeleteSettlesAuthoritativeCostBeforePermanentMeshFailure(t *testing.T) {
	f := newDeleteFixture(t)
	ledger := f.makeBilled(t)
	f.worker.MaxAttempts = 3
	f.reaper.setMeshErr(errors.New("mesh permanently unavailable"))
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	for attempt := 1; attempt <= 3; attempt++ {
		f.drain(t)
		f.deletes.advance(time.Hour)
	}

	if got := f.cleanupState(t); got != lifecycle.OutboxDeadLetter {
		t.Fatalf("cleanup repair = %q, want %q", got, lifecycle.OutboxDeadLetter)
	}
	workload, err := f.store.GetWorkload("wl_1")
	if err != nil {
		t.Fatal(err)
	}
	if workload.Cost == nil || workload.Cost.Basis != state.WorkloadCostBasisRateRuntimeToProviderDelete {
		t.Fatalf("authoritative provider-delete cost = %+v", workload.Cost)
	}
	wantCapture, err := billing.TrustedCaptureMicroUSD(workload.Cost.EstimatedUSD, workload.Billing.ReservedMicroUSD)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.captures) != 1 || ledger.captures["burst-capture:burst_1"] != wantCapture {
		t.Fatalf("economic captures = %#v, want one authoritative capture of %d", ledger.captures, wantCapture)
	}
	if got := f.reaper.teardownCount("burst_1"); got != 1 {
		t.Fatalf("provider deletes = %d, want one", got)
	}
	if got := f.deletes.get("burst_1").State; got != lifecycle.ProviderDeleteTerminated {
		t.Fatalf("provider delete = %q, want %q", got, lifecycle.ProviderDeleteTerminated)
	}
}

func TestPaidDeleteCostPersistenceExhaustionProtectsTheHold(t *testing.T) {
	f := newDeleteFixture(t)
	ledger := f.makeBilled(t)
	f.worker.MaxAttempts = 3
	f.worker.CostRecorder = &failingCostRecorder{store: f.store, err: errors.New("postgres unavailable")}
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	for attempt := 1; attempt <= 3; attempt++ {
		f.drain(t)
		f.deletes.advance(time.Hour)
	}

	if got := f.cleanupState(t); got != lifecycle.OutboxDeadLetter {
		t.Fatalf("cleanup repair = %q, want %q", got, lifecycle.OutboxDeadLetter)
	}
	workload, err := f.store.GetWorkload("wl_1")
	if err != nil {
		t.Fatal(err)
	}
	if workload.Cost != nil {
		t.Fatalf("cost unexpectedly persisted: %+v", workload.Cost)
	}
	if workload.Billing == nil || !workload.Billing.ManualAttention {
		t.Fatalf("billing association = %+v, want durable manual attention", workload.Billing)
	}
	if !f.store.BillingHoldProtected(state.DevCustomerID, "wl_1") {
		t.Fatal("pending costless hold is not protected after cleanup exhaustion")
	}
	receipts, _, err := f.store.BillingUsageReceipts(context.Background(), 10)
	if err != nil || len(receipts) != 1 || !receipts[0].ManualAttention || receipts[0].CostPresent {
		t.Fatalf("usage reconciliation receipt = %+v, err %v", receipts, err)
	}
	if len(ledger.captures) != 0 {
		t.Fatalf("captures = %#v, want none without durable authoritative cost", ledger.captures)
	}
	if got := f.reaper.teardownCount("burst_1"); got != 1 {
		t.Fatalf("provider deletes = %d, want one", got)
	}
	if got := f.deletes.get("burst_1").State; got != lifecycle.ProviderDeleteTerminated {
		t.Fatalf("provider delete = %q, want %q", got, lifecycle.ProviderDeleteTerminated)
	}
}

func TestDeleteAttemptTimeoutRecordsRetryAndReturnsToTheQueue(t *testing.T) {
	f := newDeleteFixture(t)
	f.worker.Reaper = &deadlineReaper{blockDelete: true}
	f.worker.AttemptTimeout = 10 * time.Millisecond
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	started := time.Now()
	if processed := f.drain(t); processed != 1 {
		t.Fatalf("drained %d items, want the timed-out delete", processed)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("timed-out provider call held the serial worker for %s", elapsed)
	}
	if record := f.deletes.get("burst_1"); record.State != lifecycle.ProviderDeleteRetrying {
		t.Fatalf("delete = %q, want %q after timeout", record.State, lifecycle.ProviderDeleteRetrying)
	}
}

func TestCleanupAttemptTimeoutRecordsRetryAndReturnsToTheQueue(t *testing.T) {
	f := newDeleteFixture(t)
	f.worker.Reaper = &deadlineReaper{blockCleanup: true}
	f.worker.AttemptTimeout = 10 * time.Millisecond
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	started := time.Now()
	if processed := f.drain(t); processed != 2 {
		t.Fatalf("drained %d items, want the delete and timed-out cleanup", processed)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("timed-out cleanup held the serial worker for %s", elapsed)
	}
	if got := f.cleanupState(t); got != lifecycle.OutboxFailed {
		t.Fatalf("cleanup = %q, want %q after timeout", got, lifecycle.OutboxFailed)
	}
}

func TestAttemptTimeoutNeverOutlivesLease(t *testing.T) {
	w := ProviderDeleteWorker{
		Lease:          10 * time.Second,
		AttemptTimeout: time.Minute,
	}
	if got, want := w.attemptTimeout(), 8*time.Second; got != want {
		t.Fatalf("attempt timeout = %s, want lease-bounded %s", got, want)
	}
}

// Process death BEFORE the provider call: the lease expires and the operation
// is reclaimed. Reclaiming must not burn an attempt — a crash loop would
// otherwise exhaust the retry budget without ever reaching a provider.
func TestExpiredLeaseIsReclaimedWithoutBurningAnAttempt(t *testing.T) {
	f := newDeleteFixture(t)
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	claimed, ok, err := f.deletes.ClaimProviderDelete(context.Background(), time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim: record=%+v ok=%v err=%v", claimed, ok, err)
	}
	if claimed.Attempts != 1 {
		t.Fatalf("attempts = %d after the first claim, want 1", claimed.Attempts)
	}
	// The worker holding that lease dies here. Nothing else may touch the
	// operation until the lease expires.
	if f.drain(t) != 0 {
		t.Fatal("a live lease was stolen by a second worker")
	}

	f.deletes.advance(2 * time.Minute)
	if f.drain(t) != 2 {
		t.Fatal("the expired lease was never reclaimed")
	}
	if f.deletes.reclaims != 1 {
		t.Errorf("reclaims = %d, want 1", f.deletes.reclaims)
	}
	if record := f.deletes.get("burst_1"); record.Attempts != 1 {
		t.Errorf("attempts = %d after a reclaim, want 1 — a reclaim is not a new attempt", record.Attempts)
	}
	if !f.reaper.reaped("burst_1") {
		t.Error("the reclaimed operation never reached the provider")
	}
}

// A worker that restarts drains at startup. The operation resumes with no
// Complete, no Cancel and no watchdog tick behind it.
func TestExpiredLeaseResumesOnWorkerRestartWithoutAnotherWorkloadSignal(t *testing.T) {
	f := newDeleteFixture(t)
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")
	if _, _, err := f.deletes.ClaimProviderDelete(context.Background(), time.Minute); err != nil {
		t.Fatal(err)
	}
	f.deletes.advance(2 * time.Minute)

	// A brand new process: same durable queue, no shared state, no wake-up.
	restarted := &ProviderDeleteWorker{
		Deletes: f.deletes, Reaper: f.reaper, Store: f.store, Cost: f.meter,
		Log: quietLog(), Lease: time.Minute,
	}
	processed, err := restarted.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if processed != 2 {
		t.Fatalf("the restarted worker drained %d items, want 2", processed)
	}
	if record := f.deletes.get("burst_1"); record.State != lifecycle.ProviderDeleteTerminated {
		t.Fatalf("delete = %q, want terminated", record.State)
	}
	if got := f.cleanupState(t); got != lifecycle.OutboxAcknowledged {
		t.Fatalf("cleanup repair = %q, want %q", got, lifecycle.OutboxAcknowledged)
	}
}

// A process can die after the lifecycle transaction confirms provider absence
// but before the fast path retires the legacy live-burst row. The cleanup event
// committed by that same transaction is the only guaranteed restart signal, so
// it must reconcile the row as well as the receipts behind it.
func TestCleanupRepairRetiresLiveBurstAfterSuccessReceiptCrash(t *testing.T) {
	f := newDeleteFixture(t)
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	record, ok, err := f.deletes.ClaimProviderDelete(context.Background(), time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim provider delete: record=%+v ok=%v err=%v", record, ok, err)
	}
	job, err := decodeProviderDeleteJob(record.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.reaper.DeleteProviderNode(context.Background(), &job.Burst); err != nil {
		t.Fatal(err)
	}
	if _, err := f.deletes.MarkProviderDeleteSucceeded(context.Background(), record.ID, record.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if !f.burstAlive() {
		t.Fatal("test did not stop in the crash window with the stale live burst present")
	}

	restarted := &ProviderDeleteWorker{
		Deletes: f.deletes, Reaper: f.reaper, Store: f.store, Cost: f.meter,
		Log: quietLog(), Lease: time.Minute,
	}
	processed, err := restarted.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("the restarted worker drained %d items, want the cleanup repair", processed)
	}
	if f.burstAlive() {
		t.Error("the cleanup repair acknowledged while the deleted burst remained live")
	}
	if got := f.cleanupState(t); got != lifecycle.OutboxAcknowledged {
		t.Fatalf("cleanup repair = %q, want %q", got, lifecycle.OutboxAcknowledged)
	}
	if got := f.reaper.teardownCount("burst_1"); got != 1 {
		t.Fatalf("provider delete replayed during state reconciliation: %d calls", got)
	}
}

// A cleanup repair whose worker died mid-repair is reclaimed by the next
// process on the same poll — the provider resource is already gone, so nothing
// re-derives this work from a workload signal and nothing ever would.
func TestOrphanedCleanupRepairIsReclaimedByAFreshWorker(t *testing.T) {
	f := newDeleteFixture(t)
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	// The delete confirms and terminalizes; the repair is leased and the process
	// dies holding it.
	f.worker.Store = nil // cleanup cannot run, so the first pass leaves it open
	f.drain(t)
	f.deletes.mu.Lock()
	f.deletes.outbox[0].State = lifecycle.OutboxProcessing
	f.deletes.outbox[0].LeaseToken = "orphaned-lease"
	f.deletes.outbox[0].LockedUntil = f.deletes.now.Add(time.Minute)
	f.deletes.mu.Unlock()

	if f.drain(t) != 0 {
		t.Fatal("a live cleanup lease was stolen by a second worker")
	}
	f.deletes.advance(2 * time.Minute)

	restarted := &ProviderDeleteWorker{
		Deletes: f.deletes, Reaper: f.reaper, Store: f.store, Cost: f.meter,
		Log: quietLog(), Lease: time.Minute,
	}
	processed, err := restarted.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("the restarted worker drained %d items, want the orphaned repair", processed)
	}
	if got := f.cleanupState(t); got != lifecycle.OutboxAcknowledged {
		t.Fatalf("cleanup repair = %q, want %q", got, lifecycle.OutboxAcknowledged)
	}
	if !f.reapReceipted(t) {
		t.Error("the reclaimed repair never wrote the teardown receipt")
	}
	if got := f.reaper.teardownCount("burst_1"); got != 1 {
		t.Errorf("teardown ran %d times, want 1 — a cleanup retry must never call the provider", got)
	}
}

// Process death DURING the provider call. The provider may or may not have
// acted, so the replay repeats the teardown — which is idempotent — and records
// success exactly once.
func TestDeathDuringTheProviderCallReplaysTeardownAndSucceedsOnce(t *testing.T) {
	f := newDeleteFixture(t)
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	claimed, ok, err := f.deletes.ClaimProviderDelete(context.Background(), time.Minute)
	if err != nil || !ok {
		t.Fatal(err)
	}
	job, err := decodeProviderDeleteJob(claimed.Payload)
	if err != nil {
		t.Fatal(err)
	}
	// The provider call ran; the process died before anything was recorded.
	if err := f.reaper.Teardown(context.Background(), &job.Burst); err != nil {
		t.Fatal(err)
	}
	f.deletes.advance(2 * time.Minute)

	if f.drain(t) != 2 {
		t.Fatal("the interrupted delete was not replayed")
	}
	if got := f.reaper.teardownCount("burst_1"); got != 2 {
		t.Errorf("teardown ran %d times, want 2 (the replay is the point; DeleteNode is idempotent)", got)
	}
	if got := f.deletes.successes["burst_1"]; got != 1 {
		t.Errorf("provider success recorded %d times, want exactly 1", got)
	}
}

// The issue-#9 invariant, stated as its failure mode: a cleanup that fails
// AFTER the provider confirmed absence must never move the delete or the burst
// back to a live-cost state. The old ordering did exactly that — a refused cost
// receipt pushed a delete for a node that no longer existed into retrying, and
// eight of those into manual_attention, all of it still billing.
func TestCleanupFailureCannotUndoAConfirmedProviderDelete(t *testing.T) {
	f := newDeleteFixture(t)
	// A cost recorder that refuses is the same boundary as a lost receipt write.
	failing := &failingCostRecorder{store: f.store, err: errors.New("postgres unavailable")}
	f.worker.CostRecorder = failing
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	f.drain(t)
	record := f.deletes.get("burst_1")
	if record.State != lifecycle.ProviderDeleteTerminated || record.DeletedAt == nil {
		t.Fatalf("delete = %q (deleted_at %v); provider absence is confirmed and must be recorded regardless of the cleanup",
			record.State, record.DeletedAt)
	}
	if got := f.deletes.successes["burst_1"]; got != 1 {
		t.Errorf("provider success recorded %d times, want exactly 1", got)
	}
	if f.burstAlive() {
		t.Error("the live burst record survived a confirmed provider delete; it is still counted and still billing")
	}
	if got := f.cleanupState(t); got != lifecycle.OutboxFailed {
		t.Fatalf("cleanup repair = %q, want %q — the leftover work is what retries", got, lifecycle.OutboxFailed)
	}
	if f.reapReceipted(t) {
		t.Error("a teardown receipt was written although the cleanup failed")
	}

	// Recovery: the SAME repair, retried, finishes the ledger. No second reap
	// request, no second provider call.
	failing.err = nil
	f.deletes.advance(2 * time.Hour)
	if f.drain(t) != 1 {
		t.Fatal("the retryable cleanup repair was not picked up again")
	}
	if got := f.cleanupState(t); got != lifecycle.OutboxAcknowledged {
		t.Fatalf("cleanup repair = %q, want %q after a successful retry", got, lifecycle.OutboxAcknowledged)
	}
	if !f.reapReceipted(t) {
		t.Error("the retried cleanup wrote no teardown receipt")
	}
	if got := f.reaper.teardownCount("burst_1"); got != 1 {
		t.Errorf("teardown ran %d times, want 1 — a cleanup retry must never call the provider again", got)
	}
	if got := f.deletes.successes["burst_1"]; got != 1 {
		t.Errorf("provider success recorded %d times across the retry, want exactly 1", got)
	}
}

// Cleanup that can never succeed dead-letters explicitly instead of retrying
// forever — and the delete it belongs to stays terminated throughout, because
// the node really is gone.
func TestExhaustedCleanupDeadLettersAndLeavesTheDeleteTerminated(t *testing.T) {
	f := newDeleteFixture(t)
	f.worker.MaxAttempts = 3
	f.worker.CostRecorder = &failingCostRecorder{store: f.store, err: errors.New("postgres unavailable")}
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	for attempt := 1; attempt <= 3; attempt++ {
		f.drain(t)
		if record := f.deletes.get("burst_1"); record.State != lifecycle.ProviderDeleteTerminated {
			t.Fatalf("attempt %d moved the delete to %q; a confirmed deletion is absorbing", attempt, record.State)
		}
		f.deletes.advance(time.Hour)
	}

	if got := f.cleanupState(t); got != lifecycle.OutboxDeadLetter {
		t.Fatalf("cleanup repair = %q after exhausting retries, want %q", got, lifecycle.OutboxDeadLetter)
	}
	if f.drain(t) != 0 {
		t.Error("a dead-lettered repair was claimed again; exhaustion must be explicit, not a retry loop")
	}
	if got := f.reaper.teardownCount("burst_1"); got != 1 {
		t.Errorf("teardown ran %d times across the whole retry budget, want 1", got)
	}
}

// The cleanup claim is filtered to its own event type. A publication event
// shares the table and must be invisible here — settling a workload that was
// never torn down is the failure the filter prevents.
func TestCleanupDrainIgnoresUnrelatedOutboxEvents(t *testing.T) {
	f := newDeleteFixture(t)
	f.deletes.unrelatedOutboxEvent(`{"workload_id":"wl_1"}`)
	f.deletes.unrelatedOutboxEvent(`{"workload_id":"wl_2"}`)

	if f.drain(t) != 0 {
		t.Fatal("the delete worker claimed a publication event")
	}
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")
	if got := f.drain(t); got != 2 {
		t.Fatalf("drained %d items, want 2 — the delete and ONLY its own repair", got)
	}
	f.deletes.mu.Lock()
	defer f.deletes.mu.Unlock()
	for _, event := range f.deletes.outbox {
		if event.EventType == lifecycle.ProviderDeleteCleanupEventType {
			continue
		}
		if event.State != lifecycle.OutboxPending || event.Attempts != 0 {
			t.Errorf("publication %q was touched by the delete worker: state=%q attempts=%d",
				event.EventKey, event.State, event.Attempts)
		}
	}
}

// A worker with no provider client cannot confirm absence. Confirming it anyway
// would stop the billing, release the /24 and settle the ledger for a node that
// is still running.
func TestWorkerWithoutAReaperFailsClosed(t *testing.T) {
	f := newDeleteFixture(t)
	f.worker.Reaper = nil
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	f.drain(t)
	record := f.deletes.get("burst_1")
	if record.State != lifecycle.ProviderDeleteManualAttention {
		t.Fatalf("delete = %q, want immediate %q — a worker with no provider client is a wiring fault",
			record.State, lifecycle.ProviderDeleteManualAttention)
	}
	if record.DeletedAt != nil {
		t.Fatal("a delete receipt was stamped without any provider call")
	}
	if f.deletes.successes["burst_1"] != 0 {
		t.Error("provider absence was recorded with no provider behind it")
	}
	if !f.burstAlive() {
		t.Error("the burst was retired although nothing confirmed the node is gone")
	}
	if f.reapReceipted(t) {
		t.Error("a teardown receipt was written with no teardown")
	}
	if got := f.cleanupState(t); got != "" {
		t.Errorf("a cleanup repair = %q was enqueued for a node that was never deleted", got)
	}
}

// A provider that refuses the delete leaves a node that is still running and
// still billing. The burst stays live-cost through every retry, and exhaustion
// lands in an operator-requeueable manual_attention rather than a log line.
func TestDeleteFailuresStayLiveCostAndExhaustIntoManualAttention(t *testing.T) {
	f := newDeleteFixture(t)
	f.worker.MaxAttempts = 3
	f.reaper.setErr(errors.New("provider refused the delete"))
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	for attempt := 1; attempt <= 3; attempt++ {
		if f.drain(t) != 1 {
			t.Fatalf("attempt %d was not drained", attempt)
		}
		record := f.deletes.get("burst_1")
		if record.Attempts != attempt {
			t.Fatalf("attempts = %d after pass %d", record.Attempts, attempt)
		}
		if !f.burstAlive() {
			t.Fatal("a failed delete retired the burst; the node is still running and still billing")
		}
		if f.reapReceipted(t) {
			t.Fatal("a failed delete wrote a teardown receipt")
		}
		f.deletes.advance(time.Hour)
	}

	record := f.deletes.get("burst_1")
	if record.State != lifecycle.ProviderDeleteManualAttention {
		t.Fatalf("delete = %q after exhausting retries, want %q", record.State, lifecycle.ProviderDeleteManualAttention)
	}
	if record.LastError == "" {
		t.Error("manual attention carries no reason for an operator to act on")
	}
	if f.drain(t) != 0 {
		t.Error("manual attention is not a retry state; it must not be claimed automatically")
	}

	// The operator path out: requeue, fix the provider, and the SAME operation
	// finishes. No new reap request is needed.
	if err := f.deletes.requeue("burst_1"); err != nil {
		t.Fatal(err)
	}
	f.reaper.setErr(nil)
	if f.drain(t) != 2 {
		t.Fatal("the requeued delete was not claimable")
	}
	if got := f.deletes.get("burst_1"); got.State != lifecycle.ProviderDeleteTerminated {
		t.Fatalf("delete = %q after the operator requeue, want terminated", got.State)
	}
	if f.deletes.count() != 1 {
		t.Error("the operator path created a second operation for one resource")
	}
}

// Redis is a wake-up cache. With it dead, the delete is still recorded, still
// claimable and still completed — nothing about the obligation lived there.
func TestRedisUnavailableCannotLoseAuthoritativeDeleteWork(t *testing.T) {
	f := newDeleteFixture(t)
	dead := &deadBroker{}
	f.wls.Teardowns = dead

	if got := f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded"); got != ReapOutcomeDeletePending {
		t.Fatalf("reapBurst = %v, want ReapOutcomeDeletePending with the broker down", got)
	}
	if dead.publishes != 0 {
		t.Errorf("the lifecycle reap published %d broker messages; the durable row is the only record", dead.publishes)
	}
	if f.deletes.count() != 1 {
		t.Fatal("no durable delete was recorded while the broker was down")
	}
	if f.drain(t) != 2 {
		t.Fatal("the delete did not run with the broker down")
	}
	if record := f.deletes.get("burst_1"); record.State != lifecycle.ProviderDeleteTerminated {
		t.Fatalf("delete = %q, want terminated", record.State)
	}
}

// Identity comes from the booking, under lock, and nothing may substitute it —
// not a replayed reap and not a payload the worker decodes.
func TestProviderIdentityCannotBeSubstituted(t *testing.T) {
	t.Run("replayed reap naming a different resource is refused", func(t *testing.T) {
		f := newDeleteFixture(t)
		f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

		// Something rewrites the local booking to point at another VM.
		booked, err := f.store.GetBurst("burst_1")
		if err != nil {
			t.Fatal(err)
		}
		substituted := *booked
		substituted.BackendID = "linode_000000"
		if err := f.store.PutBurst(&substituted); err != nil {
			t.Fatal(err)
		}
		if got := f.wls.reapBurst(context.Background(), "burst_1", "workload cancelled"); got != ReapOutcomeUnknown {
			t.Fatalf("reapBurst = %v, want ReapOutcomeUnknown — a substituted identity must fail closed", got)
		}
		if record := f.deletes.get("burst_1"); record.ProviderResourceID != "linode_987654" {
			t.Fatalf("stored resource = %q, want the originally booked linode_987654", record.ProviderResourceID)
		}
		if !f.burstAlive() {
			t.Error("a refused reap still retired the burst")
		}
	})

	// Every immutable field the operation records is compared, not just the
	// resource ID: a payload that disagrees about the account, the burst or the
	// SKU is not one this process may act on either, and a resource ID that
	// matches by accident must not carry the rest of a wrong payload past the
	// check.
	t.Run("worker refuses a payload that disagrees with the booking", func(t *testing.T) {
		for _, tc := range []struct {
			field  string
			tamper func(*state.Burst)
		}{
			{"provider resource", func(b *state.Burst) { b.BackendID = "linode_000000" }},
			{"customer", func(b *state.Burst) { b.CustomerID = "cust_someone_else" }},
			{"burst", func(b *state.Burst) { b.ID = "burst_2" }},
			{"provider", func(b *state.Burst) { b.Backend = "hetzner" }},
			{"region", func(b *state.Burst) { b.Region = "eu-west" }},
			{"cloud account", func(b *state.Burst) { b.CloudAccountID = "account_substituted" }},
			{"SKU", func(b *state.Burst) { b.SKU = "g6-standard-64" }},
		} {
			t.Run(tc.field, func(t *testing.T) {
				f := newDeleteFixture(t)
				f.worker.MaxAttempts = 8
				f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

				// Tamper with the durable payload only — the booked identity is
				// unchanged.
				f.deletes.mu.Lock()
				record := f.deletes.byBurst["burst_1"]
				var job teardownJob
				if err := json.Unmarshal(record.Payload, &job); err != nil {
					f.deletes.mu.Unlock()
					t.Fatal(err)
				}
				tc.tamper(&job.Burst)
				tampered, err := json.Marshal(job)
				if err != nil {
					f.deletes.mu.Unlock()
					t.Fatal(err)
				}
				record.Payload = tampered
				f.deletes.mu.Unlock()

				f.drain(t)
				if f.reaper.reaped("burst_1") {
					t.Fatal("a payload that disagreed with the booking reached the provider")
				}
				if got := f.deletes.get("burst_1"); got.State != lifecycle.ProviderDeleteManualAttention {
					t.Fatalf("delete = %q, want immediate %q — retrying cannot fix a mismatched identity",
						got.State, lifecycle.ProviderDeleteManualAttention)
				}
			})
		}
	})
}

// Fail closed: an unrecordable delete leaves the burst standing, still billing
// and still sweepable. It must NOT fall through to the legacy claim, which
// would destroy a paid resource with no durable record that it was meant to.
func TestUnrecordableIntentNeverFallsBackToTheLegacyClaim(t *testing.T) {
	f := newDeleteFixture(t)
	f.wls.Teardowns = broker.NewMemory(time.Hour)
	f.deletes.requestErr = errors.New("lifecycle database unavailable")

	if got := f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded"); got != ReapOutcomeUnknown {
		t.Fatalf("reapBurst = %v, want ReapOutcomeUnknown", got)
	}
	if !f.burstAlive() {
		t.Error("the burst was claimed away although no delete was ever recorded")
	}
	if f.reaper.reaped("burst_1") {
		t.Error("the provider was torn down with no durable delete record")
	}
	if f.deletes.count() != 0 {
		t.Error("a delete operation was recorded despite the store refusing")
	}
}

// No customer-visible answer may claim cleanup is done while the operation is
// nonterminal — and the terminal answer must still arrive once it is.
func TestNoResponseReportsReapedWhileCleanupIsNonterminal(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*deleteFixture, *state.Customer) *httptest.ResponseRecorder
	}{
		{
			name: "complete",
			call: func(f *deleteFixture, cust *state.Customer) *httptest.ResponseRecorder {
				rec := httptest.NewRecorder()
				f.wls.Complete(rec, completeReq(cust, "wl_1", `{"phase":"Succeeded"}`))
				return rec
			},
		},
		{
			name: "cancel",
			call: func(f *deleteFixture, cust *state.Customer) *httptest.ResponseRecorder {
				rec := httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodDelete, "/v1/workloads/wl_1", nil)
				r.SetPathValue("id", "wl_1")
				f.wls.Cancel(rec, r.WithContext(context.WithValue(r.Context(), ctxCustomer, cust)))
				return rec
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeleteFixture(t)
			cust, err := f.store.CustomerByID(state.DevCustomerID)
			if err != nil {
				t.Fatal(err)
			}

			rec := tc.call(f, cust)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			body := rec.Body.String()
			if strings.Contains(body, "reaped") {
				t.Fatalf("response = %q; nothing is reaped until the provider confirms", body)
			}
			if !strings.Contains(body, burstDeletingStatus) {
				t.Fatalf("response = %q, want status %q", body, burstDeletingStatus)
			}

			// Once the worker confirms, the terminal answer is honest.
			f.drain(t)
			rec = tc.call(f, cust)
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "already_reaped") {
				t.Fatalf("post-confirmation replay = %d %q, want 200 already_reaped", rec.Code, rec.Body.String())
			}
		})
	}
}

// The API vocabulary. "reaped" and "already_reaped" both assert the provider
// resource is gone, so exactly two outcomes may use them — and the one that
// says another caller owns it is a LOST CLAIM, nothing else. Everything
// nonterminal or unknown gets its own word and a retryable code.
func TestReapStatusNeverClaimsCleanupForANonterminalOrUnknownOutcome(t *testing.T) {
	for _, tc := range []struct {
		outcome ReapOutcome
		status  string
		code    int
	}{
		{ReapOutcomeReaped, burstReapedStatus, http.StatusOK},
		{ReapOutcomeNotOwned, burstAlreadyReapedStatus, http.StatusOK},
		{ReapOutcomeDeletePending, burstDeletingStatus, http.StatusOK},
		{ReapOutcomeRequeued, burstCleanupPendingStatus, http.StatusServiceUnavailable},
		{ReapOutcomeReceiptPending, burstCleanupPendingStatus, http.StatusServiceUnavailable},
		{ReapOutcomeUnknown, burstCleanupUnknownStatus, http.StatusServiceUnavailable},
	} {
		t.Run(tc.status+"/"+fmt.Sprint(int(tc.outcome)), func(t *testing.T) {
			if got := reapStatus(tc.outcome); got != tc.status {
				t.Fatalf("reapStatus(%v) = %q, want %q", tc.outcome, got, tc.status)
			}
			rec := httptest.NewRecorder()
			writeReapStatus(rec, tc.outcome)
			if rec.Code != tc.code {
				t.Fatalf("status code = %d, want %d", rec.Code, tc.code)
			}
			body := rec.Body.String()
			if tc.outcome != ReapOutcomeNotOwned && strings.Contains(body, burstAlreadyReapedStatus) {
				t.Fatalf("outcome %v answered %q; only a lost claim may say that", tc.outcome, body)
			}
			// The quotes matter: "already_reaped" does not contain `"reaped"`.
			if tc.outcome != ReapOutcomeReaped && strings.Contains(body, `"`+burstReapedStatus+`"`) {
				t.Fatalf("outcome %v answered %q; the provider resource is not confirmed gone", tc.outcome, body)
			}
		})
	}
}

// End to end: a teardown that definitively did NOT happen must not answer a
// customer with a word that means it did.
func TestCompleteAndCancelReportARequeuedTeardownAsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*deleteFixture, *state.Customer) *httptest.ResponseRecorder
	}{
		{
			name: "complete",
			call: func(f *deleteFixture, cust *state.Customer) *httptest.ResponseRecorder {
				rec := httptest.NewRecorder()
				f.wls.Complete(rec, completeReq(cust, "wl_1", `{"phase":"Succeeded"}`))
				return rec
			},
		},
		{
			name: "cancel",
			call: func(f *deleteFixture, cust *state.Customer) *httptest.ResponseRecorder {
				rec := httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodDelete, "/v1/workloads/wl_1", nil)
				r.SetPathValue("id", "wl_1")
				f.wls.Cancel(rec, r.WithContext(context.WithValue(r.Context(), ctxCustomer, cust)))
				return rec
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeleteFixture(t)
			// Inline mode with a provider that refuses: the burst is re-queued and
			// the node is still running.
			f.wls.Deletes = nil
			f.reaper.setErr(errors.New("provider refused the delete"))
			cust, err := f.store.CustomerByID(state.DevCustomerID)
			if err != nil {
				t.Fatal(err)
			}

			rec := tc.call(f, cust)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503 — the caller should retry", rec.Code)
			}
			if body := rec.Body.String(); !strings.Contains(body, burstCleanupPendingStatus) {
				t.Fatalf("response = %q, want %q", body, burstCleanupPendingStatus)
			}
			if strings.Contains(rec.Body.String(), "reaped") {
				t.Fatalf("response = %q; the provider refused and the node is still there", rec.Body.String())
			}
		})
	}
}

// A terminated delete whose live record survived (a worker that could not
// retire it, a replica that never saw the delete) is reconciled by the next
// reap — the resource really is gone, so "reaped" is the honest answer.
func TestStaleLiveRecordAfterATerminatedDeleteIsReconciled(t *testing.T) {
	f := newDeleteFixture(t)
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")
	f.drain(t)

	// Put the stale live record back, as a replica that missed the retirement
	// would still be holding it.
	if err := f.store.PutBurst(&state.Burst{
		ID: "burst_1", CustomerID: state.DevCustomerID, ClusterID: "cluster_1",
		Backend: "linode", BackendID: "linode_987654", Region: "us-east", SKU: "g6-standard-2",
		CreatedAt: time.Now().Add(-30 * time.Minute).UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if got := f.wls.reapBurst(context.Background(), "burst_1", "workload cancelled"); got != ReapOutcomeReaped {
		t.Fatalf("reapBurst = %v, want ReapOutcomeReaped for an already-terminated delete", got)
	}
	if f.burstAlive() {
		t.Error("the stale live record was not retired")
	}
	if got := f.reaper.teardownCount("burst_1"); got != 1 {
		t.Errorf("teardown ran %d times, want 1 — reconciliation must not re-delete", got)
	}
}

// The compatibility seam: with lifecycle explicitly disabled, the inline path
// is untouched — claim first, tear down, receipt, done.
func TestInlinePathIsUnchangedWhenLifecycleIsDisabled(t *testing.T) {
	f := newDeleteFixture(t)
	f.wls.Deletes = nil

	if got := f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded"); got != ReapOutcomeReaped {
		t.Fatalf("reapBurst = %v, want ReapOutcomeReaped on the inline path", got)
	}
	if f.burstAlive() {
		t.Error("the inline path left the burst record behind")
	}
	if !f.reaper.reaped("burst_1") {
		t.Error("the inline path never called the provider")
	}
	if f.deletes.count() != 0 {
		t.Error("a delete operation was recorded with lifecycle disabled")
	}
}

// The backoff is a schedule, not a hammer: it grows and it is capped.
func TestProviderDeleteBackoffGrowsAndIsCapped(t *testing.T) {
	if first, second := providerDeleteBackoff(1), providerDeleteBackoff(2); second <= first {
		t.Errorf("backoff did not grow: %v then %v", first, second)
	}
	if got := providerDeleteBackoff(50); got != providerDeleteMaxBackoff {
		t.Errorf("backoff at attempt 50 = %v, want the %v cap", got, providerDeleteMaxBackoff)
	}
	if got := providerDeleteBackoff(0); got != providerDeleteBaseBackoff {
		t.Errorf("backoff at attempt 0 = %v, want the %v base", got, providerDeleteBaseBackoff)
	}
}

// failingCostRecorder refuses the receipt write while err is set and otherwise
// delegates, so the recovery half of a test exercises the real store.
type failingCostRecorder struct {
	store *state.Store
	err   error
}

func (f *failingCostRecorder) EnsureWorkloadCostForBurst(ctx context.Context, c state.WorkloadCost) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return f.store.EnsureWorkloadCostForBurst(ctx, c)
}

func TestCleanupRepairFailsOnPodCIDRReleaseError(t *testing.T) {
	f := newDeleteFixture(t)
	f.worker.releasePodSlot = func(context.Context, *state.Store, *slog.Logger, *state.Burst) error {
		return errors.New("injected podcidr release error")
	}
	f.wls.reapBurst(context.Background(), "burst_1", "workload Succeeded")

	processed, err := f.worker.drainDeletes(context.Background())
	if err != nil {
		t.Fatalf("drainDeletes: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected 1 delete processed, got %d", processed)
	}

	processed, err = f.worker.drainCleanups(context.Background())
	if err != nil {
		t.Fatalf("drainCleanups: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected 1 cleanup processed, got %d", processed)
	}

	if got := f.cleanupState(t); got != lifecycle.OutboxFailed {
		t.Fatalf("cleanup repair state = %q, want %q", got, lifecycle.OutboxFailed)
	}

	if f.reapReceipted(t) {
		t.Error("teardown receipt was written despite cleanup failure")
	}
}
