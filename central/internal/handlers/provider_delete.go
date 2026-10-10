package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/state"
)

const (
	// providerDeleteLease bounds how long one worker owns a delete before
	// another may reclaim it. Long enough for a slow cloud API, short enough
	// that a pod killed mid-delete does not hold a paid resource hostage until
	// someone notices.
	providerDeleteLease = 5 * time.Minute

	// providerDeleteInterval is the idle poll. The wake-up channel makes the
	// same-replica case immediate; this is what makes a delete queued on ANOTHER
	// replica — or inherited across a restart — still run without any further
	// workload signal.
	providerDeleteInterval = 30 * time.Second

	// providerDeleteMaxAttempts mirrors teardownMaxAttempts: the same number of
	// automatic tries before a paid resource becomes an operator's problem. The
	// difference is where it lands — a durable manual_attention row an operator
	// can requeue, rather than a dead-letter stream entry.
	providerDeleteMaxAttempts = 8

	// providerDeleteImmediateAttention forces manual attention on the first
	// failure. Used only for failures that retrying cannot fix: a payload that
	// cannot be decoded, or one whose provider identity does not match the
	// booking it was recorded against.
	providerDeleteImmediateAttention = 1

	providerDeleteBaseBackoff = 30 * time.Second
	providerDeleteMaxBackoff  = 15 * time.Minute

	// providerDeleteDrainLimit bounds one drain pass so a large backlog cannot
	// starve the ctx check or hold the loop forever.
	providerDeleteDrainLimit = 64

	providerDeleteActor = "central-reaper"
)

// ProviderDeletes is the authoritative provider-delete state machine as the
// workload path uses it. It is an interface for the same reason Journal is: the
// fail-closed behaviour behind it is only ever exercised in production
// otherwise, and here the difference is a durable delete obligation versus a
// silently orphaned cloud node.
//
// Implemented by *lifecycle.Store. Nil means lifecycle mode is disabled and the
// inline OSS/dev path stays in charge — see reapBurst.
type ProviderDeletes interface {
	RequestProviderDeleteForBooking(ctx context.Context, booking lifecycle.ProviderDeleteBooking) (lifecycle.ProviderDeleteResponse, error)
	ClaimProviderDelete(ctx context.Context, lease time.Duration) (lifecycle.ProviderDeleteRecord, bool, error)
	MarkProviderDeleteSucceeded(ctx context.Context, id int64, leaseToken string) (time.Time, error)
	MarkProviderDeleteFailed(ctx context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error

	// The post-delete repair queue. MarkProviderDeleteSucceeded enqueues one of
	// these in the SAME transaction as the delete receipt, so the three below
	// are what finishes a teardown whose provider half is already done. They are
	// the EXISTING outbox transitions — claim, acknowledge, retry/dead-letter —
	// with the claim filtered to the cleanup event type, not a second state
	// machine.
	ClaimProviderDeleteCleanup(ctx context.Context, lease time.Duration) (lifecycle.OutboxEvent, bool, error)
	AcknowledgeOutboxEvent(ctx context.Context, id int64, leaseToken string) error
	MarkOutboxFailed(ctx context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error
}

// recordProviderDeleteIntent projects the booked burst into the lifecycle
// aggregate and records — or replays — the authoritative intent to delete its
// provider resource.
//
// Every identity field is read off the booking, never off request input, and
// the payload is the EXISTING teardownJob contract: the whole burst plus the
// reason (and a legacy terminal cost when one already exists). That is what lets
// the delete worker reconstruct the same teardown call the inline path and the
// Redis worker already make, without a second provider abstraction to keep in
// step.
//
// Cost is deliberately absent from a new intent. The authoritative burst stays
// live-cost through queueing, retries and manual attention; the cleanup event's
// transaction timestamp freezes cost only when provider absence is receipted.
// TerminalCost is retained solely for a legacy receipt-repair booking whose
// provider was already confirmed gone before lifecycle mode took ownership.
func (h *Workloads) recordProviderDeleteIntent(ctx context.Context, b *state.Burst, reason string) (lifecycle.ProviderDeleteResponse, error) {
	booked := *b
	booked.Region = lifecycle.CanonicalProviderDeleteRegion(booked.Region)
	job := teardownJob{Burst: booked, Reason: reason}
	if b.TerminalCost != nil {
		terminal := *b.TerminalCost
		job.Cost = &terminal
	}
	payload, err := json.Marshal(job)
	if err != nil {
		return lifecycle.ProviderDeleteResponse{}, fmt.Errorf("encode provider-delete payload: %w", err)
	}
	spec, err := json.Marshal(&booked)
	if err != nil {
		return lifecycle.ProviderDeleteResponse{}, fmt.Errorf("encode booking spec: %w", err)
	}
	return h.Deletes.RequestProviderDeleteForBooking(ctx, lifecycle.ProviderDeleteBooking{
		CustomerID:         b.CustomerID,
		ClusterID:          b.ClusterID,
		BurstID:            b.ID,
		Provider:           b.Backend,
		Region:             booked.Region,
		CloudAccountID:     b.CloudAccountID,
		SKU:                b.SKU,
		ProviderResourceID: b.BackendID,
		ProviderCreatedAt:  b.CreatedAt,
		Spec:               spec,
		Reason:             reason,
		Payload:            payload,
		Actor:              providerDeleteActor,
	})
}

// wakeDeleteWorker nudges an in-process delete worker. Best-effort and
// non-blocking by construction: it is a cache in front of the poll, and the
// poll is what makes the durable row the source of truth. A dropped wake-up
// costs one interval; nothing is lost.
func (h *Workloads) wakeDeleteWorker() {
	if h.DeleteWake == nil {
		return
	}
	select {
	case h.DeleteWake <- struct{}{}:
	default:
	}
}

// ProviderDeleteWorker is the only component that calls a provider on a delete
// once lifecycle mode is on. It leases one durable operation at a time, replays
// the exact recorded teardown, and — the moment the provider confirms the
// resource is gone — commits that fact and hands the rest to a durable cleanup
// repair it drains from the same loop.
//
// The ordering is the invariant, and it has exactly one hinge: the Reaper
// returning nil. Before it, nothing may be released, receipted or settled and
// the burst is a live-cost obligation; a failure there keeps the operation
// retryable and can reach manual_attention. After it, the delete is TERMINAL and
// stays terminal — the node no longer exists, so no receipt, settlement or node
// drain that fails afterwards may push it back to retrying or manual_attention
// and resume charging for a resource that is gone. That leftover work is the
// cleanup repair, and it fails on its own outbox row.
//
// A crash anywhere leaves an expired lease, which the next claim reclaims and
// replays — every step in here is idempotent precisely so that replay is free.
type ProviderDeleteWorker struct {
	Deletes      ProviderDeletes
	Reaper       ProviderDeleteReaper
	Store        *state.Store
	Commands     ConnectorCommandLedger
	CostRecorder workloadCostRecorder
	Billing      BurstBilling
	Cost         *cost.Meter
	Log          *slog.Logger

	// Lease, Interval and MaxAttempts are optional; zero takes the package
	// default.
	Lease          time.Duration
	Interval       time.Duration
	MaxAttempts    int
	AttemptTimeout time.Duration
	// Now is an optional clock seam for deterministic retry scheduling tests.
	// Production leaves it nil and uses time.Now.
	Now func() time.Time
	// releasePodSlot is a package-local failure seam for cleanup tests.
	// Production leaves it nil and uses the shared releasePodSlot helper.
	releasePodSlot func(context.Context, *state.Store, *slog.Logger, *state.Burst) error

	// Wake is the in-process wake-up cache. Nil is fine: the poll alone is a
	// complete implementation, which is the point — nothing outside the database
	// may be load-bearing for a delete obligation.
	Wake <-chan struct{}
}

// Run drains at startup and then on every wake-up or tick. The startup drain is
// what makes an expired lease resume after a restart without another workload
// signal.
func (w *ProviderDeleteWorker) Run(ctx context.Context) {
	w.Log.Info("provider delete worker started",
		"lease", w.lease().String(), "interval", w.interval().String(),
		"max_attempts", w.maxAttempts())
	timer := time.NewTicker(w.interval())
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if _, err := w.Drain(ctx); err != nil && ctx.Err() == nil {
			w.Log.Warn("provider delete drain incomplete; the durable queue is unchanged and the next pass retries",
				"error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-w.Wake:
		case <-timer.C:
		}
	}
}

// Drain processes every delete operation that is due right now and then every
// cleanup repair that is, each up to a bounded batch. It returns the number of
// durable items it moved.
//
// Both halves are polled, not just signalled, and that is what makes a restart
// complete: a delete whose lease expired with a previous pod resumes here, and
// so does a cleanup repair whose provider half finished long ago on a replica
// that then died. Neither needs another workload signal.
func (w *ProviderDeleteWorker) Drain(ctx context.Context) (int, error) {
	processed, deleteErr := w.drainDeletes(ctx)
	// Attempted even when the deletes failed: a cleanup repair is work for a
	// resource that is already gone, and there is no reason to make finishing
	// its ledger wait on the queue that no longer contains it.
	repaired, cleanupErr := w.drainCleanups(ctx)
	if deleteErr == nil {
		deleteErr = cleanupErr
	}
	return processed + repaired, deleteErr
}

func (w *ProviderDeleteWorker) drainDeletes(ctx context.Context) (int, error) {
	processed := 0
	for processed < providerDeleteDrainLimit {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		record, ok, err := w.Deletes.ClaimProviderDelete(ctx, w.lease())
		if err != nil {
			return processed, err
		}
		if !ok {
			return processed, nil
		}
		w.process(ctx, record)
		processed++
	}
	return processed, nil
}

// process runs one leased delete up to the provider's answer, and commits that
// answer. Everything after it belongs to the cleanup repair.
func (w *ProviderDeleteWorker) process(ctx context.Context, record lifecycle.ProviderDeleteRecord) {
	job, err := decodeProviderDeleteJob(record.Payload)
	if err != nil {
		// An undecodable delete names a paid resource nobody can describe. It is
		// NOT dropped the way an undecodable queue message is: the queue message
		// was a copy of a durable record, and this IS the durable record.
		w.fail(ctx, record, fmt.Sprintf("provider-delete payload undecodable: %v", err), providerDeleteImmediateAttention)
		return
	}
	b := job.Burst
	if mismatch := providerIdentityMismatch(&b, record); mismatch != "" {
		// The booked identity is the one under lock in lifecycle.bursts. A
		// payload that disagrees with it in ANY recorded field must never reach a
		// provider: it would be this process choosing the resource to destroy.
		w.fail(ctx, record, "provider-delete payload does not match the booked provider identity: "+mismatch,
			providerDeleteImmediateAttention)
		return
	}
	if w.Reaper == nil {
		// Fail closed. A worker with no provider client cannot confirm absence,
		// and confirming it anyway would terminalize a delete — stop the billing,
		// release the /24, settle the ledger — for a node that is still running.
		// This is a wiring fault, so it goes straight to an operator.
		w.fail(ctx, record, "provider delete worker has no provider client configured",
			providerDeleteImmediateAttention)
		return
	}
	attemptCtx, cancelAttempt := context.WithTimeout(ctx, w.attemptTimeout())
	err = w.Reaper.DeleteProviderNode(attemptCtx, &b)
	cancelAttempt()
	if err != nil {
		// Nothing is confirmed deleted, so nothing downstream may run. The
		// operation stays queued-for-retry, which is what keeps the burst a
		// live-cost obligation, and exhausting the retries lands it in
		// manual_attention with the node still counted.
		w.fail(ctx, record, "provider teardown failed: "+err.Error(), w.maxAttempts())
		return
	}

	// Provider absence is confirmed, so it is recorded NOW — together with the
	// cleanup repair that carries the rest, in the store's own transaction.
	confirmedAt, err := w.Deletes.MarkProviderDeleteSucceeded(ctx, record.ID, record.LeaseToken)
	if err != nil {
		// The lease may have expired under a slow provider call, or the process
		// may be racing its own replacement. Either way the record is untouched
		// and the next claim replays an idempotent teardown before trying again.
		// Success is never recorded twice because the transition requires this
		// lease token and a 'deleting' state.
		w.Log.Error("provider resource deleted but the delete receipt was not recorded; the operation replays",
			"burst", b.ID, "customer", b.CustomerID, "delete", record.ID, "error", err)
		w.recordReap("fail")
		return
	}

	// The delete is terminal, so the legacy live record is now stale. This is
	// the same atomic removal the legacy path used — moved to AFTER provider
	// absence is durably marked, which is the ordering it had backwards.
	if w.Store != nil {
		if _, _, err := w.Store.ClaimBurst(b.ID); err != nil {
			w.Log.Error("provider delete confirmed but the live burst record was not retired; a later reap reconciles it against the terminated delete",
				"burst", b.ID, "error", err)
		}
	}
	if w.Cost != nil {
		observed := job.Cost
		if observed == nil {
			confirmed := providerDeleteCostForBurst(&b, confirmedAt)
			observed = &confirmed
		}
		w.Cost.BurstReaped(b.Backend, b.HourlyUSD, observed.Runtime)
	}
	w.recordReap("ok")
	w.Log.Info("burst torn down via authoritative provider delete",
		"burst", b.ID, "backend", b.Backend, "reason", job.Reason, "attempts", record.Attempts)
}

// providerIdentityMismatch names the first recorded identity field the replay
// payload disagrees with, or "" when every one of them matches.
//
// Every provider credential/resource-routing field the operation stores is
// compared, not just the resource ID. The resource ID alone is the field an
// attacker or a corrupted payload would have to change to redirect the delete
// — but it is also the field most likely to be right by accident on a payload
// that is wrong about everything else, and a delete whose account, region,
// burst or SKU disagrees with the booking is not one this process may act on.
//
// Cluster is deliberately absent: the store substitutes a placeholder for a
// booking that predates cluster IDs, so the stored value legitimately differs
// from the payload's empty one.
func providerIdentityMismatch(b *state.Burst, record lifecycle.ProviderDeleteRecord) string {
	for _, field := range []struct{ name, booked, replayed string }{
		{"customer", record.CustomerID, b.CustomerID},
		{"burst", record.BurstID, b.ID},
		{"provider", record.Provider, b.Backend},
		{"region", record.Region, lifecycle.CanonicalProviderDeleteRegion(b.Region)},
		{"cloud account", record.CloudAccountID, b.CloudAccountID},
		{"SKU", record.SKU, b.SKU},
		{"provider resource", record.ProviderResourceID, b.BackendID},
	} {
		if strings.TrimSpace(field.booked) != strings.TrimSpace(field.replayed) {
			return field.name
		}
	}
	return ""
}

// drainCleanups finishes the deletes whose provider half is already done. The
// resource is gone for every event in here, so nothing this loop does may
// change the delete operation or the burst's terminal state — the only durable
// outcome it produces is the repair's own acknowledgement or dead-lettering.
func (w *ProviderDeleteWorker) drainCleanups(ctx context.Context) (int, error) {
	repaired := 0
	for repaired < providerDeleteDrainLimit {
		if err := ctx.Err(); err != nil {
			return repaired, err
		}
		event, ok, err := w.Deletes.ClaimProviderDeleteCleanup(ctx, w.lease())
		if err != nil {
			return repaired, err
		}
		if !ok {
			return repaired, nil
		}
		w.repair(ctx, event)
		repaired++
	}
	return repaired, nil
}

// repair runs one leased cleanup to a durable conclusion. It never calls the
// provider: the delete that enqueued it already did, and the whole reason this
// is a separate record is that the provider call must not be repeated to retry
// a receipt.
func (w *ProviderDeleteWorker) repair(ctx context.Context, event lifecycle.OutboxEvent) {
	job, err := decodeProviderDeleteJob(event.Payload)
	if err != nil {
		// The payload is the delete operation's own immutable one, so this is not
		// a retryable condition — it is a repair nobody can perform.
		w.failCleanup(ctx, event, fmt.Sprintf("provider-delete cleanup payload undecodable: %v", err),
			nil, false, providerDeleteImmediateAttention)
		return
	}
	b := job.Burst
	if strings.TrimSpace(b.CustomerID) != strings.TrimSpace(event.CustomerID) {
		// Settlement and receipts are written against a customer. A repair whose
		// payload names a different one would bill the wrong tenant for a node.
		w.failCleanup(ctx, event, "provider-delete cleanup payload does not match the recorded customer",
			nil, false, providerDeleteImmediateAttention)
		return
	}
	attemptCtx, cancelAttempt := context.WithTimeout(ctx, w.attemptTimeout())
	economicSettled, err := w.cleanup(attemptCtx, &b, job, event.CreatedAt)
	cancelAttempt()
	if err != nil {
		w.Log.Warn("provider resource is gone but post-delete cleanup did not complete; the repair is durable and retries",
			"burst", b.ID, "customer", b.CustomerID, "cleanup", event.ID,
			"attempt", event.Attempts, "error", err)
		w.failCleanup(ctx, event, err.Error(), b.Billing, economicSettled, w.maxAttempts())
		return
	}
	if err := w.Deletes.AcknowledgeOutboxEvent(ctx, event.ID, event.LeaseToken); err != nil {
		// The lease expired or the database went away. Every cleanup step is
		// idempotent, so the reclaimed repair simply runs them again.
		w.Log.Error("post-delete cleanup completed but the repair was not acknowledged; it replays, and every step of it is idempotent",
			"burst", b.ID, "cleanup", event.ID, "error", err)
		return
	}
	w.Log.Info("post-delete cleanup complete",
		"burst", b.ID, "customer", b.CustomerID, "cleanup", event.ID, "attempts", event.Attempts)
}

// failCleanup records the failure against the repair's own outbox row, using
// the existing retry and dead-letter transitions. It never touches the delete
// operation or the burst: both are terminal, and the provider resource they
// describe really is gone. Before an economically incomplete paid repair is
// dead-lettered, it patches the exact workload billing association so the hold
// remains fail-closed for operator reconciliation.
func (w *ProviderDeleteWorker) failCleanup(
	ctx context.Context,
	event lifecycle.OutboxEvent,
	safeError string,
	billingAssociation *state.WorkloadBilling,
	economicSettled bool,
	maxAttempts int,
) {
	if event.Attempts >= maxAttempts && billingAssociation != nil && !economicSettled {
		// Dead-lettering a paid repair with neither a capture nor a durable
		// operator flag would leave a pending costless hold looking healthy to
		// usage reconciliation and eligible for expiry. Patch only this exact
		// tenant/workload/hold association; a whole-row rewrite could erase a
		// concurrent cost observation or lifecycle update.
		if w.Store == nil {
			w.Log.Error("post-delete economic cleanup exhausted but its billing association cannot be protected; the repair lease will expire and retry",
				"cleanup", event.ID, "customer", event.CustomerID, "error", safeError)
			return
		}
		protected, err := w.Store.MarkWorkloadBillingManualAttention(ctx, event.CustomerID,
			billingAssociation.WorkloadRef, billingAssociation.HoldID)
		if err != nil || !protected {
			w.Log.Error("post-delete economic cleanup exhausted but manual attention was not durable; the repair lease will expire and retry",
				"cleanup", event.ID, "customer", event.CustomerID, "cause", safeError,
				"protected", protected, "error", err)
			return
		}
	}
	retryAt := w.now().Add(providerDeleteBackoff(event.Attempts))
	if err := w.Deletes.MarkOutboxFailed(ctx, event.ID, event.LeaseToken, safeError, retryAt, maxAttempts); err != nil {
		w.Log.Error("post-delete cleanup failed and the failure itself was not recorded; the lease expires and the repair is reclaimed",
			"cleanup", event.ID, "customer", event.CustomerID, "cause", safeError, "error", err)
		return
	}
	if event.Attempts >= maxAttempts {
		// Explicit on exhaustion: the money half of this teardown stopped when the
		// provider confirmed, so nobody is being overcharged — but a receipt, a
		// settlement or a node object is missing and only an operator can close it.
		w.Log.Error("post-delete cleanup exhausted automatic retries and is dead-lettered; the provider resource IS gone, the history behind it is incomplete",
			"cleanup", event.ID, "customer", event.CustomerID, "aggregate", event.AggregateID,
			"attempts", event.Attempts, "error", safeError)
		return
	}
	w.Log.Warn("post-delete cleanup failed; will retry",
		"cleanup", event.ID, "customer", event.CustomerID, "attempt", event.Attempts,
		"retry_at", retryAt.UTC().Format(time.RFC3339), "error", safeError)
}

// cleanup runs the existing post-provider steps for a resource that is already
// confirmed gone. It returns whether the prepaid obligation was provably
// settled so exhaustion can distinguish an unrelated cleanup failure from an
// economically unsafe one. Every step is idempotent, so a replay after a crash
// costs a repeated write and nothing else.
func (w *ProviderDeleteWorker) cleanup(ctx context.Context, b *state.Burst, job teardownJob, confirmedAt time.Time) (bool, error) {
	if w.Store == nil {
		return false, errors.New("state store is unavailable for post-delete cleanup")
	}

	// Provider absence is the economic hinge. Freeze the cost at its durable
	// confirmation timestamp and settle the prepaid hold before mesh, account,
	// PodCIDR, node, or legacy-state cleanup can fail. Replays use first-write-
	// wins cost persistence and stable settlement keys.
	observed := job.Cost
	if observed == nil {
		if confirmedAt.IsZero() {
			return false, errors.New("provider confirmation timestamp is unavailable for cost settlement")
		}
		confirmed := providerDeleteCostForBurst(b, confirmedAt.UTC())
		observed = &confirmed
	}
	if w.costRecorder() != nil {
		recorded, err := w.costRecorder().EnsureWorkloadCostForBurst(ctx, *observed)
		if err != nil {
			return false, fmt.Errorf("cost receipt not durable: %w", err)
		}
		if !recorded {
			if b.Billing != nil {
				return false, errors.New("authoritative cost receipt is unavailable for billed burst")
			}
			w.Log.Info("deleted burst has no workload row for a cost observation; recording the independent reap receipt",
				"burst", b.ID, "customer", b.CustomerID)
		}
	} else if b.Billing != nil {
		return false, errors.New("cost recorder is unavailable for billed burst")
	}
	if b.Billing != nil {
		if err := settleBurstBilling(ctx, w.Billing, b, observed); err != nil {
			return false, fmt.Errorf("prepaid settlement failed: %w", err)
		}
	}
	economicSettled := true

	// MarkProviderDeleteSucceeded and this repair are one durable handoff. The
	// fast path retires the live burst immediately after that transaction, but a
	// process can die in the gap. Repeating the idempotent claim here makes the
	// repair close that gap instead of acknowledging cleanup while a deleted GPU
	// remains visible as running (and continues to consume admission capacity).
	// Do this before fallible mesh cleanup so an unrelated coordination outage
	// cannot keep a provider-confirmed-absent resource live in the state store.
	if _, _, err := w.Store.ClaimBurst(b.ID); err != nil {
		return economicSettled, fmt.Errorf("live burst retirement not durable: %w", err)
	}
	if w.Reaper == nil {
		return economicSettled, errors.New("provider delete worker has no post-provider cleanup configured")
	}
	if err := w.Reaper.CleanupMesh(ctx, b); err != nil {
		return economicSettled, fmt.Errorf("mesh cleanup failed: %w", err)
	}
	if b.CloudAccountID != "" {
		if err := w.Store.ReleaseCloudAccountLease(ctx, b.ID); err != nil {
			return economicSettled, fmt.Errorf("cloud-account lease release failed: %w", err)
		}
	}
	// The provider node is gone, so its /24 can no longer collide with a new
	// burst's — see releasePodSlot for why this may not run any earlier.
	release := w.releasePodSlot
	if release == nil {
		release = releasePodSlot
	}
	if err := release(ctx, w.Store, w.Log, b); err != nil {
		return economicSettled, fmt.Errorf("pod slot release failed: %w", err)
	}
	if recorded, err := w.Store.RecordBurstReap(ctx, b.ID, b.CustomerID); err != nil {
		return economicSettled, fmt.Errorf("teardown receipt not durable: %w", err)
	} else if !recorded {
		return economicSettled, errors.New("teardown receipt not durable")
	}
	if err := drainBurstNode(ctx, w.Store, w.Commands, w.Log, b, job.Reason); err != nil {
		return economicSettled, fmt.Errorf("node cleanup not acknowledged: %w", err)
	}
	return economicSettled, nil
}

func providerDeleteCostForBurst(b *state.Burst, confirmedAt time.Time) state.WorkloadCost {
	observed := workloadCostForBurst(b, confirmedAt)
	observed.Basis = state.WorkloadCostBasisRateRuntimeToProviderDelete
	return observed
}

// fail records the failure against the durable operation. Every caller is on
// the pre-provider side of the hinge, so this is always a resource that may
// still exist: the operation stays nonterminal, the burst stays live-cost, and
// exhaustion is manual_attention rather than a silent give-up.
//
// It never restores the legacy live burst record either — the lifecycle row is
// what says whether the resource exists, and resurrecting the burst would hand
// a second reaper a resource this operation still owns.
func (w *ProviderDeleteWorker) fail(ctx context.Context, record lifecycle.ProviderDeleteRecord, safeError string, maxAttempts int) {
	w.recordReap("fail")
	retryAt := w.now().Add(providerDeleteBackoff(record.Attempts))
	if err := w.Deletes.MarkProviderDeleteFailed(ctx, record.ID, record.LeaseToken, safeError, retryAt, maxAttempts); err != nil {
		w.Log.Error("provider delete failed and the failure itself was not recorded; the lease expires and the operation is reclaimed",
			"burst", record.BurstID, "delete", record.ID, "cause", safeError, "error", err)
		return
	}
	if record.Attempts >= maxAttempts {
		w.Log.Error("provider delete exhausted automatic retries; durable manual-attention record created",
			"burst", record.BurstID, "customer", record.CustomerID, "provider", record.Provider,
			"provider_resource", record.ProviderResourceID, "attempts", record.Attempts, "error", safeError)
		return
	}
	w.Log.Warn("provider delete failed; will retry",
		"burst", record.BurstID, "attempt", record.Attempts,
		"retry_at", retryAt.UTC().Format(time.RFC3339), "error", safeError)
}

func (w *ProviderDeleteWorker) recordReap(result string) {
	if w.Cost != nil {
		w.Cost.RecordReap(result)
	}
}

func (w *ProviderDeleteWorker) costRecorder() workloadCostRecorder {
	if w.CostRecorder != nil {
		return w.CostRecorder
	}
	if w.Store == nil {
		return nil
	}
	return w.Store
}

func (w *ProviderDeleteWorker) lease() time.Duration {
	if w.Lease > 0 {
		return w.Lease
	}
	return providerDeleteLease
}

func (w *ProviderDeleteWorker) interval() time.Duration {
	if w.Interval > 0 {
		return w.Interval
	}
	return providerDeleteInterval
}

func (w *ProviderDeleteWorker) maxAttempts() int {
	if w.MaxAttempts > 0 {
		return w.MaxAttempts
	}
	return providerDeleteMaxAttempts
}

// attemptTimeout keeps one provider or cleanup dependency from monopolizing
// the serial worker beyond its lease. The default leaves a fifth of the lease
// for recording the result before another replica may reclaim the row.
func (w *ProviderDeleteWorker) attemptTimeout() time.Duration {
	limit := w.lease() * 4 / 5
	if limit <= 0 {
		limit = w.lease()
	}
	if w.AttemptTimeout > 0 && w.AttemptTimeout < limit {
		return w.AttemptTimeout
	}
	return limit
}

func (w *ProviderDeleteWorker) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

// providerDeleteBackoff doubles per attempt and is capped, so a provider
// outage is retried patiently rather than hammered.
func providerDeleteBackoff(attempts int) time.Duration {
	backoff := providerDeleteBaseBackoff
	for i := 1; i < attempts && backoff < providerDeleteMaxBackoff; i++ {
		backoff *= 2
	}
	if backoff > providerDeleteMaxBackoff {
		backoff = providerDeleteMaxBackoff
	}
	return backoff
}

// decodeProviderDeleteJob reconstructs the teardown payload the delete was
// recorded with. It is deliberately the SAME struct the broker path carries, so
// there is exactly one description of "what a teardown needs".
func decodeProviderDeleteJob(payload []byte) (teardownJob, error) {
	var job teardownJob
	if len(payload) == 0 {
		return teardownJob{}, errors.New("empty payload")
	}
	if err := json.Unmarshal(payload, &job); err != nil {
		return teardownJob{}, err
	}
	if job.Burst.ID == "" {
		return teardownJob{}, errors.New("payload carries no burst")
	}
	return job, nil
}
