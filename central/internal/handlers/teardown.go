package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/broker"
)

const (
	// TeardownStream and TeardownGroup are exported within central so the
	// metrics collector observes the exact queue the worker consumes.
	TeardownStream           = "yscale:teardown"
	TeardownGroup            = "teardown"
	teardownDeadLetterStream = "yscale:teardown:dead"
	// Keep package-local names for the worker and its same-package tests.
	teardownStream = TeardownStream
	teardownGroup  = TeardownGroup
	// teardownMaxAttempts caps redelivery of a teardown job that keeps failing
	// (a poison job — e.g. a backend that no longer recognises the id, or a
	// permanently-revoked credential). After this many deliveries the complete
	// job is copied to teardownDeadLetterStream BEFORE the live queue item is
	// acknowledged. If that durable copy fails, the live item remains pending
	// and will retry rather than reducing an owned paid resource to a log line.
	teardownMaxAttempts = 8
)

// teardownJob is the durable teardown request carried on the broker. It embeds
// the whole burst so the worker is self-contained: it needs no database read,
// which is what lets it run as a separate process (the OSS/enterprise seam).
type teardownJob struct {
	Burst  state.Burst `json:"burst"`
	Reason string      `json:"reason"`
	// Cost is frozen when teardown is accepted, not when a worker eventually
	// receives it. The worker replays this write before acknowledging the queue,
	// so a transient Postgres failure cannot erase the durable receipt an agent
	// needs after its NodeEventAck was lost.
	Cost *state.WorkloadCost `json:"cost,omitempty"`
}

// teardownDeadLetter is the operator-recovery record for a queue item that has
// exhausted automatic retries. SourceMessageID is stable across redelivery, so
// a consumer can deduplicate the at-least-once dead-letter publication if the
// worker dies after publishing but before acknowledging the source message.
type teardownDeadLetter struct {
	SourceMessageID string      `json:"source_message_id"`
	Job             teardownJob `json:"job"`
	Deliveries      int64       `json:"deliveries"`
	Error           string      `json:"error"`
	FailedAt        time.Time   `json:"failed_at"`
}

// enqueueTeardown publishes a durable teardown job for a claimed burst.
func (h *Workloads) enqueueTeardown(ctx context.Context, b *state.Burst, reason string) (*state.WorkloadCost, error) {
	observed := workloadCostForBurst(b, time.Now().UTC())
	payload, err := json.Marshal(teardownJob{Burst: *b, Reason: reason, Cost: &observed})
	if err != nil {
		return nil, err
	}
	if err := h.Teardowns.Publish(ctx, teardownStream, payload); err != nil {
		return nil, err
	}
	return &observed, nil
}

// TeardownWorker drains the durable teardown queue: for each job it performs
// the cloud teardown (Reaper) + node drain, acking on success and leaving a
// failed job un-ack'd so the broker redelivers it after its retry window
// (surviving a central restart). It can run in-process in central or as its own
// process consuming the same stream. Idempotent: DeleteNode is a no-op once the
// node is gone, so a redelivered job is safe.
type TeardownWorker struct {
	Broker       broker.Broker
	Reaper       Reaper
	Store        *state.Store
	CostRecorder workloadCostRecorder
	Log          *slog.Logger
	Consumer     string // unique per worker instance (e.g. the central pod name)
	// Commands is the durable connector-command ledger the node drain is written
	// to. It is what lets this worker run in a process that holds no connector
	// socket at all: it persists the drain, and the replica that does own the
	// socket delivers it. nil keeps the legacy synchronous ack-wait below.
	Commands ConnectorCommandLedger
}

type workloadCostRecorder interface {
	EnsureWorkloadCostForBurst(context.Context, state.WorkloadCost) (bool, error)
}

// Run consumes until ctx is cancelled. Consume blocks internally up to its poll
// window, so this is not a busy loop.
func (w *TeardownWorker) Run(ctx context.Context) {
	w.Log.Info("teardown worker started", "stream", teardownStream, "consumer", w.Consumer)
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := w.Broker.Consume(ctx, teardownStream, teardownGroup, w.Consumer)
		if err != nil {
			w.Log.Warn("teardown consume failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if msg == nil {
			continue // poll window elapsed with nothing ready
		}
		w.process(ctx, msg)
	}
}

// process handles one job. Acks on success. A job that exhausts automatic
// retries is first copied to the durable dead-letter stream and only then
// acknowledged; if that copy cannot be secured, the original remains pending.
func (w *TeardownWorker) process(ctx context.Context, msg *broker.Message) {
	var job teardownJob
	if err := json.Unmarshal(msg.Payload, &job); err != nil {
		// An undecodable job can never identify a resource safely enough to
		// recover it. Ack it so corrupt queue data cannot starve valid teardown
		// work; queue corruption is separately visible as an error log.
		w.Log.Error("teardown job undecodable; dropping", "error", err)
		_ = w.ack(ctx, msg.ID)
		return
	}
	b := job.Burst
	if b.CloudAccountID != "" && w.Reaper == nil {
		w.Log.Warn("tenant cloud-account teardown has no reaper; job will retry", "burst", b.ID)
		return
	}

	if w.Reaper != nil {
		if err := w.Reaper.Teardown(ctx, &b); err != nil {
			if msg.Deliveries >= teardownMaxAttempts {
				w.deadLetter(ctx, msg, job, err, "provider teardown exhausted automatic retries")
				return
			}
			w.Log.Warn("teardown failed; will retry",
				"burst", b.ID, "attempt", msg.Deliveries, "reason", job.Reason, "error", err)
			return // do NOT ack → redelivered after the retry window
		}
	}
	if b.CloudAccountID != "" {
		if w.Store == nil {
			w.Log.Warn("teardown complete but cloud-account lease store is unavailable; job will retry", "burst", b.ID)
			return
		}
		if err := w.Store.ReleaseCloudAccountLease(ctx, b.ID); err != nil {
			w.Log.Warn("teardown complete but cloud-account lease release failed; job will retry",
				"burst", b.ID, "cloud_account", b.CloudAccountID, "attempt", msg.Deliveries, "error", err)
			return
		}
	}
	// The provider node is gone, so its /24 can no longer collide with a new
	// burst's. Released here rather than at claim time — see releasePodSlot.
	releasePodSlot(ctx, w.Store, w.Log, &b)

	// The queue item is also the recovery path for the customer-visible teardown
	// receipt. Workloads freezes this optimistically after Publish; if that write
	// failed, the worker retries it here with the exact same frozen timestamp and
	// runtime. Do not Ack on an error: an agent whose NodeEventAck was lost must
	// eventually be able to prove this teardown from durable history.
	if job.Cost != nil && w.costRecorder() != nil {
		recorded, err := w.costRecorder().EnsureWorkloadCostForBurst(ctx, *job.Cost)
		if err != nil {
			w.Log.Warn("teardown complete but cost receipt not durable; job will retry receipt write",
				"burst", b.ID, "attempt", msg.Deliveries, "recorded", recorded, "error", err)
			return
		}
		if !recorded {
			w.Log.Info("teardown has no workload row for a cost observation; recording the independent reap receipt",
				"burst", b.ID, "customer", b.CustomerID)
		}
	}
	// This tenant-bound receipt is the recovery proof for a lost NodeEventAck.
	// It is separate from workload cost because node-only capacity may have no
	// workload row at all. Never retire the queue item until other replicas and
	// restarts can verify it.
	if w.Store == nil {
		w.Log.Warn("teardown complete but reap receipt store is unavailable; job will retry", "burst", b.ID)
		return
	}
	if recorded, err := w.Store.RecordBurstReap(ctx, b.ID, b.CustomerID); err != nil || !recorded {
		w.Log.Warn("teardown complete but reap receipt not durable; job will retry receipt write",
			"burst", b.ID, "customer", b.CustomerID, "attempt", msg.Deliveries,
			"recorded", recorded, "error", err)
		return
	}

	// With a ledger this returns once the drain is DURABLE, not once a connector
	// acknowledged it: the ledger owns the retry and the dead-letter from here,
	// so holding the queue item open for an ack would only duplicate a recovery
	// that already exists. Without one it still waits for the ack, and a job that
	// exhausts its deliveries still dead-letters.
	if err := drainBurstNode(ctx, w.Store, w.Commands, w.Log, &b, job.Reason); err != nil {
		if msg.Deliveries >= teardownMaxAttempts {
			w.deadLetter(ctx, msg, job, err, "node cleanup exhausted automatic retries")
			return
		}
		w.Log.Warn("node cleanup not acknowledged; teardown job will retry",
			"burst", b.ID, "node", b.NodeName,
			"attempt", msg.Deliveries, "error", err)
		return
	}
	_ = w.ack(ctx, msg.ID)
	w.Log.Info("burst torn down via queue", "burst", b.ID, "backend", b.Backend, "reason", job.Reason)
}

// deadLetter secures the complete recovery record before retiring the live
// queue item. Publication and source acknowledgement cannot be atomic across the
// generic Broker interface, so the crash-safe direction is deliberate: publish
// first. A crash in the gap can duplicate the dead-letter record, but the stable
// SourceMessageID makes that harmless; acknowledging first could lose the only
// durable description of a still-billing resource.
func (w *TeardownWorker) deadLetter(ctx context.Context, msg *broker.Message, job teardownJob, cause error, summary string) {
	record := teardownDeadLetter{
		SourceMessageID: msg.ID,
		Job:             job,
		Deliveries:      msg.Deliveries,
		Error:           cause.Error(),
		FailedAt:        time.Now().UTC(),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		w.Log.Error("teardown dead-letter encode failed; live job left pending",
			"burst", job.Burst.ID, "attempts", msg.Deliveries, "error", err)
		return
	}
	if err := w.Broker.Publish(ctx, teardownDeadLetterStream, payload); err != nil {
		w.Log.Error("teardown dead-letter publish failed; live job left pending",
			"burst", job.Burst.ID, "attempts", msg.Deliveries, "error", err)
		return
	}
	if err := w.ack(ctx, msg.ID); err != nil {
		w.Log.Error("teardown dead-letter persisted but source ack failed; duplicate dead-letter is possible",
			"burst", job.Burst.ID, "source_message", msg.ID, "attempts", msg.Deliveries, "error", err)
		return
	}
	w.Log.Error(summary+"; durable manual-attention record created",
		"burst", job.Burst.ID, "backend", job.Burst.Backend, "backend_id", job.Burst.BackendID,
		"attempts", msg.Deliveries, "dead_letter_stream", teardownDeadLetterStream,
		"source_message", msg.ID, "error", cause)
}

// ack keeps acknowledgement authoritative and retention best-effort. Once
// XACK succeeds, a trim failure cannot be turned into a retry because the
// message is no longer pending; it is surfaced for operators and the next Ack
// retries compaction against the same monotonic group boundaries.
func (w *TeardownWorker) ack(ctx context.Context, msgID string) error {
	if err := w.Broker.Ack(ctx, teardownStream, teardownGroup, msgID); err != nil {
		return err
	}
	if trimmer, ok := w.Broker.(broker.AckedHistoryTrimmer); ok {
		if err := trimmer.TrimAckedHistory(ctx, teardownStream); err != nil {
			w.Log.Warn("teardown stream compaction failed; acknowledgement retained",
				"stream", teardownStream, "message", msgID, "error", err)
		}
	}
	return nil
}

func (w *TeardownWorker) costRecorder() workloadCostRecorder {
	if w.CostRecorder != nil {
		return w.CostRecorder
	}
	return w.Store
}
