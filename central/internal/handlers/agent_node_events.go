package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// BurstLifecycle is the teardown half of the node-event path: the single-winner
// reap every other path already goes through (*Workloads). It is an interface
// so the stream stays constructible without it — the OSS wiring and every test
// that only cares about routes or the handshake passes three arguments and gets
// a handler that records phases and tears nothing down.
//
// Narrow on purpose. A connector reporting that a node disappeared is allowed
// to trigger the reap central would have run anyway; it is not allowed to reach
// the decider, the cost meter, or the workload callbacks, all of which sit on
// the same *Workloads and none of which an agent event may drive.
//
// It answers with a ReapOutcome rather than a bool because "not reaped" is four
// different facts. The idle path keeps its preflight cordon for every one of
// them: an explicit retry must not admit new work, and every other outcome is
// too ambiguous to make an uncordon safe.
type BurstLifecycle interface {
	ReapBurst(ctx context.Context, burstID, reason string) ReapOutcome
}

// burstNodePhaseWriter is the durable half, and the store satisfies it. It
// exists as a seam because "fail-closed" is a claim about what happens when the
// WRITE fails, and *state.Store's persister is unexported — there is no other
// way to prove from here that a refused write tears nothing down.
type burstNodePhaseWriter interface {
	UpdateBurstNodePhase(ctx context.Context, u state.BurstNodePhaseUpdate) (bool, error)
}

// burstNodeRecordReader is the read BOTH teardown reports go through, and the
// store satisfies it too.
//
// Ownership alone is not enough for either of them, and a Removed is the case
// that proves it: a tenant may connect several clusters on one token, so a
// connector compromised inside one of them could name a burst running in
// another and, with only a tenant check in the way, trigger the claim that
// destroys it. What decides whether a teardown is allowed lives on the burst
// record central itself wrote — which cluster it booked the capacity in, what it
// named the node, and (for an Idle) whether this is nodeOnly capacity at all.
// None of that may be taken from the event: it is what the event is checked
// against.
//
// It is a seam for the writer's reason — an unreachable backend must leave the
// report retryable, and only a read that can fail proves it.
type burstNodeRecordReader interface {
	BurstForNodeTeardown(ctx context.Context, burstID, customerID string) (*state.Burst, bool, error)
}

const (
	// maxNodeEventReasonBytes bounds the note attached to a phase. It is stored
	// on the burst and rendered to the customer, so it is capped before it is
	// ever written rather than trusted at the length the connector sent.
	maxNodeEventReasonBytes = 256

	// maxBurstIDBytes is longer than any id central mints. The point is not to
	// validate the format — the store decides whether a burst exists — but to
	// keep an unbounded string out of a log line and a query parameter.
	maxBurstIDBytes = 128

	// nodeEventTimeout bounds the durable work one reported phase may do, so a
	// slow database cannot pin the connection's read goroutine indefinitely.
	nodeEventTimeout = 30 * time.Second

	// nodeReapTimeout bounds the teardown a Removed event triggers. Generous
	// next to nodeEventTimeout because it covers the broker enqueue and, with no
	// broker, an inline provider delete plus a drain acknowledgement.
	nodeReapTimeout = 2 * time.Minute

	// idlePreflightTimeout bounds the cordon-and-recheck an idle teardown must
	// pass before anything is claimed. Short: it is one Get, one Update and one
	// List against the connector's own API server, and a connector that cannot
	// answer that promptly is one central will not destroy a node on the word of.
	idlePreflightTimeout = 30 * time.Second

	// idleReleaseTimeout bounds the give-back of a cordon whose teardown did not
	// happen. It is one conditional patch, and it runs after the reap has already
	// spent its own budget — so it is short, and expiring costs a log line naming
	// the node an operator has to look at rather than any further action.
	idleReleaseTimeout = 15 * time.Second

	// maxPreflightResultBytes and maxCordonResourceVersionBytes bound the cordon
	// token a connector returns in its preflight acknowledgement. Central never
	// interprets it — it goes straight back to the same connector — so these keep
	// an unbounded string out of a log line and a queued frame, nothing more.
	maxPreflightResultBytes       = 1024
	maxCordonResourceVersionBytes = 128

	nodeRemovedReason = "burst node removed from the cluster"
	nodeIdleReason    = "burst node reported idle by its connector"

	// maxWorkloadIDBytes bounds the workload id on a PodEvent. Central mints
	// short ids; this cap keeps an unbounded string out of log lines and queries.
	maxWorkloadIDBytes = 128

	// maxTimestampSkew is the maximum clock skew tolerated in pod event
	// timestamps. A ScheduledAt far in the future is rejected as impossible
	// and replaced with receipt time.
	maxTimestampSkew = 10 * time.Minute
)

// handlePodEvent records a connector-observed pod lifecycle event on the
// workload. The scheduling observation (Scheduled=true, non-empty PodName and
// NodeName) is stamped once and never overwritten.
//
// Tenancy is the connection's, never the body's: agent.CustomerID comes from
// the bearer token this socket was admitted under.
//
// ScheduledAt is validated as WIRE evidence before it is used. A non-nil zero
// (a wire bug or an aggressive default) and a value more than maxTimestampSkew
// in the future (a clock-skewed or forged timestamp) are REJECTED entirely —
// no persist, no ACK — because persisting receipt time under a different
// identity would leave a fresher event uncleared while an ACK for a value
// central does not hold clears the connector's cache. A nil ScheduledAt is
// the legacy path: it is persisted at receipt time and its ACK carries
// PodName but a nil ScheduledAt, which the versioned connector will refuse
// as a match for any cached event that itself has ScheduledAt.
func (h *AgentStream) handlePodEvent(agent *state.Agent, ev protocol.PodEvent) {
	workloadID := strings.TrimSpace(ev.WorkloadID)
	if workloadID == "" || len(ev.WorkloadID) > maxWorkloadIDBytes {
		h.Log.Warn("pod event with no usable workload id", "agent", agent.ID)
		return
	}
	h.Log.Info("pod event",
		"agent", agent.ID,
		"workload", workloadID,
		"phase", ev.Phase,
		"node", ev.NodeName,
		"pod", ev.PodName,
		"scheduled", ev.Scheduled,
		"scheduling_state", ev.SchedulingState,
		"scheduling_reason", ev.SchedulingReason,
	)
	if ev.SchedulingState == protocol.SchedulingStateWaiting {
		if ev.Scheduled {
			h.Log.Warn("pod event rejected: Waiting conflicts with Scheduled=true", "agent", agent.ID, "workload", workloadID)
			return
		}
		h.handlePodEventWaiting(agent, workloadID, ev)
		return
	}
	if ev.SchedulingState != "" && ev.SchedulingState != protocol.SchedulingStateScheduled {
		return
	}
	if !ev.Scheduled || ev.PodName == "" || ev.NodeName == "" {
		return
	}
	h.handlePodEventScheduled(agent, workloadID, ev)
}

func (h *AgentStream) handlePodEventWaiting(agent *state.Agent, workloadID string, ev protocol.PodEvent) {
	if !protocol.ValidSchedulingReason(ev.SchedulingReason) || ev.PodName == "" || !isDNSSubdomain(ev.PodName) {
		return
	}
	if ev.SchedulingObservedAt == nil || ev.SchedulingObservedAt.IsZero() {
		return
	}
	now := time.Now().UTC()
	if ev.SchedulingObservedAt.After(now.Add(maxTimestampSkew)) {
		return
	}
	observedAt := ev.SchedulingObservedAt.UTC()
	ctx, cancel := context.WithTimeout(context.Background(), nodeEventTimeout)
	defer cancel()
	result, err := h.Store.StampWorkloadSchedulingObservation(
		ctx,
		workloadID,
		agent.CustomerID,
		agent.ClusterID,
		protocol.SchedulingStateWaiting,
		ev.SchedulingReason,
		protocol.SchedulingReasonMessage(ev.SchedulingReason),
		ev.PodName,
		observedAt,
	)
	if err != nil {
		h.Log.Error("pod scheduling observation persist failure", "agent", agent.ID, "workload", workloadID, "error", err)
		return
	}
	if result == state.ObservationApplied || result == state.ObservationResolved {
		h.ackSchedulingPodEvent(agent, protocol.PodEventAck{
			WorkloadID:           workloadID,
			PodName:              ev.PodName,
			SchedulingState:      protocol.SchedulingStateWaiting,
			SchedulingObservedAt: &observedAt,
		})
	}
}

func (h *AgentStream) handlePodEventScheduled(agent *state.Agent, workloadID string, ev protocol.PodEvent) {
	if !isDNSSubdomain(ev.PodName) {
		h.Log.Warn("pod event rejected: pod name is not a valid DNS subdomain", "agent", agent.ID, "pod", ev.PodName)
		return
	}
	if !isDNSSubdomain(ev.NodeName) {
		h.Log.Warn("pod event rejected: node name is not a valid DNS subdomain", "agent", agent.ID, "node", ev.NodeName)
		return
	}
	podName := ev.PodName
	nodeName := ev.NodeName
	now := time.Now().UTC()
	scheduledAt, legacy, ok := validatePodEventScheduledAt(ev.ScheduledAt, now)
	if !ok {
		h.Log.Warn("pod event rejected: ScheduledAt is invalid; not persisting, not acknowledging",
			"agent", agent.ID, "workload", workloadID, "pod", podName,
			"scheduled_at", ev.ScheduledAt)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), nodeEventTimeout)
	defer cancel()
	outcome, persisted, err := h.Store.StampWorkloadPodObservationOutcome(ctx, workloadID, agent.CustomerID, agent.ClusterID, podName, nodeName, scheduledAt)
	if err != nil {
		h.Log.Error("pod event persist failure", "agent", agent.ID, "workload", workloadID, "error", err)
		return
	}
	if !outcome.Acknowledgeable() {
		// Rejected: unknown workload, wrong tenant/cluster/burst, or a stale
		// in-memory phantom whose durable row is gone. Leaving the connector's
		// retry outstanding is the whole point of the outcome distinction.
		return
	}
	if ev.SchedulingState == protocol.SchedulingStateScheduled {
		if ev.SchedulingObservedAt == nil || ev.SchedulingObservedAt.IsZero() || ev.SchedulingObservedAt.After(now.Add(maxTimestampSkew)) {
			return
		}
		observedAt := ev.SchedulingObservedAt.UTC()
		result, err := h.Store.StampWorkloadSchedulingObservation(
			ctx,
			workloadID,
			agent.CustomerID,
			agent.ClusterID,
			protocol.SchedulingStateScheduled,
			"",
			"",
			ev.PodName,
			observedAt,
		)
		if err != nil {
			h.Log.Error("scheduling observation persist failure; withholding ACK for retry", "agent", agent.ID, "workload", workloadID, "error", err)
			return
		}
		if result != state.ObservationApplied && result != state.ObservationResolved {
			return
		}
		ack := protocol.PodEventAck{
			WorkloadID:           workloadID,
			SchedulingState:      protocol.SchedulingStateScheduled,
			SchedulingObservedAt: &observedAt,
		}
		if persisted != nil {
			ack.PodName = persisted.PodName
			if !legacy && !persisted.ScheduledAt.IsZero() {
				t := persisted.ScheduledAt.UTC()
				ack.ScheduledAt = &t
			}
		}
		h.ackSchedulingPodEvent(agent, ack)
		return
	}
	// Echo the EXACT durable identity. On Applied that is the values we just
	// persisted; on Resolved it is what the durable store confirmed — which
	// may differ if a buggy connector retried a workload with a different
	// pod. The ACK must never silently substitute what central sent for what
	// central holds.
	h.ackPodEvent(agent, workloadID, persisted, legacy)
}

// validatePodEventScheduledAt guards the ScheduledAt wire value. It returns
// the value to persist, whether this is a legacy (nil-source) path, and ok=false
// when the field is present but unusable — a zero time.Time or a value more than
// maxTimestampSkew in the future. Nil is the legacy shape and returns receipt
// time paired with legacy=true.
func validatePodEventScheduledAt(source *time.Time, now time.Time) (time.Time, bool, bool) {
	if source == nil {
		return now, true, true
	}
	if source.IsZero() {
		return time.Time{}, false, false
	}
	if source.After(now.Add(maxTimestampSkew)) {
		return time.Time{}, false, false
	}
	return source.UTC(), false, true
}

// handleNodeEvent applies one reported burst-node lifecycle phase. Joining,
// Ready and NotReady are durable status and land here; Removed is a teardown and
// goes to handleRemovedNode.
//
// Tenancy is the connection's, never the body's: agent.CustomerID comes from
// the bearer token this socket was admitted under, and it is passed into the
// same write that applies the phase so ownership cannot be decided against a
// record another replica has already claimed. A burst id belonging to another
// tenant is indistinguishable here from one that does not exist, which is the
// intent — a connector must not be able to probe the fleet for live ids.
//
// Nothing else in the event is trusted. The phase is checked against the closed
// set before anything is stored, the reason is bounded, and a body that does not
// decode is dropped: a frame central cannot read must not be the thing that
// finishes a customer's workload or destroys a billing node.
func (h *AgentStream) handleNodeEvent(agent *state.Agent, body json.RawMessage) {
	var ev protocol.NodeEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		h.Log.Warn("decode node event", "agent", agent.ID, "error", err)
		return
	}
	burstID := strings.TrimSpace(ev.BurstID)
	// The RAW field is what the bound applies to, because the raw field is what
	// an acknowledgement echoes: the connector keys its cache on the id it sent,
	// so trimming for the length check would let an oversized id through on the
	// only string that goes back out.
	if burstID == "" || len(ev.BurstID) > maxBurstIDBytes {
		h.Log.Warn("node event with no usable burst id", "agent", agent.ID, "phase", ev.Phase)
		return
	}
	if !protocol.ValidNodePhase(ev.Phase) {
		h.Log.Warn("node event with an unknown phase; ignored",
			"agent", agent.ID, "burst", burstID, "phase", ev.Phase)
		return
	}

	// Removed is not a phase to record, it is a teardown to secure. It takes the
	// path that does the irreversible work FIRST and writes nothing durable
	// before it — see handleRemovedNode.
	if ev.Phase == protocol.NodePhaseRemoved {
		h.handleRemovedNode(agent, burstID, ev)
		return
	}
	// Idle is not a phase either, and unlike Removed it is not even a report:
	// it is a request to destroy a node that is still running. It is authorised
	// against central's own record before anything happens — see handleIdleNode.
	if ev.Phase == protocol.NodePhaseIdle {
		h.handleIdleNode(agent, burstID, ev)
		return
	}

	phaseAt, sourceTimestamped := resolveNodePhaseTimestamp(ev.ObservedAt)
	receiptTime := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), nodeEventTimeout)
	defer cancel()
	update := state.BurstNodePhaseUpdate{
		BurstID:           burstID,
		CustomerID:        agent.CustomerID,
		ClusterID:         agent.ClusterID,
		Phase:             ev.Phase,
		Reason:            nodeEventReason(ev.Reason),
		ObservedAt:        phaseAt,
		SourceTimestamped: sourceTimestamped,
		ReceiptTime:       receiptTime,
		// Carried through the SAME write, so the burst's silence ceiling can only
		// ever be reset by an event that passed this tenant and burst check. The
		// flag is taken at face value otherwise: a connector claiming it read the
		// cluster's pods is claiming it about its own cluster, and the worst it can
		// buy itself is the teardown it could have asked for outright.
		OccupancyObserved: ev.OccupancyObserved,
		GPUAllocatable:    ev.GPUAllocatable,
		GPUAllocatableAt:  ev.GPUAllocatableAt,
	}
	applied, err := h.nodePhaseWriter().UpdateBurstNodePhase(ctx, update)
	if err != nil {
		// Fail-closed. The durable record is what any other replica reads to know
		// what this node is doing, so a phase central could not write is a phase
		// central does not claim. Nothing is torn down on this path either way,
		// and the connector's next transition report re-asserts what is current.
		h.Log.Error("burst node phase not recorded; not acting on it",
			"agent", agent.ID, "customer", agent.CustomerID,
			"burst", burstID, "phase", ev.Phase, "error", err)
		return
	}
	if !applied {
		// The burst is gone or belongs to another tenant. The first is the
		// ORDINARY case — a node object outliving the teardown that already
		// claimed the burst — so this is not a warning.
		h.Log.Info("node event changed nothing; the burst is already gone, terminal, or not this tenant's",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID, "phase", ev.Phase)
		return
	}
	h.Log.Info("burst node phase recorded",
		"agent", agent.ID, "customer", agent.CustomerID,
		"burst", burstID, "node", ev.NodeName, "phase", ev.Phase)
}

// handleRemovedNode runs the one phase that is a lifecycle event.
//
// NOTHING DURABLE HAPPENS FIRST. The old order — record the terminal phase, then
// tear down on a goroutine nobody tracks — left a window where a crash between
// the two produced a burst durably marked Removed with its node still running
// and still billing, and the store's terminal guard then refused every replay of
// the report that would have retried the teardown. So the write is gone: on the
// winning path ClaimBurst deletes the row anyway, and there is no phase left to
// record on a burst that no longer exists.
//
// What remains before the teardown is a READ, and it reads the RECORD rather
// than asking a yes/no ownership question. The claim deletes by burst id alone,
// so everything that makes this connector the right one to trigger it has to be
// established first:
//
//   - The TENANT is the socket's, and the read is scoped to it, so a connector
//     naming another tenant's burst id gets the same answer as one naming an id
//     that was never minted.
//   - The CLUSTER must be the one the burst was booked in. A tenant may connect
//     several clusters on one token, and a tenant check alone let a connector
//     compromised inside one of them name capacity running in another and have
//     central destroy it. A legacy record with no stored cluster is unverifiable
//     and is refused rather than guessed from current connectivity.
//   - The NODE NAME must be the one central assigned. The connector derives the
//     burst id from the node name, so this closes the loop: the pair has to be
//     the pair central minted. It is required NON-EMPTY with no fallback of any
//     kind: a record that cannot say what it named the node is unverifiable, and
//     an unverifiable claim is not a weaker claim when what it buys is the
//     destruction of a billing node — the watchdog is what ends a burst nothing
//     can vouch for.
//
// A mismatch tears nothing down, finishes nothing, and acknowledges nothing. The
// one thing that still answers on a record this read cannot find is the durable
// teardown receipt: a burst already GONE is the ordinary case — a node object
// outliving the teardown that claimed it — and the receipt is what releases the
// connector from re-reporting it.
//
// A brief window between the read and the claim is accepted, as it was between
// the write and the claim before it. Central mints burst ids, so a row that
// changes tenant or cluster under a fixed id is not a thing that happens; what
// does happen is the burst being claimed by another reaper in the gap, and the
// claim itself is what decides that.
func (h *AgentStream) handleRemovedNode(agent *state.Agent, burstID string, ev protocol.NodeEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), nodeEventTimeout)
	defer cancel()
	b, found, err := h.nodeRecordReader().BurstForNodeTeardown(ctx, burstID, agent.CustomerID)
	if err != nil {
		// Unknown, not "no". The burst may well be live, so this tears nothing
		// down AND acknowledges nothing: the connector keeps the report and
		// re-sends it, which is the retry that makes a state-backend outage cost
		// a delay rather than an unreaped node.
		h.Log.Error("burst could not be read; not acting on the removal and not acknowledging it",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID, "error", err)
		return
	}
	if !found {
		// Gone and foreign are deliberately indistinguishable. Absence alone is
		// not proof of teardown: DELETE ... RETURNING can commit while its caller
		// receives an unknown-outcome error. A frozen cost receipt is written only
		// after teardown was queued or completed, so only that proof releases the
		// connector from its retry.
		h.Log.Info("removed node names no live burst of this tenant's; checking for a teardown receipt",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID)
		h.ackIfBurstReapRecorded(agent, burstID, ev)
		return
	}
	if b.ClusterID == "" || b.ClusterID != agent.ClusterID {
		h.Log.Warn("removal came from a cluster that cannot be shown to be the one the burst was booked in; ignored",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID,
			"reporting_cluster", agent.ClusterID, "burst_cluster", b.ClusterID)
		return
	}
	if b.NodeName == "" || b.NodeName != ev.NodeName {
		h.Log.Warn("removal names a node this burst does not have; ignored",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID,
			"reported_node", ev.NodeName, "burst_node", b.NodeName)
		return
	}
	go h.reapRemovedBurst(agent, burstID, ev)
}

// reapRemovedBurst runs the teardown a verified Removed owes, OFF the read
// goroutine.
//
// It cannot run inline. reapBurst's in-process path sends this connector a
// drain_node command and waits for its CommandAck — and that acknowledgement is
// delivered by readPump, the goroutine that would be sitting here. Reaping
// inline wedges the connection for the whole ack timeout on every burst torn
// down this way, and stalls every frame queued behind it.
//
// Exactly-once is not enforced here: ClaimBurst elects one winner across
// replicas, so a repeated Removed, a normal completion racing this, and the
// watchdog all converge on a single teardown. What IS enforced here is that only
// the winner touches the workload, and that the connector is released from its
// report only once the burst is definitively somebody's — this call's, another
// winner's, or nobody's.
func (h *AgentStream) reapRemovedBurst(agent *state.Agent, burstID string, ev protocol.NodeEvent) {
	reaped := false
	if h.nodeReapDone != nil {
		defer func() { h.nodeReapDone(burstID, reaped) }()
	}
	if h.lifecycle == nil {
		// No teardown seam: the node is still up, so this is NOT resolved and is
		// not acknowledged. The connector keeps re-reporting it, which is the
		// only pressure on a deployment that has no reap path wired.
		h.Log.Warn("burst node removed but no lifecycle seam is wired; the burst stands until the reaper watchdog takes it",
			"customer", agent.CustomerID, "burst", burstID)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), nodeReapTimeout)
	defer cancel()
	if !h.lifecycle.ReapBurst(ctx, burstID, nodeRemovedReason).Reaped() {
		// Three different things, and they do not get the same answer. The claim
		// may have been lost to another winner — settled, acknowledge it — or the
		// claim errored, or the teardown failed and re-queued the record, and both
		// of those leave a node that is still running.
		//
		// The burst row is what tells them apart: a lost claim leaves nothing
		// behind, while both failures leave the burst there to be reaped again. An
		// unreadable answer is treated as the failing case and left unacknowledged.
		h.ackIfBurstReapRecorded(agent, burstID, ev)
		return
	}
	reaped = true

	// The node backing this workload is gone, so the run did not finish on its
	// own. FinishWorkloadForBurst applies the onlyIfUnfinished guard: a workload
	// whose connector already reported Succeeded keeps that status and the
	// outcome receipt that explains it. This is the record catching up with a
	// destroyed node, never a teardown overwriting a reported result.
	if _, err := h.Store.FinishWorkloadForBurst(ctx, burstID, "failed", time.Now().UTC()); err != nil {
		h.Log.Warn("workload not marked failed after its node was removed; the burst is torn down either way",
			"customer", agent.CustomerID, "burst", burstID, "error", err)
	} else {
		h.Log.Info("burst reaped after its node was removed",
			"customer", agent.CustomerID, "burst", burstID)
	}
	// A queue handoff is not yet provider teardown. Release the connector only
	// after the worker (or the inline path) has written the tenant-bound receipt;
	// a lost ack then rechecks that same durable proof.
	h.ackIfBurstReapRecorded(agent, burstID, ev)
}

// handleIdleNode authorises, and then runs, the teardown an idle report asks
// for.
//
// This is the one node event that is a REQUEST rather than a report, and it is
// the only one whose subject is a node that is still up. A Removed that central
// gets wrong destroys a Node object that Kubernetes had already lost; an Idle
// that central gets wrong destroys a node serving a customer's traffic. So
// nothing in the event is taken on trust and NOTHING happens before the record
// central itself wrote has been read back:
//
//   - The tenant is the socket's, and the read is scoped to it, so a connector
//     naming another tenant's burst id gets the same answer as one naming an id
//     that does not exist.
//   - The CLUSTER must be the one the burst was booked in. A tenant may connect
//     several clusters on one token, and without this any of them could ask for
//     the teardown of capacity running in another. It is required NON-EMPTY, on
//     the same rule a removal follows: a record that cannot say where central
//     booked the capacity is unverifiable, and the connectivity central happens
//     to see now is not evidence about a booking made earlier.
//   - The NODE NAME must be the one central assigned. The connector derives the
//     burst id from the node name, so this closes the loop: the pair has to be
//     the pair central minted, not a burst id attached to whatever node the
//     event happened to name. A burst with no recorded node name cannot satisfy
//     it and is refused — an unverifiable claim is not a weaker claim here, and
//     idle teardown is new enough that no such record is in flight.
//
// A mismatch tears nothing down AND acknowledges nothing. It is either a bug in
// a connector or an attempt on someone else's capacity, and the honest response
// to both is to leave the request outstanding and say so in the log.
//
// Passing all of it is still not enough to destroy anything. Every check here is
// against central's own record, which cannot see a pod bound since the
// connector's last sweep; the preflight in reapIdleBurst is what closes that.
func (h *AgentStream) handleIdleNode(agent *state.Agent, burstID string, ev protocol.NodeEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), nodeEventTimeout)
	defer cancel()
	b, found, err := h.nodeRecordReader().BurstForNodeTeardown(ctx, burstID, agent.CustomerID)
	if err != nil {
		// Unknown, not "no" — the same call a state-backend outage forces on every
		// other lifecycle path. The connector keeps the request and re-sends it.
		h.Log.Error("burst could not be read; not acting on the idle report and not acknowledging it",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID, "error", err)
		return
	}
	if !found {
		// Gone and foreign are indistinguishable, exactly as they are for a
		// removal. Gone is the ordinary case: a connector that asked for a teardown
		// central has since run, whose receipt is what releases it.
		h.Log.Info("idle report names no live burst of this tenant's; checking for a teardown receipt",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID)
		h.ackIfBurstReapRecorded(agent, burstID, ev)
		return
	}
	if b.ClusterID == "" || b.ClusterID != agent.ClusterID {
		h.Log.Warn("idle report came from a cluster that cannot be shown to be the one the burst was booked in; ignored",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID,
			"reporting_cluster", agent.ClusterID, "burst_cluster", b.ClusterID)
		return
	}
	if b.NodeName == "" || b.NodeName != ev.NodeName {
		h.Log.Warn("idle report names a node this burst does not have; ignored",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID,
			"reported_node", ev.NodeName, "burst_node", b.NodeName)
		return
	}

	// Central's record decides what this burst IS, never the connector. A managed
	// Job burst has a completion path of its own, and an idle node under one means
	// the Job has not started or has already been torn down — not that a customer
	// wants their run destroyed. It is acknowledged rather than dropped so the
	// connector stops re-sending a request central will never honour.
	if !b.NodeOnly {
		h.Log.Info("idle report is for a managed job burst; central owns its lifecycle, so this is a no-op",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID)
		h.ackNodeEvent(agent, ev)
		return
	}
	go h.reapIdleBurst(agent, b, ev)
}

// reapIdleBurst runs the teardown an authorised idle request owes, OFF the read
// goroutine, for the reason reapRemovedBurst documents: the reap pushes this
// connector a drain_node and waits for the CommandAck that readPump — the
// goroutine that would be sitting here — is the only reader of.
//
// It is deliberately the SAME machinery as a removal's, not a parallel one.
// ClaimBurst elects one winner across replicas, so an idle request racing the
// workload's own completion, a removal, the watchdog, or a second copy of itself
// converges on a single teardown and a single cost accrual. The only differences
// are the reason recorded and the status the workload lands in.
//
// What it does NOT share is the preflight. Every check handleIdleNode made was
// against central's own record, and none of them can see the pod that landed on
// the node after the connector's last idle sweep. The claim is irreversible, so
// the last thing before it is a question put to the cluster that owns the node —
// see confirmIdleTeardown.
//
// Nor does it share what the preflight LEAVES BEHIND. A confirmed node stays
// cordoned on purpose, so that the answer is still true when the claim runs
// behind it — and from that moment until the provider teardown, this call is the
// only thing that knows the node is unschedulable for a reason central chose. If
// the teardown does not happen, nothing else takes the cordon back: the burst is
// re-queued, and the watchdog now owns retrying that explicit pending reap even
// when the burst has no budget. The cordon must therefore stay: releasing it
// would let the scheduler place work on a node the watchdog is about to delete.
// Every non-reaped outcome keeps the cordon until cleanup is proven by the
// durable receipt and the connector retires the request.
func (h *AgentStream) reapIdleBurst(agent *state.Agent, b *state.Burst, ev protocol.NodeEvent) {
	burstID := b.ID
	reaped := false
	if h.nodeReapDone != nil {
		defer func() { h.nodeReapDone(burstID, reaped) }()
	}
	if h.lifecycle == nil {
		h.Log.Warn("burst node reported idle but no lifecycle seam is wired; the burst stands until the reaper watchdog takes it",
			"customer", agent.CustomerID, "burst", burstID)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), nodeReapTimeout)
	defer cancel()
	_, err := h.confirmIdleTeardown(ctx, agent, b)
	if err != nil {
		// Nothing claimed, nothing destroyed, nothing finished, and NOT
		// acknowledged: the request stands, and the connector re-sends it. If the
		// node really is spare the next preflight confirms it; if it took work
		// again, the watcher withdraws the request instead.
		//
		// No cordon to release either: a preflight that did not succeed took its own
		// back before answering.
		h.Log.Warn("idle teardown was not confirmed by the cluster that owns the node; tearing nothing down",
			"agent", agent.ID, "customer", agent.CustomerID,
			"burst", burstID, "node", b.NodeName, "error", err)
		return
	}
	outcome := h.lifecycle.ReapBurst(ctx, burstID, nodeIdleReason)
	if !outcome.Reaped() {
		// Acknowledgement is unchanged and is still the durable receipt's to give:
		// a teardown this call did not do is retired only by proof that something
		// did it. Row absence never answers that. See ackIfBurstReapRecorded.
		//
		// The CORDON is the second question, and the receipt alone cannot answer
		// it. A missing receipt is equally consistent with a node being destroyed
		// right now by another winner, a claim whose outcome is unknown, a
		// provider delete that already succeeded and only failed to write its
		// history, or an explicit pending reap the watchdog will retry. Uncordoning
		// any of those re-opens a node that is gone or scheduled for deletion.
		h.ackIfBurstReapRecorded(agent, burstID, ev)
		return
	}
	reaped = true

	// Cancelled, not failed: nothing went wrong. The capacity was spare and
	// central took it back, which is the same thing a tenant's own delete does.
	// FinishWorkloadForBurst carries the onlyIfUnfinished guard, so a workload
	// that already reported an outcome keeps it and the receipt that explains it.
	if _, err := h.Store.FinishWorkloadForBurst(ctx, burstID, "cancelled", time.Now().UTC()); err != nil {
		h.Log.Warn("workload not marked cancelled after its idle node was reaped; the burst is torn down either way",
			"customer", agent.CustomerID, "burst", burstID, "error", err)
	} else {
		h.Log.Info("idle burst reaped",
			"customer", agent.CustomerID, "burst", burstID)
	}
	// A queue handoff is not yet provider teardown, so the connector is released
	// only by the same durable receipt a removal waits for.
	h.ackIfBurstReapRecorded(agent, burstID, ev)
}

// confirmIdleTeardown asks the reporting connector to cordon the node and prove,
// cluster-wide, that it is STILL empty. It returns nil only on an explicit
// success acknowledgement.
//
// This is the barrier the idle path was missing. An idle report describes what
// the connector saw on its last sweep, minutes ago, and every check central makes
// against its own record is blind to a pod bound since. Only the customer's API
// server can answer, and only in that order: cordon first so nothing new can
// land, then list. On success the node stays cordoned, so the answer is still
// true when the claim runs behind it.
//
// It goes through the same authenticated command/CommandAck seam every other
// push does, on THIS socket — the connector that made the request is the one that
// must stand behind it. A refusal, a dropped connection, a timeout and a
// connector too old to know the command are the same answer, which is no.
//
// A success carries one thing back: the resourceVersion the connector's cordon
// produced. That is the token releaseIdleCordon needs to undo THIS cordon and no
// other, so it is kept for as long as the teardown behind it is unresolved. It is
// absent from an older connector's acknowledgement and empty when the node was
// already unschedulable, and both mean the same thing — there is no cordon of
// this request's to give back.
func (h *AgentStream) confirmIdleTeardown(ctx context.Context, agent *state.Agent, b *state.Burst) (protocol.IdleTeardownPreflight, error) {
	var result protocol.IdleTeardownPreflight
	body, err := json.Marshal(protocol.PrepareIdleTeardown{NodeName: b.NodeName})
	if err != nil {
		return result, err
	}
	commandID := newID("cmd")
	// Registered BEFORE the enqueue: a connector fast enough to answer between the
	// two would otherwise ack into a waiter that does not exist yet.
	ackCh, unregister := agent.RegisterCommandAck(commandID)
	defer unregister()
	if err := agent.Enqueue(protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       protocol.TypePrepareIdleTeardown,
		ID:         commandID,
		Timestamp:  time.Now().UTC(),
		Body:       body,
	}); err != nil {
		return result, err
	}
	timer := time.NewTimer(idlePreflightTimeout)
	defer timer.Stop()
	select {
	case ack := <-ackCh:
		if !ack.Success {
			if ack.Error == "" {
				return result, errors.New("connector refused the idle teardown")
			}
			return result, errors.New(ack.Error)
		}
		return preflightResult(ack), nil
	case <-ctx.Done():
		return result, ctx.Err()
	case <-timer.C:
		return result, errors.New("timed out waiting for the idle teardown preflight acknowledgement")
	}
}

// preflightResult reads the cordon token out of a successful preflight
// acknowledgement.
//
// Everything here is bounded and nothing is refused. The token is opaque to
// central and only ever goes back to the connector that minted it, so the point
// of the bounds is to keep an unbounded string out of a log line and a queued
// frame — not to validate a resourceVersion format central has no business
// knowing. Anything that fails them is dropped, which costs the recovery and
// leaves a cordon behind; it does not stop the teardown that was already
// confirmed.
func preflightResult(ack protocol.CommandAck) protocol.IdleTeardownPreflight {
	var result protocol.IdleTeardownPreflight
	if len(ack.Result) == 0 || len(ack.Result) > maxPreflightResultBytes {
		return protocol.IdleTeardownPreflight{}
	}
	if err := json.Unmarshal(ack.Result, &result); err != nil {
		return protocol.IdleTeardownPreflight{}
	}
	if len(result.CordonResourceVersion) > maxCordonResourceVersionBytes {
		return protocol.IdleTeardownPreflight{}
	}
	return result
}

// releaseIdleCordon gives back the cordon the preflight left on a node whose
// teardown then did not happen.
//
// The preflight leaves the node unschedulable deliberately: it is what keeps the
// node empty across the gap between the connector's answer and central's claim.
// On the winning path the provider destroys the node and the cordon goes with it.
// This runs on the one path that proved the opposite — the provider delete failed
// and the live burst is back on the queue — where the node is still there,
// running, billing, and refusing work for a teardown that never came.
//
// It names the exact resourceVersion the cordon produced, and the connector
// applies its undo only against that version and only while the node is still
// unschedulable. So an operator who cordoned the node since, or a drain in
// flight, is not reversed by this: the release simply fails, the node stays out
// of service on somebody else's decision, and that is said plainly in the log.
//
// Best-effort by construction. It is a command to a socket that may already be
// gone, on its own context because the reap's may have been cancelled — and it
// changes nothing durable, claims nothing, and acknowledges nothing. A cordon it
// could not release is a node that needs an operator, so it is logged as such.
//
// What it CANNOT recover is a cordon whose token died with the process: central
// holds the resourceVersion only in memory, so a crash between the preflight and
// the reap leaves a node the next request will find already unschedulable — and
// therefore not its to uncordon. That is the bound of this command protocol, and
// it is the safe side of it: an uncordon that guessed at ownership would put work
// onto nodes operators had deliberately emptied.
func (h *AgentStream) releaseIdleCordon(agent *state.Agent, b *state.Burst, p protocol.IdleTeardownPreflight) {
	if p.CordonResourceVersion == "" {
		// The preflight cordoned nothing — the node was already unschedulable when
		// it arrived — so there is nothing of this request's to give back.
		return
	}
	body, err := json.Marshal(protocol.ReleaseIdleTeardown{
		NodeName:              b.NodeName,
		CordonResourceVersion: p.CordonResourceVersion,
	})
	if err != nil {
		h.Log.Warn("idle teardown cordon could not be released; the node stays unschedulable until an operator uncordons it",
			"agent", agent.ID, "customer", agent.CustomerID,
			"burst", b.ID, "node", b.NodeName, "error", err)
		return
	}
	// Its own context, deliberately: the reap's may already be cancelled or
	// expired, and that is one of the very cases this recovery exists for.
	ctx, cancel := context.WithTimeout(context.Background(), idleReleaseTimeout)
	defer cancel()
	commandID := newID("cmd")
	ackCh, unregister := agent.RegisterCommandAck(commandID)
	defer unregister()
	if err := agent.Enqueue(protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       protocol.TypeReleaseIdleTeardown,
		ID:         commandID,
		Timestamp:  time.Now().UTC(),
		Body:       body,
	}); err != nil {
		h.Log.Warn("idle teardown cordon could not be released; the node stays unschedulable until an operator uncordons it",
			"agent", agent.ID, "customer", agent.CustomerID,
			"burst", b.ID, "node", b.NodeName, "error", err)
		return
	}
	select {
	case ack := <-ackCh:
		if ack.Success {
			h.Log.Info("idle teardown did not complete; the cordon it placed was released",
				"agent", agent.ID, "customer", agent.CustomerID,
				"burst", b.ID, "node", b.NodeName)
			return
		}
		reason := ack.Error
		if reason == "" {
			reason = "the connector refused the release"
		}
		h.Log.Warn("idle teardown cordon was not released; the node stays unschedulable until an operator or a later teardown takes it",
			"agent", agent.ID, "customer", agent.CustomerID,
			"burst", b.ID, "node", b.NodeName, "error", reason)
	case <-ctx.Done():
		h.Log.Warn("idle teardown cordon release was not acknowledged; the node may stay unschedulable until an operator uncordons it",
			"agent", agent.ID, "customer", agent.CustomerID,
			"burst", b.ID, "node", b.NodeName, "error", ctx.Err())
	}
}

// ackIfBurstReapRecorded acknowledges a terminal report this call did not tear
// down only when durable history proves provider teardown completed.
// Row absence alone is ambiguous after a failed ClaimBurst response and must
// leave the connector's retry outstanding.
//
// Shared by both teardown reports, because the question is the same one for
// both: this call did not do it, so did anything? The receipt is what answers
// that, and a removal and an idle request are equally entitled to it.
//
// It reports whether it acknowledged, which is the same fact under a second
// name: teardown is PROVEN. The idle path keeps its cordon either way; a proven
// teardown is a node on its way out, and a missing receipt may be an explicit
// retry or an ambiguous result that is equally unsafe to reopen.
func (h *AgentStream) ackIfBurstReapRecorded(agent *state.Agent, burstID string, ev protocol.NodeEvent) bool {
	ctx, cancel := context.WithTimeout(context.Background(), nodeEventTimeout)
	defer cancel()
	recorded, err := h.Store.BurstReapRecorded(ctx, burstID, agent.CustomerID)
	if err != nil {
		h.Log.Warn("burst not torn down here and its teardown receipt could not be read; leaving the report outstanding",
			"agent", agent.ID, "customer", agent.CustomerID,
			"burst", burstID, "phase", ev.Phase, "error", err)
		return false
	}
	if !recorded {
		// This covers a still-live row, a foreign/unknown id, and the dangerous
		// ambiguous-delete case. The same no-ack response preserves tenant
		// indistinguishability and never retires the only remaining retry.
		h.Log.Warn("burst node report has no teardown receipt; leaving it outstanding for a retry",
			"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID, "phase", ev.Phase)
		return false
	}
	h.Log.Info("burst node report has a durable teardown receipt; acknowledged",
		"agent", agent.ID, "customer", agent.CustomerID, "burst", burstID, "phase", ev.Phase)
	h.ackNodeEvent(agent, ev)
	return true
}

// ackNodeEvent releases the connector from a terminal report it no longer owes.
//
// It goes out through agent.Enqueue — the same queue the write pump is the sole
// reader of — because this runs on the reap goroutine and gorilla permits
// exactly one writer per connection.
//
// The id echoed back is the RAW one the connector sent, not the trimmed one
// central worked with: the connector matches an acknowledgement against its own
// cache key, and an id central quietly reshaped would clear nothing.
//
// A drop is survivable and deliberately quiet at Info: the connector's cache is
// what holds the report, and it re-sends on its own cadence and after every
// reconnect. Losing this frame costs one retry interval.
func (h *AgentStream) ackNodeEvent(agent *state.Agent, ev protocol.NodeEvent) {
	body, err := json.Marshal(protocol.NodeEventAck{BurstID: ev.BurstID, Phase: ev.Phase})
	if err != nil {
		h.Log.Warn("encode node event ack", "agent", agent.ID, "burst", ev.BurstID, "error", err)
		return
	}
	// No Envelope.ID: this is a receipt, and an id would ask the connector for a
	// CommandAck that nothing here is waiting on.
	if err := agent.Enqueue(protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       protocol.TypeNodeEventAck,
		Timestamp:  time.Now().UTC(),
		Body:       body,
	}); err != nil {
		h.Log.Info("node event ack not delivered; the connector will re-report",
			"agent", agent.ID, "burst", ev.BurstID, "error", err)
	}
}

// ackPodEvent releases the connector from a pod scheduling observation the
// durable store now holds under a known identity.
//
// The ACK echoes the EXACT persisted identity (PodName + UTC-normalised
// ScheduledAt), because that is the tuple the connector's cache is keyed on.
// A legacy path — one where the connector sent no ScheduledAt — is ACKed with
// PodName but a NIL ScheduledAt on purpose: a versioned connector's cached
// event always carries ScheduledAt, so the legacy ACK does not match and does
// not clear it, while a legacy cached event (also nil) does.
func (h *AgentStream) ackPodEvent(agent *state.Agent, workloadID string, persisted *state.PodObservation, legacy bool) {
	ack := protocol.PodEventAck{WorkloadID: workloadID}
	if persisted != nil {
		ack.PodName = persisted.PodName
		if !legacy && !persisted.ScheduledAt.IsZero() {
			scheduledAt := persisted.ScheduledAt.UTC()
			ack.ScheduledAt = &scheduledAt
		}
	}
	body, err := json.Marshal(ack)
	if err != nil {
		h.Log.Warn("encode pod event ack", "agent", agent.ID, "workload", workloadID, "error", err)
		return
	}
	if err := agent.Enqueue(protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       protocol.TypePodEventAck,
		Timestamp:  time.Now().UTC(),
		Body:       body,
	}); err != nil {
		h.Log.Info("pod event ack not delivered; the connector will re-report",
			"agent", agent.ID, "workload", workloadID, "error", err)
	}
}

func (h *AgentStream) ackSchedulingPodEvent(agent *state.Agent, ack protocol.PodEventAck) {
	body, err := json.Marshal(ack)
	if err != nil {
		h.Log.Warn("encode pod event ack", "agent", agent.ID, "workload", ack.WorkloadID, "error", err)
		return
	}
	if err := agent.Enqueue(protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       protocol.TypePodEventAck,
		Timestamp:  time.Now().UTC(),
		Body:       body,
	}); err != nil {
		h.Log.Info("pod event ack not delivered; the connector will re-report",
			"agent", agent.ID, "workload", ack.WorkloadID, "error", err)
	}
}

// isDNSSubdomain validates a Kubernetes DNS subdomain name per RFC 1123:
// lowercase alphanumerics and hyphens, split by dots, each label 1-63 chars,
// total <= 253 chars, no leading/trailing hyphen per label.
func isDNSSubdomain(s string) bool {
	if len(s) == 0 || len(s) > 253 {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		n := len(label)
		if n == 0 || n > 63 {
			return false
		}
		for i := 0; i < n; i++ {
			c := label[i]
			if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
				continue
			}
			if c == '-' && i > 0 && i < n-1 {
				continue
			}
			return false
		}
	}
	return true
}

// nodePhaseWriter is the store unless a test substituted one.
func (h *AgentStream) nodePhaseWriter() burstNodePhaseWriter {
	if h.nodePhases != nil {
		return h.nodePhases
	}
	return h.Store
}

// nodeRecordReader is the store unless a test substituted one.
func (h *AgentStream) nodeRecordReader() burstNodeRecordReader {
	if h.nodeRecords != nil {
		return h.nodeRecords
	}
	return h.Store
}

// nodeEventReason bounds the optional note a connector attaches to a phase.
//
// A reason that is oversized, invalid UTF-8, or carries control characters is
// DROPPED rather than refused, which is the opposite of what the /complete
// callback does with the same shape of field. The difference is what is at
// stake: there, a bad receipt means central would publish a claim it cannot
// justify, so the request is rejected. Here the reason is decoration on a phase
// that may be the Removed behind a billing node's teardown, and losing that to
// a stray byte in a kubelet message would be the worse failure.
func nodeEventReason(reason string) string {
	if reason == "" {
		return ""
	}
	if len(reason) > maxNodeEventReasonBytes || !utf8.ValidString(reason) {
		return ""
	}
	for _, r := range reason {
		if unicode.IsControl(r) {
			return ""
		}
	}
	return reason
}

// resolveNodePhaseTimestamp returns the phase ordering timestamp and whether
// the connector supplied a source timestamp. Source timestamps are preserved
// exactly — they are stable Kubernetes Node timestamps (condition
// LastTransitionTime, node CreationTimestamp) that do not vary across reconnect
// replays, so rewriting them to receipt time is what CREATES the ordering bug
// this function exists to fix. Absent timestamps fall back to receipt time,
// which is what the field would have been before source timestamps existed.
//
// The two classes are tracked through the update so a source-timestamped event
// always upgrades a legacy (receipt-time) record and a legacy event can never
// regress source-ordered state.
func resolveNodePhaseTimestamp(source *time.Time) (observedAt time.Time, sourceTimestamped bool) {
	if source == nil || source.IsZero() {
		return time.Now().UTC(), false
	}
	return source.UTC(), true
}
