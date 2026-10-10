package state

import (
	"context"
	"time"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

// ObservationResult distinguishes a fresh scheduling write from a durable
// replay and a rejected observation. Applied and Resolved may be acknowledged;
// Rejected must remain queued at the connector.
type ObservationResult uint8

const (
	ObservationApplied ObservationResult = iota
	ObservationRejected
	ObservationResolved
)

// Burst statuses. "provisioning" is what every burst is created with and the
// only value that existed before connectors reported node lifecycle; the rest
// are what a reported phase means to someone reading a dashboard.
//
// They are deliberately not the protocol's phase names. The phase is what the
// connector saw in Kubernetes; the status is what central is willing to claim
// about the burst, and the two drifting apart is normal — a burst whose node
// went NotReady is "degraded", not "the Node object says NotReady".
const (
	BurstStatusProvisioning = "provisioning"
	BurstStatusJoining      = "joining"
	BurstStatusRunning      = "running"
	BurstStatusDegraded     = "degraded"
	BurstStatusRemoved      = "removed"
)

// BurstStatusForNodePhase maps a reported phase onto the dashboard status.
// An unknown phase keeps "provisioning": callers validate the phase before they
// get here, so this arm is only reachable if that check is ever bypassed, and
// inventing a status from an unrecognised phase is the wrong way to find out.
func BurstStatusForNodePhase(phase string) string {
	switch phase {
	case protocol.NodePhaseJoining:
		return BurstStatusJoining
	case protocol.NodePhaseReady:
		return BurstStatusRunning
	case protocol.NodePhaseNotReady:
		return BurstStatusDegraded
	case protocol.NodePhaseRemoved:
		return BurstStatusRemoved
	}
	return BurstStatusProvisioning
}

// BurstNodePhaseUpdate is one connector-observed node lifecycle transition,
// already validated by the caller.
//
// CustomerID is the tenant the WEBSOCKET was authenticated as, never a field
// from the event body, and it is carried here rather than checked by the caller
// because the check has to happen inside the same write that applies the phase.
// A caller that read the burst, compared the tenant, then wrote would be
// deciding ownership against a record another replica may already have claimed.
type BurstNodePhaseUpdate struct {
	BurstID    string
	CustomerID string
	ClusterID  string
	Phase      string
	Reason     string
	ObservedAt time.Time

	// SourceTimestamped is true when the connector supplied an ObservedAt. Source
	// timestamps are stable Kubernetes Node timestamps that do not vary across
	// reconnect replays, so a source-timestamped event always upgrades a legacy
	// (receipt-time) record and a legacy event can never regress source-ordered
	// state. False means ObservedAt is central's receipt time, which is what
	// legacy connectors that omit the field produce.
	SourceTimestamped bool

	// ReceiptTime is central's own wall-clock time for this event, used for
	// OccupancyObservedAt. The client deliberately strips OccupancyObserved from
	// reconnect-cached events, so when it arrives it is always a fresh
	// observation and receipt time is the correct clock for it.
	ReceiptTime time.Time

	// OccupancyObserved additionally stamps OccupancyObservedAt. It is a claim
	// the connector has to make explicitly — that it read the whole cluster's
	// pods and so could have reported this node idle — and false is the default
	// for every other health report, including the repeats of a phase that has
	// not changed.
	OccupancyObserved bool

	// GPUAllocatable records that the connector observed positive nvidia.com/gpu
	// allocatable on this burst node. GPUAllocatableAt is the observation time.
	// Additive: older connectors omit both and the workload's GPUObservation
	// stays nil. CPU-only bursts never set it.
	GPUAllocatable   bool
	GPUAllocatableAt *time.Time
}

// burstNodePhaseResult is what the durable persister returns after applying a
// node phase update. A nil result means the update was refused entirely (no
// such row, wrong tenant, or terminal phase). PhaseApplied distinguishes a
// phase transition from an occupancy-only refresh so the Store caller does not
// have to infer it from the returned burst's fields.
type burstNodePhaseResult struct {
	Burst        *Burst
	PhaseApplied bool
}

// UpdateBurstNodePhase records one node lifecycle phase on an EXISTING burst
// and reports whether it applied. It is the only writer of the three NodePhase
// fields.
//
// applied=false is the ordinary refusal, not an error: the burst is gone
// (already reaped, or never existed), it belongs to another tenant, or it has
// already reached the terminal phase. Every one of those means "this event
// changes nothing", and the caller must treat them identically — a connector
// that could tell an unknown burst from another tenant's would have a burst-id
// oracle across the fleet.
//
// It is emphatically NOT a read-modify-write over the record GetBurst hands
// back. Two things break there. A pointer mutation would edit a record other
// goroutines are reading unlocked, and the write-back would be a whole-row
// upsert — which, run a moment after ClaimBurst deleted the row, RESURRECTS a
// burst that has already been torn down and hands the reaper watchdog a VM that
// no longer exists to bill for. So the durable path is a conditional UPDATE
// that can only ever touch a row that is still there, and the in-memory path
// replaces the map entry with a copy rather than editing the record in place.
//
// A durable backend decides on its own, exactly as ClaimBurst does: this
// replica's map may not hold a burst another central created, and cannot see a
// claim another central has just won. Memory follows that verdict and is never
// consulted for it.
// Phases that are not node HEALTH are refused outright rather than written: an
// idle teardown request rides the same envelope but describes what a connector
// wants done, not what the node is, and a burst that carried "Idle" as its
// status after a refused teardown would be a lie the customer reads on a
// dashboard. See protocol.PersistableNodePhase.
func (s *Store) UpdateBurstNodePhase(ctx context.Context, u BurstNodePhaseUpdate) (bool, error) {
	if u.BurstID == "" || u.CustomerID == "" || !protocol.PersistableNodePhase(u.Phase) {
		return false, nil
	}
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return s.updateBurstNodePhaseInMemory(u), nil
	}

	// Not under s.mu: the atomicity lives in the UPDATE's row lock, so holding
	// it would block every other store reader on a database round trip for
	// nothing — the same trade ClaimBurst makes.
	res, err := p.updateBurstNodePhase(ctx, u)
	if err != nil {
		s.recordPersistenceFailure("burst_node_phase", "update", err)
		return false, err
	}
	if res == nil {
		return false, err
	}
	// Refresh the local copy from the row the database actually holds, and only
	// if this replica still has one. Inserting here would re-create a burst a
	// concurrent claim has removed from this map — the resurrection the durable
	// UPDATE was written to avoid, reached from the other side.
	s.mu.Lock()
	if _, live := s.bursts[u.BurstID]; live {
		s.bursts[u.BurstID] = res.Burst
	}
	if res.PhaseApplied {
		s.stampWorkloadNodeObservation(u, res.Burst.NodeName)
	}
	if u.GPUAllocatable && u.GPUAllocatableAt != nil && u.ClusterID != "" && res.Burst.ClusterID == u.ClusterID {
		s.stampWorkloadGPUObservation(u)
	}
	s.mu.Unlock()
	return true, nil
}

// BurstOwnedBy reports whether a burst exists and belongs to customerID,
// WITHOUT mutating it.
//
// It answers tenancy alone. That is deliberately NOT enough to authorise a
// teardown and no teardown path uses it: ClaimBurst deletes by id, and a tenant
// may connect several clusters on one token, so a connector compromised in one
// of them would pass this check while naming capacity running in another. Both
// teardown reports read the record instead — see BurstForNodeTeardown.
//
// ok=false covers "no such burst" and "another tenant's burst" alike, and
// callers must keep it that way on the wire: the difference is a burst-id
// oracle across the fleet.
//
// An error is NOT ok=false. The burst may well exist, so the caller must treat
// it as unknown and leave the report retryable — the same call the durable
// backend being down forces on every other lifecycle path.
//
// Durable state decides when there is any: this replica's map holds only the
// bursts it created, so a memory answer would refuse teardown for a burst
// another central admitted. Memory answers alone when there is no backend.
func (s *Store) BurstOwnedBy(ctx context.Context, burstID, customerID string) (bool, error) {
	if burstID == "" || customerID == "" {
		return false, nil
	}
	s.mu.RLock()
	p := s.persist
	if p == nil {
		// Read the record under the same lock: with no backend this map IS the
		// durable set, and the answer must not be assembled from a pointer taken
		// before the lock went away.
		b, ok := s.bursts[burstID]
		owned := ok && b.CustomerID == customerID
		s.mu.RUnlock()
		return owned, nil
	}
	s.mu.RUnlock()
	return p.burstOwnedBy(ctx, burstID, customerID)
}

// BurstForNodeTeardown reads the burst a connector's teardown REQUEST names,
// scoped to that connector's tenant, without touching it.
//
// It is what BOTH teardown reports are authorised against, because neither is
// self-authorising on tenancy alone. Answering them needs the record itself:
// which cluster central booked the capacity in, what it named the node, and —
// for an idle request — whether this is nodeOnly capacity at all. None of those
// may be taken from the event: they are what the event is being checked against.
//
// found=false covers "no such burst" and "another tenant's burst" alike, and an
// error is neither — the burst may well exist, so the caller must treat it as
// unknown and leave the report retryable. Same rules as BurstOwnedBy, for the
// same reasons.
//
// Durable state decides when there is any: this replica's map holds only the
// bursts it created, and a memory answer would refuse teardown for a burst
// another central admitted.
func (s *Store) BurstForNodeTeardown(ctx context.Context, burstID, customerID string) (*Burst, bool, error) {
	if burstID == "" || customerID == "" {
		return nil, false, nil
	}
	s.mu.RLock()
	p := s.persist
	if p == nil {
		// Copy under the same lock: with no backend this map IS the durable set,
		// and callers of GetBurst read the live pointer unlocked, so handing one
		// out here would race a phase update replacing the entry.
		b, ok := s.bursts[burstID]
		if !ok || b.CustomerID != customerID {
			s.mu.RUnlock()
			return nil, false, nil
		}
		cp := *b
		s.mu.RUnlock()
		return &cp, true, nil
	}
	s.mu.RUnlock()
	return p.burstForTenant(ctx, burstID, customerID)
}

// RecordBurstReap records durable proof that this tenant's provider teardown
// was secured. The receipt is separate from workload history because node-only
// capacity has no workload row to carry a cost observation.
func (s *Store) RecordBurstReap(ctx context.Context, burstID, customerID string) (bool, error) {
	if burstID == "" || customerID == "" {
		return false, nil
	}
	s.mu.Lock()
	p := s.persist
	if p == nil {
		if owner, exists := s.burstReapReceipts[burstID]; exists {
			s.mu.Unlock()
			return owner == customerID, nil
		}
		s.burstReapReceipts[burstID] = customerID
		s.mu.Unlock()
		return true, nil
	}
	s.mu.Unlock()
	recorded, err := p.recordBurstReapReceipt(ctx, burstID, customerID)
	if err != nil {
		s.recordPersistenceFailure("burst_reap_receipt", "record", err)
	}
	return recorded, err
}

// BurstReapRecorded reports whether durable teardown history proves that the
// burst's teardown was secured. An absent burst row is not enough: a database
// client can lose the result of DELETE ... RETURNING after the delete commits,
// leaving ClaimBurst's outcome unknown and no caller holding the provider data
// needed to enqueue teardown. The dedicated receipt is written only after the
// provider is gone, so it is what an agent-side Removed event may safely
// acknowledge after losing the claim race.
//
// false deliberately combines missing history, another tenant's history, and
// history without a cost receipt. That keeps the query tenant-scoped and makes
// uncertainty retain the connector's retry instead of falsely closing it.
func (s *Store) BurstReapRecorded(ctx context.Context, burstID, customerID string) (bool, error) {
	if burstID == "" || customerID == "" {
		return false, nil
	}
	s.mu.RLock()
	p := s.persist
	if p == nil {
		recorded := s.burstReapReceipts[burstID] == customerID
		s.mu.RUnlock()
		return recorded, nil
	}
	s.mu.RUnlock()
	return p.burstReapRecorded(ctx, burstID, customerID)
}

// updateBurstNodePhaseInMemory is the whole update when there is no durable
// backend: one process, so the mutex IS the atomicity.
//
// The record is copied and the map entry replaced rather than mutated in place.
// GetBurst, ListBursts and BurstsForCustomer all hand out the live pointer and
// their callers read it without the lock, so editing the record behind them is
// a data race — and one that would show a dashboard a half-applied phase.
func (s *Store) updateBurstNodePhaseInMemory(u BurstNodePhaseUpdate) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bursts[u.BurstID]
	if !ok || b.CustomerID != u.CustomerID || (u.ClusterID != "" && b.ClusterID != "" && b.ClusterID != u.ClusterID) || b.NodePhase == protocol.NodePhaseRemoved {
		return false
	}
	phaseFresh := nodePhaseIsFresh(b.NodePhaseAt, b.NodePhaseSourceTimestamped, u.ObservedAt, u.SourceTimestamped)
	occupancyTime := u.ReceiptTime.UTC()
	if occupancyTime.IsZero() {
		occupancyTime = u.ObservedAt.UTC()
	}
	occupancyFresh := u.OccupancyObserved &&
		(b.OccupancyObservedAt == nil || occupancyTime.After(*b.OccupancyObservedAt))
	gpuApplicable := u.GPUAllocatable && u.GPUAllocatableAt != nil && u.ClusterID != "" && b.ClusterID == u.ClusterID
	if !phaseFresh && !occupancyFresh && !gpuApplicable {
		return false
	}
	if phaseFresh || occupancyFresh {
		observed := u.ObservedAt.UTC()
		cp := *b
		if phaseFresh {
			cp.NodePhase = u.Phase
			cp.NodePhaseReason = u.Reason
			cp.NodePhaseAt = &observed
			cp.NodePhaseSourceTimestamped = u.SourceTimestamped
			cp.Status = BurstStatusForNodePhase(u.Phase)
		}
		if occupancyFresh {
			cp.OccupancyObservedAt = &occupancyTime
		}
		s.bursts[u.BurstID] = &cp
		if phaseFresh {
			s.stampWorkloadNodeObservation(u, cp.NodeName)
		}
	}
	if gpuApplicable {
		s.stampWorkloadGPUObservation(u)
	}
	return true
}

// nodePhaseIsFresh decides whether an incoming phase timestamp is newer than
// the stored one, accounting for the source-vs-legacy class of each.
//
// A source-timestamped event always upgrades a legacy (receipt-time) record:
// the first connector to supply stable Kubernetes timestamps is strictly more
// trustworthy than receipt time, regardless of the values. A legacy event can
// never regress source-ordered state: a connector that omits the field is
// older, and its receipt time says nothing about when the transition actually
// happened. Within the same class, strictly-newer wins.
func nodePhaseIsFresh(storedAt *time.Time, storedSource bool, incomingAt time.Time, incomingSource bool) bool {
	if storedAt == nil {
		return true
	}
	if incomingSource && !storedSource {
		return true
	}
	if !incomingSource && storedSource {
		return false
	}
	return incomingAt.After(*storedAt)
}

// stampWorkloadNodeObservation copies the connector-observed node snapshot onto
// the permanent workload record that references this burst. Called under s.mu
// by both the in-memory and durable paths so the burst phase and the workload
// observation move together.
//
// Copy-on-write: the workload record is replaced, never mutated, for the same
// reason the burst is — GetWorkload hands out live pointers that handlers read
// without the lock.
//
// A missing workload is normal: nodeOnly bursts have no workload row.
func (s *Store) stampWorkloadNodeObservation(u BurstNodePhaseUpdate, nodeName string) {
	for _, w := range s.workloads {
		if w.BurstID == u.BurstID && w.CustomerID == u.CustomerID && (u.ClusterID == "" || w.ClusterID == "" || w.ClusterID == u.ClusterID) {
			if w.NodeObservation != nil &&
				!nodePhaseIsFresh(&w.NodeObservation.ObservedAt, w.NodeObservation.SourceTimestamped, u.ObservedAt, u.SourceTimestamped) {
				return
			}
			wcp := *w
			wcp.NodeObservation = &NodeObservation{
				NodeName:          nodeName,
				Phase:             u.Phase,
				Reason:            u.Reason,
				ObservedAt:        u.ObservedAt.UTC(),
				SourceTimestamped: u.SourceTimestamped,
			}
			s.workloads[wcp.ID] = &wcp
			return
		}
	}
}

// stampWorkloadGPUObservation writes the GPU readiness observation onto the
// workload that references this burst. Once set, never overwritten — GPU
// readiness is a one-shot event. Called under s.mu.
func (s *Store) stampWorkloadGPUObservation(u BurstNodePhaseUpdate) {
	if u.ClusterID == "" {
		return
	}
	for _, w := range s.workloads {
		if w.BurstID == u.BurstID && w.CustomerID == u.CustomerID && w.ClusterID == u.ClusterID {
			if w.GPUObservation != nil {
				return
			}
			wcp := *w
			wcp.GPUObservation = &GPUObservation{
				AllocatableAt: u.GPUAllocatableAt.UTC(),
			}
			s.workloads[wcp.ID] = &wcp
			return
		}
	}
}

// PodObservationOutcome names the three answers StampWorkloadPodObservation
// gives its caller, so a stale-in-memory hit at one replica cannot look the
// same as a fresh apply that survived retirement at another.
//
// Rejected — nothing was durably recorded that names this identity for this
// tenant and cluster: an unknown workload, wrong tenant/cluster/burst, or a
// once-applied observation that the durable store no longer holds (a stale
// in-memory phantom). The connector MUST NOT be released from its retry.
//
// Applied — this call is the one that wrote it. Central hands the connector an
// ACK carrying the values it just persisted.
//
// Resolved — the observation was already durably present with the SAME identity
// this call names, and the persister confirmed it. This is the safe answer to
// a cross-replica retry: whichever replica the retry lands on, only a durable
// live observation with matching identity ACKs, and stale memory alone cannot.
type PodObservationOutcome uint8

const (
	PodObservationRejected PodObservationOutcome = iota
	PodObservationApplied
	PodObservationResolved
)

// Acknowledgeable reports whether the outcome authorises central to ACK the
// connector's report. True only for outcomes proven by a durable read or a
// durable write — never for a stale in-memory hit.
func (o PodObservationOutcome) Acknowledgeable() bool {
	return o == PodObservationApplied || o == PodObservationResolved
}

// StampWorkloadPodObservation is the bool-shape wrapper existing callers and
// tests still use: it maps the outcome enum onto a single Acknowledgeable bit
// and forwards the error. Prefer StampWorkloadPodObservationOutcome for new
// callers that need the exact durable identity for the ACK.
func (s *Store) StampWorkloadPodObservation(ctx context.Context, workloadID, customerID, clusterID, podName, nodeName string, scheduledAt time.Time) (bool, error) {
	outcome, _, err := s.StampWorkloadPodObservationOutcome(ctx, workloadID, customerID, clusterID, podName, nodeName, scheduledAt)
	if err != nil {
		return false, err
	}
	return outcome.Acknowledgeable(), nil
}

// StampWorkloadPodObservationOutcome records the connector-observed pod
// scheduling onto the workload. Once set, never overwritten. The caller must
// have validated tenancy — customerID is bound from the authenticated socket,
// not from the event body. Requires exact non-empty cluster identity.
//
// Fail-closed: the durable predicate succeeds BEFORE memory is mutated, so
// an injected persistence failure leaves memory absent and a retry can succeed.
//
// The returned outcome is what the caller ACKs on. On Applied/Resolved the
// echoed *PodObservation is the exact identity now durably held, which the ACK
// must copy verbatim so the connector's cache-key match is against the same
// values central has.
func (s *Store) StampWorkloadPodObservationOutcome(ctx context.Context, workloadID, customerID, clusterID, podName, nodeName string, scheduledAt time.Time) (PodObservationOutcome, *PodObservation, error) {
	scheduledAt = scheduledAt.UTC()
	if clusterID == "" {
		return PodObservationRejected, nil, nil
	}
	s.mu.Lock()
	w, ok := s.workloads[workloadID]
	if !ok || w.CustomerID != customerID || w.ClusterID == "" || w.ClusterID != clusterID {
		s.mu.Unlock()
		return PodObservationRejected, nil, nil
	}
	b, bOk := s.bursts[w.BurstID]
	burstOK := bOk && b.CustomerID == customerID && b.ClusterID != "" && b.ClusterID == clusterID && b.NodeName == nodeName
	p := s.persist
	s.mu.Unlock()

	if p != nil {
		n, err := p.stampWorkloadPodObservation(ctx, workloadID, customerID, clusterID, podName, nodeName, scheduledAt)
		if err != nil {
			s.recordPersistenceFailure("workload_pod_observation", "stamp", err)
			return PodObservationRejected, nil, err
		}
		if n == 0 {
			// The write refused: either write-once (already applied), a wrong
			// burst NodeName, the burst row was retired, or the workload row is
			// gone entirely. The durable read is what tells them apart, and it
			// is the only thing that authorises the ACK on a retry that hits a
			// replica whose memory was never the winner. The re-read joins the
			// workload to the currently live burst row (same customer, cluster,
			// NodeName) so a retired burst leaves no live identity to ACK
			// against — a stale in-memory PodObservation on the workload row
			// alone must NOT be enough.
			durable, exists, err := p.getWorkloadPodObservation(ctx, workloadID, customerID, clusterID, nodeName)
			if err != nil {
				s.recordPersistenceFailure("workload_pod_observation", "read_after_stamp", err)
				return PodObservationRejected, nil, err
			}
			if !exists || durable == nil {
				return PodObservationRejected, nil, nil
			}
			s.mu.Lock()
			if w, ok := s.workloads[workloadID]; ok &&
				w.CustomerID == customerID && w.ClusterID == clusterID &&
				w.PodObservation == nil {
				wcp := *w
				dcp := *durable
				wcp.PodObservation = &dcp
				s.workloads[wcp.ID] = &wcp
			}
			s.mu.Unlock()
			return PodObservationResolved, durable, nil
		}
	} else {
		// In-memory only: memory IS the durable state, and an existing
		// observation is the resolved answer to any retry with matching
		// identity. Write-once means we return the FIRST identity so the ACK
		// carries what is actually held — a retry with different identity
		// gets an ACK for the older one, and its cache-key match fails.
		s.mu.Lock()
		w, ok := s.workloads[workloadID]
		if !ok || w.CustomerID != customerID || w.ClusterID != clusterID {
			s.mu.Unlock()
			return PodObservationRejected, nil, nil
		}
		if w.PodObservation != nil {
			existing := *w.PodObservation
			s.mu.Unlock()
			return PodObservationResolved, &existing, nil
		}
		if !burstOK {
			s.mu.Unlock()
			return PodObservationRejected, nil, nil
		}
		wcp := *w
		wcp.PodObservation = &PodObservation{
			PodName:     podName,
			NodeName:    nodeName,
			ScheduledAt: scheduledAt,
		}
		s.workloads[wcp.ID] = &wcp
		obs := *wcp.PodObservation
		s.mu.Unlock()
		return PodObservationApplied, &obs, nil
	}

	// Reached only when p != nil AND the persister write returned n > 0. The
	// SQL guard is PodObservation IS NULL, so exactly one writer wins and its
	// identity is authoritative. Echo THAT identity, not whatever memory holds.
	appliedObs := &PodObservation{PodName: podName, NodeName: nodeName, ScheduledAt: scheduledAt}
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok = s.workloads[workloadID]
	if !ok || w.CustomerID != customerID || w.ClusterID != clusterID {
		// Row retired between the persister write and the memory refresh —
		// durable still has our write, so the ACK reflects that.
		return PodObservationApplied, appliedObs, nil
	}
	if w.PodObservation == nil {
		wcp := *w
		obs := *appliedObs
		wcp.PodObservation = &obs
		s.workloads[wcp.ID] = &wcp
	}
	return PodObservationApplied, appliedObs, nil
}

// schedulingObservationFresh enforces a monotonic scheduling lifecycle.
// Scheduled is terminal; otherwise a newer source timestamp or a replacement
// pod at the same timestamp may advance the observation.
func schedulingObservationFresh(stored, incoming *SchedulingObservation) bool {
	if stored == nil {
		return true
	}
	if incoming == nil || stored.State == protocol.SchedulingStateScheduled {
		return false
	}
	if incoming.State == protocol.SchedulingStateScheduled || incoming.ObservedAt.After(stored.ObservedAt) {
		return true
	}
	return incoming.PodName != "" && stored.PodName != "" &&
		incoming.PodName != stored.PodName && incoming.ObservedAt.Equal(stored.ObservedAt)
}

// StampWorkloadSchedulingObservation records the connector-observed scheduling
// state after enforcing tenant, cluster, live-burst, and positive-proof bounds.
// The durable predicate succeeds before memory changes.
func (s *Store) StampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, schedulingState, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	if clusterID == "" || !protocol.ValidSchedulingState(schedulingState) {
		return ObservationRejected, nil
	}
	if schedulingState == protocol.SchedulingStateWaiting && !protocol.ValidSchedulingReason(reason) {
		return ObservationRejected, nil
	}

	incoming := &SchedulingObservation{
		State:      schedulingState,
		Reason:     reason,
		Message:    message,
		ObservedAt: observedAt.UTC(),
		PodName:    podName,
	}

	s.mu.Lock()
	w, ok := s.workloads[workloadID]
	if !ok || w.CustomerID != customerID || w.ClusterID == "" || w.ClusterID != clusterID || w.BurstID == "" {
		s.mu.Unlock()
		return ObservationRejected, nil
	}
	b, burstOK := s.bursts[w.BurstID]
	if !burstOK || b.CustomerID != customerID || b.ClusterID == "" || b.ClusterID != clusterID ||
		(schedulingState == protocol.SchedulingStateWaiting && w.PodObservation != nil) ||
		(schedulingState == protocol.SchedulingStateScheduled && w.PodObservation == nil) {
		s.mu.Unlock()
		return ObservationRejected, nil
	}
	fresh := schedulingObservationFresh(w.SchedulingObservation, incoming)
	p := s.persist
	s.mu.Unlock()

	if p != nil {
		result, err := p.stampWorkloadSchedulingObservation(ctx, workloadID, customerID, clusterID, schedulingState, reason, message, podName, observedAt.UTC())
		if err != nil {
			s.recordPersistenceFailure("workload_scheduling_observation", "stamp", err)
			return ObservationRejected, err
		}
		if result != ObservationApplied {
			return result, nil
		}
	} else if !fresh {
		return ObservationResolved, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok = s.workloads[workloadID]
	if !ok || !schedulingObservationFresh(w.SchedulingObservation, incoming) {
		return ObservationResolved, nil
	}
	wcp := *w
	wcp.SchedulingObservation = incoming
	s.workloads[wcp.ID] = &wcp
	return ObservationApplied, nil
}
