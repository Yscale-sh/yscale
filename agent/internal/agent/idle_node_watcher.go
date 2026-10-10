package agent

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	v1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// IdleNodeWatcher asks central to tear down burst nodes that have stopped doing
// any work.
//
// # Problem
//
// A nodeOnly burst is provisioned for pods central never created: KEDA scales a
// Deployment up, PendingPodWatcher submits capacity, the scheduler binds the
// pods. Nothing then reports a completion, because there is no Job to complete —
// so when KEDA scales back to zero the pods go away and the node keeps billing
// until its declared deadline expires. Everything else in the lifecycle is
// driven by a signal only the customer's cluster can see, and this is the one
// that was missing.
//
// # What counts as busy
//
// An assigned Pending or Running pod that is not being deleted. Everything else
// on the node is either not work (DaemonSets, the mirror pods a static manifest
// creates) or work that is over (Succeeded, Failed, a pod with a deletion
// timestamp already draining). A node carrying only those is one no customer
// workload is on.
//
// # What counts as idle
//
// Continuous, observed idleness for the whole grace period. The timer starts at
// the first pass that SAW the node idle and is reset by anything else — the node
// going busy, the node disappearing, and every answer this connector could not
// obtain. A pod list that failed is unknown, and unknown is never idle: the cost
// of not reaping is a node that bills for another grace period, and central's
// own nodeOnly max lifetime is the backstop for a connector that can never
// answer. The cost of the other mistake is destroying a node serving traffic.
//
// A restart therefore begins a fresh grace window rather than reaping on its
// first pass: the map starts empty, so the first observation only records a
// start time. That is deliberate — this process has no evidence about what the
// node was doing while it was down.
//
// # Withdrawal
//
// A request that has been made can be TAKEN BACK, and must be. The connector
// holds an unacknowledged idle request and re-sends it until central answers, so
// one made during a central outage is still pending when central returns — by
// which time the autoscaler may have put pods back on that node. Whenever a node
// with an outstanding request stops being idle, this watcher reports its current
// lifecycle phase through WithdrawIdleNodeEvent, which is the ONLY call that
// clears a held idle request from the connector's cache. Withdrawing takes
// having looked at the node's pods, and this watcher is the only thing that
// does; an ordinary health report must never cancel a teardown by arriving. The
// request is re-asserted on every pass the node is still idle, so a withdrawal
// that turns out to be premature costs one sweep.
//
// It is not the safety barrier. Central runs its own preflight against the
// customer's API server before it claims anything — see
// protocol.DrainModePrepareIdleTeardown. This cache rule only decides which
// unsent, still-retrying request the connector holds.
//
// # Occupancy observations
//
// This watcher is also the ONLY thing that may tell central the idle signal is
// alive, and it may only say so on a sweep whose cluster-wide pod LIST
// succeeded. Central expires a nodeOnly burst that has gone that long without an
// observation, so the claim has to be exactly "someone can still see whether
// this node is idle" — not "the node is healthy", which a connector with no pod
// visibility at all goes on reporting forever. See OccupancyInterval.
//
// Central's ceiling starts at the FIRST observation, never at the burst's
// creation. A deployment that cannot make the claim is not a connector gone
// quiet, and treating it as one destroyed active nodes at 6h for a capability
// the default namespaced install was never granted.
//
// # Scope
//
// Burst nodes only, matched exactly as NodeWatcher matches them
// (burstIDForNodeName), and only while they are Ready. A node that is not Ready
// is not idle capacity, it is a health problem, and NodeWatcher's phases and
// NodeGC already own it. Occupancy observations are the exception: they are
// about this connector's visibility rather than the node's, so every burst node
// in a readable sweep gets one.
//
// The watcher does not run at all without cluster-wide pod visibility, and that
// visibility must be granted explicitly rather than inferred from the absence of
// a namespace scope; see Run.
type IdleNodeWatcher struct {
	K8s     kubernetes.Interface
	Central IdleNodeEventReporter
	Log     *slog.Logger
	// ClusterID scopes both node discovery and the final defensive check to the
	// same connector identity NodeWatcher uses.
	ClusterID string

	// WorkloadNamespace mirrors the connector's -workload-namespace. It is NOT
	// the namespace this watcher lists pods in — it lists every namespace — it is
	// one of the two signals that it may not run at all. See Run.
	WorkloadNamespace string

	// AuthoritativePodVisibility is the operator's assertion that this connector
	// is actually granted the cluster-wide pod LIST every claim here rests on.
	// Default FALSE, and that default is the whole point: an empty
	// WorkloadNamespace says nobody scoped this connector to one namespace, which
	// is not the same as saying its RBAC can read the cluster's pods. The shipped
	// chart is namespaced, so a connector that assumed the second from the first
	// spent every sweep failing a list it was never granted — see Run.
	AuthoritativePodVisibility bool

	// IdleGrace is how long a burst node must be continuously observed idle
	// before its teardown is requested. Default: 10 m.
	IdleGrace time.Duration

	// SweepInterval is how often nodes and their pods are listed. Default: 1 m.
	SweepInterval time.Duration

	// OccupancyInterval bounds how often one burst re-states that this connector
	// could still see whether its node is idle. Default: 5 m. It is the ONLY
	// thing central's nodeOnly silence ceiling measures from, so it must stay
	// well under YSCALE_NODE_ONLY_MAX_LIFETIME.
	//
	// Low-rate on purpose: the sweep runs every minute and the claim it carries
	// changes on the timescale of a connector dying, not of a pod moving. The
	// first successful sweep reports immediately, so a fresh or restarted
	// connector does not spend an interval looking dead.
	//
	// Non-positive disables the observations entirely. Central's silence ceiling
	// then never applies to those bursts at all — it only measures from a claim
	// that was made and stopped — so they are bounded by their declared budget
	// alone.
	OccupancyInterval time.Duration

	// JoiningGrace is NodeWatcher's, and this watcher needs it for one reason: a
	// withdrawal has to name the node's CURRENT phase, and that phase is a
	// function of this grace. Default: 5 m, the same default.
	JoiningGrace time.Duration

	// mu guards tracked and occupancy.
	mu      sync.Mutex
	tracked map[string]idleNodeState
	// occupancy is when each burst last had an observation sent, and is kept
	// apart from tracked deliberately: tracked entries are deleted the moment a
	// node goes busy, and a busy node is exactly the one that must keep saying
	// this connector can still see it.
	occupancy map[string]time.Time
}

// idleNodeState is what this watcher remembers about one burst between passes.
//
// The two fields have different lifetimes, and that is the whole point.
//
// since is EVIDENCE and is thrown away the moment it stops holding — the node
// took work, went NotReady, or the connector could not read its pods. A window
// that survived an unobserved pass would be counting time nobody watched.
//
// reported is a DEBT and outlives the evidence. Central is holding an idle
// request that this connector made and has not seen answered, and the request
// is only valid while the node is still idle. So the flag stays set until either
// the request is withdrawn or the node is gone — including across passes where
// this connector could see nothing at all, because a debt it forgot is a
// teardown it can no longer call off.
type idleNodeState struct {
	since    time.Time
	reported bool
}

const (
	defaultNodeIdleGrace     = 10 * time.Minute
	defaultIdleSweepInterval = time.Minute
	defaultOccupancyInterval = 5 * time.Minute

	nodeIdleReason = "burst node carried no schedulable pods for the idle grace period"

	// podConfigMirrorAnnotation marks a pod the kubelet created from a static
	// manifest rather than one the scheduler assigned. It has no controller and
	// no API-server owner, so an owner-reference check does not see it.
	podConfigMirrorAnnotation = "kubernetes.io/config.mirror"
)

// NewIdleNodeWatcher constructs a watcher with production defaults.
func NewIdleNodeWatcher(k8s kubernetes.Interface, central IdleNodeEventReporter, clusterID string, log *slog.Logger) *IdleNodeWatcher {
	if log == nil {
		log = slog.Default()
	}
	return &IdleNodeWatcher{
		K8s:               k8s,
		Central:           central,
		Log:               log,
		ClusterID:         clusterID,
		IdleGrace:         defaultNodeIdleGrace,
		SweepInterval:     defaultIdleSweepInterval,
		JoiningGrace:      defaultNodeJoiningGrace,
		OccupancyInterval: defaultOccupancyInterval,
		tracked:           make(map[string]idleNodeState),
		occupancy:         make(map[string]time.Time),
	}
}

// Run sweeps burst nodes on every SweepInterval tick until ctx is cancelled.
//
// It REFUSES TO START unless BOTH hold, and that refusal is the whole safety
// argument for the feature. "Idle" here is a claim about every pod in the
// cluster, and there is no partial version of it: either the pod list is
// authoritative or the signal is wrong.
//
//   - No workload namespace scope. A namespace-scoped connector can only see one
//     namespace's pods, so a burst node busy serving another team's Deployment
//     would look empty and this watcher would ask central to destroy it.
//   - AuthoritativePodVisibility. Scope is what an operator CONFIGURED; this is
//     whether the connector was actually granted the cluster-wide pod LIST. The
//     shipped chart's RBAC is namespaced with no workload namespace set, so
//     inferring the grant from the empty scope put this watcher into a loop of
//     forbidden lists — every sweep blind, no occupancy observation sent, and a
//     capability central had no way to know was never there.
//
// Returning nil rather than an error is deliberate for both: a namespaced or
// shared connector is a supported topology, not a fault.
func (w *IdleNodeWatcher) Run(ctx context.Context) error {
	if w.WorkloadNamespace != "" {
		w.Log.Warn("idle burst teardown disabled: this connector is scoped to one namespace, so it cannot tell an idle burst node from one busy with another namespace's pods",
			"workload_namespace", w.WorkloadNamespace)
		return nil
	}
	if !w.AuthoritativePodVisibility {
		w.Log.Warn("idle burst teardown disabled: this connector has not been granted authoritative cluster-wide pod visibility, so it cannot tell an idle burst node from one it simply cannot see. Install with rbac.scope=cluster and no workloadNamespace, or give nodeOnly workloads an explicit budget")
		return nil
	}
	if w.ClusterID == "" {
		return errors.New("watch idle burst nodes: cluster ID is required")
	}
	w.Log.Info("idle burst node watcher starting",
		"idle_grace", w.idleGrace(), "sweep_interval", w.sweepInterval(),
		"occupancy_interval", w.OccupancyInterval)
	ticker := time.NewTicker(w.sweepInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			w.sweep(ctx, time.Now())
		}
	}
}

// sweep runs one pass: report every burst node that has now been idle for the
// whole grace period, and reset the timer for every one that has not.
//
// A node still idle past the grace is re-reported on EVERY pass, not once. The
// client coalesces them into the one cached entry it already holds, so this
// costs a frame per idle burst per sweep — and it is what makes the report
// withdrawable: stopBeingIdle may revoke a held request outright, and only a
// watcher that keeps re-asserting the request can be allowed to do that.
//
// Two lists per pass, not one per node: see occupiedNodes.
func (w *IdleNodeWatcher) sweep(ctx context.Context, now time.Time) {
	if w.ClusterID == "" {
		w.Log.Error("idle sweep refused: cluster ID is required")
		w.restartAllWindows()
		return
	}
	selector := klabels.Set{v1.LabelClusterID: w.ClusterID}.AsSelector().String()
	nodes, err := w.K8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		// Nothing was observed, so no grace window survives this pass. Restarting
		// every timer costs one more grace period on a flaky API server; carrying
		// them across an unobserved window would let idle time accrue for a node
		// this connector cannot currently see at all. Outstanding requests are
		// KEPT — this pass learned nothing that withdraws one, and forgetting the
		// debt would leave a teardown this connector can no longer call off.
		w.Log.Warn("idle sweep: list nodes failed; restarting every idle grace window", "error", err)
		w.restartAllWindows()
		return
	}
	// ONE pod list answers for every burst node in the pass, so one failure blinds
	// the pass entirely: podsKnown=false makes every node below take the unknown
	// branch, which ends its window and withdraws any request it had outstanding.
	occupied, podsKnown := w.occupiedNodes(ctx)

	seen := make(map[string]bool, len(nodes.Items))
	for i := range nodes.Items {
		node := &nodes.Items[i]
		burstID, ok := burstIDForNodeName(node.Name)
		if !ok {
			continue
		}
		if node.Labels[v1.LabelClusterID] != w.ClusterID {
			continue
		}
		seen[burstID] = true

		// Before anything is decided about this node: the pass got an
		// authoritative pod list, so central may reset its silence ceiling. A pass
		// that could not read one falls through and says nothing, which is what
		// makes the ceiling fire on a connector that has lost pod visibility.
		if podsKnown {
			w.reportOccupancy(node, burstID, now)
		}

		cond := readyCondition(node)
		if cond == nil || cond.Status != corev1.ConditionTrue {
			// Not idle capacity — a health problem, and one NodeWatcher's phases
			// and NodeGC already own.
			w.stopBeingIdle(node, now)
			continue
		}
		// Busy and unknown get the same answer, which is the whole safety
		// argument: a pod list this connector could not read says nothing about
		// the node, and "says nothing" must never resolve to "idle".
		if !podsKnown || occupied[node.Name] {
			w.stopBeingIdle(node, now)
			continue
		}

		idleFor, ready := w.observeIdle(burstID, now)
		if !ready {
			continue
		}
		w.Log.Info("burst node has been idle for the grace period; requesting teardown",
			"burst", burstID, "node", node.Name, "idle_for", idleFor.Round(time.Second))
		w.Central.ReportNodeEvent(protocol.NodeEvent{
			BurstID:  burstID,
			NodeName: node.Name,
			Phase:    protocol.NodePhaseIdle,
			Reason:   nodeIdleReason,
		})
	}
	w.forgetUnseen(seen)
}

// stopBeingIdle ends a node's grace window and, if this connector has an idle
// request outstanding for it, WITHDRAWS that request.
//
// An idle request is held by the connector and re-sent until central
// acknowledges it, so one made while central was unreachable is still pending
// when central comes back — and by then KEDA may have scaled the Deployment up
// again onto this very node. WithdrawIdleNodeEvent is the explicit revocation:
// it drops the held request AND states the phase the connector now sees. An
// ordinary report would do only the second half, deliberately: a health phase
// that arrives for its own reasons — a transition, or the occupancy observation
// riding one — must not cancel a teardown it knows nothing about.
//
// It reports the node's real phase rather than inventing a "not idle" signal:
// central has a durable meaning for the lifecycle phases already, and this is
// simply what the connector now sees.
func (w *IdleNodeWatcher) stopBeingIdle(node *corev1.Node, now time.Time) {
	burstID, ok := burstIDForNodeName(node.Name)
	if !ok {
		return
	}
	if !w.endWindow(burstID) {
		return
	}
	phase, reason := nodePhase(node, false, w.joiningGrace(), now.UTC())
	observedAt := nodePhaseObservedAt(node, false, phase, w.joiningGrace(), now.UTC())
	w.Log.Info("burst node is no longer idle; withdrawing the teardown request",
		"burst", burstID, "node", node.Name, "phase", phase)
	w.Central.WithdrawIdleNodeEvent(protocol.NodeEvent{
		BurstID:    burstID,
		NodeName:   node.Name,
		Phase:      phase,
		Reason:     reason,
		ObservedAt: &observedAt,
	})
}

// reportOccupancy tells central, at most once per OccupancyInterval per burst,
// that this connector read the whole cluster's pods for this node — which is the
// only evidence that the idle teardown signal still works, and so the only thing
// central's nodeOnly silence ceiling may measure from.
//
// It rides an ordinary health phase because central already has a durable
// meaning for one; the flag is what carries the claim. The phase is a bonus, not
// the point, and central must stamp the observation ONLY on the flag.
//
// Callers must have a successful pod LIST for this sweep. There is no arm here
// that reports without one: an observation sent on a failed list would be the
// exact lie the ceiling exists to catch.
func (w *IdleNodeWatcher) reportOccupancy(node *corev1.Node, burstID string, now time.Time) {
	if !w.occupancyDue(burstID, now) {
		return
	}
	phase, reason := nodePhase(node, false, w.joiningGrace(), now.UTC())
	observedAt := nodePhaseObservedAt(node, false, phase, w.joiningGrace(), now.UTC())
	w.Central.ReportNodeEvent(protocol.NodeEvent{
		BurstID:           burstID,
		NodeName:          node.Name,
		Phase:             phase,
		Reason:            reason,
		ObservedAt:        &observedAt,
		OccupancyObserved: true,
	})
}

// occupancyDue reports whether this burst is due an observation, recording that
// one was taken when it is. A burst with no entry is due immediately: a
// connector that just started is watching, and making it look dead for an
// interval would spend the ceiling it is there to hold open.
func (w *IdleNodeWatcher) occupancyDue(burstID string, now time.Time) bool {
	interval := w.OccupancyInterval
	if interval <= 0 {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.occupancy == nil {
		w.occupancy = make(map[string]time.Time)
	}
	if last, ok := w.occupancy[burstID]; ok && now.Sub(last) < interval {
		return false
	}
	w.occupancy[burstID] = now
	return true
}

// occupiedNodes builds the set of node names carrying work, from ONE cluster-wide
// pod LIST. known=false is "could not tell", which the caller must treat as busy
// for EVERY node — this is the only pod evidence the pass has.
//
// One list per sweep, not one per burst node. A per-node list with a field
// selector reads better, but the API server pays a full pod scan for each one, so
// a fleet of n burst nodes turned one sweep into n scans of the same data.
func (w *IdleNodeWatcher) occupiedNodes(ctx context.Context) (occupied map[string]bool, known bool) {
	pods, err := w.K8s.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		w.Log.Warn("idle sweep: list pods failed; every node in this pass is unknown, not idle", "error", err)
		return nil, false
	}
	occupied = make(map[string]bool)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName == "" {
			continue
		}
		if podOccupiesNode(pod) {
			occupied[pod.Spec.NodeName] = true
		}
	}
	return occupied, true
}

// podOccupiesNode reports whether one pod is a reason to keep a burst node
// alive.
//
// DaemonSet pods and mirror pods are infrastructure: they follow the node rather
// than keeping it, and every burst node carries some, so counting them would
// mean no node is ever idle. A pod already terminating is not work either — the
// thing that deleted it has moved on, and waiting for it to disappear would just
// mean waiting out its grace period. Succeeded and Failed pods are finished.
func podOccupiesNode(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	if isDaemonSetPod(pod) || isMirrorPod(pod) {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodPending, corev1.PodRunning:
		return true
	}
	return false
}

// isMirrorPod is true for a pod the kubelet mirrored from a static manifest on
// the node. It has no controller to reschedule it, so it cannot keep a node
// occupied in any sense a customer cares about.
func isMirrorPod(pod *corev1.Pod) bool {
	_, ok := pod.Annotations[podConfigMirrorAnnotation]
	return ok
}

// observeIdle records that this burst's node was seen idle now, and reports how
// long it has been idle and whether that has reached the grace period. A pass
// past the grace re-arms the outstanding-request flag, because the request is
// re-asserted on every such pass.
func (w *IdleNodeWatcher) observeIdle(burstID string, now time.Time) (time.Duration, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tracked == nil {
		w.tracked = make(map[string]idleNodeState)
	}
	st := w.tracked[burstID]
	if st.since.IsZero() {
		st.since = now
		w.tracked[burstID] = st
		return 0, false
	}
	idleFor := now.Sub(st.since)
	if idleFor < w.idleGrace() {
		return idleFor, false
	}
	st.reported = true
	w.tracked[burstID] = st
	return idleFor, true
}

// endWindow clears a burst's grace window and reports whether an idle request
// was outstanding — i.e. whether the caller owes central a withdrawal.
func (w *IdleNodeWatcher) endWindow(burstID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	st, tracked := w.tracked[burstID]
	if !tracked {
		return false
	}
	if !st.reported {
		delete(w.tracked, burstID)
		return false
	}
	delete(w.tracked, burstID)
	return true
}

// forgetUnseen drops every burst whose node was not in this pass's LIST,
// outstanding request and all.
//
// Dropping the request here is safe where dropping it on an unreadable pass is
// not: the Node object is GONE, so NodeWatcher reports Removed for that burst,
// and a Removed supersedes a held idle request in the connector's cache. The
// withdrawal has effectively already been sent, by the stronger signal. A node
// that comes back under the same name starts its window over, which is the same
// fresh-evidence rule a restart follows.
func (w *IdleNodeWatcher) forgetUnseen(seen map[string]bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for burstID := range w.tracked {
		if !seen[burstID] {
			delete(w.tracked, burstID)
		}
	}
	for burstID := range w.occupancy {
		if !seen[burstID] {
			delete(w.occupancy, burstID)
		}
	}
}

// restartAllWindows throws away every grace window but KEEPS every outstanding
// request. See the type comment on idleNodeState: the window is evidence and the
// request is a debt, and a pass that observed nothing invalidates the first
// without settling the second.
func (w *IdleNodeWatcher) restartAllWindows() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for burstID, st := range w.tracked {
		if !st.reported {
			delete(w.tracked, burstID)
			continue
		}
		st.since = time.Time{}
		w.tracked[burstID] = st
	}
}

func (w *IdleNodeWatcher) joiningGrace() time.Duration {
	if w.JoiningGrace > 0 {
		return w.JoiningGrace
	}
	return defaultNodeJoiningGrace
}

func (w *IdleNodeWatcher) idleGrace() time.Duration {
	if w.IdleGrace > 0 {
		return w.IdleGrace
	}
	return defaultNodeIdleGrace
}

func (w *IdleNodeWatcher) sweepInterval() time.Duration {
	if w.SweepInterval > 0 {
		return w.SweepInterval
	}
	return defaultIdleSweepInterval
}
