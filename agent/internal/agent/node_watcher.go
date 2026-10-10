package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"github.com/yscale-sh/yscale/pkg/backends"
	v1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// NodeEventReporter is the slice of *Client the node watcher needs: publish one
// burst-node lifecycle phase. Kept as an interface so the watcher is testable
// without a live WebSocket, exactly as routeReporter is for gateway routes.
type NodeEventReporter interface {
	ReportNodeEvent(ev protocol.NodeEvent)
}

// IdleNodeEventReporter is IdleNodeWatcher's larger slice of *Client: it also
// REVOKES the idle teardown request it made. Separate from NodeEventReporter
// because the revocation is a privilege, not a convenience — a held request may
// only be dropped by the watcher that looked at the node's pods, and this is what
// keeps the health watcher from being able to.
type IdleNodeEventReporter interface {
	NodeEventReporter
	WithdrawIdleNodeEvent(ev protocol.NodeEvent)
}

// NodeWatcher tells central what has happened to the Kubernetes Nodes behind
// its bursts.
//
// Central has no other view of them. It knows a backend VM was created and it
// knows when a workload reports itself finished, but between those two it is
// blind: a node that never joins, one whose kubelet dies, and one an operator
// deletes all look identical from there — a burst in "provisioning" that keeps
// billing until a budget runs out. This is the signal that closes that gap, and
// on Removed it is what lets central tear the node down promptly instead of
// waiting for the watchdog.
//
// # Scope
//
// Yscale burst nodes ONLY, matched by the cluster identity central stamped and
// the name central itself derived (backends.NodeNamePrefix). A customer's own
// nodes and another connector's burst nodes are not reported: their lifecycle
// is none of this socket's business. A node missing either identity is dropped
// rather than guessed.
//
// # Restarts
//
// Every pass begins with a LIST, so a connector that restarts reports the
// current phase of every burst node it can see rather than waiting for the next
// transition. Without that a node that went Ready while the connector was down
// would sit in "provisioning" until something else moved it.
//
// # Not a liveness signal
//
// What this watcher sends is NODE HEALTH and nothing more. It says nothing about
// whether anything in this cluster can still see that a burst has gone idle: a
// namespace-scoped connector, and one that has lost cluster-wide pod LIST, both
// keep reporting Ready forever while the teardown signal is dead. Central's
// nodeOnly ceiling measures silence from NodeEvent.OccupancyObserved, which only
// IdleNodeWatcher sends — a health report must never postpone it.
type NodeWatcher struct {
	K8s     kubernetes.Interface
	Central NodeEventReporter
	Log     *slog.Logger
	// ClusterID is the stable identity central assigned this connector. A burst
	// node without this exact identity belongs to another installation (or
	// predates ownership labels) and must not be reported through this socket.
	ClusterID string

	// JoiningGrace is how long a burst node that is not Ready is still reported
	// as Joining rather than NotReady. Default: 5 m.
	//
	// A kubelet registers its Node object before it can serve anything, so every
	// healthy burst spends its first minute or two Ready=False. Reporting that
	// as NotReady would show a node failing during the bring-up every burst goes
	// through, and would make the phase useless for telling a slow boot from a
	// node that has actually stopped answering.
	JoiningGrace time.Duration

	// RetryInterval bounds how fast a dropped watch is re-established.
	// Default: 5 s.
	RetryInterval time.Duration

	// mu guards reported, the last phase sent for each burst. It is what makes a
	// re-LIST cheap: a node whose phase has not changed is not re-reported, so
	// the steady state costs nothing on the wire.
	mu       sync.Mutex
	reported map[string]reportedState
}

type reportedState struct {
	phase          string
	gpuAllocatable bool
}

const (
	defaultNodeJoiningGrace  = 5 * time.Minute
	defaultNodeWatchRetry    = 5 * time.Second
	burstNodeNameSuffixLen   = 12
	nodeRemovedReasonDeleted = "node object deleted from the cluster"
	nodeJoiningReasonNoCond  = "kubelet has not reported a Ready condition yet"

	gpuNotReadyTaintKey = "nvidia.com/gpu-not-ready"
)

// NewNodeWatcher constructs a watcher with production defaults.
func NewNodeWatcher(k8s kubernetes.Interface, central NodeEventReporter, clusterID string, log *slog.Logger) *NodeWatcher {
	if log == nil {
		log = slog.Default()
	}
	return &NodeWatcher{
		K8s:           k8s,
		Central:       central,
		Log:           log,
		ClusterID:     clusterID,
		JoiningGrace:  defaultNodeJoiningGrace,
		RetryInterval: defaultNodeWatchRetry,
		reported:      make(map[string]reportedState),
	}
}

// Run watches burst Nodes until ctx is cancelled, re-establishing the watch
// whenever it drops. Modelled on PendingPodWatcher.Run.
func (w *NodeWatcher) Run(ctx context.Context) error {
	w.Log.Info("burst node watcher starting", "joining_grace", w.joiningGrace())
	for {
		if err := w.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.Log.Warn("node watch ended; retrying", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.retryInterval()):
			}
		}
	}
}

// runOnce reports the current state of every burst node, then follows changes
// until the watch drops.
//
// The LIST comes first and the watch resumes from its resourceVersion, so the
// two together cover the whole timeline: nothing that was already true is
// missed, and nothing that changes between the two calls falls in the gap.
func (w *NodeWatcher) runOnce(ctx context.Context) error {
	if w.ClusterID == "" {
		return errors.New("watch burst nodes: cluster ID is required")
	}
	selector := klabels.Set{v1.LabelClusterID: w.ClusterID}.AsSelector().String()
	nodes, err := w.K8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	for i := range nodes.Items {
		w.observe(&nodes.Items[i], false)
	}

	// Keep the watch unfiltered. Kubernetes represents an object leaving a
	// label-selected watch as Deleted even when only its label changed; treating
	// that synthetic event as a removed Node could tear down a live provider VM.
	// observe applies the same ownership check defensively to every real event.
	wi, err := w.K8s.CoreV1().Nodes().Watch(ctx, metav1.ListOptions{
		ResourceVersion: nodes.ResourceVersion,
	})
	if err != nil {
		return fmt.Errorf("watch nodes: %w", err)
	}
	defer wi.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-wi.ResultChan():
			if !ok {
				return errors.New("node watch channel closed")
			}
			w.handleEvent(ev)
		}
	}
}

func (w *NodeWatcher) handleEvent(ev watch.Event) {
	node, ok := ev.Object.(*corev1.Node)
	if !ok {
		// watch.Error frames and anything else the API server sends that is not
		// a Node. The watch itself ends on its own when the connection breaks.
		return
	}
	switch ev.Type {
	case watch.Added, watch.Modified:
		w.observe(node, false)
	case watch.Deleted:
		w.observe(node, true)
	}
}

// observe reports one node's phase, if it is a burst node and the phase has
// changed since the last report for that burst.
//
// The dedupe is against what was REPORTED, not against the node's previous
// object: a Modified event fires for every status refresh a kubelet makes, and
// central gains nothing from being told twice that a node is still Ready.
//
// A terminal report drops the entry rather than keeping a tombstone. The Node
// object is gone, so nothing in this cluster can produce another phase for that
// burst — an entry kept "just in case" would be one this process never frees.
func (w *NodeWatcher) observe(node *corev1.Node, deleted bool) {
	burstID, ok := burstIDForNodeName(node.Name)
	if !ok {
		return
	}
	if w.ClusterID == "" || node.Labels[v1.LabelClusterID] != w.ClusterID {
		return
	}

	if !deleted {
		w.reconcileGPUMetadata(node)
	}

	now := time.Now().UTC()
	phase, reason := nodePhase(node, deleted, w.joiningGrace(), now)
	observedAt := nodePhaseObservedAt(node, deleted, phase, w.joiningGrace(), now)
	hasGPU := !deleted && gpuAllocatable(node)

	w.mu.Lock()
	if w.reported == nil {
		w.reported = make(map[string]reportedState)
	}
	current := reportedState{phase: phase, gpuAllocatable: hasGPU}
	if last, seen := w.reported[burstID]; seen && last == current {
		w.mu.Unlock()
		return
	}
	if phase == protocol.NodePhaseRemoved {
		delete(w.reported, burstID)
	} else {
		w.reported[burstID] = current
	}
	w.mu.Unlock()

	ev := protocol.NodeEvent{
		BurstID:    burstID,
		NodeName:   node.Name,
		Phase:      phase,
		Reason:     reason,
		ObservedAt: &observedAt,
	}
	if hasGPU {
		ev.GPUAllocatable = true
		ev.GPUAllocatableAt = &now
	}
	w.Log.Info("burst node phase changed",
		"burst", burstID, "node", node.Name, "phase", phase, "reason", reason,
		"gpu_allocatable", hasGPU)
	w.Central.ReportNodeEvent(ev)
}

// reconcileGPUMetadata ensures a burst node with positive nvidia.com/gpu
// allocatable carries the standard presence label and no gpu-not-ready taint.
// The label is set ONLY from observed allocatable quantity — never from
// requested/planned GPU shape — so CPU nodes, pre-ready GPU nodes, and
// non-yscale customer nodes never receive it.
//
// The update is conflict-safe: it re-reads the node, applies both mutations,
// and retries on conflict. A transient failure is not fatal — the next watch
// event re-enters observe and retries naturally.
func (w *NodeWatcher) reconcileGPUMetadata(node *corev1.Node) {
	if !gpuAllocatable(node) {
		return
	}
	if !hasGPUNotReadyTaint(node) && hasGPUPresentLabel(node) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for attempt := 0; attempt < 3; attempt++ {
		fresh, err := w.K8s.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return
			}
			w.Log.Warn("gpu metadata reconciliation: get failed", "node", node.Name, "error", err)
			return
		}
		if !gpuAllocatable(fresh) {
			return
		}
		needsUpdate := false
		if hasGPUNotReadyTaint(fresh) {
			filtered := make([]corev1.Taint, 0, len(fresh.Spec.Taints))
			for _, t := range fresh.Spec.Taints {
				if t.Key != gpuNotReadyTaintKey {
					filtered = append(filtered, t)
				}
			}
			fresh.Spec.Taints = filtered
			needsUpdate = true
		}
		if !hasGPUPresentLabel(fresh) {
			if fresh.Labels == nil {
				fresh.Labels = make(map[string]string)
			}
			fresh.Labels[v1.LabelNvidiaGPUPresent] = "true"
			needsUpdate = true
		}
		if !needsUpdate {
			return
		}
		if _, err := w.K8s.CoreV1().Nodes().Update(ctx, fresh, metav1.UpdateOptions{}); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			w.Log.Warn("gpu metadata reconciliation: update failed", "node", node.Name, "error", err)
			return
		}
		w.Log.Info("reconciled gpu node metadata", "node", node.Name)
		return
	}
	w.Log.Warn("gpu metadata reconciliation: exhausted retries", "node", node.Name)
}

func hasGPUNotReadyTaint(node *corev1.Node) bool {
	for _, t := range node.Spec.Taints {
		if t.Key == gpuNotReadyTaintKey {
			return true
		}
	}
	return false
}

func hasGPUPresentLabel(node *corev1.Node) bool {
	return node.Labels[v1.LabelNvidiaGPUPresent] == "true"
}

func gpuAllocatable(node *corev1.Node) bool {
	q := node.Status.Allocatable[corev1.ResourceName("nvidia.com/gpu")]
	return !q.IsZero()
}

func (w *NodeWatcher) joiningGrace() time.Duration {
	if w.JoiningGrace > 0 {
		return w.JoiningGrace
	}
	return defaultNodeJoiningGrace
}

func (w *NodeWatcher) retryInterval() time.Duration {
	if w.RetryInterval > 0 {
		return w.RetryInterval
	}
	return defaultNodeWatchRetry
}

// nodePhase maps a Node object onto the protocol's closed phase set. A pure
// function of the object, the deletion flag and the clock, so what a connector
// reports never depends on what it happened to have seen before — two
// connectors looking at the same cluster agree.
func nodePhase(node *corev1.Node, deleted bool, joiningGrace time.Duration, now time.Time) (phase, reason string) {
	if deleted {
		return protocol.NodePhaseRemoved, nodeRemovedReasonDeleted
	}
	cond := readyCondition(node)
	switch {
	case cond == nil:
		// The kubelet has registered but posted no conditions yet.
		return protocol.NodePhaseJoining, nodeJoiningReasonNoCond
	case cond.Status == corev1.ConditionTrue:
		return protocol.NodePhaseReady, ""
	case now.Sub(node.CreationTimestamp.Time) < joiningGrace:
		return protocol.NodePhaseJoining, cond.Reason
	default:
		return protocol.NodePhaseNotReady, cond.Reason
	}
}

// readyAfterCreation is how far after the node's CreationTimestamp a Ready
// report is stamped at the earliest. A millisecond survives every hop: the
// RFC 3339 nanosecond wire format and Postgres's microsecond timestamptz.
const readyAfterCreation = time.Millisecond

// nodePhaseObservedAt returns the best available timestamp for a phase
// transition. Kubernetes Node timestamps (condition LastTransitionTime, node
// CreationTimestamp) are stable across reconnect replays — the same Ready event
// always carries the same time — so central can order replayed and fresh events
// correctly without rewriting timestamps. Falls back to wall-clock time when the
// Node object provides nothing.
func nodePhaseObservedAt(node *corev1.Node, deleted bool, phase string, joiningGrace time.Duration, now time.Time) time.Time {
	if deleted {
		if node.DeletionTimestamp != nil && !node.DeletionTimestamp.IsZero() {
			return node.DeletionTimestamp.Time.UTC()
		}
		return now
	}
	cond := readyCondition(node)
	switch phase {
	case protocol.NodePhaseReady:
		// Joining is stamped with the CreationTimestamp and central keeps only a
		// strictly newer phase. Both are whole seconds, from two clocks: the
		// apiserver's for creation, the burst kubelet's for the condition. A node
		// that turns Ready within its creation second, or whose clock lags the
		// apiserver's, would carry a Ready no later than its Joining and central
		// would drop it for good. Ready cannot precede the node's existence, so
		// the floor is just after creation; it stays a pure function of the
		// object, so a replay still carries the same time.
		var observed time.Time
		if cond != nil && !cond.LastTransitionTime.IsZero() {
			observed = cond.LastTransitionTime.Time.UTC()
		}
		if !node.CreationTimestamp.IsZero() {
			floor := node.CreationTimestamp.Time.UTC().Add(readyAfterCreation)
			if observed.Before(floor) {
				observed = floor
			}
		}
		if !observed.IsZero() {
			return observed
		}
	case protocol.NodePhaseNotReady:
		var observed time.Time
		if cond != nil && !cond.LastTransitionTime.IsZero() {
			observed = cond.LastTransitionTime.Time.UTC()
		}
		if !node.CreationTimestamp.IsZero() {
			boundary := node.CreationTimestamp.Time.UTC().Add(joiningGrace)
			if observed.IsZero() || boundary.After(observed) {
				observed = boundary
			}
		}
		if !observed.IsZero() {
			return observed
		}
	case protocol.NodePhaseJoining:
		if !node.CreationTimestamp.IsZero() {
			return node.CreationTimestamp.Time.UTC()
		}
	}
	if !node.CreationTimestamp.IsZero() {
		return node.CreationTimestamp.Time.UTC()
	}
	return now
}

func readyCondition(node *corev1.Node) *corev1.NodeCondition {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == corev1.NodeReady {
			return &node.Status.Conditions[i]
		}
	}
	return nil
}

// burstIDForNodeName recovers central's burst id from a burst node's canonical
// name. It is the exact inverse of expectedBurstNodeName, which mirrors
// central's own derivation (decider.go: backends.NodeNamePrefix + the id with
// its "burst_" prefix stripped).
//
// Central mints burst IDs as "burst_" + randHex(6), producing exactly 12
// lowercase hexadecimal characters. The node name is NodeNamePrefix + those 12
// hex chars. This parser accepts only that exact shape: exactly
// burstNodeNameSuffixLen lowercase hex characters after the prefix.
//
// Deriving rather than reading an id off the node is deliberate: a label is
// writable by anything with node access in the customer's cluster, and an id
// taken from one would let a pod in that cluster name another burst. The name
// is the only part central assigned. It is still only a claim — central checks
// the burst against the tenant this connector authenticated as — so the job
// here is to refuse anything that is not a name central could have minted.
func burstIDForNodeName(name string) (string, bool) {
	suffix, ok := strings.CutPrefix(name, backends.NodeNamePrefix)
	if !ok || len(suffix) != burstNodeNameSuffixLen {
		return "", false
	}
	for i := 0; i < len(suffix); i++ {
		c := suffix[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", false
		}
	}
	return "burst_" + suffix, true
}
