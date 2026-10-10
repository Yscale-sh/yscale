package agent

import (
	"context"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/yscale-sh/yscale/pkg/backends"
)

// NodeGC is a periodic garbage-collector for ghost burst Nodes.
//
// # Problem
//
// Central tears down a burst by pushing a DrainNode command to the
// agent exactly once, best-effort (drainBurstNode in central). If the
// push fails (agent briefly offline, WS blip), the dead burst's k8s
// Node object lingers NotReady in the customer cluster forever —
// Kubernetes never garbage-collects Node objects on its own.
//
// # Safety rationale
//
// Deleting a Node object is recoverable: a live kubelet re-registers
// on reconnect. The grace period (default 15 m) only bounds how
// quickly a partitioned-but-alive burst node gets evicted, and burst
// nodes have a 90 m maximum lifetime anyway. We only touch nodes
// whose name carries the burst prefix, so customer-managed nodes are
// never affected.
type NodeGC struct {
	K8s     kubernetes.Interface
	Handler *RealHandler
	Log     *slog.Logger

	// SweepInterval is how often to LIST nodes and consider candidates.
	// Default: 5 m. Override in tests via struct field before calling Run.
	SweepInterval time.Duration

	// GracePeriod is how long a burst node must be continuously
	// NotReady before the GC deletes it.
	// Default: 15 m. Override in tests via struct field before calling Run.
	GracePeriod time.Duration
}

// NewNodeGC constructs a NodeGC with production defaults.
func NewNodeGC(k8s kubernetes.Interface, handler *RealHandler, log *slog.Logger) *NodeGC {
	if log == nil {
		log = slog.Default()
	}
	return &NodeGC{
		K8s:           k8s,
		Handler:       handler,
		Log:           log,
		SweepInterval: 5 * time.Minute,
		GracePeriod:   15 * time.Minute,
	}
}

// Run sweeps burst Nodes on every SweepInterval tick until ctx is
// cancelled. Modelled on WorkloadReconciler.Run.
func (g *NodeGC) Run(ctx context.Context) error {
	g.Log.Info("node gc starting",
		"sweep_interval", g.SweepInterval,
		"grace_period", g.GracePeriod,
	)
	ticker := time.NewTicker(g.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			g.sweep(ctx)
		}
	}
}

// sweep lists all Nodes and GC-s any ghost burst nodes.
func (g *NodeGC) sweep(ctx context.Context) {
	nodes, err := g.K8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		g.Log.Error("node gc: list nodes failed", "error", err)
		return
	}
	now := time.Now()
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if !isBurstNode(node) {
			continue
		}
		notReadyFor, ok := notReadyDuration(node, now)
		if !ok {
			// Node is Ready — not a GC candidate.
			continue
		}
		if notReadyFor < g.GracePeriod {
			g.Log.Info("node gc: burst node not-ready within grace period, skipping",
				"node", node.Name,
				"not_ready_for", notReadyFor.Round(time.Second),
			)
			continue
		}
		g.Log.Info("node gc: deleting ghost burst node",
			"node", node.Name,
			"not_ready_for", notReadyFor.Round(time.Second),
		)
		if err := g.Handler.drainAndDeleteNode(ctx, node.Name, true); err != nil {
			// Log-and-continue: one bad node must not stop the sweep.
			g.Log.Error("node gc: failed to drain/delete node",
				"node", node.Name,
				"error", err,
			)
		}
	}
}

// isBurstNode returns true iff the node name carries the burst prefix.
func isBurstNode(node *corev1.Node) bool {
	return len(node.Name) > len(backends.NodeNamePrefix) &&
		node.Name[:len(backends.NodeNamePrefix)] == backends.NodeNamePrefix
}

// notReadyDuration returns how long the node has been continuously
// NotReady (false, Ready, or Unknown condition), and whether it is
// currently not-ready. If the node has no Ready condition at all, its
// CreationTimestamp is used as the transition time.
func notReadyDuration(node *corev1.Node, now time.Time) (time.Duration, bool) {
	for _, cond := range node.Status.Conditions {
		if cond.Type != corev1.NodeReady {
			continue
		}
		if cond.Status == corev1.ConditionTrue {
			return 0, false // Node is Ready — not a GC candidate.
		}
		return now.Sub(cond.LastTransitionTime.Time), true
	}
	// No Ready condition at all: treat as not-ready since creation.
	return now.Sub(node.CreationTimestamp.Time), true
}
