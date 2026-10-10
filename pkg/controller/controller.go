package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/config"
	"github.com/yscale-sh/yscale/pkg/cost"
	"github.com/yscale-sh/yscale/pkg/metrics"
)

const (
	LabelBurstNode      = "yscale.sh/burst-node"
	LabelPrewarm        = "yscale.sh/prewarm"
	AnnotationMachineID = "yscale.sh/machine-id"

	// scaleUpCooldown prevents over-provisioning by waiting after a node
	// becomes ready before considering more scale-ups. Gives the scheduler
	// time to place pending pods on the new node.
	scaleUpCooldown = 30 * time.Second

	drainTimeout         = 60 * time.Second
	evictionPollInterval = 5 * time.Second
)

func isDaemonSetPod(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

func isNodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

type burstNode struct {
	backendID string
	nodeName  string
	createdAt time.Time
	ready     bool
	readyAt   *time.Time // when the node first became ready
	idleSince *time.Time
	// gpuTaintCleared tracks whether the nvidia.com/gpu-not-ready taint has been
	// cleared (true for CPU nodes too — they never carry it — so the readiness
	// loop stops re-checking them). See removeGpuNotReadyTaint.
	gpuTaintCleared bool
}

type Controller struct {
	clientset   kubernetes.Interface
	podLister   listersv1.PodLister
	nodeLister  listersv1.NodeLister
	podsSynced  cache.InformerSynced
	nodesSynced cache.InformerSynced
	backend     backends.Backend
	cfg         *config.Config
	log         *slog.Logger
	metrics     *metrics.Metrics
	costs       *cost.Tracker

	// scopeHash is the stable pool-ownership hash (cluster + join config)
	// applied via backends.ScopedBackend at startup; configHash fingerprints
	// the join-identity config so stale prewarm snapshots are rejected.
	// suspendMode is true only when config requests suspend pooling AND the
	// backend implements both WarmCapability and ScopedBackend.
	scopeHash   string
	configHash  string
	suspendMode bool

	// prewarmRefusalOnce keeps the fail-closed prewarm refusal to a single
	// log line instead of one per reconcile tick.
	prewarmRefusalOnce sync.Once

	mu           sync.Mutex
	nodes        map[string]*burstNode
	provisioning int
	// prewarming tracks prewarm machines that are created and running but
	// not yet suspended (booting or joining), keyed by backend ID. They are
	// intentionally NOT in c.nodes: scaleUp/scaleDown must never touch them.
	prewarming map[string]*prewarmingNode

	// scaleUpGeneration tracks how many scale-ups have happened.
	// When a new node becomes ready but pods remain unschedulable,
	// we increment failedScaleUps. If it hits the active node count
	// we stop scaling — the pods can't be satisfied by burst nodes.
	failedScaleUps    int
	lastOrphanCleanup time.Time
}

func New(
	clientset kubernetes.Interface,
	factory informers.SharedInformerFactory,
	backend backends.Backend,
	cfg *config.Config,
	log *slog.Logger,
	m *metrics.Metrics,
	costs *cost.Tracker,
) *Controller {
	podInformer := factory.Core().V1().Pods()
	nodeInformer := factory.Core().V1().Nodes()

	c := &Controller{
		clientset:   clientset,
		podLister:   podInformer.Lister(),
		nodeLister:  nodeInformer.Lister(),
		podsSynced:  podInformer.Informer().HasSynced,
		nodesSynced: nodeInformer.Informer().HasSynced,
		backend:     backend,
		cfg:         cfg,
		log:         log,
		metrics:     m,
		costs:       costs,
		nodes:       make(map[string]*burstNode),
		prewarming:  make(map[string]*prewarmingNode),
	}
	c.initPoolIdentity()
	return c
}

func (c *Controller) Run(ctx context.Context) error {
	c.log.Info("starting autoscaler", "backend", c.backend.Name(), "maxNodes", c.cfg.Scaling.MaxNodes)
	c.logPrewarmPosture()

	if !cache.WaitForCacheSync(ctx.Done(), c.podsSynced, c.nodesSynced) {
		return fmt.Errorf("timed out waiting for cache sync")
	}

	c.recoverNodes()
	c.cleanupOrphans(ctx)

	ticker := time.NewTicker(c.cfg.Scaling.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.log.Info("autoscaler stopped")
			return nil
		case <-ticker.C:
			c.reconcile(ctx)
		}
	}
}

func (c *Controller) reconcile(ctx context.Context) {
	start := time.Now()
	defer func() {
		if c.metrics != nil {
			c.metrics.ReconcileDurationSec.Observe(time.Since(start).Seconds())
		}
	}()

	c.scaleUp(ctx)
	c.updateNodeStatus()
	c.scaleDown(ctx)
	c.maintainPrewarmPool(ctx)
	c.suspendReadyPrewarmNodes(ctx)

	// Refresh tracked-state gauges once per reconcile.
	if c.metrics != nil {
		c.mu.Lock()
		active := len(c.nodes)
		provisioning := c.provisioning
		ready := 0
		for _, bn := range c.nodes {
			if bn.ready {
				ready++
			}
		}
		c.mu.Unlock()
		c.metrics.NodesActive.Set(float64(active))
		c.metrics.NodesProvisioning.Set(float64(provisioning))
		c.metrics.NodesReady.Set(float64(ready))
	}

	// Run orphan cleanup periodically (every 5 minutes) to catch
	// resources leaked by crashed controller runs.
	c.mu.Lock()
	shouldCleanup := time.Since(c.lastOrphanCleanup) > 5*time.Minute
	c.mu.Unlock()
	if shouldCleanup {
		c.cleanupOrphans(ctx)
	}
}

// scaleUp checks for unschedulable pods and provisions new burst nodes.
func (c *Controller) scaleUp(ctx context.Context) {
	if c.costs != nil && c.costs.CapReached() {
		c.log.Warn("monthly cost cap reached, skipping scale-up",
			"monthSpentUSD", c.costs.MonthSpentUSD(),
			"capUSD", c.cfg.Scaling.MonthlyCapUSD)
		return
	}

	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		c.log.Error("listing pods", "error", err)
		return
	}

	var unschedulable int
	for _, pod := range pods {
		if c.isPendingUnschedulable(pod) {
			unschedulable++
		}
	}

	if c.metrics != nil {
		c.metrics.UnschedulablePods.Set(float64(unschedulable))
	}

	if unschedulable == 0 {
		c.mu.Lock()
		c.failedScaleUps = 0
		c.mu.Unlock()
		return
	}

	c.mu.Lock()
	currentNodes := len(c.nodes)
	provisioning := c.provisioning
	maxConcurrent := c.cfg.Scaling.MaxConcurrentProvisions
	failedScaleUps := c.failedScaleUps

	// Count nodes still booting.
	pendingNodes := 0
	for _, bn := range c.nodes {
		if !bn.ready {
			pendingNodes++
		}
	}

	// Don't provision if a node recently became ready — give the scheduler
	// time to place pods on it before creating more.
	recentlyReady := false
	for _, bn := range c.nodes {
		if bn.readyAt != nil && time.Since(*bn.readyAt) < scaleUpCooldown {
			recentlyReady = true
			break
		}
	}
	c.mu.Unlock()

	// If we've already scaled up and the pods are still unschedulable after
	// nodes came up idle, stop scaling — the pods likely need more resources
	// than a single burst node can provide.
	if failedScaleUps > 0 && pendingNodes == 0 && provisioning == 0 {
		c.log.Warn("pods still unschedulable after scale-up, not scaling further",
			"unschedulable", unschedulable, "failedAttempts", failedScaleUps)
		return
	}

	inFlight := pendingNodes + provisioning
	if inFlight >= maxConcurrent {
		c.log.Info("at concurrent provision limit",
			"pending", pendingNodes, "provisioning", provisioning, "limit", maxConcurrent)
		return
	}

	if recentlyReady {
		c.log.Info("cooldown: waiting for scheduler to place pods on new node")
		return
	}

	totalActive := currentNodes + provisioning
	if totalActive >= c.cfg.Scaling.MaxNodes {
		c.log.Warn("max burst nodes reached", "current", currentNodes, "max", c.cfg.Scaling.MaxNodes)
		return
	}

	c.log.Info("unschedulable pods detected, scaling up", "unschedulable", unschedulable, "currentNodes", currentNodes)
	// Reserve synchronously before launching the goroutine. cleanupOrphans uses
	// this count as a destructive-operation lease; incrementing inside the
	// goroutine leaves a scheduler window where cleanup can observe zero.
	c.reserveProvision()
	go c.provisionNode(ctx)
}

func (c *Controller) reserveProvision() {
	c.mu.Lock()
	c.provisioning++
	c.mu.Unlock()
}

func (c *Controller) isPendingUnschedulable(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodPending {
		return false
	}
	if isDaemonSetPod(pod) {
		return false
	}
	if pod.CreationTimestamp.Time.After(time.Now().Add(-c.cfg.Scaling.PendingThreshold)) {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse && cond.Reason == "Unschedulable" {
			return true
		}
	}
	return false
}

func (c *Controller) provisionNode(ctx context.Context) {
	defer func() {
		c.mu.Lock()
		c.provisioning--
		c.mu.Unlock()
	}()

	start := time.Now()
	if c.startPooledNode(ctx) {
		c.recordProvision("resume", true, time.Since(start))
		return
	}
	coldStart := time.Now()
	ok := c.createNewNode(ctx)
	c.recordProvision("cold", ok, time.Since(coldStart))
}

// recordProvision records the outcome and latency of a single
// provisionNode attempt. Safe to call when c.metrics is nil.
func (c *Controller) recordProvision(mode string, success bool, dur time.Duration) {
	if c.metrics == nil {
		return
	}
	backend := c.backend.Name()
	result := "success"
	if !success {
		result = "fail"
	}
	c.metrics.NodesProvisioned.WithLabelValues(backend, mode, result).Inc()
	if success {
		c.metrics.ProvisionDurationSec.WithLabelValues(backend, mode).Observe(dur.Seconds())
	}
}

func (c *Controller) startPooledNode(ctx context.Context) bool {
	pooled, err := c.backend.ListPooledNodes(ctx)
	if err != nil {
		c.log.Error("listing pooled nodes", "error", err)
		return false
	}

	// Prefer a suspended prewarm snapshot (sub-second resume with kubelet
	// already joined) over a cold-stopped machine (clean boot + re-join).
	var candidate, cold *backends.PooledNode
	for i := range pooled {
		if pooled[i].Lifecycle != backends.LifecyclePrewarm {
			if cold == nil {
				cold = &pooled[i]
			}
			continue
		}
		if reason := c.prewarmClaimBlocked(&pooled[i]); reason != "" {
			c.log.Warn("ignoring prewarm pool candidate",
				"operation", "pool_claim", "backend", c.backend.Name(),
				"name", pooled[i].Name, "backendID", pooled[i].BackendID,
				"lifecycle", pooled[i].Lifecycle, "reason", reason)
			continue
		}
		candidate = &pooled[i]
		break
	}
	if candidate == nil {
		candidate = cold
	}
	if candidate == nil {
		return false
	}

	node := *candidate
	c.log.Info("starting pooled burst node (fast path)",
		"operation", "pool_claim", "backend", c.backend.Name(),
		"name", node.Name, "backendID", node.BackendID,
		"lifecycle", node.Lifecycle)

	if err := c.backend.StartNode(ctx, node.BackendID); err != nil {
		c.log.Error("failed to start pooled node",
			"operation", "pool_start", "backend", c.backend.Name(),
			"name", node.Name, "backendID", node.BackendID,
			"lifecycle", node.Lifecycle, "error", err)
		return false
	}

	if node.Lifecycle == backends.LifecyclePrewarm {
		if !c.confirmPrewarmResume(ctx, &node) {
			return false
		}
		if err := c.setPrewarmSchedulable(ctx, node.Name, true); err != nil {
			c.log.Error("failed to uncordon resumed prewarm node",
				"operation", "pool_start", "name", node.Name, "backendID", node.BackendID, "error", err)
		}
	}

	c.mu.Lock()
	c.nodes[node.Name] = &burstNode{
		backendID: node.BackendID,
		nodeName:  node.Name,
		createdAt: time.Now(),
	}
	c.mu.Unlock()

	// A pooled node needs the same durable backend-ID annotation as a newly
	// created node. Without it, restart recovery records an empty backend ID and
	// orphan cleanup can destroy the live machine that was just resumed.
	go c.annotateNodeWhenReady(ctx, node.Name, node.BackendID)

	if c.costs != nil {
		c.costs.Start(
			node.BackendID,
			c.backend.Name(),
			c.cfg.Scaling.NodeResources.CPUMillis,
			c.cfg.Scaling.NodeResources.MemoryMB,
			"",
		)
	}

	c.log.Info("pooled burst node start accepted",
		"operation", "pool_start", "backend", c.backend.Name(),
		"name", node.Name, "backendID", node.BackendID,
		"lifecycle", node.Lifecycle)
	return true
}

func (c *Controller) buildNodeSpec(nodeName string) *backends.NodeSpec {
	spec := &backends.NodeSpec{
		Name:       nodeName,
		AgentImage: c.cfg.AgentImage(),
		JoinMode:   c.cfg.Join.Mode,
		TSAuthKey:  c.cfg.Tailscale.AuthKey,
		TSHostname: "hs-" + nodeName,
		ScopeHash:  c.scopeHash,
		ConfigHash: c.configHash,
		NodeLabels: map[string]string{
			LabelBurstNode: "true",
		},
		Resources: backends.ResourceRequirements{
			CPUMillis: c.cfg.Scaling.NodeResources.CPUMillis,
			MemoryMB:  c.cfg.Scaling.NodeResources.MemoryMB,
		},
	}

	switch c.cfg.Join.Mode {
	case config.JoinModeKubelet:
		spec.APIServer = c.cfg.Join.APIServer
		spec.ClusterCA = c.cfg.Join.ClusterCA
	default: // k3s
		spec.K3sServerURL = c.cfg.Join.K3sServerURL
		spec.K3sToken = c.cfg.Join.K3sToken
	}

	return spec
}

func (c *Controller) createNewNode(ctx context.Context) bool {
	nodeName := c.generateNodeName()

	c.log.Info("creating new burst node (cold start)", "name", nodeName, "mode", c.cfg.Join.Mode)

	spec := c.buildNodeSpec(nodeName)

	if c.cfg.Join.Mode == config.JoinModeKubelet {
		token, err := CreateBootstrapToken(ctx, c.clientset)
		if err != nil {
			c.log.Error("failed to create bootstrap token", "error", err)
			return false
		}
		spec.BootstrapToken = token
	}

	backendID, err := c.backend.CreateNode(ctx, spec)
	if err != nil {
		c.log.Error("failed to create burst node", "name", nodeName, "error", err)
		return false
	}

	c.mu.Lock()
	c.nodes[nodeName] = &burstNode{
		backendID: backendID,
		nodeName:  nodeName,
		createdAt: time.Now(),
	}
	c.mu.Unlock()

	if c.costs != nil {
		c.costs.Start(
			backendID,
			c.backend.Name(),
			c.cfg.Scaling.NodeResources.CPUMillis,
			c.cfg.Scaling.NodeResources.MemoryMB,
			"",
		)
	}

	// Annotate the K8s node with the backend machine ID once it appears.
	// This enables recovery after controller restarts.
	go c.annotateNodeWhenReady(ctx, nodeName, backendID)

	c.log.Info("burst node created", "name", nodeName, "backendID", backendID)
	return true
}

func (c *Controller) annotateNodeWhenReady(ctx context.Context, nodeName, backendID string) {
	if ctx.Err() != nil {
		return
	}
	if c.tryAnnotateNode(ctx, nodeName, backendID) {
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	deadline := time.After(c.cfg.Scaling.NodeReadyTimeout)
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			return
		case <-ticker.C:
			if c.tryAnnotateNode(ctx, nodeName, backendID) {
				return
			}
		}
	}
}

func (c *Controller) tryAnnotateNode(ctx context.Context, nodeName, backendID string) bool {
	node, err := c.clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return false
	}
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[AnnotationMachineID] = backendID
	// A claimed node is active, not pool inventory: drop the prewarm label so
	// restart recovery re-adopts it into c.nodes instead of the prewarm pool.
	delete(node.Labels, LabelPrewarm)
	if _, err := c.clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		c.log.Error("failed to annotate burst node",
			"operation", "node_annotate", "name", nodeName,
			"backendID", backendID, "error", err)
		return false
	}
	c.log.Info("annotated burst node with machine ID",
		"operation", "node_annotate", "name", nodeName, "backendID", backendID)
	return true
}

// gpuNotReadyTaintKey is the NoSchedule taint applied at kubelet registration
// (see pkg/backends/{aws,linode}/bootstrap*.sh) to keep GPU Jobs Pending until
// the NVIDIA device plugin advertises nvidia.com/gpu. It mirrors the Cilium
// node.cilium.io/agent-not-ready gate, but for GPU readiness. The taint is
// removed CONTROLLER-side (removeGpuNotReadyTaint), never by the kubelet:
// NodeRestriction admission forbids a kubelet from mutating its own Node's
// taints, so a kubelet-side removal retries forever and GPU pods stay Pending.
const gpuNotReadyTaintKey = "nvidia.com/gpu-not-ready"

// hasGpuNotReadyTaint reports whether the node currently carries the
// nvidia.com/gpu-not-ready NoSchedule taint.
func hasGpuNotReadyTaint(node *corev1.Node) bool {
	for _, t := range node.Spec.Taints {
		if t.Key == gpuNotReadyTaintKey {
			return true
		}
	}
	return false
}

// gpuAllocatable reports whether the NVIDIA device plugin has registered
// nvidia.com/gpu as allocatable on the node — i.e. it is up and advertising
// GPUs the scheduler can grant. A zero or absent quantity means the plugin has
// not registered yet.
func gpuAllocatable(node *corev1.Node) bool {
	q := node.Status.Allocatable[corev1.ResourceName("nvidia.com/gpu")]
	return !q.IsZero()
}

// removeGpuNotReadyTaint clears the nvidia.com/gpu-not-ready taint from a burst
// node now that nvidia.com/gpu is allocatable. It performs a fresh Get + Update
// with the controller's OWN API credentials (mirroring tryAnnotateNode), never
// via a kubelet-identity pod. Returns true when the taint is absent or was
// successfully removed, so the caller can stop re-checking; false on error or if
// the resource is not yet allocatable against the authoritative view.
func (c *Controller) removeGpuNotReadyTaint(ctx context.Context, name string) bool {
	node, err := c.clientset.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		c.log.Error("failed to get burst node for gpu-taint removal",
			"operation", "gpu_taint_clear", "name", name, "error", err)
		return false
	}
	if !hasGpuNotReadyTaint(node) {
		return true // already gone (CPU node or concurrent clear) — nothing to do
	}
	if !gpuAllocatable(node) {
		return false // device plugin not registered yet against this view; retry next tick
	}
	filtered := make([]corev1.Taint, 0, len(node.Spec.Taints))
	for _, t := range node.Spec.Taints {
		if t.Key == gpuNotReadyTaintKey {
			continue
		}
		filtered = append(filtered, t)
	}
	node.Spec.Taints = filtered
	if _, err := c.clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		c.log.Error("failed to clear nvidia.com/gpu-not-ready taint",
			"operation", "gpu_taint_clear", "name", name, "error", err)
		return false
	}
	c.log.Info("cleared nvidia.com/gpu-not-ready taint (NVIDIA device plugin ready)",
		"operation", "gpu_taint_clear", "name", name)
	return true
}

// updateNodeStatus checks if provisioned nodes have joined the cluster
// and cleans up nodes that failed to become ready within the timeout.
func (c *Controller) updateNodeStatus() {
	c.mu.Lock()
	var timedOut []string
	var clearGpuTaint []string // burst nodes whose gpu-not-ready taint can now be removed
	for name, bn := range c.nodes {
		if bn.ready && bn.gpuTaintCleared {
			continue
		}
		node, err := c.nodeLister.Get(name)
		if err != nil {
			// The join timeout only applies to nodes that never became Ready.
			if !bn.ready && time.Since(bn.createdAt) > c.cfg.Scaling.NodeReadyTimeout {
				c.log.Error("burst node failed to join within timeout, will clean up", "name", name)
				timedOut = append(timedOut, name)
			}
			continue
		}
		if !bn.ready && isNodeReady(node) {
			bn.ready = true
			now := time.Now()
			bn.readyAt = &now
			if c.metrics != nil {
				c.metrics.NodeReadyDurationSec.Observe(now.Sub(bn.createdAt).Seconds())
			}
			c.log.Info("burst node is ready", "name", name)
		}
		// GPU device-plugin readiness (issue #39). The nvidia.com/gpu-not-ready
		// taint is applied at kubelet registration, but the NVIDIA device plugin
		// registers nvidia.com/gpu only AFTER the node is Ready. So ready nodes
		// are re-checked every tick until the resource is allocatable, then the
		// taint is cleared controller-side (below) — never via the kubelet
		// (NodeRestriction forbids a kubelet from mutating its own Node taints).
		// A node that never carried the taint (CPU burst) is marked cleared on
		// the first pass so it is not re-checked.
		if bn.ready && !bn.gpuTaintCleared {
			switch {
			case !hasGpuNotReadyTaint(node):
				bn.gpuTaintCleared = true
			case gpuAllocatable(node):
				clearGpuTaint = append(clearGpuTaint, name)
			}
		}
	}

	// Remove timed-out nodes from tracking so they don't block future scale-ups.
	stuckNodes := make(map[string]*burstNode)
	for _, name := range timedOut {
		stuckNodes[name] = c.nodes[name]
		delete(c.nodes, name)
	}
	c.mu.Unlock()

	// Clear the GPU device-plugin taint outside the lock, using the controller's
	// own API credentials. On success the node is marked cleared so it is not
	// re-checked; a transient API error leaves it un-cleared and it is retried.
	for _, name := range clearGpuTaint {
		if c.removeGpuNotReadyTaint(context.Background(), name) {
			c.mu.Lock()
			if bn := c.nodes[name]; bn != nil {
				bn.gpuTaintCleared = true
			}
			c.mu.Unlock()
		}
	}

	// Clean up stuck nodes outside the lock.
	for name, bn := range stuckNodes {
		c.log.Info("destroying stuck burst node", "name", name, "backendID", bn.backendID)
		if err := c.backend.DeleteNode(context.Background(), bn.backendID); err != nil {
			c.log.Error("failed to destroy stuck node", "name", name, "error", err)
		}
		if c.costs != nil {
			c.costs.Stop(bn.backendID)
		}
		_ = c.clientset.CoreV1().Nodes().Delete(context.Background(), name, metav1.DeleteOptions{})
		if c.metrics != nil {
			c.metrics.NodesDestroyed.WithLabelValues("stuck").Inc()
		}
	}
}

// scaleDown removes idle burst nodes by stopping them (returning to pool).
func (c *Controller) scaleDown(ctx context.Context) {
	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		c.log.Error("listing pods for scale-down", "error", err)
		return
	}

	c.mu.Lock()
	var toRemove []string
	for name, bn := range c.nodes {
		if !bn.ready {
			continue
		}
		idle := isNodeIdle(name, pods)
		if idle && bn.idleSince == nil {
			now := time.Now()
			bn.idleSince = &now
			// If the node went idle within the cooldown window of becoming ready,
			// it means the unschedulable pods couldn't use it.
			if bn.readyAt != nil && time.Since(*bn.readyAt) < scaleUpCooldown+c.cfg.Scaling.PollInterval {
				c.failedScaleUps++
				if c.metrics != nil {
					c.metrics.FailedScaleUps.Inc()
				}
				c.log.Warn("burst node went idle immediately after ready — pods may exceed node capacity",
					"name", name, "failedAttempts", c.failedScaleUps)
			} else {
				c.log.Info("burst node is idle", "name", name)
			}
		} else if !idle {
			bn.idleSince = nil
		}
		if bn.idleSince != nil && time.Since(*bn.idleSince) > c.cfg.Scaling.ScaleDownDelay {
			toRemove = append(toRemove, name)
		}
	}
	c.mu.Unlock()

	for _, name := range toRemove {
		c.removeNode(ctx, name)
	}
}

// maintainPrewarmPool keeps the fast-start pool at the configured size. It
// only ever runs in suspend mode (config + backend capability): prewarm
// machines are created, boot, join, and are later suspended by
// suspendReadyPrewarmNodes. Without suspend mode it fails closed and touches
// no provider state — there is deliberately no cold-stop fallback, because a
// stopped-but-created pool machine can keep incurring provider costs outside
// cost tracking and the monthly cap.
func (c *Controller) maintainPrewarmPool(ctx context.Context) {
	desired := c.cfg.Scaling.PrewarmPool
	if desired <= 0 {
		return
	}
	if !c.suspendMode {
		// Config validation rejects this combination, but a Controller can be
		// built directly (tests, embedders), so refuse here independently
		// rather than trusting the config gate.
		c.logPrewarmRefusal(desired)
		return
	}

	pooled, err := c.backend.ListPooledNodes(ctx)
	if err != nil {
		c.log.Error("listing pooled nodes for prewarm", "error", err)
		return
	}

	if c.metrics != nil {
		c.metrics.PoolSize.Set(float64(len(pooled)))
	}

	c.maintainSuspendPool(ctx, pooled, desired)
}

// logPrewarmRefusal reports the fail-closed prewarm posture once per process;
// the reconcile loop hits this every poll interval, and the condition can only
// change by restarting with a different config or backend.
func (c *Controller) logPrewarmRefusal(desired int) {
	c.prewarmRefusalOnce.Do(func() {
		c.log.Error("prewarmPool requested without provider-suspend mode; prewarming is disabled",
			"operation", "prewarm_refused", "backend", c.backend.Name(),
			"prewarmPool", desired, "prewarmSuspend", c.cfg.Scaling.PrewarmSuspendEnabled())
	})
}

func isNodeIdle(nodeName string, pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if pod.Spec.NodeName != nodeName {
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if !isDaemonSetPod(pod) {
			return false
		}
	}
	return true
}

func (c *Controller) removeNode(ctx context.Context, nodeName string) {
	c.mu.Lock()
	bn, ok := c.nodes[nodeName]
	if !ok {
		c.mu.Unlock()
		return
	}
	delete(c.nodes, nodeName)
	c.mu.Unlock()

	c.log.Info("scaling down burst node", "name", nodeName)

	// Cordon the node.
	node, err := c.clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err == nil {
		node.Spec.Unschedulable = true
		_, _ = c.clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	}

	// Drain non-DaemonSet pods.
	c.drainNode(ctx, nodeName)

	// Stop the machine (return to pool). Volume stays attached.
	if err := c.backend.StopNode(ctx, bn.backendID); err != nil {
		c.log.Error("failed to stop burst node, destroying instead", "name", nodeName, "error", err)
		_ = c.backend.DeleteNode(ctx, bn.backendID)
		if c.metrics != nil {
			c.metrics.NodesDestroyed.WithLabelValues("idle").Inc()
		}
	} else {
		if c.metrics != nil {
			c.metrics.NodesPaused.Inc()
		}
		c.log.Info("burst node stopped (returned to pool)", "name", nodeName)
	}

	if c.costs != nil {
		runCost := c.costs.Stop(bn.backendID)
		c.log.Info("burst node cost accrued",
			"name", nodeName, "backendID", bn.backendID, "runCostUSD", runCost)
	}

	// Remove the node from K8s.
	if err := c.clientset.CoreV1().Nodes().Delete(ctx, nodeName, metav1.DeleteOptions{}); err != nil {
		c.log.Error("failed to delete K8s node", "name", nodeName, "error", err)
	}

	c.log.Info("burst node removed", "name", nodeName)
}

// drainNode evicts non-DaemonSet pods on the node, respecting PDBs but falling
// back to direct delete after drainTimeout to avoid getting stuck.
func (c *Controller) drainNode(ctx context.Context, nodeName string) {
	pods, err := c.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		c.log.Error("listing pods for drain", "name", nodeName, "error", err)
		return
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if isDaemonSetPod(pod) {
			continue
		}

		deadline := time.Now().Add(drainTimeout)
		evicted := false
		for time.Now().Before(deadline) {
			err := c.clientset.PolicyV1().Evictions(pod.Namespace).Evict(ctx, &policyv1.Eviction{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pod.Name,
					Namespace: pod.Namespace,
				},
			})
			if err == nil || apierrors.IsNotFound(err) {
				evicted = true
				if c.metrics != nil && err == nil {
					c.metrics.PodsEvicted.WithLabelValues("success").Inc()
				}
				break
			}
			if apierrors.IsTooManyRequests(err) {
				c.log.Info("eviction blocked by PDB, retrying", "pod", pod.Name, "namespace", pod.Namespace)
				select {
				case <-ctx.Done():
					return
				case <-time.After(evictionPollInterval):
				}
				continue
			}
			c.log.Error("eviction failed", "pod", pod.Name, "namespace", pod.Namespace, "error", err)
			break
		}

		if !evicted {
			c.log.Warn("drain timeout, falling back to delete", "pod", pod.Name, "namespace", pod.Namespace)
			if delErr := c.clientset.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); delErr == nil {
				if c.metrics != nil {
					c.metrics.PodsEvicted.WithLabelValues("delete-fallback").Inc()
				}
			}
		}
	}
}

// recoverNodes rebuilds the tracked map from existing K8s burst nodes.
func (c *Controller) recoverNodes() {
	nodes, err := c.nodeLister.List(labels.Everything())
	if err != nil {
		c.log.Error("listing nodes for recovery", "error", err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, node := range nodes {
		if node.Labels[LabelBurstNode] != "true" {
			continue
		}
		backendID := node.Annotations[AnnotationMachineID]
		ready := isNodeReady(node)
		if node.Labels[LabelPrewarm] == "true" {
			// Prewarm nodes must never enter c.nodes: updateNodeStatus would
			// reap a suspended (NotReady) snapshot as "failed to join", and
			// scaleDown would stop an in-flight one. A Ready prewarm node is
			// an in-flight machine from a previous run — re-adopt it so it
			// gets suspended; a NotReady one backs a suspended snapshot that
			// ListPooledNodes surfaces (or a dead machine cleanupOrphans reaps).
			if ready && backendID != "" {
				c.prewarming[backendID] = &prewarmingNode{
					backendID: backendID,
					nodeName:  node.Name,
					createdAt: node.CreationTimestamp.Time,
				}
				c.log.Info("recovered in-flight prewarm node", "name", node.Name, "backendID", backendID)
			} else {
				c.log.Info("skipping prewarm node during recovery", "name", node.Name, "ready", ready)
			}
			continue
		}
		c.nodes[node.Name] = &burstNode{
			backendID: backendID,
			nodeName:  node.Name,
			createdAt: node.CreationTimestamp.Time,
			ready:     ready,
		}
		c.log.Info("recovered burst node", "name", node.Name, "ready", ready)
	}
}

// cleanupOrphans removes backend machines and K8s nodes that aren't tracked
// by this controller instance. Handles stale resources from previous runs.
func (c *Controller) cleanupOrphans(ctx context.Context) {
	// Provider creation/start and local tracking are not atomic. Running cleanup
	// while a provision goroutine is between those steps can destroy the machine
	// it just created. Fail closed and retry on the next reconcile instead.
	c.mu.Lock()
	if c.provisioning > 0 {
		inFlight := c.provisioning
		c.mu.Unlock()
		c.log.Info("skipping orphan cleanup while provisioning",
			"operation", "cleanup_orphans", "provisioning", inFlight)
		return
	}

	// Snapshot active state under one lock. In-flight prewarm machines are
	// running but in neither c.nodes nor the pooled listing (they are not yet
	// suspended), so they must be tracked here or cleanup destroys them
	// mid-boot.
	trackedIDs := make(map[string]bool)
	trackedNames := make(map[string]bool)
	for _, bn := range c.nodes {
		if bn.backendID != "" {
			trackedIDs[bn.backendID] = true
		}
		trackedNames[bn.nodeName] = true
	}
	for id, pw := range c.prewarming {
		trackedIDs[id] = true
		trackedNames[pw.nodeName] = true
	}
	c.mu.Unlock()

	// Intentionally stopped cold machines are durable pool inventory, not
	// orphans. Protect only enough to stay under maxNodes; excess owned entries
	// remain untracked and the backend cleanup reaps them. If listing fails, skip
	// destructive cleanup entirely rather than guessing about ownership.
	pooled, err := c.backend.ListPooledNodes(ctx)
	if err != nil {
		c.log.Error("listing pooled nodes before orphan cleanup",
			"operation", "cleanup_orphans", "error", err)
		return
	}
	poolCapacity := c.cfg.Scaling.MaxNodes - len(trackedIDs)
	if poolCapacity < 0 {
		poolCapacity = 0
	}
	protectedPool := 0
	for _, node := range pooled {
		if protectedPool >= poolCapacity || node.BackendID == "" {
			continue
		}
		trackedIDs[node.BackendID] = true
		// Suspended prewarm snapshots keep a live (NotReady) K8s Node object;
		// its name must be protected or the sweep below deletes it.
		if node.Lifecycle == backends.LifecyclePrewarm && node.Name != "" {
			trackedNames[node.Name] = true
		}
		protectedPool++
	}

	c.mu.Lock()
	c.lastOrphanCleanup = time.Now()
	c.mu.Unlock()
	c.log.Info("checking for orphaned resources",
		"operation", "cleanup_orphans", "active", len(trackedNames),
		"pooled", len(pooled), "poolProtected", protectedPool)

	// Clean up orphaned backend machines and volumes.
	destroyed, err := c.backend.CleanupOrphans(ctx, trackedIDs)
	if destroyed > 0 {
		if c.metrics != nil {
			c.metrics.OrphansDestroyed.Add(float64(destroyed))
			c.metrics.NodesDestroyed.WithLabelValues("orphan").Add(float64(destroyed))
		}
		c.log.Info("destroyed orphaned machines",
			"operation", "cleanup_orphans", "count", destroyed)
	}
	if err != nil {
		c.log.Error("backend orphan cleanup completed with errors",
			"operation", "cleanup_orphans", "destroyed", destroyed, "error", err)
	}

	// Remove K8s burst nodes that aren't tracked by this controller.
	nodes, err := c.nodeLister.List(labels.Everything())
	if err != nil {
		return
	}

	for _, node := range nodes {
		if node.Labels[LabelBurstNode] != "true" {
			continue
		}
		if !trackedNames[node.Name] {
			c.log.Info("removing orphaned K8s burst node", "name", node.Name)
			_ = c.clientset.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{})
		}
	}

	c.log.Info("orphan cleanup complete", "tracked", len(trackedIDs))
}

func (c *Controller) generateNodeName() string {
	s, _ := randomHex(4)
	return "ys-burst-" + s
}
