package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/config"
)

const (
	// maxSuspendFailures bounds retries before a running-but-unsuspendable
	// prewarm machine is destroyed rather than left running (and billing)
	// outside the pool.
	maxSuspendFailures = 3

	// providerStateWaitTimeout bounds the WaitNodeState confirmation after a
	// suspend or resume so a reconcile pass cannot hang on the provider.
	providerStateWaitTimeout = 45 * time.Second
)

// prewarmingNode tracks a prewarm machine between CreateNode and the
// SuspendNode that turns it into a discoverable pool snapshot.
type prewarmingNode struct {
	backendID       string
	nodeName        string
	createdAt       time.Time
	suspendFailures int
}

// initPoolIdentity derives the stable pool scope + join-config fingerprint
// and wires them into the backend. scopeHash is only adopted when the backend
// implements ScopedBackend: backends validate spec.ScopeHash against their
// configured scope, so stamping a scope the backend never accepted would
// reject every CreateNode.
func (c *Controller) initPoolIdentity() {
	c.configHash = computeConfigHash(c.cfg)
	if sb, ok := c.backend.(backends.ScopedBackend); ok {
		c.scopeHash = computeScopeHash(c.cfg)
		sb.SetPoolScope(c.scopeHash)
	}
	_, warm := c.backend.(backends.WarmCapability)
	c.suspendMode = c.cfg.Scaling.PrewarmSuspendEnabled() && warm && c.scopeHash != ""
}

// computeScopeHash binds the pool-ownership domain to the cluster's join
// identity. It must be stable across controller restarts and never derived
// from process, hostname, or replica identity, or suspended snapshots become
// orphans after every restart.
func computeScopeHash(cfg *config.Config) string {
	h := sha256.New()
	writeHashField(h, "yscale/pool-scope/v1")
	writeHashField(h, cfg.Join.Mode)
	switch cfg.Join.Mode {
	case config.JoinModeKubelet:
		writeHashField(h, cfg.Join.APIServer)
	default:
		writeHashField(h, cfg.Join.K3sServerURL)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// computeConfigHash fingerprints the config that shapes a burst node's join
// identity, so prewarm snapshots created under an older config are rejected
// instead of resumed. Secrets contribute only presence (TSAuthKey) or a
// one-way digest (K3sToken), never their value.
func computeConfigHash(cfg *config.Config) string {
	h := sha256.New()
	writeHashField(h, "yscale/config-hash/v1")
	writeHashField(h, cfg.AgentImage())
	writeHashField(h, cfg.Join.Mode)
	if cfg.Tailscale.AuthKey != "" {
		writeHashField(h, "ts-authkey:present")
	} else {
		writeHashField(h, "ts-authkey:absent")
	}
	switch cfg.Join.Mode {
	case config.JoinModeKubelet:
		writeHashField(h, cfg.Join.APIServer)
		writeHashField(h, cfg.Join.ClusterCA)
	default:
		writeHashField(h, cfg.Join.K3sServerURL)
		tok := sha256.Sum256([]byte(cfg.Join.K3sToken))
		writeHashField(h, hex.EncodeToString(tok[:]))
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func writeHashField(h io.Writer, field string) {
	_, _ = io.WriteString(h, field)
	_, _ = h.Write([]byte{0})
}

// logPrewarmPosture emits the startup warnings the suspend pool depends on;
// neither condition is detectable at runtime, so operators must be told loudly.
func (c *Controller) logPrewarmPosture() {
	if c.cfg.Scaling.PrewarmPool <= 0 {
		return
	}
	if !c.suspendMode {
		// Fail closed: no cold-stop fallback exists, so the pool simply stays
		// empty rather than accumulating stopped-but-billing machines.
		c.logPrewarmRefusal(c.cfg.Scaling.PrewarmPool)
		return
	}
	c.log.Warn("prewarm-suspend requires a REUSABLE, NON-EPHEMERAL Tailscale auth key; " +
		"an ephemeral key will orphan suspended pool nodes after ~30min.")
	if !isDigestPinnedImage(c.cfg.AgentImage()) {
		c.log.Warn("agent image is not digest-pinned; backends refuse prewarm machine creation without an @sha256 digest",
			"image", c.cfg.AgentImage())
	}
}

// isDigestPinnedImage mirrors the flyio backend's private check so the
// controller can warn at startup instead of failing every reconcile.
func isDigestPinnedImage(image string) bool {
	const marker = "@sha256:"
	i := strings.LastIndex(image, marker)
	if i <= 0 {
		return false
	}
	digest := image[i+len(marker):]
	if len(digest) != 64 {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func (c *Controller) buildPrewarmSpec(nodeName string) *backends.NodeSpec {
	spec := c.buildNodeSpec(nodeName)
	spec.Lifecycle = backends.LifecyclePrewarm
	spec.NodeLabels[LabelPrewarm] = "true"
	return spec
}

// setPrewarmSchedulable cordons (schedulable=false) or uncordons a prewarm
// node's K8s Node object. A prewarm node joins SCHEDULABLE, so while it sits
// Ready in the pool the scheduler can bind a real pod that a subsequent
// SuspendNode would then strand frozen. Cordoning is the proportionate fix
// available today; full race-freedom would require the node to REGISTER
// NoSchedule via kubelet register-with-taints (no such plumbing exists yet).
// A cordoned node stays Ready, so isNodeReady is unaffected.
func (c *Controller) setPrewarmSchedulable(ctx context.Context, name string, schedulable bool) error {
	node, err := c.clientset.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node.Spec.Unschedulable == !schedulable {
		return nil
	}
	node.Spec.Unschedulable = !schedulable
	_, err = c.clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	return err
}

// maintainSuspendPool keeps the suspend-based pool at the desired size:
// suspended snapshots plus in-flight (booting/joining) machines. Stale or
// expired snapshots are destroyed first so they free capacity for
// replacements.
func (c *Controller) maintainSuspendPool(ctx context.Context, pooled []backends.PooledNode, desired int) {
	snapshots, coldPooled := c.reapStaleSnapshots(ctx, pooled)

	c.mu.Lock()
	inflight := len(c.prewarming)
	active := len(c.nodes) + c.provisioning
	c.mu.Unlock()

	effective := snapshots + inflight
	deficit := desired - effective
	if deficit <= 0 {
		return
	}
	available := c.cfg.Scaling.MaxNodes - active - coldPooled - effective
	if available <= 0 {
		return
	}
	if deficit > available {
		deficit = available
	}

	c.log.Info("prewarming pool (suspend mode)",
		"snapshots", snapshots, "inflight", inflight,
		"desired", desired, "creating", deficit)

	for i := 0; i < deficit; i++ {
		nodeName := c.generateNodeName()
		spec := c.buildPrewarmSpec(nodeName)

		if c.cfg.Join.Mode == config.JoinModeKubelet {
			token, err := CreateBootstrapToken(ctx, c.clientset)
			if err != nil {
				c.log.Error("failed to create bootstrap token for prewarm node", "error", err)
				return
			}
			spec.BootstrapToken = token
		}

		backendID, err := c.backend.CreateNode(ctx, spec)
		if err != nil {
			c.log.Error("failed to create prewarm node", "name", nodeName, "error", err)
			continue
		}
		c.mu.Lock()
		c.prewarming[backendID] = &prewarmingNode{
			backendID: backendID,
			nodeName:  nodeName,
			createdAt: time.Now(),
		}
		c.mu.Unlock()
		c.log.Info("prewarm node created, booting toward suspend",
			"operation", "prewarm_create", "name", nodeName, "backendID", backendID)
	}
}

// reapStaleSnapshots destroys suspended prewarm snapshots whose join config
// no longer matches or whose age exceeds PrewarmMaxAge, and returns the
// counts of remaining fresh snapshots and cold-stopped pooled machines. A
// snapshot whose destroy fails still counts as pool inventory so the deficit
// math cannot over-create; it is retried next reconcile.
func (c *Controller) reapStaleSnapshots(ctx context.Context, pooled []backends.PooledNode) (snapshots, coldPooled int) {
	maxAge := c.cfg.Scaling.PrewarmMaxAge
	now := time.Now()
	for i := range pooled {
		p := &pooled[i]
		if p.Lifecycle != backends.LifecyclePrewarm {
			coldPooled++
			continue
		}
		stale := p.ConfigHash != c.configHash
		expired := maxAge > 0 && !p.CreatedAt.IsZero() && now.Sub(p.CreatedAt) > maxAge
		if !stale && !expired {
			snapshots++
			continue
		}
		c.log.Info("destroying prewarm snapshot",
			"operation", "prewarm_reap", "name", p.Name, "backendID", p.BackendID,
			"stale", stale, "expired", expired, "createdAt", p.CreatedAt)
		if err := c.backend.DeleteNode(ctx, p.BackendID); err != nil {
			c.log.Error("failed to destroy prewarm snapshot",
				"operation", "prewarm_reap", "name", p.Name, "backendID", p.BackendID, "error", err)
			snapshots++
			continue
		}
		if p.Name != "" {
			_ = c.clientset.CoreV1().Nodes().Delete(ctx, p.Name, metav1.DeleteOptions{})
		}
	}
	return snapshots, coldPooled
}

// suspendReadyPrewarmNodes walks the in-flight prewarm machines and suspends
// each one whose K8s node is Ready and idle, turning it into a fast-resume
// pool snapshot. Runs after maintainPrewarmPool in the reconcile loop.
func (c *Controller) suspendReadyPrewarmNodes(ctx context.Context) {
	if !c.suspendMode {
		return
	}
	c.mu.Lock()
	entries := make([]*prewarmingNode, 0, len(c.prewarming))
	for _, pw := range c.prewarming {
		entries = append(entries, pw)
	}
	c.mu.Unlock()
	if len(entries) == 0 {
		return
	}

	wc, ok := c.backend.(backends.WarmCapability)
	if !ok {
		return
	}

	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		c.log.Error("listing pods for prewarm suspend", "error", err)
		return
	}

	for _, pw := range entries {
		node, err := c.nodeLister.Get(pw.nodeName)
		if err != nil || !isNodeReady(node) {
			if time.Since(pw.createdAt) > c.cfg.Scaling.NodeReadyTimeout {
				c.log.Error("prewarm node failed to become ready, destroying",
					"operation", "prewarm_timeout", "name", pw.nodeName, "backendID", pw.backendID)
				c.destroyPrewarming(ctx, pw)
			}
			continue
		}
		if !isNodeIdle(pw.nodeName, pods) {
			c.log.Info("prewarm node claimed by scheduler before suspend, promoting to active",
				"operation", "prewarm_promote", "name", pw.nodeName, "backendID", pw.backendID)
			c.promotePrewarming(ctx, pw)
			continue
		}

		// STRAND-race guard: the node joined schedulable, so a pod can bind in
		// the idle-check→suspend window. Cordon first, then re-list pods and
		// re-check idle; a pod that bound after the cordon is promoted (which
		// uncordons) instead of being frozen by SuspendNode below.
		if err := c.setPrewarmSchedulable(ctx, pw.nodeName, false); err != nil {
			c.log.Error("failed to cordon prewarm node before suspend",
				"operation", "prewarm_cordon", "name", pw.nodeName, "backendID", pw.backendID, "error", err)
			continue
		}
		freshPods, err := c.podLister.List(labels.Everything())
		if err != nil {
			c.log.Error("re-listing pods before prewarm suspend", "error", err)
			continue
		}
		if !isNodeIdle(pw.nodeName, freshPods) {
			c.log.Info("prewarm node claimed by scheduler after cordon, promoting to active",
				"operation", "prewarm_promote", "name", pw.nodeName, "backendID", pw.backendID)
			c.promotePrewarming(ctx, pw)
			continue
		}

		suspendCtx, cancel := context.WithTimeout(ctx, providerStateWaitTimeout)
		err = wc.SuspendNode(suspendCtx, pw.backendID)
		if err == nil {
			err = wc.WaitNodeState(suspendCtx, pw.backendID, backends.ProviderStateSuspended)
		}
		cancel()
		if err != nil {
			c.mu.Lock()
			pw.suspendFailures++
			failures := pw.suspendFailures
			c.mu.Unlock()
			c.log.Error("failed to suspend prewarm node",
				"operation", "prewarm_suspend", "name", pw.nodeName,
				"backendID", pw.backendID, "attempt", failures, "error", err)
			if failures >= maxSuspendFailures {
				c.log.Error("prewarm node repeatedly failed to suspend, destroying to avoid an unpooled running machine",
					"operation", "prewarm_suspend", "name", pw.nodeName, "backendID", pw.backendID)
				c.destroyPrewarming(ctx, pw)
			}
			continue
		}

		c.mu.Lock()
		delete(c.prewarming, pw.backendID)
		c.mu.Unlock()
		c.log.Info("prewarm node suspended, fast-resume snapshot ready",
			"operation", "prewarm_suspend", "name", pw.nodeName, "backendID", pw.backendID)
	}
}

func (c *Controller) destroyPrewarming(ctx context.Context, pw *prewarmingNode) {
	c.mu.Lock()
	delete(c.prewarming, pw.backendID)
	c.mu.Unlock()
	if err := c.backend.DeleteNode(ctx, pw.backendID); err != nil {
		c.log.Error("failed to destroy prewarm machine",
			"operation", "prewarm_destroy", "name", pw.nodeName, "backendID", pw.backendID, "error", err)
	}
	_ = c.clientset.CoreV1().Nodes().Delete(ctx, pw.nodeName, metav1.DeleteOptions{})
	if c.metrics != nil {
		c.metrics.NodesDestroyed.WithLabelValues("prewarm").Inc()
	}
}

// promotePrewarming moves a prewarm machine that received real pods into
// c.nodes so the normal ready/idle/scale-down lifecycle owns it from here.
func (c *Controller) promotePrewarming(ctx context.Context, pw *prewarmingNode) {
	now := time.Now()
	c.mu.Lock()
	delete(c.prewarming, pw.backendID)
	c.nodes[pw.nodeName] = &burstNode{
		backendID: pw.backendID,
		nodeName:  pw.nodeName,
		createdAt: pw.createdAt,
		ready:     true,
		readyAt:   &now,
	}
	c.mu.Unlock()

	if err := c.setPrewarmSchedulable(ctx, pw.nodeName, true); err != nil {
		c.log.Error("failed to uncordon promoted prewarm node",
			"operation", "prewarm_promote", "name", pw.nodeName, "backendID", pw.backendID, "error", err)
	}

	if c.costs != nil {
		c.costs.Start(
			pw.backendID,
			c.backend.Name(),
			c.cfg.Scaling.NodeResources.CPUMillis,
			c.cfg.Scaling.NodeResources.MemoryMB,
			"",
		)
	}
	go c.annotateNodeWhenReady(ctx, pw.nodeName, pw.backendID)
}

// prewarmClaimBlocked reports why a prewarm pool candidate must not be
// claimed; empty means claimable.
func (c *Controller) prewarmClaimBlocked(p *backends.PooledNode) string {
	if c.cfg.Scaling.PrewarmPool <= 0 {
		return "warm pool disabled"
	}
	if !c.suspendMode {
		return "suspend mode inactive"
	}
	if p.ConfigHash != c.configHash {
		return "stale config hash"
	}
	return ""
}

// confirmPrewarmResume verifies the provider reached "started" after a
// resume. Provider-state success still only proves the VM is up; Kubernetes
// readiness is verified by updateNodeStatus before pods land. On failure the
// machine and its K8s node are destroyed so the caller can fall back to a
// cold create.
func (c *Controller) confirmPrewarmResume(ctx context.Context, node *backends.PooledNode) bool {
	wc, ok := c.backend.(backends.WarmCapability)
	if !ok {
		return true
	}
	waitCtx, cancel := context.WithTimeout(ctx, providerStateWaitTimeout)
	err := wc.WaitNodeState(waitCtx, node.BackendID, backends.ProviderStateStarted)
	cancel()
	if err == nil {
		return true
	}
	c.log.Error("resumed prewarm machine never confirmed started, destroying",
		"operation", "pool_start", "name", node.Name, "backendID", node.BackendID, "error", err)
	// A DeleteNode failure here leaves a running untracked machine; the
	// backend's CleanupOrphans force-destroys it on the next sweep (bounded,
	// self-healing), so no additional recovery is needed at this call site.
	if delErr := c.backend.DeleteNode(ctx, node.BackendID); delErr != nil {
		c.log.Error("failed to destroy unresumable prewarm machine",
			"operation", "pool_start", "name", node.Name, "backendID", node.BackendID, "error", delErr)
	}
	if node.Name != "" {
		_ = c.clientset.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{})
	}
	return false
}
