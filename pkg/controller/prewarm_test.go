package controller

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	listersv1 "k8s.io/client-go/listers/core/v1"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/config"
)

func boolPtr(v bool) *bool { return &v }

type warmTestFixture struct {
	*Controller
	nodeIndexer cache.Indexer
	podIndexer  cache.Indexer
}

func (f *warmTestFixture) addNode(t *testing.T, node *corev1.Node) {
	t.Helper()
	if err := f.nodeIndexer.Add(node); err != nil {
		t.Fatalf("adding node to lister: %v", err)
	}
	_, err := f.clientset.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("adding node to clientset: %v", err)
	}
}

func (f *warmTestFixture) addPod(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	if err := f.podIndexer.Add(pod); err != nil {
		t.Fatalf("adding pod to lister: %v", err)
	}
}

func newWarmTestController(t *testing.T, backend backends.Backend, prewarmPool int, suspend bool) *warmTestFixture {
	t.Helper()
	nodeIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	c := &Controller{
		clientset:  fake.NewSimpleClientset(),
		nodeLister: listersv1.NewNodeLister(nodeIndexer),
		podLister:  listersv1.NewPodLister(podIndexer),
		backend:    backend,
		cfg: &config.Config{
			Join: config.JoinConfig{Mode: config.JoinModeK3s, K3sServerURL: "https://100.64.0.1:6443"},
			Scaling: config.ScalingConfig{
				MaxNodes:         10,
				PrewarmPool:      prewarmPool,
				PrewarmSuspend:   boolPtr(suspend),
				PrewarmMaxAge:    6 * time.Hour,
				NodeReadyTimeout: 2 * time.Minute,
				ScaleDownDelay:   5 * time.Minute,
				NodeResources:    config.NodeResources{CPUMillis: 1000, MemoryMB: 1024},
			},
		},
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		nodes:      make(map[string]*burstNode),
		prewarming: make(map[string]*prewarmingNode),
	}
	c.initPoolIdentity()
	return &warmTestFixture{Controller: c, nodeIndexer: nodeIndexer, podIndexer: podIndexer}
}

func warmTestNode(name string, ready, prewarm bool) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{LabelBurstNode: "true"},
	}}
	if prewarm {
		n.Labels[LabelPrewarm] = "true"
	}
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}}
	return n
}

func warmTestPod(name, nodeName string, daemonSet bool) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if daemonSet {
		p.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet", Name: "ds"}}
	}
	return p
}

func freshSnapshot(c *Controller, name, id string) backends.PooledNode {
	return backends.PooledNode{
		BackendID:  id,
		Name:       name,
		Lifecycle:  backends.LifecyclePrewarm,
		ScopeHash:  c.scopeHash,
		ConfigHash: c.configHash,
		CreatedAt:  time.Now(),
	}
}

func TestInitPoolIdentitySetsScopeOnBackend(t *testing.T) {
	backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
	c := newWarmTestController(t, backend, 1, true)

	if c.scopeHash == "" || c.configHash == "" {
		t.Fatalf("scopeHash/configHash = %q/%q, want both non-empty", c.scopeHash, c.configHash)
	}
	if backend.scope != c.scopeHash {
		t.Fatalf("backend scope = %q, want %q", backend.scope, c.scopeHash)
	}
	if !c.suspendMode {
		t.Fatal("suspendMode = false, want true for warm+scoped backend with prewarmSuspend on")
	}

	again := newWarmTestController(t, &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}, 1, true)
	if again.scopeHash != c.scopeHash || again.configHash != c.configHash {
		t.Fatalf("hashes not stable across restarts: %q/%q vs %q/%q",
			again.scopeHash, again.configHash, c.scopeHash, c.configHash)
	}
}

func TestMaintainPrewarmPoolSuspendMode(t *testing.T) {
	tests := []struct {
		name        string
		desired     int
		maxNodes    int
		activeNodes int
		inflight    int
		snapshots   int
		wantCreates int
	}{
		{name: "creates deficit as running prewarm", desired: 2, maxNodes: 10, wantCreates: 2},
		{name: "in-flight machines count toward pool", desired: 2, maxNodes: 10, inflight: 2, wantCreates: 0},
		{name: "suspended snapshots count toward pool", desired: 2, maxNodes: 10, snapshots: 2, wantCreates: 0},
		{name: "partial deficit tops up", desired: 3, maxNodes: 10, snapshots: 1, inflight: 1, wantCreates: 1},
		{name: "respects maxNodes across active plus pool", desired: 2, maxNodes: 3, activeNodes: 2, wantCreates: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
			c := newWarmTestController(t, backend, tt.desired, true)
			c.cfg.Scaling.MaxNodes = tt.maxNodes
			for i := 0; i < tt.activeNodes; i++ {
				name := c.generateNodeName()
				c.nodes[name] = &burstNode{backendID: "active-" + name, nodeName: name}
			}
			for i := 0; i < tt.inflight; i++ {
				name := c.generateNodeName()
				c.prewarming["pw-"+name] = &prewarmingNode{backendID: "pw-" + name, nodeName: name, createdAt: time.Now()}
			}
			for i := 0; i < tt.snapshots; i++ {
				name := c.generateNodeName()
				backend.pooled = append(backend.pooled, freshSnapshot(c.Controller, name, "snap-"+name))
			}

			c.maintainPrewarmPool(context.Background())

			if len(backend.created) != tt.wantCreates {
				t.Fatalf("created %d machines, want %d", len(backend.created), tt.wantCreates)
			}
			if len(backend.stopped) != 0 {
				t.Fatalf("StopNode called on %v; suspend-mode prewarm machines must stay running", backend.stopped)
			}
			for _, spec := range backend.created {
				if spec.Lifecycle != backends.LifecyclePrewarm {
					t.Errorf("spec %s lifecycle = %q, want prewarm", spec.Name, spec.Lifecycle)
				}
				if spec.ScopeHash != c.scopeHash || spec.ScopeHash == "" {
					t.Errorf("spec %s scopeHash = %q, want %q", spec.Name, spec.ScopeHash, c.scopeHash)
				}
				if spec.ConfigHash != c.configHash || spec.ConfigHash == "" {
					t.Errorf("spec %s configHash = %q, want %q", spec.Name, spec.ConfigHash, c.configHash)
				}
				if spec.NodeLabels[LabelPrewarm] != "true" || spec.NodeLabels[LabelBurstNode] != "true" {
					t.Errorf("spec %s labels = %v, want prewarm+burst labels", spec.Name, spec.NodeLabels)
				}
			}
			if got := len(c.prewarming); got != tt.inflight+tt.wantCreates {
				t.Fatalf("prewarming tracked = %d, want %d", got, tt.inflight+tt.wantCreates)
			}
		})
	}
}

func TestSuspendReadyPrewarmNodes(t *testing.T) {
	cancelled := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}

	t.Run("suspends ready idle prewarm node", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
		c := newWarmTestController(t, backend, 1, true)
		c.addNode(t, warmTestNode("ys-burst-a", true, true))
		c.addPod(t, warmTestPod("ds-pod", "ys-burst-a", true))
		c.prewarming["m-a"] = &prewarmingNode{backendID: "m-a", nodeName: "ys-burst-a", createdAt: time.Now()}

		c.suspendReadyPrewarmNodes(cancelled())

		if len(backend.suspended) != 1 || backend.suspended[0] != "m-a" {
			t.Fatalf("suspended = %v, want [m-a]", backend.suspended)
		}
		if len(backend.waited) != 1 || backend.waited[0] != "m-a:suspended" {
			t.Fatalf("waited = %v, want [m-a:suspended]", backend.waited)
		}
		if len(c.prewarming) != 0 {
			t.Fatalf("prewarming = %v, want empty after suspend", c.prewarming)
		}
		if len(backend.deleted) != 0 {
			t.Fatalf("deleted = %v, want none", backend.deleted)
		}
		nodeAfter, err := c.clientset.CoreV1().Nodes().Get(context.Background(), "ys-burst-a", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get node after: %v", err)
		}
		if !nodeAfter.Spec.Unschedulable {
			t.Fatal("suspended prewarm node not cordoned; a pod could bind before suspend")
		}
	})

	t.Run("pod bound after cordon is promoted and uncordoned", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
		c := newWarmTestController(t, backend, 1, true)
		c.addNode(t, warmTestNode("ys-burst-race", true, true))
		c.prewarming["m-race"] = &prewarmingNode{backendID: "m-race", nodeName: "ys-burst-race", createdAt: time.Now()}

		fakeCS := c.clientset.(*fake.Clientset)
		var bound bool
		fakeCS.PrependReactor("update", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
			if !bound {
				bound = true
				c.addPod(t, warmTestPod("late-workload", "ys-burst-race", false))
			}
			return false, nil, nil
		})

		c.suspendReadyPrewarmNodes(cancelled())

		if len(backend.suspended) != 0 {
			t.Fatalf("suspended = %v, want none — a pod bound after cordon must abort suspend", backend.suspended)
		}
		if _, ok := c.prewarming["m-race"]; ok {
			t.Fatal("raced prewarm node still tracked as prewarming")
		}
		bn, ok := c.nodes["ys-burst-race"]
		if !ok || bn.backendID != "m-race" {
			t.Fatalf("promoted node = %+v (present=%v), want active m-race", bn, ok)
		}
		nodeAfter, err := c.clientset.CoreV1().Nodes().Get(context.Background(), "ys-burst-race", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get node after: %v", err)
		}
		if nodeAfter.Spec.Unschedulable {
			t.Fatal("promoted node still cordoned; an active burst node must be schedulable")
		}
	})

	t.Run("leaves not-ready node alone within timeout", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
		c := newWarmTestController(t, backend, 1, true)
		c.addNode(t, warmTestNode("ys-burst-b", false, true))
		c.prewarming["m-b"] = &prewarmingNode{backendID: "m-b", nodeName: "ys-burst-b", createdAt: time.Now()}

		c.suspendReadyPrewarmNodes(cancelled())

		if len(backend.suspended) != 0 || len(backend.deleted) != 0 {
			t.Fatalf("suspended/deleted = %v/%v, want none", backend.suspended, backend.deleted)
		}
		if _, ok := c.prewarming["m-b"]; !ok {
			t.Fatal("not-ready prewarm entry was dropped, want retained")
		}
	})

	t.Run("promotes node claimed by scheduler instead of suspending", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
		c := newWarmTestController(t, backend, 1, true)
		c.addNode(t, warmTestNode("ys-burst-c", true, true))
		c.addPod(t, warmTestPod("workload", "ys-burst-c", false))
		c.prewarming["m-c"] = &prewarmingNode{backendID: "m-c", nodeName: "ys-burst-c", createdAt: time.Now()}

		c.suspendReadyPrewarmNodes(cancelled())

		if len(backend.suspended) != 0 {
			t.Fatalf("suspended = %v, want none for a claimed node", backend.suspended)
		}
		if len(c.prewarming) != 0 {
			t.Fatal("claimed node still tracked as prewarming")
		}
		bn, ok := c.nodes["ys-burst-c"]
		if !ok || bn.backendID != "m-c" || !bn.ready {
			t.Fatalf("promoted node = %+v (present=%v), want ready active node m-c", bn, ok)
		}
	})

	t.Run("destroys node that never becomes ready in time", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
		c := newWarmTestController(t, backend, 1, true)
		c.prewarming["m-d"] = &prewarmingNode{
			backendID: "m-d", nodeName: "ys-burst-d",
			createdAt: time.Now().Add(-10 * time.Minute),
		}

		c.suspendReadyPrewarmNodes(cancelled())

		if len(backend.deleted) != 1 || backend.deleted[0] != "m-d" {
			t.Fatalf("deleted = %v, want [m-d]", backend.deleted)
		}
		if len(c.prewarming) != 0 {
			t.Fatal("timed-out prewarm entry still tracked")
		}
	})

	t.Run("suspend failure below bound is retried, past bound destroyed", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}, suspendErr: context.DeadlineExceeded}
		c := newWarmTestController(t, backend, 1, true)
		c.addNode(t, warmTestNode("ys-burst-e", true, true))
		c.prewarming["m-e"] = &prewarmingNode{backendID: "m-e", nodeName: "ys-burst-e", createdAt: time.Now()}

		for i := 1; i < maxSuspendFailures; i++ {
			c.suspendReadyPrewarmNodes(cancelled())
			if len(backend.deleted) != 0 {
				t.Fatalf("deleted after %d failures = %v, want retry not destroy", i, backend.deleted)
			}
		}
		c.suspendReadyPrewarmNodes(cancelled())
		if len(backend.deleted) != 1 || backend.deleted[0] != "m-e" {
			t.Fatalf("deleted = %v, want [m-e] after %d failures", backend.deleted, maxSuspendFailures)
		}
		if len(c.prewarming) != 0 {
			t.Fatal("unsuspendable prewarm entry still tracked")
		}
	})
}

func TestStartPooledNodeResumesSuspendedPrewarm(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("resumes suspended snapshot and tracks it", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
		c := newWarmTestController(t, backend, 1, true)
		backend.pooled = []backends.PooledNode{freshSnapshot(c.Controller, "ys-burst-w", "m-w")}

		if !c.startPooledNode(cancelledCtx) {
			t.Fatal("startPooledNode = false, want prewarm snapshot claimed")
		}
		if len(backend.started) != 1 || backend.started[0] != "m-w" {
			t.Fatalf("started = %v, want [m-w]", backend.started)
		}
		if len(backend.waited) != 1 || backend.waited[0] != "m-w:started" {
			t.Fatalf("waited = %v, want [m-w:started]", backend.waited)
		}
		if bn, ok := c.nodes["ys-burst-w"]; !ok || bn.backendID != "m-w" {
			t.Fatalf("c.nodes[ys-burst-w] = %+v (present=%v), want tracked m-w", bn, ok)
		}
	})

	t.Run("prefers prewarm snapshot over cold machine", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
		c := newWarmTestController(t, backend, 1, true)
		backend.pooled = []backends.PooledNode{
			{BackendID: "m-cold", Name: "ys-burst-cold", Lifecycle: backends.LifecycleCold},
			freshSnapshot(c.Controller, "ys-burst-warm", "m-warm"),
		}

		if !c.startPooledNode(cancelledCtx) {
			t.Fatal("startPooledNode = false, want claim")
		}
		if len(backend.started) != 1 || backend.started[0] != "m-warm" {
			t.Fatalf("started = %v, want prewarm [m-warm] preferred over cold", backend.started)
		}
	})

	t.Run("skips stale config-hash snapshot", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
		c := newWarmTestController(t, backend, 1, true)
		stale := freshSnapshot(c.Controller, "ys-burst-s", "m-s")
		stale.ConfigHash = "0000000000000000"
		backend.pooled = []backends.PooledNode{stale}

		if c.startPooledNode(cancelledCtx) {
			t.Fatal("startPooledNode = true, want stale snapshot rejected")
		}
		if len(backend.started) != 0 {
			t.Fatalf("started = %v, want none", backend.started)
		}
	})

	t.Run("failed resume confirmation destroys the machine", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}, waitErr: context.DeadlineExceeded}
		c := newWarmTestController(t, backend, 1, true)
		backend.pooled = []backends.PooledNode{freshSnapshot(c.Controller, "ys-burst-f", "m-f")}

		if c.startPooledNode(cancelledCtx) {
			t.Fatal("startPooledNode = true, want failure on unconfirmed resume")
		}
		if len(backend.deleted) != 1 || backend.deleted[0] != "m-f" {
			t.Fatalf("deleted = %v, want [m-f]", backend.deleted)
		}
		if _, ok := c.nodes["ys-burst-f"]; ok {
			t.Fatal("unconfirmed resume still tracked in c.nodes")
		}
	})

	t.Run("uncordons the resumed snapshot", func(t *testing.T) {
		backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
		c := newWarmTestController(t, backend, 1, true)
		cordoned := warmTestNode("ys-burst-u", false, true)
		cordoned.Spec.Unschedulable = true
		c.addNode(t, cordoned)
		backend.pooled = []backends.PooledNode{freshSnapshot(c.Controller, "ys-burst-u", "m-u")}

		if !c.startPooledNode(cancelledCtx) {
			t.Fatal("startPooledNode = false, want prewarm snapshot claimed")
		}
		nodeAfter, err := c.clientset.CoreV1().Nodes().Get(context.Background(), "ys-burst-u", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get node after: %v", err)
		}
		if nodeAfter.Spec.Unschedulable {
			t.Fatal("resumed prewarm node still cordoned; the claiming workload cannot land")
		}
		if _, ok := c.nodes["ys-burst-u"]; !ok {
			t.Fatal("resumed node not tracked in c.nodes")
		}
	})
}

func TestScaleDownIgnoresSuspendedPrewarm(t *testing.T) {
	backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
	c := newWarmTestController(t, backend, 1, true)
	backend.pooled = []backends.PooledNode{freshSnapshot(c.Controller, "ys-burst-warm", "m-warm")}

	activeNode := warmTestNode("ys-burst-active", true, false)
	c.clientset = fake.NewSimpleClientset(activeNode, warmTestNode("ys-burst-warm", false, true))
	idleSince := time.Now().Add(-10 * time.Minute)
	c.nodes["ys-burst-active"] = &burstNode{
		backendID: "m-active", nodeName: "ys-burst-active",
		ready: true, idleSince: &idleSince,
	}

	c.scaleDown(context.Background())

	if len(backend.stopped) != 1 || backend.stopped[0] != "m-active" {
		t.Fatalf("stopped = %v, want only the active idle node m-active", backend.stopped)
	}
	for _, id := range append(backend.deleted, backend.stopped...) {
		if id == "m-warm" {
			t.Fatal("scaleDown touched the suspended prewarm machine")
		}
	}
	if _, err := c.clientset.CoreV1().Nodes().Get(context.Background(), "ys-burst-warm", metav1.GetOptions{}); err != nil {
		t.Fatalf("suspended prewarm K8s node was removed: %v", err)
	}
}

func TestMaintainPrewarmPoolReapsStaleAndExpired(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*Controller, *backends.PooledNode)
		wantReap bool
	}{
		{name: "fresh snapshot kept", mutate: func(*Controller, *backends.PooledNode) {}, wantReap: false},
		{
			name:     "expired snapshot destroyed",
			mutate:   func(_ *Controller, p *backends.PooledNode) { p.CreatedAt = time.Now().Add(-7 * time.Hour) },
			wantReap: true,
		},
		{
			name:     "stale config-hash snapshot destroyed",
			mutate:   func(_ *Controller, p *backends.PooledNode) { p.ConfigHash = "0000000000000000" },
			wantReap: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
			c := newWarmTestController(t, backend, 1, true)
			snap := freshSnapshot(c.Controller, "ys-burst-old", "m-old")
			tt.mutate(c.Controller, &snap)
			backend.pooled = []backends.PooledNode{snap}
			c.clientset = fake.NewSimpleClientset(warmTestNode("ys-burst-old", false, true))

			c.maintainPrewarmPool(context.Background())

			reaped := len(backend.deleted) == 1 && backend.deleted[0] == "m-old"
			if reaped != tt.wantReap {
				t.Fatalf("deleted = %v, wantReap = %v", backend.deleted, tt.wantReap)
			}
			_, err := c.clientset.CoreV1().Nodes().Get(context.Background(), "ys-burst-old", metav1.GetOptions{})
			if tt.wantReap {
				if !apierrors.IsNotFound(err) {
					t.Fatalf("K8s node after reap: err = %v, want NotFound", err)
				}
				if len(backend.created) != 1 {
					t.Fatalf("created = %d, want 1 replacement after reap", len(backend.created))
				}
			} else {
				if err != nil {
					t.Fatalf("fresh snapshot K8s node missing: %v", err)
				}
				if len(backend.created) != 0 {
					t.Fatalf("created = %d, want 0 when pool is full", len(backend.created))
				}
			}
		})
	}
}

// There is no cold-stop fallback: a stopped-but-created pool machine can keep
// incurring provider costs outside cost tracking and the monthly cap (on Linode it is a
// powered-off, still-charged instance). Config validation rejects these
// combinations, but a Controller built directly must independently refuse to
// touch the provider at all.
func TestMaintainPrewarmPoolFailsClosedWithoutSuspendMode(t *testing.T) {
	tests := []struct {
		name    string
		backend backends.Backend
		suspend bool
	}{
		{
			name:    "explicit prewarmSuspend false on a suspend-capable backend",
			backend: &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}},
			suspend: false,
		},
		{
			name:    "linode-shaped backend without provider suspend",
			backend: &poolTestBackend{name: backends.TypeLinode},
			suspend: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newWarmTestController(t, tt.backend, 2, tt.suspend)
			if c.suspendMode {
				t.Fatal("suspendMode = true, want false")
			}

			c.maintainPrewarmPool(context.Background())
			c.suspendReadyPrewarmNodes(context.Background())

			var cold *poolTestBackend
			switch b := tt.backend.(type) {
			case *warmPoolTestBackend:
				cold = b.poolTestBackend
				if len(b.suspended) != 0 {
					t.Fatalf("suspended = %v, want no provider mutation", b.suspended)
				}
			case *poolTestBackend:
				cold = b
			}
			if len(cold.created) != 0 {
				t.Fatalf("created = %v, want no machines created without suspend mode", cold.created)
			}
			if len(cold.stopped) != 0 {
				t.Fatalf("stopped = %v, want no cold-stop fallback", cold.stopped)
			}
			if len(cold.started) != 0 || len(cold.deleted) != 0 {
				t.Fatalf("started/deleted = %v/%v, want no provider mutation", cold.started, cold.deleted)
			}
			if len(c.prewarming) != 0 {
				t.Fatalf("prewarming = %v, want empty when prewarming is refused", c.prewarming)
			}
		})
	}
}

// The positive control for the fail-closed test above: Fly.io (warm + scoped)
// with prewarmSuspend on still creates running prewarm machines and suspends
// them into pool snapshots.
func TestMaintainPrewarmPoolFlySuspendModeStillPools(t *testing.T) {
	backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
	c := newWarmTestController(t, backend, 1, true)
	if !c.suspendMode {
		t.Fatal("suspendMode = false, want true for flyio with prewarmSuspend enabled")
	}

	c.maintainPrewarmPool(context.Background())

	if len(backend.created) != 1 {
		t.Fatalf("created = %d, want 1 prewarm machine", len(backend.created))
	}
	if backend.created[0].Lifecycle != backends.LifecyclePrewarm {
		t.Fatalf("lifecycle = %q, want prewarm", backend.created[0].Lifecycle)
	}
	if len(backend.stopped) != 0 {
		t.Fatalf("stopped = %v, want prewarm machines left running until suspend", backend.stopped)
	}

	// Drive the machine to Ready+idle so the suspend step turns it into a
	// pool snapshot.
	nodeName := backend.created[0].Name
	c.addNode(t, warmTestNode(nodeName, true, true))

	c.suspendReadyPrewarmNodes(context.Background())

	if len(backend.suspended) != 1 {
		t.Fatalf("suspended = %v, want the prewarm machine suspended", backend.suspended)
	}
	if len(c.prewarming) != 0 {
		t.Fatalf("prewarming = %v, want empty once suspended into the pool", c.prewarming)
	}
}

func TestCleanupOrphansProtectsPrewarm(t *testing.T) {
	backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
	c := newWarmTestController(t, backend, 2, true)
	backend.pooled = []backends.PooledNode{freshSnapshot(c.Controller, "ys-burst-snap", "m-snap")}
	c.prewarming["m-boot"] = &prewarmingNode{backendID: "m-boot", nodeName: "ys-burst-boot", createdAt: time.Now()}
	c.clientset = fake.NewSimpleClientset(
		warmTestNode("ys-burst-snap", false, true),
		warmTestNode("ys-burst-boot", true, true),
	)
	c.addNode(t, warmTestNode("ys-burst-snap", false, true))
	c.addNode(t, warmTestNode("ys-burst-boot", true, true))

	c.cleanupOrphans(context.Background())

	if !backend.cleanupTracked["m-boot"] {
		t.Fatal("in-flight prewarm machine not protected from orphan cleanup")
	}
	if !backend.cleanupTracked["m-snap"] {
		t.Fatal("suspended prewarm snapshot not protected from orphan cleanup")
	}
	for _, name := range []string{"ys-burst-snap", "ys-burst-boot"} {
		if _, err := c.clientset.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{}); err != nil {
			t.Fatalf("prewarm K8s node %s deleted by orphan sweep: %v", name, err)
		}
	}
}

func TestRecoverNodesPrewarmDisposition(t *testing.T) {
	backend := &warmPoolTestBackend{poolTestBackend: &poolTestBackend{}}
	c := newWarmTestController(t, backend, 1, true)

	readyPrewarm := warmTestNode("ys-burst-inflight", true, true)
	readyPrewarm.Annotations = map[string]string{AnnotationMachineID: "m-inflight"}
	suspendedPrewarm := warmTestNode("ys-burst-frozen", false, true)
	suspendedPrewarm.Annotations = map[string]string{AnnotationMachineID: "m-frozen"}
	active := warmTestNode("ys-burst-live", true, false)
	active.Annotations = map[string]string{AnnotationMachineID: "m-live"}
	for _, n := range []*corev1.Node{readyPrewarm, suspendedPrewarm, active} {
		c.addNode(t, n)
	}

	c.recoverNodes()

	if _, ok := c.nodes["ys-burst-live"]; !ok {
		t.Fatal("active burst node not recovered into c.nodes")
	}
	if _, ok := c.nodes["ys-burst-inflight"]; ok {
		t.Fatal("in-flight prewarm node recovered into c.nodes; must go to prewarming")
	}
	if _, ok := c.nodes["ys-burst-frozen"]; ok {
		t.Fatal("suspended prewarm node recovered into c.nodes; updateNodeStatus would destroy it")
	}
	if pw, ok := c.prewarming["m-inflight"]; !ok || pw.nodeName != "ys-burst-inflight" {
		t.Fatalf("prewarming = %+v, want in-flight node re-adopted", c.prewarming)
	}
	if _, ok := c.prewarming["m-frozen"]; ok {
		t.Fatal("suspended snapshot tracked as in-flight; suspend would be retried on a frozen machine")
	}
}
