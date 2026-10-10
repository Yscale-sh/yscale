package controller

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// poolTestBackend is the cold-only fake: it deliberately does NOT implement
// backends.WarmCapability or backends.ScopedBackend, so it exercises the
// capability-gated refusal paths. warmPoolTestBackend layers those on.
type poolTestBackend struct {
	// name overrides the reported backend name so a provider without any
	// suspend capability (e.g. linode) can be exercised; empty means flyio.
	name           string
	pooled         []backends.PooledNode
	created        []*backends.NodeSpec
	started        []string
	stopped        []string
	deleted        []string
	startErr       error
	cleanupCalls   int
	cleanupTracked map[string]bool
}

func (b *poolTestBackend) CreateNode(_ context.Context, spec *backends.NodeSpec) (string, error) {
	b.created = append(b.created, spec)
	return fmt.Sprintf("m-%d", len(b.created)), nil
}
func (b *poolTestBackend) StartNode(_ context.Context, id string) error {
	if b.startErr != nil {
		return b.startErr
	}
	b.started = append(b.started, id)
	return nil
}
func (b *poolTestBackend) StopNode(_ context.Context, id string) error {
	b.stopped = append(b.stopped, id)
	return nil
}
func (b *poolTestBackend) DeleteNode(_ context.Context, id string) error {
	b.deleted = append(b.deleted, id)
	return nil
}
func (b *poolTestBackend) GetNodeStatus(context.Context, string) (*backends.NodeStatus, error) {
	return &backends.NodeStatus{}, nil
}
func (b *poolTestBackend) ListPooledNodes(context.Context) ([]backends.PooledNode, error) {
	return b.pooled, nil
}

func (b *poolTestBackend) CleanupOrphans(_ context.Context, tracked map[string]bool) (int, error) {
	b.cleanupCalls++
	b.cleanupTracked = make(map[string]bool, len(tracked))
	for id, owned := range tracked {
		b.cleanupTracked[id] = owned
	}
	return 0, nil
}
func (b *poolTestBackend) Name() string {
	if b.name != "" {
		return b.name
	}
	return backends.TypeFlyIO
}

type warmPoolTestBackend struct {
	*poolTestBackend
	scope      string
	suspended  []string
	waited     []string
	suspendErr error
	waitErr    error
}

func (b *warmPoolTestBackend) SetPoolScope(hash string) { b.scope = hash }

func (b *warmPoolTestBackend) SuspendNode(_ context.Context, id string) error {
	if b.suspendErr != nil {
		return b.suspendErr
	}
	b.suspended = append(b.suspended, id)
	return nil
}

func (b *warmPoolTestBackend) WaitNodeState(_ context.Context, id string, state backends.ProviderNodeState) error {
	if b.waitErr != nil {
		return b.waitErr
	}
	b.waited = append(b.waited, id+":"+string(state))
	return nil
}

func TestStartPooledNodeFailsClosedWhenWarmPoolDisabled(t *testing.T) {
	warm := backends.PooledNode{BackendID: "warm-1", Name: "ys-burst-warm", Lifecycle: backends.LifecyclePrewarm}
	cold := backends.PooledNode{BackendID: "cold-1", Name: "ys-burst-cold", Lifecycle: backends.LifecycleCold}

	t.Run("skips warm and preserves cold fast path", func(t *testing.T) {
		backend := &poolTestBackend{pooled: []backends.PooledNode{warm, cold}}
		controller := newPoolTestController(backend)
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // the fake backend ignores cancellation; annotation exits immediately
		if !controller.startPooledNode(ctx) {
			t.Fatal("expected cold candidate to start")
		}
		if len(backend.started) != 1 || backend.started[0] != cold.BackendID {
			t.Fatalf("started IDs = %v, want only %s", backend.started, cold.BackendID)
		}
	})

	t.Run("never starts a warm-only pool", func(t *testing.T) {
		backend := &poolTestBackend{pooled: []backends.PooledNode{warm}}
		controller := newPoolTestController(backend)
		if controller.startPooledNode(context.Background()) {
			t.Fatal("warm candidate started while prewarmPool=0")
		}
		if len(backend.started) != 0 {
			t.Fatalf("started IDs = %v, want none", backend.started)
		}
	})
}

func TestAnnotatePooledNodePersistsBackendID(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ys-burst-pooled"}})
	controller := newPoolTestController(&poolTestBackend{})
	controller.clientset = client

	controller.annotateNodeWhenReady(context.Background(), "ys-burst-pooled", "machine-123")
	node, err := client.CoreV1().Nodes().Get(context.Background(), "ys-burst-pooled", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get annotated node: %v", err)
	}
	if got := node.Annotations[AnnotationMachineID]; got != "machine-123" {
		t.Fatalf("machine annotation = %q, want machine-123", got)
	}
}

func TestCleanupOrphansProtectsPoolAndProvisioningWindow(t *testing.T) {
	t.Run("pooled machine is tracked", func(t *testing.T) {
		backend := &poolTestBackend{pooled: []backends.PooledNode{{
			BackendID: "pooled-1",
			Name:      "ys-burst-pooled",
		}}}
		controller := newPoolTestController(backend)
		configureCleanupTest(controller, fake.NewSimpleClientset())

		controller.cleanupOrphans(context.Background())
		if backend.cleanupCalls != 1 || !backend.cleanupTracked["pooled-1"] {
			t.Fatalf("cleanup calls/tracked = %d/%v, want pooled-1 protected", backend.cleanupCalls, backend.cleanupTracked)
		}
	})

	t.Run("in-flight provision skips destructive cleanup", func(t *testing.T) {
		backend := &poolTestBackend{}
		controller := newPoolTestController(backend)
		configureCleanupTest(controller, fake.NewSimpleClientset())
		controller.reserveProvision() // must happen before a provision goroutine is launched

		controller.cleanupOrphans(context.Background())
		if backend.cleanupCalls != 0 {
			t.Fatalf("cleanup calls = %d, want zero during provisioning", backend.cleanupCalls)
		}
	})
}

// A cold pool lives only in provider inventory, so a controller restart loses
// every in-memory reference to it. This pins the complete recovery sequence:
// the new controller discovers the stopped machine, protects it during startup
// orphan cleanup, then reuses that same machine instead of creating another.
// Cold machines reach that inventory through scale-down (StopNode), never
// through prewarming: cold-stop prewarming was removed because it creates
// powered-off but still-billing machines outside cost tracking.
func TestColdPoolSurvivesControllerRestartCleanupAndReuse(t *testing.T) {
	ctx := context.Background()
	backend := &poolTestBackend{}
	beforeRestart := newPoolTestController(backend)

	// Scale-down path: a burst machine is created for real work and stopped
	// back into provider inventory when it goes idle.
	nodeName := beforeRestart.generateNodeName()
	pooledID, err := backend.CreateNode(ctx, beforeRestart.buildNodeSpec(nodeName))
	if err != nil {
		t.Fatalf("seeding burst machine: %v", err)
	}
	if err := backend.StopNode(ctx, pooledID); err != nil {
		t.Fatalf("stopping burst machine into the pool: %v", err)
	}
	backend.pooled = []backends.PooledNode{{
		BackendID: pooledID,
		Name:      nodeName,
	}}

	afterRestart := newPoolTestController(backend)
	configureCleanupTest(afterRestart, fake.NewSimpleClientset())
	afterRestart.cleanupOrphans(context.Background())
	if backend.cleanupCalls != 1 || !backend.cleanupTracked[pooledID] {
		t.Fatalf("restart cleanup calls/tracked = %d/%v, want %s protected",
			backend.cleanupCalls, backend.cleanupTracked, pooledID)
	}

	startCtx, cancel := context.WithCancel(context.Background())
	cancel() // annotation polling is outside this provider-inventory contract
	if !afterRestart.startPooledNode(startCtx) {
		t.Fatal("restarted controller did not reuse the surviving pool machine")
	}
	if len(backend.started) != 1 || backend.started[0] != pooledID {
		t.Fatalf("started IDs = %v, want exactly %s", backend.started, pooledID)
	}
	if len(backend.created) != 1 {
		t.Fatalf("created machines = %d, restart reuse must not create another", len(backend.created))
	}
}

func configureCleanupTest(controller *Controller, client kubernetes.Interface) {
	controller.clientset = client
	controller.nodeLister = listersv1.NewNodeLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}))
}

func newPoolTestController(backend backends.Backend) *Controller {
	c := &Controller{
		backend: backend,
		cfg: &config.Config{Scaling: config.ScalingConfig{
			MaxNodes:    10,
			PrewarmPool: 0,
			NodeResources: config.NodeResources{
				CPUMillis: 1000,
				MemoryMB:  1024,
			},
		}},
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		nodes:      make(map[string]*burstNode),
		prewarming: make(map[string]*prewarmingNode),
	}
	c.initPoolIdentity()
	return c
}
