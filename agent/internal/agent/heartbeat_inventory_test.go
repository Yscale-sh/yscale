package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

type heartbeatObserverHandler struct {
	SilentHandler
	hb  protocol.Heartbeat
	err error
}

func (h heartbeatObserverHandler) ObserveClusterInventory(context.Context) (protocol.Heartbeat, error) {
	return h.hb, h.err
}

func TestHeartbeatUsesObservedInventoryWhenCollectionSucceeds(t *testing.T) {
	k8s := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:   "burst-a",
			Labels: map[string]string{burstNodeLabel: "true"},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "team-a"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
	)
	hb, err := (&RealHandler{K8s: k8s, WorkloadNamespace: "team-a"}).ObserveClusterInventory(context.Background())
	if err != nil {
		t.Fatalf("observe inventory: %v", err)
	}

	if !hb.InventoryObserved || !hb.NodeInventoryObserved || !hb.PodInventoryObserved {
		t.Fatalf("heartbeat inventory flags = combined:%v node:%v pod:%v, want all observed",
			hb.InventoryObserved, hb.NodeInventoryObserved, hb.PodInventoryObserved)
	}
	if hb.NodeCount != 2 || hb.BurstCount != 1 || hb.PendingPods != 1 {
		t.Fatalf("heartbeat = %+v, want 2 nodes, 1 burst node, 1 pending pod in team-a", hb)
	}
}

func TestRealHandlerObserveClusterInventoryKeepsNodesWhenPodsForbidden(t *testing.T) {
	k8s := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:   "burst-a",
			Labels: map[string]string{burstNodeLabel: "true"},
		}},
	)
	k8s.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", errors.New("forbidden"))
	})

	hb, err := (&RealHandler{K8s: k8s}).ObserveClusterInventory(context.Background())
	if err == nil {
		t.Fatal("ObserveClusterInventory returned nil error for forbidden pods")
	}
	if !hb.NodeInventoryObserved || hb.NodeCount != 2 || hb.BurstCount != 1 {
		t.Fatalf("node inventory = observed:%v nodes:%d bursts:%d, want observed 2/1",
			hb.NodeInventoryObserved, hb.NodeCount, hb.BurstCount)
	}
	if hb.PodInventoryObserved || hb.PendingPods != 0 || hb.InventoryObserved {
		t.Fatalf("pod/combined inventory = pod:%v pending:%d combined:%v, want pod unobserved and combined false",
			hb.PodInventoryObserved, hb.PendingPods, hb.InventoryObserved)
	}
}

func TestRealHandlerObserveClusterInventoryCountsAllowedNamespaces(t *testing.T) {
	k8s := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "team-a"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "team-b"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "team-c"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
	)
	hb, err := (&RealHandler{K8s: k8s, PodInventoryNamespaces: []string{"team-a", "team-b"}}).ObserveClusterInventory(context.Background())
	if err != nil {
		t.Fatalf("observe namespaced inventory: %v", err)
	}
	if !hb.PodInventoryObserved || hb.PendingPods != 2 {
		t.Fatalf("pod inventory = observed:%v pending:%d, want two allowed namespaces only", hb.PodInventoryObserved, hb.PendingPods)
	}
}

func TestRealHandlerObserveClusterInventoryUsesPendingSelectorAndNamespace(t *testing.T) {
	k8s := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "team-a"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "team-a"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "team-b"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
	)
	var nodeOptions, podOptions metav1.ListOptions
	var podNamespace string
	k8s.PrependReactor("list", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		nodeOptions = listOptionsFromAction(t, action)
		return false, nil, nil
	})
	k8s.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		podNamespace = action.GetNamespace()
		podOptions = listOptionsFromAction(t, action)
		return true, &corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "team-a"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
		}}, nil
	})

	hb, err := (&RealHandler{K8s: k8s, WorkloadNamespace: "team-a"}).ObserveClusterInventory(context.Background())
	if err != nil {
		t.Fatalf("observe inventory: %v", err)
	}
	if podNamespace != "team-a" {
		t.Fatalf("pod list namespace = %q, want team-a", podNamespace)
	}
	if podOptions.FieldSelector != fields.OneTermEqualSelector("status.phase", string(corev1.PodPending)).String() {
		t.Fatalf("pod field selector = %q, want Pending selector", podOptions.FieldSelector)
	}
	if podOptions.ResourceVersion != "0" || nodeOptions.ResourceVersion != "0" {
		t.Fatalf("resource versions = nodes:%q pods:%q, want both cache reads with rv 0",
			nodeOptions.ResourceVersion, podOptions.ResourceVersion)
	}
	if hb.PendingPods != 1 {
		t.Fatalf("pending pods = %d, want 1", hb.PendingPods)
	}
}

func TestHeartbeatLoopSendsImmediateCachedInventory(t *testing.T) {
	now := time.Now().UTC()
	c := &Client{cfg: Config{HeartbeatInterval: time.Hour}, send: make(chan protocol.Envelope, 1)}
	c.applyInventoryObservation(protocol.Heartbeat{
		NodeInventoryObserved:   true,
		NodeInventoryObservedAt: timePointer(now),
		NodeCount:               2,
		BurstCount:              1,
		PodInventoryObserved:    true,
		PodInventoryObservedAt:  timePointer(now),
		PendingPods:             0,
	}, now)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.heartbeatLoop(ctx)

	select {
	case env := <-c.send:
		if env.Type != protocol.TypeHeartbeat {
			t.Fatalf("env type = %q, want heartbeat", env.Type)
		}
		var hb protocol.Heartbeat
		if err := json.Unmarshal(env.Body, &hb); err != nil {
			t.Fatalf("decode heartbeat: %v", err)
		}
		if !hb.InventoryObserved || hb.NodeCount != 2 || hb.BurstCount != 1 || hb.PendingPods != 0 {
			t.Fatalf("heartbeat = %+v, want immediate cached 2/1/0", hb)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not send immediately")
	}
}

type blockingInventoryHandler struct {
	SilentHandler
	started chan struct{}
}

func (h blockingInventoryHandler) ObserveClusterInventory(ctx context.Context) (protocol.Heartbeat, error) {
	close(h.started)
	<-ctx.Done()
	return protocol.Heartbeat{}, ctx.Err()
}

func TestHeartbeatLivenessDoesNotWaitForInventoryRead(t *testing.T) {
	started := make(chan struct{})
	c := &Client{
		cfg:     Config{HeartbeatInterval: time.Hour},
		handler: blockingInventoryHandler{started: started},
		log:     discardLogger(),
		send:    make(chan protocol.Envelope, 2),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.inventoryRefreshLoop(ctx)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("inventory observer did not start")
	}
	go c.heartbeatLoop(ctx)
	select {
	case env := <-c.send:
		if env.Type != protocol.TypeHeartbeat {
			t.Fatalf("frame type = %q, want heartbeat", env.Type)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("heartbeat waited for blocked inventory read")
	}
}

func TestInventoryOutageWarningIsRateLimitedAndRecoveryResetsIt(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	c := &Client{log: logger}
	failure := errors.New("inventory unavailable")
	c.logInventoryObservationFailure(protocol.Heartbeat{}, failure)
	c.logInventoryObservationFailure(protocol.Heartbeat{}, failure)
	if got := strings.Count(output.String(), "heartbeat inventory unavailable"); got != 1 {
		t.Fatalf("total outage warnings = %d, want one rate-limited warning: %s", got, output.String())
	}
	c.logInventoryObservationFailure(protocol.Heartbeat{NodeInventoryObserved: true}, failure)
	c.logInventoryObservationFailure(protocol.Heartbeat{}, failure)
	if got := strings.Count(output.String(), "heartbeat inventory unavailable"); got != 2 {
		t.Fatalf("warning after partial recovery = %d, want a fresh outage warning: %s", got, output.String())
	}
}

func TestHeartbeatCachePreservesLastGoodAcrossFailedRefresh(t *testing.T) {
	now := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	c := &Client{cfg: Config{HeartbeatInterval: 30 * time.Second}}
	c.applyInventoryObservation(protocol.Heartbeat{
		NodeInventoryObserved: true,
		NodeCount:             2,
		BurstCount:            1,
		PodInventoryObserved:  true,
		PendingPods:           3,
	}, now)
	c.applyInventoryObservation(protocol.Heartbeat{}, now.Add(30*time.Second))

	hb := c.heartbeatAt(now.Add(30 * time.Second))
	if !hb.NodeInventoryObserved || !hb.PodInventoryObserved || hb.NodeCount != 2 || hb.BurstCount != 1 || hb.PendingPods != 3 {
		t.Fatalf("heartbeat = %+v, want last-good cache after one failed refresh", hb)
	}
}

func TestHeartbeatCacheExpiresOnlyFailedScope(t *testing.T) {
	start := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	c := &Client{cfg: Config{HeartbeatInterval: 30 * time.Second}}
	c.applyInventoryObservation(protocol.Heartbeat{
		NodeInventoryObserved: true,
		NodeCount:             2,
		BurstCount:            1,
		PodInventoryObserved:  true,
		PendingPods:           4,
	}, start)
	c.applyInventoryObservation(protocol.Heartbeat{
		PodInventoryObserved: true,
		PendingPods:          0,
	}, start.Add(30*time.Second))

	hb := c.heartbeatAt(start.Add(61 * time.Second))
	if hb.NodeInventoryObserved || hb.NodeCount != 0 || hb.BurstCount != 0 {
		t.Fatalf("node inventory = observed:%v nodes:%d bursts:%d, want expired/unobserved",
			hb.NodeInventoryObserved, hb.NodeCount, hb.BurstCount)
	}
	if !hb.PodInventoryObserved || hb.PendingPods != 0 {
		t.Fatalf("pod inventory = observed:%v pending:%d, want fresh observed zero",
			hb.PodInventoryObserved, hb.PendingPods)
	}
}

func TestHeartbeatCacheStoresObservedZeroValuesAndIgnoresInvalidCounts(t *testing.T) {
	now := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	c := &Client{cfg: Config{HeartbeatInterval: 30 * time.Second}}
	c.applyInventoryObservation(protocol.Heartbeat{
		NodeInventoryObserved: true,
		NodeCount:             0,
		BurstCount:            0,
		PodInventoryObserved:  true,
		PendingPods:           0,
	}, now)
	c.applyInventoryObservation(protocol.Heartbeat{
		NodeInventoryObserved: true,
		NodeCount:             1,
		BurstCount:            2,
		PodInventoryObserved:  true,
		PendingPods:           -1,
	}, now.Add(time.Second))

	hb := c.heartbeatAt(now.Add(time.Second))
	if !hb.NodeInventoryObserved || !hb.PodInventoryObserved || hb.NodeCount != 0 || hb.BurstCount != 0 || hb.PendingPods != 0 {
		t.Fatalf("heartbeat = %+v, want observed zero values preserved after invalid update", hb)
	}
}

func TestHeartbeatCacheKeepsScopeObservationTimesIndependent(t *testing.T) {
	now := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	nodeAt := now.Add(-time.Second)
	c := &Client{cfg: Config{HeartbeatInterval: 30 * time.Second}}
	c.applyInventoryObservation(protocol.Heartbeat{
		NodeInventoryObserved: true, NodeInventoryObservedAt: timePointer(nodeAt), NodeCount: 1,
		PodInventoryObserved: true, PendingPods: 2,
	}, now)
	if !c.inventory.nodeObservedAt.Equal(nodeAt) || !c.inventory.podObservedAt.Equal(now) {
		t.Fatalf("cache times = node:%v pod:%v, want independent %v/%v", c.inventory.nodeObservedAt, c.inventory.podObservedAt, nodeAt, now)
	}
}

func TestHeartbeatStaysLiveWhenInventoryIsUnobserved(t *testing.T) {
	for _, tt := range []struct {
		name    string
		handler Handler
	}{
		{name: "no observer", handler: SilentHandler{}},
		{name: "nil real handler", handler: (*RealHandler)(nil)},
		{name: "observer error", handler: heartbeatObserverHandler{err: errors.New("inventory unavailable")}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{handler: tt.handler, log: discardLogger()}
			c.refreshInventory(context.Background())
			hb := c.heartbeat(context.Background())
			if hb.InventoryObserved || hb.NodeInventoryObserved || hb.PodInventoryObserved ||
				hb.NodeCount != 0 || hb.BurstCount != 0 || hb.PendingPods != 0 {
				t.Fatalf("heartbeat = %+v, want unobserved zero-value heartbeat", hb)
			}
		})
	}
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func listOptionsFromAction(t *testing.T, action ktesting.Action) metav1.ListOptions {
	t.Helper()
	withOptions, ok := action.(interface{ GetListOptions() metav1.ListOptions })
	if !ok {
		t.Fatalf("list action %T does not expose ListOptions", action)
	}
	return withOptions.GetListOptions()
}
