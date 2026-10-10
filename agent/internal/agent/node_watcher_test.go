package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	v1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// recordingNodeReporter captures what the watcher published, in order. The
// ORDER is part of the contract: a Removed reported ahead of the Ready that
// preceded it would tell central the wrong thing about a live node.
type recordingNodeReporter struct {
	mu     sync.Mutex
	events []protocol.NodeEvent
	// withdrawals counts the reports that came through the revoking call. Which
	// call was used is the contract, not a detail: an ordinary report leaves a
	// held idle request standing, and only this one takes it back.
	withdrawals int
}

func (r *recordingNodeReporter) ReportNodeEvent(ev protocol.NodeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recordingNodeReporter) WithdrawIdleNodeEvent(ev protocol.NodeEvent) {
	r.mu.Lock()
	r.withdrawals++
	r.mu.Unlock()
	r.ReportNodeEvent(ev)
}

func (r *recordingNodeReporter) withdrawn() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.withdrawals
}

func (r *recordingNodeReporter) snapshot() []protocol.NodeEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]protocol.NodeEvent(nil), r.events...)
}

func (r *recordingNodeReporter) phases() []string {
	out := []string{}
	for _, ev := range r.snapshot() {
		out = append(out, ev.Phase)
	}
	return out
}

func newTestNodeWatcher() (*NodeWatcher, *recordingNodeReporter) {
	rec := &recordingNodeReporter{}
	return NewNodeWatcher(fake.NewSimpleClientset(), rec, testClusterID, discardLogger()), rec
}

// burstNode builds a burst-prefixed Node with the given Ready condition, aged
// so the joining grace is unambiguous.
func burstNode(suffix string, ready corev1.ConditionStatus, age time.Duration) *corev1.Node {
	return makeNode(makeNodeOpts{name: burstNodeName(suffix), ready: ready, creationTimestamp: time.Now().Add(-age)})
}

// A connector that restarts must report what it FINDS, not wait for the next
// transition. Without the initial LIST a burst that went Ready while the
// connector was down reads "provisioning" in central until something else
// happens to that node — which, for a node that is simply working, is nothing.
func TestNodeWatcherReportsCurrentStateOnStart(t *testing.T) {
	burst := burstNode("abc123abc123", corev1.ConditionTrue, 10*time.Minute)
	// A customer's own node, in the same LIST. Its lifecycle is not central's
	// business and its name yields no burst id.
	customer := makeNode(makeNodeOpts{name: "worker-01", ready: corev1.ConditionTrue})

	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(fake.NewSimpleClientset(burst, customer), rec, testClusterID, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.runOnce(ctx) }()

	waitForNodeEvents(t, rec, 1)
	cancel()
	<-done

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("reported %+v, want exactly the burst node", got)
	}
	if got[0].BurstID != "burst_abc123abc123" || got[0].Phase != protocol.NodePhaseReady {
		t.Fatalf("initial report = %+v, want burst_abc123abc123 Ready", got[0])
	}
	if got[0].NodeName != burst.Name {
		t.Fatalf("report node name = %q, want %q", got[0].NodeName, burst.Name)
	}
	if got[0].ObservedAt == nil || got[0].ObservedAt.IsZero() {
		t.Fatalf("initial report observation time = %v, want the source observation timestamp", got[0].ObservedAt)
	}
}

func TestNodeWatcherReportsOnlyItsClusterNodes(t *testing.T) {
	owned := burstNode("aaa111aaa111", corev1.ConditionTrue, time.Hour)
	foreign := burstNode("bbb222bbb222", corev1.ConditionTrue, time.Hour)
	foreign.Labels[v1.LabelClusterID] = "cl_ffffffffffffffffffffffffffffffff"
	unlabeled := burstNode("ccc333ccc333", corev1.ConditionTrue, time.Hour)
	delete(unlabeled.Labels, v1.LabelClusterID)

	w, rec := newTestNodeWatcher()
	for _, node := range []*corev1.Node{foreign, unlabeled, owned} {
		w.handleEvent(watch.Event{Type: watch.Deleted, Object: node})
	}

	events := rec.snapshot()
	if len(events) != 1 || events[0].BurstID != "burst_aaa111aaa111" || events[0].Phase != protocol.NodePhaseRemoved {
		t.Fatalf("events = %+v, want only this connector's removed burst", events)
	}
}

// This watcher reports TRANSITIONS and nothing else, and must never claim more
// than that.
//
// It used to re-state an unchanged phase on a timer, and central measured a
// nodeOnly burst's silence ceiling from those re-statements — which meant a
// connector that could see Nodes but not the cluster's pods held the ceiling
// open forever while being unable to ever ask for the teardown. The liveness
// claim moved to IdleNodeWatcher's occupancy observations, which are made only
// after an authoritative pod list. What is left here is health, and health alone.
func TestNodeWatcherReportsNoOccupancyObservation(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(fake.NewSimpleClientset(node), rec, testClusterID, discardLogger())

	w.observe(node, false)
	w.observe(node, false)

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("reports = %+v, want the dedupe to swallow the unchanged repeat", got)
	}
	if got[0].OccupancyObserved {
		t.Fatal("a health report claimed occupancy visibility; that would reset the nodeOnly ceiling for a connector that cannot see pods at all")
	}
}

// Each watch event type maps onto one phase, and the joining grace is what
// keeps an ordinary bring-up from being reported as a node that failed.
func TestNodeWatcherMapsWatchEventsToPhases(t *testing.T) {
	noCondition := makeNode(makeNodeOpts{name: burstNodeName("ccc333ccc333"), creationTimestamp: time.Now()})

	tests := []struct {
		name      string
		event     watch.EventType
		node      *corev1.Node
		wantPhase string
	}{
		{name: "ready is Ready", event: watch.Added, node: burstNode("aaa111aaa111", corev1.ConditionTrue, time.Hour), wantPhase: protocol.NodePhaseReady},
		// A kubelet registers its Node before it can serve anything, so this is
		// what every healthy burst looks like for its first minute.
		{name: "not-ready inside the grace is Joining", event: watch.Added, node: burstNode("bbb222bbb222", corev1.ConditionFalse, time.Minute), wantPhase: protocol.NodePhaseJoining},
		{name: "no Ready condition is Joining", event: watch.Added, node: noCondition, wantPhase: protocol.NodePhaseJoining},
		{name: "not-ready past the grace is NotReady", event: watch.Modified, node: burstNode("ddd444ddd444", corev1.ConditionFalse, time.Hour), wantPhase: protocol.NodePhaseNotReady},
		{name: "unknown past the grace is NotReady", event: watch.Modified, node: burstNode("eee555eee555", corev1.ConditionUnknown, time.Hour), wantPhase: protocol.NodePhaseNotReady},
		// The only terminal phase, and the only one central tears down on.
		{name: "deleted is Removed", event: watch.Deleted, node: burstNode("fff666fff666", corev1.ConditionTrue, time.Hour), wantPhase: protocol.NodePhaseRemoved},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, rec := newTestNodeWatcher()
			w.handleEvent(watch.Event{Type: tc.event, Object: tc.node})
			got := rec.snapshot()
			if len(got) != 1 {
				t.Fatalf("reported %+v, want exactly one event", got)
			}
			if got[0].Phase != tc.wantPhase {
				t.Fatalf("phase = %q, want %q", got[0].Phase, tc.wantPhase)
			}
		})
	}
}

// Nothing that is not a burst node may be reported, and no name that central
// could not have minted may be turned into a burst id.
func TestNodeWatcherIgnoresForeignAndMalformedNodeNames(t *testing.T) {
	names := []string{
		// A customer's own node.
		"worker-01",
		// The prefix with no identifier behind it.
		"ys-burst-",
		// Dots, capitals, underscores and hyphens are not part of a minted id.
		"ys-burst-my.node",
		"ys-burst-ABC123ABC123",
		"ys-burst-abc_123abc12",
		"ys-burst-abc123-shadow",
		// The prefix has to be at the start.
		"not-ys-burst-abc123abc123",
		// Human-readable prefix collision: a customer node that happens to
		// start with the burst prefix but carries a descriptive suffix.
		"ys-burst-customer",
		// Non-hex characters (g-z) after the prefix — valid lowercase
		// alphanumerics but not hex.
		"ys-burst-abcdefghijkl",
		// Correct prefix, wrong length (too short / too long).
		"ys-burst-abc123",
		"ys-burst-abc123abc123ff",
		// Uppercase hex.
		"ys-burst-DEADBEEF0011",
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			w, rec := newTestNodeWatcher()
			node := makeNode(makeNodeOpts{name: name, ready: corev1.ConditionTrue})
			w.handleEvent(watch.Event{Type: watch.Added, Object: node})
			w.handleEvent(watch.Event{Type: watch.Deleted, Object: node})
			if got := rec.snapshot(); len(got) != 0 {
				t.Fatalf("reported %+v for %q, want nothing", got, name)
			}
		})
	}
}

// The derivation is the exact inverse of central's, so a burst node's name and
// the id central acts on cannot drift apart.
func TestBurstIDForNodeName(t *testing.T) {
	const id = "burst_deadbeef0011"
	if got, ok := burstIDForNodeName(expectedBurstNodeName(id)); !ok || got != id {
		t.Fatalf("round trip = %q ok=%v, want %q", got, ok, id)
	}
	if _, ok := burstIDForNodeName(burstNodeName("")); ok {
		t.Fatal("an empty suffix must not yield an id")
	}
	oversized := burstNodeName(strings.Repeat("a", burstNodeNameSuffixLen+1))
	if _, ok := burstIDForNodeName(oversized); ok {
		t.Fatal("an oversized suffix must not yield an id")
	}
	short := burstNodeName(strings.Repeat("a", burstNodeNameSuffixLen-1))
	if _, ok := burstIDForNodeName(short); ok {
		t.Fatal("a short suffix must not yield an id")
	}
}

func TestBurstIDForNodeNameRejectsNonHexAndCollisions(t *testing.T) {
	rejects := []struct {
		name  string
		input string
	}{
		{"non-hex lowercase g", "ys-burst-gggggggggggg"},
		{"non-hex lowercase z", "ys-burst-zzzzzzzzzzzz"},
		{"uppercase hex", "ys-burst-AABBCCDDEEFF"},
		{"human-readable customer", "ys-burst-customer"},
		{"mixed hex and non-hex", "ys-burst-abc123ghijkl"},
	}
	for _, tc := range rejects {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := burstIDForNodeName(tc.input); ok {
				t.Fatalf("burstIDForNodeName(%q) = ok, want rejection", tc.input)
			}
		})
	}
}

// A kubelet refreshes its node status constantly, so Modified fires for a node
// nothing has happened to. Central gains nothing from being told twice that a
// node is still Ready, and a real transition must still get through.
func TestNodeWatcherDeduplicatesIdenticalPhases(t *testing.T) {
	w, rec := newTestNodeWatcher()
	ready := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	broken := burstNode("abc123abc123", corev1.ConditionFalse, time.Hour)

	w.handleEvent(watch.Event{Type: watch.Added, Object: ready})
	w.handleEvent(watch.Event{Type: watch.Modified, Object: ready})
	w.handleEvent(watch.Event{Type: watch.Modified, Object: ready})
	if got := rec.phases(); len(got) != 1 || got[0] != protocol.NodePhaseReady {
		t.Fatalf("phases after three Ready observations = %v, want one Ready", got)
	}

	w.handleEvent(watch.Event{Type: watch.Modified, Object: broken})
	w.handleEvent(watch.Event{Type: watch.Modified, Object: broken})
	if got := rec.phases(); len(got) != 2 || got[1] != protocol.NodePhaseNotReady {
		t.Fatalf("phases after the node stopped answering = %v, want Ready then NotReady", got)
	}

	// Recovery is a third report, not a duplicate of the first.
	w.handleEvent(watch.Event{Type: watch.Modified, Object: ready})
	if got := rec.phases(); len(got) != 3 || got[2] != protocol.NodePhaseReady {
		t.Fatalf("phases after recovery = %v, want a third report", got)
	}
}

// The dedupe map must not become a per-burst tombstone that only grows: a node
// that is gone can produce no further phase, so its entry goes with it.
func TestNodeWatcherDropsDedupeStateOnRemoval(t *testing.T) {
	w, rec := newTestNodeWatcher()
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)

	w.handleEvent(watch.Event{Type: watch.Added, Object: node})
	w.handleEvent(watch.Event{Type: watch.Deleted, Object: node})
	if got := rec.phases(); len(got) != 2 || got[1] != protocol.NodePhaseRemoved {
		t.Fatalf("phases = %v, want Ready then Removed", got)
	}

	w.mu.Lock()
	held := len(w.reported)
	w.mu.Unlock()
	if held != 0 {
		t.Fatalf("watcher still holds %d dedupe entries after the node was removed", held)
	}
}

func waitForNodeEvents(t *testing.T, rec *recordingNodeReporter, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshot()) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d node events; got %+v", n, rec.snapshot())
}

// gpuBurstNode builds a burst node carrying the gpu-not-ready taint and an
// nvidia.com/gpu allocatable quantity. Both are what a real GPU burst looks like
// after the NVIDIA device plugin starts but before the taint is cleared.
func gpuBurstNode(suffix string, tainted bool, gpuAlloc string) *corev1.Node {
	n := burstNode(suffix, corev1.ConditionTrue, time.Hour)
	if tainted {
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{
			Key:    gpuNotReadyTaintKey,
			Value:  "true",
			Effect: corev1.TaintEffectNoSchedule,
		})
	}
	if gpuAlloc != "" {
		n.Status.Allocatable = corev1.ResourceList{
			corev1.ResourceName("nvidia.com/gpu"): resource.MustParse(gpuAlloc),
		}
	}
	return n
}

func TestNodeWatcherRemovesGPUTaintWhenAllocatable(t *testing.T) {
	node := gpuBurstNode("f00100f00100", true, "1")
	k8s := fake.NewSimpleClientset(node)
	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())

	w.observe(node, false)

	after, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasGPUNotReadyTaint(after) {
		t.Fatal("gpu-not-ready taint still present after observe with allocatable GPU")
	}
	if !hasGPUPresentLabel(after) {
		t.Fatal("nvidia.com/gpu.present label not set after observe with allocatable GPU")
	}
}

func TestNodeWatcherRetainsGPUTaintWhenNotAllocatable(t *testing.T) {
	node := gpuBurstNode("f00200f00200", true, "")
	k8s := fake.NewSimpleClientset(node)
	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())

	w.observe(node, false)

	after, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasGPUNotReadyTaint(after) {
		t.Fatal("gpu-not-ready taint removed before nvidia.com/gpu was allocatable")
	}
}

func TestNodeWatcherPreservesUnrelatedTaints(t *testing.T) {
	node := gpuBurstNode("f00300f00300", true, "1")
	node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{
		Key:    "node.cilium.io/agent-not-ready",
		Effect: corev1.TaintEffectNoSchedule,
	})
	k8s := fake.NewSimpleClientset(node)
	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())

	w.observe(node, false)

	after, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasGPUNotReadyTaint(after) {
		t.Fatal("gpu-not-ready taint still present")
	}
	hasCilium := false
	for _, taint := range after.Spec.Taints {
		if taint.Key == "node.cilium.io/agent-not-ready" {
			hasCilium = true
		}
	}
	if !hasCilium {
		t.Fatal("unrelated cilium taint was removed")
	}
	if !hasGPUPresentLabel(after) {
		t.Fatal("nvidia.com/gpu.present label not set during taint removal")
	}
}

func TestNodeWatcherDoesNotTouchNonBurstNodes(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-01"},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{{
				Key:    gpuNotReadyTaintKey,
				Value:  "true",
				Effect: corev1.TaintEffectNoSchedule,
			}},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
		},
	}
	k8s := fake.NewSimpleClientset(node)
	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())

	w.observe(node, false)

	after, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasGPUNotReadyTaint(after) {
		t.Fatal("taint was removed from a non-burst node")
	}
}

func TestNodeWatcherSetsLabelWhenTaintAlreadyAbsent(t *testing.T) {
	node := gpuBurstNode("f00400f00400", false, "1")
	k8s := fake.NewSimpleClientset(node)
	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())

	w.observe(node, false)

	after, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Spec.Taints) != 0 {
		t.Fatalf("unexpected taints after observe: %v", after.Spec.Taints)
	}
	if !hasGPUPresentLabel(after) {
		t.Fatal("nvidia.com/gpu.present label not set even though taint was already absent")
	}
}

func TestNodeWatcherGPUTaintConflictReGetsAndPreservesUnrelatedTaint(t *testing.T) {
	node := gpuBurstNode("f00600f00600", true, "1")
	k8s := fake.NewSimpleClientset(node)

	conflictOnce := true
	k8s.PrependReactor("update", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		if !conflictOnce {
			return false, nil, nil
		}
		conflictOnce = false
		updateAction := action.(ktesting.UpdateAction)
		updatedNode := updateAction.GetObject().(*corev1.Node)

		// Simulate a concurrent taint addition between our Get and Update.
		freshNode := updatedNode.DeepCopy()
		freshNode.Spec.Taints = append(freshNode.Spec.Taints, corev1.Taint{
			Key:    "node.cilium.io/agent-not-ready",
			Effect: corev1.TaintEffectNoSchedule,
		})
		// Re-add the gpu-not-ready taint to simulate the conflict: the
		// object we got has a different resource version than what's in etcd.
		freshNode.Spec.Taints = append(freshNode.Spec.Taints, corev1.Taint{
			Key:    gpuNotReadyTaintKey,
			Value:  "true",
			Effect: corev1.TaintEffectNoSchedule,
		})
		// Update the fake store directly so the next Get returns this.
		k8s.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), freshNode, "")

		return true, nil, apierrors.NewConflict(
			corev1.Resource("nodes"), node.Name, errors.New("the object has been modified"))
	})

	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())
	w.observe(node, false)

	after, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasGPUNotReadyTaint(after) {
		t.Fatal("gpu-not-ready taint still present after conflict retry")
	}
	hasCilium := false
	for _, taint := range after.Spec.Taints {
		if taint.Key == "node.cilium.io/agent-not-ready" {
			hasCilium = true
		}
	}
	if !hasCilium {
		t.Fatal("concurrent cilium taint lost during conflict retry — re-Get did not pick it up")
	}
	if !hasGPUPresentLabel(after) {
		t.Fatal("nvidia.com/gpu.present label not set after conflict retry")
	}
}

func TestNodeWatcherGPUTaintTransientFailureLeavesForNextWatch(t *testing.T) {
	node := gpuBurstNode("f00700f00700", true, "1")
	k8s := fake.NewSimpleClientset(node)

	k8s.PrependReactor("update", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})

	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())
	w.observe(node, false)

	after, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasGPUNotReadyTaint(after) {
		t.Fatal("transient failure should leave gpu-not-ready taint for the next watch event")
	}
	if hasGPUPresentLabel(after) {
		t.Fatal("nvidia.com/gpu.present label should not be set when update failed")
	}
}

func TestNodeWatcherSkipsGPUTaintRemovalOnDelete(t *testing.T) {
	node := gpuBurstNode("f00500f00500", true, "1")
	k8s := fake.NewSimpleClientset(node)
	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())

	w.observe(node, true)

	after, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasGPUNotReadyTaint(after) {
		t.Fatal("GPU taint was removed on a delete event — deletion should skip taint removal")
	}
}

// nodePhaseObservedAt must prefer stable Kubernetes timestamps over the
// connector's wall-clock time so the same event replayed after a reconnect
// carries the same timestamp.
func TestNodePhaseObservedAtPrefersK8sTimestamps(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	transition := time.Date(2026, 8, 21, 11, 50, 0, 0, time.UTC)
	creation := time.Date(2026, 8, 21, 11, 45, 0, 0, time.UTC)

	readyNode := makeNode(makeNodeOpts{
		name: burstNodeName("a10000a10000"), ready: corev1.ConditionTrue,
		lastTransition: transition, creationTimestamp: creation,
	})
	if got := nodePhaseObservedAt(readyNode, false, protocol.NodePhaseReady, defaultNodeJoiningGrace, now); !got.Equal(transition) {
		t.Fatalf("Ready ObservedAt = %v, want LastTransitionTime %v", got, transition)
	}

	notReadyNode := makeNode(makeNodeOpts{
		name: burstNodeName("a20000a20000"), ready: corev1.ConditionFalse,
		lastTransition: transition, creationTimestamp: creation,
	})
	if got := nodePhaseObservedAt(notReadyNode, false, protocol.NodePhaseNotReady, defaultNodeJoiningGrace, now); !got.Equal(transition) {
		t.Fatalf("NotReady ObservedAt = %v, want LastTransitionTime %v", got, transition)
	}

	joiningNode := makeNode(makeNodeOpts{
		name: burstNodeName("a30000a30000"), creationTimestamp: creation,
	})
	if got := nodePhaseObservedAt(joiningNode, false, protocol.NodePhaseJoining, defaultNodeJoiningGrace, now); !got.Equal(creation) {
		t.Fatalf("Joining ObservedAt = %v, want CreationTimestamp %v", got, creation)
	}

	deletedNode := makeNode(makeNodeOpts{
		name: burstNodeName("a40000a40000"), ready: corev1.ConditionTrue,
		lastTransition: transition, creationTimestamp: creation,
	})
	deletionTime := time.Date(2026, 8, 21, 11, 55, 0, 0, time.UTC)
	deletedNode.DeletionTimestamp = &metav1.Time{Time: deletionTime}
	if got := nodePhaseObservedAt(deletedNode, true, protocol.NodePhaseRemoved, defaultNodeJoiningGrace, now); !got.Equal(deletionTime) {
		t.Fatalf("Removed ObservedAt = %v, want DeletionTimestamp %v", got, deletionTime)
	}

	// No condition timestamp still uses the stable node creation time, just
	// after it so the Ready outranks the Joining stamped at creation.
	noTransition := makeNode(makeNodeOpts{
		name: burstNodeName("a50000a50000"), ready: corev1.ConditionTrue,
		creationTimestamp: creation,
	})
	if got := nodePhaseObservedAt(noTransition, false, protocol.NodePhaseReady, defaultNodeJoiningGrace, now); !got.Equal(creation.Add(readyAfterCreation)) {
		t.Fatalf("Ready without LastTransitionTime = %v, want just after CreationTimestamp %v", got, creation)
	}

	// A NotReady transition WITHIN the joining grace window is clamped to the
	// grace boundary so central does not report a node that has not finished
	// joining as degraded at the actual transition time.
	earlyNotReady := makeNode(makeNodeOpts{
		name: burstNodeName("a60000a60000"), ready: corev1.ConditionFalse,
		lastTransition:    creation.Add(2 * time.Minute), // within the 5m grace
		creationTimestamp: creation,
	})
	graceBoundary := creation.Add(defaultNodeJoiningGrace)
	if got := nodePhaseObservedAt(earlyNotReady, false, protocol.NodePhaseNotReady, defaultNodeJoiningGrace, now); !got.Equal(graceBoundary) {
		t.Fatalf("NotReady within grace = %v, want grace boundary %v", got, graceBoundary)
	}
}

// A Ready whose condition time is no later than the node's creation (the same
// whole second, or a burst clock behind the apiserver's) must still be stamped
// after the Joining that carries the CreationTimestamp. Central keeps only a
// strictly newer phase, so an equal or earlier Ready was dropped for good and
// the burst read "joining" while its pod ran — seen live on 2026-10-04.
func TestNodePhaseObservedAtReadyAfterJoining(t *testing.T) {
	now := time.Date(2026, 10, 4, 22, 7, 3, 0, time.UTC)
	creation := time.Date(2026, 10, 4, 22, 7, 2, 0, time.UTC)
	for name, transition := range map[string]time.Time{
		"same second":  creation,
		"clock behind": creation.Add(-3 * time.Second),
	} {
		t.Run(name, func(t *testing.T) {
			node := makeNode(makeNodeOpts{
				name: burstNodeName("a70000a70000"), ready: corev1.ConditionTrue,
				lastTransition: transition, creationTimestamp: creation,
			})
			joining := nodePhaseObservedAt(node, false, protocol.NodePhaseJoining, defaultNodeJoiningGrace, now)
			ready := nodePhaseObservedAt(node, false, protocol.NodePhaseReady, defaultNodeJoiningGrace, now)
			if !ready.After(joining) {
				t.Fatalf("Ready ObservedAt %v is not after Joining %v", ready, joining)
			}
			// Postgres keeps microseconds; the order must survive the round trip.
			if !ready.Truncate(time.Microsecond).After(joining.Truncate(time.Microsecond)) {
				t.Fatalf("Ready %v and Joining %v collapse at microsecond precision", ready, joining)
			}
			if again := nodePhaseObservedAt(node, false, protocol.NodePhaseReady, defaultNodeJoiningGrace, now.Add(time.Hour)); !again.Equal(ready) {
				t.Fatalf("replayed Ready ObservedAt = %v, want the same %v", again, ready)
			}
		})
	}
}

func TestNodeWatcherReportsGPUAllocatable(t *testing.T) {
	w, rec := newTestNodeWatcher()
	node := gpuBurstNode("f00100f00100", false, "1")

	w.handleEvent(watch.Event{Type: watch.Added, Object: node})

	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if !events[0].GPUAllocatable {
		t.Error("GPUAllocatable = false, want true")
	}
	if events[0].GPUAllocatableAt == nil {
		t.Error("GPUAllocatableAt = nil")
	}
}

func TestNodeWatcherNoCPUNodeGPUSignal(t *testing.T) {
	w, rec := newTestNodeWatcher()
	node := burstNode("c00100c00100", corev1.ConditionTrue, time.Hour)

	w.handleEvent(watch.Event{Type: watch.Added, Object: node})

	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].GPUAllocatable {
		t.Error("GPUAllocatable = true on CPU-only node")
	}
	if events[0].GPUAllocatableAt != nil {
		t.Error("GPUAllocatableAt set on CPU-only node")
	}
}

func TestNodeWatcherReReportsOnGPUChange(t *testing.T) {
	w, rec := newTestNodeWatcher()
	noGPU := burstNode("f00200f00200", corev1.ConditionTrue, time.Hour)
	w.handleEvent(watch.Event{Type: watch.Added, Object: noGPU})

	withGPU := gpuBurstNode("f00200f00200", false, "1")
	w.handleEvent(watch.Event{Type: watch.Modified, Object: withGPU})

	events := rec.snapshot()
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2 (GPU change triggers re-report)", len(events))
	}
	if events[0].GPUAllocatable {
		t.Error("first event should not have GPUAllocatable")
	}
	if !events[1].GPUAllocatable {
		t.Error("second event should have GPUAllocatable after GPU became available")
	}
}

func TestNodeWatcherGPUDeletedNodeNotReported(t *testing.T) {
	w, rec := newTestNodeWatcher()
	node := gpuBurstNode("f00300f00300", false, "1")
	w.handleEvent(watch.Event{Type: watch.Added, Object: node})
	w.handleEvent(watch.Event{Type: watch.Deleted, Object: node})

	events := rec.snapshot()
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if events[1].GPUAllocatable {
		t.Error("Removed event should not carry GPUAllocatable")
	}
}

func TestNodeWatcherGPUPresentLabelNotSetBeforeAllocatable(t *testing.T) {
	node := gpuBurstNode("f01000f01000", true, "")
	k8s := fake.NewSimpleClientset(node)
	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())

	w.observe(node, false)

	after, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasGPUPresentLabel(after) {
		t.Fatal("nvidia.com/gpu.present label was set before gpu allocatable was observed")
	}
	if !hasGPUNotReadyTaint(after) {
		t.Fatal("gpu-not-ready taint was removed before allocatable was positive")
	}
}

func TestNodeWatcherGPUMetadataIdempotentWhenFullyReconciled(t *testing.T) {
	node := gpuBurstNode("f01100f01100", false, "1")
	node.Labels = map[string]string{
		v1.LabelClusterID:        testClusterID,
		v1.LabelNvidiaGPUPresent: "true",
	}
	k8s := fake.NewSimpleClientset(node)

	var updates int
	k8s.PrependReactor("update", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		updates++
		return false, nil, nil
	})

	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())
	w.observe(node, false)

	if updates != 0 {
		t.Fatalf("expected zero updates when metadata is already reconciled, got %d", updates)
	}
}

func TestNodeWatcherGPUEventReportedDespiteReconcileFailure(t *testing.T) {
	node := gpuBurstNode("f01200f01200", true, "1")
	k8s := fake.NewSimpleClientset(node)

	k8s.PrependReactor("update", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("api unavailable")
	})

	rec := &recordingNodeReporter{}
	w := NewNodeWatcher(k8s, rec, testClusterID, discardLogger())
	w.observe(node, false)

	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if !events[0].GPUAllocatable {
		t.Error("GPU-ready event not reported despite metadata reconciliation failure")
	}
}
