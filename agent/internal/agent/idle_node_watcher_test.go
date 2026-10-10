package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	v1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

const (
	idleGraceForTest         = 10 * time.Minute
	occupancyIntervalForTest = 5 * time.Minute
)

// idlePodOpts builds one pod already assigned to a node. The zero value is the
// thing that keeps a node alive — a Running pod with an ordinary controller —
// so each test only names the way its pod differs from that.
type idlePodOpts struct {
	name      string
	namespace string
	nodeName  string
	phase     corev1.PodPhase
	daemonSet bool
	mirror    bool
	deleting  bool
}

func idlePod(o idlePodOpts) *corev1.Pod {
	if o.namespace == "" {
		o.namespace = "default"
	}
	if o.phase == "" {
		o.phase = corev1.PodRunning
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: o.name, Namespace: o.namespace},
		Spec:       corev1.PodSpec{NodeName: o.nodeName},
		Status:     corev1.PodStatus{Phase: o.phase},
	}
	if o.daemonSet {
		pod.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet", Name: "node-agent"}}
	}
	if o.mirror {
		pod.Annotations = map[string]string{podConfigMirrorAnnotation: "0xdeadbeef"}
	}
	if o.deleting {
		now := metav1.Now()
		pod.DeletionTimestamp = &now
	}
	return pod
}

// scopePodListsByNode makes the fake honour the spec.nodeName field selector the
// watcher lists with.
//
// The fake clientset ignores field selectors entirely, so without this every pod
// in the cluster comes back for every node — and a test with a busy node and an
// idle one would pass for the wrong reason, or fail for one.
func scopePodListsByNode(k8s *fake.Clientset) {
	k8s.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		obj, err := k8s.Tracker().List(
			schema.GroupVersionResource{Version: "v1", Resource: "pods"},
			schema.GroupVersionKind{Version: "v1", Kind: "Pod"},
			action.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		pods, ok := obj.(*corev1.PodList)
		if !ok {
			return true, obj, nil
		}
		want, exact := action.(ktesting.ListAction).GetListRestrictions().Fields.RequiresExactMatch("spec.nodeName")
		if !exact {
			return true, pods, nil
		}
		out := &corev1.PodList{ListMeta: pods.ListMeta}
		for i := range pods.Items {
			if pods.Items[i].Spec.NodeName == want {
				out.Items = append(out.Items, pods.Items[i])
			}
		}
		return true, out, nil
	})
}

// failListing installs a reactor whose failure the test switches on and off
// through the returned pointer. That is how "this connector could not see" is
// staged — the state the watcher must read as unknown rather than as idle.
//
// A switch rather than a fixed error, and rather than a reactor a test removes
// again: reaching into ReactionChain to un-break a client asserts on the fake's
// internals, and a test that does it is one refactor of client-go away from
// passing for no reason.
func failListing(k8s *fake.Clientset, resource string) *error {
	var failure error
	k8s.PrependReactor("list", resource, func(ktesting.Action) (bool, runtime.Object, error) {
		if failure == nil {
			return false, nil, nil
		}
		return true, nil, failure
	})
	return &failure
}

// newIdleWatcher wires a watcher to a fake cluster with the node-scoped pod
// lister in place. Sweeps are driven directly, so SweepInterval is irrelevant.
//
// Occupancy observations are OFF here so these tests see only the teardown
// signal they are about. They are a separate claim on a separate timer and have
// their own tests below, which switch them on explicitly.
func newIdleWatcher(objs ...runtime.Object) (*IdleNodeWatcher, *recordingNodeReporter, *fake.Clientset) {
	k8s := fake.NewSimpleClientset(objs...)
	scopePodListsByNode(k8s)
	rec := &recordingNodeReporter{}
	w := NewIdleNodeWatcher(k8s, rec, testClusterID, discardLogger())
	w.IdleGrace = idleGraceForTest
	w.OccupancyInterval = 0
	return w, rec, k8s
}

// newOccupancyWatcher is newIdleWatcher with the observations switched on and
// the idle grace pushed out of reach, so a sweep's only output is the occupancy
// signal.
func newOccupancyWatcher(objs ...runtime.Object) (*IdleNodeWatcher, *recordingNodeReporter, *fake.Clientset) {
	w, rec, k8s := newIdleWatcher(objs...)
	w.IdleGrace = 24 * time.Hour
	w.OccupancyInterval = occupancyIntervalForTest
	return w, rec, k8s
}

// observations returns the events that carried the occupancy claim.
func observations(rec *recordingNodeReporter) []protocol.NodeEvent {
	var out []protocol.NodeEvent
	for _, ev := range rec.snapshot() {
		if ev.OccupancyObserved {
			out = append(out, ev)
		}
	}
	return out
}

// sweepPast runs the pass that starts the grace window and then one past the
// far end of it. Two passes is the minimum the contract allows: the first can
// only record when idleness was first SEEN, never conclude from it.
func sweepPast(w *IdleNodeWatcher, start time.Time) {
	ctx := context.Background()
	w.sweep(ctx, start)
	w.sweep(ctx, start.Add(idleGraceForTest+time.Minute))
}

// A node running a customer's pod is capacity in use. Reporting it idle is the
// one mistake here that destroys work rather than costing money, so it is the
// first thing pinned.
func TestIdleWatcherDoesNotReportABusyNode(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, _ := newIdleWatcher(node, idlePod(idlePodOpts{name: "inference-0", nodeName: node.Name}))

	sweepPast(w, time.Now())

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("reported %+v for a node running a pod, want nothing", got)
	}
}

// Everything a burst node carries that ISN'T customer work: the DaemonSets on
// every node, the kubelet's own static-manifest mirrors, runs that are over, and
// pods already being deleted. If any of them counted, no node would ever be idle
// and the whole signal would be dead on arrival.
func TestIdleWatcherIgnoresPodsThatAreNotWork(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	tests := []struct {
		name string
		pod  *corev1.Pod
	}{
		{"daemonset", idlePod(idlePodOpts{name: "node-exporter", nodeName: node.Name, daemonSet: true})},
		{"mirror", idlePod(idlePodOpts{name: "static-kube-proxy", nodeName: node.Name, mirror: true})},
		{"succeeded", idlePod(idlePodOpts{name: "batch-done", nodeName: node.Name, phase: corev1.PodSucceeded})},
		{"failed", idlePod(idlePodOpts{name: "batch-lost", nodeName: node.Name, phase: corev1.PodFailed})},
		{"deleting", idlePod(idlePodOpts{name: "draining", nodeName: node.Name, deleting: true})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, rec, _ := newIdleWatcher(node, tc.pod)

			sweepPast(w, time.Now())

			if got := rec.phases(); len(got) != 1 || got[0] != protocol.NodePhaseIdle {
				t.Fatalf("phases = %v, want a single Idle — %s pods must not keep a node alive", got, tc.name)
			}
		})
	}
}

// A Pending pod is work: it is waiting for THIS node's capacity, and reaping
// underneath it would destroy the very thing the burst was provisioned for.
func TestIdleWatcherTreatsAPendingPodAsBusy(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, _ := newIdleWatcher(node, idlePod(idlePodOpts{
		name: "queued", nodeName: node.Name, phase: corev1.PodPending,
	}))

	sweepPast(w, time.Now())

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("reported %+v for a node with an assigned Pending pod, want nothing", got)
	}
}

// The grace period is the whole safety margin, so it is measured, not
// approximated: one tick short reports nothing, and the report only appears once
// the node has been continuously idle for the full window.
func TestIdleWatcherWaitsOutTheFullGrace(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, _ := newIdleWatcher(node)
	start := time.Now()

	w.sweep(context.Background(), start)
	w.sweep(context.Background(), start.Add(idleGraceForTest-time.Second))
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("reported %+v one second inside the grace period, want nothing", got)
	}

	w.sweep(context.Background(), start.Add(idleGraceForTest))
	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("reports = %+v, want exactly one at the end of the grace period", got)
	}
	if got[0].BurstID != "burst_abc123abc123" || got[0].Phase != protocol.NodePhaseIdle {
		t.Fatalf("report = %+v, want burst_abc123abc123 Idle", got[0])
	}
	if got[0].NodeName != node.Name {
		t.Fatalf("report node name = %q, want %q — central checks this against the name it assigned",
			got[0].NodeName, node.Name)
	}
}

// Idleness has to be CONTINUOUS. A node that took work halfway through its
// window starts over, or a burst serving traffic every nine minutes would be
// torn down on the strength of the gaps between requests.
func TestIdleWatcherResetsWhenTheNodeGoesBusy(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newIdleWatcher(node)
	start := time.Now()

	w.sweep(context.Background(), start)

	pod := idlePod(idlePodOpts{name: "inference-0", nodeName: node.Name})
	if _, err := k8s.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	w.sweep(context.Background(), start.Add(idleGraceForTest/2))

	if err := k8s.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	// Past the ORIGINAL window's end. This pass is the one that starts the second
	// window, so it reports nothing — the busy pass in between threw the first
	// window away.
	secondWindowStart := start.Add(idleGraceForTest + time.Second)
	w.sweep(context.Background(), secondWindowStart)
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("reported %+v; the busy pass must have restarted the window", got)
	}

	w.sweep(context.Background(), secondWindowStart.Add(idleGraceForTest))
	if got := rec.phases(); len(got) != 1 || got[0] != protocol.NodePhaseIdle {
		t.Fatalf("phases = %v, want a single Idle once the SECOND window is out", got)
	}
}

// ONE pod list per sweep, whatever the fleet size, with occupancy still
// attributed to the right node.
//
// The per-node field-selected list this replaced looked cheaper and was not: the
// API server scans every pod to serve one, so n burst nodes cost n scans of the
// same data every sweep interval, for the lifetime of the cluster.
func TestIdleWatcherListsPodsOncePerSweep(t *testing.T) {
	busy := burstNode("aaa111aaa111", corev1.ConditionTrue, time.Hour)
	spare := burstNode("bbb222bbb222", corev1.ConditionTrue, time.Hour)
	alsoSpare := burstNode("ccc333ccc333", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newIdleWatcher(busy, spare, alsoSpare,
		idlePod(idlePodOpts{name: "inference-0", nodeName: busy.Name}))

	start := time.Now()
	w.sweep(context.Background(), start)
	k8s.ClearActions()
	w.sweep(context.Background(), start.Add(idleGraceForTest))

	podLists, nodeLists := 0, 0
	for _, a := range k8s.Actions() {
		if a.GetVerb() != "list" {
			continue
		}
		switch a.GetResource().Resource {
		case "pods":
			podLists++
		case "nodes":
			nodeLists++
		}
	}
	if podLists != 1 {
		t.Fatalf("pod lists = %d across 3 burst nodes, want exactly 1 per sweep", podLists)
	}
	if nodeLists != 1 {
		t.Fatalf("node lists = %d, want exactly 1 per sweep", nodeLists)
	}

	got := rec.snapshot()
	if len(got) != 2 {
		t.Fatalf("reports = %+v, want the two spare nodes and not the busy one", got)
	}
	for _, ev := range got {
		if ev.BurstID == "burst_aaa111aaa111" {
			t.Fatalf("reported %+v; the shared pod list must still keep the busy node's occupancy on it", ev)
		}
		if ev.Phase != protocol.NodePhaseIdle {
			t.Fatalf("reports = %+v, want both to be Idle", got)
		}
	}
}

// A pod list that failed says nothing about the node. Reading it as "no pods
// found" is how a connector that lost its RBAC, or hit a throttled API server,
// would ask central to destroy a cluster's worth of live capacity.
func TestIdleWatcherTreatsAPodListErrorAsUnknown(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newIdleWatcher(node)
	fail := failListing(k8s, "pods")
	*fail = errors.New("etcdserver: request timed out")

	sweepPast(w, time.Now())

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("reported %+v while the pod list was failing, want nothing", got)
	}
}

// The same rule one level up, and the reason the window is restarted rather than
// merely not advanced: time spent unable to see the cluster is not time the node
// was observed idle, so it must not count toward the grace.
func TestIdleWatcherRestartsTheWindowAfterAPodListError(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newIdleWatcher(node)
	fail := failListing(k8s, "pods")
	start := time.Now()

	w.sweep(context.Background(), start)
	*fail = errors.New("etcdserver: request timed out")
	w.sweep(context.Background(), start.Add(idleGraceForTest/2))

	// The blind pass sits inside the original window. If it had merely been
	// skipped, this pass would report.
	*fail = nil
	w.sweep(context.Background(), start.Add(idleGraceForTest+time.Second))
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("reported %+v on a window that included a pass this connector could not see", got)
	}
}

// A node list that failed is the same unknown, and it is the one that covers
// every burst at once.
func TestIdleWatcherTreatsANodeListErrorAsUnknown(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newIdleWatcher(node)
	fail := failListing(k8s, "nodes")
	*fail = errors.New("connection refused")

	sweepPast(w, time.Now())

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("reported %+v while the node list was failing, want nothing", got)
	}
}

// A connector restart must not be able to reap on its first pass. This process
// has no evidence about what the node was doing while it was down, and the only
// honest thing to do with no evidence is start counting again.
func TestIdleWatcherRestartBeginsAFreshGrace(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, 24*time.Hour)
	w, rec, k8s := newIdleWatcher(node)
	start := time.Now()
	sweepPast(w, start)
	if got := rec.phases(); len(got) != 1 {
		t.Fatalf("phases = %v, want the first process to report once", got)
	}

	// A new process over the same long-idle cluster: same node, same absence of
	// pods, no memory.
	restarted := NewIdleNodeWatcher(k8s, rec, testClusterID, discardLogger())
	restarted.IdleGrace = idleGraceForTest
	restarted.OccupancyInterval = 0
	restarted.sweep(context.Background(), start.Add(idleGraceForTest*3))
	if got := rec.phases(); len(got) != 1 {
		t.Fatalf("phases = %v, want the restarted process to report NOTHING on its first pass", got)
	}

	restarted.sweep(context.Background(), start.Add(idleGraceForTest*4+time.Second))
	if got := rec.phases(); len(got) != 2 {
		t.Fatalf("phases = %v, want the restarted process to report once its own window is out", got)
	}
}

func TestIdleWatcherReportsOnlyItsClusterNodes(t *testing.T) {
	owned := burstNode("aaa111aaa111", corev1.ConditionTrue, 24*time.Hour)
	foreign := burstNode("bbb222bbb222", corev1.ConditionTrue, 24*time.Hour)
	foreign.Labels[v1.LabelClusterID] = "cl_ffffffffffffffffffffffffffffffff"
	unlabeled := burstNode("ccc333ccc333", corev1.ConditionTrue, 24*time.Hour)
	delete(unlabeled.Labels, v1.LabelClusterID)

	w, rec, _ := newIdleWatcher(foreign, unlabeled, owned)
	sweepPast(w, time.Now())

	events := rec.snapshot()
	if len(events) != 1 || events[0].BurstID != "burst_aaa111aaa111" || events[0].Phase != protocol.NodePhaseIdle {
		t.Fatalf("events = %+v, want only this connector's idle burst", events)
	}
}

// Only Ready nodes. A node that is not answering is a health problem, and one
// NodeWatcher's phases and NodeGC already own — asking for an idle teardown of
// it would race those paths over a node whose pod list cannot be trusted anyway.
func TestIdleWatcherIgnoresNodesThatAreNotReady(t *testing.T) {
	for _, ready := range []corev1.ConditionStatus{corev1.ConditionFalse, corev1.ConditionUnknown, ""} {
		t.Run(string("ready="+ready), func(t *testing.T) {
			node := makeNode(makeNodeOpts{
				name: burstNodeName("abc123abc123"), ready: ready,
				creationTimestamp: time.Now().Add(-24 * time.Hour),
			})
			w, rec, _ := newIdleWatcher(node)

			sweepPast(w, time.Now())

			if got := rec.snapshot(); len(got) != 0 {
				t.Fatalf("reported %+v for a node that is not Ready, want nothing", got)
			}
		})
	}
}

// The customer's own nodes are not central's business and their names yield no
// burst id. An idle worker-01 is a cluster with spare capacity, not a bill.
func TestIdleWatcherIgnoresNodesThatAreNotBursts(t *testing.T) {
	w, rec, _ := newIdleWatcher(makeNode(makeNodeOpts{
		name: "worker-01", ready: corev1.ConditionTrue,
		creationTimestamp: time.Now().Add(-24 * time.Hour),
	}))

	sweepPast(w, time.Now())

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("reported %+v for a customer's own node, want nothing", got)
	}
}

// A node that left mid-window takes its window with it. Whatever comes back
// under that name is a new node as far as this connector's evidence goes.
func TestIdleWatcherForgetsANodeThatDisappears(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newIdleWatcher(node)
	start := time.Now()

	w.sweep(context.Background(), start)
	if err := k8s.CoreV1().Nodes().Delete(context.Background(), node.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	w.sweep(context.Background(), start.Add(idleGraceForTest/2))

	// A fresh object under the same name: whatever comes back is a new node as
	// far as this connector's evidence goes.
	if _, err := k8s.CoreV1().Nodes().Create(context.Background(), node.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatalf("recreate node: %v", err)
	}
	w.sweep(context.Background(), start.Add(idleGraceForTest+time.Second))
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("reported %+v using a window that belonged to a node which had left", got)
	}
}

// The request is re-asserted every pass for as long as the node stays idle. That
// is what lets the connector treat a held idle report as WITHDRAWABLE the moment
// a live phase supersedes it — see Client.ReportNodeEvent. A watcher that
// reported once could not be allowed to do that.
func TestIdleWatcherKeepsAssertingWhileTheNodeStaysIdle(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, _ := newIdleWatcher(node)
	start := time.Now()

	w.sweep(context.Background(), start)
	for i := 1; i <= 3; i++ {
		w.sweep(context.Background(), start.Add(idleGraceForTest+time.Duration(i)*time.Minute))
	}

	got := rec.phases()
	if len(got) != 3 {
		t.Fatalf("phases = %v, want one Idle per pass past the grace", got)
	}
	for _, phase := range got {
		if phase != protocol.NodePhaseIdle {
			t.Fatalf("phases = %v, want every one to be Idle", got)
		}
	}
}

// The withdrawal. An idle request is held by the connector and re-sent until
// central acknowledges it, so one made while central was unreachable is still
// pending when central comes back — by which time KEDA may have scaled the
// Deployment onto this very node.
//
// It goes through the EXPLICIT revoking call, which is the half that matters:
// an ordinary report of the same phase leaves the request standing, so nothing
// but this watcher, having looked at the pods, can take one back.
func TestIdleWatcherWithdrawsAReportedRequestWhenTheNodeGetsBusy(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newIdleWatcher(node)
	start := time.Now()

	sweepPast(w, start)
	if got := rec.phases(); len(got) != 1 || got[0] != protocol.NodePhaseIdle {
		t.Fatalf("phases = %v, want the idle request to have been made first", got)
	}

	pod := idlePod(idlePodOpts{name: "inference-0", nodeName: node.Name})
	if _, err := k8s.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	w.sweep(context.Background(), start.Add(idleGraceForTest+2*time.Minute))

	got := rec.snapshot()
	if len(got) != 2 {
		t.Fatalf("reports = %+v, want the idle request followed by a withdrawal", got)
	}
	if got[1].Phase != protocol.NodePhaseReady {
		t.Fatalf("withdrawal phase = %q, want the node's live phase (Ready)", got[1].Phase)
	}
	if got[1].BurstID != "burst_abc123abc123" || got[1].NodeName != node.Name {
		t.Fatalf("withdrawal = %+v, want it to name the same burst and node as the request", got[1])
	}
	if rec.withdrawn() != 1 {
		t.Fatalf("explicit withdrawals = %d, want 1 — an ordinary report does not revoke a held request",
			rec.withdrawn())
	}
}

// A pod list this connector can no longer read is not evidence the node is
// still spare, so an outstanding request is withdrawn on that too. Losing
// visibility must never leave a teardown request standing that nothing can call
// off.
func TestIdleWatcherWithdrawsAReportedRequestWhenVisibilityIsLost(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newIdleWatcher(node)
	fail := failListing(k8s, "pods")
	start := time.Now()

	sweepPast(w, start)
	if got := rec.phases(); len(got) != 1 {
		t.Fatalf("phases = %v, want the idle request to have been made first", got)
	}

	*fail = errors.New("etcdserver: request timed out")
	w.sweep(context.Background(), start.Add(idleGraceForTest+2*time.Minute))

	if got := rec.phases(); len(got) != 2 || got[1] != protocol.NodePhaseReady {
		t.Fatalf("phases = %v, want the request withdrawn once this connector went blind", got)
	}
}

// A node that vanished needs no withdrawal from here: NodeWatcher reports
// Removed for that burst, and a Removed outranks a held idle request in the
// connector's cache. Sending a live phase for a Node object that is gone would
// be a report about a node this connector can no longer see.
func TestIdleWatcherDoesNotWithdrawForANodeThatIsGone(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newIdleWatcher(node)
	start := time.Now()

	sweepPast(w, start)
	if got := rec.phases(); len(got) != 1 {
		t.Fatalf("phases = %v, want the idle request to have been made first", got)
	}

	if err := k8s.CoreV1().Nodes().Delete(context.Background(), node.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	w.sweep(context.Background(), start.Add(idleGraceForTest+2*time.Minute))

	if got := rec.phases(); len(got) != 1 {
		t.Fatalf("phases = %v, want nothing more once the node is gone", got)
	}
}

// The debt outlives the blind pass. A node list that failed tells this connector
// nothing, so it must not conclude the request was settled — otherwise a burst
// that goes busy after the outage has a teardown request standing behind it that
// nothing will ever withdraw.
func TestIdleWatcherKeepsAnOutstandingRequestAcrossABlindPass(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newIdleWatcher(node)
	nodeFail := failListing(k8s, "nodes")
	start := time.Now()

	sweepPast(w, start)
	if got := rec.phases(); len(got) != 1 {
		t.Fatalf("phases = %v, want the idle request to have been made first", got)
	}

	// A pass that saw nothing at all.
	*nodeFail = errors.New("connection refused")
	w.sweep(context.Background(), start.Add(idleGraceForTest+time.Minute))
	*nodeFail = nil

	// The node is working again by the time visibility comes back.
	pod := idlePod(idlePodOpts{name: "inference-0", nodeName: node.Name})
	if _, err := k8s.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	w.sweep(context.Background(), start.Add(idleGraceForTest+2*time.Minute))

	if got := rec.phases(); len(got) != 2 || got[1] != protocol.NodePhaseReady {
		t.Fatalf("phases = %v, want the request still withdrawable after a blind pass", got)
	}
}

// Both halves of the gate, and each one alone is enough to refuse.
//
// A namespace-scoped connector cannot see the pods that would make a node busy.
// A connector without the visibility grant may not be scoped to anything and
// still have no cluster-wide pod read — which is the SHIPPED default, since the
// chart's RBAC is namespaced with no workloadNamespace set. Inferring the grant
// from the empty scope is exactly the defect: it put the watcher into a loop of
// forbidden lists while central was told nothing at all.
//
// Not "run it carefully" in either case — the pod list obtainable here is not the
// question being asked, and a partial answer is indistinguishable from an idle
// node.
func TestIdleWatcherRefusesToRunWithoutAuthoritativeVisibility(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		authoritative bool
	}{
		{name: "scoped to a namespace", namespace: "ys-cust-team-a", authoritative: true},
		// The shipped chart default: nothing scoped it, and nothing granted it
		// cluster-wide pod reads either.
		{name: "no visibility grant", namespace: "", authoritative: false},
		{name: "neither", namespace: "ys-cust-team-a", authoritative: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := burstNode("abc123abc123", corev1.ConditionTrue, 24*time.Hour)
			w, rec, k8s := newIdleWatcher(node)
			w.WorkloadNamespace = tc.namespace
			w.AuthoritativePodVisibility = tc.authoritative
			w.SweepInterval = time.Millisecond

			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			if err := w.Run(ctx); err != nil {
				t.Fatalf("Run = %v, want it to decline and return nil", err)
			}
			if ctx.Err() != nil {
				t.Fatal("Run blocked instead of declining immediately")
			}
			if got := rec.snapshot(); len(got) != 0 {
				t.Fatalf("a connector with no authoritative pod visibility reported %+v", got)
			}
			for _, a := range k8s.Actions() {
				if a.GetVerb() == "list" {
					t.Fatalf("listed %s; a connector that cannot answer the question must not sweep at all", a.GetResource().Resource)
				}
			}
		})
	}
}

// The occupancy observation is the ONLY thing central's nodeOnly silence ceiling
// may measure from, and this is what it means: this connector read the whole
// cluster's pods, so it could have said the node was idle. It is made for every
// burst node in a readable sweep — a busy one above all, since a busy node is
// precisely the one that must not be reaped for being quiet.
func TestIdleWatcherObservesOccupancyAfterAnAuthoritativePodList(t *testing.T) {
	busy := burstNode("aaa111aaa111", corev1.ConditionTrue, time.Hour)
	spare := burstNode("bbb222bbb222", corev1.ConditionTrue, time.Hour)
	w, rec, _ := newOccupancyWatcher(busy, spare,
		idlePod(idlePodOpts{name: "inference-0", nodeName: busy.Name}))

	w.sweep(context.Background(), time.Now())

	got := observations(rec)
	if len(got) != 2 {
		t.Fatalf("observations = %+v, want one per burst node in the sweep", got)
	}
	for _, ev := range got {
		if !protocol.PersistableNodePhase(ev.Phase) {
			t.Fatalf("observation = %+v, want it to ride a phase central will record", ev)
		}
		if ev.NodeName == "" || ev.BurstID == "" {
			t.Fatalf("observation = %+v, want it to name the burst and node central assigned", ev)
		}
		if ev.ObservedAt == nil || ev.ObservedAt.IsZero() {
			t.Fatalf("observation = %+v, want the source observation timestamp", ev)
		}
	}
}

// A pod list that failed proves nothing about anything, so it must not renew the
// ceiling. This is the whole defect: a connector that has lost cluster-wide pod
// LIST goes on seeing Nodes perfectly well, and reading that as liveness left
// capacity nobody could ever tear down looking permanently observed.
func TestIdleWatcherObservesNoOccupancyWhenThePodListFails(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, k8s := newOccupancyWatcher(node)
	fail := failListing(k8s, "pods")
	*fail = errors.New(`pods is forbidden: User "system:serviceaccount:yscale-system:yscale-agent" cannot list resource "pods" at the cluster scope`)

	w.sweep(context.Background(), time.Now())

	if got := observations(rec); len(got) != 0 {
		t.Fatalf("observed %+v on a pod list this connector could not read", got)
	}
}

// Low-rate, and per burst. The sweep runs every minute; the claim it carries
// changes on the timescale of a connector dying. The first sweep reports at once
// so a restart does not spend an interval looking dead.
func TestIdleWatcherThrottlesOccupancyObservations(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, _ := newOccupancyWatcher(node)
	start := time.Now()

	w.sweep(context.Background(), start)
	if got := observations(rec); len(got) != 1 {
		t.Fatalf("observations = %+v, want the first successful sweep to report immediately", got)
	}

	w.sweep(context.Background(), start.Add(occupancyIntervalForTest-time.Second))
	if got := observations(rec); len(got) != 1 {
		t.Fatalf("observations = %+v, want nothing more inside the interval", got)
	}

	w.sweep(context.Background(), start.Add(occupancyIntervalForTest))
	if got := observations(rec); len(got) != 2 {
		t.Fatalf("observations = %+v, want a second once the interval is out", got)
	}
}

// Zero is off, and off means a nodeOnly burst has a finite ceiling from its
// creation. That is the honest outcome rather than a silent one: with no
// observation, nothing is claiming the teardown signal still works.
func TestIdleWatcherObservesNoOccupancyWhenTheIntervalIsZero(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
	w, rec, _ := newOccupancyWatcher(node)
	w.OccupancyInterval = 0

	w.sweep(context.Background(), time.Now())
	w.sweep(context.Background(), time.Now().Add(time.Hour))

	if got := observations(rec); len(got) != 0 {
		t.Fatalf("observed %+v with observations switched off", got)
	}
}

// A connector that never runs this watcher never makes the claim — which is the
// point, and it is what makes central's ceiling safe to leave switched off for
// those bursts. Its nodeOnly capacity is genuinely unobservable, so an explicit
// budget is the only thing that may end it.
func TestIdleWatcherThatDoesNotRunObservesNoOccupancy(t *testing.T) {
	for _, tc := range []struct {
		name          string
		namespace     string
		authoritative bool
	}{
		{name: "scoped to a namespace", namespace: "ys-cust-team-a", authoritative: true},
		{name: "no visibility grant", namespace: "", authoritative: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := burstNode("abc123abc123", corev1.ConditionTrue, 24*time.Hour)
			w, rec, _ := newOccupancyWatcher(node)
			w.WorkloadNamespace = tc.namespace
			w.AuthoritativePodVisibility = tc.authoritative
			w.SweepInterval = time.Millisecond

			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			if err := w.Run(ctx); err != nil {
				t.Fatalf("Run = %v, want it to decline", err)
			}
			if got := observations(rec); len(got) != 0 {
				t.Fatalf("a connector that must not run observed %+v", got)
			}
		})
	}
}

// The unscoped connector holding an explicit cluster-wide pod grant is the one
// topology where the pod list IS the answer, so that is the one where the watcher
// runs.
func TestIdleWatcherRunsWhenPodVisibilityIsClusterWide(t *testing.T) {
	node := burstNode("abc123abc123", corev1.ConditionTrue, 24*time.Hour)
	w, rec, _ := newIdleWatcher(node)
	w.AuthoritativePodVisibility = true
	w.IdleGrace = time.Millisecond
	w.SweepInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	waitForNodeEvents(t, rec, 1)
	cancel()
	<-done

	if got := rec.phases(); got[0] != protocol.NodePhaseIdle {
		t.Fatalf("phases = %v, want Idle", got)
	}
}
