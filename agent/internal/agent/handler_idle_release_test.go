package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

// nodeRV reads the version the cluster currently holds for the preflight's node.
// The release's whole ownership argument is a comparison against this, so tests
// read it rather than assuming what the fake does with resourceVersions.
func nodeRV(t *testing.T, k8s *fake.Clientset) string {
	t.Helper()
	node, err := k8s.CoreV1().Nodes().Get(context.Background(), preflightNodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	return node.ResourceVersion
}

// setNodeVersion stages another writer having changed the node since — written
// through the tracker for the reason the preflight tests give: a reactor runs
// while the fake holds its own lock, so a nested client call deadlocks.
func setNodeVersion(t *testing.T, k8s *fake.Clientset, rv string) {
	t.Helper()
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "nodes"}
	obj, err := k8s.Tracker().Get(gvr, "", preflightNodeName)
	if err != nil {
		t.Fatalf("get node from tracker: %v", err)
	}
	node := obj.(*corev1.Node)
	node.ResourceVersion = rv
	if err := k8s.Tracker().Update(gvr, node, ""); err != nil {
		t.Fatalf("update node in tracker: %v", err)
	}
}

func nodeWrites(k8s *fake.Clientset) int {
	n := 0
	for _, a := range k8s.Actions() {
		if a.GetResource().Resource != "nodes" {
			continue
		}
		if a.GetVerb() == "update" || a.GetVerb() == "patch" {
			n++
		}
	}
	return n
}

// The cordon a successful preflight leaves behind outlives the command that
// placed it, so the acknowledgement has to carry the one fact that makes it
// recoverable: the version that cordon produced. Without it central can only
// name a node, and naming a node is not enough to know whose cordon it is.
func TestPrepareIdleTeardownHandsBackTheVersionOfTheCordonItPlaced(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(false))

	got, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand())
	if err != nil {
		t.Fatalf("preflight on an empty node = %v, want approval", err)
	}
	if !cordoned(t, k8s) {
		t.Fatal("the confirmed-empty node was not left cordoned")
	}
	if got.CordonResourceVersion == "" {
		t.Fatal("the preflight cordoned the node and handed back no version; central could never release it")
	}
	if want := nodeRV(t, k8s); got.CordonResourceVersion != want {
		t.Fatalf("cordon_resource_version = %q, want the node's own version %q", got.CordonResourceVersion, want)
	}
}

// A node that was ALREADY unschedulable was taken out of service by something
// else, and the preflight is entitled to confirm it is empty without claiming
// the cordon. An empty version is how it says so, and it is what stops central
// asking for an uncordon it has no right to.
func TestPrepareIdleTeardownHandsBackNothingWhenItPlacedNoCordon(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(true))

	got, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand())
	if err != nil {
		t.Fatalf("preflight on an empty, already-cordoned node = %v, want approval", err)
	}
	if got.CordonResourceVersion != "" {
		t.Fatalf("cordon_resource_version = %q, want empty — this preflight cordoned nothing", got.CordonResourceVersion)
	}
	if nodeWrites(k8s) != 0 {
		t.Fatal("the preflight wrote to a node that was already unschedulable")
	}
}

// The recovery, end to end from the connector's side: central failed to reap
// behind a successful preflight, hands back the version that preflight cordoned
// at, and the node goes back into service. Nothing else about it changes.
func TestReleaseIdleTeardownUncordonsTheCordonItOwns(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(false))
	preflight, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand())
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}

	if err := h.OnReleaseIdleTeardown(context.Background(), protocol.ReleaseIdleTeardown{
		NodeName:              preflightNodeName,
		CordonResourceVersion: preflight.CordonResourceVersion,
	}); err != nil {
		t.Fatalf("release = %v, want the cordon given back", err)
	}
	if cordoned(t, k8s) {
		t.Fatal("the node is still unschedulable after its teardown failed; it refuses work and bills for it")
	}
}

// The ownership rule, and the reason the version is carried at all. Between the
// preflight and the release, another drain — an operator, the autoscaler, a
// teardown already in flight — can take the node out of service for reasons of
// its own. Reversing that would put pods back onto a node somebody is draining,
// so the release fails instead and says the node is still cordoned.
func TestReleaseIdleTeardownLeavesACordonSomebodyElseOwns(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(false))
	preflight, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand())
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	setNodeVersion(t, k8s, "99")

	err = h.OnReleaseIdleTeardown(context.Background(), protocol.ReleaseIdleTeardown{
		NodeName:              preflightNodeName,
		CordonResourceVersion: preflight.CordonResourceVersion,
	})
	if err == nil {
		t.Fatal("the release reported success on a node it did not uncordon")
	}
	if !cordoned(t, k8s) {
		t.Fatal("the release uncordoned a node another writer had claimed since the preflight cordoned it")
	}
}

// A node cordoned by somebody else entirely — the release names a version that
// was never this connector's to begin with. Same answer, and it is the answer
// that matters most: no write.
func TestReleaseIdleTeardownNeverUncordonsANodeItDoesNotOwn(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(true))

	err := h.OnReleaseIdleTeardown(context.Background(), protocol.ReleaseIdleTeardown{
		NodeName:              preflightNodeName,
		CordonResourceVersion: "some-other-version",
	})
	if err == nil {
		t.Fatal("the release reported success for a cordon it does not own")
	}
	if !cordoned(t, k8s) {
		t.Fatal("the release uncordoned a node an operator had taken out of service")
	}
}

// Idempotent, because central retries and because a release can race the very
// teardown it was sent for. The second one finds the node already schedulable
// and says so — succeeding without writing, rather than reporting a failure that
// would send an operator looking at a node that is fine.
func TestReleaseIdleTeardownIsIdempotent(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(false))
	preflight, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand())
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	cmd := protocol.ReleaseIdleTeardown{
		NodeName:              preflightNodeName,
		CordonResourceVersion: preflight.CordonResourceVersion,
	}

	if err := h.OnReleaseIdleTeardown(context.Background(), cmd); err != nil {
		t.Fatalf("first release = %v, want the cordon given back", err)
	}
	if err := h.OnReleaseIdleTeardown(context.Background(), cmd); err != nil {
		t.Fatalf("second release = %v, want it satisfied by a node that is already schedulable", err)
	}
	if cordoned(t, k8s) {
		t.Fatal("the node ended up cordoned again")
	}
}

// The node is already gone: the teardown central could not confirm actually
// happened, or something else destroyed it. Nothing is stranded, so nothing is
// wrong.
func TestReleaseIdleTeardownIsSatisfiedByAMissingNode(t *testing.T) {
	h, _ := newPreflightHandler()

	if err := h.OnReleaseIdleTeardown(context.Background(), protocol.ReleaseIdleTeardown{
		NodeName:              preflightNodeName,
		CordonResourceVersion: "1",
	}); err != nil {
		t.Fatalf("release on a node that no longer exists = %v, want it satisfied", err)
	}
}

// Without the version there is no way to tell this request's cordon from an
// operator's. Refused before anything is read or written — an uncordon that
// guesses is the one mistake here that puts work back onto a node somebody
// deliberately emptied.
func TestReleaseIdleTeardownRefusesWithoutTheCordonVersion(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(true))

	err := h.OnReleaseIdleTeardown(context.Background(), protocol.ReleaseIdleTeardown{NodeName: preflightNodeName})
	if err == nil || !strings.Contains(err.Error(), preflightNodeName) {
		t.Fatalf("release = %v, want a refusal naming the node", err)
	}
	if len(k8s.Actions()) != 0 {
		t.Fatalf("a release with no cordon version issued %d API calls, want none", len(k8s.Actions()))
	}
}

// An API that cannot be read is not a released cordon. The node may still be
// unschedulable, so the answer is no — central logs a node an operator has to
// look at rather than recording a give-back that never happened.
func TestReleaseIdleTeardownReportsAFailureItCouldNotResolve(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(false))
	preflight, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand())
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	k8s.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("etcdserver: request timed out")
	})

	if err := h.OnReleaseIdleTeardown(context.Background(), protocol.ReleaseIdleTeardown{
		NodeName:              preflightNodeName,
		CordonResourceVersion: preflight.CordonResourceVersion,
	}); err == nil {
		t.Fatal("the release reported success on a patch it could not land")
	}
	if !cordoned(t, k8s) {
		t.Fatal("the node was uncordoned by a release that failed")
	}
}

// The release is a give-back, never a teardown. It must not evict, delete or
// remove anything — the node it is handing back is one a customer is about to be
// able to use again.
func TestReleaseIdleTeardownDestroysNothing(t *testing.T) {
	h, k8s := newPreflightHandler(
		preflightNode(false),
		idlePod(idlePodOpts{name: "node-exporter", nodeName: preflightNodeName, daemonSet: true}),
	)
	preflight, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand())
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}

	if err := h.OnReleaseIdleTeardown(context.Background(), protocol.ReleaseIdleTeardown{
		NodeName:              preflightNodeName,
		CordonResourceVersion: preflight.CordonResourceVersion,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	for _, a := range k8s.Actions() {
		if a.GetVerb() == "delete" || a.GetVerb() == "create" || a.GetVerb() == "deletecollection" {
			t.Fatalf("the release issued a %s on %s", a.GetVerb(), a.GetResource().Resource)
		}
	}
	if _, err := k8s.CoreV1().Nodes().Get(context.Background(), preflightNodeName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the release removed the Node object: %v", err)
	}
}

// The ordinary sweep stays a reader. Uncordoning is central's explicit command
// and nothing else — a watcher that gave back cordons on its own would fight
// every operator in the cluster, and it cannot tell which cordons are its to
// give back in the first place.
func TestIdleSweepNeverWritesToANode(t *testing.T) {
	node := burstNode("abc123", corev1.ConditionTrue, time.Hour)
	node.Spec.Unschedulable = true
	w, _, k8s := newIdleWatcher(node)

	sweepPast(w, time.Now())

	if n := nodeWrites(k8s); n != 0 {
		t.Fatalf("the idle sweep issued %d node write(s); it must only ever read", n)
	}
	updated, err := k8s.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if !updated.Spec.Unschedulable {
		t.Fatal("the idle sweep uncordoned a node somebody else had taken out of service")
	}
}
