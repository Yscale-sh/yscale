package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

const preflightNodeName = "ys-burst-idle1"

// preflightNode carries a resourceVersion because the rollback's precondition is
// one: a real Node always has it, and a fixture without it would be testing a
// shape the API server never produces.
func preflightNode(unschedulable bool) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: preflightNodeName, ResourceVersion: "1"},
		Spec:       corev1.NodeSpec{Unschedulable: unschedulable},
	}
}

func preflightCommand() protocol.PrepareIdleTeardown {
	return protocol.PrepareIdleTeardown{NodeName: preflightNodeName}
}

// newPreflightHandler wires a handler to a fake cluster that honours the
// spec.nodeName field selector the preflight lists with — without it the fake
// hands back every pod in the cluster and a node would look occupied by work
// that is somewhere else entirely.
func newPreflightHandler(objs ...runtime.Object) (*RealHandler, *fake.Clientset) {
	k8s := fake.NewSimpleClientset(objs...)
	scopePodListsByNode(k8s)
	return NewRealHandler(k8s, nil, discardLogger()), k8s
}

func cordoned(t *testing.T, k8s *fake.Clientset) bool {
	t.Helper()
	node, err := k8s.CoreV1().Nodes().Get(context.Background(), preflightNodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	return node.Spec.Unschedulable
}

// The success path, and the ORDER is the whole contract. A list taken before the
// cordon describes an instant the scheduler was still free to bind into — which
// is exactly the race central is asking about, since the connector's own idle
// sweep already took one of those minutes ago.
func TestPrepareIdleTeardownCordonsBeforeItLooks(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(false))

	if _, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand()); err != nil {
		t.Fatalf("preflight on an empty node = %v, want approval", err)
	}

	var order []string
	for _, a := range k8s.Actions() {
		switch {
		case a.GetVerb() == "update" && a.GetResource().Resource == "nodes":
			order = append(order, "cordon")
		case a.GetVerb() == "list" && a.GetResource().Resource == "pods":
			order = append(order, "list")
		}
	}
	if len(order) != 2 || order[0] != "cordon" || order[1] != "list" {
		t.Fatalf("actions = %v, want the cordon before the pod list", order)
	}
	// Left cordoned deliberately: central claims and reaps next, and an uncordon
	// in between reopens the window this command exists to shut.
	if !cordoned(t, k8s) {
		t.Fatal("the confirmed-empty node was uncordoned; new work could land before central reaps it")
	}
}

// The defect this closes. A pod bound after the connector's last idle sweep is
// invisible to every check central makes against its own record, so the answer
// has to come from the cluster — and it has to be no.
func TestPrepareIdleTeardownRefusesANodeThatTookWork(t *testing.T) {
	h, k8s := newPreflightHandler(
		preflightNode(false),
		idlePod(idlePodOpts{name: "inference-0", namespace: "team-a", nodeName: preflightNodeName}),
	)

	_, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand())
	if err == nil || !strings.Contains(err.Error(), "team-a/inference-0") {
		t.Fatalf("preflight = %v, want a refusal naming the pod that occupies the node", err)
	}
	// Rolled back: this command added the cordon and did not get to use it, so
	// leaving the node unschedulable would strand capacity the customer is using.
	if cordoned(t, k8s) {
		t.Fatal("a refused preflight left its own cordon behind")
	}
}

// Same occupancy rules as the idle sweep, or the two would disagree about the
// same node and the preflight would refuse teardowns the watcher correctly asked
// for.
func TestPrepareIdleTeardownIgnoresPodsThatAreNotWork(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
	}{
		{"daemonset", idlePod(idlePodOpts{name: "node-exporter", nodeName: preflightNodeName, daemonSet: true})},
		{"mirror", idlePod(idlePodOpts{name: "static-kube-proxy", nodeName: preflightNodeName, mirror: true})},
		{"succeeded", idlePod(idlePodOpts{name: "batch-done", nodeName: preflightNodeName, phase: corev1.PodSucceeded})},
		{"deleting", idlePod(idlePodOpts{name: "draining", nodeName: preflightNodeName, deleting: true})},
		{"another node's", idlePod(idlePodOpts{name: "elsewhere", nodeName: "ys-burst-other"})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newPreflightHandler(preflightNode(false), tc.pod)

			if _, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand()); err != nil {
				t.Fatalf("preflight = %v, want approval — a %s pod does not occupy the node", err, tc.name)
			}
		})
	}
}

// Unknown is not empty. A pod list this connector could not read says nothing
// about the node, and the whole point of the preflight is that central acts only
// on an answer.
func TestPrepareIdleTeardownRefusesWhenThePodListFails(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(false))
	k8s.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("etcdserver: request timed out")
	})

	if _, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand()); err == nil {
		t.Fatal("preflight approved a teardown on a pod list it could not read")
	}
	if cordoned(t, k8s) {
		t.Fatal("a refused preflight left its own cordon behind")
	}
	// One write to place the cordon, one conditional patch to take it back. A
	// second update would mean the rollback re-read the node and wrote over
	// whatever it found, which is the race this closes.
	updates, patches := 0, 0
	for _, a := range k8s.Actions() {
		if a.GetResource().Resource != "nodes" {
			continue
		}
		switch a.GetVerb() {
		case "update":
			updates++
		case "patch":
			patches++
		}
	}
	if updates != 1 || patches != 1 {
		t.Fatalf("node writes = %d update(s), %d patch(es); want the cordon as an update and the rollback as one conditional patch",
			updates, patches)
	}
}

// Roll back only what this command did. A node that was ALREADY unschedulable
// was taken out of service by something else — an operator, a drain in flight —
// and uncordoning it would put work back on a node somebody deliberately
// emptied.
func TestPrepareIdleTeardownNeverUncordonsANodeItDidNotCordon(t *testing.T) {
	h, k8s := newPreflightHandler(
		preflightNode(true),
		idlePod(idlePodOpts{name: "inference-0", nodeName: preflightNodeName}),
	)

	if _, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand()); err == nil {
		t.Fatal("preflight approved a teardown for an occupied node")
	}
	if !cordoned(t, k8s) {
		t.Fatal("the preflight uncordoned a node it found already unschedulable")
	}
	for _, a := range k8s.Actions() {
		if (a.GetVerb() == "update" || a.GetVerb() == "patch") && a.GetResource().Resource == "nodes" {
			t.Fatalf("the preflight issued a %s on a node whose cordon it did not place", a.GetVerb())
		}
	}
}

// The concurrency defect. Between this command's cordon and its rollback,
// another drain — an operator, the autoscaler, a teardown already in flight —
// can take the node out of service for reasons of its own. A rollback that
// GETs and uncordons would reverse THAT decision and put pods back on a node
// somebody is draining.
//
// So the undo carries its own preconditions and is never retried: the node must
// still be at the exact version this command's cordon produced, and must still
// be unschedulable. Failing either, the node stays cordoned — capacity idle
// until something uncordons it, which is the cheaper of the two mistakes.
func TestPrepareIdleTeardownLeavesTheCordonWhenTheNodeChangedUnderIt(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(false))
	// The other writer lands between the cordon and the rollback, which is
	// exactly where the race is: the pod list is what triggers the rollback.
	//
	// Written through the tracker, not the clientset: a reactor runs while the
	// fake holds its own lock, so a nested client call deadlocks.
	nodesGVR := schema.GroupVersionResource{Version: "v1", Resource: "nodes"}
	k8s.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		obj, err := k8s.Tracker().Get(nodesGVR, "", preflightNodeName)
		if err != nil {
			return true, nil, err
		}
		node := obj.(*corev1.Node)
		node.ResourceVersion = "99"
		node.Annotations = map[string]string{"yscale.sh/test-drain-owner": "someone-else"}
		if err := k8s.Tracker().Update(nodesGVR, node, ""); err != nil {
			return true, nil, err
		}
		return true, nil, errors.New("etcdserver: request timed out")
	})

	if _, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand()); err == nil {
		t.Fatal("preflight approved a teardown on a pod list it could not read")
	}
	if !cordoned(t, k8s) {
		t.Fatal("the rollback uncordoned a node another drain had claimed since the preflight cordoned it")
	}
}

// IdleNodeWatcher's rule, enforced a second time at the point of action: a
// namespace-scoped connector cannot tell an empty node from one busy with
// another namespace's pods, and there is no partial version of that answer.
func TestPrepareIdleTeardownRefusesWithoutClusterWideVisibility(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(false))
	h.WorkloadNamespace = "ys-cust-team-a"

	_, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand())
	if err == nil || !strings.Contains(err.Error(), "ys-cust-team-a") {
		t.Fatalf("preflight = %v, want a refusal naming the namespace scope", err)
	}
	// Refused before anything was touched: a connector that cannot answer the
	// question must not cordon a node on the strength of it.
	if len(k8s.Actions()) != 0 {
		t.Fatalf("a scoped connector issued %d API calls, want none", len(k8s.Actions()))
	}
}

// The preflight is a question, never a teardown. It must not evict, delete or
// remove anything — central has not claimed the burst yet, and on a refusal it
// never will.
func TestPrepareIdleTeardownDestroysNothing(t *testing.T) {
	h, k8s := newPreflightHandler(preflightNode(false))

	if _, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand()); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	for _, a := range k8s.Actions() {
		if a.GetVerb() == "delete" || a.GetVerb() == "create" {
			t.Fatalf("the preflight issued a %s on %s", a.GetVerb(), a.GetResource().Resource)
		}
	}
	if _, err := k8s.CoreV1().Nodes().Get(context.Background(), preflightNodeName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the preflight removed the Node object: %v", err)
	}
}
