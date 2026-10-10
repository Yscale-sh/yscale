package agent

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const drainNodeName = "ys-burst-drain-1"

// drainPod builds a Pod already scheduled onto the node being drained.
func drainPod(namespace, name string, daemonSet bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       corev1.PodSpec{NodeName: drainNodeName},
	}
	if daemonSet {
		pod.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet", Name: "node-agent"}}
	}
	return pod
}

// recordEvictions answers the eviction subresource and returns an accessor for
// the "namespace/name" of every pod the drain evicted.
//
// Reactored rather than read back off the tracker: the fake's default handling
// of a subresource create stores the Eviction object itself under the Pod's
// key, so a later pod List would decode garbage.
func recordEvictions(k8s *fake.Clientset, result error) func() []string {
	var evicted []string
	k8s.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		ev, ok := action.(ktesting.CreateAction).GetObject().(*policyv1.Eviction)
		if !ok {
			return false, nil, nil
		}
		evicted = append(evicted, ev.Namespace+"/"+ev.Name)
		return true, nil, result
	})
	return func() []string { return evicted }
}

// podActions returns the namespace (and, where the verb names one, the pod) of
// every pods request the drain issued under verb. An empty namespace is the
// cluster-scope call.
func podActions(k8s *fake.Clientset, verb string) []string {
	var out []string
	for _, a := range k8s.Actions() {
		if a.GetVerb() != verb || a.GetResource().Resource != "pods" || a.GetSubresource() != "" {
			continue
		}
		if named, ok := a.(ktesting.DeleteAction); ok {
			out = append(out, a.GetNamespace()+"/"+named.GetName())
			continue
		}
		out = append(out, a.GetNamespace())
	}
	return out
}

// A hosted connector's pod grant is a Role in its tenant namespace, so the
// drain has to list pods there and nowhere else — an unscoped list is refused
// at the cluster scope before a single pod is evicted.
func TestDrainScopesPodListToTheWorkloadNamespace(t *testing.T) {
	k8s := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: drainNodeName}},
		drainPod("ys-cust-hosted-demo", "job-pod", false),
		drainPod("ys-cust-hosted-demo", "node-agent-xyz", true),
		drainPod("kube-system", "other-tenant-pod", false),
	)
	evicted := recordEvictions(k8s, nil)
	h := NewRealHandler(k8s, nil, discardLogger())
	h.WorkloadNamespace = "ys-cust-hosted-demo"

	if err := h.drainAndDeleteNode(context.Background(), drainNodeName, true); err != nil {
		t.Fatalf("drainAndDeleteNode: %v", err)
	}

	if got := podActions(k8s, "list"); !slices.Equal(got, []string{"ys-cust-hosted-demo"}) {
		t.Errorf("pods listed in %q, want only the workload namespace", got)
	}
	// The DaemonSet pod stays (its controller would just recreate it on the
	// cordoned node); the pod outside the workload namespace isn't this
	// connector's to touch.
	if got := evicted(); !slices.Equal(got, []string{"ys-cust-hosted-demo/job-pod"}) {
		t.Errorf("evicted %q, want only the workload namespace's non-DaemonSet pod", got)
	}
	// NodeGC and WS teardown both still remove the Node object itself, which
	// stays a cluster-scoped call.
	if _, err := k8s.CoreV1().Nodes().Get(context.Background(), drainNodeName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("node get after drain = %v, want NotFound", err)
	}
}

// The chart's namespaced default leaves -workload-namespace unset and grants pods
// only in rbac.allowedNamespaces. The drain must list in each of those, never at
// the cluster scope: that list is forbidden, the drain failed before evicting
// anything, and the burst's Node object was left behind (seen live 2026-10-04).
func TestDrainUnderNamespacedRBACListsEachAllowedNamespace(t *testing.T) {
	k8s := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: drainNodeName}},
		drainPod("team-a", "job-a", false),
		drainPod("team-b", "job-b", false),
		drainPod("kube-system", "not-ours", false),
	)
	k8s.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetNamespace() == "" {
			return true, nil, apierrors.NewForbidden(corev1.Resource("pods"), "", errors.New("namespaced Role only"))
		}
		return false, nil, nil
	})
	evicted := recordEvictions(k8s, nil)
	h := NewRealHandler(k8s, nil, discardLogger())
	h.PodInventoryNamespaces = []string{"team-a", "team-b"}

	if err := h.drainAndDeleteNode(context.Background(), drainNodeName, true); err != nil {
		t.Fatalf("drainAndDeleteNode: %v", err)
	}
	if got := podActions(k8s, "list"); !slices.Equal(got, []string{"team-a", "team-b"}) {
		t.Errorf("pods listed in %q, want each allowed namespace and no cluster-scope list", got)
	}
	if got := evicted(); !slices.Equal(got, []string{"team-a/job-a", "team-b/job-b"}) {
		t.Errorf("evicted %q, want the allowed namespaces' pods", got)
	}
	if _, err := k8s.CoreV1().Nodes().Get(context.Background(), drainNodeName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("node get after drain = %v, want NotFound", err)
	}
}

func TestReadableNamespaces(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pinned    string
		allowlist []string
		want      []string
	}{
		{"pinned wins", "team-b", []string{"team-a", "team-b"}, []string{"team-b"}},
		{"namespaced allowlist", "", []string{"team-a", "team-b"}, []string{"team-a", "team-b"}},
		{"cluster scope", "", nil, []string{""}},
	} {
		if got := ReadableNamespaces(tc.pinned, tc.allowlist); !slices.Equal(got, tc.want) {
			t.Errorf("%s: ReadableNamespaces(%q, %q) = %q, want %q", tc.name, tc.pinned, tc.allowlist, got, tc.want)
		}
	}
}

// BYOC installs leave -workload-namespace unset. Empty has to keep meaning
// "every namespace" rather than collapsing to the default one, or a drain there
// silently leaves pods behind.
func TestDrainWithoutWorkloadNamespaceCoversEveryNamespace(t *testing.T) {
	k8s := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: drainNodeName}},
		drainPod("team-a", "job-pod", false),
		drainPod("kube-system", "infra-pod", false),
	)
	evicted := recordEvictions(k8s, nil)
	h := NewRealHandler(k8s, nil, discardLogger())

	if err := h.drainAndDeleteNode(context.Background(), drainNodeName, false); err != nil {
		t.Fatalf("drainAndDeleteNode: %v", err)
	}

	if got := podActions(k8s, "list"); !slices.Equal(got, []string{""}) {
		t.Errorf("pods listed in %q, want the cluster-scope list", got)
	}
	got := evicted()
	slices.Sort(got)
	if !slices.Equal(got, []string{"kube-system/infra-pod", "team-a/job-pod"}) {
		t.Errorf("evicted %q, want both namespaces' pods", got)
	}
}

// An eviction that doesn't take falls back to deleting the Pod outright — a
// PDB still blocking at the drain deadline reaches the same call. It runs in
// the pod's own namespace, which is why the namespaced Role needs `delete` on
// pods and not just `create` on pods/eviction.
func TestDrainFallsBackToDeleteInTheWorkloadNamespace(t *testing.T) {
	k8s := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: drainNodeName}},
		drainPod("ys-cust-hosted-demo", "job-pod", false),
	)
	recordEvictions(k8s, apierrors.NewInternalError(errors.New("eviction webhook unavailable")))
	h := NewRealHandler(k8s, nil, discardLogger())
	h.WorkloadNamespace = "ys-cust-hosted-demo"

	if err := h.drainAndDeleteNode(context.Background(), drainNodeName, false); err != nil {
		t.Fatalf("drainAndDeleteNode: %v", err)
	}

	if got := podActions(k8s, "delete"); !slices.Equal(got, []string{"ys-cust-hosted-demo/job-pod"}) {
		t.Errorf("deleted %q, want the un-evicted pod in the workload namespace", got)
	}
}

func TestDrainReportsFallbackDeleteFailure(t *testing.T) {
	k8s := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: drainNodeName}},
		drainPod("ys-cust-hosted-demo", "job-pod", false),
	)
	recordEvictions(k8s, apierrors.NewInternalError(errors.New("eviction webhook unavailable")))
	k8s.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(corev1.Resource("pods"), "job-pod", errors.New("delete denied"))
	})
	h := NewRealHandler(k8s, nil, discardLogger())
	h.WorkloadNamespace = "ys-cust-hosted-demo"

	err := h.drainAndDeleteNode(context.Background(), drainNodeName, false)
	if err == nil || !strings.Contains(err.Error(), "delete pod ys-cust-hosted-demo/job-pod") {
		t.Fatalf("drain error = %v, want fallback delete failure", err)
	}
}
