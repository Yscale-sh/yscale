package agent

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"

	k8sv1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/workload"
)

func TestPendingPodConstraintRejection(t *testing.T) {
	baseSpec := workload.Spec{
		Backend: "linode",
		Region:  "us-sea",
		GPU:     &workload.GPURequest{Kind: "l4", Count: 1},
	}
	requiredNodeAffinity := &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key: "example.com/pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"training"},
			}}}},
		},
	}}
	preferredNodeAffinity := &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
			Weight: 1,
			Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key: "example.com/pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"training"},
			}}},
		}},
	}}

	tests := []struct {
		name string
		edit func(*corev1.Pod)
		spec workload.Spec
		want string
	}{
		{name: "supported base contract", spec: baseSpec},
		{
			name: "supported stable selectors",
			edit: func(pod *corev1.Pod) {
				pod.Spec.NodeSelector[k8sv1.LabelNvidiaGPUPresent] = "true"
				pod.Spec.NodeSelector[k8sv1.LabelGPUKind] = "l4"
				pod.Spec.NodeSelector[k8sv1.LabelGPUCount] = "1"
				pod.Spec.NodeSelector[k8sv1.LabelProviderClass] = "linode"
				pod.Spec.NodeSelector[k8sv1.LabelRegion] = "us-sea"
				pod.Spec.NodeSelector[corev1.LabelOSStable] = "linux"
				pod.Spec.NodeSelector[corev1.LabelArchStable] = "amd64"
			},
			spec: baseSpec,
		},
		{
			name: "preferred affinity is non-blocking",
			edit: func(pod *corev1.Pod) { pod.Spec.Affinity = preferredNodeAffinity },
			spec: baseSpec,
		},
		{
			name: "missing burst toleration",
			edit: func(pod *corev1.Pod) { pod.Spec.Tolerations = nil },
			spec: baseSpec,
			want: pendingPodRejectMissingBurstToleration,
		},
		{
			name: "pinned node name",
			edit: func(pod *corev1.Pod) { pod.Spec.NodeName = "existing-node" },
			spec: baseSpec,
			want: pendingPodRejectUnsupportedNodeName,
		},
		{
			name: "required affinity",
			edit: func(pod *corev1.Pod) { pod.Spec.Affinity = requiredNodeAffinity },
			spec: baseSpec,
			want: pendingPodRejectUnsupportedAffinity,
		},
		{
			name: "unknown selector",
			edit: func(pod *corev1.Pod) { pod.Spec.NodeSelector["example.com/pool"] = "training" },
			spec: baseSpec,
			want: pendingPodRejectUnsupportedSelector,
		},
		{
			name: "known selector conflicts with template",
			edit: func(pod *corev1.Pod) { pod.Spec.NodeSelector[k8sv1.LabelGPUKind] = "a100" },
			spec: baseSpec,
			want: pendingPodRejectSelectorConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := newPendingPod("constraints", "ml", "gpu: {kind: l4, count: 1}")
			if tt.edit != nil {
				tt.edit(pod)
			}
			if got := pendingPodConstraintRejection(pod, tt.spec); got != tt.want {
				t.Fatalf("rejection = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPendingPodConstraintRejectionIsAnnotatedBeforeCentralSubmission(t *testing.T) {
	pod := newPendingPod("missing-toleration", "ml", "gpu: {kind: l4, count: 1}")
	pod.Spec.Tolerations = nil
	central := &fakeCentral{}
	k8s := fake.NewSimpleClientset(pod.DeepCopy())
	w := NewPendingPodWatcher(k8s, central, nil)

	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})
	if central.submitCount() != 0 {
		t.Fatalf("central submissions = %d, want zero", central.submitCount())
	}
	stored, err := k8s.CoreV1().Pods("ml").Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.Annotations[workload.AnnotationBurstRejection]; got != pendingPodRejectMissingBurstToleration {
		t.Fatalf("rejection annotation = %q, want %q", got, pendingPodRejectMissingBurstToleration)
	}

	w.handleEvent(context.Background(), watch.Event{Type: watch.Modified, Object: stored})
	if central.submitCount() != 0 {
		t.Fatalf("replayed rejected pod submitted %d workloads, want zero", central.submitCount())
	}
}
