package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"

	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
)

// fakeCentral records SubmitWorkload calls so tests can assert what
// the watcher pushed.
type fakeCentral struct {
	mu         sync.Mutex
	workloadID string
	burstID    string
	calls      []struct {
		ns      string
		yamlStr string
		key     string
		origin  protocol.SubmissionOrigin
	}
	failNext error
}

func (f *fakeCentral) SubmitWorkload(_ context.Context, ns string, yamlBytes []byte, key string, origin protocol.SubmissionOrigin) (string, string, string, float64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return "", "", "", 0, err
	}
	f.calls = append(f.calls, struct {
		ns      string
		yamlStr string
		key     string
		origin  protocol.SubmissionOrigin
	}{ns, string(yamlBytes), key, origin})
	workloadID := f.workloadID
	if workloadID == "" {
		workloadID = "wl_test"
	}
	burstID := f.burstID
	if burstID == "" {
		burstID = "burst_test"
	}
	return workloadID, burstID, "reliable", 1.64, nil
}
func (f *fakeCentral) CancelWorkload(_ context.Context, _ string) error { return nil }
func (f *fakeCentral) submitCount() int                                 { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }
func (f *fakeCentral) lastYAML() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return ""
	}
	return f.calls[len(f.calls)-1].yamlStr
}
func (f *fakeCentral) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.key)
	}
	return out
}
func (f *fakeCentral) origins() []protocol.SubmissionOrigin {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]protocol.SubmissionOrigin, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.origin)
	}
	return out
}

func newPendingPod(name, ns string, tplYAML string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID("uid-" + name),
			Annotations: map[string]string{
				workload.AnnotationBurstTemplate: tplYAML,
			},
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{
				workload.LabelBurstNode: "true",
			},
			Tolerations: []corev1.Toleration{{
				Key: workload.LabelBurstNode, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule,
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
}

// assertSchedulingContract verifies the complete scheduling and bounded-burst
// contract carried by the controller-created Pod.
func assertSchedulingContract(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	if v := pod.Spec.NodeSelector[workload.LabelBurstNode]; v != "true" {
		t.Fatalf("nodeSelector %s = %q, want \"true\"", workload.LabelBurstNode, v)
	}
	tpl := pod.Annotations[workload.AnnotationBurstTemplate]
	if tpl == "" {
		t.Fatal("annotation " + workload.AnnotationBurstTemplate + " is missing or empty")
	}
	spec, err := parseBurstTemplate(tpl)
	if err != nil {
		t.Fatalf("parse %s: %v", workload.AnnotationBurstTemplate, err)
	}
	if spec.GPU == nil || spec.GPU.Kind != "l4" || spec.GPU.Count != 1 {
		t.Fatalf("burst template GPU = %#v, want one l4", spec.GPU)
	}
	if spec.Budget == nil || spec.Budget.MaxUSD <= 0 || spec.Budget.Deadline <= 0 {
		t.Fatalf("burst template budget = %#v, want positive maxUSD and deadline", spec.Budget)
	}
	hasTol := false
	for _, tol := range pod.Spec.Tolerations {
		if tol.Key == workload.LabelBurstNode &&
			tol.Operator == corev1.TolerationOpExists &&
			tol.Effect == corev1.TaintEffectNoSchedule {
			hasTol = true
			break
		}
	}
	if !hasTol {
		t.Fatalf("missing toleration for %s:NoSchedule", workload.LabelBurstNode)
	}
	gpuRes := corev1.ResourceName("nvidia.com/gpu")
	hasGPU := false
	for _, c := range pod.Spec.Containers {
		if lim, ok := c.Resources.Limits[gpuRes]; ok && lim.Value() == 1 {
			hasGPU = true
			break
		}
	}
	if !hasGPU {
		t.Fatal("no container declares nvidia.com/gpu resource limit of exactly 1")
	}
}

func jobController(name string) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Name:       name,
		Controller: &controller,
	}
}

func TestWatcherSubmitsForPendingBurstPod(t *testing.T) {
	central := &fakeCentral{}
	k8s := fake.NewSimpleClientset()
	w := NewPendingPodWatcher(k8s, central, nil)

	pod := newPendingPod("inference-1", "ml", `
gpu: { kind: a100, count: 1, reliability: reliable }
budget: { maxUSD: 5, deadline: 1h }
`)
	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})

	if central.submitCount() != 1 {
		t.Fatalf("expected 1 submit, got %d", central.submitCount())
	}
	y := central.lastYAML()
	for _, want := range []string{
		"nodeOnly: true",
		"namespace: ml",
		"kind: a100",
		"reliability: reliable",
	} {
		if !strings.Contains(y, want) {
			t.Errorf("submitted yaml missing %q:\n%s", want, y)
		}
	}
}

func TestPendingPodDeploymentExampleExercisesWatcher(t *testing.T) {
	path := filepath.Join("..", "..", "..", "examples", "workloads", "pending-pod-deployment.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	var deployment appsv1.Deployment
	if err := yaml.Unmarshal(data, &deployment); err != nil {
		t.Fatalf("parse example: %v", err)
	}
	pod := &corev1.Pod{
		ObjectMeta: deployment.Spec.Template.ObjectMeta,
		Spec:       deployment.Spec.Template.Spec,
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	pod.Name = deployment.Name + "-example"
	pod.Namespace = deployment.Namespace
	pod.UID = "pending-pod-example"
	assertSchedulingContract(t, pod)

	central := &fakeCentral{}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)
	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})

	if central.submitCount() != 1 {
		t.Fatalf("example submissions = %d, want 1", central.submitCount())
	}
	if got := central.origins(); len(got) != 1 || got[0] != protocol.OriginPendingPod {
		t.Fatalf("example origins = %q, want pending-pod", got)
	}
	for _, want := range []string{"nodeOnly: true", "kind: l4"} {
		if !strings.Contains(central.lastYAML(), want) {
			t.Errorf("submitted example YAML missing %q: %s", want, central.lastYAML())
		}
	}
}

func TestPendingPodArgoWorkflowExampleExercisesWatcher(t *testing.T) {
	path := filepath.Join("..", "..", "..", "examples", "workloads", "pending-pod-argo-workflow.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read example: %v", err)
	}

	// Minimal struct mirroring the Argo Workflow CRD shape down to the
	// pod template fields. Argo templates carry pod metadata, nodeSelector,
	// tolerations, and a single container inline — not inside a nested
	// PodTemplateSpec.
	type argoContainer struct {
		Image     string                      `json:"image"`
		Command   []string                    `json:"command,omitempty"`
		Args      []string                    `json:"args,omitempty"`
		Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	}
	type argoTemplate struct {
		Name         string              `json:"name"`
		Metadata     metav1.ObjectMeta   `json:"metadata"`
		NodeSelector map[string]string   `json:"nodeSelector,omitempty"`
		Tolerations  []corev1.Toleration `json:"tolerations,omitempty"`
		Container    argoContainer       `json:"container"`
	}
	type argoWorkflow struct {
		metav1.TypeMeta   `json:",inline"`
		metav1.ObjectMeta `json:"metadata"`
		Spec              struct {
			Entrypoint string         `json:"entrypoint"`
			Templates  []argoTemplate `json:"templates"`
		} `json:"spec"`
	}

	var wf argoWorkflow
	if err := yaml.Unmarshal(data, &wf); err != nil {
		t.Fatalf("parse example: %v", err)
	}
	var tmpl argoTemplate
	for _, t := range wf.Spec.Templates {
		if t.Name == wf.Spec.Entrypoint {
			tmpl = t
			break
		}
	}
	if tmpl.Name == "" {
		t.Fatalf("no template matches entrypoint %q", wf.Spec.Entrypoint)
	}

	// Build a synthetic Pod that models the scheduling-relevant shape an
	// Argo-created pod would carry; not a full Argo pod simulation.
	pod := &corev1.Pod{
		ObjectMeta: tmpl.Metadata,
		Spec: corev1.PodSpec{
			NodeSelector: tmpl.NodeSelector,
			Tolerations:  tmpl.Tolerations,
			Containers: []corev1.Container{{
				Name:      tmpl.Name,
				Image:     tmpl.Container.Image,
				Command:   tmpl.Container.Command,
				Args:      tmpl.Container.Args,
				Resources: tmpl.Container.Resources,
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	pod.Name = "burst-train-abc123-gpu-train"
	pod.Namespace = wf.Namespace
	pod.UID = "pending-pod-argo-example"
	assertSchedulingContract(t, pod)

	central := &fakeCentral{}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)
	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})

	if central.submitCount() != 1 {
		t.Fatalf("argo example submissions = %d, want 1", central.submitCount())
	}
	if got := central.origins(); len(got) != 1 || got[0] != protocol.OriginPendingPod {
		t.Fatalf("argo example origins = %v, want pending-pod", got)
	}
	for _, want := range []string{"nodeOnly: true", "kind: l4"} {
		if !strings.Contains(central.lastYAML(), want) {
			t.Errorf("submitted argo YAML missing %q: %s", want, central.lastYAML())
		}
	}
}

func TestPendingPodKEDAScaledJobExampleExercisesWatcher(t *testing.T) {
	path := filepath.Join("..", "..", "..", "examples", "workloads", "pending-pod-keda-scaledjob.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read example: %v", err)
	}

	// Minimal struct mirroring the KEDA ScaledJob CRD shape. The pod
	// template lives at spec.jobTargetRef.template — the same nesting as
	// a batch/v1 JobSpec.
	type kedaScaledJob struct {
		metav1.TypeMeta   `json:",inline"`
		metav1.ObjectMeta `json:"metadata"`
		Spec              struct {
			JobTargetRef batchv1.JobSpec `json:"jobTargetRef"`
		} `json:"spec"`
	}

	var sj kedaScaledJob
	if err := yaml.Unmarshal(data, &sj); err != nil {
		t.Fatalf("parse example: %v", err)
	}

	// Build a synthetic Pod that models the scheduling-relevant shape a
	// KEDA-created pod would carry via the jobTargetRef template.
	pod := &corev1.Pod{
		ObjectMeta: sj.Spec.JobTargetRef.Template.ObjectMeta,
		Spec:       sj.Spec.JobTargetRef.Template.Spec,
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	pod.Name = "burst-etl-job-12345-abcde"
	pod.Namespace = sj.Namespace
	pod.UID = "pending-pod-keda-example"
	assertSchedulingContract(t, pod)

	central := &fakeCentral{}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)
	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})

	if central.submitCount() != 1 {
		t.Fatalf("keda example submissions = %d, want 1", central.submitCount())
	}
	if got := central.origins(); len(got) != 1 || got[0] != protocol.OriginPendingPod {
		t.Fatalf("keda example origins = %v, want pending-pod", got)
	}
	for _, want := range []string{"nodeOnly: true", "kind: l4"} {
		if !strings.Contains(central.lastYAML(), want) {
			t.Errorf("submitted keda YAML missing %q: %s", want, central.lastYAML())
		}
	}
}

// Every pending-pod submission reports the PATH it came in on. It is not
// "keda": KEDA, an HPA, Argo and a hand-applied Job all leave the same Pending
// pod behind, and this watcher cannot tell them apart.
func TestWatcherReportsPendingPodOrigin(t *testing.T) {
	central := &fakeCentral{}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)

	w.handleEvent(context.Background(), watch.Event{
		Type:   watch.Added,
		Object: newPendingPod("inference-1", "ml", "gpu: { kind: a100 }"),
	})

	origins := central.origins()
	if len(origins) != 1 || origins[0] != protocol.OriginPendingPod {
		t.Fatalf("submitted origins = %q, want one %q", origins, protocol.OriginPendingPod)
	}
}

func TestSubmitForPodLabelsOwningJob(t *testing.T) {
	const (
		workloadID = "wl_pending_job_123"
		burstID    = "burst_pending_job_456"
	)
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      "inference-job",
		Namespace: "ml",
		Labels:    map[string]string{"existing": "preserved"},
	}}
	k8s := fake.NewSimpleClientset(job)
	central := &fakeCentral{workloadID: workloadID, burstID: burstID}
	w := NewPendingPodWatcher(k8s, central, nil)
	pod := newPendingPod("inference-job-abcde", "ml", "gpu: { kind: l4 }")
	pod.OwnerReferences = []metav1.OwnerReference{jobController(job.Name)}

	if err := w.submitForPod(context.Background(), pod, pod.Annotations[workload.AnnotationBurstTemplate]); err != nil {
		t.Fatalf("submitForPod() error = %v", err)
	}

	patched, err := k8s.BatchV1().Jobs("ml").Get(context.Background(), job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get patched Job: %v", err)
	}
	if got := patched.Labels[labelWorkloadID]; got != workloadID {
		t.Errorf("workload label = %q, want %q", got, workloadID)
	}
	if got := patched.Labels[labelBurstID]; got != burstID {
		t.Errorf("burst label = %q, want %q", got, burstID)
	}
	if got := patched.Labels["existing"]; got != "preserved" {
		t.Errorf("existing label = %q, want preserved", got)
	}
}

func TestSubmitForPodDoesNotPatchNonJobOwners(t *testing.T) {
	tests := []struct {
		name  string
		owner *metav1.OwnerReference
	}{
		{name: "bare pod"},
		{name: "non-batch Job", owner: func() *metav1.OwnerReference {
			owner := jobController("custom-job")
			owner.APIVersion = "example.com/v1"
			return &owner
		}()},
		{name: "deployment", owner: func() *metav1.OwnerReference {
			owner := jobController("web")
			owner.APIVersion = "apps/v1"
			owner.Kind = "Deployment"
			return &owner
		}()},
		{name: "replica set", owner: func() *metav1.OwnerReference {
			owner := jobController("web-abcde")
			owner.APIVersion = "apps/v1"
			owner.Kind = "ReplicaSet"
			return &owner
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8s := fake.NewSimpleClientset()
			w := NewPendingPodWatcher(k8s, &fakeCentral{}, nil)
			pod := newPendingPod("web-abcde", "apps", "gpu: { kind: l4 }")
			if tt.owner != nil {
				pod.OwnerReferences = []metav1.OwnerReference{*tt.owner}
			}

			if err := w.submitForPod(context.Background(), pod, pod.Annotations[workload.AnnotationBurstTemplate]); err != nil {
				t.Fatalf("submitForPod() error = %v", err)
			}
			for _, action := range k8s.Actions() {
				if action.GetVerb() == "patch" && action.GetResource().Resource == "jobs" {
					t.Errorf("unexpected job patch action for %s owner", tt.name)
				}
			}
		})
	}
}

func TestWatcherJobPatchFailureDoesNotRetryProvisioning(t *testing.T) {
	central := &fakeCentral{}
	k8s := fake.NewSimpleClientset()
	w := NewPendingPodWatcher(k8s, central, nil)
	w.SubmitCooldown = 0
	pod := newPendingPod("missing-job-abcde", "ml", "gpu: { kind: l4 }")
	pod.OwnerReferences = []metav1.OwnerReference{jobController("missing-job")}

	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})
	w.handleEvent(context.Background(), watch.Event{Type: watch.Modified, Object: pod})

	if central.submitCount() != 1 {
		t.Errorf("expected one provision despite patch failure, got %d", central.submitCount())
	}
	patches := 0
	for _, action := range k8s.Actions() {
		if action.GetVerb() == "patch" && action.GetResource().Resource == "jobs" {
			patches++
		}
	}
	if patches != 1 {
		t.Errorf("expected one Job patch attempt, got %d", patches)
	}
}

func TestWatcherSkipsNonPending(t *testing.T) {
	central := &fakeCentral{}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)

	pod := newPendingPod("running-1", "ml", "gpu: { kind: l4 }")
	pod.Status.Phase = corev1.PodRunning

	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})
	if central.submitCount() != 0 {
		t.Errorf("expected no submits, got %d", central.submitCount())
	}
}

func TestWatcherSkipsWithoutBurstSelector(t *testing.T) {
	central := &fakeCentral{}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)

	pod := newPendingPod("regular", "ml", "gpu: { kind: l4 }")
	pod.Spec.NodeSelector = nil // remove the burst-node selector

	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})
	if central.submitCount() != 0 {
		t.Errorf("regular pods should not trigger submits, got %d", central.submitCount())
	}
}

func TestWatcherDedupsByPodUID(t *testing.T) {
	central := &fakeCentral{}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)
	w.SubmitCooldown = 0 // disable cooldown for this test

	pod := newPendingPod("same-pod", "ml", "gpu: { kind: l4 }")
	for i := 0; i < 5; i++ {
		w.handleEvent(context.Background(), watch.Event{Type: watch.Modified, Object: pod})
	}
	if central.submitCount() != 1 {
		t.Errorf("expected dedup to 1 submit, got %d", central.submitCount())
	}
}

func TestWatcherCooldownDedupsByTemplate(t *testing.T) {
	central := &fakeCentral{}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)
	w.SubmitCooldown = 1 * time.Minute

	tpl := "gpu: { kind: a100, count: 1 }"
	// Five different pods, same template hash, same namespace.
	for i := 0; i < 5; i++ {
		pod := newPendingPod("pod-"+string(rune('a'+i)), "ml", tpl)
		w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})
	}
	if central.submitCount() != 1 {
		t.Errorf("expected cooldown to limit to 1 submit, got %d", central.submitCount())
	}
}

func TestWatcherForgetsDeletedPods(t *testing.T) {
	central := &fakeCentral{}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)
	w.SubmitCooldown = 0

	pod := newPendingPod("ephemeral", "ml", "gpu: { kind: l4 }")
	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})
	w.handleEvent(context.Background(), watch.Event{Type: watch.Deleted, Object: pod})
	// New event for "same UID" should re-submit (we forgot it).
	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})

	if central.submitCount() != 2 {
		t.Errorf("expected 2 submits across Add/Delete/Add, got %d", central.submitCount())
	}
}

func TestSanitizeName(t *testing.T) {
	cases := []struct{ in, out string }{
		{"burst-inference-1-abc12345", "burst-inference-1-abc12345"},
		{"BURST-X", "burst-x"},
		{"my.pod.name", "my-pod-name"},
		{strings.Repeat("a", 100), strings.Repeat("a", 63)},
	}
	for _, c := range cases {
		if got := sanitizeName(c.in); got != c.out {
			t.Errorf("sanitize(%q) = %q, want %q", c.in, got, c.out)
		}
	}
}

func TestWatcherSubmitFailureDoesntMarkPodSeen(t *testing.T) {
	central := &fakeCentral{failNext: errors.New("network blip")}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)
	w.SubmitCooldown = 0

	pod := newPendingPod("flaky", "ml", "gpu: { kind: l4 }")
	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})
	// Same pod retries successfully on next event.
	w.handleEvent(context.Background(), watch.Event{Type: watch.Modified, Object: pod})
	if central.submitCount() != 1 {
		t.Errorf("expected 1 successful submit after the retry, got %d", central.submitCount())
	}
}

// TestPodSubmitKeyIsStableAcrossWatchRestart: seenPodUIDs only spans one
// process, so a watch reconnect or an agent restart hands the same Pending pod
// back as new. The Idempotency-Key derived from the pod UID is what turns that
// second submit into a replay instead of a second burst.
func TestPodSubmitKeyIsStableAcrossWatchRestart(t *testing.T) {
	pod := newPendingPod("inference-1", "ml", "gpu: { kind: a100, count: 1 }")

	central := &fakeCentral{}
	for i := 0; i < 2; i++ {
		// A fresh watcher has fresh dedup maps — exactly the post-restart state.
		w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)
		w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})
	}

	keys := central.keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 submits across restarts, got %d", len(keys))
	}
	if keys[0] == "" {
		t.Fatal("submitted with no idempotency key")
	}
	if keys[0] != keys[1] {
		t.Errorf("key changed across restart: %q then %q", keys[0], keys[1])
	}
}

// TestPodSubmitKeyDiffersPerPodUID: a genuinely different pod — including one
// recreated under the same name, which is a new UID and a real second request
// for capacity — must not replay the first pod's burst.
func TestPodSubmitKeyDiffersPerPodUID(t *testing.T) {
	central := &fakeCentral{}
	w := NewPendingPodWatcher(fake.NewSimpleClientset(), central, nil)
	w.SubmitCooldown = 0

	first := newPendingPod("inference-1", "ml", "gpu: { kind: a100, count: 1 }")
	other := newPendingPod("inference-2", "ml", "gpu: { kind: a100, count: 1 }")
	recreated := newPendingPod("inference-1", "ml", "gpu: { kind: a100, count: 1 }")
	recreated.UID = types.UID("uid-inference-1-recreated")

	for _, pod := range []*corev1.Pod{first, other, recreated} {
		w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})
	}

	keys := central.keys()
	if len(keys) != 3 {
		t.Fatalf("expected 3 submits, got %d", len(keys))
	}
	seen := make(map[string]string, len(keys))
	for i, k := range keys {
		if prev, dup := seen[k]; dup {
			t.Errorf("submit %d shares key %q with %s", i, k, prev)
		}
		seen[k] = fmt.Sprintf("submit %d", i)
	}
}

// Correction B: the triggering pod itself is labeled so PodSchedulingWatcher
// observes it regardless of owner kind (Deployment, KEDA, Argo, standalone).

func TestSubmitForPodLabelsTriggeringPod(t *testing.T) {
	const (
		workloadID = "wl_deploy_123"
		burstID    = "burst_deploy_456"
	)
	pod := newPendingPod("inference-deploy-abc", "ml", "gpu: { kind: l4 }")
	// Deployment-owned: no batch/v1 Job owner.
	controller := true
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "ReplicaSet",
		Name:       "inference-deploy-abc-rs",
		Controller: &controller,
	}}
	k8s := fake.NewSimpleClientset(pod)
	central := &fakeCentral{workloadID: workloadID, burstID: burstID}
	w := NewPendingPodWatcher(k8s, central, nil)

	if err := w.submitForPod(context.Background(), pod, pod.Annotations[workload.AnnotationBurstTemplate]); err != nil {
		t.Fatalf("submitForPod() error = %v", err)
	}

	patched, err := k8s.CoreV1().Pods("ml").Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get patched pod: %v", err)
	}
	if got := patched.Labels[labelWorkloadID]; got != workloadID {
		t.Errorf("pod workload label = %q, want %q", got, workloadID)
	}
	if got := patched.Labels[labelBurstID]; got != burstID {
		t.Errorf("pod burst label = %q, want %q", got, burstID)
	}
}

func TestSubmitForPodLabelsStandalonePod(t *testing.T) {
	const (
		workloadID = "wl_standalone_789"
		burstID    = "burst_standalone_012"
	)
	pod := newPendingPod("standalone-gpu", "default", "gpu: { kind: a100 }")
	// No owner at all.
	k8s := fake.NewSimpleClientset(pod)
	central := &fakeCentral{workloadID: workloadID, burstID: burstID}
	w := NewPendingPodWatcher(k8s, central, nil)

	if err := w.submitForPod(context.Background(), pod, pod.Annotations[workload.AnnotationBurstTemplate]); err != nil {
		t.Fatalf("submitForPod() error = %v", err)
	}

	patched, err := k8s.CoreV1().Pods("default").Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get patched pod: %v", err)
	}
	if got := patched.Labels[labelWorkloadID]; got != workloadID {
		t.Errorf("pod workload label = %q, want %q", got, workloadID)
	}
}

func TestScheduledPatchedPodProducesPodEvent(t *testing.T) {
	const (
		workloadID = "wl_sched_123"
		burstID    = "burst_sched_456"
	)
	pod := newPendingPod("keda-worker-abc", "ml", "gpu: { kind: l4 }")
	k8s := fake.NewSimpleClientset(pod)
	central := &fakeCentral{workloadID: workloadID, burstID: burstID}
	pendingW := NewPendingPodWatcher(k8s, central, nil)

	if err := pendingW.submitForPod(context.Background(), pod, pod.Annotations[workload.AnnotationBurstTemplate]); err != nil {
		t.Fatalf("submitForPod() error = %v", err)
	}

	// Simulate the pod becoming Scheduled (metadata patch produces a Modified
	// event with PodScheduled=True).
	patched, _ := k8s.CoreV1().Pods("ml").Get(context.Background(), pod.Name, metav1.GetOptions{})
	patched.Spec.NodeName = "ys-burst-node"
	patched.Status.Conditions = append(patched.Status.Conditions, corev1.PodCondition{
		Type:               corev1.PodScheduled,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	})

	var reporter fakePodReporter
	schedW := NewPodSchedulingWatcher(k8s, &reporter, nil)
	schedW.observe(patched)

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 1 {
		t.Fatalf("expected 1 pod event, got %d", len(reporter.events))
	}
	ev := reporter.events[0]
	if ev.WorkloadID != workloadID {
		t.Errorf("WorkloadID = %q, want %q", ev.WorkloadID, workloadID)
	}
	if ev.NodeName != "ys-burst-node" {
		t.Errorf("NodeName = %q, want %q", ev.NodeName, "ys-burst-node")
	}
}

type fakePodReporter struct {
	mu     sync.Mutex
	events []protocol.PodEvent
}

func (f *fakePodReporter) ReportPodEvent(ev protocol.PodEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
}

func TestTriggeringPodPatchFailureIsRetryable(t *testing.T) {
	central := &fakeCentral{}
	pod := newPendingPod("patch-fail", "ml", "gpu: { kind: l4 }")
	k8s := fake.NewSimpleClientset(pod)
	k8s.PrependReactor("patch", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("pod patch denied")
	})
	w := NewPendingPodWatcher(k8s, central, nil)
	w.SubmitCooldown = 0

	w.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: pod})
	w.handleEvent(context.Background(), watch.Event{Type: watch.Modified, Object: pod})

	if central.submitCount() != 2 {
		t.Errorf("expected idempotent submit replay on patch retry, got %d calls", central.submitCount())
	}
	// Both watch events retry the patch because neither failure marks the Pod
	// handled. Central binds both calls to the same immutable Pod UID key.
	patches := 0
	for _, action := range k8s.Actions() {
		if action.GetVerb() == "patch" && action.GetResource().Resource == "pods" {
			patches++
		}
	}
	if patches != 2 {
		t.Errorf("expected 2 pod patch attempts, got %d", patches)
	}
}
