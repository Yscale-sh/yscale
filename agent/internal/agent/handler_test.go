package agent

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

func TestRealHandlerOnBurstAnnounce(t *testing.T) {
	// Without a BootstrapServer the handler should still succeed and log;
	// real authorization happens via Bootstrap.AuthorizeBurst.
	h := NewRealHandler(nil, nil, nil)
	if err := h.OnBurstAnnounce(context.Background(), protocol.BurstAnnounce{
		BurstID: "burst_1", TSHostname: "yscale-burst-1",
	}); err != nil {
		t.Fatalf("OnBurstAnnounce: %v", err)
	}
}

// The announce is where the agent learns which namespace it may read
// storage credentials from for a burst. An older central omits it; the
// burst is still authorized, storage signing is just refused later.
func TestRealHandlerOnBurstAnnounceRecordsNamespace(t *testing.T) {
	src := protocol.BucketRef{Bucket: "models", CredentialsSecret: "r2-creds"}
	bindings := []protocol.StorageBinding{{Name: "models", Type: "cache", Source: &src}}

	for _, tc := range []struct{ name, announced, want string }{
		{"namespace announced", "team-a", "team-a"},
		{"older central omits it", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			boot := &BootstrapServer{grants: map[string]*burstGrant{}, nodes: map[string]string{}}
			h := NewRealHandler(nil, boot, discardLogger())

			if err := h.OnBurstAnnounce(context.Background(), protocol.BurstAnnounce{
				BurstID: "burst_1", TSHostname: "yscale-burst-1",
				Namespace: tc.announced, Storage: bindings,
			}); err != nil {
				t.Fatalf("OnBurstAnnounce: %v", err)
			}

			ns, got, ok := boot.authorizedStorage("burst_1")
			if !ok {
				t.Fatal("burst was not authorized")
			}
			if ns != tc.want {
				t.Errorf("grant namespace = %q, want %q", ns, tc.want)
			}
			if len(got) != 1 || got[0].Name != "models" {
				t.Errorf("bindings = %+v, want the announced cache binding", got)
			}
		})
	}
}

func TestRealHandlerOnCreateJob(t *testing.T) {
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "wl-job", Namespace: "default"},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "c", Image: "nginx"}},
				},
			},
		},
	}
	raw, _ := json.Marshal(job)

	k8s := fake.NewSimpleClientset()
	h := NewRealHandler(k8s, nil, nil)
	err := h.OnCreateJob(context.Background(), protocol.CreateJob{
		WorkloadID: "wl_abc", Namespace: "default", JobSpec: raw,
	})
	if err != nil {
		t.Fatalf("OnCreateJob: %v", err)
	}
	created, err := k8s.BatchV1().Jobs("default").Get(context.Background(), "wl-job", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if created.Labels["yscale.sh/workload-id"] != "wl_abc" {
		t.Errorf("workload-id label missing: %v", created.Labels)
	}
}

func TestRealHandlerOnCreateJobIdempotent(t *testing.T) {
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "duplicate", Namespace: "default"},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: "c", Image: "nginx"}},
			},
		}},
	}
	raw, _ := json.Marshal(job)
	k8s := fake.NewSimpleClientset(&job)

	h := NewRealHandler(k8s, nil, nil)
	if err := h.OnCreateJob(context.Background(), protocol.CreateJob{
		WorkloadID: "wl_dup", Namespace: "default", JobSpec: raw,
	}); err != nil {
		t.Errorf("expected idempotent success on AlreadyExists, got %v", err)
	}
}

func TestRealHandlerSyncRuntimeBindingsReplacesFixedSecretData(t *testing.T) {
	k8s := fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "yscale-runtime-bindings", Namespace: "team-a", Labels: map[string]string{"kubernetes.io/metadata.name": "keep"},
	}, Data: map[string][]byte{"OLD": []byte("remove")}})
	h := &RealHandler{K8s: k8s, WorkloadNamespace: "team-a", Log: discardLogger()}
	err := h.OnSyncRuntimeBindings(context.Background(), protocol.SyncRuntimeBindings{
		Namespace: "team-a", Revision: 42, Bindings: map[string]string{"API_TOKEN": "secret-value"},
	})
	if err != nil {
		t.Fatalf("OnSyncRuntimeBindings: %v", err)
	}
	secret, err := k8s.CoreV1().Secrets("team-a").Get(context.Background(), protocol.RuntimeBindingsSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["API_TOKEN"]) != "secret-value" || secret.Data["OLD"] != nil {
		t.Fatalf("secret data = %v", secret.Data)
	}
	if secret.Labels["kubernetes.io/metadata.name"] != "keep" {
		t.Fatalf("labels were not preserved: %v", secret.Labels)
	}
	if secret.Annotations["yscale.sh/runtime-bindings-revision"] != "42" {
		t.Fatalf("revision annotation = %v", secret.Annotations)
	}
	if err := h.OnSyncRuntimeBindings(context.Background(), protocol.SyncRuntimeBindings{
		Namespace: "team-a", Revision: 41, Bindings: map[string]string{"STALE": "must-not-win"},
	}); err != nil {
		t.Fatalf("stale sync should be an idempotent success: %v", err)
	}
	secret, err = k8s.CoreV1().Secrets("team-a").Get(context.Background(), protocol.RuntimeBindingsSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["API_TOKEN"]) != "secret-value" || secret.Data["STALE"] != nil || secret.Annotations["yscale.sh/runtime-bindings-revision"] != "42" {
		t.Fatalf("stale sync overwrote desired data: data=%v annotations=%v", secret.Data, secret.Annotations)
	}
	if err := h.OnSyncRuntimeBindings(context.Background(), protocol.SyncRuntimeBindings{
		Namespace: "team-b", Revision: 43, Bindings: map[string]string{"API_TOKEN": "secret-value"},
	}); err == nil {
		t.Fatal("cross-scope namespace sync succeeded")
	}
	if err := h.OnSyncRuntimeBindings(context.Background(), protocol.SyncRuntimeBindings{
		Namespace: "team-a", Revision: 44, Bindings: map[string]string{"bad": "secret-value"},
	}); err == nil {
		t.Fatal("invalid key sync succeeded")
	}
}

func TestRealHandlerSyncRuntimeBindingsWaitsForGitOpsSecret(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: protocol.RuntimeBindingsSecretName, Namespace: "team-a",
	}}
	k8s := fake.NewSimpleClientset(secret)
	gets := 0
	k8s.Fake.PrependReactor("get", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 1 {
			return true, nil, apierrors.NewNotFound(corev1.Resource("secrets"), protocol.RuntimeBindingsSecretName)
		}
		return false, nil, nil
	})
	h := &RealHandler{K8s: k8s, WorkloadNamespace: "team-a", Log: discardLogger()}
	if err := h.OnSyncRuntimeBindings(context.Background(), protocol.SyncRuntimeBindings{
		Namespace: "team-a", Revision: 1, Bindings: map[string]string{"API_TOKEN": "write-only"},
	}); err != nil {
		t.Fatalf("OnSyncRuntimeBindings: %v", err)
	}
	if gets < 2 {
		t.Fatalf("Secret GET calls = %d, want retry after rollout race", gets)
	}
}

func TestRealHandlerOnDeleteJob(t *testing.T) {
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: "delme", Namespace: "default",
			Labels: map[string]string{"yscale.sh/workload-id": "wl_xyz"},
		},
	}
	k8s := fake.NewSimpleClientset(&job)

	h := NewRealHandler(k8s, nil, nil)
	if err := h.OnDeleteJob(context.Background(), protocol.DeleteJob{
		WorkloadID: "wl_xyz", Namespace: "default",
	}); err != nil {
		t.Fatalf("OnDeleteJob: %v", err)
	}
	if _, err := k8s.BatchV1().Jobs("default").Get(context.Background(), "delme", metav1.GetOptions{}); err == nil {
		t.Errorf("job should have been deleted")
	}
}

func TestRealHandlerOnDeleteJobMissing(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	h := NewRealHandler(k8s, nil, nil)
	if err := h.OnDeleteJob(context.Background(), protocol.DeleteJob{
		WorkloadID: "ghost", Namespace: "default",
	}); err != nil {
		t.Errorf("missing job should not error: %v", err)
	}
}

func TestRealHandlerFetchesOnlyStoredWorkloadLogsAndClampsTail(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "console-job", Namespace: "jobs",
		Labels: map[string]string{labelWorkloadID: "wl_console"},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "console-job-abc", Namespace: "jobs",
		Labels: map[string]string{"batch.kubernetes.io/job-name": "console-job"},
	}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}, {Name: "metrics"}}}}
	otherJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "other-job", Namespace: "jobs",
		Labels: map[string]string{labelWorkloadID: "wl_other"},
	}}
	otherPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "other-job-abc", Namespace: "jobs",
		Labels: map[string]string{"batch.kubernetes.io/job-name": "other-job"},
	}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}}}
	k8s := fake.NewSimpleClientset(job, pod, otherJob, otherPod)
	h := NewRealHandler(k8s, nil, nil)
	var opened []string
	h.logStream = func(_ context.Context, namespace, pod string, options *corev1.PodLogOptions) (io.ReadCloser, error) {
		if namespace != "jobs" || options.TailLines == nil || *options.TailLines != maxLogTailLines || !options.Timestamps {
			t.Fatalf("log request = namespace:%q options:%+v", namespace, options)
		}
		opened = append(opened, pod+"/"+options.Container)
		return io.NopCloser(strings.NewReader("2026-08-14T00:00:00Z ready\n")), nil
	}

	got, err := h.OnFetchWorkloadLogs(context.Background(), protocol.FetchWorkloadLogs{
		WorkloadID: "wl_console", Namespace: "jobs", TailLines: 5000,
	})
	if err != nil {
		t.Fatalf("OnFetchWorkloadLogs: %v", err)
	}
	if strings.Join(opened, ",") != "console-job-abc/main,console-job-abc/metrics" {
		t.Fatalf("opened = %v, want only the selected workload's containers", opened)
	}
	if len(got.Streams) != 2 || got.Streams[0].Pod != "console-job-abc" || got.Streams[0].Output == "" || got.Truncated {
		t.Fatalf("logs = %+v", got)
	}
}

func TestRealHandlerBoundsWorkloadLogBytes(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "bounded", Namespace: "jobs", Labels: map[string]string{labelWorkloadID: "wl_bounded"},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "bounded-abc", Namespace: "jobs", Labels: map[string]string{"batch.kubernetes.io/job-name": "bounded"},
	}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}}}
	h := NewRealHandler(fake.NewSimpleClientset(job, pod), nil, nil)
	h.logStream = func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(strings.Repeat("x", 64))), nil
	}

	got, err := h.OnFetchWorkloadLogs(context.Background(), protocol.FetchWorkloadLogs{
		WorkloadID: "wl_bounded", Namespace: "jobs", TailLines: 10, MaxBytes: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Truncated || len(got.Streams) != 1 || len(got.Streams[0].Output) != 16 {
		t.Fatalf("bounded logs = %+v, want one 16-byte truncated stream", got)
	}
}

func TestRealHandlerWorkloadLogsMissingIsEmpty(t *testing.T) {
	h := NewRealHandler(fake.NewSimpleClientset(), nil, nil)
	got, err := h.OnFetchWorkloadLogs(context.Background(), protocol.FetchWorkloadLogs{
		WorkloadID: "wl_missing", Namespace: "jobs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Streams == nil || len(got.Streams) != 0 || got.ObservedAt.IsZero() {
		t.Fatalf("missing result = %+v, want a timestamped empty snapshot", got)
	}
}
