package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/yscale-sh/yscale/pkg/workload"
)

var (
	testComputeUID  = types.UID("compute-uid")
	testUploaderUID = types.UID("uploader-uid")
)

// artifactComputeJob is a succeeded workload Job that configured artifact
// export: the annotations the uploader is built from, and a staged host path
// under the artifact root so the uploader Job can be rendered.
func artifactComputeJob(t *testing.T, wlID string, uid types.UID) *batchv1.Job {
	t.Helper()
	artifactJSON, err := json.Marshal([]workload.ArtifactSpec{{
		Name: "results", Target: "/outputs", MaxFiles: 10, MaxSizeGB: 1,
		To: workload.BucketRef{Bucket: "runs", CredentialsSecret: "r2-creds"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	job := terminalJob("train", wlID, batchv1.JobComplete)
	job.UID = uid
	job.Annotations = map[string]string{
		workload.AnnotationArtifacts:              string(artifactJSON),
		workload.AnnotationCacheBurstID:           "burst_x",
		workload.AnnotationCacheBootstrapEndpoint: "http://yscale-agent:8080",
	}
	hostPathType := corev1.HostPathDirectoryOrCreate
	job.Spec.Template.Spec.Volumes = []corev1.Volume{{
		Name: "artifact-0",
		VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: "/var/lib/yscale/artifacts/0123456789abcdef0123456789abcdef", Type: &hostPathType,
		}},
	}}
	return job
}

func succeededComputePod(jobUID types.UID) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "train-pod", Namespace: "default",
			Labels:          map[string]string{"batch.kubernetes.io/job-name": "train"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Job", UID: jobUID, Controller: &controller}},
		},
		Spec:   corev1.PodSpec{NodeName: "burst-node"},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
}

func newArtifactWatcher(k8s kubernetes.Interface, central WorkloadCompleter) *CompletionWatcher {
	cw := NewCompletionWatcher(k8s, nil, central, nil)
	cw.ArtifactImage = "ghcr.io/yscale/agent@sha256:" + strings.Repeat("a", 64)
	cw.ArtifactNamespace = "yscale"
	cw.ArtifactTimeout = 5 * time.Second
	return cw
}

// finishedUploaderJob is the uploader Job the watcher would have created,
// already terminal. Pre-creating it under the deterministic name exercises the
// same reuse path a resync takes: Create returns AlreadyExists and the watcher
// reads back the Job that already ran.
func finishedUploaderJob(wlID string, computeUID, uploaderUID types.UID, cond batchv1.JobConditionType) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: artifactUploadJobName(wlID, computeUID), Namespace: "yscale", UID: uploaderUID,
			Annotations: map[string]string{
				"yscale.sh/artifact-workload-id": wlID,
				"yscale.sh/artifact-source-job":  string(computeUID),
			},
		},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: cond, Status: corev1.ConditionTrue}}},
	}
}

func uploaderPod(name, jobName string, jobUID types.UID, exitCode int32, finishedAt time.Time, message string) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "yscale",
			Labels:          map[string]string{"batch.kubernetes.io/job-name": jobName},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Job", UID: jobUID, Controller: &controller}},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: artifactUploaderContainer,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: exitCode, FinishedAt: metav1.NewTime(finishedAt), Message: message,
			}},
		}}},
	}
}

// exportReceipt drives one finished uploader Job through the completion path
// and returns the receipt central was sent.
func exportReceipt(t *testing.T, wlID string, cond batchv1.JobConditionType, pods ...*corev1.Pod) *WorkloadOutcome {
	t.Helper()
	return uploaderReceipt(t, wlID, finishedUploaderJob(wlID, testComputeUID, testUploaderUID, cond), pods...)
}

// uploaderReceipt drives one already-terminal uploader Job through the
// completion path and returns the receipt central was sent. Callers that need
// the Job controller's own condition reason build the Job themselves.
func uploaderReceipt(t *testing.T, wlID string, uploader *batchv1.Job, pods ...*corev1.Pod) *WorkloadOutcome {
	t.Helper()
	job := artifactComputeJob(t, wlID, testComputeUID)
	objects := []runtime.Object{succeededComputePod(testComputeUID), uploader}
	for _, pod := range pods {
		objects = append(objects, pod)
	}
	central := &fakeCompleter{}
	newArtifactWatcher(fake.NewSimpleClientset(objects...), central).
		finishWithArtifacts(context.Background(), job, wlID)

	return central.onlyReceipt(t, wlID+"/"+jobPhase(uploader))
}

// failedUploaderJob is an uploader Job the controller has already failed, with
// the terminal condition reason it recorded for doing so.
func failedUploaderJob(wlID, conditionReason string) *batchv1.Job {
	uploader := finishedUploaderJob(wlID, testComputeUID, testUploaderUID, batchv1.JobFailed)
	uploader.Status.Conditions[0].Reason = conditionReason
	return uploader
}

// uploaderJobName is the deterministic name a test's pods must be labeled with
// to belong to the uploader Job the watcher will find.
func uploaderJobName(wlID string) string {
	return artifactUploadJobName(wlID, testComputeUID)
}

func wantCounts(t *testing.T, got *ArtifactOutcome, objects, bytes int64) {
	t.Helper()
	if got == nil || got.ObjectsUploaded == nil || got.BytesUploaded == nil {
		t.Fatalf("artifact counts are absent: %+v", got)
	}
	if *got.ObjectsUploaded != objects || *got.BytesUploaded != bytes {
		t.Fatalf("counts = %d objects / %d bytes, want %d / %d",
			*got.ObjectsUploaded, *got.BytesUploaded, objects, bytes)
	}
}

// wantNoCounts asserts the export was reported at the Job's own verdict with
// nothing invented to fill the counts in.
func wantNoCounts(t *testing.T, got *ArtifactOutcome, result, reason string) {
	t.Helper()
	if got == nil || got.Result != result {
		t.Fatalf("artifacts = %+v, want %s", got, result)
	}
	if got.ObjectsUploaded != nil || got.BytesUploaded != nil {
		t.Fatalf("counts were invented: %+v", got)
	}
	if got.Reason != reason {
		t.Fatalf("artifact reason = %q, want %q", got.Reason, reason)
	}
}

// A succeeded compute plus a succeeded export reports both, with the counts the
// uploader actually completed.
func TestArtifactReceiptCarriesUploaderCounts(t *testing.T) {
	got := exportReceipt(t, "wl_a", batchv1.JobComplete,
		uploaderPod("upload-0", uploaderJobName("wl_a"), testUploaderUID, 0, time.Now(),
			`{"result":"succeeded","objects_uploaded":3,"bytes_uploaded":4096}`))

	if got.Compute.Result != OutcomeResultSucceeded {
		t.Errorf("compute = %+v, want succeeded", got.Compute)
	}
	if got.Artifacts == nil || got.Artifacts.Result != OutcomeResultSucceeded || got.Artifacts.Reason != "" {
		t.Fatalf("artifacts = %+v, want succeeded with no reason", got.Artifacts)
	}
	wantCounts(t, got.Artifacts, 3, 4096)
}

// An export that ran and moved nothing is a real observation. Its zeros must
// arrive as zeros, not as absent counts — "uploaded nothing" and "did not
// count" are different facts on a record the customer reads back.
func TestArtifactReceiptCarriesExplicitZeroCounts(t *testing.T) {
	got := exportReceipt(t, "wl_zero", batchv1.JobComplete,
		uploaderPod("upload-0", uploaderJobName("wl_zero"), testUploaderUID, 0, time.Now(),
			`{"result":"succeeded","objects_uploaded":0,"bytes_uploaded":0}`))

	wantCounts(t, got.Artifacts, 0, 0)
}

// The honest case the receipt exists for: the Job did its work, the export did
// not. The phase stays Failed so the Workload CR and the run verdict stay
// failed, and the compute result is never collapsed into that failure.
func TestArtifactReceiptKeepsComputeSucceededWhenExportFails(t *testing.T) {
	got := exportReceipt(t, "wl_partial", batchv1.JobFailed,
		uploaderPod("upload-0", uploaderJobName("wl_partial"), testUploaderUID, 1, time.Now(),
			`{"result":"failed","reason":"upload","objects_uploaded":2,"bytes_uploaded":128}`))

	if got.Compute.Result != OutcomeResultSucceeded {
		t.Fatalf("compute = %+v, want succeeded — a lost upload is not a failed job", got.Compute)
	}
	if got.Artifacts == nil || got.Artifacts.Result != OutcomeResultFailed {
		t.Fatalf("artifacts = %+v, want failed", got.Artifacts)
	}
	if got.Artifacts.Reason != artifactFailureReasons[artifactFailureUpload] {
		t.Errorf("artifact reason = %q, want the upload-category phrase", got.Artifacts.Reason)
	}
	wantCounts(t, got.Artifacts, 2, 128)
}

// A failure the watcher hits before any uploader Job exists still reports the
// export honestly: failed, categorised, and with no counts at all.
func TestArtifactReceiptReportsPreflightFailureWithoutCounts(t *testing.T) {
	job := artifactComputeJob(t, "wl_cfg", testComputeUID)
	job.Annotations[workload.AnnotationCacheBurstID] = ""
	central := &fakeCompleter{}

	newArtifactWatcher(fake.NewSimpleClientset(), central).
		finishWithArtifacts(context.Background(), job, "wl_cfg")

	got := central.onlyReceipt(t, "wl_cfg/Failed")
	if got.Compute.Result != OutcomeResultSucceeded {
		t.Errorf("compute = %+v, want succeeded", got.Compute)
	}
	wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureConfiguration])
}

// The compute pod the export would be staged from is looked up before any
// uploader Job is created, so a lookup that fails cannot be a verdict the
// uploader gave — there is no uploader. The receipt has to say the export could
// not be prepared; a customer told the uploader Job failed goes looking for a
// Job that never existed.
func TestArtifactReceiptCategorisesAMissingWorkloadPodAsPreparation(t *testing.T) {
	cases := []struct {
		name string
		k8s  func() *fake.Clientset
	}{
		{"no succeeded workload pod", func() *fake.Clientset { return fake.NewSimpleClientset() }},
		{"the connector may not list pods", func() *fake.Clientset {
			k8s := fake.NewSimpleClientset(succeededComputePod(testComputeUID))
			k8s.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(corev1.Resource("pods"), "", errors.New("no list access"))
			})
			return k8s
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k8s := tc.k8s()
			central := &fakeCompleter{}
			newArtifactWatcher(k8s, central).finishWithArtifacts(context.Background(),
				artifactComputeJob(t, "wl_nopod", testComputeUID), "wl_nopod")

			got := central.onlyReceipt(t, "wl_nopod/Failed")
			if got.Compute.Result != OutcomeResultSucceeded {
				t.Errorf("compute = %+v, want succeeded", got.Compute)
			}
			// The configuration phrase, which is not the uploader-Job one.
			wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureConfiguration])

			jobs, err := k8s.BatchV1().Jobs("yscale").List(context.Background(), metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs.Items) != 0 {
				t.Fatalf("uploader Jobs = %d, want none created before the pod lookup", len(jobs.Items))
			}
		})
	}
}

// A failed uploader Job with no summary the watcher can vouch for is still
// described by the one account it does trust: the Job controller's own terminal
// condition reason. The uploader Job carries an activeDeadlineSeconds, so a run
// the controller killed there ran out of time — calling it a failed uploader
// sends the customer hunting for an error the uploader never got to write.
func TestArtifactReceiptFallsBackToTimeoutOnDeadlineExceeded(t *testing.T) {
	// Every case here pre-creates the terminal uploader Job under the
	// deterministic name, which is the shape a resync meets: Create returns
	// AlreadyExists and the watcher reads back the Job that already ran.
	t.Run("no attempt left a summary", func(t *testing.T) {
		got := uploaderReceipt(t, "wl_dl", failedUploaderJob("wl_dl", jobReasonDeadlineExceeded))
		wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureTimeout])
	})

	t.Run("the only summary cannot be vouched for", func(t *testing.T) {
		got := uploaderReceipt(t, "wl_dl", failedUploaderJob("wl_dl", jobReasonDeadlineExceeded),
			uploaderPod("upload-0", uploaderJobName("wl_dl"), testUploaderUID, 1, time.Now(), "{not json"))
		wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureTimeout])
	})

	// A transient central failure re-drives the whole completion against the Job
	// that already finished. The category is read off that Job every time, so
	// the second report says what the first one would have.
	t.Run("re-driven after a failed report", func(t *testing.T) {
		job := artifactComputeJob(t, "wl_dl_resync", testComputeUID)
		k8s := fake.NewSimpleClientset(succeededComputePod(testComputeUID),
			failedUploaderJob("wl_dl_resync", jobReasonDeadlineExceeded))
		central := &fakeCompleter{}
		central.setDoneErr(errors.New("central unreachable (transient)"))
		cw := newArtifactWatcher(k8s, central)

		cw.finishWithArtifacts(context.Background(), job, "wl_dl_resync")
		central.setDoneErr(nil) // central recovers; the resync re-drives the report
		cw.finishWithArtifacts(context.Background(), job, "wl_dl_resync")

		got := central.onlyReceipt(t, "wl_dl_resync/Failed")
		wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureTimeout])
		jobs, err := k8s.BatchV1().Jobs("yscale").List(context.Background(), metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs.Items) != 1 {
			t.Fatalf("uploader Jobs = %d, want the one that already ran", len(jobs.Items))
		}
	})
}

// Every other terminal condition reason — and a controller that recorded none —
// stays the Job-level failure it has always been. The timeout category is
// earned by the deadline reason alone, not by any summary-less failure.
func TestArtifactReceiptFallsBackToJobFailureForOrdinaryFailures(t *testing.T) {
	for _, reason := range []string{"BackoffLimitExceeded", "PodFailurePolicy", ""} {
		name := reason
		if name == "" {
			name = "no reason recorded"
		}
		t.Run(name, func(t *testing.T) {
			got := uploaderReceipt(t, "wl_jobfail", failedUploaderJob("wl_jobfail", reason))
			wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureJob])
		})
	}
}

// A termination message the watcher cannot vouch for costs the receipt its
// counts and nothing else: the Job's own verdict is still reported, with a safe
// generic note in place of the summary that never arrived.
func TestArtifactReceiptIgnoresUntrustworthySummaries(t *testing.T) {
	name := uploaderJobName("wl_x")
	now := time.Now()

	t.Run("malformed json", func(t *testing.T) {
		got := exportReceipt(t, "wl_x", batchv1.JobComplete,
			uploaderPod("p", name, testUploaderUID, 0, now, "{not json"))
		wantNoCounts(t, got.Artifacts, OutcomeResultSucceeded, missingArtifactSummaryReason)
	})

	t.Run("unknown field", func(t *testing.T) {
		got := exportReceipt(t, "wl_x", batchv1.JobComplete,
			uploaderPod("p", name, testUploaderUID, 0, now,
				`{"result":"succeeded","objects_uploaded":1,"bytes_uploaded":1,"signed_url":"https://x"}`))
		wantNoCounts(t, got.Artifacts, OutcomeResultSucceeded, missingArtifactSummaryReason)
	})

	t.Run("trailing json value", func(t *testing.T) {
		got := exportReceipt(t, "wl_x", batchv1.JobComplete,
			uploaderPod("p", name, testUploaderUID, 0, now,
				`{"result":"succeeded","objects_uploaded":1,"bytes_uploaded":1}{"result":"succeeded"}`))
		wantNoCounts(t, got.Artifacts, OutcomeResultSucceeded, missingArtifactSummaryReason)
	})

	t.Run("no pod at all", func(t *testing.T) {
		got := exportReceipt(t, "wl_x", batchv1.JobComplete)
		wantNoCounts(t, got.Artifacts, OutcomeResultSucceeded, missingArtifactSummaryReason)
	})

	t.Run("pod this job does not control", func(t *testing.T) {
		got := exportReceipt(t, "wl_x", batchv1.JobComplete,
			uploaderPod("p", name, "someone-elses-job", 0, now,
				`{"result":"succeeded","objects_uploaded":99,"bytes_uploaded":99}`))
		wantNoCounts(t, got.Artifacts, OutcomeResultSucceeded, missingArtifactSummaryReason)
	})

	t.Run("container that is not the uploader", func(t *testing.T) {
		pod := uploaderPod("p", name, testUploaderUID, 0, now,
			`{"result":"succeeded","objects_uploaded":99,"bytes_uploaded":99}`)
		pod.Status.ContainerStatuses[0].Name = "sidecar"
		got := exportReceipt(t, "wl_x", batchv1.JobComplete, pod)
		wantNoCounts(t, got.Artifacts, OutcomeResultSucceeded, missingArtifactSummaryReason)
	})

	t.Run("success claimed by a failed attempt", func(t *testing.T) {
		got := exportReceipt(t, "wl_x", batchv1.JobFailed,
			uploaderPod("p", name, testUploaderUID, 1, now,
				`{"result":"succeeded","objects_uploaded":50,"bytes_uploaded":500}`))
		wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureJob])
	})

	t.Run("counts below zero", func(t *testing.T) {
		got := exportReceipt(t, "wl_x", batchv1.JobFailed,
			uploaderPod("p", name, testUploaderUID, 1, now,
				`{"result":"failed","reason":"upload","objects_uploaded":-1,"bytes_uploaded":-1}`))
		wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureJob])
	})

	t.Run("oversized message", func(t *testing.T) {
		got := exportReceipt(t, "wl_x", batchv1.JobComplete,
			uploaderPod("p", name, testUploaderUID, 0, now,
				`{"result":"succeeded","objects_uploaded":1,"bytes_uploaded":1}`+
					strings.Repeat(" ", maxArtifactSummaryBytes)))
		wantNoCounts(t, got.Artifacts, OutcomeResultSucceeded, missingArtifactSummaryReason)
	})

	t.Run("category nobody allowlisted", func(t *testing.T) {
		got := exportReceipt(t, "wl_x", batchv1.JobFailed,
			uploaderPod("p", name, testUploaderUID, 1, now,
				`{"result":"failed","reason":"https://signed.example/put?sig=abc","objects_uploaded":1,"bytes_uploaded":1}`))
		if got.Artifacts.Reason != genericArtifactFailureReason {
			t.Fatalf("artifact reason = %q, want the generic phrase", got.Artifacts.Reason)
		}
		wantCounts(t, got.Artifacts, 1, 1)
	})
}

// A retried uploader Job leaves one pod per attempt. A completed Job is
// described by the attempt that exited 0 — never by the failed retry that
// preceded it.
func TestArtifactReceiptPicksSuccessfulUploaderAttempt(t *testing.T) {
	name := uploaderJobName("wl_retry")
	first := time.Now().Add(-2 * time.Minute)
	got := exportReceipt(t, "wl_retry", batchv1.JobComplete,
		uploaderPod("upload-0", name, testUploaderUID, 1, first,
			`{"result":"failed","reason":"upload","objects_uploaded":1,"bytes_uploaded":10}`),
		uploaderPod("upload-1", name, testUploaderUID, 0, first.Add(time.Minute),
			`{"result":"succeeded","objects_uploaded":5,"bytes_uploaded":500}`))

	if got.Artifacts == nil || got.Artifacts.Result != OutcomeResultSucceeded {
		t.Fatalf("artifacts = %+v, want succeeded", got.Artifacts)
	}
	wantCounts(t, got.Artifacts, 5, 500)
}

// A Job that exhausted its retries is described by its most recent attempt —
// the one that decided it — not by whichever pod the API server listed first.
func TestArtifactReceiptPicksMostRecentFailedAttempt(t *testing.T) {
	name := uploaderJobName("wl_lastfail")
	first := time.Now().Add(-2 * time.Minute)
	got := exportReceipt(t, "wl_lastfail", batchv1.JobFailed,
		uploaderPod("upload-1", name, testUploaderUID, 1, first.Add(time.Minute),
			`{"result":"failed","reason":"signer","objects_uploaded":0,"bytes_uploaded":0}`),
		uploaderPod("upload-0", name, testUploaderUID, 1, first,
			`{"result":"failed","reason":"upload","objects_uploaded":4,"bytes_uploaded":40}`))

	if got.Artifacts.Reason != artifactFailureReasons[artifactFailureSigner] {
		t.Errorf("artifact reason = %q, want the signer phrase from the last attempt", got.Artifacts.Reason)
	}
	wantCounts(t, got.Artifacts, 0, 0)
}

// A transient central failure must not re-run the export. The completion is
// retried against the uploader Job that already finished, reports the same
// counts it would have reported the first time, and is then final.
func TestArtifactReceiptRetryReusesFinishedUploaderJob(t *testing.T) {
	job := artifactComputeJob(t, "wl_reuse", testComputeUID)
	uploader := finishedUploaderJob("wl_reuse", testComputeUID, testUploaderUID, batchv1.JobComplete)
	k8s := fake.NewSimpleClientset(succeededComputePod(testComputeUID), uploader,
		uploaderPod("upload-0", uploader.Name, testUploaderUID, 0, time.Now(),
			`{"result":"succeeded","objects_uploaded":7,"bytes_uploaded":700}`))
	central := &fakeCompleter{}
	central.setDoneErr(errors.New("central unreachable (transient)"))
	cw := newArtifactWatcher(k8s, central)

	cw.finishWithArtifacts(context.Background(), job, "wl_reuse")
	if got := central.doneCalls(); len(got) != 0 {
		t.Fatalf("failed report recorded %v, want nothing", got)
	}

	central.setDoneErr(nil) // central recovers; the resync re-drives the report
	cw.finishWithArtifacts(context.Background(), job, "wl_reuse")

	wantCounts(t, central.onlyReceipt(t, "wl_reuse/Succeeded").Artifacts, 7, 700)
	jobs, err := k8s.BatchV1().Jobs("yscale").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 || jobs.Items[0].UID != testUploaderUID {
		t.Fatalf("retry did not reuse the finished uploader Job: %d Jobs", len(jobs.Items))
	}

	// The recovered report is final: a further watch event must not re-report.
	cw.handle(context.Background(), job)
	if got := central.doneCalls(); len(got) != 1 {
		t.Errorf("done calls = %v, want exactly one", got)
	}
}

// readFailsWith builds a watcher whose reads of the uploader Job run endWait
// and then fail with getErr, so the wait is already over by the time the error
// comes back — the ordering a real client produces when the context it is
// carrying ends mid-request. Nothing is pre-created under the uploader's name,
// so Create succeeds and the failing read is the one inside the wait loop.
func readFailsWith(endWait func(), getErr error) (*CompletionWatcher, *fakeCompleter) {
	k8s := fake.NewSimpleClientset(succeededComputePod(testComputeUID))
	k8s.PrependReactor("get", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
		endWait()
		return true, nil, getErr
	})
	central := &fakeCompleter{}
	return newArtifactWatcher(k8s, central), central
}

// A read carrying the wait's own context fails the moment that context ends, so
// the deadline and the cancellation surface through it as readily as through
// the select that follows it. Which side notices is an accident of timing, and
// the receipt must not turn on it: neither is a verdict the uploader gave.
func TestArtifactReceiptCategorisesAReadThatOutlivedItsWait(t *testing.T) {
	t.Run("the export ran past its deadline", func(t *testing.T) {
		cw, central := readFailsWith(func() { time.Sleep(20 * time.Millisecond) }, context.DeadlineExceeded)
		cw.ArtifactTimeout = time.Millisecond

		cw.finishWithArtifacts(context.Background(),
			artifactComputeJob(t, "wl_deadline", testComputeUID), "wl_deadline")

		got := central.onlyReceipt(t, "wl_deadline/Failed")
		wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureTimeout])
	})

	t.Run("the agent stopped waiting", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cw, central := readFailsWith(cancel, context.Canceled)

		cw.finishWithArtifacts(ctx, artifactComputeJob(t, "wl_cancel", testComputeUID), "wl_cancel")

		got := central.onlyReceipt(t, "wl_cancel/Failed")
		wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureInterrupted])
	})
}

// An API error on a wait that is still live is the Job-level failure it has
// always been: the agent asked, the answer never came, and no deadline of its
// own explains it.
func TestArtifactReceiptReportsAnOrdinaryReadFailureAsAJobFailure(t *testing.T) {
	cw, central := readFailsWith(func() {},
		apierrors.NewInternalError(errors.New("etcdserver: request timed out")))

	cw.finishWithArtifacts(context.Background(),
		artifactComputeJob(t, "wl_apierror", testComputeUID), "wl_apierror")

	got := central.onlyReceipt(t, "wl_apierror/Failed")
	wantNoCounts(t, got.Artifacts, OutcomeResultFailed, artifactFailureReasons[artifactFailureJob])
}

// Nothing the customer's own spec named — bucket, secret reference, endpoint,
// staged path, node — may appear in a receipt. The reasons are a closed
// vocabulary precisely so this holds by construction.
func TestArtifactReceiptCarriesNoSensitiveMaterial(t *testing.T) {
	got := exportReceipt(t, "wl_safe", batchv1.JobFailed,
		uploaderPod("upload-0", uploaderJobName("wl_safe"), testUploaderUID, 1, time.Now(),
			`{"result":"failed","reason":"upload","objects_uploaded":1,"bytes_uploaded":1}`))

	encoded, err := json.Marshal(completeRequest{Phase: "Failed", Outcome: got})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"runs", "r2-creds", "yscale-agent", "burst_x", "burst-node", "/var/lib/yscale", "http"} {
		if strings.Contains(string(encoded), secret) {
			t.Errorf("receipt leaked %q: %s", secret, encoded)
		}
	}
}
