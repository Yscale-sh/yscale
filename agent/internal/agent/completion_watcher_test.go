package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// fakeCompleter records ReportWorkloadDone / ReportWorkloadStarted calls so
// tests can assert what the completion watcher reported to central.
type fakeCompleter struct {
	mu         sync.Mutex
	done       []string           // "workloadID/phase"
	outcomes   []*WorkloadOutcome // receipt sent alongside each done call
	started    []string           // workloadID
	startedErr error              // when non-nil, ReportWorkloadStarted fails (and records nothing)
	doneErr    error              // when non-nil, ReportWorkloadDone fails (and records nothing)
}

func (f *fakeCompleter) ReportWorkloadDone(_ context.Context, wlID, phase string, outcome *WorkloadOutcome) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.doneErr != nil {
		return f.doneErr
	}
	f.done = append(f.done, wlID+"/"+phase)
	f.outcomes = append(f.outcomes, outcome)
	return nil
}

func (f *fakeCompleter) receipts() []*WorkloadOutcome {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*WorkloadOutcome(nil), f.outcomes...)
}

// onlyReceipt returns the single receipt central was sent, failing the test
// when the workload was reported any other number of times.
func (f *fakeCompleter) onlyReceipt(t *testing.T, wantCall string) *WorkloadOutcome {
	t.Helper()
	calls, got := f.doneCalls(), f.receipts()
	if len(calls) != 1 || calls[0] != wantCall {
		t.Fatalf("done calls = %v, want [%s]", calls, wantCall)
	}
	if got[0] == nil {
		t.Fatal("completion carried no outcome receipt")
	}
	return got[0]
}

func (f *fakeCompleter) setDoneErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.doneErr = err
}

func (f *fakeCompleter) ReportWorkloadStarted(_ context.Context, wlID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startedErr != nil {
		return f.startedErr
	}
	f.started = append(f.started, wlID)
	return nil
}

func (f *fakeCompleter) setStartedErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startedErr = err
}

func (f *fakeCompleter) startedCount(wlID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, id := range f.started {
		if id == wlID {
			n++
		}
	}
	return n
}

func (f *fakeCompleter) doneCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.done...)
}

// runningJob builds a yscale-labeled Job whose pods have actually begun
// running (Ready > 0) but that has no terminal condition.
func runningJob(name, wlID string) *batchv1.Job {
	now := metav1.NewTime(time.Now())
	ready := int32(1)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default",
			Labels: map[string]string{labelWorkloadID: wlID},
		},
		Status: batchv1.JobStatus{StartTime: &now, Active: 1, Ready: &ready},
	}
}

// terminalJob builds a yscale-labeled Job with the given terminal condition.
func terminalJob(name, wlID string, cond batchv1.JobConditionType) *batchv1.Job {
	now := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default",
			Labels: map[string]string{labelWorkloadID: wlID},
		},
		Status: batchv1.JobStatus{
			StartTime:  &now,
			Conditions: []batchv1.JobCondition{{Type: cond, Status: corev1.ConditionTrue}},
		},
	}
}

// A Job that has started but not finished must produce exactly one
// ReportWorkloadStarted — repeat watch events (Modified, resync replays) must
// not re-report — and no completion report.
func TestCompletionWatcherReportsStartedOnce(t *testing.T) {
	central := &fakeCompleter{}
	cw := NewCompletionWatcher(fake.NewSimpleClientset(), nil, central, nil)

	job := runningJob("wl-job", "wl_1")
	cw.handle(context.Background(), job)
	cw.handle(context.Background(), job) // watch re-delivery

	if n := central.startedCount("wl_1"); n != 1 {
		t.Errorf("ReportWorkloadStarted called %d times, want exactly 1", n)
	}
	if len(central.doneCalls()) != 0 {
		t.Errorf("running job must not report completion, got %v", central.doneCalls())
	}
}

func TestCompletionWatcherResyncScopesJobsToNamespace(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	cw := NewCompletionWatcher(k8s, nil, &fakeCompleter{}, nil)
	cw.Namespace = "tenant-a"

	cw.resync(context.Background())

	actions := k8s.Actions()
	if len(actions) != 1 {
		t.Fatalf("actions = %d, want 1", len(actions))
	}
	if got := actions[0].GetNamespace(); got != "tenant-a" {
		t.Fatalf("list namespace = %q, want tenant-a", got)
	}
}

// A Job whose pod is still Pending must NOT report started, even though the
// controller has already stamped StartTime and counts the pod in Active —
// the yscale Job is pushed while the burst node is still provisioning, so
// those fire minutes before work begins. Only Ready > 0 counts.
func TestCompletionWatcherIgnoresPendingJob(t *testing.T) {
	central := &fakeCompleter{}
	cw := NewCompletionWatcher(fake.NewSimpleClientset(), nil, central, nil)

	now := metav1.NewTime(time.Now())
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: "wl-job", Namespace: "default",
			Labels: map[string]string{labelWorkloadID: "wl_1"},
		},
		// Pod created but Unschedulable: StartTime set, Active=1, no Ready.
		Status: batchv1.JobStatus{StartTime: &now, Active: 1},
	}
	cw.handle(context.Background(), job)

	if n := central.startedCount("wl_1"); n != 0 {
		t.Errorf("pending job reported started %d times, want 0", n)
	}
}

// A terminal Job that was labeled by PendingPodWatcher for a NodeOnly burst
// reports completion exactly as before (no regression from the started path),
// and does not report started. With no artifact annotation the receipt is the
// compute result alone — an artifacts half would claim an export that was
// never in play.
func TestCompletionWatcherReportsNodeOnlyJobDone(t *testing.T) {
	central := &fakeCompleter{}
	cw := NewCompletionWatcher(fake.NewSimpleClientset(), nil, central, nil)

	cw.handle(context.Background(), terminalJob("nodeonly-job", "wl_nodeonly_job", batchv1.JobComplete))

	got := central.onlyReceipt(t, "wl_nodeonly_job/Succeeded")
	if got.Compute.Result != OutcomeResultSucceeded || got.Compute.Reason != "" {
		t.Errorf("compute = %+v, want succeeded with no reason", got.Compute)
	}
	if got.Artifacts != nil {
		t.Errorf("artifacts = %+v, want none", got.Artifacts)
	}
	if n := central.startedCount("wl_nodeonly_job"); n != 0 {
		t.Errorf("terminal job reported started %d times, want 0", n)
	}
}

// A failed Job reports a failed compute, and takes its reason from the Job
// controller's own condition reason — never from the condition MESSAGE, which
// a failing pod can steer.
func TestCompletionWatcherReportsFailedComputeReason(t *testing.T) {
	central := &fakeCompleter{}
	cw := NewCompletionWatcher(fake.NewSimpleClientset(), nil, central, nil)

	job := terminalJob("wl-job", "wl_1", batchv1.JobFailed)
	job.Status.Conditions[0].Reason = "BackoffLimitExceeded"
	job.Status.Conditions[0].Message = "https://signed.example/put?sig=abc leaked into the message"
	cw.handle(context.Background(), job)

	got := central.onlyReceipt(t, "wl_1/Failed")
	if got.Compute.Result != OutcomeResultFailed {
		t.Errorf("compute result = %q, want failed", got.Compute.Result)
	}
	if got.Compute.Reason != computeFailureReasons["BackoffLimitExceeded"] {
		t.Errorf("compute reason = %q, want the allowlisted phrase", got.Compute.Reason)
	}
}

// An unrecognised condition reason means the agent cannot honestly say why the
// Job failed, so it says nothing rather than forwarding whatever text the
// controller (or the pod behind it) put there.
func TestCompletionWatcherOmitsUnknownComputeReason(t *testing.T) {
	central := &fakeCompleter{}
	cw := NewCompletionWatcher(fake.NewSimpleClientset(), nil, central, nil)

	job := terminalJob("wl-job", "wl_2", batchv1.JobFailed)
	job.Status.Conditions[0].Reason = "SomethingNobodyAllowlisted"
	cw.handle(context.Background(), job)

	if got := central.onlyReceipt(t, "wl_2/Failed").Compute; got.Reason != "" {
		t.Errorf("compute reason = %q, want it omitted", got.Reason)
	}
}

// A workload that configured artifact export but whose compute FAILED reports
// the export as skipped — with no counts, positive or otherwise — and never
// starts an uploader: there is nothing worth uploading.
func TestCompletionWatcherSkipsArtifactsWhenComputeFails(t *testing.T) {
	central := &fakeCompleter{}
	k8s := fake.NewSimpleClientset()
	cw := newArtifactWatcher(k8s, central)

	job := artifactComputeJob(t, "wl_skip", "compute-uid")
	job.Status.Conditions = []batchv1.JobCondition{{
		Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded",
	}}
	cw.handle(context.Background(), job)

	got := central.onlyReceipt(t, "wl_skip/Failed")
	if got.Compute.Result != OutcomeResultFailed {
		t.Errorf("compute result = %q, want failed", got.Compute.Result)
	}
	if got.Artifacts == nil || got.Artifacts.Result != OutcomeResultSkipped {
		t.Fatalf("artifacts = %+v, want skipped", got.Artifacts)
	}
	if got.Artifacts.ObjectsUploaded != nil || got.Artifacts.BytesUploaded != nil {
		t.Errorf("skipped export reported counts: %+v", got.Artifacts)
	}
	jobs, err := k8s.BatchV1().Jobs("yscale").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Errorf("failed compute started %d uploader Jobs, want 0", len(jobs.Items))
	}
}

// A terminal Job whose completion report fails must be retried by the resync
// backstop (not left to linger until the k8s TTL/budget watchdog): the first
// resync fails to report, a later resync after central recovers reports exactly
// once, and it never double-reports.
func TestCompletionWatcherResyncRetriesFailedDone(t *testing.T) {
	central := &fakeCompleter{}
	central.setDoneErr(errors.New("central unreachable (transient)"))
	k8s := fake.NewSimpleClientset(terminalJob("wl-job", "wl_1", batchv1.JobComplete))
	cw := NewCompletionWatcher(k8s, nil, central, nil)

	cw.resync(context.Background()) // report fails → nothing recorded, not marked reported
	if got := central.doneCalls(); len(got) != 0 {
		t.Fatalf("failed resync recorded %v, want no completion", got)
	}

	central.setDoneErr(nil) // central recovers
	cw.resync(context.Background())
	cw.resync(context.Background()) // subsequent resync must not double-report
	if got := central.doneCalls(); len(got) != 1 || got[0] != "wl_1/Succeeded" {
		t.Errorf("done calls after recovery = %v, want [wl_1/Succeeded]", got)
	}
}

// A failed started report must be retried on the next watch event: the id is
// only marked reported on success.
func TestCompletionWatcherRetriesStartedOnFailure(t *testing.T) {
	central := &fakeCompleter{}
	central.setStartedErr(errors.New("central unreachable (transient)"))
	cw := NewCompletionWatcher(fake.NewSimpleClientset(), nil, central, nil)

	job := runningJob("wl-job", "wl_1")
	cw.handle(context.Background(), job)
	if n := central.startedCount("wl_1"); n != 0 {
		t.Fatalf("failed report recorded %d started calls, want 0", n)
	}

	// Central recovers; the next event must retry and succeed exactly once.
	central.setStartedErr(nil)
	cw.handle(context.Background(), job)
	cw.handle(context.Background(), job)
	if n := central.startedCount("wl_1"); n != 1 {
		t.Errorf("ReportWorkloadStarted called %d times after recovery, want exactly 1", n)
	}
}

func TestCompletionWatcherGatesSuccessOnArtifactUploaderJob(t *testing.T) {
	jobUID := types.UID("job-uid")
	job := artifactComputeJob(t, "wl_artifact", jobUID)
	pod := succeededComputePod(jobUID)
	pod.Spec.Tolerations = []corev1.Toleration{{Key: "yscale.sh/burst"}}
	k8s := fake.NewSimpleClientset(pod)
	central := &fakeCompleter{}
	cw := newArtifactWatcher(k8s, central)

	cw.handle(context.Background(), job)
	var uploader *batchv1.Job
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		jobs, listErr := k8s.BatchV1().Jobs("yscale").List(context.Background(), metav1.ListOptions{})
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(jobs.Items) == 1 {
			uploader = jobs.Items[0].DeepCopy()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if uploader == nil {
		t.Fatal("artifact uploader Job was not created")
	}
	if got := central.doneCalls(); len(got) != 0 {
		t.Fatalf("workload reported complete before artifact upload: %v", got)
	}
	if uploader.Spec.Template.Spec.NodeName != "burst-node" || len(uploader.Spec.Template.Spec.Volumes) != 1 || !uploader.Spec.Template.Spec.Containers[0].VolumeMounts[0].ReadOnly {
		t.Fatalf("uploader is not pinned to the staged output: %+v", uploader.Spec.Template.Spec)
	}
	if uploader.Spec.Template.Spec.AutomountServiceAccountToken == nil || *uploader.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatal("artifact uploader received a Kubernetes service-account token")
	}
	if uploader.Spec.Template.Spec.DNSPolicy != corev1.DNSDefault {
		t.Fatalf("artifact uploader must use the burst node resolver for MagicDNS: %q", uploader.Spec.Template.Spec.DNSPolicy)
	}
	security := uploader.Spec.Template.Spec.Containers[0].SecurityContext
	if security == nil || security.RunAsUser == nil || *security.RunAsUser != 65532 || security.RunAsGroup == nil || *security.RunAsGroup != 65532 {
		t.Fatalf("artifact uploader must use the distroless numeric uid/gid: %+v", security)
	}
	// The watcher reads the uploader's own summary out of its termination
	// message, so the policy must never fall back to the container's logs.
	container := uploader.Spec.Template.Spec.Containers[0]
	if container.TerminationMessagePolicy != corev1.TerminationMessageReadFile || container.TerminationMessagePath != artifactTerminationMessagePath {
		t.Fatalf("uploader termination message is not pinned to the summary file: %q %q",
			container.TerminationMessagePolicy, container.TerminationMessagePath)
	}
	uploader.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := k8s.BatchV1().Jobs("yscale").UpdateStatus(context.Background(), uploader, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if got := central.doneCalls(); len(got) == 1 {
			// This uploader Job left no pod behind, so there is no summary to
			// trust: the export is reported at the Job's own verdict with the
			// counts absent rather than guessed at.
			receipt := central.onlyReceipt(t, "wl_artifact/Succeeded")
			if receipt.Compute.Result != OutcomeResultSucceeded {
				t.Fatalf("compute = %+v, want succeeded", receipt.Compute)
			}
			if receipt.Artifacts == nil || receipt.Artifacts.Result != OutcomeResultSucceeded {
				t.Fatalf("artifacts = %+v, want succeeded", receipt.Artifacts)
			}
			if receipt.Artifacts.ObjectsUploaded != nil || receipt.Artifacts.BytesUploaded != nil {
				t.Fatalf("counts invented without a summary: %+v", receipt.Artifacts)
			}
			if receipt.Artifacts.Reason != missingArtifactSummaryReason {
				t.Fatalf("artifact reason = %q, want the missing-summary note", receipt.Artifacts.Reason)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("workload completion was not reported after artifact upload: %v", central.doneCalls())
}
