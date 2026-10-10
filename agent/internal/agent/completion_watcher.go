package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/yscale-sh/yscale/pkg/workload"
)

// labelWorkloadID is stamped on every Job yscale creates (see
// RealHandler.OnCreateJob). The completion watcher selects on it.
const labelWorkloadID = "yscale.sh/workload-id"

// resyncInterval bounds how long a terminal Job whose completion report failed
// (or that the watch missed) can go un-reported. A healthy Job emits no further
// watch events once terminal, and the watch only replays on reconnect, so
// without this periodic re-list a single transient report failure would leave
// the burst running — billing — until its k8s TTL deletes the Job (10m) or the
// budget watchdog fires (opt-in, often disabled). The resync re-lists Jobs and
// re-drives handle(), which is idempotent (reported/startedReported guards), so
// a completed burst is torn down promptly even when the first report is lost.
const resyncInterval = 30 * time.Second

// CompletionWatcher watches the Jobs yscale created and, when one
// reaches a terminal state, reports it to central (so the burst behind
// it gets reaped) and mirrors the terminal phase onto the Workload CR.
// It also reports the moment a Job actually begins running, so central
// can stamp the workload's StartedAt.
//
// Without this, burst nodes run forever after their workload finishes:
// the burst's main process is the kubelet, which never exits, so the
// backend's auto-destroy (which fires on process exit) never triggers.
type CompletionWatcher struct {
	K8s     kubernetes.Interface
	Dyn     dynamic.Interface // optional; nil disables Workload CR status updates
	Central WorkloadCompleter
	Log     *slog.Logger
	// Namespace limits Job list/watch operations to the connector's assigned
	// workload namespace. Empty preserves the cluster-wide single-connector
	// behavior.
	Namespace string

	// ArtifactImage is the exact connector image reference used for post-run
	// upload Jobs. ArtifactNamespace is the connector namespace, where its
	// private-registry pull secrets are available.
	ArtifactImage       string
	ArtifactNamespace   string
	ArtifactPullSecrets []corev1.LocalObjectReference
	ArtifactTimeout     time.Duration

	mu              sync.Mutex
	reported        map[string]bool // workload-id -> completion reported (per process lifetime)
	startedReported map[string]bool // workload-id -> start reported (per process lifetime)
	finishing       map[string]bool // workload-id -> artifact upload is in progress
}

// NewCompletionWatcher wires the watcher. central is how it reports
// terminal Jobs upstream; dyn (optional) lets it mirror the terminal
// phase onto the Workload CR.
func NewCompletionWatcher(k8s kubernetes.Interface, dyn dynamic.Interface, central WorkloadCompleter, log *slog.Logger) *CompletionWatcher {
	if log == nil {
		log = slog.Default()
	}
	return &CompletionWatcher{
		K8s:             k8s,
		Dyn:             dyn,
		Central:         central,
		Log:             log,
		reported:        make(map[string]bool),
		startedReported: make(map[string]bool),
		finishing:       make(map[string]bool),
		ArtifactTimeout: 20 * time.Minute,
	}
}

// Run watches yscale Jobs until ctx is cancelled, reconnecting the
// watch on transient failure. A fresh watch replays existing Jobs as
// ADDED events, so a Job that finished while the agent was down is
// still picked up on the next start.
func (cw *CompletionWatcher) Run(ctx context.Context) error {
	cw.Log.Info("completion watcher starting", "label", labelWorkloadID, "resync", resyncInterval.String())
	// A periodic resync backstops the watch: a terminal Job emits no further
	// events, so a transiently-failed completion report would otherwise never
	// retry until the watch happens to reconnect. The resync re-drives handle()
	// (idempotent) so a done burst is reaped promptly regardless.
	go cw.resyncLoop(ctx)
	for {
		if err := cw.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			cw.Log.Warn("job watch ended; retrying", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// resyncLoop periodically re-lists all yscale Jobs and re-drives handle() on
// each, so a terminal-but-unreported Job (report failed, or a watch.Deleted the
// watch loop ignores) is retried within resyncInterval instead of lingering.
func (cw *CompletionWatcher) resyncLoop(ctx context.Context) {
	t := time.NewTicker(resyncInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cw.resync(ctx)
		}
	}
}

// resync lists yscale Jobs and re-drives handle() on each. handle() is
// idempotent (reported/startedReported guards + terminal-only completion), so
// re-running it only retries what a prior report failed to deliver.
func (cw *CompletionWatcher) resync(ctx context.Context) {
	jobs, err := cw.K8s.BatchV1().Jobs(cw.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelWorkloadID,
	})
	if err != nil {
		cw.Log.Warn("completion resync list failed", "error", err)
		return
	}
	for i := range jobs.Items {
		cw.handle(ctx, &jobs.Items[i])
	}
}

func (cw *CompletionWatcher) runOnce(ctx context.Context) error {
	wi, err := cw.K8s.BatchV1().Jobs(cw.Namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: labelWorkloadID,
	})
	if err != nil {
		return fmt.Errorf("watch jobs: %w", err)
	}
	defer wi.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-wi.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			job, isJob := ev.Object.(*batchv1.Job)
			if isJob && (ev.Type == watch.Added || ev.Type == watch.Modified) {
				cw.handle(ctx, job)
			}
		}
	}
}

// handle reports a Job to central if it has reached a terminal state
// and hasn't been reported yet this process lifetime, then mirrors the
// terminal phase onto the Workload CR. A non-terminal Job that has
// actually begun running is reported as started instead (once).
func (cw *CompletionWatcher) handle(ctx context.Context, job *batchv1.Job) {
	wlID := job.Labels[labelWorkloadID]
	if wlID == "" {
		return
	}
	phase := jobPhase(job)
	if phase == "" {
		// Still running. Report the start once so central stamps StartedAt.
		if jobStarted(job) {
			cw.reportStarted(ctx, job, wlID)
		}
		return
	}

	cw.mu.Lock()
	already := cw.reported[wlID]
	cw.mu.Unlock()
	if already {
		return
	}
	if phase == "Succeeded" && job.Annotations[workload.AnnotationArtifacts] != "" {
		cw.mu.Lock()
		if cw.finishing[wlID] {
			cw.mu.Unlock()
			return
		}
		cw.finishing[wlID] = true
		cw.mu.Unlock()
		jobCopy := job.DeepCopy()
		go cw.finishWithArtifacts(ctx, jobCopy, wlID)
		return
	}

	cw.reportDone(ctx, job, wlID, phase, jobOutcome(job, phase))
}

func (cw *CompletionWatcher) reportDone(ctx context.Context, job *batchv1.Job, wlID, phase string, outcome *WorkloadOutcome) bool {
	if err := cw.Central.ReportWorkloadDone(ctx, wlID, phase, outcome); err != nil {
		// Leave it unreported so the next watch event / resync retries.
		cw.Log.Warn("report workload completion failed",
			"workload", wlID, "phase", phase, "error", err)
		return false
	}

	cw.mu.Lock()
	cw.reported[wlID] = true
	cw.mu.Unlock()
	cw.Log.Info("reported workload completion",
		"workload", wlID, "phase", phase, "job", job.Name)

	// Mirror the terminal phase onto the Workload CR. The rendered Job
	// is named after the CR (same namespace + name), so the Job's
	// identity addresses the CR directly.
	cw.markWorkloadStatus(ctx, job.Namespace, job.Name, phase)
	return true
}

// reportStarted reports a running (non-terminal) Job's start to central,
// at most once per workload id per process lifetime. On failure the id is
// left unmarked so the next watch event / resync retries — central's
// /started handler is idempotent, so an occasional re-send is harmless.
func (cw *CompletionWatcher) reportStarted(ctx context.Context, job *batchv1.Job, wlID string) {
	cw.mu.Lock()
	already := cw.startedReported[wlID]
	cw.mu.Unlock()
	if already {
		return
	}

	if err := cw.Central.ReportWorkloadStarted(ctx, wlID); err != nil {
		// Leave it unreported so the next watch event / resync retries.
		cw.Log.Warn("report workload start failed", "workload", wlID, "error", err)
		return
	}

	cw.mu.Lock()
	cw.startedReported[wlID] = true
	cw.mu.Unlock()
	cw.Log.Info("reported workload start", "workload", wlID, "job", job.Name)
}

// markWorkloadStatus patches the Workload CR's .status.phase to the
// terminal phase, so `kubectl get workload` reflects reality — the
// agent otherwise leaves it at "Provisioning" forever. Best-effort.
func (cw *CompletionWatcher) markWorkloadStatus(ctx context.Context, namespace, name, phase string) {
	if cw.Dyn == nil {
		return
	}
	patch, err := json.Marshal(map[string]any{"status": map[string]any{"phase": phase}})
	if err != nil {
		return
	}
	_, err = cw.Dyn.Resource(workloadGVR).Namespace(namespace).
		Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status")
	if err != nil && !apierrors.IsNotFound(err) {
		cw.Log.Warn("workload status patch failed", "workload", namespace+"/"+name, "error", err)
	}
}

// jobStarted reports whether a Job's pods have actually begun running.
// Status.StartTime and Status.Active are NOT that signal: the controller
// stamps StartTime as soon as it processes the Job, and Active counts
// Pending pods — both fire while the pod still sits Unschedulable waiting
// for the burst node to join (it is nodeSelector-pinned to
// yscale.sh/burst-node). Status.Ready counts pods that are Running and
// Ready — the moment work truly starts. (JobReadyPods is on by default
// since k8s 1.24, stable 1.29; on an older cluster Ready stays nil and
// StartedAt simply goes unstamped.)
func jobStarted(job *batchv1.Job) bool {
	return job.Status.Ready != nil && *job.Status.Ready > 0
}

// jobPhase returns "Succeeded" / "Failed" once a Job has the matching
// terminal condition set True, or "" while it's still running.
func jobPhase(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return "Succeeded"
		case batchv1.JobFailed:
			return "Failed"
		}
	}
	return ""
}
