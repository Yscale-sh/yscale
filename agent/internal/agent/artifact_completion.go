package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
)

const (
	artifactStagingRoot  = "/var/lib/yscale/artifacts"
	artifactUploaderRoot = "/artifacts"
	// artifactUploaderContainer names the one container whose termination
	// message the watcher will read a summary from.
	artifactUploaderContainer = "uploader"
)

func (cw *CompletionWatcher) finishWithArtifacts(ctx context.Context, job *batchv1.Job, wlID string) {
	phase := "Succeeded"
	artifacts, err := cw.runArtifactUpload(ctx, job, wlID)
	if err != nil {
		// A lost export still fails the run: the phase drives the Workload CR
		// and the customer's verdict, and outputs that never arrived are not a
		// success. The receipt is what keeps the two halves apart — the compute
		// stays succeeded, because it did.
		phase = "Failed"
		cw.Log.Error("post-run artifact upload failed", "workload", wlID, "job", job.Name, "error", err)
	}
	outcome := &WorkloadOutcome{
		Compute:   ComputeOutcome{Result: OutcomeResultSucceeded},
		Artifacts: artifacts,
	}

	reported := cw.reportDone(ctx, job, wlID, phase, outcome)
	cw.mu.Lock()
	delete(cw.finishing, wlID)
	if !reported {
		delete(cw.reported, wlID)
	}
	cw.mu.Unlock()
}

// runArtifactUpload drives the export and returns what it observed. The
// receipt half is returned on every path, including the failures: an export
// that broke at configuration, at the signer, mid-upload, or at its deadline is
// still an observation, and the caller reports it rather than inventing one.
func (cw *CompletionWatcher) runArtifactUpload(ctx context.Context, job *batchv1.Job, wlID string) (*ArtifactOutcome, error) {
	if cw.ArtifactImage == "" {
		return failedExport(artifactFailureConfiguration), fmt.Errorf("connector image reference is not configured")
	}
	if cw.ArtifactNamespace == "" {
		return failedExport(artifactFailureConfiguration), fmt.Errorf("connector namespace is not configured")
	}
	burstID := job.Annotations[workload.AnnotationCacheBurstID]
	bootstrapEndpoint := job.Annotations[workload.AnnotationCacheBootstrapEndpoint]
	if burstID == "" || bootstrapEndpoint == "" {
		return failedExport(artifactFailureConfiguration), fmt.Errorf("workload is missing artifact signer annotations")
	}

	var artifacts []workload.ArtifactSpec
	if err := json.Unmarshal([]byte(job.Annotations[workload.AnnotationArtifacts]), &artifacts); err != nil {
		return failedExport(artifactFailureConfiguration), fmt.Errorf("decode artifact outputs: %w", err)
	}
	if len(artifacts) == 0 {
		return failedExport(artifactFailureConfiguration), fmt.Errorf("artifact output annotation is empty")
	}
	// The uploader Job does not exist yet, so nothing here can be a verdict it
	// gave. Failing to find the compute pod the export would have been staged
	// from — no succeeded pod, or a list the connector was not granted — is the
	// export failing to be prepared, same as a missing annotation above it.
	pod, err := cw.successfulJobPod(ctx, job)
	if err != nil {
		return failedExport(artifactFailureConfiguration), err
	}
	uploadJob, err := cw.artifactUploadJob(job, pod, wlID, burstID, bootstrapEndpoint, artifacts)
	if err != nil {
		return failedExport(artifactFailureConfiguration), err
	}

	jobs := cw.K8s.BatchV1().Jobs(cw.ArtifactNamespace)
	created, err := jobs.Create(ctx, uploadJob, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, err = jobs.Get(ctx, uploadJob.Name, metav1.GetOptions{})
	}
	if err != nil {
		return failedExport(artifactFailureJob), fmt.Errorf("create artifact uploader Job: %w", err)
	}
	if created.Annotations["yscale.sh/artifact-workload-id"] != wlID || created.Annotations["yscale.sh/artifact-source-job"] != string(job.UID) {
		return failedExport(artifactFailureJob), fmt.Errorf("existing artifact uploader Job does not match workload")
	}

	timeout := cw.ArtifactTimeout
	if timeout <= 0 {
		timeout = 20 * time.Minute
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		current, getErr := jobs.Get(waitCtx, uploadJob.Name, metav1.GetOptions{})
		if getErr != nil {
			// A read carrying the wait's own context fails the moment that
			// context ends, so the deadline and the cancellation reach the
			// receipt through this error as readily as through the select
			// below. Which one noticed says nothing about the export, and
			// calling either a Job failure would name a verdict the uploader
			// never gave.
			if waitErr := waitCtx.Err(); waitErr != nil {
				return abandonedWait(waitErr)
			}
			return failedExport(artifactFailureJob), fmt.Errorf("read artifact uploader Job: %w", getErr)
		}
		switch jobPhase(current) {
		case "Succeeded":
			return cw.exportOutcome(ctx, current, OutcomeResultSucceeded), nil
		case "Failed":
			return cw.exportOutcome(ctx, current, OutcomeResultFailed), fmt.Errorf("artifact uploader Job failed")
		}
		select {
		case <-waitCtx.Done():
			return abandonedWait(waitCtx.Err())
		case <-ticker.C:
		}
	}
}

// abandonedWait is the receipt and error for a wait that ended before the
// uploader Job reached a verdict: the deadline the agent set on the export, or
// a cancellation from above it.
func abandonedWait(err error) (*ArtifactOutcome, error) {
	category := artifactFailureTimeout
	if !errors.Is(err, context.DeadlineExceeded) {
		category = artifactFailureInterrupted
	}
	return failedExport(category), fmt.Errorf("wait for artifact uploader Job: %w", err)
}

// exportOutcome turns a finished uploader Job into the export half of the
// receipt. The Job's own verdict is the truth about whether the export worked;
// the uploader's termination summary only ever ADDS counts and a category to
// that verdict, and only when it came from a pod this Job controls and agrees
// with it. Anything else is reported as the Job-level result with no counts at
// all — a missing count is honest, an invented one is not.
func (cw *CompletionWatcher) exportOutcome(ctx context.Context, job *batchv1.Job, result string) *ArtifactOutcome {
	summary, ok := cw.trustedUploadSummary(ctx, job, result)
	if !ok {
		if result == OutcomeResultSucceeded {
			return &ArtifactOutcome{Result: result, Reason: missingArtifactSummaryReason}
		}
		// With no summary to categorise the failure, the Job controller's own
		// terminal condition is the only account left — and the uploader Job
		// carries an activeDeadlineSeconds, so a run the controller killed at
		// that deadline is a timeout rather than an uploader that reported a
		// failure. The condition REASON is a closed vocabulary the controller
		// sets; the condition message is free text and is never read.
		if terminalConditionReason(job, batchv1.JobFailed) == jobReasonDeadlineExceeded {
			return failedExport(artifactFailureTimeout)
		}
		return failedExport(artifactFailureJob)
	}
	objects, uploaded := summary.ObjectsUploaded, summary.BytesUploaded
	out := &ArtifactOutcome{Result: result, ObjectsUploaded: &objects, BytesUploaded: &uploaded}
	if result == OutcomeResultFailed {
		out.Reason = artifactFailureReason(summary.Reason)
	}
	return out
}

// trustedUploadSummary reads the termination summary of the attempt that
// decided this Job. A Job may have retried, so the attempt matters: a completed
// Job was decided by the one whose uploader exited 0, and a failed Job by the
// most recent terminated attempt. An attempt whose summary disagrees with its
// own exit code, or with the Job's verdict, is discarded — that is what stops a
// failed retry being read as the success that never happened.
func (cw *CompletionWatcher) trustedUploadSummary(ctx context.Context, job *batchv1.Job, result string) (artifactUploadSummary, bool) {
	pods, err := cw.K8s.CoreV1().Pods(job.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "batch.kubernetes.io/job-name=" + job.Name,
	})
	if err != nil {
		cw.Log.Warn("read artifact uploader summary failed", "job", job.Name, "error", err)
		return artifactUploadSummary{}, false
	}
	var best artifactUploadSummary
	var bestAt time.Time
	found := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !controlledByJob(pod, job.UID) {
			continue
		}
		state := uploaderTerminationState(pod)
		if state == nil || (state.ExitCode == 0) != (result == OutcomeResultSucceeded) {
			continue
		}
		summary, ok := parseArtifactUploadSummary(state.Message)
		if !ok || summary.Result != result {
			continue
		}
		if found && !state.FinishedAt.Time.After(bestAt) {
			continue
		}
		best, bestAt, found = summary, state.FinishedAt.Time, true
	}
	return best, found
}

// uploaderTerminationState returns the terminated state of the uploader
// container itself — never another container in the pod, and never one that has
// not finished.
func uploaderTerminationState(pod *corev1.Pod) *corev1.ContainerStateTerminated {
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name == artifactUploaderContainer && status.State.Terminated != nil {
			return status.State.Terminated
		}
	}
	return nil
}

func (cw *CompletionWatcher) successfulJobPod(ctx context.Context, job *batchv1.Job) (*corev1.Pod, error) {
	pods, err := cw.K8s.CoreV1().Pods(job.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "batch.kubernetes.io/job-name=" + job.Name,
	})
	if err != nil {
		return nil, fmt.Errorf("list workload pods: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodSucceeded || pod.Spec.NodeName == "" || !controlledByJob(pod, job.UID) {
			continue
		}
		return pod.DeepCopy(), nil
	}
	return nil, fmt.Errorf("no succeeded workload pod found")
}

func controlledByJob(pod *corev1.Pod, uid types.UID) bool {
	if uid == "" {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "Job" && owner.UID == uid && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}

func (cw *CompletionWatcher) artifactUploadJob(job *batchv1.Job, pod *corev1.Pod, wlID, burstID, bootstrapEndpoint string, artifacts []workload.ArtifactSpec) (*batchv1.Job, error) {
	volumes := make(map[string]corev1.Volume, len(job.Spec.Template.Spec.Volumes))
	for _, volume := range job.Spec.Template.Spec.Volumes {
		volumes[volume.Name] = volume
	}

	uploads := make([]ArtifactUpload, 0, len(artifacts))
	uploaderVolumes := make([]corev1.Volume, 0, len(artifacts))
	uploaderMounts := make([]corev1.VolumeMount, 0, len(artifacts))
	for i, artifact := range artifacts {
		name := fmt.Sprintf("artifact-%d", i)
		volume, ok := volumes[name]
		if !ok || volume.HostPath == nil || !safeArtifactHostPath(volume.HostPath.Path) {
			return nil, fmt.Errorf("artifact %q has no safe staging volume", artifact.Name)
		}
		localPath := fmt.Sprintf("%s/%d", artifactUploaderRoot, i)
		uploaderVolumes = append(uploaderVolumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: volume.HostPath.Path,
				Type: hostPathType(corev1.HostPathDirectory),
			}},
		})
		uploaderMounts = append(uploaderMounts, corev1.VolumeMount{Name: name, MountPath: localPath, ReadOnly: true})
		uploads = append(uploads, ArtifactUpload{
			LocalPath: localPath,
			KeyPrefix: wlID + "/" + pod.Name,
			Source: protocol.BucketRef{
				Bucket: artifact.To.Bucket, Prefix: artifact.To.Prefix, Endpoint: artifact.To.Endpoint,
				Region: artifact.To.Region, CredentialsSecret: artifact.To.CredentialsSecret,
			},
			MaxFiles: artifact.MaxFiles,
			MaxBytes: int64(artifact.MaxSizeGB) * 1024 * 1024 * 1024,
		})
	}
	uploadJSON, err := json.Marshal(uploads)
	if err != nil {
		return nil, fmt.Errorf("encode artifact uploader config: %w", err)
	}
	name := artifactUploadJobName(wlID, job.UID)
	backoff := int32(2)
	ttl := int32(600)
	deadline := int64(cw.ArtifactTimeout.Seconds())
	if deadline <= 0 {
		deadline = int64((20 * time.Minute).Seconds())
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: cw.ArtifactNamespace,
			Labels: map[string]string{"app.kubernetes.io/managed-by": "yscale", "yscale.sh/component": "artifact-uploader"},
			Annotations: map[string]string{
				"yscale.sh/artifact-workload-id": wlID,
				"yscale.sh/artifact-source-job":  string(job.UID),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, TTLSecondsAfterFinished: &ttl, ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"yscale.sh/component": "artifact-uploader"}},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: boolRef(false),
					DNSPolicy:                    corev1.DNSDefault,
					NodeName:                     pod.Spec.NodeName,
					Tolerations:                  append([]corev1.Toleration(nil), pod.Spec.Tolerations...),
					ImagePullSecrets:             append([]corev1.LocalObjectReference(nil), cw.ArtifactPullSecrets...),
					Volumes:                      uploaderVolumes,
					SecurityContext:              &corev1.PodSecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					Containers: []corev1.Container{{
						Name: artifactUploaderContainer, Image: cw.ArtifactImage, Args: []string{"artifact-upload"},
						// Pinned to the file the uploader writes its summary to.
						// FallbackToLogsOnError would make raw container output
						// the termination message, and the watcher turns that
						// message into a record the customer reads back.
						TerminationMessagePath:   artifactTerminationMessagePath,
						TerminationMessagePolicy: corev1.TerminationMessageReadFile,
						Env: []corev1.EnvVar{
							{Name: "YSCALE_ARTIFACT_UPLOADS", Value: string(uploadJSON)},
							{Name: "BURST_ID", Value: burstID},
							{Name: "BOOTSTRAP_ENDPOINT", Value: bootstrapEndpoint},
						},
						VolumeMounts: uploaderMounts,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("25m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: boolRef(false), ReadOnlyRootFilesystem: boolRef(true), RunAsNonRoot: boolRef(true),
							RunAsUser: int64Ref(65532), RunAsGroup: int64Ref(65532),
							Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
					}},
				},
			},
		},
	}, nil
}

func safeArtifactHostPath(value string) bool {
	clean := filepath.Clean(value)
	if clean != value || !strings.HasPrefix(clean, artifactStagingRoot+string(filepath.Separator)) {
		return false
	}
	return filepath.Dir(clean) == artifactStagingRoot && filepath.Base(clean) != "."
}

func artifactUploadJobName(wlID string, jobUID types.UID) string {
	sum := sha256.Sum256([]byte(wlID + "\x00" + string(jobUID)))
	return "yscale-artifact-" + hex.EncodeToString(sum[:12])
}

func hostPathType(value corev1.HostPathType) *corev1.HostPathType { return &value }
func boolRef(value bool) *bool                                    { return &value }
func int64Ref(value int64) *int64                                 { return &value }
