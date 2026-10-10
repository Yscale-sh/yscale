package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"gopkg.in/yaml.v3"

	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
)

const labelBurstID = "yscale.sh/burst-id"

// PendingPodWatcher is the bridge between KEDA / HPA / Deployments
// and yscale's burst provisioning. It watches Pending pods with two
// markers:
//
//  1. nodeSelector: yscale.sh/burst-node=true  (signals "I want a burst")
//  2. annotation:   yscale.sh/burst-template:<YAML>  (describes the burst shape)
//
// For each Pending pod that meets both criteria, the watcher
// submits a nodeOnly Workload to central. Central provisions the
// burst, kubelet joins, the customer's pod (which K8s already
// created — KEDA scaled the Deployment, etc) schedules onto it.
//
// Dedup is per-pod UID, and the in-memory map only covers one agent
// lifetime: after a restart the watcher sees the same Pending pod as
// new. What keeps that from provisioning a second burst is the
// Idempotency-Key each submit carries, derived from the pod UID —
// central holds a durable claim on it and replays the first answer.
type PendingPodWatcher struct {
	K8s     kubernetes.Interface
	Central CentralAPI
	Log     *slog.Logger

	// SubmitCooldown bounds how often we submit for a given (namespace,
	// template hash) tuple — it throttles a flood of Pending pods while
	// a burst is still warming up, so the scheduler gets a chance to bind
	// them to capacity already on the way. It is warm-up pacing, not
	// correctness: duplicate submits are prevented by the per-submission
	// Idempotency-Key, which survives the restarts this timer does not.
	SubmitCooldown time.Duration

	// Namespace scopes which namespace's pending pods this watcher acts on.
	// Empty (default) = all namespaces. Set it (same value as the workload
	// reconciler's) when multiple agents share one cluster so two agents
	// don't both provision capacity for the same pending pod.
	Namespace string

	mu              sync.Mutex
	seenPodUIDs     map[types.UID]string // pod UID → last submit's burst-id (or "submitted")
	lastSubmitGroup map[string]time.Time // (ns + tplHash) → last submit
}

// NewPendingPodWatcher constructs a watcher with a reasonable
// default cooldown.
func NewPendingPodWatcher(k8s kubernetes.Interface, central CentralAPI, log *slog.Logger) *PendingPodWatcher {
	if log == nil {
		log = slog.Default()
	}
	return &PendingPodWatcher{
		K8s:             k8s,
		Central:         central,
		Log:             log,
		SubmitCooldown:  30 * time.Second,
		seenPodUIDs:     make(map[types.UID]string),
		lastSubmitGroup: make(map[string]time.Time),
	}
}

// Run watches Pods across the cluster (scoped via the agent's RBAC)
// and reacts to events until ctx is cancelled.
func (w *PendingPodWatcher) Run(ctx context.Context) error {
	w.Log.Info("pending-pod watcher starting")
	for {
		if err := w.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.Log.Warn("pod watch ended; retrying", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (w *PendingPodWatcher) runOnce(ctx context.Context) error {
	wi, err := w.K8s.CoreV1().Pods(w.Namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("watch pods: %w", err)
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
			w.handleEvent(ctx, ev)
		}
	}
}

func (w *PendingPodWatcher) handleEvent(ctx context.Context, ev watch.Event) {
	pod, ok := ev.Object.(*corev1.Pod)
	if !ok {
		return
	}
	switch ev.Type {
	case watch.Deleted:
		w.mu.Lock()
		delete(w.seenPodUIDs, pod.UID)
		w.mu.Unlock()
		return
	case watch.Added, watch.Modified:
		// Only act on Pending pods that have our markers.
		if pod.Status.Phase != corev1.PodPending {
			return
		}
		if !needsBurst(pod) {
			return
		}
		if pod.Annotations[workload.AnnotationBurstRejection] != "" {
			return
		}
		tplYAML, ok := pod.Annotations[workload.AnnotationBurstTemplate]
		if !ok {
			return
		}
		w.mu.Lock()
		_, alreadyHandled := w.seenPodUIDs[pod.UID]
		groupKey := pod.Namespace + ":" + hashStr(tplYAML)
		last := w.lastSubmitGroup[groupKey]
		w.mu.Unlock()
		if alreadyHandled {
			return
		}
		if time.Since(last) < w.SubmitCooldown {
			// Wait for the in-flight burst to come up before we
			// submit another. K8s scheduler will bind this pod to
			// the new burst as soon as it's Ready.
			return
		}
		if err := w.submitForPod(ctx, pod, tplYAML); err != nil {
			var rejection *pendingPodRejection
			if errors.As(err, &rejection) {
				w.mu.Lock()
				w.seenPodUIDs[pod.UID] = "rejected:" + rejection.code
				w.mu.Unlock()
				w.Log.Warn("pending pod burst rejected",
					"pod", pod.Namespace+"/"+pod.Name, "reason", rejection.code)
				return
			}
			w.Log.Warn("submit burst for pending pod failed",
				"pod", pod.Namespace+"/"+pod.Name, "error", err)
			return
		}
		w.mu.Lock()
		w.seenPodUIDs[pod.UID] = "submitted"
		w.lastSubmitGroup[groupKey] = time.Now()
		w.mu.Unlock()
	}
}

// needsBurst returns true iff the pod's nodeSelector signals it
// wants a yscale burst.
func needsBurst(pod *corev1.Pod) bool {
	return pod.Spec.NodeSelector[workload.LabelBurstNode] == "true"
}

// submitForPod parses the burst-template annotation as a WorkloadSpec
// fragment, sets nodeOnly=true, and POSTs to central.
func (w *PendingPodWatcher) submitForPod(ctx context.Context, pod *corev1.Pod, tplYAML string) error {
	spec, err := parseBurstTemplate(tplYAML)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}
	spec.NodeOnly = true
	if spec.Replicas <= 0 {
		spec.Replicas = 1
	}
	if code := pendingPodConstraintRejection(pod, spec); code != "" {
		if err := w.annotatePendingPodRejection(ctx, pod, code); err != nil {
			return fmt.Errorf("record pending pod rejection %s: %w", code, err)
		}
		return &pendingPodRejection{code: code}
	}

	wlYAML, err := workloadYAMLFor(pod, spec)
	if err != nil {
		return err
	}
	// pending-pod, not keda: this watcher sees a Pending pod and nothing about
	// what created it, and KEDA, an HPA, Argo and a hand-applied Job all arrive
	// here identically.
	wlID, burstID, backend, est, err := w.Central.SubmitWorkload(
		ctx, pod.Namespace, wlYAML, podSubmissionKey(pod), protocol.OriginPendingPod)
	if err != nil {
		return err
	}
	if err := w.labelTriggeringPod(ctx, pod, wlID, burstID); err != nil {
		// Central already accepted the idempotency key. Returning the label
		// failure deliberately leaves this Pod unhandled so its next watch event
		// replays that same key and retries only the metadata patch.
		return fmt.Errorf("label triggering pod after accepted workload %s: %w", wlID, err)
	}
	w.labelOwningJob(ctx, pod, wlID, burstID)
	w.Log.Info("provisioned burst for pending pod",
		"pod", pod.Namespace+"/"+pod.Name,
		"workload", wlID, "burst", burstID,
		"backend", backend, "est_usd", est)
	return nil
}

type pendingPodRejection struct {
	code string
}

func (e *pendingPodRejection) Error() string {
	return "pending pod burst rejected: " + e.code
}

func (w *PendingPodWatcher) annotatePendingPodRejection(ctx context.Context, pod *corev1.Pod, code string) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{workload.AnnotationBurstRejection: code},
		},
	})
	if err != nil {
		return err
	}
	_, err = w.K8s.CoreV1().Pods(pod.Namespace).Patch(
		ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{},
	)
	return err
}

func (w *PendingPodWatcher) labelTriggeringPod(ctx context.Context, pod *corev1.Pod, workloadID, burstID string) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"labels": map[string]string{
				labelWorkloadID: workloadID,
				labelBurstID:    burstID,
			},
		},
	})
	if err == nil {
		_, err = w.K8s.CoreV1().Pods(pod.Namespace).Patch(
			ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{},
		)
	}
	if err != nil {
		// A stale watch event may race Pod deletion. There is no object left to
		// schedule or label, so retrying that UID cannot recover anything.
		if apierrors.IsNotFound(err) {
			return nil
		}
		w.Log.Warn("label triggering pod with workload failed",
			"pod", pod.Namespace+"/"+pod.Name,
			"workload", workloadID,
			"error", err)
	}
	return err
}

func (w *PendingPodWatcher) labelOwningJob(ctx context.Context, pod *corev1.Pod, workloadID, burstID string) {
	owner := metav1.GetControllerOf(pod)
	// Only batch Jobs have a terminal condition CompletionWatcher can report.
	// Deployment- and Argo-owned pods need separate completion semantics; that
	// follow-up is intentionally out of scope for this Job-backed NodeOnly path.
	if owner == nil || owner.APIVersion != "batch/v1" || owner.Kind != "Job" {
		return
	}

	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"labels": map[string]string{
				labelWorkloadID: workloadID,
				labelBurstID:    burstID,
			},
		},
	})
	if err == nil {
		_, err = w.K8s.BatchV1().Jobs(pod.Namespace).Patch(
			ctx, owner.Name, types.MergePatchType, patch, metav1.PatchOptions{},
		)
	}
	if err != nil {
		w.Log.Warn("label owning job with workload failed",
			"pod", pod.Namespace+"/"+pod.Name,
			"job", pod.Namespace+"/"+owner.Name,
			"workload", workloadID,
			"error", err)
	}
}

// parseBurstTemplate decodes the YAML annotation into a Spec. The
// annotation is a partial spec (gpu / size / cpu / memory / storage /
// budget / reliability), not a full Workload. We fill in nodeOnly +
// the metadata in workloadYAMLFor.
func parseBurstTemplate(s string) (workload.Spec, error) {
	var spec workload.Spec
	if err := yaml.Unmarshal([]byte(s), &spec); err != nil {
		return workload.Spec{}, err
	}
	return spec, nil
}

// workloadYAMLFor produces the YAML body to POST to central. Image
// is set to a sentinel because validation requires one even on
// nodeOnly today — central skips Job creation regardless.
func workloadYAMLFor(pod *corev1.Pod, spec workload.Spec) ([]byte, error) {
	if spec.Image == "" {
		spec.Image = "yscale/burst-placeholder:none"
	}
	wl := workload.Workload{
		APIVersion: workload.APIVersion,
		Kind:       workload.Kind,
		Metadata: workload.Metadata{
			Name:      sanitizeName("burst-" + pod.Name + "-" + shortUID(pod.UID)),
			Namespace: pod.Namespace,
		},
		Spec: spec,
	}
	return yaml.Marshal(wl)
}

// sanitizeName strips characters that would make the Workload name
// invalid as a K8s resource name (DNS-1123 label). At most 63 chars.
func sanitizeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + 32) // ASCII to lower
		case r == '-':
			b.WriteRune(r)
		case r == '_', r == '.', r == '/':
			b.WriteByte('-')
		}
	}
	out := b.String()
	if len(out) > 63 {
		out = out[:63]
	}
	return strings.Trim(out, "-")
}

// shortUID returns a short suffix from a UID so generated Workload
// names stay readable.
func shortUID(uid types.UID) string {
	s := string(uid)
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// hashStr returns a short deterministic key for template-grouping.
// Not cryptographic — collision risk is fine because the worst case
// is one extra burst.
func hashStr(s string) string {
	const fnvPrime = uint64(1099511628211)
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime
	}
	return fmt.Sprintf("%016x", h)
}
