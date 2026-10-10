package agent

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

// PodEventReporter is the slice of *Client the pod scheduling watcher needs.
type PodEventReporter interface {
	ReportPodEvent(ev protocol.PodEvent)
}

// PodSchedulingWatcher watches workload pods (labeled yscale.sh/workload-id)
// and reports a PodEvent when a pod transitions to PodScheduled=True with a
// non-empty spec.nodeName, or to PodScheduled=False with a classifiable reason.
// Each workload-id is reported at most once per stable observation per process
// lifetime.
type PodSchedulingWatcher struct {
	K8s     kubernetes.Interface
	Central PodEventReporter
	Log     *slog.Logger

	Namespace     string
	RetryInterval time.Duration

	mu       sync.Mutex
	reported map[string]string
}

func NewPodSchedulingWatcher(k8s kubernetes.Interface, central PodEventReporter, log *slog.Logger) *PodSchedulingWatcher {
	if log == nil {
		log = slog.Default()
	}
	return &PodSchedulingWatcher{
		K8s:           k8s,
		Central:       central,
		Log:           log,
		RetryInterval: 5 * time.Second,
		reported:      make(map[string]string),
	}
}

func (w *PodSchedulingWatcher) Run(ctx context.Context) error {
	w.Log.Info("pod scheduling watcher starting", "label", labelWorkloadID)
	for {
		if err := w.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.Log.Warn("pod scheduling watch ended; retrying", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.retryInterval()):
			}
		}
	}
}

func (w *PodSchedulingWatcher) retryInterval() time.Duration {
	if w.RetryInterval > 0 {
		return w.RetryInterval
	}
	return 5 * time.Second
}

func (w *PodSchedulingWatcher) runOnce(ctx context.Context) error {
	pods, err := w.K8s.CoreV1().Pods(w.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelWorkloadID,
	})
	if err != nil {
		return fmt.Errorf("list workload pods: %w", err)
	}
	for i := range pods.Items {
		w.observe(&pods.Items[i])
	}

	wi, err := w.K8s.CoreV1().Pods(w.Namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector:   labelWorkloadID,
		ResourceVersion: pods.ResourceVersion,
	})
	if err != nil {
		return fmt.Errorf("watch workload pods: %w", err)
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
			pod, isPod := ev.Object.(*corev1.Pod)
			if isPod && (ev.Type == watch.Added || ev.Type == watch.Modified) {
				w.observe(pod)
			}
		}
	}
}

func (w *PodSchedulingWatcher) observe(pod *corev1.Pod) {
	wlID := pod.Labels[labelWorkloadID]
	if wlID == "" {
		return
	}

	if pod.Spec.NodeName != "" && podIsScheduled(pod) {
		w.observeScheduled(pod, wlID)
		return
	}

	if reason, message, observedAt := classifyWaitingPod(pod); reason != "" {
		w.observeWaiting(pod, wlID, reason, message, observedAt)
	}
}

func (w *PodSchedulingWatcher) observeScheduled(pod *corev1.Pod, wlID string) {
	stateKey := protocol.SchedulingStateScheduled
	w.mu.Lock()
	if w.reported[wlID] == stateKey {
		w.mu.Unlock()
		return
	}
	w.reported[wlID] = stateKey
	w.mu.Unlock()

	scheduledAt := podScheduledAt(pod)
	w.Log.Info("workload pod scheduled",
		"workload", wlID, "pod", pod.Name, "node", pod.Spec.NodeName)
	w.Central.ReportPodEvent(protocol.PodEvent{
		WorkloadID:           wlID,
		Phase:                string(pod.Status.Phase),
		PodName:              pod.Name,
		NodeName:             pod.Spec.NodeName,
		Scheduled:            true,
		ScheduledAt:          scheduledAt,
		SchedulingState:      protocol.SchedulingStateScheduled,
		SchedulingObservedAt: scheduledAt,
	})
}

func (w *PodSchedulingWatcher) observeWaiting(pod *corev1.Pod, wlID, reason, message string, observedAt *time.Time) {
	var timePart string
	if observedAt != nil {
		timePart = observedAt.UTC().Format(time.RFC3339Nano)
	}
	stateKey := protocol.SchedulingStateWaiting + ":" + reason + ":" + pod.Name + ":" + timePart
	w.mu.Lock()
	prev := w.reported[wlID]
	if prev == stateKey || prev == protocol.SchedulingStateScheduled {
		w.mu.Unlock()
		return
	}
	w.reported[wlID] = stateKey
	w.mu.Unlock()

	w.Log.Info("workload pod waiting for scheduling",
		"workload", wlID, "pod", pod.Name, "reason", reason)
	w.Central.ReportPodEvent(protocol.PodEvent{
		WorkloadID:           wlID,
		Phase:                string(pod.Status.Phase),
		PodName:              pod.Name,
		SchedulingState:      protocol.SchedulingStateWaiting,
		SchedulingReason:     reason,
		SchedulingMessage:    message,
		SchedulingObservedAt: observedAt,
	})
}

func podIsScheduled(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func podScheduledAt(pod *corev1.Pod) *time.Time {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionTrue {
			if !cond.LastTransitionTime.IsZero() {
				t := cond.LastTransitionTime.Time.UTC()
				return &t
			}
		}
	}
	return nil
}

// classifyWaitingPod examines a pod's PodScheduled=False condition and
// classifies the raw scheduler message into a bounded reason token. Returns
// empty reason when the pod is not in a classifiable waiting state (no
// PodScheduled condition, or status is not False).
func classifyWaitingPod(pod *corev1.Pod) (reason, message string, observedAt *time.Time) {
	for _, cond := range pod.Status.Conditions {
		if cond.Type != corev1.PodScheduled || cond.Status != corev1.ConditionFalse {
			continue
		}
		reason = classifySchedulerMessage(cond.Reason, cond.Message)
		message = protocol.SchedulingReasonMessage(reason)
		if !cond.LastTransitionTime.IsZero() {
			t := cond.LastTransitionTime.Time.UTC()
			observedAt = &t
		}
		return
	}
	return "", "", nil
}

// classifySchedulerMessage maps the raw Kubernetes scheduler reason/message
// into a bounded PascalCase token. The raw text never leaves this function.
func classifySchedulerMessage(condReason, condMessage string) string {
	msg := strings.ToLower(condMessage)
	reason := strings.ToLower(condReason)

	if strings.Contains(msg, "nvidia.com/gpu") || strings.Contains(msg, "gpu") {
		return protocol.SchedulingReasonInsufficientGPU
	}
	if strings.Contains(msg, "insufficient cpu") {
		return protocol.SchedulingReasonInsufficientCPU
	}
	if strings.Contains(msg, "insufficient memory") {
		return protocol.SchedulingReasonInsufficientMemory
	}
	if strings.Contains(msg, "untolerated taint") || strings.Contains(msg, "had taint") {
		return protocol.SchedulingReasonUntoleratedTaint
	}
	if strings.Contains(msg, "node selector") || strings.Contains(msg, "node affinity") ||
		strings.Contains(msg, "nodeselector") || strings.Contains(msg, "didn't match") ||
		strings.Contains(msg, "node(s) didn't match pod affinity") {
		return protocol.SchedulingReasonNodeSelectorMismatch
	}
	if strings.Contains(msg, "unsupported") && (strings.Contains(msg, "selector") || strings.Contains(msg, "label")) {
		return protocol.SchedulingReasonUnsupportedSelector
	}

	if reason == "unschedulable" || strings.Contains(msg, "unschedulable") {
		return protocol.SchedulingReasonUnschedulable
	}

	return protocol.SchedulingReasonUnschedulable
}
