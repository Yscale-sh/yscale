package v1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Standard condition types for Workload status (issue #96 contract).
// These are the stable contract kubectl describe, kstatus, and
// automation are written against. A condition whose signal is not yet
// observed should be absent rather than set to Unknown — absent is
// honest; Unknown claims the controller looked and couldn't tell.
const (
	// ConditionAccepted is True once central accepted the submission.
	// False with Reason SubmitFailed on a pre-submit rejection.
	ConditionAccepted = "Accepted"

	// ConditionCapacityReady is True once capacity has been reserved.
	// Signal owned by the placement path — may remain absent until
	// the capacity signal is wired.
	ConditionCapacityReady = "CapacityReady"

	// ConditionNodeReady is True once the burst node has joined the
	// cluster and is Ready. Signal owned by #37/#38 — may remain absent.
	ConditionNodeReady = "NodeReady"

	// ConditionGPUReady is True once GPU devices are available on the
	// burst node. Signal owned by the GPU device plugin — may remain
	// absent for CPU-only workloads or until the signal is wired.
	ConditionGPUReady = "GPUReady"

	// ConditionScheduled is True once the workload pod(s) have been
	// scheduled onto a burst node. Signal owned by #40 — may remain
	// absent.
	ConditionScheduled = "Scheduled"

	// ConditionComplete is True once the workload reached a terminal
	// success; False with a Reason on failure.
	ConditionComplete = "Complete"

	// ConditionCleanupComplete is True once burst resources have been
	// released after the workload finished. May remain absent until
	// the cleanup signal is wired.
	ConditionCleanupComplete = "CleanupComplete"
)

// Stable condition reasons. Each is a PascalCase token central and the
// agent set on the Reason field of a metav1.Condition.
const (
	ReasonSubmitFailed         = "SubmitFailed"
	ReasonSubmitOutcomeUnknown = "SubmitOutcomeUnknown"
	ReasonAccepted             = "Accepted"
	ReasonPlacementAccepted    = "PlacementAccepted"
	ReasonProvisioningStarted  = "ProvisioningStarted"
	ReasonWaitingForPod        = "WaitingForPod"
	ReasonWorkloadRunning      = "WorkloadRunning"
	ReasonWorkloadSucceeded    = "WorkloadSucceeded"
	ReasonWorkloadFailed       = "WorkloadFailed"
	ReasonWorkloadCancelled    = "WorkloadCancelled"
	ReasonBackoffLimitExceeded = "BackoffLimitExceeded"
	ReasonDeadlineExceeded     = "DeadlineExceeded"
	ReasonGatewayRequired      = "GatewayRequired"
	ReasonNodeJoining          = "NodeJoining"
	ReasonNodeReady            = "KubeletReady"
	ReasonNodeNotReady         = "KubeletNotReady"
	ReasonNodeRemoved          = "NodeRemoved"

	// GPU readiness reasons.
	ReasonGPUReady       = "GPUAllocatable"
	ReasonGPUNotObserved = "GPUNotObserved"

	// Pod scheduling reasons.
	ReasonScheduled = "PodScheduled"

	// Cleanup condition reasons. CleanupComplete is True only when durable
	// provider deletion is proven by a non-nil deleted_at; the False reasons
	// distinguish in-progress from stuck.
	ReasonCleanupProven   = "ProviderDeletionProven"
	ReasonCleanupQueued   = "CleanupQueued"
	ReasonCleanupDeleting = "CleanupDeleting"
	ReasonCleanupRetrying = "CleanupRetrying"
	ReasonCleanupFailed   = "CleanupFailed"
	ReasonManualAttention = "ManualAttention"
)

// SetCondition upserts a condition by type. If a condition of the same
// type already exists, it is updated in place; otherwise a new one is
// appended. LastTransitionTime is set only when the status actually
// changes, following the standard K8s convention.
func SetCondition(conditions *[]metav1.Condition, c metav1.Condition) {
	if conditions == nil {
		return
	}
	now := metav1.NewTime(time.Now())
	for i := range *conditions {
		if (*conditions)[i].Type == c.Type {
			if (*conditions)[i].Status != c.Status {
				c.LastTransitionTime = now
			} else {
				c.LastTransitionTime = (*conditions)[i].LastTransitionTime
			}
			(*conditions)[i] = c
			return
		}
	}
	if c.LastTransitionTime.IsZero() {
		c.LastTransitionTime = now
	}
	*conditions = append(*conditions, c)
}

// FindCondition returns the condition with the given type, or nil if
// no such condition exists.
func FindCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

// IsConditionTrue returns whether the named condition exists and has
// status True.
func IsConditionTrue(conditions []metav1.Condition, conditionType string) bool {
	c := FindCondition(conditions, conditionType)
	return c != nil && c.Status == metav1.ConditionTrue
}
