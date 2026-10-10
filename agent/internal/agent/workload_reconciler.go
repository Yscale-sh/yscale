package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	v1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// WorkloadReconciler watches Workload CRs in the customer's cluster
// and proxies them to central. New CR → POST /v1/workloads; CR
// deleted → DELETE workload (when central exposes it; today the
// burst tears down via the Job lifecycle). Status mirrors back from
// the K8s Job yscale created for the workload.
//
// Uses dynamic.Interface rather than a typed clientset so we don't
// have to ship a generated client. The CRD's permissive schema (with
// x-kubernetes-preserve-unknown-fields) means raw YAML round-trips
// without loss.
type WorkloadReconciler struct {
	K8s     kubernetes.Interface
	Dyn     dynamic.Interface
	Log     *slog.Logger
	Central CentralAPI

	// GatewayEnabled reflects whether this customer install deployed
	// the gateway sidecar (Helm value `gateway.enabled`). When false,
	// submissions with `spec.networking.tier=full` are rejected at
	// submit time with a clear error — without the gateway, full-tier
	// bursts boot fine but their pods time out trying to reach
	// customer-cluster Service IPs (no route advertisement). Better
	// to fail loud at submit than silently 30s later. See
	// docs/architecture/cilium-gateway-sidecar.md.
	GatewayEnabled bool

	// Namespace scopes which namespace's Workload CRs this agent watches.
	// Empty (the default) watches ALL namespaces — the normal single-agent
	// install, where RBAC bounds visibility. Set it when MULTIPLE agents run
	// on ONE physical cluster (e.g. several teams/tenants, each in their own
	// namespace): without scoping, every agent's reconciler would see every
	// Workload CR and each would submit it under its own token, provisioning
	// one duplicate burst per agent. Scoping makes each agent own exactly its
	// namespace's CRs.
	Namespace string

	resyncInterval time.Duration

	mu        sync.Mutex
	submitted map[string]string // CR namespace/name -> central workload-id
}

type confirmedSubmitRejection struct{ err error }

func (e *confirmedSubmitRejection) Error() string { return e.err.Error() }
func (e *confirmedSubmitRejection) Unwrap() error { return e.err }

// CentralAPI is the slice of central's HTTP surface the reconciler
// uses. Pulled out as an interface so tests can stub it.
type CentralAPI interface {
	// SubmitWorkload POSTs a spec to central. idempotencyKey is the caller's
	// durable claim on this submission: central binds it to (tenant, submitter,
	// canonical spec), so a resubmit after a crash or restart replays the first
	// answer instead of provisioning a second node. Callers derive it from
	// something immutable about the object they are submitting for — see
	// podSubmissionKey / crSubmissionKey.
	//
	// origin names WHICH of this connector's paths is submitting, so a console
	// can tell a controller-triggered burst from one a person asked for. Each
	// controller passes its own constant rather than a string, because the value
	// is a wire contract central holds to a closed set.
	SubmitWorkload(ctx context.Context, namespace string, specYAML []byte, idempotencyKey string, origin protocol.SubmissionOrigin) (workloadID, burstID, backend string, estUSD float64, err error)
	// CancelWorkload tells central the customer deleted the Workload
	// CR; central reaps the burst (or no-ops if already reaped).
	// Idempotent at the central-side handler.
	CancelWorkload(ctx context.Context, workloadID string) error
}

// WorkloadCompleter reports a workload's terminal state to central so
// it can reap the burst. Implemented by CentralHTTPClient; used by the
// completion watcher. Kept separate from CentralAPI so adding it
// doesn't ripple through that interface's stubs.
type WorkloadCompleter interface {
	// ReportWorkloadDone posts the terminal phase and, when the agent has one,
	// the receipt for what it observed. A nil outcome is the phase-only body
	// central has always accepted.
	ReportWorkloadDone(ctx context.Context, workloadID, phase string, outcome *WorkloadOutcome) error
	// ReportWorkloadStarted tells central the workload's Job has actually
	// begun running, so central stamps StartedAt. Central's handler is
	// idempotent, so re-delivery on watch resync is harmless.
	ReportWorkloadStarted(ctx context.Context, workloadID string) error
}

// CentralWorkloadSnapshot is the slice of central's GET
// /v1/workloads/{id} response that the status-sync loop projects onto
// Workload CR status. Fields central omitted stay zero/nil so the loop
// can tell "central says nothing" apart from "central says empty".
type CentralWorkloadSnapshot struct {
	// NodeName is central's authoritative view of the burst node the
	// workload's pod was scheduled onto, as reported by the connector
	// node stream. Empty when central has observed no node yet.
	NodeName string
	// SpentUSD is the live accrued cost of the backing burst. Nil when
	// central omitted the field (e.g. the burst is already reaped) —
	// distinct from zero — so a previously known local value is never
	// erased by an omission.
	SpentUSD *float64

	// GPUUtilPercent and LastHeartbeatAt are one validated telemetry
	// observation from the burst node's nvidia-smi heartbeat. Central emits
	// them as an atomic pair — both present with a finite 0..100 utilisation
	// and a non-zero timestamp, or both absent — so a reader can tell an
	// observed idle GPU (0%) from an unknown or refused sample. Nil on
	// either field means central had nothing trustworthy to report; the
	// projection loop must keep the last good local values rather than
	// invent a 0 nobody observed.
	GPUUtilPercent  *float64
	LastHeartbeatAt *time.Time

	// Status is central's phase for the workload.
	Status string
	// CreatedAt and StartedAt are central's durable lifecycle timestamps.
	CreatedAt *time.Time
	StartedAt *time.Time
	// FinishedAt is the terminal timestamp. Nil when the workload has
	// not finished or central omitted the field.
	FinishedAt *time.Time
	// ClusterID is the cluster the workload was dispatched to.
	ClusterID string
	// FinalCostUSD is the frozen terminal cost. Nil until the burst is reaped.
	FinalCostUSD *float64
	// HourlyUSD is the per-hour rate from the frozen cost record.
	HourlyUSD *float64
	// Placement fields are the tenant-safe selected provider decision.
	Provider                string
	Region                  string
	SKU                     string
	GPUProduct              string
	PlacementHourlyMicroUSD *int64
	MaximumChargeMicroUSD   *int64
	MaximumDurationSeconds  *int64
	// NodePhase is the connector-observed Kubernetes Node phase.
	NodePhase      string
	NodeReason     string
	NodeObservedAt *time.Time
	// OutcomeResult is the compute result from the terminal observation.
	OutcomeResult string
	// ComputeReason is the machine-readable reason from the compute leg of
	// central's terminal receipt. Empty when central had nothing to report
	// (a succeeded run, or a receipt that predates the field).
	ComputeReason string
	// Artifact* mirror central's artifact leg of the outcome receipt, which
	// is emitted only when export was configured. An empty ArtifactResult
	// with nil counts means "no artifact leg on this receipt" — the
	// projection loop preserves any last-known artifact fields on the CR
	// rather than blanking them from an omission. Counts are pointers so a
	// null from central ("did not count") stays distinct from 0 ("counted
	// zero"): a nil never overwrites a previously stamped count.
	ArtifactResult          string
	ArtifactReason          string
	ArtifactObjectsUploaded *int64
	ArtifactBytesUploaded   *int64
	// CleanupState is the provider-delete state, empty when no cleanup record exists.
	CleanupState       string
	CleanupRequestedAt *time.Time
	CleanupUpdatedAt   *time.Time
	// ProviderCreatedAt is central's durable record of when the provider
	// accepted the backing resource. Nil when that evidence is unavailable.
	ProviderCreatedAt *time.Time
	// CleanupDeletedAt is the timestamp proving provider deletion. Nil when
	// the provider resource has not been confirmed deleted.
	CleanupDeletedAt *time.Time

	// BurstID is central's durable capacity booking identifier. Non-empty
	// only after the provider burst booking is persisted — the durable
	// workload record is written after the booking, so its presence proves
	// capacity was reserved.
	BurstID string

	// PodName is the workload's Kubernetes Pod name, as observed by the
	// connector's pod scheduling watcher. Empty until scheduling is observed.
	PodName string
	// ScheduledAt is when the workload's pod was scheduled onto a burst node.
	// Nil when no scheduling observation has been made.
	ScheduledAt *time.Time
	// GPUReadyAt is when GPUs became allocatable on the burst node. Nil for
	// CPU-only workloads or when no GPU observation has been made.
	GPUReadyAt *time.Time

	// SchedulingState is the latest scheduling observation: "Scheduled" or
	// "Waiting". Empty when no scheduling observation has been made.
	SchedulingState   string
	SchedulingReason  string
	SchedulingMessage string
	// SchedulingObservedAt is when the scheduling state was observed.
	SchedulingObservedAt *time.Time
}

// WorkloadStatusReader reads a single workload's live state from
// central. Implemented by CentralHTTPClient. Kept separate from
// CentralAPI so adding it doesn't ripple through that interface's
// stubs: a CentralAPI fake that doesn't care about status sync simply
// fails the type assertion and the sync loop no-ops.
type WorkloadStatusReader interface {
	// GetWorkloadStatus fetches GET /v1/workloads/{workloadID} and
	// returns the fields central is authoritative for.
	GetWorkloadStatus(ctx context.Context, workloadID string) (CentralWorkloadSnapshot, error)
}

// NewWorkloadReconciler wires up the controller. Pass the same K8s
// client + a dynamic.Interface, and a CentralAPI that knows how to
// reach central.
func NewWorkloadReconciler(k8s kubernetes.Interface, dyn dynamic.Interface, central CentralAPI, log *slog.Logger) *WorkloadReconciler {
	if log == nil {
		log = slog.Default()
	}
	return &WorkloadReconciler{
		K8s:            k8s,
		Dyn:            dyn,
		Central:        central,
		Log:            log,
		resyncInterval: 30 * time.Second,
		submitted:      make(map[string]string),
	}
}

// workloadGVR is the GroupVersionResource for our CRD.
var workloadGVR = schema.GroupVersionResource{Group: "yscale.sh", Version: "v1", Resource: "workloads"}

// Run starts watching Workload CRs across all namespaces (we filter
// to allowed namespaces server-side via RBAC; the agent's
// ServiceAccount only sees what it has permissions for). Reconciles
// each event; loops until ctx is cancelled.
func (r *WorkloadReconciler) Run(ctx context.Context) error {
	r.Log.Info("workload reconciler starting", "gvr", workloadGVR.String())

	// One periodic status-sync loop per process: lists Workload CRs and
	// projects central's authoritative nodeName and live spend onto their
	// status. Bound to ctx, so it stops when Run's caller cancels.
	go r.statusSyncLoop(ctx)

	// Rebuild the submitted map from existing CRs before watching, so a CR
	// submitted by a previous agent process is deduped (no double-burst) and
	// cancellable (CR delete -> central reap) immediately on restart, not only
	// after the watch happens to replay its ADDED event.
	if err := r.rebuildSubmitted(ctx); err != nil {
		r.Log.Warn("rebuild submitted map from existing CRs failed (continuing; watch will repopulate lazily)", "error", err)
	}

	for {
		if err := r.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.Log.Warn("workload watch ended; retrying", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// rebuildSubmitted lists existing Workload CRs and repopulates the in-memory
// submitted map from each CR's stamped status (workloadID preferred, burstID as
// a legacy fallback). Best-effort: a list failure just leaves the map empty and
// the watch's initial ADDED events repopulate it lazily via handleEvent.
func (r *WorkloadReconciler) rebuildSubmitted(ctx context.Context) error {
	list, err := r.Dyn.Resource(workloadGVR).Namespace(r.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for i := range list.Items {
		obj := &list.Items[i]
		key := obj.GetNamespace() + "/" + obj.GetName()
		if id := statusWorkloadID(obj); id != "" {
			r.submitted[key] = id
			n++
		} else if bid := statusBurstID(obj); bid != "" {
			r.submitted[key] = bid
			n++
		}
	}
	if n > 0 {
		r.Log.Info("rebuilt submitted map from existing Workload CRs", "count", n)
	}
	return nil
}

// statusSyncLoop periodically mirrors central's authoritative view
// (nodeName, live spend) onto Workload CR status. Runs on the same
// resyncInterval as the watch resync; a Central that doesn't implement
// WorkloadStatusReader (e.g. a test fake) turns the loop into a no-op.
func (r *WorkloadReconciler) statusSyncLoop(ctx context.Context) {
	reader, ok := r.Central.(WorkloadStatusReader)
	if !ok || r.Dyn == nil {
		return
	}
	interval := r.resyncInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.syncCentralStatus(ctx, reader)
		}
	}
}

// syncCentralStatus lists Workload CRs in the configured namespace and,
// for each with a stamped central workload id, fetches central's view and
// patches only the status fields that actually changed. Errors are
// per-item: one CR's fetch or patch failure logs and the loop moves on to
// the next CR rather than skipping the whole batch.
func (r *WorkloadReconciler) syncCentralStatus(ctx context.Context, reader WorkloadStatusReader) {
	list, err := r.Dyn.Resource(workloadGVR).Namespace(r.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		r.Log.Warn("status sync: list workloads failed", "error", err)
		return
	}
	for i := range list.Items {
		obj := &list.Items[i]
		wlID := statusWorkloadID(obj)
		if wlID == "" {
			continue // never submitted (or submit failed): central has no view
		}
		snap, err := reader.GetWorkloadStatus(ctx, wlID)
		if err != nil {
			r.Log.Warn("status sync: fetch central workload failed",
				"workload", obj.GetNamespace()+"/"+obj.GetName(), "id", wlID, "error", err)
			continue
		}
		r.projectCentralStatus(ctx, obj, snap)
	}
}

// projectCentralStatus MergePatches the CR's status subresource with ONLY
// the changed, nonempty fields central is authoritative for. Values that
// match what's already stamped are skipped (no patch, no watch churn), and
// fields central omitted are never written — a reaped burst answering
// without spent_usd must not erase the last known cost, and a missing node
// observation must not blank nodeName.
func (r *WorkloadReconciler) projectCentralStatus(ctx context.Context, obj *unstructured.Unstructured, snap CentralWorkloadSnapshot) {
	status := map[string]any{}
	projectStatusString(obj, status, "nodeName", snap.NodeName)
	if snap.SpentUSD != nil {
		projectStatusString(obj, status, "costSoFarUSD", formatSpentUSD(*snap.SpentUSD))
	}
	projectGPUTelemetry(obj, status, snap)
	projectStatusTime(obj, status, "submittedAt", snap.CreatedAt)
	projectStatusTime(obj, status, "startedAt", snap.StartedAt)
	projectStatusTime(obj, status, "finishedAt", snap.FinishedAt)
	projectStatusTime(obj, status, "nodeObservedAt", snap.NodeObservedAt)
	projectStatusTime(obj, status, "cleanupRequestedAt", snap.CleanupRequestedAt)
	projectStatusTime(obj, status, "cleanupUpdatedAt", snap.CleanupUpdatedAt)
	projectStatusTime(obj, status, "providerCreatedAt", snap.ProviderCreatedAt)
	projectStatusTime(obj, status, "providerDeletedAt", snap.CleanupDeletedAt)
	projectStatusString(obj, status, "clusterID", snap.ClusterID)
	if snap.FinalCostUSD != nil {
		projectStatusString(obj, status, "finalCostUSD", formatSpentUSD(*snap.FinalCostUSD))
	}
	if snap.HourlyUSD != nil {
		projectStatusString(obj, status, "hourlyUSD", formatSpentUSD(*snap.HourlyUSD))
	} else if snap.PlacementHourlyMicroUSD != nil {
		projectStatusString(obj, status, "hourlyUSD", formatSpentUSD(float64(*snap.PlacementHourlyMicroUSD)/1_000_000))
	}
	if snap.MaximumChargeMicroUSD != nil {
		projectStatusString(obj, status, "maximumChargeUSD", formatSpentUSD(float64(*snap.MaximumChargeMicroUSD)/1_000_000))
	}
	if snap.MaximumDurationSeconds != nil {
		if current, _, _ := unstructured.NestedInt64(obj.Object, "status", "maximumDurationSeconds"); current != *snap.MaximumDurationSeconds {
			status["maximumDurationSeconds"] = *snap.MaximumDurationSeconds
		}
	}
	projectStatusString(obj, status, "backend", snap.Provider)
	projectStatusString(obj, status, "region", snap.Region)
	projectStatusString(obj, status, "sku", snap.SKU)
	projectGPUProduct(obj, status, snap.GPUProduct)
	projectStatusString(obj, status, "outcome", snap.OutcomeResult)
	projectStatusString(obj, status, "computeResult", snap.OutcomeResult)
	projectStatusString(obj, status, "computeReason", snap.ComputeReason)
	projectArtifactOutcome(obj, status, snap)
	projectStatusString(obj, status, "cleanupState", snap.CleanupState)
	projectStatusString(obj, status, "podName", snap.PodName)
	projectStatusTime(obj, status, "scheduledAt", snap.ScheduledAt)
	projectStatusTime(obj, status, "gpuReadyAt", snap.GPUReadyAt)
	projectStatusTime(obj, status, "schedulingObservedAt", snap.SchedulingObservedAt)

	conditions := workloadStatusConditions(obj)
	originalConditions := append([]metav1.Condition(nil), conditions...)
	if condition := acceptedCondition(snap, obj.GetGeneration()); condition != nil {
		setProjectedCondition(&conditions, *condition)
	}
	if condition := capacityReadyCondition(snap, obj.GetGeneration()); condition != nil {
		setProjectedCondition(&conditions, *condition)
	}
	if phase, reason, condition := centralCompletionProjection(snap, obj.GetGeneration()); phase != "" {
		projectStatusString(obj, status, "phase", phase)
		projectStatusString(obj, status, "reason", reason)
		setProjectedCondition(&conditions, *condition)
	}
	// Gap 8: apply Waiting AFTER central lifecycle projection so it overrides
	// Pending/Provisioning but never overrides Running/Succeeded/Failed/Cancelled.
	if waitPhase, waitReason := waitingForPodProjection(snap, obj); waitPhase != "" {
		projectStatusString(obj, status, "phase", waitPhase)
		projectStatusString(obj, status, "reason", waitReason)
	}
	if condition := nodeReadyCondition(snap, obj.GetGeneration()); condition != nil {
		setProjectedCondition(&conditions, *condition)
	}
	if condition := gpuReadyCondition(snap, obj.GetGeneration()); condition != nil {
		setProjectedCondition(&conditions, *condition)
	}
	if condition := scheduledCondition(snap, obj.GetGeneration()); condition != nil {
		setProjectedCondition(&conditions, *condition)
	}
	if snap.CleanupState != "" {
		condition := cleanupCompleteCondition(snap, obj.GetGeneration())
		setProjectedCondition(&conditions, condition)
	}
	if !statusConditionsEqual(originalConditions, conditions) {
		status["conditions"] = conditions
	}

	if len(status) == 0 {
		return
	}
	patch, err := json.Marshal(map[string]any{"status": status})
	if err != nil {
		return
	}
	if _, err := r.Dyn.Resource(workloadGVR).
		Namespace(obj.GetNamespace()).
		Patch(ctx, obj.GetName(), types.MergePatchType, patch, metav1.PatchOptions{}, "status"); err != nil && !apierrors.IsNotFound(err) {
		r.Log.Warn("status sync: status patch failed",
			"workload", obj.GetNamespace()+"/"+obj.GetName(), "error", err)
	}
}

func statusConditionsEqual(a, b []metav1.Condition) bool {
	aJSON, aErr := json.Marshal(a)
	bJSON, bErr := json.Marshal(b)
	return aErr == nil && bErr == nil && bytes.Equal(aJSON, bJSON)
}

func setProjectedCondition(conditions *[]metav1.Condition, condition metav1.Condition) {
	previous := v1.FindCondition(*conditions, condition.Type)
	statusChanged := previous == nil || previous.Status != condition.Status
	transitionTime := condition.LastTransitionTime
	v1.SetCondition(conditions, condition)
	if statusChanged && !transitionTime.IsZero() {
		if projected := v1.FindCondition(*conditions, condition.Type); projected != nil {
			projected.LastTransitionTime = transitionTime
		}
	}
}

func projectStatusString(obj *unstructured.Unstructured, status map[string]any, key, value string) {
	if value == "" {
		return
	}
	if current, _, _ := unstructured.NestedString(obj.Object, "status", key); current != value {
		status[key] = value
	}
}

func projectStatusTime(obj *unstructured.Unstructured, status map[string]any, key string, value *time.Time) {
	if value == nil {
		return
	}
	projectStatusString(obj, status, key, value.UTC().Format(time.RFC3339))
}

// projectArtifactOutcome writes the artifact leg of central's receipt onto
// status. An empty ArtifactResult means central emitted no artifact leg (no
// export was part of the workload) and the projection leaves any last-known
// artifact fields on the CR alone rather than blanking them from an
// omission. When the leg IS present, the reason and counts are projected
// individually: a nil count from central ("not counted") never overwrites a
// count already on the CR, but an observed 0 ("counted zero") is written
// verbatim so a reader can distinguish an upload that moved nothing from
// one nobody measured. The reason string preserves the same "empty means
// unchanged" contract as every other projectStatusString field.
func projectArtifactOutcome(obj *unstructured.Unstructured, status map[string]any, snap CentralWorkloadSnapshot) {
	if snap.ArtifactResult == "" {
		return
	}
	projectStatusString(obj, status, "artifactResult", snap.ArtifactResult)
	projectStatusString(obj, status, "artifactReason", snap.ArtifactReason)
	projectStatusCountPointer(obj, status, "artifactObjectsUploaded", snap.ArtifactObjectsUploaded)
	projectStatusCountPointer(obj, status, "artifactBytesUploaded", snap.ArtifactBytesUploaded)
}

// projectStatusCountPointer stamps an int64 count only when central provided
// one (non-nil pointer), preserving the pointer's nil-vs-zero semantics: a
// central omission (nil) never overwrites the CR, and an observed 0 lands as
// 0. Reads the stored value as int64 (the dynamic client's normalisation for
// whole JSON numbers) so a re-projection of the same value is a no-op — every
// re-patch is a watch event on the customer's apiserver.
func projectStatusCountPointer(obj *unstructured.Unstructured, status map[string]any, key string, value *int64) {
	if value == nil {
		return
	}
	current, found, err := unstructured.NestedInt64(obj.Object, "status", key)
	if err == nil && found && current == *value {
		return
	}
	status[key] = *value
}

func projectGPUProduct(obj *unstructured.Unstructured, status map[string]any, product string) {
	if product == "" {
		return
	}
	current, _, _ := unstructured.NestedString(obj.Object, "status", "gpu", "product")
	if current == product {
		return
	}
	status["gpu"] = map[string]any{"product": product}
}

// projectGPUTelemetry writes gpuUtilPercent + lastHeartbeatAt as an atomic
// pair when — and only when — central returned a valid pair. A partial or
// missing observation leaves whatever the CR last held, so an observed idle
// 0% never gets faked from a central answer that omitted the reading.
// Preserves the no-patch-churn contract: if both stamped values already
// match, nothing is added to the patch.
func projectGPUTelemetry(obj *unstructured.Unstructured, status map[string]any, snap CentralWorkloadSnapshot) {
	util, heartbeat, ok := validGPUTelemetry(snap.GPUUtilPercent, snap.LastHeartbeatAt)
	if !ok {
		return
	}
	nextUtil := *util
	nextHeartbeat := heartbeat.UTC().Format(time.RFC3339)
	currentHeartbeat, heartbeatFound, _ := unstructured.NestedString(obj.Object, "status", "lastHeartbeatAt")
	currentUtil, utilFound := nestedGPUUtilPercent(obj)
	if utilFound && heartbeatFound && currentUtil == nextUtil && currentHeartbeat == nextHeartbeat {
		return
	}
	status["gpuUtilPercent"] = nextUtil
	status["lastHeartbeatAt"] = nextHeartbeat
}

// nestedGPUUtilPercent reads status.gpuUtilPercent as a float, tolerating the
// int64 an apimachinery unmarshal leaves behind for whole-number JSON values
// (`0`, `100`). Without the int64 branch, a valid stored 0% would look absent
// to the dedup check and every resync would re-patch it — a watch storm on
// unchanged data.
func nestedGPUUtilPercent(obj *unstructured.Unstructured) (float64, bool) {
	val, found, err := unstructured.NestedFieldNoCopy(obj.Object, "status", "gpuUtilPercent")
	if err != nil || !found {
		return 0, false
	}
	switch v := val.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	}
	return 0, false
}

// acceptedCondition derives the Accepted condition from central's durable
// workload record. Returns nil when CreatedAt is absent, so a workload
// central has never seen stays honestly absent rather than Unknown.
func acceptedCondition(snap CentralWorkloadSnapshot, generation int64) *metav1.Condition {
	if snap.CreatedAt == nil {
		return nil
	}
	return &metav1.Condition{
		Type:               v1.ConditionAccepted,
		Status:             metav1.ConditionTrue,
		Reason:             v1.ReasonAccepted,
		Message:            "workload accepted by central",
		ObservedGeneration: generation,
		LastTransitionTime: metav1.NewTime(snap.CreatedAt.UTC()),
	}
}

// capacityReadyCondition derives the CapacityReady condition from central's
// durable burst booking. The durable workload record is written only after
// the provider burst booking is persisted, so a non-empty BurstID proves
// capacity was reserved. Returns nil when BurstID is absent — never inferred
// from requested spec, provider name, or local phase.
func capacityReadyCondition(snap CentralWorkloadSnapshot, generation int64) *metav1.Condition {
	if snap.BurstID == "" {
		return nil
	}
	condition := &metav1.Condition{
		Type:               v1.ConditionCapacityReady,
		Status:             metav1.ConditionTrue,
		Reason:             v1.ReasonPlacementAccepted,
		Message:            "capacity booking confirmed",
		ObservedGeneration: generation,
	}
	if snap.CreatedAt != nil {
		condition.LastTransitionTime = metav1.NewTime(snap.CreatedAt.UTC())
	}
	return condition
}

func centralCompletionProjection(snap CentralWorkloadSnapshot, generation int64) (string, string, *metav1.Condition) {
	phase, reason, message := "", "", ""
	conditionStatus := metav1.ConditionFalse
	switch strings.ToLower(snap.Status) {
	case "provisioning":
		if snap.GPUReadyAt != nil && snap.ScheduledAt == nil {
			phase, reason, message = "WaitingForPod", v1.ReasonWaitingForPod, "GPU capacity is ready; no workload pod scheduling receipt has been observed"
		} else {
			phase, reason, message = "Provisioning", v1.ReasonProvisioningStarted, "workload capacity is provisioning"
		}
	case "running":
		phase, reason, message = "Running", v1.ReasonWorkloadRunning, "workload is running"
	case "succeeded":
		phase, reason, message = "Succeeded", v1.ReasonWorkloadSucceeded, "workload completed successfully"
		conditionStatus = metav1.ConditionTrue
	case "failed":
		phase, reason, message = "Failed", v1.ReasonWorkloadFailed, "workload completed with failure"
	case "cancelled":
		phase, reason, message = "Cancelled", v1.ReasonWorkloadCancelled, "workload was cancelled"
	default:
		return "", "", nil
	}
	condition := &metav1.Condition{
		Type:               v1.ConditionComplete,
		Status:             conditionStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	}
	if snap.FinishedAt != nil && (phase == "Succeeded" || phase == "Failed" || phase == "Cancelled") {
		condition.LastTransitionTime = metav1.NewTime(snap.FinishedAt.UTC())
	} else if snap.StartedAt != nil && phase == "Running" {
		condition.LastTransitionTime = metav1.NewTime(snap.StartedAt.UTC())
	} else if snap.GPUReadyAt != nil && phase == "WaitingForPod" {
		condition.LastTransitionTime = metav1.NewTime(snap.GPUReadyAt.UTC())
	} else if snap.CreatedAt != nil && phase == "Provisioning" {
		condition.LastTransitionTime = metav1.NewTime(snap.CreatedAt.UTC())
	}
	return phase, reason, condition
}

func nodeReadyCondition(snap CentralWorkloadSnapshot, generation int64) *metav1.Condition {
	status, reason, message := metav1.ConditionFalse, "", ""
	switch snap.NodePhase {
	case protocol.NodePhaseReady:
		status, reason, message = metav1.ConditionTrue, v1.ReasonNodeReady, "burst node is Ready"
	case protocol.NodePhaseJoining:
		reason, message = v1.ReasonNodeJoining, "burst node is joining"
	case protocol.NodePhaseNotReady:
		reason, message = v1.ReasonNodeNotReady, "burst node is not Ready"
	case protocol.NodePhaseRemoved:
		reason, message = v1.ReasonNodeRemoved, "burst node was removed"
	default:
		return nil
	}
	if snap.NodeReason != "" {
		message += ": " + snap.NodeReason
	}
	condition := &metav1.Condition{
		Type:               v1.ConditionNodeReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	}
	if snap.NodeObservedAt != nil {
		condition.LastTransitionTime = metav1.NewTime(snap.NodeObservedAt.UTC())
	}
	return condition
}

// gpuReadyCondition derives the GPUReady condition from central's GPU
// observation. Returns nil when no observation has been made, so CPU-only
// workloads never get a GPUReady condition (absent is honest).
func gpuReadyCondition(snap CentralWorkloadSnapshot, generation int64) *metav1.Condition {
	if snap.GPUReadyAt == nil {
		return nil
	}
	return &metav1.Condition{
		Type:               v1.ConditionGPUReady,
		Status:             metav1.ConditionTrue,
		Reason:             v1.ReasonGPUReady,
		Message:            "GPU devices are allocatable on the burst node",
		ObservedGeneration: generation,
		LastTransitionTime: metav1.NewTime(snap.GPUReadyAt.UTC()),
	}
}

// scheduledCondition derives the Scheduled condition from central's scheduling
// observation. "Waiting" → Scheduled=False with the classified reason;
// "Scheduled" → Scheduled=True. Falls back to pod observation for legacy
// connectors. Returns nil when no observation has been made.
//
// Gap 8: a positive PodObservation (ScheduledAt) takes precedence over any
// stale Waiting observation for the Scheduled condition.
func scheduledCondition(snap CentralWorkloadSnapshot, generation int64) *metav1.Condition {
	switch snap.SchedulingState {
	case protocol.SchedulingStateWaiting:
		// Positive PodObservation supersedes stale Waiting for the condition.
		if snap.ScheduledAt != nil {
			return &metav1.Condition{
				Type:               v1.ConditionScheduled,
				Status:             metav1.ConditionTrue,
				Reason:             v1.ReasonScheduled,
				Message:            "workload pod scheduled onto burst node",
				ObservedGeneration: generation,
				LastTransitionTime: metav1.NewTime(snap.ScheduledAt.UTC()),
			}
		}
		reason := snap.SchedulingReason
		if reason == "" {
			reason = v1.ReasonWaitingForPod
		}
		message := snap.SchedulingMessage
		if message == "" {
			message = "pod cannot be scheduled"
		}
		condition := &metav1.Condition{
			Type:               v1.ConditionScheduled,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: generation,
		}
		if snap.SchedulingObservedAt != nil {
			condition.LastTransitionTime = metav1.NewTime(snap.SchedulingObservedAt.UTC())
		}
		return condition
	case protocol.SchedulingStateScheduled:
		condition := &metav1.Condition{
			Type:               v1.ConditionScheduled,
			Status:             metav1.ConditionTrue,
			Reason:             v1.ReasonScheduled,
			Message:            "workload pod scheduled onto burst node",
			ObservedGeneration: generation,
		}
		if snap.SchedulingObservedAt != nil {
			condition.LastTransitionTime = metav1.NewTime(snap.SchedulingObservedAt.UTC())
		} else if snap.ScheduledAt != nil {
			condition.LastTransitionTime = metav1.NewTime(snap.ScheduledAt.UTC())
		}
		return condition
	default:
		if snap.ScheduledAt == nil {
			return nil
		}
		return &metav1.Condition{
			Type:               v1.ConditionScheduled,
			Status:             metav1.ConditionTrue,
			Reason:             v1.ReasonScheduled,
			Message:            "workload pod scheduled onto burst node",
			ObservedGeneration: generation,
			LastTransitionTime: metav1.NewTime(snap.ScheduledAt.UTC()),
		}
	}
}

// waitingForPodProjection projects WaitingForPod phase+reason when the latest
// scheduling observation is "Waiting" and the current/projected phase is not
// authoritative/local Running, Succeeded, Failed, or Cancelled.
//
// Gap 8: a positive PodObservation or ScheduledAt takes precedence over any
// stale negative observation for the Scheduled condition and phase, so
// WaitingForPod is suppressed when the pod has already been observed scheduled.
func waitingForPodProjection(snap CentralWorkloadSnapshot, obj *unstructured.Unstructured) (phase, reason string) {
	if snap.SchedulingState != protocol.SchedulingStateWaiting {
		return "", ""
	}
	// Positive scheduling proof supersedes any stale negative observation.
	if snap.ScheduledAt != nil {
		return "", ""
	}
	// Never override authoritative/local Running or terminal phases.
	current, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	switch current {
	case "Running", "Succeeded", "Failed", "Cancelled":
		return "", ""
	}
	// Never override central's own Running/terminal projection (which was
	// already applied to status before this function runs).
	switch strings.ToLower(snap.Status) {
	case "running", "succeeded", "failed", "cancelled":
		return "", ""
	}
	reason = snap.SchedulingReason
	if reason == "" {
		reason = v1.ReasonWaitingForPod
	}
	return "WaitingForPod", reason
}

// cleanupCompleteCondition derives the CleanupComplete condition from the
// provider-delete state. True only when provider deletion is proven by
// non-nil deleted_at. Fail closed: terminated without deleted_at is False.
func cleanupCompleteCondition(snap CentralWorkloadSnapshot, generation int64) metav1.Condition {
	if snap.CleanupDeletedAt != nil {
		return metav1.Condition{
			Type:               v1.ConditionCleanupComplete,
			Status:             metav1.ConditionTrue,
			Reason:             v1.ReasonCleanupProven,
			Message:            "provider resource deletion confirmed",
			ObservedGeneration: generation,
			LastTransitionTime: metav1.NewTime(snap.CleanupDeletedAt.UTC()),
		}
	}
	reason := v1.ReasonCleanupFailed
	message := "provider resource deletion not yet confirmed"
	switch snap.CleanupState {
	case "queued":
		reason = v1.ReasonCleanupQueued
		message = "provider resource deletion queued"
	case "deleting":
		reason = v1.ReasonCleanupDeleting
		message = "provider resource deletion in progress"
	case "retrying":
		reason = v1.ReasonCleanupRetrying
		message = "provider resource deletion retrying"
	case "manual_attention":
		reason = v1.ReasonManualAttention
		message = "provider resource deletion requires manual attention"
	case "terminated":
		reason = v1.ReasonCleanupFailed
		message = "provider resource terminated without confirmed deletion"
	}
	condition := metav1.Condition{
		Type:               v1.ConditionCleanupComplete,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	}
	if snap.CleanupRequestedAt != nil {
		condition.LastTransitionTime = metav1.NewTime(snap.CleanupRequestedAt.UTC())
	}
	return condition
}

// formatSpentUSD renders a dollar amount as a fixed six-decimal string so
// successive syncs of the same value byte-match (and thus don't re-patch),
// and `kubectl get` shows sub-cent GPU spend without float drift.
func formatSpentUSD(v float64) string {
	return strconv.FormatFloat(v, 'f', 6, 64)
}

// validGPUTelemetry mirrors central's fail-closed guard for the GPU
// heartbeat pair: both fields must be present, the utilisation finite in
// [0, 100], and the timestamp non-zero. Partial or malformed input returns
// (nil, nil, false) so the projection loop preserves the last good local
// values rather than inventing a 0 nobody observed.
func validGPUTelemetry(util *float64, heartbeat *time.Time) (*float64, *time.Time, bool) {
	if util == nil || heartbeat == nil || heartbeat.IsZero() {
		return nil, nil, false
	}
	v := *util
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 100 {
		return nil, nil, false
	}
	return util, heartbeat, true
}

func (r *WorkloadReconciler) runOnce(ctx context.Context) error {
	wi, err := r.Dyn.Resource(workloadGVR).Namespace(r.Namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("watch workloads: %w", err)
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
			r.handleEvent(ctx, ev)
		}
	}
}

func (r *WorkloadReconciler) handleEvent(ctx context.Context, ev watch.Event) {
	obj, ok := ev.Object.(*unstructured.Unstructured)
	if !ok {
		return
	}
	key := obj.GetNamespace() + "/" + obj.GetName()

	switch ev.Type {
	case watch.Added, watch.Modified:
		r.mu.Lock()
		alreadySubmitted := r.submitted[key]
		r.mu.Unlock()
		if alreadySubmitted != "" {
			return // already accepted by central; status reconciler handles updates
		}
		// Durable dedup across agent restarts: the in-memory submitted map
		// is empty after a restart, but a prior process that submitted this
		// CR stamped the central workload-id (and burst-id) onto .status.
		// Repopulate from status.workloadID so (a) we don't re-submit and
		// double-burst, and (b) a later CR delete can still cancel at central
		// with the RIGHT id. Fall back to burstID for CRs written by a
		// pre-upgrade agent (dedup still works; cancel can't — legacy edge).
		if wlID := statusWorkloadID(obj); wlID != "" {
			r.mu.Lock()
			r.submitted[key] = wlID
			r.mu.Unlock()
			return
		}
		if burstID := statusBurstID(obj); burstID != "" {
			r.mu.Lock()
			r.submitted[key] = burstID
			r.mu.Unlock()
			return
		}
		// Mark attempted BEFORE submitting: submit() failure -> markStatus
		// patches .status -> that patch is a Modified event -> without
		// this guard the reconciler resubmits in a tight loop, and every
		// CreateNode retry leaks a real backend VM. A failed workload is
		// left Failed; the user deletes+reapplies to retry deliberately.
		r.mu.Lock()
		r.submitted[key] = "attempted"
		r.mu.Unlock()
		if err := r.submit(ctx, obj); err != nil {
			r.Log.Error("submit workload to central", "workload", key, "error", err)
			r.markStatus(ctx, obj, "Failed", "", "", "", err)
		}
	case watch.Deleted:
		r.mu.Lock()
		wlID := r.submitted[key]
		delete(r.submitted, key)
		r.mu.Unlock()
		// Tell central to reap. Without this, a customer cancelling
		// mid-run leaks the burst until budget.deadline (rarely set).
		// "attempted" placeholder means we tried submit but never got
		// an id back — nothing to cancel central-side.
		if wlID != "" && wlID != "attempted" {
			if err := r.Central.CancelWorkload(ctx, wlID); err != nil {
				r.Log.Warn("cancel workload at central",
					"workload", key, "id", wlID, "error", err)
			} else {
				r.Log.Info("workload cancelled at central",
					"workload", key, "id", wlID)
			}
		}
	}
}

// submit converts the CR's spec to YAML, POSTs to central, and
// stamps the returned workload-id + burst-id + backend onto status.
func (r *WorkloadReconciler) submit(ctx context.Context, obj *unstructured.Unstructured) error {
	key := obj.GetNamespace() + "/" + obj.GetName()

	// Tier preflight runs BEFORE the gateway diagnostic: a spec that
	// names the removed lite tier is unsupported no matter how the
	// gateway is configured, and reporting it as a gateway problem
	// would send the customer to fix the wrong thing. Central refuses
	// the same value in pkg/workload.Validate — the local check keeps
	// the failure loud on the CR before the round trip.
	if tier, unsupported := unsupportedNetworkingTier(obj); unsupported {
		msg := fmt.Sprintf("spec.networking.tier=%q is not supported: the lite tier has been removed; omit spec.networking.tier or set it to \"full\" (empty defaults to full)", tier)
		r.Log.Warn("rejecting workload with unsupported networking tier",
			"workload", key, "tier", tier)
		return &confirmedSubmitRejection{err: errors.New(msg)}
	}

	// Local preflight: full is now the only supported networking tier and it
	// requires the gateway sidecar to reach customer-cluster Services (no
	// tailnet route advertisement without it). Reject before submitting so
	// the customer sees the wiring problem before central provisions a burst
	// whose pods have nowhere to route.
	if !r.GatewayEnabled && workloadRequiresGateway(obj) {
		// This string is surfaced to the user on the Workload status, so it
		// states the fix rather than citing a document they may not have.
		msg := "spec.networking.tier=full is the only supported tier and requires the gateway sidecar; set gateway.enabled=true and gateway.advertiseRoutes=<pod-CIDR> in your yscale-agent Helm values"
		r.Log.Warn("rejecting full-tier submission (gateway not enabled)",
			"workload", key)
		return &confirmedSubmitRejection{err: errors.New(msg)}
	}

	specYAML, err := specToYAML(obj)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	// Keyed on the CR's immutable UID: rebuildSubmitted and the status stamp
	// cover a restart that happened between submit and the next watch event, but
	// a crash BETWEEN central accepting and markStatus landing leaves no stamp at
	// all. The key makes that resubmit a replay of the same workload rather than
	// a second burst. This reconciler does not turn edits into new runs; a new CR
	// (and therefore a new UID) is the boundary for a new request.
	wlID, burstID, backend, estUSD, err := r.Central.SubmitWorkload(
		ctx, obj.GetNamespace(), specYAML, crSubmissionKey(obj), protocol.OriginWorkloadCR)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.submitted[key] = wlID
	r.mu.Unlock()

	r.markStatus(ctx, obj, "Provisioning", wlID, burstID, backend, nil)
	r.Log.Info("workload submitted to central",
		"workload", key, "id", wlID, "burst", burstID,
		"backend", backend, "est_usd", estUSD)
	return nil
}

// markStatus patches the CR's status subresource via the dynamic
// client. Idempotent — repeat calls just overwrite the phase.
//
// workloadID is the central-assigned id. It's stamped onto status so the
// agent can recover it after a restart (the in-memory submitted map is gone):
// without it, a CR deleted post-restart can't be cancelled at central and its
// burst leaks. burstID is kept too for human/debug correlation.
func (r *WorkloadReconciler) markStatus(ctx context.Context, obj *unstructured.Unstructured, phase, workloadID, burstID, backend string, submitErr error) {
	status := map[string]any{
		"phase": phase,
	}
	if burstID != "" {
		status["burstID"] = burstID
	}
	if backend != "" {
		status["backend"] = backend
		status["providerClass"] = backend
	}
	if workloadID != "" {
		status["workloadID"] = workloadID
	}
	if region, found, _ := unstructured.NestedString(obj.Object, "spec", "region"); found && region != "" {
		status["region"] = region
	}
	if gpu, found, _ := unstructured.NestedMap(obj.Object, "spec", "gpu"); found {
		if _, ok := gpu["count"]; !ok {
			gpu["count"] = int64(1)
		}
		status["gpu"] = gpu
	}
	if submitErr != nil {
		var rejected *confirmedSubmitRejection
		if errors.As(submitErr, &rejected) {
			status["reason"] = v1.ReasonSubmitFailed
			conditions := workloadStatusConditions(obj)
			v1.SetCondition(&conditions, metav1.Condition{
				Type:               v1.ConditionAccepted,
				Status:             metav1.ConditionFalse,
				Reason:             v1.ReasonSubmitFailed,
				Message:            submitErr.Error(),
				ObservedGeneration: obj.GetGeneration(),
			})
			status["conditions"] = conditions
		} else {
			status["reason"] = v1.ReasonSubmitOutcomeUnknown
		}
	} else if workloadID != "" {
		status["reason"] = v1.ReasonAccepted
		conditions := workloadStatusConditions(obj)
		v1.SetCondition(&conditions, metav1.Condition{
			Type:               v1.ConditionAccepted,
			Status:             metav1.ConditionTrue,
			Reason:             v1.ReasonAccepted,
			Message:            "workload accepted by central",
			ObservedGeneration: obj.GetGeneration(),
		})
		status["conditions"] = conditions
	}

	patch := map[string]any{"status": status}
	data, _ := json.Marshal(patch)
	_, err := r.Dyn.Resource(workloadGVR).
		Namespace(obj.GetNamespace()).
		Patch(ctx, obj.GetName(), types.MergePatchType, data, metav1.PatchOptions{}, "status")
	if err != nil && !apierrors.IsNotFound(err) {
		r.Log.Warn("status patch failed", "workload", obj.GetName(), "error", err)
	}
}

func workloadStatusConditions(obj *unstructured.Unstructured) []metav1.Condition {
	raw, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found || len(raw) == 0 {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var conditions []metav1.Condition
	if err := json.Unmarshal(data, &conditions); err != nil {
		return nil
	}
	return conditions
}

// statusBurstID extracts .status.burstID from a Workload CR, or "" if
// unset. A non-empty value means a prior submit succeeded — the agent
// uses it to dedup across restarts, when the in-memory submitted map
// is gone.
func statusBurstID(obj *unstructured.Unstructured) string {
	id, found, err := unstructured.NestedString(obj.Object, "status", "burstID")
	if err != nil || !found {
		return ""
	}
	return id
}

// statusWorkloadID extracts .status.workloadID — the central-assigned id — from
// a Workload CR, or "" if unset. The agent restores the submitted map from this
// after a restart so a CR delete can be cancelled at central with the right id.
func statusWorkloadID(obj *unstructured.Unstructured) string {
	id, found, err := unstructured.NestedString(obj.Object, "status", "workloadID")
	if err != nil || !found {
		return ""
	}
	return id
}

// specToYAML marshals the unstructured CR's .spec into YAML the way
// central's POST /v1/workloads expects. The CR's spec is itself a
// map[string]any so we just YAML it directly.
// workloadRequiresGateway returns true whenever the CR carries a spec: full
// is now the only supported networking tier and it always needs the gateway
// sidecar to route to customer-cluster Services. A missing spec means there
// is nothing to submit yet, and we return false to let the reconciler no-op
// on it rather than manufacturing a gateway complaint out of an empty CR.
func workloadRequiresGateway(obj *unstructured.Unstructured) bool {
	_, ok := obj.Object["spec"].(map[string]any)
	return ok
}

// unsupportedNetworkingTier returns the CR's spec.networking.tier value and
// whether it names a tier the platform no longer supports. Empty and "full"
// resolve to the supported full tier (mirroring pkg/workload.Validate and
// ResolveNetworkingTier), so this only flags an explicit "lite" or any other
// non-empty non-"full" value. It runs before the gateway diagnostic in
// submit so a legacy spec fails with a tier-shaped error instead of a
// misleading gateway-wiring complaint.
func unsupportedNetworkingTier(obj *unstructured.Unstructured) (string, bool) {
	tier, found, err := unstructured.NestedString(obj.Object, "spec", "networking", "tier")
	if err != nil || !found || tier == "" {
		return "", false
	}
	if tier == "full" {
		return "", false
	}
	return tier, true
}

func specToYAML(obj *unstructured.Unstructured) ([]byte, error) {
	wlYAML := map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata": map[string]any{
			"name":      obj.GetName(),
			"namespace": obj.GetNamespace(),
		},
		"spec": obj.Object["spec"],
	}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(wlYAML); err != nil {
		return nil, err
	}
	_ = enc.Close()
	return buf.Bytes(), nil
}

// --- Default CentralAPI implementation ---

// CentralHTTPClient is the production CentralAPI: POSTs to
// /v1/workloads using the customer's YSCALE_TOKEN.
type CentralHTTPClient struct {
	Endpoint string
	Token    string
	// ClusterID is this agent's own cluster — the same id it announces in its
	// Hello. The token authenticates the TENANT, so on a tenant running more
	// than one connector it is the only thing that tells central which cluster
	// a submission is for. Empty is allowed and means "don't say", which is
	// what an agent with no configured id and central's single-connector
	// fallback have always done between them.
	ClusterID string
	HTTP      *http.Client
	// Log receives the one line this client emits on its own: the warning
	// ReportWorkloadDone leaves when a receipt central refused is dropped for
	// the phase-only body. Nil uses the default logger, which main has already
	// pointed at the agent's own.
	Log *slog.Logger
}

func (c *CentralHTTPClient) logger() *slog.Logger {
	if c.Log == nil {
		return slog.Default()
	}
	return c.Log
}

// NewCentralHTTPClient returns a client with a 30-second default timeout.
//
// The endpoint may be either an HTTP (`http://`/`https://`) URL or a
// WebSocket URL (`ws://`/`wss://`) — they share a single config key
// in the helm chart so the agent's WS stream and HTTP submit paths
// can both run against the same central. The HTTP client rewrites
// `ws://` -> `http://` and `wss://` -> `https://` so callers don't
// have to maintain two URLs.
func NewCentralHTTPClient(endpoint, token, clusterID string) *CentralHTTPClient {
	endpoint = strings.Replace(endpoint, "ws://", "http://", 1)
	endpoint = strings.Replace(endpoint, "wss://", "https://", 1)
	return &CentralHTTPClient{
		Endpoint:  endpoint,
		Token:     token,
		ClusterID: clusterID,
		HTTP:      &http.Client{Timeout: 30 * time.Second},
	}
}

// SubmitWorkload satisfies the CentralAPI interface.
func (c *CentralHTTPClient) SubmitWorkload(ctx context.Context, namespace string, specYAML []byte, idempotencyKey string, origin protocol.SubmissionOrigin) (string, string, string, float64, error) {
	req, err := http.NewRequestWithContext(ctx, "POST",
		strings.TrimSuffix(c.Endpoint, "/")+"/v1/workloads", bytes.NewReader(specYAML))
	if err != nil {
		return "", "", "", 0, err
	}
	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("Authorization", "Bearer "+c.Token)
	// Verbatim: central scopes and digests the key itself, so any rewriting
	// here would make one retry look like a second request — and a second
	// request is a second paid node. An empty key is the pre-claim behaviour
	// central still accepts from a cluster token: submitted once, no claim.
	if idempotencyKey != "" {
		req.Header.Set(idempotencyHeader, idempotencyKey)
	}
	// Name the cluster the burst is for. This agent submits on behalf of pods
	// and CRs in ITS cluster, so the answer is never anything but its own id —
	// and central needs it to route the burst's node and commands back here
	// rather than to the tenant's other cluster. Left off when unset, so an
	// agent that never learned its id still submits the way it always has.
	if c.ClusterID != "" {
		req.Header.Set(clusterIDHeader, c.ClusterID)
	}
	// Which of this connector's paths is submitting. Sent only for the two
	// values a connector may claim, because central answers 400 on anything
	// else: a label is not worth refusing a customer's burst over, so an origin
	// this client does not recognise is dropped and central leaves provenance
	// unlabelled. Sending nothing is also what an agent built before this field
	// did, and central still accepts it unchanged.
	if origin.ConnectorReported() {
		req.Header.Set(protocol.WorkloadOriginHeader, string(origin))
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", "", "", 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return "", "", "", 0, &confirmedSubmitRejection{err: err}
		}
		return "", "", "", 0, err
	}

	var out struct {
		ID           string  `json:"id"`
		Status       string  `json:"status"`
		Backend      string  `json:"backend"`
		BurstID      string  `json:"burst_id"`
		EstimatedUSD float64 `json:"estimated_usd"`
		Message      string  `json:"message"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", "", "", 0, fmt.Errorf("decode response: %w", err)
	}
	if out.ID == "" {
		return "", "", "", 0, fmt.Errorf("central returned no workload id: %s", out.Message)
	}
	return out.ID, out.BurstID, out.Backend, out.EstimatedUSD, nil
}

// GetWorkloadStatus satisfies WorkloadStatusReader: GETs
// /v1/workloads/{id} and decodes the fields central is authoritative
// for — the observed burst node name and, when the burst is still
// live, the accrued spend.
func (c *CentralHTTPClient) GetWorkloadStatus(ctx context.Context, workloadID string) (CentralWorkloadSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, "GET",
		strings.TrimSuffix(c.Endpoint, "/")+"/v1/workloads/"+url.PathEscape(workloadID),
		nil)
	if err != nil {
		return CentralWorkloadSnapshot{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return CentralWorkloadSnapshot{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return CentralWorkloadSnapshot{}, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		Status          string     `json:"status,omitempty"`
		BurstID         string     `json:"burst_id,omitempty"`
		CreatedAt       *time.Time `json:"created_at,omitempty"`
		StartedAt       *time.Time `json:"started_at,omitempty"`
		NodeObservation *struct {
			NodeName   string     `json:"node_name,omitempty"`
			Phase      string     `json:"phase,omitempty"`
			Reason     string     `json:"reason,omitempty"`
			ObservedAt *time.Time `json:"observed_at,omitempty"`
		} `json:"node_observation,omitempty"`
		SpentUSD        *float64   `json:"spent_usd,omitempty"`
		GPUUtilPercent  *float64   `json:"gpu_util_percent,omitempty"`
		LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`
		FinishedAt      *time.Time `json:"finished_at,omitempty"`
		ClusterID       string     `json:"cluster_id,omitempty"`
		Cost            *struct {
			USD       float64 `json:"usd"`
			HourlyUSD float64 `json:"hourly_usd"`
		} `json:"cost,omitempty"`
		Placement *struct {
			GPUProduct string `json:"gpu_product,omitempty"`
			Receipt    *struct {
				Selected struct {
					Provider               string `json:"provider,omitempty"`
					Region                 string `json:"region,omitempty"`
					SKU                    string `json:"sku,omitempty"`
					HourlyMicroUSD         *int64 `json:"hourly_micro_usd,omitempty"`
					MaximumChargeMicroUSD  *int64 `json:"maximum_charge_micro_usd,omitempty"`
					MaximumDurationSeconds *int64 `json:"maximum_duration_seconds,omitempty"`
				} `json:"selected"`
			} `json:"receipt,omitempty"`
		} `json:"placement,omitempty"`
		Outcome *struct {
			Compute struct {
				Result string `json:"result"`
				Reason string `json:"reason,omitempty"`
			} `json:"compute"`
			Artifacts *struct {
				Result          string `json:"result"`
				Reason          string `json:"reason,omitempty"`
				ObjectsUploaded *int64 `json:"objects_uploaded,omitempty"`
				BytesUploaded   *int64 `json:"bytes_uploaded,omitempty"`
			} `json:"artifacts,omitempty"`
		} `json:"outcome,omitempty"`
		Cleanup *struct {
			State             string     `json:"state"`
			RequestedAt       *time.Time `json:"requested_at,omitempty"`
			UpdatedAt         *time.Time `json:"updated_at,omitempty"`
			DeletedAt         *time.Time `json:"deleted_at,omitempty"`
			ProviderCreatedAt *time.Time `json:"provider_created_at,omitempty"`
		} `json:"cleanup,omitempty"`
		PodObservation *struct {
			PodName     string     `json:"pod_name,omitempty"`
			NodeName    string     `json:"node_name,omitempty"`
			ScheduledAt *time.Time `json:"scheduled_at,omitempty"`
		} `json:"pod_observation,omitempty"`
		GPUObservation *struct {
			AllocatableAt *time.Time `json:"allocatable_at,omitempty"`
		} `json:"gpu_observation,omitempty"`
		SchedulingObservation *struct {
			State      string     `json:"state,omitempty"`
			Reason     string     `json:"reason,omitempty"`
			Message    string     `json:"message,omitempty"`
			ObservedAt *time.Time `json:"observed_at,omitempty"`
		} `json:"scheduling_observation,omitempty"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return CentralWorkloadSnapshot{}, fmt.Errorf("decode response: %w", err)
	}
	snap := CentralWorkloadSnapshot{
		SpentUSD:   out.SpentUSD,
		Status:     out.Status,
		BurstID:    out.BurstID,
		CreatedAt:  out.CreatedAt,
		StartedAt:  out.StartedAt,
		FinishedAt: out.FinishedAt,
		ClusterID:  out.ClusterID,
	}
	if util, heartbeat, ok := validGPUTelemetry(out.GPUUtilPercent, out.LastHeartbeatAt); ok {
		snap.GPUUtilPercent = util
		snap.LastHeartbeatAt = heartbeat
	}
	if out.NodeObservation != nil {
		snap.NodeName = out.NodeObservation.NodeName
		snap.NodePhase = out.NodeObservation.Phase
		snap.NodeReason = out.NodeObservation.Reason
		snap.NodeObservedAt = out.NodeObservation.ObservedAt
	}
	if out.Cost != nil {
		snap.FinalCostUSD = &out.Cost.USD
		snap.HourlyUSD = &out.Cost.HourlyUSD
	}
	if out.Placement != nil && out.Placement.Receipt != nil {
		snap.GPUProduct = out.Placement.GPUProduct
		selected := out.Placement.Receipt.Selected
		snap.Provider = selected.Provider
		snap.Region = selected.Region
		snap.SKU = selected.SKU
		snap.PlacementHourlyMicroUSD = selected.HourlyMicroUSD
		snap.MaximumChargeMicroUSD = selected.MaximumChargeMicroUSD
		snap.MaximumDurationSeconds = selected.MaximumDurationSeconds
	}
	if out.Outcome != nil {
		snap.OutcomeResult = out.Outcome.Compute.Result
		snap.ComputeReason = out.Outcome.Compute.Reason
		if out.Outcome.Artifacts != nil {
			snap.ArtifactResult = out.Outcome.Artifacts.Result
			snap.ArtifactReason = out.Outcome.Artifacts.Reason
			snap.ArtifactObjectsUploaded = out.Outcome.Artifacts.ObjectsUploaded
			snap.ArtifactBytesUploaded = out.Outcome.Artifacts.BytesUploaded
		}
	}
	if out.Cleanup != nil {
		snap.CleanupState = out.Cleanup.State
		snap.CleanupRequestedAt = out.Cleanup.RequestedAt
		snap.CleanupUpdatedAt = out.Cleanup.UpdatedAt
		snap.CleanupDeletedAt = out.Cleanup.DeletedAt
		snap.ProviderCreatedAt = out.Cleanup.ProviderCreatedAt
	}
	if out.PodObservation != nil {
		snap.PodName = out.PodObservation.PodName
		snap.ScheduledAt = out.PodObservation.ScheduledAt
	}
	if out.GPUObservation != nil {
		snap.GPUReadyAt = out.GPUObservation.AllocatableAt
	}
	if out.SchedulingObservation != nil {
		snap.SchedulingState = out.SchedulingObservation.State
		snap.SchedulingReason = out.SchedulingObservation.Reason
		snap.SchedulingMessage = out.SchedulingObservation.Message
		snap.SchedulingObservedAt = out.SchedulingObservation.ObservedAt
	}
	return snap, nil
}

// ReportWorkloadDone satisfies WorkloadCompleter: POSTs the terminal
// phase to /v1/workloads/{id}/complete so central reaps the burst.
// CancelWorkload is the customer-cancelled twin of ReportWorkloadDone:
// the agent saw the Workload CR get deleted, and tells central to reap
// the burst (or no-op if already reaped). Central's handler is
// idempotent so re-delivery on agent reconnect is harmless.
func (c *CentralHTTPClient) CancelWorkload(ctx context.Context, workloadID string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE",
		strings.TrimSuffix(c.Endpoint, "/")+"/v1/workloads/"+url.PathEscape(workloadID),
		nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

func (c *CentralHTTPClient) ReportWorkloadDone(ctx context.Context, workloadID, phase string, outcome *WorkloadOutcome) error {
	body, err := json.Marshal(completeRequest{Phase: phase, Outcome: outcome})
	if err != nil {
		return err
	}
	rejected := c.postWorkloadComplete(ctx, workloadID, body)
	badRequest := completeRejectedAsBadRequest(rejected)
	if outcome == nil || badRequest == nil {
		return rejected
	}
	// A central that predates the receipt decodes this body with unknown fields
	// rejected, so it answers 400 to a field it has never heard of. That is the
	// connector running ahead of central — self-hosted ones upgrade on their own
	// schedule — not a run that cannot be reported, so the phase-only body every
	// central has always accepted gets exactly one more attempt. Any other status
	// and any transport failure is left alone: the first is central refusing the
	// completion on its merits, and the second never reached a decoder at all.
	legacy, marshalErr := json.Marshal(completeRequest{Phase: phase})
	if marshalErr != nil {
		return rejected
	}
	if err := c.postWorkloadComplete(ctx, workloadID, legacy); err != nil {
		return fmt.Errorf("central rejected the outcome (%v) and the phase-only retry failed: %w", rejected, err)
	}
	// The run is reported and the receipt is not, and this line is the only
	// record that the two came apart. The id and the status are the whole of
	// it: the body central refused, what it said about it, and the endpoint it
	// came from are customer data or an operator's credential surface, and a
	// log line outlives the run they describe.
	c.logger().Warn("central rejected the outcome receipt; completion recorded with phase only",
		"workload", workloadID, "status", badRequest.status)
	return nil
}

// postWorkloadComplete delivers one completion body. A status central actually
// returned comes back as *completeRejection so the caller can tell it from a
// transport failure; anything else is the transport's own error.
func (c *CentralHTTPClient) postWorkloadComplete(ctx context.Context, workloadID string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST",
		strings.TrimSuffix(c.Endpoint, "/")+"/v1/workloads/"+url.PathEscape(workloadID)+"/complete",
		bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.Token)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		return &completeRejection{status: resp.StatusCode, body: strings.TrimSpace(string(b))}
	}
	return nil
}

// completeRejection is a completion central answered and refused. It carries
// the status so the caller can act on it, and renders the way this client has
// always rendered a refused request.
type completeRejection struct {
	status int
	body   string
}

func (e *completeRejection) Error() string {
	return fmt.Sprintf("status %d: %s", e.status, e.body)
}

// completeRejectedAsBadRequest returns the 400 central answered with, or nil
// for any other status and for a transport failure that never reached one.
func completeRejectedAsBadRequest(err error) *completeRejection {
	var rejection *completeRejection
	if errors.As(err, &rejection) && rejection.status == http.StatusBadRequest {
		return rejection
	}
	return nil
}

// ReportWorkloadStarted satisfies WorkloadCompleter: POSTs to
// /v1/workloads/{id}/started so central stamps the workload's StartedAt.
// No body — central uses its own clock for the timestamp.
func (c *CentralHTTPClient) ReportWorkloadStarted(ctx context.Context, workloadID string) error {
	req, err := http.NewRequestWithContext(ctx, "POST",
		strings.TrimSuffix(c.Endpoint, "/")+"/v1/workloads/"+url.PathEscape(workloadID)+"/started",
		nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}
