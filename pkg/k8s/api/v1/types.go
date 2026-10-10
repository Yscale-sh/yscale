// Package v1 defines the yscale.sh/v1 Kubernetes CRDs. Today there's
// one Kind: Workload. The CRD shape mirrors pkg/workload.Spec so a
// customer's YAML works identically whether they submit via
// `kubectl apply -f` or `POST /v1/workloads` against central.
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/yscale-sh/yscale/pkg/workload"
)

// GroupName is the API group these CRDs register under.
const GroupName = "yscale.sh"

// GroupVersion is the canonical version stamp.
var GroupVersion = "v1"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=wl
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=Phase,type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name=Reason,type=string,JSONPath=`.status.reason`,priority=1
// +kubebuilder:printcolumn:name=Backend,type=string,JSONPath=`.status.backend`
// +kubebuilder:printcolumn:name=Region,type=string,JSONPath=`.status.region`,priority=1
// +kubebuilder:printcolumn:name=GPU,type=string,JSONPath=`.status.gpu.kind`
// +kubebuilder:printcolumn:name=GPUs,type=integer,JSONPath=`.status.gpu.count`,priority=1
// +kubebuilder:printcolumn:name=Burst,type=string,JSONPath=`.status.burstID`
// +kubebuilder:printcolumn:name=Node,type=string,JSONPath=`.status.nodeName`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name=Cost,type=string,JSONPath=`.status.costSoFarUSD`
// +kubebuilder:printcolumn:name=FinalCost,type=string,JSONPath=`.status.finalCostUSD`,priority=1
// +kubebuilder:printcolumn:name=SKU,type=string,JSONPath=`.status.sku`,priority=1
// +kubebuilder:printcolumn:name=Outcome,type=string,JSONPath=`.status.outcome`,priority=1
// +kubebuilder:printcolumn:name=Finished,type=string,JSONPath=`.status.finishedAt`,priority=1
// +kubebuilder:printcolumn:name=Cleanup,type=string,JSONPath=`.status.cleanupState`,priority=1

// Workload is one user-submitted job that yscale should provision a
// burst node for. The spec mirrors what the user would submit to
// `yscale apply` / POST /v1/workloads; status surfaces the central
// server's view of the workload's lifecycle.
type Workload struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkloadSpec   `json:"spec,omitempty"`
	Status WorkloadStatus `json:"status,omitempty"`
}

// WorkloadSpec is intentionally identical to workload.Spec — the
// alias keeps the YAML schema in one place. (Defined as a struct
// field rather than `type WorkloadSpec = workload.Spec` so we get
// our own deepcopy method.)
type WorkloadSpec struct {
	workload.Spec `json:",inline"`
}

// WorkloadStatus is the controller's view: lifecycle phase, the
// backing burst node, accumulated cost. Updated by the agent's
// status reconciler from the Job/Pod yscale created for this
// Workload.
type WorkloadStatus struct {
	// Phase reflects the workload's lifecycle:
	//   Pending      — accepted, waiting for capacity
	//   Provisioning — burst booting on backend
	//   Running      — pod scheduled and running
	//   Succeeded    — pod exited 0
	//   Failed       — pod exited non-zero (and exhausted retries)
	//   Cancelled    — user deleted the CR mid-flight
	Phase string `json:"phase,omitempty"`

	// Reason is a brief machine-readable token for the current phase
	// transition (e.g. "SubmitFailed", "BackoffLimitExceeded"). Empty
	// when the phase is self-explanatory.
	Reason string `json:"reason,omitempty"`

	// WorkloadID is the central-assigned identifier for this workload.
	// Empty until central accepts the submission. The agent stamps it
	// so a restart can recover the id for cancellation.
	WorkloadID string `json:"workloadID,omitempty"`

	// BurstID is the central-server-assigned id for the burst node
	// backing this workload. Empty until central accepts the submit.
	BurstID string `json:"burstID,omitempty"`

	// Backend identifies the provider implementation that accepted the burst
	// (for example linode, aws, gcp, azure, or flyio).
	Backend string `json:"backend,omitempty"`

	// Region is the provider region the burst was placed in. Empty
	// until placement resolves; mirrors spec.region when pinned,
	// otherwise populated from the placement receipt.
	Region string `json:"region,omitempty"`

	// ProviderClass is the stable provider family used by node selectors and
	// yscale.sh/provider-class labels. It intentionally excludes account IDs.
	ProviderClass string `json:"providerClass,omitempty"`

	// GPU projects the workload's GPU request onto status so kubectl
	// get/describe shows it without parsing .spec. Nil for non-GPU
	// workloads.
	GPU *GPUStatus `json:"gpu,omitempty"`

	// PodName is the K8s Pod that ran (or is running) the workload.
	// kubectl logs <PodName> shows the customer's app output.
	PodName string `json:"podName,omitempty"`

	// NodeName is the burst node the workload's pod is scheduled on, as
	// observed by central's connector node stream. Central is the only
	// authoritative source (the node lives outside the customer cluster),
	// and the agent projects it here from GET /v1/workloads/{id}. Empty
	// until central observes a node; a previously projected value is kept
	// when central's answer omits the observation.
	NodeName string `json:"nodeName,omitempty"`

	// ScheduledAt is when the workload's pod was scheduled onto a burst node,
	// projected from central's durable pod observation. Absent until central
	// observes scheduling.
	ScheduledAt *metav1.Time `json:"scheduledAt,omitempty"`
	// GpuReadyAt is when GPUs became allocatable on the burst node, projected
	// from central's durable GPU observation. Absent for CPU-only workloads or
	// until central observes GPU readiness.
	GpuReadyAt *metav1.Time `json:"gpuReadyAt,omitempty"`

	// Lifecycle timestamps are projected from central's durable record.
	SubmittedAt        *metav1.Time `json:"submittedAt,omitempty"`
	StartedAt          *metav1.Time `json:"startedAt,omitempty"`
	FinishedAt         *metav1.Time `json:"finishedAt,omitempty"`
	NodeObservedAt     *metav1.Time `json:"nodeObservedAt,omitempty"`
	CleanupRequestedAt *metav1.Time `json:"cleanupRequestedAt,omitempty"`
	CleanupUpdatedAt   *metav1.Time `json:"cleanupUpdatedAt,omitempty"`
	// ProviderCreatedAt is the durable provider-creation timestamp returned by
	// central. It is absent until central has authoritative provider evidence.
	ProviderCreatedAt *metav1.Time `json:"providerCreatedAt,omitempty"`
	ProviderDeletedAt *metav1.Time `json:"providerDeletedAt,omitempty"`

	// CostSoFarUSD is the accrued cost in dollars, updated by central
	// as the burst runs. Stored as string (per the printcolumn) to
	// avoid floating-point drift in display.
	CostSoFarUSD string `json:"costSoFarUSD,omitempty"`

	// FinalCostUSD is the frozen terminal cost written by the reap that
	// tore the burst down. Absent on a workload still running, one whose
	// reap failed, or one that predates the record.
	FinalCostUSD string `json:"finalCostUSD,omitempty"`

	// SKU is the provider machine type selected for this burst, projected
	// from the placement receipt. Empty until placement resolves.
	SKU string `json:"sku,omitempty"`

	// HourlyUSD is the per-hour rate the burst was booked at, projected
	// from the frozen terminal cost. Stored as string for the same reason
	// as CostSoFarUSD.
	HourlyUSD string `json:"hourlyUSD,omitempty"`
	// MaximumChargeUSD and MaximumDurationSeconds are the admitted bounds from
	// the durable placement receipt. Zero/empty means no bound was declared.
	MaximumChargeUSD       string `json:"maximumChargeUSD,omitempty"`
	MaximumDurationSeconds int64  `json:"maximumDurationSeconds,omitempty"`

	// ClusterID is the cluster this workload was dispatched to. Empty on
	// records written before multi-cluster routing was recorded.
	ClusterID string `json:"clusterID,omitempty"`

	// Outcome is the compute result as reported by the agent's terminal
	// observation (e.g. "Succeeded", "Failed"). Empty until terminal.
	Outcome string `json:"outcome,omitempty"`

	// ComputeResult is the workload Job's terminal compute outcome, projected
	// from central's stored receipt. Mirrors the legacy Outcome string (kept
	// for existing tooling) and pairs with ComputeReason so a reader has the
	// compute leg of the receipt separate from the artifact leg below.
	ComputeResult string `json:"computeResult,omitempty"`
	// ComputeReason is the machine-readable reason from the compute leg of
	// the terminal receipt. Empty when central had no reason to report — a
	// succeeded run, or a receipt that predates the field.
	ComputeReason string `json:"computeReason,omitempty"`

	// ArtifactResult is the artifact export's terminal outcome (succeeded,
	// failed, or skipped) when export was configured. Absent means no
	// artifact leg is on this workload's receipt — distinct from an export
	// that ran and moved nothing.
	ArtifactResult string `json:"artifactResult,omitempty"`
	// ArtifactReason is the machine-readable reason from the artifact leg.
	// Fixed vocabulary from the agent's uploader; no bucket/endpoint/URL or
	// other submitter- or credential-bearing data reaches the CR.
	ArtifactReason string `json:"artifactReason,omitempty"`
	// ArtifactObjectsUploaded and ArtifactBytesUploaded are what the export
	// actually moved. Pointers so JSON preserves the "not counted"
	// (null/omitted) vs "counted zero" distinction — a receipt from an
	// export the uploader could not summarise never masquerades as an
	// upload that moved zero objects.
	ArtifactObjectsUploaded *int64 `json:"artifactObjectsUploaded,omitempty"`
	ArtifactBytesUploaded   *int64 `json:"artifactBytesUploaded,omitempty"`

	// CleanupState is the durable provider-delete state. CleanupComplete is
	// independently gated on ProviderDeletedAt so a state token alone can never
	// claim provider absence.
	CleanupState string `json:"cleanupState,omitempty"`

	// GPUUtilPercent is central's most recent validated GPU utilisation for
	// the backing burst, in [0, 100]. Pointer so JSON distinguishes an
	// observed idle GPU (0%) from absent telemetry (null/omitted). Projected
	// atomically with LastHeartbeatAt: both are written together only when
	// central emitted a trustworthy pair, and either being nil preserves the
	// last good local value rather than blanking the CR.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	GPUUtilPercent *float64 `json:"gpuUtilPercent,omitempty"`

	// LastHeartbeatAt is the timestamp of the most recent GPU telemetry
	// observation projected from central. Twin of GPUUtilPercent — see
	// that field for the atomicity rule.
	LastHeartbeatAt *metav1.Time `json:"lastHeartbeatAt,omitempty"`

	// Conditions follow the standard K8s Condition shape so existing
	// tooling (kubectl describe, kstatus, etc.) can read them.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// GPUStatus projects the workload's GPU shape onto status for
// kubectl visibility. Values are authoritative only when projected
// from a validated spec or a placement receipt.
type GPUStatus struct {
	Kind    string `json:"kind,omitempty"`
	Count   int    `json:"count,omitempty"`
	Product string `json:"product,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WorkloadList is the standard K8s list shape for collection
// operations (kubectl get workloads, list watches).
type WorkloadList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Workload `json:"items"`
}

// DeepCopyObject implements runtime.Object. Hand-written to avoid
// pulling in controller-gen as a build dep.
func (w *Workload) DeepCopyObject() runtime.Object {
	if w == nil {
		return nil
	}
	out := *w
	out.ObjectMeta = *w.ObjectMeta.DeepCopy()
	out.Spec.Spec = deepCopySpec(w.Spec.Spec)
	// Status conditions need a deep copy because []Condition is a slice.
	if len(w.Status.Conditions) > 0 {
		out.Status.Conditions = make([]metav1.Condition, len(w.Status.Conditions))
		copy(out.Status.Conditions, w.Status.Conditions)
	}
	if w.Status.StartedAt != nil {
		t := *w.Status.StartedAt
		out.Status.StartedAt = &t
	}
	if w.Status.FinishedAt != nil {
		t := *w.Status.FinishedAt
		out.Status.FinishedAt = &t
	}
	for _, pair := range []struct {
		source *metav1.Time
		target **metav1.Time
	}{
		{w.Status.SubmittedAt, &out.Status.SubmittedAt},
		{w.Status.ScheduledAt, &out.Status.ScheduledAt},
		{w.Status.GpuReadyAt, &out.Status.GpuReadyAt},
		{w.Status.NodeObservedAt, &out.Status.NodeObservedAt},
		{w.Status.CleanupRequestedAt, &out.Status.CleanupRequestedAt},
		{w.Status.CleanupUpdatedAt, &out.Status.CleanupUpdatedAt},
		{w.Status.ProviderCreatedAt, &out.Status.ProviderCreatedAt},
		{w.Status.ProviderDeletedAt, &out.Status.ProviderDeletedAt},
	} {
		source, target := pair.source, pair.target
		if source != nil {
			t := *source
			*target = &t
		}
	}
	if w.Status.GPU != nil {
		g := *w.Status.GPU
		out.Status.GPU = &g
	}
	if w.Status.GPUUtilPercent != nil {
		v := *w.Status.GPUUtilPercent
		out.Status.GPUUtilPercent = &v
	}
	if w.Status.LastHeartbeatAt != nil {
		t := *w.Status.LastHeartbeatAt
		out.Status.LastHeartbeatAt = &t
	}
	if w.Status.ArtifactObjectsUploaded != nil {
		v := *w.Status.ArtifactObjectsUploaded
		out.Status.ArtifactObjectsUploaded = &v
	}
	if w.Status.ArtifactBytesUploaded != nil {
		v := *w.Status.ArtifactBytesUploaded
		out.Status.ArtifactBytesUploaded = &v
	}
	return &out
}

// DeepCopyObject implements runtime.Object for the list shape.
func (l *WorkloadList) DeepCopyObject() runtime.Object {
	if l == nil {
		return nil
	}
	out := *l
	out.ListMeta = *l.ListMeta.DeepCopy()
	if len(l.Items) > 0 {
		out.Items = make([]Workload, len(l.Items))
		for i := range l.Items {
			out.Items[i] = *l.Items[i].DeepCopyObject().(*Workload)
		}
	}
	return &out
}

func deepCopySpec(in workload.Spec) workload.Spec {
	out := in
	if in.Networking != nil {
		v := *in.Networking
		out.Networking = &v
	}
	if in.Machine != nil {
		v := *in.Machine
		out.Machine = &v
	}
	if in.GPU != nil {
		v := *in.GPU
		out.GPU = &v
	}
	out.Command = append([]string(nil), in.Command...)
	out.Args = append([]string(nil), in.Args...)
	if len(in.Env) > 0 {
		out.Env = make([]workload.EnvVar, len(in.Env))
		for i := range in.Env {
			out.Env[i] = in.Env[i]
			if in.Env[i].ValueFrom != nil {
				valueFrom := *in.Env[i].ValueFrom
				if valueFrom.SecretKeyRef != nil {
					ref := *valueFrom.SecretKeyRef
					valueFrom.SecretKeyRef = &ref
				}
				if valueFrom.ConfigMapKeyRef != nil {
					ref := *valueFrom.ConfigMapKeyRef
					valueFrom.ConfigMapKeyRef = &ref
				}
				out.Env[i].ValueFrom = &valueFrom
			}
		}
	}
	if in.Storage != nil {
		storage := *in.Storage
		storage.Cache = append([]workload.CacheSpec(nil), in.Storage.Cache...)
		storage.Artifacts = append([]workload.ArtifactSpec(nil), in.Storage.Artifacts...)
		if len(in.Storage.Persistent) > 0 {
			storage.Persistent = make([]workload.PersistentSpec, len(in.Storage.Persistent))
			copy(storage.Persistent, in.Storage.Persistent)
			for i := range storage.Persistent {
				if storage.Persistent[i].Snapshot != nil {
					snapshot := *storage.Persistent[i].Snapshot
					storage.Persistent[i].Snapshot = &snapshot
				}
			}
		}
		if in.Storage.R2 != nil {
			r2 := *in.Storage.R2
			storage.R2 = &r2
		}
		out.Storage = &storage
	}
	if in.Budget != nil {
		v := *in.Budget
		out.Budget = &v
	}
	return out
}
