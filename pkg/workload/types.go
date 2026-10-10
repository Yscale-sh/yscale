// Package workload defines the user-facing Workload spec — a typed,
// YAML-friendly description of "run this image with these resources" —
// and translates it into a Kubernetes Job that the existing controller
// will scale a burst node for.
//
// This is the OSS engine's surface for "yscale apply -f workload.yaml".
// SaaS distributions wrap a richer API on top (auth, billing, multi
// tenancy) but emit the same Workload structs internally.
package workload

import "time"

// APIVersion is the version stamp on Workload YAML. Bumped only on
// breaking changes; the decoder accepts older versions where possible.
const APIVersion = "yscale.sh/v1"

// Kind is the only kind this package decodes.
const Kind = "Workload"

// Reliability tier. Only ReliabilityReliable (and the empty default, which
// means the same) is honored: every live provider launch is on-demand
// capacity. The preemptible tiers are kept as named constants so callers and
// price rows that reference them keep compiling, but validation refuses them
// rather than accept a request it would run as reliable.
const (
	ReliabilityReliable = "reliable" // on-demand, no preemption (default)
	ReliabilitySpot     = "spot"     // reserved; rejected by Validate
	ReliabilityAny      = "any"      // reserved; rejected by Validate
)

// Burst-autoscaling annotations.
//
// Pods that want yscale to provision a burst node for them carry these
// on their PodTemplate. The agent's PendingPodWatcher reads them when
// it sees the pod stuck Pending due to the burst-node nodeSelector.
const (
	// AnnotationBurstTemplate is a YAML-encoded WorkloadSpec fragment.
	// Sets gpu/cpu/memory/budget/storage for the burst yscale will
	// provision on behalf of any pod carrying this annotation.
	AnnotationBurstTemplate = "yscale.sh/burst-template"

	// AnnotationBurstRejection is set on a triggering Pod when its hard
	// scheduling constraints cannot match supported burst nodes. The value is
	// a stable machine-readable rejection code and is written before central
	// receives a capacity request.
	AnnotationBurstRejection = "yscale.sh/burst-rejection"

	// LabelBurstNode marks a node as yscale-managed burst capacity.
	// Pods opt in via nodeSelector; central stamps the taint on
	// nodes that match.
	LabelBurstNode = "yscale.sh/burst-node"
)

// Workload is the top-level YAML document users hand to `yscale apply`.
// It mirrors the K8s shape (apiVersion + kind + metadata + spec) so the
// document is familiar to anyone who's used kubectl.
type Workload struct {
	APIVersion string   `yaml:"apiVersion" json:"apiVersion"`
	Kind       string   `yaml:"kind"       json:"kind"`
	Metadata   Metadata `yaml:"metadata"   json:"metadata"`
	Spec       Spec     `yaml:"spec"       json:"spec"`
}

// Metadata is the standard K8s ObjectMeta-like header. Namespace
// defaults to "default" when empty.
type Metadata struct {
	Name      string            `yaml:"name"                json:"name"`
	Namespace string            `yaml:"namespace,omitempty" json:"namespace,omitempty"`
	Labels    map[string]string `yaml:"labels,omitempty"    json:"labels,omitempty"`
	Tags      map[string]string `yaml:"tags,omitempty"      json:"tags,omitempty"` // owner/project for SaaS billing
}

// NetworkingSpec selects the burst-side networking shape for a workload.
//
// "full" is the only supported value (and the default when omitted): bursts
// run the host-mode cilium-agent baked into the rootfs, so cross-cluster
// pod-to-pod + Cilium identity + policy work.
//
// The "lite" fast-boot tier is permanently unsupported. Validate rejects any
// explicit "lite" request (and every other non-empty non-"full" value) before
// provider, mesh, billing or PodCIDR side effects.
type NetworkingSpec struct {
	// Tier must be empty or "full". Validate rejects "lite" and any other
	// non-empty value; ResolveNetworkingTier always resolves to "full" so
	// defensive callers cannot re-open a disabled path.
	Tier string `yaml:"tier,omitempty" json:"tier,omitempty"`
}

// Networking-tier string values. NetworkingTierFull is the only supported
// value (and the default when Tier is empty). NetworkingTierLite is retained
// only so callers that still name the removed tier — for example, to reject
// or log it — do not have to hard-code the literal string.
const (
	NetworkingTierFull = "full"
	NetworkingTierLite = "lite"
)

// Spec is the workload payload.
type Spec struct {
	// Image is the OCI image to run. Required.
	Image string `yaml:"image" json:"image"`

	// Replicas is the number of pods to run for this Workload. Maps
	// to Job.spec.parallelism + Job.spec.completions. Defaults to 1.
	// Higher values produce a single Job with N parallel pods on a
	// single burst node sized to fit them all.
	Replicas int32 `yaml:"replicas,omitempty" json:"replicas,omitempty"`

	// NodeOnly provisions the burst node without creating a Job. The
	// customer's own pods (from a Deployment / ScaledJob / Argo
	// workflow / KEDA-driven ScaledObject) land on the burst via
	// standard K8s scheduling — yscale provides capacity, the
	// customer's tooling provides workload.
	//
	// Image, command, args, env etc are ignored when NodeOnly=true;
	// only the resource shape (gpu/cpu/memory) and storage matter.
	NodeOnly bool `yaml:"nodeOnly,omitempty" json:"nodeOnly,omitempty"`

	// Backend pins the cloud. "" or "auto" lets the decider pick;
	// explicit values are "flyio", "linode", or "aws". On the SaaS path this
	// short-circuits the routing algorithm.
	Backend string `yaml:"backend,omitempty" json:"backend,omitempty"`

	// Region pins the burst to a provider region. Empty lets the backend choose
	// its configured region or another region with available capacity.
	Region string `yaml:"region,omitempty" json:"region,omitempty"`

	// Networking selects burst-side CNI behaviour. Default is "full".
	Networking *NetworkingSpec `yaml:"networking,omitempty" json:"networking,omitempty"`

	// Machine pins concrete sizing on the chosen backend. Mutually
	// exclusive with Size and with explicit CPU/Memory.
	Machine *MachineSpec `yaml:"machine,omitempty" json:"machine,omitempty"`

	// Size selects a preset (nano|small|medium|large|xlarge|2xlarge).
	// Mutually exclusive with explicit CPU/Memory.
	Size string `yaml:"size,omitempty" json:"size,omitempty"`

	// CPU and Memory override Size when set explicitly.
	CPU    string `yaml:"cpu,omitempty"    json:"cpu,omitempty"`    // e.g. "2", "500m"
	Memory string `yaml:"memory,omitempty" json:"memory,omitempty"` // e.g. "4Gi", "2048Mi"

	// GPU is non-nil when a GPU is needed.
	GPU *GPURequest `yaml:"gpu,omitempty" json:"gpu,omitempty"`

	// Command and Args override the image's entrypoint/cmd.
	Command []string `yaml:"command,omitempty" json:"command,omitempty"`
	Args    []string `yaml:"args,omitempty"    json:"args,omitempty"`

	// Env passes environment variables. Supports literal values and
	// secret references (matching the K8s EnvVar shape).
	Env []EnvVar `yaml:"env,omitempty" json:"env,omitempty"`

	// Storage describes BYO object inputs, post-run artifact outputs, and
	// reserved persistent-volume declarations.
	Storage *Storage `yaml:"storage,omitempty" json:"storage,omitempty"`

	// Budget caps spend and wall-clock for this workload.
	Budget *Budget `yaml:"budget,omitempty" json:"budget,omitempty"`

	// Retries maps to Job.spec.backoffLimit. 0 = no retries.
	Retries int32 `yaml:"retries,omitempty" json:"retries,omitempty"`

	// ModelVolume names a pre-seeded Linode Block Storage volume (by
	// label) to attach to the burst and mount read-only into the pod at
	// /models (with HF_HOME=/models/hf). It turns a multi-GB model
	// download into a zero-download mount: the volume is seeded once and
	// reused across bursts. The volume is single-attach (RWO), so
	// workloads using it run serially. Wired on the Linode backend only.
	ModelVolume string `yaml:"modelVolume,omitempty" json:"modelVolume,omitempty"`
}

// GPURequest specifies the GPU shape needed.
type GPURequest struct {
	Kind  string `yaml:"kind,omitempty"  json:"kind,omitempty"`  // l4 | l40s | a100 | h100 | h200 | rtx4090 | a6000 | any
	Count int    `yaml:"count,omitempty" json:"count,omitempty"` // default 1

	// SKU is reserved for future exact backend-SKU pinning. Current central
	// admission rejects non-empty values before side effects because the live
	// providers cannot yet guarantee exact launch and authoritative pricing.
	SKU string `yaml:"sku,omitempty" json:"sku,omitempty"`

	// Reliability picks the placement tier. Empty and "reliable" both mean
	// on-demand, non-preemptible capacity. "spot" and "any" are rejected by
	// central admission before side effects until a provider launch path can
	// actually request interruptible capacity and recover from preemption.
	Reliability string `yaml:"reliability,omitempty" json:"reliability,omitempty"`

	// MaxHourlyUSD caps the per-GPU customer price automatic selection may
	// accept. It must be finite. Explicit SKU requests are currently rejected
	// regardless of this value until exact provider launch+pricing is available.
	// Positive AWS caps are also rejected until regional authoritative pricing
	// replaces the current estimate-only catalog.
	//
	// Note: per-GPU price is not linear across multi-GPU Linode plans — a
	// larger plan bundles more vCPU/RAM, so e.g. a 4x rtx4000ada resolves to a
	// higher $/GPU than the single-GPU price index advertises. Size the cap to
	// the shape (kind AND count) you actually request, not the index headline.
	MaxHourlyUSD float64 `yaml:"maxHourlyUSD,omitempty" json:"maxHourlyUSD,omitempty"`
}

// MachineSpec pins backend-specific machine sizing.
type MachineSpec struct {
	// FlyType is a Fly.io machine class — shared-cpu-1x .. shared-cpu-8x,
	// performance-1x .. performance-16x. Implies backend=flyio.
	FlyType string `yaml:"flyType,omitempty" json:"flyType,omitempty"`
	// Region overrides the backend's default region.
	Region string `yaml:"region,omitempty" json:"region,omitempty"`
}

// EnvVar is the literal-or-secret-ref env var shape, mirroring K8s.
type EnvVar struct {
	Name      string         `yaml:"name"                  json:"name"`
	Value     string         `yaml:"value,omitempty"       json:"value,omitempty"`
	ValueFrom *EnvVarFromRef `yaml:"valueFrom,omitempty"   json:"valueFrom,omitempty"`
}

// EnvVarFromRef references a Secret or ConfigMap key. Mirrors K8s'
// EnvVarSource subset that we actually support.
type EnvVarFromRef struct {
	SecretKeyRef    *KeyRef `yaml:"secretKeyRef,omitempty"    json:"secretKeyRef,omitempty"`
	ConfigMapKeyRef *KeyRef `yaml:"configMapKeyRef,omitempty" json:"configMapKeyRef,omitempty"`
}

// KeyRef is a {name, key} reference into a Secret or ConfigMap.
type KeyRef struct {
	Name string `yaml:"name" json:"name"`
	Key  string `yaml:"key"  json:"key"`
}

// Storage is the BYO data-mount spec. Two flavors:
//   - Cache: read-only mirror of an S3/R2 prefix into a per-burst
//     persistent volume. Source of truth is the customer's bucket;
//     yscale fetches once per cache_key and reuses across bursts.
//   - Persistent: writable backend volume that survives across burst
//     restarts. Optional periodic snapshot back to the customer's
//     bucket as a durability backstop.
type Storage struct {
	Cache      []CacheSpec      `yaml:"cache,omitempty"      json:"cache,omitempty"`
	Persistent []PersistentSpec `yaml:"persistent,omitempty" json:"persistent,omitempty"`
	Artifacts  []ArtifactSpec   `yaml:"artifacts,omitempty"  json:"artifacts,omitempty"`

	// R2Mount is the legacy OSS-controller storage shape — kept so old
	// YAMLs still parse. New deployments should use Cache or Persistent.
	R2 *R2Mount `yaml:"r2,omitempty" json:"r2,omitempty"`
}

// ArtifactSpec stages a writable directory on the burst node and uploads its
// regular files after the workload Job succeeds. The connector waits for that
// upload before reporting the Yscale workload complete. Remote object keys are
// nested under a server-generated workload/pod-attempt prefix so retries never
// overwrite one another.
type ArtifactSpec struct {
	Name      string    `yaml:"name"      json:"name"`
	Target    string    `yaml:"target"    json:"target"` // writable directory inside the workload container
	To        BucketRef `yaml:"to"        json:"to"`
	MaxFiles  int       `yaml:"maxFiles"  json:"maxFiles"`
	MaxSizeGB int       `yaml:"maxSizeGB" json:"maxSizeGB"`
}

// CacheSpec mirrors a bucket prefix into a read-only, pod-local emptyDir using
// HTTP downloads from pre-signed URLs before the workload starts. The cache is ephemeral:
// every workload fetches its own copy until backend cache volumes are wired.
type CacheSpec struct {
	Name       string    `yaml:"name"             json:"name"` // pod-visible label; opaque to system
	Source     BucketRef `yaml:"source"           json:"source"`
	Target     string    `yaml:"target"           json:"target"`                   // mount path inside pod
	Retention  string    `yaml:"retention,omitempty" json:"retention,omitempty"`   // empty | ephemeral; durable policies fail admission
	SizeHintGB int       `yaml:"sizeHintGB,omitempty" json:"sizeHintGB,omitempty"` // emptyDir capacity limit when positive
}

// PersistentSpec is a writable backend volume bound to (tenant, name).
// Survives across bursts; optionally synced to the customer's bucket.
type PersistentSpec struct {
	Name      string        `yaml:"name"               json:"name"` // unique per tenant
	SizeGB    int           `yaml:"sizeGB"             json:"sizeGB"`
	Target    string        `yaml:"target"             json:"target"`               // mount path inside pod
	Retention string        `yaml:"retention,omitempty" json:"retention,omitempty"` // default keep
	Snapshot  *SnapshotSpec `yaml:"snapshot,omitempty"  json:"snapshot,omitempty"`
}

// SnapshotSpec configures periodic upload of the persistent volume's
// contents to the customer's bucket. Diff-aware via ETag — only
// changed objects upload.
type SnapshotSpec struct {
	To       BucketRef     `yaml:"to"               json:"to"`
	Interval time.Duration `yaml:"interval,omitempty" json:"interval,omitempty"` // e.g. 5m
}

// BucketRef points at an S3-compatible bucket the customer owns.
// Credentials live in a K8s Secret; yscale never sees them — the
// agent (in the customer's cluster) reads the Secret and signs short-
// lived URLs the burst can use.
type BucketRef struct {
	Bucket            string `yaml:"bucket"             json:"bucket"`
	Prefix            string `yaml:"prefix,omitempty"   json:"prefix,omitempty"`
	Endpoint          string `yaml:"endpoint,omitempty" json:"endpoint,omitempty"` // R2 needs this; native S3 omits
	Region            string `yaml:"region,omitempty"   json:"region,omitempty"`   // S3; "auto" for R2
	CredentialsSecret string `yaml:"credentialsSecret"  json:"credentialsSecret"`  // Secret with AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY
}

// URI returns a stable string identifier for this bucket+prefix. Used
// in cache_key derivation; case-sensitive.
func (b BucketRef) URI() string {
	if b.Endpoint != "" {
		return b.Endpoint + "/" + b.Bucket + "/" + b.Prefix
	}
	return "s3://" + b.Bucket + "/" + b.Prefix
}

// R2Mount is the deprecated OSS-controller storage shape. New
// deployments should use Cache or Persistent. Kept so existing YAMLs
// don't fail to parse.
type R2Mount struct {
	Endpoint       string `yaml:"endpoint"                 json:"endpoint"`
	Bucket         string `yaml:"bucket"                   json:"bucket"`
	CredentialsRef string `yaml:"credentialsRef"           json:"credentialsRef"`
	MountPath      string `yaml:"mountPath"                json:"mountPath"`
	ReadOnly       bool   `yaml:"readOnly,omitempty"       json:"readOnly,omitempty"`
}

// Budget caps a workload's spend and wall-clock.
type Budget struct {
	MaxUSD   float64       `yaml:"maxUSD,omitempty"   json:"maxUSD,omitempty"`
	Deadline time.Duration `yaml:"deadline,omitempty" json:"deadline,omitempty"`
}
