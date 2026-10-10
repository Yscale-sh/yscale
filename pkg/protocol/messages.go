// Package protocol defines the wire messages exchanged between the
// central yscale.sh server and the cluster-side agent.
//
// Transport: JSON over WebSocket. Each message is a JSON-encoded
// `Envelope` with a discriminator (`Type`) and a typed `Body` that the
// receiver decodes by switching on Type.
//
// All messages are versioned via the package APIVersion constant.
// Bumping APIVersion is a hard break; both sides reject mismatched
// streams during the handshake.
package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"time"
	"unicode/utf8"
)

// APIVersion is the wire-format version. Bumped on breaking changes.
const APIVersion = "v1"

// Envelope wraps every message. Sender sets Type and Body; receiver
// decodes Body into the matching struct from this package.
type Envelope struct {
	APIVersion string          `json:"apiVersion"`
	Type       MessageType     `json:"type"`
	ID         string          `json:"id,omitempty"` // request id (for ACK correlation)
	Timestamp  time.Time       `json:"timestamp"`
	Body       json.RawMessage `json:"body,omitempty"`
}

// MessageType is a discriminator. Server-bound and agent-bound types
// share one namespace so a misrouted message is obviously wrong.
type MessageType string

const (
	// Agent → server (handshake & telemetry).
	TypeHello         MessageType = "hello"          // agent identifies itself
	TypeHeartbeat     MessageType = "heartbeat"      // periodic liveness
	TypePodEvent      MessageType = "pod_event"      // pending/scheduled/finished
	TypeNodeEvent     MessageType = "node_event"     // ready/notReady/deleted
	TypeCommandAck    MessageType = "command_ack"    // ack of a server command
	TypeClusterRoutes MessageType = "cluster_routes" // gateway-advertised CIDRs

	// Server → agent (control plane).
	TypePodEventAck         MessageType = "pod_event_ack"         // pod observation is durably stored; stop retrying it
	TypeNodeEventAck        MessageType = "node_event_ack"        // terminal node event is resolved; stop retrying it
	TypeBurstAnnounce       MessageType = "burst_announce"        // burst about to come up on tailnet; carries storage bindings
	TypeCreateJob           MessageType = "create_job"            // create a K8s Job for a workload
	TypeDeleteJob           MessageType = "delete_job"            // tear down a finished workload
	TypeDrainNode           MessageType = "drain_node"            // cordon + evict before destroy
	TypeFetchWorkloadLogs   MessageType = "fetch_workload_logs"   // bounded pod-log snapshot
	TypeSyncRuntimeBindings MessageType = "sync_runtime_bindings" // sync tenant-owned runtime env bindings

	// TypePrepareIdleTeardown asks whether an idle burst node is STILL idle. It
	// is its own type rather than a mode on drain_node for one reason: an older
	// connector ignores a field it does not know and would run the full drain,
	// evicting the customer's pods to answer a question. An unknown TYPE is
	// answered with a failed CommandAck, which is the only safe default here.
	TypePrepareIdleTeardown MessageType = "prepare_idle_teardown"

	// TypeReleaseIdleTeardown gives back the cordon a prepare_idle_teardown
	// placed, when the teardown it was placed for did not happen. Separate from
	// the preflight because it is the only path allowed to uncordon: a connector
	// too old to know it answers with a failed CommandAck, which says the node is
	// still cordoned rather than pretending it was released.
	TypeReleaseIdleTeardown MessageType = "release_idle_teardown"
)

// ----- Agent → server bodies -----

// Hello is the first message after WS handshake. Agent identifies the
// cluster; server accepts the connection or closes on rejection.
//
// On the Tailscale-based SaaS path the agent runs as a tailnet device
// itself (hostname `yscale-agent-<cluster-id>`); central uses the
// cluster ID to compute the burst's BOOTSTRAP_ENDPOINT (resolved by
// MagicDNS) so no WG-endpoint fields are needed in Hello.
type Hello struct {
	AgentVersion string `json:"agent_version"`
	ClusterID    string `json:"cluster_id"` // stable per cluster (UUID stored in Secret)
	K8sVersion   string `json:"k8s_version,omitempty"`
	// WorkloadNamespace is the connector's single namespace scope when it is
	// pinned to one namespace. Empty means the connector is not namespace-pinned.
	WorkloadNamespace string `json:"workload_namespace,omitempty"`

	// AuthoritativeOccupancy says this connector is equipped to read the WHOLE
	// cluster's pods, and so is the kind of connector that can end a nodeOnly
	// burst by reporting its node idle. It is the admission-time half of the
	// contract NodeEvent.OccupancyObserved reports at runtime: central needs to
	// know at SUBMIT time whether the teardown signal is ever coming, because by
	// the time the silence would be measurable the capacity is already running.
	//
	// omitempty, and false is the honest default for every connector too old to
	// send it: an absent claim is not a claim. Central admits those nodeOnly
	// bursts with a finite deadline instead of waiting for an observation that
	// will never arrive.
	AuthoritativeOccupancy bool `json:"authoritative_occupancy,omitempty"`
}

// Heartbeat is sent periodically (~30s). Lets server detect dead
// agents and surface basic cluster size in dashboards.
type Heartbeat struct {
	// InventoryObserved says the connector successfully read the inventory
	// fields below in this heartbeat. Absent/false means the counts are not
	// facts, preserving old connectors and failed reads as "not reported".
	InventoryObserved bool `json:"inventory_observed"`

	// NodeInventoryObserved scopes NodeCount and BurstCount. PodInventoryObserved
	// scopes PendingPods. The timestamps are when the connector observed each
	// scope; old connectors omit them and central uses receipt time.
	NodeInventoryObserved   bool       `json:"node_inventory_observed,omitempty"`
	PodInventoryObserved    bool       `json:"pod_inventory_observed,omitempty"`
	NodeInventoryObservedAt *time.Time `json:"node_inventory_observed_at,omitempty"`
	PodInventoryObservedAt  *time.Time `json:"pod_inventory_observed_at,omitempty"`

	NodeCount   int `json:"node_count"`
	BurstCount  int `json:"burst_count"`
	PendingPods int `json:"pending_pods"`

	// GPUTelemetry carries bounded GPU utilisation samples from burst nodes
	// that reported to this connector since the last heartbeat. Additive:
	// older connectors omit it and central ignores a nil/empty slice.
	GPUTelemetry []GPUTelemetrySample `json:"gpu_telemetry,omitempty"`
}

// MaxGPUTelemetrySamples caps how many samples a single heartbeat may carry.
const MaxGPUTelemetrySamples = 64

// MaxGPUTelemetryAge is how old an observation may be before central rejects
// it as stale. Generous: a sample from a burst that reported 5 minutes ago is
// still useful dashboard state.
const MaxGPUTelemetryAge = 5 * time.Minute

// GPUTelemetrySample is one burst node's GPU utilisation observation,
// reported by the burst's nvidia-smi heartbeat through the connector.
type GPUTelemetrySample struct {
	BurstID     string    `json:"burst_id"`
	NodeName    string    `json:"node_name"`
	Utilization float64   `json:"utilization"`
	ObservedAt  time.Time `json:"observed_at"`
}

// ValidGPUTelemetrySample checks that a single sample is well-formed: burst id
// and node name present, utilization in [0,100], finite, and the observation
// timestamp is not zero. Freshness and tenancy are the caller's responsibility.
func ValidGPUTelemetrySample(s GPUTelemetrySample) bool {
	if s.BurstID == "" || s.NodeName == "" {
		return false
	}
	if s.ObservedAt.IsZero() {
		return false
	}
	if math.IsNaN(s.Utilization) || math.IsInf(s.Utilization, 0) {
		return false
	}
	if s.Utilization < 0 || s.Utilization > 100 {
		return false
	}
	return true
}

// ClusterRoutes reports the masked route set the gateway advertises.
type ClusterRoutes struct {
	Routes []string `json:"routes"` // flat masked union; matches the ConfigMap and gateway advertisement
}

// PodEvent informs the server of pod lifecycle changes for workloads
// the server cares about (i.e. spawned by it). Matched via the
// `yscale.sh/workload` label.
type PodEvent struct {
	WorkloadID string    `json:"workload_id"`
	Phase      string    `json:"phase"`            // Pending | Running | Succeeded | Failed
	Reason     string    `json:"reason,omitempty"` // e.g. "Unschedulable"
	NodeName   string    `json:"node_name,omitempty"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	ExitCode   *int      `json:"exit_code,omitempty"`

	// PodName is the Kubernetes Pod name the connector observed. Additive:
	// older connectors omit it and central leaves the field empty.
	PodName string `json:"pod_name,omitempty"`

	// Scheduled is true when the connector observed the Pod with
	// PodScheduled=True AND a non-empty spec.nodeName. ScheduledAt is the
	// PodScheduled condition's LastTransitionTime, stable across replays.
	// Both are additive: older connectors omit them and central treats
	// absence as "not observed".
	Scheduled   bool       `json:"scheduled,omitempty"`
	ScheduledAt *time.Time `json:"scheduled_at,omitempty"`

	// SchedulingState is the explicit scheduling observation. Older
	// connectors omit it and continue through the positive-only path above.
	SchedulingState string `json:"scheduling_state,omitempty"`
	// SchedulingReason is a bounded classification for Waiting observations;
	// raw scheduler messages never cross the wire.
	SchedulingReason string `json:"scheduling_reason,omitempty"`
	// SchedulingMessage is the fixed safe message for SchedulingReason.
	SchedulingMessage string `json:"scheduling_message,omitempty"`
	// SchedulingObservedAt is the source transition time used for monotonic
	// ordering and ACK identity.
	SchedulingObservedAt *time.Time `json:"scheduling_observed_at,omitempty"`
}

// NodeEvent informs the server of burst-node lifecycle changes.
type NodeEvent struct {
	BurstID  string `json:"burst_id"` // server-assigned id from AddPeer
	NodeName string `json:"node_name,omitempty"`
	Phase    string `json:"phase"` // Joining | Ready | NotReady | Removed
	Reason   string `json:"reason,omitempty"`

	// ObservedAt is when the connector's NodeWatcher actually observed this
	// phase transition. Central uses it for authoritative ordering so a delayed
	// or replayed event cannot overwrite a newer observation.
	//
	// Additive: older connectors omit it and central falls back to its own
	// receipt time, which is what the field would have been before source
	// timestamps existed. Official connectors use stable Kubernetes object
	// timestamps so reconnect replay preserves the original ordering key.
	ObservedAt *time.Time `json:"observed_at,omitempty"`

	// OccupancyObserved says the connector read the WHOLE cluster's pods for
	// this sweep and so could have told central the node was idle. It is the
	// only thing central's nodeOnly silence ceiling measures from, because it is
	// the only claim that means "the teardown signal is still working".
	//
	// A health phase does not imply it and must never be read as if it did: a
	// namespace-scoped connector, or one that has lost cluster-wide pod LIST,
	// still sees Nodes perfectly well while being blind to exactly the thing
	// that ends a nodeOnly burst. Only IdleNodeWatcher sets it, and only after
	// the pod list it acts on succeeded.
	OccupancyObserved bool `json:"occupancy_observed,omitempty"`

	// GPUAllocatable is true when the connector observed a positive
	// nvidia.com/gpu allocatable quantity on this burst node. Additive:
	// older connectors omit it; central treats absence as "not observed".
	// CPU-only bursts never set it.
	GPUAllocatable bool `json:"gpu_allocatable,omitempty"`
	// GPUAllocatableAt is when the connector first observed GPUs available.
	// Stable across replays when derived from a Kubernetes object timestamp.
	GPUAllocatableAt *time.Time `json:"gpu_allocatable_at,omitempty"`
}

// The closed set of phases a NodeEvent may report. They are wire values and
// stable strings: a stored burst carries the last one observed, so a phase
// renamed here would silently reclassify records written by an older agent.
//
// Joining, Ready and NotReady are STATUS — what the connector saw in
// Kubernetes. NotReady in particular is a node that stopped answering, which a
// burst recovers from often enough that tearing down on it would destroy live
// work.
//
// Removed and Idle are TERMINAL: each asks central to end the burst, and each is
// held by the connector until central acknowledges it. They are not the same
// claim. Removed is a fact — the Node object is gone from the customer's
// cluster, so nothing in it can produce another phase for that burst. Idle is a
// REQUEST, and only ever about a node that is still there: the connector has
// watched a node-only burst carry no schedulable work for its whole idle grace
// and is asking for the teardown a KEDA-style scale-down has no other way to
// trigger. Central decides whether to honour it; see PersistableNodePhase for
// why it is never written down as health.
const (
	NodePhaseJoining  = "Joining"
	NodePhaseReady    = "Ready"
	NodePhaseNotReady = "NotReady"
	NodePhaseRemoved  = "Removed"
	NodePhaseIdle     = "Idle"
)

// NodeEventAck tells the connector that a terminal node event is RESOLVED and
// need not be reported again: the burst behind it is definitively settled — torn
// down by this report, claimed by another winner, never this tenant's to begin
// with, or (for Idle alone) a burst central owns the lifecycle of and will not
// tear down on a connector's request.
//
// It is a delivery receipt, not a command: it carries no Envelope.ID and the
// connector answers it with nothing. A CommandAck back would be an
// acknowledgement of an acknowledgement, and central has no correlation waiting
// for one.
//
// The three outcomes are ONE message on purpose. A connector that could tell
// "another tenant holds that id" from "no such burst" would have a fleet-wide
// burst-id oracle, so both are the same receipt, and neither says whether a
// teardown ran.
//
// Its absence is the meaningful signal. Central withholds it whenever the burst
// may still exist — a state, broker or provider failure — and the connector
// keeps the report queued until it arrives.
type NodeEventAck struct {
	BurstID string `json:"burst_id"`
	Phase   string `json:"phase"`
}

// PodEventAck tells the connector that the pod scheduling observation for a
// workload has been durably stored. The connector evicts the cached event on
// receipt, and only when the acknowledged identity matches the cached one.
// Without this acknowledgement the connector retains and re-sends the event on
// reconnect and on its retry loop.
//
// PodName and ScheduledAt echo the EXACT identity central persisted. They are
// the tokens the connector matches against its cache: an ACK that names a
// different pod, or a different ScheduledAt for the same pod, is about a
// different observation and MUST NOT clear a cached versioned event. A legacy
// ACK omits both — from a central too old to send them — and clears only a
// cached event that itself carries no ScheduledAt (a legacy pre-versioned
// observation). This is what keeps a stale receipt from retiring a fresher
// event held by a versioned connector.
type PodEventAck struct {
	WorkloadID           string     `json:"workload_id"`
	PodName              string     `json:"pod_name,omitempty"`
	ScheduledAt          *time.Time `json:"scheduled_at,omitempty"`
	SchedulingState      string     `json:"scheduling_state,omitempty"`
	SchedulingObservedAt *time.Time `json:"scheduling_observed_at,omitempty"`
}

const (
	SchedulingStateScheduled = "Scheduled"
	SchedulingStateWaiting   = "Waiting"
)

func ValidSchedulingState(state string) bool {
	return state == SchedulingStateScheduled || state == SchedulingStateWaiting
}

const (
	SchedulingReasonInsufficientCPU      = "InsufficientCPU"
	SchedulingReasonInsufficientMemory   = "InsufficientMemory"
	SchedulingReasonInsufficientGPU      = "InsufficientGPU"
	SchedulingReasonUntoleratedTaint     = "UntoleratedTaint"
	SchedulingReasonNodeSelectorMismatch = "NodeSelectorMismatch"
	SchedulingReasonUnsupportedSelector  = "UnsupportedSelector"
	SchedulingReasonUnschedulable        = "Unschedulable"
)

func ValidSchedulingReason(reason string) bool {
	switch reason {
	case SchedulingReasonInsufficientCPU,
		SchedulingReasonInsufficientMemory,
		SchedulingReasonInsufficientGPU,
		SchedulingReasonUntoleratedTaint,
		SchedulingReasonNodeSelectorMismatch,
		SchedulingReasonUnsupportedSelector,
		SchedulingReasonUnschedulable:
		return true
	}
	return false
}

func SchedulingReasonMessage(reason string) string {
	switch reason {
	case SchedulingReasonInsufficientCPU:
		return "insufficient CPU available on any node"
	case SchedulingReasonInsufficientMemory:
		return "insufficient memory available on any node"
	case SchedulingReasonInsufficientGPU:
		return "insufficient GPU devices available on any node"
	case SchedulingReasonUntoleratedTaint:
		return "no node with a tolerated taint available"
	case SchedulingReasonNodeSelectorMismatch:
		return "no node matches the required node selector or affinity"
	case SchedulingReasonUnsupportedSelector:
		return "node selector references an unsupported or unknown label"
	default:
		return "pod cannot be scheduled"
	}
}

// ValidNodePhase reports whether phase is one of the lifecycle phases. Central
// checks it before anything is stored or torn down: an unrecognised phase is a
// newer agent or a corrupt frame, and neither is a reason to guess.
func ValidNodePhase(phase string) bool {
	switch phase {
	case NodePhaseJoining, NodePhaseReady, NodePhaseNotReady, NodePhaseRemoved, NodePhaseIdle:
		return true
	}
	return false
}

// TerminalNodePhase reports whether a phase asks central to END the burst, and
// so must be held by the connector and re-sent until central acknowledges it.
// The live phases are re-asserted by the next transition and by the reconnect
// flush, so nothing is owed on them.
func TerminalNodePhase(phase string) bool {
	switch phase {
	case NodePhaseRemoved, NodePhaseIdle:
		return true
	}
	return false
}

// PersistableNodePhase reports whether a phase describes the node's HEALTH and
// may therefore be recorded on the burst.
//
// Idle is the one that may not. It is a request to tear a node down, not
// something that is true of the node — the kubelet is Ready and answering
// throughout — so writing it would replace a live burst's status with a phase no
// customer could act on, and would leave that status behind on a burst whose
// teardown was then refused or lost. The teardown is the whole effect an idle
// report is allowed to have.
func PersistableNodePhase(phase string) bool {
	return ValidNodePhase(phase) && phase != NodePhaseIdle
}

// CommandAck is the response to any server-sent command.
type CommandAck struct {
	CommandID string `json:"command_id"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`
	// Result is an optional command-specific response. Keeping it on the
	// existing acknowledgement preserves request correlation and remains wire
	// compatible with agents that predate result-bearing commands.
	Result json.RawMessage `json:"result,omitempty"`
}

// ----- Server → agent bodies -----

// BurstAnnounce informs the agent that a burst is being provisioned
// (TS auth key minted, backend CreateNode in flight). Carries storage
// bindings the agent should serve when the burst fetches its bootstrap
// kubeconfig over the tailnet.
type BurstAnnounce struct {
	BurstID    string `json:"burst_id"`
	TSHostname string `json:"ts_hostname"` // expected TS device hostname (for log correlation)

	// Tier is the burst's networking tier. Only "full" is emitted by any
	// supported central: the "lite" tier has been removed and is refused
	// during workload admission, so no announce for a lite burst is ever
	// authored. Empty (older central) also means full. The field is kept on
	// the wire so old agents keep decoding announces without a schema break.
	Tier string `json:"tier,omitempty"`

	// Namespace is the workload's Kubernetes namespace, as central
	// resolved it from the validated workload spec. It is the ONLY
	// namespace the agent will read a storage-credentials Secret from
	// for this burst — the burst's own sign-urls request never gets to
	// name one. Empty means an older central that predates the field;
	// the agent then refuses to sign (see StorageSigner.handleSign),
	// while everything else about the burst keeps working.
	Namespace string `json:"namespace,omitempty"`

	// Storage bindings the agent should remember for this burst. The
	// burst fetches these from the agent during its bootstrap call;
	// they describe where to mount cache + persistent volumes and how
	// to (optionally) snapshot back to the customer's bucket.
	Storage []StorageBinding `json:"storage,omitempty"`
}

// StorageBinding is one cache or persistent volume the burst should
// wire up. Sent from central to agent via AddPeer; forwarded from
// agent to burst via the bootstrap response.
type StorageBinding struct {
	Name      string `json:"name"`
	Type      string `json:"type"`       // "cache" | "persistent" | "artifact"
	LocalPath string `json:"local_path"` // path on burst where backend volume is mounted
	Target    string `json:"target"`     // path inside the customer's pod

	// CacheKey is set on cache bindings — burst uses it to namespace
	// the local cache directory. Empty for persistent bindings.
	CacheKey string `json:"cache_key,omitempty"`

	// Source is set for cache bindings; burst calls the agent's
	// /storage/sign-urls endpoint with this to get pre-signed GETs.
	Source *BucketRef `json:"source,omitempty"`

	// SnapshotTo + SnapshotInterval are set for persistent bindings
	// when snapshot is configured. Burst pushes diffs to this bucket
	// every Interval; agent signs PUTs on demand.
	SnapshotTo       *BucketRef `json:"snapshot_to,omitempty"`
	SnapshotInterval string     `json:"snapshot_interval,omitempty"` // e.g. "5m"

	// WriteTo is the exact write-only object sink for a post-run artifact
	// output. It is separate from SnapshotTo so a signer can distinguish a
	// durable-volume mirror from a one-shot Job result without guessing from
	// the binding type.
	WriteTo *BucketRef `json:"write_to,omitempty"`
}

// BucketRef mirrors workload.BucketRef at the protocol level so the
// protocol package stays free of workload-spec deps.
type BucketRef struct {
	Bucket            string `json:"bucket"`
	Prefix            string `json:"prefix,omitempty"`
	Endpoint          string `json:"endpoint,omitempty"`
	Region            string `json:"region,omitempty"`
	CredentialsSecret string `json:"credentials_secret"` // K8s Secret in customer's cluster
}

// CreateJob instructs the agent to create a K8s Job in the customer's
// cluster. The full Job spec is rendered by the server (it owns the
// Workload→Job translation); the agent just applies it.
type CreateJob struct {
	WorkloadID string          `json:"workload_id"`
	Namespace  string          `json:"namespace"`
	JobSpec    json.RawMessage `json:"job_spec"` // serialized batchv1.Job
}

const (
	RuntimeBindingsSecretName         = "yscale-runtime-bindings"
	MaxRuntimeBindings                = 32
	MaxRuntimeBindingValueBytes       = 8192
	MaxRuntimeBindingTotalBytes       = MaxRuntimeBindings * MaxRuntimeBindingValueBytes
	MaxRuntimeBindingRevision   int64 = 1<<62 - 1
)

var (
	runtimeBindingKeyRE   = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,62}$`)
	kubernetesNamespaceRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

// SyncRuntimeBindings replaces the fixed runtime-bindings Secret data in one
// tenant namespace. The Secret name is intentionally not on the wire.
type SyncRuntimeBindings struct {
	Namespace string            `json:"namespace"`
	Revision  int64             `json:"revision"`
	Bindings  map[string]string `json:"bindings"`
}

func ValidRuntimeBindingKey(key string) bool {
	return runtimeBindingKeyRE.MatchString(key)
}

func ValidateRuntimeBindingValue(value string) error {
	if value == "" || len(value) > MaxRuntimeBindingValueBytes || !utf8.ValidString(value) {
		return errors.New("invalid runtime binding value")
	}
	for _, r := range value {
		if r == 0 {
			return errors.New("invalid runtime binding value")
		}
	}
	return nil
}

func ValidKubernetesNamespace(namespace string) bool {
	return kubernetesNamespaceRE.MatchString(namespace)
}

func ValidateSyncRuntimeBindings(cmd SyncRuntimeBindings) error {
	if !ValidKubernetesNamespace(cmd.Namespace) {
		return fmt.Errorf("invalid runtime binding namespace")
	}
	if cmd.Revision < 0 || cmd.Revision > MaxRuntimeBindingRevision {
		return fmt.Errorf("invalid runtime binding revision")
	}
	if len(cmd.Bindings) > MaxRuntimeBindings {
		return fmt.Errorf("too many runtime bindings")
	}
	total := 0
	for key, value := range cmd.Bindings {
		if !ValidRuntimeBindingKey(key) {
			return fmt.Errorf("invalid runtime binding key")
		}
		if err := ValidateRuntimeBindingValue(value); err != nil {
			return err
		}
		total += len(value)
		if total > MaxRuntimeBindingTotalBytes {
			return fmt.Errorf("runtime bindings too large")
		}
	}
	return nil
}

func RuntimeBindingKeys(bindings map[string]string) []string {
	keys := make([]string, 0, len(bindings))
	for key := range bindings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// DeleteJob instructs the agent to tear down a workload's Job.
type DeleteJob struct {
	WorkloadID string `json:"workload_id"`
	Namespace  string `json:"namespace"`
}

// DrainNode instructs the agent to cordon + evict pods on a node
// (typically just before the server destroys the backing burst VM).
//
// When Delete is set, the agent also removes the Node object from the
// cluster API after draining — used by burst teardown, where the
// backing VM is already gone and the Node would otherwise linger
// NotReady forever (k8s won't garbage-collect it without a cloud
// provider integration).
type DrainNode struct {
	NodeName string `json:"node_name"`
	Timeout  string `json:"timeout,omitempty"` // e.g. "60s"
	Delete   bool   `json:"delete,omitempty"`
}

// PrepareIdleTeardown asks the connector to CONFIRM, against the customer's own
// API server, that a burst node is still carrying no work — and to keep it that
// way by leaving it cordoned. It destroys nothing.
//
// It exists because an idle report is evidence from the connector's last sweep,
// and the scheduler is free to bind a pod into the gap between that sweep and
// central's claim. Nothing central holds can see that pod. The connector cordons
// first and lists after, so a successful acknowledgement is a fact that stays
// true until central destroys the node rather than a snapshot that was already
// stale when it was taken.
//
// The CommandAck is the whole answer: success means empty and cordoned, and
// anything else — a refusal, a pod found, an unreadable API, a connector too old
// to know the type, a dropped socket — means central tears nothing down.
type PrepareIdleTeardown struct {
	NodeName string `json:"node_name"`
}

// IdleTeardownPreflight is the CommandAck.Result of a successful
// prepare_idle_teardown, and it is what makes the cordon recoverable.
//
// The cordon outlives the command that placed it — deliberately, since central
// claims and reaps behind it. The resourceVersion remains a compatibility token
// that lets an explicit ReleaseIdleTeardown undo exactly this cordon; current
// central keeps it in place while a pending reap is retried.
//
// EMPTY means this preflight placed no cordon, because the node was already
// unschedulable when it arrived. That cordon belongs to something else — an
// operator, a drain in flight — and there is nothing here to give back.
type IdleTeardownPreflight struct {
	CordonResourceVersion string `json:"cordon_resource_version,omitempty"`
}

// ReleaseIdleTeardown asks the connector to give back the cordon one specific
// prepare_idle_teardown placed. It is a compatibility/recovery command for a
// teardown that has been cancelled; a pending reap keeps its cordon while the
// watchdog retries. It evicts nothing and deletes nothing.
//
// CordonResourceVersion is what makes this safe to send at all. It names the
// exact node version this request's OWN cordon produced, and the connector
// applies the undo only against that version and only while the node is still
// unschedulable. Anything else — an operator cordoning the node since, a drain
// in flight, another version entirely — is somebody else's decision about the
// node, and the release leaves it alone rather than reversing it.
//
// Sending it twice is safe: the second one finds the node already schedulable
// and succeeds without writing.
type ReleaseIdleTeardown struct {
	NodeName              string `json:"node_name"`
	CordonResourceVersion string `json:"cordon_resource_version"`
}

// FetchWorkloadLogs asks the connector that owns a workload's cluster for a
// bounded snapshot. Namespace and workload id come from central's stored
// workload record, never from a browser-supplied pod or container name.
type FetchWorkloadLogs struct {
	WorkloadID string `json:"workload_id"`
	Namespace  string `json:"namespace"`
	TailLines  int64  `json:"tail_lines"`
	MaxBytes   int64  `json:"max_bytes"`
}

// WorkloadLogStream is one regular container's timestamped stdout/stderr.
type WorkloadLogStream struct {
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Output    string `json:"output"`
}

// WorkloadLogs is the safe result returned through CommandAck.Result.
type WorkloadLogs struct {
	ObservedAt time.Time           `json:"observed_at"`
	Streams    []WorkloadLogStream `json:"streams"`
	Truncated  bool                `json:"truncated"`
}
