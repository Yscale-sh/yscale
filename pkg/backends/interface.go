package backends

import (
	"context"
	"os"
	"strings"
	"time"
)

// NodeNamePrefix is the prefix the controller stamps on every burst-node
// name. Backends use this in CleanupOrphans / ListPooledNodes to filter
// out non-yscale resources that share the same Fly.io app or cloud
// account. Centralised so renames can't drift across backends.
const NodeNamePrefix = "ys-burst-"

// OrphanGracePeriod is the minimum provider-reported age an owned but
// untracked instance must reach before CleanupOrphans may destroy it.
//
// It exists because CreateNode writes the VM at the provider BEFORE the caller
// persists the record that tracks it. For that window the provider says "this
// is yours" while the tracked set says "never heard of it" — indistinguishable
// from a leak, so an ungated sweep against a live caller can delete a burst
// that is merely mid-create. Provider creation time is the one signal that
// separates the two without shared state: it needs no in-memory in-flight set
// (the caller cannot even know the provider-assigned ID until CreateNode
// returns), no locking, and it survives a restart.
//
// The value must comfortably exceed the longest create-to-persist window. The
// slowest of those is a long-running ARM/GCE create plus the record write —
// seconds to low minutes even when a provider is degraded. 15 minutes clears
// that with room to spare, and it is what makes a periodic sweep safe rather
// than startup-only (see decider.SweepOrphans).
const OrphanGracePeriod = 15 * time.Minute

// OrphanTooYoung reports whether an instance created at createdAt is still
// inside the create-race window and must be left alone this pass. A zero
// createdAt means the provider reported no creation time; that counts as too
// young, because skipping a real leak costs one more sweep interval while
// destroying a live burst costs the customer their job.
func OrphanTooYoung(createdAt, now time.Time) bool {
	return createdAt.IsZero() || now.Sub(createdAt) < OrphanGracePeriod
}

// Join modes determine how a burst node joins the cluster.
const (
	JoinModeK3s     = "k3s"
	JoinModeKubelet = "kubelet"
)

// Backend type names — use as the `backend.type` value in config and as the
// expected Backend.Name() return. BackendAuto is the workload-spec opt-out
// that lets the central decider pick.
const (
	TypeFlyIO   = "flyio"
	TypeLinode  = "linode"
	TypeAWS     = "aws"
	TypeGCP     = "gcp"
	TypeAzure   = "azure"
	BackendAuto = "auto"
)

// defaultAgentImage is the prebuilt OCI image burst nodes pull as
// their root filesystem (kubelet + tailscale + containerd). Hosted
// on Fly's registry — built and pushed via `make burst-image-push`
// (see Makefile). Fly machines pull this directly without
// imagePullSecrets because it lives in the same Fly org as the
// burst-machine call.
//
// That last property is also why this default is useless to anyone else: the
// image is private to ITS Fly org, and the credential-free pull only works
// from inside that org. A different operator builds their own and sets
// YSCALE_BURST_IMAGE.
const defaultAgentImage = "registry.fly.io/yscale-burst-image:kubelet-latest"

// AgentImage resolves the burst node image, preferring YSCALE_BURST_IMAGE over
// the compiled-in default so a fresh install can boot at all. Resolved per call
// rather than cached in a package var: an env var read at init() is invisible
// to tests and to any process that configures its environment after start.
func AgentImage() string {
	if v := strings.TrimSpace(os.Getenv("YSCALE_BURST_IMAGE")); v != "" {
		return v
	}
	return defaultAgentImage
}

// NodeLifecycle labels how the node was created. The zero-value
// (empty string) is treated as a cold node for backward compatibility.
type NodeLifecycle string

const (
	LifecycleCold    NodeLifecycle = "cold"
	LifecyclePrewarm NodeLifecycle = "prewarm"
)

// NodeSpec holds the information needed to create a burst node on a backend.
type NodeSpec struct {
	Name    string
	BurstID string // central-assigned ID like "burst_abcdef"; doubles as the auth secret for the bootstrap fetch
	// Region is a hard provider-region constraint when non-empty.
	Region     string
	AgentImage string
	// JoinMode is "k3s" or "kubelet".
	JoinMode string
	// K3s mode fields.
	K3sServerURL string
	K3sToken     string
	// Kubelet mode fields (SaaS).
	APIServer      string
	ClusterCA      string
	BootstrapToken string
	// BootstrapEndpoint is the URL the burst node hits to fetch its
	// real kubelet kubeconfig once Tailscale is up. SaaS only; format
	// is "http://<agent-ts-hostname>:8080" so MagicDNS resolves it
	// inside the yscale tailnet.
	BootstrapEndpoint string
	// Tailscale.
	// TSAuthKey is an ephemeral pre-authorized key minted by central
	// per burst (SaaS) or a shared key from the operator's config (OSS).
	// TSHostname is the device hostname registered with Tailscale.
	// TSTags are ACL tags (e.g. "tag:customer-acme-burst") that scope
	// the device's reachable peers; required on SaaS for isolation.
	TSAuthKey  string
	TSHostname string
	TSTags     []string
	// LoginServer, when non-empty, points the burst's 'tailscale up' at a
	// self-hosted self-hosted coordination server (per-customer factory box).
	// Empty (default) = Tailscale SaaS, no --login-server flag.
	LoginServer string
	// PodCIDR is the /24 carved for pods scheduled on this burst
	// (e.g. "10.244.42.0/24"). Used by CNI on the burst.
	PodCIDR string
	// Tier selects which CNI runs on the burst. "full" (empty = full) is the
	// only supported value: bursts run the host-mode cilium-agent baked into
	// the rootfs so cross-cluster pod-to-pod, Cilium identity and
	// NetworkPolicy work. The "lite" bridge-CNI tier has been removed and is
	// refused during workload admission, so no NodeSpec built by central
	// carries anything else. The field stays a string so wire-decoded specs
	// keep round-tripping.
	Tier string
	// Labels to apply to the node.
	NodeLabels map[string]string
	// Lifecycle is "cold" (default) or "prewarm". LifecyclePrewarm is a
	// provider capability marker only: callers must implement durable claim,
	// readiness, expiry, and one-shot destruction before creating one.
	Lifecycle NodeLifecycle
	// ScopeHash is a stable non-secret ownership-domain hash that survives
	// controller restarts. SaaS scopes should bind deployment, tenant, and
	// cluster; never derive this from a process or replica identity. Backends
	// sharing a resource pool use it for exact isolation. Empty = legacy
	// unscoped ownership, not a wildcard.
	ScopeHash string
	// ConfigHash is a non-secret hash of the join configuration that
	// affects node identity. Used to reject stale prewarm nodes whose
	// config no longer matches (e.g. apiServer changed).
	ConfigHash string
	// VM sizing.
	Resources ResourceRequirements
	// ModelVolume, when non-empty, names a pre-seeded backend block-
	// storage volume (by label) to attach to this burst so the workload
	// pod can mount pre-downloaded model weights instead of fetching
	// them. Honored by the Linode backend; ignored elsewhere.
	ModelVolume string
}

type ResourceRequirements struct {
	CPUMillis int64 // 1000 = 1 CPU
	MemoryMB  int64
	// Region is a hard provider-region constraint when non-empty.
	Region string
	// GPU is non-nil when a GPU is required.
	GPU *GPUSpec
}

// GPUSpec describes the requested GPU shape. Kind+Count is the supported live
// provider contract. SKUs carries a requested explicit pin into central
// admission, which currently rejects it until providers guarantee exact launch
// and authoritative pricing.
type GPUSpec struct {
	Kind  string
	Count int
	// CPUMillis and MemoryMB are the complete burst-node requirement. GPU
	// backends use them to select a GPU plan tier large enough for the pod.
	CPUMillis int64
	MemoryMB  int64
	// SKUs is reserved for future exact provider SKU selection. Current central
	// admission rejects non-empty values before invoking a backend.
	SKUs []string
}

// NodeStatus represents the current state of a burst node on a backend.
type NodeStatus struct {
	Phase     NodePhase
	IP        string
	StartedAt time.Time
}

type NodePhase string

const (
	NodeRunning  NodePhase = "running"
	NodeStarting NodePhase = "starting"
	NodeStopped  NodePhase = "stopped"
	NodeFailed   NodePhase = "failed"
	NodeUnknown  NodePhase = "unknown"
)

// ProviderNodeState is an exact provider lifecycle state used by optional
// capability interfaces. It is deliberately separate from NodePhase, whose
// scheduler-facing mapping collapses cold-stopped and suspended machines.
type ProviderNodeState string

const (
	ProviderStateStarted   ProviderNodeState = "started"
	ProviderStateStopped   ProviderNodeState = "stopped"
	ProviderStateSuspended ProviderNodeState = "suspended"
	ProviderStateDestroyed ProviderNodeState = "destroyed"
)

// PooledNode represents a backend-reported fast-start candidate. Backends may
// return cold-stopped nodes or suspended snapshots according to their contract;
// callers must still validate ownership and lifecycle before claiming one.
type PooledNode struct {
	BackendID  string
	Name       string
	Lifecycle  NodeLifecycle // "cold", "prewarm", or "" for legacy
	ScopeHash  string        // pool scope for multi-controller isolation
	ConfigHash string        // join-config hash for warm matching
	CreatedAt  time.Time     // machine creation time; zero if unknown
}

// OwnedNode is a provider resource that the backend has proven carries exact
// Yscale ownership metadata. A name prefix alone must never produce one of
// these; implementations should reuse the same tag/label/metadata checks that
// guard CleanupOrphans, without deleting anything.
type OwnedNode struct {
	BackendID string
	Name      string
	BurstID   string
	CreatedAt time.Time
}

// Backend is the interface that all burst backends must implement.
type Backend interface {
	// CreateNode provisions a new burst node from scratch.
	CreateNode(ctx context.Context, spec *NodeSpec) (backendID string, err error)

	// StartNode starts a stopped/pooled machine. Much faster than CreateNode.
	StartNode(ctx context.Context, backendID string) error

	// StopNode stops a running machine without destroying it (returns to pool).
	StopNode(ctx context.Context, backendID string) error

	// DeleteNode destroys a burst node permanently.
	DeleteNode(ctx context.Context, backendID string) error

	// GetNodeStatus returns the current status of a burst node.
	GetNodeStatus(ctx context.Context, backendID string) (*NodeStatus, error)

	// ListPooledNodes returns stopped machines available for fast start.
	ListPooledNodes(ctx context.Context) ([]PooledNode, error)

	// CleanupOrphans destroys machines and volumes that are running but not
	// tracked by the controller. Implementations MUST skip any candidate
	// younger than OrphanGracePeriod, which is what lets a live caller run
	// this on a timer without deleting a burst that is still mid-create.
	CleanupOrphans(ctx context.Context, trackedBackendIDs map[string]bool) (destroyed int, err error)

	// Name returns the backend name for logging/metrics.
	Name() string
}

// InventoryBackend is the optional read-only provider reconciliation seam.
// Backend stays stable for existing callers; central uses this capability only
// where a configured provider can prove exact ownership before reconciliation.
type InventoryBackend interface {
	ListOwnedNodes(ctx context.Context) ([]OwnedNode, error)
}

// WarmCapability is implemented by backends that expose a provider-level
// suspend operation (currently Fly.io). It intentionally does not imply that
// the controller has enabled warm pooling. A safe caller must pair this seam
// with durable ownership, an atomic one-shot claim, provider-state waits,
// Kubernetes readiness checks, expiry, and cold-start fallback.
type WarmCapability interface {
	// SuspendNode suspends a running machine into a low-cost paused
	// state. Unlike StopNode (which cold-stops), a suspended machine
	// retains its process state, Tailscale identity, and kubelet
	// state. Resume is best-effort; the caller owns TTL and fallback policy.
	SuspendNode(ctx context.Context, backendID string) error

	// WaitNodeState blocks until the provider confirms an exact state or the
	// context ends. A successful wait for "started" proves only provider state;
	// callers must still detect cold-boot fallback and verify Kubernetes/mesh
	// readiness before assigning work.
	WaitNodeState(ctx context.Context, backendID string, state ProviderNodeState) error
}

// ScopedBackend is implemented by backends that support exact pool scoping to
// isolate controllers sharing the same provider resource pool. Scope matching
// is strict in both directions: an empty scope owns only legacy unscoped
// resources and is never a wildcard over scoped resources.
type ScopedBackend interface {
	SetPoolScope(hash string)
}
