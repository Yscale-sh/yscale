package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/broker"
	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
	"gopkg.in/yaml.v3"
)

// Workloads handles /v1/workloads.
type Workloads struct {
	Store   *state.Store
	Decider Decider
	Reaper  Reaper
	Log     *slog.Logger
	// Commands is the durable connector-command outbox. nil keeps the legacy
	// direct-send seam for embedders and focused tests; production wires Store.
	Commands ConnectorCommandLedger
	Cost     *cost.Meter // optional; nil = no cost metering
	// Teardowns, when set, is the durable queue reaps hand cloud teardown to,
	// so retries survive a central restart and can run from a separate worker
	// (the TeardownWorker in teardown.go). nil = tear down inline in-process
	// (the single-binary / OSS-local path).
	Teardowns broker.Broker

	// Deletes, when set, makes the durable provider-delete state machine
	// authoritative for every reap: reapBurst records intent and a
	// ProviderDeleteWorker performs the provider call, the cleanup and the
	// receipts. nil is the explicit compatibility seam — the inline OSS/dev
	// path, and the Redis queue in front of it, stay exactly as they were.
	//
	// The two are not layered: a production lifecycle deployment never falls
	// back to claiming a burst away before its provider resource is gone,
	// because that claim is what makes an unreachable provider an untracked
	// paid node.
	Deletes ProviderDeletes
	// DeleteWake is the optional in-process nudge to the delete worker, so a
	// reap on this replica does not wait out a poll interval. Purely a cache in
	// front of the durable queue; dropping every send costs latency and nothing
	// else.
	DeleteWake chan<- struct{}

	// Evidence is the optional reader for issue #93 durable evidence fields:
	// provider-created timestamp and durable reap receipt. nil omits both
	// from the cleanup projection, keeping existing tests/OSS paths unchanged.
	Evidence BurstEvidenceReader

	// Journal is the audited-write seam: the two store calls the submit path
	// must fail CLOSED on when the durable journal refuses them. nil means the
	// Store itself, which is every production path. It is named as an interface
	// for the reason accountStore and the store's own persister seam are — a
	// fail-closed path that cannot be driven against a refusing backend is only
	// ever observed in production, and here the difference is a reaped burst and
	// a 503 versus a leaked node behind a 202.
	Journal Journal

	// NodeOnlyMaxLifetime is the same duration the reaper watchdog enforces as
	// the observation-silence ceiling (YSCALE_NODE_ONLY_MAX_LIFETIME), read here
	// for the case the watchdog can never reach: a nodeOnly burst placed on a
	// connector that cannot observe occupancy at all. Silence is unreadable for
	// those, so the bound has to be a deadline handed out at admission instead,
	// and it is the SAME number so an operator has one knob to reason about.
	//
	// It applies only when the submission declared no budget of its own. Zero or
	// negative disables the injection — and an unobservable nodeOnly submission
	// with no explicit budget is then REFUSED rather than admitted unbounded.
	NodeOnlyMaxLifetime time.Duration

	// PlacementTokens signs the opaque launch credential a placement preview
	// hands out and authenticates the one a launch presents back. The key behind
	// it is operator-supplied and STABLE — not process-local — because a preview
	// answered by one replica is launched against another, and a restart must not
	// invalidate the previews customers are holding.
	//
	// nil disables preview binding, and does so fail-closed rather than as a
	// fallback: the preview still answers the decision but hands out no
	// credential, and a launch that presents one is REFUSED instead of honored
	// unverified. A launch that presents none is untouched — that is the
	// cluster-token and existing-CLI path, which never previewed and takes its
	// own fresh decision exactly as it always has.
	PlacementTokens *state.PlacementTokenSigner

	// admission serializes per-tenant admission so concurrent Creates can't all
	// pass the spend/concurrency check before any of them persists a burst. Lazily
	// built (struct-literal construction leaves it nil); see adm().
	admissionOnce    sync.Once
	admissionGateVal *admissionGate
}

// Journal is the slice of the state store the workload path uses for writes
// that must be durable before the decision they record is acted on.
type Journal interface {
	// SubmitWorkload records a workload and the decision that admitted it as
	// one durable write, publishing neither unless both land.
	SubmitWorkload(w *state.Workload, ev *state.AuditEvent) error
	// AppendAudit writes one journal row on its own — a refusal, or an
	// observation of something that already happened.
	AppendAudit(ev *state.AuditEvent) error
	// PutBurst books the provisioned node. It is on this seam for the same
	// reason the two above are: it is a post-Plan durable write whose failure
	// has to reap a node that is already running and answer 503, and that path
	// is otherwise only ever taken in production. A failure must be compensated
	// from the exact Burst handed in here; it is not safe to assume a database-
	// elected claim can see a write that just failed.
	PutBurst(b *state.Burst) error
}

func (h *Workloads) journal() Journal {
	if h.Journal != nil {
		return h.Journal
	}
	return h.Store
}

// adm returns the handler's admission gate, building it on first use so
// struct-literal construction (tests, main) doesn't have to.
func (h *Workloads) adm() *admissionGate {
	h.admissionOnce.Do(func() { h.admissionGateVal = newAdmissionGate(h.Store) })
	return h.admissionGateVal
}

// Reaper tears down the backend + tailnet resources behind a burst
// when its workload finishes. Implemented by the decider (it owns the
// backend clients and Tailscale credentials). The k8s Node object is
// removed separately, by the agent.
type Reaper interface {
	Teardown(ctx context.Context, b *state.Burst) error
}

// ProviderDeleteReaper splits the provider absence proof from mesh cleanup for
// the authoritative lifecycle worker. The legacy Reaper remains intact for
// inline and broker teardown, while lifecycle can terminalize strictly after
// DeleteProviderNode and repair CleanupMesh durably afterwards.
type ProviderDeleteReaper interface {
	DeleteProviderNode(ctx context.Context, b *state.Burst) error
	CleanupMesh(ctx context.Context, b *state.Burst) error
}

// Decider is the central scheduling brain. Implementations pick a
// backend, mint a Tailscale auth key for the burst, and return the
// metadata the API layer turns into agent commands.
//
// Defined here (not in pkg/cloud/decider) so handlers don't have a
// circular dep on the decider package.
type Decider interface {
	// Plan provisions a burst node for the given workload and returns
	// the metadata needed to wire it up: backend id, the rendered Job
	// spec the agent should apply, any storage bindings the agent
	// will serve to the burst at bootstrap.
	Plan(ctx context.Context, wl *workload.Workload, opts PlanOptions) (*Plan, error)
}

// QuotedDecider separates trusted, side-effect-free provider pricing from the
// paid create. PlanQuoted must provision the exact shape named by quote.
type QuotedDecider interface {
	Decider
	Quote(context.Context, *workload.Workload, PlanOptions) (*BurstQuote, error)
	PlanQuoted(context.Context, *workload.Workload, PlanOptions, *BurstQuote) (*Plan, error)
	CreateOutcomeAmbiguous(error) bool
}

type BurstQuote struct {
	Backend        string
	CloudAccountID string
	ShapeHash      string
	Region         string
	SKU            string
	HourlyUSD      float64
	// Placement is the normalized decision this quote priced: the same typed
	// receipt a preview shows and a launch binds itself to. nil from a decider
	// that produces none, which keeps every existing quoting implementation
	// valid and simply means no digest can be bound.
	Placement *state.PlacementReceipt
}

// PlanOptions carries everything Plan needs from the calling agent /
// customer beyond the workload itself. Kept here so the decider
// package doesn't import pkg/cloud/state.
type PlanOptions struct {
	CustomerID string
	ClusterID  string // used to compute the burst's BOOTSTRAP_ENDPOINT via MagicDNS
	// ClusterPlacement is the cluster routing decision routeToCluster already
	// made for this submission. It is PASSED rather than re-derived so the
	// placement receipt records the one decision that actually admitted the
	// workload — a decider that re-asked the store could answer differently
	// from the router that chose the connector.
	ClusterPlacement ClusterPlacementDecision
	// WorkloadID is the durable identity this submission was elected under —
	// minted WITH the Idempotency-Key claim, so it is stable across every retry
	// of one submission and unique across two. It is what authoritative
	// admission keys durable create intent on; a decider running with
	// authoritative admission enabled refuses a submission that carries none,
	// because a provider create that cannot be replay-protected must not happen.
	WorkloadID string
}

// ClusterPlacementDecision is the bounded cluster half of a placement: what the
// submitter asked for, what central granted, and the policy that said so.
type ClusterPlacementDecision struct {
	RequestedClusterID string
	GrantedClusterID   string
	Mode               string
	Rule               string
	RuleVersion        string
}

// Plan is the decider's output for one workload submission.
type Plan struct {
	BurstID         string          // server-assigned, used in BurstAnnounce + state.Burst
	Backend         string          // "flyio" | "linode" | "aws" | ...
	BackendID       string          // backend-assigned VM/pod id
	Region          string          // provider routing region used for create/delete
	CloudAccountID  string          // tenant BYOC account used; empty for platform-funded
	NodeName        string          // k8s Node name the burst kubelet registers as
	TSHostname      string          // burst's Tailscale device hostname (for log correlation)
	MeshProvider    string          // "tailscale" | "box"; coordination server used for this burst
	MeshLoginServer string          // coordination server base URL; "" for shared Tailscale SaaS
	PodCIDR         string          // burst's Pod CIDR /24 (CNI uses this)
	JobSpec         json.RawMessage // pre-rendered batchv1.Job for CreateJob
	EstimatedUSD    float64         // for surfacing in the response (rate x deadline)
	HourlyUSD       float64         // upstream per-hour rate; recorded on the burst for cost accrual
	SKU             string          // backend SKU (machine class / instance type / plan)
	Tier            string          // networking tier; always "full" (lite has been removed). Forwarded in BurstAnnounce.
	Deadline        time.Duration   // spec.budget.deadline; 0 = unset. Recorded on the burst for the reaper watchdog.
	MaxUSD          float64         // spec.budget.maxUSD; 0 = unset. Recorded on the burst for the reaper watchdog.

	// StorageBindings is what the agent serves at the burst's
	// bootstrap call; BurstAnnounce.Storage is filled from this.
	StorageBindings []protocol.StorageBinding

	// Placement is the decision this plan launched, carried out of the decider
	// so the submission path persists the same receipt the launch was bound to
	// rather than assembling a second one. nil from a decider that produces none.
	Placement *state.PlacementReceipt
}

// CreateWorkloadRequest is the request body. The body is YAML to match
// the CLI's wire format (yaml.v3 is used to parse). Accept both YAML
// and JSON; YAML is the default since users hand us yaml.
type CreateWorkloadResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	// Code is a stable machine-readable refusal code, set only on the refusals
	// that have one. Absent on success and on refusals whose message is the
	// whole contract.
	Code         string             `json:"code,omitempty"`
	Backend      string             `json:"backend,omitempty"`
	BackendID    string             `json:"backend_id,omitempty"`
	BurstID      string             `json:"burst_id,omitempty"`
	EstimatedUSD float64            `json:"estimated_usd,omitempty"`
	Message      string             `json:"message,omitempty"`
	Placement    *PlacementResponse `json:"placement,omitempty"`
}

type PlacementResponse struct {
	RequestedClusterID string `json:"requested_cluster_id,omitempty"`
	GrantedClusterID   string `json:"granted_cluster_id,omitempty"`
	Mode               string `json:"mode,omitempty"`
	// GPUProduct is the model guaranteed by the selected provider SKU. It is
	// derived at render time so adding it does not alter the sealed receipt or
	// invalidate older stored decisions.
	GPUProduct string `json:"gpu_product,omitempty"`
	// Receipt is the provider placement decision, when one was recorded, on its
	// tenant-facing projection. The three fields above stay exactly as they were
	// so every existing reader of this shape keeps working unchanged.
	Receipt *state.PublicPlacementReceipt `json:"receipt,omitempty"`
}

// fabricProvisioningError is implemented only by the decider's retryable
// onboarding sentinel. Keeping this neutral avoids a handlers→decider import
// cycle while still preserving errors.Is(err, decider.ErrFabricProvisioning).
type fabricProvisioningError interface {
	FabricProvisioning()
}

func isFabricProvisioning(err error) bool {
	var provisioning fabricProvisioningError
	return errors.As(err, &provisioning)
}

// placementRefusal is implemented only by the decider's placement refusal. Like
// fabricProvisioningError it is declared structurally, so the API layer can read
// the stable customer code and the bounded explanation off a decision it
// refused without importing the decider (which would cycle).
type placementRefusal interface {
	PlacementRejectionReason() string
	PlacementRejectionReceipt() *state.PlacementReceipt
}

// placementRefusalOf returns the stable rejection code and bounded receipt a
// decider attached to a refusal, or ("", nil) for any other error. The
// PROVIDER's own message is never part of what it returns.
func placementRefusalOf(err error) (string, *state.PlacementReceipt) {
	var refusal placementRefusal
	if !errors.As(err, &refusal) {
		return "", nil
	}
	return refusal.PlacementRejectionReason(), state.SafePlacementReceipt(refusal.PlacementRejectionReceipt())
}

// routeToCluster picks the connector a submission is dispatched to, returning
// (agent, 0, "") on success and (nil, status, message) on a refusal the caller
// answers verbatim.
//
// A cluster token authenticates a TENANT, so on a tenant running two connectors
// the request has to say which cluster it means. clusterID is the caller's
// answer, from X-Cluster-ID:
//
//   - Named and this tenant's: routed exactly there, and never anywhere else.
//     A named cluster that is not connected is 409, because the fix is the
//     connector coming back and not a different cluster running the job.
//   - Named and another tenant's: 403. Ownership is the same check the
//     ts-auth-key mint makes, and for the same reason — the header is
//     caller-supplied, and a tenant must not be able to aim work at a cluster
//     that is not theirs.
//   - Not named: the single-connector fallback, which every deployed tenant
//     uses and which stays exactly as it was — 503 with no connector at all.
//     What it will not do is choose between two clusters: that is 409 asking
//     for the header, because the alternative is a job landing on whichever
//     cluster sorted first and a bill on the wrong one.
type clusterPlacement struct {
	requested string
	granted   string
	mode      string
	policy    bool
}

const (
	clusterPlacementRule        = "tenant.cluster_policy"
	clusterPlacementRuleVersion = "v1"
)

func placementResponse(p clusterPlacement, receipt *state.PlacementReceipt) *PlacementResponse {
	if p.granted == "" {
		return nil
	}
	public := state.PublicPlacementReceiptOf(receipt)
	return &PlacementResponse{
		RequestedClusterID: p.requested,
		GrantedClusterID:   p.granted,
		Mode:               p.mode,
		GPUProduct:         placementGPUProduct(public),
		Receipt:            public,
	}
}

// decision is the cluster placement in the shape the decider records it in,
// so the receipt and the stored WorkloadPlacement name one decision.
func (p clusterPlacement) decision() ClusterPlacementDecision {
	out := ClusterPlacementDecision{
		RequestedClusterID: p.requested,
		GrantedClusterID:   p.granted,
		Mode:               p.mode,
	}
	if p.policy {
		out.Rule = clusterPlacementRule
		out.RuleVersion = clusterPlacementRuleVersion
	}
	return out
}

func workloadPlacement(p clusterPlacement, decidedAt time.Time, receipt *state.PlacementReceipt) *state.WorkloadPlacement {
	if p.granted == "" {
		return nil
	}
	decision := p.decision()
	return &state.WorkloadPlacement{
		RequestedClusterID: decision.RequestedClusterID,
		GrantedClusterID:   decision.GrantedClusterID,
		Mode:               decision.Mode,
		Rule:               decision.Rule,
		RuleVersion:        decision.RuleVersion,
		DecidedAt:          decidedAt,
		Receipt:            state.SafePlacementReceipt(receipt),
	}
}

func storedPlacementResponse(p *state.WorkloadPlacement) *PlacementResponse {
	if p == nil {
		return nil
	}
	public := state.PublicPlacementReceiptOf(p.Receipt)
	return &PlacementResponse{
		RequestedClusterID: p.RequestedClusterID,
		GrantedClusterID:   p.GrantedClusterID,
		Mode:               p.Mode,
		GPUProduct:         placementGPUProduct(public),
		Receipt:            public,
	}
}

func placementGPUProduct(receipt *state.PublicPlacementReceipt) string {
	if receipt == nil {
		return ""
	}
	return backends.ResolvedGPUProduct(receipt.Selected.Provider, receipt.Selected.SKU)
}

// OutcomeResponse renders a stored run receipt on both workload read surfaces.
// The two results stay separate here for the reason they are separate in
// storage: "the job ran and your outputs never landed" is the answer a console
// is asked for, and one merged verdict cannot give it.
type OutcomeResponse struct {
	Compute   ComputeOutcomeResponse   `json:"compute"`
	Artifacts *ArtifactOutcomeResponse `json:"artifacts,omitempty"`
}

type ComputeOutcomeResponse struct {
	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`
}

// ArtifactOutcomeResponse carries what the export moved and nothing about where
// it moved it — the stored receipt holds no bucket, prefix or endpoint to
// render, by construction.
type ArtifactOutcomeResponse struct {
	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`
	// Counts stay pointers on the wire: null is "not counted", 0 is "counted
	// nothing", and a reader shown 0 for both would be handed a measurement
	// nobody took.
	ObjectsUploaded *int64 `json:"objects_uploaded"`
	BytesUploaded   *int64 `json:"bytes_uploaded"`
}

func storedOutcomeResponse(o *state.WorkloadOutcome) *OutcomeResponse {
	if o == nil {
		return nil
	}
	out := &OutcomeResponse{
		Compute: ComputeOutcomeResponse{Result: o.Compute.Result, Reason: o.Compute.Reason},
	}
	if o.Artifacts != nil {
		out.Artifacts = &ArtifactOutcomeResponse{
			Result:          o.Artifacts.Result,
			Reason:          o.Artifacts.Reason,
			ObjectsUploaded: o.Artifacts.ObjectsUploaded,
			BytesUploaded:   o.Artifacts.BytesUploaded,
		}
	}
	return out
}

// ProviderDeleteGetter is the optional read interface a lifecycle-mode Deletes
// may satisfy. Existing ProviderDeletes fakes and OSS seams that do not carry
// this method keep working unchanged — Get() type-asserts and silently omits
// the cleanup view when the implementation does not satisfy it.
type ProviderDeleteGetter interface {
	GetProviderDelete(ctx context.Context, customerID, clusterID, burstID string) (lifecycle.ProviderDeleteRecord, error)
}

// BurstEvidenceReader is the optional interface that supplies the durable
// evidence fields added by issue #93: the truthful provider-creation
// timestamp and the durable reap receipt. Both are additive and absent on
// implementations that predate the seam.
type BurstEvidenceReader interface {
	GetBurstProviderCreatedAt(ctx context.Context, customerID, clusterID, burstID string) (*time.Time, error)
	BurstReapRecorded(ctx context.Context, burstID, customerID string) (bool, error)
}

// CleanupResponse is the tenant-safe projection of a provider-delete record.
// Cloud account IDs, provider resource IDs, payload, and lease tokens are
// absent by construction — they are operator/provider internals, and a console
// or CLI needs only the state, the timestamps, and the safe routing fields the
// tenant already sees on the placement receipt.
type CleanupResponse struct {
	State              string     `json:"state"`
	Provider           string     `json:"provider,omitempty"`
	Region             string     `json:"region,omitempty"`
	SKU                string     `json:"sku,omitempty"`
	Reason             string     `json:"reason,omitempty"`
	RequestedAt        time.Time  `json:"requested_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	DeletedAt          *time.Time `json:"deleted_at,omitempty"`
	ProviderCreatedAt  *time.Time `json:"provider_created_at,omitempty"`
	DurableReapReceipt bool       `json:"durable_reap_receipt"`
}

func cleanupResponse(r lifecycle.ProviderDeleteRecord) *CleanupResponse {
	return &CleanupResponse{
		State:       r.State,
		Provider:    r.Provider,
		Region:      r.Region,
		SKU:         r.SKU,
		Reason:      r.Reason,
		RequestedAt: r.RequestedAt,
		UpdatedAt:   r.UpdatedAt,
		DeletedAt:   r.DeletedAt,
	}
}

// CleanupSummary is the tenant-safe list-level projection of a provider-delete
// record. It exposes only the authoritative state and the optional deleted_at
// timestamp — enough for a console to distinguish manual_attention, terminated,
// and proven deletion without inferring from workload phase.
type CleanupSummary struct {
	State     string     `json:"state"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

func providerAbsenceConfirmed(cleanupState string, deletedAt *time.Time) bool {
	return cleanupState == lifecycle.ProviderDeleteTerminated && deletedAt != nil && !deletedAt.IsZero()
}

// CleanupSummaryGetter is the optional batch-read interface for list-level
// cleanup summaries. Implementations return one summary per matching burst ID
// in a single database call, scoped by customer_id and the exact non-empty
// burst IDs in the current list. Existing ProviderDeletes fakes and OSS seams
// that do not carry this method keep working unchanged — HandleListWorkloads
// type-asserts and silently omits cleanup when the implementation does not
// satisfy it.
type CleanupSummaryGetter interface {
	GetProviderDeleteSummaries(ctx context.Context, customerID string, refs []lifecycle.ProviderDeleteSummaryRef) (map[lifecycle.ProviderDeleteSummaryRef]lifecycle.ProviderDeleteSummary, error)
}

// TenantWorkloadCost is the frozen tenant-safe cost observation shared by the
// workload detail and history surfaces. It lives in the public-core handler
// file so the OSS export retains the renderer used by GET /v1/workloads/{id}.
type TenantWorkloadCost struct {
	USD            float64   `json:"usd"`
	HourlyUSD      float64   `json:"hourly_usd"`
	RuntimeSeconds float64   `json:"runtime_seconds"`
	FrozenAt       time.Time `json:"frozen_at"`
	Backend        string    `json:"backend"`
	BurstID        string    `json:"burst_id"`
	Basis          string    `json:"basis"`
}

func tenantWorkloadCost(c *state.WorkloadCost) *TenantWorkloadCost {
	if c == nil {
		return nil
	}
	return &TenantWorkloadCost{
		USD:            c.EstimatedUSD,
		HourlyUSD:      c.HourlyUSD,
		RuntimeSeconds: c.Runtime.Seconds(),
		FrozenAt:       c.FrozenAt,
		Backend:        c.Backend,
		BurstID:        c.BurstID,
		Basis:          c.Basis,
	}
}

// NodeObservationResponse is the API projection of a workload's last observed
// Kubernetes Node snapshot. Omitted entirely when no connector has reported a
// persistable phase, so a workload whose burst has not registered says nothing.
type NodeObservationResponse struct {
	NodeName   string    `json:"node_name,omitempty"`
	Phase      string    `json:"phase,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	ObservedAt time.Time `json:"observed_at"`
}

func nodeObservationResponse(obs *state.NodeObservation) *NodeObservationResponse {
	if obs == nil {
		return nil
	}
	return &NodeObservationResponse{
		NodeName:   obs.NodeName,
		Phase:      obs.Phase,
		Reason:     obs.Reason,
		ObservedAt: obs.ObservedAt,
	}
}

// PodObservationResponse is the API projection of a workload's pod scheduling
// observation. Omitted entirely when no connector has reported scheduling.
type PodObservationResponse struct {
	PodName     string    `json:"pod_name,omitempty"`
	NodeName    string    `json:"node_name,omitempty"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

func podObservationResponse(obs *state.PodObservation) *PodObservationResponse {
	if obs == nil {
		return nil
	}
	return &PodObservationResponse{
		PodName:     obs.PodName,
		NodeName:    obs.NodeName,
		ScheduledAt: obs.ScheduledAt,
	}
}

// GPUObservationResponse is the API projection of a workload's GPU readiness
// observation. Omitted entirely for CPU-only workloads or when no connector
// has reported GPU allocatable.
type GPUObservationResponse struct {
	AllocatableAt time.Time `json:"allocatable_at"`
}

// SchedulingObservationResponse is the API projection of a workload's
// scheduling state observation. Omitted entirely when no connector has
// reported a scheduling observation.
type SchedulingObservationResponse struct {
	State      string    `json:"state"`
	Reason     string    `json:"reason,omitempty"`
	Message    string    `json:"message,omitempty"`
	ObservedAt time.Time `json:"observed_at"`
}

func schedulingObservationResponse(obs *state.SchedulingObservation) *SchedulingObservationResponse {
	if obs == nil {
		return nil
	}
	return &SchedulingObservationResponse{
		State:      obs.State,
		Reason:     obs.Reason,
		Message:    obs.Message,
		ObservedAt: obs.ObservedAt,
	}
}

func gpuObservationResponse(obs *state.GPUObservation) *GPUObservationResponse {
	if obs == nil {
		return nil
	}
	return &GPUObservationResponse{
		AllocatableAt: obs.AllocatableAt,
	}
}

func clusterPolicyEligible(policy state.ClusterPolicy, clusterID string) (bool, string) {
	for _, denied := range policy.Deny {
		if denied == clusterID {
			return false, "denied"
		}
	}
	if len(policy.Allow) == 0 {
		return true, "eligible"
	}
	for _, allowed := range policy.Allow {
		if allowed == clusterID {
			return true, "eligible"
		}
	}
	return false, "not_allowed"
}

func uniqueClusterAgents(agents []*state.Agent) []*state.Agent {
	out := make([]*state.Agent, 0, len(agents))
	for _, a := range agents {
		if n := len(out); n > 0 && out[n-1].ClusterID == a.ClusterID {
			continue
		}
		out = append(out, a)
	}
	return out
}

func pickOrderedCluster(policy state.ClusterPolicy, agents []*state.Agent) *state.Agent {
	byID := make(map[string]*state.Agent, len(agents))
	for _, a := range agents {
		if ok, _ := clusterPolicyEligible(policy, a.ClusterID); ok {
			byID[a.ClusterID] = a
		}
	}
	for _, id := range policy.Allow {
		if a := byID[id]; a != nil {
			return a
		}
	}
	for _, a := range agents {
		if byID[a.ClusterID] != nil {
			return a
		}
	}
	return nil
}

func (h *Workloads) routeToCluster(cust *state.Customer, clusterID string) (*state.Agent, int, string, clusterPlacement) {
	customerID := cust.ID
	if clusterID != "" {
		if !state.ValidClusterID(clusterID) {
			return nil, http.StatusBadRequest, "X-Cluster-ID must be a valid cluster id", clusterPlacement{}
		}
		if owner, known := h.Store.CustomerForClusterID(clusterID); known && owner != customerID {
			h.Log.Warn("rejected cross-tenant cluster id on workload submit",
				"customer", customerID, "cluster", clusterID, "owner", owner)
			return nil, http.StatusForbidden, "cluster id is registered to another tenant", clusterPlacement{}
		}
		if cust.ClusterPolicy != nil {
			policy := cust.ClusterPolicy.Effective()
			if ok, reason := clusterPolicyEligible(policy, clusterID); !ok {
				h.Log.Warn("rejected cluster id by tenant cluster policy",
					"customer", customerID, "cluster", clusterID, "reason", reason)
				return nil, http.StatusForbidden, "cluster id is forbidden by tenant cluster policy", clusterPlacement{}
			}
		}
		agent, err := h.Store.AgentForCluster(customerID, clusterID)
		if err != nil {
			// A cluster the tenant REGISTERED is a known member of their fleet,
			// so name the real problem — the connector is offline — rather than
			// answering as if the id were unknown. The cross-tenant check above
			// already refused anyone else's id, so an owner here is this tenant.
			if owner, known := h.Store.CustomerForClusterID(clusterID); known && owner == customerID {
				return nil, http.StatusConflict,
					"the requested cluster is registered but its Cluster Connector is not connected; bring the connector back online or choose another cluster", clusterPlacement{}
			}
			return nil, http.StatusConflict,
				"no Cluster Connector connected for the requested cluster; confirm the Yscale Cluster Connector in that cluster is connected", clusterPlacement{}
		}
		return agent, 0, "", clusterPlacement{requested: clusterID, granted: agent.ClusterID, mode: state.ClusterPlacementModePinned, policy: cust.ClusterPolicy != nil}
	}
	if cust.ClusterPolicy != nil {
		policy := cust.ClusterPolicy.Effective()
		agents := uniqueClusterAgents(h.Store.AgentsForCustomer(customerID))
		if len(agents) == 0 {
			return nil, http.StatusServiceUnavailable,
				"no Cluster Connector connected for this account; install the Yscale Cluster Connector and confirm it is connected", clusterPlacement{}
		}
		eligible := make([]*state.Agent, 0, len(agents))
		hostedEligible := false
		for _, a := range agents {
			if ok, _ := clusterPolicyEligible(policy, a.ClusterID); ok {
				if _, hosted := h.Store.HostedNamespaceForCluster(customerID, a.ClusterID); hosted {
					hostedEligible = true
					continue
				}
				eligible = append(eligible, a)
			}
		}
		if len(eligible) == 0 {
			if hostedEligible {
				return nil, http.StatusConflict,
					"hosted clusters require X-Cluster-ID so central can enforce their reserved namespace", clusterPlacement{}
			}
			return nil, http.StatusConflict,
				"tenant cluster policy has no connected eligible cluster; connect an allowed cluster or change the policy", clusterPlacement{}
		}
		if policy.Auto != state.ClusterPolicyAutoOrdered {
			if len(eligible) > 1 {
				return nil, http.StatusConflict,
					"this tenant's cluster policy requires X-Cluster-ID when more than one eligible cluster is connected", clusterPlacement{}
			}
			agent := eligible[0]
			return agent, 0, "", clusterPlacement{granted: agent.ClusterID, mode: state.ClusterPlacementModeAuto, policy: true}
		}
		agent := pickOrderedCluster(policy, eligible)
		if agent == nil {
			return nil, http.StatusConflict,
				"tenant cluster policy has no connected eligible cluster; connect an allowed cluster or change the policy", clusterPlacement{}
		}
		return agent, 0, "", clusterPlacement{granted: agent.ClusterID, mode: state.ClusterPlacementModeAuto, policy: true}
	}
	agent, err := h.Store.AgentForCustomer(customerID)
	switch {
	case errors.Is(err, state.ErrAmbiguousCluster):
		return nil, http.StatusConflict,
			"this account has Cluster Connectors for more than one cluster; set the X-Cluster-ID header to choose one", clusterPlacement{}
	case err != nil:
		return nil, http.StatusServiceUnavailable,
			"no Cluster Connector connected for this account; install the Yscale Cluster Connector and confirm it is connected", clusterPlacement{}
	}
	if _, hosted := h.Store.HostedNamespaceForCluster(customerID, agent.ClusterID); hosted {
		return nil, http.StatusConflict,
			"hosted clusters require X-Cluster-ID so central can enforce their reserved namespace", clusterPlacement{}
	}
	return agent, 0, "", clusterPlacement{granted: agent.ClusterID, mode: state.ClusterPlacementModeAuto}
}

// unboundableNodeOnlyMessage answers the one submission central can neither bound
// nor let run: nodeOnly capacity on a connector that cannot observe occupancy,
// with no declared budget, on a deployment whose operator switched the fallback
// deadline off. It names the fix the submitter can actually apply.
const unboundableNodeOnlyMessage = "nodeOnly workloads on this cluster must declare spec.budget.deadline or spec.budget.maxUSD: its connector cannot observe cluster-wide pod occupancy, so nothing can tell yscale when this capacity is spare"

// Create handles POST /v1/workloads.
func (h *Workloads) Create(w http.ResponseWriter, r *http.Request) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var wl workload.Workload
	if err := yaml.Unmarshal(body, &wl); err != nil {
		http.Error(w, "parsing workload yaml: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := workload.Validate(&wl); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Who is asking. The human surface injects the signed-in account; every
	// other route reaching here presented a cluster token, which authenticates a
	// cluster and never a person.
	by := submitterOrCluster(r.Context(), cust.ID)
	requestedNS := wl.Metadata.Namespace

	// Which submission path says it produced this, resolved here because here is
	// still free: nothing has been admitted, no capacity reserved, no provider
	// called and nothing written. A malformed header is a client bug in the same
	// class as a malformed spec — operational, not an authorization decision —
	// so it is answered 400 and, like the refusals around it, not journaled.
	origin, err := submissionOrigin(r, by)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, CreateWorkloadResponse{Status: "rejected", Message: err.Error()})
		return
	}

	// Pin the namespace to one this tenant is authorized for, BEFORE
	// Plan renders a Job into it and before the announce tells the
	// connector which namespace it may read bucket credentials from.
	// Submitted unvalidated, metadata.namespace is a request for another
	// team's Secrets; pinned here it is central's own answer, and every
	// downstream derivation reads it back off the spec.
	ns, err := authorizeWorkloadNamespace(cust, requestedNS)
	if err != nil {
		// The refusal is journaled before it is answered, and a journal that
		// cannot take the row refuses the request instead: an authorization
		// decision central made and did not record is the gap this slice closes,
		// and a denial is as much a decision as an approval. Nothing has been
		// provisioned yet, so there is nothing to reap on that path.
		//
		// This is the only submit refusal that is journaled, and deliberately:
		// the ones above and below it — a malformed spec, no connected
		// connector, a tenant at its spend ceiling, a scheduling failure — are
		// operational, not authorization. Journaling them would put an
		// unauthenticated-shaped rate limiter on the evidence (one durable write
		// per rejected request) and bury the decisions a reader is looking for.
		if auditErr := h.journal().AppendAudit(state.NewAuditEvent(state.AuditEvent{
			CustomerID: cust.ID,
			Actor:      by.Actor,
			Action:     state.ActionWorkloadSubmit,
			Outcome:    state.OutcomeDenied,
			TargetKind: state.TargetTenant,
			TargetID:   cust.ID,
			Detail: state.AuditDetail{
				Reason:             state.ReasonNamespaceNotAuthorized,
				Role:               by.Role,
				Rule:               workloadNamespaceRule,
				RuleVersion:        workloadNamespaceRuleVersion,
				RequestedNamespace: state.SafeNamespace(requestedNS),
			},
		})); auditErr != nil {
			h.Log.Error("audit submit denial", "customer", cust.ID, "error", auditErr)
			writeJSON(w, http.StatusServiceUnavailable, CreateWorkloadResponse{
				Status:  "rejected",
				Message: "could not record the authorization decision; please retry",
			})
			return
		}
		writeJSON(w, http.StatusForbidden, CreateWorkloadResponse{Status: "rejected", Message: err.Error()})
		return
	}
	wl.Metadata.Namespace = ns

	// The launch template this submission claims to come from, when it names
	// one. Resolved BEFORE the claim and before anything is provisioned,
	// because the reference decides what the tenant published and a workload
	// running under a template it does not match is provenance nobody can read
	// back.
	//
	// A retry carries its source's stamp in the request context instead, and it
	// is PRESERVED rather than re-decided: the source run really was launched
	// from that entry, and a catalog the tenant edited afterwards does not
	// change what already happened. The retry route refuses a caller-supplied
	// pair, so nothing reaching here can forge one.
	//
	// Every refusal below is journaled on the namespace refusal's terms —
	// written before it is answered, and answered as retryable when it cannot be
	// written. A named template is an authorization question the tenant's own
	// catalog answers, so a half-sent pair is as much a decision central made as
	// a stale version is: both are a submission refused on the reference, and an
	// operator reading the journal should find the same evidence for either.
	templateRef := storedTemplateRef(r.Context())
	templateID, templateVersion, named, err := templateReference(r)
	if templateRef == nil && err != nil {
		if auditErr := h.journal().AppendAudit(state.NewAuditEvent(state.AuditEvent{
			CustomerID: cust.ID,
			Actor:      by.Actor,
			Action:     state.ActionWorkloadSubmit,
			Outcome:    state.OutcomeDenied,
			TargetKind: state.TargetTenant,
			TargetID:   cust.ID,
			Detail: state.AuditDetail{
				Reason:          state.ReasonTemplateReferenceInvalid,
				Role:            by.Role,
				Rule:            workloadTemplateRule,
				RuleVersion:     workloadTemplateRuleVersion,
				TemplateID:      state.SafeTemplateID(templateID),
				TemplateVersion: templateVersion,
			},
		})); auditErr != nil {
			h.Log.Error("audit invalid template reference", "customer", cust.ID, "error", auditErr)
			writeJSON(w, http.StatusServiceUnavailable, CreateWorkloadResponse{
				Status:  "rejected",
				Message: "could not record the authorization decision; please retry",
			})
			return
		}
		writeJSON(w, http.StatusBadRequest, CreateWorkloadResponse{Status: "rejected", Message: err.Error()})
		return
	}
	if templateRef == nil && named {
		ref, refusal := verifyWorkloadTemplate(cust, templateID, templateVersion, &wl)
		if refusal != nil {
			if auditErr := h.journal().AppendAudit(state.NewAuditEvent(state.AuditEvent{
				CustomerID: cust.ID,
				Actor:      by.Actor,
				Action:     state.ActionWorkloadSubmit,
				Outcome:    state.OutcomeDenied,
				TargetKind: state.TargetTenant,
				TargetID:   cust.ID,
				Detail: state.AuditDetail{
					Reason:                  refusal.reason,
					Role:                    by.Role,
					Rule:                    workloadTemplateRule,
					RuleVersion:             workloadTemplateRuleVersion,
					TemplateID:              state.SafeTemplateID(templateID),
					TemplateVersion:         templateVersion,
					TemplateCatalogRevision: cust.TemplateCatalog.Revision(),
				},
			})); auditErr != nil {
				h.Log.Error("audit template submit denial", "customer", cust.ID, "error", auditErr)
				writeJSON(w, http.StatusServiceUnavailable, CreateWorkloadResponse{
					Status:  "rejected",
					Message: "could not record the authorization decision; please retry",
				})
				return
			}
			writeJSON(w, refusal.status, CreateWorkloadResponse{Status: "rejected", Message: refusal.message})
			return
		}
		templateRef = ref
	}

	// Canonicalise the spec NOW, while a failure still costs nothing: this
	// is what gets stored, and it is the document central actually acted on
	// rather than the bytes that arrived. Re-marshalled from the validated
	// struct, so it carries the pinned namespace — the one the Job lands in
	// and the one the connector reads credentials from — instead of the
	// submitter's request for a different one. Storing the raw body made the
	// console's workload list disagree with reality for every unqualified or
	// re-pinned submission.
	specYAML, err := yaml.Marshal(&wl)
	if err != nil {
		h.Log.Error("canonicalise workload spec", "customer", cust.ID, "error", err)
		writeJSON(w, http.StatusInternalServerError, CreateWorkloadResponse{
			Status:  "rejected",
			Message: "could not record the workload spec",
		})
		return
	}

	// Bind the submission to its Idempotency-Key, durably, while nothing has
	// been provisioned yet.
	//
	// Everything below this line — the connector lookup, the admission
	// reservation, Plan's call into a backend's CreateNode — used to run once
	// per REQUEST, and a request is the one thing central does not control the
	// number of. A browser that gave up at thirty seconds, a proxy that retried
	// a POST, a user who clicked twice: each was a second machine on the
	// tenant's bill, indistinguishable from a second submission. The claim moves
	// that decision to durable state and makes it once per key, so of N arrivals
	// exactly one owns the provision and the rest are told what happened to it.
	sub, done := h.beginIdempotent(w, r, cust, by, specYAML, templateRef)
	if done {
		return
	}
	// Minted with the claim above, so a retry arriving mid-provision — or after
	// this process dies — is handed the same id this attempt will publish.
	wlID := sub.workloadID

	// Resolve the connector this submission runs on: it carries the cluster's
	// identity (used by the decider for IP allocation) and it is what commands
	// are pushed to. X-Cluster-ID names it when the tenant has more than one.
	agent, status, msg, placement := h.routeToCluster(cust, r.Header.Get(clusterIDHeader))
	if status != 0 {
		// Nothing has been planned on any of these — a disconnected connector, a
		// cluster that is not this tenant's, an unanswerable choice between two
		// of them — so the key is unspent. Release it rather than wedging the
		// tenant out of the key their client will keep retrying with.
		sub.release(r.Context())
		if retryOfWorkload(r.Context()) != "" && strings.Contains(msg, "X-Cluster-ID") {
			msg = "this retry cannot be placed automatically under the current tenant policy; start a new workload and choose an eligible cluster"
		}
		writeJSON(w, status, CreateWorkloadResponse{Status: "rejected", Message: msg})
		return
	}
	if namespace, hosted := h.Store.HostedNamespaceForCluster(cust.ID, agent.ClusterID); hosted && wl.Metadata.Namespace != namespace {
		sub.release(r.Context())
		writeJSON(w, http.StatusConflict, CreateWorkloadResponse{
			Status:  "rejected",
			Message: fmt.Sprintf("hosted cluster %s accepts workloads only in its reserved namespace %s", agent.ClusterID, namespace),
		})
		return
	}

	// Bound the one submission shape nothing else can bound: nodeOnly capacity on
	// a connector that cannot observe occupancy.
	//
	// nodeOnly bursts are ended by a signal only the customer's cluster sends, and
	// a connector without cluster-wide pod visibility can never send it. The
	// watchdog's silence ceiling cannot cover them either — there is no
	// observation to go silent — so without this such a burst runs until someone
	// notices the bill. The bound is therefore decided HERE, where the connector
	// that will (or will not) do the observing is finally known, and it is a real
	// deadline written onto the spec rather than a policy applied invisibly later:
	// the stored workload and every API read show the limit the burst is actually
	// running under.
	//
	// A declared budget is left exactly as submitted. An explicit deadline or
	// maxUSD is the submitter's own bound and is not shortened, lengthened, or
	// second-guessed here.
	if needsNodeOnlyBound(&wl, agent) {
		if h.NodeOnlyMaxLifetime <= 0 {
			// The fallback is switched off and there is nothing left to bound this
			// burst with. Refusing is the honest answer: admitting it would book a
			// node with no deadline, no spend cap and no connector able to end it.
			sub.release(r.Context())
			writeJSON(w, http.StatusBadRequest, CreateWorkloadResponse{
				Status:  "rejected",
				Message: unboundableNodeOnlyMessage,
			})
			return
		}
		wl.Spec.Budget = &workload.Budget{Deadline: h.NodeOnlyMaxLifetime}
		// Re-canonicalise: the spec stored on the workload record — and served
		// back on every read of it — must be the document central acted on,
		// deadline included. The idempotency claim above deliberately keeps
		// hashing the SUBMITTED bytes, so a retry of the same request still
		// replays the same claim.
		specYAML, err = yaml.Marshal(&wl)
		if err != nil {
			h.Log.Error("canonicalise workload spec after budget injection", "customer", cust.ID, "error", err)
			sub.release(r.Context())
			writeJSON(w, http.StatusInternalServerError, CreateWorkloadResponse{
				Status:  "rejected",
				Message: "could not record the workload spec",
			})
			return
		}
		h.Log.Info("nodeOnly workload admitted with a default deadline; its connector cannot observe pod occupancy so no idle teardown is coming",
			"customer", cust.ID, "cluster", agent.ClusterID, "deadline", h.NodeOnlyMaxLifetime.String())
	}

	planOpts := PlanOptions{
		CustomerID: cust.ID,
		ClusterID:  agent.ClusterID,
		// The claim above already made this submission once-per-key; carrying
		// its id into the decider is what lets durable admission key the same
		// decision, so the two idempotency layers agree on what one submission
		// is instead of each inventing its own answer.
		WorkloadID: wlID,
		// The cluster decision routeToCluster just made, carried into the
		// placement receipt so the answer a customer reads names the cluster
		// this submission was actually routed to.
		ClusterPlacement: placement.decision(),
	}

	// A submission may present the placement it was previewed under. Verified
	// HERE — after the spec is final and the cluster is chosen, and before the
	// admission reservation, the mesh key, the /24 and the
	// provider call — because a credential that does not authenticate, and a
	// decision that moved, must both be answered while nothing has been spent.
	verified, refusalStatus, refusal, ok := h.verifyPlacementToken(r, &wl, planOpts)
	if !ok {
		sub.release(r.Context())
		writeJSON(w, refusalStatus, refusal)
		return
	}

	// Obtain a trusted quote BEFORE admission whenever a QuotedDecider is
	// available and the tenant has configured limits, so the candidate's hourly
	// rate is known at admission time rather than filled in after Plan returns.
	// This closes the window where a racing admission sees $0 for an in-flight
	// burst's cost.
	var quote *BurstQuote
	var quoted QuotedDecider
	tenantHasLimits := cust.MaxConcurrentBursts > 0 || cust.MaxHourlyUSD > 0

	if quoter, isQuoter := h.Decider.(QuotedDecider); isQuoter {
		quoted = quoter
		quote = verified
		if quote == nil && cust.MaxHourlyUSD > 0 {
			var quoteErr error
			quote, quoteErr = quoter.Quote(r.Context(), &wl, planOpts)
			if quoteErr != nil {
				sub.release(r.Context())
				writeJSON(w, http.StatusServiceUnavailable, CreateWorkloadResponse{Status: "rejected", Message: "provider quote unavailable for admission rate check"})
				return
			}
		}
	} else if tenantHasLimits && cust.MaxHourlyUSD > 0 {
		sub.release(r.Context())
		writeJSON(w, http.StatusServiceUnavailable, CreateWorkloadResponse{Status: "rejected", Message: "trusted provider quoting is unavailable for admission rate check"})
		return
	}
	if cust.MaxHourlyUSD > 0 && quote == nil {
		sub.release(r.Context())
		writeJSON(w, http.StatusServiceUnavailable, CreateWorkloadResponse{Status: "rejected", Message: "trusted provider quote unavailable for admission rate check"})
		return
	}
	candidateHourlyUSD := float64(0)
	if quote != nil {
		candidateHourlyUSD = quote.HourlyUSD
	}

	// Per-tenant admission control: refuse to provision when the tenant is at
	// its concurrent-burst or aggregate spend-rate ceiling, so a pilot can't run
	// up a surprise bill. 0 limits (existing customers) = unlimited. The check and
	// the reservation are atomic (the state store serializes the read→decide→reserve
	// window), so concurrent Creates for the same tenant can't all observe spend
	// below the cap before any burst persists and overshoot the ceiling — the
	// reservation is counted alongside live bursts until this burst persists or
	// its provision fails.
	adm := h.adm()
	rsvID, status, msg := adm.reserve(r.Context(), cust, wlID, candidateHourlyUSD)
	if status != 0 {
		// A ceiling refusal provisions nothing, and the tenant is expected to
		// resubmit once a burst finishes — under the same key, since their
		// client has no reason to pick a new one. Release it.
		sub.release(r.Context())
		writeJSON(w, status, CreateWorkloadResponse{Status: "rejected", Message: msg})
		return
	}

	var plan *Plan
	if quoted != nil && quote != nil {
		// A quote obtained for admission — pass it to PlanQuoted so Plan
		// does not quote again.
		plan, err = quoted.PlanQuoted(r.Context(), &wl, planOpts, quote)
	} else {
		plan, err = h.Decider.Plan(r.Context(), &wl, planOpts)
	}
	if err != nil {
		h.Log.Error("decider failed", "customer", cust.ID, "error", err)
		ambiguousCreate := quoted != nil && quoted.CreateOutcomeAmbiguous(err)
		if !ambiguousCreate {
			adm.release(r.Context(), rsvID)
		}
		if isFabricProvisioning(err) {
			// The onboarding sentinel is raised before the decider reaches a
			// backend at all, so nothing was created and the retry this answer
			// asks for must be allowed to happen — under the same key, because
			// this is one submission being told to wait, not a new one.
			sub.release(r.Context())
			w.Header().Set("Retry-After", "15")
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "provisioning",
				"reason": "fabric_provisioning",
			})
			return
		}
		// Every other Plan failure is AMBIGUOUS: the decider may have got as far
		// as a backend's CreateNode and failed after it returned a machine. So
		// the claim is not released — it is completed with this refusal, and the
		// retry replays it instead of buying a second node on the guess that the
		// first one never existed. A tenant who really does want to try again
		// sends a new key; a machine nobody knows about is not recoverable that
		// cheaply.
		sub.answer(r.Context(), w, http.StatusServiceUnavailable, CreateWorkloadResponse{
			Status:  "failed",
			Message: "scheduling failed: " + err.Error(),
		})
		return
	}
	// The workload record, built ONCE here and used by both outcomes below.
	//
	// It is the same struct either way: the same id, the same immutable submitter
	// provenance, the same authorization decision. What differs is only whether it
	// is published as a live submission or as a failed attempt, and building it in
	// one place is what keeps those two from drifting — a failure path that
	// assembled its own record is how a failed attempt ends up with no submitter
	// and no rule, which is the record an operator most needs to read.
	decidedAt := time.Now().UTC()
	rec := &state.Workload{
		ID:          wlID,
		CustomerID:  cust.ID,
		AgentID:     agent.ID,
		ClusterID:   agent.ClusterID,
		Status:      "provisioning",
		BurstID:     plan.BurstID,
		CreatedAt:   decidedAt,
		SpecYAML:    specYAML,
		SubmittedBy: &by.Actor,
		Authorization: &state.WorkloadAuthorization{
			RequestedNamespace: state.SafeNamespace(requestedNS),
			GrantedNamespace:   ns,
			Rule:               workloadNamespaceRule,
			RuleVersion:        workloadNamespaceRuleVersion,
			Role:               by.Role,
			DecidedAt:          decidedAt,
		},
		Placement:         workloadPlacement(placement, decidedAt, plan.Placement),
		RetryOfWorkloadID: retryOfWorkload(r.Context()),
		TemplateRef:       templateRef,
		SubmissionOrigin:  origin,
	}

	// failAttempt records the attempt and returns the retryable response. The
	// caller owns cleanup because a burst whose first durable write failed is not
	// claimable through the normal database-elected reap path.
	//
	// Answered through the claim, not written directly: a node was provisioned
	// on this key, so the refusal is this key's terminal outcome. A retry
	// replaying "failed" is the honest answer; a retry re-entering Plan would
	// provision again on top of whatever the compensation left behind.
	failAttempt := func(clientMsg string) {
		h.failWorkloadAttempt(rec)
		sub.answer(r.Context(), w, http.StatusServiceUnavailable, CreateWorkloadResponse{
			ID:      wlID,
			Status:  "failed",
			Message: clientMsg,
		})
	}

	// failSubmission reaps the just-provisioned burst and records the attempt as
	// failed before returning an error, so a post-provision failure can't leave an
	// orphaned backend node billing indefinitely (an unbudgeted workload has no
	// watchdog safety net). reapBurst is the shared teardown path — no second one.
	// Declared before the burst and workload persists below: a failed durable
	// write strands the same provisioned node a failed dispatch does, and takes
	// this same path.
	failSubmission := func(reason, clientMsg string) {
		// Accepted: in lifecycle mode the compensation this path owes is a
		// DURABLE delete obligation, not a completed teardown. The loud warning
		// below is for the case nothing at all took responsibility.
		if !h.reapBurst(r.Context(), plan.BurstID, reason).Accepted() {
			// Teardown failed and reapBurst re-queued the burst. For a burst with
			// no budget/deadline and the safety net disabled, nothing may retry
			// the teardown — surface it loudly rather than reporting a clean
			// "failed" while a backend node keeps billing.
			h.Log.Error("dispatch-failure reap did not tear down the burst; backend node may still be billing — needs watchdog/manual cleanup",
				"burst", plan.BurstID, "workload", wlID, "reason", reason)
		}
		failAttempt(clientMsg)
	}

	// Persist the burst booking. Torn down by Complete() when the
	// agent reports the workload's Job has finished.
	//
	burst := &state.Burst{
		ID:         plan.BurstID,
		CustomerID: cust.ID,
		AgentID:    agent.ID,
		// The cluster this burst was routed to, so its teardown drains the node
		// from the SAME cluster. AgentID cannot stand in: it names a socket that
		// is gone by the next reconnect, and the reap that matters most is the
		// one running after a restart.
		ClusterID:       agent.ClusterID,
		Backend:         plan.Backend,
		BackendID:       plan.BackendID,
		Region:          plan.Region,
		CloudAccountID:  plan.CloudAccountID,
		NodeName:        plan.NodeName,
		TSHostname:      plan.TSHostname,
		PodCIDR:         plan.PodCIDR,
		MeshProvider:    plan.MeshProvider,
		MeshLoginServer: plan.MeshLoginServer,
		CreatedAt:       time.Now().UTC(),
		Status:          "provisioning",
		SKU:             plan.SKU,
		HourlyUSD:       plan.HourlyUSD,
		Deadline:        plan.Deadline,
		MaxUSD:          plan.MaxUSD,
		NodeOnly:        wl.Spec.NodeOnly,
		// What the connector holding this burst promised at admission. Recorded
		// on the burst rather than looked up at sweep time because the socket
		// that made the promise is gone by then — a dead connector is exactly the
		// case the watchdog exists for, and it has no Agent left to ask. Only
		// nodeOnly bursts carry it: a managed Job burst has a completion path of
		// its own and never depends on occupancy at all.
		OccupancyObservationExpected: wl.Spec.NodeOnly && agent.AuthoritativeOccupancy,
	}
	if err := h.journal().PutBurst(burst); err != nil {
		// The node is provisioned but nothing durable records it: after a restart
		// — or from any other replica — no reaper would ever find it. Tear it down
		// now instead of billing for a burst only this process knows about.
		h.Log.Error("persist burst failed; reaping the provisioned node",
			"burst", plan.BurstID, "workload", wlID, "error", err)
		adm.release(r.Context(), rsvID) // being torn down; free the reserved slot
		if !h.compensateProvisionedBurst(r.Context(), burst, "burst persist failed") {
			h.Log.Error("provisioning compensation failed; backend node may still be billing",
				"burst", plan.BurstID, "workload", wlID)
		}
		failAttempt("could not record the burst durably; cleanup was scheduled — please retry")
		return
	}
	if plan.CloudAccountID != "" {
		if err := h.Store.ReleaseCloudAccountLease(r.Context(), plan.BurstID); err != nil {
			h.Log.Error("release cloud-account create lease", "burst", plan.BurstID, "error", err)
		}
	}
	// Only a durable booking enters the active/cost lifecycle. A failed first
	// write is provisioning compensation, not a customer-visible burst reap.
	h.Cost.BurstCreated(plan.Backend, plan.HourlyUSD)
	// The burst is persisted, so the store now counts it; delete the reservation
	// directly. A failed delete leaves a conservative double-count until TTL.
	if err := adm.release(r.Context(), rsvID); err != nil {
		h.Log.Error("release admission reservation after burst persist",
			"reservation", rsvID, "burst", plan.BurstID, "error", err)
	}

	// The accepted decision, assembled before the write that carries it. The
	// template fields are set rather than declared because they are absent on
	// every submission that named none, and a zero id and version rendered on
	// those would read as a launch from a template with no identity.
	detail := state.AuditDetail{
		Reason:             state.ReasonNamespaceAuthorized,
		Role:               by.Role,
		Rule:               workloadNamespaceRule,
		RuleVersion:        workloadNamespaceRuleVersion,
		RequestedNamespace: state.SafeNamespace(requestedNS),
		GrantedNamespace:   ns,
		RequestedClusterID: placement.requested,
		GrantedClusterID:   placement.granted,
		PlacementMode:      placement.mode,
		BurstID:            plan.BurstID,
	}
	// The journal references the placement by the SAME identity the receipt
	// uses, rather than restating the decision: one digest, one decision, two
	// readers.
	if plan.Placement != nil {
		detail.PlacementDigest = plan.Placement.Digest
	}
	if templateRef != nil {
		detail.TemplateID = templateRef.ID
		detail.TemplateVersion = templateRef.Version
		detail.TemplateCatalogRevision = templateRef.CatalogRevision
	}

	// Record the workload, its immutable submitter provenance, and the
	// authorization decision that admitted it — as ONE durable write, and only
	// then publish the workload to the working set.
	//
	// This replaces PutWorkload specifically because that write-through is
	// best-effort, and best-effort is the wrong trade for the authoritative
	// record of a submission central has already provisioned a node for. A
	// workload that exists only in this replica's memory is a job no other
	// replica and no restart can account for; one whose authorization never
	// landed is a decision nobody can audit. So the failure path is the one a
	// failed dispatch takes: reap the burst, record the attempt as failed,
	// answer 503 — no live VM leaks, and nothing that claims to be an accepted
	// live submission is published.
	//
	// AFTER the burst persist, not before, and that ordering is load-bearing:
	// failSubmission reaps through ClaimBurst, which can only claim a burst the
	// store already holds. Recording the workload first would mean a failure
	// here had no burst record to reap and the provisioned node would leak —
	// the exact outcome this path exists to prevent.
	var commands []state.ConnectorCommand
	if h.Commands != nil {
		commands, err = workloadConnectorCommands(agent, wlID, &wl, plan)
		if err != nil {
			h.Log.Error("build durable connector commands", "agent", agent.ID, "error", err)
			failSubmission("connector command construction failed", "could not prepare connector commands durably")
			return
		}
	}
	auditEvent := state.NewAuditEvent(state.AuditEvent{
		CustomerID: cust.ID,
		Actor:      by.Actor,
		Action:     state.ActionWorkloadSubmit,
		Outcome:    state.OutcomeAccepted,
		TargetKind: state.TargetWorkload,
		TargetID:   wlID,
		Detail:     detail,
	})
	commandsPersisted := false
	var submitErr error
	if h.Commands != nil && h.Journal == nil {
		if atomic, ok := h.Commands.(ConnectorCommandAdmissionJournal); ok {
			submitErr = atomic.SubmitWorkloadWithConnectorCommands(r.Context(), rec, auditEvent, commands)
			commandsPersisted = submitErr == nil
		}
	}
	if !commandsPersisted && submitErr == nil {
		submitErr = h.journal().SubmitWorkload(rec, auditEvent)
	}
	if submitErr != nil {
		h.Log.Error("recording the workload submission failed; reaping the provisioned node",
			"burst", plan.BurstID, "workload", wlID, "error", submitErr)
		failSubmission("workload submission not recorded",
			"could not record the workload submission durably; it was torn down — please retry")
		return
	}

	// Persist lifecycle-critical connector commands before accepting the
	// submission. The create command explicitly depends on the announce ACK, so
	// reconnect/restart replay cannot invert them. Legacy embedders with no
	// ledger retain the direct queue path below.
	if h.Commands != nil {
		if !commandsPersisted {
			if err := h.Commands.PutConnectorCommands(r.Context(), commands); err != nil {
				h.Log.Error("persist durable connector commands", "agent", agent.ID, "error", err)
				failSubmission("connector command persistence failed", "could not persist connector commands; provisioned capacity was torn down")
				return
			}
		}
	} else if err := pushBurstAnnounce(agent, &wl, plan); err != nil {
		h.Log.Error("send burst_announce failed", "agent", agent.ID, "error", err)
		failSubmission("burst_announce dispatch failed", "agent send queue full; reconfirm agent connection")
		return
	}
	if !wl.Spec.NodeOnly {
		// yscale's own Job claims the burst GPU; warn so users who wanted
		// bare capacity for their own controller (Deployment/KEDA/Argo)
		// notice instead of discovering a starved Pending pod later.
		if wl.Spec.GPU != nil {
			h.Log.Warn("non-nodeOnly GPU workload will schedule yscale's Job on the burst (claiming nvidia.com/gpu); set spec.nodeOnly=true for bare capacity",
				"workload", wlID, "gpu_kind", wl.Spec.GPU.Kind)
		}
		if h.Commands == nil {
			if err := pushCreateJob(agent, wlID, &wl, plan); err != nil {
				// Same leak as a failed announce: the node is provisioned but nothing
				// schedules yscale's Job onto it. Reap it instead of only logging.
				h.Log.Error("send create_job failed", "agent", agent.ID, "error", err)
				failSubmission("create_job dispatch failed", "agent send queue full; reconfirm agent connection")
				return
			}
		}
	}

	sub.answer(r.Context(), w, http.StatusAccepted, CreateWorkloadResponse{
		ID:           wlID,
		Status:       "provisioning",
		Backend:      plan.Backend,
		BackendID:    plan.BackendID,
		BurstID:      plan.BurstID,
		EstimatedUSD: plan.EstimatedUSD,
		Placement:    placementResponse(placement, plan.Placement),
	})
}

// hasDeclaredBudget reports whether the submitter bounded this workload
// themselves. Either half counts: a deadline and a spend cap are both terminal
// for the burst, so a workload carrying one is already bounded and central has
// no business adding a second bound on top of it.
func hasDeclaredBudget(wl *workload.Workload) bool {
	return wl.Spec.Budget != nil && (wl.Spec.Budget.Deadline > 0 || wl.Spec.Budget.MaxUSD > 0)
}

// failWorkloadAttempt records a submission that got as far as a provisioned node
// and no further: the attempt becomes a FAILED workload, so the id the caller
// was handed in the 503 is still answerable.
//
// It publishes before it finishes, because the record may exist nowhere yet: a
// failed burst persist or a failed SubmitWorkload leaves the working set without
// the workload at all, and the 503 hands back an id that GET then answered 404
// for — a submitter watching a node they were charged for provision and reap,
// with no record of either. PutWorkload is the right write here precisely
// because it is best-effort: the durable backend is usually the thing that just
// failed, and a failed ATTEMPT is not the authorization record the submit path
// fails closed on. The provenance rides along unchanged, so the attempt is
// attributable to the same actor under the same rule.
//
// FinishWorkload does the rest: it marks the record terminal under the store
// lock and emits the bounded workload.failed observation best-effort, so a
// journal that is down cannot turn a teardown into an error path. onlyIfUnfinished
// keeps it honest on the dispatch-failure path, where the workload is already
// published and a concurrent reaper may have finished it first.
func (h *Workloads) failWorkloadAttempt(rec *state.Workload) {
	h.Store.PutWorkload(rec)
	h.Store.FinishWorkload(rec.ID, "failed", time.Now().UTC(), true)
}

// compensateProvisionedBurst cleans up a provider node whose first durable
// booking failed. It deliberately does not use reapBurst: with a configured
// persister ClaimBurst elects from Postgres, and a row whose INSERT failed may
// not exist there. Conversely, treating the local map as authoritative would
// race an ambiguously-committed row claimed by another replica.
//
// The teardown operation is idempotent, so scheduling it from the exact Plan
// result is safe even if the write committed but its acknowledgement was lost.
// No cost is accrued here: the submission was never accepted into the durable
// lifecycle. Once cleanup is secured, ClaimBurst only reconciles any row that
// did commit; it never drives a second teardown from this path.
func (h *Workloads) compensateProvisionedBurst(ctx context.Context, b *state.Burst, reason string) bool {
	cleaned := false
	if h.Teardowns != nil {
		if _, err := h.enqueueTeardown(ctx, b, reason); err != nil {
			h.Log.Error("enqueue provisioning compensation failed; trying inline teardown",
				"burst", b.ID, "reason", reason, "error", err)
		} else {
			cleaned = true
		}
	}
	if !cleaned && h.Reaper != nil {
		if err := h.Reaper.Teardown(ctx, b); err != nil {
			h.Log.Error("inline provisioning compensation failed",
				"burst", b.ID, "reason", reason, "error", err)
		} else {
			leaseReleased := true
			if b.CloudAccountID != "" {
				if err := h.Store.ReleaseCloudAccountLease(ctx, b.ID); err != nil {
					h.Log.Error("inline provisioning compensation deleted provider node but cloud-account lease release failed",
						"burst", b.ID, "cloud_account", b.CloudAccountID, "reason", reason, "error", err)
					leaseReleased = false
				}
			}
			releasePodSlot(ctx, h.Store, h.Log, b)
			if err := drainBurstNode(ctx, h.Store, h.Commands, h.Log, b, reason); err != nil {
				h.Log.Warn("provisioned node deleted but cluster node cleanup was not acknowledged; NodeGC is fallback",
					"burst", b.ID, "node", b.NodeName, "reason", reason, "error", err)
			}
			if leaseReleased {
				cleaned = true
			}
		}
	}
	if !cleaned {
		// Re-establish a durable watchdog record if the failure was transient or
		// ambiguous. Upsert is idempotent if the first write actually committed.
		markReapPending(b, reason)
		if err := h.Store.PutBurst(b); err != nil {
			h.Log.Error("provisioning compensation and durable retry both failed; manual cleanup required",
				"burst", b.ID, "reason", reason, "error", err)
		}
		return false
	}

	// Remove the local marker and atomically discard an ambiguously committed
	// row. A failed claim is safe: cleanup is already queued or complete, and a
	// surviving row remains available to the ordinary reaper for reconciliation.
	if _, _, err := h.Store.ClaimBurst(b.ID); err != nil {
		h.Log.Error("provider cleanup secured but burst record reconciliation failed",
			"burst", b.ID, "reason", reason, "error", err)
	}
	return true
}

// Get handles GET /v1/workloads/{id}.
func (h *Workloads) Get(w http.ResponseWriter, r *http.Request) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	id := r.PathValue("id")
	boundClusterID := AgentClusterFromContext(r.Context())
	var snapshot state.WorkloadSnapshot
	if approved, ok := r.Context().Value(ctxConnectorWorkloadRead).(connectorWorkloadRead); ok {
		// Do not re-read after cluster authorization: another committed row
		// could name a different cluster. Nor can approval be reused after a
		// wrapper changes this request's workload, binding, tenant, or store.
		if approved.store != h.Store || approved.clusterID != boundClusterID ||
			approved.snapshot.Workload == nil || approved.snapshot.Workload.ID != id {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		snapshot = approved.snapshot
	} else {
		snapshot, err = h.Store.WorkloadSnapshotForCustomer(r.Context(), cust.ID, id)
	}
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
		} else {
			if h.Log != nil {
				h.Log.Warn("durable workload read failed", "workload", id, "error", err)
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "workload store unavailable"})
		}
		return
	}
	wl := snapshot.Workload
	if wl.CustomerID != cust.ID || (boundClusterID != "" && !connectorOwnsWorkload(cust, boundClusterID, wl.ClusterID)) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	resp := map[string]any{
		"id":         wl.ID,
		"status":     wl.Status,
		"burst_id":   wl.BurstID,
		"cluster_id": wl.ClusterID,
		"placement":  storedPlacementResponse(wl.Placement),
		"created_at": wl.CreatedAt,
		"started_at": wl.StartedAt, // null until the agent reports the Job running
	}
	if wl.RetryOfWorkloadID != "" {
		resp["retry_of"] = wl.RetryOfWorkloadID
	}
	if wl.TemplateRef != nil {
		resp["template"] = wl.TemplateRef
	}
	if wl.Outcome != nil {
		resp["outcome"] = storedOutcomeResponse(wl.Outcome)
	}
	if h.Commands != nil {
		commands, commandErr := h.Commands.ListConnectorCommandsForWorkload(r.Context(), cust.ID, wl.ID)
		if commandErr != nil {
			h.Log.Warn("list workload connector commands", "customer", cust.ID, "workload", wl.ID, "error", commandErr)
		} else if len(commands) > 0 {
			resp["connector_commands"] = commands
		}
	}
	// Return the same deliberately reduced document the human workload list
	// uses. It includes only the execution shape so a detail view can confirm
	// whether a custom recipe ran, while omitting its possibly-secret values as
	// well as environment values, secret references, storage coordinates,
	// labels, and tags.
	if spec, err := tenantWorkloadSpec(wl.SpecYAML); err == nil {
		resp["spec"] = spec
	}
	// Omitted rather than defaulted on a record that predates the field: an "api"
	// central inferred is indistinguishable on the wire from one a submission
	// actually took, and only one of those is something anybody observed.
	if wl.SubmissionOrigin != "" {
		resp["submission_origin"] = wl.SubmissionOrigin
	}
	if wl.NodeObservation != nil {
		resp["node_observation"] = nodeObservationResponse(wl.NodeObservation)
	}
	if wl.PodObservation != nil {
		resp["pod_observation"] = podObservationResponse(wl.PodObservation)
	}
	if wl.GPUObservation != nil {
		resp["gpu_observation"] = gpuObservationResponse(wl.GPUObservation)
	}
	if wl.SchedulingObservation != nil {
		resp["scheduling_observation"] = schedulingObservationResponse(wl.SchedulingObservation)
	}
	// Omitted on a workload still running or one that predates the field.
	if wl.FinishedAt != nil {
		resp["finished_at"] = wl.FinishedAt
	}
	// Frozen terminal cost, written once by the reap that tore the burst down.
	// Omitted on a workload still running, one whose reap failed, or one that
	// predates the record — a console showing $0.00 for those would state a
	// measurement nobody took.
	if wl.Cost != nil {
		resp["cost"] = tenantWorkloadCost(wl.Cost)
	}
	// Live spend and telemetry use the same fail-closed projection as the burst
	// and tenant-list surfaces. They are omitted when the burst is already
	// reaped or its stored inputs are not trustworthy.
	if b := snapshot.Burst; b != nil && b.CustomerID == wl.CustomerID {
		now := time.Now()
		if wl.Cost == nil && b.TerminalCost == nil {
			if spent, ok := liveBurstSpendUSD(b, now); ok {
				resp["spent_usd"] = spent
			}
		}
		if util, heartbeat := liveBurstGPUTelemetry(b); util != nil {
			resp["gpu_util_percent"] = *util
			resp["last_heartbeat_at"] = heartbeat
		}
	}
	// Optional tenant-safe provider cleanup view. Detected via a separate
	// optional reader interface so existing ProviderDeletes fakes and OSS seams
	// that do not carry GetProviderDelete keep working unchanged.
	if getter, ok := h.Deletes.(ProviderDeleteGetter); ok && wl.BurstID != "" && wl.ClusterID != "" {
		record, err := getter.GetProviderDelete(r.Context(), wl.CustomerID, wl.ClusterID, wl.BurstID)
		if err == nil {
			cr := cleanupResponse(record)
			if h.Evidence != nil {
				if t, err := h.Evidence.GetBurstProviderCreatedAt(r.Context(), wl.CustomerID, wl.ClusterID, wl.BurstID); err == nil {
					cr.ProviderCreatedAt = t
				}
				if ok, err := h.Evidence.BurstReapRecorded(r.Context(), wl.BurstID, wl.CustomerID); err == nil {
					cr.DurableReapReceipt = ok
				}
			}
			resp["cleanup"] = cr
			// A stale live-booking projection cannot keep accruing after the
			// authoritative delete record proves provider absence.
			if providerAbsenceConfirmed(cr.State, cr.DeletedAt) {
				delete(resp, "spent_usd")
			}
		} else if !errors.Is(err, lifecycle.ErrNotFound) && h.Log != nil {
			h.Log.Warn("read provider cleanup failed", "workload_id", wl.ID, "error", err)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// Started handles POST /v1/workloads/{id}/started — the agent's report
// that a workload's Job has actually begun running (Job.Status.Ready > 0,
// i.e. a pod is Running and Ready on the burst node). Central records
// StartedAt and flips Status to "running" atomically in the store.
//
// Idempotent: a repeat report (the agent may re-deliver on watch
// resync) or a report arriving after the workload already finished
// returns 200 with no change — StartWorkload's first-write-wins /
// never-unfinish guards are evaluated under the store lock.
func (h *Workloads) Started(w http.ResponseWriter, r *http.Request) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	id := r.PathValue("id")
	target, err := h.workloadReportTarget(r, cust)
	if err != nil {
		writeWorkloadTransitionError(w, err)
		return
	}

	report, err := h.Store.RecordWorkloadStarted(r.Context(), target, workloadCredentialFromContext(r.Context(), cust), time.Now().UTC())
	if err != nil {
		writeWorkloadTransitionError(w, err)
		return
	}
	if !report.Applied {
		// Already started, or already finished — nothing to change.
		writeJSON(w, http.StatusOK, map[string]string{"status": "unchanged"})
		return
	}
	wl := report.Workload
	// Observe provision latency: time from the workload being created (at
	// the POST /v1/workloads handler) to the agent reporting the burst is
	// running. wl.CreatedAt is stamped at creation time.
	h.Cost.ObserveProvisionLatency(time.Since(wl.CreatedAt))
	h.Log.Info("workload started", "workload", id, "burst", wl.BurstID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

// CompleteRequest is the body of POST /v1/workloads/{id}/complete.
type CompleteRequest struct {
	Phase string `json:"phase"` // "Succeeded" | "Failed"
	// Outcome is the optional receipt for this observation. Omitted by every
	// agent that reports only the phase — the legacy body and the only body
	// older connectors ever send — and stored as nothing when omitted.
	Outcome *CompleteOutcome `json:"outcome,omitempty"`
}

// CompleteOutcome is the wire form of state.WorkloadOutcome: what the compute
// Job did, and separately what the artifact export did.
//
// Compute is a pointer only so its absence is distinguishable from a zero
// struct — a receipt that says nothing about the compute is rejected, not
// stored as an empty result.
type CompleteOutcome struct {
	Compute   *CompleteComputeOutcome  `json:"compute,omitempty"`
	Artifacts *CompleteArtifactOutcome `json:"artifacts,omitempty"`
}

// CompleteComputeOutcome is the Job's own terminal result.
type CompleteComputeOutcome struct {
	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`
}

// CompleteArtifactOutcome is the artifact export's result, sent only when
// export was configured or attempted. Nothing about the destination travels
// with it: bucket, prefix, endpoint, secret reference and signed URL are all
// submitter-controlled or credential-bearing, and none is needed to say whether
// the upload worked.
type CompleteArtifactOutcome struct {
	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`
	// Counts are pointers so "did not count" stays distinct from "counted
	// zero"; both must be non-negative.
	ObjectsUploaded *int64 `json:"objects_uploaded,omitempty"`
	BytesUploaded   *int64 `json:"bytes_uploaded,omitempty"`
}

// maxCompleteReasonBytes caps the free-text note on a receipt. The body limit
// alone is not a bound on what is STORED: a workload record is read back for
// the life of the tenant, and a reason is a note, not a log shipper.
const maxCompleteReasonBytes = 512

// completeReason checks the free-text note on a receipt before it can become a
// stored record. The byte cap bounds how much a workload carries for the life
// of the tenant; the other two checks are the stance selfServiceTenantName
// takes on every customer-supplied string central stores and renders back. A
// note that is not valid UTF-8, or that carries a NUL or an escape sequence, is
// not a note — it is a payload aimed at whatever reads the record later, and
// the agent is as capable of sending one as any other client.
func completeReason(field, reason string) (string, error) {
	if len(reason) > maxCompleteReasonBytes {
		return "", fmt.Errorf("%s must be at most %d bytes", field, maxCompleteReasonBytes)
	}
	if !utf8.ValidString(reason) {
		return "", fmt.Errorf("%s must be valid UTF-8", field)
	}
	for _, r := range reason {
		if unicode.IsControl(r) {
			return "", fmt.Errorf("%s must not contain control characters", field)
		}
	}
	return reason, nil
}

// workloadOutcome validates the optional receipt against the phase the same
// request reported and converts it to the stored form. A nil result with a nil
// error is the legacy body: nothing observed, so nothing stored.
//
// The phase stays the workload's verdict and the receipt has to be consistent
// with it, but the two are not the same statement, so the rule is only that
// nothing contradicts:
//
//   - Succeeded cannot carry a failed compute. Central would be publishing a
//     success its own agent said did not happen.
//   - Failed must be explained by something the agent observed — the compute
//     failed, or the compute succeeded and the export did not. A failed phase
//     whose receipt reports everything working describes a different run.
//
// A succeeded phase with a failed export is deliberately allowed: whether a
// lost upload fails the run is the agent's call, and central refusing the
// combination would force it to misreport one half to record the other.
func workloadOutcome(phase string, in *CompleteOutcome) (*state.WorkloadOutcome, error) {
	if in == nil {
		return nil, nil
	}
	if in.Compute == nil {
		return nil, errors.New("outcome.compute is required when outcome is present")
	}
	compute := state.WorkloadComputeOutcome{Result: in.Compute.Result}
	switch compute.Result {
	case state.WorkloadResultSucceeded, state.WorkloadResultFailed:
	default:
		return nil, errors.New("outcome.compute.result must be succeeded or failed")
	}
	reason, err := completeReason("outcome.compute.reason", in.Compute.Reason)
	if err != nil {
		return nil, err
	}
	compute.Reason = reason

	out := &state.WorkloadOutcome{Compute: compute}
	if in.Artifacts != nil {
		artifacts, err := workloadArtifactOutcome(in.Artifacts)
		if err != nil {
			return nil, err
		}
		out.Artifacts = artifacts
	}

	exportFailed := out.Artifacts != nil && out.Artifacts.Result == state.WorkloadResultFailed
	switch {
	case phase == "Succeeded" && compute.Result != state.WorkloadResultSucceeded:
		return nil, errors.New("outcome.compute.result must be succeeded when phase is Succeeded")
	case phase == "Failed" && compute.Result == state.WorkloadResultSucceeded && !exportFailed:
		return nil, errors.New("phase Failed needs a failed compute or a failed artifact export")
	}
	return out, nil
}

func workloadArtifactOutcome(in *CompleteArtifactOutcome) (*state.WorkloadArtifactOutcome, error) {
	switch in.Result {
	case state.WorkloadResultSucceeded, state.WorkloadResultFailed, state.WorkloadResultSkipped:
	default:
		return nil, errors.New("outcome.artifacts.result must be succeeded, failed, or skipped")
	}
	reason, err := completeReason("outcome.artifacts.reason", in.Reason)
	if err != nil {
		return nil, err
	}
	objects, err := uploadCount("outcome.artifacts.objects_uploaded", in.Result, in.ObjectsUploaded)
	if err != nil {
		return nil, err
	}
	bytes, err := uploadCount("outcome.artifacts.bytes_uploaded", in.Result, in.BytesUploaded)
	if err != nil {
		return nil, err
	}
	return &state.WorkloadArtifactOutcome{
		Result:          in.Result,
		Reason:          reason,
		ObjectsUploaded: objects,
		BytesUploaded:   bytes,
	}, nil
}

// uploadCount copies a reported count after checking it is a count: never
// negative, and never positive on an export that reports it was skipped — an
// export that moved objects was attempted, whatever it calls itself.
func uploadCount(field, result string, n *int64) (*int64, error) {
	if n == nil {
		return nil, nil
	}
	if *n < 0 {
		return nil, fmt.Errorf("%s must not be negative", field)
	}
	if result == state.WorkloadResultSkipped && *n > 0 {
		return nil, fmt.Errorf("%s must be zero when the export was skipped", field)
	}
	v := *n
	return &v, nil
}

// Complete handles POST /v1/workloads/{id}/complete — the agent's
// report that a workload's Job has reached a terminal state. Central
// reaps the burst behind it: backend node + Tailscale device via the
// Reaper, then a DrainNode command so the agent removes the k8s Node
// object (the kubelet is dead, so the Node would linger NotReady).
//
// Idempotent: a repeat report after the burst is already reaped
// returns 200 — the agent may re-deliver on watch resync.
func (h *Workloads) Complete(w http.ResponseWriter, r *http.Request) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	id := r.PathValue("id")
	target, err := h.workloadReportTarget(r, cust)
	if err != nil {
		writeWorkloadTransitionError(w, err)
		return
	}

	// Completion controls both customer-visible status and billable resource
	// teardown. Never turn a malformed, empty, oversized, or future/unknown
	// payload into an implicit success.
	var req CompleteRequest
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "invalid completion payload", http.StatusBadRequest)
		}
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "invalid completion payload", http.StatusBadRequest)
		return
	}
	phase := req.Phase
	if phase != "Succeeded" && phase != "Failed" {
		http.Error(w, "phase must be Succeeded or Failed", http.StatusBadRequest)
		return
	}
	// Validated with the phase and BEFORE anything is mutated or reaped: a
	// receipt central cannot make sense of must not be the thing that decides a
	// workload's terminal status or tears a billable node down.
	outcome, err := workloadOutcome(phase, req.Outcome)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	status := "succeeded"
	if phase == "Failed" {
		status = "failed"
	}
	// Current credential authority, the first terminal observation and pending
	// cleanup commit together, even when lifecycle storage is unavailable or a
	// provider result has not booked its resource yet.
	report, err := h.Store.RecordWorkloadCompletion(r.Context(), target, workloadCredentialFromContext(r.Context(), cust), status, time.Now().UTC(), outcome)
	if err != nil {
		writeWorkloadTransitionError(w, err)
		return
	}

	// reap, not outcome: the completion receipt above is already called that,
	// and these two answer different questions about the same call.
	wl := report.Workload
	reap := h.reapAuthorizedWorkload(r.Context(), wl, report.Burst, "workload "+phase)
	h.Log.Info("workload complete", "workload", id, "burst", wl.BurstID,
		"phase", phase, "observation_applied", report.Applied, "cleanup", reapStatus(reap))
	writeReapStatus(w, reap)
}

// The customer-visible vocabulary for what a reap established. Only the first
// two assert that the provider resource is gone.
const (
	// burstReapedStatus: this call tore the resource down and finished.
	burstReapedStatus = "reaped"

	// burstAlreadyReapedStatus: another caller owns this burst and it is gone
	// or going. It is the answer for exactly ONE outcome — a lost claim.
	burstAlreadyReapedStatus = "already_reaped"

	// burstDeletingStatus is the honest answer while an authoritative provider
	// delete is queued, leased, retrying or waiting on an operator: central owns
	// the teardown, and it has not finished.
	burstDeletingStatus = "deleting"

	// burstCleanupPendingStatus: this call tried and did NOT finish — the
	// teardown was re-queued, or the provider call landed and its receipt would
	// not persist. Retryable, and nothing about it is reaped.
	burstCleanupPendingStatus = "cleanup_pending"

	// burstCleanupUnknownStatus: the outcome is genuinely unknown — a claim that
	// may or may not have committed, a booking that could not be read, a delete
	// intent that would not record. Reporting a lost race here is what let an
	// unknown outcome read to a customer as a completed teardown.
	burstCleanupUnknownStatus = "cleanup_unknown"
)

// reapStatus maps a reap outcome to the customer-visible cleanup status.
//
// The mapping used to be `if !Reaped() { already_reaped }`, which folded three
// non-terminal facts — unknown, re-queued, receipt-pending — into a claim that
// the node is gone. Every outcome is named explicitly here; the trailing return
// exists only because Go needs one, and it is deliberately the most pessimistic
// word so a ReapOutcome added without touching this function cannot inherit a
// reassuring one.
func reapStatus(outcome ReapOutcome) string {
	switch outcome {
	case ReapOutcomeReaped:
		return burstReapedStatus
	case ReapOutcomeNotOwned:
		return burstAlreadyReapedStatus
	case ReapOutcomeDeletePending:
		return burstDeletingStatus
	case ReapOutcomeRequeued, ReapOutcomeReceiptPending:
		return burstCleanupPendingStatus
	case ReapOutcomeUnknown:
		return burstCleanupUnknownStatus
	}
	return burstCleanupUnknownStatus
}

// writeReapStatus answers with that status and the code a client should act on.
//
// The three outcomes that leave cleanup unfinished and unowned get 503 — the
// same retryable answer this handler already gives for an unavailable
// dependency. The caller's own completion is already durable by this point, so
// a retry re-enters an idempotent reap, which is exactly what should happen. A
// delete that a worker owns is 200: nothing about it needs the caller again.
func writeReapStatus(w http.ResponseWriter, outcome ReapOutcome) {
	code := http.StatusOK
	switch outcome {
	case ReapOutcomeUnknown, ReapOutcomeRequeued, ReapOutcomeReceiptPending:
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]string{"status": reapStatus(outcome)})
}

// Cancel handles DELETE /v1/workloads/{id} — the agent saw the
// customer delete the Workload CR. Reaps the burst (idempotent: if
// it's already gone the response is the same "already_reaped" the
// Complete handler returns).
//
// Distinct from Complete because (a) the workload didn't finish, so
// status becomes "cancelled" not "succeeded"/"failed", and (b) the
// HTTP shape is DELETE with no body, matching REST conventions for
// resource-cancellation.
func (h *Workloads) Cancel(w http.ResponseWriter, r *http.Request) {
	h.cancelWorkload(w, r, "", h.Store)
}

type workloadCancellationStore interface {
	RequestWorkloadCancellation(context.Context, string, string, state.WorkloadCancelPrincipal) (state.WorkloadCancellation, error)
}

func (h *Workloads) cancelWorkload(w http.ResponseWriter, r *http.Request, accountID string, store workloadCancellationStore) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	id := r.PathValue("id")
	principal := workloadCredentialFromContext(r.Context(), cust)
	if accountID != "" {
		principal = state.WorkloadCancelPrincipal{AccountID: accountID}
	}
	cancellation, err := store.RequestWorkloadCancellation(r.Context(), cust.ID, id, principal)
	if err != nil {
		writeWorkloadTransitionError(w, err)
		return
	}
	if !cancellation.Decision.Allowed {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: cancelling this workload requires owner or admin, or the member who submitted it"})
		return
	}
	wl := cancellation.Workload
	// Cleanup can fail or the process can stop here: authorization, status and
	// the pending marker already share one commit and any watchdog can replay it.
	reap := h.reapAuthorizedWorkload(r.Context(), wl, cancellation.Burst, "workload cancelled")
	h.Log.Info("workload cancelled", "workload", id, "burst", wl.BurstID,
		"cleanup", reapStatus(reap))
	writeReapStatus(w, reap)
}

func workloadCredentialFromContext(ctx context.Context, cust *state.Customer) state.WorkloadActionPrincipal {
	principal, _ := ctx.Value(ctxWorkloadCredential).(state.WorkloadActionPrincipal)
	if principal.CredentialHash != "" || principal.AccountID != "" {
		return principal
	}
	// Trusted internal callers may carry an authenticated customer snapshot.
	// Real HTTP middleware always captures the verifier actually presented,
	// which must not be replaced by a newer credential on a mutable snapshot.
	principal.ClusterID = AgentClusterFromContext(ctx)
	principal.CredentialHash = cust.TokenHash
	if principal.ClusterID != "" {
		principal.CredentialHash = ""
		for _, cluster := range cust.RegisteredClusters {
			if cluster != nil && cluster.ClusterID == principal.ClusterID {
				principal.CredentialHash = cluster.CredentialHash
			}
		}
	}
	return principal
}

func (h *Workloads) workloadReportTarget(r *http.Request, cust *state.Customer) (state.WorkloadTransitionTarget, error) {
	id := r.PathValue("id")
	var snapshot state.WorkloadSnapshot
	if observed, ok := r.Context().Value(ctxConnectorWorkloadRead).(connectorWorkloadRead); ok {
		if observed.store != h.Store || observed.clusterID != AgentClusterFromContext(r.Context()) || observed.snapshot.Workload == nil || observed.snapshot.Workload.ID != id || observed.snapshot.Workload.CustomerID != cust.ID {
			return state.WorkloadTransitionTarget{}, state.ErrNotFound
		}
		snapshot = observed.snapshot
	} else {
		var err error
		snapshot, err = h.Store.WorkloadSnapshotForCustomer(r.Context(), cust.ID, id)
		if err != nil {
			return state.WorkloadTransitionTarget{}, err
		}
	}
	return workloadTransitionTarget(snapshot.Workload), nil
}

// A missing booking or a lost claim is not provider absence. Both completion
// and cancellation need positive receipt/lifecycle evidence before saying so.
func (h *Workloads) reapAuthorizedWorkload(ctx context.Context, wl *state.Workload, booking *state.Burst, reason string) ReapOutcome {
	if booking != nil {
		result := h.reapBurst(ctx, wl.BurstID, reason)
		if result != ReapOutcomeNotOwned {
			return result
		}
	}
	recorded, err := h.Store.BurstReapRecorded(ctx, wl.BurstID, wl.CustomerID)
	if err != nil {
		return ReapOutcomeUnknown
	}
	if recorded {
		return ReapOutcomeNotOwned
	}
	clusterID := wl.ClusterID
	if clusterID == "" && booking != nil {
		clusterID = booking.ClusterID
	}
	if reader, ok := h.Deletes.(ProviderDeleteGetter); ok && clusterID != "" && wl.BurstID != "" {
		deleted, err := reader.GetProviderDelete(ctx, wl.CustomerID, clusterID, wl.BurstID)
		if err == nil {
			if deleted.State == lifecycle.ProviderDeleteTerminated {
				return ReapOutcomeNotOwned
			}
			return ReapOutcomeDeletePending
		}
		if !errors.Is(err, lifecycle.ErrNotFound) {
			return ReapOutcomeUnknown
		}
	}
	if h.Deletes == nil || wl.BurstID == "" {
		return ReapOutcomeUnknown
	}
	// Completion/cancellation is durable. A late bound booking inherits its
	// pending-cleanup marker and the watchdog retries through the same lifecycle.
	return ReapOutcomeRequeued
}

func workloadTransitionTarget(w *state.Workload) state.WorkloadTransitionTarget {
	return state.WorkloadTransitionTarget{WorkloadID: w.ID, CustomerID: w.CustomerID, ClusterID: w.ClusterID, BurstID: w.BurstID}
}

func writeWorkloadTransitionError(w http.ResponseWriter, err error) {
	if errors.Is(err, state.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "workload store unavailable"})
}

// ReapOutcome is what a reap call knows about the burst it was asked to tear
// down, and it exists because "not reaped" is four different facts about a
// running node. A bool collapses a claim whose outcome is unknown, a claim
// another winner took, a provider delete that definitively did NOT run, and a
// provider delete that DID run whose receipt would not persist — and only the
// third of those describes a node still there for this call to hand back.
//
// The idle path is where that difference is load-bearing: it holds a cordon it
// placed itself, and giving that cordon back on any of the other three either
// re-opens a node another worker is destroying or puts work onto a node whose
// VM is already gone. Every other caller only ever wanted Reaped.
type ReapOutcome int

const (
	// ReapOutcomeUnknown is the zero value on purpose: a wrapper that answers
	// without knowing must answer with the outcome no caller may act on. It is
	// what ClaimBurst returns an error as — the claim may or may not have
	// committed — and what a failed teardown whose re-queue would not persist
	// returns, because nothing there can promise the retry it names.
	ReapOutcomeUnknown ReapOutcome = iota

	// ReapOutcomeReaped: this call won the claim and the burst is definitively
	// reaped — teardown ran inline with its receipts durable, or was handed to the
	// durable teardown queue. Either way the node is on its way out.
	ReapOutcomeReaped

	// ReapOutcomeNotOwned: the claim was lost. Another winner owns this burst and
	// may be tearing it down right now; nothing here touched it.
	ReapOutcomeNotOwned

	// ReapOutcomeRequeued: the claim was won, the provider delete FAILED, and the
	// live burst was durably restored for a later reap. The only outcome that
	// proves the node is still up and still this call's to give back.
	ReapOutcomeRequeued

	// ReapOutcomeReceiptPending: the provider delete SUCCEEDED but the cost
	// observation or the teardown receipt would not persist, so the burst was
	// re-queued to retry the receipt. The node is gone; the history is what is
	// missing, and the re-queued row must not be read as a live node.
	ReapOutcomeReceiptPending

	// ReapOutcomeDeletePending: lifecycle mode is on and the authoritative
	// provider-delete operation for this burst is durable — queued, leased,
	// retrying, or waiting on an operator. Exactly one such operation exists per
	// burst, so a racing Complete, Cancel or watchdog gets this same answer.
	//
	// It is NOT reaped. The provider resource has not been confirmed deleted, so
	// the burst is still a live-cost obligation, the pod /24 is still reserved,
	// the ledger is still unsettled, and no customer response may say otherwise.
	// It is also NOT Requeued: the node cannot be handed back, because a delete
	// worker may be inside the provider call for it right now.
	ReapOutcomeDeletePending
)

// Reaped is the answer every caller but the idle path needs: teardown is this
// call's, and it is done or durably queued.
func (o ReapOutcome) Reaped() bool { return o == ReapOutcomeReaped }

// Accepted reports whether this call owns the end of the burst — either it is
// definitively reaped, or the authoritative delete that ends it is durable and
// a worker owns the rest.
//
// It is the right question for a caller deciding what to record ABOUT the
// workload (an over-budget expiry, a failed dispatch): those facts are true the
// moment the delete is durable. It is the wrong question for a caller deciding
// what to tell a customer about cleanup, which is what Reaped answers.
func (o ReapOutcome) Accepted() bool {
	return o == ReapOutcomeReaped || o == ReapOutcomeDeletePending
}

// reapBurst tears down the backend + tailnet resources behind a burst and
// clears its record. It is the single reap path shared by Complete, Cancel,
// and the reaper watchdog. Exactly one caller ends a given burst — across
// replicas, not just goroutines — so teardown and cost accrual happen exactly
// once even if two reapers fire concurrently.
//
// Which primitive enforces that depends on the mode. With the authoritative
// delete machine wired (Deletes non-nil) it is the durable delete operation:
// one per burst, leased to one worker at a time, and recorded BEFORE anything
// destructive. Without it, it is ClaimBurst, whose removal-first ordering is
// the OSS/dev behaviour this integration exists to stop shipping to production.
//
// It returns ReapOutcomeReaped only when teardown succeeded (or was durably
// queued) and the record is cleared; every other outcome names which of the
// non-reaped states this call is actually in — see ReapOutcome.
// ReapBurst is the exported entry point to the reap path, so other components
// (e.g. tenant offboard wired in main) can reuse it without re-implementing the
// claim + durable-teardown logic.
func (h *Workloads) ReapBurst(ctx context.Context, burstID, reason string) ReapOutcome {
	return h.reapBurst(ctx, burstID, reason)
}

func (h *Workloads) reapBurst(ctx context.Context, burstID, reason string) ReapOutcome {
	if h.Deletes != nil {
		return h.reapBurstViaLifecycle(ctx, burstID, reason)
	}
	return h.reapBurstInline(ctx, burstID, reason)
}

// reapBurstViaLifecycle is the production path: the durable delete operation is
// recorded (or replayed) FIRST, and nothing destructive happens until a worker
// has confirmed the provider resource is actually gone.
//
// Three things are deliberately absent compared to the inline path. It does not
// claim the burst away — the live record is what keeps the burst counted, swept
// and billed, and retiring it before the provider call is what turns an
// unreachable cloud API into an untracked paid node. It does not accrue cost,
// because nothing is over yet. And it does not publish to the
// broker as if a queue handoff were a teardown; the wake-up below is a cache in
// front of the durable row, and losing every one of them changes only latency.
func (h *Workloads) reapBurstViaLifecycle(ctx context.Context, burstID, reason string) ReapOutcome {
	b, err := h.bookedBurst(ctx, burstID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			// No live booking: either another path already carried this burst all
			// the way through a confirmed delete, or it never existed. Nothing here
			// can name a provider resource, so nothing here may act on one.
			return ReapOutcomeNotOwned
		}
		h.Log.Error("burst booking unreadable; NOT recording a provider delete — the identity a delete acts on must come from the booking",
			"burst", burstID, "reason", reason, "error", err)
		return ReapOutcomeUnknown
	}
	if b.BackendID == "" {
		// A booking that never received a provider resource has no provider
		// deletion to be authoritative about, and the delete machine is keyed on
		// exactly that ID. The inline path is not destructive here for the same
		// reason: there is nothing at the provider to strand.
		h.Log.Warn("burst has no backend resource id; cleaning it up inline — there is no provider resource for the delete machine to own",
			"burst", burstID, "reason", reason)
		return h.reapBurstInline(ctx, burstID, reason)
	}

	resp, err := h.recordProviderDeleteIntent(ctx, b, reason)
	if err != nil {
		// Fail CLOSED. The burst stands, still billing and still sweepable, which
		// is recoverable; proceeding would tear down a paid resource with no
		// durable record that it was ever supposed to happen.
		h.Log.Error("provider-delete intent not durable; the burst stands and a later reap retries it",
			"burst", burstID, "reason", reason, "error", err)
		return ReapOutcomeUnknown
	}
	if resp.State == lifecycle.ProviderDeleteTerminated {
		// The provider resource is confirmed gone and its cleanup already ran;
		// this is a stale live record catching up (a worker that could not retire
		// it, or a replica that had not seen the delete). Retiring it here is
		// reconciliation, not teardown.
		if _, _, err := h.Store.ClaimBurst(burstID); err != nil {
			h.Log.Error("provider delete is terminated but the stale burst record was not retired; a later reap reconciles it",
				"burst", burstID, "reason", reason, "error", err)
			return ReapOutcomeUnknown
		}
		return ReapOutcomeReaped
	}
	h.wakeDeleteWorker()
	h.Log.Info("provider delete recorded; the burst stays live-cost until the provider confirms deletion",
		"burst", burstID, "delete", resp.DeleteID, "state", resp.State,
		"created", resp.Inserted, "reason", reason)
	return ReapOutcomeDeletePending
}

// bookedBurst resolves the current durable booking a delete derives its
// provider identity from. A stale cache is neither permission to delete an old
// resource nor a fallback when the authoritative database is unavailable.
func (h *Workloads) bookedBurst(ctx context.Context, burstID string) (*state.Burst, error) {
	return h.Store.BurstSnapshotContext(ctx, burstID)
}

// reapBurstInline is the pre-lifecycle path, unchanged: claim, then tear down
// (through the Redis queue when one is configured, otherwise in-process). It is
// the OSS/dev behaviour and stays reachable only while Deletes is nil.
func (h *Workloads) reapBurstInline(ctx context.Context, burstID, reason string) ReapOutcome {
	b, ok, err := h.Store.ClaimBurst(burstID)
	if err != nil {
		// Unknown outcome: the claim may or may not have committed, so another
		// reaper may already own this burst. Tearing down on a maybe-claim is the
		// one mistake that double-bills a customer for a single burst. The durable
		// record survives a failed claim, so a later reap retries it; until then
		// the VM keeps billing, which is the accepted half of that trade.
		h.Log.Error("burst claim failed; NOT tearing down — the outcome is unknown and a second teardown would double-bill; the burst stays claimable for a later reap",
			"burst", burstID, "reason", reason, "error", err)
		return ReapOutcomeUnknown
	}
	if !ok {
		return ReapOutcomeNotOwned
	}

	// Durable path: hand cloud teardown to the broker queue so retries survive
	// a central restart and can run from a separate worker process. The burst
	// is definitively reaped the moment it's claimed (record gone), so cost
	// accrues once here; the worker performs only the idempotent cloud teardown
	// + node drain, retried via redelivery.
	if h.Teardowns != nil {
		observed, err := h.enqueueTeardown(ctx, b, reason)
		if err != nil {
			// Broker enqueue failed — fall through to inline teardown so the
			// VM is never orphaned by a broker outage.
			h.Log.Error("enqueue teardown failed; tearing down inline",
				"burst", b.ID, "reason", reason, "error", err)
		} else {
			// The pod /24 stays reserved: the node is still up until the worker
			// runs, and it is still advertising that prefix. TeardownWorker
			// releases the slot once the provider confirms the node is gone.
			h.accrueObservedCost(ctx, b, *observed)
			h.Cost.RecordReap("ok")
			return ReapOutcomeReaped
		}
	}

	// In-process path (no broker, or enqueue failed): tear down now.
	if h.Reaper != nil {
		if err := h.Reaper.Teardown(ctx, b); err != nil {
			// Teardown failed — the backend VM may still be running and
			// billing. ClaimBurst already removed the record, so if we drop it
			// here nothing ever retries the teardown and the VM is orphaned
			// (silent cost leak). Re-insert the record so a later sweep
			// retries; skip drain + cost accrual (not definitively reaped yet).
			// DeleteNode is idempotent, so retrying is a safe no-op.
			h.Log.Error("burst teardown failed; re-queued for retry",
				"burst", b.ID, "reason", reason, "error", err)
			markReapPending(b, reason)
			if perr := h.Store.PutBurst(b); perr != nil {
				// The retry this path promises is now in-memory only: a restart
				// loses the record and nothing sweeps the VM. Unknown, not
				// Requeued — the node needs an operator, and a caller holding a
				// cordon has no restored record to hand it back to.
				h.Log.Error("re-queued burst not persisted; backend node may still be billing with no durable record — needs manual cleanup",
					"burst", b.ID, "reason", reason, "error", perr)
				h.Cost.RecordReap("fail")
				return ReapOutcomeUnknown
			}
			h.Cost.RecordReap("fail")
			return ReapOutcomeRequeued
		}
	}
	releasePodSlot(ctx, h.Store, h.Log, b)
	if err := drainBurstNode(ctx, h.Store, h.Commands, h.Log, b, reason); err != nil {
		// Inline mode has no durable command queue. The provider is already
		// deleted; NodeGC remains the safe fallback, but surface the degraded
		// cleanup instead of silently pretending the command was delivered.
		h.Log.Warn("burst provider reaped but node cleanup not acknowledged; NodeGC is fallback",
			"burst", b.ID, "node", b.NodeName, "reason", reason, "error", err)
	}
	observed := workloadCostForBurst(b, time.Now().UTC())
	if b.TerminalCost != nil {
		observed = *b.TerminalCost
	}
	confirmed, costErr := h.Store.EnsureWorkloadCostForBurst(ctx, observed)
	if costErr != nil {
		h.Log.Error("burst provider reaped but cost receipt not durable; re-queued to retry the receipt",
			"burst", b.ID, "reason", reason, "recorded", confirmed, "error", costErr)
		markReapPending(b, reason)
		if perr := h.Store.PutBurst(b); perr != nil {
			h.Log.Error("receipt retry burst not persisted; teardown history needs manual repair",
				"burst", b.ID, "reason", reason, "error", perr)
		}
		h.Cost.RecordReap("fail")
		return ReapOutcomeReceiptPending
	}
	if !confirmed {
		h.Log.Info("reaped burst has no workload row for a cost observation; recording the independent reap receipt",
			"burst", b.ID, "customer", b.CustomerID)
	}
	if recorded, receiptErr := h.Store.RecordBurstReap(ctx, b.ID, b.CustomerID); receiptErr != nil || !recorded {
		h.Log.Error("burst provider reaped but teardown receipt not durable; re-queued to retry the receipt",
			"burst", b.ID, "customer", b.CustomerID, "reason", reason,
			"recorded", recorded, "error", receiptErr)
		markReapPending(b, reason)
		if perr := h.Store.PutBurst(b); perr != nil {
			h.Log.Error("receipt retry burst not persisted; teardown history needs manual repair",
				"burst", b.ID, "reason", reason, "error", perr)
		}
		h.Cost.RecordReap("fail")
		return ReapOutcomeReceiptPending
	}
	h.accrueObservedCost(ctx, b, observed)
	h.Cost.RecordReap("ok")
	return ReapOutcomeReaped
}

// markReapPending records enough of the already-requested reap for the
// watchdog to finish both halves after the original caller is gone: provider
// cleanup and the workload's terminal state. A verified idle node is ordinary
// scale-in, so it cancels; every other unfinished workload lost its capacity or
// failed cleanup and therefore fails. FinishWorkloadForBurst never overwrites a
// status the workload already reported.
func markReapPending(b *state.Burst, reason string) {
	b.ReapPending = true
	if b.ReapPendingStatus != "" {
		// A retry uses a generic reason. Preserve the terminal outcome recorded
		// by the original caller, especially idle scale-in's "cancelled".
		return
	}
	b.ReapPendingStatus = "failed"
	if reason == nodeIdleReason {
		b.ReapPendingStatus = "cancelled"
	}
}

// accrueCost records the burst's upstream cost (rate × lifetime) exactly once,
// when it is reaped: to the process's cost meter, and to the workload's durable
// history. Both take ONE runtime, measured here — the meter is a live fleet
// series and the observation is what a customer reads back months later, and
// two clock reads would let those two answers disagree about the same node.
//
// It runs only on the paths that definitively reaped the burst: the claim was
// won AND teardown was either handed to the durable queue or completed inline.
// A teardown that failed re-queues the burst and records nothing, because that
// node is still running and its cost is not final yet.
//
// Metering is a no-op when disabled; the observation is not, and does not
// depend on it.
//
// The counters the meter feeds are per-process and additive, so they are exact.
// The active/hourly GAUGES are per-process too, and the BurstCreated that
// matched this decrement may have run on another replica — so a single pod's
// series can sit high or go negative. sum() across pods stays exact, which is
// the only way a per-pod gauge may be read anyway; the same is already true
// single-replica, since nothing re-primes these gauges from the bursts hydrated
// at boot. Making a pod's own series meaningful means publishing fleet-wide
// values from every pod, which inverts that aggregation — a dashboards and
// recording-rule change, not one to smuggle in here.
func (h *Workloads) accrueCost(ctx context.Context, b *state.Burst) {
	observed := workloadCostForBurst(b, time.Now().UTC())
	h.accrueObservedCost(ctx, b, observed)
}

func workloadCostForBurst(b *state.Burst, frozenAt time.Time) state.WorkloadCost {
	runtime := frozenAt.Sub(b.CreatedAt)
	if runtime < 0 {
		// CreatedAt comes off a durable row another replica wrote, so it is that
		// replica's clock against this one. A negative lifetime would credit the
		// cost counter and store a negative estimate; zero says "no measurable
		// runtime", which is the honest reading of a node reaped at its birth.
		runtime = 0
	}
	return state.WorkloadCost{
		EstimatedUSD: b.HourlyUSD * runtime.Hours(),
		HourlyUSD:    b.HourlyUSD,
		Runtime:      runtime,
		FrozenAt:     frozenAt,
		Backend:      b.Backend,
		BurstID:      b.ID,
		Basis:        state.WorkloadCostBasisRateRuntime,
	}
}

func (h *Workloads) accrueObservedCost(ctx context.Context, b *state.Burst, observed state.WorkloadCost) {
	if h.Cost != nil {
		h.Cost.BurstReaped(b.Backend, b.HourlyUSD, observed.Runtime)
	}
	h.freezeWorkloadCost(ctx, observed)
}

// freezeWorkloadCost writes the historical observation onto the workload the
// burst backed. Best-effort by construction: the teardown it records has
// already happened and cannot be undone, so a store that refuses the write
// costs the customer their record of this run and nothing else.
//
// The failure log carries every field the observation was built from, so the
// row can be reconstructed by hand from it. Bounded to exactly those: the burst
// carries the tenant's mesh hostname and its backend booking, and a log line
// that dumped the record to make an operator's life easier would put them in
// whatever aggregator reads these.
func (h *Workloads) freezeWorkloadCost(ctx context.Context, observed state.WorkloadCost) {
	if _, err := h.Store.RecordWorkloadCostForBurst(ctx, observed); err != nil {
		h.Log.Error("workload cost observation not recorded; the teardown stands and the meter has the same runtime — these fields are the whole record",
			"burst", observed.BurstID, "backend", observed.Backend,
			"hourly_usd", observed.HourlyUSD, "runtime_seconds", observed.Runtime.Seconds(),
			"frozen_at", observed.FrozenAt.Format(time.RFC3339Nano), "usd", observed.EstimatedUSD,
			"basis", observed.Basis, "error", err)
	}
}

// releasePodSlot frees the burst's durable pod /24 reservation. Shared by the
// inline reap path and the teardown worker, and called from both only AFTER the
// provider has confirmed the node destroyed.
//
// The ordering is the whole point. A burst advertises its /24 as a mesh subnet
// route, so a slot handed to a new burst while the old node is still up gives
// the mesh two peers claiming one prefix — the same duplicate-/24 outage the
// durable reservation exists to prevent, just reached from the other end. That
// rules out releasing at claim time: the claim only elects a reaper, and the
// node it elected a reaper for is still running.
//
// A release that never happens costs one /24 until the reservation ages out. A
// release that happens too early costs two customers their networking, so every
// caller invokes this only after provider absence. Lifecycle cleanup propagates
// the error and retries before recording its reap receipt; legacy inline callers
// retain their best-effort behavior by discarding the returned error.
func releasePodSlot(ctx context.Context, store *state.Store, log *slog.Logger, b *state.Burst) error {
	if err := store.ReleasePodSlot(ctx, b.ID); err != nil {
		log.Warn("pod cidr reservation not released; the slot is unavailable until it ages out",
			"burst", b.ID, "pod_cidr", b.PodCIDR, "error", err)
		return err
	}
	return nil
}

// drainBurstNode tells the burst's customer agent to delete the (now-dead) k8s
// Node object. Idempotent; shared by the inline reap path and the teardown
// worker. No-op for a burst that never registered a node.
//
// There are two ways it can get there, and which one runs is decided by whether
// the burst knows its cluster.
//
// With the durable ledger (the production wiring), the drain is a row: any
// replica can persist it, because persisting needs the burst's tenant and
// cluster and no socket at all, and the replica that owns the socket claims and
// delivers it. That is what closes the gap this comment used to describe — the
// agent's websocket is held by exactly ONE replica, so a reap running anywhere
// else found no agent, returned, and left a NotReady ghost Node until agent-side
// NodeGC took it. The broker teardown path covered some of it by accident
// (redelivery eventually lands on the socket holder); the inline path covered
// none of it.
//
// Without one — a legacy burst with no ClusterID, or an embedder that wired no
// ledger — it stays the synchronous command below: enqueue onto the connected
// agent and block on its ack up to drainCommandAckTimeout. A tenant with two
// clusters and a burst that names neither is refused rather than guessed at, and
// falling through still costs only the ghost Node: no billing, no routing.
const drainCommandAckTimeout = 3 * time.Minute

// agentForBurst resolves the connector that owns the cluster a burst was booked
// on. The drain deletes a k8s Node object by name, and Node names are only
// unique within a cluster — sent to the tenant's OTHER cluster it is at best a
// no-op and at worst a delete of an unrelated node that happens to share the
// name.
//
// A burst booked before ClusterID was recorded carries none, and for those the
// single-connector lookup is what it always was. It stays safe by refusing:
// AgentForCustomer answers only while the tenant has one cluster, so an
// ambiguous legacy burst returns an error instead of guessing. The caller
// treats that like any other undeliverable drain — the node is already
// destroyed, and agent-side NodeGC removes the ghost.
func agentForBurst(store *state.Store, b *state.Burst) (*state.Agent, error) {
	if b.ClusterID != "" {
		return store.AgentForCluster(b.CustomerID, b.ClusterID)
	}
	return store.AgentForCustomer(b.CustomerID)
}

// agentForWorkload is the read-side equivalent of agentForBurst. Current
// records carry the cluster selected at admission; a legacy record may use the
// old single-connector fallback, which refuses rather than guesses when a
// tenant now has more than one cluster.
func agentForWorkload(store *state.Store, w *state.Workload) (*state.Agent, error) {
	if w.ClusterID != "" {
		return store.AgentForCluster(w.CustomerID, w.ClusterID)
	}
	return store.AgentForCustomer(w.CustomerID)
}

// drainNodeConnectorCommand builds the durable drain for a burst, under an
// identity derived only from facts that cannot change: the burst's tenant, the
// burst's cluster, and the burst id. Two reaps of the same burst — an inline
// compensation and the teardown worker's retry, or the same queue job
// redelivered — therefore produce the SAME command id, the same digest and the
// same envelope, so the second one finds the first rather than queueing a
// second drain of a node that is already being deleted.
func drainNodeConnectorCommand(b *state.Burst) (state.ConnectorCommand, error) {
	body, err := json.Marshal(protocol.DrainNode{NodeName: b.NodeName, Delete: true})
	if err != nil {
		return state.ConnectorCommand{}, err
	}
	semantic := connectorCommandSemantic(protocol.TypeDrainNode, b.ID)
	commandID := state.StableConnectorCommandID(b.CustomerID, b.ClusterID, semantic)
	return state.NewConnectorCommand(b.CustomerID, b.ClusterID, semantic, "burst:"+b.ID,
		"", b.ID, "", protocol.Envelope{
			APIVersion: protocol.APIVersion,
			Type:       protocol.TypeDrainNode,
			ID:         commandID,
			Timestamp:  time.Now().UTC(),
			Body:       body,
		})
}

func drainBurstNode(ctx context.Context, store *state.Store, commands ConnectorCommandLedger, log *slog.Logger, b *state.Burst, reason string) error {
	if b.NodeName == "" {
		return nil
	}
	// The durable path, and the reason the comment above about "the drain
	// usually gets a turn" is no longer the whole story: persisting the command
	// needs the burst's tenant and cluster and NOTHING else. A reap running on a
	// replica that has never seen this connector's socket writes the row, and
	// whichever replica does own the socket claims and dispatches it — with the
	// ledger's retry, ordering and dead-letter behind it instead of one
	// in-process ack wait that dies with the request.
	//
	// It needs the burst's exact cluster to do that, because a command row is
	// leased per (customer, cluster) and a node name is unique only inside one
	// cluster. A legacy burst that predates ClusterID has no cluster to name, so
	// it keeps the synchronous single-connector lookup below — which refuses to
	// guess when the tenant now has more than one cluster, exactly as before.
	if commands != nil && b.ClusterID != "" {
		cmd, err := drainNodeConnectorCommand(b)
		if err != nil {
			return err
		}
		if err := commands.PutConnectorCommands(ctx, []state.ConnectorCommand{cmd}); err != nil {
			return fmt.Errorf("persist durable node drain: %w", err)
		}
		log.Info("drain_node command persisted for durable delivery",
			"customer", b.CustomerID, "cluster", b.ClusterID, "node", b.NodeName,
			"burst", b.ID, "command", cmd.ID, "reason", reason)
		return nil
	}
	agent, err := agentForBurst(store, b)
	if err != nil {
		return fmt.Errorf("no connected agent for node cleanup: %w", err)
	}
	body, err := json.Marshal(protocol.DrainNode{NodeName: b.NodeName, Delete: true})
	if err != nil {
		return err
	}
	commandID := newID("cmd")
	ackCh, unregister := agent.RegisterCommandAck(commandID)
	defer unregister()
	if err := enqueue(agent, protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       protocol.TypeDrainNode,
		ID:         commandID,
		Timestamp:  time.Now().UTC(),
		Body:       body,
	}); err != nil {
		return err
	}
	log.Info("drain_node command enqueued",
		"agent", agent.ID, "node", b.NodeName, "burst", b.ID,
		"command", commandID, "reason", reason)

	timer := time.NewTimer(drainCommandAckTimeout)
	defer timer.Stop()
	select {
	case ack := <-ackCh:
		if !ack.Success {
			// The connector's own error text is deliberately dropped rather than
			// wrapped: every caller of this function logs the error it returns,
			// and that text is authored inside the customer's cluster. The
			// stable reason code is what central can stand behind.
			return fmt.Errorf("agent rejected drain command (reason %s)", state.ConnectorCommandReasonConnectorRejected)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("timed out after %s waiting for drain acknowledgement", drainCommandAckTimeout)
	}
}

// RunReaperWatchdog retries any explicitly pending reap, then enforces each
// burst's declared budget centrally. A pending reap is independent of budget:
// cleanup was already requested and must finish even when every ceiling below
// is disabled. For bursts without pending cleanup, central has NO liveness
// signal — the agent reports
// completion over HTTP, not a heartbeat — so this is a budget enforcer, not a
// health probe: a burst is reaped only when it exceeds its workload's declared
// Budget.Deadline (wall-clock) or Budget.MaxUSD (accrued spend). This bounds
// the cost of a burst whose completion report never arrives (stuck pod, lost
// signal, image-pull failure) instead of letting it run indefinitely.
//
// safetyNetMaxLifetime is an OPT-IN global cap for ordinary (non-nodeOnly)
// bursts that declared no per-workload budget at all; pass 0 to disable it.
// Explicit per-workload budgets are authoritative, so the global fallback
// never shortens a declared Deadline or MaxUSD budget.
//
// nodeOnlyMaxLifetime is a different thing and deliberately not the same knob:
// the longest a nodeOnly burst may go UNOBSERVED, applied whether or not a
// budget was declared. nodeOnly bursts back pods central never created, so the
// signal that ends one comes from the customer's cluster — a connector reporting
// the node removed or idle. When that connector is gone, no signal is coming, and
// the burst has nothing else to stop it.
//
// It is silence, not age — and the silence measured is specifically the absence
// of an OCCUPANCY observation (IdleNodeWatcher.OccupancyInterval), never a health
// report. A node that is up and busy under a connector with cluster-wide pod
// visibility keeps saying so and never expires here however long it runs, which
// is the whole point for capacity behind a Deployment meant to stay up.
//
// It applies only where an observation was OWED — either one has arrived, or the
// burst was admitted on a connector that claimed the capability and is recorded
// as having promised it (Burst.OccupancyObservationExpected). A connector that
// never made the claim — the shipped namespaced chart, or any install without
// the pod-visibility grant — is not silent, it is unequipped, and this ceiling
// has nothing to measure. Enforcing it against those anyway destroyed active
// nodes for a capability the default install was never given.
//
// Those bursts are not unbounded, they are bounded somewhere else: admission
// gives an unobservable nodeOnly submission with no declared budget a deadline of
// this same duration, so the two halves of the policy are one number. See
// Workloads.NodeOnlyMaxLifetime and nodeObservationSilence.
//
// Pass 0 to disable it and accept an observed-then-abandoned burst running
// forever; the shipped default is finite. Blocks until ctx is cancelled.
func (h *Workloads) RunReaperWatchdog(ctx context.Context, interval, safetyNetMaxLifetime, nodeOnlyMaxLifetime time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	h.Log.Info("reaper watchdog started",
		"interval", interval.String(),
		"safety_net_max_lifetime", safetyNetMaxLifetime.String(),
		"node_only_max_lifetime", nodeOnlyMaxLifetime.String())
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.sweepExpiredBursts(ctx, safetyNetMaxLifetime, nodeOnlyMaxLifetime)
		}
	}
}

// sweepExpiredBursts retries pending reaps and reaps every burst that has
// overrun its budget. One pass. Explicit per-workload budgets are authoritative.
// The global safety-net cap is a fallback only for unbudgeted ordinary
// (non-nodeOnly) jobs. The nodeOnly cap is a ceiling rather than a fallback —
// see RunReaperWatchdog.
//
// It sweeps DURABLE state, not this process's map, so ANY replica enforces ANY
// burst's pending cleanup or budget — a burst whose creating replica has died
// or restarted still has a watchdog, and ClaimBurst still lets exactly one of
// them tear it down.
func (h *Workloads) sweepExpiredBursts(ctx context.Context, safetyNetMaxLifetime, nodeOnlyMaxLifetime time.Duration) {
	// Bounded independently of the watchdog's process-lifetime ctx: a read that
	// hangs would stop every budget from being enforced, silently.
	readCtx, cancelRead := context.WithTimeout(ctx, 30*time.Second)
	bursts, durable, err := h.Store.DurableBursts(readCtx)
	cancelRead()
	if err != nil {
		// Skip the cycle and let the next tick retry. Milder than the orphan
		// sweep's equivalent — a missed pass leaves budgets unenforced for a
		// tick, which costs money, where a blind orphan sweep destroys live
		// bursts and costs a customer their job. Falling back to the in-memory
		// map would still be wrong: it silently restores the per-replica
		// blindness this read exists to remove, and a burst another replica
		// created would go entirely unwatched rather than briefly unswept.
		h.Log.Error("burst budget sweep skipped: could not read durable burst set; budgets go unenforced until the next tick",
			"error", err)
		return
	}
	if !durable {
		// No durable backend: one process, so its map IS the burst set.
		bursts = h.Store.ListBursts()
	}
	now := time.Now()
	for _, b := range bursts {
		age := now.Sub(b.CreatedAt)
		silence, observed := nodeObservationSilence(b, now)
		switch {
		case b.ReapPending:
			// An accepted cancellation or failed inline cleanup left this durable
			// retry marker. It is not a provider-delete claim, nor is its retry a
			// budget policy: an unbudgeted burst with the global safety net disabled
			// still has to finish the teardown a caller already initiated.
			if h.reapBurst(ctx, b.ID, "retrying pending burst cleanup").Reaped() {
				status := b.ReapPendingStatus
				if status == "" {
					// Compatibility for a pending record written before the status
					// field existed. Failing an unfinished workload is safer than
					// leaving it running after its provider node is gone.
					status = "failed"
				}
				finishCtx, cancelFinish := context.WithTimeout(ctx, 10*time.Second)
				_, finishErr := h.Store.FinishWorkloadForBurst(finishCtx, b.ID, status, time.Now().UTC())
				cancelFinish()
				if finishErr != nil {
					h.Log.Error("pending burst cleanup finished but its workload status did not",
						"burst", b.ID, "status", status, "error", finishErr)
				}
			}
		case b.Deadline > 0 && age > b.Deadline:
			h.expireBurst(ctx, b, fmt.Sprintf("workload deadline exceeded (age %s > %s)",
				age.Round(time.Second), b.Deadline))
		case b.MaxUSD > 0 && b.HourlyUSD*age.Hours() > b.MaxUSD:
			h.expireBurst(ctx, b, fmt.Sprintf("workload budget exceeded ($%.4f accrued > $%.4f)",
				b.HourlyUSD*age.Hours(), b.MaxUSD))
		case safetyNetMaxLifetime > 0 && b.Deadline == 0 && b.MaxUSD == 0 && !b.NodeOnly && age > safetyNetMaxLifetime:
			h.expireBurst(ctx, b, fmt.Sprintf("safety-net max lifetime exceeded (age %s > %s) for unbudgeted ordinary job; global fallback only applies when no per-workload budget is declared",
				age.Round(time.Second), safetyNetMaxLifetime))
		case nodeOnlyMaxLifetime > 0 && b.NodeOnly && observed && silence > nodeOnlyMaxLifetime:
			// Reached only when the declared budget has NOT already expired the
			// burst: a nodeOnly burst that declared a longer deadline is still cut
			// short once it goes silent, because the connector that was supposed to
			// end it early is the thing presumed dead. A shorter declared budget
			// wins on the arms above.
			h.expireBurst(ctx, b, fmt.Sprintf("node-only burst unobserved for %s (> %s); the connector that was proving it could still see this node has stopped, so no teardown signal is coming",
				silence.Round(time.Second), nodeOnlyMaxLifetime))
		}
	}
}

// nodeObservationSilence is how long since a connector last proved it could
// still see whether this burst's node is idle, and whether any connector ever
// has.
//
// OccupancyObservedAt, NOT NodePhaseAt. Node health is reported by connectors
// that can never end a nodeOnly burst — one scoped to a namespace, one without
// the cluster-wide pod LIST — so measuring from it kept the ceiling open forever
// for capacity nothing was actually watching. Only IdleNodeWatcher stamps this,
// and only after the pod list it acted on succeeded.
//
// Before the first observation the answer depends on what was PROMISED, and
// OccupancyObservationExpected is the record of that promise.
//
// Expected: the connector this burst was admitted on claimed cluster-wide pod
// visibility, so observations were owed from the start. Measuring from CreatedAt
// is then correct rather than a guess — it covers the connector that made the
// claim and died before its first sweep, which is otherwise a burst that bills
// forever having never been observed once.
//
// Not expected: nothing was ever promised, so a missing observation is not
// silence — it is a capability that was never there, and the ceiling has nothing
// to measure. Aging those from CreatedAt is precisely the defect that destroyed
// active nodes at 6h for a feature the default namespaced install never had.
// They are bounded instead by the deadline admission gives them (see Create) or
// by the budget their submitter declared.
func nodeObservationSilence(b *state.Burst, now time.Time) (time.Duration, bool) {
	if b.OccupancyObservedAt == nil || b.OccupancyObservedAt.IsZero() {
		if !b.OccupancyObservationExpected {
			return 0, false
		}
		return now.Sub(b.CreatedAt), true
	}
	return now.Sub(*b.OccupancyObservedAt), true
}

// expireBurst reaps an over-budget burst and marks its workload failed. Reap is
// race-safe (reapBurst claims atomically); the workload status is updated only
// if this caller won the claim and the workload hasn't already finished.
//
// The burst may have been created by another replica, whose workload this
// process has never held — so the lookup has to reach durable state, not the
// in-memory map. Getting that wrong is not cosmetic: the customer polls a
// workload that reads "provisioning" for a job whose node was destroyed.
func (h *Workloads) expireBurst(ctx context.Context, b *state.Burst, reason string) {
	// Accepted, not Reaped: a durable delete is enough to say the budget ended
	// this workload. The teardown behind it may still be running, but the
	// workload's terminal status is a fact about the BUDGET, and leaving it
	// reading "provisioning" until a provider API answers is exactly the stale
	// record the durable lookup below exists to avoid.
	if !h.reapBurst(ctx, b.ID, reason).Accepted() {
		return // another reaper won; nothing to do
	}
	h.Log.Warn("reaped over-budget burst", "burst", b.ID, "backend", b.Backend, "reason", reason)
	// Bounded independently of the watchdog's process-lifetime ctx, like the
	// sweep's own durable read: a lookup that hangs would stall the rest of the
	// pass, leaving every burst behind this one unenforced.
	wlCtx, cancelWL := context.WithTimeout(ctx, 10*time.Second)
	applied, err := h.Store.FinishWorkloadForBurst(wlCtx, b.ID, "failed", time.Now().UTC())
	cancelWL()
	if err != nil {
		h.Log.Error("over-budget burst reaped but its workload was not marked failed; it reads as still running",
			"burst", b.ID, "reason", reason, "error", err)
		return
	}
	if !applied {
		// Lifecycle mode deliberately keeps the burst live until provider absence
		// is confirmed, so every watchdog pass sees it. The workload transition is
		// the idempotency boundary for this observation: only its winner records
		// that the budget expired, while later passes merely replay delete intent.
		return
	}
	// The expiry observation is the watchdog's own, and it is journaled here
	// rather than in the store because only this caller knows a budget is what
	// ended the burst — FinishWorkloadForBurst sees "failed" and nothing else.
	// It is the tenant's row, with the burst named in the detail: the burst may
	// have been created by another replica, whose workload id this process has
	// never held, and the store's own workload.reaped row already carries that
	// id. Best-effort, and deliberately AFTER the reap: a journal that is down
	// must never keep a burst billing.
	if auditErr := h.journal().AppendAudit(state.NewAuditEvent(state.AuditEvent{
		CustomerID: b.CustomerID,
		Actor:      state.SystemActor(),
		Action:     state.ActionWorkloadExpired,
		Outcome:    state.OutcomeObserved,
		TargetKind: state.TargetTenant,
		TargetID:   b.CustomerID,
		Detail:     state.AuditDetail{Status: "failed", BurstID: b.ID},
	})); auditErr != nil {
		h.Log.Warn("expired burst not journaled; cleanup already done", "burst", b.ID, "error", auditErr)
	}
}

// pushBurstAnnounce tells the agent about a burst that's being
// provisioned so the agent can hold the storage bindings ready for
// the burst's bootstrap fetch. With Tailscale, no peer-add step is
// needed — the burst joins via its ephemeral auth key and the agent
// is reachable by MagicDNS hostname.
func pushBurstAnnounce(agent *state.Agent, wl *workload.Workload, plan *Plan) error {
	body, err := burstAnnounceBody(wl, plan)
	if err != nil {
		return err
	}
	return enqueue(agent, protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       protocol.TypeBurstAnnounce,
		ID:         newID("cmd"),
		Timestamp:  time.Now().UTC(),
		Body:       body,
	})
}

func burstAnnounceBody(wl *workload.Workload, plan *Plan) ([]byte, error) {
	return json.Marshal(protocol.BurstAnnounce{
		BurstID:    plan.BurstID,
		TSHostname: plan.TSHostname,
		Tier:       plan.Tier,
		Namespace:  workloadNamespace(wl),
		Storage:    plan.StorageBindings,
	})
}

// workloadNamespace is the namespace central acts in for a workload:
// the spec's own, defaulted the way Kubernetes defaults it. Every
// command that names a namespace derives it here so the Job, its
// Secrets and the agent's storage-signing scope can't drift apart.
//
// This is a DERIVATION, not an authorization. Create pins
// wl.Metadata.Namespace to an authorized value before Plan ever sees
// the spec (see authorizeWorkloadNamespace); by the time anything calls
// this, the field is central's answer rather than the submitter's.
func workloadNamespace(wl *workload.Workload) string {
	if wl == nil || wl.Metadata.Namespace == "" {
		return defaultWorkloadNamespace
	}
	return wl.Metadata.Namespace
}

// defaultWorkloadNamespace is where an unqualified submission lands,
// and the whole authorized set for a tenant that has none configured.
// Matches how Kubernetes itself defaults an unqualified object.
const defaultWorkloadNamespace = "default"

// workloadNamespaceRule names the policy authorizeWorkloadNamespace applies, and
// the version stamps which shape of it decided a given submission. Both are
// recorded on the workload and in the journal so a reader can answer "what
// allowed this" from the evidence rather than from today's code — the rule will
// change, and a decision record that only says "allowed" cannot say against
// what. Bump the version when the rule's INPUTS or effect change, not when the
// tenant's namespace list does: the list is data the rule reads, and the journal
// records that separately.
const (
	workloadNamespaceRule        = "tenant.workload_namespaces"
	workloadNamespaceRuleVersion = "v1"
)

// authorizedWorkloadNamespaces is the set a tenant may submit into. It
// is deliberately NOT derived from the connector: the connector reports
// no namespace allowlist on the wire today, so there is nothing to
// trust there, and taking the submitter's word for it is the hole this
// closes. A tenant with none configured gets the fail-closed set.
func authorizedWorkloadNamespaces(cust *state.Customer) []string {
	if cust != nil && len(cust.WorkloadNamespaces) > 0 {
		return cust.WorkloadNamespaces
	}
	return []string{defaultWorkloadNamespace}
}

// errInvalidWorkloadNamespaces is what the operator routes answer 400 on. It
// separates "this request is malformed" from the provisioning conflicts the
// same call otherwise returns, which are 409.
var errInvalidWorkloadNamespaces = errors.New("invalid workload namespaces")

// dns1123Label matches a Kubernetes namespace name: RFC 1123 label syntax,
// which is also every namespace the API server will accept.
var dns1123Label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// maxNamespaceLen is the DNS-1123 label limit the API server enforces.
const maxNamespaceLen = 63

// validateWorkloadNamespaces canonicalises an operator-supplied list into the
// authorized set: non-empty, deduplicated, and every entry a namespace the API
// server would actually accept.
//
// Both halves of that matter, for different reasons. The list is a tenant's
// authorization boundary, so a duplicate or an empty entry is a set nobody
// meant to write — and "" would authorize the unqualified submission twice
// over while making the tenant's first namespace ambiguous. And the same list
// is interpolated into `--set rbac.allowedNamespaces={...}` in a command an
// operator pastes into a shell, so an entry carrying a comma is a second
// namespace nobody authorized and one carrying a quote, `$`, or `;` is a
// second command nobody typed. The DNS-1123 check refuses all of it by
// construction — every one of those characters is outside the label alphabet —
// which is why this runs BEFORE anything is persisted or rendered rather than
// leaving the render to escape its way out of bad input.
func validateWorkloadNamespaces(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("%w: at least one namespace is required", errInvalidWorkloadNamespaces)
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for i, ns := range in {
		switch {
		case ns == "":
			return nil, fmt.Errorf("%w: workload_namespaces[%d] is empty", errInvalidWorkloadNamespaces, i)
		case len(ns) > maxNamespaceLen:
			return nil, fmt.Errorf("%w: workload_namespaces[%d] %q is longer than %d characters",
				errInvalidWorkloadNamespaces, i, ns, maxNamespaceLen)
		case !dns1123Label.MatchString(ns):
			return nil, fmt.Errorf("%w: workload_namespaces[%d] %q is not a Kubernetes namespace name "+
				"(lowercase letters, digits and '-', starting and ending alphanumeric); "+
				"one entry per namespace — commas, quotes, spaces and shell syntax are not separators here",
				errInvalidWorkloadNamespaces, i, ns)
		case seen[ns]:
			return nil, fmt.Errorf("%w: workload_namespaces[%d] %q is listed twice", errInvalidWorkloadNamespaces, i, ns)
		}
		seen[ns] = true
		out = append(out, ns)
	}
	return out, nil
}

// authorizeWorkloadNamespace resolves the namespace central will act in
// for this submission, or an error naming what the tenant may use.
// An unqualified submission lands in the tenant's first namespace.
func authorizeWorkloadNamespace(cust *state.Customer, requested string) (string, error) {
	allowed := authorizedWorkloadNamespaces(cust)
	if requested == "" {
		return allowed[0], nil
	}
	for _, ns := range allowed {
		if ns == requested {
			return ns, nil
		}
	}
	return "", fmt.Errorf("metadata.namespace %q is not authorized for this account (authorized: %s); "+
		"ask your operator to add it to the account's workload namespaces and to the connector's rbac.allowedNamespaces",
		requested, strings.Join(allowed, ", "))
}

// pushCreateJob enqueues a CreateJob command to the agent.
func pushCreateJob(agent *state.Agent, workloadID string, wl *workload.Workload, plan *Plan) error {
	body, err := createJobBody(workloadID, wl, plan)
	if err != nil {
		return err
	}
	return enqueue(agent, protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       protocol.TypeCreateJob,
		ID:         newID("cmd"),
		Timestamp:  time.Now().UTC(),
		Body:       body,
	})
}

func createJobBody(workloadID string, wl *workload.Workload, plan *Plan) ([]byte, error) {
	return json.Marshal(protocol.CreateJob{
		WorkloadID: workloadID,
		Namespace:  workloadNamespace(wl),
		JobSpec:    plan.JobSpec,
	})
}

func workloadConnectorCommands(agent *state.Agent, workloadID string, wl *workload.Workload, plan *Plan) ([]state.ConnectorCommand, error) {
	if agent == nil || plan == nil {
		return nil, state.ErrInvalidConnectorCommand
	}
	now := time.Now().UTC()
	orderingKey := "burst:" + plan.BurstID
	announceSemantic := connectorCommandSemantic(protocol.TypeBurstAnnounce, plan.BurstID)
	announceID := state.StableConnectorCommandID(agent.CustomerID, agent.ClusterID, announceSemantic)
	announceBody, err := burstAnnounceBody(wl, plan)
	if err != nil {
		return nil, err
	}
	announce, err := state.NewConnectorCommand(agent.CustomerID, agent.ClusterID, announceSemantic,
		orderingKey, workloadID, plan.BurstID, "", protocol.Envelope{
			APIVersion: protocol.APIVersion, Type: protocol.TypeBurstAnnounce,
			ID: announceID, Timestamp: now, Body: announceBody,
		})
	if err != nil {
		return nil, err
	}
	commands := []state.ConnectorCommand{announce}
	if wl.Spec.NodeOnly {
		return commands, nil
	}
	createSemantic := connectorCommandSemantic(protocol.TypeCreateJob, workloadID)
	createID := state.StableConnectorCommandID(agent.CustomerID, agent.ClusterID, createSemantic)
	createBody, err := createJobBody(workloadID, wl, plan)
	if err != nil {
		return nil, err
	}
	create, err := state.NewConnectorCommand(agent.CustomerID, agent.ClusterID, createSemantic,
		orderingKey, workloadID, plan.BurstID, announceID, protocol.Envelope{
			APIVersion: protocol.APIVersion, Type: protocol.TypeCreateJob,
			ID: createID, Timestamp: now, Body: createBody,
		})
	if err != nil {
		return nil, err
	}
	return append(commands, create), nil
}

func enqueue(agent *state.Agent, env protocol.Envelope) error {
	return agent.Enqueue(env)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("workloads: crypto/rand read failed: " + err.Error())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}
