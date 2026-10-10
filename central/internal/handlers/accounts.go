// yscale:proprietary

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
	"gopkg.in/yaml.v3"
)

// accountStore is the slice of the state store this surface touches. Named as
// an interface, like the store's own persister seam, so the fail-closed path
// can be exercised against a store whose durable backend is refusing writes —
// otherwise a 503 that answers 401 is only caught in production.
// Tenant summaries are detached, credential-free durable reads rather than
// CustomerByID's live mutation pointer. Joined projections also bind the rows
// they render to the same snapshot as their liveness and authority checks.
type accountStore interface {
	UpsertAccount(issuer, subject string, p state.AccountProfile) (*state.Account, error)
	// AccountByIdentity is the management routes' entry point, and the
	// difference from UpsertAccount is deliberate: managing a roster is not a
	// sign-in, so a valid access token for a human who has never signed in must
	// not mint an account (nor a membership-shaped answer) on the way to being
	// told the tenant is not theirs.
	AccountByIdentity(issuer, subject string) (*state.Account, error)
	AccountTenantsContext(context.Context, string) ([]state.AccountTenant, error)
	// TenantRosterFor is one call rather than the membership lookup, the roster
	// read and the per-member account read it replaced: the authorization and
	// the rows it authorizes have to be one snapshot, or a revoke landing
	// between them renders a roster for a tenant that no longer exists.
	TenantRosterForContext(context.Context, string, string, state.RosterQuery) (state.TenantRoster, error)
	// AddTenantMemberByEmail and SetTenantMemberRole are the write half of the
	// roster, and they are the human methods rather than the operator ones for
	// the reason RemoveTenantMembership is: they take the caller's account id
	// and decide its authority under the SAME lock as the mutation. Calling the
	// operator methods here after checking a role in this handler would read the
	// authority one lock earlier than it acts, which is exactly the window an
	// admin racing their own demotion needs to mint an owner.
	AddTenantMemberByEmail(customerID, callerAccountID, issuer, email, role string, by state.Actor) (*state.TenantMembership, bool, error)
	SetTenantMemberRole(customerID, callerAccountID, targetAccountID, role string, by state.Actor) (*state.TenantMembership, string, error)
	RemoveTenantMembership(customerID, callerAccountID, targetAccountID string, by state.Actor) (*state.TenantMembership, error)
	// TenantUsageFor is the live burst snapshot behind the tenant usage route,
	// authorized and taken under one lock like the roster read — and readable by
	// every role, which is why it is separate from TenantRosterFor rather than a
	// field on it.
	TenantUsageFor(customerID, callerAccountID string) (state.TenantUsage, error)
	// TenantClustersFor is the durable cluster registry behind the tenant
	// cluster route — registry rows joined with this replica's live sockets —
	// authorized and taken under one lock for the same reason as the usage
	// read, and readable by every role for the same reason too: the ids it
	// returns are what a member's own submissions have to name.
	TenantClustersFor(customerID, callerAccountID string) ([]state.TenantCluster, error)
	TenantHostedCapacityFor(customerID, callerAccountID string) (state.TenantHostedCapacity, error)
	RequestTenantHostedCapacity(customerID, callerAccountID string, by state.Actor) (state.TenantHostedCapacity, bool, error)
	// The registry writes. Owner/admin only, decided by the store under the
	// same lock as the mutation; register and rotate hand back the plaintext
	// connector credential exactly once, and the store keeps only its hash.
	RegisterTenantCluster(customerID, callerAccountID, clusterID, name string, by state.Actor) (state.TenantCluster, string, string, error)
	RotateTenantClusterCredential(customerID, callerAccountID, clusterID string, by state.Actor) (state.TenantCluster, string, string, error)
	DeleteTenantCluster(customerID, callerAccountID, clusterID string, by state.Actor) (string, error)
	TenantClusterPolicyFor(customerID, callerAccountID string) (state.ClusterPolicy, string, error)
	SetTenantClusterPolicy(customerID, callerAccountID string, policy state.ClusterPolicy, by state.Actor) (state.ClusterPolicy, bool, string, error)
	// The launch catalog pair, authorized under one lock for the cluster-policy
	// pair's reasons. The read is every member's — a submission has to name an
	// id from it — and the replacement is the managers', decided by the store so
	// a refusal is journaled with the authority the write would have had.
	TenantTemplateCatalogFor(customerID, callerAccountID string) (state.TenantTemplateCatalogView, error)
	SetTenantTemplateCatalog(customerID, callerAccountID string, catalog state.WorkloadTemplateCatalog, by state.Actor) (state.TenantTemplateCatalogView, error)
	SetTenantTemplateCatalogIfRevision(customerID, callerAccountID, expectedRevision string, catalog state.WorkloadTemplateCatalog, by state.Actor) (state.TenantTemplateCatalogView, error)
	CatalogPublishersFor(customerID, callerAccountID string) ([]state.CatalogPublisherSummary, string, error)
	CreateCatalogPublisher(customerID, callerAccountID, name string) (state.CatalogPublisherSummary, string, string, error)
	RotateCatalogPublisherCredential(customerID, callerAccountID, publisherID string) (state.CatalogPublisherSummary, string, string, error)
	DeleteCatalogPublisher(customerID, callerAccountID, publisherID string) (string, error)
	AutomationTenantTemplateCatalogFor(customerID, publisherID string) (state.TenantTemplateCatalogView, error)
	SetAutomationTenantTemplateCatalogIfRevision(customerID, publisherID, expectedRevision string, catalog state.WorkloadTemplateCatalog) (state.TenantTemplateCatalogView, error)

	// The GitOps source registry pair, authorized under one lock for the launch
	// catalog's reasons. Only the conditional write is here: this registry has no
	// internal caller, so an unconditional replacement would be a way for a
	// browser client to skip the stale-write check rather than a seam anything
	// needs.
	TenantGitOpsSourcesFor(customerID, callerAccountID string) (state.TenantGitOpsSourcesView, error)
	SetTenantGitOpsSourcesIfRevision(customerID, callerAccountID, expectedRevision string, sources []state.GitOpsSource, by state.Actor) (state.TenantGitOpsSourcesView, error)

	MembershipFor(accountID, customerID string) (*state.TenantMembership, error)
	TenantSummaryByIDContext(context.Context, string) (state.TenantSummary, error)
	LinodeCloudAccount(customerID string) (*state.CloudAccount, error)
	TenantLinodeCloudAccountFor(customerID, callerAccountID string) (*state.CloudAccountSummary, string, error)
	SetLinodeCloudAccount(customerID string, next state.CloudAccount, by state.Actor) (*state.CloudAccount, bool, error)
	DisconnectLinodeCloudAccount(customerID, accountID string, now time.Time, by state.Actor) error
	RuntimeBindingsFor(customerID, callerAccountID, syncState string) ([]state.RuntimeBindingSummary, string, error)
	RuntimeBindingIDForKey(customerID, key string) (string, error)
	RuntimeBindingRows(customerID string) ([]*state.RuntimeBinding, error)
	SetRuntimeBinding(customerID, callerAccountID string, next state.RuntimeBinding) (state.RuntimeBindingSummary, string, error)
	DeleteRuntimeBinding(customerID, callerAccountID, key string) (string, string, error)
	CustomerByID(id string) (*state.Customer, error)
	AgentsForCustomer(customerID string) []*state.Agent
	// CreateFirstTenant backs self-service signup: the zero-membership check,
	// tenant row, and owner grant land as one durable operation.
	CreateFirstTenant(c *state.Customer, ownerAccountID string) (*state.Customer, *state.TenantMembership, error)
	WorkloadsForCustomer(customerID string, limit int) []*state.Workload
	// Cancellation commits the current authorization, journal and recoverable
	// cleanup request together before handing off to the provider lifecycle.
	RequestWorkloadCancellation(context.Context, string, string, state.WorkloadCancelPrincipal) (state.WorkloadCancellation, error)
	PrepareWorkloadRetry(context.Context, string, string, string) (state.WorkloadRetryPreparation, error)
	ReserveWorkloadRetry(context.Context, *state.WorkloadRetryApproval, string, int64) (string, *state.AuditEvent, error)
	AuthorizeWorkloadRead(customerID, accountID, workloadID string) (state.WorkloadReadDecision, error)
	// AppendAudit is the journal write for a decision with no state change to
	// ride along with — a refusal or an independent observation. Its error is
	// what makes the human surface fail closed: a decision central could not
	// record is a decision it does not act on.
	AppendAudit(ev *state.AuditEvent) error
	TenantAuditFor(ctx context.Context, customerID, callerAccountID string, q state.AuditQuery) (state.AuditPage, error)
}

// Accounts serves the SaaS-only human account surface. It is the ONLY route
// family authenticated by a Yscale ID access token; every other route on this
// central authenticates a cluster with Customer.Token, and the two credential
// types are never interchangeable — a cluster token presented here resolves to
// no identity, and an access token presented to /v1/workloads authenticates
// nothing.
//
// Multi-tenancy is an enterprise feature, so this handler is wired only by the
// proprietary route hook (central/cmd/yscale-cloud/enterprise.go).
type Accounts struct {
	Store accountStore
	// Resolver validates the presented human access token. nil means Yscale ID
	// is not configured for this deployment, which DISABLES the route (404) the
	// same way an unset YSCALE_ADMIN_TOKEN disables the admin routes.
	Resolver IdentityResolver
	// Issuer is the identity provider's issuer URL (YSCALE_ID_ISSUER). It is
	// half of an account's durable identity key, so an unset issuer is a
	// misconfiguration rather than a default: the route refuses to mint
	// accounts it could not key correctly.
	Issuer string
	Log    *slog.Logger
	// Workloads is the existing cluster-token workload lifecycle. Tenant routes
	// authenticate a human, inject the authorized tenant into its request
	// context, then delegate to Create/Get/Cancel rather than reproducing them.
	Workloads *Workloads
	// WorkloadLogTimeout bounds the request/response hop over the connected
	// cluster agent. Zero uses the production default; tests set a short value.
	WorkloadLogTimeout time.Duration
	// Endpoint is central's URL for the connector install command the cluster
	// registration response renders (YSCALE_CENTRAL_ENDPOINT — the Tenants
	// handler's field, wired from the same env). Empty omits the endpoint
	// flag, leaving the chart default.
	Endpoint string
	// AllowSelfServiceTenants enables POST /v1/account/tenants — first-run SaaS
	// onboarding, where a signed-in human with no tenant creates their own
	// workspace. Wired ONLY from the exact env YSCALE_SELF_SERVICE_TENANTS=true;
	// false (the default) makes the route act absent (404), the same way an
	// unset Resolver disables the whole surface — self-provisioning is a
	// deliberate opt-in, not something a deployment gets by upgrading.
	AllowSelfServiceTenants bool
	// Reconciler is the coordination-policy seam the cluster DELETE drives: a
	// removed cluster's routes have to leave the tenant's policy and its own
	// gateway node has to give the approvals back, and neither happens on an
	// agent event because the connector is gone by then. nil (the OSS build, and
	// every test that does not care) simply skips the enqueue — the durable
	// intent is already correct, so the worst case is a stale policy until the
	// next reconcile, not a wrong one.
	Reconciler PolicyReconciler
	// CredentialCipher and ValidateLinodeAccount are enterprise-only seams.
	// A nil cipher disables connect while preserving safe reads/disconnects.
	CredentialCipher      CredentialCipher
	ValidateLinodeAccount func(context.Context, string, string) (string, error)
}

// retryAfterSeconds is the Retry-After sent with a locally-shed lookup. One
// second: the bucket refills continuously, so the next attempt is worth making
// almost immediately — the header exists to stop a client busy-looping, not to
// park it.
const retryAfterSeconds = "1"

// AccountResponse is what a signed-in human sees about themselves. It is
// deliberately narrow: the account's own profile plus a summary of the tenants
// they are a member of. Customer.Token, mesh/coordination-server internals, the
// upstream access token, and any tenant they have no membership in are all
// absent by construction, not by filtering.
type AccountResponse struct {
	AccountID     string                    `json:"account_id"`
	Issuer        string                    `json:"issuer"`
	Subject       string                    `json:"subject"`
	Email         string                    `json:"email,omitempty"`
	EmailVerified bool                      `json:"email_verified"`
	Name          string                    `json:"name,omitempty"`
	CreatedAt     time.Time                 `json:"created_at"`
	Tenants       []TenantMembershipSummary `json:"tenants"`
}

// TenantMembershipSummary summarises one tenant this human belongs to.
type TenantMembershipSummary struct {
	CustomerID string `json:"customer_id"`
	// Name is the tenant's display name. Additive and omitted when empty —
	// operator-provisioned tenants and everything that predates the field have
	// none, and a console must keep rendering those by id.
	Name   string       `json:"name,omitempty"`
	Role   string       `json:"role"`
	Plan   string       `json:"plan"`
	Limits TenantLimits `json:"limits"`
	// WorkloadNamespaces is the EFFECTIVE set this tenant may submit into,
	// in configured order — never the raw stored list. A tenant with none
	// configured reports the fail-closed set, because that is what its
	// submissions are actually judged against; reporting an empty list here
	// would tell a console the tenant may submit nowhere, when in fact it
	// may submit to "default".
	WorkloadNamespaces []string `json:"workload_namespaces"`
}

// TenantWorkloadsResponse is the stable list envelope used by the SaaS console.
type TenantWorkloadsResponse struct {
	Workloads []TenantWorkload `json:"workloads"`
}

// TenantWorkload is the deliberately narrow human-facing workload record.
// Customer credentials, mesh state, provider data, and unrelated tenants are
// absent by construction.
type TenantWorkload struct {
	ID         string             `json:"id"`
	ClusterID  string             `json:"cluster_id,omitempty"`
	Status     string             `json:"status"`
	CreatedAt  time.Time          `json:"created_at"`
	StartedAt  *time.Time         `json:"started_at"`
	FinishedAt *time.Time         `json:"finished_at"`
	BurstID    string             `json:"burst_id"`
	RetryOf    string             `json:"retry_of,omitempty"`
	Spec       TenantWorkloadSpec `json:"spec"`
	// SubmittedBy and Authorization answer the two questions a console cannot
	// answer from the spec: who submitted this, and what let them. Both are
	// omitted on a record that predates provenance, which is why they are
	// pointers — an empty actor rendered as an object would read as "submitted
	// by nobody" rather than "not recorded".
	SubmittedBy   *TenantWorkloadActor         `json:"submitted_by,omitempty"`
	Authorization *TenantWorkloadAuthorization `json:"authorization,omitempty"`
	Placement     *PlacementResponse           `json:"placement,omitempty"`
	// Template is the catalog entry this run was launched from. Omitted — not
	// zeroed — on a submission that named none: the raw API, a connector, and
	// every record that predates the catalog. A zero object here would read as
	// "launched from a template with no id", which is a claim nobody made.
	Template *state.TemplateRef `json:"template,omitempty"`
	// Cost is the rate-times-runtime estimate frozen when teardown was accepted
	// by the durable queue or completed inline.
	// Omitted — not zeroed — on a workload that has no observation: one still
	// running, one whose reap failed, or one that predates the record. A console
	// showing $0.00 for those would be stating a measurement nobody took, and
	// this is a history, not a bill.
	Cost *TenantWorkloadCost `json:"cost,omitempty"`
	// SpentUSD is central's live rate-times-age estimate while the backing burst
	// exists. It is distinct from the immutable terminal Cost receipt and is
	// omitted when the workload is terminal or the stored inputs are invalid.
	SpentUSD *float64 `json:"spent_usd,omitempty"`
	// GPUUtilPercent and LastHeartbeatAt are one validated telemetry observation.
	// Both are omitted together when no trustworthy sample exists; a pointer
	// preserves an observed idle GPU (0%) as distinct from no observation.
	GPUUtilPercent  *float64   `json:"gpu_util_percent,omitempty"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`
	// Outcome is the agent's receipt for the terminal observation: what the
	// compute did, and separately what the artifact export did. Omitted on a run
	// still going, on one whose agent reported only the phase, and on every
	// record that predates the receipt — status already says what happened at
	// the coarse level, and inventing a receipt to match it would state an
	// observation nobody made.
	// OutcomeResponse and its constructor live beside PlacementResponse in
	// workloads.go: both read surfaces render the same stored receipt.
	Outcome *OutcomeResponse `json:"outcome,omitempty"`
	// SubmissionOrigin is which path submitted this run — "api" for the CLI, the
	// console and a customer's own automation, "pending-pod" and "workload-cr"
	// for the two connector controllers. Omitted, not defaulted, on a record that
	// predates the field: "api" there would state that a person submitted a run
	// nobody can say that about, and the whole point of the field is to tell a
	// controller-triggered burst from one somebody asked for.
	SubmissionOrigin protocol.SubmissionOrigin `json:"submission_origin,omitempty"`
	// NodeObservation is the last connector-observed Kubernetes Node snapshot.
	// Omitted until a connector reports a persistable phase for this workload's
	// burst, so a record that predates the feature or one whose burst has not
	// registered says nothing rather than inventing an observation nobody made.
	NodeObservation *NodeObservationResponse `json:"node_observation,omitempty"`
	PodObservation  *PodObservationResponse  `json:"pod_observation,omitempty"`
	GPUObservation  *GPUObservationResponse  `json:"gpu_observation,omitempty"`
	// Cleanup is the authoritative lifecycle cleanup summary for this burst.
	// Omitted when the optional batch reader is absent, when no provider-delete
	// record exists for this burst, or when the batch read errors — the list
	// stays available and never fabricates state.
	Cleanup *CleanupSummary `json:"cleanup,omitempty"`
}

// TenantWorkloadActor is the submitter as a co-member may see them: the kind of
// principal, and — for a human — the account id the roster already shows them
// under. The identity provider's issuer and subject are absent by construction,
// exactly as they are on TenantMemberSummary, and a cluster credential has no
// token field to omit because Actor has never carried one.
type TenantWorkloadActor struct {
	Kind      string `json:"kind"`
	AccountID string `json:"account_id,omitempty"`
}

// TenantWorkloadAuthorization is the decision record: what namespace was asked
// for, what central granted, and the rule and version that said so.
type TenantWorkloadAuthorization struct {
	RequestedNamespace string    `json:"requested_namespace,omitempty"`
	GrantedNamespace   string    `json:"granted_namespace,omitempty"`
	Rule               string    `json:"rule,omitempty"`
	RuleVersion        string    `json:"rule_version,omitempty"`
	Role               string    `json:"role,omitempty"`
	DecidedAt          time.Time `json:"decided_at"`
}

func tenantWorkloadActor(a *state.Actor) *TenantWorkloadActor {
	if a == nil {
		return nil
	}
	return &TenantWorkloadActor{Kind: a.Kind, AccountID: a.AccountID}
}

func tenantWorkloadAuthorization(d *state.WorkloadAuthorization) *TenantWorkloadAuthorization {
	if d == nil {
		return nil
	}
	return &TenantWorkloadAuthorization{
		RequestedNamespace: d.RequestedNamespace,
		GrantedNamespace:   d.GrantedNamespace,
		Rule:               d.Rule,
		RuleVersion:        d.RuleVersion,
		Role:               d.Role,
		DecidedAt:          d.DecidedAt,
	}
}

// TenantWorkloadSpec is a safe summary of the validated workload document.
// Command arguments, environment values, storage endpoints, secret references,
// labels, and tags are omitted because arbitrary user input there may contain
// secrets. Execution reports shape only, never the submitted values.
type TenantWorkloadSpec struct {
	APIVersion string                 `json:"apiVersion"`
	Kind       string                 `json:"kind"`
	Metadata   TenantWorkloadMetadata `json:"metadata"`
	Spec       TenantWorkloadSpecBody `json:"spec"`
}

type TenantWorkloadMetadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

type TenantWorkloadSpecBody struct {
	Image       string                    `json:"image,omitempty"`
	Execution   *TenantWorkloadExecution  `json:"execution,omitempty"`
	Replicas    int32                     `json:"replicas,omitempty"`
	NodeOnly    bool                      `json:"nodeOnly,omitempty"`
	Backend     string                    `json:"backend,omitempty"`
	Region      string                    `json:"region,omitempty"`
	Networking  *TenantWorkloadNetworking `json:"networking,omitempty"`
	Machine     *TenantWorkloadMachine    `json:"machine,omitempty"`
	Size        string                    `json:"size,omitempty"`
	CPU         string                    `json:"cpu,omitempty"`
	Memory      string                    `json:"memory,omitempty"`
	GPU         *TenantWorkloadGPU        `json:"gpu,omitempty"`
	Budget      *TenantWorkloadBudget     `json:"budget,omitempty"`
	Retries     int32                     `json:"retries,omitempty"`
	ModelVolume string                    `json:"modelVolume,omitempty"`
	Data        *TenantWorkloadData       `json:"data,omitempty"`
}

// TenantWorkloadExecution proves whether the image default or a custom recipe
// was submitted without echoing arbitrary command or argument values back to
// every member who may read tenant history.
type TenantWorkloadExecution struct {
	Mode           string `json:"mode"`
	CommandTokens  int    `json:"command_tokens"`
	ArgumentTokens int    `json:"argument_tokens"`
}

// TenantWorkloadData is a deliberately coarse receipt. Storage names,
// mount paths, buckets, prefixes, endpoints, and Secret references are all
// submitter-controlled strings and may contain sensitive data, so co-members
// see only fixed transfer classes and numeric capacity totals.
type TenantWorkloadData struct {
	PullBeforeRunInputs int `json:"pull_before_run_inputs"`
	InputSizeHintGB     int `json:"input_size_hint_gb,omitempty"`
	PushAfterRunOutputs int `json:"push_after_run_outputs"`
	OutputMaxUploadGB   int `json:"output_max_upload_gb,omitempty"`
}

type TenantWorkloadNetworking struct {
	Tier string `json:"tier,omitempty"`
}

type TenantWorkloadMachine struct {
	FlyType string `json:"flyType,omitempty"`
	Region  string `json:"region,omitempty"`
}

type TenantWorkloadGPU struct {
	Kind         string  `json:"kind,omitempty"`
	Count        int     `json:"count,omitempty"`
	Reliability  string  `json:"reliability,omitempty"`
	MaxHourlyUSD float64 `json:"maxHourlyUSD,omitempty"`
}

type TenantWorkloadBudget struct {
	MaxUSD   float64 `json:"maxUSD,omitempty"`
	Deadline string  `json:"deadline,omitempty"`
}

const tenantWorkloadListLimit = 100

// tenantLiveBurstReader is optional so account-store fakes and deployments
// without the live burst index retain the stable history response.
type tenantLiveBurstReader interface {
	BurstsForCustomer(customerID string) []*state.Burst
}

// The production Store reads workload and burst rows together from its durable
// backend. The old interface remains for lightweight account-store fakes.
type tenantWorkloadSnapshotReader interface {
	WorkloadSnapshotsForCustomer(context.Context, string, int) ([]state.WorkloadSnapshot, error)
}

// HandleListWorkloads serves GET /v1/tenants/{tenant_id}/workloads.
func (a *Accounts) HandleListWorkloads(w http.ResponseWriter, r *http.Request) {
	_, _, ok := a.tenantWorkloadRequest(w, r)
	if !ok {
		return
	}

	tenantID := r.PathValue("tenant_id")
	var records []*state.Workload
	liveBursts := make(map[string]*state.Burst)
	if reader, ok := a.Store.(tenantWorkloadSnapshotReader); ok {
		snapshots, err := reader.WorkloadSnapshotsForCustomer(r.Context(), tenantID, tenantWorkloadListLimit)
		if err != nil {
			if a.Log != nil {
				a.Log.Warn("account: durable workload read failed", "tenant", tenantID, "error", err)
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "workload store unavailable"})
			return
		}
		for _, snapshot := range snapshots {
			records = append(records, snapshot.Workload)
			if snapshot.Burst != nil {
				liveBursts[snapshot.Burst.ID] = snapshot.Burst
			}
		}
	} else {
		records = a.Store.WorkloadsForCustomer(tenantID, tenantWorkloadListLimit)
		if reader, ok := a.Store.(tenantLiveBurstReader); ok {
			for _, burst := range reader.BurstsForCustomer(tenantID) {
				if burst != nil {
					liveBursts[burst.ID] = burst
				}
			}
		}
	}
	now := time.Now()
	response := TenantWorkloadsResponse{Workloads: make([]TenantWorkload, 0, len(records))}
	for _, record := range records {
		spec, err := tenantWorkloadSpec(record.SpecYAML)
		if err != nil {
			a.Log.Warn("account: skipping unreadable stored workload spec", "workload", record.ID, "error", err)
			continue
		}
		workload := TenantWorkload{
			ID:               record.ID,
			ClusterID:        record.ClusterID,
			Status:           record.Status,
			CreatedAt:        record.CreatedAt,
			StartedAt:        record.StartedAt,
			FinishedAt:       record.FinishedAt,
			BurstID:          record.BurstID,
			RetryOf:          record.RetryOfWorkloadID,
			Spec:             spec,
			SubmittedBy:      tenantWorkloadActor(record.SubmittedBy),
			Authorization:    tenantWorkloadAuthorization(record.Authorization),
			Placement:        storedPlacementResponse(record.Placement),
			Template:         record.TemplateRef,
			Cost:             tenantWorkloadCost(record.Cost),
			Outcome:          storedOutcomeResponse(record.Outcome),
			SubmissionOrigin: record.SubmissionOrigin,
			NodeObservation:  nodeObservationResponse(record.NodeObservation),
			PodObservation:   podObservationResponse(record.PodObservation),
			GPUObservation:   gpuObservationResponse(record.GPUObservation),
		}
		// Compute completion does not end provider cost. The live booking stays
		// until confirmed deletion; a frozen receipt always takes precedence.
		if record.Cost == nil {
			if burst := liveBursts[record.BurstID]; burst != nil && burst.TerminalCost == nil {
				if spent, ok := liveBurstSpendUSD(burst, now); ok {
					workload.SpentUSD = &spent
				}
				workload.GPUUtilPercent, workload.LastHeartbeatAt = liveBurstGPUTelemetry(burst)
			}
		}
		response.Workloads = append(response.Workloads, workload)
	}

	if getter, ok := cleanupSummaryGetter(a.Workloads); ok && len(response.Workloads) > 0 {
		refs := make([]lifecycle.ProviderDeleteSummaryRef, 0, len(response.Workloads))
		for i := range response.Workloads {
			if response.Workloads[i].ClusterID != "" && response.Workloads[i].BurstID != "" {
				refs = append(refs, lifecycle.ProviderDeleteSummaryRef{
					ClusterID: response.Workloads[i].ClusterID,
					BurstID:   response.Workloads[i].BurstID,
				})
			}
		}
		if len(refs) > 0 {
			summaries, err := getter.GetProviderDeleteSummaries(r.Context(), tenantID, refs)
			if err != nil {
				if a.Log != nil {
					a.Log.Warn("account: batch cleanup summary read failed; omitting cleanup from list", "tenant", tenantID, "error", err)
				}
			} else {
				for i := range response.Workloads {
					ref := lifecycle.ProviderDeleteSummaryRef{
						ClusterID: response.Workloads[i].ClusterID,
						BurstID:   response.Workloads[i].BurstID,
					}
					if s, found := summaries[ref]; found {
						response.Workloads[i].Cleanup = &CleanupSummary{
							State:     s.State,
							DeletedAt: s.DeletedAt,
						}
						if providerAbsenceConfirmed(s.State, s.DeletedAt) {
							response.Workloads[i].SpentUSD = nil
						}
					}
				}
			}
		}
	}

	writeJSON(w, http.StatusOK, response)
}

func cleanupSummaryGetter(workloads *Workloads) (CleanupSummaryGetter, bool) {
	if workloads == nil {
		return nil, false
	}
	getter, ok := workloads.Deletes.(CleanupSummaryGetter)
	return getter, ok
}

// HandleCreateWorkload serves POST /v1/tenants/{tenant_id}/workloads by
// delegating to the existing workload lifecycle after human authorization.
//
// The submitter rides the request context beside the tenant, so the lifecycle
// path stamps provenance from the credential that was actually authenticated
// rather than re-deriving it. Create's own default is the cluster credential —
// the OSS path, where no human is involved — so this is the only place a human
// submission is named as one.
//
// A viewer is refused HERE rather than in the shared lookup, and that is the
// point: their refusal is an authenticated authorization decision, exactly like
// the member's refused cancel below and the unauthorized-namespace refusal in
// Create, so it is journaled on the same terms — written before it is answered,
// and answered as retryable when it cannot be written. Refusing them in the
// lookup meant the one denial nobody could produce evidence for was the flattest
// one: a read-only account trying to submit.
func (a *Accounts) HandleCreateWorkload(w http.ResponseWriter, r *http.Request) {
	r, caller, ok := a.tenantWorkloadRequest(w, r)
	if !ok {
		return
	}
	if caller.membership.Role == state.RoleViewer {
		tenantID := caller.membership.CustomerID
		if err := a.Store.AppendAudit(state.NewAuditEvent(state.AuditEvent{
			CustomerID: tenantID,
			Actor:      state.HumanActor(caller.account.ID, tenantID),
			Action:     state.ActionWorkloadSubmit,
			Outcome:    state.OutcomeDenied,
			TargetKind: state.TargetTenant,
			TargetID:   tenantID,
			Detail: state.AuditDetail{
				Reason: state.ReasonRoleReadOnly,
				Role:   caller.membership.Role,
			},
		})); err != nil {
			a.Log.Error("account: audit workload submit denial", "tenant", tenantID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "audit store unavailable"})
			return
		}
		// Nothing was planned and nothing provisioned: the refusal is decided from
		// the role alone, before the lifecycle path is entered at all.
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: workload changes require owner, admin, or member"})
		return
	}
	a.Workloads.Create(w, r.WithContext(withSubmitter(r.Context(), submitter{
		Actor: state.HumanActor(caller.account.ID, caller.membership.CustomerID),
		Role:  caller.membership.Role,
	})))
}

// HandlePreviewPlacement serves POST /v1/tenants/{tenant_id}/placement-preview:
// the decision a launch of this document would make right now, without making
// it.
//
// Same credential, same membership lookup and same tenant scoping as every
// other route on this surface, and it delegates to the SAME lifecycle path a
// launch takes — so a preview cannot answer from a second implementation.
//
// Every role may preview, including a viewer. Unlike a submit, this decides
// nothing and spends nothing: it reads the tenant's own routing, catalog and
// account state, all of which a read-only seat can already see on the workload
// list and the price endpoint. Refusing it would take a console's "what would
// this cost" away from exactly the seat most likely to ask.
func (a *Accounts) HandlePreviewPlacement(w http.ResponseWriter, r *http.Request) {
	r, _, ok := a.tenantWorkloadRequest(w, r)
	if !ok {
		return
	}
	a.Workloads.PreviewPlacement(w, r)
}

// HandleGetWorkload serves GET /v1/tenants/{tenant_id}/workloads/{id} through
// the existing owner-checking workload path.
func (a *Accounts) HandleGetWorkload(w http.ResponseWriter, r *http.Request) {
	r, _, ok := a.tenantWorkloadRequest(w, r)
	if !ok {
		return
	}
	a.Workloads.Get(w, r)
}

const (
	defaultTenantLogTail      int64 = 200
	maxTenantLogTail          int64 = 1000
	maxTenantLogBytes         int64 = 256 << 10
	maxTenantLogResultBytes         = 2 << 20
	defaultWorkloadLogTimeout       = 12 * time.Second
)

func (a *Accounts) workloadLogTimeout() time.Duration {
	if a.WorkloadLogTimeout > 0 {
		return a.WorkloadLogTimeout
	}
	return defaultWorkloadLogTimeout
}

func tenantLogTail(r *http.Request) (int64, bool) {
	query := r.URL.Query()
	for key := range query {
		if key != "tail" {
			return 0, false
		}
	}
	values := query["tail"]
	if len(values) == 0 {
		return defaultTenantLogTail, true
	}
	if len(values) != 1 {
		return 0, false
	}
	tail, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || tail < 1 || tail > maxTenantLogTail {
		return 0, false
	}
	return tail, true
}

// HandleGetWorkloadLogs serves a bounded snapshot through the connector that
// owns the stored workload's cluster. Browser input can select only the tail
// length; namespace, cluster, pod and container all come from stored state or
// the connector's Kubernetes lookup.
func (a *Accounts) HandleGetWorkloadLogs(w http.ResponseWriter, r *http.Request) {
	_, caller, ok := a.tenantWorkloadRequest(w, r)
	if !ok {
		return
	}
	if r.ContentLength > 0 || len(r.Header.Values("X-Cluster-ID")) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "workload log snapshots do not accept a body or cluster selector"})
		return
	}
	tail, ok := tenantLogTail(r)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tail must be one integer from 1 to 1000"})
		return
	}
	tenantID, workloadID := caller.membership.CustomerID, r.PathValue("id")
	decision, err := a.Store.AuthorizeWorkloadRead(tenantID, caller.account.ID, workloadID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			tenantNotFound(w)
			return
		}
		a.Log.Error("account: authorize workload logs", "tenant", tenantID, "workload", workloadID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	if err := a.Store.AppendAudit(state.NewAuditEvent(state.AuditEvent{
		CustomerID: tenantID,
		Actor:      state.HumanActor(caller.account.ID, tenantID),
		Action:     state.ActionWorkloadLogs,
		Outcome:    state.OutcomeAccepted,
		TargetKind: state.TargetWorkload,
		TargetID:   workloadID,
		Detail: state.AuditDetail{
			Reason:  state.ReasonRoleAuthorized,
			Role:    decision.Role,
			BurstID: decision.Source.BurstID,
		},
	})); err != nil {
		a.Log.Error("account: audit workload logs", "tenant", tenantID, "workload", workloadID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "audit store unavailable"})
		return
	}

	var parsed workload.Workload
	if err := yaml.Unmarshal(decision.Source.SpecYAML, &parsed); err != nil {
		a.Log.Error("account: decode workload for logs", "tenant", tenantID, "workload", workloadID, "error", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "workload log snapshot unavailable"})
		return
	}
	agent, err := agentForWorkload(a.Workloads.Store, &decision.Source)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "cluster connector is offline"})
		return
	}
	body, err := json.Marshal(protocol.FetchWorkloadLogs{
		WorkloadID: workloadID,
		Namespace:  workloadNamespace(&parsed),
		TailLines:  tail,
		MaxBytes:   maxTenantLogBytes,
	})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "workload log snapshot unavailable"})
		return
	}
	commandID := newID("cmd")
	ackCh, unregister := agent.RegisterCommandAck(commandID)
	defer unregister()
	if err := enqueue(agent, protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       protocol.TypeFetchWorkloadLogs,
		ID:         commandID,
		Timestamp:  time.Now().UTC(),
		Body:       body,
	}); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "cluster connector is offline"})
		return
	}

	timer := time.NewTimer(a.workloadLogTimeout())
	defer timer.Stop()
	select {
	case ack := <-ackCh:
		if !ack.Success {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "cluster connector could not read workload logs"})
			return
		}
		if len(ack.Result) == 0 || len(ack.Result) > maxTenantLogResultBytes {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "cluster connector returned an invalid log snapshot"})
			return
		}
		var result protocol.WorkloadLogs
		if err := json.Unmarshal(ack.Result, &result); err != nil || len(result.Streams) > 128 {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "cluster connector returned an invalid log snapshot"})
			return
		}
		var total int64
		for _, stream := range result.Streams {
			total += int64(len(stream.Output))
			if stream.Pod == "" || stream.Container == "" || total > maxTenantLogBytes {
				writeJSON(w, http.StatusBadGateway, map[string]string{"error": "cluster connector returned an invalid log snapshot"})
				return
			}
		}
		writeJSON(w, http.StatusOK, result)
	case <-r.Context().Done():
		if errors.Is(r.Context().Err(), context.DeadlineExceeded) {
			writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "workload log snapshot timed out"})
		}
	case <-timer.C:
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "workload log snapshot timed out"})
	}
}

// HandleCancelWorkload serves DELETE /v1/tenants/{tenant_id}/workloads/{id}.
//
// This is the one place the human surface is NARROWER than the cluster
// credential, and deliberately so: a member may cancel a workload they
// submitted and no other, while an owner or admin may cancel any of their
// tenant's. Current tenant liveness, membership, submitter and resource binding
// are checked in the same state transaction as the accepted/denied audit and
// the recoverable cancellation request. An audit failure rolls back the action.
// Provider deletion runs after commit; the audit proves authorization, not
// physical absence. A viewer's refusal uses this same journaled decision path.
func (a *Accounts) HandleCancelWorkload(w http.ResponseWriter, r *http.Request) {
	r, caller, ok := a.tenantWorkloadRequest(w, r)
	if !ok {
		return
	}
	a.Workloads.cancelWorkload(w, r, caller.account.ID, a.Store)
}

// HandleRetryWorkload serves POST /v1/tenants/{tenant_id}/workloads/{id}/retry.
func (a *Accounts) HandleRetryWorkload(w http.ResponseWriter, r *http.Request) {
	if len(r.Header.Values(clusterIDHeader)) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-Cluster-ID is not accepted on workload retry"})
		return
	}
	// A retry re-runs a stored submission, so its template reference is the
	// stored one. A caller-supplied pair would let a retry re-attribute a run to
	// a template it never came from, under an id whose history says otherwise.
	if len(r.Header.Values(templateIDHeader)) != 0 || len(r.Header.Values(templateVersionHeader)) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "template headers are not accepted on workload retry"})
		return
	}
	key, err := validateSingleIdempotencyKey(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid Idempotency-Key"})
		return
	}
	if !emptyRequestBody(w, r) {
		return
	}

	r, caller, ok := a.tenantWorkloadRequest(w, r)
	if !ok {
		return
	}
	tenantID, sourceID := caller.membership.CustomerID, r.PathValue("id")
	prepared, err := a.Store.PrepareWorkloadRetry(r.Context(), tenantID, caller.account.ID, sourceID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			tenantNotFound(w)
			return
		}
		a.Log.Error("account: authorize workload retry", "tenant", tenantID, "workload", sourceID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}

	decision := prepared.Decision
	if !decision.Allowed {
		if decision.Reason == state.ReasonWorkloadNotTerminal {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "source workload must be terminal before it can be retried"})
			return
		}
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error": "forbidden: retrying this workload requires owner or admin, or the member who submitted it",
		})
		return
	}
	if prepared.Approval == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "retry preparation unavailable"})
		return
	}

	nextContext := withRetryOfWorkload(r.Context(), sourceID)
	nextContext = withTemplateRef(nextContext, decision.Source.TemplateRef)
	// The preparation's policy and source travel together. Admission rechecks
	// both and journals acceptance before any provider side effect.
	nextContext = context.WithValue(nextContext, ctxCustomer, tenantWorkloadCustomer(prepared.Tenant))
	nextContext = context.WithValue(nextContext, retryAdmissionKey{}, retryAdmission{store: a.Store, approval: prepared.Approval})
	next := r.Clone(nextContext)
	next.Method = http.MethodPost
	next.Body = io.NopCloser(bytes.NewReader(decision.Source.SpecYAML))
	next.ContentLength = int64(len(decision.Source.SpecYAML))
	next.Header = r.Header.Clone()
	next.Header.Del(clusterIDHeader)
	next.Header.Set(idempotencyHeader, key)
	// The template the source run was launched from travels in private request
	// context, not a caller-controlled header. It is the exact stored reference,
	// including its catalog revision, so retry provenance does not move when the
	// tenant later edits or retires that template.
	next.Header.Del(templateIDHeader)
	next.Header.Del(templateVersionHeader)
	if decision.Source.Placement != nil &&
		decision.Source.Placement.Mode == state.ClusterPlacementModePinned &&
		decision.Source.Placement.RequestedClusterID != "" {
		next.Header.Set(clusterIDHeader, decision.Source.Placement.RequestedClusterID)
	}
	a.Workloads.Create(w, next.WithContext(withSubmitter(next.Context(), submitter{
		Actor: state.HumanActor(caller.account.ID, tenantID),
		Role:  decision.Role,
	})))
}

func emptyRequestBody(w http.ResponseWriter, r *http.Request) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body is not accepted"})
			return false
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "could not read request body"})
		return false
	}
	if len(body) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body is not accepted"})
		return false
	}
	return true
}

// tenantCaller is the authenticated human behind a tenant-scoped request, with
// the grant that got them in. Both come from the same front-half lookup, so a
// route needing either has the other without a second read.
type tenantCaller struct {
	account    *state.Account
	membership *state.TenantMembership
}

// tenantWorkloadRequest is the front half every tenant workload route shares:
// who is calling, what grant they hold on the tenant, and the tenant record the
// lifecycle path expects in the request context.
//
// It decides membership and liveness and stops there. It deliberately does NOT
// decide the action: a role that may not submit or cancel is refused by the
// handler for that verb, because the refusal is an authorization decision that
// has to be journaled with its reason, and a shared check cannot write one
// without inventing an action it does not know. Reads have no such row and no
// role bar — every member of a tenant may see its workloads.
func (a *Accounts) tenantWorkloadRequest(w http.ResponseWriter, r *http.Request) (*http.Request, tenantCaller, bool) {
	account, ok := a.callerAccount(w, r)
	if !ok {
		return nil, tenantCaller{}, false
	}
	membership, err := a.Store.MembershipFor(account.ID, r.PathValue("tenant_id"))
	if err != nil {
		a.writeMembershipReadError(w, err)
		return nil, tenantCaller{}, false
	}
	if !state.ValidRole(membership.Role) {
		writeForbidden(w)
		return nil, tenantCaller{}, false
	}
	tenant, err := a.Store.TenantSummaryByIDContext(r.Context(), membership.CustomerID)
	if err != nil {
		a.writeMembershipReadError(w, err)
		return nil, tenantCaller{}, false
	}
	if tenant.Revoked {
		tenantNotFound(w)
		return nil, tenantCaller{}, false
	}
	if a.Workloads == nil {
		a.Log.Error("account workload route enabled without workload lifecycle")
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "workload service unavailable"})
		return nil, tenantCaller{}, false
	}
	ctx := context.WithValue(r.Context(), ctxCustomer, tenantWorkloadCustomer(tenant))
	return r.WithContext(ctx), tenantCaller{account: account, membership: membership}, true
}

func tenantWorkloadCustomer(tenant state.TenantSummary) *state.Customer {
	return &state.Customer{
		ID:                  tenant.ID,
		Plan:                tenant.Plan,
		MaxConcurrentBursts: tenant.MaxConcurrentBursts,
		MaxHourlyUSD:        tenant.MaxHourlyUSD,
		WorkloadNamespaces:  tenant.WorkloadNamespaces,
		ClusterPolicy:       tenant.ClusterPolicy,
		TemplateCatalog:     tenant.TemplateCatalog,
	}
}

func tenantWorkloadSpec(raw []byte) (TenantWorkloadSpec, error) {
	var parsed workload.Workload
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return TenantWorkloadSpec{}, err
	}
	var networking *TenantWorkloadNetworking
	if parsed.Spec.Networking != nil {
		networking = &TenantWorkloadNetworking{Tier: parsed.Spec.Networking.Tier}
	}
	var machine *TenantWorkloadMachine
	if parsed.Spec.Machine != nil {
		machine = &TenantWorkloadMachine{FlyType: parsed.Spec.Machine.FlyType, Region: parsed.Spec.Machine.Region}
	}
	var gpu *TenantWorkloadGPU
	if parsed.Spec.GPU != nil {
		gpu = &TenantWorkloadGPU{Kind: parsed.Spec.GPU.Kind, Count: parsed.Spec.GPU.Count, Reliability: parsed.Spec.GPU.Reliability, MaxHourlyUSD: parsed.Spec.GPU.MaxHourlyUSD}
	}
	var budget *TenantWorkloadBudget
	if parsed.Spec.Budget != nil {
		budget = &TenantWorkloadBudget{MaxUSD: parsed.Spec.Budget.MaxUSD, Deadline: parsed.Spec.Budget.Deadline.String()}
	}
	var data *TenantWorkloadData
	if parsed.Spec.Storage != nil && (len(parsed.Spec.Storage.Cache) > 0 || len(parsed.Spec.Storage.Artifacts) > 0) {
		data = &TenantWorkloadData{
			PullBeforeRunInputs: len(parsed.Spec.Storage.Cache),
			PushAfterRunOutputs: len(parsed.Spec.Storage.Artifacts),
		}
		for _, input := range parsed.Spec.Storage.Cache {
			data.InputSizeHintGB += input.SizeHintGB
		}
		for _, output := range parsed.Spec.Storage.Artifacts {
			data.OutputMaxUploadGB += output.MaxSizeGB
		}
	}
	var execution *TenantWorkloadExecution
	if len(parsed.Spec.Command) > 0 || len(parsed.Spec.Args) > 0 {
		execution = &TenantWorkloadExecution{
			Mode:           "custom",
			CommandTokens:  len(parsed.Spec.Command),
			ArgumentTokens: len(parsed.Spec.Args),
		}
	}
	return TenantWorkloadSpec{
		APIVersion: parsed.APIVersion,
		Kind:       parsed.Kind,
		Metadata: TenantWorkloadMetadata{
			Name:      parsed.Metadata.Name,
			Namespace: parsed.Metadata.Namespace,
		},
		Spec: TenantWorkloadSpecBody{
			Image:       parsed.Spec.Image,
			Execution:   execution,
			Replicas:    parsed.Spec.Replicas,
			NodeOnly:    parsed.Spec.NodeOnly,
			Backend:     parsed.Spec.Backend,
			Region:      parsed.Spec.Region,
			Networking:  networking,
			Machine:     machine,
			Size:        parsed.Spec.Size,
			CPU:         parsed.Spec.CPU,
			Memory:      parsed.Spec.Memory,
			GPU:         gpu,
			Budget:      budget,
			Retries:     parsed.Spec.Retries,
			ModelVolume: parsed.Spec.ModelVolume,
			Data:        data,
		},
	}, nil
}

// HandleGet serves GET /v1/account: validate the caller's Yscale ID access
// token, refresh their stored profile, and return the account with its tenant
// memberships.
func (a *Accounts) HandleGet(w http.ResponseWriter, r *http.Request) {
	identity, ok := a.resolveCaller(w, r)
	if !ok {
		return
	}

	account, err := a.Store.UpsertAccount(a.Issuer, identity.Subject, state.AccountProfile{
		Email:         identity.Email,
		EmailVerified: identity.EmailVerified,
		Name:          identity.Name,
	})
	if err != nil {
		if errors.Is(err, state.ErrPersistence) {
			// The credential was fine and the store refused the write. Answering
			// 401 here would tell a signed-in human to sign in again over a
			// database problem.
			a.Log.Error("account: persist account", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
			return
		}
		// Otherwise: an identity the store won't key (blank issuer/subject),
		// which the resolver already rejects — a refused credential, not a
		// server fault.
		a.Log.Warn("account: rejected identity", "error", err)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized: invalid identity"})
		return
	}

	tenants, err := a.tenantsFor(r.Context(), account.ID)
	if err != nil {
		a.Log.Error("account: read tenants", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, AccountResponse{
		AccountID:     account.ID,
		Issuer:        account.Issuer,
		Subject:       account.Subject,
		Email:         account.Email,
		EmailVerified: account.EmailVerified,
		Name:          account.Name,
		CreatedAt:     account.CreatedAt,
		Tenants:       tenants,
	})
}

// resolveCaller is the front half every human-account route shares: is this
// surface configured at all, is there a Bearer credential, and whose is it. It
// writes its own failure response and reports ok=false; the caller returns.
//
// It is one function rather than one per route on purpose. The disabled-route
// 404, the unconfigured-issuer 503, the 401s and the 429 + Retry-After are the
// contract of the credential, not of any single route, and a second hand-rolled
// copy of it is how one route ends up shedding without Retry-After — or, worse,
// answering 401 for an identity provider that is merely down and telling a
// signed-in human to sign in again.
//
// It deliberately stops at the identity. Whether that identity is turned into
// an account, and whether an account is CREATED to hold it, is each route's own
// decision: /v1/account is a sign-in and mints one, the tenant-scoped routes
// resolve an existing one and never write.
func (a *Accounts) resolveCaller(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	if a.Resolver == nil {
		// Yscale ID isn't configured — the SaaS account surface does not exist
		// on this deployment. 404, not 401: there is nothing to authenticate to.
		http.NotFound(w, r)
		return Identity{}, false
	}
	if a.Issuer == "" {
		a.Log.Error("account route enabled without YSCALE_ID_ISSUER; refusing to key accounts")
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "identity issuer not configured"})
		return Identity{}, false
	}

	token, ok := bearerToken(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized: missing Bearer token"})
		return Identity{}, false
	}
	identity, err := a.Resolver.Resolve(r.Context(), token)
	if err != nil {
		switch {
		case errors.Is(err, ErrRateLimited):
			// Shed locally: the token was never presented upstream, so this says
			// nothing about the credential. Retry-After keeps a polling
			// dashboard from tightening the loop it is already losing.
			a.Log.Warn("account: identity resolution shed", "error", err)
			w.Header().Set("Retry-After", retryAfterSeconds)
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "too many identity lookups; retry shortly"})
		case errors.Is(err, ErrIdentityUnavailable):
			a.Log.Warn("account: identity provider unavailable", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "identity provider unavailable"})
		default:
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized: invalid access token"})
		}
		return Identity{}, false
	}
	return identity, true
}

// tenantsFor renders the store's joined grant/settings snapshot. Missing,
// revoked and tombstoned tenants are excluded by that read. Any other read
// failure aborts the response rather than advertising a partial tenant list.
func (a *Accounts) tenantsFor(ctx context.Context, accountID string) ([]TenantMembershipSummary, error) {
	rows, err := a.Store.AccountTenantsContext(ctx, accountID)
	if err != nil {
		return nil, err
	}
	out := make([]TenantMembershipSummary, 0, len(rows))
	for _, row := range rows {
		m, tenant := row.Membership, row.Tenant
		out = append(out, TenantMembershipSummary{
			CustomerID: tenant.ID,
			Name:       tenant.Name,
			Role:       m.Role,
			Plan:       tenant.Plan,
			Limits:     TenantLimits{tenant.MaxConcurrentBursts, tenant.MaxHourlyUSD},
			// The same function the submission gate asks, so the console is
			// told the set it will actually be judged against. The summary is
			// already a detached copy, so what lands here shares nothing with
			// the store.
			WorkloadNamespaces: authorizedWorkloadNamespaces(&state.Customer{WorkloadNamespaces: tenant.WorkloadNamespaces}),
		})
	}
	return out, nil
}

// TenantMembersResponse is one page of a tenant's member roster.
type TenantMembersResponse struct {
	Members []TenantMemberSummary `json:"members"`
	// NextAfter is present ONLY when there are more members after this page,
	// and is what the caller sends back as ?after= to get them. Absent means
	// the roster ends here, so a client that ignores it still sees a complete
	// roster for every tenant under the page limit.
	NextAfter string `json:"next_after,omitempty"`
}

// TenantMemberSummary is one human on a tenant, as their fellow members may see
// them. Issuer and Subject are absent by construction: they are the identity
// provider's key for that person, they identify them across every tenant on
// this central, and a co-member has no need of them to manage a roster.
// CreatedAt is when the GRANT was made, not when the human signed up.
type TenantMemberSummary struct {
	AccountID string    `json:"account_id"`
	Email     string    `json:"email,omitempty"`
	Name      string    `json:"name,omitempty"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

// tenantNotFound is the ONE answer to every way a caller has no business with a
// tenant: it does not exist, it is revoked, or they are not a member. The body
// is byte-identical across all three because the difference is exactly what an
// attacker would use to enumerate tenant ids — a distinguishable "not a member"
// confirms the tenant exists.
func tenantNotFound(w http.ResponseWriter) {
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
}

func (a *Accounts) writeMembershipReadError(w http.ResponseWriter, err error) {
	if errors.Is(err, state.ErrNotFound) {
		tenantNotFound(w)
		return
	}
	a.Log.Error("account: read human authority", "error", err)
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
}

func writeForbidden(w http.ResponseWriter) {
	writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing members requires owner or admin"})
}

// writeOwnerProtected is the OTHER 403 the removal route answers: the caller
// may manage this roster and the target is an owner, which only an owner may
// remove. It is a separate message because writeForbidden's is false here — an
// admin told the operation "requires owner or admin" goes looking for a
// permission they already hold.
func writeOwnerProtected(w http.ResponseWriter) {
	writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: only an owner may remove an owner"})
}

// parseRosterQuery reads the roster page bounds off a request. Everything it
// rejects, it rejects before the tenant is looked at, so a 400 is the same
// answer for a tenant that exists and one that does not.
//
// Repeated and empty values are refused rather than resolved: ?limit=10&limit=9
// carries two intents and ?after= carries none, and picking one — or ignoring
// the parameter — hands back a page the caller did not ask for while looking
// exactly like the page they did. A limit outside the range is refused for the
// same reason; silently clamping 10000 to 500 reports a truncated roster as a
// complete one. Unknown parameters are refused too: a caller who thinks they
// sent a cursor and got page one has no way to tell.
func parseRosterQuery(raw string) (state.RosterQuery, error) {
	values, err := url.ParseQuery(raw)
	if err != nil {
		return state.RosterQuery{}, errors.New("invalid query string")
	}
	var q state.RosterQuery
	for key, vals := range values {
		if key != "limit" && key != "after" {
			return state.RosterQuery{}, fmt.Errorf("unsupported query parameter %q; this route takes limit and after", key)
		}
		if len(vals) != 1 || vals[0] == "" {
			return state.RosterQuery{}, fmt.Errorf("%s must be given exactly once with a value", key)
		}
	}
	if v := values.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > state.MaxRosterLimit {
			return state.RosterQuery{}, fmt.Errorf("limit must be an integer between 1 and %d", state.MaxRosterLimit)
		}
		q.Limit = n
	}
	q.After = values.Get("after")
	return q, nil
}

// HandleListMembers serves GET /v1/tenants/{tenant_id}/members: one page of the
// roster of one tenant, for an owner or admin of that tenant.
//
// The whole read — tenant liveness, the caller's authority, and the rows it
// authorizes — is one store snapshot. This handler decides nothing
// about who may see what; it maps errors to statuses and renders the snapshot
// it was handed, which is the only way the answer cannot be a mix of two
// instants.
func (a *Accounts) HandleListMembers(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	// After the credential, before the tenant: a malformed page bound is the
	// caller's own mistake and answering it does not depend on the tenant
	// existing, while answering it BEFORE the credential would tell a caller on
	// a deployment with no Yscale ID that the route is there at all.
	query, err := parseRosterQuery(r.URL.RawQuery)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	roster, err := a.Store.TenantRosterForContext(r.Context(), tenantID, caller.ID, query)
	if err != nil {
		switch {
		case errors.Is(err, state.ErrNotAuthorized):
			// A member or viewer already knows the tenant exists — they hold a
			// grant on it — so there is nothing left to conceal, and 403 tells
			// them the truth: the roster is a management surface, their role is not.
			writeForbidden(w)
		case errors.Is(err, state.ErrNotFound):
			// Unknown tenant, revoked tenant and non-member are one answer; the
			// store collapses them so this cannot accidentally tell them apart.
			tenantNotFound(w)
		default:
			// Every error this read can return today is one of the two above,
			// and the mapping is explicit so it stays that way. A future fault
			// here — a durable read, a backend timeout — must not be reported as
			// "tenant not found", which tells the caller their live tenant was
			// deleted and leaves nothing in the log to contradict it.
			a.Log.Error("account: list tenant members", "tenant", tenantID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		}
		return
	}

	members := make([]TenantMemberSummary, 0, len(roster.Members))
	for _, m := range roster.Members {
		members = append(members, TenantMemberSummary{
			AccountID: m.AccountID,
			Email:     m.Email,
			Name:      m.Name,
			Role:      m.Role,
			CreatedAt: m.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, TenantMembersResponse{Members: members, NextAfter: roster.NextAfter})
}

// AddTenantMemberRequest is the POST body: the email of an EXISTING Yscale
// account, and the role to grant it.
//
// Email rather than an account id because a manager knows their colleague's
// address and nothing else about them, and it is an address on an account that
// already exists — this route sends no mail, creates no account and leaves
// nothing pending. A human who has never signed in cannot be added; they sign
// in first, which is what makes their address verified in the first place.
type AddTenantMemberRequest struct {
	Email string `json:"email"`
	Role  string `json:"role"`
}

// TenantMemberRoleRequest is the PATCH body: the role the member should hold
// from now on. It is the whole document — a role change is the only thing this
// route does, and a body carrying anything else is a caller expecting an
// operation that does not exist.
type TenantMemberRoleRequest struct {
	Role string `json:"role"`
}

// maxMemberRequestBytes bounds a roster mutation body. Both documents are two
// short fields, so anything larger is a mistake or a probe, and reading it into
// memory before rejecting it is the cost of finding out.
const maxMemberRequestBytes = 4 << 10

// decodeMemberRequest reads one bounded JSON object off a request, and is as
// strict as parseRosterQuery is with the query string, for the same reason: an
// unknown field means the caller believes they asked for something this route
// silently ignored. It never reports the decoder's own error, which quotes the
// body back — a request body is caller-controlled input and an echo of it is
// how one ends up rendered somewhere it is trusted.
func decodeMemberRequest(r *http.Request, into any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxMemberRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return errors.New("body must be a JSON object with the documented fields")
	}
	if decoder.More() {
		return errors.New("body must be a single JSON object")
	}
	return nil
}

// memberRole normalizes a caller-typed role and refuses anything outside the
// closed set. Case and surrounding space are the caller's typing rather than
// their intent — the set is all-lowercase and has no two members that differ by
// either — but an unrecognised value is refused rather than defaulted: a
// silently-defaulted role is a grant nobody asked for.
func memberRole(raw string) (string, error) {
	role := strings.ToLower(strings.TrimSpace(raw))
	if !state.ValidRole(role) {
		return "", fmt.Errorf("role must be one of %q, %q, %q or %q",
			state.RoleOwner, state.RoleAdmin, state.RoleMember, state.RoleViewer)
	}
	return role, nil
}

// writeMemberWriteError maps a roster mutation's refusal to a status. Shared by
// both write routes so the two cannot answer the same refusal differently —
// targetMissing is the one message that legitimately differs, because one route
// names its target by email and the other by account id.
//
// The order is the contract. ErrOwnerRestricted is checked before
// ErrNotAuthorized, which it wraps, so an admin is told the owner ROLE is out of
// reach rather than that they need a permission they already hold — exactly why
// HandleRemoveMember checks ErrOwnerProtected first. ErrTargetNotFound is
// checked before ErrNotFound, which it wraps, so a manager looking at their own
// roster is not told their live tenant does not exist.
func (a *Accounts) writeMemberWriteError(w http.ResponseWriter, op, tenantID, targetMissing string, err error) {
	switch {
	case errors.Is(err, state.ErrOwnerRestricted):
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error": "forbidden: only an owner may grant or change the owner role"})
	case errors.Is(err, state.ErrNotAuthorized):
		writeForbidden(w)
	case errors.Is(err, state.ErrTargetNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": targetMissing})
	case errors.Is(err, state.ErrNotFound):
		// Unknown tenant, revoked tenant and non-member are one answer.
		tenantNotFound(w)
	case errors.Is(err, state.ErrInvalidRole), errors.Is(err, state.ErrInvalidIdentity):
		// The handler validated both before calling, so this is a disagreement
		// between the two — still the caller's input, so still a 400 and never a
		// 503 that invites them to retry a request that cannot succeed.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid member request"})
	case errors.Is(err, state.ErrRoleConflict):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "conflict: that account is already a member with a different role; change their role instead"})
	case errors.Is(err, state.ErrLastOwner):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "tenant must keep at least one owner"})
	default:
		// ErrPersistence: the durable write was refused and the store rolled the
		// change back, so nothing happened and a retry is worth making. Anything
		// else unexpected fails the same way — closed.
		a.Log.Error("account: "+op, "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
	}
}

// HandleAddMember serves POST /v1/tenants/{tenant_id}/members: an owner or
// admin grants an existing verified Yscale account access to their tenant.
//
// Authorization, the target's identity, the owner floor, the grant and its
// audit row are one store call under one write lock. This handler validates the
// SHAPE of the request and maps errors to statuses, and decides nothing about
// who may do what — re-reading the caller's role here first would read it one
// lock earlier than the store acts on it, which is the window an admin racing
// their own demotion needs.
//
// 201 for a grant this call created, 200 for an identical one that already
// existed. The two are the same access and deliberately not the same status:
// the store reports which happened from inside the lock, so neither answer nor
// the audit log can claim a grant another caller made.
func (a *Accounts) HandleAddMember(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	// After the credential, before the tenant: parseRosterQuery's ordering, and
	// for its reason — answering a malformed body first would tell a caller on a
	// deployment with no Yscale ID that the route is there at all.
	var request AddTenantMemberRequest
	if err := decodeMemberRequest(r, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if strings.TrimSpace(request.Email) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "email is required"})
		return
	}
	role, err := memberRole(request.Role)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	membership, created, err := a.Store.AddTenantMemberByEmail(tenantID, caller.ID, a.Issuer,
		request.Email, role, state.HumanActor(caller.ID, tenantID))
	if err != nil {
		// One answer for an unknown address, an unverified one and an ambiguous
		// one, so an authorized manager cannot use this route to learn which
		// addresses have accounts on this central. It is deliberately not the
		// tenant 404's body: the caller manages this tenant, and telling them it
		// vanished would send them chasing the wrong thing.
		a.writeMemberWriteError(w, "add tenant member", tenantID,
			"no verified Yscale account matches that email", err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
		a.Log.Info("tenant member added", "tenant", tenantID, "caller_account", caller.ID,
			"account", membership.AccountID, "role", membership.Role)
	}
	// The grant, not the profile: the store answered with the membership, and
	// rendering an email and name off a second read would be reading them one
	// lock later than the grant they describe. The roster is where a console
	// gets the profile, under the lock that authorized it.
	writeJSON(w, status, TenantMemberSummary{
		AccountID: membership.AccountID,
		Role:      membership.Role,
		CreatedAt: membership.CreatedAt,
	})
}

// HandleUpdateMemberRole serves PATCH /v1/tenants/{tenant_id}/members/{account_id}:
// an owner or admin changes what an existing member may do.
//
// Same contract as the grant above, including who decides it — the store, under
// one lock — plus the two rules that keep the role floor from being decorative:
// an admin may not promote anyone to owner and may not change an existing
// owner, so no admin can promote themselves out of the ceiling they are under.
// 200 for a real change and for setting the role a member already holds; the
// store writes no audit row for the second, because nothing changed.
func (a *Accounts) HandleUpdateMemberRole(w http.ResponseWriter, r *http.Request) {
	tenantID, targetID := r.PathValue("tenant_id"), r.PathValue("account_id")
	if tenantID == "" || targetID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	var request TenantMemberRoleRequest
	if err := decodeMemberRequest(r, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	role, err := memberRole(request.Role)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	membership, previous, err := a.Store.SetTenantMemberRole(tenantID, caller.ID, targetID, role,
		state.HumanActor(caller.ID, tenantID))
	if err != nil {
		a.writeMemberWriteError(w, "set tenant member role", tenantID,
			"no such member on this tenant", err)
		return
	}
	if previous != membership.Role {
		a.Log.Info("tenant member role changed", "tenant", tenantID, "caller_account", caller.ID,
			"account", membership.AccountID, "role", membership.Role, "previous_role", previous)
	}
	writeJSON(w, http.StatusOK, TenantMemberSummary{
		AccountID: membership.AccountID,
		Role:      membership.Role,
		CreatedAt: membership.CreatedAt,
	})
}

// TenantUsageResponse is what one tenant is running RIGHT NOW, and nothing
// else. It is not an invoice, not a bill and not a ledger: there is no history
// behind it, nothing accrues into it, and a burst that finishes leaves it at
// once — so the numbers answer "what am I running and how close am I to my
// ceilings", never "what do I owe". ObservedAt is stamped so a console cannot
// present a stale poll as the current state.
//
// The figures are the SAME computation as GET /v1/spend, which is also what the
// admission path caps against; a tenant surface that derived its own would
// disagree with the cap that actually refuses their next burst.
type TenantUsageResponse struct {
	TenantID   string    `json:"tenant_id"`
	ObservedAt time.Time `json:"observed_at"`
	// RunningBursts and HourlyUSD describe live bursts only.
	RunningBursts int     `json:"running_bursts"`
	HourlyUSD     float64 `json:"hourly_usd"`
	// ProjectedDailyUSD is the worst case if everything running now kept running
	// for a day. It is a projection off the current rate, not a day's spend.
	ProjectedDailyUSD float64      `json:"projected_daily_usd"`
	Limits            TenantLimits `json:"limits"`
}

// HandleGetUsage serves GET /v1/tenants/{tenant_id}/usage: the live burst usage
// of one tenant, for any member of it.
//
// Every role may read it, viewers included — a read-only seat that cannot see
// what the tenant is running is not a seat — and no role may read another
// tenant's, which is the whole reason the store authorizes the read against the
// caller's grant in the same lock it takes the snapshot in.
func (a *Accounts) HandleGetUsage(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	usage, err := a.Store.TenantUsageFor(tenantID, caller.ID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			// Unknown tenant, revoked tenant and non-member are one answer, so
			// this route cannot be used to enumerate tenant ids either.
			tenantNotFound(w)
			return
		}
		// This read returns nothing else today, and the mapping is explicit so a
		// future fault is never reported as a tenant that went away.
		a.Log.Error("account: read tenant usage", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	spend := spendFromBursts(usage.Bursts, TenantLimits{usage.MaxConcurrentBursts, usage.MaxHourlyUSD})
	writeJSON(w, http.StatusOK, TenantUsageResponse{
		TenantID:          tenantID,
		ObservedAt:        time.Now().UTC(),
		RunningBursts:     spend.RunningBursts,
		HourlyUSD:         spend.HourlyUSD,
		ProjectedDailyUSD: spend.ProjectedDailyUSD,
		Limits:            spend.Limits,
	})
}

// HandleRemoveMember serves DELETE /v1/tenants/{tenant_id}/members/{account_id}.
//
// Every authorization decision belongs to the store, which takes them together
// with the delete under one write lock. Re-checking the caller's role here
// first would read a role, release the lock, and act on it — which is the exact
// window two owners removing each other need to both succeed. So this handler
// resolves the human and maps errors to statuses, and decides nothing.
//
// Removing a member does NOT rotate the tenant's Customer.Token: that
// credential belongs to the cluster, not to any human, and a departing member
// who copied it keeps working access until an operator rotates it. See
// docs/internals/tenant-isolation.md §7a.
func (a *Accounts) HandleRemoveMember(w http.ResponseWriter, r *http.Request) {
	tenantID, targetID := r.PathValue("tenant_id"), r.PathValue("account_id")
	if tenantID == "" || targetID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	removed, err := a.Store.RemoveTenantMembership(tenantID, caller.ID, targetID, state.HumanActor(caller.ID, tenantID))
	switch {
	case err == nil:
		// 204 covers both "removed" and "was already absent". They are the same
		// outcome to the caller, and only reachable once the store has confirmed
		// this caller may manage this tenant's roster. The audit log is NOT the
		// same for both: it records access that was actually revoked, and a
		// retried removal of an absent target revoked none. The store reports
		// which happened from inside the write lock, so the log cannot claim a
		// deletion another caller made.
		if removed != nil {
			a.Log.Info("tenant member removed", "tenant", tenantID, "caller_account", caller.ID,
				"account", removed.AccountID, "role", removed.Role)
		}
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, state.ErrNotFound):
		tenantNotFound(w)
	case errors.Is(err, state.ErrOwnerProtected):
		// Checked before ErrNotAuthorized, which it wraps: this caller manages
		// the roster and it is the target that is out of reach.
		writeOwnerProtected(w)
	case errors.Is(err, state.ErrNotAuthorized):
		writeForbidden(w)
	case errors.Is(err, state.ErrLastOwner):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "tenant must keep at least one owner"})
	default:
		// ErrPersistence: the durable delete was refused and the store rolled
		// the removal back, so the member still has access and a retry is worth
		// making. Anything else unexpected fails the same way — closed.
		a.Log.Error("account: remove tenant membership",
			"tenant", tenantID, "target", targetID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
	}
}

// callerAccount resolves the human behind a tenant-scoped request to an
// EXISTING account. Unlike /v1/account it never mints one: a management route
// is not a sign-in, and a valid access token for a human who has never signed
// in must not write a row on its way to being told the tenant is not theirs.
// Having no account is having no membership, so it answers the same 404.
func (a *Accounts) callerAccount(w http.ResponseWriter, r *http.Request) (*state.Account, bool) {
	identity, ok := a.resolveCaller(w, r)
	if !ok {
		return nil, false
	}
	account, err := a.Store.AccountByIdentity(a.Issuer, identity.Subject)
	if err != nil {
		if errors.Is(err, state.ErrPersistence) {
			a.Log.Error("account: resolve human account", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
			return nil, false
		}
		if errors.Is(err, state.ErrNotFound) {
			tenantNotFound(w)
			return nil, false
		}
		// An identity the store will not key — the resolver already rejects a
		// blank subject, so this is a refused credential, not a server fault.
		a.Log.Warn("account: rejected identity", "error", err)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized: invalid identity"})
		return nil, false
	}
	return account, true
}

// bearerToken pulls the raw credential out of an Authorization header.
func bearerToken(r *http.Request) (string, bool) {
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	return token, token != ""
}
