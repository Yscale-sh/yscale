// yscale:proprietary

package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// The hosted-capacity operator surface: Yscale runs one shared physical
// cluster and grants each participating tenant its own connector release on
// it. The grant is a hosted row in the TENANT'S registry — the connector
// authenticates as the real tenant, bound to a globally unique virtual
// cluster id — so every existing tenant-scoped path (placement, audit, spend,
// Get, agent lifecycle) works on it unchanged, and the tenant lists and
// routes to it through the APIs they already have. What the tenant cannot do
// is rotate or delete it: the release lives on the operator's hardware, so
// the lifecycle answers to the AdminAuth credential, exactly like the
// workload-namespace mutator these routes sit beside.

// assignHostedClusterRequest is the POST body; every field is optional.
// cluster_id and namespace default to a generated hosted id and a
// tenant-derived namespace; name defaults to a fixed display label.
type assignHostedClusterRequest struct {
	ClusterID string `json:"cluster_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

// HostedClusterSummary is the safe metadata of one hosted assignment: the
// store's view fields and nothing off the registry row itself, so the
// credential hash has no field to leak into — TenantClusterSummary's
// construction, minus the tenant-policy eligibility join that has no meaning
// on an operator route.
type HostedClusterSummary struct {
	ClusterID       string    `json:"cluster_id"`
	Name            string    `json:"name,omitempty"`
	Source          string    `json:"source"`
	State           string    `json:"state"`
	HostedNamespace string    `json:"hosted_namespace"`
	RegisteredAt    time.Time `json:"registered_at"`
}

// HostedClusterCredentialResponse answers the assign and rotate routes.
// ConnectorToken is the single reveal of the credential, exactly as on the
// tenant registry routes. The helm fields are the concrete per-tenant release
// on the shared cluster: a UNIQUE release name per assignment, the tenant's
// virtual cluster id, and rbac.allowedNamespaces scoped to exactly the
// reserved namespace — the chart's namespaced default is deliberately left in
// place, so the release never holds cluster-wide Secret access. The command
// references $YSCALE_CONNECTOR_TOKEN rather than embedding the secret, for
// TenantClusterCredentialResponse's reason.
type HostedClusterCredentialResponse struct {
	TenantID       string               `json:"tenant_id"`
	Cluster        HostedClusterSummary `json:"cluster"`
	ConnectorToken string               `json:"connector_token"`
	HelmRelease    string               `json:"helm_release"`
	// ConnectorRBACSet is the one --set that scopes the release's Secret
	// reads, on its own for operators scripting against the response.
	ConnectorRBACSet string `json:"connector_rbac_set"`
	HelmCommand      string `json:"helm_command"`
}

// HostedClusterDeleteResponse confirms an unassignment and names the
// namespace whose reservation it freed — the operator still owes the shared
// cluster a `helm uninstall` of the release, which central cannot run.
type HostedClusterDeleteResponse struct {
	TenantID        string `json:"tenant_id"`
	ClusterID       string `json:"cluster_id"`
	HostedNamespace string `json:"hosted_namespace,omitempty"`
	Deleted         bool   `json:"deleted"`
}

// PendingHostedCapacityRequestsResponse is the operator's pending queue: the
// tenants waiting on shared capacity, oldest request first. The rows are the
// store's safe view — tenant id, optional display name, plan, requested_at —
// so no customer bearer token, email, membership, connector credential or
// credential hash has a field to serialize into.
type PendingHostedCapacityRequestsResponse struct {
	Requests []PendingHostedCapacityRequestView `json:"requests"`
}

// PendingHostedCapacityRequestView is one queue row. Name mirrors the
// membership summaries: additive and omitted when empty, so a console keeps
// rendering those tenants by id.
type PendingHostedCapacityRequestView struct {
	TenantID    string    `json:"tenant_id"`
	Name        string    `json:"name,omitempty"`
	Plan        string    `json:"plan"`
	RequestedAt time.Time `json:"requested_at"`
}

// HostedClusterInventoryResponse is the operator's standing hosted inventory:
// every active assignment across live tenants, in a stable envelope. The rows
// are the store's safe copy-out view — the same reason the pending queue above
// carries what it carries — so no customer bearer token, email, membership,
// connector credential or credential hash has a field to serialize into.
type HostedClusterInventoryResponse struct {
	Clusters []HostedClusterInventoryRow `json:"clusters"`
}

// HostedClusterInventoryRow is one assignment: who holds it, on what plan, and
// the SAME HostedClusterSummary the assign and rotate responses return, so a
// console reads one shape everywhere. Name mirrors the queue row's: additive
// and omitted when empty, so a console keeps rendering those tenants by id.
type HostedClusterInventoryRow struct {
	TenantID string               `json:"tenant_id"`
	Name     string               `json:"name,omitempty"`
	Plan     string               `json:"plan"`
	Cluster  HostedClusterSummary `json:"cluster"`
}

func hostedClusterSummary(c state.TenantCluster) HostedClusterSummary {
	return HostedClusterSummary{
		ClusterID:       c.ClusterID,
		Name:            c.Name,
		Source:          c.Source,
		State:           c.State,
		HostedNamespace: c.HostedNamespace,
		RegisteredAt:    c.RegisteredAt,
	}
}

// hostedHelmRelease names the per-tenant agent release. One release per
// assignment on one shared cluster, so the name must be unique there — the
// hosted cluster id already is, globally, and its grammar is bounded so this
// stays inside helm's 53-character release limit.
func hostedHelmRelease(clusterID string) string {
	return "yscale-agent-" + clusterID
}

// hostedHelmInstall renders the install command for one hosted assignment.
// clusterID and namespace are shell-inert by their grammars (the store
// validated both); the endpoint is operator config and gets the same quoting
// the other rendered commands apply. installCRD=false because this is an
// ADDITIONAL release on a cluster whose platform baseline — the Workload CRD,
// the namespaces themselves — is owned by the operator's GitOps, not by any
// tenant's release.
func hostedHelmInstall(endpoint, clusterID, namespace string) string {
	cmd := "helm install " + hostedHelmRelease(clusterID) + " " + agentChartRef + " " +
		"--namespace yscale-system " +
		"--set clusterID=" + clusterID + " --set token=\"$YSCALE_CONNECTOR_TOKEN\" " +
		"--set installCRD=false --set rbac.scope=namespaced " + allowedNamespacesFlag([]string{namespace})
	if endpoint != "" {
		cmd += " --set endpoint=" + shellQuote(endpoint)
	}
	return cmd
}

// hostedHelmUpgrade is the rotation counterpart: same release, an UPGRADE
// with --reuse-values so the assignment's namespace scoping and endpoint
// survive, with the rotated token as the one explicit override.
func hostedHelmUpgrade(clusterID string) string {
	return "helm upgrade " + hostedHelmRelease(clusterID) + " " + agentChartRef + " " +
		"--namespace yscale-system --reuse-values " +
		"--set clusterID=" + clusterID + " --set token=\"$YSCALE_CONNECTOR_TOKEN\""
}

// writeHostedClusterError maps the store's hosted-lifecycle refusals onto
// stable statuses. ErrClusterNotFound is checked before the ErrNotFound it
// wraps so "no such hosted assignment" and "no such tenant" stay two answers
// — this is an operator route, so there is no enumeration oracle to protect.
func (t *Tenants) writeHostedClusterError(w http.ResponseWriter, op, tenantID string, err error) {
	switch {
	case errors.Is(err, state.ErrClusterNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such hosted cluster assignment"})
	case errors.Is(err, state.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
	case errors.Is(err, state.ErrInvalidCluster), errors.Is(err, state.ErrInvalidHostedNamespace):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, state.ErrHostedClusterExists):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "tenant already has a hosted cluster assignment"})
	case errors.Is(err, state.ErrHostedNamespaceTaken):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "hosted namespace is already reserved"})
	case errors.Is(err, state.ErrClusterExists):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "cluster id is already in use"})
	case errors.Is(err, state.ErrClusterLimit):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, state.ErrClusterNamedByPolicy):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "cluster is still named by the tenant cluster policy; the tenant removes it from allow/deny first"})
	default:
		t.Log.Error("admin: "+op, "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
	}
}

// HandleListHostedCapacityRequests serves
// GET /v1/admin/hosted-capacity/requests: the pending shared-capacity queue,
// so an operator can discover requests without already knowing each tenant
// id and then grant them through the assign route. Read-only by
// construction — the store method is a copy-out view under the read lock —
// so this route writes nothing and appends no audit row.
func (t *Tenants) HandleListHostedCapacityRequests(w http.ResponseWriter, r *http.Request) {
	pending := t.Store.PendingHostedCapacityRequests()
	rows := make([]PendingHostedCapacityRequestView, len(pending))
	for i, req := range pending {
		rows[i] = PendingHostedCapacityRequestView{
			TenantID:    req.TenantID,
			Name:        req.Name,
			Plan:        req.Plan,
			RequestedAt: req.RequestedAt,
		}
	}
	writeJSON(w, http.StatusOK, PendingHostedCapacityRequestsResponse{Requests: rows})
}

// HandleListHostedClusters serves GET /v1/operator/hosted-clusters and its
// admin-token-protected inventory twin GET /v1/admin/hosted-clusters: the
// standing inventory the browser console renders beside the pending queue —
// every hosted assignment across live tenants, so an operator can see what the
// shared cluster is actually carrying before granting more of it. Read-only by
// construction — the store method is a copy-out view under the read lock — so
// this route writes nothing and appends no audit row.
func (t *Tenants) HandleListHostedClusters(w http.ResponseWriter, r *http.Request) {
	assignments := t.Store.HostedClusterAssignments()
	rows := make([]HostedClusterInventoryRow, len(assignments))
	for i, a := range assignments {
		rows[i] = HostedClusterInventoryRow{
			TenantID: a.TenantID,
			Name:     a.Name,
			Plan:     a.Plan,
			Cluster:  hostedClusterSummary(a.Cluster),
		}
	}
	writeJSON(w, http.StatusOK, HostedClusterInventoryResponse{Clusters: rows})
}

// operatorLifecycleActor attributes an operator-credentialed change — a hosted
// assign, rotate or delete, or a connector-command requeue — to the principal
// whose credential carried the request: the signed-in human on the
// browser-operator routes — OperatorAuth put their account id in the context —
// and the static operator actor on the AdminAuth routes, whose shared token
// holds no per-operator identity to record. Nothing else decides: not a tenant
// role, not an email, not a request header or body — only the account id the
// middleware itself placed in the context after verifying the credential. The
// actor is account and tenant ids and nothing else: no issuer, subject, email
// or token reaches the journal.
//
// There is deliberately no cluster fallback. Every route that calls this runs
// behind AdminAuth or OperatorAuth, so "no operator account in the context"
// means the static admin token — an operator — and never a cluster credential.
// state.ClusterActor belongs to the connector path and must not appear on a
// change no connector was allowed to ask for.
func operatorLifecycleActor(r *http.Request, tenantID string) state.Actor {
	if accountID := OperatorAccountFromContext(r.Context()); accountID != "" {
		return state.HumanActor(accountID, tenantID)
	}
	return state.OperatorActor()
}

// HandleAssignHostedCluster serves
// POST /v1/admin/tenants/{tenant_id}/hosted-clusters and its browser-operator
// twin POST /v1/operator/tenants/{tenant_id}/hosted-clusters: grant the tenant
// one hosted connector release on the shared physical cluster. The response is
// the single reveal of the connector credential, plus the exact helm command
// the operator runs there. The cluster row and the namespace reservation land
// in one durable write, so a failure publishes neither.
func (t *Tenants) HandleAssignHostedCluster(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	var req assignHostedClusterRequest
	if r.Body != nil {
		// Every field is optional, so an absent or empty body is a valid
		// request — but a body that PARSES WRONG is refused rather than
		// ignored: silently dropping a mistyped namespace would reserve a
		// derived one the operator did not ask for.
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil && err != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid hosted cluster assignment JSON"})
			return
		}
	}

	cluster, token, err := t.Store.AssignHostedCluster(tenantID, req.ClusterID, req.Name, req.Namespace, operatorLifecycleActor(r, tenantID))
	if err != nil {
		t.writeHostedClusterError(w, "assign hosted cluster", tenantID, err)
		return
	}
	t.Log.Info("hosted cluster assigned", "tenant", tenantID,
		"cluster", cluster.ClusterID, "namespace", cluster.HostedNamespace)
	writeJSON(w, http.StatusCreated, HostedClusterCredentialResponse{
		TenantID:         tenantID,
		Cluster:          hostedClusterSummary(cluster),
		ConnectorToken:   token,
		HelmRelease:      hostedHelmRelease(cluster.ClusterID),
		ConnectorRBACSet: allowedNamespacesFlag([]string{cluster.HostedNamespace}),
		HelmCommand:      hostedHelmInstall(t.Endpoint, cluster.ClusterID, cluster.HostedNamespace),
	})
}

// HandleRotateHostedClusterCredential serves
// POST /v1/admin/tenants/{tenant_id}/hosted-clusters/{cluster_id}/credential
// and its browser-operator twin
// POST /v1/operator/tenants/{tenant_id}/hosted-clusters/{cluster_id}/credential:
// replace one hosted assignment's connector credential. The old credential
// stops authenticating immediately and any socket it holds here is evicted;
// the new plaintext appears in this response and nowhere else.
func (t *Tenants) HandleRotateHostedClusterCredential(w http.ResponseWriter, r *http.Request) {
	tenantID, clusterID := r.PathValue("tenant_id"), r.PathValue("cluster_id")
	if tenantID == "" || clusterID == "" {
		http.NotFound(w, r)
		return
	}
	cluster, token, err := t.Store.RotateHostedClusterCredential(tenantID, clusterID, operatorLifecycleActor(r, tenantID))
	if err != nil {
		t.writeHostedClusterError(w, "rotate hosted cluster credential", tenantID, err)
		return
	}
	t.Log.Info("hosted cluster credential rotated", "tenant", tenantID, "cluster", clusterID)
	writeJSON(w, http.StatusOK, HostedClusterCredentialResponse{
		TenantID:         tenantID,
		Cluster:          hostedClusterSummary(cluster),
		ConnectorToken:   token,
		HelmRelease:      hostedHelmRelease(cluster.ClusterID),
		ConnectorRBACSet: allowedNamespacesFlag([]string{cluster.HostedNamespace}),
		HelmCommand:      hostedHelmUpgrade(cluster.ClusterID),
	})
}

// HandleDeleteHostedCluster serves
// DELETE /v1/admin/tenants/{tenant_id}/hosted-clusters/{cluster_id} and its
// browser-operator twin
// DELETE /v1/operator/tenants/{tenant_id}/hosted-clusters/{cluster_id}: release
// the assignment — row, credential, namespace reservation and the namespace's
// entry in the tenant's authorized set, in one durable write. The uninstall
// of the release on the shared cluster is the operator's follow-up.
func (t *Tenants) HandleDeleteHostedCluster(w http.ResponseWriter, r *http.Request) {
	tenantID, clusterID := r.PathValue("tenant_id"), r.PathValue("cluster_id")
	if tenantID == "" || clusterID == "" {
		http.NotFound(w, r)
		return
	}
	namespace, err := t.Store.DeleteHostedCluster(tenantID, clusterID, operatorLifecycleActor(r, tenantID))
	if err != nil {
		t.writeHostedClusterError(w, "delete hosted cluster", tenantID, err)
		return
	}
	// Same follow-up as the tenant surface's cluster delete: the durable write
	// moved the intent, the reconciler converges the coordination server and
	// withdraws this gateway node's approvals off the request path.
	if t.Reconciler != nil {
		t.Reconciler.EnqueueClusterRemoval(tenantID, clusterID)
	}
	t.Log.Info("hosted cluster unassigned", "tenant", tenantID, "cluster", clusterID, "namespace", namespace)
	writeJSON(w, http.StatusOK, HostedClusterDeleteResponse{
		TenantID:        tenantID,
		ClusterID:       clusterID,
		HostedNamespace: namespace,
		Deleted:         true,
	})
}

type OperatorTenantRow struct {
	TenantID          string       `json:"tenant_id"`
	TenantName        string       `json:"tenant_name"`
	Plan              string       `json:"plan"`
	RunningBursts     int          `json:"running_bursts"`
	HourlyUSD         float64      `json:"hourly_usd"`
	ProjectedDailyUSD float64      `json:"projected_daily_usd"`
	Limits            TenantLimits `json:"limits"`
}

type OperatorTenantsResponse struct {
	Tenants   []OperatorTenantRow `json:"tenants"`
	NextAfter string              `json:"next_after,omitempty"`
}

type PatchTenantLimitsRequest struct {
	MaxConcurrentBursts *int     `json:"max_concurrent_bursts"`
	MaxHourlyUSD        *float64 `json:"max_hourly_usd"`
}

type PatchTenantLimitsResponse struct {
	Tenant  OperatorTenantRow `json:"tenant"`
	Changed bool              `json:"changed"`
}

const maxTenantLimitsRequestBytes = 4 << 10

func parseOperatorTenantQuery(raw string) (after string, limit int, err error) {
	values, err := url.ParseQuery(raw)
	if err != nil {
		return "", 0, errors.New("invalid query string")
	}
	for key, vals := range values {
		if key != "limit" && key != "after" {
			return "", 0, fmt.Errorf("unsupported query parameter %q; this route takes limit and after", key)
		}
		if len(vals) != 1 || vals[0] == "" {
			return "", 0, fmt.Errorf("%s must be given exactly once with a value", key)
		}
	}
	limit = state.DefaultOperatorTenantLimit
	if value := values.Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > state.MaxOperatorTenantLimit {
			return "", 0, fmt.Errorf("limit must be an integer between 1 and %d", state.MaxOperatorTenantLimit)
		}
	}
	return values.Get("after"), limit, nil
}

func buildOperatorTenantRow(store tenantStore, cust *state.Customer) OperatorTenantRow {
	var running []*state.Burst
	if store != nil {
		running = store.BurstsForCustomer(cust.ID)
	}
	spend := spendFromBursts(running, TenantLimits{
		MaxConcurrentBursts: cust.MaxConcurrentBursts,
		MaxHourlyUSD:        cust.MaxHourlyUSD,
	})
	return OperatorTenantRow{
		TenantID:          cust.ID,
		TenantName:        cust.Name,
		Plan:              cust.Plan,
		RunningBursts:     spend.RunningBursts,
		HourlyUSD:         spend.HourlyUSD,
		ProjectedDailyUSD: spend.ProjectedDailyUSD,
		Limits:            spend.Limits,
	}
}

// HandleListOperatorTenants serves GET /v1/operator/tenants: safe, paginated
// inventory of live tenants and their current spend guardrails and live usage.
func (t *Tenants) HandleListOperatorTenants(w http.ResponseWriter, r *http.Request) {
	after, limit, err := parseOperatorTenantQuery(r.URL.RawQuery)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	custs, nextAfter := t.Store.ListOperatorTenants(after, limit)
	rows := make([]OperatorTenantRow, 0, len(custs))
	for _, c := range custs {
		rows = append(rows, buildOperatorTenantRow(t.Store, c))
	}

	writeJSON(w, http.StatusOK, OperatorTenantsResponse{
		Tenants:   rows,
		NextAfter: nextAfter,
	})
}

// HandlePatchTenantLimits serves PATCH /v1/operator/tenants/{tenant_id}/limits:
// update a tenant's MaxConcurrentBursts and MaxHourlyUSD atomically.
func (t *Tenants) HandlePatchTenantLimits(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxTenantLimitsRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req PatchTenantLimitsRequest
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body must contain exactly one JSON object"})
		return
	}
	if req.MaxConcurrentBursts == nil || req.MaxHourlyUSD == nil {
		http.Error(w, "both max_concurrent_bursts and max_hourly_usd are required", http.StatusBadRequest)
		return
	}

	maxBursts := *req.MaxConcurrentBursts
	maxHourly := *req.MaxHourlyUSD

	if maxBursts < 0 {
		http.Error(w, "max_concurrent_bursts must be non-negative", http.StatusBadRequest)
		return
	}
	if maxHourly < 0 || math.IsNaN(maxHourly) || math.IsInf(maxHourly, 0) {
		http.Error(w, "max_hourly_usd must be finite and non-negative", http.StatusBadRequest)
		return
	}

	accountID := OperatorAccountFromContext(r.Context())
	if accountID == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "operator authentication required"})
		return
	}
	actor := state.HumanActor(accountID, tenantID)

	cust, changed, err := t.Store.SetCustomerLimits(tenantID, maxBursts, maxHourly, actor)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
			return
		}
		if t.Log != nil {
			t.Log.Error("operator: set tenant limits", "tenant", tenantID, "error", err)
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}

	row := buildOperatorTenantRow(t.Store, cust)
	writeJSON(w, http.StatusOK, PatchTenantLimitsResponse{
		Tenant:  row,
		Changed: changed,
	})
}
