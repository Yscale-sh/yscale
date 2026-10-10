package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// TenantClusterSummary is one durable registry row of the tenant's fleet,
// joined with the live sockets this replica holds. It renders the store's
// TenantCluster and NOTHING off the registry row itself — the credential hash
// in particular has no field here to leak into, by construction.
//
// State is one of never_connected, connected_here, disconnected.
// connected_here is deliberately not "connected": a websocket lives on exactly
// one central replica, so this response can attest a socket it holds and
// nothing about global liveness — disconnected means only "no socket here and
// it has connected before".
type TenantClusterSummary struct {
	ClusterID    string    `json:"cluster_id"`
	Name         string    `json:"name,omitempty"`
	Source       string    `json:"source"`
	State        string    `json:"state"`
	RegisteredAt time.Time `json:"registered_at"`

	// HostedNamespace travels on platform-managed hosted rows only: it is the
	// namespace this tenant's submissions to the hosted cluster must name.
	// Absent on registered and claimed rows, so the pre-hosted list shape is
	// unchanged.
	HostedNamespace string `json:"hosted_namespace,omitempty"`

	FirstConnectedAt   *time.Time `json:"first_connected_at,omitempty"`
	LastConnectedAt    *time.Time `json:"last_connected_at,omitempty"`
	LastDisconnectedAt *time.Time `json:"last_disconnected_at,omitempty"`
	LastObservedAt     *time.Time `json:"last_observed_at,omitempty"`

	// Live is the socket observation on THIS replica; absent when none.
	Live *TenantClusterLive `json:"live,omitempty"`

	Eligible bool   `json:"eligible"`
	Reason   string `json:"reason,omitempty"`
}

// TenantClusterLive is one cluster's live-socket facts. Connections is how
// many sockets that cluster holds here. Normally 1; a 2 is a connector that
// reconnected before central reaped the old socket, which is worth showing
// rather than hiding because it is also what a second connector deployed with
// a copied credential looks like.
type TenantClusterLive struct {
	ConnectedAt             time.Time  `json:"connected_at"`
	LastSeen                time.Time  `json:"last_seen"`
	AgentVersion            string     `json:"agent_version,omitempty"`
	Connections             int        `json:"connections"`
	InventoryObserved       bool       `json:"inventory_observed"`
	NodeInventoryObserved   bool       `json:"node_inventory_observed"`
	NodeInventoryObservedAt *time.Time `json:"node_inventory_observed_at,omitempty"`
	PodInventoryObserved    bool       `json:"pod_inventory_observed"`
	PodInventoryObservedAt  *time.Time `json:"pod_inventory_observed_at,omitempty"`
	NodeCount               *int       `json:"node_count,omitempty"`
	BurstCount              *int       `json:"burst_count,omitempty"`
	PendingPods             *int       `json:"pending_pods,omitempty"`
}

// TenantClustersResponse lists the tenant's durable cluster fleet. The ROWS
// are complete — a registered cluster stays in the fleet while disconnected —
// which is what the old always-partial socket census could not say.
//
// LivePartial is ALWAYS true and scopes to the LIVE observation only: the
// per-row Live join covers the sockets THIS replica holds, so on a
// multi-replica deployment a cluster reported disconnected may be
// connected_here on another replica. Callers must not read State as global
// liveness; the registry rows themselves are not partial.
type TenantClustersResponse struct {
	TenantID    string                 `json:"tenant_id"`
	ObservedAt  time.Time              `json:"observed_at"`
	Policy      state.ClusterPolicy    `json:"policy"`
	Clusters    []TenantClusterSummary `json:"clusters"`
	LivePartial bool                   `json:"live_partial"`
}

type TenantClusterPolicyResponse struct {
	TenantID string              `json:"tenant_id"`
	Policy   state.ClusterPolicy `json:"policy"`
	Role     string              `json:"role,omitempty"`
	Changed  bool                `json:"changed,omitempty"`
}

type tenantClusterPolicyRequest struct {
	Allow []string `json:"allow"`
	Deny  []string `json:"deny"`
	Auto  string   `json:"auto"`
}

// tenantClusterSummary renders one store row for the response. Live facts
// travel only when the store saw a socket here.
func tenantClusterSummary(c state.TenantCluster, policy state.ClusterPolicy) TenantClusterSummary {
	eligible, reason := clusterPolicyEligible(policy, c.ClusterID)
	row := TenantClusterSummary{
		ClusterID:          c.ClusterID,
		Name:               c.Name,
		Source:             c.Source,
		State:              c.State,
		RegisteredAt:       c.RegisteredAt,
		HostedNamespace:    c.HostedNamespace,
		FirstConnectedAt:   c.FirstConnectedAt,
		LastConnectedAt:    c.LastConnectedAt,
		LastDisconnectedAt: c.LastDisconnectedAt,
		LastObservedAt:     c.LastObservedAt,
		Eligible:           eligible,
		Reason:             reason,
	}
	if c.Connections > 0 {
		row.Live = &TenantClusterLive{
			ConnectedAt:           c.ConnectedAt,
			LastSeen:              c.LastSeen,
			AgentVersion:          c.AgentVersion,
			Connections:           c.Connections,
			InventoryObserved:     c.InventoryObserved,
			NodeInventoryObserved: c.NodeInventoryObserved,
			PodInventoryObserved:  c.PodInventoryObserved,
		}
		if c.NodeInventoryObserved {
			nodeCount, burstCount, observedAt := c.NodeCount, c.BurstCount, c.NodeInventoryObservedAt
			row.Live.NodeCount = &nodeCount
			row.Live.BurstCount = &burstCount
			row.Live.NodeInventoryObservedAt = &observedAt
		}
		if c.PodInventoryObserved {
			pendingPods, observedAt := c.PendingPods, c.PodInventoryObservedAt
			row.Live.PendingPods = &pendingPods
			row.Live.PodInventoryObservedAt = &observedAt
		}
	}
	return row
}

// HandleListClusters serves GET /v1/tenants/{tenant_id}/clusters.
//
// Same credential, same authority and same failure shape as the usage read:
// every member may see it, viewers included — the cluster ids are what their
// own submissions have to name, and a seat that cannot read them cannot submit
// — and no role sees another tenant's, which is why the store authorizes the
// read against the caller's grant in the lock it takes the rows in.
func (a *Accounts) HandleListClusters(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	rows, err := a.Store.TenantClustersFor(tenantID, caller.ID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			// Unknown tenant, revoked tenant and non-member are one answer, so
			// this route cannot be used to enumerate tenant ids either.
			tenantNotFound(w)
			return
		}
		a.Log.Error("account: read tenant clusters", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	policy, _, err := a.Store.TenantClusterPolicyFor(tenantID, caller.ID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			tenantNotFound(w)
			return
		}
		a.Log.Error("account: read tenant cluster policy", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	clusters := make([]TenantClusterSummary, 0, len(rows))
	for _, c := range rows {
		clusters = append(clusters, tenantClusterSummary(c, policy))
	}
	writeJSON(w, http.StatusOK, TenantClustersResponse{
		TenantID:    tenantID,
		ObservedAt:  time.Now().UTC(),
		Policy:      policy,
		Clusters:    clusters,
		LivePartial: true,
	})
}

func (a *Accounts) HandleGetClusterPolicy(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	policy, role, err := a.Store.TenantClusterPolicyFor(tenantID, caller.ID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			tenantNotFound(w)
			return
		}
		a.Log.Error("account: read tenant cluster policy", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, TenantClusterPolicyResponse{TenantID: tenantID, Policy: policy, Role: role})
}

func (a *Accounts) HandlePutClusterPolicy(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req tenantClusterPolicyRequest
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cluster policy JSON"})
		}
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cluster policy JSON"})
		return
	}
	policy, changed, role, err := a.Store.SetTenantClusterPolicy(tenantID, caller.ID, state.ClusterPolicy{
		Allow: req.Allow,
		Deny:  req.Deny,
		Auto:  req.Auto,
	}, state.HumanActor(caller.ID, tenantID))
	if err != nil {
		switch {
		case errors.Is(err, state.ErrNotFound):
			tenantNotFound(w)
		case errors.Is(err, state.ErrNotAuthorized):
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing cluster policy requires owner or admin"})
		case errors.Is(err, state.ErrInvalidClusterPolicy):
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		default:
			a.Log.Error("account: set tenant cluster policy", "tenant", tenantID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		}
		return
	}
	writeJSON(w, http.StatusOK, TenantClusterPolicyResponse{TenantID: tenantID, Policy: policy, Role: role, Changed: changed})
}

// registerClusterRequest is the POST body. cluster_id is optional — central
// generates one when it is absent, which is the non-coder path — and name is
// the required display name.
type registerClusterRequest struct {
	ClusterID string `json:"cluster_id,omitempty"`
	Name      string `json:"name"`
}

// TenantClusterCredentialResponse answers the two writes that mint a
// connector credential. ConnectorToken is the ONLY place the plaintext ever
// appears — central stores its SHA-256 and nothing else, so this response
// cannot be re-fetched; losing the value means rotating.
//
// HelmCommand is a copyable helm command — install on registration, upgrade
// on rotation — that deliberately does NOT embed the token: it references the
// YSCALE_CONNECTOR_TOKEN environment variable, so pasting the command into a
// shell, a runbook or a ticket never carries the secret with it. The
// structured cluster_id/connector_token fields are the source of truth; the
// command is a convenience rendering of them. The JSON key stays helm_install
// so existing consumers keep rendering it; the console payload rename rides
// the website migration.
type TenantClusterCredentialResponse struct {
	TenantID       string               `json:"tenant_id"`
	Cluster        TenantClusterSummary `json:"cluster"`
	ConnectorToken string               `json:"connector_token"`
	HelmCommand    string               `json:"helm_install,omitempty"`
	Role           string               `json:"role,omitempty"`
}

// clusterHelmInstall renders the connector install command for one registered
// cluster. clusterID is already validated against the cluster-id grammar
// (shell-inert by construction); the endpoint is operator config and gets the
// same quoting the admin provision path applies. The token is an env-var
// reference on purpose — see TenantClusterCredentialResponse.
func clusterHelmInstall(endpoint, clusterID string) string {
	cmd := "helm install yscale-agent " + agentChartRef + " " +
		"--namespace yscale-system --create-namespace " +
		"--set clusterID=" + clusterID + " --set token=\"$YSCALE_CONNECTOR_TOKEN\""
	if endpoint != "" {
		cmd += " --set endpoint=" + shellQuote(endpoint)
	}
	return cmd
}

// clusterHelmUpgrade renders the command a credential rotation hands back:
// same release, same chart, but an UPGRADE with --reuse-values so
// the operator's endpoint and overrides survive, with the rotated token as an
// explicit override. clusterID is re-asserted because a claimed row's
// connector may have been installed before the chart carried that value.
func clusterHelmUpgrade(clusterID string) string {
	return "helm upgrade yscale-agent " + agentChartRef + " " +
		"--namespace yscale-system --reuse-values " +
		"--set clusterID=" + clusterID + " --set token=\"$YSCALE_CONNECTOR_TOKEN\""
}

// writeClusterWriteError maps the store's cluster-registry refusals onto the
// human surface's uniform answers. The order matters: ErrClusterNotFound
// wraps ErrNotFound, and a proven member asking about a cluster the tenant
// does not hold is told THAT — the generic tenant 404 would send them
// doubting a grant they demonstrably have.
func (a *Accounts) writeClusterWriteError(w http.ResponseWriter, op, tenantID string, err error) {
	switch {
	case errors.Is(err, state.ErrClusterNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such registered cluster"})
	case errors.Is(err, state.ErrNotFound):
		tenantNotFound(w)
	case errors.Is(err, state.ErrNotAuthorized):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing clusters requires owner or admin"})
	case errors.Is(err, state.ErrInvalidCluster):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, state.ErrClusterExists):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "cluster id is already in use"})
	case errors.Is(err, state.ErrClusterLimit):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, state.ErrClusterNamedByPolicy):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "cluster is still named by the tenant cluster policy; remove it from allow/deny first"})
	case errors.Is(err, state.ErrClusterPlatformManaged):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "cluster is platform-managed hosted capacity; its credential and lifecycle are managed by the Yscale operator"})
	default:
		a.Log.Error("account: "+op, "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
	}
}

// HandleRegisterCluster serves POST /v1/tenants/{tenant_id}/clusters: add one
// cluster to the tenant's durable registry and mint its connector credential.
// Owner/admin only — the store decides that under the same lock it writes in.
// The response is the single reveal of the credential.
func (a *Accounts) HandleRegisterCluster(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req registerClusterRequest
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cluster registration JSON"})
		}
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cluster registration JSON"})
		return
	}
	cluster, token, role, err := a.Store.RegisterTenantCluster(tenantID, caller.ID, req.ClusterID, req.Name, state.HumanActor(caller.ID, tenantID))
	if err != nil {
		a.writeClusterWriteError(w, "register tenant cluster", tenantID, err)
		return
	}
	policy, _, perr := a.Store.TenantClusterPolicyFor(tenantID, caller.ID)
	if perr != nil {
		policy = state.ClusterPolicy{Auto: state.ClusterPolicyAutoRequirePin}
	}
	writeJSON(w, http.StatusCreated, TenantClusterCredentialResponse{
		TenantID:       tenantID,
		Cluster:        tenantClusterSummary(cluster, policy),
		ConnectorToken: token,
		HelmCommand:    clusterHelmInstall(a.Endpoint, cluster.ClusterID),
		Role:           role,
	})
}

// HandleRotateClusterCredential serves
// POST /v1/tenants/{tenant_id}/clusters/{cluster_id}/credential: replace one
// registered cluster's connector credential. The old credential stops
// authenticating immediately; the new plaintext appears in this response and
// nowhere else.
func (a *Accounts) HandleRotateClusterCredential(w http.ResponseWriter, r *http.Request) {
	tenantID, clusterID := r.PathValue("tenant_id"), r.PathValue("cluster_id")
	if tenantID == "" || clusterID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	cluster, token, role, err := a.Store.RotateTenantClusterCredential(tenantID, caller.ID, clusterID, state.HumanActor(caller.ID, tenantID))
	if err != nil {
		a.writeClusterWriteError(w, "rotate tenant cluster credential", tenantID, err)
		return
	}
	policy, _, perr := a.Store.TenantClusterPolicyFor(tenantID, caller.ID)
	if perr != nil {
		policy = state.ClusterPolicy{Auto: state.ClusterPolicyAutoRequirePin}
	}
	writeJSON(w, http.StatusOK, TenantClusterCredentialResponse{
		TenantID:       tenantID,
		Cluster:        tenantClusterSummary(cluster, policy),
		ConnectorToken: token,
		HelmCommand:    clusterHelmUpgrade(cluster.ClusterID),
		Role:           role,
	})
}

// TenantClusterDeleteResponse confirms a removal. Nothing of the row travels
// back: the caller named the id, and the credential it invalidated is not a
// thing to echo.
type TenantClusterDeleteResponse struct {
	TenantID  string `json:"tenant_id"`
	ClusterID string `json:"cluster_id"`
	Deleted   bool   `json:"deleted"`
	Role      string `json:"role,omitempty"`
}

// HandleDeleteCluster serves DELETE /v1/tenants/{tenant_id}/clusters/{cluster_id}:
// remove one cluster from the registry and invalidate its credential. A
// cluster the tenant's allow/deny policy still names is refused with 409 —
// the policy edit comes first, so deleting a cluster can never silently move
// an authorization boundary.
func (a *Accounts) HandleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	tenantID, clusterID := r.PathValue("tenant_id"), r.PathValue("cluster_id")
	if tenantID == "" || clusterID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	role, err := a.Store.DeleteTenantCluster(tenantID, caller.ID, clusterID, state.HumanActor(caller.ID, tenantID))
	if err != nil {
		a.writeClusterWriteError(w, "delete tenant cluster", tenantID, err)
		return
	}
	// The durable write already dropped this cluster's routes and recomputed
	// the tenant's union. Converging the coordination server to it — and taking
	// back what its own gateway node was approved for — is the reconciler's,
	// and it runs off this request rather than inside it.
	if a.Reconciler != nil {
		a.Reconciler.EnqueueClusterRemoval(tenantID, clusterID)
	}
	writeJSON(w, http.StatusOK, TenantClusterDeleteResponse{
		TenantID:  tenantID,
		ClusterID: clusterID,
		Deleted:   true,
		Role:      role,
	})
}
