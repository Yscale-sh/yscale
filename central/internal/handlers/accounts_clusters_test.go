// yscale:proprietary

package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// clustersMux wires the cluster routes exactly as the enterprise build does, so
// the test exercises the real pattern — including the requests that never reach
// a handler at all.
func clustersMux(a *Accounts) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/clusters", http.HandlerFunc(a.HandleListClusters))
	mux.Handle("POST /v1/tenants/{tenant_id}/clusters", http.HandlerFunc(a.HandleRegisterCluster))
	mux.Handle("POST /v1/tenants/{tenant_id}/clusters/{cluster_id}/credential", http.HandlerFunc(a.HandleRotateClusterCredential))
	mux.Handle("DELETE /v1/tenants/{tenant_id}/clusters/{cluster_id}", http.HandlerFunc(a.HandleDeleteCluster))
	mux.Handle("GET /v1/tenants/{tenant_id}/cluster-policy", http.HandlerFunc(a.HandleGetClusterPolicy))
	mux.Handle("PUT /v1/tenants/{tenant_id}/cluster-policy", http.HandlerFunc(a.HandlePutClusterPolicy))
	return mux
}

func callClusters(a *Accounts, method, path, token, body string) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	clustersMux(a).ServeHTTP(rec, req)
	return rec
}

func listClusters(a *Accounts, token, tenantID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/"+tenantID+"/clusters", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	clustersMux(a).ServeHTTP(rec, req)
	return rec
}

func getClusterPolicy(a *Accounts, token, tenantID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/"+tenantID+"/cluster-policy", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	clustersMux(a).ServeHTTP(rec, req)
	return rec
}

func putClusterPolicy(a *Accounts, token, tenantID, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, "/v1/tenants/"+tenantID+"/cluster-policy", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	clustersMux(a).ServeHTTP(rec, req)
	return rec
}

func decodeClusters(t *testing.T, rec *httptest.ResponseRecorder) TenantClustersResponse {
	t.Helper()
	var res TenantClustersResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode clusters response: %v (body %q)", err, rec.Body.String())
	}
	return res
}

func decodeClusterPolicy(t *testing.T, rec *httptest.ResponseRecorder) TenantClusterPolicyResponse {
	t.Helper()
	var res TenantClusterPolicyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode cluster policy response: %v (body %q)", err, rec.Body.String())
	}
	return res
}

// connectAgent claims the cluster and registers the socket in the order the
// agent stream does: bind first, AddAgent second.
func connectAgent(t *testing.T, store *state.Store, id, customerID, clusterID, version string, connectedAt time.Time) *state.Agent {
	t.Helper()
	if _, _, err := store.ClaimAgentCluster(customerID, clusterID, false, connectedAt); err != nil {
		t.Fatalf("claim %s/%s: %v", customerID, clusterID, err)
	}
	a := &state.Agent{
		ID: id, CustomerID: customerID, ClusterID: clusterID,
		AgentVersion: version, ConnectedAt: connectedAt,
		Send: make(chan protocol.Envelope, 1),
	}
	a.MarkSeen(connectedAt)
	store.AddAgent(a)
	return a
}

// A viewer reads their own tenant's fleet: the ids are what their own
// submissions have to name in X-Cluster-ID, so a read-only seat that cannot see
// them cannot submit. The rows are durable registry rows, deduped per cluster,
// joined with this replica's sockets — and the partial flag scopes to the LIVE
// join only, because a websocket lives on exactly one replica.
func TestListClustersIsReadableByEveryMember(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "vera": state.RoleViewer,
	})
	base := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	connectAgent(t, store, "agent_e1", "cust_alice", "cluster_east", "yscale-agent/v0", base)
	eastNew := connectAgent(t, store, "agent_e2", "cust_alice", "cluster_east", "yscale-agent/v1", base.Add(time.Hour))
	inventoryAt := time.Now().UTC()
	eastNew.ApplyClusterInventory(state.ClusterInventoryUpdate{
		NodeValid: true, NodeObserved: true, NodeObservedAt: inventoryAt,
		PodValid: true, PodObserved: true, PodObservedAt: inventoryAt,
	})
	connectAgent(t, store, "agent_w1", "cust_alice", "cluster_west", "yscale-agent/v1", base.Add(time.Minute))

	for _, caller := range []string{"human_alice", "human_vera"} {
		rec := listClusters(accounts, caller, "cust_alice")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200: %s", caller, rec.Code, rec.Body)
		}
		res := decodeClusters(t, rec)
		if !res.LivePartial {
			t.Errorf("%s: live_partial = false; the live join is this replica's sockets only", caller)
		}
		if res.TenantID != "cust_alice" {
			t.Errorf("%s: tenant = %q", caller, res.TenantID)
		}
		if len(res.Clusters) != 2 {
			t.Fatalf("%s: clusters = %+v, want one row per cluster", caller, res.Clusters)
		}
		east, west := res.Clusters[0], res.Clusters[1]
		if east.ClusterID != "cluster_east" || west.ClusterID != "cluster_west" {
			t.Fatalf("%s: rows are not ordered by cluster id: %+v", caller, res.Clusters)
		}
		if east.State != state.ClusterStateConnectedHere || east.Source != state.ClusterSourceClaimed {
			t.Errorf("%s: east state/source = %q/%q, want connected_here/claimed", caller, east.State, east.Source)
		}
		if east.Live == nil || east.Live.Connections != 2 {
			t.Fatalf("%s: east live = %+v, want 2 connections", caller, east.Live)
		}
		if east.Live.AgentVersion != "yscale-agent/v1" || !east.Live.ConnectedAt.Equal(base.Add(time.Hour)) {
			t.Errorf("%s: east row is not the newest connection: %+v", caller, east.Live)
		}
		if east.Live.LastSeen.IsZero() {
			t.Errorf("%s: east last_seen is unset", caller)
		}
		if !east.Live.InventoryObserved {
			t.Errorf("%s: east inventory_observed = false, want true", caller)
		}
		if !east.Live.NodeInventoryObserved || !east.Live.PodInventoryObserved {
			t.Errorf("%s: east per-scope inventory = node:%v pod:%v, want both true",
				caller, east.Live.NodeInventoryObserved, east.Live.PodInventoryObserved)
		}
		if east.Live.NodeInventoryObservedAt == nil || !east.Live.NodeInventoryObservedAt.Equal(inventoryAt) {
			t.Errorf("%s: east node_inventory_observed_at = %v, want newest inventory time",
				caller, east.Live.NodeInventoryObservedAt)
		}
		if east.Live.PodInventoryObservedAt == nil || !east.Live.PodInventoryObservedAt.Equal(inventoryAt) {
			t.Errorf("%s: east pod_inventory_observed_at = %v, want newest inventory time",
				caller, east.Live.PodInventoryObservedAt)
		}
		if east.Live.NodeCount == nil || east.Live.BurstCount == nil || east.Live.PendingPods == nil {
			t.Fatalf("%s: east observed inventory did not include zero count fields: %+v", caller, east.Live)
		}
		if *east.Live.NodeCount != 0 || *east.Live.BurstCount != 0 || *east.Live.PendingPods != 0 {
			t.Errorf("%s: east inventory = %d/%d/%d, want observed zero values",
				caller, *east.Live.NodeCount, *east.Live.BurstCount, *east.Live.PendingPods)
		}
		if west.Live == nil || west.Live.InventoryObserved || west.Live.NodeInventoryObserved || west.Live.PodInventoryObserved ||
			west.Live.NodeCount != nil || west.Live.BurstCount != nil || west.Live.PendingPods != nil ||
			west.Live.NodeInventoryObservedAt != nil || west.Live.PodInventoryObservedAt != nil {
			t.Errorf("%s: west unobserved live inventory = %+v, want flag false and omitted counts", caller, west.Live)
		}
		if east.FirstConnectedAt == nil || !east.FirstConnectedAt.Equal(base) {
			t.Errorf("%s: east first_connected_at = %v, want the claim time", caller, east.FirstConnectedAt)
		}
	}
}

func TestHeartbeatInventoryFlowsToClusterAPIAndExpires(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})
	now := time.Now().UTC()
	agent := connectAgent(t, store, "agent_live", "cust_alice", "cluster_live", "yscale-agent/v1", now)
	stream := &AgentStream{Log: quietLog()}

	stream.handleHeartbeat(agent, mustHeartbeatBody(t, protocol.Heartbeat{
		NodeInventoryObserved: true, NodeCount: 4, BurstCount: 1,
		PodInventoryObserved: true, PendingPods: 3,
	}), now)
	response := decodeClusters(t, listClusters(accounts, "human_alice", "cust_alice"))
	live := response.Clusters[0].Live
	if live == nil || !live.InventoryObserved || live.NodeCount == nil || *live.NodeCount != 4 ||
		live.BurstCount == nil || *live.BurstCount != 1 || live.PendingPods == nil || *live.PendingPods != 3 {
		t.Fatalf("fresh heartbeat inventory did not reach cluster JSON: %+v", live)
	}

	staleAt := now.Add(-3 * time.Minute)
	stream.handleHeartbeat(agent, mustHeartbeatBody(t, protocol.Heartbeat{
		NodeInventoryObserved: true, NodeInventoryObservedAt: heartbeatTimePointer(staleAt), NodeCount: 9, BurstCount: 2,
		PodInventoryObserved: true, PodInventoryObservedAt: heartbeatTimePointer(staleAt), PendingPods: 8,
	}), now)
	response = decodeClusters(t, listClusters(accounts, "human_alice", "cust_alice"))
	live = response.Clusters[0].Live
	if live == nil || live.InventoryObserved || live.NodeInventoryObserved || live.PodInventoryObserved ||
		live.NodeCount != nil || live.BurstCount != nil || live.PendingPods != nil {
		t.Fatalf("stale heartbeat inventory remained published: %+v", live)
	}
}

// The tenant scope is the point: another tenant's connectors are not readable,
// and the answer for a tenant that is not yours is the same as for one that
// does not exist — so the route cannot enumerate tenant ids.
func TestListClustersRefusesCrossTenantAndUnauthenticated(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_bob", Token: "ysk_bob", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})
	rosterFixture(t, store, "cust_bob", map[string]string{"bob": state.RoleOwner})
	connectAgent(t, store, "agent_bob", "cust_bob", "cluster_bob", "yscale-agent/v1", time.Now().UTC())

	rec := listClusters(accounts, "human_alice", "cust_bob")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant read = %d, want 404: %s", rec.Code, rec.Body)
	}
	if body := rec.Body.String(); strings.Contains(body, "cluster_bob") {
		t.Fatalf("ISOLATION BREACH: another tenant's cluster id leaked: %s", body)
	}
	if code := listClusters(accounts, "human_alice", "cust_nope").Code; code != http.StatusNotFound {
		t.Errorf("unknown tenant = %d, want the same 404 a non-member gets", code)
	}

	// A cluster credential is not an identity, and no credential is not either.
	if code := listClusters(accounts, "ysk_alice", "cust_alice").Code; code != http.StatusUnauthorized {
		t.Errorf("cluster token on the cluster route = %d, want 401", code)
	}
	if code := listClusters(accounts, "", "cust_alice").Code; code != http.StatusUnauthorized {
		t.Errorf("unauthenticated read = %d, want 401", code)
	}

	// Without Yscale ID the route does not exist on this deployment, exactly as
	// the rest of the human surface does not.
	disabled := &Accounts{Store: store, Issuer: testIssuer, Log: quietLog()}
	if code := listClusters(disabled, "human_alice", "cust_alice").Code; code != http.StatusNotFound {
		t.Errorf("cluster route on an unconfigured deployment = %d, want 404", code)
	}
}

func TestClusterPolicyGETPUTRBACIsolationAndInventoryEligibility(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_bob", Token: "ysk_bob", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "adm": state.RoleAdmin, "mem": state.RoleMember, "view": state.RoleViewer,
	})
	rosterFixture(t, store, "cust_bob", map[string]string{"bob": state.RoleOwner})
	base := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	connectAgent(t, store, "agent_east", "cust_alice", "cluster_east", "yscale-agent/v1", base)
	connectAgent(t, store, "agent_west", "cust_alice", "cluster_west", "yscale-agent/v1", base.Add(time.Minute))
	connectAgent(t, store, "agent_bob", "cust_bob", "cluster_bob", "yscale-agent/v1", base)

	if rec := getClusterPolicy(accounts, "human_view", "cust_alice"); rec.Code != http.StatusOK {
		t.Fatalf("viewer get policy = %d, want 200: %s", rec.Code, rec.Body)
	} else if got := decodeClusterPolicy(t, rec); got.Policy.Auto != state.ClusterPolicyAutoRequirePin {
		t.Fatalf("default policy = %+v", got.Policy)
	}
	if rec := putClusterPolicy(accounts, "human_mem", "cust_alice", `{"auto":"ordered"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("member put policy = %d, want 403: %s", rec.Code, rec.Body)
	}
	if rec := putClusterPolicy(accounts, "human_alice", "cust_bob", `{"auto":"ordered"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant put policy = %d, want 404: %s", rec.Code, rec.Body)
	}
	if rec := putClusterPolicy(accounts, "human_alice", "cust_alice", `{"allow":["cluster_east"],"deny":["cluster_east"]}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid policy = %d, want 400: %s", rec.Code, rec.Body)
	}

	rec := putClusterPolicy(accounts, "human_adm", "cust_alice", `{"allow":["cluster_west"],"deny":["cluster_east"],"auto":"ordered"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin put policy = %d, want 200: %s", rec.Code, rec.Body)
	}
	written := decodeClusterPolicy(t, rec)
	if !written.Changed || written.Role != state.RoleAdmin || written.Policy.Auto != state.ClusterPolicyAutoOrdered {
		t.Fatalf("written policy response = %+v", written)
	}
	if _, _, err := store.TenantClusterPolicyFor("cust_alice", ids["alice"]); err != nil {
		t.Fatalf("policy was not readable from store: %v", err)
	}

	list := listClusters(accounts, "human_view", "cust_alice")
	if list.Code != http.StatusOK {
		t.Fatalf("list clusters = %d, want 200: %s", list.Code, list.Body)
	}
	clusters := decodeClusters(t, list)
	if clusters.Policy.Auto != state.ClusterPolicyAutoOrdered || len(clusters.Clusters) != 2 {
		t.Fatalf("clusters response = %+v", clusters)
	}
	if clusters.Clusters[0].ClusterID != "cluster_east" || clusters.Clusters[0].Eligible || clusters.Clusters[0].Reason != "denied" {
		t.Fatalf("denied row hidden or mislabeled: %+v", clusters.Clusters)
	}
	if clusters.Clusters[1].ClusterID != "cluster_west" || !clusters.Clusters[1].Eligible {
		t.Fatalf("allowed row mislabeled: %+v", clusters.Clusters)
	}

	again := putClusterPolicy(accounts, "human_alice", "cust_alice", `{"allow":["cluster_west"],"deny":["cluster_east"],"auto":"ordered"}`)
	if again.Code != http.StatusOK {
		t.Fatalf("same policy put = %d: %s", again.Code, again.Body)
	}
	if got := decodeClusterPolicy(t, again); got.Changed {
		t.Fatalf("same policy reported changed: %+v", got)
	}
}

func decodeCredential(t *testing.T, rec *httptest.ResponseRecorder) TenantClusterCredentialResponse {
	t.Helper()
	var res TenantClusterCredentialResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode credential response: %v (body %q)", err, rec.Body.String())
	}
	return res
}

// The registration lifecycle a non-coder owner walks: register with a name,
// receive the connector credential once, see the cluster in the fleet as
// never_connected, rotate when the credential leaks, and delete when the
// cluster is retired. Roles hold at every step, and no response ever carries
// a credential hash.
func TestRegisterRotateDeleteClusterRoutes(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "adm": state.RoleAdmin, "mem": state.RoleMember, "view": state.RoleViewer,
	})

	// Member and viewer cannot register; owner can, and gets the credential
	// exactly once, plus an install command that does NOT embed it.
	for _, denied := range []string{"human_mem", "human_view"} {
		if rec := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_alice/clusters", denied, `{"name":"Prod"}`); rec.Code != http.StatusForbidden {
			t.Fatalf("%s register = %d, want 403: %s", denied, rec.Code, rec.Body)
		}
	}
	rec := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_alice/clusters", "human_alice", `{"cluster_id":"cl-prod","name":"Prod"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("owner register = %d, want 201: %s", rec.Code, rec.Body)
	}
	created := decodeCredential(t, rec)
	if created.Cluster.ClusterID != "cl-prod" || created.Cluster.Name != "Prod" ||
		created.Cluster.State != state.ClusterStateNeverConnected || created.Cluster.Source != state.ClusterSourceRegistered {
		t.Fatalf("created cluster = %+v", created.Cluster)
	}
	if created.ConnectorToken == "" {
		t.Fatal("registration did not reveal the connector credential")
	}
	if strings.Contains(created.HelmCommand, created.ConnectorToken) {
		t.Fatalf("helm command embeds the credential: %s", created.HelmCommand)
	}
	if !strings.Contains(created.HelmCommand, "cl-prod") || !strings.Contains(created.HelmCommand, "$YSCALE_CONNECTOR_TOKEN") {
		t.Fatalf("helm command = %q", created.HelmCommand)
	}
	if !strings.HasPrefix(created.HelmCommand, "helm install ") {
		t.Fatalf("registration helm command = %q, want an install", created.HelmCommand)
	}
	// The hash never rides ANY response on this surface.
	hash := state.HashClusterCredential(created.ConnectorToken)
	if strings.Contains(rec.Body.String(), hash) {
		t.Fatal("credential hash leaked into the registration response")
	}

	// A member can SEE the fleet row; the row carries no credential material.
	list := listClusters(accounts, "human_mem", "cust_alice")
	if list.Code != http.StatusOK {
		t.Fatalf("member list = %d: %s", list.Code, list.Body)
	}
	if body := list.Body.String(); strings.Contains(body, hash) || strings.Contains(body, created.ConnectorToken) {
		t.Fatal("credential material leaked into the cluster list")
	}
	fleet := decodeClusters(t, list)
	if len(fleet.Clusters) != 1 || fleet.Clusters[0].State != state.ClusterStateNeverConnected {
		t.Fatalf("fleet = %+v, want the registered never_connected row", fleet.Clusters)
	}

	// Duplicate id (even from another tenant) is a 409; bad name a 400.
	if rec := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_alice/clusters", "human_adm", `{"cluster_id":"cl-prod","name":"Again"}`); rec.Code != http.StatusConflict {
		t.Fatalf("duplicate register = %d, want 409: %s", rec.Code, rec.Body)
	}
	if rec := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_alice/clusters", "human_adm", `{"name":"   "}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("blank name = %d, want 400: %s", rec.Code, rec.Body)
	}

	// Rotation: admin may, viewer may not; the new credential authenticates
	// and the old one is dead.
	if rec := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_alice/clusters/cl-prod/credential", "human_view", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("viewer rotate = %d, want 403: %s", rec.Code, rec.Body)
	}
	rot := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_alice/clusters/cl-prod/credential", "human_adm", "")
	if rot.Code != http.StatusOK {
		t.Fatalf("admin rotate = %d, want 200: %s", rot.Code, rot.Body)
	}
	rotated := decodeCredential(t, rot)
	if rotated.ConnectorToken == "" || rotated.ConnectorToken == created.ConnectorToken {
		t.Fatal("rotation did not mint a fresh credential")
	}
	// Rotation hands back an UPGRADE of the already-installed release — same
	// shipped chart, values preserved — never a second install.
	if !strings.HasPrefix(rotated.HelmCommand, "helm upgrade ") || !strings.Contains(rotated.HelmCommand, "--reuse-values") {
		t.Fatalf("rotation helm command = %q, want an upgrade with --reuse-values", rotated.HelmCommand)
	}
	if !strings.Contains(rotated.HelmCommand, " ./deploy/helm/yscale-agent ") ||
		strings.Contains(rotated.HelmCommand, "oci://") ||
		!strings.Contains(rotated.HelmCommand, "--namespace yscale-system") {
		t.Fatalf("rotation helm command left the shipped chart/release shape: %q", rotated.HelmCommand)
	}
	if !strings.Contains(rotated.HelmCommand, "$YSCALE_CONNECTOR_TOKEN") || strings.Contains(rotated.HelmCommand, rotated.ConnectorToken) {
		t.Fatalf("rotation helm command mishandles the credential: %q", rotated.HelmCommand)
	}
	if _, _, err := store.AuthClusterCredential(created.ConnectorToken); err == nil {
		t.Fatal("old credential still authenticates after rotation")
	}
	if _, bound, err := store.AuthClusterCredential(rotated.ConnectorToken); err != nil || bound != "cl-prod" {
		t.Fatalf("rotated credential = (%q,%v)", bound, err)
	}
	if rec := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_alice/clusters/cl-gone/credential", "human_alice", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("rotate unknown cluster = %d, want 404: %s", rec.Code, rec.Body)
	}

	// Delete: refused while the policy names the cluster, then allowed, and
	// the credential dies with the row.
	if rec := putClusterPolicy(accounts, "human_alice", "cust_alice", `{"allow":["cl-prod"]}`); rec.Code != http.StatusOK {
		t.Fatalf("set policy = %d: %s", rec.Code, rec.Body)
	}
	if rec := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_alice/clusters/cl-prod", "human_alice", ""); rec.Code != http.StatusConflict {
		t.Fatalf("delete of policy-named cluster = %d, want 409: %s", rec.Code, rec.Body)
	}
	if rec := putClusterPolicy(accounts, "human_alice", "cust_alice", `{}`); rec.Code != http.StatusOK {
		t.Fatalf("clear policy = %d: %s", rec.Code, rec.Body)
	}
	if rec := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_alice/clusters/cl-prod", "human_mem", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("member delete = %d, want 403: %s", rec.Code, rec.Body)
	}
	del := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_alice/clusters/cl-prod", "human_alice", "")
	if del.Code != http.StatusOK {
		t.Fatalf("owner delete = %d, want 200: %s", del.Code, del.Body)
	}
	if _, _, err := store.AuthClusterCredential(rotated.ConnectorToken); err == nil {
		t.Fatal("credential survived its cluster's deletion")
	}
	if rec := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_alice/clusters/cl-prod", "human_alice", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("second delete = %d, want 404: %s", rec.Code, rec.Body)
	}
}

// The write routes are tenant-scoped exactly as the reads are: another
// tenant's manager gets the same 404 a stranger gets, a cluster token is not
// an identity, and an unconfigured deployment has no routes at all. A
// registered cluster that has disconnected REMAINS in the fleet.
func TestClusterRoutesIsolationAndDisconnectedFleet(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_bob", Token: "ysk_bob", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})
	rosterFixture(t, store, "cust_bob", map[string]string{"bob": state.RoleOwner})

	// Cross-tenant writes are the non-member 404, disclosing nothing.
	if rec := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_bob/clusters", "human_alice", `{"name":"Steal"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant register = %d, want 404: %s", rec.Code, rec.Body)
	}
	if rec := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_bob/clusters/cl-b", "human_alice", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant delete = %d, want 404: %s", rec.Code, rec.Body)
	}
	// Credential-shaped strangers.
	if rec := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_alice/clusters", "ysk_alice", `{"name":"X"}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("cluster token on register = %d, want 401", rec.Code)
	}
	disabled := &Accounts{Store: store, Issuer: testIssuer, Log: quietLog()}
	if rec := callClusters(disabled, http.MethodPost, "/v1/tenants/cust_alice/clusters", "human_alice", `{"name":"X"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("register on unconfigured deployment = %d, want 404", rec.Code)
	}

	// A registered cluster connects, disconnects, and STAYS a fleet member —
	// reported disconnected with its lifecycle stamps, and still refusable as
	// a workload target while offline.
	now := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	connectAgent(t, store, "agent_d", "cust_alice", "cl-flaky", "yscale-agent/v1", now)
	store.RemoveAgent("agent_d")
	store.MarkClusterDisconnected("cust_alice", "cl-flaky", now.Add(time.Hour), now.Add(50*time.Minute))

	fleet := decodeClusters(t, listClusters(accounts, "human_alice", "cust_alice"))
	if len(fleet.Clusters) != 1 {
		t.Fatalf("fleet = %+v, want the disconnected cluster still present", fleet.Clusters)
	}
	row := fleet.Clusters[0]
	if row.ClusterID != "cl-flaky" || row.State != state.ClusterStateDisconnected || row.Live != nil {
		t.Fatalf("disconnected row = %+v", row)
	}
	if row.LastDisconnectedAt == nil || !row.LastDisconnectedAt.Equal(now.Add(time.Hour)) ||
		row.LastObservedAt == nil || !row.LastObservedAt.Equal(now.Add(50*time.Minute)) {
		t.Fatalf("disconnected stamps = %+v", row)
	}
	if !fleet.LivePartial {
		t.Fatal("live_partial = false; the live join never claims global liveness")
	}
}

// A connector credential now submits and cancels its own cluster's work, and
// that is the whole widening: the human account surface is unchanged and still
// requires an identity, so the credential cannot read the fleet, register a
// cluster, or mint itself a new one.
func TestConnectorCredentialCannotReachTheClusterAccountSurface(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})
	_, connectorToken, _, err := store.RegisterTenantCluster("cust_alice", ids["alice"], "cl-alice", "Alice", state.HumanActor(ids["alice"], "cust_alice"))
	if err != nil {
		t.Fatalf("RegisterTenantCluster: %v", err)
	}

	routes := []struct{ method, path, body string }{
		{http.MethodGet, "/v1/tenants/cust_alice/clusters", ""},
		{http.MethodPost, "/v1/tenants/cust_alice/clusters", `{"cluster_id":"cl-new"}`},
		{http.MethodPost, "/v1/tenants/cust_alice/clusters/cl-alice/credential", ""},
		{http.MethodDelete, "/v1/tenants/cust_alice/clusters/cl-alice", ""},
		{http.MethodGet, "/v1/tenants/cust_alice/cluster-policy", ""},
	}
	for _, route := range routes {
		rec := callClusters(accounts, route.method, route.path, connectorToken, route.body)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("connector credential on %s %s = %d, want 401: %s", route.method, route.path, rec.Code, rec.Body)
		}
	}

	// Nothing the refusals touched happened: the fleet is the one cluster, and
	// the credential the owner holds is still the one that was minted.
	fleet := decodeClusters(t, listClusters(accounts, "human_alice", "cust_alice"))
	if len(fleet.Clusters) != 1 || fleet.Clusters[0].ClusterID != "cl-alice" {
		t.Fatalf("fleet after the refusals = %+v, want the single registered cluster", fleet.Clusters)
	}
	if _, clusterID, err := store.AuthClusterCredential(connectorToken); err != nil || clusterID != "cl-alice" {
		t.Fatalf("credential after the refusals = %q err=%v, want cl-alice", clusterID, err)
	}
}
