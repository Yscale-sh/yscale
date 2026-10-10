// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/central/internal/state"
)

// hostedAdminMux wires the operator hosted routes exactly as the enterprise
// build does, so the tests exercise the real path-value pattern.
func hostedAdminMux(tn *Tenants) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/admin/hosted-capacity/requests", http.HandlerFunc(tn.HandleListHostedCapacityRequests))
	mux.Handle("GET /v1/admin/hosted-clusters", http.HandlerFunc(tn.HandleListHostedClusters))
	mux.Handle("POST /v1/admin/tenants/{tenant_id}/hosted-clusters", http.HandlerFunc(tn.HandleAssignHostedCluster))
	mux.Handle("POST /v1/admin/tenants/{tenant_id}/hosted-clusters/{cluster_id}/credential", http.HandlerFunc(tn.HandleRotateHostedClusterCredential))
	mux.Handle("DELETE /v1/admin/tenants/{tenant_id}/hosted-clusters/{cluster_id}", http.HandlerFunc(tn.HandleDeleteHostedCluster))
	return mux
}

func callHosted(tn *Tenants, method, path, body string) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	rec := httptest.NewRecorder()
	hostedAdminMux(tn).ServeHTTP(rec, httptest.NewRequest(method, path, rd))
	return rec
}

func decodeHostedCredential(t *testing.T, rec *httptest.ResponseRecorder) HostedClusterCredentialResponse {
	t.Helper()
	var res HostedClusterCredentialResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode hosted response: %v (body %q)", err, rec.Body.String())
	}
	return res
}

// The assignment response is everything the operator needs and nothing more:
// the one-time credential, the safe row metadata, and a per-tenant release
// whose helm command pins the tenant's virtual cluster id and scopes
// rbac.allowedNamespaces to exactly the reserved namespace — with the chart's
// namespaced RBAC default left in place and the CRD ownership left to the
// shared cluster's platform baseline.
func TestAssignHostedClusterRendersScopedPerTenantRelease(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro"})
	tn := newTenants(store)

	rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_h/hosted-clusters",
		`{"cluster_id":"hosted-h","namespace":"ys-h"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("assign = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}
	res := decodeHostedCredential(t, rec)
	if res.TenantID != "cust_h" || res.Cluster.ClusterID != "hosted-h" ||
		res.Cluster.Source != state.ClusterSourceHosted || res.Cluster.HostedNamespace != "ys-h" {
		t.Fatalf("response = %+v", res)
	}
	if !strings.HasPrefix(res.ConnectorToken, "yscale_cluster_") {
		t.Fatalf("connector_token = %q", res.ConnectorToken)
	}
	if res.HelmRelease != "yscale-agent-hosted-h" {
		t.Fatalf("helm_release = %q, want a unique per-tenant release", res.HelmRelease)
	}
	if res.ConnectorRBACSet != `--set 'rbac.allowedNamespaces={ys-h}'` {
		t.Fatalf("connector_rbac_set = %q", res.ConnectorRBACSet)
	}
	for _, want := range []string{
		"helm install yscale-agent-hosted-h ",
		"--set clusterID=hosted-h",
		`--set 'rbac.allowedNamespaces={ys-h}'`,
		"--set installCRD=false",
		"--set rbac.scope=namespaced",
		`--set token="$YSCALE_CONNECTOR_TOKEN"`,
		"--set endpoint='ws://yscale-cloud.yscale:8443'",
	} {
		if !strings.Contains(res.HelmCommand, want) {
			t.Errorf("helm_command missing %q: %q", want, res.HelmCommand)
		}
	}
	// The command must never embed the secret, and must never widen the
	// chart's namespaced RBAC scope.
	if strings.Contains(res.HelmCommand, res.ConnectorToken) {
		t.Fatal("helm_command embeds the plaintext credential")
	}
	if strings.Contains(res.HelmCommand, "scope=cluster") {
		t.Fatalf("helm_command widens the RBAC scope: %q", res.HelmCommand)
	}
	// No credential hash anywhere in the body, by construction.
	cust, err := store.CustomerByID("cust_h")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rec.Body.String(), cust.RegisteredClusters[0].CredentialHash) {
		t.Fatal("credential hash leaked into the response")
	}

	// The credential binds to the assigned tenant and virtual id, and the
	// EXISTING submission gate accepts the reserved namespace — while an
	// unqualified submission still lands in the fail-closed default, never in
	// the shared cluster's namespace.
	if c, bound, err := store.AuthClusterCredential(res.ConnectorToken); err != nil || c.ID != "cust_h" || bound != "hosted-h" {
		t.Fatalf("AuthClusterCredential = (%v,%q,%v)", c, bound, err)
	}
	if ns, err := authorizeWorkloadNamespace(cust, "ys-h"); err != nil || ns != "ys-h" {
		t.Fatalf("authorizeWorkloadNamespace(ys-h) = (%q,%v)", ns, err)
	}
	if ns, err := authorizeWorkloadNamespace(cust, ""); err != nil || ns != "default" {
		t.Fatalf("unqualified submission namespace = (%q,%v), want default", ns, err)
	}
}

func TestHostedReleaseNamesFitKubernetesAndTenantTokenCannotMint(t *testing.T) {
	const maxID = "aaaaaaaaaaaaaaaaaaaaa"
	release := hostedHelmRelease(maxID)
	if got := len(release + "-yscale-agent-tailscale-serve"); got > 63 {
		t.Fatalf("longest hosted resource name is %d characters, want <= 63", got)
	}

	store := state.New()
	cust := &state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro", Mesh: &state.MeshEndpoint{
		Provider: "box", LoginServer: "https://box.example", APIKey: "k", User: "hosted",
	}}
	store.AddCustomer(cust)
	_, token, err := store.AssignHostedCluster("cust_h", "hosted-h", "", "ys-h", state.OperatorActor())
	if err != nil {
		t.Fatal(err)
	}
	prov := &fakeMeshProvider{}
	h := &AgentAuth{
		Store:       store,
		Log:         quietLog(),
		BoxProvider: func(string, string, string) mesh.Provider { return prov },
	}
	if rec := callMint(h, cust, "hosted-h"); rec.Code != http.StatusForbidden {
		t.Fatalf("tenant token mint = %d, want 403: %s", rec.Code, rec.Body)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/agent/ts-auth-key", strings.NewReader("{}"))
	ctx := context.WithValue(context.Background(), ctxCustomer, cust)
	ctx = context.WithValue(ctx, ctxAgentCluster, "hosted-h")
	rec := httptest.NewRecorder()
	h.MintKey(rec, req.WithContext(ctx))
	if rec.Code != http.StatusOK {
		t.Fatalf("hosted scoped credential mint = %d, want 200: %s", rec.Code, rec.Body)
	}
	if _, bound, err := store.AuthClusterCredential(token); err != nil || bound != "hosted-h" {
		t.Fatalf("hosted credential binding = (%q,%v)", bound, err)
	}
}

// Every refusal is a stable status: unknown tenant 404, bad grammar 400,
// duplicate assignment and cross-tenant collisions 409 — and a body that
// parses wrong is refused rather than silently treated as empty.
func TestAssignHostedClusterStatuses(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_a", Token: "ysk_a", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_b", Token: "ysk_b", Plan: "pro"})
	tn := newTenants(store)

	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_nope/hosted-clusters", ""); rec.Code != http.StatusNotFound {
		t.Errorf("unknown tenant = %d, want 404", rec.Code)
	}
	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_a/hosted-clusters", `{"namespace":"Not A Label"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("invalid namespace = %d, want 400", rec.Code)
	}
	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_a/hosted-clusters", `{"cluster_id":"Bad_ID"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("invalid cluster id = %d, want 400", rec.Code)
	}
	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_a/hosted-clusters", `{"namespace":`); rec.Code != http.StatusBadRequest {
		t.Errorf("malformed JSON = %d, want 400", rec.Code)
	}
	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_a/hosted-clusters", `{"namespaec":"ys-a"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("unknown field = %d, want 400", rec.Code)
	}

	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_a/hosted-clusters", `{"cluster_id":"hosted-a","namespace":"ys-a"}`); rec.Code != http.StatusCreated {
		t.Fatalf("assign = %d (body %q)", rec.Code, rec.Body.String())
	}
	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_a/hosted-clusters", ""); rec.Code != http.StatusConflict {
		t.Errorf("duplicate assignment = %d, want 409", rec.Code)
	}
	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_b/hosted-clusters", `{"namespace":"ys-a"}`); rec.Code != http.StatusConflict {
		t.Errorf("cross-tenant namespace collision = %d, want 409", rec.Code)
	}
	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_b/hosted-clusters", `{"cluster_id":"hosted-a"}`); rec.Code != http.StatusConflict {
		t.Errorf("cross-tenant cluster id collision = %d, want 409", rec.Code)
	}
}

// The tenant surface can SEE the hosted row — with its namespace — but its
// rotate and delete refuse with the stable platform-managed conflict, so
// tenant ownership of their own registry is intact while the hosted row's
// lifecycle stays with the operator.
func TestTenantSurfaceListsButRefusesHostedMutation(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_h", map[string]string{"alice": state.RoleOwner})
	tn := newTenants(store)

	rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_h/hosted-clusters", `{"cluster_id":"hosted-h","namespace":"ys-h"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("assign = %d (body %q)", rec.Code, rec.Body.String())
	}

	list := listClusters(accounts, "human_alice", "cust_h")
	if list.Code != http.StatusOK {
		t.Fatalf("tenant list = %d (body %q)", list.Code, list.Body.String())
	}
	fleet := decodeClusters(t, list)
	if len(fleet.Clusters) != 1 || fleet.Clusters[0].Source != state.ClusterSourceHosted ||
		fleet.Clusters[0].HostedNamespace != "ys-h" {
		t.Fatalf("tenant view = %+v", fleet.Clusters)
	}

	rotate := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_h/clusters/hosted-h/credential", "human_alice", "")
	if rotate.Code != http.StatusConflict || !strings.Contains(rotate.Body.String(), "platform-managed hosted capacity") {
		t.Fatalf("tenant rotate = %d (body %q), want the stable 409", rotate.Code, rotate.Body.String())
	}
	del := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_h/clusters/hosted-h", "human_alice", "")
	if del.Code != http.StatusConflict || !strings.Contains(del.Body.String(), "platform-managed hosted capacity") {
		t.Fatalf("tenant delete = %d (body %q), want the stable 409", del.Code, del.Body.String())
	}

	// The tenant's OWN registry writes are untouched: registering, rotating
	// and deleting a non-hosted cluster all still work beside the hosted row.
	reg := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_h/clusters", "human_alice", `{"cluster_id":"cl-own","name":"Own"}`)
	if reg.Code != http.StatusCreated {
		t.Fatalf("tenant register beside hosted row = %d (body %q)", reg.Code, reg.Body.String())
	}
	if rec := callClusters(accounts, http.MethodPost, "/v1/tenants/cust_h/clusters/cl-own/credential", "human_alice", ""); rec.Code != http.StatusOK {
		t.Fatalf("tenant rotate of own cluster = %d", rec.Code)
	}
	if rec := callClusters(accounts, http.MethodDelete, "/v1/tenants/cust_h/clusters/cl-own", "human_alice", ""); rec.Code != http.StatusOK {
		t.Fatalf("tenant delete of own cluster = %d", rec.Code)
	}
}

// The operator lifecycle over the routes: rotation reveals a fresh credential
// with an upgrade command for the SAME release, delete confirms the freed
// namespace, and a second delete is 404.
func TestOperatorHostedLifecycleRoutes(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro"})
	tn := newTenants(store)

	assigned := decodeHostedCredential(t, callHosted(tn, http.MethodPost,
		"/v1/admin/tenants/cust_h/hosted-clusters", `{"cluster_id":"hosted-h"}`))

	rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_h/hosted-clusters/hosted-h/credential", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("operator rotate = %d (body %q)", rec.Code, rec.Body.String())
	}
	rotated := decodeHostedCredential(t, rec)
	if rotated.ConnectorToken == assigned.ConnectorToken || rotated.ConnectorToken == "" {
		t.Fatal("rotation did not mint a fresh credential")
	}
	if !strings.Contains(rotated.HelmCommand, "helm upgrade yscale-agent-hosted-h ") ||
		!strings.Contains(rotated.HelmCommand, "--reuse-values") {
		t.Fatalf("rotate helm_command = %q, want an upgrade of the same release", rotated.HelmCommand)
	}
	if _, _, err := store.AuthClusterCredential(assigned.ConnectorToken); err == nil {
		t.Fatal("old credential still authenticates after rotation")
	}

	// Rotating an unknown assignment is 404, not a mint.
	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_h/hosted-clusters/hosted-nope/credential", ""); rec.Code != http.StatusNotFound {
		t.Errorf("rotate unknown assignment = %d, want 404", rec.Code)
	}

	del := callHosted(tn, http.MethodDelete, "/v1/admin/tenants/cust_h/hosted-clusters/hosted-h", "")
	if del.Code != http.StatusOK {
		t.Fatalf("operator delete = %d (body %q)", del.Code, del.Body.String())
	}
	var report HostedClusterDeleteResponse
	if err := json.Unmarshal(del.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode delete response: %v", err)
	}
	if !report.Deleted || report.ClusterID != "hosted-h" || report.HostedNamespace != "ys-cust-h" {
		t.Fatalf("delete report = %+v", report)
	}
	if rec := callHosted(tn, http.MethodDelete, "/v1/admin/tenants/cust_h/hosted-clusters/hosted-h", ""); rec.Code != http.StatusNotFound {
		t.Errorf("second delete = %d, want 404", rec.Code)
	}
}

// The queue response is the operator-safe view over the real registration:
// pending tenants oldest-first with the tenant id tie break, the exact stable
// JSON field set, an empty ARRAY when nobody waits, and none of the tenant's
// private material anywhere in the body.
func TestListHostedCapacityRequestsQueueResponse(t *testing.T) {
	store := state.New()
	older := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	tie := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	for _, c := range []*state.Customer{
		{ID: "cust_b", Token: "ysk_secret_b", Email: "b@acme.com", Plan: "free", HostedCapacityRequestedAt: &tie},
		{ID: "cust_a", Token: "ysk_secret_a", Email: "a@acme.com", Plan: "pro", Name: "Team A", HostedCapacityRequestedAt: &tie},
		{ID: "cust_old", Token: "ysk_secret_old", Email: "old@acme.com", Plan: "enterprise", HostedCapacityRequestedAt: &older},
		{ID: "cust_assigned", Token: "ysk_secret_as", Email: "as@acme.com", Plan: "pro", HostedCapacityRequestedAt: &older,
			RegisteredClusters: []*state.RegisteredCluster{{ClusterID: "hosted-as", Source: state.ClusterSourceHosted}}},
	} {
		store.AddCustomer(c)
	}
	tn := newTenants(store)

	rec := callHosted(tn, http.MethodGet, "/v1/admin/hosted-capacity/requests", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("queue = %d (body %q)", rec.Code, rec.Body.String())
	}
	var res PendingHostedCapacityRequestsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode queue response: %v (body %q)", err, rec.Body.String())
	}
	if len(res.Requests) != 3 {
		t.Fatalf("queue rows = %+v, want the three pending tenants", res.Requests)
	}
	wantIDs := []string{"cust_old", "cust_a", "cust_b"}
	for i, id := range wantIDs {
		if res.Requests[i].TenantID != id {
			t.Fatalf("row %d = %+v, want %s", i, res.Requests[i], id)
		}
	}
	if !res.Requests[0].RequestedAt.Equal(older) || !res.Requests[1].RequestedAt.Equal(tie) {
		t.Fatalf("row timestamps = %+v, want oldest first then the tie pair", res.Requests)
	}
	if res.Requests[1].Name != "Team A" || res.Requests[1].Plan != "pro" || res.Requests[2].Plan != "free" {
		t.Fatalf("row metadata = %+v", res.Requests[1:])
	}

	// The stable field set: exactly tenant_id, optional name, plan,
	// requested_at — and never the tenant's bearer token, email, or a
	// credential hash.
	rows := rec.Body.String()
	for _, secret := range []string{"ysk_secret_a", "ysk_secret_b", "ysk_secret_old", "ysk_secret_as", "@acme.com", "credential"} {
		if strings.Contains(rows, secret) {
			t.Fatalf("queue response leaks %q: %s", secret, rows)
		}
	}
	// The row field set is closed: decode one raw row with unknown fields
	// refused, so a field added to the view fails here before it ships.
	var envelope struct {
		Requests []json.RawMessage `json:"requests"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	strict := struct {
		TenantID    string    `json:"tenant_id"`
		Name        *string   `json:"name"`
		Plan        string    `json:"plan"`
		RequestedAt time.Time `json:"requested_at"`
	}{}
	dec := json.NewDecoder(strings.NewReader(string(envelope.Requests[2])))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&strict); err != nil {
		t.Fatalf("row grew past the stable field set: %v", err)
	}
	if strict.Name != nil {
		t.Fatalf("tenant without a display name serialized %q, want the field omitted", *strict.Name)
	}
}

// Nobody waiting is an empty array under the same envelope, not null and not
// an error: a console polls this route blindly.
func TestListHostedCapacityRequestsEmptyArray(t *testing.T) {
	tn := newTenants(state.New())
	rec := callHosted(tn, http.MethodGet, "/v1/admin/hosted-capacity/requests", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("empty queue = %d (body %q)", rec.Code, rec.Body.String())
	}
	if body := strings.TrimSpace(rec.Body.String()); body != `{"requests":[]}` {
		t.Fatalf("empty queue body = %q, want {\"requests\":[]}", body)
	}
}

// The queue route answers the operator credential and nothing else, exactly
// like the mutators beside it: no bearer, a wrong bearer, and the tenant's
// own token are all 401 through the real AdminAuth wrapper — and with no
// admin token configured the route does not exist.
func TestListHostedCapacityRequestsAdminAuth(t *testing.T) {
	store := state.New()
	requested := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	store.AddCustomer(&state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro", HostedCapacityRequestedAt: &requested})
	tn := newTenants(store)

	mux := func(adminToken string) *http.ServeMux {
		m := http.NewServeMux()
		m.Handle("GET /v1/admin/hosted-capacity/requests",
			AdminAuth(adminToken, http.HandlerFunc(tn.HandleListHostedCapacityRequests)))
		return m
	}
	call := func(adminToken, bearer string) int {
		req := httptest.NewRequest(http.MethodGet, "/v1/admin/hosted-capacity/requests", nil)
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		rec := httptest.NewRecorder()
		mux(testAdminToken).ServeHTTP(rec, req)
		return rec.Code
	}

	if code := call(testAdminToken, ""); code != http.StatusUnauthorized {
		t.Errorf("missing bearer = %d, want 401", code)
	}
	if code := call(testAdminToken, "adm_wrong_token"); code != http.StatusUnauthorized {
		t.Errorf("wrong bearer = %d, want 401", code)
	}
	if code := call(testAdminToken, "ysk_h"); code != http.StatusUnauthorized {
		t.Errorf("tenant bearer = %d, want 401", code)
	}
	if code := call(testAdminToken, testAdminToken); code != http.StatusOK {
		t.Errorf("operator bearer = %d, want 200", code)
	}

	// Unset admin token: the route 404s rather than answering openly.
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/hosted-capacity/requests", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	rec := httptest.NewRecorder()
	mux("").ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("unset admin token = %d, want 404", rec.Code)
	}
}
