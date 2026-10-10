// yscale:proprietary

package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// seatedOperatorAuth is the configured middleware every test below rides:
// tok_op resolves to op_alice, who is allowlisted and whose account exists.
func seatedOperatorAuth(t *testing.T, store *state.Store) OperatorAuth {
	t.Helper()
	if _, err := store.UpsertAccount(testIssuer, "op_alice", state.AccountProfile{}); err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	return OperatorAuth{
		Store:    store,
		Resolver: staticIdentityResolver{"tok_op": {Subject: "op_alice", Email: "op_alice@acme.com"}},
		Issuer:   testIssuer,
		Subjects: ParseOperatorSubjects("op_alice"),
		Log:      quietLog(),
	}
}

// callOperator drives the real operator mux with the seated credential unless
// a different token is named.
func callOperator(mux *http.ServeMux, method, path, token, body string) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// The inventory is the operator-safe view over the real registry: every live
// tenant's hosted assignment, ordered by tenant id, with the exact stable JSON
// field set — and nothing of the tenant's private material, the tenants' OWN
// clusters, or a revoked tenant's row anywhere in the body.
func TestOperatorHostedInventoryResponse(t *testing.T) {
	registeredAt := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	revokedAt := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	store := state.New()
	for _, c := range []*state.Customer{
		{ID: "cust_b", Token: "ysk_secret_b", Email: "b@acme.com", Plan: "pro", Name: "Bravo",
			RegisteredClusters: []*state.RegisteredCluster{
				{ClusterID: "hosted-b", Name: "Yscale hosted capacity", Source: state.ClusterSourceHosted,
					HostedNamespace: "ys-cust-b", CredentialHash: "sha256-secret-b", RegisteredAt: registeredAt},
			}},
		{ID: "cust_a", Token: "ysk_secret_a", Email: "a@acme.com", Plan: "free",
			RegisteredClusters: []*state.RegisteredCluster{
				{ClusterID: "cl-own", Name: "Their cluster", Source: state.ClusterSourceRegistered,
					CredentialHash: "sha256-secret-own", RegisteredAt: registeredAt},
				{ClusterID: "hosted-a", Source: state.ClusterSourceHosted,
					HostedNamespace: "ys-cust-a", CredentialHash: "sha256-secret-a", RegisteredAt: registeredAt},
			}},
		{ID: "cust_revoked", Token: "ysk_secret_rev", Email: "rev@acme.com", Plan: "pro", RevokedAt: &revokedAt,
			RegisteredClusters: []*state.RegisteredCluster{
				{ClusterID: "hosted-rev", Source: state.ClusterSourceHosted,
					HostedNamespace: "ys-cust-rev", CredentialHash: "sha256-secret-rev", RegisteredAt: registeredAt},
			}},
	} {
		store.AddCustomer(c)
	}
	mux := operatorMux(seatedOperatorAuth(t, store), newTenants(store))

	rec := callOperator(mux, http.MethodGet, "/v1/operator/hosted-clusters", "tok_op", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("inventory = %d (body %q)", rec.Code, rec.Body.String())
	}

	var typed HostedClusterInventoryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &typed); err != nil {
		t.Fatalf("decode inventory: %v (body %q)", err, rec.Body.String())
	}
	if len(typed.Clusters) != 2 {
		t.Fatalf("clusters = %+v, want the two live hosted assignments", typed.Clusters)
	}
	if typed.Clusters[0].TenantID != "cust_a" || typed.Clusters[0].Cluster.ClusterID != "hosted-a" {
		t.Fatalf("first row = %+v, want cust_a/hosted-a", typed.Clusters[0])
	}
	if typed.Clusters[1].TenantID != "cust_b" || typed.Clusters[1].Name != "Bravo" {
		t.Fatalf("second row = %+v, want the named cust_b row", typed.Clusters[1])
	}
	if typed.Clusters[0].Plan != "free" || typed.Clusters[1].Plan != "pro" {
		t.Fatalf("plans = %+v", typed.Clusters)
	}

	// The envelope and the row shape are exact: an operator console and this
	// handler agree on the field set, and a field added by accident fails here.
	var raw struct {
		Clusters []map[string]json.RawMessage `json:"clusters"`
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if len(envelope) != 1 || envelope["clusters"] == nil {
		t.Fatalf("envelope keys = %v, want exactly {clusters}", envelope)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode rows: %v", err)
	}
	// cust_a has no display name, so name is omitted rather than blank.
	wantFirst := map[string]bool{"tenant_id": true, "plan": true, "cluster": true}
	for key := range raw.Clusters[0] {
		if !wantFirst[key] {
			t.Errorf("unnamed-tenant row carries unexpected field %q", key)
		}
		delete(wantFirst, key)
	}
	if len(wantFirst) != 0 {
		t.Errorf("unnamed-tenant row missing fields %v", wantFirst)
	}
	if _, ok := raw.Clusters[1]["name"]; !ok {
		t.Error("named tenant row dropped name")
	}

	var cluster map[string]json.RawMessage
	if err := json.Unmarshal(raw.Clusters[0]["cluster"], &cluster); err != nil {
		t.Fatalf("decode cluster: %v", err)
	}
	wantCluster := map[string]bool{"cluster_id": true, "source": true, "state": true, "hosted_namespace": true, "registered_at": true}
	for key := range cluster {
		if !wantCluster[key] && key != "name" {
			t.Errorf("cluster summary carries unexpected field %q", key)
		}
		delete(wantCluster, key)
	}
	if len(wantCluster) != 0 {
		t.Errorf("cluster summary missing fields %v", wantCluster)
	}

	// Nothing private, nothing another surface owns, and no tenant-owned row.
	body := rec.Body.String()
	for _, leak := range []string{
		"ysk_secret_a", "ysk_secret_b", "ysk_secret_rev", "sha256-secret-a", "sha256-secret-b",
		"sha256-secret-own", "a@acme.com", "b@acme.com", "rev@acme.com",
		"cl-own", "hosted-rev", "cust_revoked", "credential_hash", "token", "email", "members",
	} {
		if strings.Contains(body, leak) {
			t.Errorf("inventory body leaks %q: %s", leak, body)
		}
	}
}

// Nobody holds shared capacity: an empty ARRAY, so a console renders a list
// rather than crashing on null.
func TestOperatorHostedInventoryEmptyArray(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_empty", Token: "ysk_empty", Plan: "pro"})
	mux := operatorMux(seatedOperatorAuth(t, store), newTenants(store))

	rec := callOperator(mux, http.MethodGet, "/v1/operator/hosted-clusters", "tok_op", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("inventory = %d (body %q)", rec.Code, rec.Body.String())
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"clusters":[]}` {
		t.Fatalf("empty inventory = %s, want an empty array", got)
	}
}

// The lifecycle routes exist at exactly one method and one path each: a near
// miss is 404/405, never a fallback onto another handler.
func TestOperatorHostedRoutesAreExactMethodsAndPaths(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_x", Token: "ysk_x", Plan: "pro"})
	mux := operatorMux(seatedOperatorAuth(t, store), newTenants(store))

	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodPost, "/v1/operator/hosted-clusters", http.StatusMethodNotAllowed},
		{http.MethodDelete, "/v1/operator/hosted-clusters", http.StatusMethodNotAllowed},
		{http.MethodGet, "/v1/operator/hosted-clusters/", http.StatusNotFound},
		{http.MethodGet, "/v1/operator/hosted-cluster", http.StatusNotFound},
		{http.MethodGet, "/v1/operator/tenants/cust_x/hosted-clusters/hosted-x/credential", http.StatusMethodNotAllowed},
		{http.MethodPost, "/v1/operator/tenants/cust_x/hosted-clusters/hosted-x/credentials", http.StatusNotFound},
		{http.MethodPost, "/v1/operator/tenants/cust_x/hosted-clusters/hosted-x", http.StatusMethodNotAllowed},
		{http.MethodPut, "/v1/operator/tenants/cust_x/hosted-clusters/hosted-x", http.StatusMethodNotAllowed},
		// The admin twin is registered on a separate mux.
		{http.MethodGet, "/v1/admin/hosted-clusters", http.StatusNotFound},
		{http.MethodDelete, "/v1/admin/tenants/cust_x/hosted-clusters/hosted-x", http.StatusNotFound},
	} {
		rec := callOperator(mux, tc.method, tc.path, "tok_op", "")
		if rec.Code != tc.want {
			t.Errorf("%s %s = %d, want %d", tc.method, tc.path, rec.Code, tc.want)
		}
	}
}

// The static admin inventory twin returns the exact same safe envelope.
func TestStaticAdminInventoryTwin(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro"})
	tn := newTenants(store)

	if rec := callHosted(tn, http.MethodGet, "/v1/admin/hosted-clusters", ""); rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"clusters":[]}` {
		t.Errorf("admin inventory = %d %q, want exact empty safe envelope", rec.Code, rec.Body.String())
	}
	if rec := callHosted(tn, http.MethodGet, "/v1/admin/hosted-capacity/requests", ""); rec.Code != http.StatusOK {
		t.Errorf("admin queue = %d, want 200", rec.Code)
	}
	assigned := decodeHostedCredential(t, callHosted(tn, http.MethodPost,
		"/v1/admin/tenants/cust_h/hosted-clusters", `{"cluster_id":"hosted-h"}`))
	if assigned.ConnectorToken == "" || assigned.Cluster.ClusterID != "hosted-h" {
		t.Fatalf("admin assign = %+v", assigned)
	}
	if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_h/hosted-clusters/hosted-h/credential", ""); rec.Code != http.StatusOK {
		t.Errorf("admin rotate = %d, want 200", rec.Code)
	}
	if rec := callHosted(tn, http.MethodDelete, "/v1/admin/tenants/cust_h/hosted-clusters/hosted-h", ""); rec.Code != http.StatusOK {
		t.Errorf("admin delete = %d, want 200", rec.Code)
	}
}

// Operator auth gates all three routes on the credential and nothing else: the
// family is absent when unconfigured, a missing credential is 401, and a real
// human who holds no seat — or a seated subject with no account — is 403. No
// tenant role, header or body promotes anyone.
func TestOperatorHostedLifecycleRoutesRequireOperatorAuth(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_g", Token: "ysk_g", Plan: "pro"})
	// A tenant OWNER is still not an operator: a role decides nothing here.
	owner, err := store.UpsertAccount(testIssuer, "owner_human", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := store.AddTenantMembership(owner.ID, "cust_g", state.RoleOwner); err != nil {
		t.Fatalf("membership: %v", err)
	}
	seated := seatedOperatorAuth(t, store)
	seated.Resolver = staticIdentityResolver{
		"tok_op":    {Subject: "op_alice"},
		"tok_owner": {Subject: "owner_human"},
		"tok_ghost": {Subject: "op_ghost"},
	}
	seated.Subjects = ParseOperatorSubjects("op_alice, op_ghost")
	tn := newTenants(store)

	routes := []struct{ method, path string }{
		{http.MethodGet, "/v1/operator/hosted-clusters"},
		{http.MethodPost, "/v1/operator/tenants/cust_g/hosted-clusters/hosted-g/credential"},
		{http.MethodDelete, "/v1/operator/tenants/cust_g/hosted-clusters/hosted-g"},
	}

	unconfigured := seated
	unconfigured.Subjects = ParseOperatorSubjects(" , ")
	absentMux := operatorMux(unconfigured, tn)
	liveMux := operatorMux(seated, tn)

	for _, route := range routes {
		if rec := callOperator(absentMux, route.method, route.path, "tok_op", ""); rec.Code != http.StatusNotFound {
			t.Errorf("%s %s unconfigured = %d, want 404", route.method, route.path, rec.Code)
		}
		if rec := callOperator(liveMux, route.method, route.path, "", ""); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without a credential = %d, want 401", route.method, route.path, rec.Code)
		}
		if rec := callOperator(liveMux, route.method, route.path, "tok_unknown", ""); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s with a refused credential = %d, want 401", route.method, route.path, rec.Code)
		}
		if rec := callOperator(liveMux, route.method, route.path, "tok_owner", ""); rec.Code != http.StatusForbidden {
			t.Errorf("%s %s as a tenant owner = %d, want 403", route.method, route.path, rec.Code)
		}
		if rec := callOperator(liveMux, route.method, route.path, "tok_ghost", ""); rec.Code != http.StatusForbidden {
			t.Errorf("%s %s as a seated subject without an account = %d, want 403", route.method, route.path, rec.Code)
		}
	}
	// A header or body claiming operator identity changes nothing.
	req := httptest.NewRequest(http.MethodDelete, "/v1/operator/tenants/cust_g/hosted-clusters/hosted-g",
		strings.NewReader(`{"operator":"op_alice","role":"owner"}`))
	req.Header.Set("X-Operator-Account", "op_alice")
	req.Header.Set("X-Admin-Token", "adm_static")
	rec := httptest.NewRecorder()
	liveMux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("forged operator claim = %d, want 401", rec.Code)
	}
}

// lifecycleCapturingStore records the actor each hosted rotate and delete is
// called with, so a test can assert audit attribution without a durable
// journal.
type lifecycleCapturingStore struct {
	tenantStore
	mu       sync.Mutex
	rotated  []state.Actor
	deleted  []state.Actor
	assigned []state.Actor
}

func (s *lifecycleCapturingStore) AssignHostedCluster(customerID, clusterID, name, namespace string, by state.Actor) (state.TenantCluster, string, error) {
	s.mu.Lock()
	s.assigned = append(s.assigned, by)
	s.mu.Unlock()
	return s.tenantStore.AssignHostedCluster(customerID, clusterID, name, namespace, by)
}

func (s *lifecycleCapturingStore) RotateHostedClusterCredential(customerID, clusterID string, by state.Actor) (state.TenantCluster, string, error) {
	s.mu.Lock()
	s.rotated = append(s.rotated, by)
	s.mu.Unlock()
	return s.tenantStore.RotateHostedClusterCredential(customerID, clusterID, by)
}

func (s *lifecycleCapturingStore) DeleteHostedCluster(customerID, clusterID string, by state.Actor) (string, error) {
	s.mu.Lock()
	s.deleted = append(s.deleted, by)
	s.mu.Unlock()
	return s.tenantStore.DeleteHostedCluster(customerID, clusterID, by)
}

func (s *lifecycleCapturingStore) seen() (rotated, deleted []state.Actor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]state.Actor(nil), s.rotated...), append([]state.Actor(nil), s.deleted...)
}

// Rotate and delete are attributed to whichever credential carried them: the
// signed-in human on the operator route — account id and tenant id, exactly
// HumanActor — and the static operator on the admin route. Same store, same
// handlers; the only thing that changed is who the request came in as.
func TestOperatorRotateDeleteAttributeHumanAdminStaysOperator(t *testing.T) {
	newFixture := func(t *testing.T) (*lifecycleCapturingStore, *state.Store, *Tenants) {
		t.Helper()
		store := state.New()
		store.AddCustomer(&state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro"})
		capture := &lifecycleCapturingStore{tenantStore: store}
		tn := &Tenants{Store: capture, Log: quietLog(), Endpoint: "ws://yscale-cloud.yscale:8443"}
		if _, _, err := store.AssignHostedCluster("cust_h", "hosted-h", "", "", state.OperatorActor()); err != nil {
			t.Fatalf("AssignHostedCluster: %v", err)
		}
		return capture, store, tn
	}

	t.Run("operator routes record the human", func(t *testing.T) {
		capture, store, tn := newFixture(t)
		// seatedOperatorAuth mints the operator's account, so resolve it after.
		mux := operatorMux(seatedOperatorAuth(t, store), tn)
		seatedAcct, err := store.AccountByIdentity(testIssuer, "op_alice")
		if err != nil {
			t.Fatalf("AccountByIdentity: %v", err)
		}

		if rec := callOperator(mux, http.MethodPost, "/v1/operator/tenants/cust_h/hosted-clusters/hosted-h/credential", "tok_op", ""); rec.Code != http.StatusOK {
			t.Fatalf("operator rotate = %d (body %q)", rec.Code, rec.Body.String())
		}
		if rec := callOperator(mux, http.MethodDelete, "/v1/operator/tenants/cust_h/hosted-clusters/hosted-h", "tok_op", ""); rec.Code != http.StatusOK {
			t.Fatalf("operator delete = %d (body %q)", rec.Code, rec.Body.String())
		}
		rotated, deleted := capture.seen()
		want := state.HumanActor(seatedAcct.ID, "cust_h")
		if len(rotated) != 1 || rotated[0] != want {
			t.Fatalf("rotate actors = %+v, want %+v", rotated, want)
		}
		if len(deleted) != 1 || deleted[0] != want {
			t.Fatalf("delete actors = %+v, want %+v", deleted, want)
		}
	})

	t.Run("admin routes keep the static operator", func(t *testing.T) {
		capture, _, tn := newFixture(t)
		if rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_h/hosted-clusters/hosted-h/credential", ""); rec.Code != http.StatusOK {
			t.Fatalf("admin rotate = %d (body %q)", rec.Code, rec.Body.String())
		}
		if rec := callHosted(tn, http.MethodDelete, "/v1/admin/tenants/cust_h/hosted-clusters/hosted-h", ""); rec.Code != http.StatusOK {
			t.Fatalf("admin delete = %d (body %q)", rec.Code, rec.Body.String())
		}
		rotated, deleted := capture.seen()
		if len(rotated) != 1 || rotated[0] != state.OperatorActor() {
			t.Fatalf("rotate actors = %+v, want exactly OperatorActor", rotated)
		}
		if len(deleted) != 1 || deleted[0] != state.OperatorActor() {
			t.Fatalf("delete actors = %+v, want exactly OperatorActor", deleted)
		}
	})
}

// The whole lifecycle over the operator credential answers the SAME contract
// the admin routes established: rotation is a one-time reveal of a fresh
// credential with an upgrade of the same release, the old credential stops
// authenticating, delete confirms the freed namespace, a second delete is 404,
// and the released assignment leaves the inventory.
func TestOperatorHostedLifecyclePreservesResponseContracts(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro", Name: "Hosted"})
	tn := newTenants(store)
	mux := operatorMux(seatedOperatorAuth(t, store), tn)

	assigned := decodeHostedCredential(t, callOperator(mux, http.MethodPost,
		"/v1/operator/tenants/cust_h/hosted-clusters", "tok_op", `{"cluster_id":"hosted-h"}`))
	if assigned.ConnectorToken == "" {
		t.Fatalf("assign = %+v, lost the reveal", assigned)
	}

	rec := callOperator(mux, http.MethodGet, "/v1/operator/hosted-clusters", "tok_op", "")
	var inv HostedClusterInventoryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &inv); err != nil {
		t.Fatalf("decode inventory: %v", err)
	}
	if len(inv.Clusters) != 1 || inv.Clusters[0].Cluster.ClusterID != "hosted-h" ||
		inv.Clusters[0].TenantID != "cust_h" || inv.Clusters[0].Name != "Hosted" {
		t.Fatalf("inventory after assign = %+v", inv.Clusters)
	}
	if inv.Clusters[0].Cluster.HostedNamespace != "ys-cust-h" {
		t.Fatalf("inventory namespace = %q, want the reserved one", inv.Clusters[0].Cluster.HostedNamespace)
	}
	if strings.Contains(rec.Body.String(), assigned.ConnectorToken) {
		t.Fatal("inventory republishes the connector credential")
	}

	rec = callOperator(mux, http.MethodPost, "/v1/operator/tenants/cust_h/hosted-clusters/hosted-h/credential", "tok_op", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("operator rotate = %d (body %q)", rec.Code, rec.Body.String())
	}
	rotated := decodeHostedCredential(t, rec)
	if rotated.ConnectorToken == "" || rotated.ConnectorToken == assigned.ConnectorToken {
		t.Fatal("rotation did not mint a fresh credential")
	}
	if !strings.Contains(rotated.HelmCommand, "helm upgrade yscale-agent-hosted-h ") ||
		!strings.Contains(rotated.HelmCommand, "--reuse-values") {
		t.Fatalf("rotate helm_command = %q, want an upgrade of the same release", rotated.HelmCommand)
	}
	if _, _, err := store.AuthClusterCredential(assigned.ConnectorToken); err == nil {
		t.Fatal("old credential still authenticates after the operator rotation")
	}
	if _, _, err := store.AuthClusterCredential(rotated.ConnectorToken); err != nil {
		t.Fatalf("rotated credential does not authenticate: %v", err)
	}
	// Rotating an unknown assignment is 404, not a mint.
	if rec := callOperator(mux, http.MethodPost, "/v1/operator/tenants/cust_h/hosted-clusters/hosted-nope/credential", "tok_op", ""); rec.Code != http.StatusNotFound {
		t.Errorf("operator rotate of an unknown assignment = %d, want 404", rec.Code)
	}

	del := callOperator(mux, http.MethodDelete, "/v1/operator/tenants/cust_h/hosted-clusters/hosted-h", "tok_op", "")
	if del.Code != http.StatusOK {
		t.Fatalf("operator delete = %d (body %q)", del.Code, del.Body.String())
	}
	var report HostedClusterDeleteResponse
	if err := json.Unmarshal(del.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode delete response: %v", err)
	}
	if !report.Deleted || report.TenantID != "cust_h" || report.ClusterID != "hosted-h" || report.HostedNamespace != "ys-cust-h" {
		t.Fatalf("delete report = %+v", report)
	}
	if rec := callOperator(mux, http.MethodDelete, "/v1/operator/tenants/cust_h/hosted-clusters/hosted-h", "tok_op", ""); rec.Code != http.StatusNotFound {
		t.Errorf("second operator delete = %d, want 404", rec.Code)
	}
	rec = callOperator(mux, http.MethodGet, "/v1/operator/hosted-clusters", "tok_op", "")
	if got := strings.TrimSpace(rec.Body.String()); got != `{"clusters":[]}` {
		t.Fatalf("inventory after delete = %s, want empty", got)
	}
}
