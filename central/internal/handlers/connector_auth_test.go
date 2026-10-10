package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// registryStore is one tenant with an owner and one registered cluster, plus a
// second tenant — the smallest world in which a scoped credential has both a
// binding to honor and a neighbor to be isolated from.
type registryFixture struct {
	store   *state.Store
	token   string
	aliceID string
	bobID   string
}

func registryStore(t *testing.T) registryFixture {
	t.Helper()
	s := state.New()
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "tok_a", Plan: "pro"})
	s.AddCustomer(&state.Customer{ID: "cust_b", Token: "tok_b", Plan: "pro"})
	acct, err := s.UpsertAccount("https://id.test", "alice", state.AccountProfile{Email: "a@acme.com", EmailVerified: true})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s.AddTenantMembership(acct.ID, "cust_a", state.RoleOwner); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}
	bob, err := s.UpsertAccount("https://id.test", "bob", state.AccountProfile{Email: "b@acme.com", EmailVerified: true})
	if err != nil {
		t.Fatalf("UpsertAccount bob: %v", err)
	}
	if _, err := s.AddTenantMembership(bob.ID, "cust_b", state.RoleOwner); err != nil {
		t.Fatalf("AddTenantMembership bob: %v", err)
	}
	_, token, _, err := s.RegisterTenantCluster("cust_a", acct.ID, "cl-a", "Cluster A", state.HumanActor(acct.ID, "cust_a"))
	if err != nil {
		t.Fatalf("RegisterTenantCluster: %v", err)
	}
	return registryFixture{store: s, token: token, aliceID: acct.ID, bobID: bob.ID}
}

// probe records what the middleware resolved: which tenant, and which cluster
// the credential was bound to (empty = legacy tenant token).
type probe struct {
	customerID string
	binding    string
}

func probeHandler(got *probe) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cust, err := CustomerFromContext(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		got.customerID = cust.ID
		got.binding = AgentClusterFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
}

func callWith(h http.Handler, token, clusterHeader string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/v1/agent/stream", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if clusterHeader != "" {
		req.Header.Set(clusterIDHeader, clusterHeader)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// ConnectorAuth accepts both connector credential shapes on the agent routes:
// a scoped credential resolves to its tenant WITH its cluster binding, and the
// legacy tenant token keeps working unchanged with no binding. A scoped
// credential's X-Cluster-ID is pinned to its own cluster.
func TestConnectorAuthAcceptsScopedAndLegacyCredentials(t *testing.T) {
	fx := registryStore(t)
	s, token := fx.store, fx.token
	var got probe
	h := ConnectorAuth(s, probeHandler(&got))

	if rec := callWith(h, token, ""); rec.Code != http.StatusOK {
		t.Fatalf("scoped credential = %d, want 200: %s", rec.Code, rec.Body)
	}
	if got.customerID != "cust_a" || got.binding != "cl-a" {
		t.Fatalf("scoped resolution = %+v, want cust_a bound to cl-a", got)
	}
	if rec := callWith(h, token, "cl-a"); rec.Code != http.StatusOK {
		t.Fatalf("scoped credential with its own header = %d, want 200: %s", rec.Code, rec.Body)
	}
	if rec := callWith(h, token, "cl-other"); rec.Code != http.StatusForbidden {
		t.Fatalf("scoped credential naming another cluster = %d, want 403: %s", rec.Code, rec.Body)
	}

	got = probe{}
	if rec := callWith(h, "tok_a", "cl-anything"); rec.Code != http.StatusOK {
		t.Fatalf("legacy token = %d, want 200: %s", rec.Code, rec.Body)
	}
	if got.customerID != "cust_a" || got.binding != "" {
		t.Fatalf("legacy resolution = %+v, want cust_a with no binding", got)
	}

	if rec := callWith(h, "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token = %d, want 401", rec.Code)
	}
	if rec := callWith(h, "tok_wrong", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unknown token = %d, want 401", rec.Code)
	}
}

// A connector credential authenticates the connector middlewares and NOTHING
// else. Auth — the middleware in front of workload reads, price, spend and
// bursts — must refuse it: the connector surfaces reach the work of its own
// cluster, and Auth is where a tenant's money and history are. Revocation and
// rotation both kill it on the agent routes too.
func TestConnectorCredentialIsScopedToAgentRoutes(t *testing.T) {
	fx := registryStore(t)
	s, token := fx.store, fx.token
	var got probe

	if rec := callWith(Auth(s, probeHandler(&got)), token, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("connector credential on an Auth route = %d, want 401: %s", rec.Code, rec.Body)
	}
	if rec := callWith(Auth(s, probeHandler(&got)), "tok_a", ""); rec.Code != http.StatusOK {
		t.Fatalf("tenant token on an Auth route = %d, want 200", rec.Code)
	}
	// The operator surface never accepts it either.
	if rec := callWith(AdminAuth("admin_secret", probeHandler(&got)), token, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("connector credential on AdminAuth = %d, want 401", rec.Code)
	}

	// A revoked tenant's connector credential dies with the tenant.
	if err := s.RevokeCustomer("cust_a"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if rec := callWith(ConnectorAuth(s, probeHandler(&got)), token, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked tenant's connector credential = %d, want 401", rec.Code)
	}
}
