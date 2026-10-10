// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// failingResolver fails every resolve with the fixed error, so the middleware's
// error mapping can be exercised without a network.
type failingResolver struct{ err error }

func (f failingResolver) Resolve(context.Context, string) (Identity, error) {
	return Identity{}, f.err
}

// operatorMux wires the operator routes exactly as the enterprise build does.
func operatorMux(oa OperatorAuth, tn *Tenants) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/operator/hosted-capacity/requests",
		oa.Wrap(http.HandlerFunc(tn.HandleListHostedCapacityRequests)))
	mux.Handle("GET /v1/operator/hosted-clusters",
		oa.Wrap(http.HandlerFunc(tn.HandleListHostedClusters)))
	mux.Handle("POST /v1/operator/tenants/{tenant_id}/hosted-clusters",
		oa.Wrap(http.HandlerFunc(tn.HandleAssignHostedCluster)))
	mux.Handle("POST /v1/operator/tenants/{tenant_id}/hosted-clusters/{cluster_id}/credential",
		oa.Wrap(http.HandlerFunc(tn.HandleRotateHostedClusterCredential)))
	mux.Handle("DELETE /v1/operator/tenants/{tenant_id}/hosted-clusters/{cluster_id}",
		oa.Wrap(http.HandlerFunc(tn.HandleDeleteHostedCluster)))
	return mux
}

// The allowlist parse is trim + drop-blanks and nothing else: no case folding,
// no wildcard, no splitting on any other character — an operator seat is the
// exact subject the identity provider reports.
func TestParseOperatorSubjects(t *testing.T) {
	for raw, want := range map[string][]string{
		"":                       {},
		"   ":                    {},
		",,":                     {},
		"op_alice":               {"op_alice"},
		" op_alice ,op_bob,, \n": {"op_alice", "op_bob"},
		"op_alice,op_alice":      {"op_alice"},
		"op carol":               {"op carol"},
		"OP_ALICE":               {"OP_ALICE"},
		"https://x/~alice":       {"https://x/~alice"},
	} {
		got := ParseOperatorSubjects(raw)
		if len(got) != len(want) {
			t.Fatalf("ParseOperatorSubjects(%q) = %v, want %v", raw, got, want)
		}
		for _, subject := range want {
			if !got[subject] {
				t.Errorf("ParseOperatorSubjects(%q) missing %q", raw, subject)
			}
		}
	}
}

// Every status the middleware can answer, in one table: the route family is
// absent (404) unless resolver, issuer and allowlist are ALL configured; a
// missing or refused credential is 401; a shed lookup is 429 + Retry-After; a
// down identity provider is 503; a valid human without a seat — or with one
// but no account — is 403; only a seated, existing human reaches the handler.
func TestOperatorAuthStatusMappings(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_op", Token: "ysk_op", Plan: "pro"})
	seated, err := store.UpsertAccount(testIssuer, "op_alice", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	// A real, unseated human: an account exists, the subject just isn't listed.
	if _, err := store.UpsertAccount(testIssuer, "op_bob", state.AccountProfile{}); err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	resolver := staticIdentityResolver{
		"tok_alice": {Subject: "op_alice", Email: "op_alice@acme.com"},
		"tok_bob":   {Subject: "op_bob"},
		"tok_ghost": {Subject: "op_ghost"}, // seated below, but no account
	}

	probe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if accountID := OperatorAccountFromContext(r.Context()); accountID != seated.ID {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "no operator account in context"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"ok": "true"})
	})
	base := OperatorAuth{
		Store:    store,
		Resolver: resolver,
		Issuer:   testIssuer,
		Subjects: ParseOperatorSubjects("op_alice, op_ghost"),
		Log:      quietLog(),
	}

	cases := []struct {
		name       string
		auth       OperatorAuth
		token      string
		want       int
		retryAfter string
	}{
		{name: "no resolver", auth: func() OperatorAuth {
			a := base
			a.Resolver = nil
			return a
		}(), token: "tok_alice", want: http.StatusNotFound},
		{name: "no store", auth: func() OperatorAuth {
			a := base
			a.Store = nil
			return a
		}(), token: "tok_alice", want: http.StatusNotFound},
		{name: "no issuer", auth: func() OperatorAuth {
			a := base
			a.Issuer = ""
			return a
		}(), token: "tok_alice", want: http.StatusNotFound},
		{name: "no allowlist", auth: func() OperatorAuth {
			a := base
			a.Subjects = ParseOperatorSubjects("  , ")
			return a
		}(), token: "tok_alice", want: http.StatusNotFound},
		{name: "missing bearer", auth: base, token: "", want: http.StatusUnauthorized},
		{name: "refused credential", auth: base, token: "tok_unknown", want: http.StatusUnauthorized},
		{name: "shed locally", auth: OperatorAuth{Store: store, Resolver: failingResolver{ErrRateLimited}, Issuer: testIssuer, Subjects: base.Subjects, Log: quietLog()}, token: "tok_alice", want: http.StatusTooManyRequests, retryAfter: "1"},
		{name: "identity provider down", auth: OperatorAuth{Store: store, Resolver: failingResolver{ErrIdentityUnavailable}, Issuer: testIssuer, Subjects: base.Subjects, Log: quietLog()}, token: "tok_alice", want: http.StatusServiceUnavailable},
		{name: "identity provider down with default logger", auth: OperatorAuth{Store: store, Resolver: failingResolver{ErrIdentityUnavailable}, Issuer: testIssuer, Subjects: base.Subjects}, token: "tok_alice", want: http.StatusServiceUnavailable},
		{name: "valid human without a seat", auth: base, token: "tok_bob", want: http.StatusForbidden},
		{name: "seated subject without an account", auth: base, token: "tok_ghost", want: http.StatusForbidden},
		{name: "seated existing human", auth: base, token: "tok_alice", want: http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/operator/hosted-capacity/requests", nil)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			rec := httptest.NewRecorder()
			tc.auth.Wrap(probe).ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
			if got := rec.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
		})
	}
}

// assignCapturingStore records the actor each AssignHostedCluster is called
// with, so a test can assert audit attribution without a durable journal.
type assignCapturingStore struct {
	tenantStore
	mu   sync.Mutex
	seen []state.Actor
}

func (s *assignCapturingStore) AssignHostedCluster(customerID, clusterID, name, namespace string, by state.Actor) (state.TenantCluster, string, error) {
	s.mu.Lock()
	s.seen = append(s.seen, by)
	s.mu.Unlock()
	return s.tenantStore.AssignHostedCluster(customerID, clusterID, name, namespace, by)
}

func (s *assignCapturingStore) actors() []state.Actor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]state.Actor(nil), s.seen...)
}

// The operator route's assignment is attributed to the signed-in human —
// account id and tenant id, exactly HumanActor and nothing else — while the
// same handler on the static admin route keeps recording the operator. Same
// store, same handler, different credential: the only thing that changed is
// who the request came in as.
func TestOperatorAssignAttributesHumanActorAdminStaysOperator(t *testing.T) {
	newFixture := func(t *testing.T) (*assignCapturingStore, *Tenants, *state.Account) {
		t.Helper()
		store := state.New()
		store.AddCustomer(&state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro"})
		acct, err := store.UpsertAccount(testIssuer, "op_alice", state.AccountProfile{})
		if err != nil {
			t.Fatalf("UpsertAccount: %v", err)
		}
		capture := &assignCapturingStore{tenantStore: store}
		tn := &Tenants{Store: capture, Log: quietLog(), Endpoint: "ws://yscale-cloud.yscale:8443"}
		return capture, tn, acct
	}

	t.Run("operator route records the human", func(t *testing.T) {
		capture, tn, acct := newFixture(t)
		mux := operatorMux(OperatorAuth{
			Store:    capture.tenantStore.(*state.Store),
			Resolver: staticIdentityResolver{"tok_alice": {Subject: "op_alice"}},
			Issuer:   testIssuer,
			Subjects: ParseOperatorSubjects("op_alice"),
			Log:      quietLog(),
		}, tn)
		req := httptest.NewRequest(http.MethodPost, "/v1/operator/tenants/cust_h/hosted-clusters", nil)
		req.Header.Set("Authorization", "Bearer tok_alice")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("assign = %d, want 201 (body %q)", rec.Code, rec.Body.String())
		}
		actors := capture.actors()
		if len(actors) != 1 {
			t.Fatalf("assignments = %d, want 1", len(actors))
		}
		want := state.HumanActor(acct.ID, "cust_h")
		if actors[0] != want {
			t.Fatalf("actor = %+v, want %+v", actors[0], want)
		}
		// The response is the operator's reveal, not the human's identity.
		for _, leak := range []string{"op_alice", "op_alice@acme.com", testIssuer} {
			if strings.Contains(rec.Body.String(), `"`+leak+`"`) {
				t.Errorf("response leaks identity %q", leak)
			}
		}
	})

	t.Run("admin route keeps the static operator", func(t *testing.T) {
		capture, tn, _ := newFixture(t)
		rec := callHosted(tn, http.MethodPost, "/v1/admin/tenants/cust_h/hosted-clusters", `{}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("assign = %d, want 201 (body %q)", rec.Code, rec.Body.String())
		}
		actors := capture.actors()
		if len(actors) != 1 || actors[0] != state.OperatorActor() {
			t.Fatalf("actors = %+v, want exactly OperatorActor", actors)
		}
	})
}

// Through the real middleware and the real handlers, the operator routes
// answer the queue and assignment with the same response contract the admin
// routes established — the browser console is a new credential, not a new API.
func TestOperatorRoutesPreserveHostedResponseContracts(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_q", Token: "ysk_q", Plan: "pro", Name: "Acme"})
	requester, err := store.UpsertAccount(testIssuer, "req_human", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := store.AddTenantMembership(requester.ID, "cust_q", state.RoleOwner); err != nil {
		t.Fatalf("membership: %v", err)
	}
	if _, _, err := store.RequestTenantHostedCapacity("cust_q", requester.ID, state.HumanActor(requester.ID, "cust_q")); err != nil {
		t.Fatalf("request hosted capacity: %v", err)
	}
	tn := &Tenants{Store: store, Log: quietLog(), Endpoint: "ws://yscale-cloud.yscale:8443"}
	mux := operatorMux(OperatorAuth{
		Store:    store,
		Resolver: staticIdentityResolver{"tok_op": {Subject: "op_alice"}},
		Issuer:   testIssuer,
		Subjects: ParseOperatorSubjects("op_alice"),
		Log:      quietLog(),
	}, tn)
	// The operator needs an existing account, not a tenant role: no membership
	// anywhere, just the account a prior sign-in minted.
	if _, err := store.UpsertAccount(testIssuer, "op_alice", state.AccountProfile{}); err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	get := httptest.NewRequest(http.MethodGet, "/v1/operator/hosted-capacity/requests", nil)
	get.Header.Set("Authorization", "Bearer tok_op")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, get)
	if rec.Code != http.StatusOK {
		t.Fatalf("list = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var queue PendingHostedCapacityRequestsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &queue); err != nil {
		t.Fatalf("decode queue: %v (body %q)", err, rec.Body.String())
	}
	if len(queue.Requests) != 1 || queue.Requests[0].TenantID != "cust_q" || queue.Requests[0].Name != "Acme" {
		t.Fatalf("queue = %+v, want the cust_q request", queue.Requests)
	}

	post := httptest.NewRequest(http.MethodPost, "/v1/operator/tenants/cust_q/hosted-clusters",
		strings.NewReader(`{"cluster_id":"hosted-q","namespace":"ys-q"}`))
	post.Header.Set("Authorization", "Bearer tok_op")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, post)
	if rec.Code != http.StatusCreated {
		t.Fatalf("assign = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}
	res := decodeHostedCredential(t, rec)
	if res.TenantID != "cust_q" || res.Cluster.ClusterID != "hosted-q" || res.Cluster.HostedNamespace != "ys-q" {
		t.Fatalf("assignment response = %+v", res)
	}
	if res.HelmCommand == "" || res.ConnectorToken == "" {
		t.Fatalf("assignment response lost the reveal: %+v", res)
	}
	// The fulfilled request left the queue.
	if pending := store.PendingHostedCapacityRequests(); len(pending) != 0 {
		t.Fatalf("pending after assign = %+v, want empty", pending)
	}
}
