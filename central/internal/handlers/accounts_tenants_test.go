// yscale:proprietary

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// staticIdentityResolver resolves tokens from a fixed map with no rate
// limiting, so a concurrency test exercises the handler rather than the
// resolver's shed path.
type staticIdentityResolver map[string]Identity

func (s staticIdentityResolver) Resolve(_ context.Context, token string) (Identity, error) {
	identity, ok := s[token]
	if !ok {
		return Identity{}, ErrUnauthenticated
	}
	return identity, nil
}

// selfServiceAccounts is the handler as the enterprise hook wires it with
// YSCALE_SELF_SERVICE_TENANTS=true and Yscale ID configured.
func selfServiceAccounts(store accountStore, resolver IdentityResolver) *Accounts {
	return &Accounts{
		Store:                   store,
		Resolver:                resolver,
		Issuer:                  testIssuer,
		Log:                     quietLog(),
		AllowSelfServiceTenants: true,
	}
}

// postTenant drives POST /v1/account/tenants the way the mux does.
func postTenant(a *Accounts, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/account/tenants", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	a.HandleCreateTenant(rec, req)
	return rec
}

// Self-service signup is an opt-in. A deployment that has not set
// YSCALE_SELF_SERVICE_TENANTS=true acts as if the route does not exist — before
// the credential is looked at, so it neither mints an account nor reveals a 401.
func TestSelfServiceTenantActsAbsentWhenDisabled(t *testing.T) {
	store := state.New()
	accounts := selfServiceAccounts(store, idProvider(t, map[string]string{"human_alice": "alice"}))
	accounts.AllowSelfServiceTenants = false

	rec := postTenant(accounts, "human_alice", `{"name":"Acme"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if _, err := store.AccountByIdentity(testIssuer, "alice"); err == nil {
		t.Fatal("a disabled route minted an account")
	}
}

// The body is one bounded JSON document with one field, and the name inside it
// is 1–64 code points of printable text. Everything outside that is refused
// before anything is created.
func TestSelfServiceTenantRejectsInvalidBodiesAndNames(t *testing.T) {
	store := state.New()
	accounts := selfServiceAccounts(store, idProvider(t, map[string]string{"human_alice": "alice"}))
	cases := []struct{ name, body string }{
		{"empty body", ""},
		{"not JSON", "workspace"},
		{"array", `[]`},
		{"unknown field", `{"name":"Acme","plan":"enterprise"}`},
		{"trailing JSON", `{"name":"Acme"}{"name":"Again"}`},
		{"missing name", `{}`},
		{"blank name", `{"name":"   "}`},
		{"too many code points", fmt.Sprintf(`{"name":%q}`, strings.Repeat("日", 65))},
		{"control characters", `{"name":"line\nbreak"}`},
		{"body over the cap", `{"name":"` + strings.Repeat("a", maxSelfServiceTenantRequestBytes) + `"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := postTenant(accounts, "human_alice", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
			}
		})
	}
	// The sign-in half still ran — the route resolves like GET /v1/account —
	// but no rejected request bought a tenant.
	acct, err := store.AccountByIdentity(testIssuer, "alice")
	if err != nil {
		t.Fatalf("AccountByIdentity after rejected requests: %v", err)
	}
	if _, err := store.TenantSummaryByID(state.SelfServiceTenantID(acct.ID)); err == nil {
		t.Fatal("a rejected request created a tenant")
	}
}

// An unverified address may sign in but not provision: the email is the only
// thing anchoring the identity to a reachable person.
func TestSelfServiceTenantRequiresVerifiedEmail(t *testing.T) {
	store := state.New()
	accounts := selfServiceAccounts(store, staticIdentityResolver{
		"human_mallory": {Subject: "mallory", Email: "mallory@acme.com", EmailVerified: false},
	})

	rec := postTenant(accounts, "human_mallory", `{"name":"Acme"}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %q)", rec.Code, rec.Body.String())
	}
	acct, err := store.AccountByIdentity(testIssuer, "mallory")
	if err != nil {
		t.Fatalf("AccountByIdentity: %v", err)
	}
	if _, err := store.TenantSummaryByID(state.SelfServiceTenantID(acct.ID)); err == nil {
		t.Fatal("an unverified email created a tenant")
	}
}

// The success path: one durable trial tenant on the deterministic id, owned by
// the caller, capped at one burst and one dollar an hour, judged against the
// fail-closed namespace set — and the response is the account document the
// browser already reads, with the cluster credential nowhere in it.
func TestSelfServiceTenantCreatesOneOwnedWorkspace(t *testing.T) {
	store := state.New()
	accounts := selfServiceAccounts(store, idProvider(t, map[string]string{"human_alice": "alice"}))

	// Surrounding space is trimmed, and the bound is code points, not bytes:
	// 64 CJK runes are three bytes each and still a legal name.
	name := strings.Repeat("日", 64)
	rec := postTenant(accounts, "human_alice", fmt.Sprintf(`{"name":"  %s  "}`, name))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}
	res := decodeAccount(t, rec)
	if res.AccountID == "" || !res.EmailVerified {
		t.Fatalf("account in response = %+v", res)
	}
	if len(res.Tenants) != 1 {
		t.Fatalf("tenants = %+v, want exactly the new workspace", res.Tenants)
	}
	got := res.Tenants[0]
	wantID := state.SelfServiceTenantID(res.AccountID)
	if got.CustomerID != wantID || got.Name != name || got.Role != state.RoleOwner || got.Plan != "trial" {
		t.Fatalf("tenant summary = %+v", got)
	}
	if got.Limits.MaxConcurrentBursts != 1 || got.Limits.MaxHourlyUSD != 1.0 {
		t.Errorf("limits = %+v, want {1 1}", got.Limits)
	}
	if len(got.WorkloadNamespaces) != 1 || got.WorkloadNamespaces[0] != "default" {
		t.Errorf("workload namespaces = %v, want the fail-closed default", got.WorkloadNamespaces)
	}

	// The tenant is real: its cluster credential authenticates — and it was
	// never shown to the browser.
	cust, err := store.CustomerByID(wantID)
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if !strings.HasPrefix(cust.Token, "ysk_") {
		t.Fatalf("tenant token = %q, want a minted ysk_ credential", cust.Token)
	}
	if _, err := store.AuthCustomer(cust.Token); err != nil {
		t.Fatalf("the new tenant's token does not authenticate: %v", err)
	}
	body := rec.Body.String()
	for _, forbidden := range []string{cust.Token, "ysk_", `"token"`} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("the response leaked the tenant credential (%q): %s", forbidden, body)
		}
	}
	if m, err := store.MembershipFor(res.AccountID, wantID); err != nil || m.Role != state.RoleOwner {
		t.Fatalf("owner membership = %+v, %v", m, err)
	}
}

// failingCreateTenantStore refuses the durable create; everything before it
// behaves.
type failingCreateTenantStore struct {
	*state.Store
	err error
}

func (f *failingCreateTenantStore) CreateFirstTenant(*state.Customer, string) (*state.Customer, *state.TenantMembership, error) {
	return nil, nil, f.err
}

// A store that cannot land the tenant answers the retryable 503, and no token
// or internal error text reaches the caller.
func TestSelfServiceTenantFailsClosedWhenTheStoreRefuses(t *testing.T) {
	store := state.New()
	accounts := selfServiceAccounts(&failingCreateTenantStore{
		Store: store,
		err:   fmt.Errorf("%w: connection refused", state.ErrPersistence),
	}, idProvider(t, map[string]string{"human_alice": "alice"}))

	rec := postTenant(accounts, "human_alice", `{"name":"Acme"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); strings.Contains(body, "connection refused") || strings.Contains(body, "ysk_") {
		t.Fatalf("refusal leaked internals: %s", body)
	}
	acct, err := store.AccountByIdentity(testIssuer, "alice")
	if err != nil {
		t.Fatalf("AccountByIdentity: %v", err)
	}
	if len(store.MembershipsForAccount(acct.ID)) != 0 {
		t.Fatal("a refused create still granted a membership")
	}
}

// One account, one self-service workspace: a repeat is a conflict, and racing
// requests collide on the deterministic id so at most one tenant is ever
// created however many arrive at once.
func TestRepeatedAndConcurrentSelfServiceCreationYieldOneTenant(t *testing.T) {
	store := state.New()
	accounts := selfServiceAccounts(store, staticIdentityResolver{
		"human_alice": {Subject: "alice", Email: "alice@acme.com", EmailVerified: true, Name: "Alice"},
	})

	const attempts = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	codes := make([]int, attempts)
	for n := 0; n < attempts; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			codes[n] = postTenant(accounts, "human_alice", `{"name":"Acme"}`).Code
		}(n)
	}
	close(start)
	wg.Wait()

	var created, conflicts int
	for _, code := range codes {
		switch code {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("codes = %v, got unexpected %d", codes, code)
		}
	}
	if created != 1 || conflicts != attempts-1 {
		t.Fatalf("codes = %v, want exactly one 201", codes)
	}

	acct, err := store.AccountByIdentity(testIssuer, "alice")
	if err != nil {
		t.Fatalf("AccountByIdentity: %v", err)
	}
	if memberships := store.MembershipsForAccount(acct.ID); len(memberships) != 1 {
		t.Fatalf("memberships = %+v, want exactly one", memberships)
	}

	// And a later, sequential retry is still a conflict, not a second tenant.
	if code := postTenant(accounts, "human_alice", `{"name":"Acme Again"}`).Code; code != http.StatusConflict {
		t.Fatalf("repeat status = %d, want 409", code)
	}
}

// A human who already belongs to any tenant — self-created or granted — is not
// a first run: they have a workspace, or someone who can grant one.
func TestSelfServiceTenantRefusesAnExistingMember(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_home", Token: "ysk_home", Plan: "pro"})
	alice, err := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := store.AddTenantMembership(alice.ID, "cust_home", state.RoleViewer); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}

	accounts := selfServiceAccounts(store, idProvider(t, map[string]string{"human_alice": "alice"}))
	rec := postTenant(accounts, "human_alice", `{"name":"Second Home"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %q)", rec.Code, rec.Body.String())
	}
	if _, err := store.TenantSummaryByID(state.SelfServiceTenantID(alice.ID)); err == nil {
		t.Fatal("an existing member bought a second workspace")
	}
}
