// yscale:proprietary

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

const testIssuer = "https://id.yscale.sh"

// idProvider is a stand-in Yscale ID that knows a fixed set of access tokens.
// Anything it does not know is rejected exactly as the real /userinfo would —
// which is how a cluster Customer.Token gets refused here.
func idProvider(t *testing.T, tokens map[string]string) *YscaleID {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		sub, ok := tokens[token]
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sub": sub, "email": sub + "@acme.com", "email_verified": true, "name": sub,
		})
	}))
	t.Cleanup(srv.Close)
	return &YscaleID{BaseURL: srv.URL}
}

// getAccount drives the handler the way the mux does and returns the recorder.
func getAccount(a *Accounts, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/v1/account", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	a.HandleGet(rec, req)
	return rec
}

func decodeAccount(t *testing.T, rec *httptest.ResponseRecorder) AccountResponse {
	t.Helper()
	var res AccountResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode account response: %v (body %q)", err, rec.Body.String())
	}
	return res
}

// A verified human sees their own account and ONLY the tenants they are a
// member of — the isolation the whole surface exists to enforce.
func TestAccountReturnsOnlyItsOwnTenants(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro",
		MaxConcurrentBursts: 5, MaxHourlyUSD: 10})
	store.AddCustomer(&state.Customer{ID: "cust_bob", Token: "ysk_bob", Plan: "free"})

	alice, err := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	bob, err := store.UpsertAccount(testIssuer, "bob", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := store.AddTenantMembership(alice.ID, "cust_alice", state.RoleOwner); err != nil {
		t.Fatalf("alice membership: %v", err)
	}
	if _, err := store.AddTenantMembership(bob.ID, "cust_bob", state.RoleAdmin); err != nil {
		t.Fatalf("bob membership: %v", err)
	}

	accounts := &Accounts{
		Store:    store,
		Resolver: idProvider(t, map[string]string{"human_alice": "alice", "human_bob": "bob"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}

	rec := getAccount(accounts, "human_alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	res := decodeAccount(t, rec)
	if res.AccountID != alice.ID || res.Subject != "alice" || res.Issuer != testIssuer {
		t.Fatalf("identity in response = %+v", res)
	}
	// The profile came from userinfo on this very call.
	if res.Email != "alice@acme.com" || !res.EmailVerified {
		t.Errorf("profile not refreshed from userinfo: %+v", res)
	}
	if len(res.Tenants) != 1 {
		t.Fatalf("tenants = %+v, want exactly cust_alice", res.Tenants)
	}
	got := res.Tenants[0]
	if got.CustomerID != "cust_alice" || got.Role != state.RoleOwner || got.Plan != "pro" {
		t.Fatalf("tenant summary = %+v", got)
	}
	if got.Limits.MaxConcurrentBursts != 5 || got.Limits.MaxHourlyUSD != 10 {
		t.Errorf("tenant limits = %+v", got.Limits)
	}

	// Bob's tenant is nowhere in Alice's response, and no token of any kind is.
	body := rec.Body.String()
	for _, forbidden := range []string{"cust_bob", "ysk_alice", "ysk_bob", "human_alice"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("account response leaked %q: %s", forbidden, body)
		}
	}
}

// The response carries the account and its memberships — never the tenant's
// cluster credential, its mesh endpoint, or anything else about the tenant.
func TestAccountResponseNeverCarriesTenantSecrets(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_secret_cluster_token", Plan: "pro"})
	if err := store.SetCustomerMesh("cust_alice", &state.MeshEndpoint{
		Provider: "headscale", LoginServer: "https://box.example", APIKey: "hs_secret_key", User: "alice",
	}); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}
	alice, _ := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	if _, err := store.AddTenantMembership(alice.ID, "cust_alice", state.RoleOwner); err != nil {
		t.Fatalf("membership: %v", err)
	}

	accounts := &Accounts{
		Store:    store,
		Resolver: idProvider(t, map[string]string{"human_alice": "alice"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}
	rec := getAccount(accounts, "human_alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, secret := range []string{"ysk_secret_cluster_token", "hs_secret_key", "box.example", "headscale"} {
		if strings.Contains(body, secret) {
			t.Fatalf("account response leaked %q: %s", secret, body)
		}
	}
}

// The two credential types do not cross. A cluster Customer.Token is not an
// identity, so presenting one here authenticates nothing.
func TestAccountRejectsClusterCustomerToken(t *testing.T) {
	store := state.New()
	cust, err := store.CustomerByID(state.DevCustomerID)
	if err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	accounts := &Accounts{
		Store:    store,
		Resolver: idProvider(t, map[string]string{"human_alice": "alice"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}

	rec := getAccount(accounts, cust.Token)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("cluster token status = %d, want 401 (body %q)", rec.Code, rec.Body.String())
	}
	// And it did not quietly mint an account for the cluster's token either.
	if _, err := store.AccountByIdentity(testIssuer, cust.Token); err == nil {
		t.Fatal("a cluster token minted a human account")
	}
}

func TestAccountRejectsMissingAndInvalidTokens(t *testing.T) {
	store := state.New()
	accounts := &Accounts{
		Store:    store,
		Resolver: idProvider(t, map[string]string{"human_alice": "alice"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}
	tests := []struct {
		name  string
		token string
	}{
		{"no Authorization header", ""},
		{"unknown access token", "not-a-real-token"},
		{"empty bearer value", " "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if code := getAccount(accounts, tc.token).Code; code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", code)
			}
		})
	}
	// A non-Bearer scheme is not a Bearer token.
	req := httptest.NewRequest(http.MethodGet, "/v1/account", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	rec := httptest.NewRecorder()
	accounts.HandleGet(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("Basic auth status = %d, want 401", rec.Code)
	}
}

// Without YSCALE_ID_INTERNAL_URL there is no resolver, so the route does not
// exist on this deployment — 404, exactly as AdminAuth does with no admin token.
func TestAccountRouteDisabledWithoutIdentityConfig(t *testing.T) {
	accounts := &Accounts{Store: state.New(), Issuer: testIssuer, Log: quietLog()}
	for _, token := range []string{"", "human_alice"} {
		if code := getAccount(accounts, token).Code; code != http.StatusNotFound {
			t.Fatalf("unconfigured route status = %d, want 404", code)
		}
	}
}

// A resolver without an issuer cannot key an account correctly, so it refuses
// to create one rather than storing a half-identified human.
func TestAccountRefusesWithoutConfiguredIssuer(t *testing.T) {
	store := state.New()
	accounts := &Accounts{
		Store:    store,
		Resolver: idProvider(t, map[string]string{"human_alice": "alice"}),
		Log:      quietLog(),
	}
	rec := getAccount(accounts, "human_alice")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if _, err := store.AccountByIdentity(testIssuer, "alice"); err == nil {
		t.Fatal("an account was created without a configured issuer")
	}
}

// An unreachable identity provider is not a rejected token: the human keeps
// their session and gets a retryable 503, and no account is touched.
func TestAccountReturnsUnavailableWhenIssuerIsDown(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer down.Close()

	accounts := &Accounts{
		Store:    state.New(),
		Resolver: &YscaleID{BaseURL: down.URL},
		Issuer:   testIssuer,
		Log:      quietLog(),
	}
	if code := getAccount(accounts, "human_alice").Code; code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", code)
	}
}

// First sign-in creates the account; later ones refresh the profile in place
// rather than minting a second identity.
func TestAccountSignInUpsertsProfile(t *testing.T) {
	store := state.New()
	accounts := &Accounts{
		Store:    store,
		Resolver: idProvider(t, map[string]string{"human_alice": "alice"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}
	first := decodeAccount(t, getAccount(accounts, "human_alice"))
	if first.AccountID == "" {
		t.Fatal("first sign-in did not create an account")
	}
	if len(first.Tenants) != 0 {
		t.Fatalf("a new account has tenants: %+v", first.Tenants)
	}
	second := decodeAccount(t, getAccount(accounts, "human_alice"))
	if second.AccountID != first.AccountID {
		t.Fatalf("second sign-in minted a new account: %s vs %s", first.AccountID, second.AccountID)
	}
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Errorf("CreatedAt moved on re-sign-in: %v → %v", first.CreatedAt, second.CreatedAt)
	}
}

// A membership whose tenant was offboarded between the lookup and the render is
// dropped, not reported as a tenant with empty fields.
func TestAccountSkipsOffboardedTenants(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_live", Token: "ysk_live", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_doomed", Token: "ysk_doomed", Plan: "pro"})
	alice, _ := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	for _, id := range []string{"cust_live", "cust_doomed"} {
		if _, err := store.AddTenantMembership(alice.ID, id, state.RoleOwner); err != nil {
			t.Fatalf("membership %s: %v", id, err)
		}
	}
	store.DeleteCustomer("cust_doomed")

	accounts := &Accounts{
		Store:    store,
		Resolver: idProvider(t, map[string]string{"human_alice": "alice"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}
	res := decodeAccount(t, getAccount(accounts, "human_alice"))
	if len(res.Tenants) != 1 || res.Tenants[0].CustomerID != "cust_live" {
		t.Fatalf("tenants = %+v, want only cust_live", res.Tenants)
	}
}

// failingAccountStore is a store whose durable backend refuses account writes.
// Everything else behaves; only the write the owner grant depends on fails.
type failingAccountStore struct {
	*state.Store
	err error
}

func (f *failingAccountStore) UpsertAccount(string, string, state.AccountProfile) (*state.Account, error) {
	return nil, f.err
}

// A store that cannot persist the account is a server fault, not a bad
// credential. Answering 401 would tell a human with a perfectly good session to
// sign in again because the database is down.
func TestAccountReturnsUnavailableWhenAccountStoreFails(t *testing.T) {
	accounts := &Accounts{
		Store: &failingAccountStore{
			Store: state.New(),
			err:   fmt.Errorf("%w: persist account acct_x: connection refused", state.ErrPersistence),
		},
		Resolver: idProvider(t, map[string]string{"tok_human": "sub-1"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}
	rec := getAccount(accounts, "tok_human")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
}

// An identity the store refuses to key is still a rejected credential, so the
// 503 above must not have swallowed the 401 contract.
func TestAccountRejectsIdentityTheStoreWontKey(t *testing.T) {
	accounts := &Accounts{
		Store:    &failingAccountStore{Store: state.New(), err: state.ErrInvalidIdentity},
		Resolver: idProvider(t, map[string]string{"tok_human": "sub-1"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}
	rec := getAccount(accounts, "tok_human")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body %q)", rec.Code, rec.Body.String())
	}
}

// /v1/account is reachable with no valid credential, so an unbounded resolver
// amplifies anyone's traffic one-for-one onto Yscale ID. Past its own budget a
// credential is shed with 429 + Retry-After, and the token never leaves the box.
func TestAccountShedsWhenLocallyRateLimited(t *testing.T) {
	resolver := idProvider(t, map[string]string{"tok_human": "sub-1", "tok_other": "sub-2"})
	resolver.Burst = 1
	resolver.ResolvesPerSecond = 0.001 // no meaningful refill inside the test
	accounts := &Accounts{Store: state.New(), Resolver: resolver, Issuer: testIssuer, Log: quietLog()}

	if rec := getAccount(accounts, "tok_human"); rec.Code != http.StatusOK {
		t.Fatalf("first call status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	rec := getAccount(accounts, "tok_human")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second call status = %d, want 429 — the outbound budget is not bounded", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 carried no Retry-After, so a polling client has nothing to back off to")
	}
	if strings.Contains(rec.Body.String(), "tok_human") {
		t.Errorf("shed response echoed the access token: %q", rec.Body.String())
	}
	// The budget is per credential: another signed-in human is served normally
	// while that one is throttled. A shared budget would make one busy (or
	// hostile) client an outage for everybody.
	if rec := getAccount(accounts, "tok_other"); rec.Code != http.StatusOK {
		t.Errorf("second human's status = %d, want 200 — one spent credential starved another", rec.Code)
	}
	// A shed request decides nothing about the credential: the SAME unknown
	// token past its own budget is still 429, never a cached 401.
	if rec := getAccount(accounts, "not-a-token"); rec.Code != http.StatusUnauthorized {
		t.Errorf("first call with an unknown token = %d, want 401", rec.Code)
	}
	if rec := getAccount(accounts, "not-a-token"); rec.Code != http.StatusTooManyRequests {
		t.Errorf("unknown token past its own budget = %d, want 429", rec.Code)
	}
}

// The second bound: concurrent userinfo calls. A slow identity provider must
// not be able to pull an unbounded number of central goroutines out with it.
func TestResolveBoundsConcurrentUserinfoCalls(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"sub": "sub-1"})
	}))
	defer srv.Close()
	defer close(release)

	resolver := &YscaleID{BaseURL: srv.URL, MaxInflight: 1}
	done := make(chan error, 1)
	go func() {
		_, err := resolver.Resolve(context.Background(), "tok_human")
		done <- err
	}()
	<-entered // the first call is inside the provider and holding the only slot

	if _, err := resolver.Resolve(context.Background(), "tok_human"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("second concurrent resolve error = %v, want ErrRateLimited", err)
	}
	release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatalf("the in-flight call should still have succeeded: %v", err)
	}
}

// The account surface and an offboard are ordinary concurrent traffic: a human
// polls /v1/account while an operator revokes one of their tenants. The
// handler must never be reading a customer record while the store's writer is
// stamping it — under -race that is a hard failure, and in production it is a
// summary assembled half from a live tenant and half from a revoked one.
//
// Every tenant here is revoked while the polls run, so the only two legitimate
// answers per tenant are "listed as it was" and "gone", never a mix.
func TestAccountSummaryDoesNotRaceARevoke(t *testing.T) {
	store := state.New()
	acct, err := store.UpsertAccount(testIssuer, "alice", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	const tenants = 6
	for i := range tenants {
		id := fmt.Sprintf("cust_race_%d", i)
		store.AddCustomer(&state.Customer{ID: id, Token: "ysk_" + id, Plan: "pro",
			MaxConcurrentBursts: 5, MaxHourlyUSD: 10})
		if _, err := store.AddTenantMembership(acct.ID, id, state.RoleOwner); err != nil {
			t.Fatalf("membership on %s: %v", id, err)
		}
	}
	// One credential polling in a tight loop is exactly what the outbound
	// limiter sheds, and a 429 here would say nothing about the race under test.
	resolver := idProvider(t, map[string]string{"at_alice": "alice"})
	resolver.Burst, resolver.ResolvesPerSecond = 1000, 10000
	resolver.GlobalBurst, resolver.GlobalResolvesPerSecond = 1000, 10000
	a := &Accounts{Store: store, Resolver: resolver, Issuer: testIssuer, Log: quietLog()}

	// Readers start first and keep going until every revoke has landed, so the
	// two really do overlap: a revoke that finished before the first poll would
	// prove nothing. The tight loop is the one that puts pressure on the read of
	// the customer record itself; the HTTP polls alongside it check the whole
	// path a signed-in human actually takes.
	var wg sync.WaitGroup
	var reading atomic.Bool
	reading.Store(true)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for reading.Load() {
				tenants, err := a.tenantsFor(context.Background(), acct.ID)
				if err != nil {
					t.Errorf("tenantsFor: %v", err)
					return
				}
				for _, tenant := range tenants {
					if tenant.Plan != "pro" || tenant.Limits.MaxConcurrentBursts != 5 {
						t.Errorf("tenant %s rendered half-written: %+v", tenant.CustomerID, tenant)
						return
					}
				}
			}
		}()
	}
	// The poller decodes on its own rather than through decodeAccount: that
	// helper fails with t.Fatalf, which is runtime.Goexit, and calling it off
	// the test goroutine kills the worker without failing the test. The error
	// comes back over a channel and the test goroutine reports it.
	polling := make(chan struct{})
	pollErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(polling)
		for reading.Load() {
			rec := getAccount(a, "at_alice")
			if rec.Code != http.StatusOK {
				t.Errorf("account status = %d (body %q)", rec.Code, rec.Body.String())
				return
			}
			var res AccountResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
				select {
				case pollErr <- fmt.Errorf("decode account response: %w (body %q)", err, rec.Body.String()):
				default:
				}
				return
			}
			for _, tenant := range res.Tenants {
				if tenant.Plan != "pro" || tenant.Limits.MaxConcurrentBursts != 5 {
					t.Errorf("tenant %s rendered half-written: %+v", tenant.CustomerID, tenant)
					return
				}
			}
		}
	}()

	<-polling
	var revokes sync.WaitGroup
	for i := range tenants {
		id := fmt.Sprintf("cust_race_%d", i)
		revokes.Add(1)
		go func() {
			defer revokes.Done()
			if err := store.RevokeCustomer(id); err != nil {
				t.Errorf("RevokeCustomer(%s): %v", id, err)
			}
		}()
	}
	revokes.Wait()
	reading.Store(false)
	wg.Wait()
	select {
	case err := <-pollErr:
		t.Fatalf("poll: %v", err)
	default:
	}

	// Once every revoke has landed, a revoked tenant is nobody's tenant.
	if got := decodeAccount(t, getAccount(a, "at_alice")).Tenants; len(got) != 0 {
		t.Errorf("tenants after every revoke = %+v, want none", got)
	}
}

// memberMux wires the tenant roster and usage routes exactly as the enterprise
// build does, so these tests exercise the real patterns — including which
// requests never reach a handler at all.
func memberMux(a *Accounts) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/members", http.HandlerFunc(a.HandleListMembers))
	mux.Handle("POST /v1/tenants/{tenant_id}/members", http.HandlerFunc(a.HandleAddMember))
	mux.Handle("PATCH /v1/tenants/{tenant_id}/members/{account_id}", http.HandlerFunc(a.HandleUpdateMemberRole))
	mux.Handle("DELETE /v1/tenants/{tenant_id}/members/{account_id}", http.HandlerFunc(a.HandleRemoveMember))
	mux.Handle("GET /v1/tenants/{tenant_id}/usage", http.HandlerFunc(a.HandleGetUsage))
	return mux
}

func callMux(a *Accounts, method, path, token string) *httptest.ResponseRecorder {
	return callMuxBody(a, method, path, token, "")
}

func callMuxBody(a *Accounts, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	memberMux(a).ServeHTTP(rec, req)
	return rec
}

func listMembers(a *Accounts, token, tenantID string) *httptest.ResponseRecorder {
	return callMux(a, http.MethodGet, "/v1/tenants/"+tenantID+"/members", token)
}

func removeMember(a *Accounts, token, tenantID, accountID string) *httptest.ResponseRecorder {
	return callMux(a, http.MethodDelete, "/v1/tenants/"+tenantID+"/members/"+accountID, token)
}

func decodeMembers(t *testing.T, rec *httptest.ResponseRecorder) TenantMembersResponse {
	t.Helper()
	var res TenantMembersResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode members response: %v (body %q)", err, rec.Body.String())
	}
	return res
}

// rosterFixture builds a tenant with the named (subject → role) members, plus
// an Accounts handler whose resolver knows a "human_<subject>" token for each.
func rosterFixture(t *testing.T, store *state.Store, customerID string, roles map[string]string) (*Accounts, map[string]string) {
	t.Helper()
	tokens, ids := make(map[string]string, len(roles)), make(map[string]string, len(roles))
	for sub, role := range roles {
		acct, err := store.UpsertAccount(testIssuer, sub, state.AccountProfile{
			Email: sub + "@acme.com", EmailVerified: true, Name: strings.ToUpper(sub),
		})
		if err != nil {
			t.Fatalf("UpsertAccount %s: %v", sub, err)
		}
		if _, err := store.AddTenantMembership(acct.ID, customerID, role); err != nil {
			t.Fatalf("AddTenantMembership %s: %v", sub, err)
		}
		tokens["human_"+sub] = sub
		ids[sub] = acct.ID
	}
	return &Accounts{
		Store:    store,
		Resolver: idProvider(t, tokens),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}, ids
}

// Without Yscale ID the roster routes do not exist on this deployment, exactly
// as /v1/account does not — and the two credential types still do not cross:
// a cluster token authenticates nothing here, and an access token authenticates
// nothing on a workload route.
func TestMemberRoutesDisabledWithoutIdentityConfig(t *testing.T) {
	store := state.New()
	disabled := &Accounts{Store: store, Issuer: testIssuer, Log: quietLog()}
	for _, token := range []string{"", "human_alice"} {
		if code := listMembers(disabled, token, "cust_x").Code; code != http.StatusNotFound {
			t.Errorf("roster on an unconfigured deployment = %d, want 404", code)
		}
		if code := removeMember(disabled, token, "cust_x", "acct_y").Code; code != http.StatusNotFound {
			t.Errorf("removal on an unconfigured deployment = %d, want 404", code)
		}
	}

	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})

	// A cluster credential is not an identity, so it is refused here.
	if code := listMembers(accounts, "ysk_alice", "cust_alice").Code; code != http.StatusUnauthorized {
		t.Errorf("cluster token on the roster route = %d, want 401", code)
	}
	if code := removeMember(accounts, "ysk_alice", "cust_alice", ids["alice"]).Code; code != http.StatusUnauthorized {
		t.Errorf("cluster token on the removal route = %d, want 401", code)
	}

	// And the reverse, on a representative cluster-authenticated route.
	workloads := Auth(store, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/workloads", nil)
	req.Header.Set("Authorization", "Bearer human_alice")
	rec := httptest.NewRecorder()
	workloads.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("human access token on a workload route = %d, want 401", rec.Code)
	}
}

// The three ways a caller has no business with a tenant answer identically,
// down to the byte. Any difference enumerates other people's tenant ids.
func TestMemberRoutesHideUnknownRevokedAndNonMemberIdentically(t *testing.T) {
	store := state.New()
	for _, id := range []string{"cust_alice", "cust_revoked", "cust_other"} {
		store.AddCustomer(&state.Customer{ID: id, Token: "ysk_" + id, Plan: "pro"})
	}
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})
	if _, err := store.AddTenantMembership(ids["alice"], "cust_revoked", state.RoleOwner); err != nil {
		t.Fatalf("membership on the doomed tenant: %v", err)
	}
	if err := store.RevokeCustomer("cust_revoked"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}

	tenants := []struct{ name, id string }{
		{"unknown tenant", "cust_nope"},
		{"revoked tenant", "cust_revoked"},
		{"not a member", "cust_other"},
	}
	var wantBody string
	for i, tc := range tenants {
		rec := listMembers(accounts, "human_alice", tc.id)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: roster = %d, want 404 (body %q)", tc.name, rec.Code, rec.Body.String())
		}
		if i == 0 {
			wantBody = rec.Body.String()
		} else if rec.Body.String() != wantBody {
			t.Errorf("%s: 404 body %q differs from %q — the route is an oracle",
				tc.name, rec.Body.String(), wantBody)
		}
		del := removeMember(accounts, "human_alice", tc.id, ids["alice"])
		if del.Code != http.StatusNotFound || del.Body.String() != wantBody {
			t.Errorf("%s: removal = %d %q, want 404 %q", tc.name, del.Code, del.Body.String(), wantBody)
		}
	}

	// A verified human who has never signed in gets the same 404 — and managing
	// a roster is not a sign-in, so no account is minted on the way there.
	newcomer := &Accounts{
		Store:    store,
		Resolver: idProvider(t, map[string]string{"human_new": "newbie"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}
	rec := listMembers(newcomer, "human_new", "cust_alice")
	if rec.Code != http.StatusNotFound || rec.Body.String() != wantBody {
		t.Errorf("unknown human: roster = %d %q, want 404 %q", rec.Code, rec.Body.String(), wantBody)
	}
	if _, err := store.AccountByIdentity(testIssuer, "newbie"); err == nil {
		t.Fatal("a management route minted an account for a human who never signed in")
	}
}

// The roster is a management surface: owner and admin may read it, member and
// viewer may not — and they get 403, not 404, because they already know the
// tenant exists.
func TestMemberRosterRequiresOwnerOrAdmin(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "adm": state.RoleAdmin,
		"mem": state.RoleMember, "view": state.RoleViewer,
	})

	for _, sub := range []string{"alice", "adm"} {
		rec := listMembers(accounts, "human_"+sub, "cust_alice")
		if rec.Code != http.StatusOK {
			t.Errorf("%s listing the roster = %d, want 200 (body %q)", sub, rec.Code, rec.Body.String())
		}
	}
	for _, sub := range []string{"mem", "view"} {
		rec := listMembers(accounts, "human_"+sub, "cust_alice")
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s listing the roster = %d, want 403 (body %q)", sub, rec.Code, rec.Body.String())
		}
	}
}

// The roster carries the fields a co-member needs and nothing that identifies
// anyone to the identity provider, in an order that does not move between calls.
func TestMemberRosterFieldsAreSafeAndOrdered(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_secret_cluster_token", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "bob": state.RoleAdmin, "carol": state.RoleViewer,
	})

	rec := listMembers(accounts, "human_alice", "cust_alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	res := decodeMembers(t, rec)
	if len(res.Members) != 3 {
		t.Fatalf("members = %+v, want 3", res.Members)
	}
	for i := 1; i < len(res.Members); i++ {
		if res.Members[i-1].AccountID >= res.Members[i].AccountID {
			t.Fatalf("roster is not ordered by account id: %+v", res.Members)
		}
	}
	byID := make(map[string]TenantMemberSummary, len(res.Members))
	for _, m := range res.Members {
		byID[m.AccountID] = m
	}
	alice, ok := byID[ids["alice"]]
	if !ok {
		t.Fatalf("the owner is missing from the roster: %+v", res.Members)
	}
	if alice.Role != state.RoleOwner || alice.Email != "alice@acme.com" || alice.Name != "ALICE" {
		t.Errorf("owner row = %+v", alice)
	}
	if alice.CreatedAt.IsZero() {
		t.Error("owner row has no grant timestamp")
	}
	// Ordering is a property of the response, not of one call.
	if again := listMembers(accounts, "human_alice", "cust_alice"); again.Body.String() != rec.Body.String() {
		t.Errorf("roster body moved between identical calls:\n%s\n%s", rec.Body.String(), again.Body.String())
	}

	body := rec.Body.String()
	for _, forbidden := range []string{testIssuer, "ysk_secret_cluster_token", "human_alice", `"subject"`, `"issuer"`} {
		if strings.Contains(body, forbidden) {
			t.Errorf("roster leaked %q: %s", forbidden, body)
		}
	}
}

// The removal contract end to end: who may remove whom, what an already-absent
// target answers, and the owner floor.
func TestRemoveMemberAuthorityAndStatuses(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "bob": state.RoleOwner, "adm": state.RoleAdmin,
		"mem": state.RoleMember, "view": state.RoleViewer,
	})

	// A member has no authority over the roster.
	if code := removeMember(accounts, "human_mem", "cust_alice", ids["view"]).Code; code != http.StatusForbidden {
		t.Errorf("member removing a viewer = %d, want 403", code)
	}
	// An admin may remove a member but never an owner.
	if code := removeMember(accounts, "human_adm", "cust_alice", ids["alice"]).Code; code != http.StatusForbidden {
		t.Errorf("admin removing an owner = %d, want 403", code)
	}
	if code := removeMember(accounts, "human_adm", "cust_alice", ids["mem"]).Code; code != http.StatusNoContent {
		t.Errorf("admin removing a member = %d, want 204", code)
	}
	// Idempotent once authority is established.
	if code := removeMember(accounts, "human_adm", "cust_alice", ids["mem"]).Code; code != http.StatusNoContent {
		t.Errorf("re-removing an absent member = %d, want 204", code)
	}
	// ... but not for someone with no authority to establish.
	if code := removeMember(accounts, "human_mem", "cust_alice", ids["mem"]).Code; code != http.StatusNotFound {
		t.Errorf("a removed human removing an absent member = %d, want 404", code)
	}
	// An owner may stand down while another owner remains.
	if code := removeMember(accounts, "human_alice", "cust_alice", ids["alice"]).Code; code != http.StatusNoContent {
		t.Errorf("owner removing self = %d, want 204", code)
	}
	// The last owner is refused, and nothing changes.
	rec := removeMember(accounts, "human_bob", "cust_alice", ids["bob"])
	if rec.Code != http.StatusConflict {
		t.Fatalf("last owner removing self = %d, want 409 (body %q)", rec.Code, rec.Body.String())
	}
	if _, err := store.MembershipFor(ids["bob"], "cust_alice"); err != nil {
		t.Fatalf("the last owner's grant was removed by a 409: %v", err)
	}
	roster := decodeMembers(t, listMembers(accounts, "human_bob", "cust_alice"))
	if len(roster.Members) != 3 {
		t.Fatalf("roster = %+v, want bob, adm and view", roster.Members)
	}
}

// failingMemberStore is a store whose durable membership delete is refused. The
// store rolls the removal back (proved in the state package); this proves the
// handler answers 503 rather than reporting a removal that did not happen.
type failingMemberStore struct {
	*state.Store
	err error
}

func (f *failingMemberStore) RemoveTenantMembership(string, string, string, state.Actor) (*state.TenantMembership, error) {
	return nil, f.err
}

func TestRemoveMemberReturnsUnavailableWhenTheDeleteIsRefused(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "mem": state.RoleMember,
	})
	accounts.Store = &failingMemberStore{
		Store: store,
		err:   fmt.Errorf("%w: delete membership mbr_x: connection refused", state.ErrPersistence),
	}

	rec := removeMember(accounts, "human_alice", "cust_alice", ids["mem"])
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if _, err := store.MembershipFor(ids["mem"], "cust_alice"); err != nil {
		t.Fatalf("the grant did not survive a refused delete: %v", err)
	}
}

// A removed human's own account surface stops listing the tenant — the removal
// revokes visibility, it does not just edit a roster.
func TestRemovedMemberDropsTheTenantFromTheirAccount(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_keep", Token: "ysk_keep", Plan: "free"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "mem": state.RoleMember,
	})
	if _, err := store.AddTenantMembership(ids["mem"], "cust_keep", state.RoleViewer); err != nil {
		t.Fatalf("second membership: %v", err)
	}

	before := decodeAccount(t, getAccount(accounts, "human_mem"))
	if len(before.Tenants) != 2 {
		t.Fatalf("tenants before removal = %+v, want 2", before.Tenants)
	}
	if code := removeMember(accounts, "human_alice", "cust_alice", ids["mem"]).Code; code != http.StatusNoContent {
		t.Fatalf("removal = %d, want 204", code)
	}
	after := decodeAccount(t, getAccount(accounts, "human_mem"))
	if len(after.Tenants) != 1 || after.Tenants[0].CustomerID != "cust_keep" {
		t.Fatalf("tenants after removal = %+v, want only cust_keep", after.Tenants)
	}
	// And the tenant is now closed to them entirely.
	if code := listMembers(accounts, "human_mem", "cust_alice").Code; code != http.StatusNotFound {
		t.Errorf("removed human listing the roster = %d, want 404", code)
	}
}

// The roster routes share /v1/account's shedding contract: past the resolver's
// budget they answer 429 with Retry-After, and the token never leaves the box.
func TestMemberRoutesShedWhenLocallyRateLimited(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})
	resolver := accounts.Resolver.(*YscaleID)
	resolver.Burst = 1
	resolver.ResolvesPerSecond = 0.001 // no meaningful refill inside the test

	if code := listMembers(accounts, "human_alice", "cust_alice").Code; code != http.StatusOK {
		t.Fatalf("first call = %d, want 200", code)
	}
	rec := listMembers(accounts, "human_alice", "cust_alice")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("shed call = %d, want 429 (body %q)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got != retryAfterSeconds {
		t.Errorf("Retry-After = %q, want %q", got, retryAfterSeconds)
	}
}

// A request that does not name what these routes serve fails before it can
// change anything — wrong method, missing account id, missing tenant id.
func TestMemberRoutesRejectMalformedRequests(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "mem": state.RoleMember,
	})

	for _, tc := range []struct{ name, method, path string }{
		{"POST to the roster", http.MethodPost, "/v1/tenants/cust_alice/members"},
		{"DELETE with no account id", http.MethodDelete, "/v1/tenants/cust_alice/members"},
		{"GET a single member", http.MethodGet, "/v1/tenants/cust_alice/members/" + ids["mem"]},
		{"no tenant id", http.MethodGet, "/v1/tenants//members"},
		{"no path at all", http.MethodDelete, "/v1/tenants"},
	} {
		rec := callMux(accounts, tc.method, tc.path, "human_alice")
		if rec.Code >= 200 && rec.Code < 300 {
			t.Errorf("%s: status = %d, want a failure", tc.name, rec.Code)
		}
	}
	// Nothing moved.
	roster := decodeMembers(t, listMembers(accounts, "human_alice", "cust_alice"))
	if len(roster.Members) != 2 {
		t.Fatalf("roster after malformed requests = %+v, want 2 members", roster.Members)
	}
}

// A handler reached with no path values at all — which the mux cannot produce,
// but a future re-wiring could — refuses rather than acting on empty ids.
func TestMemberHandlersRefuseEmptyPathValues(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "mem": state.RoleMember,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants//members", nil)
	req.Header.Set("Authorization", "Bearer human_alice")
	accounts.HandleListMembers(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("roster with no tenant id = %d, want 404", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/v1/tenants/cust_alice/members/", nil)
	req.SetPathValue("tenant_id", "cust_alice")
	req.Header.Set("Authorization", "Bearer human_alice")
	accounts.HandleRemoveMember(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("removal with no account id = %d, want 404", rec.Code)
	}
	if _, err := store.MembershipFor(ids["mem"], "cust_alice"); err != nil {
		t.Fatalf("a malformed removal changed the roster: %v", err)
	}
}

// An admin who may manage the roster and reaches for an owner is told what is
// actually true. Both refusals are 403 and the messages are NOT interchangeable:
// "requires owner or admin" sends an admin looking for a permission they hold.
func TestProtectedOwnerForbiddenSaysWhyItRefused(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "adm": state.RoleAdmin, "mem": state.RoleMember,
	})

	protected := removeMember(accounts, "human_adm", "cust_alice", ids["alice"])
	if protected.Code != http.StatusForbidden {
		t.Fatalf("admin removing an owner = %d, want 403", protected.Code)
	}
	if !strings.Contains(protected.Body.String(), "only an owner may remove an owner") {
		t.Errorf("the owner-protected 403 does not say why: %s", protected.Body.String())
	}

	unauthorized := removeMember(accounts, "human_mem", "cust_alice", ids["adm"])
	if unauthorized.Code != http.StatusForbidden {
		t.Fatalf("member removing an admin = %d, want 403", unauthorized.Code)
	}
	if !strings.Contains(unauthorized.Body.String(), "requires owner or admin") {
		t.Errorf("the not-a-manager 403 changed: %s", unauthorized.Body.String())
	}
	if protected.Body.String() == unauthorized.Body.String() {
		t.Error("both 403s carry the same message; one of them is untrue")
	}
	// A refusal is still a refusal: nothing moved.
	if _, err := store.MembershipFor(ids["alice"], "cust_alice"); err != nil {
		t.Fatalf("the owner's grant went with a 403: %v", err)
	}
}

// The removal audit log records access that was actually revoked — tenant,
// caller, target and the role the target held — and stays silent for the
// idempotent retry, which revoked nothing.
func TestRemovalIsAuditedOnlyWhenItRemoves(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "mem": state.RoleMember,
	})
	var logs bytes.Buffer
	accounts.Log = slog.New(slog.NewTextHandler(&logs, nil))

	if code := removeMember(accounts, "human_alice", "cust_alice", ids["mem"]).Code; code != http.StatusNoContent {
		t.Fatalf("removal = %d, want 204", code)
	}
	audit := logs.String()
	if !strings.Contains(audit, "tenant member removed") {
		t.Fatalf("a real removal was not audited; log was:\n%s", audit)
	}
	for _, want := range []string{
		"tenant=cust_alice",
		"caller_account=" + ids["alice"],
		"account=" + ids["mem"],
		"role=member",
	} {
		if !strings.Contains(audit, want) {
			t.Errorf("the removal audit is missing %q; log was:\n%s", want, audit)
		}
	}
	// Never the credential that authorized it, nor the identity behind it.
	for _, forbidden := range []string{"human_alice", "ysk_alice", testIssuer, "Bearer"} {
		if strings.Contains(audit, forbidden) {
			t.Errorf("the removal audit leaked %q:\n%s", forbidden, audit)
		}
	}

	logs.Reset()
	if code := removeMember(accounts, "human_alice", "cust_alice", ids["mem"]).Code; code != http.StatusNoContent {
		t.Fatalf("idempotent removal = %d, want 204", code)
	}
	if strings.Contains(logs.String(), "tenant member removed") {
		t.Errorf("an already-absent target was audited as a removal:\n%s", logs.String())
	}
}

// The roster is paged deterministically: a page carries next_after only while
// there is more, the cursor resumes where the page stopped, and a client that
// walks it sees every member exactly once.
func TestMemberRosterPagesWithACursor(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	roles := map[string]string{"alice": state.RoleOwner}
	for i := 0; i < 5; i++ {
		roles[fmt.Sprintf("sub%d", i)] = state.RoleMember
	}
	accounts, _ := rosterFixture(t, store, "cust_alice", roles)

	seen, cursor := map[string]bool{}, ""
	for page := 0; page < len(roles); page++ {
		path := "/v1/tenants/cust_alice/members?limit=2"
		if cursor != "" {
			path += "&after=" + url.QueryEscape(cursor)
		}
		rec := callMux(accounts, http.MethodGet, path, "human_alice")
		if rec.Code != http.StatusOK {
			t.Fatalf("page %d = %d (body %q)", page, rec.Code, rec.Body.String())
		}
		res := decodeMembers(t, rec)
		if len(res.Members) > 2 {
			t.Fatalf("page %d carried %d members, want at most the limit of 2", page, len(res.Members))
		}
		for _, m := range res.Members {
			if seen[m.AccountID] {
				t.Fatalf("%s appeared on two pages", m.AccountID)
			}
			seen[m.AccountID] = true
		}
		if res.NextAfter == "" {
			if len(seen) != len(roles) {
				t.Fatalf("the walk ended after %d of %d members", len(seen), len(roles))
			}
			// The last page must not carry the field at all — a client that
			// keeps paging on its presence would loop forever.
			if strings.Contains(rec.Body.String(), "next_after") {
				t.Errorf("the last page still carries next_after: %s", rec.Body.String())
			}
			return
		}
		if res.NextAfter != res.Members[len(res.Members)-1].AccountID {
			t.Fatalf("next_after = %q, want the last id on the page", res.NextAfter)
		}
		cursor = res.NextAfter
	}
	t.Fatal("the cursor walk did not terminate")
}

// A roster query that does not say one thing is refused before the tenant is
// looked at — so the 400 is the same answer whether the tenant exists or not,
// and no page the caller did not ask for is ever returned.
func TestMemberRosterRejectsMalformedPageBounds(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "mem": state.RoleMember,
	})

	for _, tc := range []struct{ name, query string }{
		{"repeated limit", "?limit=1&limit=2"},
		{"repeated after", "?after=acct_a&after=acct_b"},
		{"empty limit", "?limit="},
		{"empty after", "?after="},
		{"limit zero", "?limit=0"},
		{"negative limit", "?limit=-1"},
		{"limit past the cap", fmt.Sprintf("?limit=%d", state.MaxRosterLimit+1)},
		{"limit is not a number", "?limit=all"},
		{"unknown parameter", "?page=2"},
	} {
		rec := callMux(accounts, http.MethodGet, "/v1/tenants/cust_alice/members"+tc.query, "human_alice")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400 (body %q)", tc.name, rec.Code, rec.Body.String())
			continue
		}
		// The same query against a tenant that does not exist answers
		// identically: a page bound must not become an existence oracle.
		other := callMux(accounts, http.MethodGet, "/v1/tenants/cust_nope/members"+tc.query, "human_alice")
		if other.Code != rec.Code || other.Body.String() != rec.Body.String() {
			t.Errorf("%s: unknown tenant answered %d %q, want %d %q",
				tc.name, other.Code, other.Body.String(), rec.Code, rec.Body.String())
		}
	}
	// The boundary values themselves are fine.
	for _, ok := range []string{"?limit=1", fmt.Sprintf("?limit=%d", state.MaxRosterLimit)} {
		if code := callMux(accounts, http.MethodGet, "/v1/tenants/cust_alice/members"+ok, "human_alice").Code; code != http.StatusOK {
			t.Errorf("%s = %d, want 200", ok, code)
		}
	}
	// And a deployment with no Yscale ID still has no route to send a bad
	// query to: the disabled 404 comes first.
	disabled := &Accounts{Store: store, Issuer: testIssuer, Log: quietLog()}
	if code := callMux(disabled, http.MethodGet, "/v1/tenants/cust_alice/members?limit=0", "human_alice").Code; code != http.StatusNotFound {
		t.Errorf("bad query on an unconfigured deployment = %d, want 404", code)
	}
}

// The roster a caller is handed is one instant of the store. A revoke racing
// the read lands either side of it: 200 with the roster as it stood, or the
// uniform 404 — never a 200 reporting that a tenant this caller manages has no
// members, which is what the lookup-then-list sequence could produce.
func TestMemberRosterDoesNotStraddleARevoke(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "bob": state.RoleAdmin, "mem": state.RoleMember,
	})
	// The resolver is a real HTTP round trip; give the readers room to overlap
	// the revoke rather than queue behind the rate limiter.
	resolver := accounts.Resolver.(*YscaleID)
	resolver.Burst = 64
	resolver.ResolvesPerSecond = 1000

	var wg sync.WaitGroup
	const readers = 8
	codes, sizes := make([]int, readers), make([]int, readers)
	start := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			rec := listMembers(accounts, "human_alice", "cust_alice")
			codes[i] = rec.Code
			if rec.Code == http.StatusOK {
				var res TenantMembersResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
					t.Errorf("decode roster: %v", err)
					return
				}
				sizes[i] = len(res.Members)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		if err := store.RevokeCustomer("cust_alice"); err != nil {
			t.Errorf("RevokeCustomer: %v", err)
		}
	}()
	close(start)
	wg.Wait()

	for i, code := range codes {
		switch code {
		case http.StatusOK:
			if sizes[i] == 0 {
				t.Fatalf("reader %d got 200 with an empty roster — the read straddled the revoke", i)
			}
		case http.StatusNotFound:
		default:
			t.Fatalf("reader %d = %d, want 200 or 404", i, code)
		}
	}
}

// failingRosterStore returns a fault the roster read cannot produce today. The
// point is what happens WHEN it can: the read absorbed three store calls into
// one, so the next error class added there must not be reported as a deleted
// tenant.
type failingRosterStore struct {
	*state.Store
	err error
}

func (f *failingRosterStore) TenantRosterForContext(context.Context, string, string, state.RosterQuery) (state.TenantRoster, error) {
	return state.TenantRoster{}, f.err
}

// An unrecognised roster fault is a retryable 503 with the real error in the
// log — not the uniform 404, which would tell an owner their live tenant was
// deleted and leave nothing behind to contradict it.
func TestListMembersDoesNotReportAnUnknownFaultAsAMissingTenant(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})
	var logs bytes.Buffer
	accounts.Log = slog.New(slog.NewTextHandler(&logs, nil))
	accounts.Store = &failingRosterStore{
		Store: store,
		err:   fmt.Errorf("%w: load roster: connection refused", state.ErrPersistence),
	}

	rec := listMembers(accounts, "human_alice", "cust_alice")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "connection refused") {
		t.Errorf("the backend's error reached the caller: %s", rec.Body.String())
	}
	if !strings.Contains(logs.String(), "connection refused") {
		t.Errorf("an unknown roster fault was not logged:\n%s", logs.String())
	}

	// The two errors the read DOES return today still answer as they did.
	accounts.Store = &failingRosterStore{Store: store, err: state.ErrNotFound}
	if code := listMembers(accounts, "human_alice", "cust_alice").Code; code != http.StatusNotFound {
		t.Errorf("ErrNotFound = %d, want 404", code)
	}
	accounts.Store = &failingRosterStore{Store: store, err: state.ErrNotAuthorized}
	if code := listMembers(accounts, "human_alice", "cust_alice").Code; code != http.StatusForbidden {
		t.Errorf("ErrNotAuthorized = %d, want 403", code)
	}
}
