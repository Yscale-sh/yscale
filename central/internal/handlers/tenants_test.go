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
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/factoryclient"
	"github.com/yscale-sh/yscale/central/internal/state"
)

type lifecycleFactoryFake struct {
	ensures chan ensureCall
	deletes chan string
}

type ensureCall struct {
	tenantID       string
	idempotencyKey string
}

func (f *lifecycleFactoryFake) EnsureFabric(_ context.Context, tenantID, idempotencyKey string) error {
	f.ensures <- ensureCall{tenantID: tenantID, idempotencyKey: idempotencyKey}
	return nil
}

func (f *lifecycleFactoryFake) GetFabric(context.Context, string) (factoryclient.Fabric, error) {
	return factoryclient.Fabric{}, nil
}

func (f *lifecycleFactoryFake) DeleteFabric(_ context.Context, tenantID string) error {
	f.deletes <- tenantID
	return nil
}

func newTenants(store *state.Store) *Tenants {
	// reap mimics the real reapBurst: claim removes the record (the durable
	// teardown then runs async), so offboard's verification sees 0 bursts.
	reap := func(_ context.Context, burstID, _ string) bool {
		_, ok, _ := store.ClaimBurst(burstID) // in-memory store: the persist error is always nil
		return ok
	}
	return &Tenants{Store: store, Reap: reap, Log: quietLog(), Endpoint: "ws://yscale-cloud.yscale:8443"}
}

func TestProvisionCreatesTenant(t *testing.T) {
	store := state.New()
	tn := newTenants(store)

	res, err := tn.Provision(ProvisionOpts{Email: "pilot@acme.com"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if !strings.HasPrefix(res.CustomerID, "cust_") || res.Token == "" {
		t.Fatalf("bad result: %+v", res)
	}
	// The token authenticates and the customer is stored.
	if _, err := store.AuthCustomer(res.Token); err != nil {
		t.Errorf("provisioned token should authenticate: %v", err)
	}
	// The install bundle must carry the token + endpoint so it's paste-ready.
	if !strings.Contains(res.AgentInstall, res.Token) || !strings.Contains(res.AgentInstall, res.Endpoint) {
		t.Errorf("install bundle missing token/endpoint: %q", res.AgentInstall)
	}
}

func TestRotateCustomerCredentialHandlerReturnsOneTimeToken(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_rotate", Token: "old_token", Plan: "pro"})
	tn := newTenants(store)
	mux := http.NewServeMux()
	mux.Handle("POST /v1/admin/tenants/{tenant_id}/credential", http.HandlerFunc(tn.HandleRotateCustomerCredential))

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/tenants/cust_rotate/credential", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("rotate = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var res TenantCredentialResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.CustomerID != "cust_rotate" || !strings.HasPrefix(res.Token, "ysk_") {
		t.Fatalf("response = %+v", res)
	}
	if _, err := store.AuthCustomer("old_token"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("old token authentication = %v, want ErrNotFound", err)
	}
	if got, err := store.AuthCustomer(res.Token); err != nil || got.ID != "cust_rotate" {
		t.Fatalf("new token authentication = (%v, %v)", got, err)
	}
}

// Provision applies pilot-default guardrails, honors explicit caps, and lets a
// negative value mean unlimited.
func TestProvisionAppliesLimits(t *testing.T) {
	store := state.New()
	tn := newTenants(store)

	def, _ := tn.Provision(ProvisionOpts{})
	if def.Limits.MaxConcurrentBursts != defaultMaxConcurrentBursts || def.Limits.MaxHourlyUSD != defaultMaxHourlyUSD {
		t.Errorf("default limits = %+v, want %d/%v", def.Limits, defaultMaxConcurrentBursts, defaultMaxHourlyUSD)
	}
	exp, _ := tn.Provision(ProvisionOpts{MaxConcurrentBursts: 3, MaxHourlyUSD: 7})
	if exp.Limits.MaxConcurrentBursts != 3 || exp.Limits.MaxHourlyUSD != 7 {
		t.Errorf("explicit limits = %+v, want 3/7", exp.Limits)
	}
	// Stored customer reflects the cap.
	c, _ := store.CustomerByID(exp.CustomerID)
	if c.MaxConcurrentBursts != 3 || c.MaxHourlyUSD != 7 {
		t.Errorf("stored customer limits = %d/%v", c.MaxConcurrentBursts, c.MaxHourlyUSD)
	}
	unl, _ := tn.Provision(ProvisionOpts{MaxConcurrentBursts: -1, MaxHourlyUSD: -1})
	if unl.Limits.MaxConcurrentBursts != 0 || unl.Limits.MaxHourlyUSD != 0 {
		t.Errorf("unlimited limits = %+v, want 0/0", unl.Limits)
	}
}

func TestProvisionRejectsDuplicate(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	if _, err := tn.Provision(ProvisionOpts{ID: "cust_dup"}); err != nil {
		t.Fatalf("first provision: %v", err)
	}
	if _, err := tn.Provision(ProvisionOpts{ID: "cust_dup"}); err == nil {
		t.Error("provisioning an existing tenant id should error")
	}
}

func TestProvisionStartsFabricAsyncWithStableKey(t *testing.T) {
	store := state.New()
	factory := &lifecycleFactoryFake{ensures: make(chan ensureCall, 1), deletes: make(chan string, 1)}
	tracked := make(chan string, 1)
	tn := newTenants(store)
	tn.Factory = factory
	tn.TrackFabric = func(customerID string) { tracked <- customerID }

	result, err := tn.Provision(ProvisionOpts{ID: "cust_factory"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	select {
	case customerID := <-tracked:
		if customerID != result.CustomerID {
			t.Fatalf("tracked customer = %q, want %q", customerID, result.CustomerID)
		}
	case <-time.After(time.Second):
		t.Fatal("Provision did not seed the fabric poller")
	}
	select {
	case call := <-factory.ensures:
		if call.tenantID != result.CustomerID || call.idempotencyKey != result.CustomerID {
			t.Fatalf("EnsureFabric call = %+v, want tenant and key %q", call, result.CustomerID)
		}
	case <-time.After(time.Second):
		t.Fatal("Provision did not start fabric creation")
	}
}

func TestOffboardReapsAndVerifiesClean(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	store.AddCustomer(&state.Customer{ID: "cust_a", Token: "tok_a", Plan: "pro"})
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_a"})
	store.PutBurst(&state.Burst{ID: "b2", CustomerID: "cust_a"})
	store.PutBurst(&state.Burst{ID: "other", CustomerID: "cust_b"}) // must be untouched

	rep, err := tn.Offboard(context.Background(), "cust_a")
	if err != nil {
		t.Fatalf("Offboard: %v", err)
	}
	if rep.BurstsReaped != 2 || !rep.CustomerDeleted || !rep.CustomerRecordDeleted || !rep.Clean {
		t.Fatalf("report = %+v, want 2 reaped, access cut, record deleted, clean", rep)
	}
	if _, err := store.CustomerByID("cust_a"); err == nil {
		t.Error("customer should be gone after offboard")
	}
	if _, err := store.AuthCustomer("tok_a"); err == nil {
		t.Error("offboarded token must not authenticate")
	}
	if n := len(store.BurstsForCustomer("cust_a")); n != 0 {
		t.Errorf("tenant still has %d bursts after offboard", n)
	}
	if n := len(store.BurstsForCustomer("cust_b")); n != 1 {
		t.Error("another tenant's burst must not be touched")
	}
}

func TestOffboardUnknownTenant(t *testing.T) {
	tn := newTenants(state.New())
	if _, err := tn.Offboard(context.Background(), "cust_missing"); err == nil {
		t.Error("offboarding an unknown tenant should error")
	}
}

// AdminAuth: disabled (no token) → 404; wrong/absent token → 401; correct →
// passes through.
func TestAdminAuth(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	cases := []struct {
		name, configured, sent string
		want                   int
	}{
		{"disabled when unset", "", "Bearer x", http.StatusNotFound},
		{"missing token", "secret", "", http.StatusUnauthorized},
		{"wrong token", "secret", "Bearer nope", http.StatusUnauthorized},
		{"correct token", "secret", "Bearer secret", http.StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/admin/tenants", nil)
			if c.sent != "" {
				req.Header.Set("Authorization", c.sent)
			}
			rec := httptest.NewRecorder()
			AdminAuth(c.configured, ok).ServeHTTP(rec, req)
			if rec.Code != c.want {
				t.Errorf("status = %d, want %d", rec.Code, c.want)
			}
		})
	}
}

// End-to-end through the HTTP handlers: provision returns a usable token, then
// offboard cleans it up.
func TestAdminEndpointsProvisionThenOffboard(t *testing.T) {
	store := state.New()
	tn := newTenants(store)

	rec := httptest.NewRecorder()
	tn.HandleProvision(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/tenants", strings.NewReader(`{"email":"p@acme.com"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("provision status = %d", rec.Code)
	}
	var res ProvisionResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode provision: %v", err)
	}
	if _, err := store.AuthCustomer(res.Token); err != nil {
		t.Fatalf("provisioned token should authenticate: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/tenants/"+res.CustomerID, nil)
	req.SetPathValue("id", res.CustomerID)
	rec2 := httptest.NewRecorder()
	tn.HandleOffboard(rec2, req)
	if rec2.Code != http.StatusOK {
		t.Fatalf("offboard status = %d", rec2.Code)
	}
	var rep OffboardReport
	if err := json.Unmarshal(rec2.Body.Bytes(), &rep); err != nil {
		t.Fatalf("decode offboard: %v", err)
	}
	if !rep.CustomerDeleted || !rep.CustomerRecordDeleted || !rep.Clean {
		t.Errorf("offboard report = %+v, want deleted + clean", rep)
	}
	if _, err := store.AuthCustomer(res.Token); err == nil {
		t.Error("token should be dead after offboard")
	}
}

func TestOffboardWarnsOnHeadscaleBox(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	store.AddCustomer(&state.Customer{
		ID: "cust_hs", Token: "tok_hs", Plan: "pro",
		Mesh: &state.MeshEndpoint{Provider: "headscale", LoginServer: "https://box", User: "acme", BackendID: "linode-9"},
	})

	rep, err := tn.Offboard(context.Background(), "cust_hs")
	if err != nil {
		t.Fatalf("Offboard: %v", err)
	}
	// The box itself (Linode VM + firewall) can't be removed from here, so it
	// must be flagged — not silently left as the stale-state gotcha.
	if rep.Clean {
		t.Error("offboard should NOT report clean while a Headscale box needs shell deprovision")
	}
	joined := strings.Join(rep.Warnings, " ")
	if !strings.Contains(joined, "deprovision") || !strings.Contains(joined, "linode-9") {
		t.Errorf("expected a Headscale box warning naming the box, got %v", rep.Warnings)
	}
}

func TestOffboardStartsFactoryDeleteWhenConfigured(t *testing.T) {
	store := state.New()
	factory := &lifecycleFactoryFake{ensures: make(chan ensureCall, 1), deletes: make(chan string, 1)}
	tn := newTenants(store)
	tn.Factory = factory
	store.AddCustomer(&state.Customer{
		ID: "cust_factory", Token: "tok_factory", Plan: "pro",
		Mesh: &state.MeshEndpoint{Provider: "box", LoginServer: "https://box", User: "acme", BackendID: "linode-9"},
	})

	report, err := tn.Offboard(context.Background(), "cust_factory")
	if err != nil {
		t.Fatalf("Offboard: %v", err)
	}
	if !report.Clean {
		t.Fatalf("offboard report = %+v, want clean with factory teardown", report)
	}
	select {
	case customerID := <-factory.deletes:
		if customerID != "cust_factory" {
			t.Fatalf("DeleteFabric customer = %q", customerID)
		}
	case <-time.After(time.Second):
		t.Fatal("Offboard did not start factory delete")
	}
}

// A tenant offboarded mid-provisioning has no Mesh attached yet, but its box may
// still be under construction — teardown must fire regardless or the fabric orphans.
func TestOffboardStartsFactoryDeleteBeforeMeshAttached(t *testing.T) {
	store := state.New()
	factory := &lifecycleFactoryFake{ensures: make(chan ensureCall, 1), deletes: make(chan string, 1)}
	forgotten := make(chan string, 1)
	tn := newTenants(store)
	tn.Factory = factory
	tn.ForgetFabric = func(customerID string) { forgotten <- customerID }
	// No Mesh — the fabric is still provisioning when the tenant is offboarded.
	store.AddCustomer(&state.Customer{ID: "cust_factory", Token: "tok_factory", Plan: "pro"})

	report, err := tn.Offboard(context.Background(), "cust_factory")
	if err != nil {
		t.Fatalf("Offboard: %v", err)
	}
	if !report.Clean {
		t.Fatalf("offboard report = %+v, want clean", report)
	}
	select {
	case customerID := <-factory.deletes:
		if customerID != "cust_factory" {
			t.Fatalf("DeleteFabric customer = %q", customerID)
		}
	case <-time.After(time.Second):
		t.Fatal("Offboard did not start factory delete for an in-flight fabric")
	}
	select {
	case <-forgotten:
	case <-time.After(time.Second):
		t.Fatal("Offboard did not stop tracking the fabric")
	}
}

// With an owner_subject, provisioning also installs the tenant's first human
// owner — one call stands up the tenant AND the account that can see it.
func TestProvisionAssignsOwnerBySubject(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	tn.IdentityIssuer = "https://id.yscale.sh"

	res, err := tn.Provision(ProvisionOpts{ID: "cust_owned", OwnerSubject: "alice"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if res.OwnerAccountID == "" {
		t.Fatal("provision result carries no owner account id")
	}
	acct, err := store.AccountByIdentity("https://id.yscale.sh", "alice")
	if err != nil {
		t.Fatalf("owner account not created: %v", err)
	}
	if acct.ID != res.OwnerAccountID {
		t.Errorf("result owner %s != stored account %s", res.OwnerAccountID, acct.ID)
	}
	members := store.MembershipsForAccount(acct.ID)
	if len(members) != 1 || members[0].CustomerID != "cust_owned" || members[0].Role != state.RoleOwner {
		t.Fatalf("owner membership = %+v", members)
	}
	// The owner is a human account, not a second way into the cluster token.
	if _, err := store.AuthCustomer("alice"); err == nil {
		t.Error("an owner subject authenticated as a cluster token")
	}
}

// Omitting owner_subject is the pre-existing call: same result shape, no
// owner_account_id key at all, and no account state created.
func TestProvisionWithoutOwnerSubjectIsUnchanged(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	tn.IdentityIssuer = "https://id.yscale.sh"

	rec := httptest.NewRecorder()
	tn.HandleProvision(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/tenants",
		strings.NewReader(`{"email":"p@acme.com"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("provision status = %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode provision: %v", err)
	}
	if _, present := body["owner_account_id"]; present {
		t.Errorf("owner_account_id present with no owner_subject: %v", body)
	}
	for _, want := range []string{"customer_id", "token", "endpoint", "agent_install", "limits"} {
		if _, present := body[want]; !present {
			t.Errorf("response lost %q: %v", want, body)
		}
	}
	var res ProvisionResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode provision result: %v", err)
	}
	if _, err := store.AuthCustomer(res.Token); err != nil {
		t.Fatalf("provisioned token should authenticate: %v", err)
	}
}

// owner_subject arrives over the wire (it needs its own JSON tag — Go's field
// matcher does not bridge the underscore).
func TestHandleProvisionAcceptsOwnerSubjectJSON(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	tn.IdentityIssuer = "https://id.yscale.sh"

	rec := httptest.NewRecorder()
	tn.HandleProvision(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/tenants",
		strings.NewReader(`{"id":"cust_wire","owner_subject":"alice"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("provision status = %d (body %q)", rec.Code, rec.Body.String())
	}
	acct, err := store.AccountByIdentity("https://id.yscale.sh", "alice")
	if err != nil {
		t.Fatalf("owner_subject did not reach Provision: %v", err)
	}
	members := store.MembershipsForAccount(acct.ID)
	if len(members) != 1 || members[0].CustomerID != "cust_wire" {
		t.Fatalf("owner membership = %+v", members)
	}
}

// A failed owner assignment leaves nothing behind: no tenant, no token, no
// account, and no factory job. Atomic from the caller's view.
func TestProvisionOwnerFailureLeavesNoTenant(t *testing.T) {
	tests := []struct {
		name       string
		issuer     string
		subject    string
		wantStatus int
	}{
		{"issuer not configured", "", "alice", http.StatusServiceUnavailable},
		{"blank subject", "https://id.yscale.sh", "   ", http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := state.New()
			factory := &lifecycleFactoryFake{ensures: make(chan ensureCall, 1), deletes: make(chan string, 1)}
			tn := newTenants(store)
			tn.IdentityIssuer = tc.issuer
			tn.Factory = factory
			tn.TrackFabric = func(string) { t.Error("factory job started for a failed provision") }

			if _, err := tn.Provision(ProvisionOpts{ID: "cust_fail", OwnerSubject: tc.subject}); err == nil {
				t.Fatal("Provision should fail when the owner cannot be assigned")
			}
			if _, err := store.CustomerByID("cust_fail"); err == nil {
				t.Fatal("a half-provisioned tenant survived a failed owner assignment")
			}
			select {
			case call := <-factory.ensures:
				t.Fatalf("factory job dispatched for a failed provision: %+v", call)
			default:
			}

			// And the HTTP surface reports it as its own failure, not a conflict.
			rec := httptest.NewRecorder()
			tn.HandleProvision(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/tenants",
				strings.NewReader(`{"id":"cust_fail2","owner_subject":"`+tc.subject+`"}`)))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if _, err := store.CustomerByID("cust_fail2"); err == nil {
				t.Fatal("handler left a half-provisioned tenant behind")
			}
		})
	}
}

// Offboarding an owned tenant takes the owner's membership with it while the
// account itself survives.
func TestOffboardClearsOwnerMembership(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	tn.IdentityIssuer = "https://id.yscale.sh"

	res, err := tn.Provision(ProvisionOpts{ID: "cust_owned", OwnerSubject: "alice"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if _, err := tn.Offboard(context.Background(), "cust_owned"); err != nil {
		t.Fatalf("Offboard: %v", err)
	}
	if got := store.MembershipsForAccount(res.OwnerAccountID); len(got) != 0 {
		t.Fatalf("owner kept a membership in the offboarded tenant: %+v", got)
	}
	if _, err := store.AccountByIdentity("https://id.yscale.sh", "alice"); err != nil {
		t.Errorf("offboard deleted the human account: %v", err)
	}
}

// A store that could not persist the account or the membership is a retryable
// server fault, not a conflict: the request was valid and nothing about it
// collides. The pre-existing 409 (duplicate id) and 400 (bad identity input)
// answers have to survive that addition.
func TestProvisionStatusSeparatesPersistenceFromConflict(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"account write refused", fmt.Errorf("resolve owner account: %w: persist account acct_x: down",
			state.ErrPersistence), http.StatusServiceUnavailable},
		{"membership write refused", fmt.Errorf("assign tenant owner: %w: persist membership mbr_x: down",
			state.ErrPersistence), http.StatusServiceUnavailable},
		{"identity provider not configured", errIdentityNotConfigured, http.StatusServiceUnavailable},
		{"blank subject", fmt.Errorf("resolve owner account: %w", state.ErrInvalidIdentity), http.StatusBadRequest},
		{"duplicate tenant id", errors.New(`tenant "cust_a" already exists`), http.StatusConflict},
		{"id taken durably", fmt.Errorf("create tenant cust_a: %w: cust_a",
			state.ErrCustomerExists), http.StatusConflict},
		{"owner role conflict", fmt.Errorf("assign tenant owner: %w", state.ErrRoleConflict), http.StatusConflict},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := provisionStatus(tc.err); got != tc.want {
				t.Errorf("provisionStatus(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

// revokeFailStore is a real store whose durable revoke is refusing writes —
// what a central looks like with Postgres down under it. Everything else
// behaves, so a test can watch what the offboard does with the failure.
type revokeFailStore struct {
	*state.Store
	err error
}

func (s *revokeFailStore) RevokeCustomer(string) error { return s.err }

// An offboard that cannot revoke the tenant's access must not tear anything
// down. Everything after the revoke is irreversible — a reaped burst is a
// destroyed VM, a deleted fabric is a deleted box — so doing it while the token
// still authenticates leaves the partner with no resources and a live
// credential, and the operator with nothing to retry.
func TestOffboardTearsNothingDownWhenRevokeFails(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_a", Token: "tok_a", Plan: "pro"})
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_a"})
	factory := &lifecycleFactoryFake{ensures: make(chan ensureCall, 1), deletes: make(chan string, 1)}

	tn := newTenants(store)
	tn.Store = &revokeFailStore{Store: store, err: fmt.Errorf(
		"%w: revoke customer cust_a: connection refused", state.ErrPersistence)}
	tn.Factory = factory
	tn.Reap = func(context.Context, string, string) bool {
		t.Error("bursts reaped for a tenant whose access was never revoked")
		return false
	}
	tn.ForgetFabric = func(string) { t.Error("stopped tracking the fabric of a tenant that is still live") }

	rep, err := tn.Offboard(context.Background(), "cust_a")
	if !errors.Is(err, state.ErrPersistence) {
		t.Fatalf("Offboard error = %v, want ErrPersistence", err)
	}
	if rep != nil {
		t.Errorf("a report was returned for an offboard that did nothing: %+v", rep)
	}
	select {
	case id := <-factory.deletes:
		t.Fatalf("fabric delete dispatched for %s though the revoke failed", id)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := store.AuthCustomer("tok_a"); err != nil {
		t.Errorf("the tenant's token stopped authenticating despite the failed revoke: %v", err)
	}
	if n := len(store.BurstsForCustomer("cust_a")); n != 1 {
		t.Errorf("bursts after a failed offboard = %d, want the 1 that was there", n)
	}

	// The operator is told to retry, not that the tenant is missing.
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/tenants/cust_a", nil)
	req.SetPathValue("id", "cust_a")
	rec := httptest.NewRecorder()
	tn.HandleOffboard(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}

	// And an unknown tenant is still the 404 it has always been.
	missing := httptest.NewRequest(http.MethodDelete, "/v1/admin/tenants/cust_nope", nil)
	missing.SetPathValue("id", "cust_nope")
	rec404 := httptest.NewRecorder()
	tn.HandleOffboard(rec404, missing)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("unknown tenant status = %d, want 404", rec404.Code)
	}
}

// The mirror of the case above: when the revoke lands, the token is dead the
// moment the handler answers — before any of the teardown it kicked off has
// necessarily finished.
func TestOffboardRevokesAccessBeforeTeardown(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	tn.IdentityIssuer = "https://id.yscale.sh"
	res, err := tn.Provision(ProvisionOpts{ID: "cust_owned", OwnerSubject: "alice"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_owned"})

	// The reap runs while the offboard is mid-flight; by then the credential it
	// belongs to must already be refused.
	tn.Reap = func(_ context.Context, burstID, _ string) bool {
		if _, err := store.AuthCustomer(res.Token); err == nil {
			t.Error("teardown started while the offboarded token still authenticated")
		}
		_, ok, _ := store.ClaimBurst(burstID)
		return ok
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/tenants/cust_owned", nil)
	req.SetPathValue("id", "cust_owned")
	rec := httptest.NewRecorder()
	tn.HandleOffboard(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var rep OffboardReport
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatalf("decode offboard: %v", err)
	}
	if !rep.CustomerDeleted || rep.BurstsReaped != 1 || !rep.Clean {
		t.Fatalf("report = %+v, want deleted, 1 reaped, clean", rep)
	}
	if _, err := store.AuthCustomer(res.Token); err == nil {
		t.Error("the offboarded token still authenticates")
	}
	if got := store.MembershipsForAccount(res.OwnerAccountID); len(got) != 0 {
		t.Errorf("owner kept a grant on the offboarded tenant: %+v", got)
	}
}

// customerWriteFailStore is a store whose customer rows are being refused,
// which is what stands between a provisioned tenant and its owner's grant.
type customerWriteFailStore struct {
	*state.Store
	err error
}

func (s *customerWriteFailStore) CreateTenant(*state.Customer, string) (*state.Customer, *state.TenantMembership, error) {
	return nil, nil, s.err
}

// A tenant provisioned for an owner has to be durable before the grant that
// points at it. When the customer row is refused, the provision fails whole: no
// tenant, no token, no membership, no factory job, nothing tracked by the
// onboarding poller — and a 503, because the request was fine and the backend
// was not.
func TestProvisionWithOwnerFailsClosedWhenCustomerWriteIsRefused(t *testing.T) {
	store := state.New()
	factory := &lifecycleFactoryFake{ensures: make(chan ensureCall, 1), deletes: make(chan string, 1)}
	tn := newTenants(store)
	tn.IdentityIssuer = "https://id.yscale.sh"
	tn.Factory = factory
	tn.TrackFabric = func(string) { t.Error("onboarding poller tracked a tenant that was never created") }
	tn.Store = &customerWriteFailStore{Store: store, err: fmt.Errorf(
		"%w: persist customer cust_fail: connection refused", state.ErrPersistence)}

	_, err := tn.Provision(ProvisionOpts{ID: "cust_fail", OwnerSubject: "alice"})
	if !errors.Is(err, state.ErrPersistence) {
		t.Fatalf("Provision error = %v, want ErrPersistence", err)
	}
	if _, err := store.CustomerByID("cust_fail"); err == nil {
		t.Error("a tenant survived a refused customer write")
	}
	select {
	case call := <-factory.ensures:
		t.Fatalf("factory job dispatched for a tenant that was never created: %+v", call)
	case <-time.After(100 * time.Millisecond):
	}
	// The account is resolved before the tenant and deliberately survives — it
	// is an identity record, not tenant state — but it holds no grant.
	acct, err := store.AccountByIdentity("https://id.yscale.sh", "alice")
	if err != nil {
		t.Fatalf("owner account: %v", err)
	}
	if got := store.MembershipsForAccount(acct.ID); len(got) != 0 {
		t.Errorf("grants after a failed provision = %+v, want none", got)
	}

	rec := httptest.NewRecorder()
	tn.HandleProvision(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/tenants",
		strings.NewReader(`{"id":"cust_fail2","owner_subject":"alice"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if _, err := store.CustomerByID("cust_fail2"); err == nil {
		t.Error("the handler left a tenant behind")
	}
}

// finalDeleteFailStore is a real store whose last offboard step — the delete of
// the already-revoked customer row — is being refused. Access is gone by then,
// so the only question is whether the operator can try again.
type finalDeleteFailStore struct {
	*state.Store
	err error
}

func (s *finalDeleteFailStore) DeleteRevokedCustomer(string) error { return s.err }

// An offboard that could not finish its cleanup keeps the tenant. Revoke is not
// delete: the token is already dead, so nothing is at risk in waiting, and the
// record is the only handle the operator has on the bursts the reap left
// behind. Deleting it there would strand them with no id to name them by.
func TestOffboardKeepsRevokedTenantWhenCleanupIsIncomplete(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	tn.IdentityIssuer = "https://id.yscale.sh"
	res, err := tn.Provision(ProvisionOpts{ID: "cust_stuck", OwnerSubject: "alice"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_stuck"})
	tn.Reap = func(context.Context, string, string) bool { return false } // teardown queue is wedged

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/tenants/cust_stuck", nil)
	req.SetPathValue("id", "cust_stuck")
	rec := httptest.NewRecorder()
	tn.HandleOffboard(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var rep OffboardReport
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatalf("decode offboard: %v", err)
	}
	// customer_deleted is the legacy ALIAS of customer_revoked — it names no
	// deletion and goes up with the revoke; customer_record_deleted is the one
	// that names a physical row removal, and no row was removed.
	if !rep.CustomerRevoked || !rep.CustomerDeleted || rep.CustomerRecordDeleted || rep.Clean {
		t.Fatalf("report = %+v, want access cut, record kept, no row deletion claimed", rep)
	}
	if len(rep.Warnings) != 1 || !strings.Contains(rep.Warnings[0], "retry offboard") {
		t.Fatalf("warnings = %+v, want one that tells the operator to retry", rep.Warnings)
	}
	if _, err := store.AuthCustomer(res.Token); err == nil {
		t.Error("the token of a revoked tenant still authenticates")
	}
	if got := store.MembershipsForAccount(res.OwnerAccountID); len(got) != 0 {
		t.Errorf("owner kept a grant on a revoked tenant: %+v", got)
	}
	if _, err := store.CustomerByID("cust_stuck"); err != nil {
		t.Fatalf("the record an operator has to retry against is gone: %v", err)
	}

	// The retry, with the queue working again, finishes the job: the revoke
	// no-ops, the burst is reaped, and the record finally goes.
	tn.Reap = func(_ context.Context, burstID, _ string) bool {
		_, ok, _ := store.ClaimBurst(burstID)
		return ok
	}
	retry, err := tn.Offboard(context.Background(), "cust_stuck")
	if err != nil {
		t.Fatalf("retry offboard: %v", err)
	}
	if !retry.CustomerRevoked || !retry.CustomerDeleted || !retry.CustomerRecordDeleted ||
		!retry.Clean || retry.BurstsReaped != 1 {
		t.Fatalf("retry report = %+v, want revoked, deleted, record gone, clean, 1 reaped", retry)
	}
	if _, err := store.CustomerByID("cust_stuck"); err == nil {
		t.Error("customer should be gone after a clean offboard")
	}
}

// The final delete is durable too, so it can fail. When it does the answer is
// 503 and the tenant stays exactly as it is: revoked, authenticating nothing,
// and there to try again — not half-deleted in memory and alive in Postgres.
func TestOffboardFinalDeleteFailureIsRetryable(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	res, err := tn.Provision(ProvisionOpts{ID: "cust_last"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	tn.Store = &finalDeleteFailStore{Store: store, err: fmt.Errorf(
		"%w: delete revoked customer cust_last: connection refused", state.ErrPersistence)}

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/tenants/cust_last", nil)
	req.SetPathValue("id", "cust_last")
	rec := httptest.NewRecorder()
	tn.HandleOffboard(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if _, err := store.AuthCustomer(res.Token); err == nil {
		t.Error("access came back when the final delete failed")
	}
	c, err := store.CustomerByID("cust_last")
	if err != nil {
		t.Fatalf("the revoked record was dropped despite the failed delete: %v", err)
	}
	if !c.Revoked() {
		t.Error("customer is not stamped revoked")
	}

	// With the backend back, the same call finishes.
	tn.Store = store
	rep, err := tn.Offboard(context.Background(), "cust_last")
	if err != nil {
		t.Fatalf("retry offboard: %v", err)
	}
	if !rep.CustomerDeleted || !rep.CustomerRecordDeleted || !rep.Clean {
		t.Fatalf("retry report = %+v, want deleted and clean", rep)
	}
	if _, err := store.CustomerByID("cust_last"); err == nil {
		t.Error("customer should be gone after the retry")
	}
}

// A tenant provisioned WITHOUT an owner is durable too. The token is handed to
// the operator once either way, so a best-effort create here would hand out a
// credential that stops authenticating at the next restart — the same failure
// the owner path fails closed on.
func TestProvisionWithoutOwnerIsDurable(t *testing.T) {
	store := state.New()
	factory := &lifecycleFactoryFake{ensures: make(chan ensureCall, 1), deletes: make(chan string, 1)}
	tn := newTenants(store)
	tn.Factory = factory
	tn.TrackFabric = func(string) { t.Error("onboarding poller tracked a tenant that was never created") }
	tn.Store = &customerWriteFailStore{Store: store, err: fmt.Errorf(
		"%w: persist customer cust_noowner: connection refused", state.ErrPersistence)}

	res, err := tn.Provision(ProvisionOpts{ID: "cust_noowner"})
	if !errors.Is(err, state.ErrPersistence) {
		t.Fatalf("Provision error = %v, want ErrPersistence", err)
	}
	if res != nil {
		t.Errorf("a token was returned for a tenant whose row never landed: %+v", res)
	}
	if _, err := store.CustomerByID("cust_noowner"); err == nil {
		t.Error("a tenant survived a refused customer write")
	}
	select {
	case call := <-factory.ensures:
		t.Fatalf("factory job dispatched for a tenant that was never created: %+v", call)
	case <-time.After(100 * time.Millisecond):
	}

	rec := httptest.NewRecorder()
	tn.HandleProvision(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/tenants",
		strings.NewReader(`{"id":"cust_noowner2"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
}

// A provision whose durable create was refused leaves the id FREE. There is no
// rollback to get this right any more — the tenant and its owner grant are one
// transaction — so what used to be a revoke-then-delete sequence that could
// itself fail halfway is now just an id that was never taken.
func TestProvisionLeavesTheIDFreeWhenTheCreateIsRefused(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	tn.IdentityIssuer = "https://id.yscale.sh"
	tn.Store = &customerWriteFailStore{Store: store, err: fmt.Errorf(
		"%w: persist tenant cust_roll: connection refused", state.ErrPersistence)}

	res, err := tn.Provision(ProvisionOpts{ID: "cust_roll", OwnerSubject: "alice"})
	if !errors.Is(err, state.ErrPersistence) {
		t.Fatalf("Provision error = %v, want ErrPersistence", err)
	}
	if res != nil {
		t.Errorf("a token was returned for a provision that never landed: %+v", res)
	}
	if _, err := store.CustomerByID("cust_roll"); err == nil {
		t.Fatal("a refused provision left the id occupied; the operator's retry would 409")
	}

	// The id really is free: the same provision, with the backend behaving,
	// now succeeds rather than colliding with its own leftovers.
	tn.Store = store
	if _, err := tn.Provision(ProvisionOpts{ID: "cust_roll", OwnerSubject: "alice"}); err != nil {
		t.Fatalf("re-provisioning the freed id: %v", err)
	}
}

// The owner grant rides the tenant's own transaction, so a refused grant is a
// refused tenant: no customer, no token, no account grant, no factory job. The
// account itself survives — it is an identity record, not tenant state.
func TestProvisionOwnerGrantAndTenantLandTogether(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	tn.IdentityIssuer = "https://id.yscale.sh"

	res, err := tn.Provision(ProvisionOpts{ID: "cust_pair", OwnerSubject: "alice"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	members := store.MembershipsForAccount(res.OwnerAccountID)
	if len(members) != 1 || members[0].CustomerID != "cust_pair" || members[0].Role != state.RoleOwner {
		t.Fatalf("owner grant = %+v, want one owner grant on cust_pair", members)
	}
	if _, err := store.AuthCustomer(res.Token); err != nil {
		t.Fatalf("provisioned token does not authenticate: %v", err)
	}

	// Same call against a store whose create transaction is refused: neither
	// half is left behind.
	tn.Store = &customerWriteFailStore{Store: store, err: fmt.Errorf(
		"%w: persist tenant cust_pair2: connection refused", state.ErrPersistence)}
	if _, err := tn.Provision(ProvisionOpts{ID: "cust_pair2", OwnerSubject: "bob"}); !errors.Is(err, state.ErrPersistence) {
		t.Fatalf("Provision error = %v, want ErrPersistence", err)
	}
	acct, err := store.AccountByIdentity("https://id.yscale.sh", "bob")
	if err != nil {
		t.Fatalf("owner account: %v", err)
	}
	if got := store.MembershipsForAccount(acct.ID); len(got) != 0 {
		t.Errorf("grants after a refused create = %+v, want none", got)
	}
	if _, err := store.CustomerByID("cust_pair2"); err == nil {
		t.Error("a tenant survived a refused create transaction")
	}
}

// offboardHTTP drives DELETE /v1/admin/tenants/{id} with an optional raw query
// string and decodes the report.
func offboardHTTP(t *testing.T, tn *Tenants, id, query string) (int, OffboardReport) {
	t.Helper()
	target := "/v1/admin/tenants/" + id
	if query != "" {
		target += "?" + query
	}
	req := httptest.NewRequest(http.MethodDelete, target, nil)
	req.SetPathValue("id", id)
	rec := httptest.NewRecorder()
	tn.HandleOffboard(rec, req)
	var rep OffboardReport
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
			t.Fatalf("decode offboard: %v", err)
		}
	}
	return rec.Code, rep
}

// stuckTenant provisions a tenant with one burst the reaper will never claim —
// residue that no number of retries clears.
func stuckTenant(t *testing.T, id string) (*state.Store, *Tenants) {
	t.Helper()
	store := state.New()
	tn := newTenants(store)
	if _, err := tn.Provision(ProvisionOpts{ID: id}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	store.PutBurst(&state.Burst{ID: "b_stuck", CustomerID: id})
	tn.Reap = func(context.Context, string, string) bool { return false }
	return store, tn
}

// Residue that will never reap — a burst whose provider account is gone, a
// record for a VM somebody deleted by hand — leaves a TOMBSTONE: the tenant
// stays revoked and keeps its id, forever if need be. That is the deliberate
// limit of this API. Freeing the id would hand the next tenant to hold it a
// burst record, a fabric and an orphan sweep scope that are not theirs, so the
// unusable id is the cheaper of the two failures.
func TestOffboardLeavesUnreapableResidueAsARevokedTombstone(t *testing.T) {
	store, tn := stuckTenant(t, "cust_stuck")

	code, rep := offboardHTTP(t, tn, "cust_stuck", "")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if !rep.CustomerRevoked || !rep.CustomerDeleted {
		t.Fatalf("report = %+v, want access cut and the tenant reported finished", rep)
	}
	// customer_record_deleted is the field that names a physical row removal,
	// and no row was removed.
	if rep.CustomerRecordDeleted || rep.Clean {
		t.Fatalf("report = %+v, want no row deletion claimed and clean:false", rep)
	}
	if len(rep.Warnings) != 1 || !strings.Contains(rep.Warnings[0], "retry offboard") {
		t.Fatalf("warnings = %+v, want one telling the operator to retry", rep.Warnings)
	}
	c, err := store.CustomerByID("cust_stuck")
	if err != nil {
		t.Fatalf("the record an operator retries against is gone: %v", err)
	}
	if !c.Revoked() {
		t.Error("the tenant is not stamped revoked")
	}
	if n := len(store.BurstsForCustomer("cust_stuck")); n != 1 {
		t.Errorf("tracked bursts = %d, want the residue kept and still owned by the tombstone", n)
	}
	// The id is held, not freed: nothing can be provisioned onto a tenant whose
	// resources may still exist.
	if _, err := tn.Provision(ProvisionOpts{ID: "cust_stuck"}); err == nil {
		t.Fatal("a tombstoned id was re-provisioned while its residue is still tracked")
	}
	// And the retry is the same answer, not an escalation.
	code, again := offboardHTTP(t, tn, "cust_stuck", "")
	if code != http.StatusOK || again.CustomerRecordDeleted || again.Clean {
		t.Fatalf("retry: status %d, report %+v — want the same revoked, unclean answer", code, again)
	}
}

// There is no force-delete, by design, and the parameter that used to ask for
// one is now a 400 with NO side effects — on the PRESENCE of the key, whatever
// value rides with it. An earlier revision took `?force=abandon`, which dropped
// a stuck tenant's burst records to free its id; running an ordinary offboard
// for that request instead would leave the caller believing the tenant was
// force-removed while its residue is still tracked.
//
// The empty and duplicated forms are in the table because they are the ones a
// caller sends by accident — `?force=$FORCE` from an unset shell variable, a
// script that appends the flag twice — and they mean the same thing as the rest:
// somebody is still driving this endpoint as though it had force semantics.
// Reading only the first non-empty value would run an ordinary offboard for
// them, which is exactly the silent misread this rejection exists to prevent.
func TestOffboardRejectsAnyForceParameter(t *testing.T) {
	for _, query := range []string{
		"force=abandon", "force=true", "force=1&abandon=yes",
		"force=", "force=&force=abandon", "force=1&force=",
	} {
		t.Run(query, func(t *testing.T) {
			store, tn := stuckTenant(t, "cust_noforce")
			code, _ := offboardHTTP(t, tn, "cust_noforce", query)
			if code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", code)
			}
			// No side effects at all: the rejection happens before the revoke,
			// so the tenant is exactly what it was.
			c, err := store.CustomerByID("cust_noforce")
			if err != nil {
				t.Fatalf("the tenant is gone after a rejected request: %v", err)
			}
			if c.Revoked() {
				t.Error("a rejected request revoked the tenant")
			}
			if _, err := store.AuthCustomer(c.Token); err != nil {
				t.Errorf("a rejected request cut the tenant's access: %v", err)
			}
			if n := len(store.BurstsForCustomer("cust_noforce")); n != 1 {
				t.Errorf("tracked bursts = %d, want the tenant's burst untouched", n)
			}
		})
	}
}

func TestOffboardRejectsMalformedQueryBeforeSideEffects(t *testing.T) {
	for _, rawQuery := range []string{"force=abandon;x=1", "force=%zz"} {
		t.Run(rawQuery, func(t *testing.T) {
			store, tn := stuckTenant(t, "cust_badquery")
			req := httptest.NewRequest(http.MethodDelete, "/v1/admin/tenants/cust_badquery", nil)
			req.URL.RawQuery = rawQuery
			req.SetPathValue("id", "cust_badquery")
			rec := httptest.NewRecorder()
			tn.HandleOffboard(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			c, err := store.CustomerByID("cust_badquery")
			if err != nil || c.Revoked() {
				t.Fatalf("malformed query changed tenant: customer=%+v err=%v", c, err)
			}
			if len(store.BurstsForCustomer("cust_badquery")) != 1 {
				t.Fatal("malformed query started burst cleanup")
			}
		})
	}
}

// A request with no force key at all is the ordinary offboard — the rejection
// keys off the parameter's presence, so it must not fire on anything else in the
// query string.
func TestOffboardRunsWithoutAForceParameter(t *testing.T) {
	_, tn := stuckTenant(t, "cust_noforcekey")
	code, rep := offboardHTTP(t, tn, "cust_noforcekey", "enforce=1&reason=pilot%20over")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if !rep.CustomerRevoked || rep.CustomerRecordDeleted {
		t.Fatalf("report = %+v, want the ordinary revoked, record-kept answer", rep)
	}
}

const testAdminToken = "adm_secret_operator_token"

// grantMux wires the three operator member routes behind AdminAuth with the same
// token, so these tests exercise the real credential rather than handlers with
// their guard removed.
func grantMux(tn *Tenants, adminToken string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/admin/tenants/{tenant_id}/members",
		AdminAuth(adminToken, http.HandlerFunc(tn.HandleGrantMember)))
	mux.Handle("PATCH /v1/admin/tenants/{tenant_id}/members/{account_id}",
		AdminAuth(adminToken, http.HandlerFunc(tn.HandleUpdateMemberRole)))
	mux.Handle("DELETE /v1/admin/tenants/{tenant_id}/members/{account_id}",
		AdminAuth(adminToken, http.HandlerFunc(tn.HandleRevokeMember)))
	return mux
}

// callGrantMux drives the operator member mux with the caller's credential.
// The mux itself always holds the real admin token: adminToken is what the
// CALLER presents, which is what makes a near-miss token a real test.
func callGrantMux(tn *Tenants, method, path, adminToken, body string) *httptest.ResponseRecorder {
	var req *http.Request
	if body == "" && method != http.MethodPost {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if adminToken != "" {
		req.Header.Set("Authorization", "Bearer "+adminToken)
	}
	rec := httptest.NewRecorder()
	grantMux(tn, testAdminToken).ServeHTTP(rec, req)
	return rec
}

func grantMember(tn *Tenants, adminToken, tenantID, body string) *httptest.ResponseRecorder {
	return callGrantMux(tn, http.MethodPost, "/v1/admin/tenants/"+tenantID+"/members", adminToken, body)
}

func updateMember(tn *Tenants, adminToken, tenantID, accountID, body string) *httptest.ResponseRecorder {
	return callGrantMux(tn, http.MethodPatch,
		"/v1/admin/tenants/"+tenantID+"/members/"+accountID, adminToken, body)
}

func revokeMember(tn *Tenants, adminToken, tenantID, accountID string) *httptest.ResponseRecorder {
	return callGrantMux(tn, http.MethodDelete,
		"/v1/admin/tenants/"+tenantID+"/members/"+accountID, adminToken, "")
}

// grantFixture is a tenant, a human account with no grant on it, and the admin
// handler that can bind them.
func grantFixture(t *testing.T, customerID, subject string) (*state.Store, *Tenants, *state.Account) {
	t.Helper()
	store := state.New()
	store.AddCustomer(&state.Customer{ID: customerID, Token: "ysk_" + customerID, Plan: "pro"})
	acct, err := store.UpsertAccount(testIssuer, subject, state.AccountProfile{
		Email: subject + "@acme.com", EmailVerified: true, Name: strings.ToUpper(subject),
	})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	return store, newTenants(store), acct
}

// The grant route is an operator route and nothing else opens it: not the
// human access token the roster routes take, not a tenant's cluster token, and
// not a near-miss admin token. With no YSCALE_ADMIN_TOKEN it does not exist.
func TestGrantMemberIsOperatorOnly(t *testing.T) {
	store, tn, acct := grantFixture(t, "cust_grant", "newbie")
	body := fmt.Sprintf(`{"account_id":%q,"role":"member"}`, acct.ID)

	for _, tc := range []struct {
		name, token string
		want        int
	}{
		{"no credential", "", http.StatusUnauthorized},
		{"a human access token", "human_newbie", http.StatusUnauthorized},
		{"a tenant's cluster token", "ysk_cust_grant", http.StatusUnauthorized},
		{"a near-miss admin token", testAdminToken + "x", http.StatusUnauthorized},
		{"the operator token", testAdminToken, http.StatusCreated},
	} {
		if code := grantMember(tn, tc.token, "cust_grant", body).Code; code != tc.want {
			t.Errorf("%s: status = %d, want %d", tc.name, code, tc.want)
		}
	}
	// Only the operator's call wrote anything.
	if _, err := store.MembershipFor(acct.ID, "cust_grant"); err != nil {
		t.Fatalf("the operator's grant did not land: %v", err)
	}

	// An unconfigured admin token disables the route, exactly as it does for
	// provisioning and offboarding.
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/tenants/cust_grant/members", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	rec := httptest.NewRecorder()
	grantMux(tn, "").ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("grant with no admin token configured = %d, want 404", rec.Code)
	}
}

// The status contract: a new grant is 201, the same grant again is 200 and
// writes nothing, a different role is 409 rather than a silent demotion, bad
// input is 400, and anything the request named that is not there is 404.
func TestGrantMemberStatuses(t *testing.T) {
	store, tn, acct := grantFixture(t, "cust_grant", "newbie")
	store.AddCustomer(&state.Customer{ID: "cust_dead", Token: "ysk_cust_dead", Plan: "pro"})
	if err := store.RevokeCustomer("cust_dead"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	body := fmt.Sprintf(`{"account_id":%q,"role":"admin"}`, acct.ID)

	created := grantMember(tn, testAdminToken, "cust_grant", body)
	if created.Code != http.StatusCreated {
		t.Fatalf("new grant = %d, want 201 (body %q)", created.Code, created.Body.String())
	}
	var res GrantMemberResult
	if err := json.Unmarshal(created.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode grant result: %v (body %q)", err, created.Body.String())
	}
	if res.CustomerID != "cust_grant" || res.AccountID != acct.ID || res.Role != state.RoleAdmin {
		t.Fatalf("grant result = %+v", res)
	}
	if res.CreatedAt.IsZero() {
		t.Error("grant result has no grant timestamp")
	}

	repeat := grantMember(tn, testAdminToken, "cust_grant", body)
	if repeat.Code != http.StatusOK {
		t.Fatalf("identical re-grant = %d, want 200 (body %q)", repeat.Code, repeat.Body.String())
	}
	conflict := grantMember(tn, testAdminToken, "cust_grant",
		fmt.Sprintf(`{"account_id":%q,"role":"owner"}`, acct.ID))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflicting re-grant = %d, want 409 (body %q)", conflict.Code, conflict.Body.String())
	}
	if m, err := store.MembershipFor(acct.ID, "cust_grant"); err != nil || m.Role != state.RoleAdmin {
		t.Fatalf("a 409 changed the stored role: %+v (%v)", m, err)
	}

	for _, tc := range []struct {
		name, tenant, body string
		want               int
	}{
		{"no body at all", "cust_grant", "", http.StatusBadRequest},
		{"not JSON", "cust_grant", "account_id=acct_x", http.StatusBadRequest},
		{"no account_id", "cust_grant", `{"role":"member"}`, http.StatusBadRequest},
		{"no role", "cust_grant", fmt.Sprintf(`{"account_id":%q}`, acct.ID), http.StatusBadRequest},
		{"a role outside the set", "cust_grant", fmt.Sprintf(`{"account_id":%q,"role":"superuser"}`, acct.ID), http.StatusBadRequest},
		{"unknown tenant", "cust_nope", body, http.StatusNotFound},
		{"revoked tenant", "cust_dead", body, http.StatusNotFound},
		{"unknown account", "cust_grant", `{"account_id":"acct_nobody","role":"member"}`, http.StatusNotFound},
	} {
		rec := grantMember(tn, testAdminToken, tc.tenant, tc.body)
		if rec.Code != tc.want {
			t.Errorf("%s: status = %d, want %d (body %q)", tc.name, rec.Code, tc.want, rec.Body.String())
		}
	}
	// Nothing a refused call named was written.
	if roster := store.MembershipsForTenant("cust_grant"); len(roster) != 1 {
		t.Fatalf("roster after the refused grants = %+v, want just the one admin", roster)
	}
	if len(store.MembershipsForTenant("cust_dead")) != 0 {
		t.Fatal("a grant landed on a revoked tenant")
	}
}

// grantFailStore refuses the durable membership write. The store rolls the
// grant back (proved in the state package); this proves the route answers 503
// rather than reporting access it did not create.
type grantFailStore struct {
	*state.Store
	err error
}

func (f *grantFailStore) GrantTenantMembership(string, string, string, state.Actor) (*state.TenantMembership, bool, error) {
	return nil, false, f.err
}

func TestGrantMemberReturnsUnavailableWhenTheWriteIsRefused(t *testing.T) {
	store, tn, acct := grantFixture(t, "cust_grant", "newbie")
	var logs bytes.Buffer
	tn.Log = slog.New(slog.NewTextHandler(&logs, nil))
	tn.Store = &grantFailStore{
		Store: store,
		err:   fmt.Errorf("%w: persist membership mbr_x: connection refused", state.ErrPersistence),
	}

	rec := grantMember(tn, testAdminToken, "cust_grant",
		fmt.Sprintf(`{"account_id":%q,"role":"member"}`, acct.ID))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "connection refused") {
		t.Errorf("the backend's error reached the caller: %s", rec.Body.String())
	}
	if !strings.Contains(logs.String(), "grant tenant membership") {
		t.Errorf("a refused durable write was not logged:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "tenant member granted") {
		t.Errorf("a refused grant was audited as one:\n%s", logs.String())
	}
	if _, err := store.MembershipFor(acct.ID, "cust_grant"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("a refused grant left access behind: %v", err)
	}
}

// A new grant is audited with the tenant, the account and the role it created.
// A re-grant that created nothing is not, and neither carries a credential.
func TestGrantMemberAuditsOnlyANewGrant(t *testing.T) {
	_, tn, acct := grantFixture(t, "cust_grant", "newbie")
	var logs bytes.Buffer
	tn.Log = slog.New(slog.NewTextHandler(&logs, nil))
	body := fmt.Sprintf(`{"account_id":%q,"role":"member"}`, acct.ID)

	if code := grantMember(tn, testAdminToken, "cust_grant", body).Code; code != http.StatusCreated {
		t.Fatalf("new grant = %d, want 201", code)
	}
	audit := logs.String()
	if !strings.Contains(audit, "tenant member granted") {
		t.Fatalf("a new grant was not audited:\n%s", audit)
	}
	for _, want := range []string{"tenant=cust_grant", "account=" + acct.ID, "role=member"} {
		if !strings.Contains(audit, want) {
			t.Errorf("the grant audit is missing %q:\n%s", want, audit)
		}
	}
	for _, forbidden := range []string{testAdminToken, "ysk_cust_grant", testIssuer, "newbie", "Bearer"} {
		if strings.Contains(audit, forbidden) {
			t.Errorf("the grant audit leaked %q:\n%s", forbidden, audit)
		}
	}

	logs.Reset()
	if code := grantMember(tn, testAdminToken, "cust_grant", body).Code; code != http.StatusOK {
		t.Fatalf("re-grant = %d, want 200", code)
	}
	if strings.Contains(logs.String(), "tenant member granted") {
		t.Errorf("an idempotent re-grant was audited as a new one:\n%s", logs.String())
	}
}

// The response says what was granted and nothing about who the human is to the
// identity provider, nor anything about the tenant's own credentials.
func TestGrantMemberResponseCarriesNoSecrets(t *testing.T) {
	store, tn, acct := grantFixture(t, "cust_grant", "newbie")
	store.AddCustomer(&state.Customer{ID: "cust_grant2", Token: "ysk_secret_cluster_token", Plan: "pro"})

	rec := grantMember(tn, testAdminToken, "cust_grant2",
		fmt.Sprintf(`{"account_id":%q,"role":"viewer"}`, acct.ID))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}
	for _, forbidden := range []string{
		"ysk_secret_cluster_token", testIssuer, testAdminToken, "newbie", `"subject"`, `"issuer"`, `"token"`,
	} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Errorf("the grant response leaked %q: %s", forbidden, rec.Body.String())
		}
	}
}

// The point of the route: a human an operator grants a role can immediately use
// the human surfaces — the tenant shows up on their account, they appear on its
// roster, and an owner can remove them again. Without this route only a
// tenant's first owner could ever hold a membership.
func TestGrantedMemberReachesTheHumanSurfaces(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_grant", Token: "ysk_grant", Plan: "pro",
		MaxConcurrentBursts: 5, MaxHourlyUSD: 10})
	accounts, _ := rosterFixture(t, store, "cust_grant", map[string]string{"alice": state.RoleOwner})
	newcomer, err := store.UpsertAccount(testIssuer, "newbie", state.AccountProfile{
		Email: "newbie@acme.com", EmailVerified: true, Name: "NEWBIE",
	})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	accounts.Resolver = idProvider(t, map[string]string{"human_alice": "alice", "human_newbie": "newbie"})
	tn := newTenants(store)

	// Before the grant they are a stranger to the tenant.
	if code := listMembers(accounts, "human_newbie", "cust_grant").Code; code != http.StatusNotFound {
		t.Fatalf("ungranted human listing the roster = %d, want 404", code)
	}
	if code := grantMember(tn, testAdminToken, "cust_grant",
		fmt.Sprintf(`{"account_id":%q,"role":"admin"}`, newcomer.ID)).Code; code != http.StatusCreated {
		t.Fatalf("grant = %d, want 201", code)
	}

	// After it they hold the tenant and manage its roster.
	acct := decodeAccount(t, getAccount(accounts, "human_newbie"))
	if len(acct.Tenants) != 1 || acct.Tenants[0].CustomerID != "cust_grant" ||
		acct.Tenants[0].Role != state.RoleAdmin {
		t.Fatalf("granted human's account = %+v, want cust_grant as admin", acct.Tenants)
	}
	roster := decodeMembers(t, listMembers(accounts, "human_newbie", "cust_grant"))
	if len(roster.Members) != 2 {
		t.Fatalf("roster = %+v, want the owner and the granted admin", roster.Members)
	}
	// And the grant is removable through the human surface it was made for.
	if code := removeMember(accounts, "human_alice", "cust_grant", newcomer.ID).Code; code != http.StatusNoContent {
		t.Fatalf("removing the granted member = %d, want 204", code)
	}
	if code := listMembers(accounts, "human_newbie", "cust_grant").Code; code != http.StatusNotFound {
		t.Errorf("removed human listing the roster = %d, want 404", code)
	}
}

// operatorMemberFixture is a live tenant with an owner and a member already
// granted, plus the operator handler that corrects them. Returns the account
// ids by subject, so a test names a human the way the routes do.
func operatorMemberFixture(t *testing.T, customerID string) (*state.Store, *Tenants, map[string]string) {
	t.Helper()
	store := state.New()
	store.AddCustomer(&state.Customer{ID: customerID, Token: "ysk_" + customerID, Plan: "pro",
		MaxConcurrentBursts: 5, MaxHourlyUSD: 10})
	ids := make(map[string]string, 2)
	for sub, role := range map[string]string{"alice": state.RoleOwner, "mem": state.RoleMember} {
		acct, err := store.UpsertAccount(testIssuer, sub, state.AccountProfile{
			Email: sub + "@acme.com", EmailVerified: true, Name: strings.ToUpper(sub),
		})
		if err != nil {
			t.Fatalf("UpsertAccount %s: %v", sub, err)
		}
		if _, err := store.AddTenantMembership(acct.ID, customerID, role); err != nil {
			t.Fatalf("AddTenantMembership %s: %v", sub, err)
		}
		ids[sub] = acct.ID
	}
	return store, newTenants(store), ids
}

// The correction routes are operator routes on the same terms as the grant: a
// human access token, a tenant's own cluster token and a near-miss admin token
// are all refused, and with no YSCALE_ADMIN_TOKEN the routes do not exist.
func TestOperatorMemberCorrectionIsOperatorOnly(t *testing.T) {
	store, tn, ids := operatorMemberFixture(t, "cust_fix")

	for _, tc := range []struct {
		name, token string
		want        int
	}{
		{"no credential", "", http.StatusUnauthorized},
		{"a human access token", "human_alice", http.StatusUnauthorized},
		{"a tenant's cluster token", "ysk_cust_fix", http.StatusUnauthorized},
		{"a near-miss admin token", testAdminToken + "x", http.StatusUnauthorized},
		{"the operator token", testAdminToken, http.StatusOK},
	} {
		if code := updateMember(tn, tc.token, "cust_fix", ids["mem"], `{"role":"admin"}`).Code; code != tc.want {
			t.Errorf("PATCH with %s = %d, want %d", tc.name, code, tc.want)
		}
	}
	// Only the operator's call changed anything.
	if m, err := store.MembershipFor(ids["mem"], "cust_fix"); err != nil || m.Role != state.RoleAdmin {
		t.Fatalf("membership after the refused calls = %+v (%v), want the operator's admin", m, err)
	}

	for _, tc := range []struct {
		name, token string
		want        int
	}{
		{"no credential", "", http.StatusUnauthorized},
		{"a human access token", "human_alice", http.StatusUnauthorized},
		{"a tenant's cluster token", "ysk_cust_fix", http.StatusUnauthorized},
		{"a near-miss admin token", testAdminToken + "x", http.StatusUnauthorized},
		{"the operator token", testAdminToken, http.StatusNoContent},
	} {
		if code := revokeMember(tn, tc.token, "cust_fix", ids["mem"]).Code; code != tc.want {
			t.Errorf("DELETE with %s = %d, want %d", tc.name, code, tc.want)
		}
	}
	if _, err := store.MembershipFor(ids["mem"], "cust_fix"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("the refused calls left the grant in an unexpected state: %v", err)
	}

	// An unconfigured admin token disables both, exactly as it does the grant.
	unconfigured := http.NewServeMux()
	unconfigured.Handle("PATCH /v1/admin/tenants/{tenant_id}/members/{account_id}",
		AdminAuth("", http.HandlerFunc(tn.HandleUpdateMemberRole)))
	unconfigured.Handle("DELETE /v1/admin/tenants/{tenant_id}/members/{account_id}",
		AdminAuth("", http.HandlerFunc(tn.HandleRevokeMember)))
	for _, method := range []string{http.MethodPatch, http.MethodDelete} {
		req := httptest.NewRequest(method, "/v1/admin/tenants/cust_fix/members/"+ids["alice"],
			strings.NewReader(`{"role":"admin"}`))
		req.Header.Set("Authorization", "Bearer "+testAdminToken)
		rec := httptest.NewRecorder()
		unconfigured.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s with no admin token configured = %d, want 404", method, rec.Code)
		}
	}
}

// The PATCH contract: a real change is 200 and durable, the same role again is
// an idempotent 200, bad input is 400, everything the request named that is not
// there is 404, and demoting the only owner is 409 that changes nothing.
func TestUpdateMemberRoleStatuses(t *testing.T) {
	store, tn, ids := operatorMemberFixture(t, "cust_fix")
	store.AddCustomer(&state.Customer{ID: "cust_dead", Token: "ysk_cust_dead", Plan: "pro"})
	if err := store.RevokeCustomer("cust_dead"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	stranger, err := store.UpsertAccount(testIssuer, "stranger", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	changed := updateMember(tn, testAdminToken, "cust_fix", ids["mem"], `{"role":"admin"}`)
	if changed.Code != http.StatusOK {
		t.Fatalf("role change = %d, want 200 (body %q)", changed.Code, changed.Body.String())
	}
	var res GrantMemberResult
	if err := json.Unmarshal(changed.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode role change result: %v (body %q)", err, changed.Body.String())
	}
	if res.CustomerID != "cust_fix" || res.AccountID != ids["mem"] || res.Role != state.RoleAdmin {
		t.Fatalf("role change result = %+v", res)
	}
	if res.CreatedAt.IsZero() {
		t.Error("the role change result lost the grant time")
	}
	if m, err := store.MembershipFor(ids["mem"], "cust_fix"); err != nil || m.Role != state.RoleAdmin {
		t.Fatalf("the store still holds %+v (%v), want admin", m, err)
	}

	repeat := updateMember(tn, testAdminToken, "cust_fix", ids["mem"], `{"role":"admin"}`)
	if repeat.Code != http.StatusOK {
		t.Fatalf("setting the same role = %d, want an idempotent 200 (body %q)", repeat.Code, repeat.Body.String())
	}
	if repeat.Body.String() != changed.Body.String() {
		t.Errorf("the idempotent 200 differs from the change: %q vs %q", repeat.Body.String(), changed.Body.String())
	}

	for _, tc := range []struct {
		name, tenant, account, body string
		want                        int
	}{
		{"not JSON", "cust_fix", ids["mem"], "role=admin", http.StatusBadRequest},
		{"no role", "cust_fix", ids["mem"], `{}`, http.StatusBadRequest},
		{"a role outside the set", "cust_fix", ids["mem"], `{"role":"superuser"}`, http.StatusBadRequest},
		{"unknown tenant", "cust_nope", ids["mem"], `{"role":"viewer"}`, http.StatusNotFound},
		{"revoked tenant", "cust_dead", ids["mem"], `{"role":"viewer"}`, http.StatusNotFound},
		{"an account with no grant", "cust_fix", stranger.ID, `{"role":"viewer"}`, http.StatusNotFound},
		{"an account that does not exist", "cust_fix", "acct_nobody", `{"role":"viewer"}`, http.StatusNotFound},
		{"demoting the only owner", "cust_fix", ids["alice"], `{"role":"admin"}`, http.StatusConflict},
	} {
		rec := updateMember(tn, testAdminToken, tc.tenant, tc.account, tc.body)
		if rec.Code != tc.want {
			t.Errorf("%s: status = %d, want %d (body %q)", tc.name, rec.Code, tc.want, rec.Body.String())
		}
	}
	// Nothing a refused call named moved.
	if m, err := store.MembershipFor(ids["alice"], "cust_fix"); err != nil || m.Role != state.RoleOwner {
		t.Fatalf("the only owner was demoted anyway: %+v (%v)", m, err)
	}
	if m, err := store.MembershipFor(ids["mem"], "cust_fix"); err != nil || m.Role != state.RoleAdmin {
		t.Fatalf("a refused change landed: %+v (%v)", m, err)
	}
	if len(store.MembershipsForTenant("cust_dead")) != 0 {
		t.Fatal("a role change reached a revoked tenant")
	}

	// With a second owner in place the same demotion is allowed — the 409 is
	// the owner floor, not a ban on demoting owners.
	if code := updateMember(tn, testAdminToken, "cust_fix", ids["mem"], `{"role":"owner"}`).Code; code != http.StatusOK {
		t.Fatalf("promoting a second owner = %d, want 200", code)
	}
	if code := updateMember(tn, testAdminToken, "cust_fix", ids["alice"], `{"role":"admin"}`).Code; code != http.StatusOK {
		t.Fatalf("demoting one of two owners = %d, want 200", code)
	}
}

// The DELETE contract: a real removal is 204, an absent member on a LIVE tenant
// is an idempotent 204, a tenant that is not there is 404 rather than a
// success, and the only owner is 409 that changes nothing.
func TestRevokeMemberStatuses(t *testing.T) {
	store, tn, ids := operatorMemberFixture(t, "cust_fix")
	store.AddCustomer(&state.Customer{ID: "cust_dead", Token: "ysk_cust_dead", Plan: "pro"})
	if err := store.RevokeCustomer("cust_dead"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}

	if code := revokeMember(tn, testAdminToken, "cust_fix", ids["mem"]).Code; code != http.StatusNoContent {
		t.Fatalf("operator removal = %d, want 204", code)
	}
	if _, err := store.MembershipFor(ids["mem"], "cust_fix"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("the grant survived the removal: %v", err)
	}

	for _, tc := range []struct {
		name, tenant, account string
		want                  int
	}{
		{"the same removal again", "cust_fix", ids["mem"], http.StatusNoContent},
		{"a human who never had a grant", "cust_fix", "acct_nobody", http.StatusNoContent},
		{"unknown tenant", "cust_nope", ids["mem"], http.StatusNotFound},
		{"revoked tenant", "cust_dead", ids["mem"], http.StatusNotFound},
		{"the only owner", "cust_fix", ids["alice"], http.StatusConflict},
	} {
		rec := revokeMember(tn, testAdminToken, tc.tenant, tc.account)
		if rec.Code != tc.want {
			t.Errorf("%s: status = %d, want %d (body %q)", tc.name, rec.Code, tc.want, rec.Body.String())
		}
	}
	if m, err := store.MembershipFor(ids["alice"], "cust_fix"); err != nil || m.Role != state.RoleOwner {
		t.Fatalf("the only owner was removed anyway: %+v (%v)", m, err)
	}

	// A replacement owner is the operator's way out of the 409.
	if code := grantMember(tn, testAdminToken, "cust_fix",
		fmt.Sprintf(`{"account_id":%q,"role":"owner"}`, ids["mem"])).Code; code != http.StatusCreated {
		t.Fatalf("granting a replacement owner = %d, want 201", code)
	}
	if code := revokeMember(tn, testAdminToken, "cust_fix", ids["alice"]).Code; code != http.StatusNoContent {
		t.Fatalf("removing one of two owners = %d, want 204", code)
	}
	roster := store.MembershipsForTenant("cust_fix")
	if len(roster) != 1 || roster[0].AccountID != ids["mem"] || roster[0].Role != state.RoleOwner {
		t.Fatalf("roster = %+v, want exactly the replacement owner", roster)
	}
}

// memberFailStore refuses the durable writes behind the correction routes. The
// store rolls each back (proved in the state package); this proves the routes
// answer 503 rather than reporting a change they did not make.
type memberFailStore struct {
	*state.Store
	err error
}

func (f *memberFailStore) SetTenantMembershipRole(string, string, string, state.Actor) (*state.TenantMembership, string, error) {
	return nil, "", f.err
}

func (f *memberFailStore) DeleteTenantMembership(string, string, state.Actor) (*state.TenantMembership, error) {
	return nil, f.err
}

func TestOperatorMemberCorrectionsAreRetryableWhenTheWriteIsRefused(t *testing.T) {
	store, tn, ids := operatorMemberFixture(t, "cust_fix")
	var logs bytes.Buffer
	tn.Log = slog.New(slog.NewTextHandler(&logs, nil))
	tn.Store = &memberFailStore{
		Store: store,
		err:   fmt.Errorf("%w: persist membership mbr_x: connection refused", state.ErrPersistence),
	}

	for _, tc := range []struct {
		name, wantLog string
		call          func() *httptest.ResponseRecorder
	}{
		{"role change", "set tenant member role", func() *httptest.ResponseRecorder {
			return updateMember(tn, testAdminToken, "cust_fix", ids["mem"], `{"role":"admin"}`)
		}},
		{"removal", "delete tenant membership", func() *httptest.ResponseRecorder {
			return revokeMember(tn, testAdminToken, "cust_fix", ids["mem"])
		}},
	} {
		logs.Reset()
		rec := tc.call()
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: status = %d, want 503 (body %q)", tc.name, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "connection refused") ||
			strings.Contains(rec.Body.String(), "mbr_x") {
			t.Errorf("%s: the backend's error reached the caller: %s", tc.name, rec.Body.String())
		}
		if !strings.Contains(logs.String(), tc.wantLog) ||
			!strings.Contains(logs.String(), "connection refused") {
			t.Errorf("%s: the refused durable write was not logged in full:\n%s", tc.name, logs.String())
		}
		for _, claimed := range []string{"tenant member role changed", "tenant member revoked"} {
			if strings.Contains(logs.String(), claimed) {
				t.Errorf("%s: a refused write was audited as %q:\n%s", tc.name, claimed, logs.String())
			}
		}
	}
	// The membership is exactly as it was: a 503 is a retry, not a maybe.
	if m, err := store.MembershipFor(ids["mem"], "cust_fix"); err != nil || m.Role != state.RoleMember {
		t.Fatalf("a refused correction changed the grant: %+v (%v)", m, err)
	}
}

// Every refusal on the three operator member routes answers a FIXED body per
// status. The store's text names membership ids and stored roles; echoing it
// made each refusal a different string and put internal identifiers in the
// caller's error path.
func TestOperatorMemberRefusalsHaveStableBodies(t *testing.T) {
	store, tn, ids := operatorMemberFixture(t, "cust_fix")
	stranger, err := store.UpsertAccount(testIssuer, "stranger", state.AccountProfile{})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	errorBody := func(rec *httptest.ResponseRecorder) string {
		t.Helper()
		var payload map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode error body: %v (body %q)", err, rec.Body.String())
		}
		return payload["error"]
	}

	// The same 404 whatever was missing — tenant, account or grant — on all
	// three routes. An operator route has no enumeration oracle to protect;
	// what it must not do is describe the store's insides differently each time.
	notFound := []string{
		errorBody(grantMember(tn, testAdminToken, "cust_nope",
			fmt.Sprintf(`{"account_id":%q,"role":"member"}`, ids["mem"]))),
		errorBody(grantMember(tn, testAdminToken, "cust_fix", `{"account_id":"acct_nobody","role":"member"}`)),
		errorBody(updateMember(tn, testAdminToken, "cust_nope", ids["mem"], `{"role":"viewer"}`)),
		errorBody(updateMember(tn, testAdminToken, "cust_fix", stranger.ID, `{"role":"viewer"}`)),
		errorBody(revokeMember(tn, testAdminToken, "cust_nope", ids["mem"])),
	}
	for i, got := range notFound {
		if got != notFound[0] {
			t.Errorf("404 body %d = %q, want the same string as %q", i, got, notFound[0])
		}
	}

	// The two 409s are distinct on purpose — different problems with different
	// fixes — and each is one fixed string wherever it is raised.
	conflict := errorBody(grantMember(tn, testAdminToken, "cust_fix",
		fmt.Sprintf(`{"account_id":%q,"role":"owner"}`, ids["mem"])))
	lastOwner := errorBody(updateMember(tn, testAdminToken, "cust_fix", ids["alice"], `{"role":"admin"}`))
	if conflict == lastOwner {
		t.Errorf("a role conflict and a last owner say the same thing: %q", conflict)
	}
	if got := errorBody(revokeMember(tn, testAdminToken, "cust_fix", ids["alice"])); got != lastOwner {
		t.Errorf("the last-owner body differs by route: %q vs %q", got, lastOwner)
	}

	// A role outside the closed set reads the same on both routes that take one.
	grantBad := errorBody(grantMember(tn, testAdminToken, "cust_fix",
		fmt.Sprintf(`{"account_id":%q,"role":"superuser"}`, ids["mem"])))
	updateBad := errorBody(updateMember(tn, testAdminToken, "cust_fix", ids["mem"], `{"role":"superuser"}`))
	if updateBad != grantBad {
		t.Errorf("an invalid role reads differently per route: %q vs %q", grantBad, updateBad)
	}

	// Nothing anywhere names the store's own identifiers or echoes the input.
	for _, got := range append(notFound, conflict, lastOwner, grantBad) {
		for _, forbidden := range []string{"mbr_", ids["mem"], ids["alice"], "cust_", "superuser"} {
			if strings.Contains(got, forbidden) {
				t.Errorf("a refusal body leaked %q: %s", forbidden, got)
			}
		}
	}
}

// Real mutations are audited with the tenant, the target and the roles either
// side of the change. Idempotent no-ops are not — an audit trail that records
// retries as revocations and demotions cannot be read as one.
func TestOperatorMemberCorrectionsAuditOnlyRealMutations(t *testing.T) {
	_, tn, ids := operatorMemberFixture(t, "cust_fix")
	var logs bytes.Buffer
	tn.Log = slog.New(slog.NewTextHandler(&logs, nil))

	if code := updateMember(tn, testAdminToken, "cust_fix", ids["mem"], `{"role":"viewer"}`).Code; code != http.StatusOK {
		t.Fatalf("role change = %d, want 200", code)
	}
	audit := logs.String()
	if !strings.Contains(audit, "tenant member role changed") {
		t.Fatalf("a role change was not audited:\n%s", audit)
	}
	for _, want := range []string{"tenant=cust_fix", "account=" + ids["mem"], "old_role=member", "new_role=viewer"} {
		if !strings.Contains(audit, want) {
			t.Errorf("the role-change audit is missing %q:\n%s", want, audit)
		}
	}

	logs.Reset()
	if code := updateMember(tn, testAdminToken, "cust_fix", ids["mem"], `{"role":"viewer"}`).Code; code != http.StatusOK {
		t.Fatalf("setting the same role = %d, want 200", code)
	}
	if strings.Contains(logs.String(), "tenant member role changed") {
		t.Errorf("an idempotent set was audited as a change:\n%s", logs.String())
	}

	logs.Reset()
	if code := revokeMember(tn, testAdminToken, "cust_fix", ids["mem"]).Code; code != http.StatusNoContent {
		t.Fatalf("removal = %d, want 204", code)
	}
	audit = logs.String()
	if !strings.Contains(audit, "tenant member revoked") {
		t.Fatalf("a removal was not audited:\n%s", audit)
	}
	for _, want := range []string{"tenant=cust_fix", "account=" + ids["mem"], "role=viewer"} {
		if !strings.Contains(audit, want) {
			t.Errorf("the removal audit is missing %q:\n%s", want, audit)
		}
	}
	// No audit line carries a credential or the human's identity at the provider.
	for _, forbidden := range []string{testAdminToken, "ysk_cust_fix", testIssuer, "Bearer"} {
		if strings.Contains(audit, forbidden) {
			t.Errorf("the member audit leaked %q:\n%s", forbidden, audit)
		}
	}

	logs.Reset()
	if code := revokeMember(tn, testAdminToken, "cust_fix", ids["mem"]).Code; code != http.StatusNoContent {
		t.Fatalf("removing an absent member = %d, want an idempotent 204", code)
	}
	if strings.Contains(logs.String(), "tenant member revoked") {
		t.Errorf("an idempotent removal was audited as one:\n%s", logs.String())
	}
}

// The operator's recovery path end to end: a grant made with the wrong role is
// corrected rather than re-granted (which is 409), and a human granted by
// mistake is removed by the operator without any member of the tenant having to
// do it. This is the loop the grant route left open.
func TestOperatorFixesAMisgrantedRoleWithoutTheTenant(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_fix", Token: "ysk_fix", Plan: "pro",
		MaxConcurrentBursts: 5, MaxHourlyUSD: 10})
	accounts, _ := rosterFixture(t, store, "cust_fix", map[string]string{"alice": state.RoleOwner})
	wrong, err := store.UpsertAccount(testIssuer, "wrong", state.AccountProfile{
		Email: "wrong@acme.com", EmailVerified: true, Name: "WRONG",
	})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	accounts.Resolver = idProvider(t, map[string]string{"human_alice": "alice", "human_wrong": "wrong"})
	tn := newTenants(store)

	// The mistake: granted owner instead of viewer.
	if code := grantMember(tn, testAdminToken, "cust_fix",
		fmt.Sprintf(`{"account_id":%q,"role":"owner"}`, wrong.ID)).Code; code != http.StatusCreated {
		t.Fatalf("grant = %d, want 201", code)
	}
	// A re-grant will not fix it, which is why the correction route exists.
	if code := grantMember(tn, testAdminToken, "cust_fix",
		fmt.Sprintf(`{"account_id":%q,"role":"viewer"}`, wrong.ID)).Code; code != http.StatusConflict {
		t.Fatalf("re-grant with the right role = %d, want 409", code)
	}
	if code := updateMember(tn, testAdminToken, "cust_fix", wrong.ID, `{"role":"viewer"}`).Code; code != http.StatusOK {
		t.Fatalf("correction = %d, want 200", code)
	}

	// The human surfaces agree immediately: they are a viewer, so the roster is
	// no longer theirs to manage.
	acct := decodeAccount(t, getAccount(accounts, "human_wrong"))
	if len(acct.Tenants) != 1 || acct.Tenants[0].Role != state.RoleViewer {
		t.Fatalf("corrected human's account = %+v, want cust_fix as viewer", acct.Tenants)
	}
	if code := listMembers(accounts, "human_wrong", "cust_fix").Code; code != http.StatusForbidden {
		t.Fatalf("a viewer listing the roster = %d, want 403", code)
	}
	roster := decodeMembers(t, listMembers(accounts, "human_alice", "cust_fix"))
	if len(roster.Members) != 2 {
		t.Fatalf("roster = %+v, want the owner and the corrected viewer", roster.Members)
	}

	// And the operator can take the grant away entirely, without the tenant.
	if code := revokeMember(tn, testAdminToken, "cust_fix", wrong.ID).Code; code != http.StatusNoContent {
		t.Fatalf("operator removal = %d, want 204", code)
	}
	if code := listMembers(accounts, "human_wrong", "cust_fix").Code; code != http.StatusNotFound {
		t.Errorf("the removed human still reaches the tenant = %d, want 404", code)
	}
	if code := revokeMember(tn, testAdminToken, "cust_fix", "acct_nobody").Code; code != http.StatusNoContent {
		t.Errorf("removing a stranger from a live tenant = %d, want an idempotent 204", code)
	}
}

// A tenant riding the SHARED tailnet had its gateway CIDRs granted :10250 in a
// policy document it shares with everyone else on it. Offboarding cuts access
// and removes the row, and neither touches that document — so the offboard has
// to ask, at both points where the tenant stops contributing. Nothing else can:
// what went away IS the thing that would otherwise have asked.
func TestOffboardWithdrawsTheSharedTailnetGrant(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	rec := &recordingReconciler{}
	tn.Reconciler = rec
	store.AddCustomer(&state.Customer{ID: "cust_a", Token: "tok_a", Plan: "pro"})

	rep, err := tn.Offboard(context.Background(), "cust_a")
	if err != nil {
		t.Fatalf("Offboard: %v", err)
	}
	if !rep.CustomerRecordDeleted {
		t.Fatalf("report = %+v, want the record removed so both shrinks happened", rep)
	}
	var shared int
	for _, c := range rec.snapshot() {
		if c.shared {
			shared++
			continue
		}
		t.Fatalf("offboard enqueued per-customer policy work for a tenant that is gone: %+v", c)
	}
	if shared != 2 {
		t.Fatalf("shared-tailnet reconciles = %d, want 2 (the revoke and the record going)", shared)
	}
}

// A tenant with its own coordination box contributes nothing to the shared
// document, so its offboard must not ask for a write there. The box is torn down
// through the factory, and a shared reconcile driven by every offboard would
// rewrite an ACL that never named this tenant.
func TestOffboardOnABoxDoesNotTouchTheSharedTailnet(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	rec := &recordingReconciler{}
	tn.Reconciler = rec
	store.AddCustomer(&state.Customer{ID: "cust_hs", Token: "tok_hs", Plan: "pro"})
	if err := store.SetCustomerMesh("cust_hs", &state.MeshEndpoint{
		Provider: "headscale", LoginServer: "https://hs.example", APIKey: "k", User: "u", BackendID: "linode-1",
	}); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}

	if _, err := tn.Offboard(context.Background(), "cust_hs"); err != nil {
		t.Fatalf("Offboard: %v", err)
	}
	for _, c := range rec.snapshot() {
		if c.shared {
			t.Fatal("a dedicated-box tenant's offboard asked for a shared-tailnet write")
		}
	}
}
