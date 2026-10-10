// yscale:proprietary

package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// The write half of the roster and the tenant usage read, driven through the
// same mux the enterprise build wires.

func addMember(a *Accounts, token, tenantID, body string) *httptest.ResponseRecorder {
	return callMuxBody(a, http.MethodPost, "/v1/tenants/"+tenantID+"/members", token, body)
}

func patchMember(a *Accounts, token, tenantID, accountID, body string) *httptest.ResponseRecorder {
	return callMuxBody(a, http.MethodPatch, "/v1/tenants/"+tenantID+"/members/"+accountID, token, body)
}

func getUsage(a *Accounts, token, tenantID string) *httptest.ResponseRecorder {
	return callMux(a, http.MethodGet, "/v1/tenants/"+tenantID+"/usage", token)
}

func decodeMember(t *testing.T, rec *httptest.ResponseRecorder) TenantMemberSummary {
	t.Helper()
	var res TenantMemberSummary
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode member response: %v (body %q)", err, rec.Body.String())
	}
	return res
}

// outsider is a verified human with an account and no membership anywhere —
// the person a manager is about to add.
func outsider(t *testing.T, store *state.Store, subject string) string {
	t.Helper()
	acct, err := store.UpsertAccount(testIssuer, subject, state.AccountProfile{
		Email: subject + "@acme.com", EmailVerified: true, Name: strings.ToUpper(subject),
	})
	if err != nil {
		t.Fatalf("UpsertAccount %s: %v", subject, err)
	}
	return acct.ID
}

// Neither write route exists on a deployment without Yscale ID, and neither
// accepts a cluster credential — the two properties the whole human surface
// rests on, checked here because a new route is exactly where they get missed.
func TestMemberWriteRoutesDisabledWithoutIdentityConfig(t *testing.T) {
	store := state.New()
	disabled := &Accounts{Store: store, Issuer: testIssuer, Log: quietLog()}
	for _, token := range []string{"", "human_alice"} {
		if code := addMember(disabled, token, "cust_x", `{"email":"a@acme.com","role":"member"}`).Code; code != http.StatusNotFound {
			t.Errorf("add on an unconfigured deployment = %d, want 404", code)
		}
		if code := patchMember(disabled, token, "cust_x", "acct_y", `{"role":"admin"}`).Code; code != http.StatusNotFound {
			t.Errorf("role change on an unconfigured deployment = %d, want 404", code)
		}
		if code := getUsage(disabled, token, "cust_x").Code; code != http.StatusNotFound {
			t.Errorf("usage on an unconfigured deployment = %d, want 404", code)
		}
	}

	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})
	if code := addMember(accounts, "ysk_alice", "cust_alice", `{"email":"a@acme.com","role":"member"}`).Code; code != http.StatusUnauthorized {
		t.Errorf("cluster token on the add route = %d, want 401", code)
	}
	if code := patchMember(accounts, "ysk_alice", "cust_alice", ids["alice"], `{"role":"admin"}`).Code; code != http.StatusUnauthorized {
		t.Errorf("cluster token on the role route = %d, want 401", code)
	}
	if code := getUsage(accounts, "ysk_alice", "cust_alice").Code; code != http.StatusUnauthorized {
		t.Errorf("cluster token on the usage route = %d, want 401", code)
	}
	// And a human who has never signed in gets the tenant 404 without an account
	// being minted on the way there: a management route is not a sign-in.
	newcomer := &Accounts{
		Store:    store,
		Resolver: idProvider(t, map[string]string{"human_new": "newbie"}),
		Issuer:   testIssuer,
		Log:      quietLog(),
	}
	if code := addMember(newcomer, "human_new", "cust_alice", `{"email":"a@acme.com","role":"member"}`).Code; code != http.StatusNotFound {
		t.Errorf("unknown human adding a member = %d, want 404", code)
	}
	if _, err := store.AccountByIdentity(testIssuer, "newbie"); err == nil {
		t.Fatal("a management route minted an account for a human who never signed in")
	}
}

// The add contract end to end: who may add whom, what an existing grant
// answers, and what a body has to look like.
func TestAddMemberAuthorityAndStatuses(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "adm": state.RoleAdmin,
		"mem": state.RoleMember, "view": state.RoleViewer,
	})
	newbie := outsider(t, store, "newbie")
	outsider(t, store, "second")

	// A member and a viewer hold a grant, so they are told the truth: the roster
	// is a management surface and their role is not.
	for _, sub := range []string{"mem", "view"} {
		rec := addMember(accounts, "human_"+sub, "cust_alice", `{"email":"newbie@acme.com","role":"member"}`)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s adding a member = %d, want 403 (body %q)", sub, rec.Code, rec.Body.String())
		}
	}
	// A caller with no grant at all cannot tell this tenant from one that does
	// not exist.
	unknown := addMember(accounts, "human_alice", "cust_nope", `{"email":"newbie@acme.com","role":"member"}`)
	if unknown.Code != http.StatusNotFound {
		t.Errorf("adding on an unknown tenant = %d, want 404", unknown.Code)
	}

	created := addMember(accounts, "human_alice", "cust_alice", `{"email":" NewBie@Acme.com ","role":"member"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("owner adding a member = %d, want 201 (body %q)", created.Code, created.Body.String())
	}
	member := decodeMember(t, created)
	if member.AccountID != newbie || member.Role != state.RoleMember {
		t.Fatalf("created member = %+v, want %s as a member", member, newbie)
	}
	if member.CreatedAt.IsZero() {
		t.Error("the created grant has no timestamp")
	}
	// Identical again: the same access, and a status that does not claim a grant
	// this call made.
	repeat := addMember(accounts, "human_alice", "cust_alice", `{"email":"newbie@acme.com","role":"member"}`)
	if repeat.Code != http.StatusOK {
		t.Errorf("identical re-add = %d, want 200 (body %q)", repeat.Code, repeat.Body.String())
	}
	// A different role is a conflict, not a silent overwrite.
	conflict := addMember(accounts, "human_alice", "cust_alice", `{"email":"newbie@acme.com","role":"viewer"}`)
	if conflict.Code != http.StatusConflict {
		t.Errorf("re-add with a different role = %d, want 409 (body %q)", conflict.Code, conflict.Body.String())
	}

	// An admin runs the roster, except for the owner seat.
	if code := addMember(accounts, "human_adm", "cust_alice", `{"email":"second@acme.com","role":"viewer"}`).Code; code != http.StatusCreated {
		t.Errorf("admin adding a viewer = %d, want 201", code)
	}
	ownerAttempt := addMember(accounts, "human_adm", "cust_alice", `{"email":"second@acme.com","role":"owner"}`)
	if ownerAttempt.Code != http.StatusForbidden {
		t.Fatalf("admin adding an owner = %d, want 403 (body %q)", ownerAttempt.Code, ownerAttempt.Body.String())
	}
	if !strings.Contains(ownerAttempt.Body.String(), "owner") {
		t.Errorf("the owner refusal does not say what is out of reach: %s", ownerAttempt.Body.String())
	}
	// An owner may hand out the owner role.
	if code := addMember(accounts, "human_alice", "cust_alice", `{"email":"second@acme.com","role":"owner"}`).Code; code != http.StatusConflict {
		t.Errorf("owner re-adding an existing viewer as owner = %d, want 409", code)
	}

	// An address with no unique verified account behind it is ONE answer, so an
	// authorized manager cannot probe which addresses have accounts here.
	if _, err := store.UpsertAccount(testIssuer, "unverified", state.AccountProfile{
		Email: "unverified@acme.com", EmailVerified: false,
	}); err != nil {
		t.Fatalf("UpsertAccount unverified: %v", err)
	}
	var missBody string
	for i, email := range []string{"nobody@acme.com", "unverified@acme.com"} {
		rec := addMember(accounts, "human_alice", "cust_alice",
			fmt.Sprintf(`{"email":%q,"role":"member"}`, email))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s = %d, want 404 (body %q)", email, rec.Code, rec.Body.String())
		}
		if i == 0 {
			missBody = rec.Body.String()
		} else if rec.Body.String() != missBody {
			t.Errorf("%s answered %q, not %q — the route tells verified from unknown",
				email, rec.Body.String(), missBody)
		}
	}
	if strings.Contains(missBody, testIssuer) || strings.Contains(missBody, "subject") {
		t.Errorf("the target 404 leaked identity provider detail: %s", missBody)
	}

	// Body boundaries. Every one of these is the caller's own mistake, and none
	// of them depends on the tenant existing.
	for _, tc := range []struct{ name, body string }{
		{"no body", ""},
		{"not an object", `"newbie@acme.com"`},
		{"unknown field", `{"email":"newbie@acme.com","role":"member","invite":true}`},
		{"two objects", `{"email":"newbie@acme.com","role":"member"}{"email":"x@acme.com","role":"member"}`},
		{"no email", `{"role":"member"}`},
		{"blank email", `{"email":"   ","role":"member"}`},
		{"no role", `{"email":"newbie@acme.com"}`},
		{"unrecognised role", `{"email":"newbie@acme.com","role":"superuser"}`},
	} {
		rec := addMember(accounts, "human_alice", "cust_alice", tc.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400 (body %q)", tc.name, rec.Code, rec.Body.String())
		}
	}
	// A body the caller controls is never echoed back.
	echo := addMember(accounts, "human_alice", "cust_alice", `{"email":"<script>alert(1)</script>","role":"member"}`)
	if strings.Contains(echo.Body.String(), "<script>") {
		t.Errorf("the request body was echoed into the error: %s", echo.Body.String())
	}
}

// The role-change contract: an admin's working range, the owner rules that keep
// the role floor from being decorative, and the statuses either side of them.
func TestUpdateMemberRoleAuthorityAndStatuses(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "adm": state.RoleAdmin,
		"mem": state.RoleMember, "view": state.RoleViewer,
	})
	stranger := outsider(t, store, "stranger")

	for _, sub := range []string{"mem", "view"} {
		rec := patchMember(accounts, "human_"+sub, "cust_alice", ids["view"], `{"role":"member"}`)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s changing a role = %d, want 403 (body %q)", sub, rec.Code, rec.Body.String())
		}
	}

	promoted := patchMember(accounts, "human_adm", "cust_alice", ids["view"], `{"role":"member"}`)
	if promoted.Code != http.StatusOK {
		t.Fatalf("admin promoting a viewer = %d, want 200 (body %q)", promoted.Code, promoted.Body.String())
	}
	if got := decodeMember(t, promoted); got.AccountID != ids["view"] || got.Role != state.RoleMember {
		t.Fatalf("updated member = %+v", got)
	}
	// The role they already hold: the same answer, and nothing changed.
	if code := patchMember(accounts, "human_adm", "cust_alice", ids["view"], `{"role":"member"}`).Code; code != http.StatusOK {
		t.Errorf("setting the role already held = %d, want 200", code)
	}
	// The owner seat is out of an admin's reach in both directions.
	if code := patchMember(accounts, "human_adm", "cust_alice", ids["mem"], `{"role":"owner"}`).Code; code != http.StatusForbidden {
		t.Errorf("admin promoting to owner = %d, want 403", code)
	}
	if code := patchMember(accounts, "human_adm", "cust_alice", ids["alice"], `{"role":"viewer"}`).Code; code != http.StatusForbidden {
		t.Errorf("admin demoting the owner = %d, want 403", code)
	}
	if m, err := store.MembershipFor(ids["alice"], "cust_alice"); err != nil || m.Role != state.RoleOwner {
		t.Fatalf("the owner's grant did not survive an admin's attempts: %+v %v", m, err)
	}
	// An owner may promote, and the last owner may not stand down.
	if code := patchMember(accounts, "human_alice", "cust_alice", ids["adm"], `{"role":"owner"}`).Code; code != http.StatusOK {
		t.Errorf("owner promoting an admin to owner = %d, want 200", code)
	}
	if code := patchMember(accounts, "human_alice", "cust_alice", ids["alice"], `{"role":"member"}`).Code; code != http.StatusOK {
		t.Errorf("owner standing down with another owner in place = %d, want 200", code)
	}
	last := patchMember(accounts, "human_adm", "cust_alice", ids["adm"], `{"role":"admin"}`)
	if last.Code != http.StatusConflict {
		t.Fatalf("the last owner standing down = %d, want 409 (body %q)", last.Code, last.Body.String())
	}
	if m, err := store.MembershipFor(ids["adm"], "cust_alice"); err != nil || m.Role != state.RoleOwner {
		t.Fatalf("a 409 changed the last owner's grant: %+v %v", m, err)
	}

	// A target with no grant on this tenant, and an unknown tenant, are both
	// 404 — and the first does not claim the caller's live tenant is missing.
	missing := patchMember(accounts, "human_adm", "cust_alice", stranger, `{"role":"member"}`)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("changing a non-member's role = %d, want 404 (body %q)", missing.Code, missing.Body.String())
	}
	if strings.Contains(missing.Body.String(), "tenant not found") {
		t.Errorf("a live tenant was reported as missing: %s", missing.Body.String())
	}
	if code := patchMember(accounts, "human_adm", "cust_nope", ids["mem"], `{"role":"member"}`).Code; code != http.StatusNotFound {
		t.Errorf("changing a role on an unknown tenant = %d, want 404", code)
	}

	for _, tc := range []struct{ name, body string }{
		{"no body", ""},
		{"unknown field", `{"role":"member","email":"mem@acme.com"}`},
		{"no role", `{}`},
		{"unrecognised role", `{"role":"superuser"}`},
	} {
		rec := patchMember(accounts, "human_adm", "cust_alice", ids["mem"], tc.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400 (body %q)", tc.name, rec.Code, rec.Body.String())
		}
	}
}

// failingWriteStore is a store whose durable membership write is refused. The
// store rolls the change back (proved in the state package); this proves the
// handlers answer 503 rather than reporting a change that did not happen.
type failingWriteStore struct {
	*state.Store
	err error
}

func (f *failingWriteStore) AddTenantMemberByEmail(string, string, string, string, string, state.Actor) (*state.TenantMembership, bool, error) {
	return nil, false, f.err
}

func (f *failingWriteStore) SetTenantMemberRole(string, string, string, string, state.Actor) (*state.TenantMembership, string, error) {
	return nil, "", f.err
}

func TestMemberWritesReturnUnavailableWhenTheStoreRefuses(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "mem": state.RoleMember,
	})
	outsider(t, store, "newbie")
	accounts.Store = &failingWriteStore{
		Store: store,
		err:   fmt.Errorf("%w: persist membership mbr_x: connection refused", state.ErrPersistence),
	}

	add := addMember(accounts, "human_alice", "cust_alice", `{"email":"newbie@acme.com","role":"member"}`)
	if add.Code != http.StatusServiceUnavailable {
		t.Fatalf("add against a refusing backend = %d, want 503 (body %q)", add.Code, add.Body.String())
	}
	if strings.Contains(add.Body.String(), "connection refused") {
		t.Errorf("the backend's error reached the caller: %s", add.Body.String())
	}
	patch := patchMember(accounts, "human_alice", "cust_alice", ids["mem"], `{"role":"admin"}`)
	if patch.Code != http.StatusServiceUnavailable {
		t.Fatalf("role change against a refusing backend = %d, want 503 (body %q)", patch.Code, patch.Body.String())
	}
	if _, err := store.MembershipFor("acct_missing", "cust_alice"); err == nil {
		t.Fatal("a refused add left a grant behind")
	}
	if m, err := store.MembershipFor(ids["mem"], "cust_alice"); err != nil || m.Role != state.RoleMember {
		t.Fatalf("a refused role change altered the grant: %+v %v", m, err)
	}
}

// Usage is the tenant's own live state: every role reads it, nobody reads
// another tenant's, and the numbers are the ones GET /v1/spend computes.
func TestTenantUsageIsReadableByEveryRoleAndScopedToOneTenant(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro",
		MaxConcurrentBursts: 5, MaxHourlyUSD: 12.5})
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "ysk_other", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "adm": state.RoleAdmin,
		"mem": state.RoleMember, "view": state.RoleViewer,
	})
	for _, b := range []*state.Burst{
		{ID: "burst_1", CustomerID: "cust_alice", HourlyUSD: 1.5},
		{ID: "burst_2", CustomerID: "cust_alice", HourlyUSD: 2},
		{ID: "burst_other", CustomerID: "cust_other", HourlyUSD: 99},
	} {
		if err := store.PutBurst(b); err != nil {
			t.Fatalf("PutBurst %s: %v", b.ID, err)
		}
	}

	for _, sub := range []string{"alice", "adm", "mem", "view"} {
		rec := getUsage(accounts, "human_"+sub, "cust_alice")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s reading usage = %d, want 200 (body %q)", sub, rec.Code, rec.Body.String())
		}
		var usage TenantUsageResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &usage); err != nil {
			t.Fatalf("decode usage: %v (body %q)", err, rec.Body.String())
		}
		if usage.TenantID != "cust_alice" || usage.RunningBursts != 2 || usage.HourlyUSD != 3.5 {
			t.Fatalf("%s usage = %+v, want this tenant's two bursts at $3.50/hr", sub, usage)
		}
		if usage.ProjectedDailyUSD != 3.5*24 {
			t.Errorf("projection = %v, want %v", usage.ProjectedDailyUSD, 3.5*24)
		}
		if usage.Limits.MaxConcurrentBursts != 5 || usage.Limits.MaxHourlyUSD != 12.5 {
			t.Errorf("limits = %+v, want the tenant's own", usage.Limits)
		}
		if usage.ObservedAt.IsZero() {
			t.Error("usage carries no observation time")
		}
	}

	// The same arithmetic as the cluster-authenticated spend route, because a
	// console disagreeing with the cap that refuses the next burst reads as a
	// cost-accounting bug.
	cust, err := store.CustomerByID("cust_alice")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	spend := computeSpend(store, cust)
	usage := TenantUsageResponse{}
	if err := json.Unmarshal(getUsage(accounts, "human_view", "cust_alice").Body.Bytes(), &usage); err != nil {
		t.Fatalf("decode usage: %v", err)
	}
	if usage.RunningBursts != spend.RunningBursts || usage.HourlyUSD != spend.HourlyUSD ||
		usage.ProjectedDailyUSD != spend.ProjectedDailyUSD || usage.Limits != spend.Limits {
		t.Errorf("usage %+v disagrees with GET /v1/spend %+v", usage, spend)
	}

	// Another tenant's usage, and an unknown tenant's, are the same 404 — a
	// member of one tenant cannot even tell the other exists.
	var wantBody string
	for i, tenant := range []string{"cust_other", "cust_nope"} {
		rec := getUsage(accounts, "human_alice", tenant)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("usage of %s = %d, want 404 (body %q)", tenant, rec.Code, rec.Body.String())
		}
		if i == 0 {
			wantBody = rec.Body.String()
		} else if rec.Body.String() != wantBody {
			t.Errorf("usage 404 bodies differ (%q vs %q) — the route is an oracle", rec.Body.String(), wantBody)
		}
	}
	// No credential is 401, not a leak of whether the tenant is there.
	if code := getUsage(accounts, "", "cust_alice").Code; code != http.StatusUnauthorized {
		t.Errorf("usage without a credential = %d, want 401", code)
	}
	// A revoked tenant stops answering, like every other tenant-scoped read.
	if err := store.RevokeCustomer("cust_alice"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if code := getUsage(accounts, "human_alice", "cust_alice").Code; code != http.StatusNotFound {
		t.Errorf("usage of a revoked tenant = %d, want 404", code)
	}
}

// failingUsageStore returns a fault the usage read cannot produce today. The
// point is what happens when it can: it must not be reported as a deleted
// tenant, which would tell a member their live tenant is gone.
type failingUsageStore struct {
	*state.Store
	err error
}

func (f *failingUsageStore) TenantUsageFor(string, string) (state.TenantUsage, error) {
	return state.TenantUsage{}, f.err
}

func TestTenantUsageDoesNotReportAnUnknownFaultAsAMissingTenant(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})
	accounts.Store = &failingUsageStore{
		Store: store,
		err:   fmt.Errorf("%w: read usage: connection refused", state.ErrPersistence),
	}

	rec := getUsage(accounts, "human_alice", "cust_alice")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "connection refused") {
		t.Errorf("the backend's error reached the caller: %s", rec.Body.String())
	}
}
