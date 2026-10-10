//go:build integration

// yscale:proprietary

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

type humanAuthorizationFixture struct {
	a, b                          *state.Store
	tenant, owner, member, target string
	subject                       string
	api                           *Accounts
}

type cancellationAuthorityStore struct {
	accountStore
	before func()
}

func (s *cancellationAuthorityStore) RequestWorkloadCancellation(ctx context.Context, tenant, workload string, principal state.WorkloadCancelPrincipal) (state.WorkloadCancellation, error) {
	s.before()
	return s.accountStore.RequestWorkloadCancellation(ctx, tenant, workload, principal)
}

func TestPostgresWorkloadCancellationHumanHTTP(t *testing.T) {
	for _, change := range []string{"accepted", "role-changed", "submitter-changed"} {
		t.Run(change, func(t *testing.T) {
			f := newHumanAuthorizationFixture(t)
			actor := state.HumanActor(f.member, f.tenant)
			if _, _, err := f.a.SetTenantMemberRole(f.tenant, f.owner, f.member, state.RoleMember, state.HumanActor(f.owner, f.tenant)); err != nil {
				t.Fatal(err)
			}
			id, burstID := "wl_cancel_"+f.tenant, "burst_cancel_"+f.tenant
			w := &state.Workload{ID: id, CustomerID: f.tenant, BurstID: burstID, Status: "running", SubmittedBy: &actor}
			if err := f.a.PutWorkloadDurable(w); err != nil {
				t.Fatal(err)
			}
			if err := f.a.PutBurst(&state.Burst{ID: burstID, CustomerID: f.tenant, Backend: "linode", BackendID: "synthetic-human-cancel", CreatedAt: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := f.a.DeleteBurst(burstID); err != nil {
					t.Error(err)
				}
			})
			deletes := newFakeProviderDeletes()
			f.api.Workloads = &Workloads{Store: f.b, Deletes: deletes, Reaper: &fakeReaper{}, Log: quietLog()}
			f.api.Store = &cancellationAuthorityStore{accountStore: f.b, before: func() {
				switch change {
				case "role-changed":
					if _, _, err := f.a.SetTenantMemberRole(f.tenant, f.owner, f.member, state.RoleViewer, state.HumanActor(f.owner, f.tenant)); err != nil {
						t.Fatal(err)
					}
				case "submitter-changed":
					other := state.HumanActor(f.owner, f.tenant)
					w.SubmittedBy = &other
					if err := f.a.PutWorkloadDurable(w); err != nil {
						t.Fatal(err)
					}
				}
			}}
			response := callTenantWorkload(f.api, http.MethodDelete, "/v1/tenants/"+f.tenant+"/workloads/"+id, "synthetic-human-session", "")
			want, requests, outcome := http.StatusForbidden, 0, state.OutcomeDenied
			if change == "accepted" {
				want, requests, outcome = http.StatusOK, 1, state.OutcomeAccepted
			}
			if response.Code != want || deletes.requests != requests {
				t.Fatalf("human cancellation code=%d deletes=%d, want %d/%d", response.Code, deletes.requests, want, requests)
			}
			current, err := f.a.WorkloadSnapshotForCustomer(context.Background(), f.tenant, id)
			if err != nil {
				t.Fatal(err)
			}
			if change == "accepted" {
				if current.Workload.Status != "cancelled" || !current.Burst.ReapPending {
					t.Fatal("accepted action not durable")
				}
			} else if current.Workload.Status != "running" || current.Burst.ReapPending {
				t.Fatal("denied action changed state")
			}
			page, err := f.a.TenantAuditFor(context.Background(), f.tenant, f.owner, state.AuditQuery{})
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, event := range page.Events {
				if event.Action == state.ActionWorkloadCancel && event.TargetID == id {
					count++
					if event.Outcome != outcome {
						t.Fatal("wrong cancellation audit")
					}
				}
			}
			if count != 1 {
				t.Fatalf("cancellation audits=%d", count)
			}
		})
	}
}

func newHumanAuthorizationFixture(t *testing.T) *humanAuthorizationFixture {
	t.Helper()
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	a, err := state.NewPostgres(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	suffix := fmt.Sprint(time.Now().UnixNano())
	f := &humanAuthorizationFixture{a: a, tenant: "cust_human_" + suffix, subject: "member-" + suffix}
	account := func(subject string) string {
		row, err := a.UpsertAccount(testIssuer, subject, state.AccountProfile{})
		if err != nil {
			t.Fatal(err)
		}
		return row.ID
	}
	f.owner, f.member, f.target = account("owner-"+suffix), account(f.subject), account("target-"+suffix)
	if _, _, err := a.CreateTenant(&state.Customer{ID: f.tenant, Token: "synthetic-human-" + suffix}, f.owner); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AddTenantMembership(f.member, f.tenant, state.RoleAdmin); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AddTenantMembership(f.target, f.tenant, state.RoleViewer); err != nil {
		t.Fatal(err)
	}
	f.b, err = state.NewPostgres(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.b.Close)
	f.api = &Accounts{Store: f.b, Issuer: testIssuer, Log: quietLog(),
		Resolver: staticIdentityResolver{"synthetic-human-session": {Subject: f.subject}}}
	return f
}

func TestPostgresHumanAuthorizationAcrossReplicas(t *testing.T) {
	for _, change := range []string{"removed", "downgraded", "tenant-revoked", "database-unavailable"} {
		t.Run(change, func(t *testing.T) {
			f := newHumanAuthorizationFixture(t)
			if got := listMembers(f.api, "synthetic-human-session", f.tenant).Code; got != http.StatusOK {
				t.Fatalf("initial roster = %d", got)
			}
			var err error
			want := http.StatusNotFound
			switch change {
			case "removed":
				_, err = f.a.RemoveTenantMembership(f.tenant, f.owner, f.member, state.HumanActor(f.owner, f.tenant))
			case "downgraded":
				_, _, err = f.a.SetTenantMemberRole(f.tenant, f.owner, f.member, state.RoleViewer, state.HumanActor(f.owner, f.tenant))
				want = http.StatusForbidden
			case "tenant-revoked":
				err = f.a.RevokeCustomer(f.tenant)
			case "database-unavailable":
				f.b.Close()
				want = http.StatusServiceUnavailable
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := listMembers(f.api, "synthetic-human-session", f.tenant).Code; got != want {
				t.Errorf("stale replica roster after %s = %d, want %d", change, got, want)
			}
			if change == "removed" || change == "downgraded" {
				if got := removeMember(f.api, "synthetic-human-session", f.tenant, f.target).Code; got != want {
					t.Errorf("stale replica member removal after %s = %d, want %d", change, got, want)
				}
				// A's own cache could conceal a delete performed by B. Inspect
				// another freshly loaded store to prove the target was preserved.
				fresh, err := state.NewPostgres(context.Background(), os.Getenv("YSCALE_TEST_DATABASE_URL"))
				if err != nil {
					t.Fatal(err)
				}
				defer fresh.Close()
				if _, err := fresh.MembershipFor(f.target, f.tenant); err != nil {
					t.Errorf("refused operation still removed the protected member: %v", err)
				}
			}
		})
	}
}

func TestPostgresHumanAuthorizationFindsNewAccountAndGrant(t *testing.T) {
	f := newHumanAuthorizationFixture(t)
	subject := "fresh-" + f.subject
	account, err := f.a.UpsertAccount(testIssuer, subject, state.AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.a.AddTenantMembership(account.ID, f.tenant, state.RoleAdmin); err != nil {
		t.Fatal(err)
	}
	f.api.Resolver = staticIdentityResolver{"synthetic-human-session": {Subject: subject}}
	if got := listMembers(f.api, "synthetic-human-session", f.tenant).Code; got != http.StatusOK {
		t.Fatalf("new durable human/grant is unavailable on older replica: %d", got)
	}
}

func TestPostgresMembershipWritesProtectCurrentOwnerOverHTTP(t *testing.T) {
	f := newHumanAuthorizationFixture(t)
	if _, _, err := f.a.SetTenantMembershipRole(f.target, f.tenant, state.RoleOwner, state.OperatorActor()); err != nil {
		t.Fatal(err)
	}
	if got := removeMember(f.api, "synthetic-human-session", f.tenant, f.target).Code; got != http.StatusForbidden {
		t.Fatalf("stale admin removed a current owner: HTTP %d", got)
	}
	current, err := f.a.MembershipFor(f.target, f.tenant)
	if err != nil || current.Role != state.RoleOwner {
		t.Fatalf("refused HTTP removal changed durable owner: %v", err)
	}
}

func TestPostgresAccountWritesNoOpHTTPFailsClosed(t *testing.T) {
	f := newHumanAuthorizationFixture(t)
	// The resolver still accepts the synthetic session and returns the same
	// empty profile this replica cached. The account no-op must consult SQL.
	f.b.Close()
	r := httptest.NewRequest(http.MethodGet, "/v1/account", nil)
	r.Header.Set("Authorization", "Bearer synthetic-human-session")
	w := httptest.NewRecorder()
	f.api.HandleGet(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("account no-op bypassed unavailable database: HTTP %d", w.Code)
	}
}

func TestPostgresAccountReadsTenantListAcrossReplicas(t *testing.T) {
	for _, change := range []string{"role", "removed", "revoked"} {
		t.Run(change, func(t *testing.T) {
			f := newHumanAuthorizationFixture(t)
			var err error
			switch change {
			case "role":
				_, _, err = f.a.SetTenantMembershipRole(f.member, f.tenant, state.RoleViewer, state.OperatorActor())
			case "removed":
				_, err = f.a.RemoveTenantMembership(f.tenant, f.owner, f.member, state.HumanActor(f.owner, f.tenant))
			case "revoked":
				err = f.a.RevokeCustomer(f.tenant)
			}
			if err != nil {
				t.Fatal(err)
			}
			response := getAccount(f.api, "synthetic-human-session")
			if response.Code != http.StatusOK {
				t.Fatalf("account response = %d", response.Code)
			}
			got := decodeAccount(t, response)
			if change == "role" {
				if len(got.Tenants) != 1 || got.Tenants[0].Role != state.RoleViewer {
					t.Fatalf("account returned stale role: %+v", got.Tenants)
				}
			} else if len(got.Tenants) != 0 {
				t.Fatalf("account retained removed/revoked tenant: %+v", got.Tenants)
			}
		})
	}
}

type unavailableAfterProfileRefresh struct {
	accountStore
	close func()
}

func (s unavailableAfterProfileRefresh) UpsertAccount(issuer, subject string, profile state.AccountProfile) (*state.Account, error) {
	account, err := s.accountStore.UpsertAccount(issuer, subject, profile)
	if err == nil {
		s.close()
	}
	return account, err
}

func TestPostgresAccountReadsHTTPOutageAfterProfileRefresh(t *testing.T) {
	f := newHumanAuthorizationFixture(t)
	f.api.Store = unavailableAfterProfileRefresh{accountStore: f.b, close: f.b.Close}
	response := getAccount(f.api, "synthetic-human-session")
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), f.tenant) || strings.Contains(response.Body.String(), f.member) {
		t.Fatalf("tenant read outage returned successful/partial account: HTTP %d", response.Code)
	}
}

type unavailableAfterTenantCreation struct {
	accountStore
	close func()
}

func (s unavailableAfterTenantCreation) CreateFirstTenant(c *state.Customer, owner string) (*state.Customer, *state.TenantMembership, error) {
	tenant, member, err := s.accountStore.CreateFirstTenant(c, owner)
	if err == nil {
		s.close()
	}
	return tenant, member, err
}

func TestPostgresAccountReadsHTTPOutageAfterTenantCreation(t *testing.T) {
	f := newHumanAuthorizationFixture(t)
	subject := "first-workspace-" + f.subject
	f.api.AllowSelfServiceTenants = true
	f.api.Resolver = staticIdentityResolver{"synthetic-human-session": {Subject: subject, EmailVerified: true}}
	f.api.Store = unavailableAfterTenantCreation{accountStore: f.b, close: f.b.Close}
	r := httptest.NewRequest(http.MethodPost, "/v1/account/tenants", strings.NewReader(`{"name":"Synthetic workspace"}`))
	r.Header.Set("Authorization", "Bearer synthetic-human-session")
	w := httptest.NewRecorder()
	f.api.HandleCreateTenant(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-commit read outage = HTTP %d", w.Code)
	}
	// The successful write cannot be rolled back by a later response read.
	// Recovery is a GET of the deterministic workspace, not a second create.
	f.api.Store = f.a
	response := getAccount(f.api, "synthetic-human-session")
	if response.Code != http.StatusOK {
		t.Fatalf("recovery GET = HTTP %d", response.Code)
	}
	got := decodeAccount(t, response)
	if len(got.Tenants) != 1 || got.Tenants[0].CustomerID != state.SelfServiceTenantID(got.AccountID) {
		t.Fatalf("committed workspace not recoverable: %+v", got.Tenants)
	}
}

type unavailableAfterMembershipRead struct {
	accountStore
	close func()
}

func (s unavailableAfterMembershipRead) MembershipFor(account, tenant string) (*state.TenantMembership, error) {
	member, err := s.accountStore.MembershipFor(account, tenant)
	if err == nil {
		s.close()
	}
	return member, err
}

func TestPostgresAccountReadsHTTPTenantSummaryOutage(t *testing.T) {
	t.Run("workload-summary", func(t *testing.T) {
		f := newHumanAuthorizationFixture(t)
		f.api.Store = unavailableAfterMembershipRead{accountStore: f.b, close: f.b.Close}
		r := httptest.NewRequest(http.MethodPost, "/synthetic-workload", strings.NewReader("{}"))
		r.SetPathValue("tenant_id", f.tenant)
		r.Header.Set("Authorization", "Bearer synthetic-human-session")
		w := httptest.NewRecorder()
		f.api.HandleCreateWorkload(w, r)
		if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "workload service unavailable") {
			t.Fatalf("summary outage misclassified: HTTP %d", w.Code)
		}
	})
}

func TestPostgresAccountReadsRosterProfilesOverHTTP(t *testing.T) {
	f := newHumanAuthorizationFixture(t)
	current, err := f.a.AccountByID(f.target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.a.UpsertAccount(current.Issuer, current.Subject, state.AccountProfile{Name: "Current profile", Email: "current@synthetic.invalid"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.a.SetTenantMembershipRole(f.target, f.tenant, state.RoleMember, state.OperatorActor()); err != nil {
		t.Fatal(err)
	}
	response := listMembers(f.api, "synthetic-human-session", f.tenant)
	if response.Code != http.StatusOK {
		t.Fatalf("roster response = %d", response.Code)
	}
	got := decodeMembers(t, response)
	found := false
	for _, row := range got.Members {
		if row.AccountID == f.target {
			found = true
			if row.Name != "Current profile" || row.Email != "current@synthetic.invalid" || row.Role != state.RoleMember {
				t.Fatalf("stale HTTP roster: %+v", row)
			}
		}
	}
	if !found {
		t.Fatal("current target missing from roster")
	}
}

type profileChangedBeforeFirstTenant struct {
	accountStore
	change func()
}

func (s profileChangedBeforeFirstTenant) CreateFirstTenant(c *state.Customer, owner string) (*state.Customer, *state.TenantMembership, error) {
	s.change()
	return s.accountStore.CreateFirstTenant(c, owner)
}

func TestPostgresFirstWorkspaceRejectsVerificationChangeOverHTTP(t *testing.T) {
	f := newHumanAuthorizationFixture(t)
	subject := "workspace-verification-" + f.subject
	f.api.AllowSelfServiceTenants = true
	f.api.Resolver = staticIdentityResolver{"synthetic-human-session": {Subject: subject, Email: "before@synthetic.invalid", EmailVerified: true}}
	f.api.Store = profileChangedBeforeFirstTenant{accountStore: f.b, change: func() {
		// Runs after the handler accepted its returned verified profile, but
		// before the durable workspace operation acquires the account lock.
		if _, err := f.a.UpsertAccount(testIssuer, subject, state.AccountProfile{Email: "current@synthetic.invalid"}); err != nil {
			t.Fatal(err)
		}
	}}
	r := httptest.NewRequest(http.MethodPost, "/v1/account/tenants", strings.NewReader(`{"name":"Synthetic workspace"}`))
	r.Header.Set("Authorization", "Bearer synthetic-human-session")
	w := httptest.NewRecorder()
	f.api.HandleCreateTenant(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("verification changed before create: HTTP %d, want 403", w.Code)
	}
	account, err := f.a.AccountByIdentity(testIssuer, subject)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.a.TenantSummaryByID(state.SelfServiceTenantID(account.ID)); err == nil {
		t.Fatal("refused HTTP request still created a workspace")
	}
}

type unavailableAfterHumanAccount struct {
	accountStore
	close func()
}

func (s unavailableAfterHumanAccount) AccountByIdentity(issuer, subject string) (*state.Account, error) {
	account, err := s.accountStore.AccountByIdentity(issuer, subject)
	if err == nil {
		s.close()
	}
	return account, err
}

func TestPostgresHumanAuthorizationReadOutageIsNotTenantAbsence(t *testing.T) {
	for _, tc := range []struct {
		name, method string
		handle       func(*Accounts, http.ResponseWriter, *http.Request)
	}{
		{"roster", http.MethodGet, (*Accounts).HandleListMembers},
		{"workload-create", http.MethodPost, (*Accounts).HandleCreateWorkload},
		{"cloud-account-read", http.MethodGet, (*Accounts).HandleGetLinodeCloudAccount},
		{"cloud-account-write", http.MethodPut, (*Accounts).HandlePutLinodeCloudAccount},
		{"cloud-account-delete", http.MethodDelete, (*Accounts).HandleDeleteLinodeCloudAccount},
		{"runtime-bindings", http.MethodGet, (*Accounts).HandleListRuntimeBindings},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newHumanAuthorizationFixture(t)
			f.api.Store = unavailableAfterHumanAccount{accountStore: f.b, close: f.b.Close}
			r := httptest.NewRequest(tc.method, "/synthetic-authority-boundary", strings.NewReader("{}"))
			r.Header.Set("Authorization", "Bearer synthetic-human-session")
			r.SetPathValue("tenant_id", f.tenant)
			w := httptest.NewRecorder()
			tc.handle(f.api, w, r)
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("membership read outage after successful identity lookup = %d, want 503", w.Code)
			}
		})
	}
}
