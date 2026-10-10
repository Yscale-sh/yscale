// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

func auditMux(a *Accounts) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/audit", http.HandlerFunc(a.HandleListAudit))
	return mux
}

func callAudit(a *Accounts, path, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	auditMux(a).ServeHTTP(rec, req)
	return rec
}

func decodeAudit(t *testing.T, rec *httptest.ResponseRecorder) TenantAuditResponse {
	t.Helper()
	var response TenantAuditResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode audit page: %v (body %q)", err, rec.Body.String())
	}
	return response
}

// journalStore serves the audit read off a canned page, so the handler's
// pagination, rendering and failure mapping are testable without a Postgres.
// Everything else is the real store, including the authorization the role
// matrix below exercises.
type journalStore struct {
	*state.Store
	page state.AuditPage
	err  error
	// gotQuery records what the handler passed down, which is how the limit and
	// cursor assertions prove the handler forwards rather than reinterprets.
	gotQuery state.AuditQuery
}

func (j *journalStore) TenantAuditFor(_ context.Context, _, _ string, q state.AuditQuery) (state.AuditPage, error) {
	j.gotQuery = q
	if j.err != nil {
		return state.AuditPage{}, j.err
	}
	return j.page, nil
}

// Only a tenant's own managers may read its journal, and every way a caller has
// no business with the tenant answers identically.
func TestAuditRouteRoleMatrix(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_j", Token: "ysk_j", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "ysk_other", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_j", map[string]string{
		"alice": state.RoleOwner, "adm": state.RoleAdmin,
		"mem": state.RoleMember, "view": state.RoleViewer,
	})

	for _, caller := range []string{"alice", "adm"} {
		rec := callAudit(accounts, "/v1/tenants/cust_j/audit", "human_"+caller)
		if rec.Code != http.StatusOK {
			t.Errorf("%s reading the journal = %d, want 200 (body %q)", caller, rec.Code, rec.Body.String())
		}
		// An empty journal is a 200 with an empty list, not a 404.
		page := decodeAudit(t, rec)
		if page.Events == nil {
			t.Errorf("%s got a null events list; an empty journal is an empty list", caller)
		}
		if len(page.Events) != 0 || page.NextAfter != "" {
			t.Errorf("%s page = %+v, want empty", caller, page)
		}
	}
	for _, caller := range []string{"mem", "view"} {
		if code := callAudit(accounts, "/v1/tenants/cust_j/audit", "human_"+caller).Code; code != http.StatusForbidden {
			t.Errorf("%s reading the journal = %d, want 403", caller, code)
		}
	}

	// Unknown, revoked and not-a-member collapse into one byte-identical answer.
	store.AddCustomer(&state.Customer{ID: "cust_revoked", Token: "ysk_revoked", Plan: "pro"})
	alice, err := store.AccountByIdentity(testIssuer, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddTenantMembership(alice.ID, "cust_revoked", state.RoleOwner); err != nil {
		t.Fatal(err)
	}
	if err := store.RevokeCustomer("cust_revoked"); err != nil {
		t.Fatal(err)
	}
	var want string
	for _, tenant := range []string{"cust_unknown", "cust_revoked", "cust_other"} {
		rec := callAudit(accounts, "/v1/tenants/"+tenant+"/audit", "human_alice")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s = %d, want 404 (body %q)", tenant, rec.Code, rec.Body.String())
		}
		if want == "" {
			want = rec.Body.String()
		} else if rec.Body.String() != want {
			t.Fatalf("%s body %q differs from %q; the three must be indistinguishable",
				tenant, rec.Body.String(), want)
		}
	}

	// And the credential itself: a cluster token authenticates nothing here.
	if code := callAudit(accounts, "/v1/tenants/cust_j/audit", "ysk_j").Code; code != http.StatusUnauthorized {
		t.Errorf("cluster token on the audit route = %d, want 401", code)
	}
	if code := callAudit(accounts, "/v1/tenants/cust_j/audit", "").Code; code != http.StatusUnauthorized {
		t.Errorf("no credential = %d, want 401", code)
	}
}

// Page bounds are validated before the tenant is looked at, and everything
// ambiguous is refused rather than resolved into a page the caller did not ask
// for.
func TestAuditRouteRejectsMalformedPageBounds(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_q", Token: "ysk_q", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_q", map[string]string{"alice": state.RoleOwner})

	bad := []string{
		"?limit=0", "?limit=201", "?limit=abc", "?limit=-1",
		"?limit=10&limit=9", "?after=", "?limit=",
		"?after=page-2", "?after=aud_zzz", "?cursor=x", "?since=yesterday",
	}
	for _, query := range bad {
		rec := callAudit(accounts, "/v1/tenants/cust_q/audit"+query, "human_alice")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400 (body %q)", query, rec.Code, rec.Body.String())
		}
	}
	// A malformed bound answers the same for a tenant that does not exist, so
	// the 400 is not an existence oracle.
	if code := callAudit(accounts, "/v1/tenants/cust_nope/audit?limit=0", "human_alice").Code; code != http.StatusBadRequest {
		t.Errorf("malformed bound on an unknown tenant = %d, want 400", code)
	}
	// The accepted forms reach the store as given.
	js := &journalStore{Store: store}
	accounts.Store = js
	cursor := "aud_" + strings.Repeat("0", 32)
	if code := callAudit(accounts, "/v1/tenants/cust_q/audit?limit=7&after="+cursor, "human_alice").Code; code != http.StatusOK {
		t.Fatalf("valid bounds = %d", code)
	}
	if js.gotQuery.Limit != 7 || js.gotQuery.After != cursor {
		t.Fatalf("handler forwarded %+v, want limit 7 and the cursor", js.gotQuery)
	}
}

// A journal that cannot be read is a 503 the caller can retry, never the 404
// that would tell a manager their tenant's history had been deleted.
func TestAuditRouteSurfacesDurableReadFailure(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_d", Token: "ysk_d", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_d", map[string]string{"alice": state.RoleOwner})
	accounts.Store = &journalStore{
		Store: store,
		err:   fmt.Errorf("%w: read audit for cust_d: connection refused", state.ErrPersistence),
	}

	rec := callAudit(accounts, "/v1/tenants/cust_d/audit", "human_alice")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("durable read failure = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "connection refused") {
		t.Errorf("the backend's own error reached the response: %q", rec.Body.String())
	}
}

// The rendered page carries the evidence and nothing else. The stored row here
// is deliberately loaded with the things that must never ship: the response is
// built field by field from a closed shape, so there is nowhere for them to go.
func TestAuditRouteRendersNoSensitiveFields(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_s", Token: "ysk_cluster_secret", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_s", map[string]string{"alice": state.RoleOwner})
	accounts.Store = &journalStore{
		Store: store,
		page: state.AuditPage{
			Events: []state.AuditEvent{{
				ID:         "aud_" + strings.Repeat("a", 32),
				CustomerID: "cust_s",
				At:         time.Now().UTC(),
				Actor:      state.HumanActor(ids["alice"], "cust_s"),
				Action:     state.ActionWorkloadSubmit,
				Outcome:    state.OutcomeAccepted,
				TargetKind: state.TargetWorkload,
				TargetID:   "wl_1",
				Detail: state.AuditDetail{
					Reason: state.ReasonNamespaceAuthorized, Role: state.RoleOwner,
					Rule: "tenant.workload_namespaces", RuleVersion: "v1",
					RequestedNamespace: "jobs", GrantedNamespace: "jobs", BurstID: "burst_1", RetryWorkloadID: "wl_retry_1",
				},
			}},
			NextAfter: "aud_" + strings.Repeat("b", 32),
		},
	}

	rec := callAudit(accounts, "/v1/tenants/cust_s/audit", "human_alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body %q)", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, secret := range []string{
		"ysk_cluster_secret", // the tenant's cluster token
		testIssuer,           // the identity provider's issuer
		"\"subject\"",        // and its subject
		"alice@acme.com",     // the human's email
		"SpecYAML", "spec", "env", "command", "endpoint", "Secret",
	} {
		if strings.Contains(body, secret) {
			t.Errorf("audit response leaked %q: %s", secret, body)
		}
	}
	page := decodeAudit(t, rec)
	if len(page.Events) != 1 {
		t.Fatalf("events = %+v", page.Events)
	}
	ev := page.Events[0]
	if ev.Detail.RetryWorkloadID != "wl_retry_1" {
		t.Error("audit response lost the new retry workload link")
	}
	if ev.Actor.Kind != state.ActorHuman || ev.Actor.AccountID != ids["alice"] {
		t.Errorf("actor = %+v", ev.Actor)
	}
	if ev.Detail.Rule != "tenant.workload_namespaces" || ev.Detail.RuleVersion != "v1" ||
		ev.Detail.GrantedNamespace != "jobs" {
		t.Errorf("detail = %+v", ev.Detail)
	}
	if page.NextAfter == "" {
		t.Error("next cursor dropped")
	}
}

func TestAuditRouteCopiesTenantLimitChangeIncludingZeroValues(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_limits", Token: "ysk_limits_secret", Plan: "pro"})
	accounts, ids := rosterFixture(t, store, "cust_limits", map[string]string{"alice": state.RoleOwner})
	zeroBursts, newBursts := 0, 4
	zeroHourly, newHourly := 0.0, 12.5
	accounts.Store = &journalStore{
		Store: store,
		page: state.AuditPage{Events: []state.AuditEvent{{
			ID: "aud_" + strings.Repeat("c", 32), CustomerID: "cust_limits", At: time.Now().UTC(),
			Actor: state.HumanActor(ids["alice"], "cust_limits"), Action: state.ActionTenantLimitsSet,
			Outcome: state.OutcomeAccepted, TargetKind: state.TargetTenant, TargetID: "cust_limits",
			Detail: state.AuditDetail{
				Reason: state.ReasonTenantLimitsUpdated, Rule: "tenant.limits_set", RuleVersion: "v1",
				PreviousMaxConcurrentBursts: &zeroBursts, MaxConcurrentBursts: &newBursts,
				PreviousMaxHourlyUSD: &zeroHourly, MaxHourlyUSD: &newHourly,
			},
		}}},
	}

	rec := callAudit(accounts, "/v1/tenants/cust_limits/audit", "human_alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body %q)", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		`"previous_max_concurrent_bursts":0`, `"max_concurrent_bursts":4`,
		`"previous_max_hourly_usd":0`, `"max_hourly_usd":12.5`,
		`"rule":"tenant.limits_set"`, `"rule_version":"v1"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("audit response missing %s: %s", want, body)
		}
	}
	if strings.Contains(body, "ysk_limits_secret") {
		t.Fatalf("audit response leaked tenant token: %s", body)
	}
}

// The cancel matrix over the real HTTP surface: a member cancels their own
// workload and nobody else's, an owner cancels any, and a refusal changes
// nothing — the workload stays running and its burst stays up.
func TestTenantCancelHonoursSubmitterAndRole(t *testing.T) {
	store, accounts, _, reaper := tenantWorkloadFixture(t, map[string]string{
		"alice": state.RoleOwner, "mem": state.RoleMember, "other": state.RoleMember,
	})
	mem, err := store.AccountByIdentity(testIssuer, "mem")
	if err != nil {
		t.Fatal(err)
	}
	submit := func(id string, by *state.Actor) {
		store.PutWorkload(&state.Workload{
			ID: id, CustomerID: "cust_console", BurstID: "burst_" + id, Status: "running",
			CreatedAt: time.Now().UTC(), SpecYAML: []byte(tenantWorkloadYAML), SubmittedBy: by,
		})
		if err := store.PutBurst(&state.Burst{ID: "burst_" + id, CustomerID: "cust_console",
			CreatedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	memActor := state.HumanActor(mem.ID, "cust_console")
	submit("wl_mine", &memActor)
	submit("wl_theirs", &memActor)
	submit("wl_cluster", &state.Actor{Kind: state.ActorCluster, CustomerID: "cust_console"})

	// A co-member may not cancel a workload they did not submit, and nothing
	// about it moves.
	rec := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_theirs", "human_other", "")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("co-member cancel = %d, want 403 (body %q)", rec.Code, rec.Body.String())
	}
	if reaper.teardownCount("burst_wl_theirs") != 0 {
		t.Fatal("a refused cancel tore the burst down")
	}
	stillRunning, err := store.GetWorkload("wl_theirs")
	if err != nil || stillRunning.FinishedAt != nil || stillRunning.Status != "running" {
		t.Fatalf("a refused cancel changed the workload: %+v err=%v", stillRunning, err)
	}
	if _, err := store.GetBurst("burst_wl_theirs"); err != nil {
		t.Fatalf("a refused cancel released the burst: %v", err)
	}

	// A member cancelling their own reaches the existing lifecycle.
	if code := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_mine", "human_mem", "").Code; code != http.StatusOK {
		t.Fatalf("member cancelling their own = %d, want 200", code)
	}
	if reaper.teardownCount("burst_wl_mine") != 1 {
		t.Fatal("the member's own cancel did not reach the reap path")
	}

	// A member may not cancel what a cluster credential submitted: nothing
	// records it as theirs.
	if code := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_cluster", "human_mem", "").Code; code != http.StatusForbidden {
		t.Fatalf("member cancelling a cluster submission = %d, want 403", code)
	}
	// The owner may cancel any of them.
	if code := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_cluster", "human_alice", "").Code; code != http.StatusOK {
		t.Fatalf("owner cancelling a cluster submission = %d, want 200", code)
	}
	if code := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_theirs", "human_alice", "").Code; code != http.StatusOK {
		t.Fatalf("owner cancelling a member's workload = %d, want 200", code)
	}
}

// auditFailStore refuses the journal write. Everything else is the real store,
// so what this proves is the ORDERING: the decision is recorded before it is
// acted on, and a journal that will not take the row stops the action.
type auditFailStore struct {
	*state.Store
	err error
}

func (a *auditFailStore) AppendAudit(*state.AuditEvent) error { return a.err }

func (a *auditFailStore) RequestWorkloadCancellation(context.Context, string, string, state.WorkloadCancelPrincipal) (state.WorkloadCancellation, error) {
	return state.WorkloadCancellation{}, a.err
}

func (a *auditFailStore) PrepareWorkloadRetry(ctx context.Context, customer, account, source string) (state.WorkloadRetryPreparation, error) {
	result, err := a.Store.PrepareWorkloadRetry(ctx, customer, account, source)
	if result.Audit != nil {
		return state.WorkloadRetryPreparation{}, a.err
	}
	return result, err
}

func (a *auditFailStore) ReserveWorkloadRetry(context.Context, *state.WorkloadRetryApproval, string, int64) (string, *state.AuditEvent, error) {
	return "", nil, a.err
}

// A cancel central cannot record is a cancel it does not perform.
func TestTenantCancelFailsClosedWhenTheJournalIsDown(t *testing.T) {
	store, accounts, _, reaper := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	putTenantWorkload(store, "wl_fc", "cust_console", "burst_fc", time.Now().UTC())
	accounts.Store = &auditFailStore{
		Store: store,
		err:   fmt.Errorf("%w: append audit: connection refused", state.ErrPersistence),
	}

	rec := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_fc", "human_alice", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("cancel with a dead journal = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if reaper.teardownCount("burst_fc") != 0 {
		t.Fatal("an unaudited cancel tore the burst down anyway")
	}
	wl, err := store.GetWorkload("wl_fc")
	if err != nil || wl.FinishedAt != nil {
		t.Fatalf("an unaudited cancel finished the workload: %+v err=%v", wl, err)
	}
}

// A human submission is stamped with the human, and the console can see who and
// under what rule — without the identity provider's key for that person.
func TestTenantSubmitStampsHumanProvenanceAndRendersIt(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"mem": state.RoleMember})
	mem, err := store.AccountByIdentity(testIssuer, "mem")
	if err != nil {
		t.Fatal(err)
	}

	created := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", "human_mem", tenantWorkloadYAML)
	if created.Code != http.StatusAccepted {
		t.Fatalf("create = %d (body %q)", created.Code, created.Body.String())
	}
	var response CreateWorkloadResponse
	if err := json.Unmarshal(created.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	stored, err := store.GetWorkload(response.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SubmittedBy == nil || stored.SubmittedBy.Kind != state.ActorHuman ||
		stored.SubmittedBy.AccountID != mem.ID {
		t.Fatalf("submitter = %+v, want the signed-in human", stored.SubmittedBy)
	}
	if stored.Authorization == nil || stored.Authorization.GrantedNamespace != "jobs" ||
		stored.Authorization.Rule == "" || stored.Authorization.RuleVersion == "" ||
		stored.Authorization.Role != state.RoleMember {
		t.Fatalf("authorization = %+v", stored.Authorization)
	}

	list := decodeTenantWorkloads(t, callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_mem", ""))
	if len(list.Workloads) != 1 {
		t.Fatalf("list = %+v", list.Workloads)
	}
	rendered := list.Workloads[0]
	if rendered.SubmittedBy == nil || rendered.SubmittedBy.Kind != state.ActorHuman ||
		rendered.SubmittedBy.AccountID != mem.ID {
		t.Fatalf("rendered submitter = %+v", rendered.SubmittedBy)
	}
	if rendered.Authorization == nil || rendered.Authorization.GrantedNamespace != "jobs" {
		t.Fatalf("rendered authorization = %+v", rendered.Authorization)
	}
	body := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_mem", "").Body.String()
	for _, secret := range []string{testIssuer, "ysk_cluster_secret", "env-secret", "arg-secret"} {
		if strings.Contains(body, secret) {
			t.Errorf("workload list leaked %q", secret)
		}
	}
}

// The cluster path is unchanged and is stamped as a cluster: no submitter in
// the context means the credential IS the tenant, which is the OSS shape.
func TestClusterSubmissionStampsClusterProvenance(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_cli", Token: "ysk_cli", Plan: "pro",
		MaxConcurrentBursts: 5, MaxHourlyUSD: 50, WorkloadNamespaces: []string{"jobs"}})
	store.AddAgent(&state.Agent{ID: "agent_cli", CustomerID: "cust_cli", ClusterID: "cluster_cli",
		Send: make(chan protocol.Envelope, 256)})
	wls := &Workloads{Store: store, Decider: &fakeDecider{}, Reaper: &fakeReaper{}, Log: quietLog()}

	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(tenantWorkloadYAML))
	rec := httptest.NewRecorder()
	Auth(store, http.HandlerFunc(wls.Create)).ServeHTTP(rec, withBearer(req, "ysk_cli"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("cluster create = %d (body %q)", rec.Code, rec.Body.String())
	}
	var response CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	stored, err := store.GetWorkload(response.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SubmittedBy == nil || stored.SubmittedBy.Kind != state.ActorCluster ||
		stored.SubmittedBy.AccountID != "" || stored.SubmittedBy.CustomerID != "cust_cli" {
		t.Fatalf("cluster submitter = %+v", stored.SubmittedBy)
	}
	if stored.Authorization == nil || stored.Authorization.Role != "" {
		t.Fatalf("a cluster credential holds no membership role: %+v", stored.Authorization)
	}
}

func withBearer(r *http.Request, token string) *http.Request {
	r.Header.Set("Authorization", "Bearer "+token)
	return r
}

// submitFixture is one tenant with a connected agent and the workload path
// wired to a scriptable journal, which is what the two post-Plan durable
// failures below need to be driven at all.
func submitFixture(t *testing.T, journal *failingJournal) (*state.Store, *Workloads, *fakeReaper) {
	t.Helper()
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_leak", Token: "ysk_leak", Plan: "pro",
		MaxConcurrentBursts: 5, MaxHourlyUSD: 50, WorkloadNamespaces: []string{"jobs"}})
	store.AddAgent(&state.Agent{ID: "agent_leak", CustomerID: "cust_leak", ClusterID: "cluster_leak",
		Send: make(chan protocol.Envelope, 256)})
	journal.Store = store
	reaper := &fakeReaper{}
	return store, &Workloads{
		Store: store, Decider: &fakeDecider{}, Reaper: reaper, Log: quietLog(), Journal: journal,
	}, reaper
}

func submitAsCluster(wls *Workloads, store *state.Store) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(tenantWorkloadYAML))
	rec := httptest.NewRecorder()
	Auth(store, http.HandlerFunc(wls.Create)).ServeHTTP(rec, withBearer(req, "ysk_leak"))
	return rec
}

// A submission that cannot be recorded after the node is already provisioned
// must not leak the node, and must not vanish either. The burst is reaped
// through the existing reap path, the caller gets a retryable 503 — and the id
// in that 503 still resolves, as a FAILED workload carrying the same provenance
// the accepted one would have had.
//
// Both post-Plan durable writes are covered because both strand the same node
// and both used to answer with an id GetWorkload could not find: a submitter
// watching a node they were charged for provision and disappear, with nothing
// to read afterwards.
func TestFailedSubmissionIsReapedAndStaysVisibleAsFailed(t *testing.T) {
	durable := fmt.Errorf("%w: connection refused", state.ErrPersistence)
	cases := []struct {
		name    string
		journal *failingJournal
	}{
		{"the burst booking is not durable", &failingJournal{burstErr: durable}},
		{"the audited submit is not durable", &failingJournal{submitErr: durable}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, wls, reaper := submitFixture(t, tc.journal)
			agent, err := store.AgentForCustomer("cust_leak")
			if err != nil {
				t.Fatal(err)
			}

			rec := submitAsCluster(wls, store)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("unrecordable submit = %d, want 503 (body %q)", rec.Code, rec.Body.String())
			}
			var response CreateWorkloadResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Status != "failed" || response.ID == "" {
				t.Fatalf("response = %+v; a submission that was not recorded is not accepted and still has an id", response)
			}

			// The node is gone.
			if !reaper.reaped("burst_1") {
				t.Fatal("the provisioned node was not reaped; a live VM leaked behind a submission nobody recorded")
			}
			if _, err := store.GetBurst("burst_1"); !errors.Is(err, state.ErrNotFound) {
				t.Error("the burst record survived the reap")
			}
			// Nothing was dispatched to the cluster: no announce, no job.
			if queued := len(agent.Send); queued != 0 {
				t.Fatalf("%d commands dispatched for a submission that failed to record", queued)
			}

			// The attempt is answerable, terminal, and attributable.
			wl, err := store.GetWorkload(response.ID)
			if err != nil {
				t.Fatalf("GetWorkload(%s) = %v; the id in the 503 must resolve", response.ID, err)
			}
			if wl.Status != "failed" || wl.FinishedAt == nil {
				t.Errorf("workload = %+v, want a terminal failed record", wl)
			}
			if wl.SubmittedBy == nil || wl.SubmittedBy.Kind != state.ActorCluster ||
				wl.SubmittedBy.CustomerID != "cust_leak" {
				t.Errorf("submitter = %+v; a failed attempt carries the same provenance an accepted one would",
					wl.SubmittedBy)
			}
			if wl.Authorization == nil || wl.Authorization.GrantedNamespace != "jobs" ||
				wl.Authorization.Rule == "" || wl.Authorization.RuleVersion == "" {
				t.Errorf("authorization = %+v", wl.Authorization)
			}
			list := store.WorkloadsForCustomer("cust_leak", 10)
			if len(list) != 1 || list[0].ID != response.ID || list[0].Status != "failed" {
				t.Errorf("tenant list = %+v, want the failed attempt", list)
			}
			// Nothing claims the submission was accepted. The bounded attempt row
			// itself rides FinishWorkload above — the store owns terminal
			// observations, and writes them best-effort so a journal that is down
			// cannot hold a teardown open; TestFinishWorkloadObservesEachTerminal
			// Status pins the row it writes.
			if got := tc.journal.recorded(state.ActionWorkloadSubmit); len(got) != 0 {
				t.Errorf("submit rows = %+v; a submission that never landed was recorded as one", got)
			}
		})
	}
}

// The healthy path is the control: the workload and the accepted decision land
// together, the node is not reaped, and the cluster gets its commands.
func TestRecordedSubmissionStoresTheWorkloadAndItsAcceptedDecision(t *testing.T) {
	journal := &failingJournal{}
	store, wls, reaper := submitFixture(t, journal)

	rec := submitAsCluster(wls, store)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create = %d, want 202 (body %q)", rec.Code, rec.Body.String())
	}
	var response CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	wl, err := store.GetWorkload(response.ID)
	if err != nil || wl.Status != "provisioning" || wl.FinishedAt != nil {
		t.Fatalf("workload = %+v err=%v, want a live provisioning record", wl, err)
	}
	accepted := journal.recorded(state.ActionWorkloadSubmit)
	if len(accepted) != 1 || accepted[0].Outcome != state.OutcomeAccepted ||
		accepted[0].TargetID != response.ID || accepted[0].Detail.BurstID != wl.BurstID {
		t.Fatalf("submit rows = %+v, want one accepted decision naming the workload", accepted)
	}
	if len(journal.recorded(state.ActionWorkloadFailed)) != 0 {
		t.Error("an accepted submission recorded a failed attempt")
	}
	if len(reaper.teardowns) != 0 {
		t.Error("an accepted submission reaped its own burst")
	}
}

// The watchdog's own expiry row is the tenant's, names the burst, and is
// written AFTER the reap and the failed observation — a journal that is down
// must never keep a burst billing, so it is the last thing that happens and it
// is best-effort. The failed observation's own label is pinned in the state
// package (TestFinishWorkloadObservesEachTerminalStatus); what this asserts is
// that the watchdog still adds its explicit expired row on top of it.
func TestExpiredBurstIsJournaledAfterTheReap(t *testing.T) {
	store := state.New()
	journal := &failingJournal{Store: store}
	reaper := &fakeReaper{}
	h := &Workloads{Store: store, Reaper: reaper, Log: quietLog(), Journal: journal}

	old := time.Now().Add(-2 * time.Hour).UTC()
	if err := store.PutBurst(&state.Burst{ID: "burst_exp", CustomerID: "cust_exp", Backend: "linode",
		CreatedAt: old, Status: "provisioning", HourlyUSD: 0.62, Deadline: time.Hour}); err != nil {
		t.Fatal(err)
	}
	store.PutWorkload(&state.Workload{ID: "wl_exp", CustomerID: "cust_exp", BurstID: "burst_exp",
		Status: "provisioning", CreatedAt: old})

	h.sweepExpiredBursts(context.Background(), 0, 0)

	if !reaper.reaped("burst_exp") {
		t.Fatal("the over-budget burst was not reaped")
	}
	wl, err := store.GetWorkload("wl_exp")
	if err != nil || wl.Status != "failed" || wl.FinishedAt == nil {
		t.Fatalf("workload = %+v err=%v, want failed", wl, err)
	}
	expired := journal.recorded(state.ActionWorkloadExpired)
	if len(expired) != 1 {
		t.Fatalf("expired rows = %+v, want exactly one", expired)
	}
	if expired[0].Outcome != state.OutcomeObserved || expired[0].Actor.Kind != state.ActorSystem ||
		expired[0].CustomerID != "cust_exp" || expired[0].TargetKind != state.TargetTenant ||
		expired[0].Detail.Status != "failed" || expired[0].Detail.BurstID != "burst_exp" {
		t.Errorf("expired row = %+v / %+v", expired[0], expired[0].Detail)
	}
}

// An accepted cancel whose burst is already gone still has to leave a terminal
// workload. The decision is journaled either way — that is what the accepted row
// records — but without this the workload it names reads "provisioning" forever
// and no observation ever says otherwise.
func TestAcceptedCancelWithNoBurstStillFinishesTheWorkload(t *testing.T) {
	store, accounts, _, reaper := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	putTenantWorkload(store, "wl_gone", "cust_console", "burst_gone", time.Now().UTC())
	// Whoever reaped it did not reach this replica's workload copy — a
	// cross-replica reap writes only the durable row.
	if err := store.DeleteBurst("burst_gone"); err != nil {
		t.Fatal(err)
	}
	// The fixture models confirmed teardown on another replica, not merely a
	// missing booking. Preserve that proof while keeping this workload stale.
	if _, err := store.RecordBurstReap(context.Background(), "burst_gone", "cust_console"); err != nil {
		t.Fatal(err)
	}

	rec := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_gone", "human_alice", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "already_reaped") {
		t.Fatalf("cancel of an already-reaped burst = %d (body %q), want 200 already_reaped", rec.Code, rec.Body.String())
	}
	// Terminal, and terminal as CANCELLED — which is the status that carries the
	// workload.cancelled observation (pinned in the state package).
	wl, err := store.GetWorkload("wl_gone")
	if err != nil || wl.Status != "cancelled" || wl.FinishedAt == nil {
		t.Fatalf("workload = %+v err=%v, want a terminal cancelled record", wl, err)
	}
	if reaper.teardownCount("burst_gone") != 0 {
		t.Error("a burst that was already gone was torn down again")
	}

	// Idempotent, and it does not overwrite what actually happened: a repeat
	// cancel of a workload that finished as succeeded leaves it succeeded.
	finished := time.Now().UTC()
	putTenantWorkload(store, "wl_done", "cust_console", "burst_done", finished)
	if err := store.DeleteBurst("burst_done"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordBurstReap(context.Background(), "burst_done", "cust_console"); err != nil {
		t.Fatal(err)
	}
	store.FinishWorkload("wl_done", "succeeded", finished, false)
	if code := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_done", "human_alice", "").Code; code != http.StatusOK {
		t.Fatalf("cancel of a finished workload = %d, want 200", code)
	}
	done, err := store.GetWorkload("wl_done")
	if err != nil || done.Status != "succeeded" {
		t.Fatalf("workload = %+v err=%v; a late cancel rewrote a terminal status", done, err)
	}
}

// A viewer's submit and cancel are authenticated authorization decisions, so
// they are journaled like a member's — and a journal that will not take the
// denial refuses the request rather than answering an unrecorded 403.
func TestViewerDenialsAreJournaledAndHaveNoSideEffects(t *testing.T) {
	store, accounts, decider, reaper := tenantWorkloadFixture(t, map[string]string{"view": state.RoleViewer})
	putTenantWorkload(store, "wl_v", "cust_console", "burst_v", time.Now().UTC())
	real := accounts.Store
	spy := &recordingAuditStore{accountStore: real}
	accounts.Store = spy

	create := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", "human_view", tenantWorkloadYAML)
	if create.Code != http.StatusForbidden {
		t.Fatalf("viewer submit = %d, want 403 (body %q)", create.Code, create.Body.String())
	}
	cancel := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_v", "human_view", "")
	if cancel.Code != http.StatusForbidden {
		t.Fatalf("viewer cancel = %d, want 403 (body %q)", cancel.Code, cancel.Body.String())
	}

	// Both denials are on the record, with the reason that decided them.
	submit := spy.recorded(state.ActionWorkloadSubmit)
	if len(submit) != 1 || submit[0].Outcome != state.OutcomeDenied ||
		submit[0].Detail.Reason != state.ReasonRoleReadOnly || submit[0].Detail.Role != state.RoleViewer {
		t.Errorf("submit denial rows = %+v", submit)
	}
	if len(submit) == 1 && (submit[0].Actor.Kind != state.ActorHuman || submit[0].Actor.AccountID == "") {
		t.Errorf("submit denial actor = %+v, want the signed-in human", submit[0].Actor)
	}
	cancelRows := spy.recorded(state.ActionWorkloadCancel)
	if len(cancelRows) != 1 || cancelRows[0].Outcome != state.OutcomeDenied ||
		cancelRows[0].Detail.Reason != state.ReasonRoleReadOnly || cancelRows[0].TargetID != "wl_v" {
		t.Errorf("cancel denial rows = %+v", cancelRows)
	}

	// And nothing moved: no plan, no teardown, no change to the workload.
	if decider.calls != 0 || reaper.teardownCount("burst_v") != 0 {
		t.Fatalf("a viewer reached the lifecycle: plans=%d teardowns=%d", decider.calls, reaper.teardownCount("burst_v"))
	}
	wl, err := store.GetWorkload("wl_v")
	if err != nil || wl.Status != "running" || wl.FinishedAt != nil {
		t.Fatalf("a refused viewer cancel changed the workload: %+v err=%v", wl, err)
	}
	// Reads are still theirs.
	if code := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_view", "").Code; code != http.StatusOK {
		t.Errorf("viewer list = %d, want 200", code)
	}
	if code := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_v", "human_view", "").Code; code != http.StatusOK {
		t.Errorf("viewer get = %d, want 200", code)
	}

	// With the journal down neither denial can be recorded, so neither is
	// answered as a decision: 503, and still no side effects.
	accounts.Store = &auditFailStore{
		Store: store,
		err:   fmt.Errorf("%w: append audit: connection refused", state.ErrPersistence),
	}
	if code := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", "human_view", tenantWorkloadYAML).Code; code != http.StatusServiceUnavailable {
		t.Errorf("unrecordable viewer submit denial = %d, want 503", code)
	}
	if code := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_v", "human_view", "").Code; code != http.StatusServiceUnavailable {
		t.Errorf("unrecordable viewer cancel denial = %d, want 503", code)
	}
	if decider.calls != 0 || reaper.teardownCount("burst_v") != 0 {
		t.Fatalf("an unrecordable denial reached the lifecycle: plans=%d teardowns=%d", decider.calls, reaper.teardownCount("burst_v"))
	}
}

// recordingAuditStore keeps the journal rows the human surface writes, which
// the in-memory store accepts and discards (it has no durable backend).
type recordingAuditStore struct {
	accountStore
	mu     sync.Mutex
	events []state.AuditEvent
}

func (r *recordingAuditStore) AppendAudit(ev *state.AuditEvent) error {
	r.mu.Lock()
	if ev != nil {
		r.events = append(r.events, *ev)
	}
	r.mu.Unlock()
	return r.accountStore.AppendAudit(ev)
}

func (r *recordingAuditStore) RequestWorkloadCancellation(ctx context.Context, customer, workload string, principal state.WorkloadCancelPrincipal) (state.WorkloadCancellation, error) {
	result, err := r.accountStore.RequestWorkloadCancellation(ctx, customer, workload, principal)
	if err == nil && result.Audit != nil {
		r.mu.Lock()
		r.events = append(r.events, *result.Audit)
		r.mu.Unlock()
	}
	return result, err
}

func (r *recordingAuditStore) recorded(action string) []state.AuditEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []state.AuditEvent
	for _, ev := range r.events {
		if ev.Action == action {
			out = append(out, ev)
		}
	}
	return out
}

func (r *recordingAuditStore) PrepareWorkloadRetry(ctx context.Context, customer, account, source string) (state.WorkloadRetryPreparation, error) {
	result, err := r.accountStore.PrepareWorkloadRetry(ctx, customer, account, source)
	if err == nil && result.Audit != nil {
		r.mu.Lock()
		r.events = append(r.events, *result.Audit)
		r.mu.Unlock()
	}
	return result, err
}

func (r *recordingAuditStore) ReserveWorkloadRetry(ctx context.Context, approval *state.WorkloadRetryApproval, workloadID string, rate int64) (string, *state.AuditEvent, error) {
	id, audit, err := r.accountStore.ReserveWorkloadRetry(ctx, approval, workloadID, rate)
	// A non-nil audit is returned only after commit, including a business
	// refusal. Do not append it to the real store a second time.
	if audit != nil {
		r.mu.Lock()
		r.events = append(r.events, *audit)
		r.mu.Unlock()
	}
	return id, audit, err
}

// A denied submit is journaled too, and a journal that will not take the
// refusal turns the 403 into a retryable 503 rather than an unrecorded
// authorization decision.
func TestDeniedSubmitIsAuditedAndFailsClosed(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_ns", Token: "ysk_ns", Plan: "pro",
		MaxConcurrentBursts: 5, MaxHourlyUSD: 50, WorkloadNamespaces: []string{"team-a"}})
	store.AddAgent(&state.Agent{ID: "agent_ns", CustomerID: "cust_ns", ClusterID: "cluster_ns",
		Send: make(chan protocol.Envelope, 256)})
	decider := &fakeDecider{}

	// The healthy journal (the in-memory store's no-op) answers 403.
	wls := &Workloads{Store: store, Decider: decider, Reaper: &fakeReaper{}, Log: quietLog()}
	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(tenantWorkloadYAML))
	rec := httptest.NewRecorder()
	Auth(store, http.HandlerFunc(wls.Create)).ServeHTTP(rec, withBearer(req, "ysk_ns"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("submission into an unauthorized namespace = %d, want 403 (body %q)", rec.Code, rec.Body.String())
	}

	// With the journal down the refusal cannot be recorded, so it is answered
	// as retryable instead — and nothing was provisioned either way.
	wls = &Workloads{
		Store: store, Decider: decider, Reaper: &fakeReaper{}, Log: quietLog(),
		Journal: &failingJournal{
			Store:     store,
			appendErr: fmt.Errorf("%w: append audit: connection refused", state.ErrPersistence),
		},
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/workloads", strings.NewReader(tenantWorkloadYAML))
	rec = httptest.NewRecorder()
	Auth(store, http.HandlerFunc(wls.Create)).ServeHTTP(rec, withBearer(req, "ysk_ns"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unrecordable refusal = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if decider.calls != 0 {
		t.Fatal("a refused submission reached the decider")
	}
}
