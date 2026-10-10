package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// auditFixture builds a store with a recording persister, one tenant, and the
// named humans bound to it — the shape every test below needs before it can
// decide anything about a workload or a roster.
func auditFixture(t *testing.T, customerID string, roles map[string]string) (*Store, *accountSpyPersister, map[string]string) {
	t.Helper()
	s, spy := storeWithSpy(customerID)
	ids := make(map[string]string, len(roles))
	for sub, role := range roles {
		acct, err := s.UpsertAccount("https://id.yscale.sh", sub, AccountProfile{Email: sub + "@acme.com"})
		if err != nil {
			t.Fatalf("UpsertAccount %s: %v", sub, err)
		}
		if _, _, err := s.GrantTenantMembership(acct.ID, customerID, role, OperatorActor()); err != nil {
			t.Fatalf("grant %s: %v", sub, err)
		}
		ids[sub] = acct.ID
	}
	return s, spy, ids
}

func submitEvent(customerID, workloadID string, by Actor) *AuditEvent {
	return NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionWorkloadSubmit,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetWorkload,
		TargetID:   workloadID,
	})
}

// A submission records WHO made it and what allowed it, and both survive the
// JSONB encoding the durable backend uses. A cluster credential and a human are
// stamped as different principals — that distinction is the whole basis of the
// cancel matrix below.
func TestSubmitWorkloadStampsProvenanceForHumanAndCluster(t *testing.T) {
	s, spy, ids := auditFixture(t, "cust_p", map[string]string{"alice": RoleMember})

	human := &Workload{
		ID: "wl_human", CustomerID: "cust_p", Status: "provisioning", CreatedAt: time.Now().UTC(),
		SubmittedBy: &Actor{Kind: ActorHuman, AccountID: ids["alice"], CustomerID: "cust_p"},
		Authorization: &WorkloadAuthorization{
			RequestedNamespace: "jobs", GrantedNamespace: "jobs",
			Rule: "tenant.workload_namespaces", RuleVersion: "v1", Role: RoleMember,
		},
	}
	if err := s.SubmitWorkload(human, submitEvent("cust_p", human.ID, HumanActor(ids["alice"], "cust_p"))); err != nil {
		t.Fatalf("SubmitWorkload (human): %v", err)
	}
	cluster := &Workload{
		ID: "wl_cluster", CustomerID: "cust_p", Status: "provisioning", CreatedAt: time.Now().UTC(),
		SubmittedBy:   &Actor{Kind: ActorCluster, CustomerID: "cust_p"},
		Authorization: &WorkloadAuthorization{GrantedNamespace: "default", Rule: "tenant.workload_namespaces", RuleVersion: "v1"},
	}
	if err := s.SubmitWorkload(cluster, submitEvent("cust_p", cluster.ID, ClusterActor("cust_p"))); err != nil {
		t.Fatalf("SubmitWorkload (cluster): %v", err)
	}

	got, err := s.GetWorkload("wl_human")
	if err != nil {
		t.Fatal(err)
	}
	if got.SubmittedBy == nil || got.SubmittedBy.Kind != ActorHuman || got.SubmittedBy.AccountID != ids["alice"] {
		t.Fatalf("human provenance = %+v", got.SubmittedBy)
	}
	if got.Authorization == nil || got.Authorization.GrantedNamespace != "jobs" || got.Authorization.RuleVersion != "v1" {
		t.Fatalf("human authorization = %+v", got.Authorization)
	}
	got, err = s.GetWorkload("wl_cluster")
	if err != nil {
		t.Fatal(err)
	}
	if got.SubmittedBy == nil || got.SubmittedBy.Kind != ActorCluster || got.SubmittedBy.AccountID != "" {
		t.Fatalf("cluster provenance = %+v; a cluster credential is never a human", got.SubmittedBy)
	}

	// Exactly one journal row per submission, each naming its own principal.
	rows := spy.eventsWith(ActionWorkloadSubmit)
	if len(rows) != 2 {
		t.Fatalf("submit audit rows = %d, want 2", len(rows))
	}
	kinds := map[string]string{}
	for _, ev := range rows {
		kinds[ev.TargetID] = ev.Actor.Kind
	}
	if kinds["wl_human"] != ActorHuman || kinds["wl_cluster"] != ActorCluster {
		t.Fatalf("audit actors = %v", kinds)
	}
}

// A workload written before provenance existed stays fully readable: nothing
// about the record requires the new fields, and a store that refused to serve
// one would break every tenant's history at the moment this shipped.
func TestLegacyWorkloadWithoutProvenanceStaysReadable(t *testing.T) {
	s, _ := storeWithSpy("cust_legacy")
	s.PutWorkload(&Workload{ID: "wl_old", CustomerID: "cust_legacy", Status: "running", CreatedAt: time.Now().UTC()})

	got, err := s.GetWorkload("wl_old")
	if err != nil {
		t.Fatalf("GetWorkload on a legacy record: %v", err)
	}
	if got.SubmittedBy != nil || got.Authorization != nil {
		t.Fatalf("legacy record grew provenance from nowhere: %+v", got)
	}
	if list := s.WorkloadsForCustomer("cust_legacy", 10); len(list) != 1 || list[0].ID != "wl_old" {
		t.Fatalf("legacy record missing from the tenant list: %+v", list)
	}
}

// A submission whose durable record — workload plus the decision that admitted
// it — cannot be written is not published at all. Create reaps the burst it
// already provisioned off the back of that error, so a workload visible in
// memory here would be one nobody could account for and nobody would tear down.
func TestSubmitWorkloadPublishesNothingWhenTheAuditCannotBeWritten(t *testing.T) {
	s, spy := storeWithSpy("cust_fc")
	spy.appendErr = errors.New("postgres down")

	rec := &Workload{ID: "wl_unaudited", CustomerID: "cust_fc", Status: "provisioning", CreatedAt: time.Now().UTC(),
		SubmittedBy: &Actor{Kind: ActorCluster, CustomerID: "cust_fc"}}
	err := s.SubmitWorkload(rec, submitEvent("cust_fc", rec.ID, ClusterActor("cust_fc")))
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("SubmitWorkload error = %v, want ErrPersistence", err)
	}
	if _, err := s.GetWorkload("wl_unaudited"); !errors.Is(err, ErrNotFound) {
		t.Fatal("an unaudited submission was published to the working set")
	}
	if len(spy.events()) != 0 {
		t.Fatalf("a refused append still recorded rows: %+v", spy.events())
	}
}

// The cancel matrix, decided from one snapshot: an owner and an admin may
// cancel anything of their tenant's; a member only what they submitted; a
// viewer nothing; and a legacy or cluster-submitted workload is never a
// member's, because nothing recorded that it was.
func TestAuthorizeWorkloadCancelMatrix(t *testing.T) {
	s, _, ids := auditFixture(t, "cust_m", map[string]string{
		"alice": RoleOwner, "adm": RoleAdmin, "mem": RoleMember, "other": RoleMember, "view": RoleViewer,
	})
	put := func(id string, by *Actor) {
		s.PutWorkload(&Workload{ID: id, CustomerID: "cust_m", Status: "running",
			BurstID: "burst_" + id, CreatedAt: time.Now().UTC(), SubmittedBy: by})
	}
	memActor := Actor{Kind: ActorHuman, AccountID: ids["mem"], CustomerID: "cust_m"}
	put("wl_mem", &memActor)
	put("wl_legacy", nil)
	put("wl_cluster", &Actor{Kind: ActorCluster, CustomerID: "cust_m"})

	cases := []struct {
		caller, workload string
		allowed          bool
		reason           string
	}{
		{"mem", "wl_mem", true, ReasonSubmitterMatch},
		{"other", "wl_mem", false, ReasonNotSubmitter},
		{"mem", "wl_legacy", false, ReasonNotSubmitter},
		{"mem", "wl_cluster", false, ReasonNotSubmitter},
		{"alice", "wl_mem", true, ReasonRoleAuthorized},
		{"alice", "wl_legacy", true, ReasonRoleAuthorized},
		{"alice", "wl_cluster", true, ReasonRoleAuthorized},
		{"adm", "wl_legacy", true, ReasonRoleAuthorized},
		{"adm", "wl_cluster", true, ReasonRoleAuthorized},
		{"view", "wl_mem", false, ReasonRoleReadOnly},
	}
	for _, tc := range cases {
		decision, err := s.AuthorizeWorkloadCancel("cust_m", ids[tc.caller], tc.workload)
		if err != nil {
			t.Fatalf("%s on %s: %v", tc.caller, tc.workload, err)
		}
		if decision.Allowed != tc.allowed || decision.Reason != tc.reason {
			t.Errorf("%s on %s = allowed %v reason %q, want %v/%q",
				tc.caller, tc.workload, decision.Allowed, decision.Reason, tc.allowed, tc.reason)
		}
	}
}

// Every way a caller has no business with a workload is ONE answer. A non-member
// must not learn that a tenant exists, and a member of one tenant must not learn
// that another tenant's workload id does.
func TestAuthorizeWorkloadCancelCollapsesMisses(t *testing.T) {
	s, _, ids := auditFixture(t, "cust_a", map[string]string{"alice": RoleOwner})
	s.AddCustomer(&Customer{ID: "cust_b", Token: "tok_b", Plan: "pro"})
	s.PutWorkload(&Workload{ID: "wl_a", CustomerID: "cust_a", Status: "running", CreatedAt: time.Now().UTC()})
	s.PutWorkload(&Workload{ID: "wl_b", CustomerID: "cust_b", Status: "running", CreatedAt: time.Now().UTC()})

	misses := []struct{ tenant, account, workload string }{
		{"cust_a", ids["alice"], "wl_missing"}, // unknown workload
		{"cust_a", ids["alice"], "wl_b"},       // another tenant's workload
		{"cust_b", ids["alice"], "wl_b"},       // caller is not a member there
		{"cust_missing", ids["alice"], "wl_a"}, // unknown tenant
		{"cust_a", "acct_stranger", "wl_a"},    // unknown account
	}
	for _, m := range misses {
		if _, err := s.AuthorizeWorkloadCancel(m.tenant, m.account, m.workload); !errors.Is(err, ErrNotFound) {
			t.Errorf("%+v: error = %v, want ErrNotFound", m, err)
		}
	}
}

func TestAuthorizeWorkloadReadAllowsEveryTenantRoleAndReturnsCopy(t *testing.T) {
	s, _, ids := auditFixture(t, "cust_logs", map[string]string{
		"owner": RoleOwner, "admin": RoleAdmin, "member": RoleMember, "viewer": RoleViewer,
	})
	s.PutWorkload(&Workload{ID: "wl_logs", CustomerID: "cust_logs", ClusterID: "cluster-a", SpecYAML: []byte("secret")})
	for _, caller := range []string{"owner", "admin", "member", "viewer"} {
		decision, err := s.AuthorizeWorkloadRead("cust_logs", ids[caller], "wl_logs")
		if err != nil {
			t.Fatalf("%s read: %v", caller, err)
		}
		if decision.Role == "" || decision.Source.ClusterID != "cluster-a" {
			t.Fatalf("%s decision = %+v", caller, decision)
		}
		decision.Source.SpecYAML[0] = 'X'
		stored, _ := s.GetWorkload("wl_logs")
		if string(stored.SpecYAML) != "secret" {
			t.Fatal("read decision exposed the stored workload slice")
		}
	}
}

func TestAuthorizeWorkloadReadCollapsesCrossTenantAndUnknown(t *testing.T) {
	s, _, ids := auditFixture(t, "cust_logs", map[string]string{"owner": RoleOwner})
	s.AddCustomer(&Customer{ID: "cust_other", Token: "tok_other"})
	s.PutWorkload(&Workload{ID: "wl_other", CustomerID: "cust_other"})
	for _, workloadID := range []string{"wl_missing", "wl_other"} {
		if _, err := s.AuthorizeWorkloadRead("cust_logs", ids["owner"], workloadID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s error = %v, want ErrNotFound", workloadID, err)
		}
	}
}

// Terminal cleanup never waits on the journal. A dead audit backend must not be
// able to keep a burst billing, so the lifecycle write still lands and only the
// observation is lost.
func TestTerminalObservationsDoNotStrandCleanupWhenAuditFails(t *testing.T) {
	s, spy := storeWithSpy("cust_t")
	s.PutWorkload(&Workload{ID: "wl_t", CustomerID: "cust_t", Status: "provisioning",
		BurstID: "burst_t", CreatedAt: time.Now().UTC()})
	spy.appendErr = errors.New("postgres down")

	if !s.StartWorkload("wl_t", time.Now().UTC()) {
		t.Fatal("StartWorkload refused while the journal was down")
	}
	if !s.FinishWorkload("wl_t", "succeeded", time.Now().UTC(), true) {
		t.Fatal("FinishWorkload refused while the journal was down")
	}
	got, err := s.GetWorkload("wl_t")
	if err != nil || got.FinishedAt == nil || got.Status != "succeeded" {
		t.Fatalf("workload not finished: %+v err=%v", got, err)
	}
	if len(spy.events()) != 0 {
		t.Fatalf("a refused append still recorded rows: %+v", spy.events())
	}
}

// The lifecycle observations that DO land use the closed action set, so a
// reader can filter on them rather than parse a status string.
func TestLifecycleObservationsUseClosedActions(t *testing.T) {
	s, spy := storeWithSpy("cust_o")
	s.PutWorkload(&Workload{ID: "wl_o", CustomerID: "cust_o", Status: "provisioning",
		BurstID: "burst_o", CreatedAt: time.Now().UTC()})

	s.StartWorkload("wl_o", time.Now().UTC())
	s.FinishWorkload("wl_o", "cancelled", time.Now().UTC(), false)

	if got := len(spy.eventsWith(ActionWorkloadStarted)); got != 1 {
		t.Errorf("started observations = %d, want 1", got)
	}
	cancelled := spy.eventsWith(ActionWorkloadCancelled)
	if len(cancelled) != 1 {
		t.Fatalf("cancelled observations = %d, want 1", len(cancelled))
	}
	if cancelled[0].Outcome != OutcomeObserved || cancelled[0].Actor.Kind != ActorSystem {
		t.Errorf("observation = %+v; a lifecycle event is the system observing, not a request", cancelled[0])
	}
	if cancelled[0].Detail.BurstID != "burst_o" || cancelled[0].Detail.Status != "cancelled" {
		t.Errorf("observation detail = %+v", cancelled[0].Detail)
	}
}

// Each terminal status gets the action that MEANS it. A failed workload is the
// case worth pinning: folding it into workload.completed made the journal say a
// job succeeded when a budget overrun, a dispatch failure or a nonzero exit
// ended it, and the journal cannot be corrected afterwards.
func TestFinishWorkloadObservesEachTerminalStatus(t *testing.T) {
	cases := []struct {
		status string
		want   string
	}{
		{"succeeded", ActionWorkloadCompleted},
		{"failed", ActionWorkloadFailed},
		{"cancelled", ActionWorkloadCancelled},
	}
	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			s, spy := storeWithSpy("cust_term")
			s.PutWorkload(&Workload{ID: "wl_t", CustomerID: "cust_term", Status: "running",
				BurstID: "burst_t", CreatedAt: time.Now().UTC()})

			if !s.FinishWorkload("wl_t", tc.status, time.Now().UTC(), false) {
				t.Fatal("FinishWorkload did not apply")
			}
			rows := spy.eventsWith(tc.want)
			if len(rows) != 1 {
				t.Fatalf("%s rows = %d, want 1 (all rows: %+v)", tc.want, len(rows), spy.events())
			}
			if rows[0].Outcome != OutcomeObserved || rows[0].Actor.Kind != ActorSystem ||
				rows[0].Detail.Status != tc.status || rows[0].Detail.BurstID != "burst_t" {
				t.Errorf("observation = %+v / %+v", rows[0], rows[0].Detail)
			}
			// And nothing else was filed under another verb.
			if got := len(spy.events()); got != 1 {
				t.Errorf("rows = %+v, want only the %s observation", spy.events(), tc.want)
			}
		})
	}
}

// A terminal status the closed action set has no word for writes NO row rather
// than one under a word that means something else — and the workload still
// finishes, because cleanup never waits on the journal.
func TestUnknownTerminalStatusIsNotFiledUnderAKnownAction(t *testing.T) {
	s, spy := storeWithSpy("cust_unk")
	s.PutWorkload(&Workload{ID: "wl_u", CustomerID: "cust_unk", Status: "running",
		BurstID: "burst_u", CreatedAt: time.Now().UTC()})

	if !s.FinishWorkload("wl_u", "evicted", time.Now().UTC(), false) {
		t.Fatal("an unrecognised status must still finish the workload")
	}
	got, err := s.GetWorkload("wl_u")
	if err != nil || got.FinishedAt == nil || got.Status != "evicted" {
		t.Fatalf("workload = %+v err=%v", got, err)
	}
	if rows := spy.events(); len(rows) != 0 {
		t.Fatalf("rows = %+v; an unknown status was filed under a closed action", rows)
	}
	if action := auditActionForStatus("evicted"); action != "" {
		t.Errorf("auditActionForStatus(evicted) = %q, want the empty refusal", action)
	}
}

// An idempotent no-op writes no journal row, and a real change writes exactly
// one. A journal that recorded a grant on every retry could not answer when
// access was actually given, which is the only question it exists for.
func TestIdempotentRosterAndNamespaceChangesAuditExactlyOnce(t *testing.T) {
	s, spy, ids := auditFixture(t, "cust_i", map[string]string{"alice": RoleOwner, "mem": RoleMember})
	// The fixture's own grants are real changes: two humans, two rows.
	if got := len(spy.eventsWith(ActionMembershipGrant)); got != 2 {
		t.Fatalf("grant rows after fixture = %d, want 2", got)
	}

	// Re-granting the same role changes nothing.
	if _, created, err := s.GrantTenantMembership(ids["mem"], "cust_i", RoleMember, OperatorActor()); err != nil || created {
		t.Fatalf("re-grant: created=%v err=%v", created, err)
	}
	if got := len(spy.eventsWith(ActionMembershipGrant)); got != 2 {
		t.Errorf("grant rows after an idempotent re-grant = %d, want 2", got)
	}

	// Setting the role a member already holds changes nothing.
	if _, previous, err := s.SetTenantMembershipRole(ids["mem"], "cust_i", RoleMember, OperatorActor()); err != nil || previous != RoleMember {
		t.Fatalf("no-op role set: previous=%q err=%v", previous, err)
	}
	if got := len(spy.eventsWith(ActionMembershipRoleChange)); got != 0 {
		t.Errorf("role-change rows after a no-op = %d, want 0", got)
	}
	// A real role change writes exactly one.
	if _, previous, err := s.SetTenantMembershipRole(ids["mem"], "cust_i", RoleAdmin, OperatorActor()); err != nil || previous != RoleMember {
		t.Fatalf("real role change: previous=%q err=%v", previous, err)
	}
	changes := spy.eventsWith(ActionMembershipRoleChange)
	if len(changes) != 1 {
		t.Fatalf("role-change rows = %d, want 1", len(changes))
	}
	if changes[0].Detail.PreviousRole != RoleMember || changes[0].Detail.Role != RoleAdmin ||
		changes[0].Actor.Kind != ActorOperator || changes[0].TargetID != ids["mem"] {
		t.Errorf("role-change row = %+v / %+v", changes[0], changes[0].Detail)
	}

	// Removing an absent grant revokes no access and records none.
	if removed, err := s.DeleteTenantMembership("acct_nobody", "cust_i", OperatorActor()); err != nil || removed != nil {
		t.Fatalf("absent removal: removed=%+v err=%v", removed, err)
	}
	if got := len(spy.eventsWith(ActionMembershipRemove)); got != 0 {
		t.Errorf("removal rows after a no-op = %d, want 0", got)
	}
	if _, err := s.DeleteTenantMembership(ids["mem"], "cust_i", OperatorActor()); err != nil {
		t.Fatalf("real removal: %v", err)
	}
	if got := len(spy.eventsWith(ActionMembershipRemove)); got != 1 {
		t.Errorf("removal rows = %d, want 1", got)
	}

	// The namespace set: the same list again is not a boundary change.
	if _, err := s.SetCustomerWorkloadNamespaces("cust_i", []string{"team-a"}, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetCustomerWorkloadNamespaces("cust_i", []string{"team-a"}, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	ns := spy.eventsWith(ActionWorkloadNamespacesSet)
	if len(ns) != 1 {
		t.Fatalf("namespace rows = %d, want 1 (the second call set the same list)", len(ns))
	}
	if len(ns[0].Detail.Namespaces) != 1 || ns[0].Detail.Namespaces[0] != "team-a" {
		t.Errorf("namespace row detail = %+v", ns[0].Detail)
	}
}

// An authorizing mutation whose journal row cannot be written does not happen.
// Membership and the namespace boundary both roll back to what they were, so
// there is no window in which access is wider than the evidence.
func TestAuthorizingMutationsFailClosedOnAuditFailure(t *testing.T) {
	s, spy, ids := auditFixture(t, "cust_fc", map[string]string{"alice": RoleOwner, "mem": RoleMember})
	acct, err := s.UpsertAccount("https://id.yscale.sh", "newbie", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	// The fixture's own grants are already journaled; what must not grow is the
	// count from here on.
	before := len(spy.events())
	spy.appendErr = errors.New("postgres down")

	if _, _, err := s.GrantTenantMembership(acct.ID, "cust_fc", RoleMember, OperatorActor()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("grant error = %v, want ErrPersistence", err)
	}
	if _, err := s.MembershipFor(acct.ID, "cust_fc"); !errors.Is(err, ErrNotFound) {
		t.Error("a grant that could not be audited was published anyway")
	}
	if _, _, err := s.SetTenantMembershipRole(ids["mem"], "cust_fc", RoleAdmin, OperatorActor()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("role change error = %v, want ErrPersistence", err)
	}
	if m, err := s.MembershipFor(ids["mem"], "cust_fc"); err != nil || m.Role != RoleMember {
		t.Errorf("role rolled forward on a refused audit: %+v err=%v", m, err)
	}
	if _, err := s.RemoveTenantMembership("cust_fc", ids["alice"], ids["mem"], HumanActor(ids["alice"], "cust_fc")); !errors.Is(err, ErrPersistence) {
		t.Fatalf("removal error = %v, want ErrPersistence", err)
	}
	if _, err := s.MembershipFor(ids["mem"], "cust_fc"); err != nil {
		t.Error("a removal that could not be audited took the grant anyway")
	}
	if _, err := s.SetCustomerWorkloadNamespaces("cust_fc", []string{"team-a"}, OperatorActor()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("namespace error = %v, want ErrPersistence", err)
	}
	c, err := s.CustomerByID("cust_fc")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.WorkloadNamespaces) != 0 {
		t.Errorf("the namespace boundary widened without evidence: %v", c.WorkloadNamespaces)
	}
	if got := len(spy.events()); got != before {
		t.Fatalf("refused appends recorded %d new rows", got-before)
	}
}

// The read: only a tenant's own managers, only that tenant's rows.
func TestTenantAuditForRoleMatrixAndTenantIsolation(t *testing.T) {
	s, _, ids := auditFixture(t, "cust_x", map[string]string{
		"alice": RoleOwner, "adm": RoleAdmin, "mem": RoleMember, "view": RoleViewer,
	})
	// A second tenant with its own owner and its own history.
	s.AddCustomer(&Customer{ID: "cust_y", Token: "tok_y", Plan: "pro"})
	bob, err := s.UpsertAccount("https://id.yscale.sh", "bob", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GrantTenantMembership(bob.ID, "cust_y", RoleOwner, OperatorActor()); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for _, caller := range []string{"mem", "view"} {
		if _, err := s.TenantAuditFor(ctx, "cust_x", ids[caller], AuditQuery{}); !errors.Is(err, ErrNotAuthorized) {
			t.Errorf("%s reading the journal: err = %v, want ErrNotAuthorized", caller, err)
		}
	}
	for _, caller := range []string{"alice", "adm"} {
		if _, err := s.TenantAuditFor(ctx, "cust_x", ids[caller], AuditQuery{}); err != nil {
			t.Errorf("%s reading the journal: %v", caller, err)
		}
	}
	// A non-member, an unknown tenant and a revoked one are one answer.
	if _, err := s.TenantAuditFor(ctx, "cust_y", ids["alice"], AuditQuery{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("non-member read: err = %v, want ErrNotFound", err)
	}
	if _, err := s.TenantAuditFor(ctx, "cust_missing", ids["alice"], AuditQuery{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown tenant read: err = %v, want ErrNotFound", err)
	}

	// cust_x's owner sees cust_x's grants and none of cust_y's.
	page, err := s.TenantAuditFor(ctx, "cust_x", ids["alice"], AuditQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 4 {
		t.Fatalf("cust_x journal = %d rows, want the 4 grants that built it", len(page.Events))
	}
	for _, ev := range page.Events {
		if ev.CustomerID != "cust_x" {
			t.Fatalf("another tenant's row reached this read: %+v", ev)
		}
	}
	bobPage, err := s.TenantAuditFor(ctx, "cust_y", bob.ID, AuditQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(bobPage.Events) != 1 || bobPage.Events[0].TargetID != bob.ID {
		t.Fatalf("cust_y journal = %+v, want only its own grant", bobPage.Events)
	}
}

// Pagination: newest first, bounded, exclusive cursor, and a next cursor only
// when another row is actually there.
func TestTenantAuditForPaginates(t *testing.T) {
	s, _, ids := auditFixture(t, "cust_pg", map[string]string{"alice": RoleOwner})
	ctx := context.Background()
	for i := range 5 {
		if err := s.AppendAudit(NewAuditEvent(AuditEvent{
			CustomerID: "cust_pg",
			Actor:      SystemActor(),
			Action:     ActionWorkloadStarted,
			Outcome:    OutcomeObserved,
			TargetKind: TargetWorkload,
			TargetID:   "wl_" + string(rune('a'+i)),
		})); err != nil {
			t.Fatal(err)
		}
	}

	first, err := s.TenantAuditFor(ctx, "cust_pg", ids["alice"], AuditQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Events) != 2 || first.NextAfter == "" {
		t.Fatalf("first page = %d rows, next %q", len(first.Events), first.NextAfter)
	}
	if first.Events[0].ID <= first.Events[1].ID {
		t.Fatalf("page is not newest-first: %q then %q", first.Events[0].ID, first.Events[1].ID)
	}
	if first.NextAfter != first.Events[1].ID {
		t.Fatalf("next cursor %q is not the last row of the page", first.NextAfter)
	}

	// Walk to the end; the cursor is exclusive, so nothing repeats.
	seen := map[string]bool{}
	for _, ev := range first.Events {
		seen[ev.ID] = true
	}
	cursor := first.NextAfter
	for cursor != "" {
		page, err := s.TenantAuditFor(ctx, "cust_pg", ids["alice"], AuditQuery{Limit: 2, After: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range page.Events {
			if seen[ev.ID] {
				t.Fatalf("cursor is not exclusive: %s came back twice", ev.ID)
			}
			seen[ev.ID] = true
		}
		cursor = page.NextAfter
	}
	// 5 observations + the fixture's one grant.
	if len(seen) != 6 {
		t.Fatalf("walked %d rows, want 6", len(seen))
	}

	// A page that exactly consumes the tail reports no next cursor.
	whole, err := s.TenantAuditFor(ctx, "cust_pg", ids["alice"], AuditQuery{Limit: 6})
	if err != nil {
		t.Fatal(err)
	}
	if len(whole.Events) != 6 || whole.NextAfter != "" {
		t.Fatalf("exact-tail page = %d rows, next %q; want 6 and no cursor", len(whole.Events), whole.NextAfter)
	}
}

// The bounds are the store's, not the caller's: no query can ask for the whole
// journal, and a malformed cursor is refused rather than resolved to page one.
func TestTenantAuditForBoundsAndCursorValidation(t *testing.T) {
	s, _, ids := auditFixture(t, "cust_b", map[string]string{"alice": RoleOwner})
	ctx := context.Background()

	if _, err := s.TenantAuditFor(ctx, "cust_b", ids["alice"], AuditQuery{After: "page-2"}); !errors.Is(err, ErrInvalidAudit) {
		t.Errorf("malformed cursor: err = %v, want ErrInvalidAudit", err)
	}
	if got := (AuditQuery{}).bounded().Limit; got != DefaultAuditLimit {
		t.Errorf("default limit = %d, want %d", got, DefaultAuditLimit)
	}
	if got := (AuditQuery{Limit: 10000}).bounded().Limit; got != MaxAuditLimit {
		t.Errorf("clamped limit = %d, want %d", got, MaxAuditLimit)
	}
}

// A journal that cannot be read is never reported as a tenant that is not
// there: one tells a manager to retry, the other tells them their history was
// deleted.
func TestTenantAuditForSurfacesDurableReadFailure(t *testing.T) {
	s, spy, ids := auditFixture(t, "cust_r", map[string]string{"alice": RoleOwner})
	spy.listErr = errors.New("connection refused")

	_, err := s.TenantAuditFor(context.Background(), "cust_r", ids["alice"], AuditQuery{})
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("err = %v, want ErrPersistence", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatal("a read failure was reported as a missing tenant")
	}
}

// The OSS/dev store has no journal, so an empty page is the honest answer — not
// an error, and not a refusal that would break the single-binary path.
func TestTenantAuditForOnAStoreWithNoJournal(t *testing.T) {
	s := New()
	acct, err := s.UpsertAccount("https://id.yscale.sh", "solo", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddTenantMembership(acct.ID, DevCustomerID, RoleOwner); err != nil {
		t.Fatal(err)
	}
	page, err := s.TenantAuditFor(context.Background(), DevCustomerID, acct.ID, AuditQuery{})
	if err != nil {
		t.Fatalf("in-memory journal read: %v", err)
	}
	if len(page.Events) != 0 || page.NextAfter != "" {
		t.Fatalf("page = %+v, want empty", page)
	}
	// And appending is a successful no-op, so an OSS deployment's mutations are
	// not refused for want of a backend it does not have.
	if err := s.AppendAudit(submitEvent(DevCustomerID, "wl_x", ClusterActor(DevCustomerID))); err != nil {
		t.Fatalf("in-memory append: %v", err)
	}
}

// Offboarding a tenant removes the tenant. It does NOT remove the record of
// what was done to it — that record becomes the only one left.
func TestOffboardRetainsAuditRows(t *testing.T) {
	s, spy, _ := auditFixture(t, "cust_off", map[string]string{"alice": RoleOwner})
	before := len(spy.events())
	if before == 0 {
		t.Fatal("fixture recorded no rows to lose")
	}
	if err := s.RevokeCustomer("cust_off"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRevokedCustomer("cust_off"); err != nil {
		t.Fatal(err)
	}
	if len(spy.customers) != 0 || len(spy.memberships) != 0 {
		t.Fatal("offboard left the tenant or its grants behind")
	}
	if got := len(spy.events()); got != before {
		t.Fatalf("audit rows after offboard = %d, want %d", got, before)
	}
}

// The closed sets are enforced before a row reaches durable state: the journal
// cannot be updated afterwards, so a row nobody can interpret is permanent.
func TestAppendAuditRefusesRowsOutsideTheClosedSets(t *testing.T) {
	s, spy := storeWithSpy("cust_v")
	bad := []AuditEvent{
		{CustomerID: "cust_v", Actor: SystemActor(), Action: "workload.rm", Outcome: OutcomeObserved},
		{CustomerID: "cust_v", Actor: SystemActor(), Action: ActionWorkloadSubmit, Outcome: "maybe"},
		{CustomerID: "cust_v", Actor: Actor{Kind: "root"}, Action: ActionWorkloadSubmit, Outcome: OutcomeAccepted},
		{CustomerID: "", Actor: SystemActor(), Action: ActionWorkloadSubmit, Outcome: OutcomeAccepted},
		{CustomerID: "cust_v", Actor: SystemActor(), Action: ActionWorkloadSubmit, Outcome: OutcomeAccepted, TargetKind: "secret"},
	}
	for _, ev := range bad {
		if err := s.AppendAudit(NewAuditEvent(ev)); !errors.Is(err, ErrInvalidAudit) {
			t.Errorf("%+v: err = %v, want ErrInvalidAudit", ev, err)
		}
	}
	if len(spy.events()) != 0 {
		t.Fatalf("a refused row was written: %+v", spy.events())
	}
}

// A submitted namespace only reaches the journal when it is a namespace. Any
// other bytes are recorded as the redaction sentinel, so arbitrary submitter
// input cannot enter the evidence through the one field that carries it.
func TestSafeNamespaceBoundsSubmitterInput(t *testing.T) {
	cases := map[string]string{
		"":                      "",
		"jobs":                  "jobs",
		"Jobs":                  NamespaceRedacted,
		"team a":                NamespaceRedacted,
		"jobs,secrets":          NamespaceRedacted,
		"$(cat /etc/shadow)":    NamespaceRedacted,
		strings.Repeat("a", 64): NamespaceRedacted,
		strings.Repeat("a", 63): strings.Repeat("a", 63),
	}
	for in, want := range cases {
		if got := SafeNamespace(in); got != want {
			t.Errorf("SafeNamespace(%q) = %q, want %q", in, got, want)
		}
	}
}

// The ids the journal is ordered and paginated by must sort by append time and
// be unguessable: one property makes the cursor stable, the other makes it
// opaque.
func TestAuditIDsSortByTimeAndAreOpaque(t *testing.T) {
	first := newAuditID()
	time.Sleep(time.Millisecond)
	second := newAuditID()
	if !(second > first) {
		t.Fatalf("ids do not sort by append time: %q then %q", first, second)
	}
	if !ValidAuditCursor(first) || ValidAuditCursor("aud_nope") || ValidAuditCursor(strings.TrimPrefix(first, auditIDPrefix)) {
		t.Fatalf("cursor validation is wrong for %q", first)
	}
	// The random tail means two ids minted in the same instant still differ.
	if newAuditID() == newAuditID() {
		t.Fatal("audit ids collide")
	}
}

// The id a row is stored under may never fall below the tenant's newest, even
// when the clock says otherwise.
//
// This is what makes the cursor safe across replicas. Wall clocks on two
// machines disagree by more than the gap between two commits, so an id minted
// purely from the local clock can land INSIDE a page a reader has already walked
// past — and the next page asks for ids strictly less than the cursor, so that
// row is never returned again by anyone.
func TestAuditIDsNeverFallBehindTheTenantCursor(t *testing.T) {
	// A row committed by a replica whose clock is an hour ahead.
	ahead := AuditEvent{}
	stampAudit(&ahead, "")
	future := bumpAuditID(ahead.ID)
	for range 32 { // well past any plausible skew, still a valid id
		future = bumpAuditID(future)
	}

	var ev AuditEvent
	stampAudit(&ev, future)
	if !(ev.ID > future) {
		t.Fatalf("id %q does not beat the tenant's newest %q", ev.ID, future)
	}
	if !ValidAuditCursor(ev.ID) {
		t.Fatalf("id %q is not a valid cursor", ev.ID)
	}
	if ev.At.IsZero() {
		t.Error("stampAudit left the append time unset")
	}
	// The clock is still preferred when it is ahead of the stored newest, so ids
	// stay meaningfully time-ordered rather than becoming a counter.
	var normal AuditEvent
	stampAudit(&normal, "")
	if !(normal.ID > ahead.ID) {
		t.Errorf("id %q does not sort after the earlier %q", normal.ID, ahead.ID)
	}
}

// Concurrent appends for ONE tenant come out in cursor order, because the
// journal serializes them and stamps each id only once the previous row is
// committed. Run under -race.
func TestConcurrentAppendsForOneTenantStayInCursorOrder(t *testing.T) {
	s, spy := storeWithSpy("cust_c")
	const writers, each = 8, 5

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range each {
				if err := s.AppendAudit(NewAuditEvent(AuditEvent{
					CustomerID: "cust_c",
					Actor:      SystemActor(),
					Action:     ActionWorkloadStarted,
					Outcome:    OutcomeObserved,
					TargetKind: TargetWorkload,
					TargetID:   fmt.Sprintf("wl_%d_%d", w, i),
				})); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	ids := spy.idsInCommitOrder()
	if len(ids) != writers*each {
		t.Fatalf("rows = %d, want %d", len(ids), writers*each)
	}
	for i := 1; i < len(ids); i++ {
		if !(ids[i] > ids[i-1]) {
			t.Fatalf("row %d committed after %d but sorts before it: %q then %q — "+
				"a cursor issued at the earlier row would skip the later one forever",
				i, i-1, ids[i-1], ids[i])
		}
	}
	// The whole set is reachable by paging, exactly once each.
	acct, err := s.UpsertAccount("https://id.yscale.sh", "walker", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GrantTenantMembership(acct.ID, "cust_c", RoleOwner, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for cursor, more := "", true; more; {
		page, err := s.TenantAuditFor(context.Background(), "cust_c", acct.ID, AuditQuery{Limit: 3, After: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range page.Events {
			if seen[ev.ID] {
				t.Fatalf("%s came back twice", ev.ID)
			}
			seen[ev.ID] = true
		}
		cursor, more = page.NextAfter, page.NextAfter != ""
	}
	for _, id := range ids {
		if !seen[id] {
			t.Fatalf("%s was never returned by a full walk of the journal", id)
		}
	}
}

// The durable half of the ordering guarantee, against a real Postgres: two
// appends for the same tenant cannot commit out of cursor order, and two
// different tenants never wait on each other.
//
// It drives insertAuditTx directly because the property IS the transaction
// boundary — the lock is held until commit, and the id is stamped inside it.
// Gated on YSCALE_TEST_DATABASE_URL like the other integration tests here, in a
// schema of its own so it neither sees nor leaves rows for them.
func TestPostgresAuditAppendsSerializePerTenant(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	pool := freshSchemaPool(t, dsn, "yscale_test_audit_order", 4)
	p := &pgPersister{pool: pool}
	if err := p.ensureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	observation := func(tenant, workload string) *AuditEvent {
		return NewAuditEvent(AuditEvent{
			CustomerID: tenant, Actor: SystemActor(),
			Action: ActionWorkloadStarted, Outcome: OutcomeObserved,
			TargetKind: TargetWorkload, TargetID: workload,
		})
	}

	// Same tenant: the second append blocks on the first transaction's lock, so
	// it cannot stamp an id until the first row is committed and visible.
	tx1, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := observation("cust_order", "wl_first")
	if err := insertAuditTx(ctx, tx1, first); err != nil {
		t.Fatalf("first append: %v", err)
	}
	second := observation("cust_order", "wl_second")
	blocked := make(chan error, 1)
	go func() {
		tx2, err := pool.Begin(ctx)
		if err != nil {
			blocked <- err
			return
		}
		defer func() { _ = tx2.Rollback(ctx) }()
		if err := insertAuditTx(ctx, tx2, second); err != nil {
			blocked <- err
			return
		}
		blocked <- tx2.Commit(ctx)
	}()
	select {
	case err := <-blocked:
		t.Fatalf("a same-tenant append committed while another held the append lock (err %v); "+
			"its id is stamped against a journal it cannot see", err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if err := <-blocked; err != nil {
		t.Fatalf("second append: %v", err)
	}
	if !(second.ID > first.ID) {
		t.Fatalf("the later commit sorts before the earlier one: %q then %q", first.ID, second.ID)
	}

	// A different tenant is not behind that lock: its append runs to completion
	// while cust_order's is still open.
	held, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertAuditTx(ctx, held, observation("cust_order", "wl_third")); err != nil {
		t.Fatal(err)
	}
	elsewhere := make(chan error, 1)
	go func() {
		tx, err := pool.Begin(ctx)
		if err != nil {
			elsewhere <- err
			return
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := insertAuditTx(ctx, tx, observation("cust_other", "wl_other")); err != nil {
			elsewhere <- err
			return
		}
		elsewhere <- tx.Commit(ctx)
	}()
	select {
	case err := <-elsewhere:
		if err != nil {
			t.Fatalf("another tenant's append: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an append for a different tenant waited on this one; the lock is not per tenant")
	}
	if err := held.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// The rolled-back row left nothing behind, and the two committed ones are
	// both readable in id order.
	rows, err := p.listAudit(ctx, "cust_order", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ID != second.ID || rows[1].ID != first.ID {
		t.Fatalf("journal = %+v, want the two committed rows newest first", rows)
	}
}

// The schema states append-only as a DATABASE rule, not a convention. Asserted
// on the constant so the guarantee cannot be dropped in a refactor that no
// integration test happens to run.
func TestSchemaDeclaresTheAuditJournalAppendOnly(t *testing.T) {
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS tenant_audit",
		"CREATE INDEX IF NOT EXISTS tenant_audit_customer_id ON tenant_audit (customer_id, id DESC)",
		"DROP TRIGGER IF EXISTS tenant_audit_no_rewrite ON tenant_audit",
		"BEFORE UPDATE OR DELETE ON tenant_audit",
	} {
		if !strings.Contains(pgSchema, want) {
			t.Errorf("pgSchema is missing %q", want)
		}
	}
	// The offboard deletes are scoped to the customer and membership tables.
	// tenant_audit is not among them, which is what keeps evidence past the
	// tenant it describes.
	if strings.Contains(pgSchema, "DELETE FROM tenant_audit") {
		t.Error("the schema deletes audit rows")
	}
}

// The durable half, against a real Postgres: the trigger refuses UPDATE and
// DELETE, the tenant-scoped read paginates, and an offboard leaves the journal
// standing. Gated on YSCALE_TEST_DATABASE_URL like the other integration tests
// here, so the default run needs no database.
func TestPostgresAuditJournalIsAppendOnly(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	s, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	defer s.Close()

	const tenant = "cust_audit_pg"
	s.AddCustomer(&Customer{ID: tenant, Token: "tok_audit_pg", Plan: "pro"})
	acct, err := s.UpsertAccount("https://id.yscale.sh", "audit-owner", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GrantTenantMembership(acct.ID, tenant, RoleOwner, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	ev := NewAuditEvent(AuditEvent{
		CustomerID: tenant, Actor: SystemActor(),
		Action: ActionWorkloadStarted, Outcome: OutcomeObserved,
		TargetKind: TargetWorkload, TargetID: "wl_audit_pg",
	})
	if err := s.AppendAudit(ev); err != nil {
		t.Fatalf("AppendAudit: %v", err)
	}

	p, ok := s.persist.(*pgPersister)
	if !ok {
		t.Fatalf("persister = %T, want *pgPersister", s.persist)
	}
	if _, err := p.pool.Exec(ctx,
		`UPDATE tenant_audit SET data = data WHERE id = $1`, ev.ID); err == nil {
		t.Error("UPDATE on the audit journal was allowed")
	}
	if _, err := p.pool.Exec(ctx, `DELETE FROM tenant_audit WHERE id = $1`, ev.ID); err == nil {
		t.Error("DELETE on the audit journal was allowed")
	}

	page, err := s.TenantAuditFor(ctx, tenant, acct.ID, AuditQuery{Limit: 1})
	if err != nil {
		t.Fatalf("TenantAuditFor: %v", err)
	}
	if len(page.Events) != 1 || page.NextAfter == "" {
		t.Fatalf("page = %d rows, next %q; want 1 and a cursor (the grant is behind it)", len(page.Events), page.NextAfter)
	}

	// The offboard removes the tenant and leaves its journal.
	if err := s.RevokeCustomer(tenant); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRevokedCustomer(tenant); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := p.pool.QueryRow(ctx,
		`SELECT count(*) FROM tenant_audit WHERE customer_id = $1`, tenant).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows < 2 {
		t.Fatalf("audit rows after offboard = %d, want the grant and the observation still there", rows)
	}
}
