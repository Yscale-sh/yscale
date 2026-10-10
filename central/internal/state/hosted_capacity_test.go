package state

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestTenantHostedCapacityRequestLifecycle(t *testing.T) {
	s, spy, ids := auditFixture(t, "cust_h", map[string]string{
		"owner": RoleOwner, "admin": RoleAdmin, "member": RoleMember, "viewer": RoleViewer,
	})

	for sub, role := range map[string]string{"owner": RoleOwner, "admin": RoleAdmin, "member": RoleMember, "viewer": RoleViewer} {
		view, err := s.TenantHostedCapacityFor("cust_h", ids[sub])
		if err != nil || view.Status != HostedCapacityNotRequested || view.Role != role || view.RequestedAt != nil {
			t.Fatalf("initial view for %s = (%+v,%v)", sub, view, err)
		}
	}
	for _, sub := range []string{"member", "viewer"} {
		if _, _, err := s.RequestTenantHostedCapacity("cust_h", ids[sub], HumanActor(ids[sub], "cust_h")); !errors.Is(err, ErrNotAuthorized) {
			t.Fatalf("%s request err = %v, want ErrNotAuthorized", sub, err)
		}
	}

	first, created, err := s.RequestTenantHostedCapacity("cust_h", ids["owner"], HumanActor(ids["owner"], "cust_h"))
	if err != nil || !created || first.Status != HostedCapacityRequested || first.RequestedAt == nil {
		t.Fatalf("first request = (%+v,%v,%v)", first, created, err)
	}
	if first.RequestedAt.Location() != time.UTC {
		t.Fatalf("request timestamp location = %v, want UTC", first.RequestedAt.Location())
	}
	duplicate, created, err := s.RequestTenantHostedCapacity("cust_h", ids["admin"], HumanActor(ids["admin"], "cust_h"))
	if err != nil || created || duplicate.RequestedAt == nil || !duplicate.RequestedAt.Equal(*first.RequestedAt) {
		t.Fatalf("duplicate request = (%+v,%v,%v), want original timestamp", duplicate, created, err)
	}
	rows := spy.eventsWith(ActionHostedCapacityRequest)
	if len(rows) != 1 {
		t.Fatalf("request audit rows = %d, want 1", len(rows))
	}
	ev := rows[0]
	if ev.Actor != HumanActor(ids["owner"], "cust_h") || ev.Outcome != OutcomeAccepted ||
		ev.TargetKind != TargetTenant || ev.TargetID != "cust_h" ||
		ev.Detail.Reason != ReasonHostedCapacityRequested || ev.Detail.Role != RoleOwner {
		t.Fatalf("request audit = %+v", ev)
	}

	reloaded := emptyStore()
	reloaded.applySnapshot(spy.recordedSnapshot(t))
	afterRestart, err := reloaded.TenantHostedCapacityFor("cust_h", ids["viewer"])
	if err != nil || afterRestart.RequestedAt == nil || !afterRestart.RequestedAt.Equal(*first.RequestedAt) {
		t.Fatalf("reloaded request = (%+v,%v)", afterRestart, err)
	}

	if _, _, err := s.AssignHostedCluster("cust_h", "hosted-h", "", "ys-h", OperatorActor()); err != nil {
		t.Fatalf("assign hosted cluster: %v", err)
	}
	assigned, err := s.TenantHostedCapacityFor("cust_h", ids["viewer"])
	if err != nil || assigned.Status != HostedCapacityAssigned || assigned.RequestedAt != nil {
		t.Fatalf("assigned view = (%+v,%v)", assigned, err)
	}
	persisted := spy.recordedSnapshot(t).Customers[0]
	if persisted.HostedCapacityRequestedAt != nil {
		t.Fatalf("assignment left persisted request timestamp %v", persisted.HostedCapacityRequestedAt)
	}
	if got := len(spy.eventsWith(ActionHostedCapacityRequest)); got != 1 {
		t.Fatalf("assignment changed request audit count to %d", got)
	}
}

func TestTenantHostedCapacityRequestPersistenceFailurePublishesNothing(t *testing.T) {
	s, _, ids := auditFixture(t, "cust_h", map[string]string{"owner": RoleOwner})
	fail := &nsFailPersister{err: errors.New("postgres down")}
	// Keep authorization reads healthy so this tests the failed write itself.
	s.persist = struct {
		persister
		humanAuthorizationReader
	}{fail, s.persist.(humanAuthorizationReader)}

	if _, _, err := s.RequestTenantHostedCapacity("cust_h", ids["owner"], HumanActor(ids["owner"], "cust_h")); !errors.Is(err, ErrPersistence) {
		t.Fatalf("request error = %v, want ErrPersistence", err)
	}
	view, err := s.TenantHostedCapacityFor("cust_h", ids["owner"])
	if err != nil || view.Status != HostedCapacityNotRequested || view.RequestedAt != nil {
		t.Fatalf("view after failed write = (%+v,%v)", view, err)
	}
	if got := len(fail.eventsWith(ActionHostedCapacityRequest)); got != 0 {
		t.Fatalf("failed write appended %d audit rows", got)
	}
}

func TestTenantHostedCapacityOldJSONAndAssignedPrecedence(t *testing.T) {
	var old Customer
	if err := json.Unmarshal([]byte(`{"ID":"cust_old","Plan":"pro"}`), &old); err != nil {
		t.Fatalf("unmarshal old customer: %v", err)
	}
	if old.HostedCapacityRequestedAt != nil {
		t.Fatalf("old customer acquired request timestamp %v", old.HostedCapacityRequestedAt)
	}

	stale := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	old.HostedCapacityRequestedAt = &stale
	old.RegisteredClusters = []*RegisteredCluster{{ClusterID: "hosted-old", Source: ClusterSourceHosted}}
	view := hostedCapacityLocked(&old, RoleViewer)
	if view.Status != HostedCapacityAssigned || view.RequestedAt != nil {
		t.Fatalf("assigned customer with stale request = %+v", view)
	}
}

// The operator's queue is the set of tenants an assignment decision needs:
// pending requests from active tenants, oldest first with the tenant id
// breaking ties, as value copies the caller cannot bend the store through.
func TestPendingHostedCapacityRequestsOrderingFilteringAndCopies(t *testing.T) {
	s := emptyStore()
	offZone := time.FixedZone("shop-floor", -4*60*60)
	older := time.Date(2026, 8, 1, 13, 0, 0, 0, offZone) // 17:00 UTC
	tie := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	revokedAt := time.Date(2026, 8, 4, 9, 0, 0, 0, time.UTC)

	for _, c := range []*Customer{
		{ID: "cust_newest", Token: "ysk_newest", Plan: "pro", Name: "Newest Team", HostedCapacityRequestedAt: &newer},
		{ID: "cust_oldest", Token: "ysk_oldest", Plan: "enterprise", HostedCapacityRequestedAt: &older},
		{ID: "cust_tie_z", Token: "ysk_tie_z", Plan: "free", HostedCapacityRequestedAt: &tie},
		{ID: "cust_tie_a", Token: "ysk_tie_a", Plan: "trial", HostedCapacityRequestedAt: &tie},
		// A standing hosted row wins over the stale marker, exactly as in the
		// tenant's own view.
		{ID: "cust_assigned", Token: "ysk_assigned", Plan: "pro", HostedCapacityRequestedAt: &older,
			RegisteredClusters: []*RegisteredCluster{{ClusterID: "hosted-assigned", Source: ClusterSourceHosted}}},
		// Revoked: the record survives for cleanup, the queue does not owe it.
		{ID: "cust_revoked", Token: "ysk_revoked", Plan: "pro", HostedCapacityRequestedAt: &tie, RevokedAt: &revokedAt},
		// No request marker: not queued.
		{ID: "cust_silent", Token: "ysk_silent", Plan: "pro"},
	} {
		s.AddCustomer(c)
	}

	got := s.PendingHostedCapacityRequests()
	wantIDs := []string{"cust_oldest", "cust_tie_a", "cust_tie_z", "cust_newest"}
	if len(got) != len(wantIDs) {
		t.Fatalf("queue = %+v, want exactly %v", got, wantIDs)
	}
	for i, id := range wantIDs {
		if got[i].TenantID != id {
			t.Fatalf("queue[%d] = %s, want %s (full %+v)", i, got[i].TenantID, id, got)
		}
	}
	if got[0].RequestedAt.Location() != time.UTC || !got[0].RequestedAt.Equal(older) {
		t.Fatalf("oldest row = %v (%v), want %v normalized to UTC", got[0].RequestedAt, got[0].RequestedAt.Location(), older)
	}
	if got[0].Plan != "enterprise" || got[0].Name != "" {
		t.Fatalf("oldest row = %+v, want plan and empty display name", got[0])
	}
	if got[3].Name != "Newest Team" {
		t.Fatalf("newest row = %+v, want the display name carried through", got[3])
	}

	// Value copies: caller mutations never reach store state.
	got[0].TenantID = "mutated"
	got[0].RequestedAt = newer
	if again := s.PendingHostedCapacityRequests(); again[0].TenantID != "cust_oldest" || !again[0].RequestedAt.Equal(older) {
		t.Fatalf("store adopted caller mutation: %+v", again[0])
	}
}

// An empty queue is an empty array, not null and not nil: the operator route
// renders the same JSON shape whether anyone is waiting or not.
func TestPendingHostedCapacityRequestsEmptyIsNonNil(t *testing.T) {
	s := emptyStore()
	if got := s.PendingHostedCapacityRequests(); got == nil || len(got) != 0 {
		t.Fatalf("empty store queue = %#v, want non-nil empty slice", got)
	}
	s.AddCustomer(&Customer{ID: "cust_quiet", Token: "ysk_quiet", Plan: "pro"})
	if got := s.PendingHostedCapacityRequests(); got == nil || len(got) != 0 {
		t.Fatalf("no-request store queue = %#v, want non-nil empty slice", got)
	}
}

// The queue read is a pure read: no durable write, no audit row. The request
// and assignment paths own the marker and its trail; discovery adds neither.
func TestPendingHostedCapacityRequestsWritesNoAuditRow(t *testing.T) {
	s, spy := storeWithSpy("cust_h")
	c, err := s.CustomerByID("cust_h")
	if err != nil {
		t.Fatal(err)
	}
	requestedAt := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	c.HostedCapacityRequestedAt = &requestedAt

	if got := s.PendingHostedCapacityRequests(); len(got) != 1 || got[0].TenantID != "cust_h" {
		t.Fatalf("queue = %+v, want the one pending tenant", got)
	}
	if rows := spy.events(); len(rows) != 0 {
		t.Fatalf("queue read appended %d audit rows: %+v", len(rows), rows)
	}
}
