package state

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestClusterPolicyValidation(t *testing.T) {
	valid, err := ValidateClusterPolicy(ClusterPolicy{
		Allow: []string{"cl_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "cluster-east"},
		Deny:  []string{"cluster_west"},
		Auto:  ClusterPolicyAutoOrdered,
	})
	if err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	if valid == nil || valid.Auto != ClusterPolicyAutoOrdered || !slices.Equal(valid.Allow, []string{"cl_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "cluster-east"}) {
		t.Fatalf("normalized policy = %+v", valid)
	}
	if def, err := ValidateClusterPolicy(ClusterPolicy{}); err != nil || def != nil {
		t.Fatalf("default policy = (%+v, %v), want nil/no-op", def, err)
	}
	for _, id := range []string{"a", "a" + strings.Repeat("z", 127)} {
		if _, err := ValidateClusterPolicy(ClusterPolicy{Allow: []string{id}}); err != nil {
			t.Errorf("proxy-compatible cluster id of length %d rejected: %v", len(id), err)
		}
	}

	tests := []ClusterPolicy{
		{Auto: "fast"},
		{Allow: []string{"bad cluster"}},
		{Allow: []string{"cl_a", "cl_a"}},
		{Allow: []string{"cl_a"}, Deny: []string{"cl_a"}},
		{Deny: []string{"\n"}},
		{Allow: []string{"a" + strings.Repeat("z", 128)}},
	}
	tooMany := ClusterPolicy{Allow: make([]string, 65)}
	for i := range tooMany.Allow {
		tooMany.Allow[i] = "cl_" + strings.Repeat("a", 30) + string(rune('A'+i%26))
	}
	tests = append(tests, tooMany)

	for _, policy := range tests {
		if _, err := ValidateClusterPolicy(policy); !errors.Is(err, ErrInvalidClusterPolicy) {
			t.Errorf("%+v: err = %v, want ErrInvalidClusterPolicy", policy, err)
		}
	}
}

func TestSetTenantClusterPolicyReconcilesOnlyAnExistingTenant(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_reconcile", map[string]string{"alice": RoleOwner})
	policy := ClusterPolicy{Allow: []string{"cluster_a"}, Auto: ClusterPolicyAutoOrdered}
	if _, _, _, err := s.SetTenantClusterPolicy("cust_reconcile", ids["alice"], policy, HumanActor(ids["alice"], "cust_reconcile")); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	before := len(spy.eventsWith(ActionClusterPolicySet))

	got, changed, _, err := s.SetTenantClusterPolicy("cust_reconcile", ids["alice"], policy, HumanActor(ids["alice"], "cust_reconcile"))
	if err != nil || changed {
		t.Fatalf("reconcile = (%+v, changed=%v, err=%v)", got, changed, err)
	}
	if _, ok := spy.customers["cust_reconcile"]; !ok {
		t.Fatal("unchanged policy did not reconcile the durable customer row")
	}
	delete(spy.customers, "cust_reconcile")
	if _, _, _, err := s.SetTenantClusterPolicy("cust_reconcile", ids["alice"], policy, HumanActor(ids["alice"], "cust_reconcile")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unchanged policy recreated a deleted tenant: %v", err)
	}
	if _, exists := spy.customers["cust_reconcile"]; exists {
		t.Fatal("refused reconciliation recreated the durable customer")
	}
	if after := len(spy.eventsWith(ActionClusterPolicySet)); after != before {
		t.Fatalf("unchanged reconcile added %d audit rows", after-before)
	}
}

func TestSetTenantClusterPolicyPersistsReloadsRollsBackAndSkipsNoopAudit(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_policy", map[string]string{
		"alice": RoleOwner, "adm": RoleAdmin, "mem": RoleMember,
	})
	by := HumanActor(ids["alice"], "cust_policy")
	policy := ClusterPolicy{Allow: []string{"cluster_west", "cluster_east"}, Deny: []string{"cluster_dead"}, Auto: ClusterPolicyAutoOrdered}

	got, changed, role, err := s.SetTenantClusterPolicy("cust_policy", ids["alice"], policy, by)
	if err != nil || !changed || role != RoleOwner {
		t.Fatalf("set policy = (%+v, %v, %q, %v), want change by owner", got, changed, role, err)
	}
	if got.Auto != ClusterPolicyAutoOrdered || !slices.Equal(got.Allow, policy.Allow) {
		t.Fatalf("stored policy = %+v", got)
	}
	events := spy.eventsWith(ActionClusterPolicySet)
	if len(events) != 1 {
		t.Fatalf("cluster policy audit rows = %d, want 1", len(events))
	}
	if events[0].Actor.AccountID != ids["alice"] || events[0].Detail.ClusterPolicy == nil ||
		events[0].Detail.ClusterPolicy.Auto != ClusterPolicyAutoOrdered {
		t.Fatalf("audit row = %+v", events[0])
	}

	reloaded := emptyStore()
	reloaded.applySnapshot(spy.recordedSnapshot(t))
	loaded, _, err := reloaded.TenantClusterPolicyFor("cust_policy", ids["alice"])
	if err != nil || loaded.Auto != ClusterPolicyAutoOrdered || !slices.Equal(loaded.Deny, []string{"cluster_dead"}) {
		t.Fatalf("reloaded policy = (%+v, %v)", loaded, err)
	}

	_, changed, _, err = s.SetTenantClusterPolicy("cust_policy", ids["adm"], policy, HumanActor(ids["adm"], "cust_policy"))
	if err != nil || changed {
		t.Fatalf("same policy by admin = (%v, %v), want no-op", changed, err)
	}
	if rows := spy.eventsWith(ActionClusterPolicySet); len(rows) != 1 {
		t.Fatalf("no-op wrote audit rows: %d", len(rows))
	}

	if _, _, _, err := s.SetTenantClusterPolicy("cust_policy", ids["mem"], ClusterPolicy{Auto: ClusterPolicyAutoRequirePin}, HumanActor(ids["mem"], "cust_policy")); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("member set policy err = %v, want ErrNotAuthorized", err)
	}

	spy.appendErr = errPersist
	_, _, _, err = s.SetTenantClusterPolicy("cust_policy", ids["alice"], ClusterPolicy{Deny: []string{"cluster_west"}}, by)
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("persist failure err = %v, want ErrPersistence", err)
	}
	after, _, err := s.TenantClusterPolicyFor("cust_policy", ids["alice"])
	if err != nil || !slices.Equal(after.Allow, policy.Allow) || !slices.Equal(after.Deny, policy.Deny) {
		t.Fatalf("failed write published policy: %+v err=%v", after, err)
	}
}

func TestClusterPolicySurvivesEnvSeedRefresh(t *testing.T) {
	s := New()
	c := s.AddCustomer(&Customer{ID: "cust_seed", Token: "tok_seed", Plan: "pro",
		ClusterPolicy: &ClusterPolicy{Allow: []string{"cluster_a"}, Auto: ClusterPolicyAutoOrdered}})
	if c.ClusterPolicy == nil {
		t.Fatal("seed policy was not stored")
	}
	s.AddCustomer(&Customer{ID: "cust_seed", Token: "tok_seed", Plan: "pro"})
	got, err := s.CustomerByID("cust_seed")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if got.ClusterPolicy == nil || got.ClusterPolicy.Auto != ClusterPolicyAutoOrdered || !slices.Equal(got.ClusterPolicy.Allow, []string{"cluster_a"}) {
		t.Fatalf("policy was not preserved across seed refresh: %+v", got.ClusterPolicy)
	}
}
