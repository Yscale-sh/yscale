package state

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

type failCustomerPersister struct {
	gatewayRouteSpyPersister
	err error
}

func (p *failCustomerPersister) upsertCustomer(c *Customer) error {
	if p.err != nil {
		return p.err
	}
	return p.gatewayRouteSpyPersister.upsertCustomer(c)
}

func (p *failCustomerPersister) upsertCustomerAudited(c *Customer, ev *AuditEvent) error {
	return p.withAudit(ev, func() error { return p.upsertCustomer(c) })
}

func TestListOperatorTenantsPaginationAndIsolation(t *testing.T) {
	s := New()
	delete(s.customers, DevCustomerID)

	c1 := &Customer{ID: "cust_c", Name: "Charlie", Plan: "pro", Token: "tok_c", Email: "c@acme.com"}
	c2 := &Customer{ID: "cust_a", Name: "Alice", Plan: "free", Token: "tok_a", Email: "a@acme.com"}
	c3 := &Customer{ID: "cust_b", Name: "Bob", Plan: "enterprise", Token: "tok_b", Email: "b@acme.com"}
	rev := time.Now().UTC()
	cRev := &Customer{ID: "cust_revoked", Name: "Revoked", Plan: "free", RevokedAt: &rev, Token: "tok_r", Email: "r@acme.com"}

	s.AddCustomer(c1)
	s.AddCustomer(c2)
	s.AddCustomer(c3)
	s.AddCustomer(cRev)

	// Page 1: limit 2, after ""
	page1, nextAfter1 := s.ListOperatorTenants("", 2)
	if len(page1) != 2 {
		t.Fatalf("page 1 len = %d, want 2", len(page1))
	}
	if page1[0].ID != "cust_a" || page1[1].ID != "cust_b" {
		t.Fatalf("page 1 IDs = [%s, %s], want [cust_a, cust_b]", page1[0].ID, page1[1].ID)
	}
	if nextAfter1 != "cust_b" {
		t.Fatalf("nextAfter1 = %q, want cust_b", nextAfter1)
	}

	// Page 2: limit 2, after "cust_b"
	page2, nextAfter2 := s.ListOperatorTenants("cust_b", 2)
	if len(page2) != 1 {
		t.Fatalf("page 2 len = %d, want 1", len(page2))
	}
	if page2[0].ID != "cust_c" {
		t.Fatalf("page 2 ID = %s, want cust_c", page2[0].ID)
	}
	if nextAfter2 != "" {
		t.Fatalf("nextAfter2 = %q, want empty", nextAfter2)
	}

	// Verify revoked tenant cust_revoked was excluded from all results
	all, _ := s.ListOperatorTenants("", 100)
	for _, c := range all {
		if c.ID == "cust_revoked" {
			t.Fatal("revoked customer was returned in ListOperatorTenants")
		}
	}
}

func TestSetCustomerLimitsPersistenceAndAudit(t *testing.T) {
	s := New()
	spy := &gatewayRouteSpyPersister{}
	s.persist = spy

	c := &Customer{
		ID:                  "cust_limits",
		Plan:                "pro",
		MaxConcurrentBursts: 2,
		MaxHourlyUSD:        5.0,
	}
	s.AddCustomer(c)

	actor := HumanActor("acct_op", "cust_limits")
	updated, changed, err := s.SetCustomerLimits("cust_limits", 10, 25.0, actor)
	if err != nil {
		t.Fatalf("SetCustomerLimits: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if updated.MaxConcurrentBursts != 10 || updated.MaxHourlyUSD != 25.0 {
		t.Fatalf("updated limits = %+v, want max_concurrent_bursts 10, max_hourly_usd 25.0", updated)
	}

	// Verify audit row was recorded with safe detail and actor attribution
	events := spy.eventsWith(ActionTenantLimitsSet)
	if len(events) != 1 {
		t.Fatalf("audit events = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Actor.Kind != ActorHuman || ev.Actor.AccountID != "acct_op" || ev.Actor.CustomerID != "cust_limits" {
		t.Fatalf("audit actor = %+v, want human acct_op for cust_limits", ev.Actor)
	}
	if ev.Detail.PreviousMaxConcurrentBursts == nil || *ev.Detail.PreviousMaxConcurrentBursts != 2 ||
		ev.Detail.MaxConcurrentBursts == nil || *ev.Detail.MaxConcurrentBursts != 10 {
		t.Fatalf("audit detail bursts = previous %v, new %v; want 2 and 10", ev.Detail.PreviousMaxConcurrentBursts, ev.Detail.MaxConcurrentBursts)
	}
	if ev.Detail.PreviousMaxHourlyUSD == nil || *ev.Detail.PreviousMaxHourlyUSD != 5 ||
		ev.Detail.MaxHourlyUSD == nil || *ev.Detail.MaxHourlyUSD != 25 {
		t.Fatalf("audit detail hourly = previous %v, new %v; want 5.0 and 25.0", ev.Detail.PreviousMaxHourlyUSD, ev.Detail.MaxHourlyUSD)
	}
	if ev.Detail.Rule != "tenant.limits_set" || ev.Detail.RuleVersion != "v1" || ev.Detail.Reason != ReasonTenantLimitsUpdated {
		t.Fatalf("audit detail metadata = %+v", ev.Detail)
	}
	if err := validateAudit(NewAuditEvent(AuditEvent{
		CustomerID: "cust_limits", Actor: actor, Action: ActionTenantLimitsSet,
		Outcome: OutcomeAccepted, TargetKind: TargetTenant, TargetID: "cust_limits",
	})); err != nil {
		t.Fatalf("tenant limits action rejected by audit validation: %v", err)
	}
}

func TestSetCustomerLimitsNoOpAuditBehavior(t *testing.T) {
	s := New()
	spy := &gatewayRouteSpyPersister{}
	s.persist = spy

	c := &Customer{
		ID:                  "cust_noop",
		MaxConcurrentBursts: 5,
		MaxHourlyUSD:        10.0,
	}
	s.AddCustomer(c)

	actor := OperatorActor()
	updated, changed, err := s.SetCustomerLimits("cust_noop", 5, 10.0, actor)
	if err != nil {
		t.Fatalf("SetCustomerLimits: %v", err)
	}
	if changed {
		t.Fatal("changed = true for identical update, want false")
	}
	if updated.MaxConcurrentBursts != 5 || updated.MaxHourlyUSD != 10.0 {
		t.Fatalf("updated limits = %+v", updated)
	}

	// Verify NO audit row was appended for a no-op update
	if len(spy.events()) != 0 {
		t.Fatalf("audit events = %d, want 0 for identical update", len(spy.events()))
	}
}

func TestSetCustomerLimitsNotFoundAndRevoked(t *testing.T) {
	s := New()

	_, _, err := s.SetCustomerLimits("cust_missing", 5, 10.0, OperatorActor())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing tenant error = %v, want ErrNotFound", err)
	}

	rev := time.Now().UTC()
	s.AddCustomer(&Customer{ID: "cust_rev", RevokedAt: &rev})
	_, _, err = s.SetCustomerLimits("cust_rev", 5, 10.0, OperatorActor())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoked tenant error = %v, want ErrNotFound", err)
	}
}

func TestSetCustomerLimitsRejectsInvalidValues(t *testing.T) {
	s := New()
	for _, limits := range []struct {
		bursts int
		hourly float64
	}{{-1, 0}, {0, -1}, {0, math.NaN()}, {0, math.Inf(1)}} {
		if _, _, err := s.SetCustomerLimits(DevCustomerID, limits.bursts, limits.hourly, OperatorActor()); !errors.Is(err, ErrInvalidTenantLimits) {
			t.Errorf("SetCustomerLimits(%d, %v) = %v, want ErrInvalidTenantLimits", limits.bursts, limits.hourly, err)
		}
	}
}

func TestSetCustomerLimitsAtomicPersistenceFailure(t *testing.T) {
	s := New()
	s.persist = &failCustomerPersister{err: errors.New("db error")}

	c := &Customer{
		ID:                  "cust_fail",
		MaxConcurrentBursts: 2,
		MaxHourlyUSD:        5.0,
	}
	s.AddCustomer(c)

	_, _, err := s.SetCustomerLimits("cust_fail", 20, 50.0, OperatorActor())
	if err == nil || !errors.Is(err, ErrPersistence) {
		t.Fatalf("SetCustomerLimits error = %v, want ErrPersistence", err)
	}

	// Verify live memory was NOT mutated after persistence failure
	live, err := s.CustomerByID("cust_fail")
	if err != nil {
		t.Fatal(err)
	}
	if live.MaxConcurrentBursts != 2 || live.MaxHourlyUSD != 5.0 {
		t.Fatalf("live memory mutated after failure: max_concurrent_bursts=%d, max_hourly_usd=%f", live.MaxConcurrentBursts, live.MaxHourlyUSD)
	}
}

func TestTenantLimitsAuditPreservesZeroValuesWithoutPollutingOtherActions(t *testing.T) {
	zeroBursts, bursts := 0, 5
	zeroHourly, hourly := 0.0, 12.5
	encoded, err := json.Marshal(AuditDetail{
		PreviousMaxConcurrentBursts: &zeroBursts,
		MaxConcurrentBursts:         &bursts,
		PreviousMaxHourlyUSD:        &zeroHourly,
		MaxHourlyUSD:                &hourly,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"previous_max_concurrent_bursts", "max_concurrent_bursts", "previous_max_hourly_usd", "max_hourly_usd"} {
		if !strings.Contains(string(encoded), `"`+field+`"`) {
			t.Fatalf("zero-valued limit field %q omitted from %s", field, encoded)
		}
	}
	plain, err := json.Marshal(AuditDetail{Reason: ReasonNamespaceAuthorized})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "max_concurrent_bursts") || strings.Contains(string(plain), "max_hourly_usd") {
		t.Fatalf("unrelated audit detail gained tenant limit fields: %s", plain)
	}
}
