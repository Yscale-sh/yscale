package state

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// nsFailPersister refuses the customer write and accepts everything else.
type nsFailPersister struct {
	gatewayRouteSpyPersister
	err error
}

func (p *nsFailPersister) upsertCustomer(*Customer) error { return p.err }

// upsertCustomerAudited is overridden as well as upsertCustomer: the namespace
// set now rides the audited write, and Go's embedding does not dispatch back to
// an override, so an embedded default would quietly stop exercising the refusal
// this fake exists to script.
func (p *nsFailPersister) upsertCustomerAudited(c *Customer, ev *AuditEvent) error {
	return p.withAudit(ev, func() error { return p.upsertCustomer(c) })
}

type nsBlockingPersister struct {
	gatewayRouteSpyPersister
	entered chan struct{}
	release chan struct{}
}

func (p *nsBlockingPersister) upsertCustomer(c *Customer) error {
	close(p.entered)
	<-p.release
	return p.gatewayRouteSpyPersister.upsertCustomer(c)
}

func (p *nsBlockingPersister) upsertCustomerAudited(c *Customer, ev *AuditEvent) error {
	return p.withAudit(ev, func() error { return p.upsertCustomer(c) })
}

func TestSetCustomerWorkloadNamespacesPersists(t *testing.T) {
	s := New()
	p := &gatewayRouteSpyPersister{}
	s.persist = p

	stored, err := s.SetCustomerWorkloadNamespaces(DevCustomerID, []string{"team-a", "team-b"}, SystemActor())
	if err != nil {
		t.Fatalf("SetCustomerWorkloadNamespaces: %v", err)
	}
	if !slices.Equal(stored, []string{"team-a", "team-b"}) {
		t.Fatalf("returned %v", stored)
	}
	if len(p.customers) != 1 || !slices.Equal(p.customers[0].WorkloadNamespaces, stored) {
		t.Fatalf("persisted %+v", p.customers)
	}

	// The returned slice is the caller's. A handler that rendered it into a
	// helm command and then reused the buffer must not reach the store.
	stored[0] = "mutated"
	c, err := s.CustomerByID(DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.WorkloadNamespaces, []string{"team-a", "team-b"}) {
		t.Fatalf("store aliased the returned slice: %v", c.WorkloadNamespaces)
	}
}

// A refused durable write is returned and never published in memory. This is
// an authorization boundary: publishing the widened set would authorize
// submissions this process allows and the next restart refuses, with the
// connector's RBAC already widened to match the change central lost.
func TestSetCustomerWorkloadNamespacesRollsBackARefusedWrite(t *testing.T) {
	s := New()
	s.persist = &nsFailPersister{err: errors.New("postgres down")}

	if _, err := s.SetCustomerWorkloadNamespaces(DevCustomerID, []string{"team-a"}, SystemActor()); err != nil {
		if !errors.Is(err, ErrPersistence) {
			t.Fatalf("error = %v, want ErrPersistence", err)
		}
	} else {
		t.Fatal("a refused write reported success")
	}

	c, err := s.CustomerByID(DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.WorkloadNamespaces) != 0 {
		t.Fatalf("in-memory record kept the unwritten set: %v", c.WorkloadNamespaces)
	}
}

// The durable write may block while Postgres is slow. Readers must continue to
// see the old authorization set until that write succeeds; publishing first
// creates a window where a workload can use a namespace the database may still
// refuse.
func TestSetCustomerWorkloadNamespacesPublishesAfterPersistence(t *testing.T) {
	s := New()
	p := &nsBlockingPersister{entered: make(chan struct{}), release: make(chan struct{})}
	s.persist = p

	done := make(chan error, 1)
	go func() {
		_, err := s.SetCustomerWorkloadNamespaces(DevCustomerID, []string{"team-a"}, SystemActor())
		done <- err
	}()
	<-p.entered

	c, err := s.CustomerByID(DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.WorkloadNamespaces) != 0 {
		t.Fatalf("uncommitted namespace set became visible: %v", c.WorkloadNamespaces)
	}

	close(p.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	c, err = s.CustomerByID(DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.WorkloadNamespaces, []string{"team-a"}) {
		t.Fatalf("committed namespace set was not published: %v", c.WorkloadNamespaces)
	}
}

// Setting the set a tenant already has is not a change to AUDIT, but it is
// still a write. The durable row can disagree with memory — an env-seeded
// customer's row is written best-effort at boot, and a failure there is logged,
// not returned — and the answer this call compares against comes from MEMORY. A
// version that returned early on "unchanged" therefore made the operator's
// correcting call a permanent no-op: the same list, forever accepted, never
// written, with nothing to show them why the namespaces reverted at the next
// restart.
func TestSetCustomerWorkloadNamespacesReconcilesAnUnchangedSetDurably(t *testing.T) {
	// The durable row is missing and the in-memory record is not: exactly what a
	// seeded customer whose best-effort write failed looks like.
	s := New()
	p := &gatewayRouteSpyPersister{}
	s.persist = p
	s.AddCustomer(&Customer{ID: "cust_seed", Token: "tok", Plan: "pro", WorkloadNamespaces: []string{"batch"}})
	p.customers = nil // that boot write did not land

	stored, err := s.SetCustomerWorkloadNamespaces("cust_seed", []string{"batch"}, OperatorActor())
	if err != nil {
		t.Fatalf("same-value reconcile: %v", err)
	}
	if !slices.Equal(stored, []string{"batch"}) {
		t.Fatalf("returned %v", stored)
	}
	if len(p.customers) != 1 || !slices.Equal(p.customers[0].WorkloadNamespaces, []string{"batch"}) {
		t.Fatalf("the durable row was not reconciled: %+v", p.customers)
	}
	// Written, and not RECORDED: nothing about the boundary moved, so the journal
	// must not claim it did.
	if rows := p.events(); len(rows) != 0 {
		t.Fatalf("audit rows = %+v, want none for a set that did not change", rows)
	}

	// And the reconciling write fails closed like any other: a caller told
	// "updated" whose write never landed is the outcome this whole path exists
	// to prevent.
	s2 := New()
	s2.persist = &nsFailPersister{err: errors.New("postgres down")}
	s2.AddCustomer(&Customer{ID: "cust_down", Token: "tok", Plan: "pro", WorkloadNamespaces: []string{"batch"}})
	if _, err := s2.SetCustomerWorkloadNamespaces("cust_down", []string{"batch"}, OperatorActor()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("error = %v, want ErrPersistence", err)
	}
	// The in-memory set is untouched either way — it was already this value, and
	// a failed reconcile is not a reason to narrow a live tenant.
	c, err := s2.CustomerByID("cust_down")
	if err != nil || !slices.Equal(c.WorkloadNamespaces, []string{"batch"}) {
		t.Fatalf("in-memory set = %+v err=%v", c, err)
	}
}

// A hosted assignment reserves one namespace on the shared cluster and keeps
// it on the tenant's authorized list by construction. A replacement that drops
// it would 403 every hosted submit while the console still offers the target,
// and it would bypass the HostedNamespaceAdded bookkeeping the hosted delete
// releases the grant through — so the omission is refused, named, and nothing
// is written.
func TestSetCustomerWorkloadNamespacesKeepsHostedReservedNamespace(t *testing.T) {
	s := New()
	p := &gatewayRouteSpyPersister{}
	s.persist = p
	s.AddCustomer(&Customer{ID: "cust_h", Token: "tok_h", Plan: "pro", WorkloadNamespaces: []string{"batch"}})
	if _, _, err := s.AssignHostedCluster("cust_h", "hosted-h", "", "ten-h", OperatorActor()); err != nil {
		t.Fatalf("AssignHostedCluster: %v", err)
	}
	written := len(p.customers)

	_, err := s.SetCustomerWorkloadNamespaces("cust_h", []string{"batch", "team-b"}, OperatorActor())
	if !errors.Is(err, ErrHostedNamespaceRequired) {
		t.Fatalf("error = %v, want ErrHostedNamespaceRequired", err)
	}
	if !strings.Contains(err.Error(), "ten-h") {
		t.Fatalf("the refusal does not name the reserved namespace: %v", err)
	}
	c, err := s.CustomerByID("cust_h")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.WorkloadNamespaces, []string{"batch", "ten-h"}) {
		t.Fatalf("a refused replacement changed the set: %v", c.WorkloadNamespaces)
	}
	if len(p.customers) != written {
		t.Fatalf("a refused replacement reached the durable store: %+v", p.customers[written:])
	}
	for _, ev := range p.events() {
		if ev.Action == ActionWorkloadNamespacesSet {
			t.Fatalf("a refused replacement left an audit row: %+v", ev)
		}
	}

	// Keeping the reserved namespace in the replacement is the supported edit.
	stored, err := s.SetCustomerWorkloadNamespaces("cust_h", []string{"batch", "team-b", "ten-h"}, OperatorActor())
	if err != nil {
		t.Fatalf("replacement keeping the reserved namespace: %v", err)
	}
	if !slices.Equal(stored, []string{"batch", "team-b", "ten-h"}) {
		t.Fatalf("stored = %v", stored)
	}

	// Deleting the assignment releases the reservation and the requirement
	// with it: the same narrowed set the store just refused is now accepted.
	if _, err := s.DeleteHostedCluster("cust_h", "hosted-h", OperatorActor()); err != nil {
		t.Fatalf("DeleteHostedCluster: %v", err)
	}
	if _, err := s.SetCustomerWorkloadNamespaces("cust_h", []string{"batch", "team-b"}, OperatorActor()); err != nil {
		t.Fatalf("post-delete replacement: %v", err)
	}
}

// An unknown tenant and a revoked one answer the same way: there is no live
// tenant to authorize anything for.
func TestSetCustomerWorkloadNamespacesRefusesUnknownAndRevoked(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cust_gone", Token: "tok", Plan: "pro"})
	if err := s.RevokeCustomer("cust_gone"); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"cust_missing", "cust_gone"} {
		if _, err := s.SetCustomerWorkloadNamespaces(id, []string{"team-a"}, SystemActor()); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: error = %v, want ErrNotFound", id, err)
		}
	}
}

// The env-seed path re-adds its customers from config on every boot, and that
// config has never heard of the operator's update. Losing it there would
// silently narrow a live tenant back to the fail-closed default at the next
// restart.
func TestReSeedingKeepsOperatorSetWorkloadNamespaces(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cust_seeded", Token: "tok", Plan: "pro"})

	if _, err := s.SetCustomerWorkloadNamespaces("cust_seeded", []string{"batch"}, SystemActor()); err != nil {
		t.Fatal(err)
	}
	s.AddCustomer(&Customer{ID: "cust_seeded", Token: "tok", Plan: "pro"})

	c, err := s.CustomerByID("cust_seeded")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.WorkloadNamespaces, []string{"batch"}) {
		t.Fatalf("re-seed reverted the namespaces: %v", c.WorkloadNamespaces)
	}
}
