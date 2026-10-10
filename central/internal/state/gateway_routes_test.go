package state

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// claimCluster is the registration the agent stream performs on every connect,
// before it will accept a single report from that connection. It is what makes
// a cluster CURRENTLY one of the tenant's, which SetCustomerGatewayRoutes
// requires — so a test that seeds route intent goes through it for the same
// reason production does.
// An EXISTING row — API-registered or platform-managed — is claimed the way its
// connector's cluster-scoped credential claims it; a cluster with no row yet is
// the self-claiming path, which creates one.
func claimCluster(t *testing.T, s *Store, customerID, clusterID string) {
	t.Helper()
	_, _, err := s.ClaimAgentCluster(customerID, clusterID, true, time.Now().UTC())
	if errors.Is(err, ErrClusterNotFound) {
		_, _, err = s.ClaimAgentCluster(customerID, clusterID, false, time.Now().UTC())
	}
	if err != nil {
		t.Fatalf("ClaimAgentCluster %s/%s: %v", customerID, clusterID, err)
	}
}

func TestSetCustomerGatewayRoutes(t *testing.T) {
	s := New()
	claimCluster(t, s, "cust_test", "prod-1")
	p := &gatewayRouteSpyPersister{}
	s.persist = p

	changed, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{
		"10.96.0.10/12",
		"10.42.1.1/16",
		"10.42.0.0/16",
	})
	if err != nil || !changed {
		t.Fatalf("first SetCustomerGatewayRoutes changed=%v err=%v", changed, err)
	}
	want := []string{"10.42.0.0/16", "10.96.0.0/12"}
	assertCustomerGatewayRoutes(t, s, "cust_test", want)
	assertClusterGatewayRoutes(t, s, "cust_test", "prod-1", want)
	if len(p.customers) != 1 {
		t.Fatalf("persisted customers = %d, want 1", len(p.customers))
	}
	if !reflect.DeepEqual(p.customers[0].GatewayRoutes, want) {
		t.Fatalf("persisted GatewayRoutes = %v, want %v", p.customers[0].GatewayRoutes, want)
	}
	if !reflect.DeepEqual(p.customers[0].GatewayRoutesByCluster, map[string][]string{"prod-1": want}) {
		t.Fatalf("persisted GatewayRoutesByCluster = %v, want prod-1 -> %v",
			p.customers[0].GatewayRoutesByCluster, want)
	}

	changed, err = s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{
		"10.96.0.1/12",
		"10.42.0.99/16",
	})
	if err != nil {
		t.Fatalf("same canonical SetCustomerGatewayRoutes: %v", err)
	}
	if changed {
		t.Fatal("same canonical route set should not report changed")
	}
	if len(p.customers) != 1 {
		t.Fatalf("unchanged route set persisted; count = %d, want 1", len(p.customers))
	}

	changed, err = s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{
		"10.96.0.0/12",
		"10.42.0.0/16",
		"192.168.10.99/24",
	})
	if err != nil || !changed {
		t.Fatalf("different SetCustomerGatewayRoutes changed=%v err=%v", changed, err)
	}
	want = []string{"10.42.0.0/16", "10.96.0.0/12", "192.168.10.0/24"}
	assertCustomerGatewayRoutes(t, s, "cust_test", want)
	assertClusterGatewayRoutes(t, s, "cust_test", "prod-1", want)
	if len(p.customers) != 2 {
		t.Fatalf("persisted customers = %d, want 2", len(p.customers))
	}

	changed, err = s.SetCustomerGatewayRoutes("cust_test", "prod-1", nil)
	if err != nil {
		t.Fatalf("empty SetCustomerGatewayRoutes: %v", err)
	}
	if changed {
		t.Fatal("empty route report should not report changed")
	}
	assertCustomerGatewayRoutes(t, s, "cust_test", want)
	assertClusterGatewayRoutes(t, s, "cust_test", "prod-1", want)
	if len(p.customers) != 2 {
		t.Fatalf("empty route report persisted; count = %d, want 2", len(p.customers))
	}

	changed, err = s.SetCustomerGatewayRoutes("missing", "prod-1", []string{"10.42.0.0/16"})
	if err != ErrNotFound || changed {
		t.Fatalf("missing customer changed=%v err=%v, want changed=false ErrNotFound", changed, err)
	}
}

// A route set with no cluster attached names no gateway, so there is nothing it
// could be converged onto. Accepting it into the union alone would put CIDRs
// into the tenant's policy that no gateway is ever approved for.
func TestSetCustomerGatewayRoutesRequiresACluster(t *testing.T) {
	s := New()
	changed, err := s.SetCustomerGatewayRoutes("cust_test", "", []string{"10.42.0.0/16"})
	if changed || !errors.Is(err, ErrInvalidCluster) {
		t.Fatalf("empty cluster id changed=%v err=%v, want changed=false ErrInvalidCluster", changed, err)
	}
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", "", []string{"10.42.0.0/16"}); !errors.Is(err, ErrInvalidCluster) {
		t.Fatalf("empty cluster id on the pushed setter err=%v, want ErrInvalidCluster", err)
	}
}

// Route intent is keyed by a cluster id, so the same ownership line every other
// cluster path draws applies here: a tenant cannot write intent under an id
// another tenant holds.
func TestSetCustomerGatewayRoutesRefusesAnotherTenantsCluster(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cust_a", Token: "a"})
	s.AddCustomer(&Customer{ID: "cust_b", Token: "b"})
	if _, _, err := s.ClaimAgentCluster("cust_a", "prod-1", false, time.Now().UTC()); err != nil {
		t.Fatalf("ClaimAgentCluster: %v", err)
	}
	changed, err := s.SetCustomerGatewayRoutes("cust_b", "prod-1", []string{"10.42.0.0/16"})
	if changed || !errors.Is(err, ErrClusterOwnedElsewhere) {
		t.Fatalf("cross-tenant cluster id changed=%v err=%v, want ErrClusterOwnedElsewhere", changed, err)
	}
	c, err := s.CustomerByID("cust_b")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if len(c.GatewayRoutes) != 0 || len(c.GatewayRoutesByCluster) != 0 {
		t.Fatalf("refused report still wrote intent: union=%v byCluster=%v", c.GatewayRoutes, c.GatewayRoutesByCluster)
	}
}

// A report already read off a socket the delete evicted must FAIL once the
// registry row is gone. "Not owned by anyone else" would let it through — a
// deleted cluster is owned by nobody — and it would then recreate the
// by-cluster entry the delete just dropped and widen the tenant's policy union
// back to include a cluster central has agreed no longer exists. Nothing would
// ever remove it again: the connector is what was deleted, so no smaller report
// is coming.
func TestSetCustomerGatewayRoutesRefusesReportAfterClusterDelete(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	owner := ids["alice"]
	registerCluster(t, s, "cust_r", owner, "cl-a", "A")
	registerCluster(t, s, "cust_r", owner, "cl-b", "B")
	aRoutes, bRoutes := []string{"10.42.0.0/16"}, []string{"10.43.0.0/16"}
	seedClusterRouteIntent(t, s, "cust_r", "cl-a", aRoutes)
	seedClusterRouteIntent(t, s, "cust_r", "cl-b", bRoutes)

	if _, err := s.DeleteTenantCluster("cust_r", owner, "cl-a", HumanActor(owner, "cust_r")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// The socket was evicted by the same mutation, but a frame already read off
	// it is still in flight on the stream goroutine. This is that frame.
	changed, err := s.SetCustomerGatewayRoutes("cust_r", "cl-a", []string{"10.42.0.0/16", "10.99.0.0/16"})
	if changed || !errors.Is(err, ErrClusterNotFound) {
		t.Fatalf("late report changed=%v err=%v, want changed=false ErrClusterNotFound", changed, err)
	}

	intent, err := s.CustomerGatewayRouteIntent("cust_r")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if _, ok := intent.RoutesFor("cl-a"); ok {
		t.Fatalf("the late report recreated the deleted cluster's desired set: %v", intent.ByCluster)
	}
	if !reflect.DeepEqual(intent.Union, bRoutes) {
		t.Fatalf("union = %v, want only the surviving cluster's %v — the late report widened the policy",
			intent.Union, bRoutes)
	}
	// The withdrawal tombstone is untouched, so the reconcile still knows there
	// is an approval to take back off that gateway.
	if !reflect.DeepEqual(intent.PushedRoutesFor("cl-a"), aRoutes) {
		t.Fatalf("pushed tombstone = %v, want %v", intent.PushedRoutesFor("cl-a"), aRoutes)
	}
	// The surviving cluster is unaffected: this is a liveness check on the named
	// cluster, not a freeze on the tenant.
	if _, err := s.SetCustomerGatewayRoutes("cust_r", "cl-b", []string{"10.43.0.0/16", "10.44.0.0/16"}); err != nil {
		t.Fatalf("report from a cluster that still exists: %v", err)
	}
}

// A legacy connector claims no registry row — its self-chosen id is outside the
// grammar, so ClaimAgentCluster admits it live-only — and its reports must
// still land. The live socket is the whole of its claim, which is exactly what
// the delete-vs-late-report check reads.
func TestSetCustomerGatewayRoutesAcceptsLegacyClaimedConnector(t *testing.T) {
	s := New()
	const legacyID = "Prod Cluster #1" // outside ValidClusterID on purpose
	if ValidClusterID(legacyID) {
		t.Fatalf("%q is inside the grammar; pick an id that is not", legacyID)
	}
	if claimed, _, err := s.ClaimAgentCluster("cust_test", legacyID, false, time.Now().UTC()); err != nil || claimed {
		t.Fatalf("legacy claim claimed=%v err=%v, want claimed=false and no error", claimed, err)
	}

	// No registry row was created, so only the socket can prove the claim.
	c, err := s.CustomerByID("cust_test")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if len(c.RegisteredClusters) != 0 {
		t.Fatalf("legacy claim created a registry row: %+v", c.RegisteredClusters)
	}
	if _, err := s.SetCustomerGatewayRoutes("cust_test", legacyID, []string{"10.42.0.0/16"}); !errors.Is(err, ErrClusterNotFound) {
		t.Fatalf("report before the socket exists err=%v, want ErrClusterNotFound", err)
	}

	s.AddAgent(&Agent{ID: "agent_legacy", CustomerID: "cust_test", ClusterID: legacyID, ConnectedAt: time.Now().UTC()})
	changed, err := s.SetCustomerGatewayRoutes("cust_test", legacyID, []string{"10.42.0.0/16"})
	if err != nil || !changed {
		t.Fatalf("legacy connector report changed=%v err=%v, want it accepted", changed, err)
	}
	assertClusterGatewayRoutes(t, s, "cust_test", legacyID, []string{"10.42.0.0/16"})
}

// The tenant-wide union is RECOMPUTED from every cluster's stored set, so it
// grows when a second cluster reports, shrinks when one of them drops a CIDR,
// and never carries a route no cluster is currently advertising.
func TestSetCustomerGatewayRoutesRecomputesTheUnion(t *testing.T) {
	s := New()
	claimCluster(t, s, "cust_test", "prod-1")
	claimCluster(t, s, "cust_test", "prod-2")
	aRoutes := []string{"10.42.0.0/16", "10.96.0.0/12"}
	bRoutes := []string{"10.43.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", aRoutes); err != nil {
		t.Fatalf("set prod-1: %v", err)
	}
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-2", bRoutes); err != nil {
		t.Fatalf("set prod-2: %v", err)
	}
	assertCustomerGatewayRoutes(t, s, "cust_test", []string{"10.42.0.0/16", "10.43.0.0/16", "10.96.0.0/12"})

	// A shrink on prod-1 leaves prod-2's set exactly as reported and drops the
	// withdrawn CIDR from the union.
	changed, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{"10.42.0.0/16"})
	if err != nil || !changed {
		t.Fatalf("shrink prod-1 changed=%v err=%v", changed, err)
	}
	assertClusterGatewayRoutes(t, s, "cust_test", "prod-1", []string{"10.42.0.0/16"})
	assertClusterGatewayRoutes(t, s, "cust_test", "prod-2", bRoutes)
	assertCustomerGatewayRoutes(t, s, "cust_test", []string{"10.42.0.0/16", "10.43.0.0/16"})

	// Overlapping reports collapse into one canonical union entry, and the
	// per-cluster sets stay whole.
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-2", []string{"10.42.0.0/16", "10.43.0.0/16"}); err != nil {
		t.Fatalf("overlap prod-2: %v", err)
	}
	assertCustomerGatewayRoutes(t, s, "cust_test", []string{"10.42.0.0/16", "10.43.0.0/16"})
	assertClusterGatewayRoutes(t, s, "cust_test", "prod-2", []string{"10.42.0.0/16", "10.43.0.0/16"})
}

// A legacy customer's union is the only route intent there is until a live
// cluster reports. The first report REPLACES it with what the fleet actually
// advertises rather than merging a set nobody can attribute to a gateway.
//
// On a multi-cluster legacy tenant that narrows the union until the tenant's
// other clusters report too — each connector sends a report right after it
// connects, so the window is one reconnect wide. That is the pre-existing
// last-writer-wins shape of a tenant-scoped union, not something the
// by-cluster map introduced, and it is the safe direction: an unattributed
// legacy CIDR belongs to no gateway, so keeping it would mean approving it on
// whichever node happened to report first.
func TestSetCustomerGatewayRoutesFirstReportSupersedesLegacyUnion(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cust_legacy", Token: "legacy", GatewayRoutes: []string{"10.99.0.0/16"}})
	claimCluster(t, s, "cust_legacy", "prod-1")
	claimCluster(t, s, "cust_legacy", "prod-2")
	if _, err := s.SetCustomerGatewayRoutes("cust_legacy", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("first report: %v", err)
	}
	assertCustomerGatewayRoutes(t, s, "cust_legacy", []string{"10.42.0.0/16"})
	assertClusterGatewayRoutes(t, s, "cust_legacy", "prod-1", []string{"10.42.0.0/16"})

	// The legacy CIDR is not attributed to prod-1 on the way past: its gateway
	// gets exactly what prod-1 reported, and the second cluster's report widens
	// the union back without either node ever carrying the other's set.
	if _, err := s.SetCustomerGatewayRoutes("cust_legacy", "prod-2", []string{"10.43.0.0/16"}); err != nil {
		t.Fatalf("second report: %v", err)
	}
	assertCustomerGatewayRoutes(t, s, "cust_legacy", []string{"10.42.0.0/16", "10.43.0.0/16"})
	assertClusterGatewayRoutes(t, s, "cust_legacy", "prod-1", []string{"10.42.0.0/16"})
	assertClusterGatewayRoutes(t, s, "cust_legacy", "prod-2", []string{"10.43.0.0/16"})
	intent, err := s.CustomerGatewayRouteIntent("cust_legacy")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	for clusterID, routes := range intent.ByCluster {
		for _, route := range routes {
			if route == "10.99.0.0/16" {
				t.Fatalf("legacy CIDR was attributed to %s; no gateway may inherit an unowned route", clusterID)
			}
		}
	}
}

func TestSetCustomerPushedGatewayRoutes(t *testing.T) {
	s := New()
	p := &gatewayRouteSpyPersister{}
	s.persist = p

	if err := s.SetCustomerPushedGatewayRoutes("cust_test", []string{
		"192.168.5.9/24",
		"10.42.1.1/16",
	}); err != nil {
		t.Fatalf("SetCustomerPushedGatewayRoutes: %v", err)
	}
	c, err := s.CustomerByID("cust_test")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	want := []string{"10.42.0.0/16", "192.168.5.0/24"}
	if !reflect.DeepEqual(c.PushedGatewayRoutes, want) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v", c.PushedGatewayRoutes, want)
	}
	if len(p.customers) != 1 || !reflect.DeepEqual(p.customers[0].PushedGatewayRoutes, want) {
		t.Fatalf("persisted PushedGatewayRoutes = %v, count=%d; want %v count=1",
			p.lastCustomerPushedRoutes(), len(p.customers), want)
	}
	// An already-current union is written AGAIN, and that is the point: the
	// in-memory value says nothing about the durable one, so a row whose write
	// was refused — or torn by a crash mid-upsert — is only ever repaired by a
	// re-assert that does not check first. This is the one convergence loop with
	// no other way back, and the write it costs is idempotent.
	if err := s.SetCustomerPushedGatewayRoutes("cust_test", []string{
		"10.42.0.99/16",
		"192.168.5.200/24",
	}); err != nil {
		t.Fatalf("repeat SetCustomerPushedGatewayRoutes: %v", err)
	}
	if len(p.customers) != 2 {
		t.Fatalf("identical pushed union not re-persisted; count = %d, want 2 (durable self-heal)", len(p.customers))
	}
	if !reflect.DeepEqual(p.customers[1].PushedGatewayRoutes, want) {
		t.Fatalf("self-heal wrote %v, want the canonical %v", p.customers[1].PushedGatewayRoutes, want)
	}
	// A real change still writes.
	if err := s.SetCustomerPushedGatewayRoutes("cust_test", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("changed SetCustomerPushedGatewayRoutes: %v", err)
	}
	if len(p.customers) != 3 {
		t.Fatalf("changed pushed union not persisted; count = %d, want 3", len(p.customers))
	}
	if err := s.SetCustomerPushedGatewayRoutes("missing", []string{"10.42.0.0/16"}); err != ErrNotFound {
		t.Fatalf("missing customer err=%v, want ErrNotFound", err)
	}
}

// The pushed-gateway tombstone a delete leaves behind is cleared once the
// withdrawal lands, so the map cannot grow by one dead entry per cluster the
// tenant ever deletes. Clearing what is already absent costs no durable write.
func TestClearCustomerPushedClusterGatewayRoutes(t *testing.T) {
	s := New()
	p := &gatewayRouteSpyPersister{}
	s.persist = p

	aRoutes, bRoutes := []string{"10.42.0.0/16"}, []string{"10.43.0.0/16"}
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", "prod-1", aRoutes); err != nil {
		t.Fatalf("record prod-1: %v", err)
	}
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", "prod-2", bRoutes); err != nil {
		t.Fatalf("record prod-2: %v", err)
	}
	if err := s.ClearCustomerPushedClusterGatewayRoutes("cust_test", "prod-1"); err != nil {
		t.Fatalf("clear prod-1: %v", err)
	}
	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if _, ok := intent.PushedByCluster["prod-1"]; ok {
		t.Fatalf("prod-1 tombstone survived the clear: %v", intent.PushedByCluster)
	}
	if !reflect.DeepEqual(intent.PushedRoutesFor("prod-2"), bRoutes) {
		t.Fatalf("prod-2 pushed set = %v, want the sibling untouched", intent.PushedRoutesFor("prod-2"))
	}
	if len(p.customers) != 3 {
		t.Fatalf("durable writes = %d, want 3 (two records plus the clear)", len(p.customers))
	}

	// Idempotent: a second clear, and a clear of a cluster that never had an
	// entry, are both already-true and write nothing.
	if err := s.ClearCustomerPushedClusterGatewayRoutes("cust_test", "prod-1"); err != nil {
		t.Fatalf("second clear: %v", err)
	}
	if err := s.ClearCustomerPushedClusterGatewayRoutes("cust_test", "never-reported"); err != nil {
		t.Fatalf("clear of an absent cluster: %v", err)
	}
	if len(p.customers) != 3 {
		t.Fatalf("redundant clear persisted; count = %d, want 3", len(p.customers))
	}

	// The last entry leaves the map nil rather than empty, so nothing lingers
	// in the persisted document.
	if err := s.ClearCustomerPushedClusterGatewayRoutes("cust_test", "prod-2"); err != nil {
		t.Fatalf("clear prod-2: %v", err)
	}
	if got := p.customers[len(p.customers)-1].PushedGatewayRoutesByCluster; got != nil {
		t.Fatalf("persisted PushedGatewayRoutesByCluster = %v, want nil once empty", got)
	}
	if err := s.ClearCustomerPushedClusterGatewayRoutes("missing", "prod-1"); err != ErrNotFound {
		t.Fatalf("missing customer err=%v, want ErrNotFound", err)
	}
	if err := s.ClearCustomerPushedClusterGatewayRoutes("cust_test", ""); !errors.Is(err, ErrInvalidCluster) {
		t.Fatalf("empty cluster id err=%v, want ErrInvalidCluster", err)
	}
}

// A cluster's LIFECYCLE DELETE takes its route intent with it, in the SAME
// durable customer document as the registry row: the by-cluster set goes, the
// tenant-wide policy union shrinks to what the remaining clusters advertise,
// and the PUSHED entry stays behind — it is the only record of what central
// approved on that gateway node, and the withdrawal has nothing to drive from
// once the connector is gone.
func TestDeleteTenantClusterWithdrawsRouteIntent(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	registerCluster(t, s, "cust_r", ids["alice"], "cl-a", "A")
	registerCluster(t, s, "cust_r", ids["alice"], "cl-b", "B")
	aRoutes, bRoutes := []string{"10.42.0.0/16"}, []string{"10.43.0.0/16"}
	seedClusterRouteIntent(t, s, "cust_r", "cl-a", aRoutes)
	seedClusterRouteIntent(t, s, "cust_r", "cl-b", bRoutes)
	if err := s.SetCustomerPushedGatewayRoutes("cust_r", []string{"10.42.0.0/16", "10.43.0.0/16"}); err != nil {
		t.Fatalf("record pushed union: %v", err)
	}

	// From here the spy sees exactly the delete's durable writes.
	p := &gatewayRouteSpyPersister{}
	s.persist = struct {
		persister
		humanAuthorizationReader
	}{p, s.persist.(humanAuthorizationReader)}
	if _, err := s.DeleteTenantCluster("cust_r", ids["alice"], "cl-a", HumanActor(ids["alice"], "cust_r")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	assertClusterDeleteWithdrewIntent(t, s, p, "cust_r", "cl-a", aRoutes, bRoutes)
}

// The operator's hosted release is the same lifecycle delete and owes the same
// withdrawal: a released assignment whose CIDRs stayed in policy would keep a
// tenant's coordination server approving routes for capacity they no longer
// have.
func TestDeleteHostedClusterWithdrawsRouteIntent(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro"})
	if _, _, err := s.AssignHostedCluster("cust_h", "hosted-a", "Hosted A", "ys-a", OperatorActor()); err != nil {
		t.Fatalf("assign hosted-a: %v", err)
	}
	aRoutes, bRoutes := []string{"10.42.0.0/16"}, []string{"10.43.0.0/16"}
	seedClusterRouteIntent(t, s, "cust_h", "hosted-a", aRoutes)
	seedClusterRouteIntent(t, s, "cust_h", "own-b", bRoutes)

	p := &gatewayRouteSpyPersister{}
	s.persist = p
	if _, err := s.DeleteHostedCluster("cust_h", "hosted-a", OperatorActor()); err != nil {
		t.Fatalf("delete hosted: %v", err)
	}
	assertClusterDeleteWithdrewIntent(t, s, p, "cust_h", "hosted-a", aRoutes, bRoutes)
}

// A refused durable write leaves the route intent exactly where it was. The
// registry row and the routes ride one document, so a delete that half-applied
// in memory would have a tenant whose policy dropped CIDRs the durable copy
// still says are theirs — and the next restart would put them back.
func TestDeleteTenantClusterPersistenceFailureKeepsRouteIntent(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{"alice": RoleOwner})
	registerCluster(t, s, "cust_r", ids["alice"], "cl-a", "A")
	registerCluster(t, s, "cust_r", ids["alice"], "cl-b", "B")
	aRoutes, bRoutes := []string{"10.42.0.0/16"}, []string{"10.43.0.0/16"}
	seedClusterRouteIntent(t, s, "cust_r", "cl-a", aRoutes)
	seedClusterRouteIntent(t, s, "cust_r", "cl-b", bRoutes)

	s.persist = &refusingCustomerPersister{err: errors.New("postgres down")}
	if _, err := s.DeleteTenantCluster("cust_r", ids["alice"], "cl-a", HumanActor(ids["alice"], "cust_r")); !errors.Is(err, ErrPersistence) {
		t.Fatalf("delete err = %v, want ErrPersistence", err)
	}
	intent, err := s.CustomerGatewayRouteIntent("cust_r")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if routes, ok := intent.RoutesFor("cl-a"); !ok || !reflect.DeepEqual(routes, aRoutes) {
		t.Fatalf("cl-a desired set = %v ok=%v, want %v kept after a refused write", routes, ok, aRoutes)
	}
	wantUnion := []string{"10.42.0.0/16", "10.43.0.0/16"}
	if !reflect.DeepEqual(intent.Union, wantUnion) {
		t.Fatalf("union = %v, want %v kept after a refused write", intent.Union, wantUnion)
	}
	if !reflect.DeepEqual(intent.PushedRoutesFor("cl-a"), aRoutes) {
		t.Fatalf("cl-a tombstone = %v, want %v", intent.PushedRoutesFor("cl-a"), aRoutes)
	}
	if _, ok := s.CustomerForClusterID("cl-a"); !ok {
		t.Fatal("cluster ownership released by a delete that never persisted")
	}
}

// seedClusterRouteIntent gives one cluster a reported set AND records it as
// approved on that cluster's gateway node — the state a delete has to unwind.
func seedClusterRouteIntent(t *testing.T, s *Store, customerID, clusterID string, routes []string) {
	t.Helper()
	claimCluster(t, s, customerID, clusterID)
	if _, err := s.SetCustomerGatewayRoutes(customerID, clusterID, routes); err != nil {
		t.Fatalf("SetCustomerGatewayRoutes %s: %v", clusterID, err)
	}
	if err := s.SetCustomerPushedClusterGatewayRoutes(customerID, clusterID, routes); err != nil {
		t.Fatalf("SetCustomerPushedClusterGatewayRoutes %s: %v", clusterID, err)
	}
}

// assertClusterDeleteWithdrewIntent is the contract both delete paths owe: the
// deleted cluster's desired set is gone, the union is exactly the survivor's,
// the pushed tombstone is kept, and all of it landed in ONE durable document
// alongside the registry row removal.
func assertClusterDeleteWithdrewIntent(t *testing.T, s *Store, p *gatewayRouteSpyPersister,
	customerID, deleted string, deletedRoutes, survivingRoutes []string) {
	t.Helper()
	intent, err := s.CustomerGatewayRouteIntent(customerID)
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if _, ok := intent.RoutesFor(deleted); ok {
		t.Fatalf("deleted cluster still has a desired set: %v", intent.ByCluster)
	}
	if !reflect.DeepEqual(intent.Union, survivingRoutes) {
		t.Fatalf("union after delete = %v, want only the surviving cluster's %v", intent.Union, survivingRoutes)
	}
	if !reflect.DeepEqual(intent.PushedRoutesFor(deleted), deletedRoutes) {
		t.Fatalf("pushed tombstone = %v, want %v kept so the node approval can still be withdrawn",
			intent.PushedRoutesFor(deleted), deletedRoutes)
	}

	if len(p.customers) != 1 {
		t.Fatalf("durable writes for the delete = %d, want exactly 1", len(p.customers))
	}
	wrote := p.customers[0]
	if _, ok := wrote.GatewayRoutesByCluster[deleted]; ok {
		t.Fatalf("persisted document still names the deleted cluster: %v", wrote.GatewayRoutesByCluster)
	}
	if !reflect.DeepEqual(wrote.GatewayRoutes, survivingRoutes) {
		t.Fatalf("persisted union = %v, want %v", wrote.GatewayRoutes, survivingRoutes)
	}
	if !reflect.DeepEqual(wrote.PushedGatewayRoutesByCluster[deleted], deletedRoutes) {
		t.Fatalf("persisted tombstone = %v, want it preserved", wrote.PushedGatewayRoutesByCluster[deleted])
	}
	for _, row := range wrote.RegisteredClusters {
		if row.ClusterID == deleted {
			t.Fatal("the same document that dropped the routes still carries the deleted registry row")
		}
	}
}

// refusingCustomerPersister fails the audited customer write both delete paths
// go through, leaving every other persister method the spy's.
type refusingCustomerPersister struct {
	gatewayRouteSpyPersister
	err error
}

func (p *refusingCustomerPersister) upsertCustomerAudited(*Customer, *AuditEvent) error {
	return p.err
}

// Each gateway's convergence state is written on its own. A cluster whose node
// converged must not carry a sibling's set, and a repeat must not churn the
// durable row.
func TestSetCustomerPushedClusterGatewayRoutes(t *testing.T) {
	s := New()
	p := &gatewayRouteSpyPersister{}
	s.persist = p

	aRoutes := []string{"10.42.0.0/16"}
	bRoutes := []string{"10.43.0.0/16"}
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", "prod-1", []string{"10.42.1.1/16"}); err != nil {
		t.Fatalf("record prod-1: %v", err)
	}
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", "prod-2", bRoutes); err != nil {
		t.Fatalf("record prod-2: %v", err)
	}
	c, err := s.CustomerByID("cust_test")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if !reflect.DeepEqual(c.PushedGatewayRoutesByCluster, map[string][]string{"prod-1": aRoutes, "prod-2": bRoutes}) {
		t.Fatalf("PushedGatewayRoutesByCluster = %v, want prod-1 %v / prod-2 %v",
			c.PushedGatewayRoutesByCluster, aRoutes, bRoutes)
	}
	// The tenant-wide union is a separate result and must not be moved by a
	// per-gateway record.
	if len(c.PushedGatewayRoutes) != 0 {
		t.Fatalf("PushedGatewayRoutes = %v, want empty (only the node halves were recorded)", c.PushedGatewayRoutes)
	}
	if len(p.customers) != 2 {
		t.Fatalf("persisted customers = %d, want 2", len(p.customers))
	}
	if !reflect.DeepEqual(p.customers[1].PushedGatewayRoutesByCluster, map[string][]string{"prod-1": aRoutes, "prod-2": bRoutes}) {
		t.Fatalf("persisted PushedGatewayRoutesByCluster = %v", p.customers[1].PushedGatewayRoutesByCluster)
	}

	// Same canonical set again: no write.
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", "prod-1", []string{"10.42.0.99/16"}); err != nil {
		t.Fatalf("repeat prod-1: %v", err)
	}
	if len(p.customers) != 2 {
		t.Fatalf("unchanged pushed set persisted; count = %d, want 2", len(p.customers))
	}
	if err := s.SetCustomerPushedClusterGatewayRoutes("missing", "prod-1", aRoutes); err != ErrNotFound {
		t.Fatalf("missing customer err=%v, want ErrNotFound", err)
	}
}

// The intent snapshot is a COPY: a reconcile ranging it off the live record
// would race the next report writing the map.
func TestCustomerGatewayRouteIntentIsACopy(t *testing.T) {
	s := New()
	claimCluster(t, s, "cust_test", "prod-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("set routes: %v", err)
	}
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("record pushed: %v", err)
	}
	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	routes, ok := intent.RoutesFor("prod-1")
	if !ok || !reflect.DeepEqual(routes, []string{"10.42.0.0/16"}) {
		t.Fatalf("RoutesFor(prod-1) = %v ok=%v", routes, ok)
	}
	if _, ok := intent.RoutesFor("prod-2"); ok {
		t.Fatal("RoutesFor reported a set for a cluster that never reported")
	}
	if !reflect.DeepEqual(intent.PushedRoutesFor("prod-1"), []string{"10.42.0.0/16"}) {
		t.Fatalf("PushedRoutesFor(prod-1) = %v", intent.PushedRoutesFor("prod-1"))
	}

	// Mutating the snapshot must not reach the store.
	intent.ByCluster["prod-1"] = []string{"192.168.0.0/24"}
	intent.ByCluster["prod-3"] = []string{"172.16.0.0/16"}
	intent.Union[0] = "0.0.0.0/0"
	assertClusterGatewayRoutes(t, s, "cust_test", "prod-1", []string{"10.42.0.0/16"})
	assertCustomerGatewayRoutes(t, s, "cust_test", []string{"10.42.0.0/16"})
	c, err := s.CustomerByID("cust_test")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if _, leaked := c.GatewayRoutesByCluster["prod-3"]; leaked {
		t.Fatal("a snapshot insert reached the stored map")
	}
	if _, err := s.CustomerGatewayRouteIntent("missing"); err != ErrNotFound {
		t.Fatalf("intent for a missing customer err=%v, want ErrNotFound", err)
	}
}

func TestAddCustomerPreservesGatewayRoutes(t *testing.T) {
	s := New()
	const id = "cust_routes"
	gateway := []string{"10.42.0.0/16"}
	pushed := []string{"10.96.0.0/12"}
	byCluster := map[string][]string{"prod-1": {"10.42.0.0/16"}}
	pushedByCluster := map[string][]string{"prod-1": {"10.96.0.0/12"}}
	s.AddCustomer(&Customer{
		ID: id, Token: "tok-routes", Plan: "pro",
		GatewayRoutes: gateway, PushedGatewayRoutes: pushed,
		GatewayRoutesByCluster: byCluster, PushedGatewayRoutesByCluster: pushedByCluster,
	})

	// The env-seed path re-adds its customers from config on every boot and
	// knows nothing about what the fleet has reported since.
	s.AddCustomer(&Customer{ID: id, Token: "tok-routes", Plan: "pro"})

	c, err := s.CustomerByID(id)
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if !reflect.DeepEqual(c.GatewayRoutes, gateway) {
		t.Fatalf("GatewayRoutes = %v, want %v", c.GatewayRoutes, gateway)
	}
	if !reflect.DeepEqual(c.PushedGatewayRoutes, pushed) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v", c.PushedGatewayRoutes, pushed)
	}
	if !reflect.DeepEqual(c.GatewayRoutesByCluster, byCluster) {
		t.Fatalf("GatewayRoutesByCluster = %v, want %v", c.GatewayRoutesByCluster, byCluster)
	}
	if !reflect.DeepEqual(c.PushedGatewayRoutesByCluster, pushedByCluster) {
		t.Fatalf("PushedGatewayRoutesByCluster = %v, want %v", c.PushedGatewayRoutesByCluster, pushedByCluster)
	}

	// A carried-over map is a copy, not the previous record's: writing through
	// one record must not mutate the other's view.
	claimCluster(t, s, id, "prod-2")
	if _, err := s.SetCustomerGatewayRoutes(id, "prod-2", []string{"10.43.0.0/16"}); err != nil {
		t.Fatalf("report after re-seed: %v", err)
	}
	if !reflect.DeepEqual(byCluster, map[string][]string{"prod-1": {"10.42.0.0/16"}}) {
		t.Fatalf("the seeded map was mutated in place: %v", byCluster)
	}
}

// An additive JSON round trip: a document written before these fields existed
// loads with nil maps and a union, which is exactly the legacy shape the boot
// reconcile is built for.
func TestCustomerRouteFieldsAreJSONAdditive(t *testing.T) {
	var legacy Customer
	const doc = `{"ID":"cust_legacy","Token":"t","GatewayRoutes":["10.42.0.0/16"],"PushedGatewayRoutes":["10.42.0.0/16"]}`
	if err := json.Unmarshal([]byte(doc), &legacy); err != nil {
		t.Fatalf("unmarshal legacy customer: %v", err)
	}
	if !reflect.DeepEqual(legacy.GatewayRoutes, []string{"10.42.0.0/16"}) {
		t.Fatalf("GatewayRoutes = %v", legacy.GatewayRoutes)
	}
	if legacy.GatewayRoutesByCluster != nil || legacy.PushedGatewayRoutesByCluster != nil {
		t.Fatalf("by-cluster maps = %v / %v, want nil for a pre-field document",
			legacy.GatewayRoutesByCluster, legacy.PushedGatewayRoutesByCluster)
	}

	// And a customer with nothing to say about routes still marshals without
	// the new keys, so old replicas read what they always read.
	data, err := json.Marshal(&Customer{ID: "cust_empty", Token: "t"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "ByCluster") {
		t.Fatalf("empty customer marshalled by-cluster keys: %s", data)
	}

	// Round trip with the fields populated.
	full := Customer{
		ID: "cust_full", Token: "t",
		GatewayRoutes:                []string{"10.42.0.0/16", "10.43.0.0/16"},
		GatewayRoutesByCluster:       map[string][]string{"prod-1": {"10.42.0.0/16"}, "prod-2": {"10.43.0.0/16"}},
		PushedGatewayRoutesByCluster: map[string][]string{"prod-1": {"10.42.0.0/16"}},
	}
	data, err = json.Marshal(&full)
	if err != nil {
		t.Fatalf("marshal full: %v", err)
	}
	var back Customer
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal full: %v", err)
	}
	if !reflect.DeepEqual(back.GatewayRoutesByCluster, full.GatewayRoutesByCluster) ||
		!reflect.DeepEqual(back.PushedGatewayRoutesByCluster, full.PushedGatewayRoutesByCluster) {
		t.Fatalf("round trip lost by-cluster state: %+v", back)
	}
}

func assertCustomerGatewayRoutes(t *testing.T, s *Store, customerID string, want []string) {
	t.Helper()
	c, err := s.CustomerByID(customerID)
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if !reflect.DeepEqual(c.GatewayRoutes, want) {
		t.Fatalf("GatewayRoutes = %v, want %v", c.GatewayRoutes, want)
	}
}

func assertClusterGatewayRoutes(t *testing.T, s *Store, customerID, clusterID string, want []string) {
	t.Helper()
	c, err := s.CustomerByID(customerID)
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if !reflect.DeepEqual(c.GatewayRoutesByCluster[clusterID], want) {
		t.Fatalf("GatewayRoutesByCluster[%s] = %v, want %v", clusterID, c.GatewayRoutesByCluster[clusterID], want)
	}
}

type gatewayRouteSpyPersister struct {
	auditRecorder
	credentialMu sync.RWMutex
	customers    []*Customer
}

func (p *gatewayRouteSpyPersister) upsertCustomer(c *Customer) error {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	cp := *c
	if c.Mesh != nil {
		mesh := *c.Mesh
		cp.Mesh = &mesh
	}
	cp.GatewayRoutes = append([]string(nil), c.GatewayRoutes...)
	cp.PushedGatewayRoutes = append([]string(nil), c.PushedGatewayRoutes...)
	// Deep copies, like a real backend's marshal: a spy that shared the map
	// would show whatever the store did LATER, not what was written now.
	cp.GatewayRoutesByCluster = copyRouteSets(c.GatewayRoutesByCluster)
	cp.PushedGatewayRoutesByCluster = copyRouteSets(c.PushedGatewayRoutesByCluster)
	p.customers = append(p.customers, &cp)
	return nil
}

func (p *gatewayRouteSpyPersister) createTenant(*Customer, *TenantMembership, bool) error { return nil }
func (p *gatewayRouteSpyPersister) revokeCustomer(*Customer) error                        { return nil }
func (p *gatewayRouteSpyPersister) finalizeRevokedCustomer(id string) error               { return nil }
func (p *gatewayRouteSpyPersister) deleteCustomerAndMemberships(id string) error          { return nil }
func (p *gatewayRouteSpyPersister) upsertAccount(a *Account) error                        { return nil }
func (p *gatewayRouteSpyPersister) upsertMembership(m *TenantMembership, ev *AuditEvent) error {
	return p.appendAudit(ev)
}
func (p *gatewayRouteSpyPersister) deleteMembership(id string, ev *AuditEvent) error {
	return p.appendAudit(ev)
}

// upsertCustomerAudited is the customer write plus its journal row as ONE step,
// mirroring the transaction the Postgres backend gives them: a refused audit
// append must leave no customer row behind, which is what the fail-closed
// namespace test asserts against.
func (p *gatewayRouteSpyPersister) upsertCustomerAudited(c *Customer, ev *AuditEvent) error {
	return p.withAudit(ev, func() error { return p.upsertCustomer(c) })
}
func (p *gatewayRouteSpyPersister) upsertWorkload(w *Workload) error { return nil }
func (p *gatewayRouteSpyPersister) stampWorkloadPodObservation(context.Context, string, string, string, string, string, time.Time) (int64, error) {
	return 1, nil
}
func (p *gatewayRouteSpyPersister) getWorkloadPodObservation(context.Context, string, string, string, string) (*PodObservation, bool, error) {
	return nil, false, nil
}
func (p *gatewayRouteSpyPersister) workloadByBurst(context.Context, string) (*Workload, error) {
	return nil, nil
}
func (p *gatewayRouteSpyPersister) recordWorkloadCost(context.Context, string, *WorkloadCost) (bool, error) {
	return false, nil
}

func (p *gatewayRouteSpyPersister) upsertBurst(b *Burst) error                   { return nil }
func (p *gatewayRouteSpyPersister) listBursts(context.Context) ([]*Burst, error) { return nil, nil }
func (p *gatewayRouteSpyPersister) deleteBurst(id string) error                  { return nil }
func (p *gatewayRouteSpyPersister) upsertPV(v *PersistentVolume) error           { return nil }
func (p *gatewayRouteSpyPersister) deletePV(id string) error                     { return nil }

func (p *gatewayRouteSpyPersister) updateBurstNodePhase(context.Context, BurstNodePhaseUpdate) (*burstNodePhaseResult, error) {
	return nil, nil
}
func (p *gatewayRouteSpyPersister) updateBurstGPUTelemetry(context.Context, BurstGPUTelemetryUpdate) (bool, error) {
	return false, nil
}

func (p *gatewayRouteSpyPersister) burstOwnedBy(context.Context, string, string) (bool, error) {
	return false, nil
}

func (p *gatewayRouteSpyPersister) burstForTenant(context.Context, string, string) (*Burst, bool, error) {
	return nil, false, nil
}

// loadAll is the boot read. These fakes hold no durable set to reload, so an
// empty snapshot is honest — including its tombstones: nothing here ever
// retires an id.
func (p *gatewayRouteSpyPersister) loadAll(context.Context) (*snapshot, error) {
	return &snapshot{}, nil
}

func (p *gatewayRouteSpyPersister) Close() {}

func (p *gatewayRouteSpyPersister) claimBurst(context.Context, string) (*Burst, bool, error) {
	return nil, false, nil
}
func (p *gatewayRouteSpyPersister) recordBurstReapReceipt(context.Context, string, string) (bool, error) {
	return false, nil
}
func (p *gatewayRouteSpyPersister) burstReapRecorded(context.Context, string, string) (bool, error) {
	return false, nil
}

func (p *gatewayRouteSpyPersister) reservePodSlot(context.Context, int, string, time.Duration) (bool, error) {
	return false, nil
}
func (p *gatewayRouteSpyPersister) releasePodSlot(context.Context, string) error { return nil }

// The idempotency trio: this fake's subject is elsewhere, so the claim seam is
// stubbed to "nothing was ever claimed".
func (p *gatewayRouteSpyPersister) claimIdempotency(context.Context, *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	return nil, true, nil
}
func (p *gatewayRouteSpyPersister) completeIdempotency(context.Context, string, int, []byte, time.Time) error {
	return nil
}
func (p *gatewayRouteSpyPersister) deleteIdempotency(context.Context, string) error { return nil }

func (p *gatewayRouteSpyPersister) reservedPodSlots(context.Context, time.Duration) (map[int]bool, error) {
	return nil, nil
}

func (p *gatewayRouteSpyPersister) lastCustomerPushedRoutes() []string {
	if len(p.customers) == 0 {
		return nil
	}
	return p.customers[len(p.customers)-1].PushedGatewayRoutes
}
