package state

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"
)

// legacyClusterID is a self-chosen connector id from before the grammar. It can
// never acquire a registry row, so a live socket is the whole of its claim —
// which is what makes its route intent the entry nothing else would ever remove.
const legacyClusterID = "Prod Cluster #1"

// liveOnlyTenant seeds the shape the reap exists for: one legacy cluster holding
// intent on the strength of its socket alone, and one properly registered
// cluster alongside it, so a reap that took too much is visible.
func liveOnlyTenant(t *testing.T) (*Store, *gatewayRouteSpyPersister) {
	t.Helper()
	if ValidClusterID(legacyClusterID) {
		t.Fatalf("%q is inside the grammar; pick an id that is not", legacyClusterID)
	}
	s := New()
	claimCluster(t, s, "cust_test", "prod-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{"10.43.0.0/16"}); err != nil {
		t.Fatalf("report from the registered cluster: %v", err)
	}
	if _, _, err := s.ClaimAgentCluster("cust_test", legacyClusterID, false, time.Now().UTC()); err != nil {
		t.Fatalf("legacy claim: %v", err)
	}
	connect(s, "agent_legacy", "cust_test", legacyClusterID, 0)
	if _, err := s.SetCustomerGatewayRoutes("cust_test", legacyClusterID, []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("legacy report: %v", err)
	}
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", legacyClusterID, []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed the legacy cluster's pushed entry: %v", err)
	}
	p := &gatewayRouteSpyPersister{}
	s.persist = p
	return s, p
}

// The reap is the only end a LIVE-ONLY connector's route intent has. While its
// socket is up the entry is its own; once the last socket closes the cluster is
// held by nothing, and leaving the entry would keep its CIDRs in the tenant's
// policy union with nothing able to take them out — no delete to run against a
// cluster that was never in the registry, and no reconnect obliged to report a
// smaller set.
func TestReapUnheldClusterGatewayRoutesReapsTheLiveOnlyCluster(t *testing.T) {
	s, p := liveOnlyTenant(t)

	// While the socket is live the entry is held, and nothing moves.
	pruned, err := s.ReapUnheldClusterGatewayRoutes("cust_test", nil)
	if err != nil || len(pruned) != 0 {
		t.Fatalf("prune with the socket up = (%v,%v), want nothing dropped", pruned, err)
	}
	if len(p.customers) != 0 {
		t.Fatalf("a no-op reap wrote %d customer rows, want none", len(p.customers))
	}

	s.RemoveAgent("agent_legacy")
	pruned, err = s.ReapUnheldClusterGatewayRoutes("cust_test", nil)
	if err != nil {
		t.Fatalf("prune after the disconnect: %v", err)
	}
	if !slices.Equal(pruned, []string{legacyClusterID}) {
		t.Fatalf("pruned = %v, want exactly the live-only cluster", pruned)
	}

	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if _, ok := intent.ByCluster[legacyClusterID]; ok {
		t.Fatalf("the live-only cluster's desired set survived: %v", intent.ByCluster)
	}
	// The registered cluster is untouched — this is a liveness check on each
	// cluster, not a teardown of the tenant.
	if got := intent.ByCluster["prod-1"]; !slices.Equal(got, []string{"10.43.0.0/16"}) {
		t.Fatalf("prod-1 desired set = %v, want it untouched", got)
	}
	// The union shrinks to exactly what is still held.
	if !slices.Equal(intent.Union, []string{"10.43.0.0/16"}) {
		t.Fatalf("union = %v, want only the held cluster's routes", intent.Union)
	}
	// The pushed half stays: it is the durable tombstone the node withdrawal is
	// driven from, and the only record central ever approved anything there.
	if got := intent.PushedRoutesFor(legacyClusterID); !slices.Equal(got, []string{"10.42.0.0/16"}) {
		t.Fatalf("pushed tombstone = %v, want it retained for the withdrawal", got)
	}

	// Durable, not just in memory: a restart must not find the entry again.
	if len(p.customers) != 1 {
		t.Fatalf("persisted writes = %d, want the one reap", len(p.customers))
	}
	wrote := p.customers[0]
	if _, ok := wrote.GatewayRoutesByCluster[legacyClusterID]; ok {
		t.Fatalf("persisted document still names the reaped cluster: %v", wrote.GatewayRoutesByCluster)
	}
	if !slices.Equal(wrote.GatewayRoutes, []string{"10.43.0.0/16"}) {
		t.Fatalf("persisted union = %v, want the shrunk one", wrote.GatewayRoutes)
	}
	if got := wrote.PushedGatewayRoutesByCluster[legacyClusterID]; !slices.Equal(got, []string{"10.42.0.0/16"}) {
		t.Fatalf("persisted tombstone = %v, want it retained", got)
	}

	// Idempotent: a second pass has nothing left to find and writes nothing.
	pruned, err = s.ReapUnheldClusterGatewayRoutes("cust_test", nil)
	if err != nil || len(pruned) != 0 {
		t.Fatalf("second prune = (%v,%v), want a clean no-op", pruned, err)
	}
	if len(p.customers) != 1 {
		t.Fatalf("persisted writes after the second prune = %d, want still 1", len(p.customers))
	}
}

// A reconnect that lands before the dead socket is reaped means TWO sockets for
// one legacy id. The reap must read the tenant's live set, not the one socket
// that just closed: dropping the entry there would take the routes off a
// connector that is up and reporting.
func TestReapUnheldClusterGatewayRoutesKeepsAClusterWithASecondSocket(t *testing.T) {
	s, _ := liveOnlyTenant(t)
	connect(s, "agent_legacy_2", "cust_test", legacyClusterID, 1)

	s.RemoveAgent("agent_legacy")
	pruned, err := s.ReapUnheldClusterGatewayRoutes("cust_test", nil)
	if err != nil || len(pruned) != 0 {
		t.Fatalf("prune with a second socket up = (%v,%v), want nothing dropped", pruned, err)
	}
	if got, err := s.CustomerGatewayRouteIntent("cust_test"); err != nil ||
		!slices.Equal(got.ByCluster[legacyClusterID], []string{"10.42.0.0/16"}) {
		t.Fatalf("the surviving socket lost its routes: %v (%v)", got.ByCluster, err)
	}

	s.RemoveAgent("agent_legacy_2")
	if pruned, err = s.ReapUnheldClusterGatewayRoutes("cust_test", nil); err != nil ||
		!slices.Equal(pruned, []string{legacyClusterID}) {
		t.Fatalf("prune after the LAST socket = (%v,%v), want the cluster dropped", pruned, err)
	}
}

// A report that arrives after its cluster's intent was reaped must not put it
// back. The store's own liveness gate is what refuses it, so this holds for a
// socket the reap raced as well as for one that is long gone.
func TestReportAfterAReapCannotRecreateTheIntent(t *testing.T) {
	s, _ := liveOnlyTenant(t)
	s.RemoveAgent("agent_legacy")
	if _, err := s.ReapUnheldClusterGatewayRoutes("cust_test", nil); err != nil {
		t.Fatalf("prune: %v", err)
	}

	changed, err := s.SetCustomerGatewayRoutes("cust_test", legacyClusterID, []string{"10.42.0.0/16"})
	if !errors.Is(err, ErrClusterNotFound) || changed {
		t.Fatalf("late report changed=%v err=%v, want ErrClusterNotFound and no write", changed, err)
	}
	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if _, ok := intent.ByCluster[legacyClusterID]; ok {
		t.Fatalf("the late report recreated the reaped entry: %v", intent.ByCluster)
	}
	if !slices.Equal(intent.Union, []string{"10.43.0.0/16"}) {
		t.Fatalf("union = %v, want the late report to have widened nothing", intent.Union)
	}
}

// An unknown tenant is ErrNotFound rather than "nothing to do". The difference
// matters at the caller: a store that could not find the customer has not proven
// the intent is gone, and the caller must not read the empty list as success.
func TestReapUnheldClusterGatewayRoutesRefusesAnUnknownTenant(t *testing.T) {
	s := New()
	if _, err := s.ReapUnheldClusterGatewayRoutes("cust_nope", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("prune of an unknown tenant err=%v, want ErrNotFound", err)
	}
}

// Load must reap NOTHING. A process that has just booted holds no sockets at
// all, so every live-only connector in the fleet looks unheld until it
// reconnects — and dropping their intent there withdrew running clusters'
// routes on every central restart. Load leaves the candidates for the boot
// wiring to take (ConsumeUnheldRouteClusters), and that gives each the same grace
// lease a disconnect gives one.
func TestApplySnapshotKeepsUnheldClusterRouteIntentForTheGrace(t *testing.T) {
	s := emptyStore()
	p := &gatewayRouteSpyPersister{}
	s.persist = p
	s.applySnapshot(&snapshot{Customers: []*Customer{{
		ID:    "cust_a",
		Token: "tok_a",
		RegisteredClusters: []*RegisteredCluster{
			{ClusterID: "prod-1", Source: ClusterSourceClaimed, RegisteredAt: clusterEpoch},
		},
		GatewayRoutes: []string{"10.42.0.0/16", "10.43.0.0/16"},
		GatewayRoutesByCluster: map[string][]string{
			"prod-1":        {"10.43.0.0/16"},
			legacyClusterID: {"10.42.0.0/16"},
		},
		PushedGatewayRoutes: []string{"10.42.0.0/16", "10.43.0.0/16"},
		PushedGatewayRoutesByCluster: map[string][]string{
			"prod-1":        {"10.43.0.0/16"},
			legacyClusterID: {"10.42.0.0/16"},
		},
	}}})

	intent, err := s.CustomerGatewayRouteIntent("cust_a")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if got := intent.ByCluster[legacyClusterID]; !slices.Equal(got, []string{"10.42.0.0/16"}) {
		t.Fatalf("boot dropped a live-only cluster's routes before it could reconnect: %v", intent.ByCluster)
	}
	if !slices.Equal(intent.Union, []string{"10.42.0.0/16", "10.43.0.0/16"}) {
		t.Fatalf("union after boot = %v, want the persisted one carried through the grace", intent.Union)
	}
	if len(p.customers) != 0 {
		t.Fatalf("boot wrote %d customer rows, want none — it decides nothing", len(p.customers))
	}

	// What boot DOES produce is the candidate list, which is what the startup
	// wiring arms leases from. The registered cluster is not on it: its registry
	// row holds it whether or not a socket is up.
	unheld := s.ConsumeUnheldRouteClusters()
	if !slices.Equal(unheld["cust_a"], []string{legacyClusterID}) {
		t.Fatalf("unheld candidates = %v, want exactly the live-only cluster", unheld)
	}

	// And the reap the lease eventually runs is the durable one, with the
	// withdrawal tombstone left in place for the attach reconcile to replay.
	pruned, err := s.ReapUnheldClusterGatewayRoutes("cust_a", []string{legacyClusterID})
	if err != nil {
		t.Fatalf("reap at lease expiry: %v", err)
	}
	if !slices.Equal(pruned, []string{legacyClusterID}) {
		t.Fatalf("pruned = %v, want the leased cluster", pruned)
	}
	intent, err = s.CustomerGatewayRouteIntent("cust_a")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent after the reap: %v", err)
	}
	if !slices.Equal(intent.Union, []string{"10.43.0.0/16"}) {
		t.Fatalf("union after the reap = %v, want it recomputed from the held clusters", intent.Union)
	}
	if got := intent.PushedRoutesFor(legacyClusterID); !slices.Equal(got, []string{"10.42.0.0/16"}) {
		t.Fatalf("the reap dropped the withdrawal tombstone: %v", got)
	}
	if len(p.customers) != 1 {
		t.Fatalf("the reap wrote %d customer rows, want 1", len(p.customers))
	}
	if _, ok := p.customers[0].GatewayRoutesByCluster[legacyClusterID]; ok {
		t.Fatal("the reap was in memory only; the next restart would find it again")
	}
}

// The boot reap's work list is CLAIMED, once per store and per cluster. Two agent
// streams over one store are one process's view of one durable record, so the
// second must arm nothing: arming again restarts a grace the first is already
// counting down, and a wiring that constructs streams repeatedly would defer the
// reap forever. The reap the armed lease eventually runs still finds its cluster,
// because what the second caller is refused is the ARMING, not the work.
func TestConsumeUnheldRouteClustersHandsEachClusterOutOnce(t *testing.T) {
	s := emptyStore()
	s.applySnapshot(&snapshot{Customers: []*Customer{{
		ID:                     "cust_a",
		Token:                  "tok_a",
		GatewayRoutes:          []string{"10.42.0.0/16"},
		GatewayRoutesByCluster: map[string][]string{legacyClusterID: {"10.42.0.0/16"}},
	}}})

	first := s.ConsumeUnheldRouteClusters()
	if !slices.Equal(first["cust_a"], []string{legacyClusterID}) {
		t.Fatalf("first consume = %v, want the live-only cluster", first)
	}
	if second := s.ConsumeUnheldRouteClusters(); len(second) != 0 {
		t.Fatalf("second consume = %v, want nothing — that cluster's grace is already armed", second)
	}
	// The claim arms, it does not reap: the cluster is still unheld, and the reap
	// the first caller's lease will eventually run still has it to take.
	pruned, err := s.ReapUnheldClusterGatewayRoutes("cust_a", []string{legacyClusterID})
	if err != nil {
		t.Fatalf("reap at lease expiry: %v", err)
	}
	if !slices.Equal(pruned, []string{legacyClusterID}) {
		t.Fatalf("the reap took %v, want the cluster the second consume was refused", pruned)
	}

	// Per CLUSTER, not per call: a store whose first caller found nothing has not
	// spent anything, so a cluster that is unheld at the next construction still
	// gets its lease.
	fresh := emptyStore()
	if got := fresh.ConsumeUnheldRouteClusters(); len(got) != 0 {
		t.Fatalf("consume on an empty store = %v, want nothing", got)
	}
	fresh.applySnapshot(&snapshot{Customers: []*Customer{{
		ID:                     "cust_a",
		Token:                  "tok_a",
		GatewayRoutes:          []string{"10.42.0.0/16"},
		GatewayRoutesByCluster: map[string][]string{legacyClusterID: {"10.42.0.0/16"}},
	}}})
	if got := fresh.ConsumeUnheldRouteClusters(); !slices.Equal(got["cust_a"], []string{legacyClusterID}) {
		t.Fatalf("consume after the record loaded = %v, want the live-only cluster", got)
	}
}

// Shedding a registry row at the cap removes a cluster the tenant holds, so the
// row's route intent has to go in the SAME durable write. Two writes have an
// ordering that leaves the CIDRs of a cluster nobody holds in the tenant's
// policy with nothing left to remove them — the connector is disconnected by
// construction and has no row to come back to.
func TestClaimAgentClusterPruneDropsTheShedRowsRouteIntent(t *testing.T) {
	s, _, _ := manageStore(t, "cust_a", map[string]string{"alice": RoleOwner})

	// cl-old is the oldest safe claimed row, so it is the one the cap sheds.
	if _, _, err := s.ClaimAgentCluster("cust_a", "cl-old", false, clusterEpoch); err != nil {
		t.Fatalf("claim cl-old: %v", err)
	}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "cl-old", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("cl-old report: %v", err)
	}
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_a", "cl-old", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed cl-old's pushed entry: %v", err)
	}
	for i := 1; i < MaxRegisteredClusters; i++ {
		id := fmt.Sprintf("cl-fill-%02d", i)
		if _, _, err := s.ClaimAgentCluster("cust_a", id, false, clusterEpoch.Add(time.Hour+time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("claim %s: %v", id, err)
		}
	}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "cl-fill-01", []string{"10.43.0.0/16"}); err != nil {
		t.Fatalf("cl-fill-01 report: %v", err)
	}

	p := &gatewayRouteSpyPersister{}
	s.persist = p
	claimed, pruned, err := s.ClaimAgentCluster("cust_a", "cl-new", false, clusterEpoch.Add(2*time.Hour))
	if err != nil || !claimed {
		t.Fatalf("claim at capacity = (%v,%v), want a shed slot", claimed, err)
	}
	if pruned != "cl-old" {
		t.Fatalf("pruned cluster = %q, want cl-old — the caller has no other way to learn what it owes", pruned)
	}

	intent, err := s.CustomerGatewayRouteIntent("cust_a")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if _, ok := intent.ByCluster["cl-old"]; ok {
		t.Fatalf("the shed row kept its desired set: %v", intent.ByCluster)
	}
	if !slices.Equal(intent.Union, []string{"10.43.0.0/16"}) {
		t.Fatalf("union = %v, want the shed row's CIDRs out of the tenant policy", intent.Union)
	}
	if got := intent.PushedRoutesFor("cl-old"); !slices.Equal(got, []string{"10.42.0.0/16"}) {
		t.Fatalf("shed row's tombstone = %v, want it retained for the node withdrawal", got)
	}

	// ONE durable write carries the row removal and the intent together.
	if len(p.customers) != 1 {
		t.Fatalf("durable writes for the claim = %d, want exactly 1", len(p.customers))
	}
	wrote := p.customers[0]
	if _, ok := wrote.GatewayRoutesByCluster["cl-old"]; ok {
		t.Fatal("the persisted row removal and the persisted intent disagree")
	}
	if _, row := registeredClusterLocked(&Customer{RegisteredClusters: wrote.RegisteredClusters}, "cl-old"); row != nil {
		t.Fatal("the shed row survived in the same document")
	}
}

// The tenant policy union is bounded by the clusters the tenant HOLDS times the
// per-report cap, not by the per-report cap alone. The reap is what makes the
// first factor finite: every entry belongs to a registry row or a live socket,
// and one that belongs to neither is dropped.
func TestTenantPolicyUnionIsBoundedByHeldClusters(t *testing.T) {
	s := New()
	// Ten distinct legacy connectors report in turn and go away, the way a
	// churning pre-registry fleet does. Without the reap each one leaves its
	// CIDR in the union forever.
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("Legacy Cluster #%d", i)
		if ValidClusterID(id) {
			t.Fatalf("%q is inside the grammar; the churn must be live-only", id)
		}
		if _, _, err := s.ClaimAgentCluster("cust_test", id, false, time.Now().UTC()); err != nil {
			t.Fatalf("claim %s: %v", id, err)
		}
		connect(s, "agent_"+id, "cust_test", id, i)
		if _, err := s.SetCustomerGatewayRoutes("cust_test", id, []string{fmt.Sprintf("10.%d.0.0/16", 100+i)}); err != nil {
			t.Fatalf("report %s: %v", id, err)
		}
		s.RemoveAgent("agent_" + id)
		if _, err := s.ReapUnheldClusterGatewayRoutes("cust_test", nil); err != nil {
			t.Fatalf("reap after %s: %v", id, err)
		}
	}

	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if len(intent.ByCluster) != 0 {
		t.Fatalf("by-cluster entries after the churn = %v, want none — no cluster is held", intent.ByCluster)
	}
	if len(intent.Union) != 0 {
		t.Fatalf("union after the churn = %v, want empty", intent.Union)
	}
}

// The shared tailnet's kubelet rule has ONE source set for every tenant on it,
// so the union has to span them all. A tenant with its own coordination box owns
// a whole policy namespace elsewhere and contributes nothing here; neither does
// a revoked one, whose access is gone and for whom an ACL entry would be a grant
// that outlived it.
func TestSharedTailnetGatewayRoutesSpansOnlyTheSharedTenants(t *testing.T) {
	s := New()
	claimCluster(t, s, "cust_test", "prod-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed cust_test: %v", err)
	}

	s.AddCustomer(&Customer{ID: "cust_b", Token: "tok_b"})
	claimCluster(t, s, "cust_b", "b-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_b", "b-1", []string{"10.96.0.0/12"}); err != nil {
		t.Fatalf("seed cust_b: %v", err)
	}

	s.AddCustomer(&Customer{ID: "cust_box", Token: "tok_box"})
	claimCluster(t, s, "cust_box", "box-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_box", "box-1", []string{"192.168.7.0/24"}); err != nil {
		t.Fatalf("seed cust_box: %v", err)
	}
	if err := s.SetCustomerMesh("cust_box", &MeshEndpoint{
		Provider: "box", LoginServer: "https://mesh.example", APIKey: "k", User: "u",
	}); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}

	want := []string{"10.42.0.0/16", "10.96.0.0/12"}
	if got := s.SharedTailnetGatewayRoutes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("shared union = %v, want %v", got, want)
	}

	if err := s.RevokeCustomer("cust_b"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if got := s.SharedTailnetGatewayRoutes(); !reflect.DeepEqual(got, []string{"10.42.0.0/16"}) {
		t.Fatalf("shared union after a revoke = %v, want the revoked tenant's grant gone", got)
	}
}

// The shared tailnet's managed rule has one Src for every tenant on it, and the
// snapshot the reconcile works from has to hand back the desired union, the one
// central last wrote, and the per-tenant halves in a single read — three answers
// taken at three different moments would describe a state that never existed.
func TestSharedTailnetRouteIntentSnapshotsBothHalves(t *testing.T) {
	s := New()
	claimCluster(t, s, "cust_test", "prod-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed cust_test: %v", err)
	}
	s.AddCustomer(&Customer{ID: "cust_b", Token: "tok_b"})
	claimCluster(t, s, "cust_b", "b-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_b", "b-1", []string{"10.96.0.0/12"}); err != nil {
		t.Fatalf("seed cust_b: %v", err)
	}

	intent := s.SharedTailnetRouteIntent()
	want := []string{"10.42.0.0/16", "10.96.0.0/12"}
	if !reflect.DeepEqual(intent.Desired, want) {
		t.Fatalf("Desired = %v, want %v", intent.Desired, want)
	}
	if len(intent.Pushed) != 0 {
		t.Fatalf("Pushed = %v, want empty before anything has been written", intent.Pushed)
	}
	if !reflect.DeepEqual(intent.ByCustomer["cust_b"], []string{"10.96.0.0/12"}) {
		t.Fatalf("ByCustomer[cust_b] = %v, want that tenant's own half", intent.ByCustomer["cust_b"])
	}

	if err := s.RecordSharedTailnetPushed(intent); err != nil {
		t.Fatalf("RecordSharedTailnetPushed: %v", err)
	}
	after := s.SharedTailnetRouteIntent()
	if !reflect.DeepEqual(after.Pushed, want) {
		t.Fatalf("Pushed = %v, want the union the write landed %v", after.Pushed, want)
	}
	// The per-tenant halves converge at the same instant, because on this path
	// the shared document IS each tenant's policy.
	for _, id := range []string{"cust_test", "cust_b"} {
		c, err := s.CustomerByID(id)
		if err != nil {
			t.Fatalf("CustomerByID(%s): %v", id, err)
		}
		if !reflect.DeepEqual(c.PushedGatewayRoutes, intent.ByCustomer[id]) {
			t.Fatalf("%s PushedGatewayRoutes = %v, want %v", id, c.PushedGatewayRoutes, intent.ByCustomer[id])
		}
	}
}

// The record of what central wrote outlives the tenants it was summed from. A
// tenant deleted outright is the case a per-customer record could not survive:
// its row is gone, and with it the only proof that the rule still granting its
// CIDRs is central's to take back.
func TestSharedTailnetPushedUnionOutlivesTheTenantsItCameFrom(t *testing.T) {
	s := New()
	claimCluster(t, s, "cust_test", "prod-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed cust_test: %v", err)
	}
	s.AddCustomer(&Customer{ID: "cust_gone", Token: "tok_gone"})
	claimCluster(t, s, "cust_gone", "gone-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_gone", "gone-1", []string{"10.96.0.0/12"}); err != nil {
		t.Fatalf("seed cust_gone: %v", err)
	}
	landed := s.SharedTailnetRouteIntent()
	if err := s.RecordSharedTailnetPushed(landed); err != nil {
		t.Fatalf("RecordSharedTailnetPushed: %v", err)
	}

	if err := s.RevokeCustomer("cust_gone"); err != nil {
		t.Fatalf("RevokeCustomer: %v", err)
	}
	if err := s.DeleteRevokedCustomer("cust_gone"); err != nil {
		t.Fatalf("DeleteRevokedCustomer: %v", err)
	}
	after := s.SharedTailnetRouteIntent()
	if !reflect.DeepEqual(after.Desired, []string{"10.42.0.0/16"}) {
		t.Fatalf("Desired = %v, want the deleted tenant's CIDRs gone", after.Desired)
	}
	if !reflect.DeepEqual(after.Pushed, landed.Desired) {
		t.Fatalf("Pushed = %v, want the union still standing on the tailnet %v", after.Pushed, landed.Desired)
	}
	if _, ok := after.ByCustomer["cust_gone"]; ok {
		t.Fatal("a deleted tenant is still named as a contributor")
	}
}

// A tenant that moves onto its own box stops contributing, and the shared union
// it left has to shrink by exactly its CIDRs — while the record of what is
// standing on the tailnet keeps them, because they are still granted there.
func TestSharedTailnetIntentDropsATenantThatTookABox(t *testing.T) {
	s := New()
	claimCluster(t, s, "cust_test", "prod-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	landed := s.SharedTailnetRouteIntent()
	if err := s.RecordSharedTailnetPushed(landed); err != nil {
		t.Fatalf("RecordSharedTailnetPushed: %v", err)
	}
	if err := s.SetCustomerMesh("cust_test", &MeshEndpoint{
		Provider: "box", LoginServer: "https://mesh.example", APIKey: "k", User: "u",
	}); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}

	after := s.SharedTailnetRouteIntent()
	if len(after.Desired) != 0 {
		t.Fatalf("Desired = %v, want empty once the only shared tenant took a box", after.Desired)
	}
	if !reflect.DeepEqual(after.Pushed, []string{"10.42.0.0/16"}) {
		t.Fatalf("Pushed = %v, want what is still granted on the shared tailnet", after.Pushed)
	}
}

// The record is the proof the NEXT write deletes a rule on, so it may not claim
// something no restart could read back. A refused durable write therefore leaves
// the store exactly as it was and reports the failure.
func TestRecordSharedTailnetPushedIsFailClosed(t *testing.T) {
	s := New()
	claimCluster(t, s, "cust_test", "prod-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	s.persist = &refusingMeshStatePersister{err: errors.New("postgres is down")}

	intent := s.SharedTailnetRouteIntent()
	if err := s.RecordSharedTailnetPushed(intent); err == nil {
		t.Fatal("a refused durable write reported success; the next ACL write would delete on a proof nobody has")
	}
	if got := s.SharedTailnetRouteIntent().Pushed; len(got) != 0 {
		t.Fatalf("Pushed = %v, want it left alone when the record could not be written", got)
	}
	c, err := s.CustomerByID("cust_test")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	if len(c.PushedGatewayRoutes) != 0 {
		t.Fatalf("PushedGatewayRoutes = %v, want untouched", c.PushedGatewayRoutes)
	}
}

type refusingMeshStatePersister struct {
	gatewayRouteSpyPersister
	err error
}

func (p *refusingMeshStatePersister) setMeshState(*meshState) error { return p.err }

// The reap may only take the clusters it was asked about. A lease that expires
// for one connector says nothing about a sibling that dropped a moment ago and
// is still inside its own grace.
func TestReapUnheldClusterGatewayRoutesTakesOnlyTheNamedClusters(t *testing.T) {
	s, _ := liveOnlyTenant(t)
	const secondLegacy = "Prod Cluster #2"
	if _, _, err := s.ClaimAgentCluster("cust_test", secondLegacy, false, time.Now().UTC()); err != nil {
		t.Fatalf("second legacy claim: %v", err)
	}
	connect(s, "agent_legacy_2", "cust_test", secondLegacy, 0)
	if _, err := s.SetCustomerGatewayRoutes("cust_test", secondLegacy, []string{"10.44.0.0/16"}); err != nil {
		t.Fatalf("second legacy report: %v", err)
	}
	s.RemoveAgent("agent_legacy")
	s.RemoveAgent("agent_legacy_2")

	pruned, err := s.ReapUnheldClusterGatewayRoutes("cust_test", []string{legacyClusterID})
	if err != nil {
		t.Fatalf("ReapUnheldClusterGatewayRoutes: %v", err)
	}
	if !reflect.DeepEqual(pruned, []string{legacyClusterID}) {
		t.Fatalf("pruned = %v, want only the cluster the lease named", pruned)
	}
	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if got, ok := intent.ByCluster[secondLegacy]; !ok || !reflect.DeepEqual(got, []string{"10.44.0.0/16"}) {
		t.Fatalf("the unnamed cluster's desired set = %v, want its own grace still running", got)
	}
	if !slices.Contains(intent.Union, "10.44.0.0/16") {
		t.Fatalf("tenant union = %v, want the unnamed cluster still in it", intent.Union)
	}

	// A named cluster that is still HELD is refused on the store's own evidence,
	// not on the caller's — the lease says what was true when it was armed.
	if pruned, err := s.ReapUnheldClusterGatewayRoutes("cust_test", []string{"prod-1"}); err != nil {
		t.Fatalf("ReapUnheldClusterGatewayRoutes: %v", err)
	} else if len(pruned) != 0 {
		t.Fatalf("pruned = %v, want a registered cluster kept whatever the caller asked", pruned)
	}
}
