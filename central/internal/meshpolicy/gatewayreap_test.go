// yscale:proprietary

package meshpolicy

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
)

// legacyCluster is a self-chosen connector id from before the cluster-id
// grammar. It can never hold a registry row, so its socket is the whole of its
// claim — and its route intent is the entry the reap exists for.
const legacyCluster = "Prod Cluster #1"

// legacyReporter seeds a tenant whose LIVE-ONLY legacy connector has reported
// and converged alongside a properly registered sibling, then hands back the
// reconciler driving it. The box has both gateways registered, so a withdrawal
// that is owed is one the box can actually be shown to have received.
func legacyReporter(t *testing.T) (*Reconciler, *state.Store, *recordingBox) {
	t.Helper()
	if state.ValidClusterID(legacyCluster) {
		t.Fatalf("%q is inside the grammar; pick an id that is not", legacyCluster)
	}
	box := &recordingBox{}
	box.register(handlers.GatewayHostname("prod-1"), "7")
	box.register(handlers.GatewayHostname(legacyCluster), "9")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := state.New()
	r := New(ctx, s, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithEnsurerFactory(func(*state.MeshEndpoint) PolicyEnsurer { return box }),
		WithDebounce(time.Millisecond),
		WithBackoff(2*time.Millisecond),
		WithMaxBackoff(20*time.Millisecond))
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "tok"})
	if err := s.SetCustomerMesh("cust_a", &state.MeshEndpoint{
		Provider: "box", LoginServer: "https://mesh.example", APIKey: "k", User: "u",
	}); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}

	reportRoutes(t, s, "cust_a", "prod-1", workerRoutesB)
	if _, _, err := s.ClaimAgentCluster("cust_a", legacyCluster, false, time.Now().UTC()); err != nil {
		t.Fatalf("legacy claim: %v", err)
	}
	connect(t, s, "cust_a", legacyCluster)
	if _, err := s.SetCustomerGatewayRoutes("cust_a", legacyCluster, workerRoutesA); err != nil {
		t.Fatalf("legacy report: %v", err)
	}

	r.Enqueue("cust_a", "prod-1", true)
	r.Enqueue("cust_a", legacyCluster, true)
	waitFor(t, func() bool {
		intent := intentOf(s, "cust_a")
		return slices.Equal(intent.PushedRoutesFor("prod-1"), workerRoutesB) &&
			slices.Equal(intent.PushedRoutesFor(legacyCluster), workerRoutesA)
	}, "both gateways to converge before the disconnect")
	return r, s, box
}

// The end of a live-only connector's last socket is the one lifecycle event that
// can retire its route intent, and the reap plus the removal enqueue are what
// turn it into a real revocation: the tenant's policy union shrinks, and the
// gateway that still holds central's approvals has them taken back.
func TestLegacyConnectorDisconnectWithdrawsItsRoutes(t *testing.T) {
	r, s, box := legacyReporter(t)

	// What the agent stream does at the disconnect boundary: the socket leaves
	// the store's indexes first, then the reap asks what is still held.
	s.RemoveAgent("agent_" + legacyCluster)
	pruned, err := s.ReapUnheldClusterGatewayRoutes("cust_a", nil)
	if err != nil || !slices.Equal(pruned, []string{legacyCluster}) {
		t.Fatalf("reap at disconnect = (%v,%v), want the live-only cluster dropped", pruned, err)
	}
	for _, id := range pruned {
		r.EnqueueClusterRemoval("cust_a", id)
	}

	waitFor(t, func() bool {
		_, ok := intentOf(s, "cust_a").PushedByCluster[legacyCluster]
		return !ok
	}, "the live-only cluster's approvals to be withdrawn and its tombstone cleared")

	// The node really was told to hold nothing, and the live sibling was never
	// opened by the legacy cluster's event.
	nodes, routes := box.approvals()
	var withdrawals int
	for i, node := range nodes {
		if node == "9" && len(routes[i]) == 0 {
			withdrawals++
		}
	}
	if withdrawals != 1 {
		t.Fatalf("approvals = %v / %v, want one empty replacement on the legacy gateway", nodes, routes)
	}

	intent := intentOf(s, "cust_a")
	if !slices.Equal(intent.Union, workerRoutesB) {
		t.Fatalf("tenant policy union = %v, want only the cluster still held", intent.Union)
	}
	if !slices.Equal(intent.PushedUnion, workerRoutesB) {
		t.Fatalf("pushed union = %v, want the shrunk policy to have converged", intent.PushedUnion)
	}
	if !slices.Equal(intent.PushedRoutesFor("prod-1"), workerRoutesB) {
		t.Fatalf("prod-1 pushed set = %v, want the live sibling untouched", intent.PushedRoutesFor("prod-1"))
	}
}

// A central that dies between the reap and the withdrawal loses the in-process
// queue, and nothing else would ask for it — the connector is gone and claims no
// row to come back to. The reap runs again at load (there are no live sockets in
// a process that has just booted), and the pushed entry it leaves standing is
// the durable queue the attach pass replays.
func TestReconcileAttachReplaysAReapedLegacyClustersWithdrawal(t *testing.T) {
	_, s, _ := legacyReporter(t)
	s.RemoveAgent("agent_" + legacyCluster)

	// The reap lands durably; nothing drains the withdrawal it enqueued.
	if _, err := s.ReapUnheldClusterGatewayRoutes("cust_a", nil); err != nil {
		t.Fatalf("reap: %v", err)
	}

	box := &recordingBox{}
	box.register(handlers.GatewayHostname("prod-1"), "7")
	box.register(handlers.GatewayHostname(legacyCluster), "9")
	r, reloaded := restart(t, s, "cust_a", box)

	if err := r.ReconcileAttach(context.Background(), "cust_a"); err != nil {
		t.Fatalf("ReconcileAttach: %v", err)
	}
	nodes, routes := box.approvals()
	if len(nodes) != 1 || nodes[0] != "9" || len(routes[0]) != 0 {
		t.Fatalf("attach approvals = %v / %v, want one empty replacement on the reaped gateway", nodes, routes)
	}
	if _, ok := intentOf(reloaded, "cust_a").PushedByCluster[legacyCluster]; ok {
		t.Fatal("the withdrawal tombstone survived the boot replay")
	}
	if got := intentOf(reloaded, "cust_a").PushedRoutesFor("prod-1"); !slices.Equal(got, workerRoutesB) {
		t.Fatalf("prod-1 pushed set = %v, want the live sibling untouched", got)
	}
}

// reRegisteredRoutes is what a cluster id reports after coming BACK — distinct
// from either seeded set, so the node's final approval names which state won.
var reRegisteredRoutes = []string{"10.44.0.0/16"}

// A cluster id can come back: a connector redeployed under the same name, a
// registry row recreated after a mistaken delete. The withdrawal already in
// flight knows nothing about it — the node call runs outside every store lock —
// so its empty replacement lands AFTER the new intent exists. Without the
// post-mutation re-read the worker then goes idle, and "hold nothing" is the
// final state of a gateway that is live and reporting.
//
// The gated box makes that ordering an arrangement rather than a race the test
// hopes to win.
func TestWithdrawalRacedByAReRegistrationDoesNotStayTheFinalState(t *testing.T) {
	box := &gatedBox{entered: make(chan string), answer: make(chan string)}
	r, s := workerTestReconciler(t, box, time.Millisecond)
	owner := tenantOwner(t, s, "cust_a")

	r.Enqueue("cust_a", "prod-1", true)
	<-box.entered
	box.answer <- "7"
	waitFor(t, func() bool {
		return slices.Equal(intentOf(s, "cust_a").PushedRoutesFor("prod-1"), workerRoutesA)
	}, "prod-1 to converge before the delete")

	// The delete leaves the tombstone the withdrawal is owed against.
	deleteCluster(t, s, "cust_a", owner, "prod-1")
	r.EnqueueClusterRemoval("cust_a", "prod-1")
	<-box.entered // the withdrawal is parked inside the node lookup

	// MID-CALL: the same id is claimed again and reports.
	reportRoutes(t, s, "cust_a", "prod-1", reRegisteredRoutes)
	box.answer <- "7" // the withdrawal lands; node 7 is told to hold nothing

	select {
	case <-box.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("no fresh reconcile after the intent moved mid-call: the withdrawal is prod-1's last word")
	}
	box.answer <- "7"

	waitFor(t, func() bool {
		return slices.Equal(intentOf(s, "cust_a").PushedRoutesFor("prod-1"), reRegisteredRoutes)
	}, "prod-1's gateway to end up holding the routes it reports now")

	// And the node's LAST replacement really is the live set, not the empty one.
	nodes, routes := box.approvals()
	var last []string
	for i, node := range nodes {
		if node == "7" {
			last = routes[i]
		}
	}
	if !slices.Equal(last, reRegisteredRoutes) {
		t.Fatalf("last replacement on prod-1's node = %v, want %v", last, reRegisteredRoutes)
	}
}

// The generation map is the race guard between an Enqueue that lands mid-attempt
// and the settle that follows it. It also has to stop growing: an id that is not
// queued, not laddered and not in flight is a dead key, and a churning fleet
// would otherwise leave one behind per cluster id it ever used.
func TestSettlePrunesDeadGenerationEntries(t *testing.T) {
	r := idleReconciler(t)
	r.Enqueue("cust_a", "prod-1", false)
	r.Enqueue("cust_a", "prod-2", false)
	w := workerFor(t, r, "cust_a")

	now := time.Now()
	req, ok := w.take(now)
	if !ok {
		t.Fatal("take drained nothing")
	}
	// prod-1 converges and prod-2 fails: only the failure has anything left to
	// key a generation on.
	r.settle(w, req, reconcileOutcome{failedClusters: []string{"prod-2"}}, now)

	w.mu.Lock()
	gens := slices.Sorted(maps.Keys(w.gen))
	_, prod2Laddered := w.retry["prod-2"]
	w.mu.Unlock()
	if !slices.Equal(gens, []string{"prod-2"}) {
		t.Fatalf("generation keys = %v, want only the requeued cluster", gens)
	}
	if !prod2Laddered {
		t.Fatal("pruning generations must not disturb the retry ladder it guards")
	}

	// The guard still holds for the item that is left: an Enqueue that lands
	// mid-attempt bumps prod-2's generation, and the older attempt's settle must
	// not re-arm the ladder that Enqueue cleared.
	req, ok = w.take(now.Add(time.Hour))
	if !ok {
		t.Fatal("second take drained nothing")
	}
	r.Enqueue("cust_a", "prod-2", true)
	r.settle(w, req, reconcileOutcome{failedClusters: []string{"prod-2"}}, now)
	if got := attemptsFor(t, r, "cust_a", "prod-2"); got != -1 {
		t.Fatalf("prod-2 ladder = %d, want cleared — the newer enqueue owns it", got)
	}
}

// A node call whose intent moved underneath it owes its own gateway another
// pass. It does not owe the tenant's POLICY one: whatever moved the intent
// enqueued that itself, and re-asking for a document the store already calls
// current is a full PUT bought with nothing.
func TestIntentMovedRequeuesOnlyTheClusterHalf(t *testing.T) {
	r := idleReconciler(t)
	r.enqueueCluster("cust_a", "prod-1", true)

	w := workerFor(t, r, "cust_a")
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.policy {
		t.Fatal("a cluster-only requeue asked for a tenant policy push")
	}
	item, queued := w.clusters["prod-1"]
	if !queued || !item.force {
		t.Fatalf("clusters[prod-1] = %+v queued=%v, want the forced cluster half alone", item, queued)
	}
	if item.withdraw {
		t.Fatal("a cluster-only requeue must not turn into a withdrawal")
	}
	if _, laddered := w.retry["prod-1"]; laddered {
		t.Fatal("fresh evidence left the cluster inside a backoff")
	}
}

// The re-read after a node mutation answers two questions, and they are not the
// same question: whether THIS cluster still says what the attempt acted on, and
// whether the TENANT's policy is stale as of the same read.
func TestClusterIntentMovedSeparatesTheTwoHalves(t *testing.T) {
	box := &recordingBox{found: map[string]string{handlers.GatewayHostname("prod-1"): "7"}}
	r, s := workerTestReconciler(t, box, time.Millisecond)

	// Converged on both halves: the cluster's set is what the attempt carried,
	// and the tenant's union is already pushed.
	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, true); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	moved, policyMoved := r.clusterIntentMoved("cust_a", gatewayWork{clusterID: "prod-1", routes: workerRoutesA})
	if moved || policyMoved {
		t.Fatalf("moved=%v policyMoved=%v, want both false on a converged tenant", moved, policyMoved)
	}

	// A set the attempt did not carry: the cluster moved, and so did the union
	// it is part of.
	reportRoutes(t, s, "cust_a", "prod-1", reRegisteredRoutes)
	moved, policyMoved = r.clusterIntentMoved("cust_a", gatewayWork{clusterID: "prod-1", routes: workerRoutesA})
	if !moved || !policyMoved {
		t.Fatalf("moved=%v policyMoved=%v, want both true after a fresh report", moved, policyMoved)
	}

	// A tenant that went away owes nothing at all — there is no gateway left to
	// converge and no policy to hold it in.
	moved, policyMoved = r.clusterIntentMoved("cust_missing", gatewayWork{clusterID: "prod-1", routes: workerRoutesA})
	if moved || policyMoved {
		t.Fatalf("moved=%v policyMoved=%v, want both false for an unknown tenant", moved, policyMoved)
	}
}
