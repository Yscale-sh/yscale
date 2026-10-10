// yscale:proprietary

package meshpolicy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/factory/client"
)

var (
	workerRoutesA = []string{"10.42.0.0/16"}
	workerRoutesB = []string{"10.43.0.0/16"}
)

// recordingBox is a mesh box whose gateway nodes register only when the test
// says so, recording which gateway hostname each attempt asked for.
type recordingBox struct {
	mu       sync.Mutex
	found    map[string]string
	lookedUp []string
	approved []string
	// approvedRoutes is what each approval actually carried, so a withdrawal
	// (the exact empty replacement) is distinguishable from a converge.
	approvedRoutes [][]string
	policies       int
}

func (b *recordingBox) EnsurePolicy(context.Context, map[string][]string, map[string][]string, []client.PolicyACL) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.policies++
	return nil
}

func (b *recordingBox) FindDeviceByHostname(_ context.Context, hostname string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lookedUp = append(b.lookedUp, hostname)
	return b.found[hostname], nil
}

func (b *recordingBox) ApproveNodeRoutes(_ context.Context, nodeID string, routes []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.approved = append(b.approved, nodeID)
	b.approvedRoutes = append(b.approvedRoutes, slices.Clone(routes))
	return nil
}

// policyPushes counts full policy replacements into the tenant's box. It is the
// number a wedged gateway's retries must not move.
func (b *recordingBox) policyPushes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.policies
}

// approvals returns each (node, routes) replacement in order.
func (b *recordingBox) approvals() (nodes []string, routes [][]string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.approved), slices.Clone(b.approvedRoutes)
}

func (b *recordingBox) register(hostname, nodeID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.found == nil {
		b.found = map[string]string{}
	}
	b.found[hostname] = nodeID
}

func (b *recordingBox) snapshot() (lookedUp, approved []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.lookedUp), slices.Clone(b.approved)
}

// approvalsOf counts the replacements written to ONE node. A whole-box total
// cannot answer "was this gateway touched again" once a test also drives an
// unrelated marker gateway to prove the worker ran.
func (b *recordingBox) approvalsOf(nodeID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, id := range b.approved {
		if id == nodeID {
			n++
		}
	}
	return n
}

// lookups counts how many times one cluster's gateway was resolved — the
// per-cluster retry evidence.
func (b *recordingBox) lookups(clusterID string) int {
	lookedUp, _ := b.snapshot()
	hostname := handlers.GatewayHostname(clusterID)
	n := 0
	for _, h := range lookedUp {
		if h == hostname {
			n++
		}
	}
	return n
}

func workerTestReconciler(t *testing.T, box PolicyEnsurer, debounce time.Duration, opts ...Option) (*Reconciler, *state.Store) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := state.New()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	base := []Option{
		WithEnsurerFactory(func(*state.MeshEndpoint) PolicyEnsurer { return box }),
		WithDebounce(debounce),
		WithBackoff(2 * time.Millisecond),
		WithMaxBackoff(20 * time.Millisecond),
	}
	r := New(ctx, s, log, append(base, opts...)...)
	s.AddCustomer(&state.Customer{ID: "cust_a", Token: "tok"})
	if err := s.SetCustomerMesh("cust_a", &state.MeshEndpoint{
		Provider: "headscale", LoginServer: "https://hs.example", APIKey: "k", User: "u",
	}); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}
	// Each cluster reports its OWN CIDRs, so any cross-cluster copy shows up as
	// the wrong set on a gateway rather than as an indistinguishable repeat.
	reportRoutes(t, s, "cust_a", "prod-1", workerRoutesA)
	reportRoutes(t, s, "cust_a", "prod-2", workerRoutesB)
	return r, s
}

// claimCluster is the registration the agent stream performs on connect, before
// it will accept a single report from that connection. The store requires that
// claim to still stand when a report lands, so tests seed intent through it for
// the same reason production does. An EXISTING row — API-registered or
// platform-managed — is claimed the way its cluster-scoped credential claims it.
func claimCluster(t *testing.T, s *state.Store, customerID, clusterID string) {
	t.Helper()
	_, _, err := s.ClaimAgentCluster(customerID, clusterID, true, time.Now().UTC())
	if errors.Is(err, state.ErrClusterNotFound) {
		_, _, err = s.ClaimAgentCluster(customerID, clusterID, false, time.Now().UTC())
	}
	if err != nil {
		t.Fatalf("ClaimAgentCluster %s/%s: %v", customerID, clusterID, err)
	}
}

// reportRoutes is one connector's route report, claim included.
func reportRoutes(t *testing.T, s *state.Store, customerID, clusterID string, routes []string) {
	t.Helper()
	claimCluster(t, s, customerID, clusterID)
	if _, err := s.SetCustomerGatewayRoutes(customerID, clusterID, routes); err != nil {
		t.Fatalf("SetCustomerGatewayRoutes %s/%s: %v", customerID, clusterID, err)
	}
}

// intentOf reads a customer's route state through the store's immutable
// SNAPSHOT. Tests must never reach for it through CustomerByID: that hands back
// the LIVE record, and the worker goroutine these tests are driving replaces
// its route maps and slices underneath any reader — a data race, not a stale
// read. CustomerGatewayRouteIntent is what production uses for the same reason.
func intentOf(s *state.Store, customerID string) state.GatewayRouteIntent {
	intent, _ := s.CustomerGatewayRouteIntent(customerID)
	return intent
}

// connect registers a live socket, which is the store evidence the reconcile
// consults before deciding a missing gateway is still worth retrying.
func connect(t *testing.T, s *state.Store, customerID, clusterID string) {
	t.Helper()
	s.AddAgent(&state.Agent{
		ID:          "agent_" + clusterID,
		CustomerID:  customerID,
		ClusterID:   clusterID,
		ConnectedAt: time.Now().UTC(),
	})
}

// idleReconciler's worker goroutines exit before doing any work, so the pending
// bookkeeping Enqueue leaves behind can be inspected exactly.
func idleReconciler(t *testing.T) *Reconciler {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return New(ctx, state.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func workerFor(t *testing.T, r *Reconciler, customerID string) *worker {
	t.Helper()
	r.mu.Lock()
	w := r.workers[customerID]
	r.mu.Unlock()
	if w == nil {
		t.Fatalf("no worker for %s", customerID)
	}
	return w
}

func pendingClusters(t *testing.T, r *Reconciler, customerID string) []string {
	t.Helper()
	w := workerFor(t, r, customerID)
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Sorted(maps.Keys(w.clusters))
}

// clusterForce and policyForce read the force each queued item carries on its
// own. There is deliberately no tenant-wide bit to read: one would leak a
// connect-time re-assert onto whatever else happened to be queued.
func clusterForce(t *testing.T, r *Reconciler, customerID, clusterID string) bool {
	t.Helper()
	w := workerFor(t, r, customerID)
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.clusters[clusterID].force
}

func policyForce(t *testing.T, r *Reconciler, customerID string) bool {
	t.Helper()
	w := workerFor(t, r, customerID)
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.policyForce
}

// attemptsFor reads one item's place in the retry ladder. -1 means the item has
// no ladder at all: it has never failed, or a success/new event cleared it.
func attemptsFor(t *testing.T, r *Reconciler, customerID, key string) int {
	t.Helper()
	w := workerFor(t, r, customerID)
	w.mu.Lock()
	defer w.mu.Unlock()
	st, ok := w.retry[key]
	if !ok {
		return -1
	}
	return st.attempts
}

// Coalescing is what keeps the debounce cheap, so it must not cost a cluster
// identity: two clusters queued inside one window are both still named when the
// worker drains. "Last one wins" would leave the other gateway's routes pending
// while PushedGatewayRoutes claims the tenant converged.
func TestEnqueueCoalescesEveryClusterIdentity(t *testing.T) {
	r := idleReconciler(t)
	r.Enqueue("cust_a", "prod-1", false)
	r.Enqueue("cust_a", "prod-2", true)
	r.Enqueue("cust_a", "prod-1", false) // a repeat adds nothing

	if got := pendingClusters(t, r, "cust_a"); !slices.Equal(got, []string{"prod-1", "prod-2"}) {
		t.Fatalf("pending clusters = %v, want both prod-1 and prod-2", got)
	}
	// Force is sticky PER ITEM, so the forced enqueue arms prod-2 and the
	// tenant's policy — and leaves prod-1, which nothing forced, alone.
	if !clusterForce(t, r, "cust_a", "prod-2") {
		t.Errorf("prod-2 force = false, want the sticky force from its own enqueue")
	}
	if clusterForce(t, r, "cust_a", "prod-1") {
		t.Errorf("prod-1 force = true, want false — its enqueues never forced")
	}
	if !policyForce(t, r, "cust_a") {
		t.Errorf("policy force = false, want sticky force from the second enqueue")
	}

	// A caller that cannot name a cluster asks for policy-only work; it must not
	// invent one, and it must not disturb the identities already queued.
	r.Enqueue("cust_a", "", false)
	if got := pendingClusters(t, r, "cust_a"); !slices.Equal(got, []string{"prod-1", "prod-2"}) {
		t.Fatalf("pending clusters after an unnamed enqueue = %v, want prod-1 and prod-2", got)
	}
}

// End to end through the worker: two clusters connecting inside one debounce
// window both get their own gateway converged with their OWN routes, and
// neither reaches the other's.
func TestWorkerConvergesEveryCoalescedCluster(t *testing.T) {
	box := &recordingBox{}
	box.register(handlers.GatewayHostname("prod-1"), "7")
	box.register(handlers.GatewayHostname("prod-2"), "8")
	r, s := workerTestReconciler(t, box, 50*time.Millisecond)

	r.Enqueue("cust_a", "prod-1", true)
	r.Enqueue("cust_a", "prod-2", true)

	waitFor(t, func() bool {
		_, approved := box.snapshot()
		return len(approved) >= 2
	}, "both gateways approved")

	lookedUp, approved := box.snapshot()
	slices.Sort(approved)
	if !slices.Equal(approved, []string{"7", "8"}) {
		t.Fatalf("approved nodes = %v, want exactly 7 and 8", approved)
	}
	for _, h := range lookedUp {
		if h != handlers.GatewayHostname("prod-1") && h != handlers.GatewayHostname("prod-2") {
			t.Fatalf("resolved %q, which no queued event named", h)
		}
	}
	waitFor(t, func() bool {
		intent := intentOf(s, "cust_a")
		return slices.Equal(intent.PushedRoutesFor("prod-1"), workerRoutesA) &&
			slices.Equal(intent.PushedRoutesFor("prod-2"), workerRoutesB)
	}, "each cluster's own pushed set to be recorded")
}

// A missing gateway whose agent is still connected keeps retrying under the
// SAME cluster identity the original event named, holds that cluster's pushed
// state back, and heals the moment the pod registers. Losing the identity on
// the retry path would turn the retry into policy-only work that "succeeds"
// while the routes stay unapproved.
func TestWorkerRetryPreservesClusterIdentityUntilGatewayAppears(t *testing.T) {
	box := &recordingBox{} // prod-1's gateway has not registered
	r, s := workerTestReconciler(t, box, time.Millisecond)
	connect(t, s, "cust_a", "prod-1")

	r.Enqueue("cust_a", "prod-1", true)

	waitFor(t, func() bool {
		return box.lookups("prod-1") >= 2 // it really is retrying, not giving up
	}, "a retry of the missing gateway")

	lookedUp, _ := box.snapshot()
	for _, h := range lookedUp {
		if h != handlers.GatewayHostname("prod-1") {
			t.Fatalf("retry looked up %q, want only prod-1's gateway", h)
		}
	}
	if got := intentOf(s, "cust_a").PushedRoutesFor("prod-1"); len(got) != 0 {
		t.Fatalf("prod-1 pushed set = %v, want empty while the gateway is missing", got)
	}

	// The gateway pod comes up; the pending retry converges it.
	box.register(handlers.GatewayHostname("prod-1"), "7")
	waitFor(t, func() bool {
		_, approved := box.snapshot()
		return len(approved) >= 1
	}, "the gateway to be approved once it registers")
	waitFor(t, func() bool {
		return slices.Equal(intentOf(s, "cust_a").PushedRoutesFor("prod-1"), workerRoutesA)
	}, "prod-1's pushed set to advance after convergence")

	// Success clears that cluster's ladder, so the next real change starts from
	// the base backoff instead of the capped one this gateway climbed.
	waitFor(t, func() bool {
		return attemptsFor(t, r, "cust_a", "prod-1") == -1
	}, "prod-1's retry ladder to be cleared by the successful converge")
}

// Only the cluster that FAILED is retried. A tenant whose A converged and whose
// B is wedged must not have A's gateway re-approved on every one of B's retries
// — that is load on the box, and each repeat is another chance to write the
// wrong set.
func TestWorkerRetriesOnlyTheFailedCluster(t *testing.T) {
	box := &recordingBox{}
	box.register(handlers.GatewayHostname("prod-1"), "7") // prod-2's gateway never comes up
	r, s := workerTestReconciler(t, box, time.Millisecond)
	connect(t, s, "cust_a", "prod-1")
	connect(t, s, "cust_a", "prod-2")

	r.Enqueue("cust_a", "prod-1", true)
	r.Enqueue("cust_a", "prod-2", true)

	waitFor(t, func() bool {
		return box.lookups("prod-2") >= 4
	}, "prod-2 to be retried several times")

	if got := box.lookups("prod-1"); got != 1 {
		t.Fatalf("prod-1 gateway resolved %d times, want exactly 1 (it converged on the first pass)", got)
	}
	_, approved := box.snapshot()
	if !slices.Equal(approved, []string{"7"}) {
		t.Fatalf("approved nodes = %v, want exactly node 7 once", approved)
	}
	intent := intentOf(s, "cust_a")
	if !slices.Equal(intent.PushedRoutesFor("prod-1"), workerRoutesA) {
		t.Fatalf("prod-1 pushed set = %v, want %v", intent.PushedRoutesFor("prod-1"), workerRoutesA)
	}
	if got := intent.PushedRoutesFor("prod-2"); len(got) != 0 {
		t.Fatalf("prod-2 pushed set = %v, want empty", got)
	}
	// The policy landed on the first pass, so its convergence state advanced
	// even while prod-2's node is still missing.
	wantUnion := []string{"10.42.0.0/16", "10.43.0.0/16"}
	if !slices.Equal(intent.PushedUnion, wantUnion) {
		t.Fatalf("PushedGatewayRoutes = %v, want the union %v", intent.PushedUnion, wantUnion)
	}
}

// New work must INTERRUPT a pending backoff, not queue behind it. A gateway pod
// that comes up while its cluster is sitting on a capped five-minute retry has
// to be converged when the agent reconnects, not five minutes later.
func TestWorkerNewWorkInterruptsRetryBackoff(t *testing.T) {
	const backoff = 3 * time.Second
	box := &recordingBox{} // prod-1's gateway is missing, so the first pass fails
	r, s := workerTestReconciler(t, box, time.Millisecond,
		WithBackoff(backoff), WithMaxBackoff(backoff))
	connect(t, s, "cust_a", "prod-1")

	r.Enqueue("cust_a", "prod-1", true)
	// Wait for the ladder entry, not just the lookup: that is the point where
	// the worker is provably asleep on the backoff timer.
	waitFor(t, func() bool {
		return attemptsFor(t, r, "cust_a", "prod-1") == 1
	}, "the first attempt to fail and enter its backoff")

	// The pod comes up and the agent reconnects. Without an interruptible sleep
	// this would wait out the full backoff.
	box.register(handlers.GatewayHostname("prod-1"), "7")
	start := time.Now()
	r.Enqueue("cust_a", "prod-1", true)
	waitFor(t, func() bool {
		_, approved := box.snapshot()
		return len(approved) >= 1
	}, "the new work to be processed without waiting out the backoff")

	if elapsed := time.Since(start); elapsed >= backoff {
		t.Fatalf("new work took %v to be processed, want well under the %v backoff", elapsed, backoff)
	}
}

// A reconnect or report for one cluster says nothing about another cluster's
// gateway, so it must not reset that cluster's ladder. A tenant whose A
// reconnects in a loop would otherwise pin permanently-missing B at the base
// backoff forever, which is exactly the load the cap exists to stop.
func TestEnqueueResetsOnlyTheNamedClusterLadder(t *testing.T) {
	r := idleReconciler(t)
	r.Enqueue("cust_a", "prod-1", false)
	w := workerFor(t, r, "cust_a")

	set := func(key string, n int) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.retry[key] = &retryState{attempts: n, due: time.Now().Add(time.Hour)}
	}

	set("prod-1", 4)
	set("prod-2", 5)
	set(policyRetryKey, 6)

	// New work for prod-1: its own ladder and the tenant-wide policy ladder
	// start over, because the event is evidence about both.
	r.Enqueue("cust_a", "prod-1", false)
	if got := attemptsFor(t, r, "cust_a", "prod-1"); got != -1 {
		t.Errorf("prod-1 ladder after its own event = %d, want cleared", got)
	}
	if got := attemptsFor(t, r, "cust_a", policyRetryKey); got != -1 {
		t.Errorf("policy ladder after a report = %d, want cleared", got)
	}
	if got := attemptsFor(t, r, "cust_a", "prod-2"); got != 5 {
		t.Fatalf("prod-2 ladder after an unrelated cluster's event = %d, want 5 (untouched)", got)
	}

	// The same holds for a forced reconnect.
	set("prod-1", 4)
	set("prod-2", 5)
	r.Enqueue("cust_a", "prod-1", true)
	if got := attemptsFor(t, r, "cust_a", "prod-2"); got != 5 {
		t.Fatalf("prod-2 ladder after a reconnect on prod-1 = %d, want 5 (untouched)", got)
	}
}

// A missing gateway whose agent has DISCONNECTED stops being retried: nothing
// can register that pod until the connector returns, and its connect forces a
// reconcile carrying the same cluster id. The decision uses current store
// evidence — a live socket — not a guessed timeout.
func TestWorkerStopsRetryingAfterAgentDisconnects(t *testing.T) {
	box := &recordingBox{} // no gateway registered
	r, s := workerTestReconciler(t, box, time.Millisecond)

	// No socket for prod-1: the agent is gone.
	r.Enqueue("cust_a", "prod-1", true)
	waitFor(t, func() bool {
		return box.lookups("prod-1") >= 1
	}, "the one attempt for a disconnected cluster")

	// Drive an unrelated gateway to completion. The worker is serialized, so a
	// retry of prod-1 that was going to happen would have happened first.
	flushWorker(t, r, box, "cust_a", "prod-2", "8")
	if got := box.lookups("prod-1"); got != 1 {
		t.Fatalf("prod-1 gateway resolved %d times, want exactly 1 (no retry without a connected agent)", got)
	}
	if got := pendingClusters(t, r, "cust_a"); len(got) != 0 {
		t.Fatalf("pending clusters = %v, want none (the cluster was given up on)", got)
	}
	if got := intentOf(s, "cust_a").PushedRoutesFor("prod-1"); len(got) != 0 {
		t.Fatalf("prod-1 pushed set = %v, want empty (nothing converged)", got)
	}

	// The connector comes back and forces a reconcile: retries resume, and the
	// gateway converges once its pod registers.
	connect(t, s, "cust_a", "prod-1")
	r.Enqueue("cust_a", "prod-1", true)
	waitFor(t, func() bool {
		return box.lookups("prod-1") >= 3
	}, "retries to resume once the agent reconnects")

	box.register(handlers.GatewayHostname("prod-1"), "7")
	waitFor(t, func() bool {
		return slices.Equal(intentOf(s, "cust_a").PushedRoutesFor("prod-1"), workerRoutesA)
	}, "prod-1 to converge after the reconnect")
}

// The retry ladder doubles and caps, so a gateway that is never coming back
// stops costing its tenant's box a request every backoff forever.
func TestRetryDelayGrowsAndCaps(t *testing.T) {
	r := New(context.Background(), state.New(), slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithBackoff(time.Second), WithMaxBackoff(8*time.Second))

	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
	for attempts, w := range want {
		if got := r.retryDelay(attempts); got != w {
			t.Errorf("retryDelay(%d) = %v, want %v", attempts, got, w)
		}
	}
	// Far past the cap: still the cap, never overflowed into a negative or
	// absurd wait.
	if got := r.retryDelay(1000); got != 8*time.Second {
		t.Errorf("retryDelay(1000) = %v, want the 8s cap", got)
	}
	// A cap below the base backoff is a misconfiguration, not a zero wait.
	tight := New(context.Background(), state.New(), slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithBackoff(5*time.Second), WithMaxBackoff(time.Second))
	if got := tight.retryDelay(3); got != 5*time.Second {
		t.Errorf("retryDelay with cap below backoff = %v, want the 5s backoff", got)
	}
}

// settle is what keeps the ladders honest: a partial failure requeues exactly
// what failed, on its own ladder, and clears the rest.
func TestSettleRequeuesOnlyWhatFailed(t *testing.T) {
	r := idleReconciler(t)
	r.Enqueue("cust_a", "prod-1", true)
	r.Enqueue("cust_a", "prod-2", false)
	w := workerFor(t, r, "cust_a")

	now := time.Now()
	req, ok := w.take(now)
	if !ok || !slices.Equal(requestClusterIDs(req), []string{"prod-1", "prod-2"}) || !req.policy || !req.policyForce {
		t.Fatalf("take = %+v ok=%v, want both clusters plus the policy, forced", req, ok)
	}
	// The force rode with the item that was enqueued forced, and with nothing
	// else: prod-2's enqueue never asked for a re-assert.
	if !req.clusters[0].force || req.clusters[1].force {
		t.Fatalf("drained cluster force = %v/%v, want prod-1 forced and prod-2 not",
			req.clusters[0].force, req.clusters[1].force)
	}

	r.settle(w, req, reconcileOutcome{failedClusters: []string{"prod-2"}}, now)
	if got := pendingClusters(t, r, "cust_a"); !slices.Equal(got, []string{"prod-2"}) {
		t.Fatalf("requeued clusters = %v, want only prod-2", got)
	}
	if got := attemptsFor(t, r, "cust_a", "prod-2"); got != 1 {
		t.Fatalf("prod-2 attempts = %d, want 1", got)
	}
	if got := attemptsFor(t, r, "cust_a", "prod-1"); got != -1 {
		t.Fatalf("prod-1 ladder = %d, want cleared (it converged)", got)
	}
	if got := attemptsFor(t, r, "cust_a", policyRetryKey); got != -1 {
		t.Fatalf("policy ladder = %d, want cleared (it converged)", got)
	}
	// prod-2 was not forced, and a partial failure must not hand it someone
	// else's re-assert. The policy converged, so its force is consumed too.
	if clusterForce(t, r, "cust_a", "prod-2") {
		t.Error("prod-2 force = true after a partial failure, want the force that belonged to prod-1 to stay there")
	}
	if policyForce(t, r, "cust_a") {
		t.Error("policy force = true after the policy converged, want it consumed")
	}

	// Items inside their backoff are not taken; the queue keeps them.
	if _, ok := w.take(now); ok {
		t.Fatal("take returned work that is still inside its backoff")
	}
	if _, ok := w.take(now.Add(time.Minute)); !ok {
		t.Fatal("take returned nothing once the backoff elapsed")
	}
}

// New work that lands DURING an attempt must not be pushed behind the backoff
// that attempt earns. Enqueue clears the item's ladder because the event is
// fresh evidence — a reconnect, a delete, a new report — and settle then runs
// with an outcome computed before any of it existed. Re-arming there makes a
// gateway that just came up wait out a climb it had nothing to do with.
func TestSettleDoesNotRearmLadderClearedByNewerEnqueue(t *testing.T) {
	r := idleReconciler(t)
	r.Enqueue("cust_a", "prod-1", false)
	r.Enqueue("cust_a", "prod-2", false)
	w := workerFor(t, r, "cust_a")

	now := time.Now()
	req, ok := w.take(now)
	if !ok || !slices.Equal(requestClusterIDs(req), []string{"prod-1", "prod-2"}) {
		t.Fatalf("take = %+v ok=%v, want both clusters drained", req, ok)
	}

	// The provider call is in flight. prod-1's gateway pod comes up and its
	// agent reconnects; the attempt about to fail knows nothing about it.
	r.Enqueue("cust_a", "prod-1", true)

	// Only now does the OLD attempt report that everything failed.
	r.settle(w, req, reconcileOutcome{policyFailed: true, failedClusters: []string{"prod-1", "prod-2"}}, now)

	if got := attemptsFor(t, r, "cust_a", "prod-1"); got != -1 {
		t.Errorf("prod-1 ladder = %d, want cleared — the reconnect that landed mid-attempt owns it", got)
	}
	if got := attemptsFor(t, r, "cust_a", policyRetryKey); got != -1 {
		t.Errorf("policy ladder = %d, want cleared — the same reconnect is evidence about it", got)
	}
	// prod-2 had no new evidence, so its failure ladders exactly as before.
	if got := attemptsFor(t, r, "cust_a", "prod-2"); got != 1 {
		t.Errorf("prod-2 attempts = %d, want 1", got)
	}

	// The observable contract: the re-enqueued work is drainable at the SAME
	// instant the failure was settled, while prod-2 serves its backoff.
	next, ok := w.take(now)
	if !ok || !slices.Equal(requestClusterIDs(next), []string{"prod-1"}) {
		t.Fatalf("second take = %+v ok=%v, want prod-1 alone and immediately due", next, ok)
	}
	if !next.policy {
		t.Error("policy not drained with the reconnect that armed it")
	}
	if !next.clusters[0].force {
		t.Error("prod-1 lost the force its reconnect carried")
	}
}

// gatedBox hands every gateway lookup to the test: the call parks until the
// test answers it, and the answer is the node id it returns. Both channels are
// unbuffered, so "an Enqueue landed DURING the provider call" is something the
// test arranges rather than a race it hopes to win.
type gatedBox struct {
	recordingBox
	entered chan string
	answer  chan string
}

func (b *gatedBox) FindDeviceByHostname(_ context.Context, hostname string) (string, error) {
	b.entered <- hostname
	return <-b.answer, nil
}

// The same defect end to end. A reconnect that arrives while the provider call
// is in flight must be served on the next pass, not after the failing attempt's
// capped backoff — which here is ten times the test's own patience, so a retry
// arriving at all is the proof.
func TestWorkerNewWorkDuringAttemptIsNotDelayedByItsBackoff(t *testing.T) {
	const backoff = 30 * time.Second
	box := &gatedBox{entered: make(chan string), answer: make(chan string)}
	r, s := workerTestReconciler(t, box, time.Millisecond, WithBackoff(backoff), WithMaxBackoff(backoff))
	connect(t, s, "cust_a", "prod-1")

	r.Enqueue("cust_a", "prod-1", true)
	<-box.entered // the first attempt is parked inside the provider call

	// The agent reconnects MID-CALL. The attempt about to fail knows nothing
	// about it, and its own lookup still finds no gateway.
	r.Enqueue("cust_a", "prod-1", true)
	box.answer <- "" // attempt one fails and settles

	// The pod is up by the time the retry looks. Without the generation stamp
	// the failed attempt has just re-armed a 30s ladder over the reconnect that
	// cleared it, and this receive never completes.
	select {
	case <-box.entered:
	case <-time.After(3 * time.Second):
		t.Fatalf("no retry within 3s: the reconnect was queued behind the failed attempt's %v backoff", backoff)
	}
	box.answer <- "7"

	waitFor(t, func() bool {
		return box.approvalsOf("7") >= 1
	}, "the gateway to converge on the reconnect's own attempt")
}

// tenantOwner mints the human whose authority the tenant-surface cluster
// delete runs under, so these tests drive the REAL delete rather than a
// hand-built approximation of the state it leaves.
func tenantOwner(t *testing.T, s *state.Store, customerID string) string {
	t.Helper()
	acct, err := s.UpsertAccount("https://id.test", "owner-"+customerID, state.AccountProfile{
		Email: "owner@acme.com", EmailVerified: true, Name: "Owner",
	})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	if _, err := s.AddTenantMembership(acct.ID, customerID, state.RoleOwner); err != nil {
		t.Fatalf("AddTenantMembership: %v", err)
	}
	return acct.ID
}

func deleteCluster(t *testing.T, s *state.Store, customerID, ownerID, clusterID string) {
	t.Helper()
	if _, err := s.DeleteTenantCluster(customerID, ownerID, clusterID,
		state.HumanActor(ownerID, customerID)); err != nil {
		t.Fatalf("DeleteTenantCluster %s: %v", clusterID, err)
	}
}

// A deleted cluster's dedicated gateway gets ONE exact empty replacement — the
// withdrawal of everything central approved on it — and its sibling's node is
// never touched. Afterwards the tombstone that drove the withdrawal is gone, so
// the pushed map cannot grow by a dead entry per deleted cluster.
func TestWorkerWithdrawsDeletedClusterGatewayRoutes(t *testing.T) {
	box := &recordingBox{}
	box.register(handlers.GatewayHostname("prod-1"), "7")
	box.register(handlers.GatewayHostname("prod-2"), "8")
	r, s := workerTestReconciler(t, box, time.Millisecond)
	owner := tenantOwner(t, s, "cust_a")

	r.Enqueue("cust_a", "prod-1", true)
	r.Enqueue("cust_a", "prod-2", true)
	waitFor(t, func() bool {
		intent := intentOf(s, "cust_a")
		return slices.Equal(intent.PushedRoutesFor("prod-1"), workerRoutesA) &&
			slices.Equal(intent.PushedRoutesFor("prod-2"), workerRoutesB)
	}, "both gateways to converge before the delete")
	nodesBefore, _ := box.approvals()
	if len(nodesBefore) != 2 {
		t.Fatalf("approvals before the delete = %v, want one per gateway", nodesBefore)
	}

	deleteCluster(t, s, "cust_a", owner, "prod-1")
	r.EnqueueClusterRemoval("cust_a", "prod-1")

	waitFor(t, func() bool {
		return len(intentOf(s, "cust_a").PushedByCluster["prod-1"]) == 0
	}, "prod-1's pushed tombstone to be cleared by the withdrawal")

	nodes, routes := box.approvals()
	if len(nodes) != 3 {
		t.Fatalf("approvals = %v, want exactly one more than before the delete", nodes)
	}
	if nodes[2] != "7" {
		t.Fatalf("withdrawal hit node %q, want prod-1's node 7", nodes[2])
	}
	if len(routes[2]) != 0 {
		t.Fatalf("withdrawal sent %v, want the exact empty replacement", routes[2])
	}

	intent := intentOf(s, "cust_a")
	if _, ok := intent.PushedByCluster["prod-1"]; ok {
		t.Fatalf("pushed tombstone survived the withdrawal: %v", intent.PushedByCluster)
	}
	if !slices.Equal(intent.PushedRoutesFor("prod-2"), workerRoutesB) {
		t.Fatalf("prod-2 pushed set = %v, want the sibling untouched", intent.PushedRoutesFor("prod-2"))
	}
	// The tenant's policy union shrank to the surviving cluster, and the policy
	// that carried it landed.
	if !slices.Equal(intent.Union, workerRoutesB) {
		t.Fatalf("union = %v, want only the surviving cluster's %v", intent.Union, workerRoutesB)
	}
	waitFor(t, func() bool {
		return slices.Equal(intentOf(s, "cust_a").PushedUnion, workerRoutesB)
	}, "the shrunk union to be pushed")

	// Idempotent: a repeat removal has no tombstone left, so it touches no node.
	r.EnqueueClusterRemoval("cust_a", "prod-1")
	flushWorker(t, r, box, "cust_a", "prod-2", "8")
	if got := box.approvalsOf("7"); got != 2 {
		t.Fatalf("writes to prod-1's node = %d, want 2 (the converge and the one withdrawal)", got)
	}
}

// A deleted cluster whose gateway pod is already gone is ALREADY withdrawn:
// an unregistered node holds no approval. That is a clean completion, not the
// missing-gateway error, so the tombstone is cleared and nothing retries.
func TestWorkerWithdrawalCompletesWhenGatewayNodeIsGone(t *testing.T) {
	box := &recordingBox{} // prod-1's gateway registers, then the pod is torn down
	box.register(handlers.GatewayHostname("prod-1"), "7")
	r, s := workerTestReconciler(t, box, time.Millisecond)
	owner := tenantOwner(t, s, "cust_a")

	r.Enqueue("cust_a", "prod-1", true)
	waitFor(t, func() bool {
		return slices.Equal(intentOf(s, "cust_a").PushedRoutesFor("prod-1"), workerRoutesA)
	}, "prod-1 to converge")

	box.register(handlers.GatewayHostname("prod-1"), "") // the pod is gone
	deleteCluster(t, s, "cust_a", owner, "prod-1")
	r.EnqueueClusterRemoval("cust_a", "prod-1")

	waitFor(t, func() bool {
		_, ok := intentOf(s, "cust_a").PushedByCluster["prod-1"]
		return !ok
	}, "the tombstone to be cleared against a node that is already gone")

	if nodes, _ := box.approvals(); len(nodes) != 1 {
		t.Fatalf("approvals = %v, want only the original converge (a missing node needs no write)", nodes)
	}
	// Clean completion: nothing is left queued to climb a retry ladder.
	if got := pendingClusters(t, r, "cust_a"); len(got) != 0 {
		t.Fatalf("pending clusters = %v, want none (the withdrawal completed)", got)
	}
	if got := attemptsFor(t, r, "cust_a", "prod-1"); got != -1 {
		t.Fatalf("prod-1 retry ladder = %d, want cleared", got)
	}
}

// The withdrawal is reachable ONLY through the explicit lifecycle delete. An
// ordinary agent enqueue for a cluster whose routes are gone must leave that
// gateway's approvals exactly where they are — an agent whose CIDR lookup has
// not resolved reports nothing, and taking that as a teardown would cut a live
// tenant's data path.
func TestWorkerPlainEnqueueNeverWithdrawsRoutes(t *testing.T) {
	box := &recordingBox{}
	box.register(handlers.GatewayHostname("prod-1"), "7")
	r, s := workerTestReconciler(t, box, time.Millisecond)
	owner := tenantOwner(t, s, "cust_a")

	r.Enqueue("cust_a", "prod-1", true)
	waitFor(t, func() bool {
		return slices.Equal(intentOf(s, "cust_a").PushedRoutesFor("prod-1"), workerRoutesA)
	}, "prod-1 to converge")

	// An empty report is ignored by the store, so the desired set is untouched;
	// then drop it the way a delete would, and drive it with a plain enqueue.
	if changed, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", nil); err != nil || changed {
		t.Fatalf("empty report changed=%v err=%v, want ignored", changed, err)
	}
	deleteCluster(t, s, "cust_a", owner, "prod-1")
	r.Enqueue("cust_a", "prod-1", true)

	flushWorker(t, r, box, "cust_a", "prod-2", "8")
	if got := box.approvalsOf("7"); got != 1 {
		t.Fatalf("writes to prod-1's node = %d, want only the original converge", got)
	}
	if got := intentOf(s, "cust_a").PushedRoutesFor("prod-1"); !slices.Equal(got, workerRoutesA) {
		t.Fatalf("prod-1 tombstone = %v, want %v (a plain enqueue may not withdraw)", got, workerRoutesA)
	}
}

// Retrying one wedged gateway must not re-PUT the tenant's current policy. The
// policy converged on the first pass, so nothing about it is stale — and a
// retry that dragged it along would write an identical policy document, and an
// identical Postgres customer row, for the life of the process.
func TestWorkerNodeRetryDoesNotRePushPolicy(t *testing.T) {
	box := &recordingBox{} // prod-1's gateway never registers
	r, s := workerTestReconciler(t, box, time.Millisecond)
	connect(t, s, "cust_a", "prod-1")

	r.Enqueue("cust_a", "prod-1", true) // a forced connect: policy AND node
	waitFor(t, func() bool {
		return box.lookups("prod-1") >= 5
	}, "the wedged gateway to be retried several times")

	if got := box.policyPushes(); got != 1 {
		t.Fatalf("EnsurePolicy calls = %d, want exactly 1 — the node retries must not re-push policy", got)
	}
	// The policy half converged on that one push, which is why the retries have
	// no policy work in them. SetCustomerPushedGatewayRoutes is only reachable
	// from that branch, so a policy call count of 1 is also a policy-state
	// write count of 1.
	wantUnion := []string{"10.42.0.0/16", "10.43.0.0/16"}
	if got := intentOf(s, "cust_a").PushedUnion; !slices.Equal(got, wantUnion) {
		t.Fatalf("PushedGatewayRoutes = %v, want the union %v", got, wantUnion)
	}
	// Force went with the item that was forced. The retried gateway keeps its
	// own re-assert; nothing hands one to the policy half it already converged.
	if policyForce(t, r, "cust_a") {
		t.Error("policy force = true, want it consumed by the push that succeeded")
	}
}

// A real policy change is different from a node retry: it is evidence about
// the tenant's policy, so it must interrupt the wedged gateway's backoff and
// push promptly rather than waiting the ladder out.
func TestWorkerPolicyChangeInterruptsNodeBackoff(t *testing.T) {
	const backoff = 5 * time.Second
	box := &recordingBox{} // prod-1's gateway never registers
	r, s := workerTestReconciler(t, box, time.Millisecond,
		WithBackoff(backoff), WithMaxBackoff(backoff))
	connect(t, s, "cust_a", "prod-1")

	r.Enqueue("cust_a", "prod-1", true)
	waitFor(t, func() bool {
		return attemptsFor(t, r, "cust_a", "prod-1") == 1
	}, "the first attempt to fail and enter its backoff")
	if got := box.policyPushes(); got != 1 {
		t.Fatalf("EnsurePolicy calls = %d, want 1 from the forced connect", got)
	}

	// A report moves the union. waitFor's own deadline is shorter than the
	// backoff, so reaching the second push at all is the proof it was not
	// queued behind prod-1's ladder.
	if changed, err := s.SetCustomerGatewayRoutes("cust_a", "prod-2", []string{"10.44.0.0/16"}); err != nil || !changed {
		t.Fatalf("report prod-2 routes changed=%v err=%v", changed, err)
	}
	start := time.Now()
	r.Enqueue("cust_a", "prod-2", false)
	waitFor(t, func() bool {
		return box.policyPushes() >= 2
	}, "the changed union to be pushed without waiting out the node backoff")
	if elapsed := time.Since(start); elapsed >= backoff {
		t.Fatalf("policy push took %v, want well under the %v node backoff", elapsed, backoff)
	}
}

// convergedThenDeleted drives both gateways to convergence and then deletes
// prod-1, leaving exactly the state a crash between the delete and its
// withdrawal leaves: no desired set for prod-1, its pushed entry still standing.
func convergedThenDeleted(t *testing.T) *state.Store {
	t.Helper()
	box := &recordingBox{}
	box.register(handlers.GatewayHostname("prod-1"), "7")
	box.register(handlers.GatewayHostname("prod-2"), "8")
	r, s := workerTestReconciler(t, box, time.Millisecond)
	owner := tenantOwner(t, s, "cust_a")

	r.Enqueue("cust_a", "prod-1", true)
	r.Enqueue("cust_a", "prod-2", true)
	waitFor(t, func() bool {
		intent := intentOf(s, "cust_a")
		return slices.Equal(intent.PushedRoutesFor("prod-1"), workerRoutesA) &&
			slices.Equal(intent.PushedRoutesFor("prod-2"), workerRoutesB)
	}, "both gateways to converge before the delete")

	// The delete lands durably; central dies before the withdrawal it enqueued
	// is drained. Nothing else is enqueued, so the old worker goes idle here.
	deleteCluster(t, s, "cust_a", owner, "prod-1")
	if got := intentOf(s, "cust_a").PushedRoutesFor("prod-1"); !slices.Equal(got, workerRoutesA) {
		t.Fatalf("delete left tombstone = %v, want %v", got, workerRoutesA)
	}
	return s
}

// restart models a process restart: the customer's DURABLE fields are reloaded
// into a fresh store with a fresh reconciler over it, so nothing the old
// worker held in memory survives — which is the whole point of the exercise.
func restart(t *testing.T, old *state.Store, customerID string, box PolicyEnsurer) (*Reconciler, *state.Store) {
	t.Helper()
	intent, err := old.CustomerGatewayRouteIntent(customerID)
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := state.New()
	r := New(ctx, s, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithEnsurerFactory(func(*state.MeshEndpoint) PolicyEnsurer { return box }),
		WithDebounce(time.Millisecond),
		WithBackoff(2*time.Millisecond),
		WithMaxBackoff(20*time.Millisecond))
	s.AddCustomer(&state.Customer{
		ID:                           customerID,
		Token:                        "tok",
		GatewayRoutes:                intent.Union,
		PushedGatewayRoutes:          intent.PushedUnion,
		GatewayRoutesByCluster:       intent.ByCluster,
		PushedGatewayRoutesByCluster: intent.PushedByCluster,
	})
	if err := s.SetCustomerMesh(customerID, &state.MeshEndpoint{
		Provider: "headscale", LoginServer: "https://hs.example", APIKey: "k", User: "u",
	}); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}
	return r, s
}

// A delete's withdrawal is queued on an IN-PROCESS worker, so a central that
// dies before it drains loses it — and nothing else brings it back. The
// connector is what was deleted, so no reconnect ever names that cluster again,
// while its chart-managed gateway pod keeps the approvals central gave it. The
// durable tombstone is what survives; the attach pass is what replays it.
func TestReconcileAttachReplaysDurableWithdrawalAfterRestart(t *testing.T) {
	s := convergedThenDeleted(t)

	box := &recordingBox{}
	box.register(handlers.GatewayHostname("prod-1"), "7")
	box.register(handlers.GatewayHostname("prod-2"), "8")
	r, reloaded := restart(t, s, "cust_a", box)

	if err := r.ReconcileAttach(context.Background(), "cust_a"); err != nil {
		t.Fatalf("ReconcileAttach: %v", err)
	}

	// Exactly one node write, and it is the deleted cluster's own gateway being
	// given the empty set. The live sibling is never opened: the work came from
	// the tombstone, not from the tenant's registry.
	nodes, routes := box.approvals()
	if len(nodes) != 1 || nodes[0] != "7" || len(routes[0]) != 0 {
		t.Fatalf("attach approvals = %v / %v, want one empty replacement on prod-1's node 7", nodes, routes)
	}
	intent := intentOf(reloaded, "cust_a")
	if _, ok := intent.PushedByCluster["prod-1"]; ok {
		t.Fatalf("tombstone survived the boot replay: %v", intent.PushedByCluster)
	}
	if !slices.Equal(intent.PushedRoutesFor("prod-2"), workerRoutesB) {
		t.Fatalf("prod-2 pushed set = %v, want the live sibling untouched", intent.PushedRoutesFor("prod-2"))
	}
	if got := box.policyPushes(); got != 1 {
		t.Fatalf("policy pushes = %d, want the one attach re-assert", got)
	}

	// A second boot finds nothing to replay: the tombstone IS the queue, and
	// the first pass cleared it.
	if err := r.ReconcileAttach(context.Background(), "cust_a"); err != nil {
		t.Fatalf("second ReconcileAttach: %v", err)
	}
	if got := box.approvalsOf("7"); got != 1 {
		t.Fatalf("writes to prod-1's node across two boots = %d, want 1", got)
	}
}

// A gateway pod that is already gone holds no approval, which is the exact
// state the withdrawal is trying to reach. That is a clean completion — the
// tombstone is cleared, and nothing is left retrying a node that will never
// answer.
func TestReconcileAttachReplayCompletesWhenGatewayNodeIsGone(t *testing.T) {
	s := convergedThenDeleted(t)

	box := &recordingBox{} // prod-1's pod was torn down with its cluster
	box.register(handlers.GatewayHostname("prod-2"), "8")
	r, reloaded := restart(t, s, "cust_a", box)

	if err := r.ReconcileAttach(context.Background(), "cust_a"); err != nil {
		t.Fatalf("ReconcileAttach: %v", err)
	}
	if nodes, _ := box.approvals(); len(nodes) != 0 {
		t.Fatalf("approvals = %v, want none — an unregistered node has nothing to take back", nodes)
	}
	if _, ok := intentOf(reloaded, "cust_a").PushedByCluster["prod-1"]; ok {
		t.Fatalf("tombstone survived a node that is already gone")
	}
}

// flakyBox refuses every gateway lookup until the test heals it. That is the
// retryable API failure: nothing has been PROVEN about the node, so the
// tombstone must stand and the work must outlive the boot pass.
type flakyBox struct {
	recordingBox
	healthy atomic.Bool
}

func (b *flakyBox) FindDeviceByHostname(ctx context.Context, hostname string) (string, error) {
	if !b.healthy.Load() {
		return "", errors.New("coordination server unavailable")
	}
	return b.recordingBox.FindDeviceByHostname(ctx, hostname)
}

// The tombstone is the only record that a withdrawal is owed, so it may only be
// cleared once the empty replacement has landed or the node is proven absent. A
// box that cannot answer proves neither — and the boot pass hands the work to
// the worker rather than being the one chance it gets.
func TestReconcileAttachReplayKeepsTombstoneUntilTheWithdrawalLands(t *testing.T) {
	s := convergedThenDeleted(t)

	box := &flakyBox{}
	r, reloaded := restart(t, s, "cust_a", box)

	if err := r.ReconcileAttach(context.Background(), "cust_a"); err == nil {
		t.Fatal("ReconcileAttach reported success while every gateway lookup was failing")
	}
	if got := intentOf(reloaded, "cust_a").PushedRoutesFor("prod-1"); !slices.Equal(got, workerRoutesA) {
		t.Fatalf("tombstone = %v, want %v kept until the withdrawal actually lands", got, workerRoutesA)
	}

	box.register(handlers.GatewayHostname("prod-1"), "7")
	box.healthy.Store(true)
	waitFor(t, func() bool {
		_, ok := intentOf(reloaded, "cust_a").PushedByCluster["prod-1"]
		return !ok
	}, "the worker to finish the withdrawal the boot pass could not")

	_, routes := box.approvals()
	if len(routes) != 1 || len(routes[0]) != 0 {
		t.Fatalf("approvals = %v, want exactly one empty replacement", routes)
	}
}

// pendingGatewayWork is where the withdrawal decision is actually made, so the
// three states it has to tell apart are asserted directly: a live cluster, a
// deleted one that had approvals, and one that never reported at all.
func TestPendingGatewayWorkWithdrawalIsExplicitAndBounded(t *testing.T) {
	intent := state.GatewayRouteIntent{
		ByCluster:       map[string][]string{"live": {"10.42.0.0/16"}},
		PushedByCluster: map[string][]string{"live": {"10.42.0.0/16"}, "deleted": {"10.43.0.0/16"}},
	}

	// A converged live cluster is not work, with or without the withdraw flag —
	// the flag never overrides a desired set that still exists.
	if got := pendingGatewayWork(intent, []clusterRequest{{id: "live"}, {id: "live", withdraw: true}}); len(got) != 0 {
		t.Fatalf("work for a converged live cluster = %+v, want none", got)
	}
	// A deleted cluster with approvals behind it is exactly one withdrawal.
	got := pendingGatewayWork(intent, []clusterRequest{{id: "deleted", withdraw: true}})
	if len(got) != 1 || !got[0].withdraw || got[0].clusterID != "deleted" || len(got[0].routes) != 0 {
		t.Fatalf("withdrawal work = %+v, want one empty replacement for deleted", got)
	}
	// Without the explicit flag it is a skip, not a teardown.
	if got := pendingGatewayWork(intent, []clusterRequest{{id: "deleted", force: true}}); len(got) != 0 {
		t.Fatalf("work for a deleted cluster without the removal flag = %+v, want none", got)
	}
	// A cluster that never reported has no approvals to take back, so even an
	// explicit removal is already clean.
	if got := pendingGatewayWork(intent, []clusterRequest{{id: "never", withdraw: true}}); len(got) != 0 {
		t.Fatalf("work for a never-reported cluster = %+v, want none", got)
	}
}

// flushWorker proves the customer's worker has completed a pass over everything
// queued before it was called. marker names a cluster whose gateway is made
// resolvable and then force-enqueued; the worker is ONE serialized goroutine, so
// by the time the marker's approval lands, whatever was already due has been
// attempted. That is what turns "and nothing else was touched" into an
// assertion, instead of a sleep picked to be long enough to hope.
func flushWorker(t *testing.T, r *Reconciler, box *recordingBox, customerID, marker, nodeID string) {
	t.Helper()
	box.register(handlers.GatewayHostname(marker), nodeID)
	before := box.approvalsOf(nodeID)
	r.Enqueue(customerID, marker, true)
	waitFor(t, func() bool {
		return box.approvalsOf(nodeID) > before
	}, "the flush marker "+marker+" to be approved")
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
