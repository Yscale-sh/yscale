// yscale:proprietary

package meshpolicy_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/decider"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
)

// The whole point of the node-level pass: the gateway of the cluster that
// triggered the reconcile ends up with EXACTLY that cluster's stored, validated
// route set approved, resolved by hostname from the cluster id the caller
// carried down. The burst pod pool is a policy approver only and must not reach
// the node.
func TestReconcileApprovesGatewayNodeRoutesExactly(t *testing.T) {
	m := &mockEnsurer{nodes: map[string]string{
		handlers.GatewayHostname("prod-1"): "7",
		// Another tenant's gateway on the same box. Resolving by hostname from
		// the reporting cluster is what keeps the approval off it.
		handlers.GatewayHostname("someone-else"): "99",
	}}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	registerCluster(t, s, "cust_a", "prod-1")
	want := []string{"10.42.0.0/16", "10.96.0.0/12"}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", want); err != nil {
		t.Fatalf("set routes: %v", err)
	}

	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, false); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("EnsurePolicy calls = %d, want 1 (policy push preserved)", len(m.calls))
	}
	if len(m.approvals) != 1 {
		t.Fatalf("ApproveNodeRoutes calls = %d, want 1", len(m.approvals))
	}
	got := m.approvals[0]
	if got.nodeID != "7" {
		t.Fatalf("approved node = %q, want 7 (the gateway for cluster prod-1)", got.nodeID)
	}
	if !slices.Equal(got.routes, want) {
		t.Fatalf("approved routes = %v, want %v exactly", got.routes, want)
	}
	if slices.Contains(got.routes, decider.BurstPodCIDRPool) {
		t.Errorf("burst pod pool %s approved on the gateway node; it is a policy approver only", decider.BurstPodCIDRPool)
	}
	if len(m.finds) != 1 || m.finds[0] != handlers.GatewayHostname("prod-1") {
		t.Errorf("hostname lookups = %v, want exactly the reporting cluster's gateway", m.finds)
	}
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), want) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v", pushedRoutes(t, s, "cust_a"), want)
	}
	if !slices.Equal(pushedClusterRoutes(t, s, "cust_a", "prod-1"), want) {
		t.Fatalf("prod-1 pushed set = %v, want %v", pushedClusterRoutes(t, s, "cust_a", "prod-1"), want)
	}
}

// Reconciling the same state again must be safe: the gate skips the round trip
// entirely, and a FORCED re-assert writes the identical set rather than
// accumulating or dropping approvals. The force case is the agent's connect
// path — the cluster's route AND policy state already match, and the re-assert
// still has to reach the named gateway, because a box that was rebuilt or
// drifted looks exactly like a converged one from central's records.
func TestReconcileGatewayApprovalIdempotent(t *testing.T) {
	m := &mockEnsurer{nodes: map[string]string{handlers.GatewayHostname("prod-1"): "7"}}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	registerCluster(t, s, "cust_a", "prod-1")
	want := []string{"10.42.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", want); err != nil {
		t.Fatalf("set routes: %v", err)
	}

	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, false); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(m.approvals) != 1 {
		t.Fatalf("approvals = %d, want 1", len(m.approvals))
	}

	// Unchanged state, not forced: the push gate short-circuits before the box
	// is touched at all. Both halves are recorded as converged.
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), want) ||
		!slices.Equal(pushedClusterRoutes(t, s, "cust_a", "prod-1"), want) {
		t.Fatalf("convergence state = union %v / prod-1 %v, want both %v",
			pushedRoutes(t, s, "cust_a"), pushedClusterRoutes(t, s, "cust_a", "prod-1"), want)
	}
	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, false); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if len(m.approvals) != 1 {
		t.Fatalf("approvals after identical reconcile = %d, want 1 (gate)", len(m.approvals))
	}
	if len(m.calls) != 1 {
		t.Fatalf("EnsurePolicy calls after identical reconcile = %d, want 1 (gate)", len(m.calls))
	}

	// Forced re-assert (the connect-time path): same node, same exact set, even
	// though nothing about the stored state moved.
	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, true); err != nil {
		t.Fatalf("reconcile forced: %v", err)
	}
	if len(m.approvals) != 2 {
		t.Fatalf("approvals after force = %d, want 2", len(m.approvals))
	}
	if m.approvals[1].nodeID != "7" || !slices.Equal(m.approvals[1].routes, want) {
		t.Fatalf("forced approval = %+v, want node 7 with %v", m.approvals[1], want)
	}
	if !slices.Equal(m.approved["7"], want) {
		t.Fatalf("converged approved set = %v, want %v", m.approved["7"], want)
	}
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), want) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v", pushedRoutes(t, s, "cust_a"), want)
	}
}

// Boot (attachHeadscaleFromEnv) runs before any agent has connected: no cluster
// identity is in hand and the tenant's gateway pod does not exist. That must
// converge the POLICY and report success — an attach that logs a failure every
// restart is an attach operators stop trusting — while touching no node at all.
func TestReconcilePolicyOnlySucceedsBeforeAnyGatewayExists(t *testing.T) {
	m := &mockEnsurer{} // no nodes: nothing has registered on the fresh box
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	// Routes survived the restart, so the boot push is NOT the empty baseline —
	// this is the case where a naive reconcile would demand a gateway node. The
	// registry row survived with them: reporting routes is what claimed it.
	want := []string{"10.42.0.0/16"}
	reportRoutes(t, s, "cust_a", "prod-1", want)

	if err := r.ReconcilePolicyOnly(context.Background(), "cust_a"); err != nil {
		t.Fatalf("boot policy-only reconcile: %v", err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("EnsurePolicy calls = %d, want 1", len(m.calls))
	}
	if len(m.finds) != 0 || len(m.approvals) != 0 {
		t.Fatalf("boot touched a node: finds=%v approvals=%+v", m.finds, m.approvals)
	}
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), want) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v (the policy really did converge)", pushedRoutes(t, s, "cust_a"), want)
	}
	// The policy converged; prod-1's gateway did not, and boot must not claim
	// otherwise — the connect-time force is what converges it.
	if got := pushedClusterRoutes(t, s, "cust_a", "prod-1"); len(got) != 0 {
		t.Fatalf("prod-1 pushed set after a policy-only boot = %v, want empty", got)
	}

	// The agent then connects and forces. Both halves re-assert; only the force
	// gets the node approved.
	m.nodes = map[string]string{handlers.GatewayHostname("prod-1"): "7"}
	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, true); err != nil {
		t.Fatalf("connect force reconcile: %v", err)
	}
	if len(m.approvals) != 1 || m.approvals[0].nodeID != "7" || !slices.Equal(m.approvals[0].routes, want) {
		t.Fatalf("approvals after connect force = %+v, want one call approving %v on node 7", m.approvals, want)
	}
}

// A customer persisted before the by-cluster fields existed has a union and
// nothing else. Boot must still re-assert that union as policy — it is the only
// route intent there is — and must NOT invent a gateway owner for it: which
// cluster reported those CIDRs was never recorded, so any node it picked would
// be a guess. The live cluster's post-connect report is what fills the gap.
func TestReconcileLegacyCustomerPolicyOnlyBootDoesNotGuessANode(t *testing.T) {
	m := &mockEnsurer{nodes: map[string]string{handlers.GatewayHostname("prod-1"): "7"}}
	r, s := newTestReconciler(t, m)
	legacy := []string{"10.42.0.0/16"}
	s.AddCustomer(&state.Customer{ID: "cust_legacy", Token: "legacy-tok", GatewayRoutes: legacy})
	if err := s.SetCustomerMesh("cust_legacy", &state.MeshEndpoint{
		Provider: "headscale", LoginServer: "https://hs.example", APIKey: "k", User: "u",
	}); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}
	registerCluster(t, s, "cust_legacy", "prod-1")

	if err := r.ReconcilePolicyOnly(context.Background(), "cust_legacy"); err != nil {
		t.Fatalf("legacy boot policy-only reconcile: %v", err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("EnsurePolicy calls = %d, want 1 (the legacy union is re-asserted)", len(m.calls))
	}
	if _, ok := m.calls[0].routeApprovers["10.42.0.0/16"]; !ok {
		t.Fatalf("legacy union not in the pushed policy: %v", m.calls[0].routeApprovers)
	}
	if len(m.finds) != 0 || len(m.approvals) != 0 {
		t.Fatalf("legacy boot picked a gateway: finds=%v approvals=%+v", m.finds, m.approvals)
	}
	if !slices.Equal(pushedRoutes(t, s, "cust_legacy"), legacy) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v", pushedRoutes(t, s, "cust_legacy"), legacy)
	}

	// Even a FORCED reconcile that names the tenant's registered cluster leaves
	// the node alone: naming a cluster is not evidence that cluster reported the
	// legacy routes.
	if err := r.Reconcile(context.Background(), "cust_legacy", []string{"prod-1"}, true); err != nil {
		t.Fatalf("forced legacy reconcile: %v", err)
	}
	if len(m.finds) != 0 || len(m.approvals) != 0 {
		t.Fatalf("forced legacy reconcile picked a gateway: finds=%v approvals=%+v", m.finds, m.approvals)
	}

	// The agent connects and reports, which is what establishes prod-1's own
	// exact set. Now — and only now — its gateway converges.
	if _, err := s.SetCustomerGatewayRoutes("cust_legacy", "prod-1", legacy); err != nil {
		t.Fatalf("post-connect report: %v", err)
	}
	if err := r.Reconcile(context.Background(), "cust_legacy", []string{"prod-1"}, true); err != nil {
		t.Fatalf("post-report reconcile: %v", err)
	}
	if len(m.approvals) != 1 || m.approvals[0].nodeID != "7" || !slices.Equal(m.approvals[0].routes, legacy) {
		t.Fatalf("approvals after the first report = %+v, want one call approving %v on node 7", m.approvals, legacy)
	}
}

// A gateway that has not registered yet leaves that cluster's routes unapproved,
// so an agent-triggered reconcile must FAIL and hold THAT CLUSTER's pushed state
// back — recording it as converged would mean the next report is the only thing
// that ever retries. The tenant-wide policy is a separate result: it really did
// land, so its own state advances.
func TestReconcileMissingGatewayNodeIsRetryable(t *testing.T) {
	t.Run("node absent then registers", func(t *testing.T) {
		m := &mockEnsurer{} // no nodes: the gateway pod is not up
		r, s := newTestReconciler(t, m)
		attachHeadscale(t, s, "cust_a")
		registerCluster(t, s, "cust_a", "prod-1")
		want := []string{"10.42.0.0/16"}
		if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", want); err != nil {
			t.Fatalf("set routes: %v", err)
		}

		if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, false); err == nil {
			t.Fatalf("expected a retryable error while the gateway node is missing")
		}
		if len(m.calls) != 1 {
			t.Fatalf("EnsurePolicy calls = %d, want 1 (policy push still happens)", len(m.calls))
		}
		if len(m.approvals) != 0 {
			t.Fatalf("approvals = %d, want 0", len(m.approvals))
		}
		if got := pushedClusterRoutes(t, s, "cust_a", "prod-1"); len(got) != 0 {
			t.Fatalf("prod-1 pushed set = %v, want empty (its node never converged)", got)
		}

		// The gateway comes up. The SAME unforced reconcile heals, because the
		// per-cluster gate still sees this cluster's set != its pushed set — even
		// though the policy half is now current.
		m.nodes = map[string]string{handlers.GatewayHostname("prod-1"): "7"}
		if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, false); err != nil {
			t.Fatalf("retry reconcile: %v", err)
		}
		if len(m.calls) != 1 {
			t.Fatalf("EnsurePolicy calls after retry = %d, want 1 (the policy was already converged)", len(m.calls))
		}
		if len(m.approvals) != 1 || !slices.Equal(m.approvals[0].routes, want) {
			t.Fatalf("approvals after retry = %+v, want one call with %v", m.approvals, want)
		}
		if !slices.Equal(pushedClusterRoutes(t, s, "cust_a", "prod-1"), want) {
			t.Fatalf("prod-1 pushed set after retry = %v, want %v", pushedClusterRoutes(t, s, "cust_a", "prod-1"), want)
		}
	})

	t.Run("lookup failure does not advance", func(t *testing.T) {
		m := &mockEnsurer{findErr: errors.New("box unreachable")}
		r, s := newTestReconciler(t, m)
		attachHeadscale(t, s, "cust_b")
		registerCluster(t, s, "cust_b", "prod-2")
		if _, err := s.SetCustomerGatewayRoutes("cust_b", "prod-2", []string{"10.42.0.0/16"}); err != nil {
			t.Fatalf("set routes: %v", err)
		}
		if err := r.Reconcile(context.Background(), "cust_b", []string{"prod-2"}, false); err == nil {
			t.Fatalf("expected an error when the node lookup fails")
		}
		if got := pushedClusterRoutes(t, s, "cust_b", "prod-2"); len(got) != 0 {
			t.Fatalf("prod-2 pushed set = %v, want empty", got)
		}
	})

	t.Run("approval failure does not advance", func(t *testing.T) {
		m := &mockEnsurer{
			nodes:      map[string]string{handlers.GatewayHostname("prod-3"): "7"},
			approveErr: errors.New("box rejected"),
		}
		r, s := newTestReconciler(t, m)
		attachHeadscale(t, s, "cust_c")
		registerCluster(t, s, "cust_c", "prod-3")
		if _, err := s.SetCustomerGatewayRoutes("cust_c", "prod-3", []string{"10.42.0.0/16"}); err != nil {
			t.Fatalf("set routes: %v", err)
		}
		if err := r.Reconcile(context.Background(), "cust_c", []string{"prod-3"}, false); err == nil {
			t.Fatalf("expected an error when approval fails")
		}
		if got := pushedClusterRoutes(t, s, "cust_c", "prod-3"); len(got) != 0 {
			t.Fatalf("prod-3 pushed set = %v, want empty", got)
		}
	})

	t.Run("policy push failure does not advance either half", func(t *testing.T) {
		m := &mockEnsurer{
			nodes: map[string]string{handlers.GatewayHostname("prod-5"): "7"},
			err:   errors.New("box refused the policy"),
		}
		r, s := newTestReconciler(t, m)
		attachHeadscale(t, s, "cust_e")
		registerCluster(t, s, "cust_e", "prod-5")
		if _, err := s.SetCustomerGatewayRoutes("cust_e", "prod-5", []string{"10.42.0.0/16"}); err != nil {
			t.Fatalf("set routes: %v", err)
		}
		if err := r.Reconcile(context.Background(), "cust_e", []string{"prod-5"}, false); err == nil {
			t.Fatalf("expected an error when the policy push fails")
		}
		// The policy IS the document a node approval is approved against, so a
		// refused policy abandons the node half rather than approving routes the
		// box has no approver rule for.
		if len(m.finds) != 0 || len(m.approvals) != 0 {
			t.Fatalf("node touched after a failed policy push: finds=%v approvals=%+v", m.finds, m.approvals)
		}
		if got := pushedRoutes(t, s, "cust_e"); len(got) != 0 {
			t.Fatalf("PushedGatewayRoutes = %v, want empty", got)
		}
		if got := pushedClusterRoutes(t, s, "cust_e", "prod-5"); len(got) != 0 {
			t.Fatalf("prod-5 pushed set = %v, want empty", got)
		}
	})

	t.Run("no routes to approve is not a failure", func(t *testing.T) {
		// A tenant whose agent has never reported. An absent desired set is
		// only ever a WITHDRAWAL when two other things hold with it — a pushed
		// entry saying central once approved routes on that gateway, and an
		// explicit removal request (a lifecycle delete, a registry-cap prune, or
		// the unheld-cluster reap). Neither is true here, and an empty report
		// cannot manufacture either: the store refuses it outright. So there is
		// no approval to place and none this reconcile is entitled to remove.
		m := &mockEnsurer{nodes: map[string]string{handlers.GatewayHostname("prod-4"): "7"}}
		r, s := newTestReconciler(t, m)
		attachHeadscale(t, s, "cust_d")
		registerCluster(t, s, "cust_d", "prod-4")
		if err := r.Reconcile(context.Background(), "cust_d", []string{"prod-4"}, true); err != nil {
			t.Fatalf("baseline reconcile: %v", err)
		}
		if len(m.calls) != 1 {
			t.Fatalf("EnsurePolicy calls = %d, want 1 (baseline policy)", len(m.calls))
		}
		if len(m.approvals) != 0 {
			t.Fatalf("approvals = %d, want 0 — an empty report is not a withdrawal", len(m.approvals))
		}
	})
}

// Exact replace, not merge: when a cluster's desired set shrinks to a smaller
// NON-EMPTY set, the approval that was dropped has to come off THAT node. A
// merge would leave a withdrawn CIDR routable indefinitely. This is sound
// because yscale-gateway-<cluster> is chart-managed and dedicated to this one
// job, so central owns its whole approved set.
func TestReconcileGatewayApprovalShrinkRemovesStale(t *testing.T) {
	m := &mockEnsurer{nodes: map[string]string{handlers.GatewayHostname("prod-1"): "7"}}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	registerCluster(t, s, "cust_a", "prod-1")
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", []string{"10.42.0.0/16", "10.96.0.0/12"}); err != nil {
		t.Fatalf("set routes: %v", err)
	}
	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, false); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !slices.Equal(m.approved["7"], []string{"10.42.0.0/16", "10.96.0.0/12"}) {
		t.Fatalf("initial approved = %v", m.approved["7"])
	}

	// The tenant drops a CIDR.
	shrunk := []string{"10.42.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", shrunk); err != nil {
		t.Fatalf("shrink routes: %v", err)
	}
	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, false); err != nil {
		t.Fatalf("reconcile after shrink: %v", err)
	}
	if len(m.approvals) != 2 {
		t.Fatalf("approvals = %d, want 2", len(m.approvals))
	}
	if !slices.Equal(m.approvals[1].routes, shrunk) {
		t.Fatalf("second approval payload = %v, want %v", m.approvals[1].routes, shrunk)
	}
	if !slices.Equal(m.approved["7"], shrunk) {
		t.Fatalf("approved set after shrink = %v, want %v (stale approval not withdrawn)", m.approved["7"], shrunk)
	}
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), shrunk) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v", pushedRoutes(t, s, "cust_a"), shrunk)
	}
	if !slices.Equal(pushedClusterRoutes(t, s, "cust_a", "prod-1"), shrunk) {
		t.Fatalf("prod-1 pushed set = %v, want %v", pushedClusterRoutes(t, s, "cust_a", "prod-1"), shrunk)
	}
}

// A shrink on one cluster is not a shrink on the tenant. B's gateway keeps its
// own exact set, the withdrawn CIDR comes off A's node ONLY, and the policy
// union follows A's new set unioned with B's unchanged one. Sharing one desired
// set between the two — the pre-existing shape — withdrew B's routes from A's
// node and vice versa on every report.
func TestReconcileShrinkOnOneClusterLeavesTheOtherExact(t *testing.T) {
	m := &mockEnsurer{nodes: map[string]string{
		handlers.GatewayHostname("prod-1"): "7",
		handlers.GatewayHostname("prod-2"): "8",
	}}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	registerCluster(t, s, "cust_a", "prod-1")
	registerCluster(t, s, "cust_a", "prod-2")

	aRoutes := []string{"10.42.0.0/16", "10.96.0.0/12"}
	bRoutes := []string{"10.43.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", aRoutes); err != nil {
		t.Fatalf("set prod-1 routes: %v", err)
	}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-2", bRoutes); err != nil {
		t.Fatalf("set prod-2 routes: %v", err)
	}
	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1", "prod-2"}, false); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// A shrinks; B is untouched and never reports again.
	aShrunk := []string{"10.42.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", aShrunk); err != nil {
		t.Fatalf("shrink prod-1: %v", err)
	}
	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, false); err != nil {
		t.Fatalf("reconcile after shrink: %v", err)
	}

	if !slices.Equal(m.approved["7"], aShrunk) {
		t.Fatalf("prod-1 gateway approved = %v, want %v (the dropped CIDR must be withdrawn)", m.approved["7"], aShrunk)
	}
	if !slices.Equal(m.approved["8"], bRoutes) {
		t.Fatalf("prod-2 gateway approved = %v, want %v (a shrink on A must not touch B)", m.approved["8"], bRoutes)
	}
	// One approval per cluster from the first pass, plus A's shrink. B's node
	// was never reopened.
	if len(m.approvals) != 3 {
		t.Fatalf("approvals = %d, want 3 (A, B, then A again)", len(m.approvals))
	}
	if m.approvals[2].nodeID != "7" {
		t.Fatalf("the shrink reached node %q, want only prod-1's node 7", m.approvals[2].nodeID)
	}
	wantUnion := []string{"10.42.0.0/16", "10.43.0.0/16"}
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), wantUnion) {
		t.Fatalf("PushedGatewayRoutes = %v, want the recomputed union %v", pushedRoutes(t, s, "cust_a"), wantUnion)
	}
	if !slices.Equal(pushedClusterRoutes(t, s, "cust_a", "prod-2"), bRoutes) {
		t.Fatalf("prod-2 pushed set = %v, want %v (unchanged)", pushedClusterRoutes(t, s, "cust_a", "prod-2"), bRoutes)
	}
}

// The node's ADVERTISED set is not an input. A gateway that offers a default
// route or a home LAN prefix gets neither approved — only what the tenant's
// validated report actually asked for.
func TestReconcileGatewayApprovalNeverWidensToAdvertisedRoutes(t *testing.T) {
	// What the gateway advertises on the box, over and above the report.
	advertisedOnly := []string{"0.0.0.0/0", "192.168.7.0/24"}

	m := &mockEnsurer{nodes: map[string]string{handlers.GatewayHostname("prod-1"): "7"}}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	registerCluster(t, s, "cust_a", "prod-1")
	reported := []string{"10.42.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", reported); err != nil {
		t.Fatalf("set routes: %v", err)
	}
	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1"}, false); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(m.approvals) != 1 {
		t.Fatalf("approvals = %d, want 1", len(m.approvals))
	}
	if !slices.Equal(m.approvals[0].routes, reported) {
		t.Fatalf("approved routes = %v, want %v exactly", m.approvals[0].routes, reported)
	}
	for _, extra := range advertisedOnly {
		if slices.Contains(m.approvals[0].routes, extra) {
			t.Errorf("approved %q, which the node advertises but the tenant never reported", extra)
		}
	}
}

// A tenant with several clusters converges the gateway of whichever cluster the
// event came from — and only that one, with only that cluster's CIDRs. Which
// cluster reported is a fact the handler holds; deriving it from the tenant's
// cluster list, or sharing one desired set between the gateways, approves one
// cluster's CIDRs on another cluster's node.
func TestReconcileMultiClusterTenantApprovesOnlyTheReportingCluster(t *testing.T) {
	aRoutes := []string{"10.42.0.0/16"}
	bRoutes := []string{"10.43.0.0/16"}
	wantUnion := []string{"10.42.0.0/16", "10.43.0.0/16"}

	newFixture := func(t *testing.T) (*mockEnsurer, *state.Store, func(clusters ...string) error) {
		t.Helper()
		m := &mockEnsurer{nodes: map[string]string{
			handlers.GatewayHostname("prod-1"): "7",
			handlers.GatewayHostname("prod-2"): "8",
		}}
		r, s := newTestReconciler(t, m)
		attachHeadscale(t, s, "cust_a")
		registerCluster(t, s, "cust_a", "prod-1")
		registerCluster(t, s, "cust_a", "prod-2")
		if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", aRoutes); err != nil {
			t.Fatalf("set prod-1 routes: %v", err)
		}
		if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-2", bRoutes); err != nil {
			t.Fatalf("set prod-2 routes: %v", err)
		}
		return m, s, func(clusters ...string) error {
			return r.Reconcile(context.Background(), "cust_a", clusters, true)
		}
	}

	t.Run("cluster A only", func(t *testing.T) {
		m, s, reconcile := newFixture(t)
		if err := reconcile("prod-1"); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(m.approvals) != 1 || m.approvals[0].nodeID != "7" {
			t.Fatalf("approvals = %+v, want exactly node 7 (prod-1's gateway)", m.approvals)
		}
		if !slices.Equal(m.approvals[0].routes, aRoutes) {
			t.Fatalf("prod-1 approved = %v, want only A's own CIDRs %v", m.approvals[0].routes, aRoutes)
		}
		if slices.Contains(m.finds, handlers.GatewayHostname("prod-2")) {
			t.Errorf("looked up prod-2's gateway on a prod-1 event: %v", m.finds)
		}
		if got := pushedClusterRoutes(t, s, "cust_a", "prod-2"); len(got) != 0 {
			t.Fatalf("prod-2 pushed set = %v, want empty (its event never ran)", got)
		}
		if !slices.Equal(pushedRoutes(t, s, "cust_a"), wantUnion) {
			t.Fatalf("PushedGatewayRoutes = %v, want the union %v", pushedRoutes(t, s, "cust_a"), wantUnion)
		}
	})

	t.Run("cluster B only", func(t *testing.T) {
		m, _, reconcile := newFixture(t)
		if err := reconcile("prod-2"); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(m.approvals) != 1 || m.approvals[0].nodeID != "8" {
			t.Fatalf("approvals = %+v, want exactly node 8 (prod-2's gateway)", m.approvals)
		}
		if !slices.Equal(m.approvals[0].routes, bRoutes) {
			t.Fatalf("prod-2 approved = %v, want only B's own CIDRs %v", m.approvals[0].routes, bRoutes)
		}
		if slices.Contains(m.finds, handlers.GatewayHostname("prod-1")) {
			t.Errorf("looked up prod-1's gateway on a prod-2 event: %v", m.finds)
		}
	})

	// Coalesced events (both clusters inside one debounce window) must converge
	// BOTH, each to its OWN set, and the policy to the union of the two. Losing
	// either identity leaves that gateway pending; sharing the sets crosses the
	// tenant's clusters.
	t.Run("coalesced A and B", func(t *testing.T) {
		m, s, reconcile := newFixture(t)
		if err := reconcile("prod-1", "prod-2"); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(m.approvals) != 2 {
			t.Fatalf("approvals = %d, want 2 (one per coalesced cluster)", len(m.approvals))
		}
		if !slices.Equal(m.approved["7"], aRoutes) {
			t.Fatalf("prod-1 gateway (node 7) approved = %v, want only %v", m.approved["7"], aRoutes)
		}
		if !slices.Equal(m.approved["8"], bRoutes) {
			t.Fatalf("prod-2 gateway (node 8) approved = %v, want only %v", m.approved["8"], bRoutes)
		}
		if !slices.Equal(pushedRoutes(t, s, "cust_a"), wantUnion) {
			t.Fatalf("PushedGatewayRoutes = %v, want the canonical union %v", pushedRoutes(t, s, "cust_a"), wantUnion)
		}
		if !slices.Equal(pushedClusterRoutes(t, s, "cust_a", "prod-1"), aRoutes) ||
			!slices.Equal(pushedClusterRoutes(t, s, "cust_a", "prod-2"), bRoutes) {
			t.Fatalf("per-cluster pushed sets = prod-1 %v / prod-2 %v, want %v / %v",
				pushedClusterRoutes(t, s, "cust_a", "prod-1"), pushedClusterRoutes(t, s, "cust_a", "prod-2"),
				aRoutes, bRoutes)
		}
	})
}

// A stale historical registration — a renamed cluster, or one that was torn
// down and never cleaned out of the registry — must not block or divert the
// cluster that is actually connected. Its gateway does not exist on the box, so
// any reconcile that consulted the registry would either approve the wrong node
// or wedge on the missing one.
func TestReconcileStaleRegistrationDoesNotBlockActiveCluster(t *testing.T) {
	m := &mockEnsurer{nodes: map[string]string{handlers.GatewayHostname("prod-new"): "7"}}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	registerCluster(t, s, "cust_a", "prod-old") // still on the books, no node on the box
	registerCluster(t, s, "cust_a", "prod-new")
	want := []string{"10.42.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-new", want); err != nil {
		t.Fatalf("set routes: %v", err)
	}

	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-new"}, true); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(m.approvals) != 1 || m.approvals[0].nodeID != "7" || !slices.Equal(m.approvals[0].routes, want) {
		t.Fatalf("approvals = %+v, want one call approving %v on node 7", m.approvals, want)
	}
	if slices.Contains(m.finds, handlers.GatewayHostname("prod-old")) {
		t.Errorf("resolved the stale registration's gateway: %v", m.finds)
	}
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), want) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v (the active cluster converged)", pushedRoutes(t, s, "cust_a"), want)
	}
}

// One wedged gateway must not stop a healthy sibling from converging: A's own
// pushed state advances, B's does not, and the policy — which really did land —
// advances too. The request as a whole still fails so B is retried, and nothing
// about A is re-attempted.
func TestReconcileOneMissingGatewayStillApprovesTheOther(t *testing.T) {
	m := &mockEnsurer{nodes: map[string]string{handlers.GatewayHostname("prod-2"): "8"}}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	registerCluster(t, s, "cust_a", "prod-1")
	registerCluster(t, s, "cust_a", "prod-2")
	aRoutes := []string{"10.42.0.0/16"}
	bRoutes := []string{"10.43.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", aRoutes); err != nil {
		t.Fatalf("set prod-1 routes: %v", err)
	}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-2", bRoutes); err != nil {
		t.Fatalf("set prod-2 routes: %v", err)
	}

	if err := r.Reconcile(context.Background(), "cust_a", []string{"prod-1", "prod-2"}, true); err == nil {
		t.Fatalf("expected a retryable error while prod-1's gateway is missing")
	}
	if !slices.Equal(m.approved["8"], bRoutes) {
		t.Fatalf("prod-2 gateway approved = %v, want %v (a wedged sibling must not block it)", m.approved["8"], bRoutes)
	}
	if !slices.Equal(pushedClusterRoutes(t, s, "cust_a", "prod-2"), bRoutes) {
		t.Fatalf("prod-2 pushed set = %v, want %v", pushedClusterRoutes(t, s, "cust_a", "prod-2"), bRoutes)
	}
	if got := pushedClusterRoutes(t, s, "cust_a", "prod-1"); len(got) != 0 {
		t.Fatalf("prod-1 pushed set = %v, want empty (its node never converged)", got)
	}
	// The policy document landed, so its own convergence state advances — the
	// retry of prod-1's node must not drag an identical policy PUT with it.
	wantUnion := []string{"10.42.0.0/16", "10.43.0.0/16"}
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), wantUnion) {
		t.Fatalf("PushedGatewayRoutes = %v, want the union %v (the policy did land)", pushedRoutes(t, s, "cust_a"), wantUnion)
	}
}

// A caller that names no cluster at all names no gateway hostname to build.
// That is policy-only work, not a licence to pick some other cluster's node —
// the tenant here HAS a registered cluster with a live gateway, and it must stay
// untouched.
func TestReconcileWithoutClusterIdentityIsPolicyOnly(t *testing.T) {
	m := &mockEnsurer{nodes: map[string]string{handlers.GatewayHostname("prod-1"): "7"}}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	registerCluster(t, s, "cust_a", "prod-1")
	want := []string{"10.42.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "prod-1", want); err != nil {
		t.Fatalf("set routes: %v", err)
	}

	if err := r.Reconcile(context.Background(), "cust_a", nil, false); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("EnsurePolicy calls = %d, want 1", len(m.calls))
	}
	if len(m.finds) != 0 || len(m.approvals) != 0 {
		t.Fatalf("named no cluster yet touched a node: finds=%v approvals=%+v", m.finds, m.approvals)
	}
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), want) {
		t.Fatalf("PushedGatewayRoutes = %v, want %v", pushedRoutes(t, s, "cust_a"), want)
	}
}

// A live socket that has not been claimed into the registry still names the
// tenant's cluster — the legacy connector path — so its gateway is approved
// like any other. The reconcile takes the id from the caller, so no registry
// row is needed at all.
func TestReconcileApprovesUnregisteredButNamedCluster(t *testing.T) {
	m := &mockEnsurer{nodes: map[string]string{handlers.GatewayHostname("legacy_1"): "7"}}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	// Connected but never claimed: the socket is the only evidence this cluster
	// exists, and the reconcile needs no more than the id the handler passes.
	connectAgent(t, s, "cust_a", "legacy_1")
	want := []string{"10.42.0.0/16"}
	if _, err := s.SetCustomerGatewayRoutes("cust_a", "legacy_1", want); err != nil {
		t.Fatalf("set routes: %v", err)
	}

	if err := r.Reconcile(context.Background(), "cust_a", []string{"legacy_1"}, false); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(m.approvals) != 1 || m.approvals[0].nodeID != "7" || !slices.Equal(m.approvals[0].routes, want) {
		t.Fatalf("approvals = %+v, want one call approving %v on node 7", m.approvals, want)
	}
}
