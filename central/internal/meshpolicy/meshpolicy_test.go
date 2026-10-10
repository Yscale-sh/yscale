// yscale:proprietary

package meshpolicy_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/decider"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/meshpolicy"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/factory/client"
)

type policyCall struct {
	tagOwners      map[string][]string
	routeApprovers map[string][]string
	extraACLs      []client.PolicyACL
}

// approvalCall records one ApproveNodeRoutes invocation.
type approvalCall struct {
	nodeID string
	routes []string
}

// mockEnsurer stands in for a customer's Headscale box. Beyond recording the
// policy PUT it models the two node-level behaviors the reconcile depends on:
// hostname lookup returns "" for a gateway that has not registered (exactly
// what the box returns before the pod comes up), and approval REPLACES a node's
// approved set rather than merging into it.
type mockEnsurer struct {
	calls []policyCall
	err   error

	// nodes maps a gateway hostname to its node id. A hostname that is absent
	// is a gateway that has not registered yet.
	nodes map[string]string

	finds     []string
	approvals []approvalCall
	// approved is the box's converged state per node id.
	approved map[string][]string

	findErr    error
	approveErr error
}

func (m *mockEnsurer) EnsurePolicy(_ context.Context, tagOwners, routeApprovers map[string][]string, extraACLs []client.PolicyACL) error {
	m.calls = append(m.calls, policyCall{tagOwners: tagOwners, routeApprovers: routeApprovers, extraACLs: extraACLs})
	if m.err != nil {
		return m.err
	}
	return nil
}

func (m *mockEnsurer) FindDeviceByHostname(_ context.Context, hostname string) (string, error) {
	m.finds = append(m.finds, hostname)
	if m.findErr != nil {
		return "", m.findErr
	}
	return m.nodes[hostname], nil
}

func (m *mockEnsurer) ApproveNodeRoutes(_ context.Context, nodeID string, routes []string) error {
	m.approvals = append(m.approvals, approvalCall{nodeID: nodeID, routes: slices.Clone(routes)})
	if m.approveErr != nil {
		return m.approveErr
	}
	if m.approved == nil {
		m.approved = map[string][]string{}
	}
	m.approved[nodeID] = slices.Clone(routes)
	return nil
}

// registerCluster claims a cluster id into the tenant's durable registry, the
// way the agent stream does on connect. It is what gives the reconciler a
// gateway hostname to resolve.
func registerCluster(t *testing.T, s *state.Store, customerID, clusterID string) {
	t.Helper()
	if _, _, err := s.ClaimAgentCluster(customerID, clusterID, false, time.Now().UTC()); err != nil {
		t.Fatalf("ClaimAgentCluster(%s): %v", clusterID, err)
	}
}

func newTestReconciler(t *testing.T, m *mockEnsurer) (*meshpolicy.Reconciler, *state.Store) {
	t.Helper()
	s := state.New()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := meshpolicy.New(context.Background(), s, log,
		meshpolicy.WithEnsurerFactory(func(*state.MeshEndpoint) meshpolicy.PolicyEnsurer { return m }))
	return r, s
}

func attachHeadscale(t *testing.T, s *state.Store, id string) {
	t.Helper()
	s.AddCustomer(&state.Customer{ID: id, Token: id + "-tok"})
	if err := s.SetCustomerMesh(id, &state.MeshEndpoint{
		Provider: "headscale", LoginServer: "https://hs.example", APIKey: "k", User: "u",
	}); err != nil {
		t.Fatalf("SetCustomerMesh: %v", err)
	}
}

// reportRoutes is one connector's route report, claim included: the store will
// not take a report for a cluster the tenant does not currently hold, which is
// the same order the agent stream does it in.
func reportRoutes(t *testing.T, s *state.Store, customerID, clusterID string, routes []string) {
	t.Helper()
	registerCluster(t, s, customerID, clusterID)
	if _, err := s.SetCustomerGatewayRoutes(customerID, clusterID, routes); err != nil {
		t.Fatalf("SetCustomerGatewayRoutes %s/%s: %v", customerID, clusterID, err)
	}
}

// routeIntent reads a customer's route state through the store's immutable
// SNAPSHOT — the same call production reconciles from. CustomerByID hands back
// the LIVE record, whose route maps and slices a worker goroutine replaces
// underneath a reader, so a test that reached for it there would be reading a
// map another goroutine is writing.
func routeIntent(t *testing.T, s *state.Store, id string) state.GatewayRouteIntent {
	t.Helper()
	intent, err := s.CustomerGatewayRouteIntent(id)
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	return intent
}

// pushedRoutes is the tenant-wide POLICY convergence state: the last union a
// policy push landed. It says nothing about any one gateway node.
func pushedRoutes(t *testing.T, s *state.Store, id string) []string {
	t.Helper()
	return routeIntent(t, s, id).PushedUnion
}

// pushedClusterRoutes is ONE gateway's convergence state: the exact set last
// approved on that cluster's node. This is what must not advance when a node
// is missing, and what must never pick up a sibling cluster's CIDRs.
func pushedClusterRoutes(t *testing.T, s *state.Store, id, clusterID string) []string {
	t.Helper()
	return routeIntent(t, s, id).PushedRoutesFor(clusterID)
}

// connectAgent registers a live socket for a cluster, which is the store
// evidence the reconcile uses to decide whether retrying a missing gateway can
// still converge anything.
func connectAgent(t *testing.T, s *state.Store, customerID, clusterID string) {
	t.Helper()
	s.AddAgent(&state.Agent{
		ID:          "agent_" + customerID + "_" + clusterID,
		CustomerID:  customerID,
		ClusterID:   clusterID,
		ConnectedAt: time.Now().UTC(),
	})
}

// BuildPolicy must emit exactly one kubelet-transparency ACL: the validated
// desired route CIDRs as Src, the burst/agent tag on :10250 as the ONLY Dst.
// This is the seam that makes kubectl logs/exec transparent for stock bursts,
// so the Src/Dst are asserted exactly and broad-port/wrong-tag regressions are
// guarded.
func TestBuildPolicyKubeletACLSrcDst(t *testing.T) {
	desired := []string{"10.0.0.0/24", "10.42.0.0/16", "10.43.0.0/16"}
	_, _, acls := meshpolicy.BuildPolicy("acme", desired)
	if len(acls) != 1 {
		t.Fatalf("kubelet acls = %d, want exactly 1", len(acls))
	}
	got := acls[0]
	if got.Action != "accept" {
		t.Errorf("action = %q, want accept", got.Action)
	}
	if got.Proto != "tcp" {
		t.Errorf("proto = %q, want tcp", got.Proto)
	}
	if !slices.Equal(got.Src, desired) {
		t.Errorf("src = %v, want %v (exact validated desired route CIDRs)", got.Src, desired)
	}
	wantDst := []string{handlers.AgentTailnetTag + ":10250"}
	if !slices.Equal(got.Dst, wantDst) {
		t.Fatalf("dst = %v, want %v (burst/agent tag on kubelet port ONLY)", got.Dst, wantDst)
	}
	// Regression guards: the kubelet rule must never widen to all ports, target
	// the gateway tag, or reach the bootstrap port.
	for _, d := range got.Dst {
		if strings.HasSuffix(d, ":*") {
			t.Errorf("dst %q opens ALL ports; kubelet rule must pin :10250", d)
		}
		if strings.HasPrefix(d, handlers.GatewayTailnetTag) {
			t.Errorf("dst %q targets the gateway tag; kubelet rule must target the burst/agent tag only", d)
		}
		if strings.HasSuffix(d, ":8080") {
			t.Errorf("dst %q reaches the bootstrap port; kubelet rule must pin :10250", d)
		}
	}
}

// Empty desired routes must NOT synthesize an empty/invalid kubelet ACL.
func TestBuildPolicyKubeletACLOmittedWhenNoRoutes(t *testing.T) {
	if _, _, acls := meshpolicy.BuildPolicy("acme", nil); len(acls) != 0 {
		t.Fatalf("kubelet acls with empty routes = %d, want 0", len(acls))
	}
	if _, _, acls := meshpolicy.BuildPolicy("acme", []string{}); len(acls) != 0 {
		t.Fatalf("kubelet acls with empty slice = %d, want 0", len(acls))
	}
}

// #4: push-success gate plus retry-on-EnsurePolicy-error.
func TestReconcilePushSuccessGateAndRetry(t *testing.T) {
	m := &mockEnsurer{}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	reportRoutes(t, s, "cust_a", "prod-1", []string{"10.42.0.0/16", "10.96.0.0/12"})

	// First reconcile pushes and advances PushedGatewayRoutes.
	if err := r.Reconcile(context.Background(), "cust_a", nil, false); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("EnsurePolicy calls = %d, want 1", len(m.calls))
	}
	// Approvers: reported CIDRs under tag:yscale-gateway + unconditional burst pool.
	got := m.calls[0].routeApprovers
	wantGW := []string{handlers.GatewayTailnetTag}
	if !reflect.DeepEqual(got["10.42.0.0/16"], wantGW) {
		t.Fatalf("approvers[10.42.0.0/16] = %v, want %v", got["10.42.0.0/16"], wantGW)
	}
	if !reflect.DeepEqual(got["10.96.0.0/12"], wantGW) {
		t.Fatalf("approvers[10.96.0.0/12] = %v, want %v", got["10.96.0.0/12"], wantGW)
	}
	if !reflect.DeepEqual(got[decider.BurstPodCIDRPool], []string{decider.BurstTailnetTag}) {
		t.Fatalf("burst approver = %v, want [%s]", got[decider.BurstPodCIDRPool], decider.BurstTailnetTag)
	}
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), []string{"10.42.0.0/16", "10.96.0.0/12"}) {
		t.Fatalf("PushedGatewayRoutes = %v", pushedRoutes(t, s, "cust_a"))
	}
	// The kubelet-transparency ACL is threaded through Reconcile -> EnsurePolicy
	// with the desired CIDRs as Src and the burst/agent tag on :10250 as Dst.
	if acls := m.calls[0].extraACLs; len(acls) != 1 ||
		acls[0].Proto != "tcp" ||
		!slices.Equal(acls[0].Src, []string{"10.42.0.0/16", "10.96.0.0/12"}) ||
		!slices.Equal(acls[0].Dst, []string{handlers.AgentTailnetTag + ":10250"}) {
		t.Fatalf("kubelet extraACLs = %+v, want desired CIDRs -> %s:10250", acls, handlers.AgentTailnetTag)
	}

	// Identical second reconcile: gate skips, no new EnsurePolicy.
	if err := r.Reconcile(context.Background(), "cust_a", nil, false); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("EnsurePolicy calls after identical = %d, want 1", len(m.calls))
	}

	// Changed set: pushes again.
	reportRoutes(t, s, "cust_a", "prod-1", []string{"10.42.0.0/16"})
	if err := r.Reconcile(context.Background(), "cust_a", nil, false); err != nil {
		t.Fatalf("reconcile 3: %v", err)
	}
	if len(m.calls) != 2 {
		t.Fatalf("EnsurePolicy calls after change = %d, want 2", len(m.calls))
	}

	// Failure: a new set whose push fails must NOT advance PushedGatewayRoutes.
	reportRoutes(t, s, "cust_a", "prod-1", []string{"10.42.0.0/16", "10.10.0.0/16"})
	m.err = errors.New("boom")
	if err := r.Reconcile(context.Background(), "cust_a", nil, false); err == nil {
		t.Fatalf("expected reconcile error on EnsurePolicy failure")
	}
	if len(m.calls) != 3 {
		t.Fatalf("EnsurePolicy calls after failed push = %d, want 3", len(m.calls))
	}
	if slices.Equal(pushedRoutes(t, s, "cust_a"), []string{"10.10.0.0/16", "10.42.0.0/16"}) {
		t.Fatalf("PushedGatewayRoutes advanced on failure: %v", pushedRoutes(t, s, "cust_a"))
	}

	// Self-heal: the SAME (identical) report retries the push because
	// desired != PushedGatewayRoutes (the gate survives a failed PUT).
	m.err = nil
	if err := r.Reconcile(context.Background(), "cust_a", nil, false); err != nil {
		t.Fatalf("retry reconcile: %v", err)
	}
	if len(m.calls) != 4 {
		t.Fatalf("EnsurePolicy calls after retry = %d, want 4", len(m.calls))
	}
	if !slices.Equal(pushedRoutes(t, s, "cust_a"), []string{"10.10.0.0/16", "10.42.0.0/16"}) {
		t.Fatalf("PushedGatewayRoutes after retry = %v", pushedRoutes(t, s, "cust_a"))
	}
}

// #5: force re-assert pushes even when desired == PushedGatewayRoutes.
func TestReconcileForceReassert(t *testing.T) {
	m := &mockEnsurer{}
	r, s := newTestReconciler(t, m)
	attachHeadscale(t, s, "cust_a")
	reportRoutes(t, s, "cust_a", "prod-1", []string{"10.42.0.0/16"})
	if err := r.Reconcile(context.Background(), "cust_a", nil, false); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(m.calls))
	}
	// desired == pushed now: non-forced skips.
	if err := r.Reconcile(context.Background(), "cust_a", nil, false); err != nil {
		t.Fatalf("reconcile non-force: %v", err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("non-force calls = %d, want 1 (skipped)", len(m.calls))
	}
	// Forced re-asserts regardless of the gate.
	if err := r.Reconcile(context.Background(), "cust_a", nil, true); err != nil {
		t.Fatalf("reconcile force: %v", err)
	}
	if len(m.calls) != 2 {
		t.Fatalf("force calls = %d, want 2", len(m.calls))
	}
}

// #6: ordering — agent-before-box stores without pushing, then a later forced
// reconcile pushes; box-before-agent pushes the empty-gateway baseline first.
func TestReconcileOrdering(t *testing.T) {
	t.Run("agent-before-box", func(t *testing.T) {
		m := &mockEnsurer{}
		r, s := newTestReconciler(t, m)
		// Customer exists but no mesh attached yet.
		s.AddCustomer(&state.Customer{ID: "cust_b", Token: "b"})
		reportRoutes(t, s, "cust_b", "prod-1", []string{"10.42.0.0/16"})
		// Not on Headscale: forced reconcile is a no-op.
		if err := r.Reconcile(context.Background(), "cust_b", nil, true); err != nil {
			t.Fatalf("reconcile pre-box: %v", err)
		}
		if len(m.calls) != 0 {
			t.Fatalf("calls pre-box = %d, want 0", len(m.calls))
		}
		// Box attaches (reconnect-force) → stored set pushes.
		if err := s.SetCustomerMesh("cust_b", &state.MeshEndpoint{
			Provider: "headscale", LoginServer: "https://hs", APIKey: "k", User: "u",
		}); err != nil {
			t.Fatalf("attach mesh: %v", err)
		}
		if err := r.Reconcile(context.Background(), "cust_b", nil, true); err != nil {
			t.Fatalf("reconcile post-box: %v", err)
		}
		if len(m.calls) != 1 {
			t.Fatalf("calls post-box = %d, want 1", len(m.calls))
		}
		if _, ok := m.calls[0].routeApprovers["10.42.0.0/16"]; !ok {
			t.Fatalf("stored route not approved: %v", m.calls[0].routeApprovers)
		}
	})

	t.Run("box-before-agent", func(t *testing.T) {
		m := &mockEnsurer{}
		r, s := newTestReconciler(t, m)
		attachHeadscale(t, s, "cust_c")
		// No agent report yet: forced boot reconcile pushes the empty-gateway baseline.
		if err := r.Reconcile(context.Background(), "cust_c", nil, true); err != nil {
			t.Fatalf("baseline reconcile: %v", err)
		}
		if len(m.calls) != 1 {
			t.Fatalf("baseline calls = %d, want 1", len(m.calls))
		}
		base := m.calls[0].routeApprovers
		if len(base) != 1 {
			t.Fatalf("baseline approvers = %v, want only burst pool", base)
		}
		if _, ok := base[decider.BurstPodCIDRPool]; !ok {
			t.Fatalf("baseline missing burst pool: %v", base)
		}
		// Agent reports → reported set pushes.
		reportRoutes(t, s, "cust_c", "prod-1", []string{"10.42.0.0/16"})
		if err := r.Reconcile(context.Background(), "cust_c", nil, false); err != nil {
			t.Fatalf("post-report reconcile: %v", err)
		}
		if len(m.calls) != 2 {
			t.Fatalf("post-report calls = %d, want 2", len(m.calls))
		}
		if _, ok := m.calls[1].routeApprovers["10.42.0.0/16"]; !ok {
			t.Fatalf("reported route not approved: %v", m.calls[1].routeApprovers)
		}
	})
}

// #8: a Tailscale-SaaS customer (Mesh==nil) with NO shared tailnet client
// configured (OSS build / TS_OAUTH_* unset) stores routes but pushes nothing —
// the legacy no-op preserved for backward compatibility. When a shared client IS
// wired, the same customer instead pushes the kubelet ACL; that path is covered
// by TestReconcileSharedTailnetKubeletACL in the internal test file.
func TestReconcileNonHeadscaleNoOp(t *testing.T) {
	m := &mockEnsurer{}
	r, s := newTestReconciler(t, m)
	s.AddCustomer(&state.Customer{ID: "cust_saas", Token: "saas"})
	reportRoutes(t, s, "cust_saas", "prod-1", []string{"10.42.0.0/16"})
	if err := r.Reconcile(context.Background(), "cust_saas", nil, true); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(m.calls) != 0 {
		t.Fatalf("EnsurePolicy calls for SaaS customer = %d, want 0", len(m.calls))
	}
}
