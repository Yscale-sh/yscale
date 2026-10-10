package state

import (
	"errors"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

var agentEpoch = time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)

// connect registers an agent whose connection time is offset minutes from a
// fixed epoch, so tests state "newer" as a number rather than by sleeping.
func connect(s *Store, id, customerID, clusterID string, offsetMinutes int) *Agent {
	a := &Agent{
		ID:          id,
		CustomerID:  customerID,
		ClusterID:   clusterID,
		ConnectedAt: agentEpoch.Add(time.Duration(offsetMinutes) * time.Minute),
		Send:        make(chan protocol.Envelope, 1),
	}
	a.MarkSeen(a.ConnectedAt)
	s.AddAgent(a)
	return a
}

func agentIDs(agents []*Agent) []string {
	out := make([]string, 0, len(agents))
	for _, a := range agents {
		out = append(out, a.ID)
	}
	return out
}

func TestAgentEmptyInventorySnapshotHasNoObservedState(t *testing.T) {
	agent := &Agent{ID: "agent_empty"}
	agent.ApplyClusterInventory(ClusterInventoryUpdate{NodeValid: true, PodValid: true})
	if got, ok := agent.ClusterInventory(); ok {
		t.Fatalf("empty snapshot remained observable: %+v", got)
	}
}

func TestAgentHeartbeatWarningIsRateLimited(t *testing.T) {
	agent := &Agent{ID: "agent_warn"}
	now := time.Now().UTC()
	if !agent.AllowHeartbeatWarning(now, time.Minute) {
		t.Fatal("first warning was suppressed")
	}
	if agent.AllowHeartbeatWarning(now.Add(30*time.Second), time.Minute) {
		t.Fatal("warning inside the interval was allowed")
	}
	if !agent.AllowHeartbeatWarning(now.Add(time.Minute), time.Minute) {
		t.Fatal("warning at the interval boundary was suppressed")
	}
}

func TestClusterInventoryFreshnessIsBounded(t *testing.T) {
	now := time.Now().UTC()
	if !clusterInventoryFresh(now, now.Add(-clusterInventoryFreshFor)) {
		t.Fatal("inventory at the freshness boundary was stale")
	}
	if clusterInventoryFresh(now, now.Add(-clusterInventoryFreshFor-time.Nanosecond)) {
		t.Fatal("inventory beyond the freshness boundary remained fresh")
	}
	if clusterInventoryFresh(now, now.Add(time.Second)) {
		t.Fatal("future inventory was treated as fresh")
	}
}

func observeAgentInventory(agent *Agent, at time.Time, nodes, bursts, pending int) {
	agent.ApplyClusterInventory(ClusterInventoryUpdate{
		NodeValid: true, NodeObserved: true, NodeObservedAt: at, NodeCount: nodes, BurstCount: bursts,
		PodValid: true, PodObserved: true, PodObservedAt: at, PendingPods: pending,
	})
}

// The listing order is the contract every routing decision is built on:
// ClusterID ascending, then newest connection, then id. Ranging the map gave a
// different answer per call, which is how one tenant's submissions landed on a
// different cluster each time.
func TestAgentsForCustomerIsStablyOrdered(t *testing.T) {
	s := New()
	// Inserted deliberately out of order, and with a pair that can only be
	// separated by the id tie-break.
	connect(s, "agent_z", "cust_a", "cl_b", 5)
	connect(s, "agent_m", "cust_a", "cl_a", 1)
	connect(s, "agent_a", "cust_a", "cl_a", 9)
	connect(s, "agent_b", "cust_a", "cl_a", 9)
	connect(s, "agent_other", "cust_b", "cl_a", 20)

	want := []string{"agent_a", "agent_b", "agent_m", "agent_z"}
	for i := 0; i < 5; i++ {
		got := agentIDs(s.AgentsForCustomer("cust_a"))
		if len(got) != len(want) {
			t.Fatalf("agents = %v, want %v", got, want)
		}
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("call %d: order = %v, want %v", i, got, want)
			}
		}
	}
	if got := s.AgentsForCustomer("cust_nobody"); len(got) != 0 {
		t.Fatalf("unknown customer = %v, want empty", got)
	}
}

// One cluster is one answer however many sockets it holds, and the answer is
// the newest of them — a connector that reconnected is reading THAT socket,
// while the previous one is a queue nobody drains.
func TestAgentForCustomerPicksNewestSocketOfOneCluster(t *testing.T) {
	s := New()
	stale := connect(s, "agent_stale", "cust_a", "cl_a", 0)
	connect(s, "agent_fresh", "cust_a", "cl_a", 30)

	got, err := s.AgentForCustomer("cust_a")
	if err != nil {
		t.Fatalf("AgentForCustomer: %v", err)
	}
	if got.ID != "agent_fresh" {
		t.Fatalf("agent = %q, want agent_fresh (the newest connection)", got.ID)
	}

	// When the overlapped old socket finally exits, cleanup is by connection
	// id. It must not unregister the newer socket for the same cluster.
	s.RemoveAgent(stale.ID)
	got, err = s.AgentForCustomer("cust_a")
	if err != nil {
		t.Fatalf("AgentForCustomer after stale disconnect: %v", err)
	}
	if got.ID != "agent_fresh" {
		t.Fatalf("agent after stale disconnect = %q, want agent_fresh", got.ID)
	}
}

// Two clusters have no single answer, and picking one spends money on the wrong
// cluster. The caller is told to name it instead.
func TestAgentForCustomerRefusesTwoClusters(t *testing.T) {
	s := New()
	connect(s, "agent_a", "cust_a", "cl_a", 0)
	connect(s, "agent_b", "cust_a", "cl_b", 1)

	if _, err := s.AgentForCustomer("cust_a"); !errors.Is(err, ErrAmbiguousCluster) {
		t.Fatalf("err = %v, want ErrAmbiguousCluster", err)
	}
	if _, err := s.AgentForCustomer("cust_none"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no connector err = %v, want ErrNotFound", err)
	}
}

// AgentForCluster is the explicit half: exact cluster, newest socket, and never
// across tenants — another customer's identical cluster id is a miss, not a
// route.
func TestAgentForClusterIsExactAndTenantScoped(t *testing.T) {
	s := New()
	connect(s, "agent_a1", "cust_a", "cl_a", 0)
	connect(s, "agent_a2", "cust_a", "cl_a", 10)
	connect(s, "agent_b1", "cust_a", "cl_b", 5)
	connect(s, "agent_foreign", "cust_b", "cl_x", 0)

	got, err := s.AgentForCluster("cust_a", "cl_a")
	if err != nil || got.ID != "agent_a2" {
		t.Fatalf("cl_a -> (%v,%v), want agent_a2", got, err)
	}
	if got, err := s.AgentForCluster("cust_a", "cl_b"); err != nil || got.ID != "agent_b1" {
		t.Fatalf("cl_b -> (%v,%v), want agent_b1", got, err)
	}
	if _, err := s.AgentForCluster("cust_a", "cl_x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another tenant's cluster err = %v, want ErrNotFound", err)
	}
	if _, err := s.AgentForCluster("cust_a", "cl_gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown cluster err = %v, want ErrNotFound", err)
	}
}

// The tenant's cluster list: registry rows joined with live sockets — one row
// per cluster whatever the socket count, carrying the newest connection's
// facts, readable by a viewer, and scoped to the caller's own tenant.
func TestTenantClustersForIsMemberScopedAndDeduped(t *testing.T) {
	s, _, ids := manageStore(t, "cust_r", map[string]string{
		"alice": RoleOwner, "view": RoleViewer,
	}, "stranger")
	s.AddCustomer(&Customer{ID: "cust_other", Token: "tok_other", Plan: "pro"})

	// Claimed the way the agent stream claims: bind before the socket is
	// registered. The rows are what make the clusters members of the fleet;
	// the sockets only decorate them.
	for _, claim := range []struct{ customer, cluster string }{
		{"cust_r", "cl_a"}, {"cust_r", "cl_b"}, {"cust_other", "cl_z"},
	} {
		if _, _, err := s.ClaimAgentCluster(claim.customer, claim.cluster, false, agentEpoch); err != nil {
			t.Fatalf("claim %s/%s: %v", claim.customer, claim.cluster, err)
		}
	}
	old := connect(s, "agent_old", "cust_r", "cl_a", 0)
	observeAgentInventory(old, time.Now().UTC(), 99, 9, 9)
	fresh := connect(s, "agent_new", "cust_r", "cl_a", 30)
	fresh.AgentVersion = "yscale-agent/v2"
	freshObservedAt := time.Now().UTC()
	observeAgentInventory(fresh, freshObservedAt, 3, 1, 0)
	connect(s, "agent_b", "cust_r", "cl_b", 5)
	foreign := connect(s, "agent_elsewhere", "cust_other", "cl_z", 0)
	observeAgentInventory(foreign, time.Now().UTC(), 42, 7, 6)

	// A viewer reads it: knowing whether your connector is up is not a
	// management action.
	rows, err := s.TenantClustersFor("cust_r", ids["view"])
	if err != nil {
		t.Fatalf("viewer reading clusters: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want one per cluster", rows)
	}
	if rows[0].ClusterID != "cl_a" || rows[1].ClusterID != "cl_b" {
		t.Fatalf("rows are not ordered by cluster id: %+v", rows)
	}
	if rows[0].Connections != 2 {
		t.Fatalf("cl_a connections = %d, want 2 (a reconnect is one cluster)", rows[0].Connections)
	}
	if !rows[0].ConnectedAt.Equal(agentEpoch.Add(30 * time.Minute)) {
		t.Fatalf("cl_a connected_at = %v, want the newest socket's", rows[0].ConnectedAt)
	}
	if rows[0].AgentVersion != "yscale-agent/v2" {
		t.Fatalf("cl_a agent_version = %q, want the newest socket's", rows[0].AgentVersion)
	}
	if !rows[0].InventoryObserved || !rows[0].NodeInventoryObserved || !rows[0].PodInventoryObserved ||
		rows[0].NodeCount != 3 || rows[0].BurstCount != 1 || rows[0].PendingPods != 0 {
		t.Fatalf("cl_a inventory = observed:%v node:%v pod:%v nodes:%d bursts:%d pending:%d, want newest socket's 3/1/0",
			rows[0].InventoryObserved, rows[0].NodeInventoryObserved, rows[0].PodInventoryObserved,
			rows[0].NodeCount, rows[0].BurstCount, rows[0].PendingPods)
	}
	if !rows[0].NodeInventoryObservedAt.Equal(freshObservedAt) ||
		!rows[0].PodInventoryObservedAt.Equal(freshObservedAt) {
		t.Fatalf("cl_a inventory observed_at = node:%v pod:%v, want newest socket timestamp",
			rows[0].NodeInventoryObservedAt, rows[0].PodInventoryObservedAt)
	}
	if rows[1].Connections != 1 {
		t.Fatalf("cl_b connections = %d, want 1", rows[1].Connections)
	}
	if rows[1].InventoryObserved || rows[1].NodeInventoryObserved || rows[1].PodInventoryObserved {
		t.Fatalf("cl_b inventory observed flags = combined:%v node:%v pod:%v, want false for a socket that never reported inventory",
			rows[1].InventoryObserved, rows[1].NodeInventoryObserved, rows[1].PodInventoryObserved)
	}
	for _, r := range rows {
		if r.ClusterID == "cl_z" {
			t.Fatalf("another tenant's cluster leaked into the list: %+v", rows)
		}
	}

	// The three ways a caller has no business with a tenant are one answer, so
	// the list cannot be used to enumerate tenant ids.
	for name, call := range map[string]struct{ tenant, account string }{
		"non-member":     {"cust_r", ids["stranger"]},
		"other tenant":   {"cust_other", ids["alice"]},
		"unknown tenant": {"cust_nope", ids["alice"]},
	} {
		if _, err := s.TenantClustersFor(call.tenant, call.account); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: err = %v, want ErrNotFound", name, err)
		}
	}
}
