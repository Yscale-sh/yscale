package handlers

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// reapGrace is the lease these tests run with: long enough that a reconnect
// inside it is a deliberate act rather than a scheduling accident, short enough
// that waiting one out is not a slow test.
const reapGrace = 30 * time.Millisecond

// reapWait bounds how long a test waits for work a real lease timer set off. It
// is only ever reached when something is broken, so it is generous: the waits
// that use it are over in milliseconds.
const reapWait = 5 * time.Second

// newReapServer stands up a real AgentStream with an explicit reap grace, so a
// test can wait a lease out instead of waiting five minutes for the default. The
// grace goes through the OPTION, which is the only way to set one the startup
// leases are actually armed with. The handler comes back too: a lease is state a
// test can read, and reading it beats sleeping past one.
func newReapServer(t *testing.T, s *state.Store, rec *recordingReconciler, grace time.Duration, opts ...AgentStreamOption) (*AgentStream, *httptest.Server) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewAgentStream(s, log, rec, append([]AgentStreamOption{WithRouteReapGrace(grace)}, opts...)...)
	srv := httptest.NewServer(Auth(s, h))
	t.Cleanup(srv.Close)
	return h, srv
}

// dialReapStream runs the real handshake against a real AgentStream on the
// tenant-token path and hands back the socket, so the reap under test is driven
// by the lifecycle boundary rather than by a call the handler would never make.
func dialReapStream(t *testing.T, srv *httptest.Server, clusterID string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	hdr := http.Header{"Authorization": {"Bearer " + state.DefaultDevToken}}
	conn, _, err := websocket.DefaultDialer.Dial(url, hdr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeHello, protocol.Hello{ClusterID: clusterID})); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	return conn
}

// A pre-grammar connector is admitted LIVE-ONLY: it claims no registry row, so
// its socket is the whole of its claim. Its route intent therefore has no other
// end — no delete to run against a cluster that was never registered, and no
// reconnect obliged to report a smaller set — and the disconnect boundary is
// where it has to be retired, with the withdrawal it owes enqueued.
func TestAgentStreamDisconnectReapsALiveOnlyClustersRoutes(t *testing.T) {
	const legacyID = "Prod Cluster #1" // outside ValidClusterID on purpose
	if state.ValidClusterID(legacyID) {
		t.Fatalf("%q is inside the grammar; pick an id that is not", legacyID)
	}
	s := state.New() // seeds cust_test
	rec := &recordingReconciler{}
	_, srv := newReapServer(t, s, rec, reapGrace)

	// A properly registered cluster reports alongside it, so a reap that took
	// too much is visible rather than indistinguishable from a correct one.
	registered := dialReapStream(t, srv, "prod-1")
	if err := registered.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
		Routes: []string{"10.43.0.0/16"},
	})); err != nil {
		t.Fatalf("write registered routes: %v", err)
	}
	defer registered.Close()

	legacy := dialReapStream(t, srv, legacyID)
	if err := legacy.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
		Routes: []string{"10.42.0.0/16"},
	})); err != nil {
		t.Fatalf("write legacy routes: %v", err)
	}
	waitFor(t, "both clusters' route reports to land", func() bool {
		intent, err := s.CustomerGatewayRouteIntent("cust_test")
		return err == nil && len(intent.ByCluster) == 2
	})
	// The node half of what central approved for it — the tombstone the
	// withdrawal is driven from.
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", legacyID, []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed the legacy cluster's pushed entry: %v", err)
	}

	legacy.Close()
	waitFor(t, "the live-only cluster's route intent to be reaped at disconnect", func() bool {
		intent, err := s.CustomerGatewayRouteIntent("cust_test")
		if err != nil {
			return false
		}
		_, still := intent.ByCluster[legacyID]
		return !still
	})

	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	// The registered sibling keeps its entry, and the tenant policy union is
	// exactly what is still held.
	if got := intent.ByCluster["prod-1"]; len(got) != 1 || got[0] != "10.43.0.0/16" {
		t.Fatalf("prod-1 desired set = %v, want it untouched by the reap", got)
	}
	if len(intent.Union) != 1 || intent.Union[0] != "10.43.0.0/16" {
		t.Fatalf("tenant union = %v, want only the held cluster's routes", intent.Union)
	}
	// The pushed entry stays as the durable withdrawal tombstone.
	if got := intent.PushedRoutesFor(legacyID); len(got) != 1 {
		t.Fatalf("pushed tombstone = %v, want it retained for the node withdrawal", got)
	}

	// And the reconciler was told, through the REMOVAL seam — the one an agent
	// event may never reach on its own, because a report with no routes is a
	// lookup that has not resolved, never a teardown.
	waitFor(t, "the reaped cluster's withdrawal to be enqueued", func() bool {
		for _, c := range rec.snapshot() {
			if c.removal && c.customerID == "cust_test" && c.clusterID == legacyID {
				return true
			}
		}
		return false
	})
	for _, c := range rec.snapshot() {
		if c.removal && c.clusterID == "prod-1" {
			t.Fatal("the reap enqueued a withdrawal for a cluster the tenant still holds")
		}
	}
}

// A registered cluster's disconnect is not a revocation: its REGISTRY ROW still
// holds the id with no socket in sight, so its route intent stays. A connector
// that drops overnight and reconnects in the morning cannot lose the tenant's
// data path in between.
//
// The ordering makes that an assertion rather than a hope: prod-1's socket is
// out of the agent index BEFORE the legacy connector's lease expires, and its
// own lease is running at the same time — so if the registry row were not
// consulted, this is the pass where prod-1 would be dropped.
func TestAgentStreamDisconnectKeepsARegisteredClustersRoutes(t *testing.T) {
	const legacyID = "Prod Cluster #1"
	s := state.New()
	rec := &recordingReconciler{}
	_, srv := newReapServer(t, s, rec, reapGrace)

	conn := dialReapStream(t, srv, "prod-1")
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
		Routes: []string{"10.42.0.0/16"},
	})); err != nil {
		t.Fatalf("write routes: %v", err)
	}
	waitFor(t, "the route report to land", func() bool {
		intent, err := s.CustomerGatewayRouteIntent("cust_test")
		return err == nil && len(intent.ByCluster["prod-1"]) == 1
	})
	conn.Close()
	waitFor(t, "prod-1's socket to leave the agent index", func() bool {
		_, err := s.AgentForCluster("cust_test", "prod-1")
		return err != nil
	})

	legacy := dialReapStream(t, srv, legacyID)
	if err := legacy.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
		Routes: []string{"10.43.0.0/16"},
	})); err != nil {
		t.Fatalf("write legacy routes: %v", err)
	}
	waitFor(t, "the legacy report to land", func() bool {
		intent, err := s.CustomerGatewayRouteIntent("cust_test")
		return err == nil && len(intent.ByCluster[legacyID]) == 1
	})
	legacy.Close()
	waitFor(t, "the live-only cluster to be reaped", func() bool {
		intent, err := s.CustomerGatewayRouteIntent("cust_test")
		if err != nil {
			return false
		}
		_, still := intent.ByCluster[legacyID]
		return !still
	})

	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if got := intent.ByCluster["prod-1"]; len(got) != 1 || got[0] != "10.42.0.0/16" {
		t.Fatalf("prod-1 desired set = %v, want a registered cluster's intent to survive a disconnect", got)
	}
	if len(intent.Union) != 1 || intent.Union[0] != "10.42.0.0/16" {
		t.Fatalf("tenant union = %v, want the disconnected-but-registered cluster's routes kept", intent.Union)
	}
	for _, c := range rec.snapshot() {
		if c.removal && c.clusterID == "prod-1" {
			t.Fatalf("a registered cluster's disconnect enqueued a withdrawal: %+v", c)
		}
	}
}

// A socket closing is not intent to give up a cluster. A live-only connector
// whose pod rolls, whose node drains, or whose network blinks disconnects and
// comes straight back — and reaping on the close alone withdrew its routes, and
// on a box its gateway's approvals, every single time. The lease is what turns
// that into a reconnect nobody notices.
func TestAgentStreamTransientDisconnectKeepsRoutesAndApprovals(t *testing.T) {
	const legacyID = "Prod Cluster #1" // live-only: no registry row to fall back on
	// A grace long enough that the reconnect below is unambiguously inside it, and
	// short enough that outliving it is not a slow test. The window this waits out
	// is measured from before the disconnect, so it covers the whole of one.
	const grace = 2 * time.Second
	s := state.New()
	rec := &recordingReconciler{}
	observe, reaped := reapObserver()
	h, srv := newReapServer(t, s, rec, grace, observe)

	legacy := dialReapStream(t, srv, legacyID)
	if err := legacy.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
		Routes: []string{"10.42.0.0/16"},
	})); err != nil {
		t.Fatalf("write legacy routes: %v", err)
	}
	waitFor(t, "the legacy report to land", func() bool {
		intent, err := s.CustomerGatewayRouteIntent("cust_test")
		return err == nil && len(intent.ByCluster[legacyID]) == 1
	})
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", legacyID, []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed the pushed entry: %v", err)
	}

	armedAt := time.Now()
	legacy.Close()
	waitFor(t, "the socket to leave the agent index", func() bool {
		_, err := s.AgentForCluster("cust_test", legacyID)
		return err != nil
	})
	// Back inside the grace, the way a restarted connector comes back.
	reconnected := dialReapStream(t, srv, legacyID)
	defer reconnected.Close()
	waitFor(t, "the reconnect to be admitted", func() bool {
		_, err := s.AgentForCluster("cust_test", legacyID)
		return err == nil
	})

	// The lease is out of the armed map, which says the CANCEL ran and nothing
	// more: an entry leaves that map when its timer fires just as surely as when
	// it is stopped, so zero there cannot tell a cancelled lease from one whose
	// reap is already under way.
	waitFor(t, "the reconnect to reach the disconnect's grace lease", func() bool {
		return armedLeases(h) == 0
	})
	// What the cancel has to mean is that the timer never RUNS, and the only way
	// to see that is to outlive the deadline it was armed with and find that its
	// reap never reported. The window is measured from before the disconnect, so
	// it covers the whole grace however long the reconnect above took.
	select {
	case id := <-reaped:
		t.Fatalf("the cancelled lease's reap ran for %s", id)
	case <-time.After(time.Until(armedAt.Add(grace)) + reapGrace):
	}
	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if got := intent.ByCluster[legacyID]; len(got) != 1 || got[0] != "10.42.0.0/16" {
		t.Fatalf("desired set = %v, want a reconnecting connector's routes kept", got)
	}
	if len(intent.Union) != 1 || intent.Union[0] != "10.42.0.0/16" {
		t.Fatalf("tenant union = %v, want the routes still in policy", intent.Union)
	}
	if got := intent.PushedRoutesFor(legacyID); len(got) != 1 {
		t.Fatalf("pushed entry = %v, want the node's approvals untouched", got)
	}
	for _, c := range rec.snapshot() {
		if c.removal {
			t.Fatalf("a transient disconnect enqueued a withdrawal: %+v", c)
		}
	}
}

// A lease that fires only decides anything by asking the store, under the same
// lock as the write. That is what makes a reconnect racing the timer safe: the
// cancel is an optimisation, the ownership re-check is the guarantee. Here the
// lease is expired before the reconnect lands, and the entry still survives
// because the cluster is held again by the time the reap looks.
func TestAgentStreamExpiredLeaseRechecksOwnershipBeforeReaping(t *testing.T) {
	const legacyID = "Prod Cluster #1"
	s := state.New()
	rec := &recordingReconciler{}
	h := NewAgentStream(s, slog.New(slog.NewTextHandler(io.Discard, nil)), rec, WithRouteReapGrace(reapGrace))
	srv := httptest.NewServer(Auth(s, h))
	t.Cleanup(srv.Close)

	conn := dialReapStream(t, srv, legacyID)
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
		Routes: []string{"10.42.0.0/16"},
	})); err != nil {
		t.Fatalf("write routes: %v", err)
	}
	waitFor(t, "the report to land", func() bool {
		intent, err := s.CustomerGatewayRouteIntent("cust_test")
		return err == nil && len(intent.ByCluster[legacyID]) == 1
	})

	// The reap called directly, with the socket still live: exactly the state a
	// lease that fired a moment after a reconnect would find.
	h.reapUnheldClusterRoutes("cust_test", legacyID)
	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if got := intent.ByCluster[legacyID]; len(got) != 1 {
		t.Fatalf("desired set = %v, want the re-check to refuse a held cluster", got)
	}
	for _, c := range rec.snapshot() {
		if c.removal {
			t.Fatalf("a held cluster's withdrawal was enqueued: %+v", c)
		}
	}

	// And once it really is unheld, the same call takes it.
	conn.Close()
	waitFor(t, "the socket to leave the agent index", func() bool {
		_, err := s.AgentForCluster("cust_test", legacyID)
		return err != nil
	})
	h.reapUnheldClusterRoutes("cust_test", legacyID)
	if intent, err := s.CustomerGatewayRouteIntent("cust_test"); err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	} else if _, still := intent.ByCluster[legacyID]; still {
		t.Fatal("an unheld live-only cluster kept its route intent")
	}
}

// A lease may only take the cluster it was armed for. Two live-only connectors
// dropping a moment apart each get their own grace, and the first expiry must
// not sweep the second one's still-running lease.
func TestAgentStreamLeaseReapsOnlyItsOwnCluster(t *testing.T) {
	s := state.New()
	rec := &recordingReconciler{}
	// This test invokes the first reap directly. Keep background timers out of
	// the assertion so scheduler load cannot expire the second cluster's lease.
	h := NewAgentStream(s, slog.New(slog.NewTextHandler(io.Discard, nil)), rec, WithRouteReapGrace(time.Minute))
	srv := httptest.NewServer(Auth(s, h))
	t.Cleanup(srv.Close)

	const first, second = "Prod Cluster #1", "Prod Cluster #2"
	clusters := []struct {
		id   string
		cidr string
	}{{first, "10.42.0.0/16"}, {second, "10.43.0.0/16"}}
	for _, cluster := range clusters {
		id, cidr := cluster.id, cluster.cidr
		conn := dialReapStream(t, srv, id)
		if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
			Routes: []string{cidr},
		})); err != nil {
			t.Fatalf("write %s routes: %v", id, err)
		}
		waitFor(t, "the report from "+id+" to land", func() bool {
			intent, err := s.CustomerGatewayRouteIntent("cust_test")
			return err == nil && len(intent.ByCluster[id]) == 1
		})
		conn.Close()
		waitFor(t, id+"'s socket to leave the agent index", func() bool {
			_, err := s.AgentForCluster("cust_test", id)
			return err != nil
		})
	}

	// Only the first cluster's lease is run.
	h.reapUnheldClusterRoutes("cust_test", first)
	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if _, still := intent.ByCluster[first]; still {
		t.Fatal("the leased cluster kept its route intent")
	}
	if got := intent.ByCluster[second]; len(got) != 1 {
		t.Fatalf("second cluster's desired set = %v, want its own grace still running", got)
	}
	for _, c := range rec.snapshot() {
		if c.removal && c.clusterID == second {
			t.Fatal("one cluster's lease enqueued another's withdrawal")
		}
	}
}
