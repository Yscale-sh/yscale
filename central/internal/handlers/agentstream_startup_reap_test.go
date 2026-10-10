package handlers

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// bootedWithLiveOnlyIntent reproduces what a restart finds in the durable
// record: one live-only cluster's route intent, its withdrawal tombstone, and no
// socket anywhere holding it — because this process has only just started.
//
// The handler it returns IS the boot: constructing it is the whole of the
// startup reap's wiring, so nothing after this arms anything. The stream that
// wrote the intent plays the process that came before.
func bootedWithLiveOnlyIntent(t *testing.T, grace time.Duration) (*state.Store, *AgentStream, *httptest.Server, *recordingReconciler) {
	t.Helper()
	const legacyID = startupLegacyID
	if state.ValidClusterID(legacyID) {
		t.Fatalf("%q is inside the grammar; pick an id that is not", legacyID)
	}
	s := state.New() // seeds cust_test
	rec := &recordingReconciler{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Written by the process that came before this one, then left with no socket
	// behind it — exactly the shape applySnapshot loads.
	prev := NewAgentStream(s, log, rec)
	prevSrv := httptest.NewServer(Auth(s, prev))
	t.Cleanup(prevSrv.Close)
	conn := dialReapStream(t, prevSrv, legacyID)
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
		Routes: []string{"10.42.0.0/16"},
	})); err != nil {
		t.Fatalf("write legacy routes: %v", err)
	}
	waitFor(t, "the persisted report to land", func() bool {
		intent, err := s.CustomerGatewayRouteIntent("cust_test")
		return err == nil && len(intent.ByCluster[legacyID]) == 1
	})
	if err := s.SetCustomerPushedClusterGatewayRoutes("cust_test", legacyID, []string{"10.42.0.0/16"}); err != nil {
		t.Fatalf("seed the withdrawal tombstone: %v", err)
	}
	conn.Close()
	waitFor(t, "the previous process's socket to be gone", func() bool {
		_, err := s.AgentForCluster("cust_test", legacyID)
		return err != nil
	})
	// That disconnect armed a lease on the process that is going away. Cancelling
	// it isolates the STARTUP lease as the only thing that can reap here.
	prev.cancelRouteReapLease("cust_test", legacyID)
	// Reset BEFORE the boot: with a short grace the startup lease may fire while
	// this function is still returning, and the withdrawal it enqueues is what
	// the tests read.
	rec.reset()

	h := NewAgentStream(s, log, rec, WithRouteReapGrace(grace))
	srv := httptest.NewServer(Auth(s, h))
	t.Cleanup(srv.Close)
	return s, h, srv, rec
}

// startupLegacyID is a pre-grammar connector id: it can hold no registry row, so
// its socket is the whole of its claim and a boot sees it held by nothing.
const startupLegacyID = "Prod Cluster #1"

// stillHasIntent reports whether the live-only cluster's desired set is still in
// the store.
func stillHasIntent(t *testing.T, s *state.Store) bool {
	t.Helper()
	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	_, ok := intent.ByCluster[startupLegacyID]
	return ok
}

// armedLeases counts the grace leases a handler currently holds. The startup
// reap arms inside the constructor, so this is what a test observes it by.
//
// It counts what is still COUNTING DOWN and nothing else. A lease drops out of
// the map as its timer fires, so zero here is not a reap that ran, and not a
// reap that was cancelled either — those two look identical from this side. Use
// withRouteReapObserver for anything about what a timer actually did.
func armedLeases(h *AgentStream) int {
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	return len(h.leases)
}

// reapObserver is a handler option plus the channel its reaps land on, so a test
// can wait for the work a REAL lease timer did — or watch a bounded window go by
// without any. Buffered: a reap must never block on a test that has stopped
// reading, and an unexpected one has to still be there to be reported.
func reapObserver() (AgentStreamOption, chan string) {
	reaped := make(chan string, 8)
	return withRouteReapObserver(func(customerID, clusterID string) {
		reaped <- customerID + "/" + clusterID
	}), reaped
}

// Constructing the stream IS the startup reap. Nothing else is called on the
// handler here — no wiring step, no exported arm — and the unheld cluster is
// still withdrawn once its grace runs out, which is the invariant a production
// entry point must not be able to ship without.
func TestNewAgentStreamArmsTheStartupReapWithNoFurtherCall(t *testing.T) {
	s, _, _, rec := bootedWithLiveOnlyIntent(t, reapGrace)

	waitFor(t, "the constructor's startup lease to expire and reap the unheld cluster", func() bool {
		return !stillHasIntent(t, s)
	})
	waitFor(t, "the reaped cluster's withdrawal to be enqueued", func() bool {
		for _, c := range rec.snapshot() {
			if c.removal && c.customerID == "cust_test" && c.clusterID == startupLegacyID {
				return true
			}
		}
		return false
	})
}

// A restart is a disconnect seen from the connector's side, and boot is the
// moment it looks worst: this process holds no sockets, so every live-only
// cluster in the fleet is momentarily held by nothing. Reaping at load — where
// this used to happen — cut a running cluster's data path on every central
// restart. The startup lease gives the reconnect the same grace a runtime
// disconnect gets, and the reconnect cancels it.
func TestStartupRouteReapGivesAReconnectItsGrace(t *testing.T) {
	// An HOUR of grace: a reconnect that has to beat a timer is a test that fails
	// on a slow machine, and the lease that must fire is armed explicitly below.
	s, h, srv, rec := bootedWithLiveOnlyIntent(t, time.Hour)

	if armed := armedLeases(h); armed != 1 {
		t.Fatalf("boot armed %d leases, want exactly the one live-only cluster", armed)
	}
	if !stillHasIntent(t, s) {
		t.Fatal("boot withdrew a live-only cluster's routes before it could reconnect")
	}

	// The connector comes back inside the grace, which is what a restart looks
	// like from the fleet's side.
	conn := dialReapStream(t, srv, startupLegacyID)
	defer conn.Close()
	waitFor(t, "the reconnecting socket to land in the agent index", func() bool {
		_, err := s.AgentForCluster("cust_test", startupLegacyID)
		return err == nil
	})

	// A lease that fires WHILE the cluster is held must still take nothing: the
	// cancellation is an optimisation, and the decision belongs to the store,
	// which re-checks ownership under the same lock as the write.
	//
	// The lease that proves it is a REAL one — armed through the same call a
	// disconnect arms one with, and left to its own timer — on a second stream
	// whose grace is short enough to wait out. Calling the reap directly would
	// assert the store's re-check while skipping the timer path that reaches it.
	// The observer is what says that reap has RETURNED; the lease map cannot,
	// because an entry is dropped as its timer fires rather than when the work
	// behind it finishes. Every assertion below therefore reads a decision that is
	// already made, not one in flight.
	observe, reaped := reapObserver()
	reaper := NewAgentStream(s, slog.New(slog.NewTextHandler(io.Discard, nil)), rec,
		WithRouteReapGrace(reapGrace), observe)
	if armed := armedLeases(reaper); armed != 0 {
		t.Fatalf("the second stream armed %d leases of its own, want the booting one still holding that cluster's grace", armed)
	}
	reaper.startRouteReapLease("cust_test", startupLegacyID)
	select {
	case <-reaped:
	case <-time.After(reapWait):
		t.Fatal("the armed lease's reap never completed")
	}

	if !stillHasIntent(t, s) {
		t.Fatal("the startup lease reaped a cluster whose connector had reconnected")
	}
	for _, c := range rec.snapshot() {
		if c.removal {
			t.Fatalf("a reconnected cluster's routes were withdrawn: %+v", c)
		}
	}
}

// Waiting is not forgetting. A cluster that never comes back is still reaped
// once its grace runs out, durably, with the withdrawal it owes enqueued —
// otherwise a live-only connector's intent would outlive every restart after it,
// which is the whole reason the reap exists.
func TestStartupRouteReapReapsWhatNeverReconnects(t *testing.T) {
	s, _, _, rec := bootedWithLiveOnlyIntent(t, reapGrace)

	waitFor(t, "the startup lease to expire and reap the unheld cluster", func() bool {
		return !stillHasIntent(t, s)
	})

	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if len(intent.Union) != 0 {
		t.Fatalf("tenant union = %v, want it recomputed without the reaped cluster", intent.Union)
	}
	// The tombstone survives the reap: it is the only record that central ever
	// approved routes on that gateway, and the attach reconcile replays it.
	if got := intent.PushedRoutesFor(startupLegacyID); len(got) != 1 {
		t.Fatalf("pushed tombstone = %v, want it retained for the withdrawal", got)
	}
	waitFor(t, "the reaped cluster's withdrawal to be enqueued", func() bool {
		for _, c := range rec.snapshot() {
			if c.removal && c.customerID == "cust_test" && c.clusterID == startupLegacyID {
				return true
			}
		}
		return false
	})
}

// A boot with nothing unheld arms nothing. Registered clusters are held by their
// registry row whether or not a socket is up, so an ordinary restart of an
// ordinary fleet costs no timers at all.
func TestStartupRouteReapArmsNothingForRegisteredClusters(t *testing.T) {
	s := state.New()
	rec := &recordingReconciler{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	prev := NewAgentStream(s, log, rec, WithRouteReapGrace(reapGrace))
	prevSrv := httptest.NewServer(Auth(s, prev))
	t.Cleanup(prevSrv.Close)

	conn := dialReapStream(t, prevSrv, "prod-1")
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{
		Routes: []string{"10.43.0.0/16"},
	})); err != nil {
		t.Fatalf("write routes: %v", err)
	}
	waitFor(t, "the report to land", func() bool {
		intent, err := s.CustomerGatewayRouteIntent("cust_test")
		return err == nil && len(intent.ByCluster["prod-1"]) == 1
	})
	conn.Close()
	waitFor(t, "the socket to leave the agent index", func() bool {
		_, err := s.AgentForCluster("cust_test", "prod-1")
		return err != nil
	})

	h := NewAgentStream(s, log, rec, WithRouteReapGrace(reapGrace))
	if armed := armedLeases(h); armed != 0 {
		t.Fatalf("boot armed %d leases, want none — a registry row holds its cluster", armed)
	}
	// Nothing armed AND nothing arm-able: the boot list the arming is driven from
	// is empty because the cluster is held by its row, which is the reason boot had
	// no work rather than a coincidence of timing. No lease exists to fire, so
	// there is nothing to wait out.
	if unheld := s.ConsumeUnheldRouteClusters(); len(unheld) != 0 {
		t.Fatalf("the boot list hands out %v, want a registered cluster held by its row", unheld)
	}
	intent, err := s.CustomerGatewayRouteIntent("cust_test")
	if err != nil {
		t.Fatalf("CustomerGatewayRouteIntent: %v", err)
	}
	if len(intent.ByCluster["prod-1"]) != 1 {
		t.Fatalf("prod-1 desired set = %v, want a registered cluster untouched by boot", intent.ByCluster["prod-1"])
	}
}

// Two agent streams wired to the SAME store are one process's view of one durable
// record, so the boot reap belongs to the store and not to whichever handler was
// constructed last. Without that, a second construction armed its own lease over
// every unheld cluster — restarting a grace the first stream had already been
// counting down, and deferring the reap by a full grace period each time.
func TestStartupRouteReapArmsOncePerStore(t *testing.T) {
	s, first, _, _ := bootedWithLiveOnlyIntent(t, time.Hour)
	if armed := armedLeases(first); armed != 1 {
		t.Fatalf("the booting stream armed %d leases, want the one live-only cluster", armed)
	}

	second := NewAgentStream(s, slog.New(slog.NewTextHandler(io.Discard, nil)), &recordingReconciler{},
		WithRouteReapGrace(time.Hour))
	if armed := armedLeases(second); armed != 0 {
		t.Fatalf("a second stream on the same store armed %d leases, want none — the first already holds that cluster's grace", armed)
	}
	if armed := armedLeases(first); armed != 1 {
		t.Fatalf("the booting stream now holds %d leases, want its original one untouched", armed)
	}
	// What the second stream was refused is the ARMING, not the work: the cluster
	// really is still unheld, and the reap the first stream's lease will run —
	// asked directly here, since that grace is an hour long — still has it to take.
	reaped, err := s.ReapUnheldClusterGatewayRoutes("cust_test", []string{startupLegacyID})
	if err != nil {
		t.Fatalf("ReapUnheldClusterGatewayRoutes: %v", err)
	}
	if !slices.Equal(reaped, []string{startupLegacyID}) {
		t.Fatalf("the reap took %v, want the live-only cluster still unheld", reaped)
	}
}
