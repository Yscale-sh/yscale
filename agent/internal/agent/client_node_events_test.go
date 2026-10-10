package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// nodeEventServer is a central that completes the handshake, forwards the first
// node_event frame it receives, and hangs up — which is what lets the client's
// read loop return so the test can join it.
//
// Driving the real client against a real socket is the point: the cache is only
// worth anything if the flush actually reaches the wire, through the same
// single writer goroutine every other frame uses.
func nodeEventServer(t *testing.T) (*httptest.Server, <-chan protocol.NodeEvent) {
	t.Helper()
	events := make(chan protocol.NodeEvent, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var env protocol.Envelope
			if err := conn.ReadJSON(&env); err != nil {
				return
			}
			if env.Type != protocol.TypeNodeEvent {
				continue
			}
			var ev protocol.NodeEvent
			if err := json.Unmarshal(env.Body, &ev); err == nil {
				events <- ev
			}
			return
		}
	}))
	t.Cleanup(srv.Close)
	return srv, events
}

// nodeAckServer completes the handshake, acknowledges the first terminal node
// event it sees, and forwards every frame the client sends so a test can assert
// on what came back — including what did NOT.
func nodeAckServer(t *testing.T, ack bool) (*httptest.Server, <-chan protocol.Envelope) {
	t.Helper()
	frames := make(chan protocol.Envelope, 64)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		acked := false
		for {
			var env protocol.Envelope
			if err := conn.ReadJSON(&env); err != nil {
				return
			}
			select {
			case frames <- env:
			default:
			}
			if !ack || acked || env.Type != protocol.TypeNodeEvent {
				continue
			}
			var ev protocol.NodeEvent
			if err := json.Unmarshal(env.Body, &ev); err != nil || ev.Phase != protocol.NodePhaseRemoved {
				continue
			}
			body, err := json.Marshal(protocol.NodeEventAck{BurstID: ev.BurstID, Phase: ev.Phase})
			if err != nil {
				return
			}
			if err := conn.WriteJSON(protocol.Envelope{
				APIVersion: protocol.APIVersion,
				Type:       protocol.TypeNodeEventAck,
				Timestamp:  time.Now().UTC(),
				Body:       body,
			}); err != nil {
				return
			}
			acked = true
		}
	}))
	t.Cleanup(srv.Close)
	return srv, frames
}

func newNodeEventClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	c, err := New(Config{
		Endpoint:          endpoint,
		Token:             "cluster-token",
		ClusterID:         "cluster-1",
		HeartbeatInterval: time.Minute,
	}, SilentHandler{}, discardLogger())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return c
}

// nodeEventPhase reports the phase cached for a burst, and whether one is held.
func nodeEventPhase(c *Client, burstID string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, held := c.nodeEvents[burstID]
	return entry.ev.Phase, held
}

func waitForClient(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A lifecycle transition observed while the socket is down must not vanish. The
// watcher reports each phase once, so if the frame is dropped on a dead
// connection and nothing re-sends it, central keeps showing a node that is long
// gone — and, for a Removed, never tears it down.
func TestClientFlushesCachedNodeEventsAfterReconnect(t *testing.T) {
	srv, events := nodeEventServer(t)

	c := newNodeEventClient(t, srv.URL)
	// Reported with no connection at all: enqueue has nowhere to go, so the
	// cache is the whole of what survives.
	c.ReportNodeEvent(protocol.NodeEvent{
		BurstID:  "burst_abc123",
		NodeName: "ys-burst-abc123",
		Phase:    protocol.NodePhaseReady,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.runOnce(ctx) }()

	select {
	case got := <-events:
		if got.BurstID != "burst_abc123" || got.Phase != protocol.NodePhaseReady {
			t.Fatalf("flushed event = %+v, want burst_abc123 Ready", got)
		}
	case <-ctx.Done():
		t.Fatal("a phase observed while disconnected never reached central after the handshake")
	}
	<-done
}

// Only the LATEST phase per burst is held: replaying the intermediate ones
// after a reconnect would report transitions that are already over as if they
// had just happened.
func TestClientCachesOnlyTheLatestPhasePerBurst(t *testing.T) {
	srv, events := nodeEventServer(t)

	c := newNodeEventClient(t, srv.URL)
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_abc123", Phase: protocol.NodePhaseJoining})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_abc123", Phase: protocol.NodePhaseReady})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_abc123", Phase: protocol.NodePhaseNotReady})

	c.mu.Lock()
	held := len(c.nodeEvents)
	c.mu.Unlock()
	if held != 1 {
		t.Fatalf("cache holds %d entries for one burst, want 1", held)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.runOnce(ctx) }()

	select {
	case got := <-events:
		if got.Phase != protocol.NodePhaseNotReady {
			t.Fatalf("flushed phase = %q, want the latest (NotReady)", got.Phase)
		}
	case <-ctx.Done():
		t.Fatal("nothing was flushed after the handshake")
	}
	<-done
}

// The occupancy claim is about ONE sweep and is never held. The cache is
// re-sent after every reconnect, so a cached one would let a connector that has
// since lost its cluster-wide pod list keep central's nodeOnly silence ceiling
// open by reconnecting — the exact thing the ceiling exists to catch.
func TestClientDoesNotCacheAnOccupancyObservation(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{
		BurstID: "burst_abc123", Phase: protocol.NodePhaseReady, OccupancyObserved: true,
	})

	c.mu.Lock()
	held := c.nodeEvents["burst_abc123"]
	c.mu.Unlock()
	if held.ev.Phase != protocol.NodePhaseReady {
		t.Fatalf("cached phase = %q, want the health phase to be held as usual", held.ev.Phase)
	}
	if held.ev.OccupancyObserved {
		t.Fatal("the occupancy claim was cached; a reconnect would re-assert an observation this connector may no longer be able to make")
	}
}

// The outage this exists for is a LONG one. Nothing but central's own
// acknowledgement may retire a Removed: the Node object is deleted, so the
// watcher cannot produce that report a second time, and a report lost to a
// clock is paid capacity nobody reaps.
func TestClientKeepsAnUnacknowledgedRemovalThroughALongOutage(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_gone", Phase: protocol.NodePhaseRemoved})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_live", Phase: protocol.NodePhaseReady})

	// A day disconnected — far past any window this used to hold.
	c.mu.Lock()
	for id, entry := range c.nodeEvents {
		entry.observedAt = time.Now().Add(-24 * time.Hour)
		c.nodeEvents[id] = entry
	}
	c.mu.Unlock()

	c.flushNodeEvents()
	c.retryTerminalNodeEvents()

	if _, held := nodeEventPhase(c, "burst_gone"); !held {
		t.Fatal("an unacknowledged Removed aged out; the only report that reaps that node is gone for good")
	}
	if _, held := nodeEventPhase(c, "burst_live"); !held {
		t.Fatal("a live node's phase was retired; central would stop hearing about a node that is still there")
	}
}

// A Removed outranks whatever the watcher says next about the same burst. A
// stale watch event or a recycled node name must not retire a report central
// has not resolved.
func TestClientKeepsATombstoneUnderALaterLivePhase(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_gone", Phase: protocol.NodePhaseRemoved})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_gone", Phase: protocol.NodePhaseReady})

	phase, held := nodeEventPhase(c, "burst_gone")
	if !held || phase != protocol.NodePhaseRemoved {
		t.Fatalf("cached phase = %q held:%v, want the Removed tombstone to stand", phase, held)
	}
}

// An idle request is a teardown central still owes an answer to, so it is held
// and retried on exactly the terms a Removed is: nothing but an acknowledgement
// retires it, and a reconnect or a retry tick puts it back on the wire.
func TestClientHoldsAndRetriesAnUnacknowledgedIdleRequest(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_idle", Phase: protocol.NodePhaseIdle})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_live", Phase: protocol.NodePhaseReady})

	c.mu.Lock()
	c.send = make(chan protocol.Envelope, 8)
	c.mu.Unlock()
	c.retryTerminalNodeEvents()

	c.mu.Lock()
	queued := len(c.send)
	c.mu.Unlock()
	if queued != 1 {
		t.Fatalf("retry queued %d frames, want exactly the unacknowledged idle request", queued)
	}
	env := <-c.send
	var ev protocol.NodeEvent
	if err := json.Unmarshal(env.Body, &ev); err != nil {
		t.Fatalf("decode node event: %v", err)
	}
	if ev.BurstID != "burst_idle" || ev.Phase != protocol.NodePhaseIdle {
		t.Fatalf("retried event = %+v, want the unacknowledged idle request", ev)
	}
}

// Withdrawal is EXPLICIT. An idle request is a teardown central still owes an
// answer to, and only the watcher that made it — after looking at the node's pods
// — may take it back. An ordinary health report may not, and that is the whole
// point: a Ready reaches this cache for reasons that have nothing to do with
// idleness, so a report that also revoked pending requests would let an
// unrelated one cancel a teardown nothing had reconsidered.
func TestClientWithdrawsAnIdleRequestOnlyOnAnExplicitWithdrawal(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_idle", Phase: protocol.NodePhaseIdle})

	// A health phase arriving on its own account, knowing nothing about idleness.
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_idle", Phase: protocol.NodePhaseReady})
	if phase, held := nodeEventPhase(c, "burst_idle"); !held || phase != protocol.NodePhaseIdle {
		t.Fatalf("cached phase = %q held:%v, want the idle request to survive an ordinary health report", phase, held)
	}

	// The idle watcher, having seen the node busy again.
	c.WithdrawIdleNodeEvent(protocol.NodeEvent{BurstID: "burst_idle", Phase: protocol.NodePhaseReady})
	if phase, held := nodeEventPhase(c, "burst_idle"); !held || phase != protocol.NodePhaseReady {
		t.Fatalf("cached phase = %q held:%v, want the explicit withdrawal to replace the request", phase, held)
	}
}

// A withdrawal is not a licence to overwrite the stronger teardown. A Removed
// outranks everything, including the idle watcher's own revocation — the Node
// object is gone, and only the removal's receipt proves a teardown was secured.
func TestClientWithdrawalDoesNotClearARemoval(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_gone", Phase: protocol.NodePhaseRemoved})

	c.WithdrawIdleNodeEvent(protocol.NodeEvent{BurstID: "burst_gone", Phase: protocol.NodePhaseReady})
	if phase, held := nodeEventPhase(c, "burst_gone"); !held || phase != protocol.NodePhaseRemoved {
		t.Fatalf("cached phase = %q held:%v, want the removal to stand", phase, held)
	}
}

// The two terminal reports meeting, in both orders. A Removed replaces an Idle
// because it is the stronger teardown and carries its own receipt; an Idle never
// replaces a Removed, because the node is already gone and only the removal's
// acknowledgement proves a teardown was secured.
func TestClientRanksRemovedAboveIdle(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")

	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_a", Phase: protocol.NodePhaseIdle})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_a", Phase: protocol.NodePhaseRemoved})
	if phase, held := nodeEventPhase(c, "burst_a"); !held || phase != protocol.NodePhaseRemoved {
		t.Fatalf("cached phase = %q held:%v, want Removed to supersede the idle request", phase, held)
	}

	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_b", Phase: protocol.NodePhaseRemoved})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_b", Phase: protocol.NodePhaseIdle})
	if phase, held := nodeEventPhase(c, "burst_b"); !held || phase != protocol.NodePhaseRemoved {
		t.Fatalf("cached phase = %q held:%v, want the removal to stand", phase, held)
	}
}

// An acknowledgement clears only the phase it names. The case that matters is a
// late Idle receipt arriving for a burst whose node has since been reported
// removed: only the removal's own receipt says a teardown was secured, so an
// idle receipt must not retire it.
func TestClientDoesNotLetAnIdleReceiptClearARemoval(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_a", Phase: protocol.NodePhaseIdle})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_a", Phase: protocol.NodePhaseRemoved})

	c.resolveNodeEvent(protocol.NodeEventAck{BurstID: "burst_a", Phase: protocol.NodePhaseIdle})
	if phase, held := nodeEventPhase(c, "burst_a"); !held || phase != protocol.NodePhaseRemoved {
		t.Fatalf("cached phase = %q held:%v, want the removal to survive an idle receipt", phase, held)
	}

	c.resolveNodeEvent(protocol.NodeEventAck{BurstID: "burst_a", Phase: protocol.NodePhaseRemoved})
	if _, held := nodeEventPhase(c, "burst_a"); held {
		t.Fatal("the removal's own receipt did not clear it")
	}
}

// The idle request's own acknowledgement does clear it — that receipt is what
// central sends once the teardown is durably proven, or once it has decided the
// burst is not one it will tear down on request.
func TestClientClearsAnIdleRequestOnCentralsAcknowledgement(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_idle", Phase: protocol.NodePhaseIdle})

	c.resolveNodeEvent(protocol.NodeEventAck{BurstID: "burst_idle", Phase: protocol.NodePhaseIdle})
	if _, held := nodeEventPhase(c, "burst_idle"); held {
		t.Fatal("an acknowledged idle request stayed in the cache; the connector would keep asking forever")
	}
}

// The cap is the backstop: whatever happens upstream, this map does not grow
// without bound.
func TestClientBoundsTheNodeEventCache(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	for i := 0; i < maxNodeEventCache+16; i++ {
		c.ReportNodeEvent(protocol.NodeEvent{
			BurstID: fmt.Sprintf("burst_%04x", i),
			Phase:   protocol.NodePhaseReady,
		})
	}

	c.mu.Lock()
	held := len(c.nodeEvents)
	c.mu.Unlock()
	if held > maxNodeEventCache {
		t.Fatalf("cache holds %d entries, want at most %d", held, maxNodeEventCache)
	}
}

// At the cap, a live phase is the cheap thing to lose — the node is still there
// and the next transition re-reports it. An unacknowledged Removed is not: it is
// the only record that a paid node has gone. The tombstone here is also the
// OLDEST entry, which is exactly what a plain oldest-first eviction would take.
func TestClientEvictsLivePhasesBeforeUnacknowledgedRemovals(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_gone", Phase: protocol.NodePhaseRemoved})
	for i := 0; i < maxNodeEventCache+16; i++ {
		c.ReportNodeEvent(protocol.NodeEvent{
			BurstID: fmt.Sprintf("burst_%04x", i),
			Phase:   protocol.NodePhaseReady,
		})
	}

	c.mu.Lock()
	held := len(c.nodeEvents)
	c.mu.Unlock()
	if held > maxNodeEventCache {
		t.Fatalf("cache holds %d entries, want at most %d", held, maxNodeEventCache)
	}
	if _, stillHeld := nodeEventPhase(c, "burst_gone"); !stillHeld {
		t.Fatal("the oldest entry was evicted because it was oldest; it was the one report that reaps a paid node")
	}
}

// When every entry is a tombstone there is nothing cheap left, so one goes.
// Bounded beats complete: the alternative is an unbounded map, and central has
// been refusing acknowledgements long enough by then that the reaper watchdog is
// the honest backstop.
func TestClientDropsATombstoneOnlyWhenTheCacheIsAllTombstones(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	for i := 0; i < maxNodeEventCache; i++ {
		c.ReportNodeEvent(protocol.NodeEvent{
			BurstID: fmt.Sprintf("burst_%04x", i),
			Phase:   protocol.NodePhaseRemoved,
		})
	}
	// Age one unambiguously rather than trusting the clock to separate 256 calls:
	// which tombstone goes is "the oldest", and the test should be asserting that
	// rule, not a timer's resolution.
	c.mu.Lock()
	oldest := c.nodeEvents["burst_0000"]
	oldest.observedAt = time.Now().Add(-time.Hour)
	c.nodeEvents["burst_0000"] = oldest
	c.mu.Unlock()

	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_newest", Phase: protocol.NodePhaseRemoved})

	c.mu.Lock()
	held := len(c.nodeEvents)
	c.mu.Unlock()
	if held != maxNodeEventCache {
		t.Fatalf("cache holds %d entries, want exactly %d", held, maxNodeEventCache)
	}
	if _, newestHeld := nodeEventPhase(c, "burst_newest"); !newestHeld {
		t.Fatal("the removal just observed was dropped instead of the oldest one")
	}
	if _, oldestHeld := nodeEventPhase(c, "burst_0000"); oldestHeld {
		t.Fatal("the oldest tombstone survived; nothing made room and the cache is over its cap")
	}
}

// The acknowledgement is the ONLY thing that clears a tombstone, and it clears
// exactly the one it names.
func TestClientClearsATombstoneOnCentralsAcknowledgement(t *testing.T) {
	srv, frames := nodeAckServer(t, true)

	c := newNodeEventClient(t, srv.URL)
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_gone", Phase: protocol.NodePhaseRemoved})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_live", Phase: protocol.NodePhaseReady})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.runOnce(ctx) }()

	waitForClient(t, "the acknowledged removal to leave the cache", func() bool {
		_, held := nodeEventPhase(c, "burst_gone")
		return !held
	})
	if _, held := nodeEventPhase(c, "burst_live"); !held {
		t.Fatal("an acknowledgement for one burst cleared another burst's phase")
	}

	// The receipt must not be answered on the wire. This frame is the ordering
	// barrier that makes that an assertion rather than a guess: the client has
	// one writer queue, so a CommandAck enqueued in response to the receipt would
	// be sitting ahead of it.
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_next", Phase: protocol.NodePhaseReady})
	deadline := time.After(3 * time.Second)
	for {
		select {
		case env := <-frames:
			if env.Type == protocol.TypeCommandAck {
				t.Fatal("the connector answered a node-event receipt with a CommandAck; nothing on central is waiting for one")
			}
			if env.Type != protocol.TypeNodeEvent {
				continue
			}
			var ev protocol.NodeEvent
			if err := json.Unmarshal(env.Body, &ev); err != nil {
				t.Fatalf("decode node event: %v", err)
			}
			if ev.BurstID == "burst_next" {
				cancel()
				<-done
				return
			}
		case <-deadline:
			t.Fatal("the frame sent after the receipt never arrived")
		}
	}
}

// An acknowledgement naming something this connector does not hold changes
// nothing: central acknowledges what it resolved, and anything else is a frame
// about state that is not here.
func TestClientIgnoresAnAcknowledgementItCannotMatch(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_gone", Phase: protocol.NodePhaseRemoved})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_live", Phase: protocol.NodePhaseReady})

	c.resolveNodeEvent(protocol.NodeEventAck{BurstID: "burst_other", Phase: protocol.NodePhaseRemoved})
	c.resolveNodeEvent(protocol.NodeEventAck{BurstID: "burst_live", Phase: protocol.NodePhaseRemoved})
	c.resolveNodeEvent(protocol.NodeEventAck{BurstID: "burst_gone", Phase: protocol.NodePhaseReady})
	c.resolveNodeEvent(protocol.NodeEventAck{Phase: protocol.NodePhaseRemoved})

	if _, held := nodeEventPhase(c, "burst_gone"); !held {
		t.Fatal("a tombstone was cleared by an acknowledgement that did not name its terminal phase")
	}
	if _, held := nodeEventPhase(c, "burst_live"); !held {
		t.Fatal("a live phase was cleared by a terminal acknowledgement")
	}
}

// A reconnect is not the only retry. A socket that stays up while central cannot
// reach its state backend would otherwise carry the removal once, into a
// failure, and never again.
func TestClientRetriesAnUnacknowledgedRemovalWhileConnected(t *testing.T) {
	srv, frames := nodeAckServer(t, false)

	c := newNodeEventClient(t, srv.URL)
	c.nodeEventRetry = 10 * time.Millisecond
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_gone", Phase: protocol.NodePhaseRemoved})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.runOnce(ctx) }()

	seen := 0
	deadline := time.After(3 * time.Second)
	for seen < 2 {
		select {
		case env := <-frames:
			if env.Type != protocol.TypeNodeEvent {
				continue
			}
			var ev protocol.NodeEvent
			if err := json.Unmarshal(env.Body, &ev); err != nil {
				t.Fatalf("decode node event: %v", err)
			}
			if ev.BurstID == "burst_gone" && ev.Phase == protocol.NodePhaseRemoved {
				seen++
			}
		case <-deadline:
			t.Fatalf("the unacknowledged removal was sent %d time(s) on a live connection, want it re-sent", seen)
		}
	}
	cancel()
	<-done
}

// Terminal events only. A live node's phase is re-asserted by its next
// transition and by the reconnect flush; putting it on a timer as well is frames
// central has no use for.
func TestClientRetriesOnlyTerminalNodeEvents(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_gone", Phase: protocol.NodePhaseRemoved})
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_live", Phase: protocol.NodePhaseReady})

	c.mu.Lock()
	c.send = make(chan protocol.Envelope, 8)
	c.mu.Unlock()

	c.retryTerminalNodeEvents()

	c.mu.Lock()
	queued := len(c.send)
	c.mu.Unlock()
	if queued != 1 {
		t.Fatalf("retry queued %d frames, want exactly the one terminal event", queued)
	}
	env := <-c.send
	var ev protocol.NodeEvent
	if err := json.Unmarshal(env.Body, &ev); err != nil {
		t.Fatalf("decode node event: %v", err)
	}
	if ev.BurstID != "burst_gone" || ev.Phase != protocol.NodePhaseRemoved {
		t.Fatalf("retried event = %+v, want the unacknowledged removal", ev)
	}
}

// The reconnect flush and a concurrent ReportNodeEvent must not interleave. If
// the flush reads the cache and queues the frame in two steps, a report landing
// between them puts the NEWER phase on the wire first and the flush then puts
// the stale one behind it — central ends up believing a node that has gone is
// back to Ready.
//
// The assertion is structural, not a race the test hopes to hit: the hook runs
// at the exact point a frame is queued, and a goroutine that cannot take the
// cache lock from there is every concurrent report, blocked. A build that
// queues outside the lock fails this every run.
func TestClientSerializesNodeEventEnqueueAgainstConcurrentReports(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_1", Phase: protocol.NodePhaseReady})

	queued := 0
	c.mu.Lock()
	c.send = make(chan protocol.Envelope, 8)
	c.nodeSendHook = func() {
		queued++
		free := make(chan bool, 1)
		go func() {
			if c.mu.TryLock() {
				c.mu.Unlock()
				free <- true
				return
			}
			free <- false
		}()
		if <-free {
			t.Error("a node event was queued without the cache lock; a concurrent report can put a stale phase behind a newer one")
		}
	}
	c.mu.Unlock()

	c.flushNodeEvents()
	c.ReportNodeEvent(protocol.NodeEvent{BurstID: "burst_1", Phase: protocol.NodePhaseNotReady})

	if queued != 2 {
		t.Fatalf("queued %d node event frames, want 2 (one flushed, one reported)", queued)
	}
	phases := make([]string, 0, 2)
	for len(c.send) > 0 {
		env := <-c.send
		var ev protocol.NodeEvent
		if err := json.Unmarshal(env.Body, &ev); err != nil {
			t.Fatalf("decode node event: %v", err)
		}
		phases = append(phases, ev.Phase)
	}
	want := []string{protocol.NodePhaseReady, protocol.NodePhaseNotReady}
	if !slices.Equal(phases, want) {
		t.Fatalf("frames = %v, want %v — the newest phase must be the last one central sees", phases, want)
	}
}

// An event with no burst id names nothing central can act on, and caching it
// would occupy a slot under the cap for good.
func TestClientDropsNodeEventsWithoutABurstID(t *testing.T) {
	c := newNodeEventClient(t, "https://central.invalid")
	c.ReportNodeEvent(protocol.NodeEvent{Phase: protocol.NodePhaseRemoved})

	c.mu.Lock()
	held := len(c.nodeEvents)
	c.mu.Unlock()
	if held != 0 {
		t.Fatalf("cache holds %d entries for an event with no burst id, want 0", held)
	}
}
