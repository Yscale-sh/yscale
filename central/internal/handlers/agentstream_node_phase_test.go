package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

const (
	nodePhaseBurstID  = "burst_abc123"
	nodePhaseNodeName = "ys-burst-abc123"
	nodePhaseWorkload = "wl_abc123"

	// preflightCordonRV is the node version this connector's preflight cordon
	// produced. Central holds it for exactly as long as the teardown behind it is
	// unresolved, and hands it back to undo that one cordon and no other.
	preflightCordonRV = "4711"
)

// reapOutcome is one Removed event's teardown, observed after it RETURNED.
type reapOutcome struct {
	burstID string
	reaped  bool
}

func TestResolveNodePhaseTimestamp(t *testing.T) {
	past := time.Date(2026, 8, 20, 11, 0, 0, 0, time.UTC)
	future := time.Date(2026, 8, 20, 13, 0, 0, 0, time.UTC)

	t.Run("legacy omission uses receipt time and marks as not source-timestamped", func(t *testing.T) {
		at, src := resolveNodePhaseTimestamp(nil)
		if src {
			t.Fatal("nil source should not be marked as source-timestamped")
		}
		if at.IsZero() {
			t.Fatal("receipt-time fallback should not be zero")
		}
	})

	t.Run("supplied timestamp is preserved exactly and marked as source-timestamped", func(t *testing.T) {
		at, src := resolveNodePhaseTimestamp(&past)
		if !src {
			t.Fatal("supplied source should be marked as source-timestamped")
		}
		if !at.Equal(past) {
			t.Fatalf("timestamp = %v, want %v preserved exactly", at, past)
		}
	})

	t.Run("far-future timestamp is preserved exactly", func(t *testing.T) {
		at, src := resolveNodePhaseTimestamp(&future)
		if !src || !at.Equal(future) {
			t.Fatalf("timestamp = %v src=%v, want %v/true — source timestamps must never be rewritten", at, src, future)
		}
	})
}

// nodePhaseFixture is a live agent socket wired to the REAL reap path: the
// workloads handler, its ClaimBurst, and a recording reaper behind it. Faking
// the reap would make "exactly one teardown" a property of the fake rather than
// of the claim that actually enforces it.
type nodePhaseFixture struct {
	store  *state.Store
	reaper *fakeReaper
	conn   *websocket.Conn
	reaps  chan reapOutcome

	// acks carries every node_event receipt central sent back. It is the whole
	// delivery contract from the connector's side: an acknowledged removal is one
	// this connector may stop reporting, and its ABSENCE is what keeps a node
	// central could not resolve on the retry path.
	acks chan protocol.NodeEventAck

	// podAcks carries every pod_event receipt central sent back. Its ABSENCE is
	// the assertion that a rejected pod event (bad ScheduledAt, foreign burst)
	// left the connector's retry outstanding.
	podAcks chan protocol.PodEventAck

	// preflights carries every prepare_idle_teardown command central pushed, and
	// nodeTookWork makes this connector answer them the way a real one does once a
	// pod has landed: refused. Both are read from the fixture's reader goroutine,
	// so nodeTookWork is atomic.
	preflights   chan protocol.PrepareIdleTeardown
	nodeTookWork atomic.Bool

	// releases carries every release_idle_teardown command central pushed — the
	// give-back of a cordon whose teardown did not happen. Its ABSENCE is half the
	// contract: a node on its way to being destroyed must stay cordoned.
	releases chan protocol.ReleaseIdleTeardown

	// writeMu serializes this end of the socket. The fixture writes from the
	// test goroutine and acknowledges commands from its reader goroutine, and
	// gorilla permits exactly one writer — the same single-writer rule the real
	// connector follows.
	writeMu sync.Mutex
}

func newNodePhaseFixture(t *testing.T, opts ...AgentStreamOption) *nodePhaseFixture {
	t.Helper()
	return newNodePhaseFixtureWith(t, nil, opts...)
}

// newNodePhaseFixtureWith is newNodePhaseFixture with a chance to wrap the REAL
// reap path before the stream gets it. The wrapper is how a test holds a
// teardown open and reads durable state while it is in flight — which is the
// only way to assert what central has NOT written yet.
func newNodePhaseFixtureWith(t *testing.T, wrap func(BurstLifecycle) BurstLifecycle, opts ...AgentStreamOption) *nodePhaseFixture {
	t.Helper()
	store := state.New()
	reaper := &fakeReaper{}
	workloads := &Workloads{Store: store, Reaper: reaper, Log: quietLog()}
	reaps := make(chan reapOutcome, 8)

	var lifecycle BurstLifecycle = workloads
	if wrap != nil {
		lifecycle = wrap(workloads)
	}
	base := []AgentStreamOption{
		WithBurstLifecycle(lifecycle),
		withNodeReapObserver(func(burstID string, reaped bool) {
			reaps <- reapOutcome{burstID: burstID, reaped: reaped}
		}),
	}
	stream := NewAgentStream(store, quietLog(), nil, append(base, opts...)...)

	srv := httptest.NewServer(Auth(store, stream))
	t.Cleanup(srv.Close)

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	hdr := http.Header{"Authorization": {"Bearer " + state.DefaultDevToken}}
	conn, _, err := websocket.DefaultDialer.Dial(url, hdr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	f := &nodePhaseFixture{
		store:      store,
		reaper:     reaper,
		conn:       conn,
		reaps:      reaps,
		acks:       make(chan protocol.NodeEventAck, 8),
		podAcks:    make(chan protocol.PodEventAck, 8),
		preflights: make(chan protocol.PrepareIdleTeardown, 8),
		releases:   make(chan protocol.ReleaseIdleTeardown, 8),
	}
	f.write(t, mustEnvelope(t, protocol.TypeHello, protocol.Hello{ClusterID: "c1"}))
	waitFor(t, "the connector to register", func() bool {
		_, err := store.AgentForCluster(state.DevCustomerID, "c1")
		return err == nil
	})
	go f.acknowledgeCommands()
	return f
}

// acknowledgeCommands answers every command central sends, as a real connector
// does. It is what makes the inline teardown finish: reapBurst pushes a
// drain_node and waits on its CommandAck.
//
// It is also the assertion nothing else can make. That acknowledgement is read
// by central's OWN read goroutine, so a teardown running inline on that
// goroutine could never receive it — the reap would sit out the full
// drain-ack timeout with the connection wedged behind it. A test that completes
// promptly is the proof the reap runs off it.
func (f *nodePhaseFixture) acknowledgeCommands() {
	for {
		var env protocol.Envelope
		if err := f.conn.ReadJSON(&env); err != nil {
			return
		}
		if env.Type == protocol.TypeNodeEventAck {
			var ack protocol.NodeEventAck
			if err := json.Unmarshal(env.Body, &ack); err == nil {
				select {
				case f.acks <- ack:
				default:
				}
			}
			// A receipt is not answered, exactly as the real connector does not
			// answer one: central has no correlation waiting on a CommandAck here.
			continue
		}
		if env.Type == protocol.TypePodEventAck {
			var ack protocol.PodEventAck
			if err := json.Unmarshal(env.Body, &ack); err == nil {
				select {
				case f.podAcks <- ack:
				default:
				}
			}
			continue
		}
		if env.ID == "" {
			continue
		}
		result := protocol.CommandAck{CommandID: env.ID, Success: true}
		if cmd, ok := decodePreflight(env); ok {
			select {
			case f.preflights <- cmd:
			default:
			}
			if f.nodeTookWork.Load() {
				// What a real connector answers once the cordon-then-list finds a
				// pod: the node is not idle, so central must not claim it.
				result = protocol.CommandAck{
					CommandID: env.ID,
					Success:   false,
					Error:     "node ys-burst-abc123 is not idle: pod team-a/inference-0 is on it",
				}
			} else {
				// A real connector hands back the version its own cordon produced. It
				// is the only thing that lets central ask for THAT cordon to be
				// released later, so a fixture that dropped it would make the recovery
				// untestable and, worse, look unnecessary.
				res, err := json.Marshal(protocol.IdleTeardownPreflight{
					CordonResourceVersion: preflightCordonRV,
				})
				if err != nil {
					return
				}
				result.Result = res
			}
		}
		if cmd, ok := decodeRelease(env); ok {
			select {
			case f.releases <- cmd:
			default:
			}
		}
		ack, err := json.Marshal(result)
		if err != nil {
			return
		}
		f.writeMu.Lock()
		err = f.conn.WriteJSON(protocol.Envelope{
			APIVersion: protocol.APIVersion,
			Type:       protocol.TypeCommandAck,
			Timestamp:  time.Now().UTC(),
			Body:       ack,
		})
		f.writeMu.Unlock()
		if err != nil {
			return
		}
	}
}

// decodePreflight recognises the cordon-and-recheck central sends before it
// claims an idle burst. A drain_node is the teardown drain and is acknowledged
// plainly.
func decodePreflight(env protocol.Envelope) (protocol.PrepareIdleTeardown, bool) {
	if env.Type != protocol.TypePrepareIdleTeardown {
		return protocol.PrepareIdleTeardown{}, false
	}
	var cmd protocol.PrepareIdleTeardown
	if err := json.Unmarshal(env.Body, &cmd); err != nil {
		return protocol.PrepareIdleTeardown{}, false
	}
	return cmd, true
}

// decodeRelease recognises the give-back central sends when the teardown it
// cordoned for did not happen.
func decodeRelease(env protocol.Envelope) (protocol.ReleaseIdleTeardown, bool) {
	if env.Type != protocol.TypeReleaseIdleTeardown {
		return protocol.ReleaseIdleTeardown{}, false
	}
	var cmd protocol.ReleaseIdleTeardown
	if err := json.Unmarshal(env.Body, &cmd); err != nil {
		return protocol.ReleaseIdleTeardown{}, false
	}
	return cmd, true
}

// awaitPreflight blocks until central has asked this connector to confirm the
// node is still empty.
func (f *nodePhaseFixture) awaitPreflight(t *testing.T) protocol.PrepareIdleTeardown {
	t.Helper()
	select {
	case cmd := <-f.preflights:
		return cmd
	case <-time.After(3 * time.Second):
		t.Fatal("central never ran the idle teardown preflight")
		return protocol.PrepareIdleTeardown{}
	}
}

// awaitRelease blocks until central has given back the cordon its preflight
// placed.
func (f *nodePhaseFixture) awaitRelease(t *testing.T) protocol.ReleaseIdleTeardown {
	t.Helper()
	select {
	case cmd := <-f.releases:
		return cmd
	case <-time.After(3 * time.Second):
		t.Fatal("central never released the cordon its preflight placed; the node stays unschedulable")
		return protocol.ReleaseIdleTeardown{}
	}
}

func (f *nodePhaseFixture) write(t *testing.T, env protocol.Envelope) {
	t.Helper()
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	if err := f.conn.WriteJSON(env); err != nil {
		t.Fatalf("write %s: %v", env.Type, err)
	}
}

// seedBurst puts a live burst for the connected tenant, exactly as the submit
// path would: booked in the cluster this connector authenticated for, carrying
// the node name central assigned, provisioning, no phase reported yet.
func (f *nodePhaseFixture) seedBurst(t *testing.T) {
	t.Helper()
	err := f.store.PutBurst(&state.Burst{
		ID:         nodePhaseBurstID,
		CustomerID: state.DevCustomerID,
		ClusterID:  "c1",
		Backend:    "linode",
		BackendID:  "vm-1",
		NodeName:   nodePhaseNodeName,
		Status:     state.BurstStatusProvisioning,
		HourlyUSD:  0.20,
		CreatedAt:  time.Now().Add(-10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("seed burst: %v", err)
	}
}

// reportRemoved sends the removal a departed burst node produces, naming the
// node central itself assigned — the pair central minted, which is what the
// teardown is authorised against.
func (f *nodePhaseFixture) reportRemoved(t *testing.T) {
	t.Helper()
	f.report(t, protocol.NodeEvent{
		BurstID:  nodePhaseBurstID,
		NodeName: nodePhaseNodeName,
		Phase:    protocol.NodePhaseRemoved,
	})
}

// seedWorkload puts the unfinished workload that burst is backing.
func (f *nodePhaseFixture) seedWorkload() {
	f.store.PutWorkload(&state.Workload{
		ID:         nodePhaseWorkload,
		CustomerID: state.DevCustomerID,
		ClusterID:  "c1",
		Status:     "provisioning",
		BurstID:    nodePhaseBurstID,
		CreatedAt:  time.Now().Add(-10 * time.Minute),
	})
}

func (f *nodePhaseFixture) report(t *testing.T, ev protocol.NodeEvent) {
	t.Helper()
	f.write(t, mustEnvelope(t, protocol.TypeNodeEvent, ev))
}

// drain closes the socket and waits for the connector to leave the store. The
// read goroutine handles frames IN ORDER, so reaching the disconnect proves
// every frame ahead of it was fully processed — which is what makes "nothing
// happened" an assertion rather than a sleep.
func (f *nodePhaseFixture) drain(t *testing.T) {
	t.Helper()
	f.conn.Close()
	waitFor(t, "the stream to process pending frames and disconnect", func() bool {
		_, err := f.store.AgentForCluster(state.DevCustomerID, "c1")
		return errors.Is(err, state.ErrNotFound)
	})
}

func (f *nodePhaseFixture) burst(t *testing.T) *state.Burst {
	t.Helper()
	b, err := f.store.GetBurst(nodePhaseBurstID)
	if err != nil {
		t.Fatalf("burst %s is gone: %v", nodePhaseBurstID, err)
	}
	return b
}

// view is the tenant's own GET /v1/bursts projection — what a dashboard reads.
func (f *nodePhaseFixture) view(t *testing.T) BurstView {
	t.Helper()
	h := &Workloads{Store: f.store, Log: quietLog()}
	req := httptest.NewRequest(http.MethodGet, "/v1/bursts", nil).
		WithContext(context.WithValue(context.Background(), ctxCustomer, &state.Customer{ID: state.DevCustomerID}))
	rec := httptest.NewRecorder()
	h.Bursts(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/bursts = %d, want 200", rec.Code)
	}
	var body struct {
		Bursts []BurstView `json:"bursts"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode bursts: %v", err)
	}
	for _, v := range body.Bursts {
		if v.ID == nodePhaseBurstID {
			return v
		}
	}
	t.Fatalf("burst %s missing from the tenant's projection: %+v", nodePhaseBurstID, body.Bursts)
	return BurstView{}
}

func (f *nodePhaseFixture) awaitReap(t *testing.T) reapOutcome {
	t.Helper()
	select {
	case out := <-f.reaps:
		return out
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the teardown a Removed event owes")
		return reapOutcome{}
	}
}

func (f *nodePhaseFixture) awaitAck(t *testing.T) protocol.NodeEventAck {
	t.Helper()
	select {
	case ack := <-f.acks:
		return ack
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the receipt that releases the connector from a removal")
		return protocol.NodeEventAck{}
	}
}

// requireNoPreflight asserts central never asked this connector to cordon
// anything, AFTER the socket has been drained. It is the assertion that a request
// central refuses on its own record leaves the customer's cluster untouched — a
// cordon is a real change to a node somebody may be using.
func (f *nodePhaseFixture) requireNoPreflight(t *testing.T) {
	t.Helper()
	select {
	case cmd := <-f.preflights:
		t.Fatalf("central cordoned %s for a teardown it had already refused", cmd.NodeName)
	default:
	}
}

// requireNoRelease asserts central left the cordon in place, AFTER the socket has
// been drained. It is the other half of the recovery contract: a node whose
// teardown IS happening must stay unschedulable until the provider destroys it,
// or the window the preflight exists to shut reopens.
func (f *nodePhaseFixture) requireNoRelease(t *testing.T) {
	t.Helper()
	select {
	case cmd := <-f.releases:
		t.Fatalf("central uncordoned %s while its teardown was in hand; new work could land on a node about to be destroyed", cmd.NodeName)
	default:
	}
}

// requireNoAck asserts nothing was acknowledged, AFTER the socket has been
// drained. drain is the barrier: frames are handled in order, so a connection
// that has finished disconnecting has finished everything queued behind it.
func (f *nodePhaseFixture) requireNoAck(t *testing.T) {
	t.Helper()
	select {
	case ack := <-f.acks:
		t.Fatalf("central acknowledged %+v; the connector would drop a removal that was never resolved", ack)
	default:
	}
}

// A node that came up must stop reading "provisioning" — that projection is the
// only thing telling a customer their capacity is live — and coming up is not a
// reason to destroy anything.
func TestAgentStreamReadyPhaseMovesTheProjectionAndTearsNothingDown(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)

	f.report(t, protocol.NodeEvent{
		BurstID:  nodePhaseBurstID,
		NodeName: nodePhaseNodeName,
		Phase:    protocol.NodePhaseReady,
	})
	waitFor(t, "the burst to leave provisioning", func() bool {
		b, err := f.store.GetBurst(nodePhaseBurstID)
		return err == nil && b.Status != state.BurstStatusProvisioning
	})
	f.drain(t)

	view := f.view(t)
	if view.Status != state.BurstStatusRunning {
		t.Fatalf("status = %q, want %q", view.Status, state.BurstStatusRunning)
	}
	if view.NodePhase != protocol.NodePhaseReady {
		t.Fatalf("node_phase = %q, want %q", view.NodePhase, protocol.NodePhaseReady)
	}
	if view.NodeName != nodePhaseNodeName {
		t.Fatalf("node_name = %q, want %q", view.NodeName, nodePhaseNodeName)
	}
	if view.NodePhaseAt == nil || view.NodePhaseAt.IsZero() {
		t.Fatal("node_phase_at is unset; a phase with no time on it cannot be read as current")
	}
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 — a node coming up is not a reason to destroy it", n)
	}
	if len(f.reaps) != 0 {
		t.Fatal("Ready reached the teardown path")
	}
}

// The occupancy stamp central's nodeOnly ceiling measures from is moved by the
// explicit wire signal and by nothing else — not by a health report, and not by
// repeats of one.
//
// It has to be checked here and not only in the store: this is the seam where a
// connector's frame becomes a durable claim, and the whole defect was central
// reading "a connector said something about this node" as "a connector can still
// see whether this node is idle". Those are different connectors.
func TestAgentStreamStampsOccupancyOnlyOnTheExplicitSignal(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)

	f.report(t, protocol.NodeEvent{BurstID: nodePhaseBurstID, Phase: protocol.NodePhaseReady})
	f.report(t, protocol.NodeEvent{BurstID: nodePhaseBurstID, Phase: protocol.NodePhaseReady})
	waitFor(t, "the burst to leave provisioning", func() bool {
		b, err := f.store.GetBurst(nodePhaseBurstID)
		return err == nil && b.Status != state.BurstStatusProvisioning
	})
	f.drain(t)

	if at := f.burst(t).OccupancyObservedAt; at != nil {
		t.Fatalf("health reports stamped an occupancy observation (%v); a connector that cannot see pods would hold the nodeOnly ceiling open forever", at)
	}

	// The same socket, now carrying the claim.
	f2 := newNodePhaseFixture(t)
	f2.seedBurst(t)
	f2.report(t, protocol.NodeEvent{
		BurstID:           nodePhaseBurstID,
		NodeName:          nodePhaseNodeName,
		Phase:             protocol.NodePhaseReady,
		OccupancyObserved: true,
	})
	waitFor(t, "the occupancy observation to land", func() bool {
		b, err := f2.store.GetBurst(nodePhaseBurstID)
		return err == nil && b.OccupancyObservedAt != nil
	})
	f2.drain(t)

	observed := f2.burst(t).OccupancyObservedAt
	if observed == nil || observed.IsZero() {
		t.Fatal("the explicit occupancy signal stamped nothing")
	}
}

// NotReady is a node that stopped answering. Bursts recover from that often
// enough that tearing down on it would destroy live work, so it moves the
// status and nothing else.
func TestAgentStreamNotReadyIsStatusOnly(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	f.report(t, protocol.NodeEvent{BurstID: nodePhaseBurstID, Phase: protocol.NodePhaseReady})
	f.report(t, protocol.NodeEvent{
		BurstID: nodePhaseBurstID,
		Phase:   protocol.NodePhaseNotReady,
		Reason:  "KubeletNotReady",
	})
	waitFor(t, "the burst to read degraded", func() bool {
		b, err := f.store.GetBurst(nodePhaseBurstID)
		return err == nil && b.Status == state.BurstStatusDegraded
	})
	f.drain(t)

	view := f.view(t)
	if view.NodePhase != protocol.NodePhaseNotReady || view.NodePhaseReason != "KubeletNotReady" {
		t.Fatalf("projection = %+v, want NotReady/KubeletNotReady", view)
	}
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 — NotReady must never tear a burst down", n)
	}
	if len(f.reaps) != 0 {
		t.Fatal("NotReady reached the teardown path")
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil || wl.FinishedAt != nil {
		t.Fatalf("workload = %+v err=%v, want it still running", wl, err)
	}
	if wl.NodeObservation == nil {
		t.Fatal("workload has no NodeObservation after a WebSocket phase update")
	}
	if wl.NodeObservation.Phase != protocol.NodePhaseNotReady {
		t.Fatalf("workload NodeObservation.Phase = %q, want %q", wl.NodeObservation.Phase, protocol.NodePhaseNotReady)
	}
	if wl.NodeObservation.NodeName != nodePhaseNodeName {
		t.Fatalf("workload NodeObservation.NodeName = %q, want %q", wl.NodeObservation.NodeName, nodePhaseNodeName)
	}
}

// The whole point of the signal: a node that left takes its burst with it, once,
// and the workload it was running is recorded as failed rather than left reading
// "provisioning" for a job whose node is gone.
func TestAgentStreamRemovedReapsOnceAndFailsTheWorkload(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	f.reportRemoved(t)
	if out := f.awaitReap(t); !out.reaped || out.burstID != nodePhaseBurstID {
		t.Fatalf("reap outcome = %+v, want burst_abc123 reaped", out)
	}

	if _, err := f.store.GetBurst(nodePhaseBurstID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("burst survived the teardown: %v", err)
	}
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 1 {
		t.Fatalf("teardowns = %d, want exactly 1", n)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil {
		t.Fatalf("workload: %v", err)
	}
	if wl.Status != "failed" || wl.FinishedAt == nil {
		t.Fatalf("workload = %+v, want failed and finished", wl)
	}

	// The teardown is what releases the connector from the report. Until this
	// arrives the removal is still owed and the connector keeps re-sending it.
	if ack := f.awaitAck(t); ack.BurstID != nodePhaseBurstID || ack.Phase != protocol.NodePhaseRemoved {
		t.Fatalf("receipt = %+v, want burst_abc123 Removed", ack)
	}

	// A second Removed — a redelivered frame, a reconnect flush — must be a
	// no-op. The burst is gone, so nothing matches and no teardown is enqueued.
	// It is acknowledged all the same: a report central has nothing left to do
	// about is one the connector must be able to stop making.
	f.reportRemoved(t)
	if ack := f.awaitAck(t); ack.BurstID != nodePhaseBurstID {
		t.Fatalf("repeated removal receipt = %+v, want burst_abc123", ack)
	}
	f.drain(t)
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 1 {
		t.Fatalf("teardowns after a repeated Removed = %d, want still 1", n)
	}
	if len(f.reaps) != 0 {
		t.Fatal("a repeated Removed reached the teardown path a second time")
	}
}

// gatedLifecycle holds a teardown at its threshold so a test can look at what
// central has written before it. It is the real reap path behind the gate —
// faking the teardown would make the assertion a property of the fake.
type gatedLifecycle struct {
	inner   BurstLifecycle
	started chan string
	release chan struct{}
}

func (g *gatedLifecycle) ReapBurst(ctx context.Context, burstID, reason string) ReapOutcome {
	g.started <- burstID
	<-g.release
	return g.inner.ReapBurst(ctx, burstID, reason)
}

// The crash-safety contract. Central used to write the terminal phase and THEN
// start the teardown on a goroutine nothing tracked; a process that died in that
// window left a burst durably marked Removed with its node still running, and
// the store's terminal guard then refused the replayed report that would have
// retried it. So at the moment teardown begins there must be no terminal record
// to be stranded by: the burst is still exactly as claimable as it was.
func TestAgentStreamRemovedWritesNothingTerminalBeforeTheTeardown(t *testing.T) {
	gate := &gatedLifecycle{started: make(chan string, 1), release: make(chan struct{})}
	f := newNodePhaseFixtureWith(t, func(inner BurstLifecycle) BurstLifecycle {
		gate.inner = inner
		return gate
	})
	f.seedBurst(t)
	f.seedWorkload()

	f.reportRemoved(t)

	select {
	case got := <-gate.started:
		if got != nodePhaseBurstID {
			t.Fatalf("teardown started for %q, want %q", got, nodePhaseBurstID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the teardown a Removed owes never started")
	}

	// This is the crash point. Everything asserted here is what a replica taking
	// over would find.
	b := f.burst(t)
	if b.NodePhase != "" {
		t.Fatalf("node_phase = %q before the teardown was secured; a crash here strands a terminal record over a live node", b.NodePhase)
	}
	if b.Status != state.BurstStatusProvisioning {
		t.Fatalf("status = %q before the teardown was secured, want it untouched", b.Status)
	}
	f.requireNoAck(t)

	close(gate.release)
	if out := f.awaitReap(t); !out.reaped {
		t.Fatalf("reap outcome = %+v, want the teardown to complete", out)
	}
	if ack := f.awaitAck(t); ack.BurstID != nodePhaseBurstID {
		t.Fatalf("receipt = %+v, want the removal acknowledged once it was torn down", ack)
	}
	if _, err := f.store.GetBurst(nodePhaseBurstID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("burst survived the teardown: %v", err)
	}
}

// A teardown that failed re-queues the burst for a later reap. The node is still
// up and still billing, so the connector must keep its report: acknowledging one
// here would retire the very signal that retries it.
func TestAgentStreamRemovedIsNotAcknowledgedWhenTeardownFails(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()
	f.reaper.setErr(errors.New("provider delete failed"))

	f.reportRemoved(t)
	if out := f.awaitReap(t); out.reaped {
		t.Fatalf("reap outcome = %+v, want a teardown that failed", out)
	}
	f.drain(t)

	f.requireNoAck(t)
	if _, err := f.store.GetBurst(nodePhaseBurstID); err != nil {
		t.Fatalf("the burst was not re-queued after a failed teardown: %v", err)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil || wl.FinishedAt != nil {
		t.Fatalf("workload = %+v err=%v, want it untouched by a teardown that did not happen", wl, err)
	}
}

// An unreadable burst is UNKNOWN, not "not yours". Central tears nothing down on
// it — the claim is irreversible — and acknowledges nothing either, so the
// removal stays with the connector and is retried when the backend is back.
func TestAgentStreamRemovedStaysOutstandingWhenTheBurstCannotBeRead(t *testing.T) {
	reader := &failingRecordReader{}
	f := newNodePhaseFixture(t, withBurstNodeRecordReader(reader))
	f.seedBurst(t)
	f.seedWorkload()

	f.reportRemoved(t)
	f.drain(t)

	if reader.count() == 0 {
		t.Fatal("the authorisation read was never attempted")
	}
	f.requireNoAck(t)
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 when the burst is unreadable", n)
	}
	if len(f.reaps) != 0 {
		t.Fatal("a removal central could not verify reached the teardown path")
	}
	b := f.burst(t)
	if b.NodePhase != "" || b.Status != state.BurstStatusProvisioning {
		t.Fatalf("burst mutated on an unverifiable removal: %+v", b)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil || wl.FinishedAt != nil {
		t.Fatalf("workload = %+v err=%v, want it untouched", wl, err)
	}
}

// A removal is authorised against central's own RECORD, not against tenancy
// alone. A tenant may connect several clusters on one token, so a connector
// compromised inside one of them could otherwise name a burst running in another
// and have central claim and destroy it — the claim deletes by burst id.
//
// Every case here tears nothing down AND acknowledges nothing. There is no
// receipt for a burst that is still live, and leaving the report outstanding is
// what keeps a connector that names the right pair from being starved by one
// that does not.
func TestAgentStreamRemovedRefusesEventsItCannotAuthorise(t *testing.T) {
	tests := []struct {
		name  string
		seed  func(t *testing.T, f *nodePhaseFixture)
		event protocol.NodeEvent
	}{
		{
			// The defect this closes: same tenant, same token, different cluster.
			name: "a burst booked in another of this tenant's clusters",
			seed: func(t *testing.T, f *nodePhaseFixture) {
				f.seedBurst(t)
				b := f.burst(t)
				moved := *b
				moved.ClusterID = "c2"
				if err := f.store.PutBurst(&moved); err != nil {
					t.Fatalf("move burst to another cluster: %v", err)
				}
			},
			event: protocol.NodeEvent{
				BurstID: nodePhaseBurstID, NodeName: nodePhaseNodeName, Phase: protocol.NodePhaseRemoved,
			},
		},
		{
			// The connector derives the burst id FROM the node name, so requiring
			// the pair central minted closes that loop.
			name: "a node this burst does not have",
			seed: func(t *testing.T, f *nodePhaseFixture) { f.seedBurst(t) },
			event: protocol.NodeEvent{
				BurstID: nodePhaseBurstID, NodeName: "ys-burst-someoneelse", Phase: protocol.NodePhaseRemoved,
			},
		},
		{
			name: "no node name at all",
			seed: func(t *testing.T, f *nodePhaseFixture) { f.seedBurst(t) },
			event: protocol.NodeEvent{
				BurstID: nodePhaseBurstID, Phase: protocol.NodePhaseRemoved,
			},
		},
		{
			// Current connectivity cannot prove which cluster historically booked a
			// legacy burst, even if only one connector happens to be live now.
			name: "a burst with no recorded cluster",
			seed: func(t *testing.T, f *nodePhaseFixture) {
				f.seedBurst(t)
				b := f.burst(t)
				legacy := *b
				legacy.ClusterID = ""
				if err := f.store.PutBurst(&legacy); err != nil {
					t.Fatalf("clear the burst's cluster: %v", err)
				}
			},
			event: protocol.NodeEvent{
				BurstID: nodePhaseBurstID, NodeName: nodePhaseNodeName, Phase: protocol.NodePhaseRemoved,
			},
		},
		{
			name: "a burst with no recorded node name",
			seed: func(t *testing.T, f *nodePhaseFixture) {
				f.seedBurst(t)
				b := f.burst(t)
				anon := *b
				anon.NodeName = ""
				if err := f.store.PutBurst(&anon); err != nil {
					t.Fatalf("clear the burst's node name: %v", err)
				}
			},
			event: protocol.NodeEvent{
				BurstID: nodePhaseBurstID, NodeName: nodePhaseNodeName, Phase: protocol.NodePhaseRemoved,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newNodePhaseFixture(t)
			tc.seed(t, f)
			f.seedWorkload()

			f.report(t, tc.event)
			// drain is the barrier: frames are handled in order, so a connection
			// that has finished disconnecting has finished everything queued
			// behind it.
			f.drain(t)
			f.requireNoAck(t)

			if n := f.reaper.teardownCount(tc.event.BurstID); n != 0 {
				t.Fatalf("teardowns = %d, want 0", n)
			}
			if len(f.reaps) != 0 {
				t.Fatal("an unauthorised removal reached the teardown path")
			}
			b := f.burst(t)
			if b.NodePhase != "" || b.Status != state.BurstStatusProvisioning {
				t.Fatalf("burst mutated by a refused removal: %+v", b)
			}
			wl, err := f.store.GetWorkload(nodePhaseWorkload)
			if err != nil || wl.FinishedAt != nil {
				t.Fatalf("workload = %+v err=%v, want it untouched", wl, err)
			}
		})
	}
}

// The other side of the same gate: the exact pair central minted, from the
// cluster it booked the capacity in, is torn down. Without this the refusals
// above would be satisfied by a handler that never reaps anything.
func TestAgentStreamRemovedReapsOnAnExactClusterAndNodeMatch(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	f.reportRemoved(t)
	if out := f.awaitReap(t); !out.reaped || out.burstID != nodePhaseBurstID {
		t.Fatalf("reap outcome = %+v, want the matching pair reaped", out)
	}
	if ack := f.awaitAck(t); ack.BurstID != nodePhaseBurstID || ack.Phase != protocol.NodePhaseRemoved {
		t.Fatalf("receipt = %+v, want burst_abc123 Removed", ack)
	}
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 1 {
		t.Fatalf("teardowns = %d, want exactly 1", n)
	}
}

// A connector must not learn from the receipt whether an id it named belongs to
// somebody else or to nobody. Neither has a tenant-scoped teardown receipt, so
// both remain unacknowledged. Besides preserving that boundary, this is what
// keeps an absent row from being mistaken for a safely reaped burst after an
// unknown-outcome ClaimBurst error.
func TestAgentStreamRemovedLeavesForeignAndUnknownIdsUnacknowledged(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)

	f.store.AddCustomer(&state.Customer{ID: "cust_other", Token: "tok_other", Plan: "pro"})
	if err := f.store.PutBurst(&state.Burst{
		ID:         "burst_other",
		CustomerID: "cust_other",
		Backend:    "linode",
		BackendID:  "vm-other",
		Status:     state.BurstStatusProvisioning,
		CreatedAt:  time.Now(),
	}); err != nil {
		t.Fatalf("seed foreign burst: %v", err)
	}

	f.report(t, protocol.NodeEvent{BurstID: "burst_other", Phase: protocol.NodePhaseRemoved})
	f.requireNoAck(t)
	f.report(t, protocol.NodeEvent{BurstID: "burst_nosuch", Phase: protocol.NodePhaseRemoved})
	f.requireNoAck(t)
	f.drain(t)
	other, err := f.store.GetBurst("burst_other")
	if err != nil {
		t.Fatalf("the other tenant's burst was torn down by this connector: %v", err)
	}
	if other.NodePhase != "" || other.Status != state.BurstStatusProvisioning {
		t.Fatalf("the other tenant's burst was mutated: %+v", other)
	}
	if n := f.reaper.teardownCount("burst_other"); n != 0 {
		t.Fatalf("teardowns of the other tenant's burst = %d, want 0", n)
	}
	if len(f.reaps) != 0 {
		t.Fatal("an id this tenant does not own reached the teardown path")
	}
}

// Row absence is not a teardown receipt. A DELETE ... RETURNING claim can have
// an unknown client outcome after the database committed; in that case the row
// is gone but nobody holds the provider record and no teardown was queued. The
// connector must retain the Removed event instead of accepting a false close.
func TestAgentStreamRemovedDoesNotAcknowledgeAnAbsentBurstWithoutCostReceipt(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()
	if _, won, err := f.store.ClaimBurst(nodePhaseBurstID); err != nil || !won {
		t.Fatalf("stage ambiguous missing row: won=%v err=%v", won, err)
	}

	f.report(t, protocol.NodeEvent{BurstID: nodePhaseBurstID, Phase: protocol.NodePhaseRemoved})
	f.requireNoAck(t)
	f.drain(t)
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 without a claim result", n)
	}
}

// A Removed racing the workload's own completion must not rewrite what the
// connector already reported. The teardown still happens — the node IS gone —
// but the terminal status and the receipt explaining it stand.
func TestAgentStreamRemovedNeverOverwritesACompletedOutcome(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	// The completion landed first: status and receipt written, burst not yet
	// claimed. This is the window the two paths overlap in.
	outcome := &state.WorkloadOutcome{
		Compute: state.WorkloadComputeOutcome{Result: state.WorkloadResultSucceeded},
	}
	if !f.store.FinishWorkloadWithOutcome(nodePhaseWorkload, "succeeded", time.Now().UTC(), false, outcome) {
		t.Fatal("could not stage the completed workload")
	}

	f.reportRemoved(t)
	if out := f.awaitReap(t); !out.reaped {
		t.Fatalf("reap outcome = %+v, want the node's teardown to still happen", out)
	}
	f.drain(t)

	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil {
		t.Fatalf("workload: %v", err)
	}
	if wl.Status != "succeeded" {
		t.Fatalf("status = %q, want succeeded — the run's own result outranks the node disappearing", wl.Status)
	}
	if wl.Outcome == nil || wl.Outcome.Compute.Result != state.WorkloadResultSucceeded {
		t.Fatalf("outcome receipt = %+v, want the reported success intact", wl.Outcome)
	}
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 1 {
		t.Fatalf("teardowns = %d, want exactly 1 across both paths", n)
	}
}

// Everything a connector can say that central must refuse. None of these may
// move a status, finish a workload, or destroy a node.
func TestAgentStreamRefusesNodeEventsThatMustChangeNothing(t *testing.T) {
	tests := []struct {
		name string
		// send writes the frame under test on the fixture's socket.
		send func(t *testing.T, f *nodePhaseFixture)
		// verify runs after the frame has been processed, for what the shared
		// assertions below cannot see.
		verify func(t *testing.T, f *nodePhaseFixture)
	}{
		{
			// The event body never chooses a tenant: tenancy comes from the
			// token this socket was admitted under. A connector that could
			// report on another tenant's burst could destroy it.
			name: "another tenant's burst",
			send: func(t *testing.T, f *nodePhaseFixture) {
				f.store.AddCustomer(&state.Customer{ID: "cust_other", Token: "tok_other", Plan: "pro"})
				err := f.store.PutBurst(&state.Burst{
					ID:         "burst_other",
					CustomerID: "cust_other",
					Backend:    "linode",
					BackendID:  "vm-other",
					Status:     state.BurstStatusProvisioning,
					CreatedAt:  time.Now(),
				})
				if err != nil {
					t.Fatalf("seed foreign burst: %v", err)
				}
				f.report(t, protocol.NodeEvent{BurstID: "burst_other", Phase: protocol.NodePhaseRemoved})
			},
			verify: func(t *testing.T, f *nodePhaseFixture) {
				other, err := f.store.GetBurst("burst_other")
				if err != nil {
					t.Fatalf("the other tenant's burst was torn down by this connector: %v", err)
				}
				if other.NodePhase != "" || other.Status != state.BurstStatusProvisioning {
					t.Fatalf("the other tenant's burst was mutated: %+v", other)
				}
				if n := f.reaper.teardownCount("burst_other"); n != 0 {
					t.Fatalf("teardowns of the other tenant's burst = %d, want 0", n)
				}
			},
		},
		{
			name: "an id no burst carries",
			send: func(t *testing.T, f *nodePhaseFixture) {
				f.report(t, protocol.NodeEvent{BurstID: "burst_nosuch", Phase: protocol.NodePhaseRemoved})
			},
		},
		{
			name: "a phase outside the closed set",
			send: func(t *testing.T, f *nodePhaseFixture) {
				f.report(t, protocol.NodeEvent{BurstID: nodePhaseBurstID, Phase: "Terminated"})
			},
		},
		{
			name: "a blank burst id",
			send: func(t *testing.T, f *nodePhaseFixture) {
				f.report(t, protocol.NodeEvent{BurstID: "   ", Phase: protocol.NodePhaseRemoved})
			},
		},
		{
			name: "a body that is not a node event",
			send: func(t *testing.T, f *nodePhaseFixture) {
				f.write(t, protocol.Envelope{
					APIVersion: protocol.APIVersion,
					Type:       protocol.TypeNodeEvent,
					Timestamp:  time.Now().UTC(),
					Body:       json.RawMessage(`["burst_abc123","Removed"]`),
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newNodePhaseFixture(t)
			f.seedBurst(t)
			f.seedWorkload()

			tc.send(t, f)
			f.drain(t)

			b := f.burst(t)
			if b.Status != state.BurstStatusProvisioning || b.NodePhase != "" {
				t.Fatalf("burst mutated by a refused event: status=%q phase=%q", b.Status, b.NodePhase)
			}
			if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
				t.Fatalf("teardowns = %d, want 0", n)
			}
			if len(f.reaps) != 0 {
				t.Fatal("a refused event reached the teardown path")
			}
			wl, err := f.store.GetWorkload(nodePhaseWorkload)
			if err != nil || wl.FinishedAt != nil {
				t.Fatalf("workload = %+v err=%v, want it untouched", wl, err)
			}
			// The socket stayed usable up to the close: one bad frame must not
			// take the connection, or a single malformed event would cost the
			// tenant every later report too.
			if _, err := f.store.AgentForCluster(state.DevCustomerID, "c1"); !errors.Is(err, state.ErrNotFound) {
				t.Fatalf("connector state after the refused event: %v", err)
			}
			if tc.verify != nil {
				tc.verify(t, f)
			}
		})
	}
}

// failingPhaseWriter is a durable backend that refuses the write.
type failingPhaseWriter struct{ calls int }

func (w *failingPhaseWriter) UpdateBurstNodePhase(context.Context, state.BurstNodePhaseUpdate) (bool, error) {
	w.calls++
	return false, errors.New("postgres is down")
}

// Fail-closed: a phase central could not record is a phase central does not
// claim. The durable record is what every other replica reads to know what a
// node is doing, and a projection moved off a write that may not have landed
// would show a customer a status nothing stands behind.
//
// The phase here is a live one. Removed does not come through this path at all —
// it writes nothing durable before its teardown, which is what
// TestAgentStreamRemovedWritesNothingTerminalBeforeTheTeardown pins down.
func TestAgentStreamFailsClosedWhenAPhaseCannotBeRecorded(t *testing.T) {
	writer := &failingPhaseWriter{}
	f := newNodePhaseFixture(t, withBurstNodePhaseWriter(writer))
	f.seedBurst(t)
	f.seedWorkload()

	f.report(t, protocol.NodeEvent{BurstID: nodePhaseBurstID, Phase: protocol.NodePhaseNotReady})
	f.drain(t)

	if writer.calls == 0 {
		t.Fatal("the phase write was never attempted")
	}
	if n := f.reaper.teardownCount(nodePhaseBurstID); n != 0 {
		t.Fatalf("teardowns = %d, want 0 after a refused durable write", n)
	}
	if len(f.reaps) != 0 {
		t.Fatal("a phase that could not be recorded still reached the teardown path")
	}
	b := f.burst(t)
	if b.NodePhase != "" || b.Status != state.BurstStatusProvisioning {
		t.Fatalf("burst moved on a phase the database refused: %+v", b)
	}
	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil || wl.FinishedAt != nil {
		t.Fatalf("workload = %+v err=%v, want it untouched", wl, err)
	}
}

func TestAgentStreamGPUObservationStampedOnReady(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	gpuAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	f.report(t, protocol.NodeEvent{
		BurstID:          nodePhaseBurstID,
		Phase:            protocol.NodePhaseReady,
		GPUAllocatable:   true,
		GPUAllocatableAt: &gpuAt,
	})
	waitFor(t, "the burst to become running", func() bool {
		b, err := f.store.GetBurst(nodePhaseBurstID)
		return err == nil && b.Status == state.BurstStatusRunning
	})
	f.drain(t)

	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if wl.GPUObservation == nil {
		t.Fatal("GPUObservation not stamped after Ready with GPU allocatable")
	}
	if !wl.GPUObservation.AllocatableAt.Equal(gpuAt) {
		t.Fatalf("GPUObservation.AllocatableAt = %v, want %v", wl.GPUObservation.AllocatableAt, gpuAt)
	}
}

func TestAgentStreamPodEventStampsObservation(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	scheduledAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	f.write(t, mustEnvelope(t, protocol.TypePodEvent, protocol.PodEvent{
		WorkloadID:  nodePhaseWorkload,
		Scheduled:   true,
		PodName:     "train-pod-0",
		NodeName:    nodePhaseNodeName,
		ScheduledAt: &scheduledAt,
	}))
	f.drain(t)

	wl, err := f.store.GetWorkload(nodePhaseWorkload)
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if wl.PodObservation == nil {
		t.Fatal("PodObservation not stamped after pod event")
	}
	if wl.PodObservation.PodName != "train-pod-0" {
		t.Fatalf("PodName = %q, want %q", wl.PodObservation.PodName, "train-pod-0")
	}
}

func TestAgentStreamPodEventRejectsOversizedName(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	scheduledAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	longName := strings.Repeat("a", 300)
	f.write(t, mustEnvelope(t, protocol.TypePodEvent, protocol.PodEvent{
		WorkloadID:  nodePhaseWorkload,
		Scheduled:   true,
		PodName:     longName,
		NodeName:    nodePhaseNodeName,
		ScheduledAt: &scheduledAt,
	}))
	f.drain(t)

	wl, _ := f.store.GetWorkload(nodePhaseWorkload)
	if wl.PodObservation != nil {
		t.Fatal("PodObservation stamped with oversized pod name — should have been rejected")
	}
}

func TestAgentStreamPodEventRejectsInvalidDNSName(t *testing.T) {
	tests := []struct {
		name    string
		podName string
		node    string
	}{
		{"uppercase pod", "Train-Pod", nodePhaseNodeName},
		{"underscore pod", "train_pod", nodePhaseNodeName},
		{"leading hyphen pod", "-train-pod", nodePhaseNodeName},
		{"trailing hyphen pod", "train-pod-", nodePhaseNodeName},
		{"uppercase node", "train-pod", "Node-A"},
		{"empty label", "train-pod", "node..name"},
		{"label too long", "train-pod", strings.Repeat("a", 64)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newNodePhaseFixture(t)
			f.seedBurst(t)
			f.seedWorkload()

			scheduledAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
			f.write(t, mustEnvelope(t, protocol.TypePodEvent, protocol.PodEvent{
				WorkloadID:  nodePhaseWorkload,
				Scheduled:   true,
				PodName:     tc.podName,
				NodeName:    tc.node,
				ScheduledAt: &scheduledAt,
			}))
			f.drain(t)

			wl, _ := f.store.GetWorkload(nodePhaseWorkload)
			if wl.PodObservation != nil {
				t.Fatalf("PodObservation stamped with invalid DNS name %q/%q", tc.podName, tc.node)
			}
		})
	}
}

func TestIsDNSSubdomain(t *testing.T) {
	valid := []string{
		"pod-0", "my-pod.ns.svc.cluster.local", "a", "node-123",
		"ys-burst-abc123", strings.Repeat("a", 63),
	}
	for _, s := range valid {
		if !isDNSSubdomain(s) {
			t.Errorf("isDNSSubdomain(%q) = false, want true", s)
		}
	}
	invalid := []string{
		"", "Pod-0", "pod_0", "-pod", "pod-", ".pod",
		"pod.", "a..b", strings.Repeat("a", 64),
		strings.Repeat("a.", 127) + "a",
	}
	for _, s := range invalid {
		if isDNSSubdomain(s) {
			t.Errorf("isDNSSubdomain(%q) = true, want false", s)
		}
	}
}

func TestAgentStreamPodEventWaitingRequiresVersionAndPod(t *testing.T) {
	tests := []struct {
		name string
		ev   protocol.PodEvent
	}{
		{
			name: "missing source timestamp",
			ev: protocol.PodEvent{
				WorkloadID:       nodePhaseWorkload,
				PodName:          "train-pod-0",
				SchedulingState:  protocol.SchedulingStateWaiting,
				SchedulingReason: protocol.SchedulingReasonInsufficientGPU,
			},
		},
		{
			name: "missing pod name",
			ev: protocol.PodEvent{
				WorkloadID:           nodePhaseWorkload,
				SchedulingState:      protocol.SchedulingStateWaiting,
				SchedulingReason:     protocol.SchedulingReasonInsufficientGPU,
				SchedulingObservedAt: ptrTime(time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newNodePhaseFixture(t)
			f.seedBurst(t)
			f.seedWorkload()
			f.write(t, mustEnvelope(t, protocol.TypePodEvent, tt.ev))
			f.drain(t)
			wl, _ := f.store.GetWorkload(nodePhaseWorkload)
			if wl.SchedulingObservation != nil {
				t.Fatalf("invalid Waiting event persisted: %+v", wl.SchedulingObservation)
			}
			f.requireNoPodAck(t)
		})
	}
}

func TestAgentStreamPodEventWaitingDerivesSafeMessage(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	f.write(t, mustEnvelope(t, protocol.TypePodEvent, protocol.PodEvent{
		WorkloadID:           nodePhaseWorkload,
		PodName:              "train-pod-0",
		SchedulingState:      protocol.SchedulingStateWaiting,
		SchedulingReason:     protocol.SchedulingReasonInsufficientGPU,
		SchedulingMessage:    "UNTRUSTED <script>alert(1)</script>",
		SchedulingObservedAt: &at,
	}))
	ack := f.awaitPodAck(t)
	if ack.SchedulingState != protocol.SchedulingStateWaiting || ack.PodName != "train-pod-0" ||
		ack.SchedulingObservedAt == nil || !ack.SchedulingObservedAt.Equal(at) {
		t.Fatalf("Waiting ACK identity = %+v", ack)
	}
	wl, _ := f.store.GetWorkload(nodePhaseWorkload)
	want := protocol.SchedulingReasonMessage(protocol.SchedulingReasonInsufficientGPU)
	if wl.SchedulingObservation == nil || wl.SchedulingObservation.Message != want {
		t.Fatalf("stored scheduling observation = %+v, want canonical message %q", wl.SchedulingObservation, want)
	}
}

func TestAgentStreamPodEventRejectsWaitingScheduledContradiction(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	f.write(t, mustEnvelope(t, protocol.TypePodEvent, protocol.PodEvent{
		WorkloadID:           nodePhaseWorkload,
		PodName:              "train-pod-0",
		NodeName:             nodePhaseNodeName,
		Scheduled:            true,
		SchedulingState:      protocol.SchedulingStateWaiting,
		SchedulingReason:     protocol.SchedulingReasonInsufficientGPU,
		SchedulingObservedAt: &at,
	}))
	f.drain(t)
	wl, _ := f.store.GetWorkload(nodePhaseWorkload)
	if wl.PodObservation != nil || wl.SchedulingObservation != nil {
		t.Fatal("contradictory Waiting+Scheduled event was accepted")
	}
	f.requireNoPodAck(t)
}

func TestAgentStreamPodEventScheduledAckIncludesSchedulingIdentity(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()
	scheduledAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	observedAt := scheduledAt.Add(time.Second)
	f.write(t, mustEnvelope(t, protocol.TypePodEvent, protocol.PodEvent{
		WorkloadID:           nodePhaseWorkload,
		Scheduled:            true,
		PodName:              "train-pod-0",
		NodeName:             nodePhaseNodeName,
		ScheduledAt:          &scheduledAt,
		SchedulingState:      protocol.SchedulingStateScheduled,
		SchedulingObservedAt: &observedAt,
	}))
	ack := f.awaitPodAck(t)
	if ack.SchedulingState != protocol.SchedulingStateScheduled || ack.PodName != "train-pod-0" ||
		ack.SchedulingObservedAt == nil || !ack.SchedulingObservedAt.Equal(observedAt) ||
		ack.ScheduledAt == nil || !ack.ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("Scheduled ACK identity = %+v", ack)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }

// requireNoPodAck asserts central sent no pod ACK after the socket has
// drained. It is the durability side of the invariant: a report whose
// ScheduledAt central refused MUST leave the connector's retry outstanding.
func (f *nodePhaseFixture) requireNoPodAck(t *testing.T) {
	t.Helper()
	select {
	case ack := <-f.podAcks:
		t.Fatalf("central acknowledged a pod event it must not have: %+v", ack)
	default:
	}
}

// awaitPodAck blocks until central sends the pod scheduling receipt.
func (f *nodePhaseFixture) awaitPodAck(t *testing.T) protocol.PodEventAck {
	t.Helper()
	select {
	case ack := <-f.podAcks:
		return ack
	case <-time.After(3 * time.Second):
		t.Fatal("central never acknowledged the pod scheduling observation")
		return protocol.PodEventAck{}
	}
}

func TestAgentStreamPodEventAckEchoesPersistedIdentity(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	scheduledAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	f.write(t, mustEnvelope(t, protocol.TypePodEvent, protocol.PodEvent{
		WorkloadID:  nodePhaseWorkload,
		Scheduled:   true,
		PodName:     "train-pod-0",
		NodeName:    nodePhaseNodeName,
		ScheduledAt: &scheduledAt,
	}))

	ack := f.awaitPodAck(t)
	if ack.WorkloadID != nodePhaseWorkload {
		t.Fatalf("WorkloadID = %q, want %q", ack.WorkloadID, nodePhaseWorkload)
	}
	if ack.PodName != "train-pod-0" {
		t.Fatalf("ACK PodName = %q, want %q", ack.PodName, "train-pod-0")
	}
	if ack.ScheduledAt == nil || !ack.ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("ACK ScheduledAt = %v, want %v echoed exactly", ack.ScheduledAt, scheduledAt)
	}
	f.drain(t)
}

func TestAgentStreamPodEventRejectsZeroScheduledAt(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	var zero time.Time
	f.write(t, mustEnvelope(t, protocol.TypePodEvent, protocol.PodEvent{
		WorkloadID:  nodePhaseWorkload,
		Scheduled:   true,
		PodName:     "train-pod-0",
		NodeName:    nodePhaseNodeName,
		ScheduledAt: &zero,
	}))
	f.drain(t)

	wl, _ := f.store.GetWorkload(nodePhaseWorkload)
	if wl.PodObservation != nil {
		t.Fatal("PodObservation persisted for a non-nil zero ScheduledAt; a wire-bug value must be refused")
	}
	f.requireNoPodAck(t)
}

func TestAgentStreamPodEventRejectsFarFutureScheduledAt(t *testing.T) {
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	// One hour ahead of now is well past the 10-minute skew tolerance.
	future := time.Now().UTC().Add(1 * time.Hour)
	f.write(t, mustEnvelope(t, protocol.TypePodEvent, protocol.PodEvent{
		WorkloadID:  nodePhaseWorkload,
		Scheduled:   true,
		PodName:     "train-pod-0",
		NodeName:    nodePhaseNodeName,
		ScheduledAt: &future,
	}))
	f.drain(t)

	wl, _ := f.store.GetWorkload(nodePhaseWorkload)
	if wl.PodObservation != nil {
		t.Fatal("PodObservation persisted for a far-future ScheduledAt; clock skew must not survive validation")
	}
	f.requireNoPodAck(t)
}

func TestAgentStreamPodEventLegacyNilScheduledAtAcksWithNil(t *testing.T) {
	// Legacy path: connector sends no ScheduledAt. Central persists at receipt
	// time, but the ACK carries a NIL ScheduledAt on purpose — a versioned
	// connector's cached event carries ScheduledAt, so a nil-ScheduledAt ACK
	// cannot clear it.
	f := newNodePhaseFixture(t)
	f.seedBurst(t)
	f.seedWorkload()

	f.write(t, mustEnvelope(t, protocol.TypePodEvent, protocol.PodEvent{
		WorkloadID: nodePhaseWorkload,
		Scheduled:  true,
		PodName:    "train-pod-0",
		NodeName:   nodePhaseNodeName,
	}))

	ack := f.awaitPodAck(t)
	if ack.ScheduledAt != nil {
		t.Fatalf("legacy ACK carried a ScheduledAt (%v); must be nil so versioned caches are not cleared", ack.ScheduledAt)
	}
	if ack.PodName != "train-pod-0" {
		t.Fatalf("legacy ACK PodName = %q, want %q", ack.PodName, "train-pod-0")
	}
	f.drain(t)
}

func TestValidatePodEventScheduledAt(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)

	t.Run("nil is legacy and uses receipt time", func(t *testing.T) {
		at, legacy, ok := validatePodEventScheduledAt(nil, now)
		if !ok || !legacy {
			t.Fatalf("nil source: legacy=%v ok=%v, want true/true", legacy, ok)
		}
		if !at.Equal(now) {
			t.Fatalf("nil source persisted %v, want receipt time %v", at, now)
		}
	})

	t.Run("non-nil zero is refused", func(t *testing.T) {
		var zero time.Time
		_, _, ok := validatePodEventScheduledAt(&zero, now)
		if ok {
			t.Fatal("non-nil zero source was accepted; a wire-bug value must be refused")
		}
	})

	t.Run("more than skew in the future is refused", func(t *testing.T) {
		future := now.Add(11 * time.Minute)
		_, _, ok := validatePodEventScheduledAt(&future, now)
		if ok {
			t.Fatal("far-future source was accepted; a skewed or forged timestamp must be refused")
		}
	})

	t.Run("within skew in the future is accepted", func(t *testing.T) {
		nearFuture := now.Add(9 * time.Minute)
		at, legacy, ok := validatePodEventScheduledAt(&nearFuture, now)
		if !ok || legacy {
			t.Fatalf("near-future source: legacy=%v ok=%v, want false/true", legacy, ok)
		}
		if !at.Equal(nearFuture.UTC()) {
			t.Fatalf("timestamp = %v, want %v preserved exactly (UTC)", at, nearFuture.UTC())
		}
	})

	t.Run("past source is accepted verbatim in UTC", func(t *testing.T) {
		past := now.Add(-2 * time.Hour)
		at, legacy, ok := validatePodEventScheduledAt(&past, now)
		if !ok || legacy {
			t.Fatalf("past source: legacy=%v ok=%v, want false/true", legacy, ok)
		}
		if !at.Equal(past) {
			t.Fatalf("timestamp = %v, want %v", at, past)
		}
	})
}
