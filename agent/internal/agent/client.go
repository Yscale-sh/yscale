// Package agent is the customer-cluster-side process that connects
// to the central yscale.sh server over WebSocket, sends Hello, and
// runs the command/status loop. Used by the cmd/yscale-agent binary.
//
// Holds NO backend credentials — only the customer-supplied
// YSCALE_TOKEN. Central pushes commands; this package executes them
// locally (K8s API, WG peer mgmt) and reports back.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// Config configures the agent client.
type Config struct {
	// Endpoint is the central server's base URL, e.g. "https://api.yscale.sh".
	// http(s) is converted to ws(s); the path /v1/agent/stream is appended.
	Endpoint string

	// Token is the customer's YSCALE_TOKEN.
	Token string

	// ClusterID is a stable per-cluster UUID. Generated once and stored
	// in a K8s Secret on first run.
	ClusterID string

	// AgentVersion is the build's version string for telemetry.
	AgentVersion string

	// AuthoritativeOccupancy is what this connector tells central about its own
	// ability to end a nodeOnly burst: true only when it really can read the
	// whole cluster's pods. Set by the binary from the explicit capability flag
	// AND an empty workload namespace — the same pair IdleNodeWatcher.Run
	// demands before it will send a single occupancy observation. Default false,
	// so a manual or namespaced install claims nothing.
	AuthoritativeOccupancy bool

	// WorkloadNamespace is the connector's single namespace pin. Empty means
	// the connector is not namespace-pinned.
	WorkloadNamespace string

	// HeartbeatInterval defaults to 30s when zero.
	HeartbeatInterval time.Duration

	// ReconnectMin / ReconnectMax bound the backoff on disconnects.
	// Defaults: 1s / 60s.
	ReconnectMin, ReconnectMax time.Duration
}

// Handler executes commands the server sends. Implementations interact
// with K8s and store the per-burst metadata served at bootstrap.
type Handler interface {
	OnBurstAnnounce(ctx context.Context, cmd protocol.BurstAnnounce) error
	OnCreateJob(ctx context.Context, cmd protocol.CreateJob) error
	OnDeleteJob(ctx context.Context, cmd protocol.DeleteJob) error
	OnDrainNode(ctx context.Context, cmd protocol.DrainNode) error
	OnPrepareIdleTeardown(ctx context.Context, cmd protocol.PrepareIdleTeardown) (protocol.IdleTeardownPreflight, error)
	OnReleaseIdleTeardown(ctx context.Context, cmd protocol.ReleaseIdleTeardown) error
	OnFetchWorkloadLogs(ctx context.Context, cmd protocol.FetchWorkloadLogs) (protocol.WorkloadLogs, error)
	OnSyncRuntimeBindings(ctx context.Context, cmd protocol.SyncRuntimeBindings) error
}

type inventoryObserver interface {
	ObserveClusterInventory(context.Context) (protocol.Heartbeat, error)
}

type gpuTelemetrySource interface {
	GPUTelemetrySamples() []protocol.GPUTelemetrySample
}

// LoggingHandler is a Handler that logs every command and acks success.
// Used for v0 testing before real K8s handlers are wired.
type LoggingHandler struct {
	Log *slog.Logger
}

func (h *LoggingHandler) OnBurstAnnounce(_ context.Context, c protocol.BurstAnnounce) error {
	h.Log.Info("(stub) burst_announce", "burst", c.BurstID, "ts_hostname", c.TSHostname, "storage_bindings", len(c.Storage))
	return nil
}
func (h *LoggingHandler) OnCreateJob(_ context.Context, c protocol.CreateJob) error {
	h.Log.Info("(stub) create_job", "workload", c.WorkloadID, "namespace", c.Namespace, "spec_bytes", len(c.JobSpec))
	return nil
}
func (h *LoggingHandler) OnDeleteJob(_ context.Context, c protocol.DeleteJob) error {
	h.Log.Info("(stub) delete_job", "workload", c.WorkloadID, "namespace", c.Namespace)
	return nil
}
func (h *LoggingHandler) OnDrainNode(_ context.Context, c protocol.DrainNode) error {
	h.Log.Info("(stub) drain_node", "node", c.NodeName, "timeout", c.Timeout)
	return nil
}

// A stub cannot look at the cluster, so it cannot approve a teardown. Refusing
// is the only honest answer, and it is the one that tears nothing down.
func (h *LoggingHandler) OnPrepareIdleTeardown(_ context.Context, c protocol.PrepareIdleTeardown) (protocol.IdleTeardownPreflight, error) {
	h.Log.Info("(stub) prepare_idle_teardown", "node", c.NodeName)
	return protocol.IdleTeardownPreflight{}, errors.New("idle teardown cannot be confirmed without a Kubernetes client")
}

// A stub cordons nothing, so it has nothing to give back. Refusing keeps the
// answer honest: central logs a cordon it could not release rather than reading
// success from a connector that never touched the node.
func (h *LoggingHandler) OnReleaseIdleTeardown(_ context.Context, c protocol.ReleaseIdleTeardown) error {
	h.Log.Info("(stub) release_idle_teardown", "node", c.NodeName)
	return errors.New("an idle teardown cordon cannot be released without a Kubernetes client")
}

func (h *LoggingHandler) OnFetchWorkloadLogs(_ context.Context, c protocol.FetchWorkloadLogs) (protocol.WorkloadLogs, error) {
	h.Log.Info("(stub) fetch_workload_logs", "workload", c.WorkloadID, "namespace", c.Namespace)
	return protocol.WorkloadLogs{}, errors.New("workload logs are unavailable without a Kubernetes client")
}

func (h *LoggingHandler) OnSyncRuntimeBindings(_ context.Context, c protocol.SyncRuntimeBindings) error {
	h.Log.Info("(stub) sync_runtime_bindings", "namespace", c.Namespace, "revision", c.Revision, "keys", protocol.RuntimeBindingKeys(c.Bindings))
	return nil
}

// Client is the long-lived agent connection.
type Client struct {
	cfg     Config
	handler Handler
	log     *slog.Logger

	// mu guards send, routes and nodeEvents. send is the current connection's
	// writer queue (non-nil only while connected, swapped each runOnce). routes
	// is the latest masked cluster-route set, cached so it can be re-enqueued
	// after every reconnect even when ReportClusterRoutes was called while
	// disconnected. nodeEvents is the same idea per burst node: the LATEST phase
	// observed for each, re-flushed on reconnect.
	//
	// It also guards the ORDER node-event frames reach the writer in. Every path
	// that enqueues one holds mu from the moment it reads the cache to the moment
	// the frame is on the channel (enqueueLocked), so a reconnect flush and a
	// concurrent ReportNodeEvent cannot interleave into a stale phase landing
	// after a newer one. The send is non-blocking, so nothing here waits on a
	// socket while holding the lock.
	mu         sync.Mutex
	send       chan protocol.Envelope
	routes     []string
	nodeEvents map[string]nodeEventEntry
	podEvents  map[string]protocol.PodEvent

	inventoryMu sync.Mutex
	inventory   heartbeatInventoryCache
	// inventoryWarnedAt rate-limits the warning for a total inventory outage.
	// Partial reads remain useful and are logged at Debug; a total loss is
	// visible at Warn without writing one line per heartbeat forever.
	inventoryWarnedAt time.Time

	// nodeEventRetry is how often unacknowledged terminal events are re-sent
	// while connected. Defaulted in New; a field rather than a bare const so a
	// test can drive the cadence without waiting one out.
	nodeEventRetry time.Duration

	// nodeSendHook, when set, runs inside the critical section every node-event
	// frame is enqueued under, after the cache has been read.
	//
	// Test wiring, and the only seam that can observe that section from outside:
	// the invariant it exists to prove is that NOTHING can enqueue between the
	// cache read and the frame, which is a claim about a window no test can
	// otherwise be inside of.
	nodeSendHook func()
}

// nodeEventEntry is one burst's latest reported lifecycle phase and when it was
// observed. The timestamp orders eviction, not expiry — an unacknowledged
// terminal event is never retired by age.
type nodeEventEntry struct {
	ev         protocol.NodeEvent
	observedAt time.Time
}

type heartbeatInventoryCache struct {
	nodeObservedAt time.Time
	nodeCount      int
	burstCount     int
	podObservedAt  time.Time
	pendingPods    int
}

// terminal reports whether this entry is one central has not yet acknowledged
// and the connector therefore still owes: a Removed, or an idle teardown
// request. Nothing times either of them out — both are held, re-flushed on
// reconnect and re-sent on the retry loop until central says the burst is
// resolved. What supersedes them differs; see ReportNodeEvent.
func (e nodeEventEntry) terminal() bool {
	return protocol.TerminalNodePhase(e.ev.Phase)
}

const (
	// maxNodeEventCache caps how many bursts' phases are held for re-flush. Far
	// above any real fleet's live burst count; it exists so a bug upstream of
	// here cannot turn this into an unbounded map.
	maxNodeEventCache = 256

	// maxPodEventCache caps how many workloads' scheduling observations are
	// held for re-flush on reconnect. Same rationale as maxNodeEventCache.
	maxPodEventCache = 256

	// nodeEventRetryInterval is how often the connector re-sends terminal events
	// central has not acknowledged, on top of the reconnect flush. The reconnect
	// alone is not enough: a socket that stays up while central cannot reach its
	// state backend would otherwise report the removal once, into a failure, and
	// never again.
	//
	// Minutes, not seconds. The thing on the other end of an unacknowledged
	// Removed is a paid node that is already gone; the cost of finding out a
	// minute later is nothing, and a tight retry against a struggling central is
	// the one thing that makes an outage worse.
	nodeEventRetryInterval      = time.Minute
	heartbeatInventoryTimeout   = 10 * time.Second
	heartbeatInventoryWarnEvery = 10 * time.Minute
)

// New constructs a client. handler may be nil to default to LoggingHandler.
func New(cfg Config, handler Handler, log *slog.Logger) (*Client, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("Endpoint required")
	}
	if cfg.Token == "" {
		return nil, errors.New("Token required")
	}
	if cfg.ClusterID == "" {
		return nil, errors.New("ClusterID required")
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.ReconnectMin == 0 {
		cfg.ReconnectMin = time.Second
	}
	if cfg.ReconnectMax == 0 {
		cfg.ReconnectMax = 60 * time.Second
	}
	if handler == nil {
		handler = &LoggingHandler{Log: log}
	}
	return &Client{
		cfg:            cfg,
		handler:        handler,
		log:            log,
		nodeEventRetry: nodeEventRetryInterval,
	}, nil
}

// Run maintains the connection until ctx is cancelled. Reconnects with
// exponential backoff on transient failures.
func (c *Client) Run(ctx context.Context) error {
	backoff := c.cfg.ReconnectMin
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := c.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			c.log.Warn("agent connection ended", "error", err, "backoff", backoff)
		}
		// backoff before reconnecting
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff *= 2
		if backoff > c.cfg.ReconnectMax {
			backoff = c.cfg.ReconnectMax
		}
	}
}

func (c *Client) runOnce(ctx context.Context) error {
	wsURL, err := buildWSURL(c.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("invalid endpoint: %w", err)
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            http.ProxyFromEnvironment,
	}
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+c.cfg.Token)

	c.log.Info("connecting to central", "url", wsURL)
	conn, resp, err := dialer.DialContext(ctx, wsURL, hdr)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial: %w (status %d)", err, resp.StatusCode)
		}
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	connectionDone := make(chan struct{})
	defer close(connectionDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-connectionDone:
		}
	}()

	// Send Hello. This is the handshake and is written directly: it runs
	// before any writer goroutine spins up, so there is no concurrent
	// writer to race with here.
	hello := protocol.Hello{
		AgentVersion:           c.cfg.AgentVersion,
		ClusterID:              c.cfg.ClusterID,
		WorkloadNamespace:      c.cfg.WorkloadNamespace,
		AuthoritativeOccupancy: c.cfg.AuthoritativeOccupancy,
	}
	if err := writeMsg(conn, protocol.TypeHello, hello); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}
	// Post-Tailscale-refactor central no longer sends a HelloAck (the
	// WGCIDR / IngressIP fields it carried are gone). Connection is
	// considered established once Hello flushes successfully; central
	// proves it accepted us by sending CreateJob / BurstAnnounce later.
	c.log.Info("connected to central", "cluster", c.cfg.ClusterID)

	// Single send channel drained by ONE writer goroutine. gorilla forbids
	// concurrent writes to a *websocket.Conn, so heartbeats, CommandAcks, and
	// cluster-route frames all enqueue here instead of writing the conn
	// directly. Hello above is the only direct write (handshake, pre-pump).
	send := make(chan protocol.Envelope, 32)
	c.mu.Lock()
	c.send = send
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.send = nil
		c.mu.Unlock()
	}()

	writeCtx, cancelWrite := context.WithCancel(ctx)
	defer cancelWrite()
	go c.writePump(writeCtx, conn, send)

	// Re-assert the cached masked route set right after the handshake so a
	// reconnect re-reports the exact set the gateway is advertising, and the
	// cached burst-node phases for the same reason: a lifecycle transition
	// observed while the socket was down would otherwise never be reported, and
	// central would keep showing a node that is long gone.
	c.flushRoutes()
	c.flushNodeEvents()
	c.flushPodEvents()

	// Reset deadline; refresh on every cloud-side ping (cloud pings,
	// agent doesn't, so PingHandler is the right hook — gorilla's
	// default already replies with a PONG, so we just refresh the
	// deadline and forward).
	_ = conn.SetReadDeadline(time.Now().Add(2 * c.cfg.HeartbeatInterval))
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(2 * c.cfg.HeartbeatInterval))
		// WriteControl is the ONLY write method gorilla allows concurrently
		// with the writePump goroutine's WriteJSON. The pong runs on the read
		// goroutine, so it must NOT use WriteMessage (which would race the
		// single-writer invariant and trip gorilla's concurrent-write panic).
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
	})

	// Heartbeat goroutine, and the retry that keeps an unacknowledged removal
	// alive on a connection that never drops.
	hbCtx, cancelHB := context.WithCancel(ctx)
	defer cancelHB()
	go c.inventoryRefreshLoop(hbCtx)
	go c.heartbeatLoop(hbCtx)
	go c.nodeEventRetryLoop(hbCtx)
	go c.podEventRetryLoop(hbCtx)

	// Read loop until error/disconnect.
	return c.readLoop(ctx, conn)
}

// writePump is the SOLE writer of conn after the Hello handshake. It drains the
// send channel until the connection's context is cancelled or a write fails.
func (c *Client) writePump(ctx context.Context, conn *websocket.Conn, send <-chan protocol.Envelope) {
	for {
		select {
		case <-ctx.Done():
			return
		case env := <-send:
			if err := writeEnvelope(conn, env); err != nil {
				c.log.Debug("agent write failed", "type", env.Type, "error", err)
				return
			}
		}
	}
}

// enqueue queues an envelope on the current connection's writer. Non-blocking:
// if disconnected (send == nil) or the buffer is full it drops and reports
// false. Dropped heartbeats are harmless (liveness) and a dropped route frame
// is re-sent from cache on the next reconnect.
func (c *Client) enqueue(env protocol.Envelope) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enqueueLocked(env)
}

// enqueueLocked is enqueue with c.mu already held, so a caller can make reading
// the cache and queueing the frame it decided on ONE operation. Callers hold
// c.mu.
//
// Holding the lock across the channel send is safe because the send is
// non-blocking: the writer goroutine never takes c.mu, and a full buffer takes
// the default arm rather than waiting. What it buys is ordering — two
// goroutines queueing phases for the same burst reach the channel in the order
// they settled the cache, so an older phase can never overtake a newer one.
func (c *Client) enqueueLocked(env protocol.Envelope) bool {
	if c.send == nil {
		return false
	}
	select {
	case c.send <- env:
		return true
	default:
		c.log.Warn("agent send buffer full; dropping frame", "type", env.Type)
		return false
	}
}

// ReportPodEvent caches a workload's latest scheduling observation and enqueues
// it when connected. The cache is re-flushed after each reconnect so a
// scheduling event observed while the socket was down is not lost.
func (c *Client) ReportPodEvent(ev protocol.PodEvent) {
	if ev.WorkloadID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.podEvents == nil {
		c.podEvents = make(map[string]protocol.PodEvent)
	}
	if len(c.podEvents) >= maxPodEventCache {
		for k := range c.podEvents {
			delete(c.podEvents, k)
			break
		}
	}
	c.podEvents[ev.WorkloadID] = ev
	env, err := buildEnvelope(protocol.TypePodEvent, ev)
	if err != nil {
		return
	}
	c.enqueueLocked(env)
}

// ReportClusterRoutes caches the latest masked route set and enqueues it when
// connected. The cache is re-enqueued after each reconnect (see flushRoutes,
// called post-Hello), so a call made before the client dials still propagates.
func (c *Client) ReportClusterRoutes(routes []string) {
	c.mu.Lock()
	c.routes = append([]string(nil), routes...)
	c.mu.Unlock()
	c.flushRoutes()
}

// ReportNodeEvent caches a burst node's latest lifecycle phase and enqueues it
// when connected. Like ReportClusterRoutes, the cache is what makes this safe
// to call at any point in the connection's life: a phase observed while the
// socket is down, or one whose frame was queued into a connection that then
// died, is re-sent by flushNodeEvents after the next handshake.
//
// LATEST-state, not a log: a burst that goes Joining → Ready → NotReady holds
// one entry, because central wants the phase that is true now and re-playing
// the intermediate ones after a reconnect would report transitions in the past
// as if they had just happened.
//
// A cached Removed is the exception and outranks anything reported after it.
// The Node object is gone, so a later phase for that burst is a stale watch
// event or a recycled name, and letting one overwrite the tombstone would
// retire the only report that reaps a node the customer is still paying for.
// Central's acknowledgement is what clears it — see resolveNodeEvent.
//
// A cached Idle is held the same way but is NOT absolute, because it is a
// request about a node that still exists rather than a fact about one that does
// not. Two things clear it, and only two: a Removed, because it is the stronger
// teardown, and an explicit WithdrawIdleNodeEvent. An ordinary health phase does
// NOT, which is the point — a health phase arrives for reasons of its own (a
// transition, an occupancy observation riding one), and a report that also
// revoked pending teardowns would make an unrelated one silently cancel a
// teardown nothing had looked at the node's pods to call off.
//
// The withdrawal exists because a connector that asked for an idle teardown
// while central was unreachable would otherwise keep asking after KEDA scaled the
// Deployment back up. Only the watcher that made the request may take it back,
// and only after it has seen the node busy or unknown — see IdleNodeWatcher.
//
// It writes nothing to the socket itself — the envelope goes through the same
// single writer goroutine every other frame does.
func (c *Client) ReportNodeEvent(ev protocol.NodeEvent) {
	c.reportNodeEvent(ev, false)
}

// WithdrawIdleNodeEvent reports a phase AND drops any idle teardown request this
// connector is holding for that burst. It is IdleNodeWatcher's alone: the
// request is that watcher's to make and so its to revoke, and nothing else has
// looked at the node's pods to know it should be revoked.
func (c *Client) WithdrawIdleNodeEvent(ev protocol.NodeEvent) {
	c.reportNodeEvent(ev, true)
}

func (c *Client) reportNodeEvent(ev protocol.NodeEvent, withdrawsIdle bool) {
	if ev.BurstID == "" {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nodeEvents == nil {
		c.nodeEvents = make(map[string]nodeEventEntry)
	}
	// The cache is re-sent after every reconnect and on every terminal retry, and
	// an occupancy observation is a claim about ONE sweep — so it goes on the
	// wire and nowhere else. Caching it would let a connector that has since lost
	// its pod list keep resetting central's silence ceiling by reconnecting.
	cached := ev
	cached.OccupancyObserved = false

	held, isHeld := c.nodeEvents[ev.BurstID]
	switch {
	case isHeld && held.ev.Phase == protocol.NodePhaseRemoved && ev.Phase != protocol.NodePhaseRemoved:
		// Keep the tombstone and re-send it rather than the phase just observed:
		// this burst's removal is still owed to central.
		ev = held.ev
	case isHeld && held.ev.Phase == protocol.NodePhaseIdle && !withdrawsIdle && !protocol.TerminalNodePhase(ev.Phase):
		// Same rule, one rank down: the teardown request stands until it is
		// withdrawn or superseded, and a health observation is neither.
		ev = held.ev
	case isHeld:
		c.nodeEvents[ev.BurstID] = nodeEventEntry{ev: cached, observedAt: now}
	default:
		if len(c.nodeEvents) >= maxNodeEventCache {
			c.evictForNodeEventLocked()
		}
		c.nodeEvents[ev.BurstID] = nodeEventEntry{ev: cached, observedAt: now}
	}
	c.enqueueNodeEventLocked(ev)
}

// flushNodeEvents re-enqueues every cached burst phase. Called after the Hello
// handshake, so a reconnect re-asserts the lifecycle state central would
// otherwise be missing for the duration of the outage.
//
// The cache read and every frame it produces happen under ONE hold of c.mu.
// Snapshotting and enqueueing separately is the race this closes: a
// ReportNodeEvent landing in that gap would queue the newer phase first and
// this loop would then queue the stale one behind it, regressing central to a
// phase the connector has already moved past.
func (c *Client) flushNodeEvents() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.nodeEvents {
		c.enqueueNodeEventLocked(entry.ev)
	}
}

// nodeEventRetryLoop re-sends unacknowledged terminal events for as long as the
// connection lives. Callers start it per connection, like the heartbeat.
func (c *Client) nodeEventRetryLoop(ctx context.Context) {
	interval := c.nodeEventRetry
	if interval <= 0 {
		interval = nodeEventRetryInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.retryTerminalNodeEvents()
		}
	}
}

// retryTerminalNodeEvents re-enqueues every teardown central has not
// acknowledged.
//
// Terminal ones only. A live node's phase is re-asserted by its next transition
// and by the reconnect flush, so re-sending it on a timer is noise; a Removed
// has nothing behind it, and until central acknowledges it the connector is the
// only thing that still knows a paid node is gone. An unacknowledged idle
// request is the same debt one step earlier — the node is still up and still
// billing for work that finished.
func (c *Client) retryTerminalNodeEvents() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.nodeEvents {
		if !entry.terminal() {
			continue
		}
		c.enqueueNodeEventLocked(entry.ev)
	}
}

// resolveNodeEvent clears the terminal report central has acknowledged.
//
// Only an exact match is cleared: same burst, same phase. An acknowledgement
// naming a phase this connector never reported, or arriving for a burst whose
// entry has since been replaced, is dropped — central acknowledges what it
// resolved, and anything else is a frame about state this connector does not
// hold. The phase check matters most where the two terminal reports meet: an
// Idle receipt that arrived after the node was actually removed must not retire
// the Removed that superseded it, because only the Removed's own receipt says a
// teardown was secured.
func (c *Client) resolveNodeEvent(ack protocol.NodeEventAck) {
	if ack.BurstID == "" || !protocol.TerminalNodePhase(ack.Phase) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, held := c.nodeEvents[ack.BurstID]
	if !held || entry.ev.Phase != ack.Phase {
		return
	}
	delete(c.nodeEvents, ack.BurstID)
	c.log.Info("central resolved a burst node teardown report; dropping the cached report",
		"burst", ack.BurstID, "phase", ack.Phase)
}

// resolvePodEvent clears a pod event the server has acknowledged, and only when
// the acknowledged identity is the SAME identity the cached event carries.
//
// The identity is the (PodName, ScheduledAt) pair, UTC-normalised. Two
// receipts for the same workload are for two DIFFERENT observations if either
// half differs — a stale receipt racing a rescheduled pod, or a receipt from a
// central too old to echo the identity racing a versioned event this connector
// already cached — and clearing the cache on any of those retires an
// observation central has not durably received.
//
// A LEGACY ACK is one whose PodName and ScheduledAt are both absent. It may
// only clear a LEGACY cached event (also empty PodName, nil ScheduledAt). A
// versioned cached event always carries ScheduledAt, so a legacy ACK cannot
// clear it — the connector re-sends until a central new enough to send the
// identity acknowledges the exact one.
func (c *Client) resolvePodEvent(ack protocol.PodEventAck) {
	if ack.WorkloadID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cached, held := c.podEvents[ack.WorkloadID]
	if !held {
		return
	}
	if !podEventAckMatchesCached(ack, cached) {
		c.log.Info("pod event ack does not match the cached observation; leaving the event queued for retry",
			"workload", ack.WorkloadID,
			"ack_pod", ack.PodName, "cached_pod", cached.PodName)
		return
	}
	delete(c.podEvents, ack.WorkloadID)
	c.log.Info("central acknowledged pod observation; dropping the cached event",
		"workload", ack.WorkloadID, "pod", cached.PodName)
}

// podEventAckMatchesCached reports whether an ACK names the exact cached
// observation. Explicit scheduling events are keyed by state, pod, and source
// transition time. Legacy positive events retain their PodName+ScheduledAt
// identity so mixed-version rollouts remain fail closed.
func podEventAckMatchesCached(ack protocol.PodEventAck, cached protocol.PodEvent) bool {
	if cached.SchedulingState != "" {
		return ack.SchedulingState == cached.SchedulingState &&
			ack.PodName == cached.PodName &&
			podEventScheduledAtEqual(ack.SchedulingObservedAt, cached.SchedulingObservedAt)
	}
	if ack.SchedulingState != "" {
		return false
	}
	if ack.PodName != cached.PodName {
		return false
	}
	return podEventScheduledAtEqual(ack.ScheduledAt, cached.ScheduledAt)
}

// podEventScheduledAtEqual compares two *time.Time by UTC value with nil equal
// only to nil. Both nil is the legacy identity; either half nil is a mismatch.
func podEventScheduledAtEqual(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.UTC().Equal(b.UTC())
}

// enqueueNodeEventLocked queues one node-event frame. Callers hold c.mu.
func (c *Client) enqueueNodeEventLocked(ev protocol.NodeEvent) {
	if c.nodeSendHook != nil {
		c.nodeSendHook()
	}
	env, err := buildEnvelope(protocol.TypeNodeEvent, ev)
	if err != nil {
		c.log.Warn("encode node event", "burst", ev.BurstID, "phase", ev.Phase, "error", err)
		return
	}
	c.enqueueLocked(env)
}

// evictForNodeEventLocked makes room at the cap, cheapest entry first. Callers
// hold c.mu.
//
// A live phase is REPLACEABLE: the node is still there, so the watcher reports
// it again on its next transition and the reconnect flush re-asserts whatever
// is current. An unacknowledged teardown is not — a Removed is the only record
// that a node the customer is paying for has gone, and nothing upstream will
// produce it a second time once the Node object is deleted. So live entries go
// first, oldest to newest, and a terminal entry is dropped only when the cache
// holds nothing else.
//
// That last case is logged at ERROR because it is capacity central may never
// hear about: 256 simultaneously unresolved teardowns means central has been
// refusing acknowledgements for a long time, and the burst dropped here falls
// to the reaper watchdog alone.
func (c *Client) evictForNodeEventLocked() {
	var (
		liveID     string
		live       time.Time
		terminalID string
		terminal   time.Time
	)
	for id, entry := range c.nodeEvents {
		if entry.terminal() {
			if terminalID == "" || entry.observedAt.Before(terminal) {
				terminalID, terminal = id, entry.observedAt
			}
			continue
		}
		if liveID == "" || entry.observedAt.Before(live) {
			liveID, live = id, entry.observedAt
		}
	}
	if liveID != "" {
		c.log.Warn("node event cache full; dropping the oldest live burst's cached phase",
			"burst", liveID, "phase", c.nodeEvents[liveID].ev.Phase, "cap", maxNodeEventCache)
		delete(c.nodeEvents, liveID)
		return
	}
	if terminalID == "" {
		return
	}
	c.log.Error("node event cache full of unacknowledged teardowns; dropping one — this burst's node may never be reported gone again",
		"burst", terminalID, "phase", c.nodeEvents[terminalID].ev.Phase, "cap", maxNodeEventCache)
	delete(c.nodeEvents, terminalID)
}

// flushRoutes enqueues the cached masked route set if non-empty.
func (c *Client) flushRoutes() {
	c.mu.Lock()
	routes := append([]string(nil), c.routes...)
	c.mu.Unlock()
	if len(routes) == 0 {
		return
	}
	env, err := buildEnvelope(protocol.TypeClusterRoutes, protocol.ClusterRoutes{Routes: routes})
	if err != nil {
		c.log.Warn("encode cluster routes", "error", err)
		return
	}
	c.enqueue(env)
}

// flushPodEvents re-enqueues every cached pod scheduling observation.
func (c *Client) flushPodEvents() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ev := range c.podEvents {
		env, err := buildEnvelope(protocol.TypePodEvent, ev)
		if err != nil {
			continue
		}
		c.enqueueLocked(env)
	}
}

// podEventRetryLoop re-sends unacknowledged pod scheduling observations while
// connected. Like nodeEventRetryLoop, it exists because a socket that stays up
// while central cannot reach its state backend would otherwise report the event
// once, into a failure, and never again. The reconnect flush alone is not enough.
func (c *Client) podEventRetryLoop(ctx context.Context) {
	t := time.NewTicker(nodeEventRetryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.flushPodEvents()
		}
	}
}

func (c *Client) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.HeartbeatInterval)
	defer t.Stop()
	c.sendHeartbeat(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.sendHeartbeat(ctx)
		}
	}
}

func (c *Client) sendHeartbeat(ctx context.Context) {
	env, err := buildEnvelope(protocol.TypeHeartbeat, c.heartbeat(ctx))
	if err != nil {
		return
	}
	c.enqueue(env)
}

func (c *Client) heartbeat(_ context.Context) protocol.Heartbeat {
	hb := c.heartbeatAt(time.Now().UTC())
	if src, ok := c.handler.(gpuTelemetrySource); ok {
		hb.GPUTelemetry = src.GPUTelemetrySamples()
	}
	return hb
}

func (c *Client) heartbeatAt(now time.Time) protocol.Heartbeat {
	grace := 2 * c.cfg.HeartbeatInterval
	if grace <= 0 {
		grace = 2 * 30 * time.Second
	}
	c.inventoryMu.Lock()
	cache := c.inventory
	c.inventoryMu.Unlock()

	var hb protocol.Heartbeat
	if !cache.nodeObservedAt.IsZero() && now.Sub(cache.nodeObservedAt) <= grace {
		hb.NodeInventoryObserved = true
		observedAt := cache.nodeObservedAt
		hb.NodeInventoryObservedAt = &observedAt
		hb.NodeCount = cache.nodeCount
		hb.BurstCount = cache.burstCount
	}
	if !cache.podObservedAt.IsZero() && now.Sub(cache.podObservedAt) <= grace {
		hb.PodInventoryObserved = true
		observedAt := cache.podObservedAt
		hb.PodInventoryObservedAt = &observedAt
		hb.PendingPods = cache.pendingPods
	}
	hb.InventoryObserved = hb.NodeInventoryObserved && hb.PodInventoryObserved
	return hb
}

func (c *Client) inventoryRefreshLoop(ctx context.Context) {
	c.refreshInventory(ctx)
	// Publish the first refresh immediately. The independent liveness loop may
	// already have sent an unobserved frame, but a reconnect should not wait a
	// full interval before replacing it with the fresh inventory.
	c.sendHeartbeat(ctx)
	t := time.NewTicker(c.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.refreshInventory(ctx)
		}
	}
}

func (c *Client) refreshInventory(ctx context.Context) {
	observer, ok := c.handler.(inventoryObserver)
	if !ok || observer == nil {
		return
	}
	observeCtx, cancel := context.WithTimeout(ctx, heartbeatInventoryTimeout)
	defer cancel()
	hb, err := observer.ObserveClusterInventory(observeCtx)
	c.applyInventoryObservation(hb, time.Now().UTC())
	if err != nil {
		c.logInventoryObservationFailure(hb, err)
		return
	}
	c.inventoryMu.Lock()
	c.inventoryWarnedAt = time.Time{}
	c.inventoryMu.Unlock()
}

func (c *Client) logInventoryObservationFailure(hb protocol.Heartbeat, err error) {
	if c.log == nil {
		return
	}
	if hb.NodeInventoryObserved || hb.PodInventoryObserved {
		c.inventoryMu.Lock()
		c.inventoryWarnedAt = time.Time{}
		c.inventoryMu.Unlock()
		c.log.Debug("heartbeat inventory partially observed", "error", err)
		return
	}
	now := time.Now().UTC()
	c.inventoryMu.Lock()
	shouldWarn := c.inventoryWarnedAt.IsZero() || now.Sub(c.inventoryWarnedAt) >= heartbeatInventoryWarnEvery
	if shouldWarn {
		c.inventoryWarnedAt = now
	}
	c.inventoryMu.Unlock()
	if shouldWarn {
		c.log.Warn("heartbeat inventory unavailable", "error", err)
	}
}

func (c *Client) applyInventoryObservation(hb protocol.Heartbeat, observedAt time.Time) {
	c.inventoryMu.Lock()
	defer c.inventoryMu.Unlock()
	if hb.NodeInventoryObserved && hb.NodeCount >= 0 && hb.BurstCount >= 0 && hb.BurstCount <= hb.NodeCount {
		nodeObservedAt := observedAt
		if hb.NodeInventoryObservedAt != nil && !hb.NodeInventoryObservedAt.IsZero() {
			nodeObservedAt = *hb.NodeInventoryObservedAt
		}
		c.inventory.nodeObservedAt = nodeObservedAt
		c.inventory.nodeCount = hb.NodeCount
		c.inventory.burstCount = hb.BurstCount
	}
	if hb.PodInventoryObserved && hb.PendingPods >= 0 {
		podObservedAt := observedAt
		if hb.PodInventoryObservedAt != nil && !hb.PodInventoryObservedAt.IsZero() {
			podObservedAt = *hb.PodInventoryObservedAt
		}
		c.inventory.podObservedAt = podObservedAt
		c.inventory.pendingPods = hb.PendingPods
	}
}

func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		var env protocol.Envelope
		if err := conn.ReadJSON(&env); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		var (
			ackErr    error
			ackResult any
		)
		switch env.Type {
		case protocol.TypeBurstAnnounce:
			var cmd protocol.BurstAnnounce
			if err := json.Unmarshal(env.Body, &cmd); err == nil {
				ackErr = c.handler.OnBurstAnnounce(ctx, cmd)
			} else {
				ackErr = err
			}
		case protocol.TypeCreateJob:
			var cmd protocol.CreateJob
			if err := json.Unmarshal(env.Body, &cmd); err == nil {
				ackErr = c.handler.OnCreateJob(ctx, cmd)
			} else {
				ackErr = err
			}
		case protocol.TypeDeleteJob:
			var cmd protocol.DeleteJob
			if err := json.Unmarshal(env.Body, &cmd); err == nil {
				ackErr = c.handler.OnDeleteJob(ctx, cmd)
			} else {
				ackErr = err
			}
		case protocol.TypeDrainNode:
			var cmd protocol.DrainNode
			if err := json.Unmarshal(env.Body, &cmd); err == nil {
				ackErr = c.handler.OnDrainNode(ctx, cmd)
			} else {
				ackErr = err
			}
		case protocol.TypePrepareIdleTeardown:
			var cmd protocol.PrepareIdleTeardown
			if err := json.Unmarshal(env.Body, &cmd); err == nil {
				ackResult, ackErr = c.handler.OnPrepareIdleTeardown(ctx, cmd)
			} else {
				ackErr = err
			}
		case protocol.TypeReleaseIdleTeardown:
			var cmd protocol.ReleaseIdleTeardown
			if err := json.Unmarshal(env.Body, &cmd); err == nil {
				ackErr = c.handler.OnReleaseIdleTeardown(ctx, cmd)
			} else {
				ackErr = err
			}
		case protocol.TypeFetchWorkloadLogs:
			var cmd protocol.FetchWorkloadLogs
			if err := json.Unmarshal(env.Body, &cmd); err == nil {
				ackResult, ackErr = c.handler.OnFetchWorkloadLogs(ctx, cmd)
			} else {
				ackErr = err
			}
		case protocol.TypeSyncRuntimeBindings:
			var cmd protocol.SyncRuntimeBindings
			if err := json.Unmarshal(env.Body, &cmd); err == nil {
				ackErr = c.handler.OnSyncRuntimeBindings(ctx, cmd)
			} else {
				ackErr = err
			}
		case protocol.TypePodEventAck:
			var ack protocol.PodEventAck
			if err := json.Unmarshal(env.Body, &ack); err != nil {
				c.log.Warn("decode pod event ack", "error", err)
				continue
			}
			c.resolvePodEvent(ack)
			continue
		case protocol.TypeNodeEventAck:
			// A receipt, not a command. It is answered by dropping the cached
			// report, and `continue` is what keeps it from being answered on the
			// wire too: central has no correlation waiting for a CommandAck here,
			// so sending one would be a frame nothing reads.
			var ack protocol.NodeEventAck
			if err := json.Unmarshal(env.Body, &ack); err != nil {
				c.log.Warn("decode node event ack", "error", err)
				continue
			}
			c.resolveNodeEvent(ack)
			continue
		case protocol.TypeHello, protocol.TypeHeartbeat:
			// Server doesn't send these; ignore if it does.
		default:
			ackErr = fmt.Errorf("unknown command type %q", env.Type)
		}

		if env.ID != "" {
			ack := protocol.CommandAck{CommandID: env.ID, Success: ackErr == nil}
			if ackErr != nil {
				ack.Error = ackErr.Error()
			} else if ackResult != nil {
				ack.Result, ackErr = json.Marshal(ackResult)
				if ackErr != nil {
					ack.Success = false
					ack.Error = "encode command result"
				}
			}
			ackEnv, err := buildEnvelope(protocol.TypeCommandAck, ack)
			if err != nil {
				return err
			}
			// Route the ack through the single writer goroutine; a write
			// failure surfaces from the next ReadJSON, so don't return here.
			if !c.enqueue(ackEnv) {
				return errors.New("command acknowledgement queue unavailable")
			}
		}
	}
}

// buildEnvelope marshals body into a protocol.Envelope.
func buildEnvelope(typ protocol.MessageType, body any) (protocol.Envelope, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return protocol.Envelope{}, err
	}
	return protocol.Envelope{
		APIVersion: protocol.APIVersion,
		Type:       typ,
		Timestamp:  time.Now().UTC(),
		Body:       raw,
	}, nil
}

// writeEnvelope writes a pre-built envelope to conn. Only the writePump
// goroutine (and the pre-pump Hello handshake) call this.
func writeEnvelope(conn *websocket.Conn, env protocol.Envelope) error {
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return conn.WriteJSON(env)
}

// writeMsg builds and writes an envelope directly. Used only for the Hello
// handshake, before the writer goroutine starts.
func writeMsg(conn *websocket.Conn, typ protocol.MessageType, body any) error {
	env, err := buildEnvelope(typ, body)
	if err != nil {
		return err
	}
	return writeEnvelope(conn, env)
}

func buildWSURL(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	case "wss", "ws":
		// already a websocket URL; accept as-is.
	default:
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/v1/agent/stream"
	}
	return u.String(), nil
}

// SilentHandler discards commands without acking — useful in tests where
// the server isn't expected to expect acks.
type SilentHandler struct{}

func (SilentHandler) OnBurstAnnounce(context.Context, protocol.BurstAnnounce) error { return nil }
func (SilentHandler) OnCreateJob(context.Context, protocol.CreateJob) error         { return nil }
func (SilentHandler) OnDeleteJob(context.Context, protocol.DeleteJob) error         { return nil }
func (SilentHandler) OnDrainNode(context.Context, protocol.DrainNode) error         { return nil }
func (SilentHandler) OnPrepareIdleTeardown(context.Context, protocol.PrepareIdleTeardown) (protocol.IdleTeardownPreflight, error) {
	return protocol.IdleTeardownPreflight{}, nil
}
func (SilentHandler) OnReleaseIdleTeardown(context.Context, protocol.ReleaseIdleTeardown) error {
	return nil
}
func (SilentHandler) OnFetchWorkloadLogs(context.Context, protocol.FetchWorkloadLogs) (protocol.WorkloadLogs, error) {
	return protocol.WorkloadLogs{}, nil
}
func (SilentHandler) OnSyncRuntimeBindings(context.Context, protocol.SyncRuntimeBindings) error {
	return nil
}
