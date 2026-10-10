package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/central/internal/routecidr"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// PolicyReconciler enqueues a self-hosted coordination policy reconcile. The
// handler only ever ENQUEUES (force on connect, change-driven on a report); the
// actual EnsurePolicy PUT runs on the reconciler's own worker goroutine, never
// inline on the WS read goroutine. The managed platform's reconciler satisfies
// this; the interface lives here so handlers stay decoupled from the
// self-hosted coordination reconciler implementation.
//
// clusterID is the cluster that actually connected or reported, taken from this
// connection. It is passed rather than looked up because the reconciler has to
// converge that cluster's gateway node: a tenant's cluster list says which
// clusters exist, not which one this event came from, so a reconciler left to
// infer it approves the wrong cluster's node after a rename or a second
// registration.
//
// EnqueueClusterRemoval is the lifecycle half, driven by the tenant and admin
// delete handlers rather than by an agent event. It is a separate method
// because a withdrawal must be traceable to an explicit delete: an agent that
// reports no routes has not resolved its CIDRs yet, and a reconciler that could
// not tell the two apart would tear down a live tenant's data path on a
// transient lookup failure.
//
// EnqueueSharedTailnet is the third: the tenants with no coordination box of
// their own share ONE ACL document, so their kubelet grant is a cross-tenant
// value with no customer to key it by. A tenant leaving that document — revoked,
// offboarded — is the change nothing else would ask about, precisely because
// there is no longer a tenant to ask on its behalf.
type PolicyReconciler interface {
	Enqueue(customerID, clusterID string, force bool)
	EnqueueClusterRemoval(customerID, clusterID string)
	EnqueueSharedTailnet(force bool)
}

// NoopReconciler is the PolicyReconciler that does nothing. OSS default: the
// Tailscale ACL is operator-managed, so there is no per-customer policy to
// reconcile.
type NoopReconciler struct{}

// Enqueue implements PolicyReconciler as a no-op.
func (NoopReconciler) Enqueue(string, string, bool) {}

// EnqueueClusterRemoval implements PolicyReconciler as a no-op.
func (NoopReconciler) EnqueueClusterRemoval(string, string) {}

// EnqueueSharedTailnet implements PolicyReconciler as a no-op.
func (NoopReconciler) EnqueueSharedTailnet(bool) {}

// AgentStream handles GET /v1/agent/stream — the long-lived WebSocket
// connection from each customer's cluster agent.
type AgentStream struct {
	Store            *state.Store
	Log              *slog.Logger
	Reconciler       PolicyReconciler
	CredentialCipher CredentialCipher
	Commands         ConnectorCommandLedger

	// Tests may shorten idle validation or disable its timing contribution.
	// Production also revalidates each application envelope independently.
	credentialPoll time.Duration

	// reapGrace is how long a cluster keeps its gateway route intent after its
	// last socket closes, before the reap may take it. 0 uses
	// defaultRouteReapGrace. See startRouteReapLease.
	//
	// UNEXPORTED, and set through WithRouteReapGrace alone: the startup reap arms
	// inside the constructor, so a grace assigned after it returns is one the boot
	// leases were never given — they would already be counting down the default.
	reapGrace time.Duration

	// leaseMu guards leases, the armed reap timers keyed by customer+cluster.
	leaseMu sync.Mutex
	leases  map[string]*time.Timer

	// routeReapDone, when set, is called once a lease's reap has RETURNED. Test
	// wiring, and unexported for it: production observes the store instead. The
	// lease map cannot answer this — an entry is dropped as its timer fires, not
	// when the work behind it finishes — so a test asking whether a timer ran has
	// nothing else to read. See withRouteReapObserver.
	routeReapDone func(customerID, clusterID string)

	// lifecycle is the single-winner reap a verified Removed node event uses.
	// nil means no teardown seam is wired — the OSS default and every test that
	// constructs the stream with three arguments — and a Removed then moves the
	// burst's status and leaves the node to the reaper watchdog.
	//
	// UNEXPORTED, and set through WithBurstLifecycle alone: a handler that can
	// destroy a customer's billing node is one whose construction said so, not
	// one a later assignment quietly upgraded.
	lifecycle BurstLifecycle

	// nodePhases overrides where reported phases are persisted. Test wiring:
	// the store's own persister is unexported, so a refused durable write has no
	// other way to be staged. Production leaves it nil and writes to Store.
	nodePhases burstNodePhaseWriter

	// nodeRecords overrides where a teardown report — a removal or an idle
	// request — reads the burst it is authorised against. Test wiring for the
	// writer's reason: "a report central cannot read tears nothing down and stays
	// unacknowledged" is a claim only a read that FAILS can prove, and the store's
	// persister is unexported. Production leaves it nil.
	nodeRecords burstNodeRecordReader

	// nodeReapDone, when set, reports each Removed event's teardown once it has
	// RETURNED, and whether it won the claim. Test wiring, for the same reason
	// routeReapDone is: the reap runs off the read goroutine, so nothing a test
	// can read says when it finished. See withNodeReapObserver.
	nodeReapDone func(burstID string, reaped bool)

	upgrader websocket.Upgrader
}

// defaultRouteReapGrace is how long a disconnected connector's route intent
// survives its socket. It is generous on purpose: the cost of waiting is a
// tenant's own stale CIDRs sitting in their own policy a few minutes longer,
// and the cost of not waiting is cutting a live cluster's data path every time
// a connector pod restarts.
const defaultRouteReapGrace = 5 * time.Minute

// AgentStreamOption tunes a handler before it is used. Options apply inside the
// constructor because construction is also when the startup reap arms, so
// anything that changes what that arming does has to be set before the
// constructor returns.
type AgentStreamOption func(*AgentStream)

// WithRouteReapGrace overrides how long a cluster keeps its gateway route intent
// once nothing holds it. A non-positive value leaves defaultRouteReapGrace,
// which is what production runs with; tests use this to wait a lease out.
func WithRouteReapGrace(grace time.Duration) AgentStreamOption {
	return func(h *AgentStream) { h.reapGrace = grace }
}

// withRouteReapObserver reports each lease's reap once it has RETURNED, which is
// the only barrier a test has for the work a timer does off the back of a
// grace period.
//
// Counting armed leases is not that barrier and must not be used as one: the map
// is emptied by a cancel and by a firing alike, and an entry goes as its timer
// fires rather than when the reap behind it finishes. A test reading zero
// therefore cannot tell a lease that was cancelled from one whose work is still
// running — the difference this exists to make observable.
func withRouteReapObserver(done func(customerID, clusterID string)) AgentStreamOption {
	return func(h *AgentStream) { h.routeReapDone = done }
}

// WithBurstLifecycle gives the stream the single-winner reap path a verified
// Removed node event needs. Without it the stream still records every phase a
// connector reports — a Removed just does not tear the node down.
//
// It is an option rather than a constructor argument so existing callers keep
// compiling: the OSS entry points and the tests that predate node lifecycle
// construct a three-argument stream, and none of them should have to learn
// about teardown to keep working.
func WithBurstLifecycle(lifecycle BurstLifecycle) AgentStreamOption {
	return func(h *AgentStream) { h.lifecycle = lifecycle }
}

func WithRuntimeBindingCipher(cipher CredentialCipher) AgentStreamOption {
	return func(h *AgentStream) { h.CredentialCipher = cipher }
}

// withBurstNodePhaseWriter substitutes where reported phases are persisted.
// Test-only: *state.Store holds its persister unexported, so a durable write
// that FAILS cannot otherwise be staged — and "a phase central could not record
// tears nothing down" is a claim only a failing write can prove.
func withBurstNodePhaseWriter(w burstNodePhaseWriter) AgentStreamOption {
	return func(h *AgentStream) { h.nodePhases = w }
}

// withBurstNodeRecordReader substitutes where a teardown report reads the burst
// it is authorised against. Test-only, and the only way to stage the failure
// that matters: a removal or an idle request central cannot read must tear
// nothing down AND go unacknowledged, so the connector keeps it.
func withBurstNodeRecordReader(r burstNodeRecordReader) AgentStreamOption {
	return func(h *AgentStream) { h.nodeRecords = r }
}

// withNodeReapObserver reports each Removed event's teardown once it has
// RETURNED, along with whether it won the claim.
//
// It is the only barrier a test has over that work. The reap runs on its own
// goroutine — it must, because the inline teardown waits on a CommandAck the
// read goroutine delivers — so a test that watched the store would be racing a
// claim it cannot see the end of, and one that watched the burst disappear
// could not tell a teardown that won from one that lost.
func withNodeReapObserver(done func(burstID string, reaped bool)) AgentStreamOption {
	return func(h *AgentStream) { h.nodeReapDone = done }
}

// NewAgentStream constructs the handler with reasonable WS defaults and arms the
// startup route reap.
//
// The reap arms HERE, not in a call the wiring has to remember: a fully wired
// Store/Reconciler/Log handler is exactly what boot means for gateway route
// intent, and a startup step that lives outside the constructor is one an OSS
// consumer, a test harness, or a second entry point silently ships without.
// Construct it after the reconciler exists — the withdrawals a reap enqueues
// have nowhere to go before that.
func NewAgentStream(store *state.Store, log *slog.Logger, reconciler PolicyReconciler, opts ...AgentStreamOption) *AgentStream {
	h := &AgentStream{
		Store:      store,
		Log:        log,
		Reconciler: reconciler,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			// Cluster agents connect from the customer's cluster — the
			// origin header is uninteresting; we authenticate via Bearer
			// token in middleware.
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
	for _, opt := range opts {
		opt(h)
	}
	h.armStartupRouteReapLeases()
	return h
}

const (
	wsHandshakeTimeout = 10 * time.Second
	wsWriteTimeout     = 10 * time.Second
	wsPongTimeout      = 60 * time.Second
	wsPingInterval     = (wsPongTimeout * 9) / 10

	maxHeartbeatInventoryCount = 1_000_000
	// CommandAck carries workload-log results on this same socket. The account
	// handler accepts up to maxTenantLogResultBytes, so the WebSocket cap must
	// leave room for that JSON plus its envelope instead of disconnecting a
	// healthy connector during a bounded log fetch.
	wsAgentReadLimit   = maxTenantLogResultBytes + (64 << 10)
	heartbeatWarnEvery = 10 * time.Minute
)

// ServeHTTP upgrades the request to WebSocket, reads the agent's Hello,
// registers the agent in the store, then runs read/write pumps until
// the connection closes.
func (h *AgentStream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	credential, err := newAgentStreamCredential(r, cust.ID)
	if err != nil {
		writeCredentialError(w, err)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	credential.cancel = cancel

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade writes its own response on failure.
		return
	}
	defer conn.Close()
	conn.SetReadLimit(wsAgentReadLimit)

	// Handshake: first message must be Hello within wsHandshakeTimeout.
	_ = conn.SetReadDeadline(time.Now().Add(wsHandshakeTimeout))
	hello, err := readHello(conn)
	if err != nil {
		h.Log.Warn("agent handshake failed", "customer", cust.ID, "error", err)
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseProtocolError, err.Error()),
			time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}

	// Bind the Hello cluster id to the durable registry BEFORE the agent is
	// registered as connected. A cluster-scoped credential may only announce
	// the cluster it is bound to; a legacy tenant token claims its
	// self-announced cluster into the registry on first connection, which is
	// how a pre-registry fleet becomes durable without redeploying. Either
	// way a cluster id another tenant holds is refused here, socket or no
	// socket on the other side.
	bound := AgentClusterFromContext(r.Context())
	if bound != "" && hello.ClusterID != bound {
		h.Log.Warn("agent hello cluster does not match credential binding",
			"customer", cust.ID, "cluster", hello.ClusterID, "bound", bound)
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation,
				"hello.cluster_id does not match this connector credential's cluster"),
			time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}
	connectedAt := time.Now().UTC()
	if !h.requireStreamCredential(ctx, conn, nil, credential) {
		return
	}
	claimed, prunedCluster, err := h.Store.ClaimAgentCluster(cust.ID, hello.ClusterID, bound != "", connectedAt)
	if err != nil {
		h.Log.Warn("agent cluster bind refused",
			"customer", cust.ID, "cluster", hello.ClusterID, "error", err)
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "cluster id refused"),
			time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}
	// Claiming persists connection metadata and may wait behind another writer.
	// Check again before the socket becomes routable or receives runtime secrets.
	if !h.requireStreamCredential(ctx, conn, nil, credential) {
		return
	}
	if claimed {
		// Best-effort, exactly like the lifecycle observations: the claim is
		// an observation of a connector that already existed, and an audit
		// backend outage must never take a fleet's reconnect path with it.
		if auditErr := h.Store.AppendAudit(state.NewAuditEvent(state.AuditEvent{
			CustomerID: cust.ID,
			Actor:      state.ClusterActor(cust.ID),
			Action:     state.ActionClusterClaim,
			Outcome:    state.OutcomeObserved,
			TargetKind: state.TargetCluster,
			TargetID:   hello.ClusterID,
			Detail:     state.AuditDetail{Reason: state.ReasonClusterClaimed},
		})); auditErr != nil {
			h.Log.Warn("cluster claim audit dropped; connection continues",
				"customer", cust.ID, "cluster", hello.ClusterID, "error", auditErr)
		}
	}
	if prunedCluster != "" {
		// A full registry shed its oldest safely-prunable claimed row to admit
		// this connector, and the same durable write dropped that cluster's
		// desired routes. Two withdrawals are owed and nothing else will ask for
		// them: the tenant's policy union no longer names those CIDRs, and that
		// cluster's chart-managed gateway may still be holding the approvals
		// central gave it.
		h.Log.Info("cluster registry pruned to admit a new claim; withdrawing its gateway routes",
			"customer", cust.ID, "cluster", prunedCluster, "admitted", hello.ClusterID)
		if h.Reconciler != nil {
			h.Reconciler.EnqueueClusterRemoval(cust.ID, prunedCluster)
		}
	}

	agent := &state.Agent{
		ID:                newID("agent"),
		CustomerID:        cust.ID,
		ClusterID:         hello.ClusterID,
		WorkloadNamespace: hello.WorkloadNamespace,
		// Recorded, not just logged: the tenant's cluster list is where an
		// operator finds out which build is running in a customer's cluster, and
		// a line in this process's log is not readable from there.
		AgentVersion: hello.AgentVersion,
		// Taken at face value, like every other self-report on this socket: a
		// connector claiming it can read its own cluster's pods buys itself
		// nothing it could not already ask for outright, and the cost of
		// disbelieving a true claim is a nodeOnly burst capped at six hours.
		AuthoritativeOccupancy: hello.AuthoritativeOccupancy,
		ConnectedAt:            connectedAt,
		Send:                   make(chan protocol.Envelope, 128),
	}
	agent.MarkSeen(agent.ConnectedAt)
	h.Store.AddAgent(agent)
	// The socket is in the store's indexes now, so this cluster is held again:
	// stop the grace timer a previous disconnect armed. Ordering matters only in
	// the cheap direction — a lease that already fired finds the cluster held
	// and leaves the entry alone.
	h.cancelRouteReapLease(cust.ID, agent.ClusterID)
	h.Log.Info("agent connected",
		"customer", cust.ID,
		"agent", agent.ID,
		"cluster", agent.ClusterID,
		"agent_version", hello.AgentVersion,
	)

	// Connect-time re-assert: force a reconcile so a drifted/restarted box (or
	// one whose earlier push failed) is healed and the stored route set is
	// re-PUT, independent of whether the routes changed. This is also the first
	// point THIS cluster's gateway can be converged — boot ran before the pod
	// existed — so the force matters even when the policy is already current.
	// Enqueue only; the push runs off this goroutine.
	if h.Reconciler != nil {
		h.Reconciler.Enqueue(cust.ID, agent.ClusterID, true)
	}
	if h.Store != nil && h.CredentialCipher != nil {
		(&Accounts{Store: h.Store, Log: h.Log, CredentialCipher: h.CredentialCipher}).syncRuntimeBindings(cust.ID)
	}

	// Configure pong-based liveness.
	_ = conn.SetReadDeadline(time.Now().Add(wsPongTimeout))
	conn.SetPongHandler(func(string) error {
		if !h.requireStreamCredential(ctx, conn, agent, credential) {
			return state.ErrNotFound
		}
		_ = conn.SetReadDeadline(time.Now().Add(wsPongTimeout))
		agent.MarkSeen(time.Now().UTC())
		return nil
	})

	// Run read + write pumps. Whichever exits first triggers cleanup.
	done := make(chan struct{})
	dispatchDone := make(chan struct{})
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		h.writePump(ctx, conn, agent, done, credential)
	}()
	go func() {
		defer close(dispatchDone)
		h.dispatchConnectorCommands(ctx, agent, done)
	}()
	h.readPump(ctx, conn, agent, credential)
	cancel()
	close(done)
	_ = conn.Close()
	h.Store.RemoveAgent(agent.ID)
	<-writeDone
	<-dispatchDone
	h.abandonConnectorCommands(agent, state.ConnectorCommandReasonSessionEnded)

	// Disconnect metadata is stamped at this lifecycle boundary only — the
	// registry never sees individual heartbeats, so a chatty connector costs
	// one durable write per connection, not per frame.
	h.Store.MarkClusterDisconnected(cust.ID, agent.ClusterID, time.Now().UTC(), agent.SeenAt())
	h.startRouteReapLease(cust.ID, agent.ClusterID)
	_ = conn.Close()
	h.Log.Info("agent disconnected", "agent", agent.ID, "customer", cust.ID)
}

// startRouteReapLease arms the grace period one cluster's gateway route intent
// gets while nothing holds it: at the disconnect boundary, AFTER the socket has
// left the store's agent indexes, and at boot, where no socket has arrived yet
// (armStartupRouteReapLeases).
//
// A lease exists because a LIVE-ONLY connector's route intent has no other end.
// A pre-grammar cluster id claims no registry row (ClaimAgentCluster), so its
// socket is the whole of its claim — there is no delete to run against it, and
// no reconnect obliged to report a smaller set. Left alone, the last socket
// closing would freeze that cluster's CIDRs into the tenant's policy union for
// good.
//
// It is a lease and not an immediate reap because a socket closing is not intent
// to give up a cluster. A connector pod rolling, a node draining, a network blip
// all close one, and reaping on the close alone withdrew a live cluster's routes
// — and, on a box, its gateway's approvals — on every restart, healing only when
// the connector came back and re-reported. The grace turns that into a
// reconnect nobody notices.
//
// Cancellation on reconnect (cancelRouteReapLease) is an optimisation, not the
// correctness argument: the reap re-checks ownership inside the store, under the
// same lock as the write, so a connector that came back keeps its entry even if
// its timer fires first. Re-arming an existing lease restarts the clock, which
// is what a second socket for the same cluster closing should do.
//
// The entry is dropped as the timer fires, BEFORE the reap it armed runs: the map
// tracks what is still counting down, not what has finished. Holding the key
// until the reap returned would leak it whenever that reap panicked, and would
// leave the cancel path believing it stopped a lease that had already gone off.
// Nothing may read an empty map as a reap that COMPLETED, for the same reason —
// that is what withRouteReapObserver is for.
func (h *AgentStream) startRouteReapLease(customerID, clusterID string) {
	grace := h.routeReapGrace()
	key := leaseKey(customerID, clusterID)
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	if h.leases == nil {
		h.leases = map[string]*time.Timer{}
	}
	if t, ok := h.leases[key]; ok {
		t.Stop()
	}
	var lease *time.Timer
	// The identity check is what keeps a lease re-armed in the gap between the
	// firing and this lock from being deleted by the run it replaced. It reads
	// lease under leaseMu, which the arming below is still holding, so a timer
	// that goes off before AfterFunc has even returned sees a settled variable.
	// The unlock is deferred: a lease left locked would wedge every reap after it.
	dropLease := func() {
		h.leaseMu.Lock()
		defer h.leaseMu.Unlock()
		if h.leases[key] == lease {
			delete(h.leases, key)
		}
	}
	lease = time.AfterFunc(grace, func() {
		dropLease()
		if h.routeReapDone != nil {
			defer h.routeReapDone(customerID, clusterID)
		}
		h.reapUnheldClusterRoutes(customerID, clusterID)
	})
	h.leases[key] = lease
}

// armStartupRouteReapLeases gives every cluster that BOOT found unheld the same
// grace lease a disconnect gives one.
//
// A restart is a disconnect seen from the connector's side, and it is the one
// that looks worst: a process that has just started holds no sockets at all, so
// every live-only cluster in the fleet is momentarily held by nothing. Reaping at
// load — where this used to happen — therefore withdrew a running cluster's
// routes on every central restart, and healed only once its connector came back
// and re-reported. Arming a lease instead gives the reconnect the grace window it
// already gets at runtime, and the reap that eventually fires re-checks ownership
// inside the store, under the same lock as the write.
//
// Unexported, run from NewAgentStream, and driven by a CONSUMING store read: a
// second pass over the same durable record would restart every startup lease's
// clock from wherever it had got to, silently extending the grace of clusters the
// boot already condemned. The claim is the store's rather than this handler's
// because two streams wired to the same store are the case a per-handler once
// cannot see (see state.ConsumeUnheldRouteClusters).
func (h *AgentStream) armStartupRouteReapLeases() {
	for customerID, clusterIDs := range h.Store.ConsumeUnheldRouteClusters() {
		for _, clusterID := range clusterIDs {
			h.startRouteReapLease(customerID, clusterID)
		}
		h.Log.Info("gateway route intent found at boot for clusters this tenant does not hold; reaping after the grace period unless they reconnect",
			"customer", customerID, "clusters", clusterIDs, "grace", h.routeReapGrace())
	}
}

// cancelRouteReapLease stops the grace timer a previous disconnect armed for
// this cluster. Called once the reconnecting socket is in the store's indexes,
// so a lease that fires in the gap still finds the cluster held.
func (h *AgentStream) cancelRouteReapLease(customerID, clusterID string) {
	key := leaseKey(customerID, clusterID)
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	if t, ok := h.leases[key]; ok {
		t.Stop()
		delete(h.leases, key)
	}
}

// routeReapGrace is the configured grace period, or the default when unset.
func (h *AgentStream) routeReapGrace() time.Duration {
	if h.reapGrace > 0 {
		return h.reapGrace
	}
	return defaultRouteReapGrace
}

// leaseKey pairs a customer with one of its clusters. The NUL separator cannot
// occur in either id, so no two pairs collide.
func leaseKey(customerID, clusterID string) string { return customerID + "\x00" + clusterID }

// reapUnheldClusterRoutes drops ONE cluster's gateway route intent, once its
// grace lease has run out and only if the tenant still does not hold it.
//
// The decision is the store's, not this handler's: it re-checks under its own
// lock, so a second socket for the same id, or a reconnect that has already
// landed, keeps the entry. Narrowing it to the one leased cluster is what keeps
// this from sweeping a sibling that disconnected a moment ago and is still
// inside its own grace. Every cluster it does drop owes a policy recompute and —
// on a per-customer box — a node withdrawal against the gateway that still holds
// central's approvals, which is what the removal enqueue asks for.
func (h *AgentStream) reapUnheldClusterRoutes(customerID, clusterID string) {
	pruned, err := h.Store.ReapUnheldClusterGatewayRoutes(customerID, []string{clusterID})
	if errors.Is(err, state.ErrNotFound) {
		return // the tenant went away while this socket was open
	}
	if err != nil {
		h.Log.Warn("reaping gateway route intent for unheld clusters failed",
			"customer", customerID, "cluster", clusterID, "error", err)
		return
	}
	for _, id := range pruned {
		h.Log.Info("dropped gateway route intent for a cluster this tenant no longer holds",
			"customer", customerID, "cluster", id)
		if h.Reconciler != nil {
			h.Reconciler.EnqueueClusterRemoval(customerID, id)
		}
	}
}

// readHello waits for the first Envelope and decodes it as Hello.
func readHello(conn *websocket.Conn) (*protocol.Hello, error) {
	var env protocol.Envelope
	if err := conn.ReadJSON(&env); err != nil {
		return nil, err
	}
	if env.APIVersion != protocol.APIVersion {
		return nil, errors.New("apiVersion mismatch")
	}
	if env.Type != protocol.TypeHello {
		return nil, errors.New("first message must be hello")
	}
	var h protocol.Hello
	if err := json.Unmarshal(env.Body, &h); err != nil {
		return nil, err
	}
	if h.ClusterID == "" {
		return nil, errors.New("hello.cluster_id required")
	}
	if h.WorkloadNamespace != "" && !protocol.ValidKubernetesNamespace(h.WorkloadNamespace) {
		return nil, errors.New("hello.workload_namespace invalid")
	}
	return &h, nil
}

func writeEnvelope(conn *websocket.Conn, env protocol.Envelope) error {
	_ = conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	return conn.WriteJSON(env)
}

func validHeartbeatNodeInventory(hb protocol.Heartbeat) bool {
	if hb.NodeCount < 0 || hb.BurstCount < 0 {
		return false
	}
	if hb.NodeCount > maxHeartbeatInventoryCount ||
		hb.BurstCount > maxHeartbeatInventoryCount {
		return false
	}
	return hb.BurstCount <= hb.NodeCount
}

func validHeartbeatPodInventory(hb protocol.Heartbeat) bool {
	return hb.PendingPods >= 0 && hb.PendingPods <= maxHeartbeatInventoryCount
}

func heartbeatObservedAt(reported *time.Time, received time.Time) time.Time {
	if reported == nil || reported.IsZero() || reported.After(received) {
		return received
	}
	return reported.UTC()
}

func (h *AgentStream) warnHeartbeat(agent *state.Agent, message string, args ...any) {
	if h == nil || h.Log == nil || agent == nil || !agent.AllowHeartbeatWarning(time.Now().UTC(), heartbeatWarnEvery) {
		return
	}
	h.Log.Warn(message, args...)
}

func (h *AgentStream) handleHeartbeat(agent *state.Agent, body json.RawMessage, observedAt time.Time) {
	if agent == nil {
		return
	}
	var hb protocol.Heartbeat
	if len(body) != 0 {
		if err := json.Unmarshal(body, &hb); err != nil {
			h.warnHeartbeat(agent, "decode heartbeat", "agent", agent.ID, "error", err)
			return
		}
	}
	legacyObserved := hb.InventoryObserved && !hb.NodeInventoryObserved && !hb.PodInventoryObserved
	nodeObserved := hb.NodeInventoryObserved || legacyObserved
	podObserved := hb.PodInventoryObserved || legacyObserved

	nodeValid := !nodeObserved || validHeartbeatNodeInventory(hb)
	podValid := !podObserved || validHeartbeatPodInventory(hb)
	if !nodeValid {
		h.warnHeartbeat(agent, "rejected heartbeat node inventory",
			"agent", agent.ID, "node_count", hb.NodeCount, "burst_count", hb.BurstCount)
	}
	if !podValid {
		h.warnHeartbeat(agent, "rejected heartbeat pod inventory",
			"agent", agent.ID, "pending_pods", hb.PendingPods)
	}

	agent.ApplyClusterInventory(state.ClusterInventoryUpdate{
		NodeValid: nodeValid, NodeObserved: nodeObserved,
		NodeObservedAt: heartbeatObservedAt(hb.NodeInventoryObservedAt, observedAt),
		NodeCount:      hb.NodeCount, BurstCount: hb.BurstCount,
		PodValid: podValid, PodObserved: podObserved,
		PodObservedAt: heartbeatObservedAt(hb.PodInventoryObservedAt, observedAt),
		PendingPods:   hb.PendingPods,
	})

	h.applyGPUTelemetry(agent, hb.GPUTelemetry, observedAt)
}

const gpuTelemetryTimeout = 5 * time.Second

func (h *AgentStream) applyGPUTelemetry(agent *state.Agent, samples []protocol.GPUTelemetrySample, received time.Time) {
	if len(samples) == 0 || agent == nil || h.Store == nil {
		return
	}
	if len(samples) > protocol.MaxGPUTelemetrySamples {
		h.warnHeartbeat(agent, "gpu telemetry truncated",
			"agent", agent.ID, "count", len(samples), "max", protocol.MaxGPUTelemetrySamples)
		samples = samples[:protocol.MaxGPUTelemetrySamples]
	}
	ctx, cancel := context.WithTimeout(context.Background(), gpuTelemetryTimeout)
	defer cancel()
	for _, s := range samples {
		if !protocol.ValidGPUTelemetrySample(s) {
			continue
		}
		if s.ObservedAt.After(received.Add(time.Minute)) {
			continue
		}
		if received.Sub(s.ObservedAt) > protocol.MaxGPUTelemetryAge {
			continue
		}
		_, err := h.Store.UpdateBurstGPUTelemetry(ctx, state.BurstGPUTelemetryUpdate{
			BurstID:     s.BurstID,
			CustomerID:  agent.CustomerID,
			ClusterID:   agent.ClusterID,
			NodeName:    s.NodeName,
			Utilization: s.Utilization,
			ObservedAt:  s.ObservedAt,
		})
		if err != nil {
			h.warnHeartbeat(agent, "gpu telemetry persistence failed",
				"burst", s.BurstID)
			break
		}
	}
}

// readPump receives status messages from the agent and updates state.
func (h *AgentStream) readPump(ctx context.Context, conn *websocket.Conn, agent *state.Agent, credential *agentStreamCredential) {
	for {
		var env protocol.Envelope
		if err := conn.ReadJSON(&env); err != nil {
			return
		}
		if !h.requireStreamCredential(ctx, conn, agent, credential) {
			return
		}
		agent.MarkSeen(time.Now().UTC())

		switch env.Type {
		case protocol.TypeHeartbeat:
			h.handleHeartbeat(agent, env.Body, time.Now().UTC())
		case protocol.TypePodEvent:
			var ev protocol.PodEvent
			if err := json.Unmarshal(env.Body, &ev); err == nil {
				h.handlePodEvent(agent, ev)
			}
		case protocol.TypeNodeEvent:
			// Tenancy and the teardown decision both live in handleNodeEvent,
			// bound to agent.CustomerID — the token this socket was admitted
			// under — exactly as the cluster-routes arm below binds to
			// agent.ClusterID. The event body never chooses a tenant.
			h.handleNodeEvent(agent, env.Body)
		case protocol.TypeClusterRoutes:
			var body protocol.ClusterRoutes
			if err := json.Unmarshal(env.Body, &body); err != nil {
				h.Log.Warn("decode cluster routes", "agent", agent.ID, "error", err)
				break
			}
			// Tenancy is bound from the connection's bearer token
			// (agent.CustomerID), never the message body.
			masked, verr := routecidr.ValidateReportedCIDRs(body.Routes)
			if verr != nil {
				// Fail-closed: reject the whole report, keep the prior set.
				h.Log.Warn("rejected reported routes", "agent", agent.ID, "error", verr)
				break
			}
			// The report is stored against the cluster identity the CONNECTION
			// was admitted under, so it can only ever replace that cluster's
			// own gateway set; the tenant-wide policy union is recomputed from
			// every cluster's set inside the store.
			changed, serr := h.Store.SetCustomerGatewayRoutes(agent.CustomerID, agent.ClusterID, masked)
			if serr != nil {
				h.Log.Warn("store gateway routes", "agent", agent.ID, "error", serr)
				break
			}
			// Enqueue only on a genuine change; the worker does the PUT off
			// this read goroutine. (Connect-time force handles re-assert.)
			// The reporting agent's own cluster is what gets converged —
			// tenancy AND cluster identity come from the connection, never
			// from the message body.
			if changed && h.Reconciler != nil {
				h.Reconciler.Enqueue(agent.CustomerID, agent.ClusterID, false)
			}
		case protocol.TypeCommandAck:
			var ack protocol.CommandAck
			if err := json.Unmarshal(env.Body, &ack); err == nil {
				delivered := agent.DeliverCommandAck(ack)
				if h.Commands != nil {
					result, commandErr := h.Commands.AcknowledgeConnectorCommand(ctx,
						agent.CustomerID, agent.ClusterID, ack, connectorCommandRetryAt(1), connectorCommandMaxAttempts)
					switch {
					case commandErr == nil:
						agent.ForgetConnectorCommandLease(ack.CommandID)
						if result.Duplicate {
							h.Log.Info("duplicate connector command acknowledgement", "agent", agent.ID, "command", ack.CommandID)
						}
					case errors.Is(commandErr, state.ErrConnectorCommandScope):
						h.auditRefusedCommandAck(agent, state.ReasonConnectorCommandAckScope)
						h.Log.Warn("refused cross-cluster connector command acknowledgement", "agent", agent.ID)
					case errors.Is(commandErr, state.ErrConnectorCommandNotFound):
						if !delivered {
							h.auditRefusedCommandAck(agent, state.ReasonConnectorCommandAckUnknown)
							h.Log.Warn("refused unknown connector command acknowledgement", "agent", agent.ID)
						}
					default:
						h.Log.Warn("persist connector command acknowledgement", "agent", agent.ID, "command", ack.CommandID, "error", commandErr)
					}
				}
				if !ack.Success {
					// The connector's own ACK error is NOT logged. It is text
					// minted inside the customer's cluster — a kubectl error
					// quoting a Secret, a provider body quoting a credential —
					// and central's log is an operator-readable, shipped,
					// retained surface. The stable reason code says everything
					// central can stand behind; the detail stays in the
					// connector's logs, where the operator who can act on it
					// already reads it. See state.sanitizeConnectorAck.
					h.Log.Warn("command failed on agent",
						"agent", agent.ID,
						"command", ack.CommandID,
						"reason", state.ConnectorCommandReasonConnectorRejected,
					)
				} else if delivered {
					h.Log.Info("command acknowledged by agent",
						"agent", agent.ID,
						"command", ack.CommandID,
					)
				}
			}
		default:
			h.Log.Warn("unknown message type from agent", "agent", agent.ID, "type", env.Type)
		}
	}
}

// writePump drains the agent's Send queue plus periodic pings.
func (h *AgentStream) writePump(ctx context.Context, conn *websocket.Conn, agent *state.Agent, done <-chan struct{}, credential *agentStreamCredential) {
	defer func() {
		_ = conn.Close()
		// The reader may be inside ledger I/O rather than ReadJSON. Every
		// writer exit must cancel that work too, not just close the socket.
		if credential.cancel != nil {
			credential.cancel()
		}
	}()
	ping := time.NewTicker(wsPingInterval)
	defer ping.Stop()
	credentials := time.NewTicker(h.streamCredentialPoll())
	defer credentials.Stop()
	for {
		select {
		case env := <-agent.Send:
			if !h.requireStreamCredential(ctx, conn, agent, credential) {
				return
			}
			leaseToken, durable := agent.ConnectorCommandLease(env.ID)
			if err := writeEnvelope(conn, env); err != nil {
				if durable && h.Commands != nil {
					agent.ForgetConnectorCommandLease(env.ID)
					markCtx, markCancel := context.WithTimeout(context.Background(), wsWriteTimeout)
					if markErr := h.Commands.MarkConnectorCommandAmbiguous(markCtx, env.ID, leaseToken,
						state.ConnectorCommandReasonWriteAmbiguous, connectorCommandRetryAt(1), connectorCommandMaxAttempts); markErr != nil {
						h.Log.Warn("record connector command write ambiguity", "command", env.ID, "error", markErr)
					}
					markCancel()
				}
				return
			}
			if durable && h.Commands != nil {
				recordCtx, recordCancel := context.WithTimeout(ctx, wsWriteTimeout)
				if err := h.Commands.RecordConnectorCommandDelivered(recordCtx, env.ID, leaseToken); err != nil {
					h.Log.Warn("record connector command delivery", "command", env.ID, "error", err)
				}
				recordCancel()
			}
		case <-agent.Evicted():
			// A rotation or delete revoked this connector while its socket was
			// open. The store already forgot the agent; closing the conn is
			// what unblocks the read pump, whose cleanup then finds nothing
			// left to remove.
			h.closeCredentialStream(conn, agent, credential, state.ErrNotFound)
			return
		case <-credentials.C:
			if !h.requireStreamCredential(ctx, conn, agent, credential) {
				return
			}
		case <-ping.C:
			_ = conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}
