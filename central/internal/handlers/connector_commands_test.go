package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

type recordingCommandLedger struct {
	*state.Store
	ackErr chan error
}

type blockFirstConnectorCommandClaim struct {
	*state.Store
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *blockFirstConnectorCommandClaim) ClaimConnectorCommand(ctx context.Context, customerID, clusterID string,
	lease time.Duration, maxAttempts int) (*state.ConnectorCommand, bool, error) {
	blocked := false
	l.once.Do(func() {
		blocked = true
		close(l.started)
	})
	if blocked {
		select {
		case <-ctx.Done():
		case <-l.release:
		}
		return nil, false, nil
	}
	return l.Store.ClaimConnectorCommand(ctx, customerID, clusterID, lease, maxAttempts)
}

func (l recordingCommandLedger) AcknowledgeConnectorCommand(ctx context.Context, customerID, clusterID string,
	ack protocol.CommandAck, retryAt time.Time, maxAttempts int) (state.ConnectorCommandAckResult, error) {
	result, err := l.Store.AcknowledgeConnectorCommand(ctx, customerID, clusterID, ack, retryAt, maxAttempts)
	select {
	case l.ackErr <- err:
	default:
	}
	return result, err
}

func bindLedgerServer(t *testing.T, store *state.Store) *httptest.Server {
	t.Helper()
	stream := NewAgentStream(store, quietLog(), nil, WithConnectorCommandLedger(store))
	srv := httptest.NewServer(ConnectorAuth(store, stream))
	t.Cleanup(srv.Close)
	return srv
}

func durableTestCommand(t *testing.T, kind protocol.MessageType, aggregateID, workloadID, burstID, dependsOn string) state.ConnectorCommand {
	t.Helper()
	semantic := connectorCommandSemantic(kind, aggregateID)
	id := state.StableConnectorCommandID(state.DevCustomerID, "cluster-ledger", semantic)
	cmd, err := state.NewConnectorCommand(state.DevCustomerID, "cluster-ledger", semantic, "burst:"+burstID,
		workloadID, burstID, dependsOn, protocol.Envelope{
			APIVersion: protocol.APIVersion,
			Type:       kind,
			ID:         id,
			Timestamp:  time.Date(2026, 8, 20, 13, 0, 0, 0, time.UTC),
			Body:       json.RawMessage(`{"aggregate":"` + aggregateID + `"}`),
		})
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}

func readCommand(t *testing.T, conn *websocket.Conn) protocol.Envelope {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	var env protocol.Envelope
	if err := conn.ReadJSON(&env); err != nil {
		t.Fatalf("read durable command: %v", err)
	}
	return env
}

func sendCommandAck(t *testing.T, conn *websocket.Conn, commandID string) {
	t.Helper()
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeCommandAck,
		protocol.CommandAck{CommandID: commandID, Success: true})); err != nil {
		t.Fatalf("send command ACK: %v", err)
	}
}

func commandStatuses(t *testing.T, store *state.Store, workloadID string) []state.ConnectorCommandStatus {
	t.Helper()
	statuses, err := store.ListConnectorCommandsForWorkload(context.Background(), state.DevCustomerID, workloadID)
	if err != nil {
		t.Fatal(err)
	}
	return statuses
}

func TestAgentStreamReplaysStableCommandsAcrossReconnectAndOrdersDependency(t *testing.T) {
	store := state.New()
	announce := durableTestCommand(t, protocol.TypeBurstAnnounce, "burst-ledger", "wl-ledger", "burst-ledger", "")
	create := durableTestCommand(t, protocol.TypeCreateJob, "wl-ledger", "wl-ledger", "burst-ledger", announce.ID)
	if err := store.PutConnectorCommands(context.Background(), []state.ConnectorCommand{announce, create}); err != nil {
		t.Fatal(err)
	}
	srv := bindLedgerServer(t, store)

	first, err := dialAgent(t, srv, state.DefaultDevToken, "cluster-ledger")
	if err != nil {
		t.Fatal(err)
	}
	firstDelivery := readCommand(t, first)
	if firstDelivery.ID != announce.ID || firstDelivery.Type != protocol.TypeBurstAnnounce {
		t.Fatalf("first delivery = %+v, want announce %s", firstDelivery, announce.ID)
	}
	time.Sleep(2 * connectorCommandPoll)
	statuses := commandStatuses(t, store, "wl-ledger")
	for _, status := range statuses {
		if status.ID == create.ID && (status.State != state.ConnectorCommandPending || status.Attempts != 0) {
			t.Fatalf("create advanced before announce ACK: %+v", status)
		}
	}
	first.Close()
	waitFor(t, "unacknowledged write to become retryable", func() bool {
		for _, status := range commandStatuses(t, store, "wl-ledger") {
			if status.ID == announce.ID {
				return status.State == state.ConnectorCommandFailed && status.LastAmbiguity != ""
			}
		}
		return false
	})

	second, err := dialAgent(t, srv, state.DefaultDevToken, "cluster-ledger")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	replay := readCommand(t, second)
	if !reflect.DeepEqual(replay, firstDelivery) {
		t.Fatalf("reconnect changed persisted envelope: first=%+v replay=%+v", firstDelivery, replay)
	}
	sendCommandAck(t, second, announce.ID)
	createDelivery := readCommand(t, second)
	if createDelivery.ID != create.ID || createDelivery.Type != protocol.TypeCreateJob {
		t.Fatalf("delivery after announce ACK = %+v, want create %s", createDelivery, create.ID)
	}
	sendCommandAck(t, second, create.ID)
	sendCommandAck(t, second, create.ID)
	waitFor(t, "duplicate ACK to leave commands acknowledged", func() bool {
		statuses := commandStatuses(t, store, "wl-ledger")
		return len(statuses) == 2 && statuses[0].State == state.ConnectorCommandAcknowledged &&
			statuses[1].State == state.ConnectorCommandAcknowledged
	})
}

func TestAgentStreamDisconnectBeforeDispatchLeavesCommandPendingForReconnect(t *testing.T) {
	store := state.New()
	cmd := durableTestCommand(t, protocol.TypeBurstAnnounce, "burst-before-dispatch",
		"wl-before-dispatch", "burst-before-dispatch", "")
	if err := store.PutConnectorCommands(context.Background(), []state.ConnectorCommand{cmd}); err != nil {
		t.Fatal(err)
	}

	ledger := &blockFirstConnectorCommandClaim{
		Store: store, started: make(chan struct{}), release: make(chan struct{}),
	}
	srv, completed := credentialStreamServerWithCompletion(t, store, time.Hour, WithConnectorCommandLedger(ledger))

	first, err := dialAgent(t, srv, state.DefaultDevToken, "cluster-ledger")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	select {
	case <-ledger.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first connector never reached the pre-dispatch claim boundary")
	}
	first.Close()
	// Client Close is not a server-side disconnect barrier. Keep the claim
	// blocked until stream cancellation unblocks it and all pumps finish.
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("first connector did not cancel its blocked claim and finish cleanup")
	}
	close(ledger.release)

	statuses := commandStatuses(t, store, "wl-before-dispatch")
	if len(statuses) != 1 || statuses[0].State != state.ConnectorCommandPending || statuses[0].Attempts != 0 ||
		statuses[0].LastAmbiguity != "" {
		t.Fatalf("command after pre-dispatch disconnect = %+v, want untouched pending state", statuses)
	}

	second, err := dialAgent(t, srv, state.DefaultDevToken, "cluster-ledger")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	delivery := readCommand(t, second)
	if !reflect.DeepEqual(delivery, cmd.Envelope) {
		t.Fatalf("reconnect delivery = %+v, want persisted envelope %+v", delivery, cmd.Envelope)
	}
	sendCommandAck(t, second, cmd.ID)
	waitFor(t, "reconnected command acknowledgement", func() bool {
		statuses := commandStatuses(t, store, "wl-before-dispatch")
		return len(statuses) == 1 && statuses[0].State == state.ConnectorCommandAcknowledged &&
			statuses[0].Attempts == 1
	})
}

func TestAgentStreamCancelledDispatchDoesNotClaim(t *testing.T) {
	store := state.New()
	cmd := durableTestCommand(t, protocol.TypeBurstAnnounce, "burst-cancelled-dispatch",
		"wl-cancelled-dispatch", "burst-cancelled-dispatch", "")
	if err := store.PutConnectorCommands(context.Background(), []state.ConnectorCommand{cmd}); err != nil {
		t.Fatal(err)
	}
	stream := NewAgentStream(store, quietLog(), nil, WithConnectorCommandLedger(store))
	agent := &state.Agent{ID: "agent-cancelled-dispatch", CustomerID: state.DevCustomerID,
		ClusterID: "cluster-ledger", Send: make(chan protocol.Envelope, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	close(done)
	stream.dispatchConnectorCommands(ctx, agent, done)
	statuses := commandStatuses(t, store, "wl-cancelled-dispatch")
	if len(statuses) != 1 || statuses[0].State != state.ConnectorCommandPending || statuses[0].Attempts != 0 {
		t.Fatalf("cancelled dispatcher claimed a command: %+v", statuses)
	}
	if len(agent.Send) != 0 {
		t.Error("cancelled dispatcher queued a command")
	}
}

func TestAgentStreamRefusesCrossClusterCommandAck(t *testing.T) {
	store := state.New()
	cmd := durableTestCommand(t, protocol.TypeDrainNode, "burst-scope", "wl-scope", "burst-scope", "")
	if err := store.PutConnectorCommands(context.Background(), []state.ConnectorCommand{cmd}); err != nil {
		t.Fatal(err)
	}
	ledger := recordingCommandLedger{Store: store, ackErr: make(chan error, 1)}
	stream := NewAgentStream(store, quietLog(), nil, WithConnectorCommandLedger(ledger))
	srv := httptest.NewServer(ConnectorAuth(store, stream))
	defer srv.Close()
	conn, err := dialAgent(t, srv, state.DefaultDevToken, "cluster-other")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitFor(t, "cross-cluster connector registration", func() bool {
		_, err := store.AgentForCluster(state.DevCustomerID, "cluster-other")
		return err == nil
	})
	sendCommandAck(t, conn, cmd.ID)
	select {
	case ackErr := <-ledger.ackErr:
		if !errors.Is(ackErr, state.ErrConnectorCommandScope) {
			t.Fatalf("cross-cluster ACK error = %v, want scope refusal", ackErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cross-cluster ACK was not processed")
	}
	statuses := commandStatuses(t, store, "wl-scope")
	if len(statuses) != 1 || statuses[0].State != state.ConnectorCommandPending {
		t.Fatalf("cross-cluster ACK completed command: %+v", statuses)
	}
}

func TestAgentStreamDrainsBeyondLegacyQueueCapacityWithoutDropping(t *testing.T) {
	store := state.New()
	const total = 140
	for start := 0; start < total; start += 20 {
		batch := make([]state.ConnectorCommand, 0, 20)
		for i := start; i < start+20; i++ {
			burstID := fmt.Sprintf("burst-pressure-%03d", i)
			batch = append(batch, durableTestCommand(t, protocol.TypeDrainNode, burstID, "wl-pressure", burstID, ""))
		}
		if err := store.PutConnectorCommands(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
	}
	srv := bindLedgerServer(t, store)
	conn, err := dialAgent(t, srv, state.DefaultDevToken, "cluster-ledger")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	seen := make(map[string]bool, total)
	for len(seen) < total {
		env := readCommand(t, conn)
		if seen[env.ID] {
			t.Fatalf("command %s was redelivered before its ACK", env.ID)
		}
		seen[env.ID] = true
		sendCommandAck(t, conn, env.ID)
	}
	waitFor(t, "all queue-pressure commands to be acknowledged", func() bool {
		statuses := commandStatuses(t, store, "wl-pressure")
		if len(statuses) != total {
			return false
		}
		for _, status := range statuses {
			if status.State != state.ConnectorCommandAcknowledged {
				return false
			}
		}
		return true
	})
}

// TestConnectorReconnectThroughCompletionAndDeleteWorkerComposition composes
// the connector WebSocket reconnect path, the Started/Complete HTTP handlers,
// and the ProviderDeleteWorker success path into a single in-process scenario.
//
// What this proves (in-memory fakes, no PostgreSQL):
//   - Command delivery and replay across connector reconnects preserve
//     envelope identity and dependency ordering.
//   - The Started and Complete HTTP handlers update workload status correctly.
//   - Completion books exactly one provider-delete; a sequential duplicate
//     reapBurst call is idempotent (not a concurrency proof).
//   - The ProviderDeleteWorker success path converges: burst retired, cost
//     frozen, teardown receipt written, cleanup repair acknowledged.
//
// What this does NOT prove — see dedicated suites:
//   - PostgreSQL process-restart durability and lease competition:
//     state.TestConnectorCommandPostgresRestartAndLeaseCompetition,
//     state.TestConnectorCommandPostgresOperatorRecovery.
//   - Provider-delete PostgreSQL state machine and retry/crash recovery:
//     lifecycle.TestPostgresProviderDeleteLifecycle,
//     lifecycle.TestProviderDeleteSchemaContainsAuthoritativeStateMachine.
//   - Command generation (workloadConnectorCommands): commands here are
//     constructed manually to test delivery/replay ordering only.
func TestConnectorReconnectThroughCompletionAndDeleteWorkerComposition(t *testing.T) {
	store := state.New()
	cust, err := store.AuthCustomerContext(context.Background(), state.DefaultDevToken)
	if err != nil {
		t.Fatal(err)
	}

	const (
		workloadID = "wl-fault"
		burstID    = "burst-fault"
		clusterID  = "cluster-ledger"
	)

	// Seed the workload and burst as a successful Create would leave them.
	store.PutWorkload(&state.Workload{
		ID: workloadID, CustomerID: state.DevCustomerID, BurstID: burstID,
		ClusterID: clusterID, Status: "provisioning",
		CreatedAt: time.Now().Add(-5 * time.Minute).UTC(),
	})
	if err := store.PutBurst(&state.Burst{
		ID: burstID, CustomerID: state.DevCustomerID, ClusterID: clusterID,
		Backend: "linode", BackendID: "linode_fault_001", Region: "us-east",
		SKU: "g6-standard-2", PodCIDR: "10.244.9.0/24", HourlyUSD: 2.0,
		NodeName:  "burst-fault-node",
		CreatedAt: time.Now().Add(-5 * time.Minute).UTC(), Status: "provisioning",
	}); err != nil {
		t.Fatal(err)
	}

	// Commands are constructed manually to test delivery/replay ordering;
	// workloadConnectorCommands generation is covered separately.
	announce := durableTestCommand(t, protocol.TypeBurstAnnounce, burstID, workloadID, burstID, "")
	create := durableTestCommand(t, protocol.TypeCreateJob, workloadID, workloadID, burstID, announce.ID)
	if err := store.PutConnectorCommands(context.Background(), []state.ConnectorCommand{announce, create}); err != nil {
		t.Fatal(err)
	}

	// Stand up the connector WebSocket delivery loop with the in-memory Store
	// implementation of the command-ledger interface.
	srv := bindLedgerServer(t, store)

	// ── Crash boundary 1: disconnect after announce delivery, before ACK ──

	first, err := dialAgent(t, srv, state.DefaultDevToken, clusterID)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	firstDelivery := readCommand(t, first)
	if firstDelivery.ID != announce.ID || firstDelivery.Type != protocol.TypeBurstAnnounce {
		t.Fatalf("first delivery = %+v, want announce %s", firstDelivery, announce.ID)
	}

	// The create must not advance while announce is unacknowledged. Wait on the
	// observable claim state rather than sleeping for an assumed number of poll
	// intervals, which makes this assertion deterministic on a loaded runner.
	waitFor(t, "announce claimed while create remains pending", func() bool {
		var announceProcessing, createPending bool
		for _, status := range commandStatuses(t, store, workloadID) {
			switch status.ID {
			case announce.ID:
				announceProcessing = status.State == state.ConnectorCommandProcessing && status.Attempts == 1
			case create.ID:
				createPending = status.State == state.ConnectorCommandPending && status.Attempts == 0
			}
		}
		return announceProcessing && createPending
	})

	// Simulate connector crash.
	first.Close()
	waitFor(t, "announce to be marked ambiguous after disconnect", func() bool {
		for _, status := range commandStatuses(t, store, workloadID) {
			if status.ID == announce.ID {
				return status.State == state.ConnectorCommandFailed && status.LastAmbiguity != ""
			}
		}
		return false
	})

	// ── Reconnect: replay must produce identical envelopes ──

	second, err := dialAgent(t, srv, state.DefaultDevToken, clusterID)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	replay := readCommand(t, second)
	if !reflect.DeepEqual(replay, firstDelivery) {
		t.Fatalf("reconnect changed persisted envelope: first=%+v replay=%+v", firstDelivery, replay)
	}

	// ACK announce, receive and ACK create.
	sendCommandAck(t, second, announce.ID)
	createDelivery := readCommand(t, second)
	if createDelivery.ID != create.ID || createDelivery.Type != protocol.TypeCreateJob {
		t.Fatalf("create delivery = %+v, want create %s", createDelivery, create.ID)
	}
	sendCommandAck(t, second, create.ID)
	waitFor(t, "both commands acknowledged", func() bool {
		statuses := commandStatuses(t, store, workloadID)
		return len(statuses) == 2 &&
			statuses[0].State == state.ConnectorCommandAcknowledged &&
			statuses[1].State == state.ConnectorCommandAcknowledged
	})

	// ── Workload Started, then crash boundary 2: disconnect mid-run ──

	deletes := newFakeProviderDeletes()
	reaper := &fakeReaper{}
	meter := cost.NewMeter()
	wls := &Workloads{
		Store:    store,
		Reaper:   reaper,
		Log:      quietLog(),
		Cost:     meter,
		Deletes:  deletes,
		Commands: store,
	}

	rec := httptest.NewRecorder()
	wls.Started(rec, startedReq(cust, workloadID))
	if rec.Code != 200 {
		t.Fatalf("Started status = %d, want 200", rec.Code)
	}
	wl, err := store.GetWorkload(workloadID)
	if err != nil {
		t.Fatal(err)
	}
	if wl.Status != "running" || wl.StartedAt == nil {
		t.Fatalf("workload after Started = status=%q started_at=%v, want running with a timestamp", wl.Status, wl.StartedAt)
	}

	// Simulate another connector crash while the workload is running.
	second.Close()
	waitFor(t, "connector cleanup after second close", func() bool {
		_, err := store.AgentForCluster(state.DevCustomerID, clusterID)
		return err != nil
	})

	// The workload must stay running and unsettled — no terminal status, no
	// cost frozen, no provider delete booked.
	wl, _ = store.GetWorkload(workloadID)
	if wl.Status != "running" {
		t.Fatalf("workload status after mid-run crash = %q, want running", wl.Status)
	}
	if wl.Cost != nil {
		t.Fatalf("cost frozen during mid-run disconnect: %+v", wl.Cost)
	}
	if deletes.count() != 0 {
		t.Fatal("provider delete booked during mid-run disconnect")
	}

	// ── Reconnect and report completion ──

	third, err := dialAgent(t, srv, state.DefaultDevToken, clusterID)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()

	// Wait for the third connector to register before reporting completion.
	// Completion itself arrives through the normal HTTP handler, not over the
	// WebSocket; the socket must be live so the cluster is reachable for any
	// subsequent command delivery.
	waitFor(t, "third connector registered for "+clusterID, func() bool {
		_, err := store.AgentForCluster(state.DevCustomerID, clusterID)
		return err == nil
	})

	rec = httptest.NewRecorder()
	wls.Complete(rec, completeReq(cust, workloadID,
		`{"phase":"Succeeded","outcome":{"compute":{"result":"succeeded"}}}`))
	if rec.Code != 200 {
		t.Fatalf("Complete status = %d, body = %s", rec.Code, rec.Body.String())
	}

	// Complete sets the workload's terminal status and receipt before
	// initiating provider teardown — cost/provider settlement remains pending.
	wl, _ = store.GetWorkload(workloadID)
	if wl.Status != "succeeded" {
		t.Fatalf("workload status after Complete = %q, want succeeded", wl.Status)
	}
	if wl.FinishedAt == nil {
		t.Fatal("workload FinishedAt not set after Complete")
	}
	if wl.Outcome == nil || wl.Outcome.Compute.Result != state.WorkloadResultSucceeded {
		t.Fatalf("workload outcome after Complete = %+v, want succeeded compute receipt", wl.Outcome)
	}

	// Exactly one provider-delete booking must exist.
	if got := deletes.count(); got != 1 {
		t.Fatalf("provider delete bookings = %d, want exactly 1", got)
	}
	deleteRecord := deletes.get(burstID)
	if deleteRecord.State != lifecycle.ProviderDeleteQueued {
		t.Fatalf("delete state = %q, want %q", deleteRecord.State, lifecycle.ProviderDeleteQueued)
	}

	// The burst is still alive (live-cost obligation); cost is not frozen
	// until the ProviderDeleteWorker confirms provider absence.
	if _, err := store.GetBurst(burstID); err != nil {
		t.Fatal("burst retired before delete worker ran")
	}
	if wl.Cost != nil {
		t.Fatalf("cost frozen before provider absence confirmed: %+v", wl.Cost)
	}
	if reaper.teardownCount(burstID) != 0 {
		t.Fatal("provider was called from the request path instead of the delete worker")
	}
	if recorded, err := store.BurstReapRecorded(context.Background(), burstID, state.DevCustomerID); err != nil || recorded {
		t.Fatalf("teardown receipt before delete worker = recorded=%v err=%v, want absent", recorded, err)
	}

	// Sequential duplicate reap is idempotent: a second call must not create
	// another booking. This is not a concurrency proof — see the dedicated
	// lifecycle integration suites for concurrent race coverage.
	wls.reapBurst(context.Background(), burstID, "workload cancelled")
	if got := deletes.count(); got != 1 {
		t.Fatalf("sequential duplicate reap created %d bookings, want 1", got)
	}
	if after := deletes.get(burstID); !reflect.DeepEqual(after, deleteRecord) {
		t.Fatalf("sequential duplicate reap mutated booking: before=%+v after=%+v", deleteRecord, after)
	}

	// ── ProviderDeleteWorker success path (fakes) ──
	//
	// This composes the worker's happy-path convergence with the handlers
	// above. Authoritative PostgreSQL state transitions and retry/crash
	// recovery are proven by lifecycle.TestPostgresProviderDeleteLifecycle
	// and lifecycle.TestProviderDeleteSchemaContainsAuthoritativeStateMachine.

	worker := &ProviderDeleteWorker{
		Deletes:  deletes,
		Reaper:   reaper,
		Store:    store,
		Commands: store,
		Cost:     meter,
		Log:      quietLog(),
		Lease:    time.Minute,
		Now:      deletes.currentTime,
	}
	processed, drainErr := worker.Drain(context.Background())
	if drainErr != nil {
		t.Fatalf("worker drain: %v", drainErr)
	}
	if processed != 2 {
		t.Fatalf("worker drained %d items, want 2 (delete + cleanup repair)", processed)
	}

	// Provider absence confirmed: the burst must be retired.
	if _, err := store.GetBurst(burstID); err == nil {
		t.Fatal("burst survived a confirmed provider delete")
	}

	// Delete terminalized.
	finalDelete := deletes.get(burstID)
	if finalDelete.State != lifecycle.ProviderDeleteTerminated || finalDelete.DeletedAt == nil {
		t.Fatalf("delete = state=%q deleted_at=%v, want terminated with receipt",
			finalDelete.State, finalDelete.DeletedAt)
	}
	if got := deletes.successes[burstID]; got != 1 {
		t.Fatalf("provider success recorded %d times, want exactly 1", got)
	}

	// Cost observation frozen only after provider absence.
	wl, _ = store.GetWorkload(workloadID)
	if wl.Cost == nil {
		t.Fatal("no cost observation after confirmed provider delete")
	}
	if wl.Cost.Runtime <= 0 {
		t.Fatalf("cost observation has no runtime: %+v", wl.Cost)
	}
	if wl.Cost.Basis != state.WorkloadCostBasisRateRuntimeToProviderDelete {
		t.Fatalf("cost basis = %q, want %q", wl.Cost.Basis, state.WorkloadCostBasisRateRuntimeToProviderDelete)
	}

	// Teardown receipt written.
	receipted, err := store.BurstReapRecorded(context.Background(), burstID, state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	if !receipted {
		t.Fatal("no durable teardown receipt after worker convergence")
	}

	// Cleanup repair acknowledged.
	cleanup := deletes.cleanupEvent(finalDelete.ID)
	if cleanup.State != lifecycle.OutboxAcknowledged {
		t.Fatalf("cleanup repair = %q, want %q", cleanup.State, lifecycle.OutboxAcknowledged)
	}

	// No further work: the queue is empty.
	if further, _ := worker.Drain(context.Background()); further != 0 {
		t.Fatalf("worker found %d more items after convergence, want 0", further)
	}
}
