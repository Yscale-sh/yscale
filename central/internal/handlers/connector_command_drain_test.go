package handlers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/broker"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// The drain no longer needs the socket. A replica that holds no connector
// session at all persists the command, and the replica that does hold the
// session claims and delivers it — which is the whole point of routing
// drain_node through the ledger. Re-driving it (a redelivered teardown job, or
// the inline compensation racing the worker) finds the SAME command rather than
// queueing a second drain of a node already being deleted.
func TestDrainBurstNodePersistsForARemoteSocketOwnerAndToleratesReplay(t *testing.T) {
	ctx := context.Background()
	store := state.New()
	burst := &state.Burst{
		ID: "burst_drain", CustomerID: state.DevCustomerID,
		ClusterID: "cluster-drain", NodeName: "ys-burst-drain",
	}
	if _, err := store.AgentForCluster(state.DevCustomerID, "cluster-drain"); err == nil {
		t.Fatal("fixture has a connected connector; this test is about the replica that owns no socket")
	}

	if err := drainBurstNode(ctx, store, store, quietLog(), burst, "complete"); err != nil {
		t.Fatalf("drain from a socket-less replica: %v", err)
	}
	// Replay: same burst, same tenant, same cluster — the stable semantic
	// identity has to absorb it instead of refusing or duplicating.
	if err := drainBurstNode(ctx, store, store, quietLog(), burst, "complete"); err != nil {
		t.Fatalf("replayed drain: %v", err)
	}

	wantID := state.StableConnectorCommandID(state.DevCustomerID, "cluster-drain",
		connectorCommandSemantic(protocol.TypeDrainNode, burst.ID))

	srv := bindLedgerServer(t, store)
	conn, err := dialAgent(t, srv, state.DefaultDevToken, "cluster-drain")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	env := readCommand(t, conn)
	if env.Type != protocol.TypeDrainNode || env.ID != wantID {
		t.Fatalf("delivered envelope = (%s,%s), want drain_node %s", env.Type, env.ID, wantID)
	}
	var drain protocol.DrainNode
	if err := json.Unmarshal(env.Body, &drain); err != nil {
		t.Fatal(err)
	}
	if drain.NodeName != burst.NodeName || !drain.Delete {
		t.Fatalf("drain body = %+v, want a delete of %s", drain, burst.NodeName)
	}
	sendCommandAck(t, conn, env.ID)

	// Exactly one command existed, so nothing else is delivered and the
	// acknowledged one is not redelivered.
	conn.SetReadDeadline(time.Now().Add(4 * connectorCommandPoll))
	var extra protocol.Envelope
	if err := conn.ReadJSON(&extra); err == nil {
		t.Fatalf("a second drain was queued for one burst: %+v", extra)
	}
}

// A burst booked before ClusterID was recorded has no cluster to lease a
// command row against, and a node name is unique only inside one cluster. Those
// keep the synchronous single-connector path even when a ledger IS wired — the
// compatibility behavior is scoped to bursts that genuinely lack cluster
// identity, not to whether the ledger exists.
func TestDrainBurstNodeLegacyBurstStaysSynchronousEvenWithALedger(t *testing.T) {
	store := state.New()
	agent := &state.Agent{
		ID: "agent_only", CustomerID: "cust_legacy", ClusterID: "cluster_only",
		ConnectedAt: time.Now().UTC(), Send: make(chan protocol.Envelope, 8),
	}
	store.AddAgent(agent)
	burst := &state.Burst{ID: "burst_legacy", CustomerID: "cust_legacy", NodeName: "ys-burst-legacy"}

	done := make(chan error, 1)
	go func() {
		done <- drainBurstNode(context.Background(), store, store, quietLog(), burst, "complete")
	}()
	var env protocol.Envelope
	select {
	case env = <-agent.Send:
	case <-time.After(2 * time.Second):
		t.Fatal("a legacy burst got no drain from the tenant's only connector")
	}
	// The discriminator: a durable command is dispatched with a tracked lease,
	// a synchronous one is not.
	if _, durable := agent.ConnectorCommandLease(env.ID); durable {
		t.Fatal("a legacy burst with no cluster identity was routed through the ledger")
	}
	if !agent.DeliverCommandAck(protocol.CommandAck{CommandID: env.ID, Success: true}) {
		t.Fatal("drain acknowledgement had no waiter")
	}
	if err := <-done; err != nil {
		t.Fatalf("legacy drain: %v", err)
	}
}

// The teardown worker can now run in a process that holds no connector socket
// at all. With a ledger the drain is retired once it is DURABLE — the ledger
// owns the retry and the dead-letter from there — instead of holding the queue
// item open waiting for an ack that could only arrive on another replica.
func TestTeardownWorkerRetiresJobOnceTheDrainIsDurable(t *testing.T) {
	ctx := context.Background()
	brk := broker.NewMemory(0)
	store := state.New()
	w := &TeardownWorker{
		Broker: brk, Reaper: &fakeReaper{}, Store: store,
		Log: quietLog(), Consumer: "w1", Commands: store,
	}
	enqueueJob(t, brk, &state.Burst{
		ID: "b1", CustomerID: state.DevCustomerID, ClusterID: "cluster-1",
		Backend: "flyio", BackendID: "m1", NodeName: "ys-burst-b1",
	}, "complete")

	// Nothing is connected in this store: no replica here owns the socket.
	w.process(ctx, mustBrokerConsume(t, brk, "w1"))
	if next, _ := brk.Consume(ctx, teardownStream, teardownGroup, "w1"); next != nil {
		t.Fatalf("teardown job redelivered although the drain is durable: %+v", next)
	}

	claimed, ok, err := store.ClaimConnectorCommand(ctx, state.DevCustomerID, "cluster-1", time.Second, 3)
	if err != nil || !ok {
		t.Fatalf("the worker retired the job without leaving a claimable drain: ok=%v err=%v", ok, err)
	}
	if claimed.CommandType != protocol.TypeDrainNode || claimed.BurstID != "b1" {
		t.Fatalf("persisted command = %+v, want a drain for b1", claimed)
	}
}
