package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/broker"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// With a broker configured, reapBurst hands teardown to the durable queue
// instead of tearing down inline: the record is claimed (gone), a job is
// enqueued carrying the burst, and the Reaper is NOT called synchronously.
func TestReapBurst_EnqueuesWhenBrokerPresent(t *testing.T) {
	ctx := context.Background()
	brk := broker.NewMemory(time.Hour)
	reaper := &fakeReaper{}
	h := &Workloads{Store: state.New(), Reaper: reaper, Log: quietLog(), Teardowns: brk}
	h.Store.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_test", Backend: "linode", BackendID: "n1", CreatedAt: time.Now()})

	if !h.reapBurst(ctx, "b1", "complete").Reaped() {
		t.Fatal("reapBurst should succeed (job enqueued)")
	}
	if _, err := h.Store.GetBurst("b1"); err == nil {
		t.Error("burst record should be claimed/removed once enqueued")
	}
	if reaper.teardownCount("b1") != 0 {
		t.Error("teardown must NOT run synchronously when a broker is present")
	}

	msg, err := brk.Consume(ctx, teardownStream, teardownGroup, "test")
	if err != nil || msg == nil {
		t.Fatalf("expected a teardown job on the queue, got msg=%v err=%v", msg, err)
	}
	var job teardownJob
	if err := json.Unmarshal(msg.Payload, &job); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if job.Burst.ID != "b1" || job.Burst.BackendID != "n1" || job.Reason != "complete" {
		t.Errorf("job carries wrong burst data: %+v", job)
	}
}

// The worker tears down a job and acks it on success (no redelivery).
func TestTeardownWorker_TearsDownAndAcks(t *testing.T) {
	ctx := context.Background()
	brk := broker.NewMemory(0) // 0 = an un-acked message redelivers immediately
	store := state.New()
	reaper := &fakeReaper{}
	w := &TeardownWorker{Broker: brk, Reaper: reaper, Store: store, Log: quietLog(), Consumer: "w1"}

	enqueueJob(t, brk, &state.Burst{ID: "b1", CustomerID: state.DevCustomerID, Backend: "linode", BackendID: "n1"}, "complete")

	msg := mustBrokerConsume(t, brk, "w1")
	w.process(ctx, msg)

	if !reaper.reaped("b1") {
		t.Error("worker should have torn the burst down")
	}
	// Acked → nothing left to redeliver.
	if m, _ := brk.Consume(ctx, teardownStream, teardownGroup, "w1"); m != nil {
		t.Errorf("successfully-torn-down job should be acked, got redelivery %v", m)
	}
}

type trimmingTestBroker struct {
	*broker.Memory
	trimCalls int
	trimErr   error
}

func (b *trimmingTestBroker) TrimAckedHistory(context.Context, string) error {
	b.trimCalls++
	return b.trimErr
}

func TestTeardownWorkerCompactionFailureDoesNotUndoAck(t *testing.T) {
	ctx := context.Background()
	brk := &trimmingTestBroker{
		Memory:  broker.NewMemory(0),
		trimErr: errors.New("redis trim unavailable"),
	}
	if err := brk.Publish(ctx, teardownStream, []byte("job")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	msg, err := brk.Consume(ctx, teardownStream, teardownGroup, "worker")
	if err != nil || msg == nil {
		t.Fatalf("consume = %v, %v", msg, err)
	}
	w := &TeardownWorker{Broker: brk, Log: quietLog()}
	if err := w.ack(ctx, msg.ID); err != nil {
		t.Fatalf("ack reported best-effort trim failure: %v", err)
	}
	if brk.trimCalls != 1 {
		t.Fatalf("trim calls = %d, want 1", brk.trimCalls)
	}
	if got, _ := brk.Consume(ctx, teardownStream, teardownGroup, "worker"); got != nil {
		t.Fatalf("acked message redelivered after trim failure: %+v", got)
	}
}

func tenantAccountLease(t *testing.T, store *state.Store, burstID string) *state.Burst {
	t.Helper()
	account := state.CloudAccount{ID: "ca_lease", Provider: state.CloudProviderLinode, ProviderIdentity: "provider-uuid", Region: "us-ord", CredentialCiphertext: "v1.ciphertext", UpdatedAt: time.Now().UTC()}
	if _, _, err := store.SetLinodeCloudAccount(state.DevCustomerID, account, state.OperatorActor()); err != nil {
		t.Fatal(err)
	}
	lease := state.CloudAccountLease{BurstID: burstID, CustomerID: state.DevCustomerID, CloudAccountID: account.ID, ExpiresAt: time.Now().UTC().Add(time.Hour)}
	if won, err := store.AcquireCloudAccountLease(t.Context(), lease); err != nil || !won {
		t.Fatalf("acquire lease = %v, %v", won, err)
	}
	return &state.Burst{ID: burstID, CustomerID: state.DevCustomerID, Backend: "linode", BackendID: "node-1", CloudAccountID: account.ID}
}

func TestTeardownWorkerReleasesTenantCloudAccountLeaseAfterProviderDelete(t *testing.T) {
	brk := broker.NewMemory(0)
	store := state.New()
	burst := tenantAccountLease(t, store, "burst_byoc_worker")
	reaper := &fakeReaper{}
	w := &TeardownWorker{Broker: brk, Reaper: reaper, Store: store, Log: quietLog(), Consumer: "w1"}
	enqueueJob(t, brk, burst, "persist compensation")
	w.process(t.Context(), mustBrokerConsume(t, brk, "w1"))
	if !reaper.reaped(burst.ID) {
		t.Fatal("provider teardown did not run")
	}
	if err := store.DisconnectLinodeCloudAccount(state.DevCustomerID, burst.CloudAccountID, time.Now().UTC(), state.OperatorActor()); err != nil {
		t.Fatalf("disconnect after worker teardown = %v", err)
	}
	if msg, _ := brk.Consume(t.Context(), teardownStream, teardownGroup, "w1"); msg != nil {
		t.Fatalf("job redelivered after lease release: %+v", msg)
	}
}

func TestInlineProvisioningCompensationReleasesTenantCloudAccountLease(t *testing.T) {
	store := state.New()
	burst := tenantAccountLease(t, store, "burst_byoc_inline")
	h := &Workloads{Store: store, Reaper: &fakeReaper{}, Log: quietLog()}
	if !h.compensateProvisionedBurst(t.Context(), burst, "burst persist failed") {
		t.Fatal("inline compensation failed")
	}
	if err := store.DisconnectLinodeCloudAccount(state.DevCustomerID, burst.CloudAccountID, time.Now().UTC(), state.OperatorActor()); err != nil {
		t.Fatalf("disconnect after inline teardown = %v", err)
	}
}

type flakyCostRecorder struct {
	calls int
	err   error
}

func (r *flakyCostRecorder) EnsureWorkloadCostForBurst(context.Context, state.WorkloadCost) (bool, error) {
	r.calls++
	if r.calls == 1 {
		return false, r.err
	}
	return true, nil
}

// A successfully deleted provider is not enough to retire the queue item: the
// workload receipt is what lets central answer a retried Removed event after
// its first acknowledgement was lost. A transient receipt failure therefore
// redelivers the job until the exact frozen observation is durable.
func TestTeardownWorker_RetriesUntilCostReceiptIsDurable(t *testing.T) {
	ctx := context.Background()
	brk := broker.NewMemory(0)
	recorder := &flakyCostRecorder{err: errors.New("postgres unavailable")}
	w := &TeardownWorker{
		Broker: brk, Reaper: &fakeReaper{}, Store: state.New(), CostRecorder: recorder,
		Log: quietLog(), Consumer: "w1",
	}
	observed := state.WorkloadCost{
		EstimatedUSD: 0.01,
		HourlyUSD:    0.10,
		Runtime:      6 * time.Minute,
		FrozenAt:     time.Now().UTC(),
		Backend:      "linode",
		BurstID:      "b1",
		Basis:        state.WorkloadCostBasisRateRuntime,
	}
	payload, err := json.Marshal(teardownJob{
		Burst:  state.Burst{ID: "b1", CustomerID: state.DevCustomerID, Backend: "linode", BackendID: "n1"},
		Reason: "complete",
		Cost:   &observed,
	})
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	if err := brk.Publish(ctx, teardownStream, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	w.process(ctx, mustBrokerConsume(t, brk, "w1"))
	redelivery := mustBrokerConsume(t, brk, "w1")
	w.process(ctx, redelivery)
	if recorder.calls != 2 {
		t.Fatalf("receipt writes = %d, want retry then success", recorder.calls)
	}
	if next, _ := brk.Consume(ctx, teardownStream, teardownGroup, "w1"); next != nil {
		t.Fatalf("job redelivered after receipt became durable: %+v", next)
	}
}

func TestTeardownWorker_WaitsForDrainAckBeforeAckingBroker(t *testing.T) {
	ctx := context.Background()
	brk := broker.NewMemory(0)
	store := state.New()
	agent := &state.Agent{
		ID: "agent-1", CustomerID: state.DevCustomerID, ClusterID: "cluster-1",
		Send: make(chan protocol.Envelope, 1),
	}
	store.AddAgent(agent)
	w := &TeardownWorker{Broker: brk, Reaper: &fakeReaper{}, Store: store, Log: quietLog(), Consumer: "w1"}
	enqueueJob(t, brk, &state.Burst{
		ID: "b1", CustomerID: state.DevCustomerID, AgentID: agent.ID,
		Backend: "flyio", BackendID: "m1", NodeName: "ys-burst-b1",
	}, "complete")
	msg := mustBrokerConsume(t, brk, "w1")

	done := make(chan struct{})
	go func() {
		w.process(ctx, msg)
		close(done)
	}()
	env := <-agent.Send
	if env.Type != protocol.TypeDrainNode || env.ID == "" {
		t.Fatalf("unexpected drain envelope: %+v", env)
	}
	select {
	case <-done:
		t.Fatal("worker acked before the agent acknowledgement")
	default:
	}
	if !agent.DeliverCommandAck(protocol.CommandAck{CommandID: env.ID, Success: true}) {
		t.Fatal("drain acknowledgement had no waiter")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not finish after successful drain acknowledgement")
	}
	if next, _ := brk.Consume(ctx, teardownStream, teardownGroup, "w1"); next != nil {
		t.Fatalf("acknowledged teardown should not redeliver: %+v", next)
	}
}

func TestTeardownWorker_RetriesWhenNoAgentCanAcknowledgeDrain(t *testing.T) {
	ctx := context.Background()
	brk := broker.NewMemory(0)
	store := state.New()
	w := &TeardownWorker{Broker: brk, Reaper: &fakeReaper{}, Store: store, Log: quietLog(), Consumer: "w1"}
	enqueueJob(t, brk, &state.Burst{
		ID: "b1", CustomerID: state.DevCustomerID,
		Backend: "flyio", BackendID: "m1", NodeName: "ys-burst-b1",
	}, "complete")

	w.process(ctx, mustBrokerConsume(t, brk, "w1"))
	if recorded, err := store.BurstReapRecorded(ctx, "b1", state.DevCustomerID); err != nil || !recorded {
		t.Fatalf("provider teardown receipt before drain retry = %v err:%v, want true/nil", recorded, err)
	}
	if next, _ := brk.Consume(ctx, teardownStream, teardownGroup, "w1"); next == nil {
		t.Fatal("unacknowledged node cleanup must remain pending for retry")
	}
}

// A transient failure leaves the job un-acked so it is redelivered; after
// teardownMaxAttempts the worker dead-letters it (acks) so it stops cycling.
func TestTeardownWorker_RetriesThenDeadLetters(t *testing.T) {
	ctx := context.Background()
	brk := broker.NewMemory(0)
	store := state.New()
	reaper := &fakeReaper{err: errors.New("backend unreachable")}
	w := &TeardownWorker{Broker: brk, Reaper: reaper, Store: store, Log: quietLog(), Consumer: "w1"}

	enqueueJob(t, brk, &state.Burst{ID: "b1", Backend: "linode", BackendID: "n1"}, "complete")

	// Drive delivery/redelivery until the job stops coming back.
	deliveries := 0
	for deliveries < teardownMaxAttempts+3 {
		msg, _ := brk.Consume(ctx, teardownStream, teardownGroup, "w1")
		if msg == nil {
			break // dead-lettered (acked) — no longer redelivered
		}
		deliveries++
		w.process(ctx, msg)
	}
	if reaper.teardownCount("b1") != teardownMaxAttempts {
		t.Errorf("teardown attempted %d times, want exactly %d before giving up", reaper.teardownCount("b1"), teardownMaxAttempts)
	}
	if m, _ := brk.Consume(ctx, teardownStream, teardownGroup, "w1"); m != nil {
		t.Error("dead-lettered job should not be redelivered")
	}
}

func enqueueJob(t *testing.T, brk broker.Broker, b *state.Burst, reason string) {
	t.Helper()
	payload, err := json.Marshal(teardownJob{Burst: *b, Reason: reason})
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	if err := brk.Publish(context.Background(), teardownStream, payload); err != nil {
		t.Fatalf("publish job: %v", err)
	}
}

func mustBrokerConsume(t *testing.T, brk broker.Broker, consumer string) *broker.Message {
	t.Helper()
	m, err := brk.Consume(context.Background(), teardownStream, teardownGroup, consumer)
	if err != nil || m == nil {
		t.Fatalf("expected a message, got m=%v err=%v", m, err)
	}
	return m
}

// TestReapReleasesPodSlotOnlyAfterTeardown pins the release ORDERING that
// keeps the pod-CIDR pool safe. A burst advertises its /24 as a mesh subnet
// route for as long as its node is up, so the slot must stay reserved until the
// provider confirms the node destroyed — releasing at claim time would let a
// new burst take a prefix the old one is still announcing, and the mesh would
// have two peers claiming it.
//
// Both reap paths are covered: the broker path defers the release to the
// worker (the node is still up when enqueue returns), and the inline path
// releases only after Reaper.Teardown succeeds.
//
// Needs a real database — a released slot is only observable through the
// durable table, and state's persister seam is unexported so no fake can stand
// in from this package. Gated on YSCALE_TEST_DATABASE_URL like
// TestSweepExpiredBursts_Durable, so ordinary CI does NOT cover it.
func TestReapReleasesPodSlotOnlyAfterTeardown(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	store, err := state.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	// Slots outside the allocator's 10..250 pool so a shared test database is
	// undisturbed, and a TTL long enough that nothing ages out mid-test.
	const ttl = time.Hour
	held := func(t *testing.T, slot int) bool {
		t.Helper()
		slots, ok, err := store.ReservedPodSlots(ctx, ttl)
		if err != nil || !ok {
			t.Fatalf("ReservedPodSlots: ok=%v err=%v", ok, err)
		}
		return slots[slot]
	}
	newBurst := func(t *testing.T, id string, slot int) *state.Burst {
		t.Helper()
		if won, _, err := store.ReservePodSlot(ctx, slot, id, ttl); err != nil || !won {
			t.Fatalf("reserve slot %d: won=%v err=%v", slot, won, err)
		}
		b := &state.Burst{
			ID: id, CustomerID: "cust_podslot_reap", Backend: "linode", BackendID: "vm-" + id,
			PodCIDR: "10.244.99.0/24", CreatedAt: time.Now().UTC(), NodeOnly: true,
		}
		if err := store.PutBurst(b); err != nil {
			t.Fatalf("PutBurst: %v", err)
		}
		t.Cleanup(func() {
			_ = store.ReleasePodSlot(ctx, id)
			_ = store.DeleteBurst(id)
		})
		return b
	}

	t.Run("broker path defers the release to the worker", func(t *testing.T) {
		const slot = 9101
		b := newBurst(t, "burst_podslot_queued", slot)
		brk := broker.NewMemory(0)
		h := &Workloads{Store: store, Reaper: &fakeReaper{}, Log: quietLog(), Teardowns: brk}

		if !h.reapBurst(ctx, b.ID, "complete").Reaped() {
			t.Fatal("reapBurst should succeed (job enqueued)")
		}
		if !held(t, slot) {
			t.Fatal("slot released at enqueue time; the node is still up and still advertising " +
				"its /24, so a new burst could be handed the same prefix")
		}

		// A teardown that fails leaves the node possibly alive: still no release.
		failing := &TeardownWorker{Broker: brk, Reaper: &fakeReaper{err: errors.New("backend unreachable")},
			Store: store, Log: quietLog(), Consumer: "w1"}
		failing.process(ctx, mustBrokerConsume(t, brk, "w1"))
		if !held(t, slot) {
			t.Error("slot released although the provider teardown failed")
		}

		w := &TeardownWorker{Broker: brk, Reaper: &fakeReaper{}, Store: store, Log: quietLog(), Consumer: "w1"}
		w.process(ctx, mustBrokerConsume(t, brk, "w1"))
		if held(t, slot) {
			t.Error("slot still reserved after the node was destroyed; the pool leaks until the TTL")
		}
	})

	t.Run("inline path releases only after teardown succeeds", func(t *testing.T) {
		const slot = 9102
		b := newBurst(t, "burst_podslot_inline", slot)
		failing := &Workloads{Store: store, Reaper: &fakeReaper{err: errors.New("backend unreachable")}, Log: quietLog()}

		if got := failing.reapBurst(ctx, b.ID, "complete"); got != ReapOutcomeRequeued {
			t.Fatalf("reapBurst = %v, want ReapOutcomeRequeued when teardown fails", got)
		}
		if !held(t, slot) {
			t.Fatal("slot released although teardown failed and the burst was re-queued")
		}

		h := &Workloads{Store: store, Reaper: &fakeReaper{}, Log: quietLog()}
		if !h.reapBurst(ctx, b.ID, "retry").Reaped() {
			t.Fatal("reapBurst should succeed once teardown succeeds")
		}
		if held(t, slot) {
			t.Error("slot still reserved after the node was destroyed; the pool leaks until the TTL")
		}
	})
}
