package state

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

// The ledger refuses the idle-teardown pair outright. Their ACK carries a
// cordon resourceVersion that belongs to ONE invocation, so a durable row under
// a stable semantic identity would replay a request nobody is waiting for and
// serve a later caller the earlier caller's result. Until a per-invocation
// request/result contract exists they stay on the synchronous, version-fenced
// path — and the refusal here is what stops someone adding them back by
// accident. See connectorLifecycleCommand.
func TestConnectorCommandRefusesInvocationSensitiveIdleTeardown(t *testing.T) {
	for _, kind := range []protocol.MessageType{protocol.TypePrepareIdleTeardown, protocol.TypeReleaseIdleTeardown} {
		_, err := NewConnectorCommand("cust-idle", "cluster-idle", string(kind)+":burst-idle:v1",
			"burst:burst-idle", "", "burst-idle", "", protocol.Envelope{
				APIVersion: protocol.APIVersion, Type: kind,
				ID:        StableConnectorCommandID("cust-idle", "cluster-idle", string(kind)+":burst-idle:v1"),
				Timestamp: time.Now().UTC(), Body: []byte(`{"node_name":"ys-burst-idle"}`),
			})
		if !errors.Is(err, ErrInvalidConnectorCommand) {
			t.Fatalf("%s durable command error = %v, want the ledger to refuse an invocation-sensitive command", kind, err)
		}
	}
	// The fire-and-forget lifecycle writes are still accepted, so this is a
	// boundary rather than a blanket refusal.
	for _, kind := range []protocol.MessageType{protocol.TypeBurstAnnounce, protocol.TypeCreateJob,
		protocol.TypeDeleteJob, protocol.TypeDrainNode} {
		if !connectorLifecycleCommand(kind) {
			t.Fatalf("%s is no longer an accepted durable lifecycle command", kind)
		}
	}
}

// No connector-authored byte reaches a durable row, a status response or the
// value a log line would print. Secrets, provider payloads and megabyte error
// dumps are all the same problem, and truncation does not solve any of them —
// only mapping onto a closed reason code does.
func TestConnectorCommandKeepsConnectorTextOutOfDurableState(t *testing.T) {
	const leaked = "kubeconfig users[0].user.token=connector-authored-sensitive-value-9f2c"
	ctx := context.Background()
	store := New()
	cmd := testConnectorCommand(t, "cust-secret", "cluster-secret", protocol.TypeDrainNode,
		"burst-secret", "wl-secret", "burst-secret", "")
	if err := store.PutConnectorCommands(ctx, []ConnectorCommand{cmd}); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimConnectorCommand(ctx, cmd.CustomerID, cmd.ClusterID, time.Second, 6)
	if err != nil || !ok {
		t.Fatalf("claim = (%v,%v)", ok, err)
	}

	// A failing ACK carrying the connector's own error AND a result body.
	result, err := store.AcknowledgeConnectorCommand(ctx, cmd.CustomerID, cmd.ClusterID, protocol.CommandAck{
		CommandID: claimed.ID,
		Success:   false,
		Error:     leaked,
		Result:    json.RawMessage(`{"cordon_resource_version":"` + leaked + `"}`),
	}, time.Now().Add(-time.Second), 6)
	if err != nil || result.State != ConnectorCommandFailed {
		t.Fatalf("negative ACK = (%+v,%v)", result, err)
	}
	stored := store.connectorCommands[cmd.ID]
	if stored.LastError != ConnectorCommandReasonConnectorRejected {
		t.Fatalf("stored last_error = %q, want the closed reason code", stored.LastError)
	}
	if stored.Ack == nil || stored.Ack.Error != ConnectorCommandReasonConnectorRejected || len(stored.Ack.Result) != 0 {
		t.Fatalf("stored acknowledgement kept connector-authored fields: %+v", stored.Ack)
	}

	// An ambiguity reason a caller invented is stored as "unspecified", not as
	// the caller's string.
	claimed, ok, err = store.ClaimConnectorCommand(ctx, cmd.CustomerID, cmd.ClusterID, time.Second, 6)
	if err != nil || !ok {
		t.Fatalf("retry claim = (%v,%v)", ok, err)
	}
	if err := store.MarkConnectorCommandAmbiguous(ctx, claimed.ID, claimed.LeaseToken,
		leaked, time.Now().Add(time.Minute), 6); err != nil {
		t.Fatal(err)
	}
	stored = store.connectorCommands[cmd.ID]
	if stored.LastError != ConnectorCommandReasonUnspecified || stored.LastAmbiguity != ConnectorCommandReasonUnspecified {
		t.Fatalf("ambiguity reason = (%q,%q), want the unspecified code", stored.LastError, stored.LastAmbiguity)
	}

	// Whole-record and whole-response sweeps, so a field added later that
	// forwards connector text fails this test rather than shipping.
	row, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	statuses, err := store.ListConnectorCommandsForWorkload(ctx, cmd.CustomerID, "wl-secret")
	if err != nil {
		t.Fatal(err)
	}
	response, err := json.Marshal(statuses)
	if err != nil {
		t.Fatal(err)
	}
	attention, err := store.ListConnectorCommandsNeedingAttention(ctx, cmd.CustomerID, 0)
	if err != nil {
		t.Fatal(err)
	}
	operatorView, err := json.Marshal(attention)
	if err != nil {
		t.Fatal(err)
	}
	for name, blob := range map[string][]byte{
		"durable record":  row,
		"workload status": response,
		"attention list":  operatorView,
	} {
		if strings.Contains(string(blob), "connector-authored-sensitive-value") ||
			strings.Contains(string(blob), "user.token") {
			t.Fatalf("%s carried connector-authored text: %s", name, blob)
		}
	}
}

// Identity bounds are enforced in the SHARED validation path, so an over-long
// or blank identity is the same refusal whichever persister is underneath. In
// memory it used to be accepted silently; against Postgres it was a CHECK
// violation nobody could map back to a caller.
func TestConnectorCommandIdentityBoundsFailBeforeThePersister(t *testing.T) {
	ctx := context.Background()
	over := strings.Repeat("x", maxConnectorCommandIdent+1)
	invalidUTF8 := string([]byte{0xff})
	valid := testConnectorCommand(t, "cust-bounds", "cluster-bounds", protocol.TypeDrainNode,
		"burst-bounds", "wl-bounds", "burst-bounds", "")

	for name, mutate := range map[string]func(c *ConnectorCommand){
		"customer over bound":    func(c *ConnectorCommand) { c.CustomerID = over },
		"cluster over bound":     func(c *ConnectorCommand) { c.ClusterID = over },
		"semantic over bound":    func(c *ConnectorCommand) { c.SemanticKey = over },
		"ordering over bound":    func(c *ConnectorCommand) { c.OrderingKey = over },
		"workload over bound":    func(c *ConnectorCommand) { c.WorkloadID = over },
		"burst over bound":       func(c *ConnectorCommand) { c.BurstID = over },
		"depends over bound":     func(c *ConnectorCommand) { c.DependsOn = over },
		"blank-space customer":   func(c *ConnectorCommand) { c.CustomerID = "   " },
		"invalid UTF-8 customer": func(c *ConnectorCommand) { c.CustomerID = invalidUTF8 },
		"NUL in cluster":         func(c *ConnectorCommand) { c.ClusterID = "cluster\x00bounds" },
		"invalid UTF-8 workload": func(c *ConnectorCommand) { c.WorkloadID = invalidUTF8 },
		"NUL in burst":           func(c *ConnectorCommand) { c.BurstID = "burst\x00bounds" },
		"payload version over":   func(c *ConnectorCommand) { c.PayloadVersion = maxConnectorCommandPayloadVersion + 1 },
	} {
		candidate := valid
		candidate.Envelope.Body = append(json.RawMessage(nil), valid.Envelope.Body...)
		mutate(&candidate)
		if err := normalizeConnectorCommand(&candidate); !errors.Is(err, ErrInvalidConnectorCommand) {
			t.Fatalf("%s error = %v, want ErrInvalidConnectorCommand", name, err)
		}
	}

	store := New()
	if _, _, err := store.ClaimConnectorCommand(ctx, over, "cluster-bounds", time.Second, 4); !errors.Is(err, ErrInvalidConnectorCommand) {
		t.Fatalf("claim with over-long customer = %v", err)
	}
	if err := store.RecordConnectorCommandDelivered(ctx, valid.ID, over); !errors.Is(err, ErrInvalidConnectorCommand) {
		t.Fatalf("delivery with over-long lease token = %v", err)
	}
	if _, err := store.AcknowledgeConnectorCommand(ctx, "cust-bounds", "cluster-bounds",
		protocol.CommandAck{CommandID: over, Success: true}, time.Now(), 4); !errors.Is(err, ErrInvalidConnectorCommand) {
		t.Fatalf("ACK with over-long command id = %v", err)
	}
	if _, err := store.ListConnectorCommandsNeedingAttention(ctx, over, 10); !errors.Is(err, ErrInvalidConnectorCommand) {
		t.Fatalf("attention list with over-long customer = %v", err)
	}
}

// Dead letters are visible, requeued only when a human says so, and never
// requeued by the delivery loop.
func TestConnectorCommandDeadLetterIsVisibleAndRequeuedOnlyExplicitly(t *testing.T) {
	ctx := context.Background()
	store := New()
	cmd := testConnectorCommand(t, "cust-dead", "cluster-dead", protocol.TypeDrainNode,
		"burst-dead", "wl-dead", "burst-dead", "")
	if err := store.PutConnectorCommands(ctx, []ConnectorCommand{cmd}); err != nil {
		t.Fatal(err)
	}
	// Burn the single permitted attempt so the next transition dead-letters.
	claimed, ok, err := store.ClaimConnectorCommand(ctx, cmd.CustomerID, cmd.ClusterID, time.Second, 1)
	if err != nil || !ok {
		t.Fatalf("claim = (%v,%v)", ok, err)
	}
	if err := store.MarkConnectorCommandAmbiguous(ctx, claimed.ID, claimed.LeaseToken,
		ConnectorCommandReasonSessionEnded, time.Now().Add(-time.Second), 1); err != nil {
		t.Fatal(err)
	}

	attention, err := store.ListConnectorCommandsNeedingAttention(ctx, cmd.CustomerID, 0)
	if err != nil || len(attention) != 1 || attention[0].State != ConnectorCommandDeadLetter {
		t.Fatalf("attention list = (%+v,%v), want one dead letter", attention, err)
	}
	if attention[0].LastError != ConnectorCommandReasonSessionEnded {
		t.Fatalf("dead-letter reason = %q, want the closed session code", attention[0].LastError)
	}

	// The delivery loop never takes a dead letter back on its own.
	for i := 0; i < 3; i++ {
		if _, ok, err := store.ClaimConnectorCommand(ctx, cmd.CustomerID, cmd.ClusterID, time.Second, 1); err != nil || ok {
			t.Fatalf("delivery loop auto-requeued a dead letter: ok=%v err=%v", ok, err)
		}
	}

	// Another tenant cannot see it or requeue it, and gets the same answer as
	// for an id that does not exist.
	if _, err := store.RequeueConnectorCommand(ctx, "cust-other", cmd.ID, requeueAudit("cust-other", cmd.ID)); !errors.Is(err, ErrConnectorCommandNotFound) {
		t.Fatalf("cross-tenant requeue = %v, want not found", err)
	}

	requeued, err := store.RequeueConnectorCommand(ctx, cmd.CustomerID, cmd.ID, requeueAudit(cmd.CustomerID, cmd.ID))
	if err != nil || requeued.State != ConnectorCommandPending || requeued.Attempts != 0 {
		t.Fatalf("requeue = (%+v,%v), want a pending command with attempts reset", requeued, err)
	}
	// Identity and the immutable envelope are untouched.
	stored := store.connectorCommands[cmd.ID]
	if stored.ID != cmd.ID || stored.SemanticKey != cmd.SemanticKey || stored.OrderingKey != cmd.OrderingKey ||
		stored.PayloadDigest != cmd.PayloadDigest || string(stored.Envelope.Body) != string(cmd.Envelope.Body) {
		t.Fatalf("requeue mutated stable identity or envelope: %+v", stored)
	}
	// The reason it dead-lettered survives the requeue: an operator asking for
	// it to run again still needs to see why it stopped.
	if stored.LastError != ConnectorCommandReasonSessionEnded {
		t.Fatalf("requeue erased the dead-letter reason: %q", stored.LastError)
	}

	// It is deliverable again, and requeuing a live command is refused.
	if _, ok, err := store.ClaimConnectorCommand(ctx, cmd.CustomerID, cmd.ClusterID, time.Second, 1); err != nil || !ok {
		t.Fatalf("requeued command was not claimable: ok=%v err=%v", ok, err)
	}
	if _, err := store.RequeueConnectorCommand(ctx, cmd.CustomerID, cmd.ID, requeueAudit(cmd.CustomerID, cmd.ID)); !errors.Is(err, ErrConnectorCommandNotDeadLettered) {
		t.Fatalf("requeue of a live command = %v, want the not-a-dead-letter refusal", err)
	}
}

func requeueAudit(customerID, commandID string) *AuditEvent {
	return NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      ClusterActor(customerID),
		Action:     ActionConnectorCommandRequeue,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetConnectorCommand,
		TargetID:   commandID,
		Detail:     AuditDetail{Reason: ReasonConnectorCommandRequeued},
	})
}
