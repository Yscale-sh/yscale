package state

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

func testConnectorCommand(t *testing.T, customerID, clusterID string, kind protocol.MessageType, aggregateID, workloadID, burstID, dependsOn string) ConnectorCommand {
	t.Helper()
	semantic := fmt.Sprintf("%s:%s:v1", kind, aggregateID)
	id := StableConnectorCommandID(customerID, clusterID, semantic)
	cmd, err := NewConnectorCommand(customerID, clusterID, semantic, "burst:"+burstID,
		workloadID, burstID, dependsOn, protocol.Envelope{
			APIVersion: protocol.APIVersion,
			Type:       kind,
			ID:         id,
			Timestamp:  time.Date(2026, 8, 20, 12, 0, 0, 123, time.UTC),
			Body:       []byte(fmt.Sprintf(`{"aggregate_id":%q}`, aggregateID)),
		})
	if err != nil {
		t.Fatalf("NewConnectorCommand: %v", err)
	}
	return cmd
}

func TestConnectorCommandsOrderReplayAndIdempotentAck(t *testing.T) {
	ctx := context.Background()
	store := New()
	announce := testConnectorCommand(t, "cust-a", "cluster-a", protocol.TypeBurstAnnounce, "burst-a", "wl-a", "burst-a", "")
	create := testConnectorCommand(t, "cust-a", "cluster-a", protocol.TypeCreateJob, "wl-a", "wl-a", "burst-a", announce.ID)
	if err := store.PutConnectorCommands(ctx, []ConnectorCommand{announce, create}); err != nil {
		t.Fatal(err)
	}

	first, ok, err := store.ClaimConnectorCommand(ctx, "cust-a", "cluster-a", time.Second, 4)
	if err != nil || !ok || first.ID != announce.ID {
		t.Fatalf("first claim = (%+v,%v,%v), want announce", first, ok, err)
	}
	if _, ok, err := store.ClaimConnectorCommand(ctx, "cust-a", "cluster-a", time.Second, 4); err != nil || ok {
		t.Fatalf("create claimed before announce ACK: ok=%v err=%v", ok, err)
	}
	if err := store.MarkConnectorCommandAmbiguous(ctx, first.ID, first.LeaseToken,
		ConnectorCommandReasonWriteAmbiguous, time.Now().Add(-time.Second), 4); err != nil {
		t.Fatal(err)
	}
	replay, ok, err := store.ClaimConnectorCommand(ctx, "cust-a", "cluster-a", time.Second, 4)
	if err != nil || !ok {
		t.Fatalf("replay claim = (%v,%v)", ok, err)
	}
	if replay.ID != first.ID || !reflect.DeepEqual(replay.Envelope, first.Envelope) {
		t.Fatalf("replay changed stable envelope: first=%+v replay=%+v", first.Envelope, replay.Envelope)
	}

	ack := protocol.CommandAck{CommandID: replay.ID, Success: true}
	result, err := store.AcknowledgeConnectorCommand(ctx, "cust-a", "cluster-a", ack, time.Now(), 4)
	if err != nil || result.Duplicate || result.State != ConnectorCommandAcknowledged {
		t.Fatalf("ack result = (%+v,%v)", result, err)
	}
	result, err = store.AcknowledgeConnectorCommand(ctx, "cust-a", "cluster-a", ack, time.Now(), 4)
	if err != nil || !result.Duplicate || result.State != ConnectorCommandAcknowledged {
		t.Fatalf("duplicate ack result = (%+v,%v)", result, err)
	}
	second, ok, err := store.ClaimConnectorCommand(ctx, "cust-a", "cluster-a", time.Second, 4)
	if err != nil || !ok || second.ID != create.ID {
		t.Fatalf("second claim = (%+v,%v,%v), want create", second, ok, err)
	}
}

func TestConnectorCommandScopeConflictAndDeadLetter(t *testing.T) {
	ctx := context.Background()
	store := New()
	cmd := testConnectorCommand(t, "cust-a", "cluster-a", protocol.TypeDrainNode, "burst-a", "wl-a", "burst-a", "")
	if err := store.PutConnectorCommands(ctx, []ConnectorCommand{cmd}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcknowledgeConnectorCommand(ctx, "cust-a", "cluster-b",
		protocol.CommandAck{CommandID: cmd.ID, Success: true}, time.Now(), 2); !errors.Is(err, ErrConnectorCommandScope) {
		t.Fatalf("cross-cluster ACK error = %v, want scope refusal", err)
	}
	changed := cmd
	changed.Envelope.Body = []byte(`{"changed":true}`)
	changed.PayloadDigest = ""
	if err := store.PutConnectorCommands(ctx, []ConnectorCommand{changed}); !errors.Is(err, ErrConnectorCommandConflict) {
		t.Fatalf("changed stable command error = %v, want conflict", err)
	}
	claimed, ok, err := store.ClaimConnectorCommand(ctx, "cust-a", "cluster-a", time.Second, 2)
	if err != nil || !ok {
		t.Fatalf("initial claim = (%v,%v)", ok, err)
	}
	if result, err := store.AcknowledgeConnectorCommand(ctx, "cust-a", "cluster-a",
		protocol.CommandAck{CommandID: claimed.ID, Success: false, Error: "temporary apply failure"},
		time.Now().Add(-time.Second), 2); err != nil || result.State != ConnectorCommandFailed {
		t.Fatalf("negative ACK = (%+v,%v), want retryable failure", result, err)
	}
	claimed, ok, err = store.ClaimConnectorCommand(ctx, "cust-a", "cluster-a", time.Second, 2)
	if err != nil || !ok {
		t.Fatalf("retry claim = (%v,%v)", ok, err)
	}
	if err := store.MarkConnectorCommandAmbiguous(ctx, claimed.ID, claimed.LeaseToken,
		ConnectorCommandReasonSessionEnded, time.Now().Add(-time.Second), 2); err != nil {
		t.Fatal(err)
	}
	statuses, err := store.ListConnectorCommandsForWorkload(ctx, "cust-a", "wl-a")
	if err != nil || len(statuses) != 1 || statuses[0].State != ConnectorCommandDeadLetter || statuses[0].Attempts != 2 {
		t.Fatalf("dead-letter status = (%+v,%v)", statuses, err)
	}
	late, err := store.AcknowledgeConnectorCommand(ctx, "cust-a", "cluster-a",
		protocol.CommandAck{CommandID: cmd.ID, Success: true}, time.Now(), 2)
	if err != nil || late.State != ConnectorCommandAcknowledged || late.Duplicate {
		t.Fatalf("late idempotency receipt did not reconcile dead-letter: (%+v,%v)", late, err)
	}
}

func TestConnectorCommandLeaseCompetitionAndQueuePressure(t *testing.T) {
	ctx := context.Background()
	store := New()
	const total = 160
	for start := 0; start < total; start += 20 {
		batch := make([]ConnectorCommand, 0, 20)
		for i := start; i < start+20; i++ {
			batch = append(batch, testConnectorCommand(t, "cust-q", "cluster-q", protocol.TypeDrainNode,
				fmt.Sprintf("burst-%03d", i), "wl-q", fmt.Sprintf("burst-%03d", i), ""))
		}
		if err := store.PutConnectorCommands(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	winners := make(chan *ConnectorCommand, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd, ok, err := store.ClaimConnectorCommand(ctx, "cust-q", "cluster-q", time.Second, 3)
			if err != nil {
				t.Errorf("competing claim: %v", err)
				return
			}
			if ok {
				winners <- cmd
			}
		}()
	}
	wg.Wait()
	close(winners)
	claimed := make(map[string]*ConnectorCommand)
	for cmd := range winners {
		if claimed[cmd.ID] != nil {
			t.Fatalf("two workers claimed %s", cmd.ID)
		}
		claimed[cmd.ID] = cmd
	}
	if len(claimed) != 2 {
		t.Fatalf("distinct lease winners = %d, want 2 distinct commands", len(claimed))
	}
	for _, cmd := range claimed {
		if _, err := store.AcknowledgeConnectorCommand(ctx, "cust-q", "cluster-q",
			protocol.CommandAck{CommandID: cmd.ID, Success: true}, time.Now(), 3); err != nil {
			t.Fatal(err)
		}
	}
	for len(claimed) < total {
		cmd, ok, err := store.ClaimConnectorCommand(ctx, "cust-q", "cluster-q", time.Second, 3)
		if err != nil || !ok {
			t.Fatalf("claim after %d = (%v,%v)", len(claimed), ok, err)
		}
		claimed[cmd.ID] = cmd
		if _, err := store.AcknowledgeConnectorCommand(ctx, "cust-q", "cluster-q",
			protocol.CommandAck{CommandID: cmd.ID, Success: true}, time.Now(), 3); err != nil {
			t.Fatal(err)
		}
	}
	statuses, err := store.ListConnectorCommandsForWorkload(ctx, "cust-q", "wl-q")
	if err != nil || len(statuses) != total {
		t.Fatalf("queue pressure statuses = %d, err=%v", len(statuses), err)
	}
	for _, status := range statuses {
		if status.State != ConnectorCommandAcknowledged {
			t.Fatalf("command %s was silently dropped in state %s", status.ID, status.State)
		}
	}
}

func TestConnectorCommandRejectsObservationTraffic(t *testing.T) {
	_, err := NewConnectorCommand("cust", "cluster", "heartbeat:x:v1", "cluster:x", "wl", "", "",
		protocol.Envelope{APIVersion: protocol.APIVersion, Type: protocol.TypeHeartbeat, ID: "cmd_x", Timestamp: time.Now(), Body: []byte(`{}`)})
	if !errors.Is(err, ErrInvalidConnectorCommand) {
		t.Fatalf("heartbeat command error = %v, want invalid", err)
	}
}
