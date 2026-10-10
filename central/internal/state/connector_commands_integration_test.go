//go:build integration

// The Postgres half of the connector-command ledger, behind the `integration`
// build tag so `go test ./...` never needs a database and CI can run it as one
// narrowly-targeted job against a throwaway PostgreSQL 16 service.
//
// What it proves cannot be proved in memory: that a claim survives the death of
// the process that made it, that the lease is arbitrated by the DATABASE rather
// than by a replica's map, and that the envelope two competing replicas race
// over is byte-identical to the one admission stored.

package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

func TestConnectorCommandPostgresRestartAndLeaseCompetition(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	s1, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	p1 := s1.persist.(*pgPersister)
	if _, err := p1.pool.Exec(ctx, `DELETE FROM connector_commands WHERE customer_id='cust-command-restart'`); err != nil {
		t.Fatal(err)
	}
	cmd := testConnectorCommand(t, "cust-command-restart", "cluster-a", protocol.TypeDrainNode,
		"burst-restart", "wl-restart", "burst-restart", "")
	if err := s1.PutConnectorCommands(ctx, []ConnectorCommand{cmd}); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := s1.ClaimConnectorCommand(ctx, cmd.CustomerID, cmd.ClusterID, time.Millisecond, 3)
	if err != nil || !ok {
		t.Fatalf("initial claim = (%v,%v)", ok, err)
	}
	wantEnvelope := claimed.Envelope
	s1.Close()
	time.Sleep(5 * time.Millisecond)

	s2, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	s3, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	type result struct {
		cmd *ConnectorCommand
		ok  bool
		err error
	}
	results := make(chan result, 2)
	for _, store := range []*Store{s2, s3} {
		go func(store *Store) {
			got, ok, err := store.ClaimConnectorCommand(ctx, cmd.CustomerID, cmd.ClusterID, time.Second, 3)
			results <- result{got, ok, err}
		}(store)
	}
	var winner *ConnectorCommand
	for i := 0; i < 2; i++ {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.ok {
			if winner != nil {
				t.Fatal("two Postgres replicas leased the same due command")
			}
			winner = got.cmd
		}
	}
	if winner == nil || winner.ID != cmd.ID || !reflect.DeepEqual(winner.Envelope, wantEnvelope) {
		t.Fatalf("restart winner = %+v, want exact envelope %+v", winner, wantEnvelope)
	}
}

// The operator recovery surface against real SQL: the attention list finds the
// dead letter, the requeue moves delivery state and journals the decision in
// the SAME transaction, and identity bounds are refused by the shared
// validation path rather than by a CHECK constraint.
func TestConnectorCommandPostgresOperatorRecovery(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	const customerID = "cust-command-recover"
	// Audit is append-only, even for an empty DELETE. Isolate the fixture
	// instead of attempting to erase a previous run's journal.
	pool := freshSchemaPool(t, dsn, fmt.Sprintf("connector_recovery_%d", time.Now().UnixNano()), 4)
	p := &pgPersister{pool: pool}
	if err := p.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	store := emptyStore()
	store.persist = p

	cmd := testConnectorCommand(t, customerID, "cluster-a", protocol.TypeDrainNode,
		"burst-recover", "wl-recover", "burst-recover", "")
	if err := store.PutConnectorCommands(ctx, []ConnectorCommand{cmd}); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimConnectorCommand(ctx, customerID, "cluster-a", time.Second, 1)
	if err != nil || !ok {
		t.Fatalf("claim = (%v,%v)", ok, err)
	}
	if err := store.MarkConnectorCommandAmbiguous(ctx, claimed.ID, claimed.LeaseToken,
		ConnectorCommandReasonSessionEnded, time.Now().Add(-time.Second), 1); err != nil {
		t.Fatal(err)
	}

	attention, err := store.ListConnectorCommandsNeedingAttention(ctx, customerID, 0)
	if err != nil || len(attention) != 1 || attention[0].State != ConnectorCommandDeadLetter ||
		attention[0].LastError != ConnectorCommandReasonSessionEnded {
		t.Fatalf("attention list = (%+v,%v)", attention, err)
	}
	// The delivery loop never takes it back on its own.
	if _, ok, err := store.ClaimConnectorCommand(ctx, customerID, "cluster-a", time.Second, 1); err != nil || ok {
		t.Fatalf("Postgres delivery loop auto-requeued a dead letter: ok=%v err=%v", ok, err)
	}

	requeued, err := store.RequeueConnectorCommand(ctx, customerID, cmd.ID, requeueAudit(customerID, cmd.ID))
	if err != nil || requeued.State != ConnectorCommandPending || requeued.Attempts != 0 {
		t.Fatalf("requeue = (%+v,%v)", requeued, err)
	}
	var journaled int
	if err := p.pool.QueryRow(ctx,
		`SELECT count(*) FROM tenant_audit WHERE customer_id=$1 AND data->>'Action'=$2`,
		customerID, ActionConnectorCommandRequeue).Scan(&journaled); err != nil {
		t.Fatal(err)
	}
	if journaled != 1 {
		t.Fatalf("requeue audit rows = %d, want exactly one written with the state change", journaled)
	}
	// The stable identity and the immutable envelope survived the requeue, and
	// the command is deliverable again.
	replay, ok, err := store.ClaimConnectorCommand(ctx, customerID, "cluster-a", time.Second, 3)
	if err != nil || !ok {
		t.Fatalf("requeued command was not claimable: ok=%v err=%v", ok, err)
	}
	if replay.ID != cmd.ID || !reflect.DeepEqual(replay.Envelope, claimed.Envelope) {
		t.Fatalf("requeue changed the stored envelope: %+v", replay.Envelope)
	}
	if _, err := store.RequeueConnectorCommand(ctx, customerID, cmd.ID, requeueAudit(customerID, cmd.ID)); !errors.Is(err, ErrConnectorCommandNotDeadLettered) {
		t.Fatalf("requeue of a live command = %v, want the not-a-dead-letter refusal", err)
	}
	if _, err := store.RequeueConnectorCommand(ctx, "cust-command-recover-other", cmd.ID,
		requeueAudit("cust-command-recover-other", cmd.ID)); !errors.Is(err, ErrConnectorCommandNotFound) {
		t.Fatalf("cross-tenant requeue = %v, want not found", err)
	}

	// Bounds fail in the shared path, so Postgres never sees the value and the
	// caller gets the same error the in-memory store gives.
	over := strings.Repeat("x", maxConnectorCommandIdent+1)
	oversized := cmd
	oversized.CustomerID = over
	if err := store.PutConnectorCommands(ctx, []ConnectorCommand{oversized}); !errors.Is(err, ErrInvalidConnectorCommand) {
		t.Fatalf("over-long customer id = %v, want ErrInvalidConnectorCommand rather than a CHECK violation", err)
	}
	if _, err := store.ListConnectorCommandsNeedingAttention(ctx, over, 10); !errors.Is(err, ErrInvalidConnectorCommand) {
		t.Fatalf("over-long customer id on the attention list = %v", err)
	}
}
