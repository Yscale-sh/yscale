//go:build integration

package handlers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

type streamCredentialFixture struct {
	a, b                                                *state.Store
	tenant, owner, cluster, tenantToken, connectorToken string
}

func newStreamCredentialFixture(t *testing.T) *streamCredentialFixture {
	t.Helper()
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	a, err := state.NewPostgres(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	suffix := fmt.Sprint(time.Now().UnixNano())
	f := &streamCredentialFixture{a: a, tenant: "cust_stream_" + suffix, cluster: "cl-stream-" + suffix, tenantToken: "synthetic-stream-" + suffix}
	owner, err := a.UpsertAccount("synthetic-stream-issuer", suffix, state.AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	f.owner = owner.ID
	if _, _, err := a.CreateTenant(&state.Customer{ID: f.tenant, Token: f.tenantToken}, f.owner); err != nil {
		t.Fatal(err)
	}
	_, f.connectorToken, _, err = a.RegisterTenantCluster(f.tenant, f.owner, f.cluster, "Synthetic stream", state.HumanActor(f.owner, f.tenant))
	if err != nil {
		t.Fatal(err)
	}
	f.b, err = state.NewPostgres(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.b.Close)
	return f
}

func (f *streamCredentialFixture) mutate(t *testing.T, fn func() error) {
	t.Helper()
	// B's successful Hello wrote connection metadata after A loaded. The first
	// guarded customer mutation can refresh A and require a caller retry.
	for attempt := 0; attempt < 3; attempt++ {
		err := fn()
		if err == nil {
			return
		}
		if !errors.Is(err, state.ErrPersistence) {
			t.Fatal(err)
		}
		if attempt == 2 {
			t.Fatalf("customer mutation after refresh: %v", err)
		}
	}
}

func waitCredentialStreamComplete(t *testing.T, completed <-chan struct{}) {
	t.Helper()
	select {
	case <-completed:
	case <-time.After(3 * time.Second):
		t.Fatal("stream pumps or command cleanup did not finish")
	}
}

func TestPostgresAgentStreamCredentialRevocation(t *testing.T) {
	for _, scenario := range []string{"tenant-outbound", "scoped-outbound", "scoped-inbound", "scoped-idle", "scoped-delete", "tenant-revoke", "tenant-finalize", "database-unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			f := newStreamCredentialFixture(t)
			token := f.connectorToken
			if scenario == "tenant-outbound" {
				token = f.tenantToken
			}
			poll := time.Hour // per-message checks cannot rely on the idle timer
			if scenario == "scoped-idle" || scenario == "tenant-revoke" || scenario == "tenant-finalize" || scenario == "database-unavailable" {
				poll = 20 * time.Millisecond
			}
			srv, completed := credentialStreamServerWithCompletion(t, f.b, poll)
			conn, err := dialAgent(t, srv, token, f.cluster)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			waitFor(t, "replica B stream registration", func() bool { _, err := f.b.AgentForCluster(f.tenant, f.cluster); return err == nil })
			agent, _ := f.b.AgentForCluster(f.tenant, f.cluster)
			wantCode := websocket.ClosePolicyViolation
			switch scenario {
			case "tenant-outbound":
				f.mutate(t, func() error { _, err := f.a.RotateCustomerCredential(f.tenant, state.OperatorActor()); return err })
			case "scoped-delete":
				f.mutate(t, func() error {
					_, err := f.a.DeleteTenantCluster(f.tenant, f.owner, f.cluster, state.HumanActor(f.owner, f.tenant))
					return err
				})
			case "tenant-revoke", "tenant-finalize":
				if err := f.a.RevokeCustomer(f.tenant); err != nil {
					t.Fatal(err)
				}
				if scenario == "tenant-finalize" {
					if err := f.a.DeleteRevokedCustomer(f.tenant); err != nil {
						t.Fatal(err)
					}
				}
			case "database-unavailable":
				f.b.Close()
				wantCode = websocket.CloseTryAgainLater
			default:
				f.mutate(t, func() error {
					_, _, _, err := f.a.RotateTenantClusterCredential(f.tenant, f.owner, f.cluster, state.HumanActor(f.owner, f.tenant))
					return err
				})
			}
			if scenario == "scoped-inbound" {
				if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeClusterRoutes, protocol.ClusterRoutes{Routes: []string{"10.85.0.0/24"}})); err != nil {
					t.Fatal(err)
				}
			} else if poll == time.Hour {
				if err := agent.Enqueue(mustEnvelope(t, protocol.TypeDrainNode, protocol.DrainNode{NodeName: "synthetic-never-executed-node"})); err != nil {
					t.Fatal(err)
				}
			}
			expectCredentialClose(t, conn, wantCode)
			waitCredentialStreamComplete(t, completed)
			if _, err := f.b.AgentForCluster(f.tenant, f.cluster); err == nil {
				t.Error("refused socket remains routable")
			}
			if scenario == "scoped-inbound" {
				current, err := f.a.AuthCustomer(f.tenantToken)
				if err != nil {
					t.Fatal(err)
				}
				if len(current.GatewayRoutes) != 0 {
					t.Error("revoked stream changed durable gateway routes")
				}
			}
		})
	}
}

func TestPostgresAgentStreamCredentialRotationDuringHello(t *testing.T) {
	f := newStreamCredentialFixture(t)
	srv, completed := credentialStreamServerWithCompletion(t, f.b, time.Hour)
	// dialAgent sends Hello immediately, so stage the HTTP upgrade separately.
	conn, _, err := websocket.DefaultDialer.Dial("ws"+srv.URL[len("http"):], map[string][]string{"Authorization": {"Bearer " + f.connectorToken}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	f.mutate(t, func() error {
		_, _, _, err := f.a.RotateTenantClusterCredential(f.tenant, f.owner, f.cluster, state.HumanActor(f.owner, f.tenant))
		return err
	})
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeHello, protocol.Hello{ClusterID: f.cluster})); err != nil {
		t.Fatal(err)
	}
	expectCredentialClose(t, conn, websocket.ClosePolicyViolation)
	waitCredentialStreamComplete(t, completed)
	if _, err := f.b.AgentForCluster(f.tenant, f.cluster); err == nil {
		t.Error("stale handshake registered a socket")
	}
}

type streamRevocationLedger struct {
	*state.Store
	claimed, release chan struct{}
	claimOnce        sync.Once
	delivered        atomic.Int32
	acknowledged     atomic.Int32
}

func (l *streamRevocationLedger) ClaimConnectorCommand(ctx context.Context, tenant, cluster string, lease time.Duration, maxAttempts int) (*state.ConnectorCommand, bool, error) {
	cmd, ok, err := l.Store.ClaimConnectorCommand(ctx, tenant, cluster, lease, maxAttempts)
	if err == nil && ok && l.claimed != nil {
		l.claimOnce.Do(func() {
			close(l.claimed)
			select {
			case <-l.release:
			case <-ctx.Done():
			}
		})
	}
	return cmd, ok, err
}

func (l *streamRevocationLedger) RecordConnectorCommandDelivered(ctx context.Context, id, lease string) error {
	l.delivered.Add(1)
	return l.Store.RecordConnectorCommandDelivered(ctx, id, lease)
}

func (l *streamRevocationLedger) AcknowledgeConnectorCommand(ctx context.Context, tenant, cluster string, ack protocol.CommandAck, retryAt time.Time, maxAttempts int) (state.ConnectorCommandAckResult, error) {
	l.acknowledged.Add(1)
	return l.Store.AcknowledgeConnectorCommand(ctx, tenant, cluster, ack, retryAt, maxAttempts)
}

func TestPostgresAgentStreamRevokedCommandRemainsReplayable(t *testing.T) {
	for _, boundary := range []string{"before-write", "before-ack", "during-claim"} {
		t.Run(boundary, func(t *testing.T) {
			f := newStreamCredentialFixture(t)
			ledger := &streamRevocationLedger{Store: f.b}
			var releaseOnce sync.Once
			if boundary != "before-ack" {
				ledger.claimed, ledger.release = make(chan struct{}), make(chan struct{})
				defer releaseOnce.Do(func() { close(ledger.release) })
			}
			poll := time.Hour
			if boundary == "during-claim" {
				poll = 20 * time.Millisecond
			}
			srv, completed := credentialStreamServerWithCompletion(t, f.b, poll, WithConnectorCommandLedger(ledger))
			conn, err := dialAgent(t, srv, f.connectorToken, f.cluster)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			waitFor(t, "stream registration", func() bool { _, err := f.b.AgentForCluster(f.tenant, f.cluster); return err == nil })
			env := mustEnvelope(t, protocol.TypeDrainNode, protocol.DrainNode{NodeName: "synthetic-never-executed-node"})
			env.ID = state.StableConnectorCommandID(f.tenant, f.cluster, "synthetic-drain-v1")
			cmd, err := state.NewConnectorCommand(f.tenant, f.cluster, "synthetic-drain-v1", "synthetic-burst", "synthetic-workload", "synthetic-burst", "", env)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.a.PutConnectorCommands(context.Background(), []state.ConnectorCommand{cmd}); err != nil {
				t.Fatal(err)
			}
			if boundary != "before-ack" {
				select {
				case <-ledger.claimed:
				case <-time.After(2 * time.Second):
					t.Fatal("command was not claimed")
				}
			} else {
				got := readCommand(t, conn)
				if !reflect.DeepEqual(got, cmd.Envelope) {
					t.Fatal("initial command envelope changed")
				}
			}
			var newToken string
			f.mutate(t, func() error {
				_, token, _, err := f.a.RotateTenantClusterCredential(f.tenant, f.owner, f.cluster, state.HumanActor(f.owner, f.tenant))
				if err == nil {
					newToken = token
				}
				return err
			})
			if boundary == "before-write" {
				releaseOnce.Do(func() { close(ledger.release) })
			} else if boundary == "before-ack" {
				sendCommandAck(t, conn, cmd.ID)
			}
			expectCredentialClose(t, conn, websocket.ClosePolicyViolation)
			waitCredentialStreamComplete(t, completed)
			if boundary != "before-ack" && ledger.delivered.Load() != 0 {
				t.Error("refused write recorded a delivery")
			}
			if ledger.acknowledged.Load() != 0 {
				t.Error("revoked socket reached acknowledgement persistence")
			}
			rows, err := f.b.ListConnectorCommandsForWorkload(context.Background(), f.tenant, "synthetic-workload")
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].State != state.ConnectorCommandFailed {
				t.Fatalf("refused command did not remain retryable: rows=%d err=%v", len(rows), err)
			}
			fresh, err := dialAgent(t, srv, newToken, f.cluster)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			got := readCommand(t, fresh)
			if !reflect.DeepEqual(got, cmd.Envelope) {
				t.Error("reconnect did not replay the identical command")
			}
			sendCommandAck(t, fresh, cmd.ID)
			waitFor(t, "fresh credential acknowledgement", func() bool {
				rows, err := f.b.ListConnectorCommandsForWorkload(context.Background(), f.tenant, "synthetic-workload")
				return err == nil && len(rows) == 1 && rows[0].State == state.ConnectorCommandAcknowledged
			})
			if ledger.acknowledged.Load() != 1 {
				t.Error("replay was not acknowledged exactly once by the fresh session")
			}
		})
	}
}
