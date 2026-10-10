package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// leakedConnectorText stands in for whatever a connector puts in an ACK error:
// a kubectl dump quoting a Secret, a provider body quoting a credential.
const leakedConnectorText = "kubeconfig users[0].user.token=connector-authored-sensitive-value-9f2c"

// syncBuffer is a log sink a test can read while the stream's goroutines are
// still writing to it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func tenantRequest(t *testing.T, method, target, customerID string) *http.Request {
	t.Helper()
	return httptest.NewRequest(method, target, nil).
		WithContext(context.WithValue(context.Background(), ctxCustomer, &state.Customer{ID: customerID}))
}

// deadLetterCommand drives one command to dead_letter through the ordinary
// delivery transitions — no test-only back door into the state.
func deadLetterCommand(t *testing.T, store *state.Store, cmd state.ConnectorCommand) {
	t.Helper()
	ctx := context.Background()
	if err := store.PutConnectorCommands(ctx, []state.ConnectorCommand{cmd}); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimConnectorCommand(ctx, cmd.CustomerID, cmd.ClusterID, time.Second, 1)
	if err != nil || !ok {
		t.Fatalf("claim = (%v,%v)", ok, err)
	}
	if err := store.MarkConnectorCommandAmbiguous(ctx, claimed.ID, claimed.LeaseToken,
		state.ConnectorCommandReasonSessionEnded, time.Now().Add(-time.Second), 1); err != nil {
		t.Fatal(err)
	}
}

// The tenant's half of the recovery surface is a READ, and it shows the tenant
// that owns a dead letter and nobody else. There is deliberately no tenant
// requeue: on the OSS and connector path this credential is the cluster's own
// token, so re-driving delivery lives on the operator surfaces
// (tenants_connector_commands.go) behind an operator credential.
func TestTenantConnectorCommandViewIsReadOnlyAndTenantScoped(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_recover", Token: "t-recover"})
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "t-other"})
	stuck := connectorCommandFor(t, "cust_recover", "cluster-recover", protocol.TypeBurstAnnounce, "burst-stuck")
	deadLetterCommand(t, store, stuck)

	h := &Workloads{Store: store, Commands: store, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.ConnectorCommands(rec, tenantRequest(t, http.MethodGet, "/v1/connector-commands", "cust_recover"))
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200", rec.Code)
	}
	var listed struct {
		Count    int                            `json:"count"`
		Commands []state.ConnectorCommandStatus `json:"connector_commands"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if listed.Count != 1 || listed.Commands[0].ID != stuck.ID ||
		listed.Commands[0].State != state.ConnectorCommandDeadLetter {
		t.Fatalf("attention list = %+v, want the one dead letter", listed)
	}

	// Another tenant sees nothing, so it cannot even learn the id.
	rec = httptest.NewRecorder()
	h.ConnectorCommands(rec, tenantRequest(t, http.MethodGet, "/v1/connector-commands", "cust_other"))
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if listed.Count != 0 {
		t.Fatalf("another tenant saw %d connector commands", listed.Count)
	}

	// The read left the command exactly where it was: a tenant-credentialed
	// request never moves delivery state.
	if after := attentionList(t, store, "cust_recover"); len(after) != 1 ||
		after[0].State != state.ConnectorCommandDeadLetter {
		t.Fatalf("tenant read changed the ledger: %+v", after)
	}
}

// attentionList reads the ledger directly, so a test can prove a handler did
// or did not move a command without going back through that handler.
func attentionList(t *testing.T, store *state.Store, customerID string) []state.ConnectorCommandStatus {
	t.Helper()
	commands, err := store.ListConnectorCommandsNeedingAttention(context.Background(), customerID, 0)
	if err != nil {
		t.Fatal(err)
	}
	return commands
}

// End to end over the real socket: a connector fails a command with secret-
// shaped text in both Error and Result. None of it reaches the durable row, the
// tenant response or the log. (The requeue audit event this text could
// otherwise ride is scanned on the operator surface that builds it — see
// tenants_connector_commands_test.go.)
func TestConnectorFailureTextNeverReachesLogsOrTenantResponses(t *testing.T) {
	sink := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug}))

	store := state.New()
	cmd := durableTestCommand(t, protocol.TypeCreateJob, "wl-leak", "wl-leak", "burst-leak", "")
	if err := store.PutConnectorCommands(context.Background(), []state.ConnectorCommand{cmd}); err != nil {
		t.Fatal(err)
	}
	stream := NewAgentStream(store, logger, nil, WithConnectorCommandLedger(store))
	srv := httptest.NewServer(ConnectorAuth(store, stream))
	defer srv.Close()

	conn, err := dialAgent(t, srv, state.DefaultDevToken, "cluster-ledger")
	if err != nil {
		t.Fatal(err)
	}
	env := readCommand(t, conn)
	if err := conn.WriteJSON(mustEnvelope(t, protocol.TypeCommandAck, protocol.CommandAck{
		CommandID: env.ID,
		Success:   false,
		Error:     leakedConnectorText,
		Result:    json.RawMessage(`{"detail":"` + leakedConnectorText + `"}`),
	})); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the failing acknowledgement to be recorded", func() bool {
		for _, status := range commandStatuses(t, store, "wl-leak") {
			if status.ID == cmd.ID {
				return status.LastError == state.ConnectorCommandReasonConnectorRejected
			}
		}
		return false
	})
	conn.Close()

	h := &Workloads{Store: store, Commands: store, Log: logger}
	rec := httptest.NewRecorder()
	h.ConnectorCommands(rec, tenantRequest(t, http.MethodGet, "/v1/connector-commands", state.DevCustomerID))
	if rec.Code != http.StatusOK {
		t.Fatalf("attention list status = %d", rec.Code)
	}

	body, err := json.Marshal(commandStatuses(t, store, "wl-leak"))
	if err != nil {
		t.Fatal(err)
	}

	for name, blob := range map[string]string{
		"tenant attention response": rec.Body.String(),
		"workload command status":   string(body),
		"central log":               sink.String(),
	} {
		if strings.Contains(blob, "connector-authored-sensitive-value") ||
			strings.Contains(blob, "user.token") {
			t.Fatalf("%s carried the connector's own error text:\n%s", name, blob)
		}
	}
	// The reason code IS there — this is a redaction, not a silence.
	if !strings.Contains(rec.Body.String(), state.ConnectorCommandReasonConnectorRejected) {
		t.Fatalf("attention response lost the stable reason code: %s", rec.Body)
	}
	waitFor(t, "the failure log to carry the stable reason code", func() bool {
		return strings.Contains(sink.String(), state.ConnectorCommandReasonConnectorRejected)
	})
}

// connectorCommandFor builds a durable command for an arbitrary tenant/cluster,
// alongside durableTestCommand's dev-tenant fixture.
func connectorCommandFor(t *testing.T, customerID, clusterID string, kind protocol.MessageType, aggregateID string) state.ConnectorCommand {
	t.Helper()
	semantic := connectorCommandSemantic(kind, aggregateID)
	cmd, err := state.NewConnectorCommand(customerID, clusterID, semantic, "burst:"+aggregateID,
		"", aggregateID, "", protocol.Envelope{
			APIVersion: protocol.APIVersion, Type: kind,
			ID:        state.StableConnectorCommandID(customerID, clusterID, semantic),
			Timestamp: time.Date(2026, 8, 20, 13, 0, 0, 0, time.UTC),
			Body:      json.RawMessage(`{"aggregate":"` + aggregateID + `"}`),
		})
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}
