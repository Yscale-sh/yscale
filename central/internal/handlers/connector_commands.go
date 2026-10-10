package handlers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

const (
	connectorCommandLease       = 30 * time.Second
	connectorCommandPoll        = 200 * time.Millisecond
	connectorCommandMaxAttempts = 12
	connectorCommandClaimBatch  = 32
	connectorCommandAuditEvery  = time.Minute
)

// ConnectorCommandLedger is the database-authoritative lifecycle-command
// outbox shared by workload producers and whichever replica owns the socket.
type ConnectorCommandLedger interface {
	PutConnectorCommands(context.Context, []state.ConnectorCommand) error
	ClaimConnectorCommand(context.Context, string, string, time.Duration, int) (*state.ConnectorCommand, bool, error)
	RecordConnectorCommandDelivered(context.Context, string, string) error
	MarkConnectorCommandAmbiguous(context.Context, string, string, string, time.Time, int) error
	AcknowledgeConnectorCommand(context.Context, string, string, protocol.CommandAck, time.Time, int) (state.ConnectorCommandAckResult, error)
	ListConnectorCommandsForWorkload(context.Context, string, string) ([]state.ConnectorCommandStatus, error)
	// ListConnectorCommandsNeedingAttention backs the tenant's read-only view
	// of stalled deliveries. The requeue that acts on what it shows is NOT on
	// this interface: it is an operator action, reached through the operator
	// surfaces, so a credential that only ever holds this ledger cannot drive
	// one (see tenants_connector_commands.go).
	ListConnectorCommandsNeedingAttention(context.Context, string, int) ([]state.ConnectorCommandStatus, error)
}

type ConnectorCommandAdmissionJournal interface {
	SubmitWorkloadWithConnectorCommands(context.Context, *state.Workload, *state.AuditEvent, []state.ConnectorCommand) error
}

// WithConnectorCommandLedger enables durable replay for lifecycle-critical
// commands. Keeping this optional preserves embedding/API compatibility; main
// wires the state store into both producer and socket paths.
func WithConnectorCommandLedger(ledger ConnectorCommandLedger) AgentStreamOption {
	return func(h *AgentStream) { h.Commands = ledger }
}

func connectorCommandRetryAt(attempts int) time.Time {
	if attempts < 1 {
		attempts = 1
	}
	delay := 250 * time.Millisecond
	for i := 1; i < attempts && delay < time.Minute; i++ {
		delay *= 2
	}
	if delay > time.Minute {
		delay = time.Minute
	}
	return time.Now().UTC().Add(delay)
}

func (h *AgentStream) dispatchConnectorCommands(ctx context.Context, agent *state.Agent, done <-chan struct{}) {
	if h.Commands == nil {
		return
	}
	ticker := time.NewTicker(connectorCommandPoll)
	defer ticker.Stop()
	for {
		for i := 0; i < connectorCommandClaimBatch; i++ {
			// A ready poll tick must not start another claim after shutdown.
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-agent.Evicted():
				return
			default:
			}
			cmd, ok, err := h.Commands.ClaimConnectorCommand(ctx, agent.CustomerID, agent.ClusterID,
				connectorCommandLease, connectorCommandMaxAttempts)
			if err != nil {
				h.Log.Warn("claim connector command", "customer", agent.CustomerID, "cluster", agent.ClusterID, "error", err)
				break
			}
			if !ok {
				break
			}
			agent.TrackConnectorCommandLease(cmd.ID, cmd.LeaseToken)
			if err := agent.Enqueue(cmd.Envelope); err != nil {
				if markErr := h.Commands.MarkConnectorCommandAmbiguous(ctx, cmd.ID, cmd.LeaseToken,
					state.ConnectorCommandReasonQueueUnavailable, connectorCommandRetryAt(cmd.Attempts), connectorCommandMaxAttempts); markErr != nil &&
					!errors.Is(markErr, state.ErrConnectorCommandLease) {
					// Revocation may cancel ctx after the claim commits. Keep
					// the lease so disconnect cleanup can retry independently.
					h.Log.Warn("record connector command queue ambiguity", "command", cmd.ID, "error", markErr)
				} else {
					agent.ForgetConnectorCommandLease(cmd.ID)
				}
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-agent.Evicted():
			return
		case <-ticker.C:
		}
	}
}

func (h *AgentStream) abandonConnectorCommands(agent *state.Agent, reason string) {
	if h.Commands == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), wsWriteTimeout)
	defer cancel()
	for id, token := range agent.DrainConnectorCommandLeases() {
		if err := h.Commands.MarkConnectorCommandAmbiguous(ctx, id, token,
			reason, connectorCommandRetryAt(1), connectorCommandMaxAttempts); err != nil &&
			!errors.Is(err, state.ErrConnectorCommandLease) {
			h.Log.Warn("record abandoned connector command", "command", id, "error", err)
		}
	}
}

func (h *AgentStream) auditRefusedCommandAck(agent *state.Agent, reason string) {
	if h.Store == nil || agent == nil {
		return
	}
	if !agent.AllowConnectorCommandAckAudit(time.Now().UTC(), connectorCommandAuditEvery) {
		return
	}
	if err := h.Store.AppendAudit(state.NewAuditEvent(state.AuditEvent{
		CustomerID: agent.CustomerID,
		Actor:      state.ClusterActor(agent.CustomerID),
		Action:     state.ActionConnectorCommandAck,
		Outcome:    state.OutcomeDenied,
		TargetKind: state.TargetCluster,
		TargetID:   agent.ClusterID,
		Detail:     state.AuditDetail{Reason: reason},
	})); err != nil {
		h.Log.Warn("audit refused connector command acknowledgement", "customer", agent.CustomerID, "cluster", agent.ClusterID, "error", err)
	}
}

func connectorCommandSemantic(kind protocol.MessageType, aggregateID string) string {
	return fmt.Sprintf("%s:%s:v1", kind, aggregateID)
}
