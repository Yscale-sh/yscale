package state

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

const (
	ConnectorCommandPending      = "pending"
	ConnectorCommandProcessing   = "processing"
	ConnectorCommandFailed       = "failed"
	ConnectorCommandAcknowledged = "acknowledged"
	ConnectorCommandDeadLetter   = "dead_letter"

	maxConnectorCommandPayload = 1 << 20

	// maxConnectorCommandIdent mirrors the connector_commands identity CHECK
	// constraints (length(btrim(x)) BETWEEN 1 AND 255, octet_length(x) <= 255 on
	// the optional aggregate columns). Enforcing it in the shared validation
	// path is what makes an over-long or blank identity the SAME
	// ErrInvalidConnectorCommand on both persisters, instead of an accepted
	// in-memory row and a Postgres 23514 nobody can map back to a caller.
	maxConnectorCommandIdent = 255
	// maxConnectorCommandPayloadVersion is the SMALLINT ceiling of
	// connector_commands.payload_version.
	maxConnectorCommandPayloadVersion = 32767
	// maxConnectorCommandAttentionPage bounds the operator attention list. A
	// tenant whose connector has been down for a day can have thousands of
	// stalled commands, and an unbounded list is a response nobody reads and a
	// scan nobody budgeted for.
	maxConnectorCommandAttentionPage = 200
)

// Connector-command reason codes: the CLOSED vocabulary that may be persisted
// in last_error/last_ambiguity, returned on a tenant API, or printed in a log
// line.
//
// The reason it is closed is the reason the audit journal's is: a connector's
// ACK carries a free-text Error minted inside the customer's cluster. It can be
// a kubectl apply dump with a Secret in it, a provider error quoting a
// credential, or simply megabytes. Truncating that was never enough — a bounded
// secret is still a secret. So no connector-authored byte reaches a durable
// row: every failure is MAPPED onto one of these before it is stored.
const (
	// ConnectorCommandReasonConnectorRejected: the connector acknowledged the
	// command and said it failed. Its own message stays in the connector's logs,
	// inside the tenant's cluster, where it is already readable by the operator
	// who can act on it.
	ConnectorCommandReasonConnectorRejected = "connector_rejected"
	// ConnectorCommandReasonWriteAmbiguous: the WebSocket write did not
	// complete, so central cannot say whether the connector saw the command.
	ConnectorCommandReasonWriteAmbiguous = "write_ambiguous"
	// ConnectorCommandReasonQueueUnavailable: the session's send queue refused
	// the envelope before any byte was written.
	ConnectorCommandReasonQueueUnavailable = "queue_unavailable"
	// ConnectorCommandReasonSessionEnded: the connector session closed with the
	// command claimed and unacknowledged.
	ConnectorCommandReasonSessionEnded = "session_ended"
	// ConnectorCommandReasonAttemptsExhausted: automatic delivery gave up; the
	// command is a dead letter awaiting an explicit operator requeue.
	ConnectorCommandReasonAttemptsExhausted = "attempts_exhausted"
	// ConnectorCommandReasonDependencyFailed: the command this one depends on
	// dead-lettered, so this one can never run in its declared order.
	ConnectorCommandReasonDependencyFailed = "dependency_failed"
	// ConnectorCommandReasonUnspecified is what an unrecognised code becomes. A
	// caller that invents a reason gets an honest "we do not have a code for
	// this" rather than its string smuggled into the durable row.
	ConnectorCommandReasonUnspecified = "unspecified"
)

var validConnectorCommandReasons = map[string]bool{
	ConnectorCommandReasonConnectorRejected: true,
	ConnectorCommandReasonWriteAmbiguous:    true,
	ConnectorCommandReasonQueueUnavailable:  true,
	ConnectorCommandReasonSessionEnded:      true,
	ConnectorCommandReasonAttemptsExhausted: true,
	ConnectorCommandReasonDependencyFailed:  true,
	ConnectorCommandReasonUnspecified:       true,
}

var (
	ErrConnectorCommandNotFound = errors.New("connector command not found")
	ErrConnectorCommandScope    = errors.New("connector command belongs to another tenant or cluster")
	ErrConnectorCommandConflict = errors.New("connector command identity conflicts with stored envelope")
	ErrConnectorCommandLease    = errors.New("connector command lease is stale")
	ErrInvalidConnectorCommand  = errors.New("invalid connector command")
	// ErrConnectorCommandNotDeadLettered separates "there is nothing to requeue"
	// from "you may not see this command". A live or already-acknowledged
	// command is a refusal an operator can act on; a command belonging to
	// another tenant is ErrConnectorCommandNotFound, so the surface never
	// confirms that an id exists somewhere else.
	ErrConnectorCommandNotDeadLettered = errors.New("connector command is not a dead letter")
)

// ConnectorCommand is the durable, tenant-and-cluster-bound envelope sent to a
// connector. Envelope is immutable after insertion: retries and ambiguous
// writes always reuse its ID, timestamp and body byte-for-byte.
type ConnectorCommand struct {
	ID             string
	CustomerID     string
	ClusterID      string
	CommandType    protocol.MessageType
	PayloadVersion int
	PayloadDigest  string
	OrderingKey    string
	SemanticKey    string
	WorkloadID     string
	BurstID        string
	DependsOn      string
	Envelope       protocol.Envelope

	State          string
	Attempts       int
	NextAttemptAt  time.Time
	LeaseToken     string
	LockedUntil    time.Time
	LastError      string
	LastAmbiguity  string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeliveredAt    *time.Time
	AcknowledgedAt *time.Time
	Ack            *protocol.CommandAck
}

// ConnectorCommandStatus is the safe operator/workload view. It deliberately
// omits the command body, acknowledgement result and lease token.
type ConnectorCommandStatus struct {
	ID             string               `json:"id"`
	Type           protocol.MessageType `json:"type"`
	State          string               `json:"state"`
	Attempts       int                  `json:"attempts"`
	NextAttemptAt  *time.Time           `json:"next_attempt_at,omitempty"`
	LeaseExpiresAt *time.Time           `json:"lease_expires_at,omitempty"`
	LastError      string               `json:"last_error,omitempty"`
	LastAmbiguity  string               `json:"last_ambiguity,omitempty"`
	DeliveredAt    *time.Time           `json:"delivered_at,omitempty"`
	AcknowledgedAt *time.Time           `json:"acknowledged_at,omitempty"`
}

type ConnectorCommandAckResult struct {
	State     string
	Duplicate bool
}

type connectorCommandPersister interface {
	insertConnectorCommands(context.Context, []ConnectorCommand) error
	claimConnectorCommand(context.Context, string, string, time.Duration, int) (*ConnectorCommand, bool, error)
	recordConnectorCommandDelivered(context.Context, string, string) error
	markConnectorCommandAmbiguous(context.Context, string, string, string, time.Time, int) error
	acknowledgeConnectorCommand(context.Context, string, string, protocol.CommandAck, time.Time, int) (ConnectorCommandAckResult, error)
	listConnectorCommandsForWorkload(context.Context, string, string) ([]ConnectorCommandStatus, error)
}

type connectorCommandAdmissionPersister interface {
	submitWorkloadWithConnectorCommands(context.Context, *Workload, *AuditEvent, []ConnectorCommand) error
}

// connectorCommandOperatorPersister is the recovery half of the ledger: the
// two operations an operator drives rather than the delivery loop.
type connectorCommandOperatorPersister interface {
	listConnectorCommandsNeedingAttention(context.Context, string, int) ([]ConnectorCommandStatus, error)
	requeueConnectorCommand(context.Context, string, string, *AuditEvent) (ConnectorCommandStatus, error)
}

// StableConnectorCommandID derives a protocol-compatible ID from immutable
// command identity. Re-driving admission therefore finds the original command
// rather than creating a second agent-side idempotency key.
func StableConnectorCommandID(customerID, clusterID, semanticKey string) string {
	sum := sha256.Sum256([]byte(customerID + "\x00" + clusterID + "\x00" + semanticKey))
	return "cmd_" + hex.EncodeToString(sum[:16])
}

// NewConnectorCommand constructs and validates the immutable stored envelope.
func NewConnectorCommand(customerID, clusterID, semanticKey, orderingKey, workloadID, burstID, dependsOn string, env protocol.Envelope) (ConnectorCommand, error) {
	cmd := ConnectorCommand{
		ID: env.ID, CustomerID: customerID, ClusterID: clusterID,
		CommandType: env.Type, PayloadVersion: 1, OrderingKey: orderingKey,
		SemanticKey: semanticKey, WorkloadID: workloadID, BurstID: burstID,
		DependsOn: dependsOn, Envelope: env, State: ConnectorCommandPending,
		NextAttemptAt: time.Now().UTC(),
	}
	if err := normalizeConnectorCommand(&cmd); err != nil {
		return ConnectorCommand{}, err
	}
	return cmd, nil
}

func normalizeConnectorCommand(cmd *ConnectorCommand) error {
	if cmd == nil || cmd.ID != cmd.Envelope.ID ||
		!connectorCommandIdent(cmd.ID) || !connectorCommandIdent(cmd.CustomerID) ||
		!connectorCommandIdent(cmd.ClusterID) || !connectorCommandIdent(cmd.SemanticKey) ||
		!connectorCommandIdent(cmd.OrderingKey) || !connectorCommandIdent(string(cmd.CommandType)) ||
		cmd.PayloadVersion < 1 || cmd.PayloadVersion > maxConnectorCommandPayloadVersion ||
		cmd.Envelope.APIVersion != protocol.APIVersion ||
		cmd.CommandType != cmd.Envelope.Type || !connectorLifecycleCommand(cmd.CommandType) ||
		cmd.Envelope.Timestamp.IsZero() || len(cmd.Envelope.Body) == 0 || len(cmd.Envelope.Body) > maxConnectorCommandPayload ||
		!json.Valid(cmd.Envelope.Body) || cmd.DependsOn == cmd.ID {
		return ErrInvalidConnectorCommand
	}
	// The aggregate columns are optional but still bounded; DependsOn is either
	// absent or a full identity, because it is a foreign key to one.
	if !connectorCommandAggregateID(cmd.WorkloadID) || !connectorCommandAggregateID(cmd.BurstID) ||
		(cmd.DependsOn != "" && !connectorCommandIdent(cmd.DependsOn)) {
		return ErrInvalidConnectorCommand
	}
	if cmd.WorkloadID == "" && cmd.BurstID == "" {
		return ErrInvalidConnectorCommand
	}
	digest := sha256.Sum256(cmd.Envelope.Body)
	computed := hex.EncodeToString(digest[:])
	if cmd.PayloadDigest != "" && cmd.PayloadDigest != computed {
		return ErrConnectorCommandConflict
	}
	cmd.PayloadDigest = computed
	cmd.Envelope.Timestamp = cmd.Envelope.Timestamp.UTC()
	if cmd.State == "" {
		cmd.State = ConnectorCommandPending
	}
	if cmd.State != ConnectorCommandPending {
		return ErrInvalidConnectorCommand
	}
	if cmd.NextAttemptAt.IsZero() {
		cmd.NextAttemptAt = time.Now().UTC()
	} else {
		cmd.NextAttemptAt = cmd.NextAttemptAt.UTC()
	}
	cmd.Envelope.Body = append(json.RawMessage(nil), cmd.Envelope.Body...)
	return nil
}

// connectorLifecycleCommand is the closed set of command types this ledger will
// accept. Every member is a fire-and-forget WRITE whose meaning is fixed by its
// stable identity: announce this burst, create this job, delete this job, drain
// this node. Replaying one is the same instruction again, and an ACK carries no
// result the next caller needs.
//
// PrepareIdleTeardown and ReleaseIdleTeardown are deliberately ABSENT, and that
// is the boundary of the ledger's identity model rather than an oversight.
// They are a request/response pair, not a write: a prepare's ACK returns the
// cordon resourceVersion that THIS invocation produced, and the matching
// release is only valid against that exact version. Under a stable semantic key
// the ledger would (a) replay one invocation's request minutes or hours later,
// re-cordoning a node on behalf of a teardown that is long over, and (b) hand a
// second invocation the FIRST one's stored result — a release fired against a
// resourceVersion somebody else's cordon now owns, which is precisely the
// "uncordon that guessed at ownership" releaseIdleCordon exists to refuse.
//
// Making them durable therefore needs a different contract from this one: a
// fresh per-invocation request id, a result that is readable exactly once by
// the invocation that asked for it, and an expiry after which the result is
// gone rather than stale. Until that exists they stay on the synchronous,
// version-fenced path in agent_node_events.go, which already fails closed — a
// refusal, a dropped socket and a timeout are all "no", and a cordon whose
// token died with the process is simply never released rather than released
// against a version it no longer owns.
func connectorLifecycleCommand(t protocol.MessageType) bool {
	switch t {
	case protocol.TypeBurstAnnounce, protocol.TypeCreateJob, protocol.TypeDeleteJob,
		protocol.TypeDrainNode:
		return true
	default:
		return false
	}
}

// connectorCommandIdent mirrors the SQL CHECK on every required identity
// column: length(btrim(x)) BETWEEN 1 AND 255, counted in characters exactly as
// Postgres length() counts them.
func connectorCommandIdent(value string) bool {
	trimmed := strings.TrimSpace(value)
	return trimmed != "" && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00') &&
		utf8.RuneCountInString(trimmed) <= maxConnectorCommandIdent
}

// connectorCommandAggregateID mirrors octet_length(x) <= 255 on workload_id and
// burst_id, where empty is a legitimate value: a burst-scoped command has no
// workload and a workload-scoped one may have no burst.
func connectorCommandAggregateID(value string) bool {
	return len(value) <= maxConnectorCommandIdent && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

func validateConnectorCommandBatch(commands []ConnectorCommand) error {
	if len(commands) == 0 || len(commands) > 32 {
		return ErrInvalidConnectorCommand
	}
	seen := make(map[string]ConnectorCommand, len(commands))
	for i := range commands {
		if err := normalizeConnectorCommand(&commands[i]); err != nil {
			return err
		}
		if _, duplicate := seen[commands[i].ID]; duplicate {
			return ErrConnectorCommandConflict
		}
		seen[commands[i].ID] = commands[i]
	}
	for _, cmd := range commands {
		if cmd.DependsOn == "" {
			continue
		}
		if dep, ok := seen[cmd.DependsOn]; ok && (dep.CustomerID != cmd.CustomerID || dep.ClusterID != cmd.ClusterID || dep.OrderingKey != cmd.OrderingKey) {
			return ErrInvalidConnectorCommand
		}
	}
	return nil
}

// PutConnectorCommands atomically persists a small ordered command batch.
// Existing identical semantic commands make this idempotent; a changed body or
// dependency under the same identity is refused.
func (s *Store) PutConnectorCommands(ctx context.Context, commands []ConnectorCommand) error {
	commands = append([]ConnectorCommand(nil), commands...)
	if err := validateConnectorCommandBatch(commands); err != nil {
		return err
	}
	if p, ok := s.persist.(connectorCommandPersister); ok {
		return p.insertConnectorCommands(ctx, commands)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putConnectorCommandsLocked(commands)
}

func (s *Store) putConnectorCommandsLocked(commands []ConnectorCommand) error {
	for _, cmd := range commands {
		if existing := s.connectorCommands[cmd.ID]; existing != nil && !sameConnectorCommand(existing, &cmd) {
			return ErrConnectorCommandConflict
		}
		for _, existing := range s.connectorCommands {
			if existing.CustomerID == cmd.CustomerID && existing.ClusterID == cmd.ClusterID && existing.SemanticKey == cmd.SemanticKey && existing.ID != cmd.ID {
				return ErrConnectorCommandConflict
			}
		}
		if cmd.DependsOn != "" {
			dep := s.connectorCommands[cmd.DependsOn]
			if dep == nil {
				for i := range commands {
					if commands[i].ID == cmd.DependsOn {
						dep = &commands[i]
						break
					}
				}
			}
			if dep == nil || dep.CustomerID != cmd.CustomerID || dep.ClusterID != cmd.ClusterID {
				return ErrInvalidConnectorCommand
			}
		}
	}
	now := time.Now().UTC()
	for i := range commands {
		cmd := commands[i]
		if s.connectorCommands[cmd.ID] != nil {
			continue
		}
		cmd.CreatedAt, cmd.UpdatedAt = now, now
		s.connectorCommands[cmd.ID] = cloneConnectorCommand(&cmd)
	}
	return nil
}

// SubmitWorkloadWithConnectorCommands commits the admitted workload, its audit
// decision and its initial connector-command batch as one durability boundary.
// A restart can therefore see all three or none of them; there is no accepted
// workload row whose announce/create commands were never recorded.
func (s *Store) SubmitWorkloadWithConnectorCommands(ctx context.Context, w *Workload, ev *AuditEvent, commands []ConnectorCommand) error {
	if w == nil {
		return ErrInvalidConnectorCommand
	}
	if err := validateAudit(ev); err != nil {
		return err
	}
	commands = append([]ConnectorCommand(nil), commands...)
	if err := validateConnectorCommandBatch(commands); err != nil {
		return err
	}
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p != nil {
		atomic, ok := p.(connectorCommandAdmissionPersister)
		if !ok {
			s.recordPersistenceFailure("connector_command", "submit_batch", ErrPersistence)
			return fmt.Errorf("%w: connector command admission transaction unavailable", ErrPersistence)
		}
		if err := atomic.submitWorkloadWithConnectorCommands(ctx, w, ev, commands); err != nil {
			s.recordPersistenceFailure("connector_command", "submit_batch", err)
			return fmt.Errorf("%w: submit workload %s with connector commands: %w", ErrPersistence, w.ID, err)
		}
		s.mu.Lock()
		preserveWorkloadTransition(w, s.workloads[w.ID])
		s.workloads[w.ID] = w
		s.mu.Unlock()
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if terminalWorkloadBindingConflict(w, s.workloads[w.ID]) {
		return fmt.Errorf("%w: terminal workload binding conflict", ErrPersistence)
	}
	if err := s.putConnectorCommandsLocked(commands); err != nil {
		return err
	}
	preserveWorkloadTransition(w, s.workloads[w.ID])
	s.workloads[w.ID] = w
	return nil
}

func sameConnectorCommand(a, b *ConnectorCommand) bool {
	return a != nil && b != nil && a.ID == b.ID && a.CustomerID == b.CustomerID && a.ClusterID == b.ClusterID &&
		a.CommandType == b.CommandType && a.PayloadVersion == b.PayloadVersion && a.PayloadDigest == b.PayloadDigest &&
		a.OrderingKey == b.OrderingKey && a.SemanticKey == b.SemanticKey && a.WorkloadID == b.WorkloadID &&
		a.BurstID == b.BurstID && a.DependsOn == b.DependsOn && a.Envelope.APIVersion == b.Envelope.APIVersion &&
		string(a.Envelope.Body) == string(b.Envelope.Body)
}

// ClaimConnectorCommand leases one due command for the exact socket scope.
// Expired leases are reclaimable and every state transition is fenced by the
// returned LeaseToken.
func (s *Store) ClaimConnectorCommand(ctx context.Context, customerID, clusterID string, lease time.Duration, maxAttempts int) (*ConnectorCommand, bool, error) {
	if !connectorCommandIdent(customerID) || !connectorCommandIdent(clusterID) ||
		lease <= 0 || lease > 15*time.Minute || maxAttempts < 1 || maxAttempts > 100 {
		return nil, false, ErrInvalidConnectorCommand
	}
	if p, ok := s.persist.(connectorCommandPersister); ok {
		return p.claimConnectorCommand(ctx, customerID, clusterID, lease, maxAttempts)
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []*ConnectorCommand
	for _, cmd := range s.connectorCommands {
		if cmd.CustomerID != customerID || cmd.ClusterID != clusterID || !commandDue(cmd, now) {
			continue
		}
		if cmd.DependsOn != "" {
			dep := s.connectorCommands[cmd.DependsOn]
			if dep == nil || dep.State == ConnectorCommandDeadLetter {
				cmd.State = ConnectorCommandDeadLetter
				cmd.LastError = ConnectorCommandReasonDependencyFailed
				cmd.UpdatedAt = now
				continue
			}
			if dep.State != ConnectorCommandAcknowledged {
				continue
			}
		}
		if cmd.Attempts >= maxAttempts {
			cmd.State = ConnectorCommandDeadLetter
			cmd.LastError = ConnectorCommandReasonAttemptsExhausted
			cmd.UpdatedAt = now
			continue
		}
		due = append(due, cmd)
	}
	if len(due) == 0 {
		return nil, false, nil
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].NextAttemptAt.Equal(due[j].NextAttemptAt) {
			return due[i].ID < due[j].ID
		}
		return due[i].NextAttemptAt.Before(due[j].NextAttemptAt)
	})
	token, err := connectorCommandToken()
	if err != nil {
		return nil, false, err
	}
	cmd := due[0]
	cmd.State = ConnectorCommandProcessing
	cmd.Attempts++
	cmd.LeaseToken = token
	cmd.LockedUntil = now.Add(lease)
	cmd.UpdatedAt = now
	return cloneConnectorCommand(cmd), true, nil
}

func commandDue(cmd *ConnectorCommand, now time.Time) bool {
	return (cmd.State == ConnectorCommandPending || cmd.State == ConnectorCommandFailed) && !cmd.NextAttemptAt.After(now) ||
		cmd.State == ConnectorCommandProcessing && !cmd.LockedUntil.After(now)
}

func (s *Store) RecordConnectorCommandDelivered(ctx context.Context, id, leaseToken string) error {
	if !connectorCommandIdent(id) || !connectorCommandIdent(leaseToken) {
		return ErrInvalidConnectorCommand
	}
	if p, ok := s.persist.(connectorCommandPersister); ok {
		return p.recordConnectorCommandDelivered(ctx, id, leaseToken)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := s.connectorCommands[id]
	if cmd == nil {
		return ErrConnectorCommandNotFound
	}
	if cmd.State == ConnectorCommandAcknowledged {
		return nil
	}
	if cmd.State != ConnectorCommandProcessing || cmd.LeaseToken != leaseToken {
		return ErrConnectorCommandLease
	}
	now := time.Now().UTC()
	cmd.DeliveredAt, cmd.UpdatedAt = &now, now
	return nil
}

// MarkConnectorCommandAmbiguous records that a claimed command's delivery
// outcome is unknown. reason must be one of the ConnectorCommandReason* codes;
// anything else is stored as ConnectorCommandReasonUnspecified.
func (s *Store) MarkConnectorCommandAmbiguous(ctx context.Context, id, leaseToken, reason string, retryAt time.Time, maxAttempts int) error {
	if !connectorCommandIdent(id) || !connectorCommandIdent(leaseToken) ||
		retryAt.IsZero() || maxAttempts < 1 || maxAttempts > 100 {
		return ErrInvalidConnectorCommand
	}
	safeError := connectorCommandReason(reason)
	if p, ok := s.persist.(connectorCommandPersister); ok {
		return p.markConnectorCommandAmbiguous(ctx, id, leaseToken, safeError, retryAt, maxAttempts)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := s.connectorCommands[id]
	if cmd == nil {
		return ErrConnectorCommandNotFound
	}
	if cmd.State == ConnectorCommandAcknowledged {
		return nil
	}
	if cmd.State != ConnectorCommandProcessing || cmd.LeaseToken != leaseToken {
		return ErrConnectorCommandLease
	}
	now := time.Now().UTC()
	cmd.State = ConnectorCommandFailed
	if cmd.Attempts >= maxAttempts {
		cmd.State = ConnectorCommandDeadLetter
	}
	cmd.LastError, cmd.LastAmbiguity = safeError, safeError
	cmd.NextAttemptAt, cmd.UpdatedAt = retryAt.UTC(), now
	cmd.LeaseToken, cmd.LockedUntil = "", time.Time{}
	return nil
}

// AcknowledgeConnectorCommand applies a connector's acknowledgement to the
// durable row. The raw ACK is validated here and then REPLACED by its sanitized
// form, so nothing below this line — the in-memory row, the Postgres ack column,
// the workload status, a log line — can ever see connector-authored text.
func (s *Store) AcknowledgeConnectorCommand(ctx context.Context, customerID, clusterID string, ack protocol.CommandAck, retryAt time.Time, maxAttempts int) (ConnectorCommandAckResult, error) {
	if !connectorCommandIdent(customerID) || !connectorCommandIdent(clusterID) ||
		!connectorCommandIdent(ack.CommandID) || maxAttempts < 1 || maxAttempts > 100 {
		return ConnectorCommandAckResult{}, ErrInvalidConnectorCommand
	}
	if len(ack.Result) > maxConnectorCommandPayload || (len(ack.Result) > 0 && !json.Valid(ack.Result)) {
		return ConnectorCommandAckResult{}, ErrInvalidConnectorCommand
	}
	ack = sanitizeConnectorAck(ack)
	if p, ok := s.persist.(connectorCommandPersister); ok {
		return p.acknowledgeConnectorCommand(ctx, customerID, clusterID, ack, retryAt, maxAttempts)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := s.connectorCommands[ack.CommandID]
	if cmd == nil {
		return ConnectorCommandAckResult{}, ErrConnectorCommandNotFound
	}
	if cmd.CustomerID != customerID || cmd.ClusterID != clusterID {
		return ConnectorCommandAckResult{}, ErrConnectorCommandScope
	}
	if cmd.State == ConnectorCommandAcknowledged || (cmd.State == ConnectorCommandDeadLetter && !ack.Success) {
		return ConnectorCommandAckResult{State: cmd.State, Duplicate: true}, nil
	}
	now := time.Now().UTC()
	copyAck := ack
	copyAck.Result = append(json.RawMessage(nil), ack.Result...)
	cmd.Ack, cmd.UpdatedAt = &copyAck, now
	cmd.LeaseToken, cmd.LockedUntil = "", time.Time{}
	if ack.Success {
		cmd.State = ConnectorCommandAcknowledged
		cmd.LastError = ""
		cmd.AcknowledgedAt = &now
	} else {
		cmd.State = ConnectorCommandFailed
		cmd.LastError = ack.Error
		cmd.NextAttemptAt = retryAt.UTC()
		if cmd.Attempts >= maxAttempts {
			cmd.State = ConnectorCommandDeadLetter
		}
	}
	return ConnectorCommandAckResult{State: cmd.State}, nil
}

func (s *Store) ListConnectorCommandsForWorkload(ctx context.Context, customerID, workloadID string) ([]ConnectorCommandStatus, error) {
	if !connectorCommandIdent(customerID) || !connectorCommandIdent(workloadID) {
		return nil, ErrInvalidConnectorCommand
	}
	if p, ok := s.persist.(connectorCommandPersister); ok {
		return p.listConnectorCommandsForWorkload(ctx, customerID, workloadID)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []ConnectorCommandStatus
	for _, cmd := range s.connectorCommands {
		if cmd.CustomerID == customerID && cmd.WorkloadID == workloadID {
			out = append(out, connectorCommandStatus(cmd))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// ListConnectorCommandsNeedingAttention returns the tenant's commands that
// automatic delivery could not complete: retryable failures and dead letters.
//
// Dead letters are the reason this exists. Nothing in the delivery loop ever
// takes one back — a command that exhausted its attempts, or whose dependency
// did, stays exactly where it is until a human looks at it — so without a way
// to SEE them a dead letter is a silent stall: a burst announced to nobody, a
// node never drained, and no signal anywhere except a log line that has already
// rotated away.
//
// Bounded by construction, and safe by reusing the same reduced status the
// workload view returns: no envelope body, no acknowledgement, no lease token.
func (s *Store) ListConnectorCommandsNeedingAttention(ctx context.Context, customerID string, limit int) ([]ConnectorCommandStatus, error) {
	if !connectorCommandIdent(customerID) {
		return nil, ErrInvalidConnectorCommand
	}
	if limit < 1 || limit > maxConnectorCommandAttentionPage {
		limit = maxConnectorCommandAttentionPage
	}
	if p, ok := s.persist.(connectorCommandOperatorPersister); ok {
		return p.listConnectorCommandsNeedingAttention(ctx, customerID, limit)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []ConnectorCommandStatus
	for _, cmd := range s.connectorCommands {
		if cmd.CustomerID != customerID {
			continue
		}
		if cmd.State != ConnectorCommandFailed && cmd.State != ConnectorCommandDeadLetter {
			continue
		}
		out = append(out, connectorCommandStatus(cmd))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// RequeueConnectorCommand puts ONE dead-letter command back on the delivery
// path, and journals the decision in the same durable step where the persister
// can express it.
//
// Explicit by design, and never automatic. A dead letter is a command central
// tried and could not land; re-driving it on a timer is how a drain that a
// connector keeps rejecting becomes an infinite loop against the customer's API
// server, and how a stale announce lands hours after the burst it describes is
// gone. Someone has to decide the command is still the right instruction, and
// this is where they say so.
//
// What it does NOT touch is identity: the id, semantic key, ordering key,
// dependency and envelope are the stored ones, byte for byte. Only the delivery
// state moves — back to pending, attempts reset, lease cleared, and the previous
// invocation's acknowledgement dropped so no reader mistakes it for this
// attempt's result. The reason codes that explain the dead-letter are kept, so
// the operator can still see WHY it stalled after asking for it to run again.
//
// A command dead-lettered because its DEPENDENCY dead-lettered will simply
// dead-letter again on its next claim: the dependency is the thing to requeue
// first. That is the honest outcome — the ledger will not run a create_job
// whose burst_announce never landed just because a human asked twice.
func (s *Store) RequeueConnectorCommand(ctx context.Context, customerID, id string, ev *AuditEvent) (ConnectorCommandStatus, error) {
	if !connectorCommandIdent(customerID) || !connectorCommandIdent(id) {
		return ConnectorCommandStatus{}, ErrInvalidConnectorCommand
	}
	if err := validateAudit(ev); err != nil {
		return ConnectorCommandStatus{}, err
	}
	if p, ok := s.persist.(connectorCommandOperatorPersister); ok {
		return p.requeueConnectorCommand(ctx, customerID, id, ev)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := s.connectorCommands[id]
	if cmd == nil || cmd.CustomerID != customerID {
		return ConnectorCommandStatus{}, ErrConnectorCommandNotFound
	}
	if cmd.State != ConnectorCommandDeadLetter {
		return ConnectorCommandStatus{}, ErrConnectorCommandNotDeadLettered
	}
	now := time.Now().UTC()
	cmd.State = ConnectorCommandPending
	cmd.Attempts = 0
	cmd.NextAttemptAt = now
	cmd.LeaseToken, cmd.LockedUntil = "", time.Time{}
	cmd.Ack = nil
	cmd.UpdatedAt = now
	return connectorCommandStatus(cmd), nil
}

func connectorCommandStatus(cmd *ConnectorCommand) ConnectorCommandStatus {
	status := ConnectorCommandStatus{
		ID: cmd.ID, Type: cmd.CommandType, State: cmd.State, Attempts: cmd.Attempts,
		LastError: cmd.LastError, LastAmbiguity: cmd.LastAmbiguity,
		DeliveredAt: cmd.DeliveredAt, AcknowledgedAt: cmd.AcknowledgedAt,
	}
	if cmd.State == ConnectorCommandPending || cmd.State == ConnectorCommandFailed {
		next := cmd.NextAttemptAt
		status.NextAttemptAt = &next
	}
	if cmd.State == ConnectorCommandProcessing {
		locked := cmd.LockedUntil
		status.LeaseExpiresAt = &locked
	}
	return status
}

func cloneConnectorCommand(cmd *ConnectorCommand) *ConnectorCommand {
	if cmd == nil {
		return nil
	}
	clone := *cmd
	clone.Envelope.Body = append(json.RawMessage(nil), cmd.Envelope.Body...)
	if cmd.Ack != nil {
		ack := *cmd.Ack
		ack.Result = append(json.RawMessage(nil), cmd.Ack.Result...)
		clone.Ack = &ack
	}
	return &clone
}

func connectorCommandToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("connector command lease token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

// connectorCommandReason maps a caller's reason onto the closed vocabulary.
// Empty stays empty — "no reason recorded" is a fact — and anything outside the
// set becomes ConnectorCommandReasonUnspecified rather than being stored. There
// is deliberately no truncation arm: a bounded prefix of a secret is still a
// secret, and the only safe bound on connector-authored text is not keeping it.
func connectorCommandReason(code string) string {
	code = strings.TrimSpace(code)
	if code == "" {
		return ""
	}
	if validConnectorCommandReasons[code] {
		return code
	}
	return ConnectorCommandReasonUnspecified
}

// sanitizeConnectorAck reduces a connector acknowledgement to the part central
// is willing to make durable: which command it names, whether it succeeded, and
// a closed reason code.
//
// Error is replaced, never truncated, for the reason above. Result is DROPPED
// entirely: it is a connector-authored body that may quote a provider payload
// or a cluster object, and no command type this ledger accepts has a result the
// ledger needs to keep — the one command family whose result IS load-bearing,
// the idle-teardown preflight, is excluded from the ledger for exactly that
// reason (see connectorLifecycleCommand). The raw ack is still validated for
// size and well-formedness before it gets here, so a malformed one is refused
// rather than silently emptied.
func sanitizeConnectorAck(ack protocol.CommandAck) protocol.CommandAck {
	safe := protocol.CommandAck{CommandID: ack.CommandID, Success: ack.Success}
	if !ack.Success {
		safe.Error = ConnectorCommandReasonConnectorRejected
	}
	return safe
}
