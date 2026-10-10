package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

func (p *pgPersister) insertConnectorCommands(ctx context.Context, commands []ConnectorCommand) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("state: begin connector command batch: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := insertConnectorCommandsTx(ctx, tx, commands); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("state: commit connector command batch: %w", err)
	}
	return nil
}

func insertConnectorCommandsTx(ctx context.Context, tx pgx.Tx, commands []ConnectorCommand) error {
	for i := range commands {
		envelope, err := json.Marshal(commands[i].Envelope)
		if err != nil {
			return fmt.Errorf("state: encode connector command: %w", err)
		}
		result, err := tx.Exec(ctx, `
			INSERT INTO connector_commands
			(id, customer_id, cluster_id, command_type, payload_version, payload_digest,
			 ordering_key, semantic_key, workload_id, burst_id, depends_on, envelope, next_attempt_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULLIF($11,''),$12,$13)
			ON CONFLICT (id) DO NOTHING`,
			commands[i].ID, commands[i].CustomerID, commands[i].ClusterID, commands[i].CommandType,
			commands[i].PayloadVersion, commands[i].PayloadDigest, commands[i].OrderingKey,
			commands[i].SemanticKey, commands[i].WorkloadID, commands[i].BurstID,
			commands[i].DependsOn, envelope, commands[i].NextAttemptAt)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return ErrConnectorCommandConflict
			}
			return fmt.Errorf("state: insert connector command: %w", err)
		}
		if result.RowsAffected() == 1 {
			continue
		}
		var existing ConnectorCommand
		var envelopeBytes []byte
		if err := tx.QueryRow(ctx, `
			SELECT id, customer_id, cluster_id, command_type, payload_version, payload_digest,
			       ordering_key, semantic_key, workload_id, burst_id, COALESCE(depends_on,''), envelope
			FROM connector_commands WHERE id=$1 FOR UPDATE`, commands[i].ID).
			Scan(&existing.ID, &existing.CustomerID, &existing.ClusterID, &existing.CommandType,
				&existing.PayloadVersion, &existing.PayloadDigest, &existing.OrderingKey,
				&existing.SemanticKey, &existing.WorkloadID, &existing.BurstID, &existing.DependsOn,
				&envelopeBytes); err != nil {
			return fmt.Errorf("state: read existing connector command: %w", err)
		}
		if err := json.Unmarshal(envelopeBytes, &existing.Envelope); err != nil {
			return fmt.Errorf("state: decode existing connector command: %w", err)
		}
		if !sameConnectorCommand(&existing, &commands[i]) {
			return ErrConnectorCommandConflict
		}
	}
	for i := range commands {
		if commands[i].DependsOn == "" {
			continue
		}
		var depCustomer, depCluster, depOrdering string
		if err := tx.QueryRow(ctx, `
			SELECT customer_id, cluster_id, ordering_key FROM connector_commands WHERE id=$1`, commands[i].DependsOn).
			Scan(&depCustomer, &depCluster, &depOrdering); errors.Is(err, pgx.ErrNoRows) {
			return ErrInvalidConnectorCommand
		} else if err != nil {
			return fmt.Errorf("state: read connector command dependency: %w", err)
		}
		if depCustomer != commands[i].CustomerID || depCluster != commands[i].ClusterID || depOrdering != commands[i].OrderingKey {
			return ErrInvalidConnectorCommand
		}
	}
	return nil
}

func (p *pgPersister) submitWorkloadWithConnectorCommands(ctx context.Context, w *Workload, ev *AuditEvent, commands []ConnectorCommand) error {
	data, err := json.Marshal(w)
	if err != nil {
		return fmt.Errorf("marshal workload %s: %w", w.ID, err)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("state: begin workload connector-command admission: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, upsertWorkloadStmt(tblWorkloads), w.ID, data)
	if err != nil {
		return fmt.Errorf("upsert %s %s: %w", tblWorkloads, w.ID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: terminal workload binding conflict", ErrPersistence)
	}
	if err := insertConnectorCommandsTx(ctx, tx, commands); err != nil {
		return err
	}
	if err := insertAuditTx(ctx, tx, ev); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("state: commit workload connector-command admission: %w", err)
	}
	return nil
}

func (p *pgPersister) claimConnectorCommand(ctx context.Context, customerID, clusterID string, lease time.Duration, maxAttempts int) (*ConnectorCommand, bool, error) {
	token, err := connectorCommandToken()
	if err != nil {
		return nil, false, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("state: begin connector command claim: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `
		UPDATE connector_commands AS c
		SET state='dead_letter', last_error='dependency_failed',
		    locked_until=NULL, lease_token=NULL, updated_at=now()
		FROM connector_commands AS dep
		WHERE c.customer_id=$1 AND c.cluster_id=$2 AND c.depends_on=dep.id
		  AND dep.state='dead_letter' AND c.state IN ('pending','processing','failed')`, customerID, clusterID); err != nil {
		return nil, false, fmt.Errorf("state: dead-letter blocked connector commands: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE connector_commands
		SET state='dead_letter', last_error='attempts_exhausted',
		    locked_until=NULL, lease_token=NULL, updated_at=now()
		WHERE customer_id=$1 AND cluster_id=$2 AND attempts >= $3
		  AND ((state IN ('pending','failed') AND next_attempt_at <= now())
		    OR (state='processing' AND locked_until <= now()))`, customerID, clusterID, maxAttempts); err != nil {
		return nil, false, fmt.Errorf("state: exhaust connector commands: %w", err)
	}
	var cmd ConnectorCommand
	var envelopeBytes []byte
	err = tx.QueryRow(ctx, `
		WITH candidate AS (
		    SELECT c.id
		    FROM connector_commands AS c
		    LEFT JOIN connector_commands AS dep ON dep.id=c.depends_on
		    WHERE c.customer_id=$1 AND c.cluster_id=$2
		      AND ((c.state IN ('pending','failed') AND c.next_attempt_at <= now())
		        OR (c.state='processing' AND c.locked_until <= now()))
		      AND (c.depends_on IS NULL OR dep.state='acknowledged')
		    ORDER BY c.next_attempt_at, c.id
		    FOR UPDATE OF c SKIP LOCKED LIMIT 1
		)
		UPDATE connector_commands AS c
		SET state='processing', attempts=c.attempts+1,
		    locked_until=now()+make_interval(secs => $3), lease_token=$4, updated_at=now()
		FROM candidate WHERE c.id=candidate.id
		RETURNING c.id, c.customer_id, c.cluster_id, c.command_type, c.payload_version,
		          c.payload_digest, c.ordering_key, c.semantic_key, c.workload_id, c.burst_id,
		          COALESCE(c.depends_on,''), c.envelope, c.state, c.attempts,
		          c.next_attempt_at, c.lease_token, c.locked_until, c.last_error,
		          c.last_ambiguity, c.created_at, c.updated_at, c.delivered_at, c.acknowledged_at`,
		customerID, clusterID, lease.Seconds(), token).
		Scan(&cmd.ID, &cmd.CustomerID, &cmd.ClusterID, &cmd.CommandType, &cmd.PayloadVersion,
			&cmd.PayloadDigest, &cmd.OrderingKey, &cmd.SemanticKey, &cmd.WorkloadID, &cmd.BurstID,
			&cmd.DependsOn, &envelopeBytes, &cmd.State, &cmd.Attempts, &cmd.NextAttemptAt,
			&cmd.LeaseToken, &cmd.LockedUntil, &cmd.LastError, &cmd.LastAmbiguity,
			&cmd.CreatedAt, &cmd.UpdatedAt, &cmd.DeliveredAt, &cmd.AcknowledgedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("state: commit empty connector command claim: %w", err)
		}
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("state: claim connector command: %w", err)
	}
	if err := json.Unmarshal(envelopeBytes, &cmd.Envelope); err != nil {
		return nil, false, fmt.Errorf("state: decode claimed connector command: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("state: commit connector command claim: %w", err)
	}
	return &cmd, true, nil
}

func (p *pgPersister) recordConnectorCommandDelivered(ctx context.Context, id, leaseToken string) error {
	result, err := p.pool.Exec(ctx, `
		UPDATE connector_commands SET delivered_at=now(), updated_at=now()
		WHERE id=$1 AND state='processing' AND lease_token=$2`, id, leaseToken)
	if err != nil {
		return fmt.Errorf("state: record connector command delivery: %w", err)
	}
	if result.RowsAffected() == 1 {
		return nil
	}
	var state string
	if err := p.pool.QueryRow(ctx, `SELECT state FROM connector_commands WHERE id=$1`, id).Scan(&state); errors.Is(err, pgx.ErrNoRows) {
		return ErrConnectorCommandNotFound
	} else if err != nil {
		return fmt.Errorf("state: inspect connector command delivery fence: %w", err)
	}
	if state == ConnectorCommandAcknowledged {
		return nil
	}
	return ErrConnectorCommandLease
}

func (p *pgPersister) markConnectorCommandAmbiguous(ctx context.Context, id, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error {
	result, err := p.pool.Exec(ctx, `
		UPDATE connector_commands
		SET state=CASE WHEN attempts >= $5 THEN 'dead_letter' ELSE 'failed' END,
		    next_attempt_at=$3, last_error=$4, last_ambiguity=$4,
		    locked_until=NULL, lease_token=NULL, updated_at=now()
		WHERE id=$1 AND state='processing' AND lease_token=$2`,
		id, leaseToken, retryAt.UTC(), safeError, maxAttempts)
	if err != nil {
		return fmt.Errorf("state: record ambiguous connector command write: %w", err)
	}
	if result.RowsAffected() == 1 {
		return nil
	}
	var state string
	if err := p.pool.QueryRow(ctx, `SELECT state FROM connector_commands WHERE id=$1`, id).Scan(&state); errors.Is(err, pgx.ErrNoRows) {
		return ErrConnectorCommandNotFound
	} else if err != nil {
		return fmt.Errorf("state: inspect connector command ambiguity fence: %w", err)
	}
	if state == ConnectorCommandAcknowledged {
		return nil
	}
	return ErrConnectorCommandLease
}

func (p *pgPersister) acknowledgeConnectorCommand(ctx context.Context, customerID, clusterID string, ack protocol.CommandAck, retryAt time.Time, maxAttempts int) (ConnectorCommandAckResult, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return ConnectorCommandAckResult{}, fmt.Errorf("state: begin connector command acknowledgement: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var ownerCustomer, ownerCluster, currentState string
	var attempts int
	if err := tx.QueryRow(ctx, `
		SELECT customer_id, cluster_id, state, attempts
		FROM connector_commands WHERE id=$1 FOR UPDATE`, ack.CommandID).
		Scan(&ownerCustomer, &ownerCluster, &currentState, &attempts); errors.Is(err, pgx.ErrNoRows) {
		return ConnectorCommandAckResult{}, ErrConnectorCommandNotFound
	} else if err != nil {
		return ConnectorCommandAckResult{}, fmt.Errorf("state: lock connector command acknowledgement: %w", err)
	}
	if ownerCustomer != customerID || ownerCluster != clusterID {
		return ConnectorCommandAckResult{}, ErrConnectorCommandScope
	}
	if currentState == ConnectorCommandAcknowledged || (currentState == ConnectorCommandDeadLetter && !ack.Success) {
		return ConnectorCommandAckResult{State: currentState, Duplicate: true}, nil
	}
	ackJSON, err := json.Marshal(ack)
	if err != nil {
		return ConnectorCommandAckResult{}, fmt.Errorf("state: encode connector command acknowledgement: %w", err)
	}
	nextState := ConnectorCommandAcknowledged
	acknowledgedAt := any(time.Now().UTC())
	lastError := ""
	if !ack.Success {
		nextState = ConnectorCommandFailed
		acknowledgedAt = nil
		lastError = ack.Error
		if attempts >= maxAttempts {
			nextState = ConnectorCommandDeadLetter
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE connector_commands
		SET state=$2, ack=$3::jsonb, next_attempt_at=$4, last_error=$5,
		    locked_until=NULL, lease_token=NULL, updated_at=now(), acknowledged_at=$6
		WHERE id=$1`, ack.CommandID, nextState, string(ackJSON), retryAt.UTC(), lastError, acknowledgedAt); err != nil {
		return ConnectorCommandAckResult{}, fmt.Errorf("state: apply connector command acknowledgement: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ConnectorCommandAckResult{}, fmt.Errorf("state: commit connector command acknowledgement: %w", err)
	}
	return ConnectorCommandAckResult{State: nextState}, nil
}

// connectorCommandStatusColumns is the exact projection ConnectorCommandStatus
// is scanned from. Named once so the two operator queries and the workload view
// cannot drift into returning different shapes of the same row.
const connectorCommandStatusColumns = `id, command_type, state, attempts, next_attempt_at, last_error,
	       last_ambiguity, locked_until, delivered_at, acknowledged_at`

func scanConnectorCommandStatus(row pgx.Row) (ConnectorCommandStatus, error) {
	var status ConnectorCommandStatus
	var nextAttempt time.Time
	if err := row.Scan(&status.ID, &status.Type, &status.State, &status.Attempts,
		&nextAttempt, &status.LastError, &status.LastAmbiguity, &status.LeaseExpiresAt,
		&status.DeliveredAt, &status.AcknowledgedAt); err != nil {
		return ConnectorCommandStatus{}, err
	}
	if status.State == ConnectorCommandPending || status.State == ConnectorCommandFailed {
		status.NextAttemptAt = &nextAttempt
	}
	if status.State != ConnectorCommandProcessing {
		status.LeaseExpiresAt = nil
	}
	return status, nil
}

func (p *pgPersister) listConnectorCommandsNeedingAttention(ctx context.Context, customerID string, limit int) ([]ConnectorCommandStatus, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT `+connectorCommandStatusColumns+`
		FROM connector_commands
		WHERE customer_id=$1 AND state IN ('failed','dead_letter')
		ORDER BY updated_at DESC, id
		LIMIT $2`, customerID, limit)
	if err != nil {
		return nil, fmt.Errorf("state: list connector commands needing attention: %w", err)
	}
	defer rows.Close()
	var out []ConnectorCommandStatus
	for rows.Next() {
		status, err := scanConnectorCommandStatus(rows)
		if err != nil {
			return nil, fmt.Errorf("state: scan connector command needing attention: %w", err)
		}
		out = append(out, status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate connector commands needing attention: %w", err)
	}
	return out, nil
}

// requeueConnectorCommand moves one dead letter back to pending and journals the
// decision in the SAME transaction. Either the operator's requeue is recorded
// and the command is runnable again, or neither happened — an unaudited requeue
// is a command that starts moving with nothing saying who asked for it.
//
// The UPDATE names state='dead_letter' in its own WHERE clause rather than
// trusting the SELECT above it: FOR UPDATE serialises two concurrent requeues,
// and the loser must find nothing to do instead of resetting the attempts of a
// delivery the winner already restarted.
func (p *pgPersister) requeueConnectorCommand(ctx context.Context, customerID, id string, ev *AuditEvent) (ConnectorCommandStatus, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return ConnectorCommandStatus{}, fmt.Errorf("state: begin connector command requeue: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var owner, currentState string
	if err := tx.QueryRow(ctx, `
		SELECT customer_id, state FROM connector_commands WHERE id=$1 FOR UPDATE`, id).
		Scan(&owner, &currentState); errors.Is(err, pgx.ErrNoRows) {
		return ConnectorCommandStatus{}, ErrConnectorCommandNotFound
	} else if err != nil {
		return ConnectorCommandStatus{}, fmt.Errorf("state: lock connector command requeue: %w", err)
	}
	if owner != customerID {
		return ConnectorCommandStatus{}, ErrConnectorCommandNotFound
	}
	if currentState != ConnectorCommandDeadLetter {
		return ConnectorCommandStatus{}, ErrConnectorCommandNotDeadLettered
	}
	status, err := scanConnectorCommandStatus(tx.QueryRow(ctx, `
		UPDATE connector_commands
		SET state='pending', attempts=0, next_attempt_at=now(), lease_token=NULL,
		    locked_until=NULL, ack=NULL, updated_at=now()
		WHERE id=$1 AND customer_id=$2 AND state='dead_letter'
		RETURNING `+connectorCommandStatusColumns, id, customerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ConnectorCommandStatus{}, ErrConnectorCommandNotDeadLettered
	} else if err != nil {
		return ConnectorCommandStatus{}, fmt.Errorf("state: requeue connector command: %w", err)
	}
	if err := insertAuditTx(ctx, tx, ev); err != nil {
		return ConnectorCommandStatus{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ConnectorCommandStatus{}, fmt.Errorf("state: commit connector command requeue: %w", err)
	}
	return status, nil
}

func (p *pgPersister) listConnectorCommandsForWorkload(ctx context.Context, customerID, workloadID string) ([]ConnectorCommandStatus, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT `+connectorCommandStatusColumns+`
		FROM connector_commands
		WHERE customer_id=$1 AND workload_id=$2
		ORDER BY created_at, id`, customerID, workloadID)
	if err != nil {
		return nil, fmt.Errorf("state: list workload connector commands: %w", err)
	}
	defer rows.Close()
	var out []ConnectorCommandStatus
	for rows.Next() {
		status, err := scanConnectorCommandStatus(rows)
		if err != nil {
			return nil, fmt.Errorf("state: scan workload connector command: %w", err)
		}
		out = append(out, status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate workload connector commands: %w", err)
	}
	return out, nil
}
