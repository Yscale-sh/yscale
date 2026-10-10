package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// ClaimProviderDelete leases one due delete operation. Expired deleting leases
// are reclaimable, so worker death cannot strand an owned provider resource.
func (s *Store) ClaimProviderDelete(ctx context.Context, lease time.Duration) (ProviderDeleteRecord, bool, error) {
	if err := s.assertReady(); err != nil {
		return ProviderDeleteRecord{}, false, err
	}
	if err := validateLease(lease); err != nil {
		return ProviderDeleteRecord{}, false, err
	}
	token, err := randomToken()
	if err != nil {
		return ProviderDeleteRecord{}, false, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ProviderDeleteRecord{}, false, fmt.Errorf("lifecycle: begin provider-delete claim: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var record ProviderDeleteRecord
	var payloadText, priorState string
	var leaseToken pgtype.Text
	var lockedUntil, deletedAt pgtype.Timestamptz
	err = tx.QueryRow(ctx, `
		WITH candidate AS (
		    SELECT id, state
		    FROM lifecycle.provider_deletes
		    WHERE ((state IN ('queued','retrying') AND next_attempt_at <= now())
		       OR (state='deleting' AND locked_until <= now()))
		    ORDER BY CASE WHEN state='deleting' THEN locked_until ELSE next_attempt_at END, id
		    FOR UPDATE SKIP LOCKED
		    LIMIT 1
		)
		UPDATE lifecycle.provider_deletes AS d
		SET state='deleting',
		    attempts=d.attempts + CASE WHEN candidate.state IN ('queued','retrying') THEN 1 ELSE 0 END,
		    locked_until=now()+make_interval(secs => $1),
		    lease_token=$2,
		    updated_at=now()
		FROM candidate
		WHERE d.id=candidate.id
		RETURNING d.id, d.customer_id, d.cluster_id, d.workload_id, d.burst_id,
		          d.provider, d.region, d.cloud_account_id, d.sku, d.provider_resource_id, d.reason,
		          d.payload_version, d.payload::text, d.state, d.generation,
		          d.attempts, d.lease_token, d.locked_until, d.next_attempt_at,
		          d.last_error, d.requested_at, d.updated_at, d.deleted_at,
		          candidate.state`, lease.Seconds(), token).
		Scan(&record.ID, &record.CustomerID, &record.ClusterID, &record.WorkloadID,
			&record.BurstID, &record.Provider, &record.Region, &record.CloudAccountID, &record.SKU,
			&record.ProviderResourceID, &record.Reason, &record.PayloadVersion,
			&payloadText, &record.State, &record.Generation, &record.Attempts,
			&leaseToken, &lockedUntil, &record.NextAttemptAt, &record.LastError,
			&record.RequestedAt, &record.UpdatedAt, &deletedAt, &priorState)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderDeleteRecord{}, false, nil
	}
	if err != nil {
		return ProviderDeleteRecord{}, false, fmt.Errorf("lifecycle: claim provider-delete operation: %w", err)
	}
	hydrateProviderDeleteRecord(&record, payloadText, leaseToken, lockedUntil, deletedAt)

	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID:    record.CustomerID,
		ClusterID:     record.ClusterID,
		AggregateType: "operation",
		AggregateID:   providerDeleteEventID(record.ID),
		EventType:     "provider_delete.claimed",
		PriorState:    priorState,
		NewState:      ProviderDeleteDeleting,
		Payload:       []byte(`{}`),
	}); err != nil {
		return ProviderDeleteRecord{}, false, err
	}
	if err := commit(ctx, tx, "provider-delete claim"); err != nil {
		return ProviderDeleteRecord{}, false, err
	}
	return record, true, nil
}
