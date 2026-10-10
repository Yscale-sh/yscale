package lifecycle

import (
	"context"
	"fmt"
	"time"
)

// MarkProviderDeleteFailed schedules another attempt or moves the delete into
// durable manual attention. Neither path terminalizes the burst or stamps a
// delete receipt: the provider resource may still exist and continues to count
// as a live-cost obligation.
func (s *Store) MarkProviderDeleteFailed(ctx context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	if id <= 0 || leaseToken == "" || !retryAt.After(time.Now()) || maxAttempts < 1 || maxAttempts > 100 {
		return fmt.Errorf("%w: delete ID, lease, future retry, and max attempts 1..100 are required", ErrInvalidArgument)
	}
	safeError = truncateUTF8(safeError, maxSafeErrorBytes)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("lifecycle: begin provider-delete failure: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	operation, err := lockProviderDelete(ctx, tx, id, leaseToken)
	if err != nil {
		return err
	}
	var burstState, burstProviderResourceID string
	if err := tx.QueryRow(ctx, `
		SELECT state, provider_resource_id
		FROM lifecycle.bursts
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3
		FOR UPDATE`, operation.CustomerID, operation.ClusterID, operation.BurstID).
		Scan(&burstState, &burstProviderResourceID); err != nil {
		return fmt.Errorf("lifecycle: lock burst for provider-delete failure: %w", err)
	}
	if burstState != BurstProviderCreated || burstProviderResourceID != operation.ProviderResourceID {
		return fmt.Errorf("%w: burst %s provider identity changed during delete", ErrInvariantViolation, operation.BurstID)
	}
	nextState := ProviderDeleteRetrying
	if operation.Attempts >= maxAttempts {
		nextState = ProviderDeleteManualAttention
	}
	if _, err := tx.Exec(ctx, `
		UPDATE lifecycle.provider_deletes
		SET state=$1, next_attempt_at=$2, last_error=$3,
		    locked_until=NULL, lease_token=NULL, updated_at=now()
		WHERE id=$4`, nextState, retryAt.UTC(), safeError, id); err != nil {
		return mapWriteError("mark provider-delete failed", err)
	}
	if tag, err := tx.Exec(ctx, `
		UPDATE lifecycle.bursts
		SET last_error=$1, updated_at=now()
		WHERE customer_id=$2 AND cluster_id=$3 AND id=$4 AND state='provider_created'`,
		safeError, operation.CustomerID, operation.ClusterID, operation.BurstID); err != nil {
		return mapWriteError("record provider-delete error on burst", err)
	} else if tag.RowsAffected() != 1 {
		return ErrInvariantViolation
	}
	details, err := eventPayload(map[string]any{"error": safeError})
	if err != nil {
		return err
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID:    operation.CustomerID,
		ClusterID:     operation.ClusterID,
		AggregateType: "operation",
		AggregateID:   providerDeleteEventID(id),
		EventType:     "provider_delete." + nextState,
		PriorState:    operation.State,
		NewState:      nextState,
		Payload:       details,
	}); err != nil {
		return err
	}
	return commit(ctx, tx, "provider-delete failure")
}
