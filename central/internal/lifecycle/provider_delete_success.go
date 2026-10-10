package lifecycle

import (
	"context"
	"fmt"
	"time"
)

// MarkProviderDeleteSucceeded is the only transition that terminalizes the
// burst. Provider deletion, the delete receipt, and the burst's terminal state
// commit together, so cost accrual cannot stop on queue admission or retries.
//
// It commits the instant the provider confirms absence, and NOT after the
// cleanup behind it. The resource is gone at that point: leaving the operation
// nonterminal until a receipt, a settlement or a node drain also succeeded
// would let one of those failures push a burst that no longer exists back into
// retrying or manual_attention — that is, keep charging for it. So the same
// transaction enqueues the outstanding cleanup as an outbox repair carrying the
// operation's own immutable teardown payload. Provider absence is recorded
// exactly once; the work behind it is durable, leased and retried on its own.
func (s *Store) MarkProviderDeleteSucceeded(ctx context.Context, id int64, leaseToken string) (time.Time, error) {
	if err := s.assertReady(); err != nil {
		return time.Time{}, err
	}
	if id <= 0 || leaseToken == "" {
		return time.Time{}, fmt.Errorf("%w: delete ID and lease token required", ErrInvalidArgument)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: begin provider-delete success: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	operation, err := lockProviderDelete(ctx, tx, id, leaseToken)
	if err != nil {
		return time.Time{}, err
	}
	var priorBurstState, burstProvider, burstRegion, burstCloudAccountID, burstSKU, burstProviderResourceID string
	if err := tx.QueryRow(ctx, `
		SELECT state, provider, region, cloud_account_id, sku, provider_resource_id
		FROM lifecycle.bursts
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3
		FOR UPDATE`, operation.CustomerID, operation.ClusterID, operation.BurstID).
		Scan(&priorBurstState, &burstProvider, &burstRegion, &burstCloudAccountID,
			&burstSKU, &burstProviderResourceID); err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: lock burst for provider-delete success: %w", err)
	}
	if priorBurstState != BurstProviderCreated || burstProvider != operation.Provider ||
		burstRegion != operation.Region || burstCloudAccountID != operation.CloudAccountID ||
		burstSKU != operation.SKU || burstProviderResourceID != operation.ProviderResourceID {
		return time.Time{}, fmt.Errorf("%w: burst %s provider identity changed during delete", ErrInvariantViolation, operation.BurstID)
	}

	var deletedAt time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE lifecycle.provider_deletes
		SET state='terminated', locked_until=NULL, lease_token=NULL,
		    last_error='', updated_at=now(), deleted_at=now()
		WHERE id=$1
		RETURNING deleted_at`, id).Scan(&deletedAt); err != nil {
		return time.Time{}, mapWriteError("mark provider-delete succeeded", err)
	}
	if tag, err := tx.Exec(ctx, `
		UPDATE lifecycle.bursts
		SET state='terminated', last_error='', updated_at=now(), terminal_at=now()
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3 AND state='provider_created'`,
		operation.CustomerID, operation.ClusterID, operation.BurstID); err != nil {
		return time.Time{}, mapWriteError("terminalize burst after provider delete", err)
	} else if tag.RowsAffected() != 1 {
		return time.Time{}, ErrInvariantViolation
	}
	// The cleanup repair, in the SAME transaction as the receipt. ON CONFLICT DO
	// NOTHING because the event key is derived from the operation: a replay that
	// somehow reached here again must not enqueue the cleanup twice.
	cleanupKey := providerDeleteCleanupEventKey(id)
	if _, err := tx.Exec(ctx, `
		INSERT INTO lifecycle.outbox
		(customer_id, cluster_id, aggregate_type, aggregate_id, event_key, event_type,
		 payload_version, payload)
		VALUES ($1,$2,'operation',$3,$4,$5,$6,$7::jsonb)
		ON CONFLICT (customer_id, cluster_id, event_key) DO NOTHING`,
		operation.CustomerID, operation.ClusterID, providerDeleteEventID(id),
		cleanupKey, ProviderDeleteCleanupEventType, outboxPayloadV1,
		operation.Payload); err != nil {
		return time.Time{}, mapWriteError("enqueue provider-delete cleanup repair", err)
	}

	details, err := eventPayload(map[string]any{
		"provider_resource_id": operation.ProviderResourceID,
		"cleanup_event_key":    cleanupKey,
	})
	if err != nil {
		return time.Time{}, err
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID:    operation.CustomerID,
		ClusterID:     operation.ClusterID,
		AggregateType: "operation",
		AggregateID:   providerDeleteEventID(id),
		EventType:     "provider_delete.terminated",
		PriorState:    operation.State,
		NewState:      ProviderDeleteTerminated,
		Payload:       details,
	}); err != nil {
		return time.Time{}, err
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID:    operation.CustomerID,
		ClusterID:     operation.ClusterID,
		AggregateType: "burst",
		AggregateID:   operation.BurstID,
		EventType:     "burst.terminated",
		PriorState:    priorBurstState,
		NewState:      BurstTerminated,
		Payload:       details,
	}); err != nil {
		return time.Time{}, err
	}
	if err := commit(ctx, tx, "provider-delete success"); err != nil {
		return time.Time{}, err
	}
	return deletedAt.UTC(), nil
}
