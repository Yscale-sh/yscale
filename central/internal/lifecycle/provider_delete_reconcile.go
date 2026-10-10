package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RetryProviderDelete is the explicit operator reconciliation path. It moves a
// manual-attention record back to the queue, starts a new attempt generation,
// and preserves the previous failure in the append-only lifecycle event.
func (s *Store) RetryProviderDelete(ctx context.Context, customerID, clusterID, burstID, actor, traceID string) (ProviderDeleteRecord, error) {
	if err := s.assertReady(); err != nil {
		return ProviderDeleteRecord{}, err
	}
	for label, value := range map[string]string{
		"customer ID": customerID,
		"cluster ID":  clusterID,
		"burst ID":    burstID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return ProviderDeleteRecord{}, err
		}
	}
	if actor != "" {
		if err := validateIdentifier("actor", actor); err != nil {
			return ProviderDeleteRecord{}, err
		}
	}
	if traceID != "" {
		if err := validateIdentifier("trace ID", traceID); err != nil {
			return ProviderDeleteRecord{}, err
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ProviderDeleteRecord{}, fmt.Errorf("lifecycle: begin provider-delete retry: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	current, err := scanProviderDelete(tx.QueryRow(ctx, providerDeleteSelect+`
		WHERE customer_id=$1 AND cluster_id=$2 AND burst_id=$3
		FOR UPDATE`, customerID, clusterID, burstID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderDeleteRecord{}, ErrNotFound
	}
	if err != nil {
		return ProviderDeleteRecord{}, fmt.Errorf("lifecycle: lock provider-delete retry: %w", err)
	}
	if current.State == ProviderDeleteTerminated {
		return ProviderDeleteRecord{}, fmt.Errorf("%w: provider delete already terminated", ErrInvariantViolation)
	}
	if current.State != ProviderDeleteManualAttention {
		return redactProviderDeleteForRead(current), nil
	}
	var burstState, burstProvider, burstRegion, burstCloudAccountID, burstSKU, burstProviderResourceID string
	if err := tx.QueryRow(ctx, `
		SELECT state, provider, region, cloud_account_id, sku, provider_resource_id
		FROM lifecycle.bursts
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3
		FOR UPDATE`, customerID, clusterID, burstID).
		Scan(&burstState, &burstProvider, &burstRegion, &burstCloudAccountID,
			&burstSKU, &burstProviderResourceID); err != nil {
		return ProviderDeleteRecord{}, fmt.Errorf("lifecycle: lock burst for provider-delete retry: %w", err)
	}
	if burstState != BurstProviderCreated || burstProvider != current.Provider ||
		burstRegion != current.Region || burstCloudAccountID != current.CloudAccountID ||
		burstSKU != current.SKU || burstProviderResourceID != current.ProviderResourceID {
		return ProviderDeleteRecord{}, fmt.Errorf("%w: burst %s provider identity changed during retry", ErrInvariantViolation, burstID)
	}
	priorError := current.LastError

	if _, err := tx.Exec(ctx, `
		UPDATE lifecycle.provider_deletes
		SET state='queued', generation=generation+1, attempts=0,
		    next_attempt_at=now(), locked_until=NULL, lease_token=NULL,
		    last_error='', updated_at=now()
		WHERE id=$1`, current.ID); err != nil {
		return ProviderDeleteRecord{}, mapWriteError("retry provider-delete operation", err)
	}
	if tag, err := tx.Exec(ctx, `
		UPDATE lifecycle.bursts
		SET last_error='', updated_at=now()
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3 AND state='provider_created'`,
		customerID, clusterID, burstID); err != nil {
		return ProviderDeleteRecord{}, mapWriteError("clear provider-delete error on burst", err)
	} else if tag.RowsAffected() != 1 {
		return ProviderDeleteRecord{}, ErrInvariantViolation
	}
	details, err := eventPayload(map[string]any{
		"previous_error": priorError,
		"generation":     current.Generation + 1,
	})
	if err != nil {
		return ProviderDeleteRecord{}, err
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID:    customerID,
		ClusterID:     clusterID,
		AggregateType: "operation",
		AggregateID:   providerDeleteEventID(current.ID),
		EventType:     "provider_delete.requeued",
		PriorState:    ProviderDeleteManualAttention,
		NewState:      ProviderDeleteQueued,
		Actor:         actor,
		TraceID:       traceID,
		Payload:       details,
	}); err != nil {
		return ProviderDeleteRecord{}, err
	}
	if err := commit(ctx, tx, "provider-delete retry"); err != nil {
		return ProviderDeleteRecord{}, err
	}
	return s.GetProviderDelete(ctx, customerID, clusterID, burstID)
}

func (s *Store) GetProviderDelete(ctx context.Context, customerID, clusterID, burstID string) (ProviderDeleteRecord, error) {
	if err := s.assertReady(); err != nil {
		return ProviderDeleteRecord{}, err
	}
	for label, value := range map[string]string{
		"customer ID": customerID,
		"cluster ID":  clusterID,
		"burst ID":    burstID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return ProviderDeleteRecord{}, err
		}
	}
	record, err := scanProviderDelete(s.pool.QueryRow(ctx, providerDeleteSelect+`
		WHERE customer_id=$1 AND cluster_id=$2 AND burst_id=$3`,
		customerID, clusterID, burstID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderDeleteRecord{}, ErrNotFound
	}
	if err != nil {
		return ProviderDeleteRecord{}, fmt.Errorf("lifecycle: read provider delete: %w", err)
	}
	return redactProviderDeleteForRead(record), nil
}

// ListProviderDeleteManualAttention exposes the bounded operator reconciliation
// queue. Authentication and tenant redaction belong to the calling handler;
// this package returns only durable lifecycle fields, never provider secrets.
func (s *Store) ListProviderDeleteManualAttention(ctx context.Context, limit int) ([]ProviderDeleteRecord, error) {
	if err := s.assertReady(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 500 {
		return nil, fmt.Errorf("%w: limit must be 1..500", ErrInvalidArgument)
	}
	rows, err := s.pool.Query(ctx, providerDeleteSelect+`
		WHERE state='manual_attention'
		ORDER BY updated_at, id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: list provider-delete manual attention: %w", err)
	}
	defer rows.Close()

	records := make([]ProviderDeleteRecord, 0)
	for rows.Next() {
		record, err := scanProviderDelete(rows)
		if err != nil {
			return nil, fmt.Errorf("lifecycle: scan provider-delete manual attention: %w", err)
		}
		records = append(records, redactProviderDeleteForRead(record))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lifecycle: list provider-delete manual attention: %w", err)
	}
	return records, nil
}

// ProviderDeleteSummary is the tenant-safe list-level projection: only the
// authoritative state and optional deleted_at. No provider internals, secrets,
// account IDs, resource IDs, payload, lease, errors, or reason.
type ProviderDeleteSummary struct {
	State     string
	DeletedAt *time.Time
}

// ProviderDeleteSummaryRef is the exact tenant workload identity used by the
// batch summary reader. Burst IDs are normally globally unique, but the schema
// enforces uniqueness on customer, cluster, and burst together, so callers must
// not collapse two clusters onto a burst-only key.
type ProviderDeleteSummaryRef struct {
	ClusterID string
	BurstID   string
}

// GetProviderDeleteSummaries returns cleanup summaries for the given exact
// cluster/burst references, scoped by customer_id, in a single database call.
// The input is bounded to at most 100 entries and deduplicated before querying.
// References with no provider-delete record are silently absent from the result.
func (s *Store) GetProviderDeleteSummaries(ctx context.Context, customerID string, refs []ProviderDeleteSummaryRef) (map[ProviderDeleteSummaryRef]ProviderDeleteSummary, error) {
	if err := validateIdentifier("customer ID", customerID); err != nil {
		return nil, err
	}
	if len(refs) > 100 {
		return nil, fmt.Errorf("%w: provider-delete summary list must be at most 100 entries", ErrInvalidArgument)
	}
	seen := make(map[ProviderDeleteSummaryRef]struct{}, len(refs))
	unique := make([]ProviderDeleteSummaryRef, 0, len(refs))
	for _, ref := range refs {
		if err := validateIdentifier("cluster ID", ref.ClusterID); err != nil {
			return nil, err
		}
		if err := validateIdentifier("burst ID", ref.BurstID); err != nil {
			return nil, err
		}
		if _, dup := seen[ref]; !dup {
			seen[ref] = struct{}{}
			unique = append(unique, ref)
		}
	}
	if len(unique) == 0 {
		return nil, nil
	}
	if err := s.assertReady(); err != nil {
		return nil, err
	}

	clusterIDs := make([]string, len(unique))
	burstIDs := make([]string, len(unique))
	for i, ref := range unique {
		clusterIDs[i] = ref.ClusterID
		burstIDs[i] = ref.BurstID
	}

	rows, err := s.pool.Query(ctx, `
		WITH requested(cluster_id, burst_id) AS (
			SELECT * FROM unnest($2::text[], $3::text[])
		)
		SELECT d.cluster_id, d.burst_id, d.state, d.deleted_at
		FROM lifecycle.provider_deletes AS d
		JOIN requested AS r
		  ON r.cluster_id = d.cluster_id AND r.burst_id = d.burst_id
		WHERE d.customer_id = $1`,
		customerID, clusterIDs, burstIDs)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: batch read provider-delete summaries: %w", err)
	}
	defer rows.Close()

	result := make(map[ProviderDeleteSummaryRef]ProviderDeleteSummary, len(unique))
	for rows.Next() {
		var ref ProviderDeleteSummaryRef
		var state string
		var deletedAt *time.Time
		if err := rows.Scan(&ref.ClusterID, &ref.BurstID, &state, &deletedAt); err != nil {
			return nil, fmt.Errorf("lifecycle: scan provider-delete summary: %w", err)
		}
		result[ref] = ProviderDeleteSummary{State: state, DeletedAt: deletedAt}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lifecycle: batch read provider-delete summaries: %w", err)
	}
	return result, nil
}
