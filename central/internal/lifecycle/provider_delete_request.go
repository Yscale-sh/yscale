package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// RequestProviderDelete atomically snapshots the provider identity from a
// provider-created burst and records queued delete intent. A replay for the
// same burst returns the existing operation without creating another one.
func (s *Store) RequestProviderDelete(ctx context.Context, req ProviderDeleteRequest) (ProviderDeleteResponse, error) {
	if err := s.assertReady(); err != nil {
		return ProviderDeleteResponse{}, err
	}
	req, err := normalizeProviderDeleteRequest(req)
	if err != nil {
		return ProviderDeleteResponse{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ProviderDeleteResponse{}, fmt.Errorf("lifecycle: begin provider-delete request: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	response, err := requestProviderDeleteTx(ctx, tx, req)
	if err != nil {
		return ProviderDeleteResponse{}, err
	}
	if err := commit(ctx, tx, "provider-delete request"); err != nil {
		return ProviderDeleteResponse{}, err
	}
	return response, nil
}

// requestProviderDeleteTx is the whole delete-intent write with no transaction
// of its own, so a caller that must be atomic WITH it — the booking projection —
// composes rather than clones it. The req it takes is already normalized.
//
// It commits nothing: every caller owns the commit, and a caller that returns
// early on a replay still has to commit whatever it wrote before calling here.
func requestProviderDeleteTx(ctx context.Context, tx pgx.Tx, req ProviderDeleteRequest) (ProviderDeleteResponse, error) {
	var existingID int64
	var existingState string
	err := tx.QueryRow(ctx, `
		SELECT id, state
		FROM lifecycle.provider_deletes
		WHERE customer_id=$1 AND cluster_id=$2 AND burst_id=$3`,
		req.CustomerID, req.ClusterID, req.BurstID).
		Scan(&existingID, &existingState)
	if err == nil {
		return ProviderDeleteResponse{
			DeleteID: existingID,
			BurstID:  req.BurstID,
			State:    existingState,
		}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ProviderDeleteResponse{}, fmt.Errorf("lifecycle: read provider-delete replay: %w", err)
	}
	if err := validateStoredProviderDeletePayload(ctx, tx, req.Payload); err != nil {
		return ProviderDeleteResponse{}, err
	}

	var workloadID, provider, region, cloudAccountID, sku, providerResourceID, burstState string
	err = tx.QueryRow(ctx, `
		SELECT workload_id, provider, region, cloud_account_id, sku, provider_resource_id, state
		FROM lifecycle.bursts
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3
		FOR UPDATE`, req.CustomerID, req.ClusterID, req.BurstID).
		Scan(&workloadID, &provider, &region, &cloudAccountID, &sku, &providerResourceID, &burstState)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderDeleteResponse{}, ErrNotFound
	}
	if err != nil {
		return ProviderDeleteResponse{}, fmt.Errorf("lifecycle: lock burst for provider delete: %w", err)
	}
	if burstState != BurstProviderCreated || strings.TrimSpace(providerResourceID) == "" {
		return ProviderDeleteResponse{}, fmt.Errorf("%w: burst %s is not provider-created", ErrInvariantViolation, req.BurstID)
	}

	var deleteID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO lifecycle.provider_deletes
		(customer_id, cluster_id, workload_id, burst_id, provider, region, cloud_account_id, sku,
		 provider_resource_id, reason, payload_version, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
		ON CONFLICT (customer_id, cluster_id, burst_id) DO NOTHING
		RETURNING id`,
		req.CustomerID, req.ClusterID, workloadID, req.BurstID,
		provider, region, cloudAccountID, sku, providerResourceID, req.Reason,
		providerDeletePayloadV1, string(req.Payload)).Scan(&deleteID)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.QueryRow(ctx, `
			SELECT id, state
			FROM lifecycle.provider_deletes
			WHERE customer_id=$1 AND cluster_id=$2 AND burst_id=$3`,
			req.CustomerID, req.ClusterID, req.BurstID).
			Scan(&existingID, &existingState); err != nil {
			return ProviderDeleteResponse{}, fmt.Errorf("lifecycle: resolve provider-delete race: %w", err)
		}
		return ProviderDeleteResponse{
			DeleteID: existingID,
			BurstID:  req.BurstID,
			State:    existingState,
		}, nil
	}
	if err != nil {
		return ProviderDeleteResponse{}, mapWriteError("insert provider-delete request", err)
	}

	details, err := eventPayload(map[string]any{
		"burst_id":             req.BurstID,
		"provider_resource_id": providerResourceID,
		"reason":               req.Reason,
	})
	if err != nil {
		return ProviderDeleteResponse{}, err
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID:    req.CustomerID,
		ClusterID:     req.ClusterID,
		AggregateType: "operation",
		AggregateID:   providerDeleteEventID(deleteID),
		EventType:     "provider_delete.queued",
		PriorState:    "",
		NewState:      ProviderDeleteQueued,
		Actor:         req.Actor,
		TraceID:       req.TraceID,
		Payload:       details,
	}); err != nil {
		return ProviderDeleteResponse{}, err
	}
	return ProviderDeleteResponse{
		DeleteID: deleteID,
		BurstID:  req.BurstID,
		State:    ProviderDeleteQueued,
		Inserted: true,
	}, nil
}
