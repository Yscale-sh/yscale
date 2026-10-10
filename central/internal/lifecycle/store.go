package lifecycle

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxIdentifierBytes = 255
	maxPayloadBytes    = 1 << 20
	maxSafeErrorBytes  = 1024
	maxLeaseDuration   = 15 * time.Minute
	outboxPayloadV1    = 1
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// AdmitWorkload atomically records the canonical workload, requested burst,
// pending provider-create operation, outbox event and lifecycle event.
func (s *Store) AdmitWorkload(ctx context.Context, req AdmissionRequest) (AdmissionResponse, error) {
	if err := s.assertReady(); err != nil {
		return AdmissionResponse{}, err
	}
	outboxPayloadProvided := len(req.OutboxPayload) > 0
	req, err := normalizeAdmission(req)
	if err != nil {
		return AdmissionResponse{}, err
	}
	payloadHash := admissionPayloadHash(req, outboxPayloadProvided)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AdmissionResponse{}, fmt.Errorf("lifecycle: begin admission: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := validateStoredAdmissionPayloads(ctx, tx, req); err != nil {
		return AdmissionResponse{}, err
	}

	var workloadID, burstID string
	err = tx.QueryRow(ctx, `
		INSERT INTO lifecycle.workloads
		(id, customer_id, cluster_id, idempotency_key, canonical_version, payload_hash, burst_id, spec)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
		ON CONFLICT (customer_id, cluster_id, idempotency_key) DO NOTHING
		RETURNING id, burst_id`,
		req.WorkloadID, req.CustomerID, req.ClusterID, req.IdempotencyKey,
		req.CanonicalVersion, payloadHash, req.BurstID, string(req.WorkloadSpec)).
		Scan(&workloadID, &burstID)
	if errors.Is(err, pgx.ErrNoRows) {
		var existingVersion int
		var existingHash string
		err = tx.QueryRow(ctx, `
			SELECT id, burst_id, canonical_version, payload_hash
			FROM lifecycle.workloads
			WHERE customer_id=$1 AND cluster_id=$2 AND idempotency_key=$3`,
			req.CustomerID, req.ClusterID, req.IdempotencyKey).
			Scan(&workloadID, &burstID, &existingVersion, &existingHash)
		if errors.Is(err, pgx.ErrNoRows) {
			return AdmissionResponse{}, ErrInvariantViolation
		}
		if err != nil {
			return AdmissionResponse{}, fmt.Errorf("lifecycle: read idempotency key: %w", err)
		}
		if existingVersion != req.CanonicalVersion || existingHash != payloadHash {
			return AdmissionResponse{}, ErrIdempotencyConflict
		}
		return AdmissionResponse{WorkloadID: workloadID, BurstID: burstID}, nil
	}
	if err != nil {
		return AdmissionResponse{}, mapWriteError("claim admission idempotency key", err)
	}

	// cloud_account_id and the canonical region are written HERE, at admission,
	// because this row is the one the authoritative provider-delete projection
	// re-reads and holds a later reap to. A burst admitted without its BYOC
	// account — or under a different spelling of "no region" — projects an
	// identity that delete cannot match, and the tenant's paid machine is then
	// refused a delete with ErrIdentityConflict. See projectBookingTx.
	if _, err := tx.Exec(ctx, `
		INSERT INTO lifecycle.bursts
		(id, customer_id, cluster_id, workload_id, provider, region, cloud_account_id, sku, request)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)`,
		req.BurstID, req.CustomerID, req.ClusterID, req.WorkloadID,
		req.Provider, req.Region, req.CloudAccountID, req.SKU, string(req.BurstSpec)); err != nil {
		return AdmissionResponse{}, mapWriteError("insert burst", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO lifecycle.operations
		(customer_id, cluster_id, workload_id, burst_id, operation_type, semantic_key,
		 provider, region, cloud_account_id, sku, payload_version, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)`,
		req.CustomerID, req.ClusterID, req.WorkloadID, req.BurstID,
		OperationProviderCreate, providerCreateKey(req.BurstID),
		req.Provider, req.Region, req.CloudAccountID, req.SKU, req.CanonicalVersion,
		string(req.ProviderCreatePayload)); err != nil {
		return AdmissionResponse{}, mapWriteError("insert provider-create operation", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO lifecycle.outbox
		(customer_id, cluster_id, aggregate_type, aggregate_id, event_key, event_type,
		 payload_version, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`,
		req.CustomerID, req.ClusterID, "workload", req.WorkloadID,
		workloadAdmittedEventKey(req.WorkloadID), "workload.admitted",
		outboxPayloadV1, string(req.OutboxPayload)); err != nil {
		return AdmissionResponse{}, mapWriteError("insert outbox event", err)
	}
	details, err := eventPayload(map[string]any{"burst_id": req.BurstID})
	if err != nil {
		return AdmissionResponse{}, err
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: req.CustomerID, ClusterID: req.ClusterID,
		AggregateType: "workload", AggregateID: req.WorkloadID,
		EventType: "workload.admitted", PriorState: "", NewState: WorkloadAdmitted,
		Actor: req.Actor, TraceID: req.TraceID, Payload: details,
	}); err != nil {
		return AdmissionResponse{}, err
	}
	if err := commit(ctx, tx, "admission"); err != nil {
		return AdmissionResponse{}, err
	}
	return AdmissionResponse{WorkloadID: workloadID, BurstID: burstID, Inserted: true}, nil
}

// ClaimProviderCreate leases one due provider-create operation for a background
// drain. Only pending and failed operations are due.
//
// An expired `processing` lease is NOT reclaimed, here or in the scoped claim.
// The lease is taken before the request is dispatched, so an attempt that
// stopped renewing it may have died after a provider accepted the create;
// handing that operation to a second worker means a second CreateNode for a
// burst that may already own a machine, and no backend central routes to offers
// an idempotency key that would collapse the two. The operation is left in
// `processing` for an operator, which is the outcome a human can still fix —
// unlike a silently duplicated machine that no burst record reaps.
//
// pending and failed are retried because both PROVE no resource was created:
// pending was never dispatched, and failed is written by a settlement whose
// attempt reported its create did not land.
func (s *Store) ClaimProviderCreate(ctx context.Context, lease time.Duration) (ProviderCreateOperation, bool, error) {
	if err := s.assertReady(); err != nil {
		return ProviderCreateOperation{}, false, err
	}
	if err := validateLease(lease); err != nil {
		return ProviderCreateOperation{}, false, err
	}
	token, err := randomToken()
	if err != nil {
		return ProviderCreateOperation{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ProviderCreateOperation{}, false, fmt.Errorf("lifecycle: begin provider-create claim: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var op ProviderCreateOperation
	var priorState string
	var payloadText string
	err = tx.QueryRow(ctx, `
		WITH candidate AS (
		    SELECT id, state FROM lifecycle.operations
		    WHERE operation_type='provider_create'
		      AND state IN ('pending','failed')
		      AND next_attempt_at <= now()
		    ORDER BY next_attempt_at, id
		    FOR UPDATE SKIP LOCKED LIMIT 1
		)
		UPDATE lifecycle.operations AS o
		SET state='processing', attempts=o.attempts + 1,
		    locked_until=now()+make_interval(secs => $1), lease_token=$2,
		    updated_at=now()
		FROM candidate WHERE o.id=candidate.id
		RETURNING o.id, o.customer_id, o.cluster_id, o.workload_id, o.burst_id,
		          o.provider, o.region, o.cloud_account_id, o.sku, o.payload_version, o.payload::text,
		          o.state, o.attempts, o.lease_token, o.locked_until,
		          o.next_attempt_at, candidate.state`,
		lease.Seconds(), token).
		Scan(&op.ID, &op.CustomerID, &op.ClusterID, &op.WorkloadID, &op.BurstID,
			&op.Provider, &op.Region, &op.CloudAccountID, &op.SKU, &op.PayloadVersion, &payloadText,
			&op.State, &op.Attempts, &op.LeaseToken, &op.LockedUntil,
			&op.NextAttemptAt, &priorState)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderCreateOperation{}, false, nil
	}
	if err != nil {
		return ProviderCreateOperation{}, false, fmt.Errorf("lifecycle: claim provider-create operation: %w", err)
	}
	op.Payload = []byte(payloadText)
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: op.CustomerID, ClusterID: op.ClusterID,
		AggregateType: "operation", AggregateID: fmt.Sprint(op.ID),
		EventType: "provider_create.claimed", PriorState: priorState,
		NewState: OperationProcessing, Payload: []byte(`{}`),
	}); err != nil {
		return ProviderCreateOperation{}, false, err
	}
	if err := commit(ctx, tx, "provider-create claim"); err != nil {
		return ProviderCreateOperation{}, false, err
	}
	return op, true, nil
}

// MarkProviderCreateSucceeded fences completion on the current unexpired lease
// and commits the burst state transition in the same transaction.
func (s *Store) MarkProviderCreateSucceeded(ctx context.Context, id int64, leaseToken, providerResourceID string) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	if id <= 0 || leaseToken == "" {
		return fmt.Errorf("%w: operation ID and lease token required", ErrInvalidArgument)
	}
	if err := validateIdentifier("provider resource ID", providerResourceID); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("lifecycle: begin provider-create success: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	op, err := lockProviderOperation(ctx, tx, id, leaseToken)
	if err != nil {
		return err
	}
	if err := guardProviderResourceOwnershipTx(ctx, tx, ProviderResourceRef{
		Provider: op.Provider, CloudAccountID: op.CloudAccountID, ProviderResourceID: providerResourceID,
	}); err != nil {
		return err
	}
	var priorBurstState string
	if err := tx.QueryRow(ctx, `
		SELECT state FROM lifecycle.bursts
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3 FOR UPDATE`,
		op.CustomerID, op.ClusterID, op.BurstID).Scan(&priorBurstState); err != nil {
		return fmt.Errorf("lifecycle: lock burst for provider-create success: %w", err)
	}
	// Either lock can wait past the lease checked above. Use database wall
	// time after both locks, not transaction-start time, before publishing success.
	var leaseLive bool
	if err := tx.QueryRow(ctx, `SELECT locked_until > clock_timestamp()
		FROM lifecycle.operations WHERE id=$1`, id).Scan(&leaseLive); err != nil {
		return fmt.Errorf("lifecycle: recheck provider-create success lease: %w", err)
	}
	if !leaseLive {
		return ErrLeaseLost
	}
	if _, err := tx.Exec(ctx, `
		UPDATE lifecycle.operations
		SET state='succeeded', provider_resource_id=$1, locked_until=NULL,
		    lease_token=NULL, last_error='', updated_at=now(), completed_at=now()
		WHERE id=$2`, providerResourceID, id); err != nil {
		return mapWriteError("mark provider-create succeeded", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE lifecycle.bursts
		SET state='provider_created', provider_resource_id=$1, updated_at=now(),
		    provider_created_at=COALESCE(provider_created_at, now())
		WHERE customer_id=$2 AND cluster_id=$3 AND id=$4`,
		providerResourceID, op.CustomerID, op.ClusterID, op.BurstID); err != nil {
		return mapWriteError("mark burst provider-created", err)
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: op.CustomerID, ClusterID: op.ClusterID,
		AggregateType: "operation", AggregateID: fmt.Sprint(id),
		EventType: "provider_create.succeeded", PriorState: op.State,
		NewState: OperationSucceeded, Payload: []byte(`{}`),
	}); err != nil {
		return err
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: op.CustomerID, ClusterID: op.ClusterID,
		AggregateType: "burst", AggregateID: op.BurstID,
		EventType: "burst.provider_created", PriorState: priorBurstState,
		NewState: BurstProviderCreated, Payload: []byte(`{}`),
	}); err != nil {
		return err
	}
	return commit(ctx, tx, "provider-create success")
}

// MarkProviderCreateFailed schedules a retry or dead-letters the operation.
// Only the current unexpired lease token can make either transition.
func (s *Store) MarkProviderCreateFailed(ctx context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	if id <= 0 || leaseToken == "" || !retryAt.After(time.Now()) || maxAttempts < 1 || maxAttempts > 100 {
		return fmt.Errorf("%w: operation ID, lease, future retry, and max attempts 1..100 are required", ErrInvalidArgument)
	}
	safeError = truncateUTF8(safeError, maxSafeErrorBytes)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("lifecycle: begin provider-create failure: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	op, err := lockProviderOperation(ctx, tx, id, leaseToken)
	if err != nil {
		return err
	}
	errPayload, err := eventPayload(map[string]any{"error": safeError})
	if err != nil {
		return err
	}
	nextState := OperationFailed
	completedAt := any(nil)
	if op.Attempts >= maxAttempts {
		nextState = OperationDeadLetter
		completedAt = time.Now().UTC()
	}
	if _, err := tx.Exec(ctx, `
		UPDATE lifecycle.operations
		SET state=$1, next_attempt_at=$2, last_error=$3, locked_until=NULL,
		    lease_token=NULL, updated_at=now(), completed_at=$4
		WHERE id=$5`,
		nextState, retryAt.UTC(), safeError, completedAt, id); err != nil {
		return mapWriteError("mark provider-create failed", err)
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: op.CustomerID, ClusterID: op.ClusterID,
		AggregateType: "operation", AggregateID: fmt.Sprint(id),
		EventType: "provider_create." + nextState, PriorState: op.State,
		NewState: nextState, Payload: errPayload,
	}); err != nil {
		return err
	}
	if nextState == OperationDeadLetter {
		var priorWorkloadState string
		if err := tx.QueryRow(ctx, `
			SELECT state FROM lifecycle.workloads
			WHERE customer_id=$1 AND cluster_id=$2 AND id=$3 FOR UPDATE`,
			op.CustomerID, op.ClusterID, op.WorkloadID).Scan(&priorWorkloadState); err != nil {
			return fmt.Errorf("lifecycle: lock workload for dead-letter: %w", err)
		}
		if priorWorkloadState != WorkloadFailed && priorWorkloadState != WorkloadSucceeded && priorWorkloadState != WorkloadCancelled {
			if _, err := tx.Exec(ctx, `
				UPDATE lifecycle.workloads
				SET state='failed', terminal_reason=$1, updated_at=now(), terminal_at=now()
				WHERE customer_id=$2 AND cluster_id=$3 AND id=$4`,
				safeError, op.CustomerID, op.ClusterID, op.WorkloadID); err != nil {
				return mapWriteError("mark workload failed on dead-letter", err)
			}
			if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
				CustomerID:    op.CustomerID,
				ClusterID:     op.ClusterID,
				AggregateType: "workload",
				AggregateID:   op.WorkloadID,
				EventType:     "workload.failed",
				PriorState:    priorWorkloadState,
				NewState:      WorkloadFailed,
				Payload:       errPayload,
			}); err != nil {
				return err
			}
		}

		var priorBurstState string
		if err := tx.QueryRow(ctx, `
			SELECT state FROM lifecycle.bursts
			WHERE customer_id=$1 AND cluster_id=$2 AND id=$3 FOR UPDATE`,
			op.CustomerID, op.ClusterID, op.BurstID).Scan(&priorBurstState); err != nil {
			return fmt.Errorf("lifecycle: lock burst for dead-letter: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE lifecycle.bursts
			SET state='manual_attention', last_error=$1, updated_at=now(), terminal_at=now()
			WHERE customer_id=$2 AND cluster_id=$3 AND id=$4`,
			safeError, op.CustomerID, op.ClusterID, op.BurstID); err != nil {
			return mapWriteError("mark burst manual-attention", err)
		}
		if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
			CustomerID: op.CustomerID, ClusterID: op.ClusterID,
			AggregateType: "burst", AggregateID: op.BurstID,
			EventType: "burst.manual_attention", PriorState: priorBurstState,
			NewState: BurstManualAttention, Payload: errPayload,
		}); err != nil {
			return err
		}
	}
	return commit(ctx, tx, "provider-create failure")
}

// ClaimOutboxEvent leases one due PUBLICATION event. A publisher can safely
// publish more than once; acknowledgement is fenced on the durable outbox row.
//
// Provider-delete cleanup repairs share the table and are deliberately excluded:
// they are work for the delete worker, not something to publish, and a publisher
// holding that lease is a confirmed-deleted node whose ledger stays unsettled
// for a lease period. ClaimProviderDeleteCleanup is the same filter inverted, so
// exactly one of the two consumers can ever see a given row.
func (s *Store) ClaimOutboxEvent(ctx context.Context, lease time.Duration) (OutboxEvent, bool, error) {
	return s.claimOutboxEvent(ctx, lease, ProviderDeleteCleanupEventType, false)
}

// ClaimProviderDeleteCleanup leases one due post-delete cleanup repair. The
// provider resource behind it is already confirmed gone — this is the releases,
// receipts, settlement and node cleanup that outlived the provider call.
func (s *Store) ClaimProviderDeleteCleanup(ctx context.Context, lease time.Duration) (OutboxEvent, bool, error) {
	return s.claimOutboxEvent(ctx, lease, ProviderDeleteCleanupEventType, true)
}

// claimOutboxEvent leases one due row whose event_type either matches
// eventType (only=true) or does not (only=false).
func (s *Store) claimOutboxEvent(ctx context.Context, lease time.Duration, eventType string, only bool) (OutboxEvent, bool, error) {
	if err := s.assertReady(); err != nil {
		return OutboxEvent{}, false, err
	}
	if err := validateLease(lease); err != nil {
		return OutboxEvent{}, false, err
	}
	token, err := randomToken()
	if err != nil {
		return OutboxEvent{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return OutboxEvent{}, false, fmt.Errorf("lifecycle: begin outbox claim: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var event OutboxEvent
	var priorState string
	var payloadText string
	err = tx.QueryRow(ctx, `
		WITH candidate AS (
		    SELECT id, state FROM lifecycle.outbox
		    WHERE ((state IN ('pending','failed') AND next_attempt_at <= now())
		       OR (state='processing' AND locked_until <= now()))
		      AND (event_type = $3) = $4
		    ORDER BY next_attempt_at, id
		    FOR UPDATE SKIP LOCKED LIMIT 1
		)
		UPDATE lifecycle.outbox AS o
		SET state='processing',
		    attempts=o.attempts + CASE WHEN candidate.state IN ('pending','failed') THEN 1 ELSE 0 END,
		    locked_until=now()+make_interval(secs => $1), lease_token=$2,
		    updated_at=now()
		FROM candidate WHERE o.id=candidate.id
		RETURNING o.id, o.customer_id, o.cluster_id, o.aggregate_type, o.aggregate_id,
		          o.event_key, o.event_type, o.payload_version, o.payload::text,
		          o.state, o.attempts, o.lease_token, o.locked_until,
		          o.next_attempt_at, o.created_at, candidate.state`,
		lease.Seconds(), token, eventType, only).
		Scan(&event.ID, &event.CustomerID, &event.ClusterID, &event.AggregateType,
			&event.AggregateID, &event.EventKey, &event.EventType,
			&event.PayloadVersion, &payloadText, &event.State, &event.Attempts,
			&event.LeaseToken, &event.LockedUntil, &event.NextAttemptAt,
			&event.CreatedAt, &priorState)
	if errors.Is(err, pgx.ErrNoRows) {
		return OutboxEvent{}, false, nil
	}
	if err != nil {
		return OutboxEvent{}, false, fmt.Errorf("lifecycle: claim outbox event: %w", err)
	}
	event.Payload = []byte(payloadText)
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: event.CustomerID, ClusterID: event.ClusterID,
		AggregateType: "outbox", AggregateID: fmt.Sprint(event.ID),
		EventType: "outbox.claimed", PriorState: priorState,
		NewState: OutboxProcessing, Payload: []byte(`{}`),
	}); err != nil {
		return OutboxEvent{}, false, err
	}
	if err := commit(ctx, tx, "outbox claim"); err != nil {
		return OutboxEvent{}, false, err
	}
	return event, true, nil
}

func (s *Store) AcknowledgeOutboxEvent(ctx context.Context, id int64, leaseToken string) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	if id <= 0 || leaseToken == "" {
		return fmt.Errorf("%w: event ID and lease token required", ErrInvalidArgument)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("lifecycle: begin outbox acknowledgement: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	event, err := lockOutboxEvent(ctx, tx, id, leaseToken)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE lifecycle.outbox
		SET state='acknowledged', locked_until=NULL, lease_token=NULL,
		    last_error='', updated_at=now(), acknowledged_at=now()
		WHERE id=$1`, id); err != nil {
		return mapWriteError("acknowledge outbox event", err)
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: event.CustomerID, ClusterID: event.ClusterID,
		AggregateType: "outbox", AggregateID: fmt.Sprint(id),
		EventType: "outbox.acknowledged", PriorState: event.State,
		NewState: OutboxAcknowledged, Payload: []byte(`{}`),
	}); err != nil {
		return err
	}
	return commit(ctx, tx, "outbox acknowledgement")
}

func (s *Store) MarkOutboxFailed(ctx context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	if id <= 0 || leaseToken == "" || !retryAt.After(time.Now()) || maxAttempts < 1 || maxAttempts > 100 {
		return fmt.Errorf("%w: event ID, lease, future retry, and max attempts 1..100 are required", ErrInvalidArgument)
	}
	safeError = truncateUTF8(safeError, maxSafeErrorBytes)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("lifecycle: begin outbox failure: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	event, err := lockOutboxEvent(ctx, tx, id, leaseToken)
	if err != nil {
		return err
	}
	errPayload, err := eventPayload(map[string]any{"error": safeError})
	if err != nil {
		return err
	}
	nextState := OutboxFailed
	acknowledgedAt := any(nil)
	if event.Attempts >= maxAttempts {
		nextState = OutboxDeadLetter
		acknowledgedAt = time.Now().UTC()
	}
	if _, err := tx.Exec(ctx, `
		UPDATE lifecycle.outbox
		SET state=$1, next_attempt_at=$2, last_error=$3, locked_until=NULL,
		    lease_token=NULL, updated_at=now(), acknowledged_at=$4
		WHERE id=$5`,
		nextState, retryAt.UTC(), safeError, acknowledgedAt, id); err != nil {
		return mapWriteError("mark outbox failed", err)
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: event.CustomerID, ClusterID: event.ClusterID,
		AggregateType: "outbox", AggregateID: fmt.Sprint(id),
		EventType: "outbox." + nextState, PriorState: event.State,
		NewState: nextState, Payload: errPayload,
	}); err != nil {
		return err
	}
	return commit(ctx, tx, "outbox failure")
}

type lockedOperation struct {
	ID             int64
	CustomerID     string
	ClusterID      string
	WorkloadID     string
	BurstID        string
	State          string
	Attempts       int
	Provider       string
	CloudAccountID string
}

func lockProviderOperation(ctx context.Context, tx pgx.Tx, id int64, leaseToken string) (lockedOperation, error) {
	var op lockedOperation
	err := tx.QueryRow(ctx, `
		SELECT id, customer_id, cluster_id, workload_id, burst_id, state, attempts, provider, cloud_account_id
		FROM lifecycle.operations
		WHERE id=$1 AND operation_type='provider_create' AND state='processing'
		  AND lease_token=$2 AND locked_until > now()
		FOR UPDATE`, id, leaseToken).
		Scan(&op.ID, &op.CustomerID, &op.ClusterID, &op.WorkloadID, &op.BurstID,
			&op.State, &op.Attempts, &op.Provider, &op.CloudAccountID)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedOperation{}, ErrLeaseLost
	}
	if err != nil {
		return lockedOperation{}, fmt.Errorf("lifecycle: lock provider-create operation: %w", err)
	}
	return op, nil
}

type lockedOutbox struct {
	ID         int64
	CustomerID string
	ClusterID  string
	State      string
	Attempts   int
}

func lockOutboxEvent(ctx context.Context, tx pgx.Tx, id int64, leaseToken string) (lockedOutbox, error) {
	var event lockedOutbox
	err := tx.QueryRow(ctx, `
		SELECT id, customer_id, cluster_id, state, attempts
		FROM lifecycle.outbox
		WHERE id=$1 AND state='processing' AND lease_token=$2 AND locked_until > now()
		FOR UPDATE`, id, leaseToken).
		Scan(&event.ID, &event.CustomerID, &event.ClusterID, &event.State, &event.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedOutbox{}, ErrLeaseLost
	}
	if err != nil {
		return lockedOutbox{}, fmt.Errorf("lifecycle: lock outbox event: %w", err)
	}
	return event, nil
}

type lifecycleEvent struct {
	CustomerID    string
	ClusterID     string
	AggregateType string
	AggregateID   string
	EventType     string
	PriorState    string
	NewState      string
	Actor         string
	TraceID       string
	Payload       []byte
}

func insertLifecycleEvent(ctx context.Context, tx pgx.Tx, event lifecycleEvent) error {
	if len(event.Payload) == 0 {
		event.Payload = []byte(`{}`)
	}
	if err := validateJSONPayload("lifecycle event payload", event.Payload); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO lifecycle.lifecycle_events
		(customer_id, cluster_id, aggregate_type, aggregate_id, event_type,
		 prior_state, new_state, actor, trace_id, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)`,
		event.CustomerID, event.ClusterID, event.AggregateType, event.AggregateID,
		event.EventType, event.PriorState, event.NewState, event.Actor,
		event.TraceID, string(event.Payload)); err != nil {
		return mapWriteError("insert lifecycle event", err)
	}
	return nil
}

func normalizeAdmission(req AdmissionRequest) (AdmissionRequest, error) {
	// One canonical region for the whole aggregate. The provider-delete
	// projection derives its region the same way from the booked burst, so a
	// workload that pinned none admits and deletes under the identical value
	// rather than under two different stand-ins for "unspecified".
	req.Region = CanonicalProviderDeleteRegion(req.Region)
	req.CloudAccountID = strings.TrimSpace(req.CloudAccountID)
	for label, value := range map[string]string{
		"customer ID": req.CustomerID, "cluster ID": req.ClusterID,
		"idempotency key": req.IdempotencyKey, "workload ID": req.WorkloadID,
		"burst ID": req.BurstID, "provider": req.Provider,
		"region": req.Region, "SKU": req.SKU,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return AdmissionRequest{}, err
		}
	}
	// Empty is the platform-funded backend, exactly as it is on a booking. When
	// present it is held to the schema's bound and no further: a cloud-account
	// id is DATA the accounts layer minted, and the projection this must agree
	// with validates it the same way.
	if req.CloudAccountID != "" {
		if err := validateBookingField("cloud account ID", req.CloudAccountID); err != nil {
			return AdmissionRequest{}, err
		}
	}
	if req.Actor != "" {
		if err := validateIdentifier("actor", req.Actor); err != nil {
			return AdmissionRequest{}, err
		}
	}
	if req.TraceID != "" {
		if err := validateIdentifier("trace ID", req.TraceID); err != nil {
			return AdmissionRequest{}, err
		}
	}
	if req.CanonicalVersion <= 0 || req.CanonicalVersion > 32767 {
		return AdmissionRequest{}, fmt.Errorf("%w: canonical version must be 1..32767", ErrInvalidArgument)
	}
	for label, payload := range map[string][]byte{
		"workload spec":           req.WorkloadSpec,
		"burst spec":              req.BurstSpec,
		"provider-create payload": req.ProviderCreatePayload,
	} {
		if err := validateJSONPayload(label, payload); err != nil {
			return AdmissionRequest{}, err
		}
	}
	if len(req.OutboxPayload) == 0 {
		payload, err := eventPayload(map[string]any{
			"workload_id": req.WorkloadID,
			"burst_id":    req.BurstID,
		})
		if err != nil {
			return AdmissionRequest{}, err
		}
		req.OutboxPayload = payload
	}
	if err := validateJSONPayload("outbox payload", req.OutboxPayload); err != nil {
		return AdmissionRequest{}, err
	}
	return req, nil
}

func validateJSONPayload(label string, payload []byte) error {
	if len(payload) == 0 || len(payload) > maxPayloadBytes {
		return fmt.Errorf("%w: %s must be 1..1048576 bytes", ErrInvalidArgument, label)
	}
	if !utf8.Valid(payload) {
		return fmt.Errorf("%w: %s must be valid UTF-8", ErrInvalidArgument, label)
	}
	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil {
		return fmt.Errorf("%w: %s must be a JSON object", ErrInvalidArgument, label)
	}
	if object == nil {
		return fmt.Errorf("%w: %s must be a JSON object", ErrInvalidArgument, label)
	}
	return nil
}

// validateStoredAdmissionPayloads applies the database's jsonb normalization
// before checking the storage limit. PostgreSQL adds formatting while
// rendering jsonb::text, so raw request length alone is not an exact bound for
// the CHECK constraints below the package seam.
func validateStoredAdmissionPayloads(ctx context.Context, tx pgx.Tx, req AdmissionRequest) error {
	var workloadBytes, burstBytes, operationBytes, outboxBytes int
	if err := tx.QueryRow(ctx, `
		SELECT octet_length($1::jsonb::text), octet_length($2::jsonb::text),
		       octet_length($3::jsonb::text), octet_length($4::jsonb::text)`,
		string(req.WorkloadSpec), string(req.BurstSpec),
		string(req.ProviderCreatePayload), string(req.OutboxPayload)).
		Scan(&workloadBytes, &burstBytes, &operationBytes, &outboxBytes); err != nil {
		return fmt.Errorf("lifecycle: validate stored payload sizes: %w", err)
	}
	for label, size := range map[string]int{
		"workload spec": workloadBytes, "burst spec": burstBytes,
		"provider-create payload": operationBytes, "outbox payload": outboxBytes,
	} {
		if size > maxPayloadBytes {
			return fmt.Errorf("%w: %s exceeds 1048576 bytes after JSON normalization", ErrInvalidArgument, label)
		}
	}
	return nil
}

func validateLease(lease time.Duration) error {
	if lease < time.Second || lease > maxLeaseDuration {
		return fmt.Errorf("%w: lease must be 1s..15m", ErrInvalidArgument)
	}
	return nil
}

func admissionPayloadHash(req AdmissionRequest, outboxPayloadProvided bool) string {
	h := sha256.New()
	writeHashPart(h, []byte(fmt.Sprint(req.CanonicalVersion)))
	writeHashPart(h, req.WorkloadSpec)
	writeHashPart(h, req.BurstSpec)
	writeHashPart(h, []byte(req.Provider))
	writeHashPart(h, []byte(req.Region))
	// The cloud account is part of the admitted identity, not decoration: a
	// replay that keeps every payload byte but names a different account is a
	// create routed to different credentials, and must be an idempotency
	// conflict rather than a silent reuse of the first admission.
	writeHashPart(h, []byte(req.CloudAccountID))
	writeHashPart(h, []byte(req.SKU))
	writeHashPart(h, req.ProviderCreatePayload)
	if outboxPayloadProvided {
		writeHashPart(h, []byte{1})
		writeHashPart(h, req.OutboxPayload)
	} else {
		writeHashPart(h, []byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

type hashWriter interface {
	Write([]byte) (int, error)
}

func writeHashPart(h hashWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(value)
}

func eventPayload(value map[string]any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: marshal event payload: %w", err)
	}
	return payload, nil
}

func providerCreateKey(burstID string) string {
	return "provider_create:" + burstID
}

func workloadAdmittedEventKey(workloadID string) string {
	return "workload_admitted:" + workloadID
}

func (s *Store) assertReady() error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("%w: nil lifecycle store", ErrInvalidArgument)
	}
	return nil
}

func randomToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("lifecycle: create lease token: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func truncateUTF8(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	value = strings.ReplaceAll(value, "\x00", "\uFFFD")
	value = strings.ToValidUTF8(value, "\uFFFD")
	if len(value) <= maxBytes {
		return value
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}

func mapWriteError(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return fmt.Errorf("%w: %s", ErrIdentityConflict, operation)
		case "23503", "23514", "22P02":
			return fmt.Errorf("%w: %s", ErrInvariantViolation, operation)
		}
	}
	return fmt.Errorf("lifecycle: %s: %w", operation, err)
}

func commit(ctx context.Context, tx pgx.Tx, operation string) error {
	if err := tx.Commit(ctx); err != nil {
		return mapWriteError("commit "+operation, err)
	}
	return nil
}

// GetBurstProviderCreatedAt reads the truthful provider-creation timestamp
// for a burst. Returns nil when the column is NULL (not yet confirmed).
func (s *Store) GetBurstProviderCreatedAt(ctx context.Context, customerID, clusterID, burstID string) (*time.Time, error) {
	if err := s.assertReady(); err != nil {
		return nil, err
	}
	var t *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT provider_created_at FROM lifecycle.bursts
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3`,
		customerID, clusterID, burstID).Scan(&t)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lifecycle: read burst provider_created_at: %w", err)
	}
	return t, nil
}
