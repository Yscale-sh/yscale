package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ProviderReconciliationSucceeded = "succeeded"
	ProviderReconciliationFailed    = "failed"

	ProviderOrphanQuarantined = "quarantined"
	ProviderOrphanDeleting    = "deleting"
	ProviderOrphanDeleted     = "deleted"
)

type ProviderResourceRef struct {
	Provider           string
	CloudAccountID     string
	ProviderResourceID string
}

type ProviderResourceObservation struct {
	Provider           string
	CloudAccountID     string
	ProviderResourceID string
	ResourceName       string
	ProviderCreatedAt  time.Time
	ObservedAt         time.Time
}

type ProviderOrphanRecord struct {
	Provider           string
	CloudAccountID     string
	ProviderResourceID string
	ResourceName       string
	ProviderCreatedAt  time.Time
	State              string
	Observations       int
	Attempts           int
	LeaseToken         string
	LockedUntil        *time.Time
	LastError          string
	FirstObservedAt    time.Time
	LastObservedAt     time.Time
	DeletedAt          *time.Time
}

type ProviderCreateReconciliation struct {
	ID                 int64
	Provider           string
	CloudAccountID     string
	ProviderResourceID string
	ResourceName       string
	BurstID            string
	State              string
	LeaseExpired       bool
}

const providerReconciliationSchema = `
CREATE TABLE IF NOT EXISTS lifecycle.provider_reconciliations (
    id                   BIGSERIAL PRIMARY KEY,
    provider             TEXT NOT NULL CHECK (length(btrim(provider)) BETWEEN 1 AND 255),
    cloud_account_id     TEXT NOT NULL DEFAULT '' CHECK (octet_length(cloud_account_id) <= 255),
    state                TEXT NOT NULL CHECK (state IN ('succeeded','failed')),
    observed             INTEGER NOT NULL DEFAULT 0 CHECK (observed >= 0),
    quarantined          INTEGER NOT NULL DEFAULT 0 CHECK (quarantined >= 0),
    deleted              INTEGER NOT NULL DEFAULT 0 CHECK (deleted >= 0),
    last_error           TEXT NOT NULL DEFAULT '' CHECK (octet_length(last_error) <= 1024),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_lifecycle_provider_reconciliations_provider
    ON lifecycle.provider_reconciliations(provider, cloud_account_id, created_at DESC);

CREATE TABLE IF NOT EXISTS lifecycle.provider_orphans (
    provider             TEXT NOT NULL CHECK (length(btrim(provider)) BETWEEN 1 AND 255),
    cloud_account_id     TEXT NOT NULL DEFAULT '' CHECK (octet_length(cloud_account_id) <= 255),
    provider_resource_id TEXT NOT NULL CHECK (length(btrim(provider_resource_id)) BETWEEN 1 AND 255),
    resource_name        TEXT NOT NULL DEFAULT '' CHECK (octet_length(resource_name) <= 255),
    provider_created_at  TIMESTAMPTZ,
    state                TEXT NOT NULL DEFAULT 'quarantined'
        CONSTRAINT lifecycle_provider_orphans_state_check
        CHECK (state IN ('quarantined','deleting','deleted')),
    observations         INTEGER NOT NULL DEFAULT 0 CHECK (observations >= 0),
    attempts             INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    locked_until         TIMESTAMPTZ,
    lease_token          TEXT CHECK (lease_token IS NULL OR length(lease_token) BETWEEN 1 AND 255),
    last_error           TEXT NOT NULL DEFAULT '' CHECK (octet_length(last_error) <= 1024),
    first_observed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_observed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at           TIMESTAMPTZ,
    deletion_receipt     JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(deletion_receipt) = 'object'
                            AND octet_length(deletion_receipt::text) BETWEEN 2 AND 4096),
    PRIMARY KEY (provider, cloud_account_id, provider_resource_id),
    CHECK ((state = 'deleting' AND locked_until IS NOT NULL AND lease_token IS NOT NULL)
        OR (state <> 'deleting' AND locked_until IS NULL AND lease_token IS NULL)),
    CHECK ((state = 'deleted') = (deleted_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_lifecycle_provider_orphans_pending
    ON lifecycle.provider_orphans(state, first_observed_at, last_observed_at);

ALTER TABLE lifecycle.provider_orphans
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS lease_token TEXT CHECK (lease_token IS NULL OR length(lease_token) BETWEEN 1 AND 255);
DO $$
DECLARE
    old_state_check text;
BEGIN
    SELECT conname INTO old_state_check
    FROM pg_constraint
    WHERE conrelid = 'lifecycle.provider_orphans'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%quarantined%'
      AND pg_get_constraintdef(oid) LIKE '%deleted%'
      AND pg_get_constraintdef(oid) NOT LIKE '%deleting%'
    LIMIT 1;
    IF old_state_check IS NOT NULL THEN
        EXECUTE format('ALTER TABLE lifecycle.provider_orphans DROP CONSTRAINT %I', old_state_check);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'lifecycle.provider_orphans'::regclass
          AND conname = 'lifecycle_provider_orphans_state_check'
    ) THEN
        ALTER TABLE lifecycle.provider_orphans
            ADD CONSTRAINT lifecycle_provider_orphans_state_check
            CHECK (state IN ('quarantined','deleting','deleted'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'lifecycle.provider_orphans'::regclass
          AND conname = 'lifecycle_provider_orphans_lease_check'
    ) THEN
        ALTER TABLE lifecycle.provider_orphans
            ADD CONSTRAINT lifecycle_provider_orphans_lease_check
            CHECK ((state = 'deleting' AND locked_until IS NOT NULL AND lease_token IS NOT NULL)
                OR (state <> 'deleting' AND locked_until IS NULL AND lease_token IS NULL));
    END IF;
END;
$$;
`

func ensureProviderReconciliationSchema(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("lifecycle: begin provider-reconciliation schema transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, providerReconciliationSchema); err != nil {
		return fmt.Errorf("lifecycle: ensure provider-reconciliation schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("lifecycle: commit provider-reconciliation schema: %w", err)
	}
	return nil
}

// ProviderInventoryTime supplies one database clock to all reconcilers. A scan
// records absence at its start and positive presence at its finish, so an
// overlapping absence cannot outrank a sighting made during either request.
func (s *Store) ProviderInventoryTime(ctx context.Context) (time.Time, error) {
	if err := s.assertReady(); err != nil {
		return time.Time{}, err
	}
	var now time.Time
	if err := s.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: read provider inventory clock: %w", err)
	}
	return now.UTC(), nil
}

func (s *Store) RecordProviderInventoryFailure(ctx context.Context, provider, cloudAccountID, safeError string) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	if err := validateProviderResourceIdentity(provider, cloudAccountID, "placeholder"); err != nil {
		return err
	}
	safeError = truncateUTF8(safeError, maxSafeErrorBytes)
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO lifecycle.provider_reconciliations
		(provider, cloud_account_id, state, last_error)
		VALUES ($1,$2,'failed',$3)`,
		provider, cloudAccountID, safeError); err != nil {
		return mapWriteError("record provider inventory failure", err)
	}
	return nil
}

func (s *Store) RecordProviderInventorySuccess(ctx context.Context, provider, cloudAccountID string, observed, quarantined, deleted int) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	if err := validateProviderResourceIdentity(provider, cloudAccountID, "placeholder"); err != nil {
		return err
	}
	if observed < 0 || quarantined < 0 || deleted < 0 {
		return fmt.Errorf("%w: reconciliation counters must be non-negative", ErrInvalidArgument)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO lifecycle.provider_reconciliations
		(provider, cloud_account_id, state, observed, quarantined, deleted)
		VALUES ($1,$2,'succeeded',$3,$4,$5)`,
		provider, cloudAccountID, observed, quarantined, deleted); err != nil {
		return mapWriteError("record provider inventory success", err)
	}
	return nil
}

func (s *Store) ProtectedProviderResources(ctx context.Context) ([]ProviderResourceRef, error) {
	if err := s.assertReady(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, protectedProviderResourcesSQL)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: list protected provider resources: %w", err)
	}
	defer rows.Close()
	var refs []ProviderResourceRef
	for rows.Next() {
		var ref ProviderResourceRef
		if err := rows.Scan(&ref.Provider, &ref.CloudAccountID, &ref.ProviderResourceID); err != nil {
			return nil, fmt.Errorf("lifecycle: scan protected provider resource: %w", err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lifecycle: list protected provider resources: %w", err)
	}
	return refs, nil
}

func (s *Store) ProviderCreatesForReconciliation(ctx context.Context, provider, cloudAccountID string) ([]ProviderCreateReconciliation, error) {
	if err := s.assertReady(); err != nil {
		return nil, err
	}
	if err := validateProviderResourceIdentity(provider, cloudAccountID, "placeholder"); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, provider, cloud_account_id, provider_resource_id, burst_id,
		       COALESCE(payload->>'node_name', ''),
		       state, (state='processing' AND locked_until <= now()) AS lease_expired
		FROM lifecycle.operations
		WHERE operation_type='provider_create'
		  AND provider=$1 AND cloud_account_id=$2
		  AND state IN ('processing','succeeded','dead_letter')`,
		provider, cloudAccountID)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: list provider creates for reconciliation: %w", err)
	}
	defer rows.Close()
	var creates []ProviderCreateReconciliation
	for rows.Next() {
		var create ProviderCreateReconciliation
		if err := rows.Scan(&create.ID, &create.Provider, &create.CloudAccountID,
			&create.ProviderResourceID, &create.BurstID, &create.ResourceName, &create.State,
			&create.LeaseExpired); err != nil {
			return nil, fmt.Errorf("lifecycle: scan provider create for reconciliation: %w", err)
		}
		creates = append(creates, create)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lifecycle: list provider creates for reconciliation: %w", err)
	}
	return creates, nil
}

func (s *Store) ReconcileExpiredProviderCreateSucceeded(ctx context.Context, operationID int64, providerResourceID string) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	if operationID <= 0 {
		return fmt.Errorf("%w: operation ID required", ErrInvalidArgument)
	}
	if err := validateBookingField("provider resource ID", providerResourceID); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("lifecycle: begin provider-create reconciliation: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var op ProviderCreateOperation
	var payloadText string
	err = tx.QueryRow(ctx, `
		SELECT id, customer_id, cluster_id, workload_id, burst_id, provider, region,
		       cloud_account_id, sku, payload_version, payload::text, state, attempts,
		       COALESCE(lease_token, ''), locked_until, next_attempt_at
		FROM lifecycle.operations
		WHERE id=$1 AND operation_type='provider_create'
		FOR UPDATE`, operationID).
		Scan(&op.ID, &op.CustomerID, &op.ClusterID, &op.WorkloadID, &op.BurstID,
			&op.Provider, &op.Region, &op.CloudAccountID, &op.SKU, &op.PayloadVersion,
			&payloadText, &op.State, &op.Attempts, &op.LeaseToken, &op.LockedUntil,
			&op.NextAttemptAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lifecycle: lock provider-create for reconciliation: %w", err)
	}
	if op.State != OperationProcessing || op.LockedUntil.After(time.Now()) || providerResourceID == "" {
		return fmt.Errorf("%w: provider-create operation is not an expired ambiguous create", ErrInvariantViolation)
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
		return fmt.Errorf("lifecycle: lock burst for provider-create reconciliation: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE lifecycle.operations
		SET state='succeeded', provider_resource_id=$1, locked_until=NULL,
		    lease_token=NULL, last_error='', updated_at=now(), completed_at=now()
		WHERE id=$2`, providerResourceID, operationID); err != nil {
		return mapWriteError("reconcile provider-create succeeded", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE lifecycle.bursts
		SET state='provider_created', provider_resource_id=$1, updated_at=now()
		WHERE customer_id=$2 AND cluster_id=$3 AND id=$4`,
		providerResourceID, op.CustomerID, op.ClusterID, op.BurstID); err != nil {
		return mapWriteError("reconcile burst provider-created", err)
	}
	details, err := eventPayload(map[string]any{"provider_resource_id": providerResourceID})
	if err != nil {
		return err
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: op.CustomerID, ClusterID: op.ClusterID,
		AggregateType: "operation", AggregateID: fmt.Sprint(operationID),
		EventType: "provider_create.reconciled", PriorState: op.State,
		NewState: OperationSucceeded, Payload: details,
	}); err != nil {
		return err
	}
	if err := insertLifecycleEvent(ctx, tx, lifecycleEvent{
		CustomerID: op.CustomerID, ClusterID: op.ClusterID,
		AggregateType: "burst", AggregateID: op.BurstID,
		EventType: "burst.provider_created", PriorState: priorBurstState,
		NewState: BurstProviderCreated, Payload: details,
	}); err != nil {
		return err
	}
	return commit(ctx, tx, "provider-create reconciliation")
}

func (s *Store) ObserveProviderOrphan(ctx context.Context, obs ProviderResourceObservation) (ProviderOrphanRecord, error) {
	if err := s.assertReady(); err != nil {
		return ProviderOrphanRecord{}, err
	}
	obs = normalizeObservation(obs)
	if err := ValidateProviderResourceObservation(obs); err != nil {
		return ProviderOrphanRecord{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ProviderOrphanRecord{}, fmt.Errorf("lifecycle: begin provider observation: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockProviderResourceTx(ctx, tx, ProviderResourceRef{
		Provider: obs.Provider, CloudAccountID: obs.CloudAccountID, ProviderResourceID: obs.ProviderResourceID,
	}); err != nil {
		return ProviderOrphanRecord{}, err
	}
	var rec ProviderOrphanRecord
	var providerCreatedAt *time.Time
	var deletedAt *time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO lifecycle.provider_orphans AS o
		(provider, cloud_account_id, provider_resource_id, resource_name, provider_created_at,
		 observations, first_observed_at, last_observed_at)
		VALUES ($1,$2,$3,$4,$5,1,$6,$6)
		ON CONFLICT (provider, cloud_account_id, provider_resource_id) DO UPDATE
		SET resource_name=CASE WHEN EXCLUDED.last_observed_at >= o.last_observed_at
		        THEN EXCLUDED.resource_name ELSE o.resource_name END,
		    provider_created_at=COALESCE(o.provider_created_at, EXCLUDED.provider_created_at),
		    observations=o.observations + 1,
		    state=CASE WHEN o.state='deleted' AND EXCLUDED.last_observed_at >= o.last_observed_at
		        THEN 'quarantined' ELSE o.state END,
		    first_observed_at=CASE WHEN o.state='deleted' AND EXCLUDED.last_observed_at >= o.last_observed_at
		        THEN EXCLUDED.last_observed_at ELSE o.first_observed_at END,
		    deleted_at=CASE WHEN o.state='deleted' AND EXCLUDED.last_observed_at >= o.last_observed_at
		        THEN NULL ELSE o.deleted_at END,
		    last_observed_at=GREATEST(o.last_observed_at, EXCLUDED.last_observed_at)
		RETURNING provider, cloud_account_id, provider_resource_id, resource_name,
		          provider_created_at, state, observations, attempts, last_error,
		          first_observed_at, last_observed_at, deleted_at`,
		obs.Provider, obs.CloudAccountID, obs.ProviderResourceID, obs.ResourceName,
		nullableTime(obs.ProviderCreatedAt), obs.ObservedAt).
		Scan(&rec.Provider, &rec.CloudAccountID, &rec.ProviderResourceID, &rec.ResourceName,
			&providerCreatedAt, &rec.State, &rec.Observations, &rec.Attempts,
			&rec.LastError, &rec.FirstObservedAt, &rec.LastObservedAt, &deletedAt)
	if err != nil {
		return ProviderOrphanRecord{}, mapWriteError("observe provider orphan", err)
	}
	if providerCreatedAt != nil {
		rec.ProviderCreatedAt = *providerCreatedAt
	}
	rec.DeletedAt = deletedAt
	if err := commit(ctx, tx, "provider observation"); err != nil {
		return ProviderOrphanRecord{}, err
	}
	return rec, nil
}

func (s *Store) ClaimProviderOrphanDelete(ctx context.Context, ref ProviderResourceRef, lease time.Duration) (ProviderOrphanRecord, bool, error) {
	if err := s.assertReady(); err != nil {
		return ProviderOrphanRecord{}, false, err
	}
	if err := validateProviderResourceRef(ref); err != nil {
		return ProviderOrphanRecord{}, false, err
	}
	if err := validateLease(lease); err != nil {
		return ProviderOrphanRecord{}, false, err
	}
	token, err := randomToken()
	if err != nil {
		return ProviderOrphanRecord{}, false, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ProviderOrphanRecord{}, false, fmt.Errorf("lifecycle: begin provider orphan delete claim: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockProviderResourceTx(ctx, tx, ref); err != nil {
		return ProviderOrphanRecord{}, false, err
	}
	protected, err := providerResourceProtectedTx(ctx, tx, ref)
	if err != nil {
		return ProviderOrphanRecord{}, false, err
	}
	if protected {
		return ProviderOrphanRecord{}, false, nil
	}
	var rec ProviderOrphanRecord
	var providerCreatedAt, deletedAt, lockedUntil pgtype.Timestamptz
	var leaseToken pgtype.Text
	err = tx.QueryRow(ctx, `
		WITH candidate AS (
		    SELECT provider, cloud_account_id, provider_resource_id, state
		    FROM lifecycle.provider_orphans
		    WHERE provider=$2 AND cloud_account_id=$3 AND provider_resource_id=$4
		      AND (state='quarantined' OR (state='deleting' AND locked_until <= clock_timestamp()))
		    FOR UPDATE SKIP LOCKED
		    LIMIT 1
		)
		UPDATE lifecycle.provider_orphans AS o
		SET state='deleting',
		    attempts=o.attempts + CASE WHEN candidate.state='quarantined' THEN 1 ELSE 0 END,
		    locked_until=clock_timestamp()+make_interval(secs => $1),
		    lease_token=$5,
		    last_observed_at=GREATEST(o.last_observed_at, clock_timestamp())
		FROM candidate
		WHERE o.provider=candidate.provider
		  AND o.cloud_account_id=candidate.cloud_account_id
		  AND o.provider_resource_id=candidate.provider_resource_id
		RETURNING o.provider, o.cloud_account_id, o.provider_resource_id, o.resource_name,
		          o.provider_created_at, o.state, o.observations, o.attempts,
		          o.lease_token, o.locked_until, o.last_error,
		          o.first_observed_at, o.last_observed_at, o.deleted_at`,
		lease.Seconds(), ref.Provider, ref.CloudAccountID, ref.ProviderResourceID, token).
		Scan(&rec.Provider, &rec.CloudAccountID, &rec.ProviderResourceID, &rec.ResourceName,
			&providerCreatedAt, &rec.State, &rec.Observations, &rec.Attempts,
			&leaseToken, &lockedUntil, &rec.LastError, &rec.FirstObservedAt,
			&rec.LastObservedAt, &deletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderOrphanRecord{}, false, nil
	}
	if err != nil {
		return ProviderOrphanRecord{}, false, fmt.Errorf("lifecycle: claim provider orphan delete: %w", err)
	}
	hydrateProviderOrphanRecord(&rec, providerCreatedAt, leaseToken, lockedUntil, deletedAt)
	if err := commit(ctx, tx, "provider orphan delete claim"); err != nil {
		return ProviderOrphanRecord{}, false, err
	}
	return rec, true, nil
}

func (s *Store) MarkProviderOrphanDeleteFailed(ctx context.Context, ref ProviderResourceRef, leaseToken, safeError string) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	if err := validateProviderResourceRef(ref); err != nil {
		return err
	}
	if leaseToken == "" {
		return fmt.Errorf("%w: orphan delete lease token required", ErrInvalidArgument)
	}
	safeError = truncateUTF8(safeError, maxSafeErrorBytes)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("lifecycle: begin orphan delete failure: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	state, err := lockProviderOrphanOutcomeTx(ctx, tx, ref)
	if err != nil || state == ProviderOrphanDeleted {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE lifecycle.provider_orphans
		SET state='quarantined', locked_until=NULL, lease_token=NULL,
		    last_error=$1, last_observed_at=GREATEST(last_observed_at, clock_timestamp())
		WHERE provider=$2 AND cloud_account_id=$3 AND provider_resource_id=$4
		  AND state='deleting' AND lease_token=$5 AND locked_until > clock_timestamp()`,
		safeError, ref.Provider, ref.CloudAccountID, ref.ProviderResourceID, leaseToken)
	if err != nil {
		return mapWriteError("mark provider orphan delete failed", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return commit(ctx, tx, "provider orphan delete failure")
}

func (s *Store) MarkProviderOrphanDeleted(ctx context.Context, ref ProviderResourceRef, leaseToken string, deletedAt time.Time) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	if err := validateProviderResourceRef(ref); err != nil {
		return err
	}
	if leaseToken == "" {
		return fmt.Errorf("%w: orphan delete lease token required", ErrInvalidArgument)
	}
	if deletedAt.IsZero() {
		var err error
		deletedAt, err = s.ProviderInventoryTime(ctx)
		if err != nil {
			return err
		}
	}
	receipt, err := eventPayload(map[string]any{
		"deleted_at": deletedAt.UTC().Format(time.RFC3339Nano),
		"source":     "provider_delete",
	})
	if err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("lifecycle: begin orphan deletion receipt: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	state, err := lockProviderOrphanOutcomeTx(ctx, tx, ref)
	if err != nil || state == ProviderOrphanDeleted {
		return err
	}
	// Use the provider-result time, not the later database commit time. A
	// sighting newer than that result must survive either write arrival order.
	tag, err := tx.Exec(ctx, `
		UPDATE lifecycle.provider_orphans
		SET state='deleted', locked_until=NULL, lease_token=NULL, last_error='', deleted_at=$1,
		    deletion_receipt=$2::jsonb, last_observed_at=$1
		WHERE provider=$3 AND cloud_account_id=$4 AND provider_resource_id=$5
		  AND state='deleting' AND lease_token=$6 AND locked_until > clock_timestamp()
		  AND last_observed_at <= $1`,
		deletedAt.UTC(), string(receipt), ref.Provider, ref.CloudAccountID,
		ref.ProviderResourceID, leaseToken)
	if err != nil {
		return mapWriteError("mark provider orphan deleted", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return commit(ctx, tx, "provider orphan deletion receipt")
}

// ReconcileProviderOrphansAbsent consumes a successful complete inventory.
// absentAt must be captured before the inventory call, not when it finishes.
// Presence wins timestamp ties; only a strictly later absence may retire it.
func (s *Store) ReconcileProviderOrphansAbsent(ctx context.Context, provider, cloudAccountID string, observedProviderResourceIDs []string, absentAt time.Time) (int, error) {
	if err := s.assertReady(); err != nil {
		return 0, err
	}
	if err := validateProviderResourceIdentity(provider, cloudAccountID, "placeholder"); err != nil {
		return 0, err
	}
	if absentAt.IsZero() {
		return 0, fmt.Errorf("%w: inventory start timestamp is required", ErrInvalidArgument)
	}
	for _, id := range observedProviderResourceIDs {
		if err := validateBookingField("observed provider resource ID", id); err != nil {
			return 0, err
		}
	}
	receipt, err := eventPayload(map[string]any{
		"deleted_at": absentAt.UTC().Format(time.RFC3339Nano),
		"source":     "provider_absent",
	})
	if err != nil {
		return 0, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("lifecycle: begin provider absence reconciliation: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	// Enumerate without row locks, then take resource fences in stable order.
	// Each candidate is rechecked after its lock; the entire batch commits once.
	rows, err := tx.Query(ctx, `SELECT provider_resource_id FROM lifecycle.provider_orphans
		WHERE provider=$1 AND cloud_account_id=$2
		  AND state IN ('quarantined','deleting')
		  AND last_observed_at < $4
		  AND NOT (provider_resource_id = ANY(COALESCE($3::text[], '{}'::text[])))
		ORDER BY provider_resource_id`, provider, cloudAccountID, observedProviderResourceIDs, absentAt.UTC())
	if err != nil {
		return 0, fmt.Errorf("lifecycle: list absent provider orphan candidates: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, fmt.Errorf("lifecycle: read absent provider orphan candidates: %w", err)
	}
	deleted := 0
	for _, id := range ids {
		ref := ProviderResourceRef{Provider: provider, CloudAccountID: cloudAccountID, ProviderResourceID: id}
		if err := lockProviderResourceTx(ctx, tx, ref); err != nil {
			return 0, err
		}
		protected, err := providerResourceProtectedTx(ctx, tx, ref)
		if err != nil {
			return 0, err
		}
		if protected {
			continue
		}
		tag, err := tx.Exec(ctx, `UPDATE lifecycle.provider_orphans
			SET state='deleted', locked_until=NULL, lease_token=NULL, last_error='', deleted_at=$1,
			    deletion_receipt=$2::jsonb, last_observed_at=$1
			WHERE provider=$3 AND cloud_account_id=$4 AND provider_resource_id=$5
			  AND state IN ('quarantined','deleting') AND last_observed_at < $1`,
			absentAt.UTC(), string(receipt), provider, cloudAccountID, id)
		if err != nil {
			return 0, mapWriteError("reconcile absent provider orphan", err)
		}
		deleted += int(tag.RowsAffected())
	}
	if err := commit(ctx, tx, "provider absence reconciliation"); err != nil {
		return 0, err
	}
	return deleted, nil
}

// lockProviderOrphanOutcomeTx waits for the row before the caller evaluates
// its lease using database wall time in a separate statement. A clock check
// within an UPDATE alone can run before that statement waits for the row.
// No resource lock is acquired after this row lock: ownership publication and
// inventory reconciliation take the opposite (resource, then row) order.
func lockProviderOrphanOutcomeTx(ctx context.Context, tx pgx.Tx, ref ProviderResourceRef) (string, error) {
	var state string
	err := tx.QueryRow(ctx, `
		SELECT state FROM lifecycle.provider_orphans
		WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3
		FOR UPDATE`,
		ref.Provider, ref.CloudAccountID, ref.ProviderResourceID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrLeaseLost
	}
	if err != nil {
		return "", fmt.Errorf("lifecycle: lock provider orphan outcome: %w", err)
	}
	return state, nil
}

func hydrateProviderOrphanRecord(rec *ProviderOrphanRecord, providerCreatedAt pgtype.Timestamptz, leaseToken pgtype.Text, lockedUntil pgtype.Timestamptz, deletedAt pgtype.Timestamptz) {
	if providerCreatedAt.Valid {
		rec.ProviderCreatedAt = providerCreatedAt.Time
	}
	if leaseToken.Valid {
		rec.LeaseToken = leaseToken.String
	}
	if lockedUntil.Valid {
		t := lockedUntil.Time
		rec.LockedUntil = &t
	}
	if deletedAt.Valid {
		t := deletedAt.Time
		rec.DeletedAt = &t
	}
}

func normalizeObservation(obs ProviderResourceObservation) ProviderResourceObservation {
	obs.Provider = stringsTrim(obs.Provider)
	obs.CloudAccountID = stringsTrim(obs.CloudAccountID)
	obs.ProviderResourceID = stringsTrim(obs.ProviderResourceID)
	obs.ResourceName = truncateUTF8(stringsTrim(obs.ResourceName), maxIdentifierBytes)
	if obs.ObservedAt.IsZero() {
		obs.ObservedAt = time.Now().UTC()
	} else {
		obs.ObservedAt = obs.ObservedAt.UTC()
	}
	if !obs.ProviderCreatedAt.IsZero() {
		obs.ProviderCreatedAt = obs.ProviderCreatedAt.UTC()
	}
	return obs
}

// ValidateProviderResourceObservation checks an observation without normalizing
// it or touching storage, so a reconciler can validate a whole inventory before
// any adoption or cleanup. Resource names may be empty and are bounded by the
// store's existing normalization, but must be encodable PostgreSQL text.
func ValidateProviderResourceObservation(obs ProviderResourceObservation) error {
	if err := validateProviderResourceIdentity(obs.Provider, obs.CloudAccountID, obs.ProviderResourceID); err != nil {
		return err
	}
	if obs.ObservedAt.IsZero() {
		return fmt.Errorf("%w: observed_at is required", ErrInvalidArgument)
	}
	if !utf8.ValidString(obs.ResourceName) || strings.ContainsRune(obs.ResourceName, 0) {
		return fmt.Errorf("%w: resource name must be valid UTF-8 without NUL", ErrInvalidArgument)
	}
	return nil
}

func validateProviderResourceRef(ref ProviderResourceRef) error {
	return validateProviderResourceIdentity(ref.Provider, ref.CloudAccountID, ref.ProviderResourceID)
}

func validateProviderResourceIdentity(provider, cloudAccountID, providerResourceID string) error {
	for label, value := range map[string]string{
		"provider":             provider,
		"provider resource ID": providerResourceID,
	} {
		if err := validateBookingField(label, value); err != nil {
			return err
		}
	}
	if cloudAccountID != "" {
		if err := validateBookingField("cloud account ID", cloudAccountID); err != nil {
			return err
		}
	}
	return nil
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

func stringsTrim(s string) string {
	return strings.TrimSpace(s)
}
