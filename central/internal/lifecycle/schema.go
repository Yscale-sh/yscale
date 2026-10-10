package lifecycle

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const lifecycleSchema = `
CREATE SCHEMA IF NOT EXISTS lifecycle;

CREATE TABLE IF NOT EXISTS lifecycle.workloads (
    id                  TEXT CHECK (length(btrim(id)) BETWEEN 1 AND 255),
    customer_id         TEXT NOT NULL CHECK (length(btrim(customer_id)) BETWEEN 1 AND 255),
    cluster_id          TEXT NOT NULL CHECK (length(btrim(cluster_id)) BETWEEN 1 AND 255),
    idempotency_key     TEXT NOT NULL CHECK (length(btrim(idempotency_key)) BETWEEN 1 AND 255),
    canonical_version   SMALLINT NOT NULL CHECK (canonical_version > 0),
    payload_hash        TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    burst_id            TEXT NOT NULL CHECK (length(btrim(burst_id)) BETWEEN 1 AND 255),
    spec                JSONB NOT NULL CHECK (jsonb_typeof(spec) = 'object'
                            AND octet_length(spec::text) BETWEEN 2 AND 1048576),
    state               TEXT NOT NULL DEFAULT 'admitted'
        CHECK (state IN ('admitted','succeeded','failed','cancelled')),
    terminal_reason     TEXT NOT NULL DEFAULT '' CHECK (octet_length(terminal_reason) <= 1024),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    terminal_at         TIMESTAMPTZ,
    PRIMARY KEY (customer_id, cluster_id, id),
    UNIQUE (customer_id, cluster_id, burst_id),
    UNIQUE (customer_id, cluster_id, idempotency_key),
    CHECK ((state IN ('succeeded','failed','cancelled')) = (terminal_at IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS lifecycle.bursts (
    id                   TEXT CHECK (length(btrim(id)) BETWEEN 1 AND 255),
    customer_id          TEXT NOT NULL CHECK (length(btrim(customer_id)) BETWEEN 1 AND 255),
    cluster_id           TEXT NOT NULL CHECK (length(btrim(cluster_id)) BETWEEN 1 AND 255),
    workload_id          TEXT NOT NULL CHECK (length(btrim(workload_id)) BETWEEN 1 AND 255),
    provider             TEXT NOT NULL CHECK (length(btrim(provider)) BETWEEN 1 AND 255),
    region               TEXT NOT NULL CHECK (length(btrim(region)) BETWEEN 1 AND 255),
    cloud_account_id     TEXT NOT NULL DEFAULT '' CHECK (octet_length(cloud_account_id) <= 255),
    sku                  TEXT NOT NULL CHECK (length(btrim(sku)) BETWEEN 1 AND 255),
    request              JSONB NOT NULL CHECK (jsonb_typeof(request) = 'object'
                             AND octet_length(request::text) BETWEEN 2 AND 1048576),
    state                TEXT NOT NULL DEFAULT 'create_requested'
        CHECK (state IN ('create_requested','provider_created','manual_attention','terminated')),
    generation           INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0),
    provider_resource_id TEXT NOT NULL DEFAULT '' CHECK (octet_length(provider_resource_id) <= 255),
    last_error           TEXT NOT NULL DEFAULT '' CHECK (octet_length(last_error) <= 1024),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    terminal_at          TIMESTAMPTZ,
    PRIMARY KEY (customer_id, cluster_id, id),
    UNIQUE (customer_id, cluster_id, workload_id),
    FOREIGN KEY (customer_id, cluster_id, workload_id)
        REFERENCES lifecycle.workloads(customer_id, cluster_id, id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK ((state IN ('manual_attention','terminated')) = (terminal_at IS NOT NULL)),
    CHECK (state <> 'provider_created' OR length(btrim(provider_resource_id)) > 0)
);

-- PR #90 may already have created lifecycle.bursts. Additive evolution keeps
-- those installations migratable, and the immutable request JSON safely
-- backfills BYOC identity for already-projected bookings.
ALTER TABLE lifecycle.bursts
    ADD COLUMN IF NOT EXISTS cloud_account_id TEXT NOT NULL DEFAULT ''
    CHECK (octet_length(cloud_account_id) <= 255);
UPDATE lifecycle.bursts
SET cloud_account_id = COALESCE(NULLIF(btrim(request->>'CloudAccountID'), ''), '')
WHERE cloud_account_id = '' AND request ? 'CloudAccountID';

-- Issue #93: truthful provider-creation timestamp, set once by
-- MarkProviderCreateSucceeded. NULL until the provider confirms creation;
-- Compatibility bookings seed this from state.Burst.CreatedAt; managed
-- creates set it only after the provider confirms creation.
ALTER TABLE lifecycle.bursts
    ADD COLUMN IF NOT EXISTS provider_created_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'lifecycle_workloads_burst_tenant_fk'
          AND conrelid = 'lifecycle.workloads'::regclass
    ) THEN
        ALTER TABLE lifecycle.workloads
        ADD CONSTRAINT lifecycle_workloads_burst_tenant_fk
        FOREIGN KEY (customer_id, cluster_id, burst_id)
        REFERENCES lifecycle.bursts(customer_id, cluster_id, id)
        DEFERRABLE INITIALLY DEFERRED;
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS lifecycle.operations (
    id                   BIGSERIAL PRIMARY KEY,
    customer_id          TEXT NOT NULL CHECK (length(btrim(customer_id)) BETWEEN 1 AND 255),
    cluster_id           TEXT NOT NULL CHECK (length(btrim(cluster_id)) BETWEEN 1 AND 255),
    workload_id          TEXT NOT NULL CHECK (length(btrim(workload_id)) BETWEEN 1 AND 255),
    burst_id             TEXT NOT NULL CHECK (length(btrim(burst_id)) BETWEEN 1 AND 255),
    operation_type       TEXT NOT NULL CHECK (operation_type IN ('provider_create')),
    semantic_key         TEXT NOT NULL CHECK (length(btrim(semantic_key)) BETWEEN 1 AND 255),
    provider             TEXT NOT NULL CHECK (length(btrim(provider)) BETWEEN 1 AND 255),
    region               TEXT NOT NULL CHECK (length(btrim(region)) BETWEEN 1 AND 255),
    cloud_account_id     TEXT NOT NULL DEFAULT '' CHECK (octet_length(cloud_account_id) <= 255),
    sku                  TEXT NOT NULL CHECK (length(btrim(sku)) BETWEEN 1 AND 255),
    payload_version      SMALLINT NOT NULL CHECK (payload_version > 0),
    payload              JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'
                             AND octet_length(payload::text) BETWEEN 2 AND 1048576),
    state                TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending','processing','succeeded','failed','dead_letter')),
    attempts             INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until         TIMESTAMPTZ,
    lease_token          TEXT CHECK (lease_token IS NULL OR length(lease_token) BETWEEN 1 AND 255),
    last_error           TEXT NOT NULL DEFAULT '' CHECK (octet_length(last_error) <= 1024),
    provider_resource_id TEXT NOT NULL DEFAULT '' CHECK (octet_length(provider_resource_id) <= 255),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at         TIMESTAMPTZ,
    UNIQUE (customer_id, cluster_id, operation_type, semantic_key),
    FOREIGN KEY (customer_id, cluster_id, workload_id)
        REFERENCES lifecycle.workloads(customer_id, cluster_id, id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (customer_id, cluster_id, burst_id)
        REFERENCES lifecycle.bursts(customer_id, cluster_id, id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK ((state = 'processing') = (locked_until IS NOT NULL AND lease_token IS NOT NULL)),
    CHECK ((state IN ('succeeded','dead_letter')) = (completed_at IS NOT NULL)),
    CHECK (state <> 'succeeded' OR length(btrim(provider_resource_id)) > 0)
);
ALTER TABLE lifecycle.operations
    ADD COLUMN IF NOT EXISTS cloud_account_id TEXT NOT NULL DEFAULT ''
    CHECK (octet_length(cloud_account_id) <= 255);
UPDATE lifecycle.operations AS o
SET cloud_account_id = COALESCE(NULLIF(btrim(b.cloud_account_id), ''),
                                NULLIF(btrim(o.payload->>'cloud_account_id'), ''),
                                '')
FROM lifecycle.bursts AS b
WHERE o.customer_id=b.customer_id AND o.cluster_id=b.cluster_id AND o.burst_id=b.id
  AND o.cloud_account_id = '';
CREATE INDEX IF NOT EXISTS idx_lifecycle_operations_claim
    ON lifecycle.operations(next_attempt_at, id)
    WHERE state IN ('pending','failed','processing');

CREATE TABLE IF NOT EXISTS lifecycle.outbox (
    id                BIGSERIAL PRIMARY KEY,
    customer_id       TEXT NOT NULL CHECK (length(btrim(customer_id)) BETWEEN 1 AND 255),
    cluster_id        TEXT NOT NULL CHECK (length(btrim(cluster_id)) BETWEEN 1 AND 255),
    aggregate_type    TEXT NOT NULL CHECK (aggregate_type IN ('workload','burst','operation')),
    aggregate_id      TEXT NOT NULL CHECK (length(btrim(aggregate_id)) BETWEEN 1 AND 255),
    event_key         TEXT NOT NULL CHECK (length(btrim(event_key)) BETWEEN 1 AND 255),
    event_type        TEXT NOT NULL CHECK (length(btrim(event_type)) BETWEEN 1 AND 255),
    payload_version   SMALLINT NOT NULL CHECK (payload_version > 0),
    payload           JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'
                           AND octet_length(payload::text) BETWEEN 2 AND 1048576),
    state             TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending','processing','acknowledged','failed','dead_letter')),
    attempts          INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until      TIMESTAMPTZ,
    lease_token       TEXT CHECK (lease_token IS NULL OR length(lease_token) BETWEEN 1 AND 255),
    last_error        TEXT NOT NULL DEFAULT '' CHECK (octet_length(last_error) <= 1024),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at   TIMESTAMPTZ,
    UNIQUE (customer_id, cluster_id, event_key),
    CHECK ((state = 'processing') = (locked_until IS NOT NULL AND lease_token IS NOT NULL)),
    CHECK ((state IN ('acknowledged','dead_letter')) = (acknowledged_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_lifecycle_outbox_claim
    ON lifecycle.outbox(next_attempt_at, id)
    WHERE state IN ('pending','failed','processing');

CREATE TABLE IF NOT EXISTS lifecycle.lifecycle_events (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     TEXT NOT NULL CHECK (length(btrim(customer_id)) BETWEEN 1 AND 255),
    cluster_id      TEXT NOT NULL CHECK (length(btrim(cluster_id)) BETWEEN 1 AND 255),
    aggregate_type  TEXT NOT NULL CHECK (aggregate_type IN ('workload','burst','operation','outbox')),
    aggregate_id    TEXT NOT NULL CHECK (length(btrim(aggregate_id)) BETWEEN 1 AND 255),
    event_type      TEXT NOT NULL CHECK (length(btrim(event_type)) BETWEEN 1 AND 255),
    prior_state     TEXT NOT NULL DEFAULT '',
    new_state       TEXT NOT NULL CHECK (length(btrim(new_state)) BETWEEN 1 AND 255),
    actor           TEXT NOT NULL DEFAULT '' CHECK (octet_length(actor) <= 255),
    trace_id        TEXT NOT NULL DEFAULT '' CHECK (octet_length(trace_id) <= 255),
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload) = 'object'
                          AND octet_length(payload::text) BETWEEN 2 AND 1048576),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_lifecycle_events_aggregate
    ON lifecycle.lifecycle_events(customer_id, cluster_id, aggregate_type, aggregate_id, id);

CREATE OR REPLACE FUNCTION lifecycle.reject_lifecycle_event_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'lifecycle events are append-only';
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'lifecycle_events_immutable'
          AND tgrelid = 'lifecycle.lifecycle_events'::regclass
    ) THEN
        CREATE TRIGGER lifecycle_events_immutable
        BEFORE UPDATE OR DELETE ON lifecycle.lifecycle_events
        FOR EACH ROW EXECUTE FUNCTION lifecycle.reject_lifecycle_event_mutation();
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION lifecycle.validate_workload_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.customer_id IS DISTINCT FROM NEW.customer_id OR
       OLD.cluster_id IS DISTINCT FROM NEW.cluster_id OR
       OLD.idempotency_key IS DISTINCT FROM NEW.idempotency_key OR
       OLD.canonical_version IS DISTINCT FROM NEW.canonical_version OR
       OLD.payload_hash IS DISTINCT FROM NEW.payload_hash OR
       OLD.burst_id IS DISTINCT FROM NEW.burst_id OR
       OLD.spec IS DISTINCT FROM NEW.spec THEN
        RAISE EXCEPTION 'lifecycle workload identity is immutable';
    END IF;
    IF OLD.state IN ('succeeded','failed','cancelled') AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'lifecycle workload terminal state is absorbing';
    END IF;
    IF OLD.state = 'admitted' AND NEW.state NOT IN ('admitted','succeeded','failed','cancelled') THEN
        RAISE EXCEPTION 'invalid workload transition';
    END IF;
    RETURN NEW;
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'lifecycle_workload_transition'
          AND tgrelid = 'lifecycle.workloads'::regclass
    ) THEN
        CREATE TRIGGER lifecycle_workload_transition
        BEFORE UPDATE ON lifecycle.workloads
        FOR EACH ROW EXECUTE FUNCTION lifecycle.validate_workload_update();
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION lifecycle.validate_burst_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.customer_id IS DISTINCT FROM NEW.customer_id OR
       OLD.cluster_id IS DISTINCT FROM NEW.cluster_id OR
       OLD.workload_id IS DISTINCT FROM NEW.workload_id OR
       OLD.provider IS DISTINCT FROM NEW.provider OR
       OLD.region IS DISTINCT FROM NEW.region OR
       OLD.cloud_account_id IS DISTINCT FROM NEW.cloud_account_id OR
       OLD.sku IS DISTINCT FROM NEW.sku OR
       OLD.request IS DISTINCT FROM NEW.request THEN
        RAISE EXCEPTION 'lifecycle burst identity is immutable';
    END IF;
    IF OLD.state IN ('manual_attention','terminated') AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'lifecycle burst terminal state is absorbing';
    END IF;
    IF OLD.state = 'create_requested' AND NEW.state NOT IN ('create_requested', 'provider_created', 'manual_attention', 'terminated') THEN
        RAISE EXCEPTION 'invalid burst transition from create_requested';
    END IF;
    IF OLD.state = 'provider_created' AND NEW.state NOT IN ('provider_created', 'manual_attention', 'terminated') THEN
        RAISE EXCEPTION 'invalid burst transition from provider_created';
    END IF;
    RETURN NEW;
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'lifecycle_burst_transition'
          AND tgrelid = 'lifecycle.bursts'::regclass
    ) THEN
        CREATE TRIGGER lifecycle_burst_transition
        BEFORE UPDATE ON lifecycle.bursts
        FOR EACH ROW EXECUTE FUNCTION lifecycle.validate_burst_update();
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION lifecycle.validate_operation_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.customer_id IS DISTINCT FROM NEW.customer_id OR
       OLD.cluster_id IS DISTINCT FROM NEW.cluster_id OR
       OLD.workload_id IS DISTINCT FROM NEW.workload_id OR
       OLD.burst_id IS DISTINCT FROM NEW.burst_id OR
       OLD.operation_type IS DISTINCT FROM NEW.operation_type OR
       OLD.semantic_key IS DISTINCT FROM NEW.semantic_key OR
       OLD.provider IS DISTINCT FROM NEW.provider OR
       OLD.region IS DISTINCT FROM NEW.region OR
       OLD.cloud_account_id IS DISTINCT FROM NEW.cloud_account_id OR
       OLD.sku IS DISTINCT FROM NEW.sku OR
       OLD.payload_version IS DISTINCT FROM NEW.payload_version OR
       OLD.payload IS DISTINCT FROM NEW.payload THEN
        RAISE EXCEPTION 'lifecycle operation identity is immutable';
    END IF;
    IF OLD.state IN ('succeeded','dead_letter') AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'lifecycle operation terminal state is absorbing';
    END IF;
    IF OLD.state = 'pending' AND NEW.state NOT IN ('pending', 'processing') THEN
        RAISE EXCEPTION 'invalid operation transition from pending';
    END IF;
    IF OLD.state = 'processing' AND NEW.state NOT IN ('processing', 'succeeded', 'failed', 'dead_letter') THEN
        RAISE EXCEPTION 'invalid operation transition from processing';
    END IF;
    IF OLD.state = 'failed' AND NEW.state NOT IN ('failed', 'processing') THEN
        RAISE EXCEPTION 'invalid operation transition from failed';
    END IF;
    RETURN NEW;
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'lifecycle_operation_transition'
          AND tgrelid = 'lifecycle.operations'::regclass
    ) THEN
        CREATE TRIGGER lifecycle_operation_transition
        BEFORE UPDATE ON lifecycle.operations
        FOR EACH ROW EXECUTE FUNCTION lifecycle.validate_operation_update();
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION lifecycle.validate_outbox_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.customer_id IS DISTINCT FROM NEW.customer_id OR
       OLD.cluster_id IS DISTINCT FROM NEW.cluster_id OR
       OLD.aggregate_type IS DISTINCT FROM NEW.aggregate_type OR
       OLD.aggregate_id IS DISTINCT FROM NEW.aggregate_id OR
       OLD.event_key IS DISTINCT FROM NEW.event_key OR
       OLD.event_type IS DISTINCT FROM NEW.event_type OR
       OLD.payload_version IS DISTINCT FROM NEW.payload_version OR
       OLD.payload IS DISTINCT FROM NEW.payload THEN
        RAISE EXCEPTION 'lifecycle outbox identity is immutable';
    END IF;
    IF OLD.state IN ('acknowledged','dead_letter') AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'lifecycle outbox terminal state is absorbing';
    END IF;
    IF OLD.state = 'pending' AND NEW.state NOT IN ('pending', 'processing') THEN
        RAISE EXCEPTION 'invalid outbox transition from pending';
    END IF;
    IF OLD.state = 'processing' AND NEW.state NOT IN ('processing', 'acknowledged', 'failed', 'dead_letter') THEN
        RAISE EXCEPTION 'invalid outbox transition from processing';
    END IF;
    IF OLD.state = 'failed' AND NEW.state NOT IN ('failed', 'processing') THEN
        RAISE EXCEPTION 'invalid outbox transition from failed';
    END IF;
    RETURN NEW;
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'lifecycle_outbox_transition'
          AND tgrelid = 'lifecycle.outbox'::regclass
    ) THEN
        CREATE TRIGGER lifecycle_outbox_transition
        BEFORE UPDATE ON lifecycle.outbox
        FOR EACH ROW EXECUTE FUNCTION lifecycle.validate_outbox_update();
    END IF;
END;
$$;
`

// EnsureSchema creates the dormant lifecycle schema idempotently. Runtime roles
// should not need DDL privileges once this package is wired into startup.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("%w: nil database pool", ErrInvalidArgument)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("lifecycle: begin schema transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, lifecycleSchema); err != nil {
		return fmt.Errorf("lifecycle: ensure schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("lifecycle: commit schema: %w", err)
	}
	// Create success also checks orphan deletion authorization. Install that
	// shared state for core-only callers, before any runtime can publish identity.
	return ensureProviderReconciliationSchema(ctx, pool)
}
