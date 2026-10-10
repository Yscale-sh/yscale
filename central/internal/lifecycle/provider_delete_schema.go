package lifecycle

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const providerDeleteSchema = `
CREATE TABLE IF NOT EXISTS lifecycle.provider_deletes (
    id                   BIGSERIAL PRIMARY KEY,
    customer_id          TEXT NOT NULL CHECK (length(btrim(customer_id)) BETWEEN 1 AND 255),
    cluster_id           TEXT NOT NULL CHECK (length(btrim(cluster_id)) BETWEEN 1 AND 255),
    workload_id          TEXT NOT NULL CHECK (length(btrim(workload_id)) BETWEEN 1 AND 255),
    burst_id             TEXT NOT NULL CHECK (length(btrim(burst_id)) BETWEEN 1 AND 255),
    provider             TEXT NOT NULL CHECK (length(btrim(provider)) BETWEEN 1 AND 255),
    region               TEXT NOT NULL CHECK (length(btrim(region)) BETWEEN 1 AND 255),
    cloud_account_id     TEXT NOT NULL DEFAULT '' CHECK (octet_length(cloud_account_id) <= 255),
    sku                  TEXT NOT NULL CHECK (length(btrim(sku)) BETWEEN 1 AND 255),
    provider_resource_id TEXT NOT NULL CHECK (length(btrim(provider_resource_id)) BETWEEN 1 AND 255),
    reason               TEXT NOT NULL DEFAULT '' CHECK (octet_length(reason) <= 1024),
    payload_version      SMALLINT NOT NULL CHECK (payload_version > 0),
    payload              JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'
                             AND octet_length(payload::text) BETWEEN 2 AND 1048576),
    state                TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued','deleting','retrying','terminated','manual_attention')),
    generation           INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0),
    attempts             INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until         TIMESTAMPTZ,
    lease_token          TEXT CHECK (lease_token IS NULL OR length(lease_token) BETWEEN 1 AND 255),
    last_error           TEXT NOT NULL DEFAULT '' CHECK (octet_length(last_error) <= 1024),
    requested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at           TIMESTAMPTZ,
    UNIQUE (customer_id, cluster_id, burst_id),
    FOREIGN KEY (customer_id, cluster_id, workload_id)
        REFERENCES lifecycle.workloads(customer_id, cluster_id, id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (customer_id, cluster_id, burst_id)
        REFERENCES lifecycle.bursts(customer_id, cluster_id, id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK ((state = 'deleting' AND locked_until IS NOT NULL AND lease_token IS NOT NULL)
        OR (state <> 'deleting' AND locked_until IS NULL AND lease_token IS NULL)),
    CHECK ((state = 'terminated') = (deleted_at IS NOT NULL))
);

-- PR #90 may already have installed provider_deletes. Evolve it in place and
-- recover the credential-routing identity from the immutable worker payload.
ALTER TABLE lifecycle.provider_deletes
    ADD COLUMN IF NOT EXISTS cloud_account_id TEXT NOT NULL DEFAULT ''
    CHECK (octet_length(cloud_account_id) <= 255);
UPDATE lifecycle.provider_deletes
SET cloud_account_id = COALESCE(NULLIF(btrim(payload #>> '{burst,CloudAccountID}'), ''), '')
WHERE cloud_account_id = '' AND payload #>> '{burst,CloudAccountID}' IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_lifecycle_provider_deletes_claim
    ON lifecycle.provider_deletes(next_attempt_at, id)
    WHERE state IN ('queued','retrying','deleting');
CREATE INDEX IF NOT EXISTS idx_lifecycle_provider_deletes_attention
    ON lifecycle.provider_deletes(updated_at, id)
    WHERE state = 'manual_attention';
CREATE INDEX IF NOT EXISTS idx_lifecycle_provider_deletes_nonterminal_resources
    ON lifecycle.provider_deletes(provider_resource_id)
    WHERE state <> 'terminated';

CREATE OR REPLACE FUNCTION lifecycle.validate_provider_delete_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.customer_id IS DISTINCT FROM NEW.customer_id OR
       OLD.cluster_id IS DISTINCT FROM NEW.cluster_id OR
       OLD.workload_id IS DISTINCT FROM NEW.workload_id OR
       OLD.burst_id IS DISTINCT FROM NEW.burst_id OR
       OLD.provider IS DISTINCT FROM NEW.provider OR
       OLD.region IS DISTINCT FROM NEW.region OR
       OLD.cloud_account_id IS DISTINCT FROM NEW.cloud_account_id OR
       OLD.sku IS DISTINCT FROM NEW.sku OR
       OLD.provider_resource_id IS DISTINCT FROM NEW.provider_resource_id OR
       OLD.reason IS DISTINCT FROM NEW.reason OR
       OLD.payload_version IS DISTINCT FROM NEW.payload_version OR
       OLD.payload IS DISTINCT FROM NEW.payload OR
       OLD.requested_at IS DISTINCT FROM NEW.requested_at THEN
        RAISE EXCEPTION 'provider-delete identity is immutable';
    END IF;
    IF OLD.state = 'terminated' AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'provider-delete terminal state is absorbing';
    END IF;
    IF OLD.state = 'terminated' AND NEW.deleted_at IS DISTINCT FROM OLD.deleted_at THEN
        RAISE EXCEPTION 'provider-delete receipt is immutable';
    END IF;
    IF NEW.generation IS DISTINCT FROM OLD.generation AND NOT (
        OLD.state = 'manual_attention' AND NEW.state = 'queued' AND
        NEW.generation = OLD.generation + 1
    ) THEN
        RAISE EXCEPTION 'provider-delete generation may advance only on operator requeue';
    END IF;
    IF NEW.attempts IS DISTINCT FROM OLD.attempts AND NOT (
        (OLD.state IN ('queued','retrying') AND NEW.state = 'deleting' AND
         NEW.attempts = OLD.attempts + 1) OR
        (OLD.state = 'manual_attention' AND NEW.state = 'queued' AND
         NEW.attempts = 0)
    ) THEN
        RAISE EXCEPTION 'invalid provider-delete attempt mutation';
    END IF;
    IF OLD.state = 'queued' AND NEW.state NOT IN ('queued','deleting') THEN
        RAISE EXCEPTION 'invalid provider-delete transition from queued';
    END IF;
    IF OLD.state = 'deleting' AND NEW.state NOT IN ('deleting','retrying','terminated','manual_attention') THEN
        RAISE EXCEPTION 'invalid provider-delete transition from deleting';
    END IF;
    IF OLD.state = 'retrying' AND NEW.state NOT IN ('retrying','deleting') THEN
        RAISE EXCEPTION 'invalid provider-delete transition from retrying';
    END IF;
    IF OLD.state = 'manual_attention' AND NEW.state NOT IN ('manual_attention','queued') THEN
        RAISE EXCEPTION 'invalid provider-delete transition from manual_attention';
    END IF;
    RETURN NEW;
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'lifecycle_provider_delete_transition'
          AND tgrelid = 'lifecycle.provider_deletes'::regclass
    ) THEN
        CREATE TRIGGER lifecycle_provider_delete_transition
        BEFORE UPDATE ON lifecycle.provider_deletes
        FOR EACH ROW EXECUTE FUNCTION lifecycle.validate_provider_delete_update();
    END IF;
END;
$$;
`

// EnsureProviderDeleteSchema installs the base lifecycle schema and the
// provider-delete state machine in separate transactions. Keeping this entry
// point explicit lets runtime wiring land independently while preserving a
// complete migration contract for tests and operators.
func EnsureProviderDeleteSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("%w: nil database pool", ErrInvalidArgument)
	}
	if err := EnsureSchema(ctx, pool); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("lifecycle: begin provider-delete schema transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, providerDeleteSchema); err != nil {
		return fmt.Errorf("lifecycle: ensure provider-delete schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("lifecycle: commit provider-delete schema: %w", err)
	}
	return nil
}
