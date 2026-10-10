package billing

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// billingSchema is deliberately relational and separate from the control
// plane's JSONB snapshot tables. The account row is a locked materialization;
// the append-only ledger is the audit source used for reconciliation.
const billingSchema = `
CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE IF NOT EXISTS billing.environment (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    livemode  BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION billing.reject_environment_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'billing environment mode is immutable';
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'billing_environment_immutable'
          AND tgrelid = 'billing.environment'::regclass
    ) THEN
        CREATE TRIGGER billing_environment_immutable
        BEFORE UPDATE OR DELETE ON billing.environment
        FOR EACH ROW EXECUTE FUNCTION billing.reject_environment_mutation();
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS billing.accounts (
    customer_id       TEXT PRIMARY KEY CHECK (length(btrim(customer_id)) > 0),
    currency          TEXT NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
    balance_micro_usd BIGINT NOT NULL DEFAULT 0 CHECK (balance_micro_usd >= 0),
    held_micro_usd    BIGINT NOT NULL DEFAULT 0 CHECK (held_micro_usd >= 0),
    debt_micro_usd    BIGINT NOT NULL DEFAULT 0 CHECK (debt_micro_usd >= 0),
    frozen            BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (held_micro_usd <= balance_micro_usd),
    CHECK (debt_micro_usd = 0 OR frozen)
);

-- One row owns each retry key. Inserting this row is the transaction's first
-- mutation, so concurrent retries serialize before any money moves.
CREATE TABLE IF NOT EXISTS billing.operations (
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    customer_id     TEXT NOT NULL REFERENCES billing.accounts(customer_id),
    operation_type  TEXT NOT NULL CHECK (operation_type IN
        ('grant','reserve','capture','release','expire','reversal','restoration')),
    canonical_version SMALLINT NOT NULL CHECK (canonical_version > 0),
    payload_hash    TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    result_hold_id  BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, idempotency_key)
);

-- Canonical economic objects prevent multiple Stripe event IDs or retry keys
-- from granting the same settled payment twice.
CREATE TABLE IF NOT EXISTS billing.funding_sources (
    id                  BIGSERIAL PRIMARY KEY,
    customer_id         TEXT NOT NULL REFERENCES billing.accounts(customer_id),
    provider            TEXT NOT NULL,
    provider_account_id TEXT NOT NULL,
    livemode            BOOLEAN NOT NULL,
    payment_object_id   TEXT NOT NULL,
    credited_micro_usd  BIGINT NOT NULL CHECK (credited_micro_usd > 0),
    reversed_micro_usd  BIGINT NOT NULL DEFAULT 0,
    grant_operation_key TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, provider_account_id, livemode, payment_object_id),
    UNIQUE (customer_id, id),
    UNIQUE (customer_id, grant_operation_key),
    FOREIGN KEY (customer_id, grant_operation_key)
        REFERENCES billing.operations(customer_id, idempotency_key),
    CHECK (reversed_micro_usd >= 0 AND reversed_micro_usd <= credited_micro_usd)
);

CREATE TABLE IF NOT EXISTS billing.credit_reversals (
    id                  BIGSERIAL PRIMARY KEY,
    customer_id         TEXT NOT NULL REFERENCES billing.accounts(customer_id),
    funding_source_id   BIGINT NOT NULL,
    provider            TEXT NOT NULL,
    provider_account_id TEXT NOT NULL,
    livemode            BOOLEAN NOT NULL,
    reversal_object_id  TEXT NOT NULL,
    reversal_kind       TEXT NOT NULL CHECK (reversal_kind IN
        ('refund','dispute','chargeback','settlement_failure')),
    amount_micro_usd    BIGINT NOT NULL CHECK (amount_micro_usd > 0),
    operation_key       TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, provider_account_id, livemode, reversal_object_id),
    UNIQUE (customer_id, operation_key),
    FOREIGN KEY (customer_id, funding_source_id)
        REFERENCES billing.funding_sources(customer_id, id),
    FOREIGN KEY (customer_id, operation_key)
        REFERENCES billing.operations(customer_id, idempotency_key)
);

-- One row serializes every observation of a canonical provider dispute. A
-- restored row is terminal so a stale withdrawal observation cannot remove
-- credit after a newer provider read proved that funds were reinstated.
CREATE TABLE IF NOT EXISTS billing.credit_disputes (
    id                         BIGSERIAL PRIMARY KEY,
    customer_id                TEXT NOT NULL REFERENCES billing.accounts(customer_id),
    funding_source_id          BIGINT NOT NULL,
    provider                   TEXT NOT NULL,
    provider_account_id        TEXT NOT NULL,
    livemode                   BOOLEAN NOT NULL,
    dispute_object_id          TEXT NOT NULL,
    amount_micro_usd           BIGINT NOT NULL CHECK (amount_micro_usd > 0),
    state                      TEXT NOT NULL CHECK (state IN ('observed','withdrawn','restored')),
    balance_reduction_micro_usd BIGINT NOT NULL DEFAULT 0 CHECK (balance_reduction_micro_usd >= 0),
    debt_increase_micro_usd    BIGINT NOT NULL DEFAULT 0 CHECK (debt_increase_micro_usd >= 0),
    reversal_operation_key     TEXT,
    restoration_operation_key  TEXT,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, provider_account_id, livemode, dispute_object_id),
    UNIQUE (customer_id, id),
    FOREIGN KEY (customer_id, funding_source_id)
        REFERENCES billing.funding_sources(customer_id, id),
    FOREIGN KEY (customer_id, reversal_operation_key)
        REFERENCES billing.operations(customer_id, idempotency_key),
    FOREIGN KEY (customer_id, restoration_operation_key)
        REFERENCES billing.operations(customer_id, idempotency_key),
    CHECK (
        (state = 'observed' AND balance_reduction_micro_usd = 0 AND debt_increase_micro_usd = 0
            AND reversal_operation_key IS NULL AND restoration_operation_key IS NULL) OR
        (state = 'withdrawn' AND balance_reduction_micro_usd + debt_increase_micro_usd = amount_micro_usd
            AND reversal_operation_key IS NOT NULL AND restoration_operation_key IS NULL) OR
        (state = 'restored' AND (
            (balance_reduction_micro_usd = 0 AND debt_increase_micro_usd = 0
                AND reversal_operation_key IS NULL AND restoration_operation_key IS NULL) OR
            (balance_reduction_micro_usd + debt_increase_micro_usd = amount_micro_usd
                AND reversal_operation_key IS NOT NULL AND restoration_operation_key IS NOT NULL)))
    )
);

CREATE TABLE IF NOT EXISTS billing.holds (
    id                  BIGSERIAL PRIMARY KEY,
    customer_id         TEXT NOT NULL REFERENCES billing.accounts(customer_id),
    workload_ref        TEXT NOT NULL CHECK (length(btrim(workload_ref)) > 0),
    amount_micro_usd    BIGINT NOT NULL CHECK (amount_micro_usd > 0),
    captured_micro_usd  BIGINT NOT NULL DEFAULT 0,
    state               TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending','captured','released','expired')),
    reservation_key     TEXT NOT NULL,
    price_quote         JSONB NOT NULL CHECK (jsonb_typeof(price_quote) = 'object'),
    expires_at          TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id, workload_ref),
    UNIQUE (customer_id, id),
    UNIQUE (customer_id, reservation_key),
    FOREIGN KEY (customer_id, reservation_key)
        REFERENCES billing.operations(customer_id, idempotency_key),
    CHECK (captured_micro_usd >= 0 AND captured_micro_usd <= amount_micro_usd),
    CHECK (expires_at > created_at),
    CHECK ((state = 'captured' AND captured_micro_usd > 0) OR
           (state <> 'captured' AND captured_micro_usd = 0))
);
CREATE INDEX IF NOT EXISTS idx_holds_customer_state
    ON billing.holds(customer_id, state);
CREATE INDEX IF NOT EXISTS idx_holds_pending_expiry
    ON billing.holds(expires_at, id) WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS billing.ledger (
    id               BIGSERIAL PRIMARY KEY,
    customer_id      TEXT NOT NULL REFERENCES billing.accounts(customer_id),
    entry_type       TEXT NOT NULL CHECK (entry_type IN
        ('credit','reserve','capture','release','expiry','reversal','restoration')),
    amount_micro_usd BIGINT NOT NULL CHECK (amount_micro_usd > 0),
    operation_key    TEXT NOT NULL,
    hold_id          BIGINT,
    external_ref     TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id, operation_key),
    FOREIGN KEY (customer_id, operation_key)
        REFERENCES billing.operations(customer_id, idempotency_key),
    FOREIGN KEY (customer_id, hold_id)
        REFERENCES billing.holds(customer_id, id),
    CHECK ((entry_type IN ('reserve','capture','release','expiry') AND hold_id IS NOT NULL) OR
           (entry_type IN ('credit','reversal','restoration') AND hold_id IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_ledger_customer_created
    ON billing.ledger(customer_id, created_at, id);

-- Existing databases predate restoration entries. Widen only the three
-- generated CHECK constraints whose definitions do not yet include them.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'billing.operations'::regclass
          AND conname = 'operations_operation_type_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%restoration%'
    ) THEN
        ALTER TABLE billing.operations DROP CONSTRAINT operations_operation_type_check;
        ALTER TABLE billing.operations ADD CONSTRAINT operations_operation_type_check
            CHECK (operation_type IN ('grant','reserve','capture','release','expire','reversal','restoration'));
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'billing.ledger'::regclass
          AND conname = 'ledger_entry_type_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%restoration%'
    ) THEN
        ALTER TABLE billing.ledger DROP CONSTRAINT ledger_entry_type_check;
        ALTER TABLE billing.ledger ADD CONSTRAINT ledger_entry_type_check
            CHECK (entry_type IN ('credit','reserve','capture','release','expiry','reversal','restoration'));
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'billing.ledger'::regclass
          AND conname = 'ledger_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%restoration%'
    ) THEN
        ALTER TABLE billing.ledger DROP CONSTRAINT ledger_check;
        ALTER TABLE billing.ledger ADD CONSTRAINT ledger_check
            CHECK ((entry_type IN ('reserve','capture','release','expiry') AND hold_id IS NOT NULL) OR
                   (entry_type IN ('credit','reversal','restoration') AND hold_id IS NULL));
    END IF;
END;
$$;

-- A checkout intent exists before the provider request. Retrying a crashed
-- request reuses both this Yscale ID and the provider idempotency key, so a
-- provider success cannot become an untracked second purchase.
CREATE TABLE IF NOT EXISTS billing.checkout_sessions (
    id                  TEXT PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 255),
    customer_id         TEXT NOT NULL REFERENCES billing.accounts(customer_id),
    idempotency_key     TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    amount_micro_usd    BIGINT NOT NULL CHECK (amount_micro_usd > 0 AND amount_micro_usd % 10000 = 0),
    currency            TEXT NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
    provider            TEXT NOT NULL CHECK (length(provider) BETWEEN 1 AND 255),
    provider_account_id TEXT NOT NULL CHECK (length(provider_account_id) BETWEEN 1 AND 255),
    provider_session_id TEXT CHECK (provider_session_id IS NULL OR length(provider_session_id) BETWEEN 1 AND 255),
    checkout_url        TEXT CHECK (checkout_url IS NULL OR length(checkout_url) BETWEEN 1 AND 4096),
    livemode            BOOLEAN NOT NULL,
    state               TEXT NOT NULL DEFAULT 'pending_provider'
        CHECK (state IN ('pending_provider','open','settled','failed','expired')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id, idempotency_key),
    UNIQUE (provider, provider_account_id, livemode, provider_session_id),
    CHECK ((state = 'pending_provider' AND provider_session_id IS NULL AND checkout_url IS NULL) OR
           (state <> 'pending_provider' AND length(provider_session_id) > 0 AND length(checkout_url) > 0))
);
CREATE INDEX IF NOT EXISTS idx_checkout_customer_created
    ON billing.checkout_sessions(customer_id, created_at DESC, id);

CREATE OR REPLACE FUNCTION billing.validate_checkout_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.customer_id <> NEW.customer_id OR
       OLD.idempotency_key <> NEW.idempotency_key OR OLD.amount_micro_usd <> NEW.amount_micro_usd OR
       OLD.currency <> NEW.currency OR OLD.provider <> NEW.provider OR
       OLD.provider_account_id <> NEW.provider_account_id OR OLD.livemode <> NEW.livemode OR
       OLD.created_at <> NEW.created_at THEN
        RAISE EXCEPTION 'billing checkout identity is immutable';
    END IF;
    IF OLD.provider_session_id IS NOT NULL AND
       (OLD.provider_session_id <> NEW.provider_session_id OR OLD.checkout_url <> NEW.checkout_url) THEN
        RAISE EXCEPTION 'billing checkout provider session is immutable';
    END IF;
    IF (OLD.state = 'pending_provider' AND NEW.state <> 'open') OR
       (OLD.state <> 'pending_provider' AND NEW.state = 'pending_provider') THEN
        RAISE EXCEPTION 'illegal billing checkout transition: % -> %', OLD.state, NEW.state;
    END IF;
    RETURN NEW;
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'billing_checkout_transition'
          AND tgrelid = 'billing.checkout_sessions'::regclass
    ) THEN
        CREATE TRIGGER billing_checkout_transition
        BEFORE UPDATE ON billing.checkout_sessions
        FOR EACH ROW EXECUTE FUNCTION billing.validate_checkout_update();
    END IF;
END;
$$;

-- Added after holds to break the operations <-> holds creation cycle.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'billing_operation_result_hold_tenant_fk'
          AND conrelid = 'billing.operations'::regclass
    ) THEN
        ALTER TABLE billing.operations
        ADD CONSTRAINT billing_operation_result_hold_tenant_fk
        FOREIGN KEY (customer_id, result_hold_id)
        REFERENCES billing.holds(customer_id, id);
    END IF;
END;
$$;

-- Stripe is at-least-once and unordered. Only verified raw bodies enter this
-- inbox. A lease makes crashed async workers reclaimable.
CREATE TABLE IF NOT EXISTS billing.webhook_inbox (
    id                  BIGSERIAL PRIMARY KEY,
    provider            TEXT NOT NULL,
    provider_account_id TEXT NOT NULL,
    provider_event_id   TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    object_id           TEXT NOT NULL,
    customer_id         TEXT,
    api_version         TEXT NOT NULL DEFAULT '',
    livemode            BOOLEAN NOT NULL,
    provider_created_at TIMESTAMPTZ NOT NULL,
    payload             BYTEA NOT NULL CHECK (octet_length(payload) BETWEEN 1 AND 1048576),
    payload_hash        TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    state               TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending','processing','processed','failed','dead_letter')),
    attempts            INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until        TIMESTAMPTZ,
    lease_token         TEXT,
    last_error          TEXT NOT NULL DEFAULT '',
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at        TIMESTAMPTZ,
    UNIQUE (provider, provider_account_id, livemode, provider_event_id),
    CHECK ((state = 'processing') = (locked_until IS NOT NULL AND lease_token IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_webhook_claim
    ON billing.webhook_inbox(next_attempt_at, id)
    WHERE state IN ('pending','failed','processing');

-- Ledger rows are immutable even if a future caller accidentally issues SQL
-- outside this package.
CREATE OR REPLACE FUNCTION billing.reject_ledger_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'billing ledger is append-only';
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'billing_ledger_immutable'
          AND tgrelid = 'billing.ledger'::regclass
    ) THEN
        CREATE TRIGGER billing_ledger_immutable
        BEFORE UPDATE OR DELETE ON billing.ledger
        FOR EACH ROW EXECUTE FUNCTION billing.reject_ledger_mutation();
    END IF;
END;
$$;

-- Only pending -> terminal is legal; identity, amount, quote and expiry are
-- immutable after reservation.
CREATE OR REPLACE FUNCTION billing.validate_hold_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.customer_id <> NEW.customer_id OR OLD.workload_ref <> NEW.workload_ref OR
       OLD.amount_micro_usd <> NEW.amount_micro_usd OR
       OLD.reservation_key <> NEW.reservation_key OR OLD.price_quote <> NEW.price_quote OR
       OLD.expires_at <> NEW.expires_at THEN
        RAISE EXCEPTION 'billing hold identity is immutable';
    END IF;
    IF OLD.state <> 'pending' OR NEW.state NOT IN ('captured','released','expired') THEN
        RAISE EXCEPTION 'illegal billing hold transition: % -> %', OLD.state, NEW.state;
    END IF;
    RETURN NEW;
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'billing_hold_transition'
          AND tgrelid = 'billing.holds'::regclass
    ) THEN
        CREATE TRIGGER billing_hold_transition
        BEFORE UPDATE ON billing.holds
        FOR EACH ROW EXECUTE FUNCTION billing.validate_hold_update();
    END IF;
END;
$$;
`

func EnsureSchema(ctx context.Context, pool *pgxpool.Pool, liveMode bool) error {
	if pool == nil {
		return fmt.Errorf("%w: nil database pool", ErrInvalidArgument)
	}
	// A mode mismatch must fail before any DDL. This protects an existing live
	// database even if a test-configured migration process is started against it.
	// Production runtime roles must not call EnsureSchema or hold DDL privileges.
	var environmentExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('billing.environment') IS NOT NULL`).Scan(&environmentExists); err != nil {
		return fmt.Errorf("billing: inspect database environment: %w", err)
	}
	if environmentExists {
		var storedMode bool
		if err := pool.QueryRow(ctx, `SELECT livemode FROM billing.environment WHERE singleton`).Scan(&storedMode); err != nil {
			if err == pgx.ErrNoRows {
				return fmt.Errorf("%w: existing billing environment has no mode sentinel", ErrInvariantViolation)
			}
			return fmt.Errorf("billing: read existing database environment: %w", err)
		}
		if err := validateEnvironmentMode(storedMode, liveMode); err != nil {
			return err
		}
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("billing: begin schema transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, billingSchema); err != nil {
		return fmt.Errorf("billing: ensure schema: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO billing.environment (singleton, livemode) VALUES (TRUE, $1)
		ON CONFLICT (singleton) DO NOTHING`, liveMode); err != nil {
		return fmt.Errorf("billing: set database environment: %w", err)
	}
	var storedMode bool
	if err := tx.QueryRow(ctx, `SELECT livemode FROM billing.environment WHERE singleton`).Scan(&storedMode); err != nil {
		return fmt.Errorf("billing: read database environment: %w", err)
	}
	if err := validateEnvironmentMode(storedMode, liveMode); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("billing: commit schema: %w", err)
	}
	return nil
}

func validateEnvironmentMode(storedMode, requestedMode bool) error {
	if storedMode != requestedMode {
		return ErrEnvironmentMismatch
	}
	return nil
}

type dbtx interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
