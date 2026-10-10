package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

// NewPostgres builds a Store whose durable backend is Postgres — the
// replacement for the old JSON-snapshot store. On start it loads all durable
// records into the in-memory working set; thereafter every mutation is
// write-through'd to Postgres per record. First boot (empty database) seeds the
// local-dev test customer and persists it, mirroring New() / the old
// NewPersistent first-boot behaviour.
//
// dsn is a libpq/pgx connection string (e.g. the value of DATABASE_URL). The
// caller owns the lifecycle: defer store.Close() to release the pool.
func NewPostgres(ctx context.Context, dsn string) (*Store, error) {
	return NewPostgresWithCredentialCipher(ctx, dsn, nil)
}

// NewPostgresWithCredentialCipher is NewPostgres with the encryption boundary
// required by customer mesh credentials. A database with no reversible
// customer credentials may still use nil; a legacy plaintext or encrypted mesh
// credential makes startup fail closed until the matching cipher is supplied.
func NewPostgresWithCredentialCipher(ctx context.Context, dsn string, credentialCipher CustomerCredentialCipher) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("state: connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("state: ping postgres: %w", err)
	}
	p := &pgPersister{pool: pool, customerCredentialCipher: credentialCipher}
	if err := p.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	s := emptyStore()
	s.persist = p
	snap, err := p.loadAll(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}
	if err := p.prepareLoadedCustomerCredentials(snap.Customers); err != nil {
		pool.Close()
		return nil, err
	}
	s.applySnapshot(snap)

	// First boot: seed + persist the local-dev test customer ONLY when
	// explicitly opted in (YSCALE_SEED_DEV_CUSTOMER=1). The seed is gated
	// (un-gated seed-on-empty would plant a dev tenant in any fresh production
	// Postgres). Its bearer token comes from YSCALE_TOKEN when set (so the
	// operator has a known credential to point the agent/CLI at); otherwise it
	// is random per seed (a well-known constant in source would be a static
	// bearer-token backdoor). The token persists with the customer. The
	// in-memory New() seed keeps DefaultDevToken (tests rely on it).
	if len(s.customers) == 0 && os.Getenv("YSCALE_SEED_DEV_CUSTOMER") == "1" {
		seedToken := os.Getenv("YSCALE_TOKEN")
		if seedToken == "" {
			seedToken = NewDevToken()
		}
		s.seedTestCustomer(seedToken)
		for _, c := range s.customers {
			s.pUpsertCustomer(c)
		}
	}
	return s, nil
}

// pgPersister write-throughs each entity as a single JSONB row keyed by id.
// Storing the whole struct as JSONB (the same encoding/json the snapshot used)
// keeps the persistence layer in lockstep with the Go structs — no per-column
// mapping to drift as fields are added — while still giving per-record
// upsert/delete. Columns can be promoted out of the JSONB later if a relational
// query (e.g. a SQL-native ClaimBurst) needs them.
type pgPersister struct {
	pool                     *pgxpool.Pool
	customerCredentialCipher CustomerCredentialCipher
}

func customerMeshCredentialAAD(customerID string, mesh *MeshEndpoint) []byte {
	provider := "mesh"
	if mesh != nil && mesh.Provider != "" {
		provider = mesh.Provider
	}
	return []byte(customerID + "\x00customer-mesh\x00" + provider)
}

// customerData is the only serializer allowed to feed a Customer to Postgres.
// It derives the bearer verifier and encrypts a new mesh key on a copy, so the
// live working set may retain the plaintext needed by API clients while the
// JSONB document never contains it.
func (p *pgPersister) customerData(c *Customer) ([]byte, error) {
	if c == nil {
		return nil, errors.New("marshal nil customer")
	}
	persisted := *c
	ensureCustomerTokenHash(&persisted)
	if persisted.Mesh != nil {
		mesh := *persisted.Mesh
		persisted.Mesh = &mesh
		if mesh.APIKey != "" && mesh.APIKeyCiphertext == "" {
			if p.customerCredentialCipher == nil {
				return nil, fmt.Errorf("marshal customer %s: mesh credential encryption is unavailable", c.ID)
			}
			envelope, err := p.customerCredentialCipher.Encrypt(mesh.APIKey, customerMeshCredentialAAD(c.ID, &mesh))
			if err != nil {
				return nil, fmt.Errorf("marshal customer %s: encrypt mesh credential: %w", c.ID, err)
			}
			mesh.APIKeyCiphertext = envelope
		}
	}
	data, err := json.Marshal(&persisted)
	if err != nil {
		return nil, fmt.Errorf("marshal customer %s: %w", c.ID, err)
	}
	return data, nil
}

// prepareLoadedCustomerCredentials migrates historical plaintext rows before
// the store is published. Token plaintext becomes a one-way verifier. Mesh
// plaintext is encrypted, while an existing envelope is authenticated and
// opened into the live working set. Any missing/wrong key aborts startup rather
// than serving a partially usable credential set.
func (p *pgPersister) prepareLoadedCustomerCredentials(customers []*Customer) error {
	for _, c := range customers {
		needsRewrite, err := p.prepareLoadedCustomerCredential(c)
		if err != nil {
			return err
		}
		if !needsRewrite {
			continue
		}
		if err := p.upsertCustomer(c); err != nil {
			return fmt.Errorf("state: migrate customer %s credentials: %w", c.ID, err)
		}
		c.Token = ""
	}
	return nil
}

func (p *pgPersister) prepareLoadedCustomerCredential(c *Customer) (bool, error) {
	legacyToken := c.Token != ""
	ensureCustomerTokenHash(c)
	if !c.Revoked() && c.TokenHash == "" {
		return false, fmt.Errorf("state: customer %s has no bearer-token verifier", c.ID)
	}
	needsRewrite := legacyToken
	if c.Mesh == nil {
		return needsRewrite, nil
	}
	mesh := c.Mesh
	legacyMeshKey := mesh.APIKey != ""
	switch {
	case mesh.APIKeyCiphertext != "":
		if p.customerCredentialCipher == nil {
			return false, fmt.Errorf("state: customer %s has an encrypted mesh credential but no credential cipher is configured", c.ID)
		}
		plain, err := p.customerCredentialCipher.Decrypt(mesh.APIKeyCiphertext, customerMeshCredentialAAD(c.ID, mesh))
		if err != nil {
			return false, fmt.Errorf("state: decrypt customer %s mesh credential: %w", c.ID, err)
		}
		if mesh.APIKey != "" && mesh.APIKey != plain {
			return false, fmt.Errorf("state: customer %s legacy and encrypted mesh credentials disagree", c.ID)
		}
		mesh.APIKey = plain
		if legacyMeshKey {
			// A legacy plaintext key can coexist with a newly added envelope
			// during a rolling migration; rewrite removes that plaintext field.
			needsRewrite = true
		}
	case mesh.APIKey != "":
		if p.customerCredentialCipher == nil {
			return false, fmt.Errorf("state: customer %s has a plaintext mesh credential but no credential cipher is configured", c.ID)
		}
		envelope, err := p.customerCredentialCipher.Encrypt(mesh.APIKey, customerMeshCredentialAAD(c.ID, mesh))
		if err != nil {
			return false, fmt.Errorf("state: encrypt customer %s legacy mesh credential: %w", c.ID, err)
		}
		mesh.APIKeyCiphertext = envelope
		needsRewrite = true
	}
	return needsRewrite, nil
}

// pgSchema is applied idempotently on every start. One table per entity; the
// table names are fixed constants below (never user input), so interpolating
// them into statements is safe.
//
// pod_slots is the exception to the JSONB-per-entity shape above: slot is the
// PRIMARY KEY because the key IS the mechanism. Two centrals racing to hand a
// burst the same pod /24 both try to write the same slot, and the unique index
// lets exactly one of them through — the allocation is decided by the database
// rather than by each replica's private map.
//
// workloads_burst_id backs workloadByBurst. Workload rows are never deleted, so
// without it a cross-replica reap scans every workload ever run to find the one
// it needs to fail.
//
// accounts and tenant_memberships need no uniqueness index beyond their primary
// key: both ids are derived from the identity they represent (see accountID /
// membershipID), so the key column already IS the uniqueness constraint and two
// replicas racing the same sign-in write the same row.
//
// tenant_memberships_customer_id backs the offboard transaction, which deletes
// a tenant's grants by customer id rather than by a list of membership ids, so
// that a row this process never held is removed too.
//
// customer_tombstones outlives the customer row it names. A finished offboard
// deletes the customers row, and after that the row's absence is the only thing
// saying the id was ever used — which is exactly what a replica still holding
// the pre-delete record cannot distinguish from "never provisioned". The
// tombstone is that record: it is written in the same transaction as the
// delete, every writer that could create a customers row consults it, and every
// replica reads the table at boot (loadAll) so the refusal lives in its memory
// too rather than costing one failed durable write at a time. It carries
// revoked_at (from the row it retires) and deleted_at so an operator can see
// when access was cut and when the id was finally retired. Rows are never
// removed — a tenant id is an identity and security boundary, ids are generated
// rather than chosen, and nothing needs to reuse one.
//
// tenant_audit is the append-only governance journal, and it breaks the
// JSONB-per-entity shape twice on purpose. customer_id is a promoted column
// because every read of this table is tenant-scoped and the scoping has to be
// in the statement rather than a filter over rows the database already handed
// back. The index is (customer_id, id DESC) because that IS the read: one
// tenant's rows, newest first, resuming after a cursor — audit ids sort by
// append time (see AuditEvent.ID), so one column serves the ordering and the
// cursor together.
//
// The trigger is what makes "append-only" a property of the DATABASE rather
// than of the code above it. Evidence central could rewrite is evidence
// central's own compromise erases; a BEFORE UPDATE OR DELETE trigger that
// always raises means no statement from this process — or from a psql session —
// can alter a row once it lands. It is written idempotently (CREATE OR REPLACE
// for the function, DROP ... IF EXISTS before CREATE TRIGGER) so an existing
// database converges on re-run like every other statement here.
//
// workload_idempotency is the submission-claim table, and it is deliberately
// NOT loaded into memory at boot. Every other collection here is a working set
// this process reads back; a claim is only ever consulted through the claim
// statement itself, and a per-replica copy would be worse than none — a central
// answering from its own map would let a second one make the same claim, which
// is one Idempotency-Key and two paid nodes. The table is the whole record and
// the PRIMARY KEY is the election, exactly as pod_slots' is.
//
// Audit rows are deliberately absent from loadAll and from deleteCustomerRows:
// they are not part of the working set, and an offboard that deleted them would
// destroy the record of a tenant at the moment it becomes the only one left.
const pgSchema = `
CREATE TABLE IF NOT EXISTS customers          (id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS customer_tombstones (id TEXT PRIMARY KEY, revoked_at TIMESTAMPTZ, deleted_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS accounts           (id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS tenant_memberships (id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS workloads          (id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS bursts             (id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS persistent_volumes (id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS pod_slots          (slot INT PRIMARY KEY, burst_id TEXT NOT NULL, reserved_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS workload_idempotency (id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS connector_commands (
    id TEXT PRIMARY KEY CHECK (length(btrim(id)) BETWEEN 1 AND 255),
    customer_id TEXT NOT NULL CHECK (length(btrim(customer_id)) BETWEEN 1 AND 255),
    cluster_id TEXT NOT NULL CHECK (length(btrim(cluster_id)) BETWEEN 1 AND 255),
    command_type TEXT NOT NULL CHECK (length(btrim(command_type)) BETWEEN 1 AND 255),
    payload_version SMALLINT NOT NULL CHECK (payload_version > 0),
    payload_digest TEXT NOT NULL CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
    ordering_key TEXT NOT NULL CHECK (length(btrim(ordering_key)) BETWEEN 1 AND 255),
    semantic_key TEXT NOT NULL CHECK (length(btrim(semantic_key)) BETWEEN 1 AND 255),
    workload_id TEXT NOT NULL DEFAULT '' CHECK (octet_length(workload_id) <= 255),
    burst_id TEXT NOT NULL DEFAULT '' CHECK (octet_length(burst_id) <= 255),
    depends_on TEXT REFERENCES connector_commands(id) DEFERRABLE INITIALLY DEFERRED,
    envelope BYTEA NOT NULL CHECK (octet_length(envelope) BETWEEN 2 AND 1114112),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','processing','failed','acknowledged','dead_letter')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until TIMESTAMPTZ,
    lease_token TEXT CHECK (lease_token IS NULL OR length(lease_token) BETWEEN 1 AND 255),
    last_error TEXT NOT NULL DEFAULT '' CHECK (octet_length(last_error) <= 1024),
    last_ambiguity TEXT NOT NULL DEFAULT '' CHECK (octet_length(last_ambiguity) <= 1024),
    ack JSONB CHECK (ack IS NULL OR (jsonb_typeof(ack) = 'object' AND octet_length(ack::text) <= 1052672)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ,
    acknowledged_at TIMESTAMPTZ,
    UNIQUE (customer_id, cluster_id, semantic_key),
    CHECK ((state = 'processing') = (locked_until IS NOT NULL AND lease_token IS NOT NULL)),
    CHECK ((state = 'acknowledged') = (acknowledged_at IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS burst_reap_receipts (burst_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS mesh_state (id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS tenant_audit        (id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, data JSONB NOT NULL, appended_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS cloud_account_leases (burst_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, cloud_account_id TEXT NOT NULL, expires_at TIMESTAMPTZ NOT NULL);
CREATE INDEX IF NOT EXISTS workloads_burst_id ON workloads ((data->>'BurstID'));
CREATE INDEX IF NOT EXISTS workloads_customer_id ON workloads ((data->>'CustomerID'));
CREATE INDEX IF NOT EXISTS customers_token_hash ON customers ((data->>'TokenHash'));
CREATE INDEX IF NOT EXISTS customers_cluster_credentials ON customers USING GIN ((data->'RegisteredClusters') jsonb_path_ops);
CREATE INDEX IF NOT EXISTS customers_publisher_credentials ON customers USING GIN ((data->'CatalogPublishers') jsonb_path_ops);
CREATE INDEX IF NOT EXISTS tenant_memberships_customer_id ON tenant_memberships ((data->>'CustomerID'));
CREATE INDEX IF NOT EXISTS tenant_memberships_account_id ON tenant_memberships ((data->>'AccountID'));
CREATE INDEX IF NOT EXISTS connector_commands_due ON connector_commands (customer_id, cluster_id, next_attempt_at, id)
    WHERE state IN ('pending','processing','failed');
CREATE INDEX IF NOT EXISTS connector_commands_workload ON connector_commands (customer_id, workload_id, created_at, id);
CREATE INDEX IF NOT EXISTS connector_commands_attention ON connector_commands (customer_id, updated_at DESC, id)
    WHERE state IN ('failed','dead_letter');
CREATE INDEX IF NOT EXISTS tenant_audit_customer_id ON tenant_audit (customer_id, id DESC);
CREATE INDEX IF NOT EXISTS cloud_account_leases_account ON cloud_account_leases (customer_id, cloud_account_id, expires_at);
CREATE TABLE IF NOT EXISTS admission_reservations (
    id               TEXT PRIMARY KEY CHECK (length(btrim(id)) BETWEEN 1 AND 255),
    customer_id      TEXT NOT NULL CHECK (length(btrim(customer_id)) BETWEEN 1 AND 255),
    workload_id      TEXT NOT NULL CHECK (length(btrim(workload_id)) BETWEEN 1 AND 255),
    hourly_micro_usd BIGINT NOT NULL CHECK (hourly_micro_usd >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id, workload_id)
);
CREATE INDEX IF NOT EXISTS idx_admission_reservations_customer
    ON admission_reservations(customer_id, created_at);
CREATE OR REPLACE FUNCTION tenant_audit_append_only() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'tenant_audit is append-only: % is refused', TG_OP;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS tenant_audit_no_rewrite ON tenant_audit;
CREATE TRIGGER tenant_audit_no_rewrite BEFORE UPDATE OR DELETE ON tenant_audit
  FOR EACH ROW EXECUTE FUNCTION tenant_audit_append_only();
`

const (
	tblCustomers   = "customers"
	tblTombstones  = "customer_tombstones"
	tblAccounts    = "accounts"
	tblMemberships = "tenant_memberships"

	tblAudit = "tenant_audit"

	tblWorkloads             = "workloads"
	tblBursts                = "bursts"
	tblPVs                   = "persistent_volumes"
	tblPodSlots              = "pod_slots"
	tblIdempotency           = "workload_idempotency"
	tblBurstReaps            = "burst_reap_receipts"
	tblMeshState             = "mesh_state"
	tblCloudAccountLeases    = "cloud_account_leases"
	tblAdmissionReservations = "admission_reservations"
)

// meshStateRowID keys the single row in tblMeshState. The table is a table
// rather than a column somewhere because the state it holds belongs to a
// tailnet, not to a tenant — see meshState — and a primary key it can be
// upserted against is what makes the write idempotent across replicas.
const meshStateRowID = "mesh"

// pgSchemaLockKey namespaces ensureSchema's advisory lock. The value is
// arbitrary ("YSCALE" in ASCII, plus a discriminator) but every central must
// agree on it, and nothing else in this database may reuse it.
const pgSchemaLockKey int64 = 0x595343414C450001

// schemaTx is the slice of pgx.Tx that applySchema drives. pgx.Tx is the only
// implementation; it is named as an interface so the lock ordering and the
// release-on-failure guarantee are testable without a Postgres.
type schemaTx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// ensureSchema applies pgSchema under an advisory lock, idempotently, on every
// start.
//
// The lock is not belt-and-braces: CREATE TABLE IF NOT EXISTS is not safe
// against itself. Two sessions creating the same missing table can both pass
// the existence check, and the loser then fails on a catalogue unique index
// ("duplicate key value violates unique constraint pg_type_typname_nsp_index",
// SQLSTATE 23505). NewPostgres returns that error, so the pod crash-loops at
// boot. One replica with Recreate never overlaps; N replicas rolling out
// together against a new database do — observed, not theorised.
//
// Serialising is preferred over retrying the DDL. A retry has to decide which
// SQLSTATEs mean "someone else got there first" — 23505 on pg_type or pg_class,
// 42P07 from the index — and Postgres documents none of that as a contract, so
// the retry set is a guess that also has to be bounded and can mask a real
// schema conflict. The lock removes the race instead of recovering from it: the
// waiter runs the batch only after the winner committed, by which point every
// IF NOT EXISTS is simply true.
//
// pg_advisory_xact_lock, not the session-scoped form: the transaction releases
// it on COMMIT and on ROLLBACK, so failing DDL cannot strand it and no unlock
// call can be skipped. Exactly one lock is ever held, so two contending pods
// have no cycle to deadlock on, and ctx bounds the wait. Uncontended — the
// single-replica case — acquiring it is one local round trip.
func (p *pgPersister) ensureSchema(ctx context.Context) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("state: ensure schema: begin: %w", err)
	}
	return applySchema(ctx, tx)
}

func applySchema(ctx context.Context, tx schemaTx) error {
	// Also the lock release on every early return; a no-op after Commit.
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, pgSchemaLockKey); err != nil {
		return fmt.Errorf("state: ensure schema: lock: %w", err)
	}
	if _, err := tx.Exec(ctx, pgSchema); err != nil {
		return fmt.Errorf("state: ensure schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("state: ensure schema: commit: %w", err)
	}
	return nil
}

// opCtx bounds a single durable operation issued from the synchronous store
// mutation path (which carries no caller context of its own).
func opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func (p *pgPersister) upsert(table, id string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s %s: %w", table, id, err)
	}
	ctx, cancel := opCtx()
	defer cancel()
	_, err = p.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, data, updated_at) VALUES ($1, $2, now())
		 ON CONFLICT (id) DO UPDATE SET data = $2, updated_at = now()`, table), id, data)
	if err != nil {
		return fmt.Errorf("upsert %s %s: %w", table, id, err)
	}
	return nil
}

func (p *pgPersister) del(table, id string) error {
	ctx, cancel := opCtx()
	defer cancel()
	if _, err := p.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, table), id); err != nil {
		return fmt.Errorf("delete %s %s: %w", table, id, err)
	}
	return nil
}

// upsertCustomerStmt writes a whole customer document, under two guards that
// protect two different states of the same id.
//
// The tombstone guard protects an id whose ROW IS GONE. Once an offboard
// finalizes, there is nothing left for an ON CONFLICT to conflict with, so an
// unguarded upsert by a replica still holding the pre-delete record is a plain
// INSERT: a live customer, with a working token, resurrected out of a stale
// snapshot. The NOT EXISTS suppresses the insert instead, which also means the
// ON CONFLICT branch never runs for a tombstoned id, and the caller sees zero
// rows affected.
//
// The RevokedAt guard protects an id whose ROW IS STILL THERE and revoked.
// Revocation is one-way: nothing in this store un-revokes a tenant, so a write
// that carries no stamp is a writer that has not seen the revoke yet, not a
// writer undoing it. The CASE keeps the stored stamp; only a row that is not
// revoked takes the incoming document whole. COALESCE folds "key absent"
// (RevokedAt is omitempty) and "key present but null" into the same
// not-revoked case.
//
// The row guard is the other write direction from revokeCustomer's jsonb_set,
// which patches the stamp on without touching fields it never read. That one
// stops a revoke from erasing a concurrent mesh attach; this one stops a
// concurrent mesh attach — or an AddCustomer re-seeding an id from env config —
// from erasing the revoke. Neither covers the other, and neither covers the
// deleted-row case the tombstone owns. Only a stale-read window on ANOTHER
// replica needs any of this SQL: Store.custMu already orders the writers on
// this one.
//
// $2 is cast to jsonb at EVERY occurrence, here and in the two statements below.
// The tombstone guard is what forces it: guarding the insert means writing
// INSERT ... SELECT rather than INSERT ... VALUES, and a parameter in a select
// list takes no type from the column it lands in — it resolves to text, which
// Postgres will not assign to a jsonb column ("column data is of type jsonb but
// expression is of type text"). That is a parse-time failure of the statement,
// not a bad row, so it would take the whole write path down. The VALUES-form
// upserts above need no cast for the same reason in reverse: there the target
// column types the parameter.
func upsertCustomerStmt(table, tombstones string) string {
	return fmt.Sprintf(
		`INSERT INTO %s (id, data, updated_at)
		 SELECT $1, $2::jsonb, now()
		 WHERE NOT EXISTS (SELECT 1 FROM %s WHERE id = $1)
		 ON CONFLICT (id) DO UPDATE SET data = CASE
		     WHEN COALESCE(%s.data->'RevokedAt', 'null'::jsonb) <> 'null'::jsonb
		     THEN jsonb_set($2::jsonb, '{RevokedAt}', %s.data->'RevokedAt')
		     ELSE $2::jsonb
		   END, updated_at = now()
		 WHERE %s.data = $3::jsonb
		 RETURNING data`, table, tombstones, table, table, table)
}

// createCustomerStmt is createTenant's insert: DO NOTHING rather than an update,
// so the primary key is the uniqueness check, under the same tombstone guard as
// upsertCustomerStmt and with the same jsonb cast on $2. Zero rows affected is
// the refusal — an id that is taken, or one that was retired.
func createCustomerStmt(table, tombstones string) string {
	return fmt.Sprintf(
		`INSERT INTO %s (id, data, updated_at)
		 SELECT $1, $2::jsonb, now()
		 WHERE NOT EXISTS (SELECT 1 FROM %s WHERE id = $1)
		 ON CONFLICT (id) DO NOTHING`, table, tombstones)
}

// insertRevokedCustomerStmt is revokeCustomer's fallback, for a revoke whose
// customer row is not there to patch. Same tombstone guard and same jsonb cast:
// "no row to patch" is also what a finalized id looks like, and an unguarded
// insert would hand a retired id a row again.
func insertRevokedCustomerStmt(table, tombstones string) string {
	return fmt.Sprintf(
		`INSERT INTO %s (id, data, updated_at)
		 SELECT $1, $2::jsonb, now()
		 WHERE NOT EXISTS (SELECT 1 FROM %s WHERE id = $1)
		 ON CONFLICT (id) DO UPDATE SET
		 data = jsonb_set(%s.data, '{RevokedAt}', $2::jsonb->'RevokedAt'),
		 updated_at = now()`, table, tombstones, table)
}

// upsertCustomer conditionally writes a whole document through writeCustomer.
// A stale working copy receives conflict state for a caller-controlled retry;
// a permanently retired id still unwraps to ErrCustomerTombstoned.
func (p *pgPersister) upsertCustomer(c *Customer) error {
	data, err := p.customerData(c)
	if err != nil {
		return err
	}
	ctx, cancel := opCtx()
	defer cancel()
	written, err := p.writeCustomer(ctx, p.pool, c, data)
	if err != nil {
		return err
	}
	c.persistedData = written
	return nil
}

func (p *pgPersister) upsertAccount(a *Account) error {
	// A cached envelope cannot say whether this was identity-only resolution
	// or an explicit issuer refresh. All Store entry points carry that intent
	// through resolveAccountIdentity instead.
	return fmt.Errorf("%w: account upsert requires a current-state operation", ErrPersistence)
}

// Legacy write-throughs cannot carry a current-state authorization decision.
// Keep the persister interface fail-closed for any forgotten caller; PostgreSQL
// Store entry points use mutateMembership and its atomic audit instead.
func (p *pgPersister) upsertMembership(m *TenantMembership, ev *AuditEvent) error {
	return fmt.Errorf("%w: membership upsert requires a current-state transaction", ErrPersistence)
}

func (p *pgPersister) deleteMembership(id string, ev *AuditEvent) error {
	return fmt.Errorf("%w: membership delete requires a current-state transaction", ErrPersistence)
}

// upsertCustomerAudited is upsertCustomer plus the audit row, in one
// transaction. The compare-and-swap and audit share a commit: a stale write
// appends no event, and a failed audit cannot advance the write precondition.
func (p *pgPersister) upsertCustomerAudited(c *Customer, ev *AuditEvent) error {
	data, err := p.customerData(c)
	if err != nil {
		return err
	}
	var written string
	err = p.inTx(fmt.Sprintf("upsert customer %s", c.ID), func(ctx context.Context, tx pgx.Tx) error {
		var err error
		written, err = p.writeCustomer(ctx, tx, c, data)
		if err != nil {
			return err
		}
		return insertAuditTx(ctx, tx, ev)
	})
	if err == nil {
		c.persistedData = written
	}
	return err
}

// submitWorkload records a new workload and the decision that admitted it in
// one transaction. ON CONFLICT DO UPDATE on the workload rather than DO NOTHING:
// ids are minted per submission so a conflict is not expected, and a retry of
// the same submission should converge rather than commit an audit row against a
// workload the statement silently skipped. It is upsertWorkloadStmt because
// that is the one statement that writes this row: a submission carries no cost
// observation, and the guard costs nothing on an id nothing has conflicted with.
func (p *pgPersister) submitWorkload(w *Workload, ev *AuditEvent) error {
	data, err := json.Marshal(w)
	if err != nil {
		return fmt.Errorf("marshal workload %s: %w", w.ID, err)
	}
	return p.inTx(fmt.Sprintf("submit workload %s", w.ID), func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, upsertWorkloadStmt(tblWorkloads), w.ID, data)
		if err != nil {
			return fmt.Errorf("upsert %s %s: %w", tblWorkloads, w.ID, err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: terminal workload binding conflict", ErrPersistence)
		}
		return insertAuditTx(ctx, tx, ev)
	})
}

// appendAudit writes one journal row on its own, in a transaction of its own
// because insertAuditTx needs one: the append lock it takes is transaction
// scoped, and the row's id is only ordered correctly while that lock is held to
// commit. inTx is that transaction — the same one the audited state writes use,
// so there is one begin/rollback/commit shape and one place it can be wrong.
func (p *pgPersister) appendAudit(ev *AuditEvent) error {
	return p.inTx(auditLabel(ev), func(ctx context.Context, tx pgx.Tx) error {
		return insertAuditTx(ctx, tx, ev)
	})
}

// auditLabel names an append for the begin/commit failure messages. By action
// and tenant, never by id: the id does not exist until the lock below is held,
// so a begin that failed has none to quote.
func auditLabel(ev *AuditEvent) string {
	if ev == nil {
		return "append audit"
	}
	return fmt.Sprintf("append audit %s for %s", ev.Action, ev.CustomerID)
}

// auditLockClass namespaces the per-tenant append lock in Postgres's two-int
// advisory space, which does not overlap the single-bigint space pgSchemaLockKey
// uses — so the two can never collide however the hash below lands. The value is
// arbitrary but every central must agree on it.
const auditLockClass int32 = 0x59534141 // "YSAA"

// auditLockID hashes a tenant id into the second half of the lock key. A
// collision costs two tenants a shared lock — throughput, briefly — and never
// correctness, which is why a 32-bit hash is enough.
func auditLockID(customerID string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(customerID))
	return int32(h.Sum32())
}

// insertAuditTx appends one journal row inside a caller's transaction. A nil
// event is a no-op, which is what an idempotent change that wrote nothing
// passes — there is no state transition to record, and no lock to take for it.
//
// The three statements are one mechanism, and the order is the whole of it.
//
//  1. Take the tenant's advisory lock. It is transaction scoped, so it is held
//     until this caller COMMITS and released by the commit itself.
//  2. Read the newest id already committed for the tenant. The previous lock
//     holder committed before it released the lock, so its row is visible here
//     (READ COMMITTED, the pool's default: this SELECT takes its own snapshot).
//  3. Stamp an id above that one and insert.
//
// Together those make the id ORDER the tenant's COMMIT order, which is what the
// descending cursor read depends on. Stamping before the lock — the shape this
// replaced — let a slow transaction commit a row whose id was already behind a
// cursor a reader had been handed, and that row is then skipped forever: the
// next page asks for ids strictly LESS than the cursor, and this one is greater.
//
// The lock is taken here, at the END of every audited transaction, and each
// takes at most one. So a transaction that holds it has no statement left but
// this INSERT, and cannot be waiting on a row lock held by a transaction queued
// behind it — there is no cycle for the state writes above to deadlock with.
// Moving the lock earlier, or taking a second one, is what would create one.
//
// The wait is bounded by the caller's ctx (opCtx's budget for a store mutation),
// so a queue that cannot drain fails the append rather than hanging it. What is
// held is one insert per waiter, measured in microseconds; the serialization is
// per TENANT, so one tenant's journal never queues behind another's.
func insertAuditTx(ctx context.Context, tx pgx.Tx, ev *AuditEvent) error {
	if ev == nil {
		return nil
	}
	// Cast both keys explicitly: the two-int overload is what puts this lock in a
	// key space the schema lock's single-bigint form can never collide with, and
	// leaving the parameter types to inference is how a caller ends up in the
	// other one.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock($1::int4, $2::int4)`,
		auditLockClass, auditLockID(ev.CustomerID)); err != nil {
		return fmt.Errorf("%s: lock: %w", auditLabel(ev), err)
	}
	var prev string
	if err := tx.QueryRow(ctx, fmt.Sprintf(
		`SELECT COALESCE(max(id), '') FROM %s WHERE customer_id = $1`, tblAudit),
		ev.CustomerID).Scan(&prev); err != nil {
		return fmt.Errorf("%s: read cursor: %w", auditLabel(ev), err)
	}
	stampAudit(ev, prev)
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal audit %s: %w", ev.ID, err)
	}
	// INSERT with no conflict arm: ids are minted under the lock and the table
	// refuses UPDATE outright, so a duplicate id is a bug worth surfacing rather
	// than an upsert to absorb.
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, customer_id, data, appended_at) VALUES ($1, $2, $3, now())`, tblAudit),
		ev.ID, ev.CustomerID, data); err != nil {
		return fmt.Errorf("append audit %s: %w", ev.ID, err)
	}
	return nil
}

// listAudit reads one tenant's journal newest-first. The customer_id predicate
// and the ordering are both served by tenant_audit_customer_id; the cursor is
// an id comparison because audit ids sort by append time, so "older than this
// row" and "after this cursor" are the same condition.
//
// Takes the caller's ctx rather than opCtx's fixed budget, like the other read
// paths that serve a request: the handler already bounds it.
func (p *pgPersister) listAudit(ctx context.Context, customerID, after string, limit int) ([]*AuditEvent, error) {
	// One statement for both cases: an empty cursor compares against '' and
	// every audit id sorts above it, so the first page needs no second query
	// (and no branch that could scope one of them differently).
	rows, err := p.pool.Query(ctx, fmt.Sprintf(
		`SELECT data FROM %s WHERE customer_id = $1 AND ($2 = '' OR id < $2)
		 ORDER BY id DESC LIMIT $3`, tblAudit), customerID, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list %s for %s: %w", tblAudit, customerID, err)
	}
	defer rows.Close()
	var out []*AuditEvent
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan %s: %w", tblAudit, err)
		}
		var ev AuditEvent
		if err := json.Unmarshal(data, &ev); err != nil {
			return nil, fmt.Errorf("decode %s: %w", tblAudit, err)
		}
		out = append(out, &ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list %s for %s: %w", tblAudit, customerID, err)
	}
	return out, nil
}

// inTx runs fn inside one bounded transaction, rolling back on any error and on
// every early return. The label prefixes the begin/commit failures so a caller's
// log says which write could not be made atomic.
func (p *pgPersister) inTx(label string, fn func(context.Context, pgx.Tx) error) error {
	ctx, cancel := opCtx()
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s: begin: %w", label, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%s: commit: %w", label, err)
	}
	return nil
}

func (p *pgPersister) acquireCloudAccountLease(ctx context.Context, lease *CloudAccountLease) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire cloud-account lease: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var accountID string
	err = tx.QueryRow(ctx, fmt.Sprintf(
		`SELECT data->'LinodeCloudAccount'->>'ID' FROM %s
		 WHERE id = $1 AND data->'LinodeCloudAccount'->>'ID' = $2 FOR KEY SHARE`, tblCustomers),
		lease.CustomerID, lease.CloudAccountID).Scan(&accountID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, fmt.Errorf("lock cloud account: %w", err)
	}
	tag, err := tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (burst_id, customer_id, cloud_account_id, expires_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (burst_id) DO UPDATE SET customer_id = EXCLUDED.customer_id,
		 cloud_account_id = EXCLUDED.cloud_account_id, expires_at = EXCLUDED.expires_at
		 WHERE %s.expires_at <= now()`, tblCloudAccountLeases, tblCloudAccountLeases),
		lease.BurstID, lease.CustomerID, lease.CloudAccountID, lease.ExpiresAt)
	if err != nil {
		return false, fmt.Errorf("insert cloud-account lease: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("acquire cloud-account lease: commit: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (p *pgPersister) releaseCloudAccountLease(ctx context.Context, burstID string) error {
	_, err := p.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE burst_id = $1`, tblCloudAccountLeases), burstID)
	if err != nil {
		return fmt.Errorf("delete cloud-account lease: %w", err)
	}
	return nil
}

func extendCloudAccountLeaseStmt(table string) string {
	return fmt.Sprintf(`UPDATE %s SET expires_at = GREATEST(expires_at, $4)
		WHERE burst_id = $1 AND customer_id = $2 AND cloud_account_id = $3`, table)
}

func (p *pgPersister) extendCloudAccountLease(ctx context.Context, burstID, customerID, accountID string, expiresAt time.Time) (bool, error) {
	tag, err := p.pool.Exec(ctx, extendCloudAccountLeaseStmt(tblCloudAccountLeases), burstID, customerID, accountID, expiresAt)
	if err != nil {
		return false, fmt.Errorf("extend cloud-account lease: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// disconnectCloudAccount locks the customer row against lease acquisition,
// checks durable bursts and unexpired leases, then clears the credential and
// appends the audit row in the same transaction.
func (p *pgPersister) disconnectCloudAccount(c *Customer, accountID string, now time.Time, ev *AuditEvent) error {
	data, err := p.customerData(c)
	if err != nil {
		return err
	}
	var written string
	err = p.inTx("disconnect cloud account "+accountID, func(ctx context.Context, tx pgx.Tx) error {
		var current *string
		var matches bool
		err := tx.QueryRow(ctx, fmt.Sprintf(
			`SELECT data->'LinodeCloudAccount'->>'ID', COALESCE(data = NULLIF($2, '')::jsonb, false)
			 FROM %s WHERE id = $1 FOR UPDATE`, tblCustomers), c.ID, c.persistedData).Scan(&current, &matches)
		if errors.Is(err, pgx.ErrNoRows) {
			return p.loadCustomerWriteConflict(ctx, tx, c.ID)
		}
		if err != nil {
			return fmt.Errorf("lock customer cloud account: %w", err)
		}
		if !matches {
			return p.loadCustomerWriteConflict(ctx, tx, c.ID)
		}
		if current == nil || *current != accountID {
			return ErrCloudAccountMismatch
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE expires_at <= $1`, tblCloudAccountLeases), now); err != nil {
			return fmt.Errorf("expire cloud-account leases: %w", err)
		}
		var inUse bool
		if err := tx.QueryRow(ctx, fmt.Sprintf(
			`SELECT EXISTS (SELECT 1 FROM %s WHERE data->>'CustomerID' = $1 AND data->>'CloudAccountID' = $2)
			 OR EXISTS (SELECT 1 FROM %s WHERE customer_id = $1 AND cloud_account_id = $2 AND expires_at > $3)`,
			tblBursts, tblCloudAccountLeases), c.ID, accountID, now).Scan(&inUse); err != nil {
			return fmt.Errorf("check cloud-account use: %w", err)
		}
		if inUse {
			return ErrCloudAccountInUse
		}
		written, err = p.writeCustomer(ctx, tx, c, data)
		if err != nil {
			return fmt.Errorf("clear customer cloud account: %w", err)
		}
		return insertAuditTx(ctx, tx, ev)
	})
	if err == nil {
		c.persistedData = written
	}
	return err
}

// createTenant inserts a brand-new tenant — and, when one is supplied, its
// first owner grant — clearing any membership rows left over for that id in the
// same transaction. INSERT ... DO NOTHING rather than an upsert: the primary key
// is then the uniqueness check, so an id held by a tenant this replica never
// loaded — or by a revoked one still awaiting cleanup — is refused as
// ErrCustomerExists instead of overwritten. Zero rows affected is that refusal,
// and it rolls the membership delete back with it, so a rejected create changes
// nothing at all.
//
// The tombstone check rides the same statement, and answers ErrCustomerExists
// for the same reason: an id that a finished offboard retired is taken, forever,
// even though its row is gone. The primary key cannot say so — that is the whole
// reason the tombstone table exists — and a create is the one caller that must
// never be told an id is free when a tenant once held it. Ids are generated, so
// nothing legitimate ever needs one back.
//
// The membership delete is what stops a reused id from inheriting a stale
// grant. Removing those rows at load time instead would be a read path deleting
// data it cannot know is unrecoverable; doing it here scopes the removal to the
// exact moment the id changes hands.
//
// The owner's account row is re-checked inside the transaction. Not to widen
// the caller's contract — Store.CreateTenant has already resolved the account
// in memory, and that is what decides whether the owner exists — but because
// resolving it was a separate durable write: an account whose row never landed
// must not get a grant written on top of it, and the same transaction as the
// insert is the only place that answer cannot go stale. A missing row aborts
// the whole create as ErrNotFound, so the tenant is not left behind for someone
// to undo.
func (p *pgPersister) createTenant(c *Customer, owner *TenantMembership, requireFirst bool) error {
	if requireFirst && owner == nil {
		return ErrNotFound
	}
	candidate := *c
	ctx, cancel := opCtx()
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("create customer %s: begin: %w", c.ID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockMembershipTenant(ctx, tx, c.ID); err != nil {
		return err
	}
	if owner != nil {
		var accountData []byte
		if err := tx.QueryRow(ctx, fmt.Sprintf(
			`SELECT data FROM %s WHERE id = $1 FOR UPDATE`, tblAccounts), owner.AccountID).Scan(&accountData); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("account %q: %w", owner.AccountID, ErrNotFound)
			}
			return fmt.Errorf("create customer %s: lock owner account: %w", c.ID, err)
		}
		if requireFirst {
			account, err := decodeAccountBinding(owner.AccountID, accountData)
			if err != nil {
				return err
			}
			var exists bool
			if err := tx.QueryRow(ctx, fmt.Sprintf(
				`SELECT EXISTS (SELECT 1 FROM %s WHERE data->>'AccountID' = $1)`, tblMemberships), owner.AccountID).Scan(&exists); err != nil {
				return fmt.Errorf("create customer %s: check owner memberships: %w", c.ID, err)
			}
			if err := firstWorkspaceEligibility(account, exists); err != nil {
				return err
			}
			candidate.Email = account.Email
		}
	}
	// Serialize only the authorized candidate. In particular its contact is
	// the verified account row held until commit, not an earlier HTTP profile.
	data, err := p.customerData(&candidate)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE data->>'CustomerID' = $1`, tblMemberships), c.ID); err != nil {
		return fmt.Errorf("create customer %s: clear stale memberships: %w", c.ID, err)
	}
	tag, err := tx.Exec(ctx, createCustomerStmt(tblCustomers, tblTombstones), c.ID, string(data))
	if err != nil {
		return fmt.Errorf("create customer %s: insert: %w", c.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrCustomerExists, c.ID)
	}
	if owner != nil {
		ownerData, err := json.Marshal(owner)
		if err != nil {
			return fmt.Errorf("marshal membership %s: %w", owner.ID, err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (id, data, updated_at) VALUES ($1, $2, now())
			 ON CONFLICT (id) DO UPDATE SET data = $2, updated_at = now()`, tblMemberships),
			owner.ID, ownerData); err != nil {
			return fmt.Errorf("create customer %s: insert owner membership: %w", c.ID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("create customer %s: commit: %w", c.ID, err)
	}
	c.persistedData = string(data)
	c.Email = candidate.Email
	return nil
}

// revokeCustomer cuts a tenant's access in one transaction: the customer row is
// stamped RevokedAt and every membership row naming it is deleted, all or
// nothing. Done as two statements outside a transaction, a failed membership
// delete followed by a successful customer write would leave a grant on a
// tenant nobody can reach — and the next tenant provisioned with that id would
// inherit it. The membership predicate reads the JSONB field (backed by
// tenant_memberships_customer_id) so rows this replica never held are covered.
//
// The customer write PATCHES one field rather than replacing the document. The
// other writers of this row — a mesh attach, a gateway-route update — read,
// modify and write the whole customer, so a revoke that wrote its own snapshot
// back could undo one of them (or be undone by one), and the field that goes
// missing is the stamp that keeps a revoked token from authenticating after the
// next restart. jsonb_set touches RevokedAt and leaves every other field as the
// database has it.
//
// That is only half of it: this patch stops the revoke from erasing a
// concurrent whole-document write, and upsertCustomerStmt stops a concurrent
// whole-document write from erasing the revoke. The in-process lock
// (Store.custMu) orders these writers on one central; the two SQL forms are
// what make them safe against a replica that read the row before the revoke.
//
// A row that isn't there yet — a customer whose best-effort AddCustomer write
// was dropped — has nothing to patch, so the stamped record is inserted whole.
// That insert carries the tombstone guard, because "no row to patch" is also
// what a finalized id looks like: without it, revoking a tenant another replica
// already offboarded would write the customer back, revoked but present, and an
// id that was permanently retired would be holding a row again. A guarded-away
// insert is not a failure — the tenant is gone and its access with it, which is
// everything this call was asked to guarantee — so the revoke reports success.
func (p *pgPersister) revokeCustomer(c *Customer) error {
	if c.RevokedAt == nil {
		return fmt.Errorf("revoke %s: customer is not stamped revoked", c.ID)
	}
	ctx, cancel := opCtx()
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("revoke %s: begin: %w", c.ID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockMembershipTenant(ctx, tx, c.ID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE data->>'CustomerID' = $1`, tblMemberships), c.ID); err != nil {
		return fmt.Errorf("revoke %s: delete memberships: %w", c.ID, err)
	}
	stamp := c.RevokedAt.Format(time.RFC3339Nano)
	tag, err := tx.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET data = jsonb_set(data, '{RevokedAt}', to_jsonb($2::text)), updated_at = now()
		 WHERE id = $1`, tblCustomers), c.ID, stamp)
	if err != nil {
		return fmt.Errorf("revoke %s: mark customer revoked: %w", c.ID, err)
	}
	if tag.RowsAffected() == 0 {
		data, err := p.customerData(c)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, insertRevokedCustomerStmt(tblCustomers, tblTombstones),
			c.ID, string(data)); err != nil {
			return fmt.Errorf("revoke %s: insert revoked customer: %w", c.ID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("revoke %s: commit: %w", c.ID, err)
	}
	return nil
}

// finalizeRevokedCustomer is an offboard's final step, run once cleanup
// verifies clean. The tombstone and both deletes are ONE transaction, so the id
// is retired or nothing happened — a crash cannot land the delete without the
// record that the id was used.
//
// Within that transaction the tombstone goes first for a plain reason: it reads
// RevokedAt off the customers row, and the delete below removes that row. Order
// them the other way and the sub-select finds nothing, so every tombstone would
// record a null revoked_at.
//
// revoked_at is read off the row being retired rather than passed in, so the
// tombstone records when access was actually cut rather than when an operator
// got round to the final delete. It is a sub-select, not a join, so the
// tombstone lands even when the row is already gone — a replica finalizing an
// id whose row another one removed still means "retire this id", and reading
// the stamp is the only part that depends on the row. ON CONFLICT DO NOTHING
// keeps a retry — the step is idempotent, and a second offboard attempt after a
// partial failure is the normal path — from restamping an id already retired.
//
// The membership delete is redundant after a revoke, which already took them,
// but a revoke that landed on another replica is not something this transaction
// can see, so it stays: it costs an indexed delete of nothing.
func (p *pgPersister) finalizeRevokedCustomer(id string) error {
	ctx, cancel := opCtx()
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("offboard %s: begin: %w", id, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockMembershipTenant(ctx, tx, id); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, revoked_at, deleted_at) VALUES ($1,
		     (SELECT (data->>'RevokedAt')::timestamptz FROM %s WHERE id = $1), now())
		 ON CONFLICT (id) DO NOTHING`, tblTombstones, tblCustomers), id); err != nil {
		return fmt.Errorf("offboard %s: record tombstone: %w", id, err)
	}
	if err := deleteCustomerRows(ctx, tx, id); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("offboard %s: commit: %w", id, err)
	}
	return nil
}

// deleteCustomerAndMemberships is the legacy best-effort DeleteCustomer path.
// It leaves no tombstone: the offboard lifecycle above is what retires an id,
// and this call is the OSS/dev "remove this record" verb, whose callers
// (re-seeding from config, a test tearing its fixture down) do reuse the id
// they just removed. The membership delete is not redundant here — there is no
// revoke to have taken them — and shares the customer row's transaction for the
// reason revokeCustomer gives.
func (p *pgPersister) deleteCustomerAndMemberships(id string) error {
	ctx, cancel := opCtx()
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("delete customer %s: begin: %w", id, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockMembershipTenant(ctx, tx, id); err != nil {
		return err
	}
	if err := deleteCustomerRows(ctx, tx, id); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("delete customer %s: commit: %w", id, err)
	}
	return nil
}

// deleteCustomerRows removes one customer and every membership naming it. Both
// final-step transactions above run it; only the tombstone differs between them.
func deleteCustomerRows(ctx context.Context, tx pgx.Tx, id string) error {
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE data->>'CustomerID' = $1`, tblMemberships), id); err != nil {
		return fmt.Errorf("delete customer %s: delete memberships: %w", id, err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE id = $1`, tblCustomers), id); err != nil {
		return fmt.Errorf("delete customer %s: delete customer: %w", id, err)
	}
	return nil
}

// upsertWorkloadStmt writes a whole workload document, preserving the stored
// Cost when the incoming document has none and preserving an exact billing
// association's sticky ManualAttention flag.
//
// It is the same shape of guard upsertCustomerStmt puts on RevokedAt, and it is
// here for the same reason: one field on this row is written by a path that
// other writers cannot see, so a whole-document write from a replica that has
// not seen it is a writer that never had the field — not one clearing it. The
// workload's lifecycle fields move constantly (started, finished, a
// cross-replica catch-up); the cost is frozen exactly once, by whichever replica
// won the teardown claim, and the burst that produced it is deleted by that same
// claim. Erasing it therefore erases it for good.
//
// The incoming side is checked too, so the ONE writer that does carry a cost —
// a document read back with the observation already on it — still writes it
// whole. COALESCE folds "key absent" (Cost is omitempty) and "key present but
// null" into the same no-cost case on both sides.
//
// No jsonb cast is needed on $2: this is the VALUES form, so the column types
// the parameter, and EXCLUDED.data is already jsonb by the time the CASE reads
// it (see upsertCustomerStmt for the INSERT ... SELECT form that does need one).
// upsertWorkloadStmt writes a whole workload document, preserving the stored
// Cost, monotonic NodeObservation, write-once PodObservation and GPUObservation,
// monotonic SchedulingObservation, and exact billing association's
// ManualAttention flag when the incoming document is stale.
func mergedWorkloadDataExpr(table string) string {
	// stored_obs_wins is true when the stored NodeObservation is present and the
	// incoming one is absent, or when the incoming one would regress the stored
	// under source-vs-legacy ordering (legacy cannot overwrite source; within
	// the same class, older-or-equal cannot overwrite newer).
	return fmt.Sprintf(
		`CASE
		     WHEN COALESCE(%[1]s.data->'NodeObservation', 'null'::jsonb) <> 'null'::jsonb
		      AND (COALESCE(EXCLUDED.data->'NodeObservation', 'null'::jsonb) = 'null'::jsonb
		           OR (NOT COALESCE((EXCLUDED.data->'NodeObservation'->>'SourceTimestamped')::bool, false)
		               AND COALESCE((%[1]s.data->'NodeObservation'->>'SourceTimestamped')::bool, false))
		           OR (COALESCE((EXCLUDED.data->'NodeObservation'->>'SourceTimestamped')::bool, false)
		               = COALESCE((%[1]s.data->'NodeObservation'->>'SourceTimestamped')::bool, false)
		               AND (EXCLUDED.data->'NodeObservation'->>'ObservedAt')::timestamptz
		                   <= (%[1]s.data->'NodeObservation'->>'ObservedAt')::timestamptz))
		     THEN jsonb_set(
		       CASE
		         WHEN COALESCE(EXCLUDED.data->'Cost', 'null'::jsonb) = 'null'::jsonb
		          AND COALESCE(%[1]s.data->'Cost', 'null'::jsonb) <> 'null'::jsonb
		         THEN jsonb_set(EXCLUDED.data, '{Cost}', %[1]s.data->'Cost')
		         ELSE EXCLUDED.data
		       END, '{NodeObservation}', %[1]s.data->'NodeObservation')
		     WHEN COALESCE(EXCLUDED.data->'Cost', 'null'::jsonb) = 'null'::jsonb
		      AND COALESCE(%[1]s.data->'Cost', 'null'::jsonb) <> 'null'::jsonb
		     THEN jsonb_set(EXCLUDED.data, '{Cost}', %[1]s.data->'Cost')
		     ELSE EXCLUDED.data
		   END
		   || CASE
		        WHEN COALESCE(%[1]s.data->'PodObservation', 'null'::jsonb) <> 'null'::jsonb
		        THEN jsonb_build_object('PodObservation', %[1]s.data->'PodObservation')
		        ELSE '{}'::jsonb
		      END
		   || CASE
		        WHEN COALESCE(%[1]s.data->'GPUObservation', 'null'::jsonb) <> 'null'::jsonb
		        THEN jsonb_build_object('GPUObservation', %[1]s.data->'GPUObservation')
		        ELSE '{}'::jsonb
		      END
		   || CASE
		        WHEN COALESCE(%[1]s.data->'SchedulingObservation', 'null'::jsonb) <> 'null'::jsonb
		         AND (
		             COALESCE(EXCLUDED.data->'SchedulingObservation', 'null'::jsonb) = 'null'::jsonb
		             OR %[1]s.data->'SchedulingObservation'->>'State' = 'Scheduled'
		             OR NOT (
		                 EXCLUDED.data->'SchedulingObservation'->>'State' = 'Scheduled'
		                 OR COALESCE(
		                        NULLIF(EXCLUDED.data->'SchedulingObservation'->>'ObservedAt', '')::timestamptz,
		                        '-infinity'::timestamptz)
		                    > COALESCE(
		                        NULLIF(%[1]s.data->'SchedulingObservation'->>'ObservedAt', '')::timestamptz,
		                        '-infinity'::timestamptz)
		                 OR (
		                     COALESCE(EXCLUDED.data->'SchedulingObservation'->>'PodName', '') <> ''
		                     AND COALESCE(%[1]s.data->'SchedulingObservation'->>'PodName', '') <> ''
		                     AND EXCLUDED.data->'SchedulingObservation'->>'PodName'
		                         <> %[1]s.data->'SchedulingObservation'->>'PodName'
		                     AND COALESCE(
		                            NULLIF(EXCLUDED.data->'SchedulingObservation'->>'ObservedAt', '')::timestamptz,
		                            '-infinity'::timestamptz)
		                         = COALESCE(
		                            NULLIF(%[1]s.data->'SchedulingObservation'->>'ObservedAt', '')::timestamptz,
		                            '-infinity'::timestamptz)
		                 )
		             )
		         )
		        THEN jsonb_build_object(
		          'SchedulingObservation', %[1]s.data->'SchedulingObservation')
		        ELSE '{}'::jsonb
		      END`, table)
}

func upsertWorkloadStmt(table string) string {
	merged := fmt.Sprintf("(%s) || (%s)", mergedWorkloadDataExpr(table), preservedWorkloadTransitionExpr(table))
	return fmt.Sprintf(
		`INSERT INTO %[1]s (id, data, updated_at) VALUES ($1, $2, now())
		 ON CONFLICT (id) DO UPDATE SET data = CASE
		   WHEN COALESCE((%[1]s.data->'Billing'->>'ManualAttention')::bool, false)
		    AND %[1]s.data->>'CustomerID' = EXCLUDED.data->>'CustomerID'
		    AND %[1]s.data->'Billing'->>'WorkloadRef' = EXCLUDED.data->'Billing'->>'WorkloadRef'
		    AND %[1]s.data->'Billing'->>'HoldID' = EXCLUDED.data->'Billing'->>'HoldID'
		   THEN jsonb_set(%[2]s, '{Billing,ManualAttention}', 'true'::jsonb)
		   ELSE %[2]s
		 END, updated_at = now()
		 WHERE %[1]s.data->>'FinishedAt' IS NULL OR (
		   %[1]s.data->>'ID' = EXCLUDED.data->>'ID'
		   AND %[1]s.data->>'CustomerID' = EXCLUDED.data->>'CustomerID'
		   AND COALESCE(%[1]s.data->>'ClusterID','') = COALESCE(EXCLUDED.data->>'ClusterID','')
		   AND COALESCE(%[1]s.data->>'BurstID','') = COALESCE(EXCLUDED.data->>'BurstID',''))`, table, merged)
}

// recordWorkloadCostStmt patches the frozen cost observation onto the workload
// row a burst backed. It is a targeted UPDATE rather than a read-modify-write
// for the reason claimBurst is a DELETE ... RETURNING: the decision must be made
// once across every replica, and two replicas each reading the row first would
// both find no cost and both write one.
//
// The predicate carries first-write-wins with it — a row that already has an
// observation matches nothing, so a redelivered teardown job cannot move a
// frozen timestamp forward. It reads the burst id out of the document for the
// same reason workloadByBurst does (workloads are stored whole, and the
// workloads_burst_id expression index keeps it a lookup), and it touches only
// the Cost key, so a lifecycle write landing concurrently keeps its status.
func recordWorkloadCostStmt(table string) string {
	return fmt.Sprintf(
		`UPDATE %[1]s SET data = jsonb_set(data, '{Cost}', $2::jsonb), updated_at = now()
		 WHERE data->>'BurstID' = $1
		   AND COALESCE(data->'Cost', 'null'::jsonb) = 'null'::jsonb`, table)
}

func (p *pgPersister) upsertWorkload(w *Workload) error {
	data, err := json.Marshal(w)
	if err != nil {
		return fmt.Errorf("marshal workload %s: %w", w.ID, err)
	}
	ctx, cancel := opCtx()
	defer cancel()
	tag, err := p.pool.Exec(ctx, upsertWorkloadStmt(tblWorkloads), w.ID, data)
	if err != nil {
		return fmt.Errorf("upsert %s %s: %w", tblWorkloads, w.ID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: terminal workload binding conflict", ErrPersistence)
	}
	return nil
}

// recordWorkloadCost receives a context detached from the initiating request
// and bounded by Store.RecordWorkloadCostForBurst. Teardown has already been
// accepted or completed, so request cancellation must not erase its history.
func (p *pgPersister) recordWorkloadCost(ctx context.Context, burstID string, c *WorkloadCost) (bool, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return false, fmt.Errorf("marshal workload cost for burst %s: %w", burstID, err)
	}
	tag, err := p.pool.Exec(ctx, recordWorkloadCostStmt(tblWorkloads), burstID, string(data))
	if err != nil {
		return false, fmt.Errorf("record workload cost for burst %s: %w", burstID, err)
	}
	return tag.RowsAffected() > 0, nil
}

func (p *pgPersister) recordBurstReapReceipt(ctx context.Context, burstID, customerID string) (bool, error) {
	if _, err := p.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (burst_id, customer_id) VALUES ($1, $2) ON CONFLICT (burst_id) DO NOTHING`,
		tblBurstReaps), burstID, customerID); err != nil {
		return false, fmt.Errorf("record %s %s: %w", tblBurstReaps, burstID, err)
	}
	return p.burstReapRecorded(ctx, burstID, customerID)
}

func (p *pgPersister) burstReapRecorded(ctx context.Context, burstID, customerID string) (bool, error) {
	var recorded bool
	if err := p.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT EXISTS (SELECT 1 FROM %s WHERE burst_id = $1 AND customer_id = $2)`,
		tblBurstReaps), burstID, customerID).Scan(&recorded); err != nil {
		return false, fmt.Errorf("read %s %s: %w", tblBurstReaps, burstID, err)
	}
	return recorded, nil
}

// workloadByBurst finds the workload a burst backs. The predicate reads the
// JSONB field because workloads are stored whole (see pgPersister); the
// workloads_burst_id expression index in pgSchema is what keeps this a lookup
// rather than a scan of every workload ever run. A burst with no workload row
// yields (nil, nil) — the caller has nothing to mark failed, which is not an
// error.
func (p *pgPersister) workloadByBurst(ctx context.Context, burstID string) (*Workload, error) {
	var data []byte
	err := p.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT data FROM %s WHERE data->>'BurstID' = $1 LIMIT 1`, tblWorkloads), burstID).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("workload for burst %s: %w", burstID, err)
	}
	var w Workload
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("decode %s for burst %s: %w", tblWorkloads, burstID, err)
	}
	return &w, nil
}

// listBursts reads every durable burst row. Unlike loadAll this is called
// repeatedly while central serves, so it takes the caller's ctx rather than
// opCtx's fixed budget — the orphan sweep already time-boxes itself.
func (p *pgPersister) listBursts(ctx context.Context) ([]*Burst, error) {
	var out []*Burst
	if err := p.eachRow(ctx, tblBursts, func(b []byte) error {
		var bu Burst
		if err := json.Unmarshal(b, &bu); err != nil {
			return err
		}
		out = append(out, &bu)
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// claimBurst is the reap winner election, decided by the database. A single
// DELETE ... RETURNING can match a given id at most once, so of N concurrent
// claimers across N replicas exactly one gets a row back; the rest see
// pgx.ErrNoRows. The returned payload is the burst as the database has it,
// which is what lets a replica reap a burst it never held in memory.
func (p *pgPersister) claimBurst(ctx context.Context, id string) (*Burst, bool, error) {
	var data []byte
	err := p.pool.QueryRow(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE id = $1 RETURNING data`, tblBursts), id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("claim %s %s: %w", tblBursts, id, err)
	}
	var b Burst
	if err := json.Unmarshal(data, &b); err != nil {
		// The row is already deleted and nothing will retry this burst, so the
		// orphan sweep is the only remaining backstop for its VM. Reporting the
		// claim as won would be worse: a burst decoded from a broken payload has
		// no backend ID to tear down.
		return nil, false, fmt.Errorf("decode claimed %s %s: %w", tblBursts, id, err)
	}
	return &b, true, nil
}

// burstOwnedBy answers the tenancy question without touching the row.
//
// SELECT rather than a read of the whole record: the caller is authorising an
// action, not describing a burst, and handing back a record invites deciding
// something else against a copy the next statement may already have lost. The
// id is the primary key, so this is one index probe.
func (p *pgPersister) burstOwnedBy(ctx context.Context, burstID, customerID string) (bool, error) {
	var owned bool
	err := p.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT true FROM %s WHERE id = $1 AND data->>'CustomerID' = $2`, tblBursts),
		burstID, customerID).Scan(&owned)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("owner check %s %s: %w", tblBursts, burstID, err)
	}
	return owned, nil
}

// burstForTenant reads the whole row, with the tenancy predicate in the SAME
// statement rather than applied to what comes back. A read that could return
// another tenant's row at all is one filter bug away from handing a connector
// the node name and cluster of a burst it must never learn exists.
func (p *pgPersister) burstForTenant(ctx context.Context, burstID, customerID string) (*Burst, bool, error) {
	var data []byte
	err := p.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT data FROM %s WHERE id = $1 AND data->>'CustomerID' = $2`, tblBursts),
		burstID, customerID).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read %s %s: %w", tblBursts, burstID, err)
	}
	var b Burst
	if err := json.Unmarshal(data, &b); err != nil {
		// Unknown, not absent. A payload this process cannot decode says nothing
		// about whether the burst is live, and the caller must keep its retry.
		return nil, false, fmt.Errorf("decode %s %s: %w", tblBursts, burstID, err)
	}
	return &b, true, nil
}

// reservePodSlot claims one pod /24 slot, with the database electing the
// winner exactly as claimBurst does for teardown. slot is the PRIMARY KEY, so
// of N replicas racing for the same slot only one write lands; the losers get
// no row back and move on to the next slot.
//
// The ON CONFLICT arm is what makes the TTL mean anything. A plain DO NOTHING
// would leave an abandoned row — a Plan that died between allocating a /24 and
// persisting its burst — wedging its slot forever, because nothing else deletes
// it. Taking an expired row over in the same statement keeps that decision in
// the database too: two replicas that both read the row as expired still yield
// one winner.
//
// Both timestamps come from the database (reserved_at defaults to now(), and
// the comparison is against now()), so a replica with a skewed clock cannot age
// out another replica's live reservation.
func (p *pgPersister) reservePodSlot(ctx context.Context, slot int, burstID string, ttl time.Duration) (bool, error) {
	var got int
	err := p.pool.QueryRow(ctx, fmt.Sprintf(
		`INSERT INTO %[1]s (slot, burst_id, reserved_at) VALUES ($1, $2, now())
		 ON CONFLICT (slot) DO UPDATE SET burst_id = EXCLUDED.burst_id, reserved_at = now()
		 WHERE %[1]s.reserved_at < now() - make_interval(secs => $3::double precision)
		 RETURNING slot`, tblPodSlots), slot, burstID, ttl.Seconds()).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reserve %s %d: %w", tblPodSlots, slot, err)
	}
	return true, nil
}

// releasePodSlot drops the reservation a burst holds.
//
// Keyed by burst id, never by slot. Slots are recycled, so a release that
// arrived by slot could delete the reservation a DIFFERENT burst is currently
// holding — handing that /24 out to a third burst as well, which is the exact
// collision this table exists to prevent.
func (p *pgPersister) releasePodSlot(ctx context.Context, burstID string) error {
	if _, err := p.pool.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE burst_id = $1`, tblPodSlots), burstID); err != nil {
		return fmt.Errorf("release %s for %s: %w", tblPodSlots, burstID, err)
	}
	return nil
}

// reservedPodSlots reads the slots currently held, excluding rows older than
// ttl. The exclusion is the durable equivalent of the allocator's in-memory
// reservation ageing: an expired row belongs to a Plan that never persisted its
// burst, and counting it would take a /24 out of the pool permanently.
func (p *pgPersister) reservedPodSlots(ctx context.Context, ttl time.Duration) (map[int]bool, error) {
	rows, err := p.pool.Query(ctx, fmt.Sprintf(
		`SELECT slot FROM %s WHERE reserved_at > now() - make_interval(secs => $1::double precision)`,
		tblPodSlots), ttl.Seconds())
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", tblPodSlots, err)
	}
	defer rows.Close()
	slots := make(map[int]bool)
	for rows.Next() {
		var slot int
		if err := rows.Scan(&slot); err != nil {
			return nil, fmt.Errorf("scan %s: %w", tblPodSlots, err)
		}
		slots[slot] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list %s: %w", tblPodSlots, err)
	}
	return slots, nil
}

// claimIdempotency is the submission election, decided by the database for the
// reason claimBurst's reap is: N retries of one keyed request may be spread
// across N replicas, and only a shared PRIMARY KEY can let exactly one of them
// provision.
//
// Two statements rather than one, and the order is what makes it correct. The
// INSERT ... ON CONFLICT DO NOTHING RETURNING either lands (this caller owns
// the key) or matches nothing; the SELECT that follows runs afterwards with its
// own snapshot, so it sees whatever row won, including one committed by another
// replica microseconds earlier.
//
// The one gap is a claim RELEASED between the two — a losing insert followed by
// the owner freeing the key on a pre-provider refusal. That leaves neither a
// won insert nor a row to read, so the whole thing is retried once: the key is
// genuinely free again, and the retry is an ordinary claim. A second empty
// result means something is releasing keys as fast as they are claimed, and the
// answer is ErrIdempotencyRaced rather than a guess — an unresolved claim that
// reported "won" would provision the second node this table exists to prevent.
func (p *pgPersister) claimIdempotency(ctx context.Context, c *IdempotencyClaim) (*IdempotencyClaim, bool, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, false, fmt.Errorf("marshal %s %s: %w", tblIdempotency, c.ID, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		var got []byte
		err := p.pool.QueryRow(ctx, fmt.Sprintf(
			`INSERT INTO %s (id, data, updated_at) VALUES ($1, $2, now())
			 ON CONFLICT (id) DO NOTHING
			 RETURNING data`, tblIdempotency), c.ID, data).Scan(&got)
		if err == nil {
			return nil, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, false, fmt.Errorf("claim %s %s: %w", tblIdempotency, c.ID, err)
		}
		existing, err := p.idempotencyByID(ctx, c.ID)
		if err != nil {
			return nil, false, err
		}
		if existing != nil {
			return existing, false, nil
		}
	}
	return nil, false, fmt.Errorf("%w: %s", ErrIdempotencyRaced, c.ID)
}

func (p *pgPersister) idempotencyByID(ctx context.Context, id string) (*IdempotencyClaim, error) {
	var data []byte
	err := p.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT data FROM %s WHERE id = $1`, tblIdempotency), id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s %s: %w", tblIdempotency, id, err)
	}
	var c IdempotencyClaim
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("decode %s %s: %w", tblIdempotency, id, err)
	}
	return &c, nil
}

// completeIdempotency stamps the terminal answer onto the claim's document.
//
// The UPDATE is guarded on the claim still being in progress, so a late writer
// cannot overwrite a terminal answer another caller already published and hand
// two retries two different responses for one submission. It merges into the
// stored document rather than reading it back and rewriting it whole, for the
// same reason: the row is the only copy, and a round trip through this process
// would reintroduce the race the guard removes.
//
// The two encodings are the ones encoding/json produces for the fields they
// replace — base64 for []byte, RFC3339Nano for time.Time — and are built in Go
// rather than cast in SQL on purpose: to_jsonb(bytea) renders \x hex and
// to_jsonb(timestamptz) renders its own offset form, and either would decode
// back into a claim the INSERT path never wrote.
func (p *pgPersister) completeIdempotency(ctx context.Context, id string, status int, body []byte, at time.Time) error {
	tag, err := p.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET data = data
		   || jsonb_build_object('State', $2::text, 'Status', $3::int,
		                         'Response', $4::text, 'CompletedAt', $5::text),
		     updated_at = now()
		 WHERE id = $1 AND data->>'State' = $6`, tblIdempotency),
		id, IdempotencyCompleted, status,
		base64.StdEncoding.EncodeToString(body), at.UTC().Format(time.RFC3339Nano),
		IdempotencyInProgress)
	if err != nil {
		return fmt.Errorf("complete %s %s: %w", tblIdempotency, id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: idempotency claim %s is gone or already terminal", ErrNotFound, id)
	}
	return nil
}

func (p *pgPersister) deleteIdempotency(ctx context.Context, id string) error {
	if _, err := p.pool.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE id = $1`, tblIdempotency), id); err != nil {
		return fmt.Errorf("release %s %s: %w", tblIdempotency, id, err)
	}
	return nil
}

func (p *pgPersister) deleteBurst(id string) error { return p.del(tblBursts, id) }

// updateBurstNodePhase merges the four phase fields into an existing row's
// document. UPDATE, not INSERT ... ON CONFLICT: a row claimBurst deleted between
// the connector's report and this statement stays deleted, and the caller is
// told nothing applied.
//
// OccupancyObservedAt is merged CONDITIONALLY, in the same statement rather than
// in a second one: it is a different claim from the phase — that a connector can
// still see whether the node is idle — and a burst whose ceiling was reset by an
// ordinary health report is the defect the split exists to close. Occupancy uses
// receipt time ($10), not the source timestamp, because the client strips
// OccupancyObserved from reconnect-cached events.
//
// Phase freshness follows source-vs-legacy ordering: a source-timestamped event
// always upgrades a legacy (receipt-time) record, a legacy event never regresses
// source-ordered state, and within the same class strictly-newer wins.
//
// The merge is `data || jsonb_build_object(...)` rather than a marshalled
// record, so a concurrent write to any other field of this burst survives —
// sending the whole document would replay whatever this replica's copy happened
// to hold. The WHERE clause carries the two decisions that must not be made
// against a stale read: the row belongs to the tenant this connector
// authenticated as, and it has not already reached the terminal phase, which is
// what makes a repeated Removed and a late Ready alike into no-ops.
//
// The keys are the Go field names because Burst carries no JSON tags; adding
// one would rename the key and orphan every stored record.
func (p *pgPersister) updateBurstNodePhase(ctx context.Context, u BurstNodePhaseUpdate) (*burstNodePhaseResult, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("update node phase begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// All transactions touching both records lock workloads before bursts.
	// Retain this exact ID set for the later updates: a newly inserted matching
	// workload must not introduce a workload lock after the burst lock.
	rows, err := tx.Query(ctx, `SELECT id FROM workloads WHERE data->>'BurstID'=$1 AND data->>'CustomerID'=$2
		AND ($3::text='' OR COALESCE(data->>'ClusterID','')='' OR data->>'ClusterID'=$3) ORDER BY id FOR UPDATE`, u.BurstID, u.CustomerID, u.ClusterID)
	if err != nil {
		return nil, err
	}
	var workloadIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		workloadIDs = append(workloadIDs, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}

	var data []byte
	var phaseApplied bool
	observedFmt := u.ObservedAt.UTC().Format(time.RFC3339Nano)
	occupancyTimeFmt := u.ReceiptTime.UTC().Format(time.RFC3339Nano)
	if u.ReceiptTime.IsZero() {
		occupancyTimeFmt = observedFmt
	}
	gpuApplicable := u.GPUAllocatable && u.GPUAllocatableAt != nil && u.ClusterID != ""
	err = tx.QueryRow(ctx, fmt.Sprintf(
		`WITH candidate AS (
		    SELECT id,
		           CASE
		             WHEN data->>'NodePhaseAt' IS NULL THEN true
		             WHEN $9::bool AND NOT COALESCE((data->>'NodePhaseSourceTimestamped')::bool, false) THEN true
		             WHEN NOT $9::bool AND COALESCE((data->>'NodePhaseSourceTimestamped')::bool, false) THEN false
		             WHEN (data->>'NodePhaseAt')::timestamptz < ($4::text)::timestamptz THEN true
		             ELSE false
		           END AS phase_fresh,
		           ($8::bool
		            AND (data->>'OccupancyObservedAt' IS NULL
		                 OR (data->>'OccupancyObservedAt')::timestamptz < ($10::text)::timestamptz)) AS occupancy_fresh
		      FROM %[1]s
		     WHERE id = $1
		       AND data->>'CustomerID' = $6
		       AND ($11::text = '' OR COALESCE(data->>'ClusterID', '') = '' OR data->>'ClusterID' = $11)
		       AND coalesce(data->>'NodePhase', '') <> $7
		     FOR UPDATE
		), updated AS (
		    UPDATE %[1]s AS burst
		       SET data = burst.data
		          || CASE WHEN candidate.phase_fresh
		                  THEN jsonb_build_object(
		                           'NodePhase',       $2::text,
		                           'NodePhaseReason', $3::text,
		                           'NodePhaseAt',     $4::text,
		                           'NodePhaseSourceTimestamped', $9::bool,
		                           'Status',          $5::text)
		                  ELSE '{}'::jsonb END
		          || CASE WHEN candidate.occupancy_fresh
		                  THEN jsonb_build_object('OccupancyObservedAt', $10::text)
		                  ELSE '{}'::jsonb END,
		           updated_at = now()
		      FROM candidate
		     WHERE burst.id = candidate.id
		       AND (candidate.phase_fresh OR candidate.occupancy_fresh OR $12::bool)
		 RETURNING burst.data, candidate.phase_fresh
		)
		SELECT data, phase_fresh FROM updated`, tblBursts),
		u.BurstID, u.Phase, u.Reason,
		observedFmt,
		BurstStatusForNodePhase(u.Phase), u.CustomerID, protocol.NodePhaseRemoved,
		u.OccupancyObserved, u.SourceTimestamped, occupancyTimeFmt, u.ClusterID,
		gpuApplicable).Scan(&data, &phaseApplied)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("update node phase %s %s: %w", tblBursts, u.BurstID, err)
	}
	var b Burst
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("decode updated %s %s: %w", tblBursts, u.BurstID, err)
	}
	// Stamp the permanent workload's NodeObservation in the SAME transaction so
	// the burst phase and the workload snapshot are atomic on disk. A workload
	// row may be absent (nodeOnly bursts); the UPDATE touching zero rows is fine.
	// The stale guard mirrors the in-memory one: source-timestamped upgrades
	// legacy, legacy never regresses source, same-class compares by time.
	if phaseApplied {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`UPDATE %s
		    SET data = data || jsonb_build_object(
		            'NodeObservation', jsonb_build_object(
		                'NodeName',          $1::text,
		                'Phase',             $2::text,
		                'Reason',            $3::text,
		                'ObservedAt',        $4::text,
		                'SourceTimestamped', $7::bool)),
		        updated_at = now()
		  WHERE data->>'BurstID' = $5
		    AND id = ANY($9::text[])
		    AND data->>'CustomerID' = $6
		    AND ($8::text = '' OR COALESCE(data->>'ClusterID', '') = '' OR data->>'ClusterID' = $8)
		    AND (data->'NodeObservation'->>'ObservedAt' IS NULL
		         OR ($7::bool AND NOT COALESCE((data->'NodeObservation'->>'SourceTimestamped')::bool, false))
		         OR (NOT ($7::bool = false AND COALESCE((data->'NodeObservation'->>'SourceTimestamped')::bool, false))
		             AND (data->'NodeObservation'->>'ObservedAt')::timestamptz < ($4::text)::timestamptz))`, tblWorkloads),
			b.NodeName, u.Phase, u.Reason, observedFmt,
			u.BurstID, u.CustomerID, u.SourceTimestamped, u.ClusterID, workloadIDs); err != nil {
			return nil, fmt.Errorf("stamp node observation %s for burst %s: %w", tblWorkloads, u.BurstID, err)
		}
	}
	if u.GPUAllocatable && u.GPUAllocatableAt != nil && u.ClusterID != "" && b.ClusterID == u.ClusterID {
		gpuAtFmt := u.GPUAllocatableAt.UTC().Format(time.RFC3339Nano)
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`UPDATE %s
			SET data = data || jsonb_build_object(
			        'GPUObservation', jsonb_build_object(
			            'AllocatableAt', $1::text)),
			    updated_at = now()
			WHERE data->>'BurstID' = $2
			  AND id = ANY($5::text[])
			  AND data->>'CustomerID' = $3
			  AND COALESCE(data->>'ClusterID', '') <> '' AND data->>'ClusterID' = $4
			  AND data->'GPUObservation' IS NULL`, tblWorkloads),
			gpuAtFmt, u.BurstID, u.CustomerID, u.ClusterID, workloadIDs); err != nil {
			return nil, fmt.Errorf("stamp gpu observation %s for burst %s: %w", tblWorkloads, u.BurstID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("update node phase commit %s: %w", u.BurstID, err)
	}
	return &burstNodePhaseResult{Burst: &b, PhaseApplied: phaseApplied}, nil
}

func (p *pgPersister) stampWorkloadPodObservation(ctx context.Context, workloadID, customerID, clusterID, podName, nodeName string, scheduledAt time.Time) (int64, error) {
	scheduledAtFmt := scheduledAt.UTC().Format(time.RFC3339Nano)
	ct, err := p.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %[1]s
		 SET data = data || jsonb_build_object(
		         'PodObservation', jsonb_build_object(
		             'PodName',     $1::text,
		             'NodeName',    $2::text,
		             'ScheduledAt', $3::text)),
		     updated_at = now()
		 WHERE id = $4
		   AND data->>'CustomerID' = $5
		   AND $6::text <> '' AND COALESCE(data->>'ClusterID', '') <> '' AND data->>'ClusterID' = $6
		   AND data->'PodObservation' IS NULL
		   AND EXISTS (
		       SELECT 1 FROM %[2]s
		       WHERE id = %[1]s.data->>'BurstID'
		         AND data->>'CustomerID' = $5
		         AND COALESCE(data->>'ClusterID', '') <> '' AND data->>'ClusterID' = $6
		         AND data->>'NodeName' = $2
		   )`, tblWorkloads, tblBursts),
		podName, nodeName, scheduledAtFmt,
		workloadID, customerID, clusterID)
	if err != nil {
		return 0, fmt.Errorf("stamp pod observation %s %s: %w", tblWorkloads, workloadID, err)
	}
	return ct.RowsAffected(), nil
}

// getWorkloadPodObservation reads the durable PodObservation off a workload
// row, tenant+cluster scoped AND joined against the currently live burst row
// named by the workload's BurstID with matching customer, non-empty cluster,
// and exact NodeName. It is the durability re-read StampWorkloadPodObservation
// runs when its UPDATE affects zero rows; the burst-identity join is what
// stops a stale replica from returning Resolved (and thus ACKing) an
// observation whose burst has since been retired while the workload row and
// its PodObservation happen to still exist.
//
// exists=false covers "no such workload", "another tenant's", "wrong cluster"
// AND "burst identity mismatch (retired burst, wrong NodeName)" without
// distinguishing them.
func (p *pgPersister) getWorkloadPodObservation(ctx context.Context, workloadID, customerID, clusterID, nodeName string) (*PodObservation, bool, error) {
	if clusterID == "" || nodeName == "" {
		return nil, false, nil
	}
	var raw []byte
	err := p.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT COALESCE(data->'PodObservation', 'null'::jsonb)::text
		   FROM %[1]s
		  WHERE id = $1
		    AND data->>'CustomerID' = $2
		    AND COALESCE(data->>'ClusterID', '') <> '' AND data->>'ClusterID' = $3
		    AND EXISTS (
		        SELECT 1 FROM %[2]s
		        WHERE id = %[1]s.data->>'BurstID'
		          AND data->>'CustomerID' = $2
		          AND COALESCE(data->>'ClusterID', '') <> '' AND data->>'ClusterID' = $3
		          AND data->>'NodeName' = $4
		    )`,
		tblWorkloads, tblBursts),
		workloadID, customerID, clusterID, nodeName).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read pod observation %s %s: %w", tblWorkloads, workloadID, err)
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, true, nil
	}
	var obs PodObservation
	if err := json.Unmarshal(raw, &obs); err != nil {
		return nil, true, fmt.Errorf("decode pod observation %s %s: %w", tblWorkloads, workloadID, err)
	}
	obs.ScheduledAt = obs.ScheduledAt.UTC()
	return &obs, true, nil
}

func (p *pgPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, schedulingState, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	// Keep the parameter typed as text in every SQL use. If PostgreSQL infers a
	// timestamptz here, casting it back to text for JSON emits its non-RFC3339
	// space/offset form, which encoding/json cannot decode into time.Time.
	observedAtFmt := observedAt.UTC().Format(time.RFC3339Nano)
	ct, err := p.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %[1]s
		 SET data = jsonb_set(
		     data,
		     '{SchedulingObservation}',
		     jsonb_build_object(
		         'State',      $1::text,
		         'Reason',     $2::text,
		         'Message',    $3::text,
		         'ObservedAt', $4::text,
		         'PodName',    $8::text)),
		     updated_at = now()
		 WHERE id = $5
		   AND data->>'CustomerID' = $6
		   AND $7::text <> '' AND COALESCE(data->>'ClusterID', '') <> '' AND data->>'ClusterID' = $7
		   AND COALESCE(data->>'BurstID', '') <> ''
		   AND EXISTS (
		       SELECT 1 FROM %[2]s
		       WHERE id = %[1]s.data->>'BurstID'
		         AND data->>'CustomerID' = $6
		         AND COALESCE(data->>'ClusterID', '') <> '' AND data->>'ClusterID' = $7
		   )
		   AND CASE
		       WHEN $1 = 'Waiting' THEN data->'PodObservation' IS NULL
		       WHEN $1 = 'Scheduled' THEN data->'PodObservation' IS NOT NULL
		       ELSE false
		   END
		   AND (
		       data->'SchedulingObservation' IS NULL
		       OR (data->'SchedulingObservation'->>'State' <> 'Scheduled'
		           AND ($1 = 'Scheduled'
		                OR ($4::text)::timestamptz > COALESCE(
		                        NULLIF(data->'SchedulingObservation'->>'ObservedAt', '')::timestamptz,
		                        '-infinity'::timestamptz)
		                OR ($8::text <> '' AND COALESCE(data->'SchedulingObservation'->>'PodName', '') <> ''
		                    AND data->'SchedulingObservation'->>'PodName' <> $8
		                    AND ($4::text)::timestamptz = COALESCE(
		                        NULLIF(data->'SchedulingObservation'->>'ObservedAt', '')::timestamptz,
		                        '-infinity'::timestamptz))))
		   )`, tblWorkloads, tblBursts),
		schedulingState, reason, message, observedAtFmt,
		workloadID, customerID, clusterID, podName)
	if err != nil {
		return ObservationRejected, fmt.Errorf("stamp scheduling observation %s %s: %w", tblWorkloads, workloadID, err)
	}
	if ct.RowsAffected() > 0 {
		return ObservationApplied, nil
	}

	var resolved bool
	err = p.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT EXISTS (
		    SELECT 1 FROM %[1]s
		    WHERE id = $1
		      AND data->>'CustomerID' = $2
		      AND $3::text <> '' AND COALESCE(data->>'ClusterID', '') <> '' AND data->>'ClusterID' = $3
		      AND data->'SchedulingObservation' IS NOT NULL
		      AND EXISTS (
		          SELECT 1 FROM %[2]s
		          WHERE id = %[1]s.data->>'BurstID'
		            AND data->>'CustomerID' = $2
		            AND COALESCE(data->>'ClusterID', '') <> '' AND data->>'ClusterID' = $3
		      )
		      AND (
		          data->'SchedulingObservation'->>'State' = 'Scheduled'
		          OR ($4 <> 'Scheduled'
		              AND COALESCE(
		                    NULLIF(data->'SchedulingObservation'->>'ObservedAt', '')::timestamptz,
		                    '-infinity'::timestamptz)
		                  >= ($5::text)::timestamptz
		              AND NOT (
		                  $6::text <> ''
		                  AND COALESCE(data->'SchedulingObservation'->>'PodName', '') <> ''
		                  AND data->'SchedulingObservation'->>'PodName' <> $6
		                  AND COALESCE(
		                        NULLIF(data->'SchedulingObservation'->>'ObservedAt', '')::timestamptz,
		                        '-infinity'::timestamptz)
		                      = ($5::text)::timestamptz))
		      )
		)`, tblWorkloads, tblBursts),
		workloadID, customerID, clusterID, schedulingState, observedAtFmt, podName).Scan(&resolved)
	if err != nil {
		return ObservationRejected, fmt.Errorf("re-check scheduling observation %s %s: %w", tblWorkloads, workloadID, err)
	}
	if resolved {
		return ObservationResolved, nil
	}
	return ObservationRejected, nil
}

// updateBurstGPUTelemetry merges one GPU utilisation sample into an existing
// burst row. UPDATE-only: a DELETE'd row stays deleted. The WHERE clause
// requires exact tenant, cluster, and node identity and a strictly newer
// observation.
func (p *pgPersister) updateBurstGPUTelemetry(ctx context.Context, u BurstGPUTelemetryUpdate) (bool, error) {
	observedFmt := u.ObservedAt.UTC().Format(time.RFC3339Nano)
	tag, err := p.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s
		    SET data = data || jsonb_build_object(
		            'GPUUtilPercent',  $2::float8,
		            'LastHeartbeatAt', $3::text),
		        updated_at = now()
		  WHERE id = $1
		    AND data->>'CustomerID' = $4
		    AND data->>'ClusterID'  = $5
		    AND data->>'NodeName'   = $6
		    AND (data->>'LastHeartbeatAt' IS NULL
		         OR (data->>'LastHeartbeatAt')::timestamptz < ($3::text)::timestamptz)`,
		tblBursts),
		u.BurstID, u.Utilization, observedFmt, u.CustomerID, u.ClusterID, u.NodeName)
	if err != nil {
		return false, fmt.Errorf("update gpu telemetry %s %s: %w", tblBursts, u.BurstID, err)
	}
	return tag.RowsAffected() > 0, nil
}

func (p *pgPersister) upsertPV(v *PersistentVolume) error { return p.upsert(tblPVs, v.ID, v) }
func (p *pgPersister) deletePV(id string) error           { return p.del(tblPVs, id) }
func (p *pgPersister) Close()                             { p.pool.Close() }

func (p *pgPersister) setMeshState(st *meshState) error {
	return p.upsert(tblMeshState, meshStateRowID, st)
}

// customerAdvisoryLockKey hashes a customer ID to a stable int64 for
// pg_advisory_xact_lock, serializing concurrent admission for the same tenant.
func customerAdvisoryLockKey(customerID string) int64 {
	var h uint64
	for _, c := range customerID {
		h = h*31 + uint64(c)
	}
	return int64(h)
}

func (p *pgPersister) reserveAdmission(ctx context.Context, customerID, workloadID string, candidateMicroUSD int64) (string, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("admission: begin reserve: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	id, err := p.reserveAdmissionTx(ctx, tx, customerID, workloadID, candidateMicroUSD)
	if err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("admission: commit reserve: %w", err)
	}
	return id, nil
}

// reserveAdmissionTx shares the existing accounting with callers which must
// commit an authorization decision in the same transaction as the reservation.
// The caller owns commit/rollback and must acquire any tenant/workload locks
// before this admission lock.
func (p *pgPersister) reserveAdmissionTx(ctx context.Context, tx pgx.Tx, customerID, workloadID string, candidateMicroUSD int64) (string, error) {
	lockKey := customerAdvisoryLockKey(customerID)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lockKey); err != nil {
		return "", fmt.Errorf("admission: advisory lock: %w", err)
	}

	// Remove expired reservations so the UNIQUE constraint does not block
	// re-reservation of the same tenant/workload after TTL.
	if _, err := tx.Exec(ctx, `
		DELETE FROM admission_reservations
		WHERE customer_id = $1
		  AND created_at <= now() - make_interval(secs => $2)`,
		customerID, AdmissionReservationTTL.Seconds()); err != nil {
		return "", fmt.Errorf("admission: expire reservations: %w", err)
	}

	// Idempotency: check for existing reservation with same tenant+workload.
	var existingID string
	var existingMicroUSD int64
	err := tx.QueryRow(ctx, `
		SELECT id, hourly_micro_usd FROM admission_reservations
		WHERE customer_id = $1 AND workload_id = $2`,
		customerID, workloadID).Scan(&existingID, &existingMicroUSD)
	if err == nil {
		if existingMicroUSD != candidateMicroUSD {
			return "", ErrAdmissionConflictingRate
		}
		return existingID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("admission: idempotency check: %w", err)
	}

	// Read authoritative customer limits as integer micro-USD. Converting each
	// persisted value at its source keeps this path identical to the in-memory
	// accounting and avoids binary-float drift at an exact cap.
	var maxBursts int
	var maxHourlyMicroUSD int64
	err = tx.QueryRow(ctx, `
		SELECT COALESCE((data->>'MaxConcurrentBursts')::int, 0),
		       COALESCE(round((data->>'MaxHourlyUSD')::numeric * 1000000), 0)::bigint
		FROM customers WHERE id = $1`, customerID).Scan(&maxBursts, &maxHourlyMicroUSD)
	if err != nil {
		return "", fmt.Errorf("admission: read customer limits: %w", err)
	}

	// Read reservations BEFORE bursts. A successful create persists its burst
	// and only then releases its reservation. Under READ COMMITTED, this order
	// therefore sees at least one side of that handoff: reading bursts first
	// could miss the insert, then read reservations after the release and count
	// neither even though the paid node exists.
	var reservedSlots int
	var reservedMicroUSD int64
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(count(*), 0), COALESCE(sum(hourly_micro_usd), 0)
		FROM admission_reservations
		WHERE customer_id = $1`,
		customerID).Scan(&reservedSlots, &reservedMicroUSD)
	if err != nil {
		return "", fmt.Errorf("admission: sum reservations: %w", err)
	}

	// Count/sum persisted live bursts for this customer, rounding each burst to
	// micro-USD before summing just as the in-memory path does.
	var runningBursts int
	var runningMicroUSD int64
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(count(*), 0),
		       COALESCE(sum(round((data->>'HourlyUSD')::numeric * 1000000)::bigint), 0)::bigint
		FROM bursts WHERE (data->>'CustomerID') = $1`,
		customerID).Scan(&runningBursts, &runningMicroUSD)
	if err != nil {
		return "", fmt.Errorf("admission: sum running bursts: %w", err)
	}

	totalSlots := runningBursts + reservedSlots + 1
	totalMicroUSD := runningMicroUSD + reservedMicroUSD + candidateMicroUSD

	if maxBursts > 0 && totalSlots > maxBursts {
		return "", ErrAdmissionSlotLimitReached
	}
	if maxHourlyMicroUSD > 0 && totalMicroUSD > maxHourlyMicroUSD {
		return "", ErrAdmissionRateLimitReached
	}

	id := newAdmissionID()
	if _, err := tx.Exec(ctx, `
		INSERT INTO admission_reservations (id, customer_id, workload_id, hourly_micro_usd)
		VALUES ($1, $2, $3, $4)`,
		id, customerID, workloadID, candidateMicroUSD); err != nil {
		return "", fmt.Errorf("admission: insert reservation: %w", err)
	}

	return id, nil
}

func (p *pgPersister) releaseAdmission(ctx context.Context, id string) error {
	if _, err := p.pool.Exec(ctx, `
		DELETE FROM admission_reservations WHERE id = $1`, id); err != nil {
		return fmt.Errorf("admission: release: %w", err)
	}
	return nil
}

// loadAll reads every durable record into a snapshot the store applies to its
// in-memory maps. Called once at construction. Customers MUST be read before
// tombstones: if finalization commits between the two reads, the later
// tombstone read still causes applySnapshot to reject the earlier customer
// row. Reversing this order would reopen a restart-time resurrection window.
func (p *pgPersister) loadAll(ctx context.Context) (*snapshot, error) {
	snap := &snapshot{}
	if err := p.eachRow(ctx, tblCustomers, func(b []byte) error {
		var c Customer
		if err := json.Unmarshal(b, &c); err != nil {
			return err
		}
		c.persistedData = string(b)
		snap.Customers = append(snap.Customers, &c)
		return nil
	}); err != nil {
		return nil, err
	}
	tombstones, err := p.tombstoneIDs(ctx)
	if err != nil {
		return nil, err
	}
	snap.Tombstones = tombstones
	if err := p.eachRow(ctx, tblAccounts, func(b []byte) error {
		var a Account
		if err := json.Unmarshal(b, &a); err != nil {
			return err
		}
		snap.Accounts = append(snap.Accounts, &a)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := p.eachRow(ctx, tblMemberships, func(b []byte) error {
		var m TenantMembership
		if err := json.Unmarshal(b, &m); err != nil {
			return err
		}
		snap.Memberships = append(snap.Memberships, &m)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := p.eachRow(ctx, tblWorkloads, func(b []byte) error {
		var w Workload
		if err := json.Unmarshal(b, &w); err != nil {
			return err
		}
		snap.Workloads = append(snap.Workloads, &w)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := p.eachRow(ctx, tblBursts, func(b []byte) error {
		var bu Burst
		if err := json.Unmarshal(b, &bu); err != nil {
			return err
		}
		snap.Bursts = append(snap.Bursts, &bu)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := p.eachRow(ctx, tblPVs, func(b []byte) error {
		var v PersistentVolume
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		snap.PVs = append(snap.PVs, &v)
		return nil
	}); err != nil {
		return nil, err
	}
	// The singleton row. eachRow rather than a keyed SELECT because the table
	// holds at most one, and an unexpected extra row is better folded into the
	// last-read value than turned into a boot failure.
	if err := p.eachRow(ctx, tblMeshState, func(b []byte) error {
		var st meshState
		if err := json.Unmarshal(b, &st); err != nil {
			return err
		}
		snap.MeshState = &st
		return nil
	}); err != nil {
		return nil, err
	}
	return snap, nil
}

// tombstoneIDs reads the retired-id set. It does not go through eachRow because
// these rows carry no document — the id IS the record, and the two timestamps
// beside it are for an operator reading the table, not for the store.
//
// A boot that skipped them would leave this store with nothing to tell a retired
// id from a free one: the rows they name are already deleted, so no other
// collection carries the fact. Its own writers would then reissue the id, and
// only the durable guards would refuse them, one failed write at a time.
func (p *pgPersister) tombstoneIDs(ctx context.Context) ([]string, error) {
	rows, err := p.pool.Query(ctx, fmt.Sprintf("SELECT id FROM %s", tblTombstones))
	if err != nil {
		return nil, fmt.Errorf("state: load %s: %w", tblTombstones, err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("state: scan %s: %w", tblTombstones, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: load %s: %w", tblTombstones, err)
	}
	return ids, nil
}

func (p *pgPersister) eachRow(ctx context.Context, table string, fn func([]byte) error) error {
	rows, err := p.pool.Query(ctx, fmt.Sprintf("SELECT data FROM %s", table))
	if err != nil {
		return fmt.Errorf("state: load %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return fmt.Errorf("state: scan %s: %w", table, err)
		}
		if err := fn(data); err != nil {
			return fmt.Errorf("state: decode %s: %w", table, err)
		}
	}
	return rows.Err()
}
