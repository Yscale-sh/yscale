package state

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaSpyTx scripts a pgx transaction: it records every statement in the
// order it ran and counts how the transaction ended, so the ordering and the
// release guarantee are observable without a Postgres. Only package state can
// see pgPersister, and *pgxpool.Pool is a concrete type — hence the schemaTx
// seam this implements.
type schemaSpyTx struct {
	failAt    int // index of the Exec that fails; -1 for none
	failErr   error
	commitErr error

	stmts     []string // raw SQL, in order
	commits   int
	rollbacks int
}

func (tx *schemaSpyTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	i := len(tx.stmts)
	tx.stmts = append(tx.stmts, sql)
	if i == tx.failAt {
		return pgconn.CommandTag{}, tx.failErr
	}
	return pgconn.CommandTag{}, nil
}

func (tx *schemaSpyTx) Commit(context.Context) error   { tx.commits++; return tx.commitErr }
func (tx *schemaSpyTx) Rollback(context.Context) error { tx.rollbacks++; return nil }

var errSchema = errors.New("postgres is down")

// TestApplySchema pins what keeps N replicas from crash-looping against a fresh
// database. CREATE TABLE IF NOT EXISTS is not safe against itself — two sessions
// can both pass the existence check and the loser fails on a catalogue unique
// index — so the DDL must run under the advisory lock, and the lock must be
// released on every path or the next replica waits behind it until this pod dies.
func TestApplySchema(t *testing.T) {
	tests := []struct {
		name      string
		failAt    int // index of the Exec that fails; -1 for none
		commitErr error

		wantErr     error
		wantStmts   int // how many statements should have run
		wantCommits int
	}{
		{
			name: "locks, applies the schema, commits",
			// Commit is the release: with pg_advisory_xact_lock there is no
			// unlock call to forget.
			failAt: -1, wantStmts: 2, wantCommits: 1,
		},
		{
			// The DDL must not run unlocked. Running it anyway is exactly the
			// race, just with an extra failed statement in front of it.
			name:   "a failed lock never reaches the DDL",
			failAt: 0, wantErr: errSchema, wantStmts: 1,
		},
		{
			name:   "failing DDL is reported and rolls back",
			failAt: 1, wantErr: errSchema, wantStmts: 2,
		},
		{
			name:   "a failed commit is reported",
			failAt: -1, commitErr: errSchema, wantErr: errSchema,
			wantStmts: 2, wantCommits: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &schemaSpyTx{failAt: tc.failAt, failErr: errSchema, commitErr: tc.commitErr}

			err := applySchema(context.Background(), tx)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("applySchema err = %v, want %v", err, tc.wantErr)
			}
			if len(tx.stmts) != tc.wantStmts {
				t.Fatalf("statements = %v, want %d", tx.stmts, tc.wantStmts)
			}
			if !strings.Contains(tx.stmts[0], "pg_advisory_xact_lock") {
				t.Errorf("first statement = %q, want the transaction-scoped advisory lock — "+
					"the DDL below it is only safe while the lock is held, and the SESSION-scoped "+
					"pg_advisory_lock would survive a rollback on a pooled connection", tx.stmts[0])
			}
			if tc.wantStmts == 2 && tx.stmts[1] != pgSchema {
				t.Errorf("second statement = %q, want pgSchema", tx.stmts[1])
			}
			if tx.commits != tc.wantCommits {
				t.Errorf("commits = %d, want %d", tx.commits, tc.wantCommits)
			}
			// Every path, success included: the deferred rollback is what
			// releases the lock when the DDL or the lock itself failed. Skipping
			// it holds the lock for the life of the pooled connection, and every
			// other replica then blocks in ensureSchema until this pod dies.
			if tx.rollbacks != 1 {
				t.Errorf("rollbacks = %d, want 1 — the transaction must always be ended", tx.rollbacks)
			}
		})
	}
}

// TestEnsureSchemaConcurrent proves the property the lock exists for: several
// centrals applying the schema at once against a database where the tables do
// NOT yet exist all succeed. Unlocked, the losers fail with "duplicate key
// value violates unique constraint pg_type_typname_nsp_index" (SQLSTATE 23505),
// NewPostgres returns that error, and the pod crash-loops at boot.
//
// Needs a real database: the race lives in Postgres's catalogue, not in Go, and
// applySchema's table above can only show that the lock is taken, not that it
// works. Gated on YSCALE_TEST_DATABASE_URL like TestPostgresRoundTrip, so
// ordinary CI does NOT cover it; CI should set it against a throwaway Postgres.
//
// It works in a schema of its own so the tables the other gated tests share are
// untouched, and so every run really does start with the tables missing — the
// only state in which the race exists.
func TestEnsureSchemaConcurrent(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()

	// One connection per racer: the advisory lock is per SESSION, so racers
	// sharing a connection would serialise for the wrong reason.
	const racers = 8
	pool := freshSchemaPool(t, dsn, "yscale_test_ensure_schema", racers)

	errs := make([]error, racers)
	var wg sync.WaitGroup
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := &pgPersister{pool: pool}
			errs[i] = p.ensureSchema(ctx)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("racer %d: %v — a replica that cannot apply the schema returns this "+
				"from NewPostgres and crash-loops at boot", i, err)
		}
	}
	// Every object really is there; all-succeed would be cheap to fake by doing
	// nothing at all. Resolved through the pool's search_path, so these are the
	// objects in the test schema.
	for _, name := range []string{tblCustomers, tblWorkloads, tblBursts, tblPVs, tblPodSlots, "workloads_burst_id"} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&exists); err != nil {
			t.Fatalf("check %s: %v", name, err)
		}
		if !exists {
			t.Errorf("%s was not created", name)
		}
	}
}

// freshSchemaPool returns a pool bound to an empty Postgres schema of its own,
// dropped when the test ends.
//
// Isolation is not tidiness here. The schema race only exists while the tables
// are ABSENT, so a gated test that shared the database with TestPostgresRoundTrip
// would find them already created and prove nothing on the second run. Every
// unqualified statement — the DDL included — lands in this schema.
//
// schema is a fixed identifier, never user input, so interpolating it is safe:
// the same rule the table-name constants follow.
func freshSchemaPool(t *testing.T, dsn, schema string, maxConns int32) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close()
	drop := func() {
		a, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			return
		}
		defer a.Close()
		_, _ = a.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	}
	// A previous run that was killed mid-test leaves the schema behind.
	if _, err := admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`); err != nil {
		t.Fatalf("clear test schema: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create test schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		drop()
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		drop()
		t.Fatalf("connect to test schema: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		drop()
	})
	return pool
}
