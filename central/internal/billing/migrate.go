package billing

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var postgresRolePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

var runtimeBillingTables = []string{
	"environment",
	"accounts",
	"operations",
	"funding_sources",
	"credit_reversals",
	"credit_disputes",
	"holds",
	"ledger",
	"checkout_sessions",
	"webhook_inbox",
}

// ValidateRuntimeRole accepts a deliberately conservative PostgreSQL role
// identifier. The grant path still quotes it; the closed grammar prevents a
// configuration value from becoming SQL syntax or a surprising case-folded
// identity.
func ValidateRuntimeRole(role string) error {
	if !postgresRolePattern.MatchString(role) {
		return fmt.Errorf("%w: BILLING_RUNTIME_ROLE must match [a-z_][a-z0-9_]{0,62}", ErrInvalidArgument)
	}
	return nil
}

// GrantRuntimePrivileges gives the runtime role only the privileges the
// current billing store uses. Schema ownership and DDL remain with the
// migrator role. No privileges are granted to PUBLIC and no default privilege
// is widened for future objects.
func GrantRuntimePrivileges(ctx context.Context, pool *pgxpool.Pool, role string) error {
	if pool == nil {
		return fmt.Errorf("%w: nil database pool", ErrInvalidArgument)
	}
	if err := ValidateRuntimeRole(role); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("billing: begin runtime grant transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var database string
	if err := tx.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil {
		return fmt.Errorf("billing: read current database: %w", err)
	}
	if err := grantRuntimePrivileges(ctx, tx, database, role); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("billing: commit runtime grants: %w", err)
	}
	return nil
}

type grantExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func grantRuntimePrivileges(ctx context.Context, execer grantExecer, database, role string) error {
	if err := ValidateRuntimeRole(role); err != nil {
		return err
	}
	if database == "" {
		return fmt.Errorf("%w: current database is empty", ErrInvariantViolation)
	}
	quotedRole := pgx.Identifier{role}.Sanitize()
	quotedDatabase := pgx.Identifier{database}.Sanitize()
	tables := make([]string, 0, len(runtimeBillingTables))
	for _, table := range runtimeBillingTables {
		tables = append(tables, pgx.Identifier{"billing", table}.Sanitize())
	}
	statements := []string{
		"GRANT CONNECT ON DATABASE " + quotedDatabase + " TO " + quotedRole,
		"GRANT USAGE ON SCHEMA billing TO " + quotedRole,
		"GRANT USAGE ON ALL SEQUENCES IN SCHEMA billing TO " + quotedRole,
		"GRANT SELECT, INSERT, UPDATE ON TABLE " + strings.Join(tables, ", ") + " TO " + quotedRole,
	}
	for _, statement := range statements {
		if _, err := execer.Exec(ctx, statement); err != nil {
			return fmt.Errorf("billing: grant runtime privileges: %w", err)
		}
	}
	return nil
}
