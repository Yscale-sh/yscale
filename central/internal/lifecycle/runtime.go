package lifecycle

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// runtimeTables are the lifecycle relations the request path and the delete
// worker touch. The list is the runtime contract in both directions: it is what
// OpenStore verifies exists, and it is the exact set the migrator grants.
var runtimeTables = []string{
	"workloads",
	"bursts",
	"operations",
	"outbox",
	"lifecycle_events",
	"provider_deletes",
	"provider_reconciliations",
	"provider_orphans",
}

// runtimeAppendOnlyTables never receive UPDATE from a runtime role. The
// append-only trigger already refuses the write; withholding the privilege
// means a runtime role cannot even attempt it.
var runtimeAppendOnlyTables = []string{"lifecycle_events"}

var runtimeRolePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// OpenStore opens the lifecycle database for RUNTIME use and verifies that the
// migrated schema is present. It intentionally performs no DDL: a request
// worker that could create its own schema could also create a divergent one,
// and the authoritative delete machine is only authoritative if exactly one
// migration produced it. Schema installation is the out-of-band privileged job
// (EnsureProviderDeleteSchema), exactly as billing.OpenStore splits it.
func OpenStore(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: open database: %w", err)
	}
	store := NewStore(pool)
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("lifecycle: ping database: %w", err)
	}
	if err := store.AssertRuntimeSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// AssertRuntimeSchema fails closed when the migration has not run, or when the
// runtime role cannot reach what it produced. Starting a delete worker against
// a half-migrated database is the one startup that looks healthy while every
// provider delete it accepts is silently unrecorded.
func (s *Store) AssertRuntimeSchema(ctx context.Context) error {
	if err := s.assertReady(); err != nil {
		return err
	}
	for _, table := range runtimeTables {
		var present bool
		if err := s.pool.QueryRow(ctx,
			`SELECT to_regclass($1) IS NOT NULL`, "lifecycle."+table).Scan(&present); err != nil {
			return fmt.Errorf("lifecycle: verify runtime schema: %w", err)
		}
		if !present {
			return fmt.Errorf("%w: lifecycle.%s is not reachable; run the lifecycle migration before starting a runtime process",
				ErrInvariantViolation, table)
		}
	}
	var database, role string
	if err := s.pool.QueryRow(ctx, `SELECT current_database(), current_user`).Scan(&database, &role); err != nil {
		return fmt.Errorf("lifecycle: identify runtime database role: %w", err)
	}
	audit, err := inspectRuntimeRole(ctx, s.pool, database, role)
	if err != nil {
		return err
	}
	if err := validateRuntimeRoleAudit(audit); err != nil {
		return fmt.Errorf("lifecycle: unsafe runtime database role %q: %w", role, err)
	}
	return nil
}

// ValidateRuntimeRole accepts a deliberately conservative PostgreSQL role
// identifier. The grant path still quotes it; the closed grammar prevents a
// configuration value from becoming SQL syntax or a surprising case-folded
// identity.
func ValidateRuntimeRole(role string) error {
	if !runtimeRolePattern.MatchString(role) {
		return fmt.Errorf("%w: LIFECYCLE_RUNTIME_ROLE must match [a-z_][a-z0-9_]{0,62}", ErrInvalidArgument)
	}
	return nil
}

// GrantRuntimePrivileges gives the runtime role only what the lifecycle store
// actually executes. Schema ownership and DDL stay with the migrator role, so a
// compromised or buggy runtime process cannot reshape the state machine that
// decides whether a paid provider resource still exists.
func GrantRuntimePrivileges(ctx context.Context, pool *pgxpool.Pool, role string) error {
	if pool == nil {
		return fmt.Errorf("%w: nil database pool", ErrInvalidArgument)
	}
	if err := ValidateRuntimeRole(role); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("lifecycle: begin runtime grant transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var database string
	if err := tx.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil {
		return fmt.Errorf("lifecycle: read current database: %w", err)
	}
	if err := grantRuntimePrivileges(ctx, tx, database, role); err != nil {
		return err
	}
	audit, err := inspectRuntimeRole(ctx, tx, database, role)
	if err != nil {
		return err
	}
	if err := validateRuntimeRoleAudit(audit); err != nil {
		return fmt.Errorf("lifecycle: runtime role %q remains unsafe after privilege normalization: %w", role, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("lifecycle: commit runtime grants: %w", err)
	}
	return nil
}

type grantExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type runtimeRoleQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// runtimeRoleAudit is an enforcement result, not a rendering of GRANT text.
// Both migration and runtime startup build it from PostgreSQL's effective
// privilege/ownership catalogs, so inherited grants and ownership cannot hide
// behind a harmless-looking migration statement.
type runtimeRoleAudit struct {
	Superuser                     bool
	CreateDatabase                bool
	CreateRole                    bool
	Replication                   bool
	BypassRLS                     bool
	InheritedElevatedRole         bool
	OwnsDatabase                  bool
	OwnsLifecycleSchema           bool
	OwnsLifecycleRelation         bool
	OwnsLifecycleFunction         bool
	InheritedLifecycleOwner       bool
	CanCreateInDatabase           bool
	CanCreateInLifecycle          bool
	CanCreateInPublic             bool
	HasForbiddenTablePrivilege    bool
	HasForbiddenAppendOnlyUpdate  bool
	MissingRequiredTablePrivilege bool
	MissingRequiredSequenceUsage  bool
}

func inspectRuntimeRole(ctx context.Context, q runtimeRoleQuerier, database, role string) (runtimeRoleAudit, error) {
	if q == nil {
		return runtimeRoleAudit{}, fmt.Errorf("%w: nil runtime role querier", ErrInvalidArgument)
	}
	if database == "" {
		return runtimeRoleAudit{}, fmt.Errorf("%w: current database is empty", ErrInvariantViolation)
	}
	if err := ValidateRuntimeRole(role); err != nil {
		return runtimeRoleAudit{}, err
	}
	var audit runtimeRoleAudit
	err := q.QueryRow(ctx, `
		WITH RECURSIVE target AS (
			SELECT oid, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
			FROM pg_roles WHERE rolname=$1
		), inherited_roles(roleid) AS (
			SELECT member_of.roleid
			FROM pg_auth_members AS member_of JOIN target ON member_of.member=target.oid
			UNION
			SELECT member_of.roleid
			FROM pg_auth_members AS member_of JOIN inherited_roles ON member_of.member=inherited_roles.roleid
		)
		SELECT target.rolsuper, target.rolcreatedb, target.rolcreaterole,
		       target.rolreplication, target.rolbypassrls,
		       EXISTS (
			   SELECT 1 FROM inherited_roles JOIN pg_roles parent ON parent.oid=inherited_roles.roleid
			   WHERE parent.rolsuper OR parent.rolcreatedb OR parent.rolcreaterole
			      OR parent.rolreplication OR parent.rolbypassrls
		       ),
		       EXISTS (SELECT 1 FROM pg_database d WHERE d.datname=$2 AND d.datdba=target.oid),
		       EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname='lifecycle' AND n.nspowner=target.oid),
		       EXISTS (
			   SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			   WHERE n.nspname='lifecycle' AND c.relowner=target.oid
		       ),
		       EXISTS (
			   SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
			   WHERE n.nspname='lifecycle' AND p.proowner=target.oid
		       ),
		       EXISTS (
			   SELECT 1 FROM inherited_roles inherited
			   WHERE EXISTS (SELECT 1 FROM pg_database d WHERE d.datname=$2 AND d.datdba=inherited.roleid)
			      OR EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname='lifecycle' AND n.nspowner=inherited.roleid)
			      OR EXISTS (
				  SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
				  WHERE n.nspname='lifecycle' AND c.relowner=inherited.roleid
			      )
			      OR EXISTS (
				  SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
				  WHERE n.nspname='lifecycle' AND p.proowner=inherited.roleid
			      )
		       ),
		       has_database_privilege(target.oid, $2, 'CREATE'),
		       COALESCE(has_schema_privilege(target.oid, 'lifecycle', 'CREATE'), false),
		       COALESCE(has_schema_privilege(target.oid, 'public', 'CREATE'), false),
		       EXISTS (
			   SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			   WHERE n.nspname='lifecycle' AND c.relkind IN ('r','p')
			     AND (has_table_privilege(target.oid, c.oid, 'DELETE')
			       OR has_table_privilege(target.oid, c.oid, 'TRUNCATE')
			       OR has_table_privilege(target.oid, c.oid, 'REFERENCES')
			       OR has_table_privilege(target.oid, c.oid, 'TRIGGER'))
		       ),
		       EXISTS (
			   SELECT 1 FROM unnest($4::text[]) AS append_only(table_name)
			   JOIN pg_class c ON c.relname=append_only.table_name
			   JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='lifecycle'
			   WHERE has_table_privilege(target.oid, c.oid, 'UPDATE')
		       ),
		       EXISTS (
			   SELECT 1 FROM unnest($3::text[]) AS required(table_name)
			   JOIN pg_class c ON c.relname=required.table_name
			   JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='lifecycle'
			   WHERE NOT has_table_privilege(target.oid, c.oid, 'SELECT')
			      OR NOT has_table_privilege(target.oid, c.oid, 'INSERT')
			      OR (required.table_name <> 'lifecycle_events'
			          AND NOT has_table_privilege(target.oid, c.oid, 'UPDATE'))
		       ),
		       EXISTS (
			   SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			   WHERE n.nspname='lifecycle'
			     AND CASE WHEN c.relkind='S'
			         THEN NOT has_sequence_privilege(target.oid, c.oid, 'USAGE')
			         ELSE false
			     END
		       )
		FROM target`, role, database, runtimeTables, runtimeAppendOnlyTables).Scan(
		&audit.Superuser, &audit.CreateDatabase, &audit.CreateRole,
		&audit.Replication, &audit.BypassRLS, &audit.InheritedElevatedRole,
		&audit.OwnsDatabase, &audit.OwnsLifecycleSchema, &audit.OwnsLifecycleRelation,
		&audit.OwnsLifecycleFunction, &audit.InheritedLifecycleOwner,
		&audit.CanCreateInDatabase, &audit.CanCreateInLifecycle, &audit.CanCreateInPublic,
		&audit.HasForbiddenTablePrivilege, &audit.HasForbiddenAppendOnlyUpdate,
		&audit.MissingRequiredTablePrivilege,
		&audit.MissingRequiredSequenceUsage)
	if err != nil {
		return runtimeRoleAudit{}, fmt.Errorf("lifecycle: inspect runtime role capabilities: %w", err)
	}
	return audit, nil
}

func validateRuntimeRoleAudit(audit runtimeRoleAudit) error {
	checks := []struct {
		unsafe bool
		name   string
	}{
		{audit.Superuser, "role is superuser"},
		{audit.CreateDatabase, "role can create databases"},
		{audit.CreateRole, "role can create roles"},
		{audit.Replication, "role has replication capability"},
		{audit.BypassRLS, "role can bypass row-level security"},
		{audit.InheritedElevatedRole, "role belongs to an elevated role"},
		{audit.OwnsDatabase, "role owns the lifecycle database"},
		{audit.OwnsLifecycleSchema, "role owns the lifecycle schema"},
		{audit.OwnsLifecycleRelation, "role owns a lifecycle relation"},
		{audit.OwnsLifecycleFunction, "role owns a lifecycle function"},
		{audit.InheritedLifecycleOwner, "role belongs to a lifecycle object owner"},
		{audit.CanCreateInDatabase, "role can create schemas in the lifecycle database"},
		{audit.CanCreateInLifecycle, "role can create objects in the lifecycle schema"},
		{audit.CanCreateInPublic, "role can create objects in the public schema"},
		{audit.HasForbiddenTablePrivilege, "role can delete, truncate, reference, or trigger lifecycle tables"},
		{audit.HasForbiddenAppendOnlyUpdate, "role can update an append-only lifecycle table"},
		{audit.MissingRequiredTablePrivilege, "role lacks required lifecycle table privileges"},
		{audit.MissingRequiredSequenceUsage, "role lacks required lifecycle sequence usage"},
	}
	for _, check := range checks {
		if check.unsafe {
			return fmt.Errorf("%w: %s", ErrInvariantViolation, check.name)
		}
	}
	return nil
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
	appendOnly := make(map[string]bool, len(runtimeAppendOnlyTables))
	for _, table := range runtimeAppendOnlyTables {
		appendOnly[table] = true
	}
	mutable := make([]string, 0, len(runtimeTables))
	insertOnly := make([]string, 0, len(runtimeAppendOnlyTables))
	for _, table := range runtimeTables {
		qualified := pgx.Identifier{"lifecycle", table}.Sanitize()
		if appendOnly[table] {
			insertOnly = append(insertOnly, qualified)
			continue
		}
		mutable = append(mutable, qualified)
	}
	statements := []string{
		"REVOKE CREATE ON DATABASE " + quotedDatabase + " FROM " + quotedRole,
		"REVOKE ALL PRIVILEGES ON SCHEMA lifecycle FROM " + quotedRole,
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA lifecycle FROM " + quotedRole,
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA lifecycle FROM " + quotedRole,
		"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA lifecycle FROM " + quotedRole,
		"GRANT CONNECT ON DATABASE " + quotedDatabase + " TO " + quotedRole,
		"GRANT USAGE ON SCHEMA lifecycle TO " + quotedRole,
		"GRANT USAGE ON ALL SEQUENCES IN SCHEMA lifecycle TO " + quotedRole,
		"GRANT SELECT, INSERT, UPDATE ON TABLE " + strings.Join(mutable, ", ") + " TO " + quotedRole,
	}
	if len(insertOnly) > 0 {
		statements = append(statements,
			"GRANT SELECT, INSERT ON TABLE "+strings.Join(insertOnly, ", ")+" TO "+quotedRole)
	}
	for _, statement := range statements {
		if _, err := execer.Exec(ctx, statement); err != nil {
			return fmt.Errorf("lifecycle: grant runtime privileges: %w", err)
		}
	}
	return nil
}
