package lifecycle

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

type recordingExecer struct{ statements []string }

func (r *recordingExecer) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	r.statements = append(r.statements, sql)
	return pgconn.CommandTag{}, nil
}

func TestValidateRuntimeRoleRejectsAnythingButAPlainRole(t *testing.T) {
	for _, role := range []string{"yscale_runtime", "runtime", "_r0le"} {
		if err := ValidateRuntimeRole(role); err != nil {
			t.Errorf("role %q rejected: %v", role, err)
		}
	}
	for _, role := range []string{
		"", "Runtime", "runtime;DROP SCHEMA lifecycle", "runtime role", `"runtime"`,
		strings.Repeat("r", 64),
	} {
		if err := ValidateRuntimeRole(role); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("role %q accepted: %v", role, err)
		}
	}
}

// The runtime role must be able to run the store and nothing else. A single
// DDL verb here is the difference between "a worker records deletes" and "a
// worker can reshape the record that decides whether a paid resource exists".
func TestRuntimeGrantsCarryNoDDLAndCoverExactlyTheRuntimeTables(t *testing.T) {
	execer := &recordingExecer{}
	if err := grantRuntimePrivileges(context.Background(), execer, "yscale", "yscale_runtime"); err != nil {
		t.Fatal(err)
	}
	all := strings.Join(execer.statements, "\n")
	for _, statement := range execer.statements {
		if !strings.HasPrefix(statement, "GRANT ") {
			continue
		}
		for _, forbidden := range []string{"CREATE ", "ALTER ", "DROP ", "TRUNCATE", "DELETE", "TRIGGER", "REFERENCES"} {
			if strings.Contains(statement, forbidden) {
				t.Fatalf("runtime grant contains %q: %s", forbidden, statement)
			}
		}
	}
	for _, required := range []string{
		"REVOKE CREATE ON DATABASE",
		"REVOKE ALL PRIVILEGES ON SCHEMA lifecycle",
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA lifecycle",
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA lifecycle",
		"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA lifecycle",
	} {
		if !strings.Contains(all, required) {
			t.Errorf("runtime privilege normalization is missing %q:\n%s", required, all)
		}
	}
	for _, table := range runtimeTables {
		if !strings.Contains(all, `"lifecycle"."`+table+`"`) {
			t.Errorf("runtime grants never mention lifecycle.%s:\n%s", table, all)
		}
	}
	// The append-only log is the one table a runtime role may not UPDATE. The
	// trigger already refuses it; withholding the privilege means the attempt
	// cannot even be made.
	for _, statement := range execer.statements {
		if !strings.Contains(statement, `"lifecycle"."lifecycle_events"`) {
			continue
		}
		if strings.Contains(statement, "UPDATE") {
			t.Fatalf("runtime role was granted UPDATE on the append-only event log: %q", statement)
		}
	}
}

// Enforcement is based on PostgreSQL's effective capability/ownership audit,
// not on whether the emitted GRANT strings happen to look restrictive.
func TestRuntimeRoleAuditFailsClosedOnEffectiveEscalation(t *testing.T) {
	if err := validateRuntimeRoleAudit(runtimeRoleAudit{}); err != nil {
		t.Fatalf("safe audit rejected: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*runtimeRoleAudit)
	}{
		{"superuser", func(a *runtimeRoleAudit) { a.Superuser = true }},
		{"create database", func(a *runtimeRoleAudit) { a.CreateDatabase = true }},
		{"create role", func(a *runtimeRoleAudit) { a.CreateRole = true }},
		{"replication", func(a *runtimeRoleAudit) { a.Replication = true }},
		{"bypass RLS", func(a *runtimeRoleAudit) { a.BypassRLS = true }},
		{"inherited elevated role", func(a *runtimeRoleAudit) { a.InheritedElevatedRole = true }},
		{"database owner", func(a *runtimeRoleAudit) { a.OwnsDatabase = true }},
		{"schema owner", func(a *runtimeRoleAudit) { a.OwnsLifecycleSchema = true }},
		{"relation owner", func(a *runtimeRoleAudit) { a.OwnsLifecycleRelation = true }},
		{"function owner", func(a *runtimeRoleAudit) { a.OwnsLifecycleFunction = true }},
		{"inherited owner", func(a *runtimeRoleAudit) { a.InheritedLifecycleOwner = true }},
		{"database create", func(a *runtimeRoleAudit) { a.CanCreateInDatabase = true }},
		{"lifecycle schema create", func(a *runtimeRoleAudit) { a.CanCreateInLifecycle = true }},
		{"public schema create", func(a *runtimeRoleAudit) { a.CanCreateInPublic = true }},
		{"forbidden table privilege", func(a *runtimeRoleAudit) { a.HasForbiddenTablePrivilege = true }},
		{"append-only update", func(a *runtimeRoleAudit) { a.HasForbiddenAppendOnlyUpdate = true }},
		{"missing table privilege", func(a *runtimeRoleAudit) { a.MissingRequiredTablePrivilege = true }},
		{"missing sequence usage", func(a *runtimeRoleAudit) { a.MissingRequiredSequenceUsage = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			audit := runtimeRoleAudit{}
			test.mutate(&audit)
			if err := validateRuntimeRoleAudit(audit); !errors.Is(err, ErrInvariantViolation) {
				t.Fatalf("audit error = %v, want ErrInvariantViolation", err)
			}
		})
	}
}

func TestRuntimeGrantsRefuseAnUnvalidatedRole(t *testing.T) {
	execer := &recordingExecer{}
	err := grantRuntimePrivileges(context.Background(), execer, "yscale", "runtime; DROP SCHEMA lifecycle CASCADE")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
	if len(execer.statements) != 0 {
		t.Fatalf("statements were executed for an invalid role: %v", execer.statements)
	}
}

// runtimeTables is the runtime contract in both directions: OpenStore verifies
// it exists and the migrator grants exactly it. A table added to the schema
// without being added here is a runtime role that silently cannot read it.
func TestRuntimeTablesCoverEveryLifecycleTable(t *testing.T) {
	pattern := regexp.MustCompile(`CREATE TABLE IF NOT EXISTS lifecycle\.(\w+)`)
	declared := make(map[string]bool, len(runtimeTables))
	for _, table := range runtimeTables {
		declared[table] = true
	}
	for _, match := range pattern.FindAllStringSubmatch(lifecycleSchema+providerDeleteSchema, -1) {
		if !declared[match[1]] {
			t.Errorf("lifecycle.%s exists in the schema but is not in runtimeTables", match[1])
		}
	}
}

// The runtime boundary is the whole point of the split: a request worker that
// could create its own schema could also create a divergent one.
func TestRuntimeStoreNeverRunsDDL(t *testing.T) {
	for _, source := range []string{runtimeSource(t, "runtime.go"), runtimeSource(t, "provider_delete_booking.go")} {
		for _, forbidden := range []string{"CREATE TABLE", "CREATE SCHEMA", "ALTER TABLE", "DROP "} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("a runtime path contains %q", forbidden)
			}
		}
	}
}

func runtimeSource(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestAssertRuntimeSchemaRefusesANilStore(t *testing.T) {
	var store *Store
	if err := store.AssertRuntimeSchema(context.Background()); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
	// Close on a nil store is what a failed OpenStore's caller runs.
	store.Close()
}
