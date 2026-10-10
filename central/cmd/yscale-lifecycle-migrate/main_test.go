package main

import (
	"os"
	"strings"
	"testing"
)

func setMigrationEnv(t *testing.T, dsn, role string) {
	t.Helper()
	t.Setenv("LIFECYCLE_MIGRATION_DATABASE_URL", dsn)
	t.Setenv("LIFECYCLE_RUNTIME_ROLE", role)
}

// Nothing is defaulted. A migrator that guessed a DSN or a role would apply the
// authoritative delete schema — and hand out privileges on it — somewhere
// nobody asked for.
func TestMigrationConfigRequiresOnlyExplicitInputs(t *testing.T) {
	setMigrationEnv(t, "", "lifecycle_runtime")
	if _, err := migrationConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "LIFECYCLE_MIGRATION_DATABASE_URL") {
		t.Fatalf("missing DSN error = %v", err)
	}
	setMigrationEnv(t, "postgres://owner@db/lifecycle", "")
	if _, err := migrationConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "LIFECYCLE_RUNTIME_ROLE") {
		t.Fatalf("missing role error = %v", err)
	}
}

func TestMigrationConfigAcceptsAPlainRuntimeRole(t *testing.T) {
	setMigrationEnv(t, "postgres://owner@db/lifecycle", "lifecycle_runtime")
	config, err := migrationConfigFromEnv()
	if err != nil {
		t.Fatalf("valid configuration rejected: %v", err)
	}
	if config.runtimeRole != "lifecycle_runtime" || config.databaseURL == "" {
		t.Fatalf("config = %+v", config)
	}
}

// The role is interpolated into GRANT statements. The grammar is closed at the
// configuration boundary so a value from the environment can never become SQL
// syntax — or a surprising case-folded identity.
func TestMigrationConfigRejectsRoleInjection(t *testing.T) {
	for _, role := range []string{
		`runtime"; GRANT ALL TO PUBLIC;--`,
		"Lifecycle_Runtime",
		"lifecycle runtime",
		"9lifecycle",
		"",
	} {
		setMigrationEnv(t, "postgres://owner@db/lifecycle", role)
		if _, err := migrationConfigFromEnv(); err == nil {
			t.Errorf("role %q accepted", role)
		}
	}
}

// Both migrators and the runtime entrypoint ship in one image: the Jobs that
// gate a rollout must run the same build as the Deployment they gate.
func TestCloudBuildkitImagePackagesBothMigratorsAndKeepsRuntimeEntrypoint(t *testing.T) {
	raw, err := os.ReadFile("../../build/Dockerfile.cloud.buildkit")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, required := range []string{
		"./central/cmd/yscale-billing-migrate",
		"COPY --from=build /yscale-billing-migrate /usr/local/bin/yscale-billing-migrate",
		"./central/cmd/yscale-lifecycle-migrate",
		"COPY --from=build /yscale-lifecycle-migrate /usr/local/bin/yscale-lifecycle-migrate",
		`ENTRYPOINT ["/usr/local/bin/yscale-cloud"]`,
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("cloud image missing %q", required)
		}
	}
}

// A DSN carries a password. Postgres and pgx both echo the connection string
// into their error text, so a wrapped database error printed to the log is a
// credential in the pod log — which is why run() returns bare errors for the
// connect path and main prints a fixed string.
func TestMigratorMainDoesNotLogConfigurationOrWrappedDatabaseErrors(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, forbidden := range []string{"log.Fatal", "log.Fatalf", "log.Panic", `log.Print(err`, `log.Printf(`} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("migrator main contains credential-leaking log seam %q", forbidden)
		}
	}
}

// The privileged half of the runtime boundary: this binary is the only one that
// installs the schema and hands out grants, and it must not have quietly grown
// a runtime responsibility.
func TestMigratorHoldsTheDDLAndNothingElse(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, required := range []string{
		"lifecycle.EnsureProviderDeleteSchema",
		"lifecycle.GrantRuntimePrivileges",
		"lifecycle.ValidateRuntimeRole",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("migrator does not call %s", required)
		}
	}
	if strings.Contains(text, "lifecycle.OpenStore") {
		t.Error("the migrator opens a runtime store; the runtime boundary splits these two on purpose")
	}
}
