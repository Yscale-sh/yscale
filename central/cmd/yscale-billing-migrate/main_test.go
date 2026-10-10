package main

import (
	"os"
	"strings"
	"testing"
)

func setMigrationEnv(t *testing.T, dsn, liveMode, role string) {
	t.Helper()
	t.Setenv("BILLING_MIGRATION_DATABASE_URL", dsn)
	t.Setenv("BILLING_LIVE_MODE", liveMode)
	t.Setenv("BILLING_RUNTIME_ROLE", role)
}

func TestMigrationConfigRequiresOnlyExplicitInputs(t *testing.T) {
	setMigrationEnv(t, "", "false", "billing_runtime")
	if _, err := migrationConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "BILLING_MIGRATION_DATABASE_URL") {
		t.Fatalf("missing DSN error = %v", err)
	}
	setMigrationEnv(t, "postgres://owner@db/billing", "", "billing_runtime")
	if _, err := migrationConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "BILLING_LIVE_MODE") {
		t.Fatalf("missing live mode error = %v", err)
	}
	setMigrationEnv(t, "postgres://owner@db/billing", "0", "billing_runtime")
	if _, err := migrationConfigFromEnv(); err == nil {
		t.Fatal("non-exact live mode accepted")
	}
	setMigrationEnv(t, "postgres://owner@db/billing", "false", "")
	if _, err := migrationConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "BILLING_RUNTIME_ROLE") {
		t.Fatalf("missing role error = %v", err)
	}
}

func TestMigrationConfigAcceptsExplicitFalseAndTrue(t *testing.T) {
	for _, mode := range []string{"false", "true"} {
		setMigrationEnv(t, "postgres://owner@db/billing", mode, "billing_runtime")
		config, err := migrationConfigFromEnv()
		if err != nil {
			t.Fatalf("mode %s: %v", mode, err)
		}
		if config.liveMode != (mode == "true") || config.runtimeRole != "billing_runtime" || config.databaseURL == "" {
			t.Fatalf("mode %s config = %+v", mode, config)
		}
	}
}

func TestMigrationConfigRejectsRoleInjection(t *testing.T) {
	setMigrationEnv(t, "postgres://owner@db/billing", "false", `runtime"; GRANT ALL TO PUBLIC;--`)
	if _, err := migrationConfigFromEnv(); err == nil {
		t.Fatal("role injection accepted")
	}
}

func TestCloudBuildkitImagePackagesMigratorAndKeepsRuntimeEntrypoint(t *testing.T) {
	raw, err := os.ReadFile("../../build/Dockerfile.cloud.buildkit")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, required := range []string{
		"./central/cmd/yscale-billing-migrate",
		"COPY --from=build /yscale-billing-migrate /usr/local/bin/yscale-billing-migrate",
		`ENTRYPOINT ["/usr/local/bin/yscale-cloud"]`,
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("cloud image missing %q", required)
		}
	}
}

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
