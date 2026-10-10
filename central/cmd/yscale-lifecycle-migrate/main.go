package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
)

const migrationTimeout = 2 * time.Minute

type migrationConfig struct {
	databaseURL string
	runtimeRole string
}

func migrationConfigFromEnv() (migrationConfig, error) {
	config := migrationConfig{
		databaseURL: os.Getenv("LIFECYCLE_MIGRATION_DATABASE_URL"),
		runtimeRole: os.Getenv("LIFECYCLE_RUNTIME_ROLE"),
	}
	if config.databaseURL == "" {
		return migrationConfig{}, errors.New("LIFECYCLE_MIGRATION_DATABASE_URL is required")
	}
	if config.runtimeRole == "" {
		return migrationConfig{}, errors.New("LIFECYCLE_RUNTIME_ROLE is required")
	}
	if err := lifecycle.ValidateRuntimeRole(config.runtimeRole); err != nil {
		return migrationConfig{}, fmt.Errorf("invalid LIFECYCLE_RUNTIME_ROLE: %w", err)
	}
	return config, nil
}

func run(ctx context.Context, config migrationConfig) error {
	pool, err := pgxpool.New(ctx, config.databaseURL)
	if err != nil {
		return errors.New("open lifecycle migration database")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return errors.New("reach lifecycle migration database")
	}
	if err := lifecycle.EnsureProviderDeleteSchema(ctx, pool); err != nil {
		return fmt.Errorf("apply lifecycle schema: %w", err)
	}
	if err := lifecycle.GrantRuntimePrivileges(ctx, pool, config.runtimeRole); err != nil {
		return fmt.Errorf("apply lifecycle runtime grants: %w", err)
	}
	return nil
}

// main is the privileged half of the runtime boundary: this job holds DDL and
// the central process does not. Run it before rolling a central that has
// LIFECYCLE_DATABASE_URL set — a runtime process refuses to start against an
// unmigrated database rather than quietly accepting deletes it cannot record.
func main() {
	config, err := migrationConfigFromEnv()
	if err != nil {
		log.Print("invalid lifecycle migration configuration")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), migrationTimeout)
	defer cancel()
	if err := run(ctx, config); err != nil {
		// Do not print the wrapped connection error: provider errors can echo a
		// DSN. The Job status and exit code carry failure without leaking it.
		log.Print("lifecycle migration failed")
		os.Exit(1)
	}
	log.Print("lifecycle migration complete")
}
