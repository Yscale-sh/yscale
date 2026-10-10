package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yscale-sh/yscale/central/internal/billing"
)

const migrationTimeout = 2 * time.Minute

type migrationConfig struct {
	databaseURL string
	liveMode    bool
	runtimeRole string
}

func migrationConfigFromEnv() (migrationConfig, error) {
	config := migrationConfig{
		databaseURL: os.Getenv("BILLING_MIGRATION_DATABASE_URL"),
		runtimeRole: os.Getenv("BILLING_RUNTIME_ROLE"),
	}
	if config.databaseURL == "" {
		return migrationConfig{}, errors.New("BILLING_MIGRATION_DATABASE_URL is required")
	}
	switch os.Getenv("BILLING_LIVE_MODE") {
	case "true":
		config.liveMode = true
	case "false":
		config.liveMode = false
	default:
		return migrationConfig{}, errors.New("BILLING_LIVE_MODE is required and must be true or false")
	}
	if config.runtimeRole == "" {
		return migrationConfig{}, errors.New("BILLING_RUNTIME_ROLE is required")
	}
	if err := billing.ValidateRuntimeRole(config.runtimeRole); err != nil {
		return migrationConfig{}, fmt.Errorf("invalid BILLING_RUNTIME_ROLE: %w", err)
	}
	return config, nil
}

func run(ctx context.Context, config migrationConfig) error {
	pool, err := pgxpool.New(ctx, config.databaseURL)
	if err != nil {
		return errors.New("open billing migration database")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return errors.New("reach billing migration database")
	}
	if err := billing.EnsureSchema(ctx, pool, config.liveMode); err != nil {
		return fmt.Errorf("apply billing schema: %w", err)
	}
	if err := billing.GrantRuntimePrivileges(ctx, pool, config.runtimeRole); err != nil {
		return fmt.Errorf("apply billing runtime grants: %w", err)
	}
	return nil
}

func main() {
	config, err := migrationConfigFromEnv()
	if err != nil {
		log.Print("invalid billing migration configuration")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), migrationTimeout)
	defer cancel()
	if err := run(ctx, config); err != nil {
		// Do not print the wrapped connection error: provider errors can echo a
		// DSN. The Job status and exit code carry failure without leaking it.
		log.Print("billing migration failed")
		os.Exit(1)
	}
	log.Print("billing migration complete")
}
