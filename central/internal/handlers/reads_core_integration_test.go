//go:build integration

package handlers

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// The public read fixtures use shared Store APIs only. Human-console and
// publisher scenarios keep their original private fixtures and tests.
func coreReadStore(t *testing.T) *state.Store {
	t.Helper()
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	s, err := state.NewPostgres(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func coreReadTenant(t *testing.T, s *state.Store) (*state.Customer, string, string) {
	t.Helper()
	suffix := fmt.Sprint(time.Now().UnixNano())
	owner, err := s.UpsertAccount("synthetic-core-read-issuer", suffix, state.AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := s.CreateTenant(&state.Customer{ID: "cust_core_read_" + suffix, Token: "synthetic-core-read-" + suffix, Plan: "pro"}, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	return c, owner.ID, suffix
}

// Simulate a legacy writer or corrupted durable binding. Current whole-record
// writers reject terminal rebinding, but reads must still refuse stale access
// if an older writer changed the row. Keep that adversarial read fixture.
func coreReadRebindWorkloadCluster(t *testing.T, id, clusterID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, os.Getenv("YSCALE_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	tag, err := p.Exec(ctx, `UPDATE workloads SET data=jsonb_set(data,'{ClusterID}',to_jsonb($2::text)) WHERE id=$1`, id, clusterID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("rebind legacy read fixture: %v", err)
	}
}
