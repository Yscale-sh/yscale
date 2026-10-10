//go:build integration

package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func burstReadFixture(t *testing.T) (*Store, *pgPersister) {
	t.Helper()
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	p := &pgPersister{pool: freshSchemaPool(t, dsn, fmt.Sprintf("burst_reads_%d", time.Now().UnixNano()), 4)}
	if err := p.ensureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := emptyStore()
	s.persist = p
	return s, p
}

func TestPostgresBurstSnapshotBinding(t *testing.T) {
	s, p := burstReadFixture(t)
	ctx := context.Background()
	for _, data := range []string{`null`, `{}`, `[]`, `{"ID":"other","CustomerID":"customer"}`, `{"ID":"burst","CustomerID":""}`, `{"ID":"burst","CustomerID":"customer","CreatedAt":false}`} {
		if _, err := p.pool.Exec(ctx, `INSERT INTO bursts (id,data) VALUES ('burst',$1::jsonb) ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data`, data); err != nil {
			t.Fatal(err)
		}
		if got, err := s.BurstSnapshotContext(ctx, "burst"); got != nil || !errors.Is(err, ErrPersistence) {
			t.Errorf("malformed booking=%v, error=%v; want persistence failure", got, err)
		}
	}
	if _, err := p.pool.Exec(ctx, `UPDATE bursts SET data='{"ID":"burst","CustomerID":"customer","BackendID":"current","future_field":{"retained":true}}'::jsonb WHERE id='burst'`); err != nil {
		t.Fatal(err)
	}
	got, err := s.BurstSnapshotContext(ctx, "burst")
	if err != nil || got.BackendID != "current" || got.Region != "" || got.ClusterID != "" {
		t.Fatalf("legacy booking with unknown fields was refused: %v", err)
	}
	if _, err := s.GetBurst("burst"); !errors.Is(err, ErrNotFound) {
		t.Error("read hydrated the local mutation cache")
	}
	var retained bool
	if err := p.pool.QueryRow(ctx, `SELECT data ? 'future_field' FROM bursts WHERE id='burst'`).Scan(&retained); err != nil || !retained {
		t.Error("read changed the durable booking")
	}
	if got, err := s.BurstSnapshotContext(ctx, "missing"); got != nil || !errors.Is(err, ErrNotFound) {
		t.Errorf("absent snapshot=%v, error=%v", got, err)
	}
}

func TestPostgresBurstSnapshotReadIsBounded(t *testing.T) {
	s, p := burstReadFixture(t)
	ctx := context.Background()
	if err := s.PutBurst(&Burst{ID: "burst", CustomerID: "customer", BackendID: "cached"}); err != nil {
		t.Fatal(err)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `LOCK TABLE bursts IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if got, err := s.BurstSnapshotContext(ctx, "burst"); got != nil || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrPersistence) {
		t.Fatalf("blocked read=%v, error=%v", got, err)
	}
	if elapsed := time.Since(started); elapsed < 4*time.Second || elapsed > 8*time.Second {
		t.Errorf("read budget elapsed=%v, expected five seconds", elapsed)
	}
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if got, err := s.BurstSnapshotContext(short, "burst"); got != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("request timeout read=%v, error=%v", got, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := s.BurstSnapshotContext(ctx, "burst"); err != nil || got.BackendID != "cached" {
		t.Fatalf("read did not recover after lock release: %v", err)
	}
}
