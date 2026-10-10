//go:build integration

package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func workloadTransitionFixture(t *testing.T) (*Store, *pgPersister) {
	t.Helper()
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway PostgreSQL database")
	}
	p := &pgPersister{pool: freshSchemaPool(t, dsn, fmt.Sprintf("workload_transitions_%d", time.Now().UnixNano()), 8)}
	if err := p.ensureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := emptyStore()
	s.persist = p
	s.PutWorkload(&Workload{ID: "wl_transition", CustomerID: "tenant_transition", BurstID: "burst_transition", Status: "provisioning"})
	return s, p
}

func transitionReplica(t *testing.T, p *pgPersister) *Store {
	t.Helper()
	snapshot, err := p.loadAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s := emptyStore()
	s.applySnapshot(snapshot)
	s.persist = &pgPersister{pool: p.pool}
	return s
}

func durableTransitionWorkload(t *testing.T, s *Store) *Workload {
	t.Helper()
	row, err := s.WorkloadSnapshotForCustomer(context.Background(), "tenant_transition", "wl_transition")
	if err != nil {
		t.Fatal(err)
	}
	return row.Workload
}

func TestPostgresWorkloadTransitionsStaleCancelPreservesCompletion(t *testing.T) {
	a, p := workloadTransitionFixture(t)
	b := transitionReplica(t, p)
	at := time.Now().UTC()
	if !a.FinishWorkloadWithOutcome("wl_transition", "succeeded", at, false, &WorkloadOutcome{
		Compute: WorkloadComputeOutcome{Result: WorkloadResultSucceeded, Reason: "observed completion"},
	}) {
		t.Fatal("completion was not applied")
	}
	if b.FinishWorkload("wl_transition", "cancelled", at.Add(time.Minute), true) {
		t.Error("stale cancel overwrote a committed completion")
	}
	w := durableTransitionWorkload(t, a)
	if w.Status != "succeeded" || w.FinishedAt == nil || !w.FinishedAt.Equal(at) || w.Outcome == nil || w.Outcome.Compute.Reason != "observed completion" {
		t.Error("durable completion status, timestamp or receipt was lost")
	}
}

func TestPostgresWorkloadTransitionsStaleStartCannotResurrect(t *testing.T) {
	a, p := workloadTransitionFixture(t)
	b := transitionReplica(t, p)
	a.FinishWorkload("wl_transition", "cancelled", time.Now().UTC(), true)
	if b.StartWorkload("wl_transition", time.Now().UTC()) {
		t.Error("stale start resurrected a terminal workload")
	}
	if w := durableTransitionWorkload(t, a); w.Status != "cancelled" || w.FinishedAt == nil || w.StartedAt != nil {
		t.Error("start changed the durable terminal record")
	}
}

func TestPostgresWorkloadTransitionsUncachedWorkload(t *testing.T) {
	a, p := workloadTransitionFixture(t)
	b := emptyStore()
	b.persist = &pgPersister{pool: p.pool}
	if !b.FinishWorkload("wl_transition", "cancelled", time.Now().UTC(), true) {
		t.Error("replica could not finish an uncached durable workload")
	}
	if durableTransitionWorkload(t, a).Status != "cancelled" {
		t.Error("uncached workload was not durably finished")
	}
}

func TestPostgresWorkloadTransitionsOutageDoesNotPublish(t *testing.T) {
	a, p := workloadTransitionFixture(t)
	p.pool.Close()
	if a.FinishWorkload("wl_transition", "cancelled", time.Now().UTC(), true) {
		t.Error("failed persistence was reported as an applied transition")
	}
	w, err := a.GetWorkload("wl_transition")
	if err != nil || w.Status != "provisioning" || w.FinishedAt != nil {
		t.Error("failed transition was published in the local cache")
	}
}

func TestPostgresWorkloadTransitionsWholeWriteCannotEraseObservation(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminal-%v", terminal), func(t *testing.T) {
			a, p := workloadTransitionFixture(t)
			b := transitionReplica(t, p)
			stale, _ := b.GetWorkload("wl_transition")
			at := time.Now().UTC()
			a.StartWorkload(stale.ID, at)
			if terminal {
				a.FinishWorkloadWithOutcome(stale.ID, "succeeded", at.Add(time.Second), false, &WorkloadOutcome{
					Compute: WorkloadComputeOutcome{Result: WorkloadResultSucceeded, Reason: "retained"},
				})
			}
			stale.Status = "dispatched"
			b.PutWorkload(stale)
			w := durableTransitionWorkload(t, a)
			if w.StartedAt == nil || !w.StartedAt.Equal(at) {
				t.Error("whole-record write erased the start observation")
			}
			if terminal {
				if w.Status != "succeeded" || w.FinishedAt == nil || w.Outcome == nil || w.Outcome.Compute.Reason != "retained" {
					t.Error("whole-record write erased the terminal observation")
				}
			} else if w.Status != "running" || w.FinishedAt != nil {
				t.Error("whole-record write regressed a running workload")
			}
		})
	}
}

func TestPostgresWorkloadTransitionsRereadAfterRowLock(t *testing.T) {
	a, p := workloadTransitionFixture(t)
	b := transitionReplica(t, p)
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	at := time.Now().UTC()
	patch, _ := json.Marshal(map[string]any{"Status": "succeeded", "FinishedAt": at})
	if _, err := tx.Exec(ctx, `UPDATE workloads SET data=data || $1::jsonb WHERE id='wl_transition'`, patch); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		applied, err := b.FinishWorkloadForBurst(ctx, "burst_transition", "failed", at.Add(time.Second))
		if applied && err == nil {
			err = errors.New("waiting watchdog overwrote committed completion")
		}
		done <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("transition did not wait for the locked row: %v", err)
		default:
		}
		var waiting bool
		if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
			AND wait_event_type='Lock' AND query LIKE '%FROM workloads%FOR UPDATE%' AND pid<>pg_backend_pid())`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("transition did not enter an actual workload row-lock wait")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if durableTransitionWorkload(t, a).Status != "succeeded" {
		t.Fatal("completion changed after releasing its row lock")
	}
}

func TestPostgresWorkloadTransitionsConcurrentReplicas(t *testing.T) {
	for _, start := range []bool{false, true} {
		t.Run(fmt.Sprintf("start-%v", start), func(t *testing.T) {
			_, p := workloadTransitionFixture(t)
			replicas := make([]*Store, 8)
			for i := range replicas {
				replicas[i] = transitionReplica(t, p)
			}
			ready := make(chan struct{})
			results := make(chan bool, len(replicas))
			var wg sync.WaitGroup
			for _, s := range replicas {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-ready
					if start {
						results <- s.StartWorkload("wl_transition", time.Now().UTC())
					} else {
						results <- s.FinishWorkload("wl_transition", "cancelled", time.Now().UTC(), true)
					}
				}()
			}
			close(ready)
			wg.Wait()
			close(results)
			winners := 0
			for applied := range results {
				if applied {
					winners++
				}
			}
			if winners != 1 {
				t.Fatalf("%d replicas applied the guarded transition, want one", winners)
			}
		})
	}
}

func TestPostgresWorkloadTransitionsPreserveOtherFields(t *testing.T) {
	a, p := workloadTransitionFixture(t)
	b := transitionReplica(t, p)
	ctx := context.Background()
	patch := `{"FutureMetadata":{"retained":true},"SpecYAML":"Y3VycmVudA==","Cost":{"EstimatedUSD":3},"StartedAt":"2026-09-05T01:00:00Z"}`
	if _, err := p.pool.Exec(ctx, `UPDATE workloads SET data=data || $1::jsonb WHERE id='wl_transition'`, patch); err != nil {
		t.Fatal(err)
	}
	var before []byte
	if err := p.pool.QueryRow(ctx, `SELECT data - ARRAY['Status','StartedAt','FinishedAt','Outcome'] FROM workloads WHERE id='wl_transition'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if !b.FinishWorkload("wl_transition", "cancelled", time.Now().UTC(), true) {
		t.Fatal("finish was not applied")
	}
	var unchanged bool
	if err := p.pool.QueryRow(ctx, `SELECT (data - ARRAY['Status','StartedAt','FinishedAt','Outcome'])=$1::jsonb FROM workloads WHERE id='wl_transition'`, before).Scan(&unchanged); err != nil || !unchanged {
		t.Fatalf("transition changed unrelated current metadata: %v", err)
	}
	w := durableTransitionWorkload(t, a)
	if w.StartedAt == nil || w.StartedAt.Hour() != 1 || string(w.SpecYAML) != "current" {
		t.Fatal("transition lost current start time or spec")
	}
}

func TestPostgresWorkloadTransitionsRefuseInvalidOrFailedWrites(t *testing.T) {
	for _, fault := range []string{"deleted", "wrong-id", "null", "bad-time", "ambiguous-burst", "constraint", "customer", "cluster", "burst"} {
		t.Run(fault, func(t *testing.T) {
			a, p := workloadTransitionFixture(t)
			ctx := context.Background()
			target := WorkloadTransitionTarget{WorkloadID: "wl_transition", CustomerID: "tenant_transition", BurstID: "burst_transition"}
			query := ""
			switch fault {
			case "deleted":
				query = `DELETE FROM workloads WHERE id='wl_transition'`
			case "wrong-id":
				query = `UPDATE workloads SET data=jsonb_set(data,'{ID}','"wrong"') WHERE id='wl_transition'`
			case "null":
				query = `UPDATE workloads SET data='null' WHERE id='wl_transition'`
			case "bad-time":
				query = `UPDATE workloads SET data=jsonb_set(data,'{FinishedAt}','false') WHERE id='wl_transition'`
			case "constraint":
				query = `ALTER TABLE workloads ADD CONSTRAINT transition_refused CHECK (data->>'Status' <> 'cancelled')`
			case "ambiguous-burst":
				a.PutWorkload(&Workload{ID: "other_workload", CustomerID: "tenant_transition", BurstID: "burst_transition"})
			case "customer":
				target.CustomerID = "other"
			case "cluster":
				target.ClusterID = "other"
			case "burst":
				target.BurstID = "other"
			}
			if query != "" {
				if _, err := p.pool.Exec(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			var applied bool
			var err error
			if fault == "ambiguous-burst" {
				applied, err = a.FinishWorkloadForBurst(ctx, "burst_transition", "cancelled", time.Now())
			} else {
				_, applied, err = a.FinishWorkloadContext(ctx, target, "cancelled", time.Now(), true, nil)
			}
			want := ErrPersistence
			if fault == "deleted" || fault == "null" || fault == "customer" || fault == "cluster" || fault == "burst" {
				want = ErrNotFound
			}
			if applied || !errors.Is(err, want) {
				t.Fatalf("applied=%v err=%v, want false/%v", applied, err, want)
			}
			cached, _ := a.GetWorkload("wl_transition")
			if cached.Status != "provisioning" || cached.FinishedAt != nil {
				t.Fatal("refusal published a local transition")
			}
		})
	}
}

func TestPostgresWorkloadTransitionsBoundedRowWait(t *testing.T) {
	a, p := workloadTransitionFixture(t)
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT id FROM workloads WHERE id='wl_transition' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	target := WorkloadTransitionTarget{WorkloadID: "wl_transition", CustomerID: "tenant_transition", BurstID: "burst_transition"}
	begin := time.Now()
	if _, applied, err := a.FinishWorkloadContext(ctx, target, "cancelled", time.Now(), true, nil); applied || !errors.Is(err, ErrPersistence) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked transition = applied %v, err %v", applied, err)
	}
	if elapsed := time.Since(begin); elapsed < 4*time.Second || elapsed > 7*time.Second {
		t.Fatalf("bounded transition took %s", elapsed)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, applied, err := a.FinishWorkloadContext(ctx, target, "cancelled", time.Now(), true, nil); err != nil || !applied {
		t.Fatalf("retry after lock release = applied %v, err %v", applied, err)
	}
}
