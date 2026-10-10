//go:build integration

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProviderReconciliationPostgresOrphanOutcomeLeaseExpiresDuringRowWait(t *testing.T) {
	for _, outcome := range []string{"deleted", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			pool := newProjectionTestPool(ctx, t)
			store := NewStore(pool)
			ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_outcome", ProviderResourceID: "resource_outcome"}
			observeResourceFenceOrphan(t, ctx, store, ref)
			claim, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, 2*time.Second)
			if err != nil || !claimed {
				t.Fatalf("prepare deletion: claimed=%v err=%v", claimed, err)
			}
			deletedAt, err := store.ProviderInventoryTime(ctx)
			if err != nil {
				t.Fatal(err)
			}
			worker, workerPID := newOrphanOutcomeWorker(t, ctx, pool)
			blocker := blockOrphanOutcomeRow(t, ctx, pool, ref)
			defer blocker.Rollback(context.Background()) //nolint:errcheck
			done := make(chan error, 1)
			go func() { done <- writeOrphanOutcome(ctx, worker, outcome, ref, claim.LeaseToken, deletedAt) }()
			waitOrphanOutcomeRow(t, ctx, pool, workerPID)
			var leaseLive bool
			if err := pool.QueryRow(ctx, `SELECT locked_until > clock_timestamp() FROM lifecycle.provider_orphans`).Scan(&leaseLive); err != nil || !leaseLive {
				t.Fatalf("writer did not start waiting while lease was live: live=%v err=%v", leaseLive, err)
			}
			waitResourceFenceCondition(t, ctx, pool, `SELECT locked_until <= clock_timestamp()
				FROM lifecycle.provider_orphans`)
			if err := blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-done; !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("expired worker wrote %s after its row-lock wait: %v", outcome, err)
			}
			rec := readAbsenceFenceRecord(t, ctx, pool, ref)
			if rec.State != ProviderOrphanDeleting || rec.LeaseToken != claim.LeaseToken || rec.DeletedAt != nil ||
				rec.LockedUntil == nil || !rec.LockedUntil.Equal(*claim.LockedUntil) || !rec.LastObservedAt.Equal(claim.LastObservedAt) {
				t.Fatalf("expired outcome changed durable claim: state=%s last=%s", rec.State, rec.LastObservedAt)
			}
			// The refused write must release its transaction and leave the
			// obligation claimable by a new worker, including with MaxConns=1.
			retry, claimed, err := worker.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
			if err != nil || !claimed || retry.LeaseToken == claim.LeaseToken {
				t.Fatalf("expired outcome prevented recovery: claimed=%v err=%v", claimed, err)
			}
			if err := worker.MarkProviderOrphanDeleted(ctx, ref, retry.LeaseToken, time.Time{}); err != nil {
				t.Fatalf("fresh worker could not settle the orphan: %v", err)
			}
		})
	}
}

func TestProviderReconciliationPostgresOrphanOutcomeRereadsAfterRowWait(t *testing.T) {
	for _, outcome := range []string{"deleted", "failed"} {
		for _, change := range []string{"live", "replacement", "settled", "newer_presence"} {
			t.Run(outcome+"/"+change, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				pool := newProjectionTestPool(ctx, t)
				store := NewStore(pool)
				ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_outcome", ProviderResourceID: "resource_outcome"}
				observeResourceFenceOrphan(t, ctx, store, ref)
				claim, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
				if err != nil || !claimed {
					t.Fatalf("prepare deletion: claimed=%v err=%v", claimed, err)
				}
				deletedAt, err := store.ProviderInventoryTime(ctx)
				if err != nil {
					t.Fatal(err)
				}
				worker, workerPID := newOrphanOutcomeWorker(t, ctx, pool)
				blocker := blockOrphanOutcomeRow(t, ctx, pool, ref)
				defer blocker.Rollback(context.Background()) //nolint:errcheck
				done := make(chan error, 1)
				go func() { done <- writeOrphanOutcome(ctx, worker, outcome, ref, claim.LeaseToken, deletedAt) }()
				waitOrphanOutcomeRow(t, ctx, pool, workerPID)
				seenAt := deletedAt.Add(time.Minute)
				switch change {
				case "replacement":
					_, err = blocker.Exec(ctx, `UPDATE lifecycle.provider_orphans
						SET lease_token='replacement-worker', locked_until=clock_timestamp()+interval '1 minute'`)
				case "settled":
					_, err = blocker.Exec(ctx, `UPDATE lifecycle.provider_orphans SET state='deleted',
						lease_token=NULL, locked_until=NULL, deleted_at=$1, last_observed_at=$1,
						deletion_receipt='{"source":"synthetic_other_worker"}'::jsonb`, seenAt)
				case "newer_presence":
					_, err = blocker.Exec(ctx, `UPDATE lifecycle.provider_orphans SET last_observed_at=$1,
						resource_name='newer-presence'`, seenAt)
				}
				if err != nil {
					t.Fatal(err)
				}
				before := orphanOutcomeSnapshot(t, ctx, blocker, ref)
				if err := blocker.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				err = <-done
				wantLost := change == "replacement" || (change == "newer_presence" && outcome == "deleted")
				if (wantLost && !errors.Is(err, ErrLeaseLost)) || (!wantLost && err != nil) {
					t.Fatalf("outcome did not use post-wait authority: wantLost=%v err=%v", wantLost, err)
				}
				if wantLost || change == "settled" {
					if after := orphanOutcomeSnapshot(t, ctx, pool, ref); after != before {
						t.Fatal("stale/idempotent outcome changed the row committed by another writer")
					}
				} else {
					rec := readAbsenceFenceRecord(t, ctx, pool, ref)
					wantState := ProviderOrphanDeleted
					if outcome == "failed" {
						wantState = ProviderOrphanQuarantined
					}
					if rec.State != wantState || rec.LeaseToken != "" || rec.LockedUntil != nil || rec.Attempts != claim.Attempts {
						t.Fatalf("live outcome did not commit: state=%s attempts=%d", rec.State, rec.Attempts)
					}
					if change == "newer_presence" && (!rec.LastObservedAt.Equal(seenAt) || rec.ResourceName != "newer-presence") {
						t.Fatal("failure outcome regressed the concurrent observation")
					}
				}
				// One-connection pools catch nested pool reads and unreleased
				// transactions on both no-op and rejected result paths.
				if _, err := worker.ProviderInventoryTime(ctx); err != nil {
					t.Fatalf("outcome did not release its connection: %v", err)
				}
			})
		}
	}
}

func TestProviderReconciliationPostgresOrphanOutcomeCancellation(t *testing.T) {
	for _, outcome := range []string{"deleted", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			pool := newProjectionTestPool(ctx, t)
			store := NewStore(pool)
			ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_outcome", ProviderResourceID: "resource_outcome"}
			observeResourceFenceOrphan(t, ctx, store, ref)
			claim, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
			if err != nil || !claimed {
				t.Fatalf("prepare deletion: claimed=%v err=%v", claimed, err)
			}
			deletedAt, err := store.ProviderInventoryTime(ctx)
			if err != nil {
				t.Fatal(err)
			}
			worker, workerPID := newOrphanOutcomeWorker(t, ctx, pool)
			blocker := blockOrphanOutcomeRow(t, ctx, pool, ref)
			defer blocker.Rollback(context.Background()) //nolint:errcheck
			before := orphanOutcomeSnapshot(t, ctx, blocker, ref)
			workerCtx, stopWorker := context.WithCancel(ctx)
			defer stopWorker()
			done := make(chan error, 1)
			go func() { done <- writeOrphanOutcome(workerCtx, worker, outcome, ref, claim.LeaseToken, deletedAt) }()
			waitOrphanOutcomeRow(t, ctx, pool, workerPID)
			stopWorker()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled outcome did not preserve cancellation: %v", err)
			}
			if err := blocker.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if after := orphanOutcomeSnapshot(t, ctx, pool, ref); after != before {
				t.Fatal("cancelled outcome changed durable state")
			}
			if err := writeOrphanOutcome(ctx, worker, outcome, ref, claim.LeaseToken, deletedAt); err != nil {
				t.Fatalf("valid retry after cancellation could not write: %v", err)
			}
		})
	}
}

func newOrphanOutcomeWorker(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (*Store, int) {
	t.Helper()
	cfg := pool.Config()
	cfg.MaxConns = 1
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
	workerPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workerPool.Close)
	var pid int
	if err := workerPool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return NewStore(workerPool), pid
}

func blockOrphanOutcomeRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ref ProviderResourceRef) pgx.Tx {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM lifecycle.provider_orphans
		WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3 FOR UPDATE`,
		ref.Provider, ref.CloudAccountID, ref.ProviderResourceID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	return tx
}

func waitOrphanOutcomeRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pid int) {
	t.Helper()
	waitResourceFenceCondition(t, ctx, pool, fmt.Sprintf(`SELECT EXISTS(
		SELECT 1 FROM pg_stat_activity WHERE pid=%d AND datname=current_database()
		AND wait_event_type='Lock')`, pid))
}

func writeOrphanOutcome(ctx context.Context, store *Store, outcome string, ref ProviderResourceRef, token string, at time.Time) error {
	if outcome == "deleted" {
		return store.MarkProviderOrphanDeleted(ctx, ref, token, at)
	}
	return store.MarkProviderOrphanDeleteFailed(ctx, ref, token, "synthetic provider timeout")
}

func orphanOutcomeSnapshot(t *testing.T, ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, ref ProviderResourceRef) string {
	t.Helper()
	var snapshot string
	if err := queryer.QueryRow(ctx, `SELECT to_jsonb(o)::text FROM lifecycle.provider_orphans AS o
		WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3`,
		ref.Provider, ref.CloudAccountID, ref.ProviderResourceID).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}
