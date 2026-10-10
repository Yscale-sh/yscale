//go:build integration

package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProviderReconciliationPostgresOwnershipBeforeOrphanClaim(t *testing.T) {
	for _, writer := range []string{"create_receipt", "expired_create", "booking"} {
		for _, scope := range []string{"same_resource", "other_account", "other_provider"} {
			t.Run(writer+"/"+scope, func(t *testing.T) {
				ctx := context.Background()
				pool := newProjectionTestPool(ctx, t)
				store := NewStore(pool)
				ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_fence", ProviderResourceID: "resource_fence"}
				publish := prepareResourceFenceOwner(t, ctx, pool, writer, ref)
				orphan := ref
				if scope == "other_account" {
					orphan.CloudAccountID = "other_account"
				}
				if scope == "other_provider" {
					orphan.Provider = "flyio"
				}
				observeResourceFenceOrphan(t, ctx, store, orphan)
				protected, err := store.ProtectedProviderResources(ctx)
				if err != nil || len(protected) != 0 {
					t.Fatalf("initial ownership snapshot: refs=%v err=%v", protected, err)
				}
				// Another replica commits ownership after the sweep's snapshot.
				if err := publish(ctx); err != nil {
					t.Fatal(err)
				}
				_, claimed, err := NewStore(pool).ClaimProviderOrphanDelete(ctx, orphan, time.Minute)
				wantClaim := scope != "same_resource"
				if err != nil || claimed != wantClaim {
					t.Fatalf("claim after ownership commit: claimed=%v want=%v err=%v", claimed, wantClaim, err)
				}
				if !wantClaim {
					var state string
					var attempts int
					if err := pool.QueryRow(ctx, `SELECT state, attempts FROM lifecycle.provider_orphans
						WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3`,
						orphan.Provider, orphan.CloudAccountID, orphan.ProviderResourceID).Scan(&state, &attempts); err != nil {
						t.Fatal(err)
					}
					if state != ProviderOrphanQuarantined || attempts != 0 {
						t.Fatalf("rejected claim mutated quarantine: state=%s attempts=%d", state, attempts)
					}
				}
			})
		}
	}
}

func TestProviderReconciliationPostgresOrphanClaimBeforeOwnership(t *testing.T) {
	for _, writer := range []string{"create_receipt", "expired_create", "booking"} {
		for _, outcome := range []string{"deleting", "lease_expired", "delete_failed", "deleted"} {
			t.Run(writer+"/"+outcome, func(t *testing.T) {
				ctx := context.Background()
				pool := newProjectionTestPool(ctx, t)
				store := NewStore(pool)
				ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_fence", ProviderResourceID: "resource_fence"}
				publish := prepareResourceFenceOwner(t, ctx, pool, writer, ref)
				observeResourceFenceOrphan(t, ctx, store, ref)
				claim, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
				if err != nil || !claimed {
					t.Fatalf("initial orphan claim: claimed=%v err=%v", claimed, err)
				}
				switch outcome {
				case "lease_expired":
					_, err = pool.Exec(ctx, `UPDATE lifecycle.provider_orphans SET locked_until=now()-interval '1 second'
						WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3`, ref.Provider, ref.CloudAccountID, ref.ProviderResourceID)
				case "delete_failed":
					err = store.MarkProviderOrphanDeleteFailed(ctx, ref, claim.LeaseToken, "synthetic ambiguous delete")
				case "deleted":
					err = store.MarkProviderOrphanDeleted(ctx, ref, claim.LeaseToken, time.Now())
				}
				if err != nil {
					t.Fatal(err)
				}
				// Lease expiry or a failed response does not cancel a provider call.
				if err := publish(ctx); !errors.Is(err, ErrInvariantViolation) {
					t.Fatalf("ownership after orphan delete %s: err=%v, want invariant refusal", outcome, err)
				}
				var owners int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM lifecycle.bursts
					WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3`,
					ref.Provider, ref.CloudAccountID, ref.ProviderResourceID).Scan(&owners); err != nil {
					t.Fatal(err)
				}
				if owners != 0 {
					t.Fatalf("refused ownership partially committed %d burst owners", owners)
				}
			})
		}
	}
}

func TestProviderReconciliationPostgresConcurrentOwnershipFence(t *testing.T) {
	for _, writer := range []string{"create_receipt", "expired_create", "booking"} {
		t.Run(writer, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			pool := newProjectionTestPool(ctx, t)
			// A database/session default must not turn the post-lock read into
			// a pre-lock snapshot. Exercise the public methods with that default.
			cfg := pool.Config()
			cfg.MaxConns = 6
			cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
			contenders, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(contenders.Close)
			var isolation string
			if err := contenders.QueryRow(ctx, `SHOW default_transaction_isolation`).Scan(&isolation); err != nil || isolation != "repeatable read" {
				t.Fatalf("contender default isolation=%q err=%v", isolation, err)
			}
			ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_fence", ProviderResourceID: "resource_fence"}
			publish := prepareResourceFenceOwner(t, ctx, contenders, writer, ref)
			observeResourceFenceOrphan(t, ctx, NewStore(pool), ref)
			blocker, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background()) //nolint:errcheck
			if err := lockProviderResourceTx(ctx, blocker, ref); err != nil {
				t.Fatal(err)
			}
			ownerDone := make(chan error, 1)
			type claimResult struct {
				claimed bool
				err     error
			}
			claimDone := make(chan claimResult, 1)
			go func() { ownerDone <- publish(ctx) }()
			go func() {
				_, claimed, err := NewStore(contenders).ClaimProviderOrphanDelete(ctx, ref, time.Minute)
				claimDone <- claimResult{claimed, err}
			}()
			// Observe both real database waiters before releasing either side.
			waitResourceFenceCondition(t, ctx, pool, `SELECT count(*) = 2 FROM pg_locks
				WHERE locktype='advisory' AND NOT granted
				AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`)
			if err := blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			ownerErr, claim := <-ownerDone, <-claimDone
			if claim.err != nil || (ownerErr != nil && !errors.Is(ownerErr, ErrInvariantViolation)) {
				t.Fatalf("unexpected contender failure: owner=%v claim=%v", ownerErr, claim.err)
			}
			if (ownerErr == nil) == claim.claimed {
				t.Fatalf("expected exactly one winner: owner=%v orphanClaimed=%v", ownerErr, claim.claimed)
			}
			protected, err := NewStore(pool).ProtectedProviderResources(ctx)
			if err != nil || (len(protected) > 0) != (ownerErr == nil) {
				t.Fatalf("durable ownership disagrees with winner: refs=%v err=%v", protected, err)
			}
		})
	}
}

func TestProviderReconciliationPostgresResourceFenceCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)
	ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_fence", ProviderResourceID: "resource_fence"}
	observeResourceFenceOrphan(t, ctx, store, ref)
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background()) //nolint:errcheck
	if err := lockProviderResourceTx(ctx, blocker, ref); err != nil {
		t.Fatal(err)
	}
	waitCtx, stopWaiting := context.WithCancel(ctx)
	defer stopWaiting()
	done := make(chan error, 1)
	go func() {
		_, _, err := store.ClaimProviderOrphanDelete(waitCtx, ref, time.Minute)
		done <- err
	}()
	waitResourceFenceCondition(t, ctx, pool, `SELECT count(*) = 1 FROM pg_locks
		WHERE locktype='advisory' AND NOT granted
		AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`)
	stopWaiting()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled lock wait: %v", err)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	claim, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
	if err != nil || !claimed || claim.Attempts != 1 {
		t.Fatalf("cancelled waiter leaked lock/claim: claimed=%v attempts=%d err=%v", claimed, claim.Attempts, err)
	}
}

func TestProviderReconciliationPostgresCreateLeaseExpiresAtResourceFence(t *testing.T) {
	for _, waitOn := range []string{"resource", "burst"} {
		t.Run(waitOn, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			pool := newProjectionTestPool(ctx, t)
			ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_fence", ProviderResourceID: "resource_fence"}
			publish := prepareResourceFenceOwner(t, ctx, pool, "create_receipt", ref)
			blocker, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background()) //nolint:errcheck
			waitQuery := `SELECT count(*) = 1 FROM pg_locks
		WHERE locktype='advisory' AND NOT granted
		AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`
			if waitOn == "resource" {
				if err := lockProviderResourceTx(ctx, blocker, ref); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := blocker.Exec(ctx, `SELECT id FROM lifecycle.bursts FOR UPDATE`); err != nil {
					t.Fatal(err)
				}
				waitQuery = `SELECT count(*) = 1 FROM pg_stat_activity
			WHERE datname=current_database() AND pid <> pg_backend_pid()
			AND wait_event_type='Lock' AND query LIKE '%SELECT state FROM lifecycle.bursts%'`
			}
			if _, err := pool.Exec(ctx, `UPDATE lifecycle.operations SET locked_until=clock_timestamp()+interval '1 second'
		WHERE operation_type='provider_create'`); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- publish(ctx) }()
			waitResourceFenceCondition(t, ctx, pool, waitQuery)
			waitResourceFenceCondition(t, ctx, pool, `SELECT bool_and(locked_until <= clock_timestamp())
		FROM lifecycle.operations WHERE operation_type='provider_create'`)
			if err := blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-done; !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("create completed after lease expired waiting for resource fence: %v", err)
			}
			var state, resourceID string
			if err := pool.QueryRow(ctx, `SELECT state, provider_resource_id FROM lifecycle.operations
		WHERE operation_type='provider_create'`).Scan(&state, &resourceID); err != nil {
				t.Fatal(err)
			}
			if state != OperationProcessing || resourceID != "" {
				t.Fatalf("expired create published ownership: state=%s resource=%q", state, resourceID)
			}
		})
	}
}

func TestProviderReconciliationPostgresOrphanLeaseStartsAfterResourceFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)
	ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_fence", ProviderResourceID: "resource_fence"}
	observeResourceFenceOrphan(t, ctx, store, ref)
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background()) //nolint:errcheck
	if err := lockProviderResourceTx(ctx, blocker, ref); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Second)
		if err == nil && !claimed {
			err = errors.New("orphan was not claimed")
		}
		done <- err
	}()
	waitResourceFenceCondition(t, ctx, pool, `SELECT count(*) = 1 FROM pg_stat_activity
		WHERE datname=current_database() AND pid <> pg_backend_pid()
		AND wait_event_type='Lock' AND wait_event='advisory'
		AND clock_timestamp()-xact_start > interval '1.1 seconds'`)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var leaseLive bool
	if err := pool.QueryRow(ctx, `SELECT locked_until > clock_timestamp() + interval '500 milliseconds'
		FROM lifecycle.provider_orphans WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3`,
		ref.Provider, ref.CloudAccountID, ref.ProviderResourceID).Scan(&leaseLive); err != nil {
		t.Fatal(err)
	}
	if !leaseLive {
		t.Fatal("new orphan lease was consumed by the pre-claim resource lock wait")
	}
}

func waitResourceFenceCondition(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ready bool
		if err := pool.QueryRow(ctx, query).Scan(&ready); err != nil {
			t.Fatalf("wait for database fence condition: %v", err)
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("database fence condition was not observed: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func prepareResourceFenceOwner(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer string, ref ProviderResourceRef) func(context.Context) error {
	t.Helper()
	store := NewStore(pool)
	if writer == "booking" {
		booking := validBooking()
		booking.Provider, booking.CloudAccountID, booking.ProviderResourceID = ref.Provider, ref.CloudAccountID, ref.ProviderResourceID
		return func(ctx context.Context) error {
			_, err := NewStore(pool).RequestProviderDeleteForBooking(ctx, booking)
			return err
		}
	}
	req := validAdmission()
	req.Provider, req.CloudAccountID = ref.Provider, ref.CloudAccountID
	if _, err := store.AdmitWorkload(ctx, req); err != nil {
		t.Fatal(err)
	}
	op, claimed, err := store.ClaimProviderCreateForBurst(ctx, req.CustomerID, req.ClusterID, req.BurstID, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("prepare create: claimed=%v err=%v", claimed, err)
	}
	if writer == "expired_create" {
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.operations SET locked_until=now()-interval '1 second' WHERE id=$1`, op.ID); err != nil {
			t.Fatal(err)
		}
		return func(ctx context.Context) error {
			return NewStore(pool).ReconcileExpiredProviderCreateSucceeded(ctx, op.ID, ref.ProviderResourceID)
		}
	}
	return func(ctx context.Context) error {
		return NewStore(pool).MarkProviderCreateSucceeded(ctx, op.ID, op.LeaseToken, ref.ProviderResourceID)
	}
}

func observeResourceFenceOrphan(t *testing.T, ctx context.Context, store *Store, ref ProviderResourceRef) {
	t.Helper()
	if _, err := store.ObserveProviderOrphan(ctx, ProviderResourceObservation{
		Provider: ref.Provider, CloudAccountID: ref.CloudAccountID, ProviderResourceID: ref.ProviderResourceID,
		ObservedAt: time.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
}
