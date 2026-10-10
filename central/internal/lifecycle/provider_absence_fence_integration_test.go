//go:build integration

package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProviderReconciliationPostgresAbsenceProtectsCurrentOwnership(t *testing.T) {
	for _, writer := range []string{"create_receipt", "expired_create", "booking"} {
		t.Run(writer, func(t *testing.T) {
			ctx := context.Background()
			pool := newProjectionTestPool(ctx, t)
			store := NewStore(pool)
			ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_absence", ProviderResourceID: "resource_absence"}
			publish := prepareResourceFenceOwner(t, ctx, pool, writer, ref)
			observeResourceFenceOrphan(t, ctx, store, ref)
			if err := publish(ctx); err != nil {
				t.Fatal(err)
			}
			deleted, err := store.ReconcileProviderOrphansAbsent(ctx, ref.Provider, ref.CloudAccountID, []string{}, time.Now())
			if err != nil || deleted != 0 {
				t.Fatalf("absence retired a current owner: deleted=%d err=%v", deleted, err)
			}
			if rec := readAbsenceFenceRecord(t, ctx, pool, ref); rec.State != ProviderOrphanQuarantined || rec.DeletedAt != nil {
				t.Fatalf("current ownership received an absence receipt: state=%s", rec.State)
			}
			other := ref
			other.CloudAccountID = "other_account"
			observeResourceFenceOrphan(t, ctx, store, other)
			if deleted, err := store.ReconcileProviderOrphansAbsent(ctx, other.Provider, other.CloudAccountID, []string{}, time.Now()); err != nil || deleted != 1 {
				t.Fatalf("unrelated account cleanup stopped: deleted=%d err=%v", deleted, err)
			}
		})
	}
}

func TestProviderReconciliationPostgresAbsenceObservationOrdering(t *testing.T) {
	for _, scenario := range []string{"newer_presence", "equal_presence", "late_older_presence", "claimed_after_snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			pool := newProjectionTestPool(ctx, t)
			store := NewStore(pool)
			ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_absence", ProviderResourceID: "resource_absence"}
			base := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
			obs := ProviderResourceObservation{Provider: ref.Provider, CloudAccountID: ref.CloudAccountID,
				ProviderResourceID: ref.ProviderResourceID, ObservedAt: base, ResourceName: "original"}
			if _, err := store.ObserveProviderOrphan(ctx, obs); err != nil {
				t.Fatal(err)
			}
			absentAt := base.Add(10 * time.Minute)
			if scenario == "claimed_after_snapshot" {
				absentAt = time.Now().UTC().Truncate(time.Microsecond)
				if _, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Minute); err != nil || !claimed {
					t.Fatalf("prepare deleting orphan: claimed=%v err=%v", claimed, err)
				}
			} else {
				obs.ObservedAt = absentAt.Add(time.Minute)
				if scenario == "equal_presence" {
					obs.ObservedAt = absentAt
				}
				obs.ResourceName = "newer-name"
				if _, err := store.ObserveProviderOrphan(ctx, obs); err != nil {
					t.Fatal(err)
				}
			}
			before := readAbsenceFenceRecord(t, ctx, pool, ref)
			if scenario == "late_older_presence" || scenario == "claimed_after_snapshot" {
				obs.ObservedAt, obs.ResourceName = base.Add(time.Minute), "stale-name"
				if _, err := store.ObserveProviderOrphan(ctx, obs); err != nil {
					t.Fatal(err)
				}
			}
			deleted, err := NewStore(pool).ReconcileProviderOrphansAbsent(ctx, ref.Provider, ref.CloudAccountID, []string{}, absentAt)
			if err != nil || deleted != 0 {
				t.Errorf("older/equal absence overrode newer evidence: deleted=%d err=%v", deleted, err)
			}
			after := readAbsenceFenceRecord(t, ctx, pool, ref)
			if after.State != before.State || after.LeaseToken != before.LeaseToken || after.DeletedAt != nil ||
				!after.LastObservedAt.Equal(before.LastObservedAt) || !after.FirstObservedAt.Equal(before.FirstObservedAt) || after.ResourceName != before.ResourceName {
				t.Errorf("stale inventory changed durable state: before=%s/%s after=%s/%s", before.State, before.ResourceName, after.State, after.ResourceName)
			}
			// A strictly newer successful empty inventory can still settle the
			// unowned resource, including an in-flight delete found absent.
			freshAt := before.LastObservedAt.Add(time.Second)
			deleted, err = store.ReconcileProviderOrphansAbsent(ctx, ref.Provider, ref.CloudAccountID, []string{}, freshAt)
			if err != nil || deleted != 1 {
				t.Errorf("fresh absence did not settle orphan: deleted=%d err=%v", deleted, err)
			}
		})
	}
}

func TestProviderReconciliationPostgresAbsenceInventoryValidation(t *testing.T) {
	for _, scenario := range []string{"nil_empty", "empty", "seen", "invalid_id", "missing_timestamp"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			pool := newProjectionTestPool(ctx, t)
			store := NewStore(pool)
			ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_absence", ProviderResourceID: "resource_absence"}
			observeResourceFenceOrphan(t, ctx, store, ref)
			ids, at, wantDeleted := []string{}, time.Now(), 1
			switch scenario {
			case "nil_empty":
				ids = nil
			case "seen":
				ids, wantDeleted = []string{ref.ProviderResourceID}, 0
			case "invalid_id":
				ids, wantDeleted = []string{""}, 0
			case "missing_timestamp":
				at, wantDeleted = time.Time{}, 0
			}
			deleted, err := store.ReconcileProviderOrphansAbsent(ctx, ref.Provider, ref.CloudAccountID, ids, at)
			invalid := scenario == "invalid_id" || scenario == "missing_timestamp"
			if deleted != wantDeleted || (invalid && !errors.Is(err, ErrInvalidArgument)) || (!invalid && err != nil) {
				t.Fatalf("inventory validation: deleted=%d want=%d err=%v", deleted, wantDeleted, err)
			}
			if invalid && readAbsenceFenceRecord(t, ctx, pool, ref).State != ProviderOrphanQuarantined {
				t.Fatal("invalid inventory mutated quarantine")
			}
		})
	}
}

func TestProviderReconciliationPostgresNewerPresenceReopensAbsence(t *testing.T) {
	ctx := context.Background()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)
	ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_absence", ProviderResourceID: "resource_absence"}
	base := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	obs := ProviderResourceObservation{Provider: ref.Provider, CloudAccountID: ref.CloudAccountID,
		ProviderResourceID: ref.ProviderResourceID, ObservedAt: base}
	if _, err := store.ObserveProviderOrphan(ctx, obs); err != nil {
		t.Fatal(err)
	}
	absentAt := base.Add(10 * time.Minute)
	if n, err := store.ReconcileProviderOrphansAbsent(ctx, ref.Provider, ref.CloudAccountID, []string{}, absentAt); err != nil || n != 1 {
		t.Fatalf("prepare absence receipt: deleted=%d err=%v", n, err)
	}
	var receiptBefore string
	if err := pool.QueryRow(ctx, `SELECT deletion_receipt::text FROM lifecycle.provider_orphans`).Scan(&receiptBefore); err != nil {
		t.Fatal(err)
	}
	obs.ObservedAt = base.Add(time.Minute)
	if _, err := store.ObserveProviderOrphan(ctx, obs); err != nil {
		t.Fatal(err)
	}
	rec := readAbsenceFenceRecord(t, ctx, pool, ref)
	if rec.State != ProviderOrphanDeleted || !rec.LastObservedAt.Equal(absentAt) {
		t.Errorf("older presence changed the current absence decision: state=%s last=%s", rec.State, rec.LastObservedAt)
	}
	obs.ObservedAt = base.Add(20 * time.Minute)
	if _, err := NewStore(pool).ObserveProviderOrphan(ctx, obs); err != nil {
		t.Fatal(err)
	}
	rec = readAbsenceFenceRecord(t, ctx, pool, ref)
	if rec.State != ProviderOrphanQuarantined || rec.DeletedAt != nil || rec.Attempts != 0 ||
		!rec.FirstObservedAt.Equal(obs.ObservedAt) || !rec.LastObservedAt.Equal(obs.ObservedAt) {
		t.Errorf("newer positive inventory stayed terminal or lost fresh grace: state=%s first=%s", rec.State, rec.FirstObservedAt)
	}
	var receiptAfter string
	if err := pool.QueryRow(ctx, `SELECT deletion_receipt::text FROM lifecycle.provider_orphans`).Scan(&receiptAfter); err != nil {
		t.Fatal(err)
	}
	if receiptAfter != receiptBefore {
		t.Fatal("reopening erased historical absence evidence")
	}
	if n, err := store.ReconcileProviderOrphansAbsent(ctx, ref.Provider, ref.CloudAccountID, []string{}, absentAt); err != nil || n != 0 {
		t.Errorf("replayed older absence erased reappearance: deleted=%d err=%v", n, err)
	}
	if n, err := store.ReconcileProviderOrphansAbsent(ctx, ref.Provider, ref.CloudAccountID, []string{}, obs.ObservedAt.Add(time.Minute)); err != nil || n != 1 {
		t.Errorf("later absence did not settle reappeared orphan: deleted=%d err=%v", n, err)
	}
}

func TestProviderReconciliationPostgresAbsenceContendsWithNewEvidence(t *testing.T) {
	for _, writer := range []string{"create_receipt", "expired_create", "booking", "presence"} {
		t.Run(writer, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			pool := newProjectionTestPool(ctx, t)
			cfg := pool.Config()
			cfg.MaxConns = 6
			cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
			contenders, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(contenders.Close)
			store := NewStore(contenders)
			ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_absence", ProviderResourceID: "resource_absence"}
			observeResourceFenceOrphan(t, ctx, store, ref)
			absentAt := time.Now().UTC().Truncate(time.Microsecond)
			seenAt := absentAt.Add(time.Minute)
			var publish func(context.Context) error
			if writer == "presence" {
				publish = func(ctx context.Context) error {
					_, err := store.ObserveProviderOrphan(ctx, ProviderResourceObservation{
						Provider: ref.Provider, CloudAccountID: ref.CloudAccountID,
						ProviderResourceID: ref.ProviderResourceID, ObservedAt: seenAt,
					})
					return err
				}
			} else {
				publish = prepareResourceFenceOwner(t, ctx, contenders, writer, ref)
			}
			blocker, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background()) //nolint:errcheck
			if err := lockProviderResourceTx(ctx, blocker, ref); err != nil {
				t.Fatal(err)
			}
			publishDone := make(chan error, 1)
			type result struct {
				deleted int
				err     error
			}
			absenceDone := make(chan result, 1)
			go func() { publishDone <- publish(ctx) }()
			go func() {
				n, err := store.ReconcileProviderOrphansAbsent(ctx, ref.Provider, ref.CloudAccountID, nil, absentAt)
				absenceDone <- result{n, err}
			}()
			waitResourceFenceCondition(t, ctx, pool, `SELECT count(*) = 2 FROM pg_locks
				WHERE locktype='advisory' AND NOT granted
				AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`)
			if err := blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			publishErr, absent := <-publishDone, <-absenceDone
			if absent.err != nil || absent.deleted < 0 || absent.deleted > 1 {
				t.Fatalf("absence result=%+v", absent)
			}
			rec := readAbsenceFenceRecord(t, ctx, pool, ref)
			if writer == "presence" {
				if publishErr != nil || rec.State != ProviderOrphanQuarantined || rec.DeletedAt != nil || !rec.LastObservedAt.Equal(seenAt) {
					t.Fatalf("newer presence lost a contested absence: state=%s err=%v", rec.State, publishErr)
				}
			} else if publishErr == nil {
				if absent.deleted != 0 || rec.State != ProviderOrphanQuarantined || rec.DeletedAt != nil {
					t.Fatal("both current ownership and absence committed")
				}
			} else if !errors.Is(publishErr, ErrInvariantViolation) || absent.deleted != 1 || rec.State != ProviderOrphanDeleted {
				t.Fatalf("no consistent winner: ownership=%v absence=%+v state=%s", publishErr, absent, rec.State)
			}
		})
	}
}

func TestProviderReconciliationPostgresAbsenceCancellationRollsBackBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)
	first := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_absence", ProviderResourceID: "a_first"}
	second := first
	second.ProviderResourceID = "b_blocked"
	observeResourceFenceOrphan(t, ctx, store, first)
	observeResourceFenceOrphan(t, ctx, store, second)
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background()) //nolint:errcheck
	if err := lockProviderResourceTx(ctx, blocker, second); err != nil {
		t.Fatal(err)
	}
	waitCtx, stopWaiting := context.WithCancel(ctx)
	defer stopWaiting()
	done := make(chan error, 1)
	go func() {
		n, err := store.ReconcileProviderOrphansAbsent(waitCtx, first.Provider, first.CloudAccountID, nil, time.Now())
		if n != 0 {
			err = errors.New("cancelled batch reported committed deletions")
		}
		done <- err
	}()
	waitResourceFenceCondition(t, ctx, pool, `SELECT count(*) FILTER (WHERE NOT granted) = 1
		AND count(*) FILTER (WHERE granted) = 2 FROM pg_locks
		WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`)
	stopWaiting()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled absence batch: %v", err)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []ProviderResourceRef{first, second} {
		if rec := readAbsenceFenceRecord(t, ctx, pool, ref); rec.State != ProviderOrphanQuarantined || rec.DeletedAt != nil {
			t.Fatalf("partial absence survived rollback for %s: state=%s", ref.ProviderResourceID, rec.State)
		}
	}
	if n, err := store.ReconcileProviderOrphansAbsent(ctx, first.Provider, first.CloudAccountID, nil, time.Now()); err != nil || n != 2 {
		t.Fatalf("recovery after cancelled batch: deleted=%d err=%v", n, err)
	}
}

func TestProviderReconciliationPostgresReappearanceRetainsDeleteAuthorization(t *testing.T) {
	ctx := context.Background()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)
	ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_absence", ProviderResourceID: "resource_absence"}
	observeResourceFenceOrphan(t, ctx, store, ref)
	claim, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("prepare deletion: claimed=%v err=%v", claimed, err)
	}
	if err := store.MarkProviderOrphanDeleted(ctx, ref, claim.LeaseToken, time.Now()); err != nil {
		t.Fatal(err)
	}
	prior := readAbsenceFenceRecord(t, ctx, pool, ref)
	seenAt := prior.LastObservedAt.Add(time.Minute)
	if _, err := store.ObserveProviderOrphan(ctx, ProviderResourceObservation{Provider: ref.Provider,
		CloudAccountID: ref.CloudAccountID, ProviderResourceID: ref.ProviderResourceID, ObservedAt: seenAt}); err != nil {
		t.Fatal(err)
	}
	rec := readAbsenceFenceRecord(t, ctx, pool, ref)
	if rec.State != ProviderOrphanQuarantined || rec.Attempts != claim.Attempts || !rec.FirstObservedAt.Equal(seenAt) {
		t.Fatalf("reappearance lost deletion history or fresh grace: state=%s attempts=%d", rec.State, rec.Attempts)
	}
	publish := prepareResourceFenceOwner(t, ctx, pool, "expired_create", ref)
	if err := publish(ctx); !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("reappearance discarded prior delete authorization: %v", err)
	}
	if err := store.MarkProviderOrphanDeleted(ctx, ref, claim.LeaseToken, seenAt); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale worker erased newer presence: %v", err)
	}
	if retry, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Minute); err != nil || !claimed || retry.Attempts != claim.Attempts+1 {
		t.Fatalf("reappeared unowned resource cannot be reclaimed: claimed=%v attempts=%d err=%v", claimed, retry.Attempts, err)
	}
}

func TestProviderReconciliationPostgresOrphanWritersPreserveObservationOrder(t *testing.T) {
	for _, writer := range []string{"claim", "failure"} {
		t.Run(writer, func(t *testing.T) {
			ctx := context.Background()
			pool := newProjectionTestPool(ctx, t)
			store := NewStore(pool)
			ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_absence", ProviderResourceID: "resource_absence"}
			observeResourceFenceOrphan(t, ctx, store, ref)
			claim, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
			if err != nil || !claimed {
				t.Fatalf("prepare deletion: claimed=%v err=%v", claimed, err)
			}
			if writer == "claim" {
				if err := store.MarkProviderOrphanDeleteFailed(ctx, ref, claim.LeaseToken, "synthetic retry"); err != nil {
					t.Fatal(err)
				}
			}
			seenAt := time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour)
			if _, err := store.ObserveProviderOrphan(ctx, ProviderResourceObservation{Provider: ref.Provider,
				CloudAccountID: ref.CloudAccountID, ProviderResourceID: ref.ProviderResourceID, ObservedAt: seenAt}); err != nil {
				t.Fatal(err)
			}
			if writer == "claim" {
				if _, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Minute); err != nil || !claimed {
					t.Fatalf("retry deletion: claimed=%v err=%v", claimed, err)
				}
			} else if err := store.MarkProviderOrphanDeleteFailed(ctx, ref, claim.LeaseToken, "synthetic retry"); err != nil {
				t.Fatal(err)
			}
			if rec := readAbsenceFenceRecord(t, ctx, pool, ref); !rec.LastObservedAt.Equal(seenAt) {
				t.Fatalf("%s regressed the observation watermark: got=%s want=%s", writer, rec.LastObservedAt, seenAt)
			}
			if n, err := store.ReconcileProviderOrphansAbsent(ctx, ref.Provider, ref.CloudAccountID, nil, seenAt.Add(-time.Minute)); err != nil || n != 0 {
				t.Fatalf("older absence overrode newer presence after %s: deleted=%d err=%v", writer, n, err)
			}
		})
	}
}

func TestProviderReconciliationPostgresDeleteReceiptArrivalOrdering(t *testing.T) {
	for _, first := range []string{"presence", "receipt"} {
		t.Run(first, func(t *testing.T) {
			ctx := context.Background()
			pool := newProjectionTestPool(ctx, t)
			store := NewStore(pool)
			ref := ProviderResourceRef{Provider: "linode", CloudAccountID: "account_absence", ProviderResourceID: "resource_absence"}
			observeResourceFenceOrphan(t, ctx, store, ref)
			claim, claimed, err := store.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
			if err != nil || !claimed {
				t.Fatalf("prepare deletion: claimed=%v err=%v", claimed, err)
			}
			deletedAt, err := store.ProviderInventoryTime(ctx)
			if err != nil {
				t.Fatal(err)
			}
			seenAt, err := store.ProviderInventoryTime(ctx)
			if err != nil || !seenAt.After(deletedAt) {
				t.Fatalf("prepare newer observation: deleted=%s seen=%s err=%v", deletedAt, seenAt, err)
			}
			observe := func() {
				t.Helper()
				if _, err := store.ObserveProviderOrphan(ctx, ProviderResourceObservation{Provider: ref.Provider,
					CloudAccountID: ref.CloudAccountID, ProviderResourceID: ref.ProviderResourceID, ObservedAt: seenAt}); err != nil {
					t.Fatal(err)
				}
			}
			if first == "presence" {
				observe()
				if err := store.MarkProviderOrphanDeleted(ctx, ref, claim.LeaseToken, deletedAt); !errors.Is(err, ErrLeaseLost) {
					t.Fatalf("stale delete receipt overwrote newer presence: %v", err)
				}
			} else {
				if err := store.MarkProviderOrphanDeleted(ctx, ref, claim.LeaseToken, deletedAt); err != nil {
					t.Fatal(err)
				}
				observe()
			}
			rec := readAbsenceFenceRecord(t, ctx, pool, ref)
			if rec.State == ProviderOrphanDeleted || rec.DeletedAt != nil || !rec.LastObservedAt.Equal(seenAt) || rec.Attempts != claim.Attempts {
				t.Fatalf("receipt arrival order hid newer presence: state=%s last=%s attempts=%d", rec.State, rec.LastObservedAt, rec.Attempts)
			}
			if n, err := store.ReconcileProviderOrphansAbsent(ctx, ref.Provider, ref.CloudAccountID, nil, seenAt.Add(time.Minute)); err != nil || n != 1 {
				t.Fatalf("fresh absence could not resolve the retained obligation: deleted=%d err=%v", n, err)
			}
		})
	}
}

func readAbsenceFenceRecord(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ref ProviderResourceRef) ProviderOrphanRecord {
	t.Helper()
	rec := ProviderOrphanRecord{Provider: ref.Provider, CloudAccountID: ref.CloudAccountID, ProviderResourceID: ref.ProviderResourceID}
	if err := pool.QueryRow(ctx, `SELECT state, resource_name, first_observed_at, last_observed_at,
		attempts, COALESCE(lease_token,''), locked_until, deleted_at, observations
		FROM lifecycle.provider_orphans WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3`,
		ref.Provider, ref.CloudAccountID, ref.ProviderResourceID).Scan(&rec.State, &rec.ResourceName,
		&rec.FirstObservedAt, &rec.LastObservedAt, &rec.Attempts, &rec.LeaseToken, &rec.LockedUntil, &rec.DeletedAt, &rec.Observations); err != nil {
		t.Fatal(err)
	}
	return rec
}
