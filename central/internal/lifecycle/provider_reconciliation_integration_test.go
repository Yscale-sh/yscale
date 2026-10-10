//go:build integration

package lifecycle

import (
	"context"
	"testing"
	"time"
)

func TestProviderReconciliationPostgresPersistenceAndFencing(t *testing.T) {
	ctx := context.Background()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)
	for _, table := range []string{"provider_reconciliations", "provider_orphans"} {
		var present bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "lifecycle."+table).Scan(&present); err != nil {
			t.Fatal(err)
		}
		if !present {
			t.Fatalf("missing lifecycle.%s", table)
		}
	}

	req := validAdmission()
	req.CloudAccountID = "ca_reconcile"
	req.ProviderCreatePayload = []byte(`{"node_name":"ys-burst-expired"}`)
	if _, err := store.AdmitWorkload(ctx, req); err != nil {
		t.Fatalf("admit workload: %v", err)
	}
	op, claimed, err := store.ClaimProviderCreateForBurst(ctx, req.CustomerID, req.ClusterID, req.BurstID, time.Second)
	if err != nil {
		t.Fatalf("claim provider create: %v", err)
	}
	if !claimed {
		t.Fatal("provider create was not claimed")
	}
	if _, err := pool.Exec(ctx, `
		UPDATE lifecycle.operations
		SET locked_until=now()-interval '1 second'
		WHERE id=$1`, op.ID); err != nil {
		t.Fatalf("expire provider-create lease: %v", err)
	}
	creates, err := store.ProviderCreatesForReconciliation(ctx, req.Provider, req.CloudAccountID)
	if err != nil {
		t.Fatalf("list provider creates: %v", err)
	}
	if len(creates) != 1 || !creates[0].LeaseExpired || creates[0].ResourceName != "ys-burst-expired" ||
		creates[0].BurstID != req.BurstID {
		t.Fatalf("provider create reconciliation view = %+v", creates)
	}
	const reconciledResourceID = "provider/resource with punctuation!"
	if err := store.ReconcileExpiredProviderCreateSucceeded(ctx, op.ID, reconciledResourceID); err != nil {
		t.Fatalf("reconcile expired provider create: %v", err)
	}
	var burstState, resourceID string
	if err := pool.QueryRow(ctx, `
		SELECT state, provider_resource_id
		FROM lifecycle.bursts
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3`,
		req.CustomerID, req.ClusterID, req.BurstID).Scan(&burstState, &resourceID); err != nil {
		t.Fatal(err)
	}
	if burstState != BurstProviderCreated || resourceID != reconciledResourceID {
		t.Fatalf("burst state/resource = %s/%s", burstState, resourceID)
	}
	protected, err := store.ProtectedProviderResources(ctx)
	if err != nil {
		t.Fatalf("protected resources: %v", err)
	}
	if len(protected) == 0 || protected[0].CloudAccountID != req.CloudAccountID || protected[0].ProviderResourceID != reconciledResourceID {
		t.Fatalf("protected resource identity = %+v", protected)
	}

	observedAt := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	obs := ProviderResourceObservation{
		Provider: req.Provider, CloudAccountID: req.CloudAccountID,
		ProviderResourceID: "orphan-1", ResourceName: "ys-burst-orphan",
		ProviderCreatedAt: observedAt.Add(-time.Hour), ObservedAt: observedAt,
	}
	first, err := store.ObserveProviderOrphan(ctx, obs)
	if err != nil {
		t.Fatalf("observe orphan: %v", err)
	}
	reopened := NewStore(pool)
	second, err := reopened.ObserveProviderOrphan(ctx, ProviderResourceObservation{
		Provider: req.Provider, CloudAccountID: req.CloudAccountID,
		ProviderResourceID: "orphan-1", ResourceName: "ys-burst-orphan",
		ObservedAt: observedAt.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("observe orphan after restart: %v", err)
	}
	if !second.FirstObservedAt.Equal(first.FirstObservedAt) || second.Observations != first.Observations+1 {
		t.Fatalf("quarantine did not persist across store reopen: first=%+v second=%+v", first, second)
	}
	ref := ProviderResourceRef{Provider: req.Provider, CloudAccountID: req.CloudAccountID, ProviderResourceID: "orphan-1"}
	claimedOrphan, claimed, err := reopened.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
	if err != nil {
		t.Fatalf("claim orphan delete: %v", err)
	}
	if !claimed || claimedOrphan.State != ProviderOrphanDeleting || claimedOrphan.LeaseToken == "" {
		t.Fatalf("orphan delete claim = %+v claimed=%v", claimedOrphan, claimed)
	}
	if _, claimedAgain, err := reopened.ClaimProviderOrphanDelete(ctx, ref, time.Minute); err != nil {
		t.Fatalf("concurrent orphan delete claim: %v", err)
	} else if claimedAgain {
		t.Fatal("concurrent orphan delete claim was not fenced")
	}
	if err := reopened.MarkProviderOrphanDeleteFailed(ctx, ref, claimedOrphan.LeaseToken, "stage=delete_node_failed provider=linode cloud_account_id=ca_reconcile"); err != nil {
		t.Fatalf("mark delete failed: %v", err)
	}
	retryClaim, retryClaimed, err := reopened.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
	if err != nil {
		t.Fatalf("retry orphan delete claim: %v", err)
	}
	if !retryClaimed || retryClaim.LeaseToken == claimedOrphan.LeaseToken {
		t.Fatalf("retry orphan delete claim = %+v claimed=%v", retryClaim, retryClaimed)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE lifecycle.provider_orphans
		SET locked_until=now()-interval '1 second'
		WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3`,
		ref.Provider, ref.CloudAccountID, ref.ProviderResourceID); err != nil {
		t.Fatalf("expire orphan delete lease: %v", err)
	}
	staleClaim, staleClaimed, err := reopened.ClaimProviderOrphanDelete(ctx, ref, time.Minute)
	if err != nil {
		t.Fatalf("stale orphan delete reclaim: %v", err)
	}
	if !staleClaimed || staleClaim.LeaseToken == retryClaim.LeaseToken {
		t.Fatalf("stale orphan delete reclaim = %+v claimed=%v", staleClaim, staleClaimed)
	}
	// The result timestamp follows the live claim, not the historical fixture
	// observation; older receipts must not override the claim's watermark.
	deletedAt, err := reopened.ProviderInventoryTime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.MarkProviderOrphanDeleted(ctx, ref, staleClaim.LeaseToken, deletedAt); err != nil {
		t.Fatalf("mark deleted: %v", err)
	}
	if err := reopened.MarkProviderOrphanDeleted(ctx, ref, staleClaim.LeaseToken, deletedAt.Add(time.Hour)); err != nil {
		t.Fatalf("idempotent second delete receipt should not fail: %v", err)
	}
	absentObs := ProviderResourceObservation{
		Provider: req.Provider, CloudAccountID: req.CloudAccountID,
		ProviderResourceID: "orphan-absent", ResourceName: "ys-burst-absent",
		ObservedAt: observedAt,
	}
	if _, err := reopened.ObserveProviderOrphan(ctx, absentObs); err != nil {
		t.Fatalf("observe absent orphan: %v", err)
	}
	absentDeleted, err := reopened.ReconcileProviderOrphansAbsent(ctx, req.Provider, req.CloudAccountID,
		[]string{reconciledResourceID}, observedAt.Add(3*time.Hour))
	if err != nil {
		t.Fatalf("reconcile absent orphan: %v", err)
	}
	if absentDeleted != 1 {
		t.Fatalf("absent deleted count = %d, want 1", absentDeleted)
	}
	var state, lastError string
	var attempts int
	if err := pool.QueryRow(ctx, `
		SELECT state, attempts, last_error
		FROM lifecycle.provider_orphans
		WHERE provider=$1 AND cloud_account_id=$2 AND provider_resource_id=$3`,
		ref.Provider, ref.CloudAccountID, ref.ProviderResourceID).Scan(&state, &attempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if state != ProviderOrphanDeleted || attempts != 2 || lastError != "" {
		t.Fatalf("delete receipt state=%s attempts=%d last_error=%q", state, attempts, lastError)
	}
}
