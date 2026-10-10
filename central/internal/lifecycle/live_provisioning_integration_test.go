//go:build integration

package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLiveProvisioningOpsPostgresProjection(t *testing.T) {
	ctx := context.Background()
	pool := newProjectionTestPool(ctx, t)
	store := NewStore(pool)

	admit := func(suffix string) AdmissionRequest {
		t.Helper()
		req := testAdmission("live_" + suffix)
		if _, err := store.AdmitWorkload(ctx, req); err != nil {
			t.Fatalf("admit %s: %v", suffix, err)
		}
		return req
	}
	claim := func(req AdmissionRequest) ProviderCreateOperation {
		t.Helper()
		op, claimed, err := store.ClaimProviderCreateForBurst(
			ctx, req.CustomerID, req.ClusterID, req.BurstID, time.Minute,
		)
		if err != nil || !claimed {
			t.Fatalf("claim %s: claimed=%v err=%v", req.BurstID, claimed, err)
		}
		return op
	}

	pending := admit("pending")
	processing := admit("processing")
	claim(processing)

	succeeded := admit("succeeded")
	succeededOp := claim(succeeded)
	if err := store.MarkProviderCreateSucceeded(ctx, succeededOp.ID, succeededOp.LeaseToken, "provider_succeeded"); err != nil {
		t.Fatalf("succeed create: %v", err)
	}

	deadLetter := admit("dead_letter")
	deadLetterOp := claim(deadLetter)
	if err := store.MarkProviderCreateFailed(ctx, deadLetterOp.ID, deadLetterOp.LeaseToken,
		"bounded failure", time.Now().Add(time.Minute), 1); err != nil {
		t.Fatalf("dead-letter create: %v", err)
	}

	failed := admit("failed")
	failedOp := claim(failed)
	if err := store.MarkProviderCreateFailed(ctx, failedOp.ID, failedOp.LeaseToken,
		"retryable failure", time.Now().Add(time.Minute), 2); err != nil {
		t.Fatalf("fail create: %v", err)
	}

	terminated := admit("terminated")
	terminatedOp := claim(terminated)
	if err := store.MarkProviderCreateSucceeded(ctx, terminatedOp.ID, terminatedOp.LeaseToken, "provider_terminated"); err != nil {
		t.Fatalf("succeed terminated create: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE lifecycle.bursts SET state='terminated', terminal_at=now()
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3`,
		terminated.CustomerID, terminated.ClusterID, terminated.BurstID); err != nil {
		t.Fatalf("terminate burst: %v", err)
	}

	records, err := store.LiveProvisioningOps(ctx)
	if err != nil {
		t.Fatalf("LiveProvisioningOps: %v", err)
	}
	got := make(map[string]string, len(records))
	for _, record := range records {
		got[record.BurstID] = record.State
		if record.CreatedAt.IsZero() {
			t.Errorf("%s has zero CreatedAt", record.BurstID)
		}
	}
	want := map[string]string{
		processing.BurstID: OperationProcessing,
		succeeded.BurstID:  OperationSucceeded,
		deadLetter.BurstID: OperationDeadLetter,
	}
	if len(got) != len(want) {
		t.Fatalf("live projection = %v, want exactly %v (pending=%s failed=%s terminated=%s must be absent)",
			got, want, pending.BurstID, failed.BurstID, terminated.BurstID)
	}
	for burstID, state := range want {
		if got[burstID] != state {
			t.Errorf("%s state = %q, want %q", burstID, got[burstID], state)
		}
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.LiveProvisioningOps(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read error = %v, want context.Canceled", err)
	}
}
