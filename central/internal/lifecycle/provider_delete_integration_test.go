//go:build integration

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresProviderDeleteLifecycle(t *testing.T) {
	dsn := os.Getenv("LIFECYCLE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set LIFECYCLE_TEST_DATABASE_URL to an isolated Postgres database")
	}
	if os.Getenv("LIFECYCLE_TEST_ALLOW_DESTRUCTIVE") != "DROP_LIFECYCLE_SCHEMA" {
		t.Fatal("set LIFECYCLE_TEST_ALLOW_DESTRUCTIVE=DROP_LIFECYCLE_SCHEMA for the isolated test database")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var databaseName string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("LIFECYCLE_TEST_EXPECT_DATABASE"); expected == "" || expected != databaseName {
		t.Fatalf("LIFECYCLE_TEST_EXPECT_DATABASE must exactly match current database %q", databaseName)
	}
	if !strings.HasSuffix(databaseName, "_lifecycle_test") {
		t.Fatalf("refusing destructive integration test against non-test database %q", databaseName)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS lifecycle CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS lifecycle CASCADE`); err != nil {
			t.Errorf("cleanup lifecycle schema: %v", err)
		}
		pool.Close()
	})
	if err := EnsureProviderDeleteSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)

	req := testAdmission("provider_delete")
	if _, err := store.AdmitWorkload(ctx, req); err != nil {
		t.Fatal(err)
	}
	create, ok, err := store.ClaimProviderCreate(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim provider create: op=%+v ok=%v err=%v", create, ok, err)
	}
	const providerResourceID = "linode_987654"
	if err := store.MarkProviderCreateSucceeded(ctx, create.ID, create.LeaseToken, providerResourceID); err != nil {
		t.Fatal(err)
	}

	uncreated := testAdmission("provider_delete_uncreated")
	if _, err := store.AdmitWorkload(ctx, uncreated); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RequestProviderDelete(ctx, ProviderDeleteRequest{
		CustomerID: uncreated.CustomerID,
		ClusterID:  uncreated.ClusterID,
		BurstID:    uncreated.BurstID,
	}); !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("delete requested before provider create: %v", err)
	}

	requested, err := store.RequestProviderDelete(ctx, ProviderDeleteRequest{
		CustomerID: req.CustomerID,
		ClusterID:  req.ClusterID,
		BurstID:    req.BurstID,
		Reason:     "workload completed",
		Payload:    []byte(`{"source":"completion"}`),
		Actor:      "reaper",
		TraceID:    "trace_delete",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !requested.Inserted || requested.State != ProviderDeleteQueued || requested.DeleteID <= 0 {
		t.Fatalf("delete request = %+v", requested)
	}
	replay, err := store.RequestProviderDelete(ctx, ProviderDeleteRequest{
		CustomerID: req.CustomerID,
		ClusterID:  req.ClusterID,
		BurstID:    req.BurstID,
		Reason:     "duplicate completion",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replay.Inserted || replay.DeleteID != requested.DeleteID || replay.State != ProviderDeleteQueued {
		t.Fatalf("delete replay = %+v, want existing %+v", replay, requested)
	}

	queued, err := store.GetProviderDelete(ctx, req.CustomerID, req.ClusterID, req.BurstID)
	if err != nil {
		t.Fatal(err)
	}
	if queued.ProviderResourceID != providerResourceID || queued.Provider != req.Provider || queued.State != ProviderDeleteQueued {
		t.Fatalf("queued provider identity = %+v", queued)
	}
	assertProviderDeleteBurstStillLive(t, ctx, pool, req.BurstID, "")

	first, ok, err := store.ClaimProviderDelete(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim provider delete: op=%+v ok=%v err=%v", first, ok, err)
	}
	if first.ID != requested.DeleteID || first.State != ProviderDeleteDeleting || first.Attempts != 1 || first.LeaseToken == "" {
		t.Fatalf("first delete claim = %+v", first)
	}
	if err := store.MarkProviderDeleteFailed(ctx, first.ID, "stale-token", "provider unavailable", time.Now().Add(time.Minute), 2); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale delete lease error = %v, want ErrLeaseLost", err)
	}

	// Keep the retry pending regardless of database or runner latency. Advance
	// this isolated fixture's persisted deadline only after proving it is gated.
	retryAt := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	if err := store.MarkProviderDeleteFailed(ctx, first.ID, first.LeaseToken, "provider unavailable", retryAt, 2); err != nil {
		t.Fatal(err)
	}
	retrying, err := store.GetProviderDelete(ctx, req.CustomerID, req.ClusterID, req.BurstID)
	if err != nil {
		t.Fatal(err)
	}
	if retrying.State != ProviderDeleteRetrying || retrying.Attempts != 1 || retrying.LeaseToken != "" || retrying.DeletedAt != nil {
		t.Fatalf("retrying delete = %+v", retrying)
	}
	if !retrying.NextAttemptAt.Equal(retryAt) {
		t.Fatalf("retry time = %s, want %s", retrying.NextAttemptAt, retryAt)
	}
	assertProviderDeleteBurstStillLive(t, ctx, pool, req.BurstID, "provider unavailable")
	if _, ok, err := store.ClaimProviderDelete(ctx, time.Minute); err != nil || ok {
		t.Fatalf("delete became claimable before retry time: ok=%v err=%v", ok, err)
	}
	if tag, err := pool.Exec(ctx, `
		UPDATE lifecycle.provider_deletes
		SET next_attempt_at=now()-interval '1 second'
		WHERE id=$1 AND state='retrying'`, first.ID); err != nil {
		t.Fatal(err)
	} else if tag.RowsAffected() != 1 {
		t.Fatalf("advance retry deadline: updated %d rows, want 1", tag.RowsAffected())
	}

	second, ok, err := store.ClaimProviderDelete(ctx, time.Minute)
	if err != nil || !ok || second.ID != first.ID || second.Attempts != 2 {
		t.Fatalf("second delete claim = %+v ok=%v err=%v", second, ok, err)
	}
	if err := store.MarkProviderDeleteFailed(ctx, second.ID, second.LeaseToken, "provider refused deletion", time.Now().Add(time.Minute), 2); err != nil {
		t.Fatal(err)
	}
	attention, err := store.GetProviderDelete(ctx, req.CustomerID, req.ClusterID, req.BurstID)
	if err != nil {
		t.Fatal(err)
	}
	if attention.State != ProviderDeleteManualAttention || attention.DeletedAt != nil || attention.LeaseToken != "" {
		t.Fatalf("manual-attention delete = %+v", attention)
	}
	assertProviderDeleteBurstStillLive(t, ctx, pool, req.BurstID, "provider refused deletion")

	manual, err := store.ListProviderDeleteManualAttention(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(manual) != 1 || manual[0].ID != attention.ID || manual[0].LeaseToken != "" {
		t.Fatalf("manual-attention list = %+v", manual)
	}

	requeued, err := store.RetryProviderDelete(ctx, req.CustomerID, req.ClusterID, req.BurstID, "operator", "trace_retry")
	if err != nil {
		t.Fatal(err)
	}
	if requeued.State != ProviderDeleteQueued || requeued.Generation != 2 || requeued.Attempts != 0 || requeued.LastError != "" {
		t.Fatalf("requeued delete = %+v", requeued)
	}
	third, ok, err := store.ClaimProviderDelete(ctx, time.Minute)
	if err != nil || !ok || third.Generation != 2 || third.Attempts != 1 {
		t.Fatalf("third delete claim = %+v ok=%v err=%v", third, ok, err)
	}
	deletedAt, err := store.MarkProviderDeleteSucceeded(ctx, third.ID, third.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if deletedAt.IsZero() {
		t.Fatal("provider-delete success returned no confirmation timestamp")
	}

	terminated, err := store.GetProviderDelete(ctx, req.CustomerID, req.ClusterID, req.BurstID)
	if err != nil {
		t.Fatal(err)
	}
	if terminated.State != ProviderDeleteTerminated || terminated.DeletedAt == nil || terminated.LeaseToken != "" {
		t.Fatalf("terminated delete = %+v", terminated)
	}
	var burstState, burstLastError string
	var terminalAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT state, last_error, terminal_at
		FROM lifecycle.bursts WHERE id=$1`, req.BurstID).
		Scan(&burstState, &burstLastError, &terminalAt); err != nil {
		t.Fatal(err)
	}
	if burstState != BurstTerminated || terminalAt == nil || burstLastError != "" {
		t.Fatalf("burst after confirmed delete: state=%s last_error=%q terminal_at=%v", burstState, burstLastError, terminalAt)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE lifecycle.provider_deletes
		SET state='queued', deleted_at=NULL
		WHERE id=$1`, terminated.ID); err == nil {
		t.Fatal("terminated provider delete was allowed to regress")
	}

	// The cleanup repair committed WITH the receipt, carrying the operation's
	// own immutable payload. Everything left to do after a confirmed deletion
	// lives here, so no failure behind it can touch the delete or the burst.
	var cleanupState, cleanupPayload, cleanupAggregate string
	if err := pool.QueryRow(ctx, `
		SELECT state, payload::text, aggregate_id
		FROM lifecycle.outbox
		WHERE customer_id=$1 AND cluster_id=$2 AND event_key=$3`,
		req.CustomerID, req.ClusterID, fmt.Sprintf("provider_delete_cleanup:%d", terminated.ID)).
		Scan(&cleanupState, &cleanupPayload, &cleanupAggregate); err != nil {
		t.Fatalf("no cleanup repair was enqueued with the delete receipt: %v", err)
	}
	if cleanupState != OutboxPending {
		t.Fatalf("cleanup repair state = %q, want %q", cleanupState, OutboxPending)
	}
	if !strings.Contains(cleanupPayload, "completion") {
		t.Fatalf("cleanup repair payload = %s, want the delete's own recorded payload", cleanupPayload)
	}
	if want := fmt.Sprintf("provider_delete:%d", terminated.ID); cleanupAggregate != want {
		t.Fatalf("cleanup repair aggregate = %q, want %q", cleanupAggregate, want)
	}

	// The two consumers partition the table. A publisher must never be handed
	// this repair: it would hold the lease a confirmed-deleted node's ledger is
	// waiting on, and it has nothing to publish.
	for i := 0; i < 8; i++ {
		event, ok, err := store.ClaimOutboxEvent(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if event.EventType == ProviderDeleteCleanupEventType {
			t.Fatal("the publication consumer claimed a provider-delete cleanup repair")
		}
	}
	repair, ok, err := store.ClaimProviderDeleteCleanup(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("cleanup claim = %+v ok=%v err=%v", repair, ok, err)
	}
	if repair.EventType != ProviderDeleteCleanupEventType || repair.Attempts != 1 ||
		repair.LeaseToken == "" || repair.CreatedAt.IsZero() {
		t.Fatalf("cleanup claim = %+v", repair)
	}
	if !repair.CreatedAt.Equal(deletedAt) {
		t.Fatalf("cleanup confirmation timestamp = %v, want delete receipt %v", repair.CreatedAt, deletedAt)
	}
	if err := store.MarkOutboxFailed(ctx, repair.ID, repair.LeaseToken, "receipt not durable", time.Now().Add(time.Minute), 8); err != nil {
		t.Fatal(err)
	}
	// A cleanup failure changes nothing about the delete or the burst.
	stillTerminated, err := store.GetProviderDelete(ctx, req.CustomerID, req.ClusterID, req.BurstID)
	if err != nil {
		t.Fatal(err)
	}
	if stillTerminated.State != ProviderDeleteTerminated || stillTerminated.DeletedAt == nil {
		t.Fatalf("a cleanup failure moved the delete to %+v", stillTerminated)
	}
	if err := pool.QueryRow(ctx, `SELECT state FROM lifecycle.bursts WHERE id=$1`, req.BurstID).
		Scan(&burstState); err != nil {
		t.Fatal(err)
	}
	if burstState != BurstTerminated {
		t.Fatalf("a cleanup failure moved the burst to %q", burstState)
	}

	for _, eventType := range []string{
		"provider_delete.queued",
		"provider_delete.claimed",
		"provider_delete.retrying",
		"provider_delete.manual_attention",
		"provider_delete.requeued",
		"provider_delete.terminated",
		"burst.terminated",
	} {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM lifecycle.lifecycle_events WHERE event_type=$1`, eventType).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count < 1 {
			t.Fatalf("missing append-only lifecycle event %q", eventType)
		}
	}
}

func TestPostgresGetProviderDeleteSummaries(t *testing.T) {
	dsn := os.Getenv("LIFECYCLE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set LIFECYCLE_TEST_DATABASE_URL to an isolated Postgres database")
	}
	if os.Getenv("LIFECYCLE_TEST_ALLOW_DESTRUCTIVE") != "DROP_LIFECYCLE_SCHEMA" {
		t.Fatal("set LIFECYCLE_TEST_ALLOW_DESTRUCTIVE=DROP_LIFECYCLE_SCHEMA for the isolated test database")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var databaseName string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("LIFECYCLE_TEST_EXPECT_DATABASE"); expected == "" || expected != databaseName {
		t.Fatalf("LIFECYCLE_TEST_EXPECT_DATABASE must exactly match current database %q", databaseName)
	}
	if !strings.HasSuffix(databaseName, "_lifecycle_test") {
		t.Fatalf("refusing destructive integration test against non-test database %q", databaseName)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS lifecycle CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS lifecycle CASCADE`); err != nil {
			t.Errorf("cleanup lifecycle schema: %v", err)
		}
		pool.Close()
	})
	if err := EnsureProviderDeleteSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)

	// Admit two workloads under the same tenant and one under a different tenant.
	// testAdmission creates unique customer/cluster per suffix, so normalize to
	// share a tenant between the two same-tenant bursts.
	req1 := testAdmission("sum1")
	req2 := testAdmission("sum2")
	req2.CustomerID = req1.CustomerID
	req2.ClusterID = req1.ClusterID
	reqForeign := testAdmission("sum_foreign")
	for _, req := range []AdmissionRequest{req1, req2, reqForeign} {
		if _, err := store.AdmitWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	// Process all three provider creates so bursts reach provider_created state.
	for i := 0; i < 3; i++ {
		op, ok, err := store.ClaimProviderCreate(ctx, time.Minute)
		if err != nil || !ok {
			t.Fatalf("claim provider create %d: ok=%v err=%v", i, ok, err)
		}
		if err := store.MarkProviderCreateSucceeded(ctx, op.ID, op.LeaseToken, fmt.Sprintf("res_%s", op.BurstID)); err != nil {
			t.Fatal(err)
		}
	}

	// Request provider delete for burst 2 first, then claim and succeed it. Burst
	// 1 is enqueued afterward so its queued-state assertion cannot depend on
	// claim ordering or mutate it to retrying while looking for burst 2.
	delResp2, err := store.RequestProviderDelete(ctx, ProviderDeleteRequest{
		CustomerID: req2.CustomerID, ClusterID: req2.ClusterID, BurstID: req2.BurstID,
		Reason: "test", Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	del2, ok, err := store.ClaimProviderDelete(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatal("claim provider delete 2", err, ok)
	}
	if del2.BurstID != req2.BurstID {
		t.Fatalf("claimed burst = %q, want %q", del2.BurstID, req2.BurstID)
	}
	if del2.ID != delResp2.DeleteID {
		t.Fatalf("delete ID mismatch: got %d, want %d", del2.ID, delResp2.DeleteID)
	}
	if _, err := store.MarkProviderDeleteSucceeded(ctx, del2.ID, del2.LeaseToken); err != nil {
		t.Fatal(err)
	}

	// Burst 1 remains queued for the nonterminal summary assertion.
	if _, err := store.RequestProviderDelete(ctx, ProviderDeleteRequest{
		CustomerID: req1.CustomerID, ClusterID: req1.ClusterID, BurstID: req1.BurstID,
		Reason: "test", Payload: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}

	// Test: batch read for the tenant's own burst IDs.
	ref1 := ProviderDeleteSummaryRef{ClusterID: req1.ClusterID, BurstID: req1.BurstID}
	ref2 := ProviderDeleteSummaryRef{ClusterID: req2.ClusterID, BurstID: req2.BurstID}
	summaries, err := store.GetProviderDeleteSummaries(ctx, req1.CustomerID, []ProviderDeleteSummaryRef{ref1, ref2})
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 2 {
		t.Fatalf("summaries = %d, want 2", len(summaries))
	}
	s1 := summaries[ref1]
	if s1.State != ProviderDeleteQueued || s1.DeletedAt != nil {
		t.Fatalf("burst_1 summary = %+v, want queued with no deleted_at", s1)
	}
	s2 := summaries[ref2]
	if s2.State != ProviderDeleteTerminated || s2.DeletedAt == nil {
		t.Fatalf("burst_2 summary = %+v, want terminated with deleted_at", s2)
	}

	// Test: foreign tenant's bursts must not appear.
	summaries, err = store.GetProviderDeleteSummaries(ctx, req1.CustomerID, []ProviderDeleteSummaryRef{{ClusterID: reqForeign.ClusterID, BurstID: reqForeign.BurstID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 0 {
		t.Fatalf("foreign burst appeared in summaries: %+v", summaries)
	}

	// Test: unknown burst IDs return empty result.
	summaries, err = store.GetProviderDeleteSummaries(ctx, req1.CustomerID, []ProviderDeleteSummaryRef{{ClusterID: req1.ClusterID, BurstID: "nonexistent_burst"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 0 {
		t.Fatalf("nonexistent burst appeared in summaries: %+v", summaries)
	}

	// A real burst ID paired with the wrong cluster is not the same lifecycle
	// operation and must not match.
	summaries, err = store.GetProviderDeleteSummaries(ctx, req1.CustomerID, []ProviderDeleteSummaryRef{{ClusterID: "other_cluster", BurstID: req1.BurstID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 0 {
		t.Fatalf("wrong-cluster burst appeared in summaries: %+v", summaries)
	}

	// Test: empty input returns nil without querying.
	summaries, err = store.GetProviderDeleteSummaries(ctx, req1.CustomerID, nil)
	if err != nil || summaries != nil {
		t.Fatalf("nil input: summaries=%v err=%v", summaries, err)
	}

	// Test: duplicate burst IDs are deduplicated.
	summaries, err = store.GetProviderDeleteSummaries(ctx, req1.CustomerID, []ProviderDeleteSummaryRef{ref1, ref1})
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 {
		t.Fatalf("duplicate input: summaries = %d, want 1", len(summaries))
	}
}

func assertProviderDeleteBurstStillLive(t *testing.T, ctx context.Context, pool *pgxpool.Pool, burstID, wantError string) {
	t.Helper()
	var state, lastError string
	var terminalAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT state, last_error, terminal_at
		FROM lifecycle.bursts WHERE id=$1`, burstID).
		Scan(&state, &lastError, &terminalAt); err != nil {
		t.Fatal(err)
	}
	if state != BurstProviderCreated || terminalAt != nil || lastError != wantError {
		t.Fatalf("burst prematurely terminalized: state=%s last_error=%q terminal_at=%v", state, lastError, terminalAt)
	}
}
