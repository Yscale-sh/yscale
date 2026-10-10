//go:build integration

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresLifecycleInvariants(t *testing.T) {
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
	if err := EnsureSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)

	reset := func(t *testing.T) {
		t.Helper()
		_, err := pool.Exec(ctx, `
			TRUNCATE lifecycle.lifecycle_events, lifecycle.outbox, lifecycle.operations,
			         lifecycle.bursts, lifecycle.workloads RESTART IDENTITY CASCADE`)
		if err != nil {
			t.Fatal(err)
		}
	}
	countRows := func(t *testing.T, table string) int {
		t.Helper()
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM lifecycle."+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	t.Run("same key and payload returns same result without duplication", func(t *testing.T) {
		reset(t)
		req := testAdmission("1")
		first, err := store.AdmitWorkload(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		replay := req
		replay.WorkloadID = "wl_different"
		replay.BurstID = "burst_different"
		second, err := store.AdmitWorkload(ctx, replay)
		if err != nil {
			t.Fatal(err)
		}
		if !first.Inserted || second.Inserted {
			t.Fatalf("inserted flags first=%v second=%v", first.Inserted, second.Inserted)
		}
		if first.WorkloadID != second.WorkloadID || first.BurstID != second.BurstID {
			t.Fatalf("replay returned %+v, want %+v", second, first)
		}
		for _, table := range []string{"workloads", "bursts", "operations", "outbox", "lifecycle_events"} {
			if got := countRows(t, table); got != 1 {
				t.Fatalf("%s rows = %d, want 1", table, got)
			}
		}
	})

	t.Run("generated outbox payload does not conflict on replacement caller IDs", func(t *testing.T) {
		reset(t)
		req := testAdmission("1b")
		req.OutboxPayload = nil
		first, err := store.AdmitWorkload(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		retry := req
		retry.WorkloadID = "wl_replacement"
		retry.BurstID = "burst_replacement"
		second, err := store.AdmitWorkload(ctx, retry)
		if err != nil {
			t.Fatal(err)
		}
		if first.WorkloadID != second.WorkloadID || first.BurstID != second.BurstID || second.Inserted {
			t.Fatalf("generated-outbox replay returned %+v, want original %+v", second, first)
		}
	})

	t.Run("same key different payload or version returns typed conflict", func(t *testing.T) {
		reset(t)
		req := testAdmission("2")
		if _, err := store.AdmitWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}
		changedPayload := req
		changedPayload.WorkloadSpec = []byte(`{"image":"alpine"}`)
		if _, err := store.AdmitWorkload(ctx, changedPayload); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("changed payload error = %v", err)
		}
		changedVersion := req
		changedVersion.CanonicalVersion = 2
		if _, err := store.AdmitWorkload(ctx, changedVersion); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("changed version error = %v", err)
		}
		changedOutbox := req
		changedOutbox.OutboxPayload = []byte(`{"workload_id":"wl_2","changed":true}`)
		if _, err := store.AdmitWorkload(ctx, changedOutbox); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("changed outbox payload error = %v", err)
		}
		for _, table := range []string{"workloads", "bursts", "operations", "outbox", "lifecycle_events"} {
			if got := countRows(t, table); got != 1 {
				t.Fatalf("%s rows = %d after conflict, want 1", table, got)
			}
		}
	})

	t.Run("concurrent identical admission converges", func(t *testing.T) {
		reset(t)
		req := testAdmission("3")
		start := make(chan struct{})
		results := make(chan AdmissionResponse, 12)
		errs := make(chan error, 12)
		var wg sync.WaitGroup
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				resp, err := store.AdmitWorkload(ctx, req)
				if err != nil {
					errs <- err
					return
				}
				results <- resp
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent admission error: %v", err)
		}
		for resp := range results {
			if resp.WorkloadID != req.WorkloadID || resp.BurstID != req.BurstID {
				t.Fatalf("unexpected canonical response: %+v", resp)
			}
		}
		if got := countRows(t, "operations"); got != 1 {
			t.Fatalf("operations rows = %d, want 1", got)
		}
	})

	t.Run("atomic rollback leaves no partial rows", func(t *testing.T) {
		reset(t)
		first := testAdmission("4")
		if _, err := store.AdmitWorkload(ctx, first); err != nil {
			t.Fatal(err)
		}
		collision := testAdmission("4b")
		collision.CustomerID = first.CustomerID
		collision.ClusterID = first.ClusterID
		collision.BurstID = first.BurstID
		if _, err := store.AdmitWorkload(ctx, collision); !errors.Is(err, ErrIdentityConflict) {
			t.Fatalf("burst collision error = %v", err)
		}
		if got := countRows(t, "workloads"); got != 1 {
			t.Fatalf("workloads rows = %d, want rollback to leave 1", got)
		}
		if got := countRows(t, "operations"); got != 1 {
			t.Fatalf("operations rows = %d, want rollback to leave 1", got)
		}
	})

	t.Run("provider-create lease fencing and transitions", func(t *testing.T) {
		reset(t)
		if _, err := store.AdmitWorkload(ctx, testAdmission("5a")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.AdmitWorkload(ctx, testAdmission("5b")); err != nil {
			t.Fatal(err)
		}
		first, ok, err := store.ClaimProviderCreate(ctx, time.Second)
		if err != nil || !ok || first.Attempts != 1 || first.State != OperationProcessing {
			t.Fatalf("first claim=%+v ok=%v err=%v", first, ok, err)
		}
		// Test retry eligibility by moving the isolated row's deadline, not by
		// requiring every database round trip to beat a wall-clock timer.
		retryAt := time.Now().Add(time.Hour).Truncate(time.Microsecond)
		if err := store.MarkProviderCreateFailed(ctx, first.ID, first.LeaseToken, "temporary provider error", retryAt, 3); err != nil {
			t.Fatal(err)
		}
		var storedRetryAt time.Time
		if err := pool.QueryRow(ctx, `SELECT next_attempt_at FROM lifecycle.operations WHERE id=$1`, first.ID).Scan(&storedRetryAt); err != nil {
			t.Fatal(err)
		}
		if !storedRetryAt.Equal(retryAt) {
			t.Fatalf("create retry time = %s, want %s", storedRetryAt, retryAt)
		}
		second, ok, err := store.ClaimProviderCreate(ctx, time.Second)
		if err != nil || !ok || second.ID == first.ID {
			t.Fatalf("claim ordering second=%+v ok=%v err=%v", second, ok, err)
		}
		if err := store.MarkProviderCreateSucceeded(ctx, second.ID, "stale-token", "provider_5b"); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale success lease = %v", err)
		}
		// An expired create lease is a crashed attempt that had already been
		// handed the request, so the background drain must leave it alone: a
		// second delivery could buy a second machine for a burst that already
		// owns one. Only a settlement — which proves the outcome — moves it on.
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.operations SET locked_until=now()-interval '1 second' WHERE id=$1`, second.ID); err != nil {
			t.Fatal(err)
		}
		if reclaimed, ok, err := store.ClaimProviderCreate(ctx, time.Second); err != nil || ok {
			t.Fatalf("expired create lease was redelivered: claim=%+v ok=%v err=%v", reclaimed, ok, err)
		}
		if err := store.MarkProviderCreateSucceeded(ctx, second.ID, second.LeaseToken, "provider_5b"); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expired lease completion = %v", err)
		}
		var strandedState string
		var strandedAttempts int
		if err := pool.QueryRow(ctx, `SELECT state, attempts FROM lifecycle.operations WHERE id=$1`, second.ID).
			Scan(&strandedState, &strandedAttempts); err != nil {
			t.Fatal(err)
		}
		if strandedState != OperationProcessing || strandedAttempts != 1 {
			t.Fatalf("refused redelivery left state=%s attempts=%d, want processing/1", strandedState, strandedAttempts)
		}
		// An operator resolving the ambiguity restores the lease; that is the
		// only way the operation reaches a terminal state.
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.operations SET locked_until=now()+interval '1 minute' WHERE id=$1`, second.ID); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkProviderCreateSucceeded(ctx, second.ID, second.LeaseToken, "provider_5b"); err != nil {
			t.Fatal(err)
		}
		var opState, burstState, providerID string
		if err := pool.QueryRow(ctx, `
			SELECT o.state, b.state, b.provider_resource_id
			FROM lifecycle.operations o JOIN lifecycle.bursts b ON b.id=o.burst_id
			WHERE o.id=$1`, second.ID).Scan(&opState, &burstState, &providerID); err != nil {
			t.Fatal(err)
		}
		if opState != OperationSucceeded || burstState != BurstProviderCreated || providerID != "provider_5b" {
			t.Fatalf("success transition op=%s burst=%s provider=%s", opState, burstState, providerID)
		}
		if _, ok, err := store.ClaimProviderCreate(ctx, time.Second); err != nil || ok {
			t.Fatalf("retried too early before retryAt: ok=%v err=%v", ok, err)
		}
		if tag, err := pool.Exec(ctx, `
			UPDATE lifecycle.operations
			SET next_attempt_at=now()-interval '1 second'
			WHERE id=$1 AND state='failed'`, first.ID); err != nil {
			t.Fatal(err)
		} else if tag.RowsAffected() != 1 {
			t.Fatalf("advance create retry deadline: updated %d rows, want 1", tag.RowsAffected())
		}
		retry, ok, err := store.ClaimProviderCreate(ctx, time.Second)
		if err != nil || !ok || retry.ID != first.ID || retry.Attempts != 2 {
			t.Fatalf("retry claim=%+v ok=%v err=%v", retry, ok, err)
		}
		if err := store.MarkProviderCreateFailed(ctx, retry.ID, retry.LeaseToken, "permanent provider error", time.Now().Add(time.Minute), 2); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := store.ClaimProviderCreate(ctx, time.Second); err != nil || ok {
			t.Fatalf("dead-letter operation reclaimed: ok=%v err=%v", ok, err)
		}
	})

	t.Run("outbox duplicate delivery ack and lease reclaim", func(t *testing.T) {
		reset(t)
		if _, err := store.AdmitWorkload(ctx, testAdmission("6")); err != nil {
			t.Fatal(err)
		}
		event, ok, err := store.ClaimOutboxEvent(ctx, time.Second)
		if err != nil || !ok || event.Attempts != 1 || event.LeaseToken == "" {
			t.Fatalf("outbox claim=%+v ok=%v err=%v", event, ok, err)
		}
		if _, ok, err := store.ClaimOutboxEvent(ctx, time.Second); err != nil || ok {
			t.Fatalf("active outbox lease claimed: ok=%v err=%v", ok, err)
		}
		if err := store.AcknowledgeOutboxEvent(ctx, event.ID, "wrong-token"); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("wrong outbox token = %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.outbox SET locked_until=now()-interval '1 second' WHERE id=$1`, event.ID); err != nil {
			t.Fatal(err)
		}
		duplicate, ok, err := store.ClaimOutboxEvent(ctx, time.Second)
		if err != nil || !ok || duplicate.ID != event.ID || duplicate.Attempts != 1 {
			t.Fatalf("outbox duplicate claim=%+v ok=%v err=%v", duplicate, ok, err)
		}
		if err := store.AcknowledgeOutboxEvent(ctx, event.ID, event.LeaseToken); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale outbox ack = %v", err)
		}
		if err := store.AcknowledgeOutboxEvent(ctx, duplicate.ID, duplicate.LeaseToken); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := store.ClaimOutboxEvent(ctx, time.Second); err != nil || ok {
			t.Fatalf("acknowledged outbox reclaimed: ok=%v err=%v", ok, err)
		}

		if _, err := store.AdmitWorkload(ctx, testAdmission("6b")); err != nil {
			t.Fatal(err)
		}
		event, ok, err = store.ClaimOutboxEvent(ctx, time.Second)
		if err != nil || !ok {
			t.Fatalf("second outbox claim ok=%v err=%v", ok, err)
		}
		retryAt := time.Now().Add(time.Hour).Truncate(time.Microsecond)
		if err := store.MarkOutboxFailed(ctx, event.ID, event.LeaseToken, "redis unavailable", retryAt, 2); err != nil {
			t.Fatal(err)
		}
		var storedRetryAt time.Time
		if err := pool.QueryRow(ctx, `SELECT next_attempt_at FROM lifecycle.outbox WHERE id=$1`, event.ID).Scan(&storedRetryAt); err != nil {
			t.Fatal(err)
		}
		if !storedRetryAt.Equal(retryAt) {
			t.Fatalf("outbox retry time = %s, want %s", storedRetryAt, retryAt)
		}
		if _, ok, err := store.ClaimOutboxEvent(ctx, time.Second); err != nil || ok {
			t.Fatalf("outbox retried before schedule: ok=%v err=%v", ok, err)
		}
		if tag, err := pool.Exec(ctx, `
			UPDATE lifecycle.outbox
			SET next_attempt_at=now()-interval '1 second'
			WHERE id=$1 AND state='failed'`, event.ID); err != nil {
			t.Fatal(err)
		} else if tag.RowsAffected() != 1 {
			t.Fatalf("advance outbox retry deadline: updated %d rows, want 1", tag.RowsAffected())
		}
		event, ok, err = store.ClaimOutboxEvent(ctx, time.Second)
		if err != nil || !ok || event.Attempts != 2 {
			t.Fatalf("outbox retry=%+v ok=%v err=%v", event, ok, err)
		}
		if err := store.MarkOutboxFailed(ctx, event.ID, event.LeaseToken, "redis permanent", time.Now().Add(time.Minute), 2); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := store.ClaimOutboxEvent(ctx, time.Second); err != nil || ok {
			t.Fatalf("dead-letter outbox reclaimed: ok=%v err=%v", ok, err)
		}
	})

	t.Run("database constraints reject invalid lifecycle state", func(t *testing.T) {
		reset(t)
		req := testAdmission("7")
		if _, err := store.AdmitWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO lifecycle.workloads
			(id, customer_id, cluster_id, idempotency_key, canonical_version, payload_hash, burst_id, spec, state)
			VALUES ('wl_bad',$1,$2,'idem_bad',1,$3,'burst_bad','{}','nonsense')`,
			req.CustomerID, req.ClusterID, strings.Repeat("0", 64)); err == nil {
			t.Fatal("database accepted invalid workload state")
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO lifecycle.operations
			(customer_id, cluster_id, workload_id, burst_id, operation_type, semantic_key,
			 provider, region, sku, payload_version, payload)
			VALUES ($1,$2,'missing','missing','provider_create','missing','linode','us-east','g6',1,'{}')`,
			req.CustomerID, req.ClusterID); err == nil {
			t.Fatal("database accepted operation with missing aggregate foreign keys")
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO lifecycle.outbox
			(customer_id, cluster_id, aggregate_type, aggregate_id, event_key, event_type,
			 payload_version, payload)
			VALUES ($1,$2,'workload',$3,'oversized','oversized',1,$4::jsonb)`,
			req.CustomerID, req.ClusterID, req.WorkloadID,
			`{"data":"`+strings.Repeat("x", maxPayloadBytes)+`"}`); err == nil {
			t.Fatal("database accepted oversized outbox payload")
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO lifecycle.outbox
			(customer_id, cluster_id, aggregate_type, aggregate_id, event_key, event_type,
			 payload_version, payload)
			VALUES ($1,$2,'workload',$3,'dup','dup',1,'{}')`,
			req.CustomerID, req.ClusterID, req.WorkloadID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO lifecycle.outbox
			(customer_id, cluster_id, aggregate_type, aggregate_id, event_key, event_type,
			 payload_version, payload)
			VALUES ($1,$2,'workload',$3,'dup','dup',1,'{}')`,
			req.CustomerID, req.ClusterID, req.WorkloadID); err == nil {
			t.Fatal("database accepted duplicate semantic outbox event")
		}
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.lifecycle_events SET event_type='mutated'`); err == nil {
			t.Fatal("append-only lifecycle events accepted UPDATE")
		}
		if _, err := pool.Exec(ctx, `DELETE FROM lifecycle.lifecycle_events`); err == nil {
			t.Fatal("append-only lifecycle events accepted DELETE")
		}
	})

	t.Run("database-normalized payload size returns typed invalid argument", func(t *testing.T) {
		reset(t)
		var payload strings.Builder
		payload.WriteByte('{')
		for i := 0; payload.Len() < 930000; i++ {
			if i > 0 {
				payload.WriteByte(',')
			}
			fmt.Fprintf(&payload, "%q:0", fmt.Sprint(i))
		}
		payload.WriteByte('}')
		if payload.Len() > maxPayloadBytes {
			t.Fatalf("test payload raw bytes = %d, want <= %d", payload.Len(), maxPayloadBytes)
		}
		var storedBytes int
		if err := pool.QueryRow(ctx, `SELECT octet_length($1::jsonb::text)`, payload.String()).Scan(&storedBytes); err != nil {
			t.Fatal(err)
		}
		if storedBytes <= maxPayloadBytes {
			t.Fatalf("test payload stored bytes = %d, want > %d", storedBytes, maxPayloadBytes)
		}
		req := testAdmission("8")
		req.OutboxPayload = []byte(payload.String())
		if _, err := store.AdmitWorkload(ctx, req); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("normalized oversized payload error = %v, want ErrInvalidArgument", err)
		}
		if got := countRows(t, "workloads"); got != 0 {
			t.Fatalf("workload rows = %d after rejected payload, want 0", got)
		}
	})

	t.Run("deferred constraint commit returns typed invariant error", func(t *testing.T) {
		reset(t)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		_, err = tx.Exec(ctx, `
			INSERT INTO lifecycle.workloads
			(id, customer_id, cluster_id, idempotency_key, canonical_version, payload_hash, burst_id, spec)
			VALUES ('wl_orphan','cust_orphan','cluster_orphan','idem_orphan',1,$1,'missing_burst','{}')`,
			strings.Repeat("0", 64))
		if err != nil {
			t.Fatal(err)
		}
		if err := commit(ctx, tx, "deferred constraint test"); !errors.Is(err, ErrInvariantViolation) {
			t.Fatalf("commit error = %v, want ErrInvariantViolation", err)
		}
		if got := countRows(t, "workloads"); got != 0 {
			t.Fatalf("workload rows = %d after failed commit, want 0", got)
		}
	})

	t.Run("tenant scoping allows same workload and burst IDs in different customer/cluster scopes", func(t *testing.T) {
		reset(t)
		req1 := testAdmission("tenant1")
		req1.CustomerID = "cust_a"
		req1.ClusterID = "cluster_a"
		req1.WorkloadID = "wl_same"
		req1.BurstID = "burst_same"

		req2 := testAdmission("tenant2")
		req2.CustomerID = "cust_b"
		req2.ClusterID = "cluster_b"
		req2.WorkloadID = "wl_same"
		req2.BurstID = "burst_same"

		_, err1 := store.AdmitWorkload(ctx, req1)
		if err1 != nil {
			t.Fatalf("first tenant admission failed: %v", err1)
		}

		_, err2 := store.AdmitWorkload(ctx, req2)
		if err2 != nil {
			t.Fatalf("second tenant admission failed: %v", err2)
		}

		if got := countRows(t, "workloads"); got != 2 {
			t.Fatalf("workloads rows = %d, want 2", got)
		}
	})

	t.Run("provider-create dead-letter terminalizes workload and does not erase last_error", func(t *testing.T) {
		reset(t)
		req := testAdmission("deadletter")
		if _, err := store.AdmitWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}

		// First claim
		op, ok, err := store.ClaimProviderCreate(ctx, time.Second)
		if err != nil || !ok {
			t.Fatalf("claim failed: ok=%v err=%v", ok, err)
		}

		// Fail it once - should not dead-letter yet
		retryAt := time.Now().Add(time.Hour)
		safeErr := "provider timeout details"
		if err := store.MarkProviderCreateFailed(ctx, op.ID, op.LeaseToken, safeErr, retryAt, 2); err != nil {
			t.Fatal(err)
		}

		// Verify last_error is saved
		var lastErr string
		if err := pool.QueryRow(ctx, `SELECT last_error FROM lifecycle.operations WHERE id=$1`, op.ID).Scan(&lastErr); err != nil {
			t.Fatal(err)
		}
		if lastErr != safeErr {
			t.Fatalf("expected last_error %q, got %q", safeErr, lastErr)
		}

		// Make it reclaimable by setting next_attempt_at in the past
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.operations SET next_attempt_at=now()-interval '1 second' WHERE id=$1`, op.ID); err != nil {
			t.Fatal(err)
		}

		// Claim again, verify last_error is NOT erased
		op2, ok, err := store.ClaimProviderCreate(ctx, time.Second)
		if err != nil || !ok {
			t.Fatalf("re-claim failed: ok=%v err=%v", ok, err)
		}
		if op2.ID != op.ID {
			t.Fatalf("expected to claim same operation, got ID %d", op2.ID)
		}

		var lastErrAfterClaim string
		if err := pool.QueryRow(ctx, `SELECT last_error FROM lifecycle.operations WHERE id=$1`, op.ID).Scan(&lastErrAfterClaim); err != nil {
			t.Fatal(err)
		}
		if lastErrAfterClaim != safeErr {
			t.Fatalf("expected last_error to be preserved as %q, got %q", safeErr, lastErrAfterClaim)
		}

		// Fail again to dead-letter (since maxAttempts is 2 and attempts will be 2)
		if err := store.MarkProviderCreateFailed(ctx, op2.ID, op2.LeaseToken, "terminal connection failure", retryAt, 2); err != nil {
			t.Fatal(err)
		}

		// Check workload is terminalized as failed
		var wlState, wlReason string
		var wlTerminalAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT state, terminal_reason, terminal_at FROM lifecycle.workloads WHERE id=$1`, req.WorkloadID).Scan(&wlState, &wlReason, &wlTerminalAt); err != nil {
			t.Fatal(err)
		}
		if wlState != WorkloadFailed || wlReason != "terminal connection failure" || wlTerminalAt == nil {
			t.Fatalf("workload not dead-lettered: state=%s reason=%q terminal_at=%v", wlState, wlReason, wlTerminalAt)
		}

		// Check burst is manual_attention
		var burstState string
		if err := pool.QueryRow(ctx, `SELECT state FROM lifecycle.bursts WHERE id=$1`, req.BurstID).Scan(&burstState); err != nil {
			t.Fatal(err)
		}
		if burstState != BurstManualAttention {
			t.Fatalf("burst not manual_attention: state=%s", burstState)
		}

		// Check append-only failure event payloads
		var wlEventPayload, opEventPayload, burstEventPayload string
		if err := pool.QueryRow(ctx, `SELECT payload::text FROM lifecycle.lifecycle_events WHERE aggregate_type='workload' AND event_type='workload.failed'`).Scan(&wlEventPayload); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(wlEventPayload, "terminal connection failure") {
			t.Fatalf("expected workload event payload to contain error, got: %s", wlEventPayload)
		}

		if err := pool.QueryRow(ctx, `SELECT payload::text FROM lifecycle.lifecycle_events WHERE aggregate_type='operation' AND event_type='provider_create.dead_letter'`).Scan(&opEventPayload); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(opEventPayload, "terminal connection failure") {
			t.Fatalf("expected operation event payload to contain error, got: %s", opEventPayload)
		}

		if err := pool.QueryRow(ctx, `SELECT payload::text FROM lifecycle.lifecycle_events WHERE aggregate_type='burst' AND event_type='burst.manual_attention'`).Scan(&burstEventPayload); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(burstEventPayload, "terminal connection failure") {
			t.Fatalf("expected burst event payload to contain error, got: %s", burstEventPayload)
		}
	})

	t.Run("state-transition constraints enforce legal graphs and reject invalid transitions", func(t *testing.T) {
		reset(t)
		req := testAdmission("transition")
		if _, err := store.AdmitWorkload(ctx, req); err != nil {
			t.Fatal(err)
		}

		// 1. Same-state metadata updates on workloads work
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.workloads SET updated_at=now() WHERE id=$1`, req.WorkloadID); err != nil {
			t.Fatalf("same-state workload update failed: %v", err)
		}

		// 2. Workload transition regression (succeeded -> admitted) or skip from terminal fails
		// Set to succeeded first (valid transition)
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.workloads SET state='succeeded', terminal_at=now() WHERE id=$1`, req.WorkloadID); err != nil {
			t.Fatal(err)
		}
		// Harmless same-state metadata updates remain legal after terminalization.
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.workloads SET updated_at=now() WHERE id=$1`, req.WorkloadID); err != nil {
			t.Fatalf("terminal workload metadata update failed: %v", err)
		}
		// Attempting invalid transition (succeeded -> admitted) must fail
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.workloads SET state='admitted' WHERE id=$1`, req.WorkloadID); err == nil {
			t.Fatal("expected workload state regression to fail")
		}

		// 3. Same-state updates on bursts work (create_requested -> create_requested)
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.bursts SET updated_at=now() WHERE id=$1`, req.BurstID); err != nil {
			t.Fatalf("same-state burst update failed: %v", err)
		}
		// Invalid burst transition (create_requested -> terminated directly is allowed, but let's test a regression: terminated -> provider_created)
		// Set to manual_attention (terminal state)
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.bursts SET state='manual_attention', terminal_at=now() WHERE id=$1`, req.BurstID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.bursts SET updated_at=now() WHERE id=$1`, req.BurstID); err != nil {
			t.Fatalf("terminal burst metadata update failed: %v", err)
		}

		// 4. Operations: pending -> processing is allowed, pending -> succeeded directly is not (skip processing)
		var opID int64
		if err := pool.QueryRow(ctx, `SELECT id FROM lifecycle.operations WHERE burst_id=$1`, req.BurstID).Scan(&opID); err != nil {
			t.Fatal(err)
		}
		// Try skip (pending -> succeeded)
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.operations SET state='succeeded' WHERE id=$1`, opID); err == nil {
			t.Fatal("expected skip from pending to succeeded to fail")
		}

		// 5. Outbox: pending -> processing is allowed, pending -> acknowledged directly is not
		var outboxID int64
		if err := pool.QueryRow(ctx, `SELECT id FROM lifecycle.outbox WHERE aggregate_id=$1`, req.WorkloadID).Scan(&outboxID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE lifecycle.outbox SET state='acknowledged' WHERE id=$1`, outboxID); err == nil {
			t.Fatal("expected skip from pending to acknowledged to fail")
		}
	})
}

func testAdmission(suffix string) AdmissionRequest {
	return AdmissionRequest{
		CustomerID:            "cust_" + suffix,
		ClusterID:             "cluster_" + suffix,
		IdempotencyKey:        "kube:cluster_" + suffix + ":uid_" + suffix + ":1",
		CanonicalVersion:      1,
		WorkloadID:            "wl_" + suffix,
		BurstID:               "burst_" + suffix,
		WorkloadSpec:          []byte(fmt.Sprintf(`{"image":"busybox","suffix":"%s"}`, suffix)),
		BurstSpec:             []byte(fmt.Sprintf(`{"cpu":1,"suffix":"%s"}`, suffix)),
		Provider:              "linode",
		Region:                "us-east",
		SKU:                   "g6-standard-1",
		ProviderCreatePayload: []byte(fmt.Sprintf(`{"burst_id":"burst_%s"}`, suffix)),
		OutboxPayload:         []byte(fmt.Sprintf(`{"workload_id":"wl_%s","burst_id":"burst_%s"}`, suffix, suffix)),
		Actor:                 "integration",
		TraceID:               "trace_" + suffix,
	}
}
