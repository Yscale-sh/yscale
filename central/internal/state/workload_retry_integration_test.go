//go:build integration

package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func retryFixture(t *testing.T) (*Store, *pgPersister, map[string]string) {
	t.Helper()
	s, p, ids := membershipWriteFixture(t)
	if _, _, err := s.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleMember, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	c, err := s.CustomerByID(membershipWriteTenant)
	if err != nil {
		t.Fatal(err)
	}
	c.Plan, c.MaxConcurrentBursts, c.MaxHourlyUSD = "pro", 2, 2
	s.AddCustomer(c)
	actor := HumanActor(ids["target"], membershipWriteTenant)
	finished := time.Now().UTC().Add(-time.Minute)
	if err := s.PutWorkloadDurable(&Workload{ID: "wl_retry_source", CustomerID: membershipWriteTenant, ClusterID: "cluster", Status: "succeeded", SubmittedBy: &actor, SpecYAML: []byte("synthetic retry spec"), FinishedAt: &finished}); err != nil {
		t.Fatal(err)
	}
	return s, p, ids
}

func retryPreparation(t *testing.T, s *Store, account string) WorkloadRetryPreparation {
	t.Helper()
	result, err := s.PrepareWorkloadRetry(context.Background(), membershipWriteTenant, account, "wl_retry_source")
	if err != nil || !result.Decision.Allowed || result.Approval == nil || result.Audit != nil {
		t.Fatalf("prepare allowed retry: allowed=%v, approval=%v, audit=%v, err=%v", result.Decision.Allowed, result.Approval != nil, result.Audit != nil, err)
	}
	return result
}

func retryCounts(t *testing.T, p *pgPersister) (int, int, int) {
	t.Helper()
	var reservations, accepted, denied int
	if err := p.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM admission_reservations),
		(SELECT count(*) FROM tenant_audit WHERE data->>'Action'='workload.retry' AND data->>'Outcome'='accepted'),
		(SELECT count(*) FROM tenant_audit WHERE data->>'Action'='workload.retry' AND data->>'Outcome'='denied')`).Scan(&reservations, &accepted, &denied); err != nil {
		t.Fatal(err)
	}
	return reservations, accepted, denied
}

func TestPostgresWorkloadRetryAuditAndReservation(t *testing.T) {
	a, p, ids := retryFixture(t)
	b := membershipWriteReplica(t, p)
	prepared := retryPreparation(t, b, ids["target"])
	if r, accepted, denied := retryCounts(t, p); r != 0 || accepted != 0 || denied != 0 {
		t.Fatal("preparation emitted an accepted decision or reserved capacity")
	}
	// Settling a source receipt must not invalidate the next run's unchanged
	// authorization/specification. Cost is not a retry input.
	if _, err := p.pool.Exec(context.Background(), `UPDATE workloads SET data=data || '{"Cost":{"EstimatedUSD":1.25}}'::jsonb WHERE id='wl_retry_source'`); err != nil {
		t.Fatal(err)
	}
	settled, err := a.WorkloadSnapshotForCustomer(context.Background(), membershipWriteTenant, "wl_retry_source")
	if err != nil || settled.Workload.Cost == nil || settled.Workload.Cost.EstimatedUSD != 1.25 {
		t.Fatalf("source cost fixture was not applied: %v", err)
	}
	id, audit, err := b.ReserveWorkloadRetry(context.Background(), prepared.Approval, "wl_retry_new", 1_000_000)
	if err != nil || id == "" || audit == nil || audit.Outcome != OutcomeAccepted || audit.TargetID != "wl_retry_source" || audit.Detail.RetryWorkloadID != "wl_retry_new" || audit.Actor.AccountID != ids["target"] {
		t.Fatalf("retry reservation lost its accepted decision: id=%s, audit=%v, err=%v", id, audit != nil, err)
	}
	var linked bool
	if err := p.pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM admission_reservations r JOIN tenant_audit a ON a.data->'Detail'->>'retry_workload_id'=r.workload_id WHERE r.id=$1 AND a.data->>'TargetID'='wl_retry_source' AND a.data->>'Outcome'='accepted')`, id).Scan(&linked); err != nil || !linked {
		t.Fatalf("durable reservation/audit link missing: %v", err)
	}
	// The same internal attempt can reserve again without counting a new slot.
	again, _, err := a.ReserveWorkloadRetry(context.Background(), prepared.Approval, "wl_retry_new", 1_000_000)
	if err != nil || again != id {
		t.Fatalf("same reservation changed: %s, %v", again, err)
	}
	if _, audit, err := a.ReserveWorkloadRetry(context.Background(), prepared.Approval, "wl_retry_new", 2_000_000); !errors.Is(err, ErrAdmissionConflictingRate) || audit != nil {
		t.Fatalf("conflicting rate=%v, audit=%v", err, audit != nil)
	}
	if r, accepted, denied := retryCounts(t, p); r != 1 || accepted != 2 || denied != 0 {
		t.Fatalf("replay accounting: reservations=%d accepted=%d denied=%d", r, accepted, denied)
	}
	if err := a.ReleaseAdmission(context.Background(), id); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresWorkloadRetryPreparationDenials(t *testing.T) {
	for _, scenario := range []string{"viewer", "other-submitter", "nonterminal", "foreign", "deleted", "revoked", "corrupt-source", "corrupt-customer"} {
		t.Run(scenario, func(t *testing.T) {
			a, p, ids := retryFixture(t)
			b := membershipWriteReplica(t, p)
			var want error
			wantReason := ReasonWorkloadNotTerminal
			switch scenario {
			case "viewer":
				if _, _, err := a.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleViewer, OperatorActor()); err != nil {
					t.Fatal(err)
				}
				wantReason = ReasonRoleReadOnly
			case "other-submitter":
				w, _ := a.GetWorkload("wl_retry_source")
				actor := HumanActor(ids["owner"], membershipWriteTenant)
				w.SubmittedBy = &actor
				if err := a.PutWorkloadDurable(w); err != nil {
					t.Fatal(err)
				}
				wantReason = ReasonNotSubmitter
			case "nonterminal":
				if _, err := p.pool.Exec(context.Background(), `UPDATE workloads SET data=jsonb_set(data,'{FinishedAt}','null') WHERE id='wl_retry_source'`); err != nil {
					t.Fatal(err)
				}
			case "foreign":
				if _, err := p.pool.Exec(context.Background(), `UPDATE workloads SET data=jsonb_set(data,'{CustomerID}','"foreign"') WHERE id='wl_retry_source'`); err != nil {
					t.Fatal(err)
				}
				want = ErrNotFound
			case "deleted":
				if _, err := p.pool.Exec(context.Background(), `DELETE FROM workloads WHERE id='wl_retry_source'`); err != nil {
					t.Fatal(err)
				}
				want = ErrNotFound
			case "revoked":
				if err := a.RevokeCustomer(membershipWriteTenant); err != nil {
					t.Fatal(err)
				}
				want = ErrNotFound
			case "corrupt-source", "corrupt-customer":
				query := `UPDATE workloads SET data='null' WHERE id='wl_retry_source'`
				if scenario == "corrupt-customer" {
					query = `UPDATE customers SET data='null' WHERE id='synthetic-membership-tenant'`
				}
				if _, err := p.pool.Exec(context.Background(), query); err != nil {
					t.Fatal(err)
				}
				// A null workload has no tenant binding and is indistinguishable
				// from a foreign source; a corrupt tenant is a storage failure.
				want = ErrNotFound
				if scenario == "corrupt-customer" {
					want = ErrPersistence
				}
			}
			result, err := b.PrepareWorkloadRetry(context.Background(), membershipWriteTenant, ids["target"], "wl_retry_source")
			if !errors.Is(err, want) || result.Decision.Allowed || result.Approval != nil {
				t.Fatalf("denial allowed=%v err=%v, want=%v", result.Decision.Allowed, err, want)
			}
			if want == nil && (result.Audit == nil || result.Audit.Outcome != OutcomeDenied || result.Decision.Reason != wantReason) {
				t.Fatalf("denial lost its reason: %s", result.Decision.Reason)
			}
			r, accepted, denied := retryCounts(t, p)
			wantDenied := 0
			if want == nil {
				wantDenied = 1
			}
			if r != 0 || accepted != 0 || denied != wantDenied {
				t.Fatalf("denial state: %d/%d/%d", r, accepted, denied)
			}
		})
	}
}

func TestPostgresWorkloadRetryRollsBackTogether(t *testing.T) {
	for _, failure := range []string{"audit-accepted", "audit-denied", "reservation", "database"} {
		t.Run(failure, func(t *testing.T) {
			a, p, ids := retryFixture(t)
			prepared := retryPreparation(t, a, ids["target"])
			if failure == "audit-denied" {
				if _, _, err := a.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleViewer, OperatorActor()); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "database" {
				p.pool.Close()
			} else {
				query := `ALTER TABLE tenant_audit ADD CONSTRAINT refuse_retry CHECK (data->>'Action' <> 'workload.retry')`
				if failure == "reservation" {
					query = `ALTER TABLE admission_reservations ADD CONSTRAINT refuse_retry CHECK (workload_id <> 'wl_retry_new')`
				}
				if _, err := p.pool.Exec(context.Background(), query); err != nil {
					t.Fatal(err)
				}
			}
			id, audit, err := a.ReserveWorkloadRetry(context.Background(), prepared.Approval, "wl_retry_new", 1_000_000)
			if !errors.Is(err, ErrPersistence) || id != "" || audit != nil {
				t.Fatalf("failed write: id=%s audit=%v err=%v", id, audit != nil, err)
			}
			if failure != "database" {
				if r, accepted, denied := retryCounts(t, p); r != 0 || accepted != 0 || denied != 0 {
					t.Fatalf("partial commit: %d/%d/%d", r, accepted, denied)
				}
			}
		})
	}
}

func TestPostgresWorkloadRetryPreparationAuditFailure(t *testing.T) {
	a, p, ids := retryFixture(t)
	if _, _, err := a.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleViewer, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.pool.Exec(context.Background(), `ALTER TABLE tenant_audit ADD CONSTRAINT refuse_retry CHECK (data->>'Action' <> 'workload.retry')`); err != nil {
		t.Fatal(err)
	}
	result, err := a.PrepareWorkloadRetry(context.Background(), membershipWriteTenant, ids["target"], "wl_retry_source")
	if !errors.Is(err, ErrPersistence) || result.Audit != nil || result.Approval != nil {
		t.Fatalf("unrecorded preparation denial returned as committed: %v", err)
	}
	if r, accepted, denied := retryCounts(t, p); r != 0 || accepted != 0 || denied != 0 {
		t.Fatalf("failed preparation committed: %d/%d/%d", r, accepted, denied)
	}
}

func TestPostgresWorkloadRetryRechecksAfterLockWait(t *testing.T) {
	for _, change := range []string{"role", "source", "policy", "removed", "revoked"} {
		t.Run(change, func(t *testing.T) {
			a, p, ids := retryFixture(t)
			prepared := retryPreparation(t, a, ids["target"])
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			tx, err := p.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background()) //nolint:errcheck
			lockQuery, waitQuery := `SELECT id FROM workloads WHERE id='wl_retry_source' FOR UPDATE`, "%workloads%FOR UPDATE%"
			if change == "policy" {
				lockQuery, waitQuery = `SELECT id FROM customers WHERE id='synthetic-membership-tenant' FOR UPDATE`, "%customers%FOR UPDATE%"
			}
			if change == "role" || change == "removed" || change == "revoked" {
				if err := lockMembershipTenant(ctx, tx, membershipWriteTenant); err != nil {
					t.Fatal(err)
				}
				waitQuery = "%pg_advisory_xact_lock($1,$2)%"
			} else if _, err := tx.Exec(ctx, lockQuery); err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				id    string
				audit *AuditEvent
				err   error
			}
			done := make(chan outcome, 1)
			go func() {
				id, audit, err := a.ReserveWorkloadRetry(ctx, prepared.Approval, "wl_retry_waiting", 1_000_000)
				done <- outcome{id, audit, err}
			}()
			waitCancellationSQLLock(t, p, waitQuery)
			query := `UPDATE workloads SET data=jsonb_set(data,'{SpecYAML}','"Y2hhbmdlZA=="') WHERE id='wl_retry_source'`
			args := []any{}
			want := ErrWorkloadRetryChanged
			switch change {
			case "role":
				query, args, want = `UPDATE tenant_memberships SET data=jsonb_set(data,'{Role}','"viewer"') WHERE id=$1`, []any{membershipID(ids["target"], membershipWriteTenant)}, ErrWorkloadRetryForbidden
			case "policy":
				query = `UPDATE customers SET data=jsonb_set(data,'{MaxHourlyUSD}','0.5') WHERE id='synthetic-membership-tenant'`
			case "removed":
				query, args, want = `DELETE FROM tenant_memberships WHERE id=$1`, []any{membershipID(ids["target"], membershipWriteTenant)}, ErrNotFound
			case "revoked":
				query, want = `UPDATE customers SET data=jsonb_set(data,'{RevokedAt}',to_jsonb(now())) WHERE id='synthetic-membership-tenant'`, ErrNotFound
			}
			if _, err := tx.Exec(ctx, query, args...); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			got := <-done
			if !errors.Is(got.err, want) || got.id != "" {
				t.Fatalf("waiting retry admitted old state: %s %v", got.id, got.err)
			}
			r, accepted, denied := retryCounts(t, p)
			wantDenied := 1
			if errors.Is(want, ErrNotFound) {
				wantDenied = 0
			}
			if r != 0 || accepted != 0 || denied != wantDenied {
				t.Fatalf("waiting refusal not atomic: %d/%d/%d", r, accepted, denied)
			}
		})
	}
}

func TestPostgresWorkloadRetryBoundedLockWait(t *testing.T) {
	for _, phase := range []string{"prepare", "reserve"} {
		t.Run(phase, func(t *testing.T) {
			a, p, ids := retryFixture(t)
			prepared := retryPreparation(t, a, ids["target"])
			tx, err := p.pool.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background()) //nolint:errcheck
			if err := lockMembershipTenant(context.Background(), tx, membershipWriteTenant); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if phase == "prepare" {
				_, err = a.PrepareWorkloadRetry(ctx, membershipWriteTenant, ids["target"], "wl_retry_source")
			} else {
				_, _, err = a.ReserveWorkloadRetry(ctx, prepared.Approval, "wl_retry_waiting", 1_000_000)
			}
			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrPersistence) {
				t.Fatalf("unbounded or obscured timeout: %v", err)
			}
			if err := tx.Rollback(context.Background()); err != nil {
				t.Fatal(err)
			}
			if r, accepted, denied := retryCounts(t, p); r != 0 || accepted != 0 || denied != 0 {
				t.Fatalf("timed out request committed: %d/%d/%d", r, accepted, denied)
			}
		})
	}
}

func TestPostgresWorkloadRetryConcurrentAdmission(t *testing.T) {
	a, p, ids := retryFixture(t)
	const attempts = 8
	var group sync.WaitGroup
	errorsSeen := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		b := membershipWriteReplica(t, p)
		prepared := retryPreparation(t, b, ids["target"])
		group.Add(1)
		go func(i int) {
			defer group.Done()
			_, _, err := b.ReserveWorkloadRetry(context.Background(), prepared.Approval, fmt.Sprintf("wl_retry_%d", i), 1_000_000)
			errorsSeen <- err
		}(i)
	}
	group.Wait()
	close(errorsSeen)
	accepted := 0
	for err := range errorsSeen {
		if err == nil {
			accepted++
		} else if !errors.Is(err, ErrAdmissionSlotLimitReached) {
			t.Errorf("unexpected refusal: %v", err)
		}
	}
	if r, audits, denied := retryCounts(t, p); accepted != 2 || r != 2 || audits != 2 || denied != 0 {
		t.Fatalf("concurrent cap/audit: accepted=%d, durable=%d/%d/%d", accepted, r, audits, denied)
	}
	// Other admissions must count these same reservations, not a separate
	// retry-only capacity ledger.
	if _, err := a.ReserveAdmission(context.Background(), membershipWriteTenant, "ordinary", 1); !errors.Is(err, ErrAdmissionSlotLimitReached) {
		t.Fatalf("normal admission bypassed retry reservations: %v", err)
	}
}
