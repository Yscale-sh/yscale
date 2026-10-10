//go:build integration

package state

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func reportFixture(t *testing.T) (*Store, *pgPersister, WorkloadTransitionTarget, WorkloadActionPrincipal) {
	t.Helper()
	s, p, _ := cancellationFixture(t)
	return s, p, WorkloadTransitionTarget{WorkloadID: "wl_cancel", CustomerID: membershipWriteTenant, ClusterID: "cluster", BurstID: "burst_cancel"}, WorkloadActionPrincipal{CredentialHash: HashCustomerToken("synthetic-member-test-token")}
}

func recordFixtureReport(ctx context.Context, s *Store, target WorkloadTransitionTarget, principal WorkloadActionPrincipal, start bool) (WorkloadReport, error) {
	if start {
		return s.RecordWorkloadStarted(ctx, target, principal, time.Now().UTC())
	}
	return s.RecordWorkloadCompletion(ctx, target, principal, "succeeded", time.Now().UTC(), &WorkloadOutcome{Compute: WorkloadComputeOutcome{Result: WorkloadResultSucceeded}})
}

func TestPostgresWorkloadReportsWaitForCurrentAuthority(t *testing.T) {
	for _, start := range []bool{true, false} {
		for _, change := range []string{"credential", "workload", "booking", "revoked"} {
			name := change + "/complete"
			if start {
				name = change + "/started"
			}
			t.Run(name, func(t *testing.T) {
				a, p, target, principal := reportFixture(t)
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				tx, err := p.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(context.Background()) //nolint:errcheck
				waiting := "%pg_advisory_xact_lock($1,$2)%"
				if change == "workload" || change == "booking" {
					query := `SELECT id FROM workloads WHERE id='wl_cancel' FOR UPDATE`
					waiting = "%workloads%FOR UPDATE%"
					if change == "booking" {
						query = `SELECT id FROM bursts WHERE id='burst_cancel' FOR UPDATE`
						waiting = "%bursts%FOR UPDATE%"
					}
					if _, err := tx.Exec(ctx, query); err != nil {
						t.Fatal(err)
					}
				} else if err := lockMembershipTenant(ctx, tx, membershipWriteTenant); err != nil {
					t.Fatal(err)
				}
				type result struct {
					report WorkloadReport
					err    error
				}
				done := make(chan result, 1)
				go func() {
					report, err := recordFixtureReport(ctx, a, target, principal, start)
					done <- result{report, err}
				}()
				waitCancellationSQLLock(t, p, waiting)
				query := `UPDATE customers SET data=jsonb_set(data,'{TokenHash}','"different-verifier"') WHERE id='synthetic-membership-tenant'`
				want := ErrNotFound
				switch change {
				case "workload":
					query = `UPDATE workloads SET data=jsonb_set(data,'{ClusterID}','"different-cluster"') WHERE id='wl_cancel'`
				case "booking":
					query = `UPDATE bursts SET data=jsonb_set(data,'{CustomerID}','"foreign"') WHERE id='burst_cancel'`
					want = ErrPersistence
				case "revoked":
					query = `UPDATE customers SET data=jsonb_set(data,'{RevokedAt}',to_jsonb(now())) WHERE id='synthetic-membership-tenant'`
				}
				if _, err := tx.Exec(ctx, query); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				got := <-done
				if !errors.Is(got.err, want) || got.report.Applied || got.report.Workload != nil {
					t.Fatalf("waiting report ignored current authority: applied=%v err=%v", got.report.Applied, got.err)
				}
				var untouched bool
				if err := p.pool.QueryRow(ctx, `SELECT data->>'StartedAt' IS NULL AND data->>'FinishedAt' IS NULL AND data->>'Status'='running' FROM workloads WHERE id='wl_cancel'`).Scan(&untouched); err != nil || !untouched {
					t.Fatalf("refused report changed state: %v", err)
				}
				var pending bool
				if err := p.pool.QueryRow(ctx, `SELECT COALESCE((data->>'ReapPending')::bool,false) FROM bursts WHERE id='burst_cancel'`).Scan(&pending); err != nil || pending {
					t.Fatalf("refused report requested cleanup: %v", err)
				}
			})
		}
	}
}

func TestPostgresWorkloadReportsRollback(t *testing.T) {
	for _, failure := range []string{"start-write", "complete-write", "cleanup-write", "outage"} {
		t.Run(failure, func(t *testing.T) {
			a, p, target, principal := reportFixture(t)
			if failure == "outage" {
				p.pool.Close()
			} else {
				query := `ALTER TABLE workloads ADD CONSTRAINT refuse_report CHECK (data->>'FinishedAt' IS NULL)`
				if failure == "start-write" {
					query = `ALTER TABLE workloads ADD CONSTRAINT refuse_report CHECK (data->>'StartedAt' IS NULL)`
				}
				if failure == "cleanup-write" {
					query = `ALTER TABLE bursts ADD CONSTRAINT refuse_report CHECK (data->>'ReapPending' IS DISTINCT FROM 'true')`
				}
				if _, err := p.pool.Exec(context.Background(), query); err != nil {
					t.Fatal(err)
				}
			}
			result, err := recordFixtureReport(context.Background(), a, target, principal, failure == "start-write")
			if !errors.Is(err, ErrPersistence) || result.Applied || result.Workload != nil {
				t.Fatalf("failed write returned success: %v", err)
			}
			cached, _ := a.GetWorkload(target.WorkloadID)
			booking, _ := a.GetBurst(target.BurstID)
			if cached.StartedAt != nil || cached.FinishedAt != nil || booking.ReapPending {
				t.Fatal("failed report published cache state")
			}
			if failure != "outage" {
				status, pending, _ := cancellationState(t, p)
				if status != "running" || pending {
					t.Fatal("failed report partially committed")
				}
			}
		})
	}
}

func TestPostgresWorkloadReportsConcurrentFirstCompletion(t *testing.T) {
	a, p, target, principal := reportFixture(t)
	const replicas = 8
	reports := make(chan WorkloadReport, replicas)
	errorsSeen := make(chan error, replicas)
	var group sync.WaitGroup
	for i := 0; i < replicas; i++ {
		b := membershipWriteReplica(t, p)
		group.Add(1)
		go func(i int) {
			defer group.Done()
			status := "succeeded"
			if i%2 == 1 {
				status = "failed"
			}
			report, err := b.RecordWorkloadCompletion(context.Background(), target, principal, status, time.Now().UTC(), &WorkloadOutcome{Compute: WorkloadComputeOutcome{Result: status}})
			reports <- report
			errorsSeen <- err
		}(i)
	}
	group.Wait()
	close(reports)
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	current, err := a.WorkloadSnapshotForCustomer(context.Background(), target.CustomerID, target.WorkloadID)
	if err != nil {
		t.Fatal(err)
	}
	wins := 0
	for report := range reports {
		if report.Applied {
			wins++
		}
		if report.Workload.Status != current.Workload.Status || !report.Workload.FinishedAt.Equal(*current.Workload.FinishedAt) || report.Workload.Outcome.Compute.Result != current.Workload.Status || report.Burst.ReapPendingStatus != current.Workload.Status {
			t.Fatal("replicas disagree about first completion")
		}
	}
	if wins != 1 || !current.Burst.ReapPending {
		t.Fatalf("completion winners=%d, pending=%v", wins, current.Burst.ReapPending)
	}
}

func TestPostgresWorkloadReportsLateBooking(t *testing.T) {
	a, p, target, principal := reportFixture(t)
	if err := a.DeleteBurst(target.BurstID); err != nil {
		t.Fatal(err)
	}
	b := membershipWriteReplica(t, p)
	report, err := recordFixtureReport(context.Background(), a, target, principal, false)
	if err != nil || report.Burst != nil || !report.Applied {
		t.Fatalf("completion without booking: %v", err)
	}
	if err := b.PutBurst(&Burst{ID: target.BurstID, CustomerID: target.CustomerID, ClusterID: target.ClusterID, Backend: "linode", BackendID: "synthetic-late", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	current, err := a.BurstSnapshotContext(context.Background(), target.BurstID)
	if err != nil || !current.ReapPending || current.ReapPendingStatus != "succeeded" {
		t.Fatalf("late booking lost completion cleanup: %v", err)
	}
}

func TestPostgresWorkloadReportsBoundedWait(t *testing.T) {
	a, p, target, principal := reportFixture(t)
	tx, err := p.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	if err := lockMembershipTenant(context.Background(), tx, target.CustomerID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := recordFixtureReport(ctx, a, target, principal, false); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrPersistence) {
		t.Fatalf("report did not preserve bounded timeout: %v", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, pending, _ := cancellationState(t, p)
	if status != "running" || pending {
		t.Fatal("timed-out report committed")
	}
}
