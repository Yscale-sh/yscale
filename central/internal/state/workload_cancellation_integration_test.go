//go:build integration

package state

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func waitCancellationSQLLock(t *testing.T, p *pgPersister, query string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		if err := p.pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1 AND pid<>pg_backend_pid())`, query).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no SQL lock wait for %s", query)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPostgresWorkloadCancellationNodePhaseLockOrder(t *testing.T) {
	_, p, _ := cancellationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SELECT id FROM workloads WHERE id='wl_cancel' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.updateBurstNodePhase(ctx, BurstNodePhaseUpdate{BurstID: "burst_cancel", CustomerID: membershipWriteTenant, ClusterID: "cluster", Phase: "Ready", ObservedAt: time.Now().UTC()})
		done <- err
	}()
	waitCancellationSQLLock(t, p, "%workloads%")
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='150ms'`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT id FROM bursts WHERE id='burst_cancel' FOR UPDATE`); err != nil {
		t.Errorf("node phase held the burst while waiting for the workload; cancellation can deadlock: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPostgresWorkloadCancellationConcurrentFirstBooking(t *testing.T) {
	a, p, _ := cancellationFixture(t)
	ctx := context.Background()
	stored := &Burst{ID: "burst_first_cancel", CustomerID: membershipWriteTenant, ClusterID: "cluster", Backend: "linode", BackendID: "synthetic-first", CreatedAt: time.Now().UTC(), ReapPending: true, ReapPendingStatus: "cancelled"}
	stale := cloneBurstSnapshot(stored)
	stale.ReapPending, stale.ReapPendingStatus = false, ""
	data, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `INSERT INTO bursts(id,data) VALUES($1,$2)`, stored.ID, data); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.PutBurst(stale) }()
	waitCancellationSQLLock(t, p, "%INSERT INTO bursts%")
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	current, err := a.BurstSnapshotContext(ctx, stored.ID)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := a.GetBurst(stored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !current.ReapPending || current.ReapPendingStatus != "cancelled" || !cached.ReapPending {
		t.Fatal("concurrent first booking erased the pending cleanup request")
	}
	if stale.ReapPending {
		t.Fatal("durable merge mutated the caller's old snapshot")
	}
}

func TestPostgresWorkloadCancellationRejectsTerminalRebinding(t *testing.T) {
	a, p, ids := cancellationFixture(t)
	b := membershipWriteReplica(t, p)
	if _, err := a.RequestWorkloadCancellation(context.Background(), membershipWriteTenant, "wl_cancel", WorkloadCancelPrincipal{AccountID: ids["target"]}); err != nil {
		t.Fatal(err)
	}
	stale, _ := b.GetWorkload("wl_cancel")
	stale.BurstID = "different-burst"
	if err := b.PutWorkloadDurable(stale); !errors.Is(err, ErrPersistence) {
		t.Fatalf("terminal binding rewrite=%v", err)
	}
	current, err := a.WorkloadSnapshotForCustomer(context.Background(), membershipWriteTenant, "wl_cancel")
	if err != nil || current.Workload.BurstID != "burst_cancel" || current.Workload.Status != "cancelled" {
		t.Fatalf("terminal binding was lost: %v", err)
	}
}

func TestPostgresWorkloadCancellationPendingWrites(t *testing.T) {
	for _, change := range []string{"stale", "resource", "tenant", "cluster", "region", "account", "sku"} {
		t.Run(change, func(t *testing.T) {
			a, p, ids := cancellationFixture(t)
			b := membershipWriteReplica(t, p)
			stale, _ := b.GetBurst("burst_cancel")
			if _, err := a.RequestWorkloadCancellation(context.Background(), membershipWriteTenant, "wl_cancel", WorkloadCancelPrincipal{AccountID: ids["target"]}); err != nil {
				t.Fatal(err)
			}
			stale = cloneBurstSnapshot(stale)
			switch change {
			case "resource":
				stale.BackendID = "wrong"
			case "tenant":
				stale.CustomerID = "wrong"
			case "cluster":
				stale.ClusterID = "wrong"
			case "region":
				stale.Region = "wrong"
			case "account":
				stale.CloudAccountID = "wrong"
			case "sku":
				stale.SKU = "wrong"
			}
			err := b.PutBurst(stale)
			if change == "stale" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrPersistence) {
				t.Fatalf("pending identity overwrite=%v", err)
			}
			current, err := a.BurstSnapshotContext(context.Background(), "burst_cancel")
			if err != nil || !current.ReapPending || current.ReapPendingStatus != "cancelled" || current.BackendID != "synthetic-resource" || current.CustomerID != membershipWriteTenant || current.ClusterID != "cluster" || current.Region != "us-east" || current.CloudAccountID != "" || current.SKU != "synthetic-sku" {
				t.Fatalf("pending booking changed: %v", err)
			}
		})
	}
}

func TestPostgresWorkloadCancellationConcurrentRepeats(t *testing.T) {
	_, p, ids := cancellationFixture(t)
	const n = 8
	replicas := make([]*Store, n)
	for i := range replicas {
		replicas[i] = membershipWriteReplica(t, p)
	}
	results := make(chan WorkloadCancellation, n)
	errorsOut := make(chan error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, s := range replicas {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			<-start
			result, err := s.RequestWorkloadCancellation(context.Background(), membershipWriteTenant, "wl_cancel", WorkloadCancelPrincipal{AccountID: ids["target"]})
			if err != nil {
				errorsOut <- err
			} else {
				results <- result
			}
		}(s)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errorsOut)
	for err := range errorsOut {
		t.Error(err)
	}
	applied, count := 0, 0
	var finished time.Time
	for result := range results {
		count++
		if result.applied {
			applied++
		}
		if finished.IsZero() {
			finished = *result.Workload.FinishedAt
		}
		if !result.Workload.FinishedAt.Equal(finished) || !result.Decision.Allowed || !result.Burst.ReapPending {
			t.Fatal("repeat changed terminal state or lost pending cleanup")
		}
	}
	status, pending, events := cancellationState(t, p)
	if count != n || applied != 1 || status != "cancelled" || !pending || events != n {
		t.Fatalf("repeats=%d applied=%d status=%s pending=%v audits=%d", count, applied, status, pending, events)
	}
}

func cancellationFixture(t *testing.T) (*Store, *pgPersister, map[string]string) {
	t.Helper()
	s, p, ids := membershipWriteFixture(t)
	if _, _, err := s.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleMember, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	actor := HumanActor(ids["target"], membershipWriteTenant)
	if err := s.PutWorkloadDurable(&Workload{ID: "wl_cancel", CustomerID: membershipWriteTenant, ClusterID: "cluster", BurstID: "burst_cancel", Status: "running", SubmittedBy: &actor}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutBurst(&Burst{ID: "burst_cancel", CustomerID: membershipWriteTenant, ClusterID: "cluster", Backend: "linode", BackendID: "synthetic-resource", Region: "us-east", SKU: "synthetic-sku", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	return s, p, ids
}

func cancellationState(t *testing.T, p *pgPersister) (string, bool, int) {
	t.Helper()
	var status string
	var pending bool
	var events int
	if err := p.pool.QueryRow(context.Background(), `SELECT w.data->>'Status', COALESCE((b.data->>'ReapPending')::bool,false),
		(SELECT count(*) FROM tenant_audit WHERE data->>'Action'='workload.cancel')
		FROM workloads w JOIN bursts b ON b.id=w.data->>'BurstID' WHERE w.id='wl_cancel'`).Scan(&status, &pending, &events); err != nil {
		t.Fatal(err)
	}
	return status, pending, events
}

func TestPostgresWorkloadCancellationUsesCurrentAuthority(t *testing.T) {
	for _, scenario := range []string{"accepted", "submitter-changed", "role-changed", "membership-removed", "tenant-revoked", "credential-rotated", "sibling-cluster"} {
		t.Run(scenario, func(t *testing.T) {
			a, p, ids := cancellationFixture(t)
			b := membershipWriteReplica(t, p)
			principal := WorkloadCancelPrincipal{AccountID: ids["target"]}
			var wantErr error
			wantAllowed := false
			switch scenario {
			case "accepted":
				wantAllowed = true
			case "submitter-changed":
				w, _ := a.GetWorkload("wl_cancel")
				actor := HumanActor(ids["owner"], membershipWriteTenant)
				w.SubmittedBy = &actor
				if err := a.PutWorkloadDurable(w); err != nil {
					t.Fatal(err)
				}
			case "role-changed":
				if _, _, err := a.SetTenantMembershipRole(ids["target"], membershipWriteTenant, RoleViewer, OperatorActor()); err != nil {
					t.Fatal(err)
				}
			case "membership-removed":
				if _, err := a.DeleteTenantMembership(ids["target"], membershipWriteTenant, OperatorActor()); err != nil {
					t.Fatal(err)
				}
				wantErr = ErrNotFound
			case "tenant-revoked":
				if err := a.RevokeCustomer(membershipWriteTenant); err != nil {
					t.Fatal(err)
				}
				wantErr = ErrNotFound
			case "credential-rotated":
				principal = WorkloadCancelPrincipal{CredentialHash: HashCustomerToken("synthetic-member-test-token")}
				if _, err := a.RotateCustomerCredential(membershipWriteTenant, OperatorActor()); err != nil {
					t.Fatal(err)
				}
				wantErr = ErrNotFound
			case "sibling-cluster":
				principal = WorkloadCancelPrincipal{ClusterID: "sibling", CredentialHash: HashClusterCredential("synthetic-sibling")}
				if _, err := p.pool.Exec(context.Background(), `UPDATE customers SET data=jsonb_set(data,'{RegisteredClusters}',jsonb_build_array(jsonb_build_object('ClusterID','sibling','CredentialHash',$1::text))) WHERE id=$2`, principal.CredentialHash, membershipWriteTenant); err != nil {
					t.Fatal(err)
				}
				wantErr = ErrNotFound
			}
			result, err := b.RequestWorkloadCancellation(context.Background(), membershipWriteTenant, "wl_cancel", principal)
			if !errors.Is(err, wantErr) || result.Decision.Allowed != wantAllowed {
				t.Fatalf("allowed=%v, error=%v; want allowed=%v, error=%v", result.Decision.Allowed, err, wantAllowed, wantErr)
			}
			status, pending, events := cancellationState(t, p)
			if wantAllowed {
				if status != "cancelled" || !pending || events != 1 || result.Audit == nil || result.Audit.ID == "" {
					t.Fatalf("accepted cancellation not atomic: status=%s, pending=%v, audits=%d", status, pending, events)
				}
			} else {
				wantEvents := 1
				if wantErr != nil {
					wantEvents = 0
				}
				if status != "running" || pending || events != wantEvents {
					t.Fatalf("refusal changed state or lost audit: status=%s, pending=%v, audits=%d", status, pending, events)
				}
			}
		})
	}
}

func TestPostgresWorkloadCancellationRollsBackTogether(t *testing.T) {
	for _, failure := range []string{"audit", "burst", "workload", "database"} {
		t.Run(failure, func(t *testing.T) {
			a, p, ids := cancellationFixture(t)
			if failure == "database" {
				p.pool.Close()
			} else {
				statement := map[string]string{
					"audit":    `ALTER TABLE tenant_audit ADD CONSTRAINT refuse_cancel CHECK (data->>'Action' <> 'workload.cancel')`,
					"burst":    `ALTER TABLE bursts ADD CONSTRAINT refuse_cancel CHECK (data->>'ReapPending' IS DISTINCT FROM 'true')`,
					"workload": `ALTER TABLE workloads ADD CONSTRAINT refuse_cancel CHECK (data->>'Status' <> 'cancelled')`,
				}[failure]
				if _, err := p.pool.Exec(context.Background(), statement); err != nil {
					t.Fatal(err)
				}
			}
			if result, err := a.RequestWorkloadCancellation(context.Background(), membershipWriteTenant, "wl_cancel", WorkloadCancelPrincipal{AccountID: ids["target"]}); !errors.Is(err, ErrPersistence) || result.Decision.Allowed {
				t.Fatalf("failed cancellation result=%v, error=%v", result.Decision, err)
			}
			local, _ := a.GetWorkload("wl_cancel")
			burst, _ := a.GetBurst("burst_cancel")
			if local.Status != "running" || burst.ReapPending {
				t.Error("failed cancellation published state")
			}
			if failure != "database" {
				status, pending, events := cancellationState(t, p)
				if status != "running" || pending || events != 0 {
					t.Fatalf("partial cancellation commit: status=%s, pending=%v, audits=%d", status, pending, events)
				}
			}
		})
	}
}

func TestPostgresWorkloadCancellationWaitsForMembershipCommit(t *testing.T) {
	a, p, ids := cancellationFixture(t)
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockMembershipTenant(ctx, tx, membershipWriteTenant); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result WorkloadCancellation
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := a.RequestWorkloadCancellation(ctx, membershipWriteTenant, "wl_cancel", WorkloadCancelPrincipal{AccountID: ids["target"]})
		done <- outcome{result, err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock($1,$2)%' AND pid<>pg_backend_pid())`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancellation did not wait on the tenant authorization lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := tx.Exec(ctx, `UPDATE tenant_memberships SET data=jsonb_set(data,'{Role}','"viewer"') WHERE id=$1`, membershipID(ids["target"], membershipWriteTenant)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	finished := <-done
	if finished.err != nil || finished.result.Decision.Allowed || finished.result.Decision.Role != RoleViewer {
		t.Fatalf("cancellation ignored the committed role: allowed=%v, error=%v", finished.result.Decision.Allowed, finished.err)
	}
	status, pending, events := cancellationState(t, p)
	if status != "running" || pending || events != 1 {
		t.Fatal("waiting cancellation was not refused atomically")
	}
}
