package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func memoryReportFixture(t *testing.T) (*Store, WorkloadTransitionTarget, WorkloadActionPrincipal) {
	t.Helper()
	s := emptyStore()
	s.AddCustomer(&Customer{ID: "report_tenant", Token: "synthetic-report"})
	target := WorkloadTransitionTarget{WorkloadID: "report_workload", CustomerID: "report_tenant", ClusterID: "report_cluster", BurstID: "report_burst"}
	s.PutWorkload(&Workload{ID: target.WorkloadID, CustomerID: target.CustomerID, ClusterID: target.ClusterID, BurstID: target.BurstID, Status: "provisioning"})
	if err := s.PutBurst(&Burst{ID: target.BurstID, CustomerID: target.CustomerID, ClusterID: target.ClusterID, BackendID: "synthetic-resource"}); err != nil {
		t.Fatal(err)
	}
	return s, target, WorkloadActionPrincipal{CredentialHash: HashCustomerToken("synthetic-report")}
}

type failedReportPersister struct{ *accountSpyPersister }

func (*failedReportPersister) recordWorkloadReport(context.Context, workloadReportRequest) (WorkloadReport, error) {
	// Even an ambiguous commit response must not publish an unconfirmed result.
	return WorkloadReport{Applied: true, Workload: &Workload{Status: "succeeded"}}, errors.New("synthetic commit failure")
}

func TestWorkloadReportsMemoryAndFailures(t *testing.T) {
	for _, operation := range []string{"started", "complete"} {
		for _, scenario := range []string{"accepted", "missing-booking", "cancelled-context", "unsupported-durable", "failed-durable", "wrong-credential", "human", "wrong-target", "revoked", "tombstoned", "foreign-burst"} {
			t.Run(operation+"/"+scenario, func(t *testing.T) {
				s, target, principal := memoryReportFixture(t)
				ctx := context.Background()
				var want error
				switch scenario {
				case "missing-booking":
					if err := s.DeleteBurst(target.BurstID); err != nil {
						t.Fatal(err)
					}
				case "cancelled-context":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
					want = context.Canceled
				case "unsupported-durable":
					s.persist = newAccountSpy()
					want = ErrPersistence
				case "failed-durable":
					s.persist = &failedReportPersister{newAccountSpy()}
					want = ErrPersistence
				case "wrong-credential":
					principal.CredentialHash = HashCustomerToken("different-synthetic-report")
					want = ErrNotFound
				case "human":
					principal.AccountID = "synthetic-human"
					want = ErrNotFound
				case "wrong-target":
					target.ClusterID = "different-cluster"
					want = ErrNotFound
				case "revoked":
					at := time.Now().UTC()
					s.customers[target.CustomerID].RevokedAt = &at
					want = ErrNotFound
				case "tombstoned":
					s.tombstoned[target.CustomerID] = true
					want = ErrNotFound
				case "foreign-burst":
					s.bursts[target.BurstID].CustomerID = "foreign"
					want = ErrPersistence
				}
				var result WorkloadReport
				var err error
				if operation == "started" {
					result, err = s.RecordWorkloadStarted(ctx, target, principal, time.Now().UTC())
				} else {
					result, err = s.RecordWorkloadCompletion(ctx, target, principal, "succeeded", time.Now().UTC(), nil)
				}
				if !errors.Is(err, want) {
					t.Fatalf("error=%v, want %v", err, want)
				}
				current, _ := s.GetWorkload(target.WorkloadID)
				booking, _ := s.GetBurst(target.BurstID)
				if want != nil {
					if result.Applied || result.Workload != nil || result.Burst != nil || current.Status != "provisioning" || current.StartedAt != nil || current.FinishedAt != nil || booking.ReapPending {
						t.Fatal("failed report published a transition or cleanup")
					}
					return
				}
				if !result.Applied || result.Workload == nil {
					t.Fatal("report not applied")
				}
				if operation == "started" {
					if current.Status != "running" || current.StartedAt == nil || (booking != nil && booking.ReapPending) {
						t.Fatal("invalid start transition")
					}
				} else if current.Status != "succeeded" || current.FinishedAt == nil || (booking != nil && (!booking.ReapPending || booking.ReapPendingStatus != "succeeded")) {
					t.Fatal("completion and cleanup not published together")
				}
				result.Workload.Status = "corrupted"
				if result.Burst != nil {
					result.Burst.BackendID = "corrupted"
				}
				if current.Status == "corrupted" || (booking != nil && booking.BackendID == "corrupted") {
					t.Fatal("report aliases store state")
				}
			})
		}
	}
}

func TestWorkloadReportsCurrentClusterScope(t *testing.T) {
	for _, scenario := range []string{"bound", "legacy-single", "legacy-multiple", "foreign-workload", "foreign-booking", "removed", "duplicate-credential"} {
		t.Run(scenario, func(t *testing.T) {
			s, target, principal := memoryReportFixture(t)
			principal.ClusterID = target.ClusterID
			principal.CredentialHash = HashClusterCredential("synthetic-scoped-report")
			c := s.customers[target.CustomerID]
			c.RegisteredClusters = []*RegisteredCluster{{ClusterID: target.ClusterID, CredentialHash: principal.CredentialHash}}
			want := ErrNotFound
			switch scenario {
			case "bound":
				want = nil
			case "legacy-single", "legacy-multiple":
				target.ClusterID = ""
				s.workloads[target.WorkloadID].ClusterID = ""
				if scenario == "legacy-single" {
					want = nil
				} else {
					c.RegisteredClusters = append(c.RegisteredClusters, &RegisteredCluster{ClusterID: "second"})
				}
			case "foreign-workload":
				target.ClusterID = "foreign"
				s.workloads[target.WorkloadID].ClusterID = "foreign"
			case "foreign-booking":
				s.bursts[target.BurstID].ClusterID = "foreign"
				want = ErrPersistence
			case "removed":
				c.RegisteredClusters = nil
			case "duplicate-credential":
				c.RegisteredClusters = append(c.RegisteredClusters, &RegisteredCluster{ClusterID: "second", CredentialHash: principal.CredentialHash})
			}
			result, err := s.RecordWorkloadCompletion(context.Background(), target, principal, "succeeded", time.Now().UTC(), nil)
			if !errors.Is(err, want) || result.Applied != (want == nil) {
				t.Fatalf("scope decision applied=%v err=%v, want %v", result.Applied, err, want)
			}
			if want != nil && (s.workloads[target.WorkloadID].FinishedAt != nil || s.bursts[target.BurstID].ReapPending) {
				t.Fatal("denied scope changed state")
			}
		})
	}
}

func TestWorkloadReportsPreserveFirstTerminalAndReceipt(t *testing.T) {
	for _, first := range []string{"succeeded", "cancelled", "no-receipt"} {
		t.Run(first, func(t *testing.T) {
			s, target, principal := memoryReportFixture(t)
			ctx, at := context.Background(), time.Now().UTC()
			outcome := &WorkloadOutcome{Compute: WorkloadComputeOutcome{Result: WorkloadResultSucceeded}}
			if first == "no-receipt" {
				outcome = nil
			}
			if first == "cancelled" {
				if _, err := s.RequestWorkloadCancellation(ctx, target.CustomerID, target.WorkloadID, principal); err != nil {
					t.Fatal(err)
				}
			} else if _, err := s.RecordWorkloadCompletion(ctx, target, principal, "succeeded", at, outcome); err != nil {
				t.Fatal(err)
			}
			current, _ := s.GetWorkload(target.WorkloadID)
			firstStatus, firstAt := current.Status, *current.FinishedAt
			if outcome != nil {
				outcome.Compute.Result = "corrupted"
			}
			// Repair cleanup even when the observation itself is a replay.
			s.bursts[target.BurstID].ReapPending = false
			result, err := s.RecordWorkloadCompletion(ctx, target, principal, "failed", at.Add(time.Hour), &WorkloadOutcome{Compute: WorkloadComputeOutcome{Result: WorkloadResultFailed}})
			if err != nil || result.Applied || result.Workload.Status != firstStatus || !result.Workload.FinishedAt.Equal(firstAt) || !result.Burst.ReapPending || result.Burst.ReapPendingStatus != firstStatus {
				t.Fatalf("replay changed terminal state or lost cleanup: %v", err)
			}
			if first == "succeeded" {
				if result.Workload.Outcome == nil || result.Workload.Outcome.Compute.Result != WorkloadResultSucceeded {
					t.Fatal("first receipt overwritten or aliased")
				}
			} else if result.Workload.Outcome != nil {
				t.Fatal("replay replaced first terminal receipt")
			}
			result, err = s.RecordWorkloadStarted(ctx, target, principal, at.Add(time.Hour))
			if err != nil || result.Applied || result.Workload.Status != firstStatus || result.Workload.StartedAt != nil {
				t.Fatalf("late start reopened terminal workload: %v", err)
			}
			if _, err := s.RecordWorkloadCompletion(ctx, target, principal, "running", at, nil); !errors.Is(err, ErrInvalidWorkloadReport) {
				t.Fatalf("invalid completion accepted: %v", err)
			}
		})
	}
}
