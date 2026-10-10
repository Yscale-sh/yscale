package state

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestPostgresSchedulingObservationWaitingABStaleA(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	s, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	defer s.Close()

	// Whole-row writes intentionally preserve observations, so a new test run
	// must not reuse a workload carrying the previous run's newer timestamp.
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	customerID, burstID, workloadID := "cust_sched1_"+suffix, "burst_sched1_"+suffix, "wl_sched1_"+suffix
	s.AddCustomer(&Customer{ID: customerID, Token: "tok_" + suffix, Plan: "pro"})
	if err := s.PutBurst(&Burst{ID: burstID, CustomerID: customerID, ClusterID: "cluster-a",
		Backend: "linode", BackendID: "1", NodeName: "ys-burst-1-" + suffix, CreatedAt: time.Now().UTC(), Status: "provisioning"}); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	if err := s.PutWorkloadDurable(&Workload{ID: workloadID, CustomerID: customerID, ClusterID: "cluster-a",
		BurstID: burstID, Status: "provisioning", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("PutWorkloadDurable: %v", err)
	}

	tA := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	tB := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)

	resultA, err := s.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Waiting", "InsufficientGPU", "msg", "pod-1", tA)
	if err != nil || resultA != ObservationApplied {
		t.Fatalf("Waiting A: result=%v err=%v", resultA, err)
	}

	resultB, err := s.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Waiting", "InsufficientCPU", "msg", "pod-1", tB)
	if err != nil || resultB != ObservationApplied {
		t.Fatalf("Waiting B: result=%v err=%v", resultB, err)
	}

	resultStaleA, err := s.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Waiting", "InsufficientGPU", "msg", "pod-1", tA)
	if err != nil {
		t.Fatalf("stale A: err=%v", err)
	}
	if resultStaleA == ObservationApplied {
		w, _ := s.GetWorkload(workloadID)
		if w.SchedulingObservation != nil && w.SchedulingObservation.Reason != "InsufficientCPU" {
			t.Fatal("stale Waiting A overwrote newer Waiting B")
		}
	}

	w, err := s.GetWorkload(workloadID)
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if w.SchedulingObservation == nil || w.SchedulingObservation.Reason != "InsufficientCPU" {
		t.Fatalf("final observation should be Waiting B (InsufficientCPU), got %+v", w.SchedulingObservation)
	}
}

func TestPostgresSchedulingObservationWaitingThenScheduledThenDelayedWaiting(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	s, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	defer s.Close()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	customerID, burstID, workloadID := "cust_sched2_"+suffix, "burst_sched2_"+suffix, "wl_sched2_"+suffix
	nodeName := "ys-burst-2-" + suffix
	s.AddCustomer(&Customer{ID: customerID, Token: "tok_" + suffix, Plan: "pro"})
	if err := s.PutBurst(&Burst{ID: burstID, CustomerID: customerID, ClusterID: "cluster-a",
		Backend: "linode", BackendID: "2", NodeName: nodeName, CreatedAt: time.Now().UTC(), Status: "provisioning"}); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	if err := s.PutWorkloadDurable(&Workload{ID: workloadID, CustomerID: customerID, ClusterID: "cluster-a",
		BurstID: burstID, Status: "provisioning", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("PutWorkloadDurable: %v", err)
	}

	tW := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	tS := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	tDelayed := time.Date(2026, 8, 21, 11, 0, 0, 0, time.UTC)

	if result, err := s.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Waiting", "InsufficientGPU", "msg", "pod-1", tW); err != nil || result != ObservationApplied {
		t.Fatalf("stamp Waiting: result=%v err=%v", result, err)
	}

	if applied, err := s.StampWorkloadPodObservation(ctx, workloadID, customerID, "cluster-a", "pod-1", nodeName, tS); err != nil || !applied {
		t.Fatalf("stamp Pod: applied=%v err=%v", applied, err)
	}
	if result, err := s.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Scheduled", "", "", "pod-1", tS); err != nil || result != ObservationApplied {
		t.Fatalf("stamp Scheduled: result=%v err=%v", result, err)
	}

	result, err := s.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Waiting", "InsufficientGPU", "msg", "pod-1", tDelayed)
	if err != nil {
		t.Fatalf("delayed Waiting: %v", err)
	}
	if result == ObservationApplied {
		w, _ := s.GetWorkload(workloadID)
		if w.SchedulingObservation != nil && w.SchedulingObservation.State != "Scheduled" {
			t.Fatal("delayed Waiting regressed terminal Scheduled")
		}
	}

	w, err := s.GetWorkload(workloadID)
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if w.SchedulingObservation == nil || w.SchedulingObservation.State != "Scheduled" {
		t.Fatalf("final state should be Scheduled, got %+v", w.SchedulingObservation)
	}
}

func TestPostgresSchedulingObservationSubsecondOrdering(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	s, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	defer s.Close()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	customerID, burstID, workloadID := "cust_sched3_"+suffix, "burst_sched3_"+suffix, "wl_sched3_"+suffix
	s.AddCustomer(&Customer{ID: customerID, Token: "tok_" + suffix, Plan: "pro"})
	if err := s.PutBurst(&Burst{ID: burstID, CustomerID: customerID, ClusterID: "cluster-a",
		Backend: "linode", BackendID: "3", NodeName: "ys-burst-3-" + suffix, CreatedAt: time.Now().UTC(), Status: "provisioning"}); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	if err := s.PutWorkloadDurable(&Workload{ID: workloadID, CustomerID: customerID, ClusterID: "cluster-a",
		BurstID: burstID, Status: "provisioning", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("PutWorkloadDurable: %v", err)
	}

	tWhole := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	tSubsecond := time.Date(2026, 8, 21, 10, 0, 0, 500000000, time.UTC)

	if result, err := s.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Waiting", "InsufficientGPU", "msg", "pod-1", tWhole); err != nil || result != ObservationApplied {
		t.Fatalf("whole-second timestamp: result=%v err=%v", result, err)
	}

	result, err := s.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Waiting", "InsufficientCPU", "msg2", "pod-1", tSubsecond)
	if err != nil || result != ObservationApplied {
		t.Fatalf("subsecond timestamp should be strictly newer than whole-second: result=%v err=%v", result, err)
	}

	w, err := s.GetWorkload(workloadID)
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if w.SchedulingObservation == nil || w.SchedulingObservation.Reason != "InsufficientCPU" {
		t.Fatalf("subsecond observation should win, got %+v", w.SchedulingObservation)
	}
}

func TestPostgresStaleWholeRowPreservesObservations(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	writer, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	defer writer.Close()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	customerID := "cust_sched4_" + suffix
	burstID := "burst_sched4_" + suffix
	workloadID := "wl_sched4_" + suffix
	nodeName := "ys-burst-4-" + suffix
	createdAt := time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)
	writer.AddCustomer(&Customer{ID: customerID, Token: "tok_" + suffix, Plan: "pro"})
	if err := writer.PutBurst(&Burst{
		ID: burstID, CustomerID: customerID, ClusterID: "cluster-a",
		Backend: "linode", BackendID: "4", NodeName: nodeName,
		CreatedAt: createdAt, Status: "provisioning",
	}); err != nil {
		t.Fatalf("PutBurst: %v", err)
	}
	writer.PutWorkload(&Workload{
		ID: workloadID, CustomerID: customerID, ClusterID: "cluster-a",
		BurstID: burstID, Status: "provisioning", CreatedAt: createdAt,
	})

	// Open another replica and take its copy before any targeted observation
	// writer runs. Its in-memory merge can therefore know nothing about the
	// durable fields written below; only the ON CONFLICT expression can save them.
	staleReplica, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("open stale replica: %v", err)
	}
	defer staleReplica.Close()
	staleW, err := staleReplica.GetWorkload(workloadID)
	if err != nil {
		t.Fatalf("load stale workload: %v", err)
	}
	staleW.Status = "running"

	nodeAt := time.Date(2026, 8, 21, 9, 0, 0, 123000000, time.UTC)
	gpuAt := nodeAt.Add(time.Minute)
	waitingAt := gpuAt.Add(time.Minute)
	podAt := waitingAt.Add(time.Minute)
	applied, err := writer.UpdateBurstNodePhase(ctx, BurstNodePhaseUpdate{
		BurstID: burstID, CustomerID: customerID, ClusterID: "cluster-a",
		Phase: "Ready", Reason: "KubeletReady", ObservedAt: nodeAt, SourceTimestamped: true,
		GPUAllocatable: true, GPUAllocatableAt: &gpuAt,
	})
	if err != nil || !applied {
		t.Fatalf("UpdateBurstNodePhase: applied=%v err=%v", applied, err)
	}
	if result, err := writer.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Waiting", "InsufficientGPU", "msg", "pod-1", waitingAt); err != nil || result != ObservationApplied {
		t.Fatalf("stamp Waiting: result=%v err=%v", result, err)
	}
	if applied, err := writer.StampWorkloadPodObservation(ctx, workloadID, customerID, "cluster-a",
		"pod-1", nodeName, podAt); err != nil || !applied {
		t.Fatalf("stamp pod: applied=%v err=%v", applied, err)
	}
	if result, err := writer.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Scheduled", "", "", "pod-1", podAt); err != nil || result != ObservationApplied {
		t.Fatalf("stamp Scheduled: result=%v err=%v", result, err)
	}
	cost := WorkloadCost{
		EstimatedUSD: 2.75, HourlyUSD: 1.1, Runtime: 150 * time.Minute,
		FrozenAt: podAt.Add(time.Hour), Backend: "linode", BurstID: burstID,
		Basis: WorkloadCostBasisRateRuntimeToProviderDelete,
	}
	if recorded, err := writer.RecordWorkloadCostForBurst(ctx, cost); err != nil || !recorded {
		t.Fatalf("RecordWorkloadCostForBurst: recorded=%v err=%v", recorded, err)
	}

	staleReplica.PutWorkload(staleW)

	reloaded, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reload after stale write: %v", err)
	}
	defer reloaded.Close()
	w, err := reloaded.GetWorkload(workloadID)
	if err != nil {
		t.Fatalf("GetWorkload after reload: %v", err)
	}
	if w == nil {
		t.Fatal("workload not found after reload")
	}
	if w.Status != "running" {
		t.Fatalf("status = %q, want stale replica's legitimate lifecycle update", w.Status)
	}
	if w.NodeObservation == nil || w.NodeObservation.NodeName != nodeName ||
		w.NodeObservation.Phase != "Ready" || w.NodeObservation.Reason != "KubeletReady" ||
		!w.NodeObservation.ObservedAt.Equal(nodeAt) || !w.NodeObservation.SourceTimestamped {
		t.Fatalf("NodeObservation changed by stale whole-row write: %+v", w.NodeObservation)
	}
	if w.PodObservation == nil || w.PodObservation.PodName != "pod-1" ||
		w.PodObservation.NodeName != nodeName || !w.PodObservation.ScheduledAt.Equal(podAt) {
		t.Fatalf("PodObservation changed by stale whole-row write: %+v", w.PodObservation)
	}
	if w.GPUObservation == nil || !w.GPUObservation.AllocatableAt.Equal(gpuAt) {
		t.Fatalf("GPUObservation changed by stale whole-row write: %+v", w.GPUObservation)
	}
	if w.SchedulingObservation == nil || w.SchedulingObservation.State != "Scheduled" ||
		w.SchedulingObservation.Reason != "" || w.SchedulingObservation.Message != "" ||
		w.SchedulingObservation.PodName != "pod-1" || !w.SchedulingObservation.ObservedAt.Equal(podAt) {
		t.Fatalf("SchedulingObservation changed by stale whole-row write: %+v", w.SchedulingObservation)
	}
	if w.Cost == nil || w.Cost.EstimatedUSD != cost.EstimatedUSD || w.Cost.HourlyUSD != cost.HourlyUSD ||
		w.Cost.Runtime != cost.Runtime || !w.Cost.FrozenAt.Equal(cost.FrozenAt) ||
		w.Cost.Backend != cost.Backend || w.Cost.BurstID != cost.BurstID || w.Cost.Basis != cost.Basis {
		t.Fatalf("Cost changed by stale whole-row write: %+v", w.Cost)
	}
}

func TestPostgresNonexistentWorkloadNotCreated(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	s, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	defer s.Close()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	customerID, workloadID := "cust_sched5_"+suffix, "wl_nonexist_"+suffix
	s.AddCustomer(&Customer{ID: customerID, Token: "tok_" + suffix, Plan: "pro"})

	result, err := s.StampWorkloadSchedulingObservation(ctx, workloadID, customerID, "cluster-a",
		"Waiting", "InsufficientGPU", "msg", "pod-1", time.Now().UTC())
	if err != nil {
		t.Fatalf("nonexistent workload: %v", err)
	}
	if result == ObservationApplied {
		t.Fatal("scheduling observation stamped on a nonexistent workload")
	}

	_, err = s.GetWorkload(workloadID)
	if err == nil {
		t.Fatal("nonexistent workload should not be found")
	}
}
