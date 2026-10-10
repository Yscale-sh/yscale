package state

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestWorkloadSnapshotsMemory(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	s := &Store{
		workloads: map[string]*Workload{
			"old": {ID: "old", CustomerID: "tenant", CreatedAt: now.Add(-time.Hour)},
			"b":   {ID: "b", CustomerID: "tenant", CreatedAt: now, BurstID: "foreign"},
			"a":   {ID: "a", CustomerID: "tenant", CreatedAt: now, BurstID: "own", SpecYAML: []byte("spec")},
			"x":   {ID: "x", CustomerID: "other", CreatedAt: now.Add(time.Hour)},
		},
		bursts: map[string]*Burst{
			"foreign": {ID: "foreign", CustomerID: "other"},
			"own": {ID: "own", CustomerID: "tenant", HourlyUSD: math.NaN(),
				TerminalCost:    &WorkloadCost{EstimatedUSD: 2},
				LastHeartbeatAt: &now, NodePhaseAt: &now, OccupancyObservedAt: &now},
		},
	}
	w := s.workloads["a"]
	w.StartedAt, w.FinishedAt = &now, &now
	w.PodObservation = &PodObservation{PodName: "pod"}
	w.GPUObservation = &GPUObservation{AllocatableAt: now}
	w.SchedulingObservation = &SchedulingObservation{State: "Scheduled"}
	rows, err := s.WorkloadSnapshotsForCustomer(ctx, "tenant", 2)
	if err != nil || len(rows) != 2 {
		t.Fatalf("list: rows=%d err=%v", len(rows), err)
	}
	if rows[0].Workload.ID != "a" || rows[1].Workload.ID != "b" || rows[1].Burst != nil {
		t.Fatal("tenant filter, tie order, limit, or burst ownership violated")
	}
	if !math.IsNaN(rows[0].Burst.HourlyUSD) {
		t.Fatal("snapshot must preserve invalid rates for fail-closed spend projection")
	}
	rows[0].Workload.SpecYAML[0] = 'X'
	*rows[0].Workload.StartedAt = time.Time{}
	*rows[0].Workload.FinishedAt = time.Time{}
	rows[0].Workload.PodObservation.PodName = "changed"
	rows[0].Workload.GPUObservation.AllocatableAt = time.Time{}
	rows[0].Workload.SchedulingObservation.State = "Waiting"
	rows[0].Burst.TerminalCost.EstimatedUSD = 99
	*rows[0].Burst.LastHeartbeatAt = time.Time{}
	*rows[0].Burst.NodePhaseAt = time.Time{}
	*rows[0].Burst.OccupancyObservedAt = time.Time{}
	if string(s.workloads["a"].SpecYAML) != "spec" || s.bursts["own"].TerminalCost.EstimatedUSD != 2 || now.IsZero() {
		t.Fatal("snapshot aliases stored spec or burst pointer fields")
	}
	if w.PodObservation.PodName != "pod" || w.GPUObservation.AllocatableAt.IsZero() || w.SchedulingObservation.State != "Scheduled" {
		t.Fatal("snapshot aliases workload observation fields")
	}
	for _, id := range []string{"x", "missing", ""} {
		if _, err := s.WorkloadSnapshotForCustomer(ctx, "tenant", id); !errors.Is(err, ErrNotFound) {
			t.Errorf("detail %q: %v, want not found", id, err)
		}
	}
	row, err := s.WorkloadSnapshotForCustomer(ctx, "tenant", "old")
	if err != nil || row.Workload.ID != "old" || row.Burst != nil {
		t.Fatalf("detail without burst: %+v, %v", row, err)
	}
	for _, tc := range []struct {
		tenant string
		limit  int
	}{{"", 2}, {"tenant", 0}, {"tenant", -1}, {"missing", 2}} {
		rows, err := s.WorkloadSnapshotsForCustomer(ctx, tc.tenant, tc.limit)
		if err != nil || rows == nil || len(rows) != 0 {
			t.Errorf("empty list (%q,%d): %#v, %v", tc.tenant, tc.limit, rows, err)
		}
	}
}

type snapshotTestPersister struct {
	persister
	read func(context.Context, string, string, int) ([]WorkloadSnapshot, error)
}

func (p snapshotTestPersister) readWorkloadSnapshots(ctx context.Context, customer, workload string, limit int) ([]WorkloadSnapshot, error) {
	return p.read(ctx, customer, workload, limit)
}

func TestWorkloadSnapshotsDurableAuthority(t *testing.T) {
	ctx := context.Background()
	stale := &Workload{ID: "w", CustomerID: "tenant", Status: "running"}
	s := &Store{workloads: map[string]*Workload{"w": stale}}
	var calls int
	s.persist = snapshotTestPersister{read: func(_ context.Context, tenant, id string, limit int) ([]WorkloadSnapshot, error) {
		calls++
		if tenant != "tenant" || id != "w" || limit != 1 {
			t.Fatalf("unscoped detail: %q %q %d", tenant, id, limit)
		}
		return []WorkloadSnapshot{{Workload: &Workload{ID: "w", CustomerID: tenant, Status: "completed"}}}, nil
	}}
	row, err := s.WorkloadSnapshotForCustomer(ctx, "tenant", "w")
	if err != nil || row.Workload.Status != "completed" || calls != 1 {
		t.Fatalf("durable detail: %+v, %v; calls=%d", row, err, calls)
	}
	if s.workloads["w"] != stale || stale.Status != "running" {
		t.Fatal("read hydrated the mutation cache")
	}
	s.persist = snapshotTestPersister{read: func(_ context.Context, tenant, id string, limit int) ([]WorkloadSnapshot, error) {
		if tenant != "tenant" || id != "" || limit != 3 {
			t.Fatalf("unscoped list: %q %q %d", tenant, id, limit)
		}
		return []WorkloadSnapshot{}, nil
	}}
	if rows, err := s.WorkloadSnapshotsForCustomer(ctx, "tenant", 3); err != nil || len(rows) != 0 {
		t.Fatalf("empty durable list fell back to cache: %+v, %v", rows, err)
	}
	wantErr := errors.New("database unavailable")
	s.persist = snapshotTestPersister{read: func(context.Context, string, string, int) ([]WorkloadSnapshot, error) {
		return nil, wantErr
	}}
	if _, err := s.WorkloadSnapshotForCustomer(ctx, "tenant", "w"); !errors.Is(err, wantErr) {
		t.Fatalf("backend failure fell back: %v", err)
	}
	if rows, err := s.WorkloadSnapshotsForCustomer(ctx, "tenant", 3); !errors.Is(err, wantErr) || rows != nil {
		t.Fatalf("backend list failure fell back: %+v, %v", rows, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.WorkloadSnapshotForCustomer(cancelled, "tenant", "w"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request: %v", err)
	}
	s.persist = struct{ persister }{}
	if _, err := s.WorkloadSnapshotForCustomer(ctx, "tenant", "w"); err == nil {
		t.Fatal("unsupported durable reader used cache")
	}
	s.persist = snapshotTestPersister{read: func(context.Context, string, string, int) ([]WorkloadSnapshot, error) {
		return nil, nil
	}}
	if _, err := s.WorkloadSnapshotForCustomer(ctx, "tenant", "w"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("durable miss used cache: %v", err)
	}
}
