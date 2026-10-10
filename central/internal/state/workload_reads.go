package state

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// WorkloadSnapshot is a read-only pair from one store/database snapshot. It is
// not a second lifecycle authority and must not be written back as a mutation.
type WorkloadSnapshot struct {
	Workload *Workload
	Burst    *Burst // absent after removal, or when it belongs to another tenant
}

type workloadSnapshotReader interface {
	readWorkloadSnapshots(context.Context, string, string, int) ([]WorkloadSnapshot, error)
}

// WorkloadSnapshotForCustomer reads durable workload and burst state when a
// backend is configured. A missing/foreign workload is ErrNotFound; a backend
// failure is never a missing record or permission to use a stale local cache.
func (s *Store) WorkloadSnapshotForCustomer(ctx context.Context, customerID, workloadID string) (WorkloadSnapshot, error) {
	if customerID == "" || workloadID == "" {
		return WorkloadSnapshot{}, ErrNotFound
	}
	rows, err := s.readWorkloadSnapshots(ctx, customerID, workloadID, 1)
	if err != nil {
		return WorkloadSnapshot{}, err
	}
	if len(rows) == 0 {
		return WorkloadSnapshot{}, ErrNotFound
	}
	return rows[0], nil
}

// WorkloadSnapshotsForCustomer returns newest-first tenant history together
// with each workload's same-tenant burst. Durable reads do not hydrate the
// process-local mutation maps: doing so could overwrite a newer local writer.
func (s *Store) WorkloadSnapshotsForCustomer(ctx context.Context, customerID string, limit int) ([]WorkloadSnapshot, error) {
	if customerID == "" || limit <= 0 {
		return []WorkloadSnapshot{}, nil
	}
	return s.readWorkloadSnapshots(ctx, customerID, "", limit)
}

func (s *Store) readWorkloadSnapshots(ctx context.Context, customerID, workloadID string, limit int) ([]WorkloadSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	if p := s.persist; p != nil {
		s.mu.RUnlock()
		reader, ok := p.(workloadSnapshotReader)
		if !ok {
			return nil, fmt.Errorf("durable workload snapshot reads unavailable")
		}
		return reader.readWorkloadSnapshots(ctx, customerID, workloadID, limit)
	}
	defer s.mu.RUnlock()
	rows := make([]WorkloadSnapshot, 0)
	for _, w := range s.workloads {
		if w.CustomerID != customerID || (workloadID != "" && w.ID != workloadID) {
			continue
		}
		row := WorkloadSnapshot{Workload: cloneWorkload(w)}
		row.Workload.StartedAt = cloneReadPointer(w.StartedAt)
		row.Workload.FinishedAt = cloneReadPointer(w.FinishedAt)
		row.Workload.PodObservation = cloneReadPointer(w.PodObservation)
		row.Workload.GPUObservation = cloneReadPointer(w.GPUObservation)
		row.Workload.SchedulingObservation = cloneReadPointer(w.SchedulingObservation)
		if b := s.bursts[w.BurstID]; b != nil && b.CustomerID == customerID {
			cp := *b
			cp.TerminalCost = cloneReadPointer(b.TerminalCost)
			cp.LastHeartbeatAt = cloneReadPointer(b.LastHeartbeatAt)
			cp.NodePhaseAt = cloneReadPointer(b.NodePhaseAt)
			cp.OccupancyObservedAt = cloneReadPointer(b.OccupancyObservedAt)
			row.Burst = &cp
		}
		rows = append(rows, row)
	}
	slices.SortFunc(rows, func(a, b WorkloadSnapshot) int {
		if byCreated := b.Workload.CreatedAt.Compare(a.Workload.CreatedAt); byCreated != 0 {
			return byCreated
		}
		return strings.Compare(a.Workload.ID, b.Workload.ID)
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func cloneReadPointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	cp := *value
	return &cp
}

func (p *pgPersister) readWorkloadSnapshots(ctx context.Context, customerID, workloadID string, limit int) ([]WorkloadSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// One SELECT gives the workload and its booking one MVCC snapshot, avoids
	// per-workload burst queries, and enforces both ownership predicates in SQL.
	rows, err := p.pool.Query(ctx, fmt.Sprintf(`
		SELECT w.data, b.data FROM %s w
		LEFT JOIN %s b ON b.id = w.data->>'BurstID' AND b.data->>'CustomerID' = $1
		WHERE w.data->>'CustomerID' = $1 AND ($2 = '' OR w.id = $2)
		ORDER BY COALESCE((w.data->>'CreatedAt')::timestamptz, '0001-01-01T00:00:00Z'::timestamptz) DESC, w.id ASC
		LIMIT $3`, tblWorkloads, tblBursts), customerID, workloadID, limit)
	if err != nil {
		return nil, fmt.Errorf("read workload snapshots: %w", err)
	}
	defer rows.Close()
	out := make([]WorkloadSnapshot, 0)
	for rows.Next() {
		var workloadData, burstData []byte
		if err := rows.Scan(&workloadData, &burstData); err != nil {
			return nil, fmt.Errorf("scan workload snapshot: %w", err)
		}
		row := WorkloadSnapshot{Workload: &Workload{}}
		if err := json.Unmarshal(workloadData, row.Workload); err != nil {
			return nil, fmt.Errorf("decode workload snapshot: %w", err)
		}
		if len(burstData) != 0 {
			row.Burst = &Burst{}
			if err := json.Unmarshal(burstData, row.Burst); err != nil {
				return nil, fmt.Errorf("decode burst snapshot: %w", err)
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read workload snapshots: %w", err)
	}
	return out, nil
}
