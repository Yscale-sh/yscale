package state

import (
	"context"
	"math"
	"time"
)

// BurstGPUTelemetryUpdate is one validated GPU utilisation sample, scoped to
// the tenant, cluster, and node the connector authenticated as.
type BurstGPUTelemetryUpdate struct {
	BurstID     string
	CustomerID  string
	ClusterID   string
	NodeName    string
	Utilization float64
	ObservedAt  time.Time
}

// ValidBurstGPUTelemetry checks identity and numeric bounds: all identity
// fields nonempty, utilization in [0,100], finite, observation not zero.
func ValidBurstGPUTelemetry(u BurstGPUTelemetryUpdate) bool {
	if u.BurstID == "" || u.CustomerID == "" || u.ClusterID == "" || u.NodeName == "" {
		return false
	}
	if u.ObservedAt.IsZero() {
		return false
	}
	if math.IsNaN(u.Utilization) || math.IsInf(u.Utilization, 0) {
		return false
	}
	if u.Utilization < 0 || u.Utilization > 100 {
		return false
	}
	return true
}

// UpdateBurstGPUTelemetry records one GPU utilisation sample on an EXISTING
// burst. It is UPDATE-only and can never INSERT or resurrect a missing burst.
//
// The update is tenant+cluster scoped: the burst must belong to the
// authenticated connector's customer AND cluster. A sample for an unknown,
// reaped, or another tenant's burst is silently refused (applied=false).
//
// Freshness: the observation must be strictly newer than what is already
// stored. A stale or replayed sample is refused.
func (s *Store) UpdateBurstGPUTelemetry(ctx context.Context, u BurstGPUTelemetryUpdate) (bool, error) {
	if !ValidBurstGPUTelemetry(u) {
		return false, nil
	}
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return s.updateBurstGPUTelemetryInMemory(u), nil
	}
	applied, err := p.updateBurstGPUTelemetry(ctx, u)
	if err != nil {
		s.recordPersistenceFailure("burst_gpu_telemetry", "update", err)
		return applied, err
	}
	if !applied {
		return applied, err
	}
	// Refresh the local copy from the update, only if this replica still
	// holds the burst. Inserting here would re-create a burst a concurrent
	// claim has removed.
	observed := u.ObservedAt.UTC()
	s.mu.Lock()
	if b, live := s.bursts[u.BurstID]; live {
		cp := *b
		cp.GPUUtilPercent = u.Utilization
		cp.LastHeartbeatAt = &observed
		s.bursts[u.BurstID] = &cp
	}
	s.mu.Unlock()
	return true, nil
}

func (s *Store) updateBurstGPUTelemetryInMemory(u BurstGPUTelemetryUpdate) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bursts[u.BurstID]
	if !ok {
		return false
	}
	if b.CustomerID != u.CustomerID || b.ClusterID != u.ClusterID || b.NodeName != u.NodeName {
		return false
	}
	observed := u.ObservedAt.UTC()
	if b.LastHeartbeatAt != nil && !observed.After(*b.LastHeartbeatAt) {
		return false
	}
	cp := *b
	cp.GPUUtilPercent = u.Utilization
	cp.LastHeartbeatAt = &observed
	s.bursts[u.BurstID] = &cp
	return true
}
