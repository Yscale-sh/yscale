package state

import (
	"context"
	"errors"
	"time"
)

// LiveProvisioning reports how many bursts are in active provisioning RIGHT
// NOW, and how long the oldest of them has been there.
//
// A burst's active-provisioning boundary depends on its kind:
//
// Managed bursts (NodeOnly == false): active while the associated workload is
// nonterminal and has not started (StartedAt == nil), regardless of burst
// phase. This matches the completed histogram's success boundary
// (Workload.StartedAt). A missing workload immediately after burst booking is
// conservatively active. A durable workload lookup error makes the whole view
// unknown.
//
// NodeOnly bursts: active only while the node has not reported Ready —
// provisioning and joining phases count; running, degraded, and removed do
// not. There is no managed workload /started report to wait for.
//
// The set is DURABLE state when there is a backend. A burst another replica
// created was never in this process's map, and a stuck-provisioning signal
// that can only see one replica's creates is not a fleet signal at all.
// ok=false is "no durable backend" — the in-memory/OSS store, where one
// process exists and its map IS the burst set.
//
// An error is never a zero. A durable read that failed means the live set is
// unknown, not empty.
//
// now is passed in rather than read here so the caller owns the clock.
//
// Ages are never fabricated. A record with no CreatedAt is counted but
// contributes no age. A CreatedAt in the future yields a negative age,
// which is discarded.
func (s *Store) LiveProvisioning(ctx context.Context, now time.Time) (active int, oldestAge time.Duration, err error) {
	active, oldestAge, _, err = s.LiveProvisioningState(ctx, now)
	return active, oldestAge, err
}

// LiveProvisioningState returns the active summary and every live state burst
// ID from one burst-list read. The composite source needs the complete ID set,
// not only active IDs: a retained succeeded lifecycle operation must not
// resurrect a burst whose workload has already started.
func (s *Store) LiveProvisioningState(ctx context.Context, now time.Time) (
	active int, oldestAge time.Duration, knownBurstIDs map[string]bool, err error,
) {
	bursts, durable, err := s.DurableBursts(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	if !durable {
		bursts = s.ListBursts()
	}

	knownBurstIDs = make(map[string]bool, len(bursts))
	for _, b := range bursts {
		if b == nil {
			continue
		}
		knownBurstIDs[b.ID] = true

		isActive, lookupErr := s.isBurstActivelyProvisioning(ctx, b, durable)
		if lookupErr != nil {
			return 0, 0, nil, lookupErr
		}
		if !isActive {
			continue
		}
		active++
		if !b.CreatedAt.IsZero() {
			if age := now.Sub(b.CreatedAt); age > oldestAge {
				oldestAge = age
			}
		}
	}
	return active, oldestAge, knownBurstIDs, nil
}

// isBurstActivelyProvisioning decides whether a burst is still in the
// paid-provisioning interval based on its kind.
func (s *Store) isBurstActivelyProvisioning(ctx context.Context, b *Burst, durable bool) (bool, error) {
	if b.NodeOnly {
		return isNodeOnlyActive(b), nil
	}
	return s.isManagedBurstActive(ctx, b, durable)
}

// isNodeOnlyActive: provisioning and joining count; running/degraded/removed
// do not. No managed workload /started report exists for nodeOnly.
func isNodeOnlyActive(b *Burst) bool {
	switch b.Status {
	case BurstStatusProvisioning, BurstStatusJoining:
		return true
	default:
		return false
	}
}

// isManagedBurstActive: active while the workload is nonterminal and
// StartedAt is nil. A missing workload immediately after booking is
// conservatively active.
func (s *Store) isManagedBurstActive(ctx context.Context, b *Burst, durable bool) (bool, error) {
	wl, err := s.lookupWorkloadForBurst(ctx, b.ID, durable)
	if err != nil {
		return false, err
	}
	if wl == nil {
		return true, nil
	}
	if wl.FinishedAt != nil {
		return false, nil
	}
	switch wl.Status {
	case "succeeded", "failed", "cancelled", "evicted":
		return false, nil
	}
	return wl.StartedAt == nil, nil
}

// lookupWorkloadForBurst resolves a burst to its workload, using the durable
// persister seam when a backend exists and the in-memory map otherwise.
func (s *Store) lookupWorkloadForBurst(ctx context.Context, burstID string, durable bool) (*Workload, error) {
	if !durable {
		wl, err := s.WorkloadByBurst(burstID)
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return wl, err
	}

	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return nil, errors.New("state: durable workload source is unavailable")
	}
	return p.workloadByBurst(ctx, burstID)
}
