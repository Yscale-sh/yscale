package cost

import (
	"context"
	"fmt"
	"time"
)

// LifecycleProvisioningRecord is a provider-create operation that is in-flight
// or may hold a paid resource. Bounded non-secret fields only.
type LifecycleProvisioningRecord struct {
	BurstID   string
	CreatedAt time.Time
}

// LifecycleSource reads durable lifecycle provider-create operations that
// are in-flight or may hold a paid resource.
type LifecycleSource interface {
	LiveProvisioningOps(ctx context.Context) ([]LifecycleProvisioningRecord, error)
}

// StateProvisioningSource reads one internally consistent state/workload
// snapshot. knownBurstIDs contains every live state burst, including ones whose
// workload already started; the composite uses it to keep a retained lifecycle
// create row from resurrecting a completed start.
type StateProvisioningSource interface {
	LiveProvisioningState(ctx context.Context, now time.Time) (
		active int, oldestAge time.Duration, knownBurstIDs map[string]bool, err error,
	)
}

// CompositeSource combines lifecycle and state provisioning sources into a
// single ProvisioningSource. When lifecycle is nil, it behaves as the
// OSS-only source backed by state alone. If either configured durable source
// read fails, it returns an error so the collector publishes source_up 0.
//
// For a lifecycle record whose BurstID exists anywhere in the state durable
// live-burst set, state/workload truth decides whether it is still
// provisioning — the lifecycle record is not counted separately, because
// state already applied the workload boundary. For lifecycle records with
// no state burst booking (pre-booking lifecycle ops), they are counted and
// aged from lifecycle CreatedAt.
type CompositeSource struct {
	state     StateProvisioningSource
	lifecycle LifecycleSource
}

// NewCompositeSource creates a composite provisioning source.
// state is required. lifecycle may be nil for OSS.
func NewCompositeSource(state StateProvisioningSource, lifecycle LifecycleSource) *CompositeSource {
	return &CompositeSource{
		state:     state,
		lifecycle: lifecycle,
	}
}

// LiveProvisioning satisfies ProvisioningSource by combining state records
// with lifecycle records, deduplicating by BurstID.
func (c *CompositeSource) LiveProvisioning(ctx context.Context, now time.Time) (int, time.Duration, error) {
	if c == nil || c.state == nil {
		return 0, 0, fmt.Errorf("composite: state source is required")
	}
	stateActive, stateOldest, knownIDs, stateErr := c.state.LiveProvisioningState(ctx, now)
	if stateErr != nil {
		return 0, 0, fmt.Errorf("composite: state source: %w", stateErr)
	}

	if c.lifecycle == nil {
		return stateActive, stateOldest, nil
	}

	lifecycleOps, lcErr := c.lifecycle.LiveProvisioningOps(ctx)
	if lcErr != nil {
		return 0, 0, fmt.Errorf("composite: lifecycle source: %w", lcErr)
	}

	active := stateActive
	oldest := stateOldest
	lifecycleSeen := make(map[string]bool, len(lifecycleOps))

	for _, lc := range lifecycleOps {
		if knownIDs[lc.BurstID] {
			continue
		}
		if !lifecycleSeen[lc.BurstID] {
			lifecycleSeen[lc.BurstID] = true
			active++
		}
		if !lc.CreatedAt.IsZero() {
			if age := now.Sub(lc.CreatedAt); age > oldest {
				oldest = age
			}
		}
	}

	return active, oldest, nil
}
