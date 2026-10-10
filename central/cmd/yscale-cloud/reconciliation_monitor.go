package main

import (
	"context"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/central/internal/decider"
	"github.com/yscale-sh/yscale/central/internal/handlers"
)

// reconcileAttemptTimeout bounds a single ReconcileProviders call. The monitor
// derives its stale threshold and watchdog cadence from this constant so a hung
// provider call cannot silently hold readiness open past what one attempt could
// possibly explain.
const reconcileAttemptTimeout = 30 * time.Second

// reconcileMonitor tracks the outcome of provider-reconciliation passes and
// drives the two reconciliation prerequisites on a *handlers.PaidGate.
//
// ProviderReconciliation is monotonic: the first fully healthy pass opens it and
// nothing here ever clears it. ReconciliationMonitor is staleness-sensitive:
// any failed pass or an aging last-success clears it immediately. The gate may
// be nil in tests that only exercise internal accounting.
type reconcileMonitor struct {
	mu             sync.Mutex
	gate           *handlers.PaidGate
	now            func() time.Time
	lastSuccess    time.Time
	everSucceeded  bool
	staleThreshold time.Duration
}

// newReconcileMonitor constructs the monitor with the derived stale threshold.
// interval is the effective reconciliation interval; attemptTimeout is the
// per-attempt timeout.
func newReconcileMonitor(gate *handlers.PaidGate, interval, attemptTimeout time.Duration) *reconcileMonitor {
	return &reconcileMonitor{
		gate:           gate,
		now:            func() time.Time { return time.Now() },
		staleThreshold: effectiveStaleThreshold(interval, attemptTimeout),
	}
}

// effectiveStaleThreshold returns 2*interval + attemptTimeout, falling back to
// safe defaults on invalid inputs so time arithmetic can never produce a
// non-positive threshold that would either fire on every tick or never fire.
func effectiveStaleThreshold(interval, attemptTimeout time.Duration) time.Duration {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	if attemptTimeout <= 0 {
		attemptTimeout = reconcileAttemptTimeout
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if interval > (maxDuration-attemptTimeout)/2 {
		return maxDuration
	}
	return 2*interval + attemptTimeout
}

// watchdogCadence returns min(interval, attemptTimeout). The floor guarantees
// the watchdog fires often enough to see a hung provider call as stale rather
// than waiting a full reconciliation interval past the deadline.
func watchdogCadence(interval, attemptTimeout time.Duration) time.Duration {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	if attemptTimeout <= 0 {
		attemptTimeout = reconcileAttemptTimeout
	}
	if interval < attemptTimeout {
		return interval
	}
	return attemptTimeout
}

// Observe records the outcome of one reconciliation pass. A pass is fully
// healthy iff err == nil and summary.Failed == 0; Targets == 0 is vacuously
// healthy and opens the gate. Any unhealthy pass clears only the monitor
// prerequisite — never ProviderReconciliation, which is monotonic.
func (m *reconcileMonitor) Observe(summary decider.ProviderReconcileSummary, err error) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil || summary.Failed > 0 {
		if m.gate != nil {
			m.gate.ClearReady(handlers.PrereqReconciliationMonitor)
		}
		return
	}
	if !m.everSucceeded {
		m.everSucceeded = true
		if m.gate != nil {
			m.gate.SetReady(handlers.PrereqProviderReconciliation)
		}
	}
	m.lastSuccess = m.now()
	if m.gate != nil {
		m.gate.SetReady(handlers.PrereqReconciliationMonitor)
	}
}

// CheckStaleness clears the monitor prerequisite when the last successful pass
// is older than the stale threshold. It runs independently of the reconciliation
// ticker so a hung ReconcileProviders call cannot keep readiness open — the
// last-success timestamp only advances via Observe.
func (m *reconcileMonitor) CheckStaleness() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.everSucceeded || m.staleThreshold <= 0 {
		return
	}
	if m.now().Sub(m.lastSuccess) > m.staleThreshold {
		if m.gate != nil {
			m.gate.ClearReady(handlers.PrereqReconciliationMonitor)
		}
	}
}

// RunWatchdog fires CheckStaleness on the given cadence until ctx is done.
func (m *reconcileMonitor) RunWatchdog(ctx context.Context, cadence time.Duration) {
	if m == nil || cadence <= 0 {
		return
	}
	t := time.NewTicker(cadence)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.CheckStaleness()
		}
	}
}

// effectiveReconcileInterval reads the reconciliation interval from env with a
// clamp so 0/negative values fall back to 10m. Called once before boot so the
// same value drives monitor construction, the periodic worker, and logging.
func effectiveReconcileInterval() time.Duration {
	interval := envDuration("YSCALE_PROVIDER_RECONCILE_INTERVAL",
		envDuration("YSCALE_ORPHAN_SWEEP_INTERVAL", 10*time.Minute))
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	return interval
}
