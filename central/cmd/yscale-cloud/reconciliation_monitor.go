package main

import (
	"time"
)

// reconcileAttemptTimeout bounds a single ReconcileProviders call, so a hung
// provider call cannot stall the boot pass or a periodic tick indefinitely.
const reconcileAttemptTimeout = 30 * time.Second

// effectiveReconcileInterval reads the reconciliation interval from env with a
// clamp so 0/negative values fall back to 10m. Called once before boot so the
// same value drives the periodic worker and logging.
func effectiveReconcileInterval() time.Duration {
	interval := envDuration("YSCALE_PROVIDER_RECONCILE_INTERVAL",
		envDuration("YSCALE_ORPHAN_SWEEP_INTERVAL", 10*time.Minute))
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	return interval
}
