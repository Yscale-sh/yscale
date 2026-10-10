package main

import (
	"testing"
	"time"
)

func TestReconcileAttemptTimeoutConstantIs30s(t *testing.T) {
	// The reconcile attempt is pinned to a single 30s constant reused across
	// the boot and periodic passes. Guard the value so a silent edit is caught.
	if reconcileAttemptTimeout != 30*time.Second {
		t.Fatalf("reconcileAttemptTimeout = %s, want 30s", reconcileAttemptTimeout)
	}
}
