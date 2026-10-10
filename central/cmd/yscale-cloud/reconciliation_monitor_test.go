package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/decider"
	"github.com/yscale-sh/yscale/central/internal/handlers"
)

// atomicClock is a race-safe fake clock. Tests advance it from one goroutine
// while the monitor's watchdog reads from another.
type atomicClock struct {
	ns atomic.Int64
}

func newAtomicClock(base time.Time) *atomicClock {
	c := &atomicClock{}
	c.ns.Store(base.UnixNano())
	return c
}

func (c *atomicClock) now() time.Time { return time.Unix(0, c.ns.Load()) }

func (c *atomicClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// newTestGate returns an enabled PaidGate — the state under which SetReady /
// ClearReady have any effect. Tests inspect its Status() missing list to prove
// prerequisite transitions without depending on other prereqs being set.
func newTestGate() *handlers.PaidGate {
	g := handlers.NewPaidGate()
	g.Enable()
	return g
}

// prereqMissing reports whether a named prerequisite appears in the gate's
// missing list.
func prereqMissing(t *testing.T, gate *handlers.PaidGate, name string) bool {
	t.Helper()
	_, _, missing := gate.Status()
	for _, m := range missing {
		if m == name {
			return true
		}
	}
	return false
}

// newMonitorAt builds a monitor whose clock reads the value at *nowPtr.
// Reassigning *nowPtr advances observed time without any sleeps.
func newMonitorAt(gate *handlers.PaidGate, stale time.Duration, nowPtr *time.Time) *reconcileMonitor {
	return &reconcileMonitor{
		gate:           gate,
		now:            func() time.Time { return *nowPtr },
		staleThreshold: stale,
	}
}

func TestReconcileMonitorInitialStateBothPrereqsClosed(t *testing.T) {
	gate := newTestGate()
	now := time.Unix(1_700_000_000, 0)
	_ = newMonitorAt(gate, time.Minute, &now)
	if !prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation should start missing")
	}
	if !prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor should start missing")
	}
}

func TestReconcileMonitorZeroTargetsFullSuccessOpensBoth(t *testing.T) {
	gate := newTestGate()
	now := time.Unix(1_700_000_000, 0)
	m := newMonitorAt(gate, time.Minute, &now)

	m.Observe(decider.ProviderReconcileSummary{Targets: 0}, nil)

	if prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation should be set after zero-target healthy pass")
	}
	if prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor should be set after zero-target healthy pass")
	}
}

func TestReconcileMonitorFailureBeforeAnySuccessStaysClosed(t *testing.T) {
	gate := newTestGate()
	now := time.Unix(1_700_000_000, 0)
	m := newMonitorAt(gate, time.Minute, &now)

	m.Observe(decider.ProviderReconcileSummary{Targets: 2, Failed: 1}, nil)
	m.Observe(decider.ProviderReconcileSummary{}, errors.New("provider timeout"))

	if !prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation must not open before a healthy pass")
	}
	if !prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor must not open before a healthy pass")
	}
}

func TestReconcileMonitorSuccessAfterFailureOpensBoth(t *testing.T) {
	gate := newTestGate()
	now := time.Unix(1_700_000_000, 0)
	m := newMonitorAt(gate, time.Minute, &now)

	m.Observe(decider.ProviderReconcileSummary{Targets: 3, Failed: 3}, errors.New("all providers down"))
	if !prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("monitor should be missing after failure")
	}

	now = now.Add(10 * time.Second)
	m.Observe(decider.ProviderReconcileSummary{Targets: 3, Observed: 5}, nil)

	if prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation should open on first healthy pass")
	}
	if prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor should open on first healthy pass")
	}
}

func TestReconcileMonitorLaterFailureClearsOnlyMonitor(t *testing.T) {
	gate := newTestGate()
	now := time.Unix(1_700_000_000, 0)
	m := newMonitorAt(gate, time.Minute, &now)

	m.Observe(decider.ProviderReconcileSummary{Targets: 2}, nil)
	if prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation should be set")
	}
	if prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor should be set")
	}

	m.Observe(decider.ProviderReconcileSummary{Targets: 2, Failed: 1}, nil)

	if prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation must remain set after later failure (monotonic)")
	}
	if !prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor must clear on failed pass")
	}

	// An error return alone also clears only the monitor.
	m.Observe(decider.ProviderReconcileSummary{Targets: 2}, nil)
	m.Observe(decider.ProviderReconcileSummary{}, errors.New("boom"))

	if prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation must remain set after error pass (monotonic)")
	}
	if !prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor must clear on error pass")
	}
}

func TestReconcileMonitorStaleIndependentCheckClearsMonitor(t *testing.T) {
	gate := newTestGate()
	now := time.Unix(1_700_000_000, 0)
	stale := 100 * time.Millisecond
	m := newMonitorAt(gate, stale, &now)

	m.Observe(decider.ProviderReconcileSummary{Targets: 1}, nil)
	if prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("monitor should be set after healthy pass")
	}

	// Advance past the stale threshold WITHOUT any new Observe call — this is
	// the hung-provider scenario the watchdog exists to catch.
	now = now.Add(stale + time.Second)
	m.CheckStaleness()

	if !prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor must clear when last success ages past threshold")
	}
	if prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation must never be cleared by staleness")
	}
}

func TestReconcileMonitorFreshCheckStaysOpen(t *testing.T) {
	gate := newTestGate()
	now := time.Unix(1_700_000_000, 0)
	stale := time.Hour
	m := newMonitorAt(gate, stale, &now)

	m.Observe(decider.ProviderReconcileSummary{Targets: 1}, nil)
	now = now.Add(time.Second)
	m.CheckStaleness()

	if prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor must remain set inside stale threshold")
	}
	if prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation must remain set inside stale threshold")
	}
}

func TestReconcileMonitorRecoveryAfterStale(t *testing.T) {
	gate := newTestGate()
	now := time.Unix(1_700_000_000, 0)
	stale := 50 * time.Millisecond
	m := newMonitorAt(gate, stale, &now)

	m.Observe(decider.ProviderReconcileSummary{Targets: 1}, nil)
	now = now.Add(stale + time.Second)
	m.CheckStaleness()
	if !prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("monitor should be cleared by staleness")
	}

	// A fresh healthy pass must reopen the monitor prereq.
	now = now.Add(time.Second)
	m.Observe(decider.ProviderReconcileSummary{Targets: 1}, nil)

	if prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor must reopen on fresh healthy pass")
	}
	if prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation must still be set after recovery")
	}
}

func TestReconcileMonitorCheckStalenessBeforeAnySuccessIsNoop(t *testing.T) {
	gate := newTestGate()
	now := time.Unix(1_700_000_000, 0)
	m := newMonitorAt(gate, time.Nanosecond, &now)

	m.CheckStaleness()
	m.CheckStaleness()

	// Nothing has been observed yet: CheckStaleness must not clear a prereq
	// that Observe never opened, and must never open one either.
	if !prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation must remain unset when no pass has run")
	}
	if !prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("reconciliation_monitor must remain unset when no pass has run")
	}
}

func TestReconcileMonitorNilReceiverIsNoop(t *testing.T) {
	var m *reconcileMonitor
	// Boot uses `reconcileMon.Observe(...)` even when the monitor is not
	// wired (paidGate nil). Method must not panic.
	m.Observe(decider.ProviderReconcileSummary{}, nil)
	m.CheckStaleness()
	m.RunWatchdog(context.Background(), time.Millisecond)
}

func TestEffectiveStaleThresholdAndWatchdogCadenceDefaults(t *testing.T) {
	// Sane inputs: 2*interval + timeout, and min(interval, timeout).
	if got := effectiveStaleThreshold(10*time.Minute, 30*time.Second); got != 20*time.Minute+30*time.Second {
		t.Fatalf("stale threshold = %s, want 20m30s", got)
	}
	if got := watchdogCadence(10*time.Minute, 30*time.Second); got != 30*time.Second {
		t.Fatalf("watchdog cadence = %s, want 30s", got)
	}
	// Non-positive inputs must fall back to defaults, not produce zero or
	// negative durations.
	if got := effectiveStaleThreshold(0, 0); got <= 0 {
		t.Fatalf("stale threshold with zero inputs must be positive, got %s", got)
	}
	if got := watchdogCadence(0, 0); got <= 0 {
		t.Fatalf("watchdog cadence with zero inputs must be positive, got %s", got)
	}
	if got := effectiveStaleThreshold(-1, -1); got <= 0 {
		t.Fatalf("stale threshold with negative inputs must be positive, got %s", got)
	}
	// interval < timeout: cadence should be the smaller.
	if got := watchdogCadence(5*time.Second, 30*time.Second); got != 5*time.Second {
		t.Fatalf("cadence with interval<timeout = %s, want 5s", got)
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if got := effectiveStaleThreshold(maxDuration, time.Second); got != maxDuration {
		t.Fatalf("overflowing stale threshold = %s, want saturation at %s", got, maxDuration)
	}
}

func TestReconcileAttemptTimeoutConstantIs30s(t *testing.T) {
	// The task pins the reconcile attempt to a single 30s constant reused
	// across boot, periodic, monitor construction, and watchdog cadence
	// derivation. Guard the value so a silent edit is caught.
	if reconcileAttemptTimeout != 30*time.Second {
		t.Fatalf("reconcileAttemptTimeout = %s, want 30s", reconcileAttemptTimeout)
	}
}

func TestReconcileMonitorObserveAndCheckStalenessAreRaceSafe(t *testing.T) {
	gate := newTestGate()
	now := time.Unix(1_700_000_000, 0)
	m := newMonitorAt(gate, time.Millisecond, &now)

	const workers = 16
	const iters = 500
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				switch (i + j) % 3 {
				case 0:
					m.Observe(decider.ProviderReconcileSummary{Targets: 1}, nil)
				case 1:
					m.Observe(decider.ProviderReconcileSummary{Targets: 1, Failed: 1}, nil)
				case 2:
					m.CheckStaleness()
				}
			}
		}(i)
	}
	wg.Wait()

	// The final state depends on which goroutine won the last write, but the
	// monotonic prereq must be set — a healthy pass ran somewhere in the mix.
	if prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("provider_reconciliation should be set after at least one healthy Observe")
	}
}

func TestReconcileMonitorWatchdogExitsOnContextCancel(t *testing.T) {
	gate := newTestGate()
	clk := newAtomicClock(time.Unix(1_700_000_000, 0))
	m := &reconcileMonitor{
		gate:           gate,
		now:            clk.now,
		staleThreshold: 10 * time.Millisecond,
	}
	m.Observe(decider.ProviderReconcileSummary{Targets: 1}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.RunWatchdog(ctx, 5*time.Millisecond)
		close(done)
	}()

	// Advance observed time past the stale threshold so the watchdog clears the
	// monitor prereq. Poll with a generous bound instead of relying on a fixed
	// sleep that can flake on a loaded self-hosted runner.
	clk.advance(time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for !prereqMissing(t, gate, "reconciliation_monitor") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !prereqMissing(t, gate, "reconciliation_monitor") {
		cancel()
		<-done
		t.Fatal("watchdog did not clear monitor within 2s")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not exit within 2s of cancel")
	}
	if !prereqMissing(t, gate, "reconciliation_monitor") {
		t.Fatal("watchdog should have cleared monitor after stale window elapsed")
	}
	if prereqMissing(t, gate, "provider_reconciliation") {
		t.Fatal("watchdog must never clear provider_reconciliation")
	}
}

// Source-wiring guard: prove Observe is called after both the boot and periodic
// ReconcileProviders returns, that the watchdog is started, and that Disable
// runs on shutdown before srv.Shutdown. A silent removal would otherwise reduce
// the monitor to a decorative type with no runtime consumer.
func TestReconciliationMonitorWiredInMain(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	wants := []string{
		"reconcileMon.RunWatchdog(",
		"paidGate.Disable()",
		"effectiveReconcileInterval()",
		"reconcileAttemptTimeout",
	}
	for _, w := range wants {
		if !containsStr(text, w) {
			t.Fatalf("main.go missing wiring token %q", w)
		}
	}
	if got := strings.Count(text, "reconcileMon.Observe("); got != 2 {
		t.Fatalf("main.go has %d reconciliation observations, want boot and periodic", got)
	}
	disableAt := strings.Index(text, "paidGate.Disable()")
	shutdownAt := strings.Index(text, "srv.Shutdown(shutdown)")
	if disableAt < 0 || shutdownAt < 0 || disableAt > shutdownAt {
		t.Fatal("paid gate must disable before HTTP shutdown")
	}
}
