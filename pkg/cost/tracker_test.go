package cost

import (
	"math"
	"testing"
	"time"
)

func flyRates() Rates {
	return Rates{
		Backends: map[string]BackendRate{
			"flyio": {
				BaseUSDPerHour:   0,
				PerCPUUSDPerHour: 0.008,
				PerGBUSDPerHour:  0.007,
				GPURates: map[string]float64{
					"a100-40gb": 1.50,
				},
			},
		},
	}
}

// withClock swaps the tracker's clock for deterministic tests.
func withClock(t *Tracker, now func() time.Time) {
	t.now = now
}

func approx(a, b, eps float64) bool {
	return math.Abs(a-b) <= eps
}

func TestStartStopAccruesCost(t *testing.T) {
	tr := New(flyRates(), 0)

	clock := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	withClock(tr, func() time.Time { return clock })
	tr.monthStart = monthStartOf(clock)

	tr.Start("m1", "flyio", 2000, 4096, "")
	clock = clock.Add(30 * time.Minute)
	cost := tr.Stop("m1")

	// 2 CPU * 0.008 + 4 GB * 0.007 = 0.016 + 0.028 = 0.044 USD/hour.
	// Half an hour: 0.022 USD.
	if !approx(cost, 0.022, 1e-9) {
		t.Fatalf("expected ~0.022 USD, got %v", cost)
	}
	if !approx(tr.MonthSpentUSD(), 0.022, 1e-9) {
		t.Fatalf("expected month spent 0.022, got %v", tr.MonthSpentUSD())
	}
}

func TestStartIdempotent(t *testing.T) {
	tr := New(flyRates(), 0)
	clock := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	withClock(tr, func() time.Time { return clock })
	tr.monthStart = monthStartOf(clock)

	tr.Start("m1", "flyio", 2000, 4096, "")
	clock = clock.Add(time.Hour)
	tr.Start("m1", "flyio", 2000, 4096, "") // should be a no-op
	clock = clock.Add(time.Hour)

	cost := tr.Stop("m1")
	// Two hours total at 0.044 USD/hour = 0.088 USD.
	if !approx(cost, 0.088, 1e-9) {
		t.Fatalf("expected 0.088 (start treated as idempotent), got %v", cost)
	}
}

func TestStopUnknownReturnsZero(t *testing.T) {
	tr := New(flyRates(), 0)
	if got := tr.Stop("ghost"); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}

func TestCapReached(t *testing.T) {
	tr := New(flyRates(), 0.05)
	clock := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	withClock(tr, func() time.Time { return clock })
	tr.monthStart = monthStartOf(clock)

	if tr.CapReached() {
		t.Fatal("cap should not be reached yet")
	}

	tr.Start("m1", "flyio", 2000, 4096, "")
	clock = clock.Add(2 * time.Hour) // 0.088 USD > 0.05 cap
	tr.Stop("m1")

	if !tr.CapReached() {
		t.Fatalf("cap should be reached: spent=%v cap=0.05", tr.MonthSpentUSD())
	}
}

func TestCapDisabledWhenZero(t *testing.T) {
	tr := New(flyRates(), 0)
	clock := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	withClock(tr, func() time.Time { return clock })
	tr.monthStart = monthStartOf(clock)

	tr.Start("m1", "flyio", 2000, 4096, "")
	clock = clock.Add(100 * time.Hour)
	tr.Stop("m1")

	if tr.CapReached() {
		t.Fatal("cap=0 should never be reached")
	}
}

func TestMonthRollover(t *testing.T) {
	tr := New(flyRates(), 0)
	clock := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	withClock(tr, func() time.Time { return clock })
	tr.monthStart = monthStartOf(clock)

	tr.Start("m1", "flyio", 2000, 4096, "")
	clock = clock.Add(time.Hour)
	tr.Stop("m1")

	if tr.MonthSpentUSD() <= 0 {
		t.Fatal("expected non-zero spend")
	}

	// Jump to next month.
	clock = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if got := tr.MonthSpentUSD(); got != 0 {
		t.Fatalf("expected month rollover to reset spend, got %v", got)
	}
	if tr.monthStart.Month() != time.June {
		t.Fatalf("expected monthStart to roll to June, got %v", tr.monthStart.Month())
	}
}

func TestSnapshotIncludesActiveAccrual(t *testing.T) {
	tr := New(flyRates(), 0)
	clock := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	withClock(tr, func() time.Time { return clock })
	tr.monthStart = monthStartOf(clock)

	tr.Start("m1", "flyio", 2000, 4096, "")
	clock = clock.Add(30 * time.Minute)

	snap := tr.Snapshot()
	if snap.ActiveCount != 1 {
		t.Fatalf("expected 1 active, got %d", snap.ActiveCount)
	}
	if !approx(snap.ActiveCostUSDHour, 0.044, 1e-9) {
		t.Fatalf("expected USD/hour=0.044, got %v", snap.ActiveCostUSDHour)
	}
	// Accrued: 0.022 USD (half an hour at 0.044/hour).
	if !approx(snap.MonthSpentUSD, 0.022, 1e-9) {
		t.Fatalf("expected accrued 0.022, got %v", snap.MonthSpentUSD)
	}
}

func TestGPURate(t *testing.T) {
	tr := New(flyRates(), 0)
	clock := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	withClock(tr, func() time.Time { return clock })
	tr.monthStart = monthStartOf(clock)

	tr.Start("m1", "flyio", 1000, 1024, "a100-40gb")
	clock = clock.Add(time.Hour)
	cost := tr.Stop("m1")

	// 1 CPU * 0.008 + 1 GB * 0.007 + 1.50 GPU = 1.515 USD/hour for 1 hour.
	if !approx(cost, 1.515, 1e-9) {
		t.Fatalf("expected GPU run cost 1.515, got %v", cost)
	}
}

func TestUnknownBackendIsZeroRate(t *testing.T) {
	tr := New(flyRates(), 0)
	clock := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	withClock(tr, func() time.Time { return clock })
	tr.monthStart = monthStartOf(clock)

	tr.Start("m1", "unknown", 2000, 4096, "")
	clock = clock.Add(time.Hour)
	if got := tr.Stop("m1"); got != 0 {
		t.Fatalf("expected 0 cost for unknown backend, got %v", got)
	}
}

func TestHistoryTruncated(t *testing.T) {
	tr := New(flyRates(), 0)
	clock := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	withClock(tr, func() time.Time { return clock })
	tr.monthStart = monthStartOf(clock)

	for i := 0; i < historyCap+50; i++ {
		id := "m" + itoa(i)
		tr.Start(id, "flyio", 1000, 1024, "")
		clock = clock.Add(time.Minute)
		tr.Stop(id)
	}
	if got := tr.Snapshot().CompletedRuns; got != historyCap {
		t.Fatalf("expected history capped at %d, got %d", historyCap, got)
	}
}

// itoa avoids strconv to keep the test self-contained.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// TestNilTrackerSafe verifies all public methods are safe on a nil receiver,
// which the controller relies on when costs is unconfigured.
func TestNilTrackerSafe(t *testing.T) {
	var tr *Tracker
	tr.Start("m1", "flyio", 1000, 1024, "")
	if got := tr.Stop("m1"); got != 0 {
		t.Fatalf("expected 0 from nil tracker Stop, got %v", got)
	}
	if got := tr.MonthSpentUSD(); got != 0 {
		t.Fatalf("expected 0 from nil tracker MonthSpentUSD, got %v", got)
	}
	if tr.CapReached() {
		t.Fatal("nil tracker should not report cap reached")
	}
	if got := tr.Snapshot(); got != (Snapshot{}) {
		t.Fatalf("expected zero snapshot, got %+v", got)
	}
}
