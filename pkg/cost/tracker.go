// Package cost tracks USD spend for burst nodes provisioned by the autoscaler.
//
// The tracker is designed for homelab operators who want a clear daily/monthly
// spend readout and a hard cap to prevent runaway costs. State is held in
// memory only — restarts begin a fresh accrual window. This is acceptable for
// v1: the cap is a safety net, not a billing system.
//
// The package is standalone (stdlib only) and safe for concurrent use.
package cost

import (
	"sync"
	"time"
)

// Rates holds per-backend pricing information used to compute the USD/hour
// cost of a running node.
type Rates struct {
	Backends map[string]BackendRate
}

// BackendRate describes how a single backend prices a node. The hourly cost
// of a node is:
//
//	BaseUSDPerHour
//	  + (cpuMillis/1000) * PerCPUUSDPerHour
//	  + (memoryMB/1024) * PerGBUSDPerHour
//	  + GPURates[gpuKind]   (only if gpuKind != "")
type BackendRate struct {
	BaseUSDPerHour   float64
	PerCPUUSDPerHour float64
	PerGBUSDPerHour  float64
	// GPURates maps a GPU kind (e.g. "a100-40gb") to its USD/hour cost.
	GPURates map[string]float64
}

// Snapshot is a point-in-time view of tracker state. It includes the running
// (estimated) cost of all currently active nodes.
type Snapshot struct {
	MonthSpentUSD     float64
	MonthCapUSD       float64
	ActiveCount       int
	ActiveCostUSDHour float64
	CompletedRuns     int
}

type activeRun struct {
	nodeID     string
	backend    string
	cpuMillis  int64
	memoryMB   int64
	gpuKind    string
	startedAt  time.Time
	usdPerHour float64
}

type completedRun struct {
	nodeID    string
	backend   string
	startedAt time.Time
	endedAt   time.Time
	costUSD   float64
}

// historyCap is the maximum number of completed runs retained. The slice is
// truncated from the front when the cap is exceeded.
const historyCap = 1000

// Tracker accrues per-node cost, enforces an optional monthly cap, and
// produces snapshots for visibility. The zero value is not usable; call New.
type Tracker struct {
	mu sync.Mutex

	rates  Rates
	capUSD float64 // monthly cap in USD; 0 disables the cap

	monthSpentUSD float64
	monthStart    time.Time

	active  map[string]*activeRun
	history []completedRun

	// now is overridable for tests. In production it is time.Now.
	now func() time.Time
}

// New constructs a Tracker with the given rates and monthly cap. A cap of 0
// disables cap enforcement.
func New(rates Rates, monthlyCapUSD float64) *Tracker {
	now := time.Now()
	return &Tracker{
		rates:      rates,
		capUSD:     monthlyCapUSD,
		monthStart: monthStartOf(now),
		active:     make(map[string]*activeRun),
		now:        time.Now,
	}
}

// Start records the beginning of a billable run for nodeID. It is idempotent:
// calling Start again for an already-tracked node is a no-op.
func (t *Tracker) Start(nodeID, backend string, cpuMillis, memoryMB int64, gpuKind string) {
	if t == nil || nodeID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.active[nodeID]; exists {
		return
	}
	rate := t.rateForLocked(backend, cpuMillis, memoryMB, gpuKind)
	t.active[nodeID] = &activeRun{
		nodeID:     nodeID,
		backend:    backend,
		cpuMillis:  cpuMillis,
		memoryMB:   memoryMB,
		gpuKind:    gpuKind,
		startedAt:  t.now(),
		usdPerHour: rate,
	}
}

// Stop accrues the final cost for nodeID and removes it from the active set.
// It returns the cost of this run in USD. If nodeID is not tracked, Stop
// returns 0.
func (t *Tracker) Stop(nodeID string) float64 {
	if t == nil || nodeID == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	run, ok := t.active[nodeID]
	if !ok {
		return 0
	}
	delete(t.active, nodeID)

	now := t.now()
	hours := now.Sub(run.startedAt).Hours()
	if hours < 0 {
		hours = 0
	}
	cost := run.usdPerHour * hours

	t.maybeRolloverLocked(now)
	t.monthSpentUSD += cost

	t.history = append(t.history, completedRun{
		nodeID:    run.nodeID,
		backend:   run.backend,
		startedAt: run.startedAt,
		endedAt:   now,
		costUSD:   cost,
	})
	if len(t.history) > historyCap {
		// Drop oldest entries; copy to release memory of the underlying array.
		excess := len(t.history) - historyCap
		t.history = append(t.history[:0:0], t.history[excess:]...)
	}

	return cost
}

// MonthSpentUSD returns the total accrued cost for the current calendar month.
// It rolls the accumulator over if the calendar month has changed.
func (t *Tracker) MonthSpentUSD() float64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.maybeRolloverLocked(t.now())
	return t.monthSpentUSD
}

// CapReached reports whether the current month's accrued spend has met or
// exceeded the configured monthly cap. Returns false if no cap is set.
func (t *Tracker) CapReached() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.capUSD <= 0 {
		return false
	}
	t.maybeRolloverLocked(t.now())
	return t.monthSpentUSD >= t.capUSD
}

// Snapshot returns a consistent view of tracker state, including the running
// estimated cost of all active nodes (computed but not yet accrued).
func (t *Tracker) Snapshot() Snapshot {
	if t == nil {
		return Snapshot{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	t.maybeRolloverLocked(now)

	var activeRate float64
	var activeAccrual float64
	for _, run := range t.active {
		activeRate += run.usdPerHour
		hours := now.Sub(run.startedAt).Hours()
		if hours < 0 {
			hours = 0
		}
		activeAccrual += run.usdPerHour * hours
	}

	return Snapshot{
		MonthSpentUSD:     t.monthSpentUSD + activeAccrual,
		MonthCapUSD:       t.capUSD,
		ActiveCount:       len(t.active),
		ActiveCostUSDHour: activeRate,
		CompletedRuns:     len(t.history),
	}
}

// rateForLocked computes USD/hour for a node given its backend and shape.
// Caller must hold t.mu.
func (t *Tracker) rateForLocked(backend string, cpuMillis, memoryMB int64, gpuKind string) float64 {
	br, ok := t.rates.Backends[backend]
	if !ok {
		return 0
	}
	cpus := float64(cpuMillis) / 1000.0
	gb := float64(memoryMB) / 1024.0
	rate := br.BaseUSDPerHour + cpus*br.PerCPUUSDPerHour + gb*br.PerGBUSDPerHour
	if gpuKind != "" {
		if g, ok := br.GPURates[gpuKind]; ok {
			rate += g
		}
	}
	return rate
}

// maybeRolloverLocked resets the monthly accumulator at month boundaries.
// Caller must hold t.mu.
func (t *Tracker) maybeRolloverLocked(now time.Time) {
	if now.Year() != t.monthStart.Year() || now.Month() != t.monthStart.Month() {
		t.monthSpentUSD = 0
		t.monthStart = monthStartOf(now)
	}
}

// monthStartOf returns the first instant of the calendar month containing t,
// in t's location.
func monthStartOf(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
}
