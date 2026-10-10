package scheduler

import (
	"fmt"
	"sort"
)

// New returns a deterministic Scheduler that ranks the given candidates
// per Schedule() call. Order of `candidates` does not affect the
// Decision — viable candidates are sorted by (cost, name) so ties break
// alphabetically.
func New(candidates []Candidate) Scheduler {
	// Defensive copy so callers can mutate their slice without
	// affecting the scheduler.
	cp := make([]Candidate, len(candidates))
	copy(cp, candidates)
	return &greedy{candidates: cp}
}

type greedy struct {
	candidates []Candidate
}

// Schedule picks the cheapest candidate that satisfies req.
//
//  1. Filter: capacity > 0, GPU kind/count match, region match (if
//     pinned), latency tier OK, per-node max CPU/mem not exceeded.
//  2. Compute cost via estimateCost.
//  3. Filter by MaxUSDPerHour (if non-zero).
//  4. Sort ascending by (cost, name).
//  5. Pick the first.
func (s *greedy) Schedule(req Requirements) (*Decision, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}

	type scored struct {
		cand Candidate
		cost float64
	}
	viable := make([]scored, 0, len(s.candidates))

	var rejections []string
	for _, c := range s.candidates {
		if reason := disqualify(c, req); reason != "" {
			rejections = append(rejections, fmt.Sprintf("%s (%s)", c.Name, reason))
			continue
		}
		cost := estimateCost(c.Pricing, req)
		if req.MaxUSDPerHour > 0 && cost > req.MaxUSDPerHour {
			rejections = append(rejections, fmt.Sprintf("%s (cost $%.4f > cap $%.4f)", c.Name, cost, req.MaxUSDPerHour))
			continue
		}
		viable = append(viable, scored{c, cost})
	}

	if len(viable) == 0 {
		return nil, &NoViableBackendError{
			Requirements: req,
			Rejections:   rejections,
		}
	}

	sort.SliceStable(viable, func(i, j int) bool {
		if viable[i].cost != viable[j].cost {
			return viable[i].cost < viable[j].cost
		}
		return viable[i].cand.Name < viable[j].cand.Name
	})

	pick := viable[0]
	return &Decision{
		Backend:       pick.cand.Name,
		EstUSDPerHour: pick.cost,
		Reason:        fmt.Sprintf("cheapest of %d viable backends", len(viable)),
	}, nil
}

// NoViableBackendError is returned when no candidate can satisfy a
// Requirements. Callers can type-assert to inspect rejection reasons.
type NoViableBackendError struct {
	Requirements Requirements
	Rejections   []string // per-candidate explanation, in input order
}

func (e *NoViableBackendError) Error() string {
	if len(e.Rejections) == 0 {
		return "no backends configured"
	}
	return fmt.Sprintf("no backend satisfies requirements: %v", e.Rejections)
}

func validateRequest(req Requirements) error {
	if req.CPUMillis < 0 {
		return fmt.Errorf("cpuMillis must be >= 0, got %d", req.CPUMillis)
	}
	if req.MemoryMB < 0 {
		return fmt.Errorf("memoryMB must be >= 0, got %d", req.MemoryMB)
	}
	if req.GPU != nil && req.GPU.Count < 0 {
		return fmt.Errorf("gpu.count must be >= 0, got %d", req.GPU.Count)
	}
	if req.MaxUSDPerHour < 0 {
		return fmt.Errorf("maxUSDPerHour must be >= 0, got %v", req.MaxUSDPerHour)
	}
	return nil
}

// disqualify returns a non-empty reason when c can't satisfy req.
func disqualify(c Candidate, req Requirements) string {
	if !c.Capacity.Infinite && c.Capacity.AvailableSlots <= 0 {
		return "no capacity"
	}
	if c.Capabilities.MaxCPUMillis > 0 && req.CPUMillis > c.Capabilities.MaxCPUMillis {
		return fmt.Sprintf("cpu %dm > max %dm", req.CPUMillis, c.Capabilities.MaxCPUMillis)
	}
	if c.Capabilities.MaxMemoryMB > 0 && req.MemoryMB > c.Capabilities.MaxMemoryMB {
		return fmt.Sprintf("memory %dMB > max %dMB", req.MemoryMB, c.Capabilities.MaxMemoryMB)
	}
	if req.Region != "" && len(c.Capabilities.Regions) > 0 && !contains(c.Capabilities.Regions, req.Region) {
		return fmt.Sprintf("region %q not served", req.Region)
	}
	if req.GPU != nil && req.GPU.Count > 0 {
		if !gpuKindMatches(req.GPU.Kind, c.Capabilities.GPUKinds) {
			return fmt.Sprintf("gpu kind %q unsupported", req.GPU.Kind)
		}
	}
	switch req.Latency {
	case LatencyInstant:
		if !c.Capabilities.SnapshotRestore {
			return "no snapshot restore"
		}
	case LatencySubMinute:
		// best-case is min(resume, coldStart). If both exceed 60s
		// the backend is too slow.
		fastest := c.Capabilities.EstimatedColdStartMS
		if c.Capabilities.EstimatedResumeMS > 0 && c.Capabilities.EstimatedResumeMS < fastest {
			fastest = c.Capabilities.EstimatedResumeMS
		}
		if fastest > 60_000 {
			return fmt.Sprintf("best-case start %dms > 60s", fastest)
		}
	}
	return ""
}

func gpuKindMatches(want string, supported []string) bool {
	if want == "" || want == "any" {
		return len(supported) > 0
	}
	return contains(supported, want)
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// estimateCost computes the per-hour cost of running a workload of
// shape req on a backend with the given Pricing. Pure function; same
// inputs -> same output (subject to floating-point determinism, which
// is fine for our scoring purposes).
func estimateCost(p Pricing, req Requirements) float64 {
	cpus := float64(req.CPUMillis) / 1000.0
	gigs := float64(req.MemoryMB) / 1024.0
	cost := p.BaseUSDPerHour + cpus*p.PerCPUUSDPerHour + gigs*p.PerGBUSDPerHour
	if req.GPU != nil && req.GPU.Count > 0 && p.GPURates != nil {
		if rate, ok := p.GPURates[req.GPU.Kind]; ok {
			cost += rate * float64(req.GPU.Count)
		}
	}
	return cost
}
