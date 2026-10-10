// Package scheduler picks the cheapest backend that satisfies a
// workload's resource requirements. Implementations are pure functions:
// same Requirements + same Candidates -> same Decision, every time.
package scheduler

// LatencyTier expresses how soon the user needs the workload running.
// Higher numerical values are stricter; backends are filtered by their
// EstimatedResumeMS / EstimatedColdStartMS.
type LatencyTier int

const (
	// LatencyAny accepts any cold-start time. Cheapest wins.
	LatencyAny LatencyTier = iota
	// LatencySubMinute requires the workload to be running within ~60s.
	// Excludes backends whose best-case (resume or cold) is over 60s.
	LatencySubMinute
	// LatencyInstant requires snapshot-restore (sub-second). Excludes
	// container-only or stop/start-style backends.
	LatencyInstant
)

// Requirements is what the user's workload needs. Hard constraints
// unless documented otherwise.
type Requirements struct {
	// CPUMillis is millicores: 1000 = 1 vCPU.
	CPUMillis int64
	// MemoryMB is requested memory in megabytes (binary, MiB).
	MemoryMB int64
	// GPU is non-nil when the workload needs a GPU.
	GPU *GPURequest
	// Region pins the workload to a backend region; empty = any.
	Region string
	// MaxUSDPerHour caps spend; 0 = no cap.
	MaxUSDPerHour float64
	// Latency is the strictness on acceptable cold-start time.
	Latency LatencyTier
}

// GPURequest specifies the GPU shape the workload needs.
type GPURequest struct {
	// Kind is the GPU SKU ("l4", "a100", "h100", "rtx4090") or "any".
	Kind string
	// Count is the number of GPUs requested (typically 1).
	Count int
}

// Capabilities describe what a backend can satisfy.
type Capabilities struct {
	// GPUKinds the backend can serve. Nil/empty = no GPU support.
	GPUKinds []string
	// MaxCPUMillis a single node can hold. 0 = effectively unlimited.
	MaxCPUMillis int64
	// MaxMemoryMB a single node can hold. 0 = effectively unlimited.
	MaxMemoryMB int64
	// Regions the backend operates in. Nil/empty = any region (e.g.
	// Linode/Fly don't always pin a region from our side).
	Regions []string
	// SnapshotRestore is true when Resume() restores from a memory
	// snapshot in <1s. False means cold boot.
	SnapshotRestore bool
	// EstimatedResumeMS is best-case time from Resume() to node Ready.
	EstimatedResumeMS int
	// EstimatedColdStartMS is best-case time from Provision() to node
	// Ready.
	EstimatedColdStartMS int
}

// Pricing is the per-resource cost model used by the scheduler. Same
// shape as config.BackendCost; copied here so the scheduler doesn't
// pull in pkg/config.
type Pricing struct {
	BaseUSDPerHour   float64            // flat base cost
	PerCPUUSDPerHour float64            // per vCPU
	PerGBUSDPerHour  float64            // per GiB
	GPURates         map[string]float64 // GPU kind -> $/hr
}

// Capacity is the backend's current free-slot accounting.
type Capacity struct {
	// AvailableSlots is the count of additional nodes the backend can
	// host right now. Ignored when Infinite is true.
	AvailableSlots int
	// Infinite is true for cloud backends with no per-host limit (Fly).
	// Self-hosted Firecracker / Linode-managed pools
	// will set finite slots.
	Infinite bool
}

// Candidate is a backend the scheduler can pick from. The wiring layer
// (cmd/yscale, pkg/controller) builds these from config + live backend
// state and hands them to a Scheduler.
type Candidate struct {
	Name         string
	Capabilities Capabilities
	Pricing      Pricing
	Capacity     Capacity
}

// Decision is the scheduler's output.
type Decision struct {
	// Backend is the chosen Candidate.Name.
	Backend string
	// EstUSDPerHour is the per-hour cost the scheduler computed for the
	// requested workload on this backend. Useful for budget tracking
	// and explaining cost to users.
	EstUSDPerHour float64
	// Reason is a human-readable rationale (used in logs and debug
	// output). Stable phrasing for a given inputs.
	Reason string
}

// Scheduler picks the best Candidate for a Requirements. Implementations
// must be deterministic and side-effect-free.
type Scheduler interface {
	Schedule(req Requirements) (*Decision, error)
}
