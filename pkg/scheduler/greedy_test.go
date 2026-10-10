package scheduler

import (
	"errors"
	"math"
	"testing"
)

// canonical fixtures used across tests
var (
	flyCPU = Candidate{
		Name: "fly-cpu",
		Capabilities: Capabilities{
			MaxCPUMillis:         8000,
			MaxMemoryMB:          16000,
			Regions:              []string{"ord", "fra", "sin"},
			SnapshotRestore:      false,
			EstimatedResumeMS:    1500,
			EstimatedColdStartMS: 30000,
		},
		Pricing: Pricing{
			PerCPUUSDPerHour: 0.008,
			PerGBUSDPerHour:  0.007,
		},
		Capacity: Capacity{Infinite: true},
	}
	linodeGPU = Candidate{
		Name: "linode-gpu",
		Capabilities: Capabilities{
			GPUKinds:             []string{"l4", "a100", "h100", "rtx4090"},
			MaxCPUMillis:         16000,
			MaxMemoryMB:          64000,
			SnapshotRestore:      false,
			EstimatedResumeMS:    30000,
			EstimatedColdStartMS: 60000,
		},
		Pricing: Pricing{
			PerCPUUSDPerHour: 0.004,
			PerGBUSDPerHour:  0.002,
			GPURates:         map[string]float64{"l4": 0.43, "a100": 1.99, "h100": 2.49, "rtx4090": 0.39},
		},
		Capacity: Capacity{Infinite: true},
	}
	flyGPU = Candidate{
		Name: "fly-gpu",
		Capabilities: Capabilities{
			GPUKinds:             []string{"a10", "l40s", "a100"},
			MaxCPUMillis:         8000,
			MaxMemoryMB:          16000,
			SnapshotRestore:      false,
			EstimatedResumeMS:    1500,
			EstimatedColdStartMS: 30000,
		},
		Pricing: Pricing{
			PerCPUUSDPerHour: 0.008,
			PerGBUSDPerHour:  0.007,
			GPURates:         map[string]float64{"a10": 1.25, "l40s": 2.50, "a100": 3.50},
		},
		Capacity: Capacity{Infinite: true},
	}
	tower = Candidate{
		Name: "tower",
		Capabilities: Capabilities{
			MaxCPUMillis:         32000,
			MaxMemoryMB:          65536,
			SnapshotRestore:      true,
			EstimatedResumeMS:    200,
			EstimatedColdStartMS: 5000,
		},
		Pricing:  Pricing{},
		Capacity: Capacity{AvailableSlots: 4},
	}
)

func nearly(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func TestEstimateCostCPUOnly(t *testing.T) {
	got := estimateCost(flyCPU.Pricing, Requirements{CPUMillis: 2000, MemoryMB: 4096})
	// 2 vCPU * 0.008 + 4 GB * 0.007 = 0.016 + 0.028 = 0.044
	if !nearly(got, 0.044) {
		t.Errorf("cost = %v, want 0.044", got)
	}
}

func TestEstimateCostGPU(t *testing.T) {
	got := estimateCost(linodeGPU.Pricing, Requirements{
		CPUMillis: 4000,
		MemoryMB:  16384,
		GPU:       &GPURequest{Kind: "l4", Count: 1},
	})
	// 4 * 0.004 + 16 * 0.002 + 0.43 = 0.016 + 0.032 + 0.43 = 0.478
	if !nearly(got, 0.478) {
		t.Errorf("cost = %v, want 0.478", got)
	}
}

func TestScheduleCheapestWins(t *testing.T) {
	s := New([]Candidate{flyCPU, tower})
	d, err := s.Schedule(Requirements{CPUMillis: 2000, MemoryMB: 4096})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if d.Backend != "tower" {
		t.Errorf("Backend = %q, want tower (free CPU beats Fly)", d.Backend)
	}
}

func TestScheduleGPUKindFilter(t *testing.T) {
	// User asks for L4 -> only Linode has it, even though Fly is cheaper for CPU.
	s := New([]Candidate{flyGPU, linodeGPU, flyCPU})
	d, err := s.Schedule(Requirements{
		CPUMillis: 4000,
		MemoryMB:  16384,
		GPU:       &GPURequest{Kind: "l4", Count: 1},
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if d.Backend != "linode-gpu" {
		t.Errorf("Backend = %q, want linode-gpu (only one with L4)", d.Backend)
	}
}

func TestScheduleGPUCheapestAcrossBackends(t *testing.T) {
	// User asks for A100 -> both linode and fly have it. Linode is cheaper.
	s := New([]Candidate{flyGPU, linodeGPU})
	d, err := s.Schedule(Requirements{
		CPUMillis: 4000,
		MemoryMB:  16384,
		GPU:       &GPURequest{Kind: "a100", Count: 1},
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if d.Backend != "linode-gpu" {
		t.Errorf("Backend = %q, want linode-gpu (cheaper A100)", d.Backend)
	}
}

func TestScheduleRespectsMaxUSDPerHour(t *testing.T) {
	// Cap is below Linode's L4 cost, so we should fall through. With
	// only a GPU-needing request and no candidate under the cap, we get
	// NoViableBackendError.
	s := New([]Candidate{flyGPU, linodeGPU})
	req := Requirements{
		CPUMillis:     4000,
		MemoryMB:      16384,
		GPU:           &GPURequest{Kind: "h100", Count: 1},
		MaxUSDPerHour: 1.00, // Linode H100 ~$2.49, Fly doesn't have H100 -> reject all
	}
	_, err := s.Schedule(req)
	var nvbe *NoViableBackendError
	if !errors.As(err, &nvbe) {
		t.Fatalf("expected NoViableBackendError, got %v", err)
	}
}

func TestScheduleLatencyInstantRequiresSnapshot(t *testing.T) {
	// Tower has snapshot restore; nothing else does.
	s := New([]Candidate{flyCPU, linodeGPU, tower})
	d, err := s.Schedule(Requirements{
		CPUMillis: 1000,
		MemoryMB:  2048,
		Latency:   LatencyInstant,
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if d.Backend != "tower" {
		t.Errorf("Backend = %q, want tower (only snapshot-capable)", d.Backend)
	}
}

func TestScheduleLatencySubMinuteFiltersSlow(t *testing.T) {
	slow := Candidate{
		Name: "slow",
		Capabilities: Capabilities{
			EstimatedResumeMS:    120_000,
			EstimatedColdStartMS: 180_000,
		},
		Pricing:  Pricing{},
		Capacity: Capacity{Infinite: true},
	}
	s := New([]Candidate{flyCPU, slow})
	d, err := s.Schedule(Requirements{
		CPUMillis: 1000,
		MemoryMB:  2048,
		Latency:   LatencySubMinute,
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if d.Backend == "slow" {
		t.Errorf("scheduler picked the >1min backend under sub-minute tier")
	}
}

func TestScheduleRegionPin(t *testing.T) {
	// Fly has 'fra'; Linode has no regions configured (treated as any).
	s := New([]Candidate{flyCPU, linodeGPU})
	d, err := s.Schedule(Requirements{
		CPUMillis: 1000,
		MemoryMB:  2048,
		Region:    "fra",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	// Both pass region check; cheapest wins. Linode is cheaper (0.004
	// vs 0.008 per CPU).
	if d.Backend != "linode-gpu" {
		t.Errorf("Backend = %q, want linode-gpu (cheaper)", d.Backend)
	}

	// Region not served by anyone with regions configured -> Fly should
	// still pass (since Fly's Regions list excludes 'unknown'). Linode
	// has no regions so it's also viable.
	_, err = s.Schedule(Requirements{
		CPUMillis: 1000,
		MemoryMB:  2048,
		Region:    "atlantis",
	})
	// linodeGPU has no Regions set -> any region matches -> still
	// viable. So we get a Decision, not an error.
	if err != nil {
		t.Errorf("region-unknown with one region-agnostic candidate should still succeed, got %v", err)
	}
}

func TestScheduleCapacityRespected(t *testing.T) {
	full := tower
	full.Capacity = Capacity{AvailableSlots: 0}
	s := New([]Candidate{full, flyCPU})
	d, err := s.Schedule(Requirements{CPUMillis: 1000, MemoryMB: 2048})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if d.Backend != "fly-cpu" {
		t.Errorf("Backend = %q, want fly-cpu (tower full)", d.Backend)
	}
}

func TestScheduleNoCandidates(t *testing.T) {
	s := New(nil)
	_, err := s.Schedule(Requirements{CPUMillis: 1000, MemoryMB: 2048})
	if err == nil {
		t.Fatal("expected error with no candidates")
	}
}

func TestScheduleDeterministic(t *testing.T) {
	// Same inputs -> same output, irrespective of input slice order.
	req := Requirements{
		CPUMillis: 4000,
		MemoryMB:  16384,
		GPU:       &GPURequest{Kind: "a100", Count: 1},
	}
	d1, _ := New([]Candidate{flyGPU, linodeGPU}).Schedule(req)
	d2, _ := New([]Candidate{linodeGPU, flyGPU}).Schedule(req)
	d3, _ := New([]Candidate{flyGPU, linodeGPU}).Schedule(req)
	if d1.Backend != d2.Backend || d2.Backend != d3.Backend {
		t.Errorf("non-deterministic: %s / %s / %s", d1.Backend, d2.Backend, d3.Backend)
	}
	if !nearly(d1.EstUSDPerHour, d2.EstUSDPerHour) || !nearly(d2.EstUSDPerHour, d3.EstUSDPerHour) {
		t.Errorf("non-deterministic cost: %v / %v / %v", d1.EstUSDPerHour, d2.EstUSDPerHour, d3.EstUSDPerHour)
	}
}

func TestScheduleTieBreaksAlphabetically(t *testing.T) {
	// Two candidates with identical cost/capability -> stable name tie-break.
	a := Candidate{
		Name:         "a-backend",
		Pricing:      Pricing{PerCPUUSDPerHour: 0.01},
		Capacity:     Capacity{Infinite: true},
		Capabilities: Capabilities{},
	}
	z := a
	z.Name = "z-backend"
	d, err := New([]Candidate{z, a}).Schedule(Requirements{CPUMillis: 1000})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if d.Backend != "a-backend" {
		t.Errorf("tie-break: got %q, want a-backend", d.Backend)
	}
}

func TestValidateRequest(t *testing.T) {
	cases := []struct {
		name string
		req  Requirements
		want string
	}{
		{"negative cpu", Requirements{CPUMillis: -1}, "cpuMillis"},
		{"negative mem", Requirements{MemoryMB: -1}, "memoryMB"},
		{"negative gpu count", Requirements{GPU: &GPURequest{Count: -1}}, "gpu.count"},
		{"negative max usd", Requirements{MaxUSDPerHour: -1}, "maxUSDPerHour"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateRequest(c.req)
			if err == nil || !contains([]string{err.Error()}, "") && !containsSubstr(err.Error(), c.want) {
				t.Errorf("got %v, want error containing %q", err, c.want)
			}
		})
	}
}

func containsSubstr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
