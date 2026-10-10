package pricing

import "testing"

func TestLookupLinodeGPU(t *testing.T) {
	if p, ok := LookupLinodeGPU("rtx4000ada"); !ok || p != 0.52 {
		t.Errorf("rtx4000ada: got (%v, %v), want (0.52, true)", p, ok)
	}
	if p, ok := LookupLinodeGPU("rtx6000"); !ok || p != 1.50 {
		t.Errorf("rtx6000: got (%v, %v), want (1.50, true)", p, ok)
	}
}

func TestLookupLinodeGPUCaseInsensitive(t *testing.T) {
	if p, ok := LookupLinodeGPU("RTX4000ADA"); !ok || p != 0.52 {
		t.Errorf("case-insensitive lookup failed: got (%v, %v)", p, ok)
	}
}

func TestLookupLinodeGPUUnknown(t *testing.T) {
	if _, ok := LookupLinodeGPU("h100"); ok {
		t.Errorf("h100 is not a Linode GPU kind; want ok=false")
	}
}

func TestLinodePlanExactGPUPrices(t *testing.T) {
	want := map[string]float64{
		"g2-gpu-rtx4000a1-s":  0.52,
		"g2-gpu-rtx4000a1-m":  0.67,
		"g2-gpu-rtx4000a1-l":  0.96,
		"g2-gpu-rtx4000a1-xl": 1.53,
		"g2-gpu-rtx4000a2-s":  1.05,
		"g2-gpu-rtx4000a2-m":  1.34,
		"g2-gpu-rtx4000a4-s":  2.96,
		"g2-gpu-rtx4000a4-m":  3.57,
		"g1-gpu-rtx6000-1":    1.50,
		"g1-gpu-rtx6000-2":    3.00,
		"g1-gpu-rtx6000-3":    4.50,
		"g1-gpu-rtx6000-4":    6.00,
	}
	for plan, hourly := range want {
		if got, ok := LinodePlan(plan); !ok || got != hourly {
			t.Errorf("LinodePlan(%q) = (%v, %v), want (%v, true)", plan, got, ok, hourly)
		}
	}
}

func TestCustomerPriceAppliesMargin(t *testing.T) {
	if got := CustomerPrice(1.00); got != MarginMultiplier {
		t.Errorf("CustomerPrice(1.00) = %v, want %v", got, MarginMultiplier)
	}
}

func TestFlyMachine(t *testing.T) {
	if p, ok := FlyMachine("performance-2x"); !ok || p <= 0 {
		t.Errorf("performance-2x: (%v, %v)", p, ok)
	}
	if _, ok := FlyMachine("not-a-machine"); ok {
		t.Errorf("unknown machine should return false")
	}
}

// AWSInstance must price the GPU instance types the aws backend's
// mapGPUType emits, not just the CPU set — otherwise an AWS GPU burst
// records a $0 rate and the budget/cost meter under-charges it.
func TestAWSInstanceCoversGPU(t *testing.T) {
	for _, it := range []string{
		"g4dn.xlarge", "g5.xlarge", "g6.xlarge", "g6e.xlarge",
		"g6.12xlarge", "g6.48xlarge", "g4dn.metal",
		"p4d.24xlarge", "p5.48xlarge", "p5e.48xlarge",
	} {
		if p, ok := AWSInstance(it); !ok || p <= 0 {
			t.Errorf("AWSInstance(%q) = (%v, %v), want a positive rate", it, p, ok)
		}
	}
}

// The CPU instance types must still resolve after folding in the GPU set.
func TestAWSInstanceCPUStillResolves(t *testing.T) {
	if p, ok := AWSInstance("t3.small"); !ok || p <= 0 {
		t.Errorf("AWSInstance(t3.small) = (%v, %v)", p, ok)
	}
}

// The unified HourlyUSD lookup must price an AWS GPU instance type.
func TestHourlyUSDAWSGPU(t *testing.T) {
	if p, ok := HourlyUSD("aws", "g6.xlarge"); !ok || p <= 0 {
		t.Errorf("HourlyUSD(aws, g6.xlarge) = (%v, %v), want positive", p, ok)
	}
}
