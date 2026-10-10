package pricing

import (
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/gcp"
)

// TestEstimateGCPCPUParity proves the pricing estimator returns the SAME E2
// machine type (and a real rate) the gcp backend will actually launch,
// across the whole memory range. Same guarantee TestEstimateAzureCPUParity
// guards for Azure: the two ladders are written out separately in
// pkg/backends/gcp.mapType and pricing.EstimateGCPCPU, so only a test
// keeps them from drifting and silently mis-reporting accrued spend —
// which is also the budget cap central enforces on the burst.
func TestEstimateGCPCPUParity(t *testing.T) {
	// Every threshold, the value just past it, and well beyond the ladder top.
	cases := []int64{
		0,
		1024,  // e2-small / e2-medium boundary
		1025,  // first e2-medium
		3072,  // e2-medium / e2-standard-2 boundary
		3073,  // first e2-standard-2
		6144,  // e2-standard-2 / e2-standard-4 boundary
		6145,  // first e2-standard-4
		14336, // e2-standard-4 / e2-standard-8 boundary
		14337, // first e2-standard-8
		65536, // way past the ladder top — still e2-standard-8
		1 << 30,
	}
	for _, mb := range cases {
		launched := gcp.MapType(backends.ResourceRequirements{MemoryMB: mb})
		rate, estimated := EstimateGCPCPU(mb)
		if estimated != launched {
			t.Errorf("memory=%d MiB: estimator picked %q, backend launches %q — ladders disagree",
				mb, estimated, launched)
			continue
		}
		if rate <= 0 {
			t.Errorf("memory=%d MiB: machine type %q priced at %v (want positive rate)", mb, estimated, rate)
		}
	}
}

// TestGCPRatesCoverEveryLaunchableSize catches the other half of the drift:
// a machine type added to the backend's mapType but never given a rate
// would price at zero, and a zero rate means a burst that bills real
// money reports as free.
func TestGCPRatesCoverEveryLaunchableSize(t *testing.T) {
	seen := map[string]bool{}
	for mb := int64(0); mb <= 65536; mb += 64 {
		seen[gcp.MapType(backends.ResourceRequirements{MemoryMB: mb})] = true
	}
	for mt := range seen {
		if rate, ok := GCPMachine(mt); !ok || rate <= 0 {
			t.Errorf("machine type %q is launchable but has no positive rate (got %v, ok=%v)", mt, rate, ok)
		}
	}
}

func TestGCPMachineExactRates(t *testing.T) {
	cases := map[string]float64{
		"e2-small":      0.016752855,
		"e2-medium":     0.03350571,
		"e2-standard-2": 0.06701142,
		"e2-standard-4": 0.13402284,
		"e2-standard-8": 0.26804568,
	}
	for mt, want := range cases {
		got, ok := GCPMachine(mt)
		if !ok {
			t.Errorf("GCPMachine(%q): not found in catalog", mt)
			continue
		}
		if diff := got - want; diff > 1e-9 || diff < -1e-9 {
			t.Errorf("GCPMachine(%q) = %v, want %v", mt, got, want)
		}
	}
}

// TestGCPRegionPriced covers bare regions, zones, and fail-closed inputs.
func TestGCPRegionPriced(t *testing.T) {
	priced := []string{
		"us-central1", // bare region
		"us-east1",
		"us-west1",
		"us-central1-a", // zone of a priced region
		"us-central1-b",
		"us-east1-c",
		"us-west1-a", // zone requires three hyphen-separated parts; "a" suffix qualifies
	}
	for _, r := range priced {
		if !GCPRegionPriced(r) {
			t.Errorf("GCPRegionPriced(%q) = false, want true (region is in the priced region list)", r)
		}
	}
	unpriced := []string{
		"",                // the decider must resolve the backend's configured region
		"europe-west1",    // unverified region — not in catalog
		"asia-northeast1", // unverified region
		"australia-southeast1",
		"us-west2",   // not the same as us-west1 — typo-vulnerable
		"us-centra1", // typo
		"centralus",  // not a real GCP region
	}
	for _, r := range unpriced {
		if GCPRegionPriced(r) {
			t.Errorf("GCPRegionPriced(%q) = true, want false (region is NOT in the priced region list; the gate must fail closed)", r)
		}
	}
}

// TestHourlyUSDGCP confirms the unified lookup now prices a GCP machine
// type — without this, /v1/skus-style reports would still show $0 for GCP.
func TestHourlyUSDGCP(t *testing.T) {
	if p, ok := HourlyUSD("gcp", "e2-small"); !ok || p <= 0 {
		t.Errorf("HourlyUSD(gcp, e2-small) = (%v, %v), want positive rate", p, ok)
	}
	if _, ok := HourlyUSD("gcp", "not-a-machine-type"); ok {
		t.Errorf("HourlyUSD(gcp, not-a-machine-type) ok = true, want false")
	}
}
