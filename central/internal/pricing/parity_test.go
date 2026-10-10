package pricing

import (
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/linode"
)

// TestEstimateLinodeCPUParity proves the pricing estimator returns the SAME
// plan (and rate) that the linode backend will actually launch, across the
// whole memory range. EstimateLinodeCPU previously fell back to g6-standard-4
// for everything above 4096 MiB while mapType launches g6-standard-6 above
// 6144 MiB — silently under-reporting accrued spend and the budget cap it
// enforces. This test fails the moment the two ladders drift apart again.
func TestEstimateLinodeCPUParity(t *testing.T) {
	// Probe every boundary, the value just past each, and well beyond the
	// top of the ladder. linode.MapType is the source of truth for what is
	// actually launched.
	cases := []int64{
		0,
		512,   // nanode-1 / standard-1 boundary (<=512 -> g6-nanode-1)
		513,   // first g6-standard-1
		1536,  // standard-1 / standard-2 boundary
		1537,  // first g6-standard-2
		3072,  // standard-2 / standard-4 boundary
		3073,  // first g6-standard-4
		6144,  // standard-4 / standard-6 boundary (the regression)
		6145,  // first g6-standard-6 — was wrongly priced as g6-standard-4
		16384, // g6-standard-6's own 16 GB
		1 << 30,
	}
	for _, mb := range cases {
		launched := linode.MapType(backends.ResourceRequirements{MemoryMB: mb})
		rate, estimated := EstimateLinodeCPU(mb)
		if estimated != launched {
			t.Errorf("memory=%d MiB: estimator picked %q, backend launches %q — ladders disagree",
				mb, estimated, launched)
			continue
		}
		if rate <= 0 {
			t.Errorf("memory=%d MiB: plan %q priced at %v (want positive rate)", mb, estimated, rate)
		}
	}
}
