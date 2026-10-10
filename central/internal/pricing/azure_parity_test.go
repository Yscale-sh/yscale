package pricing

import (
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/azure"
)

// TestEstimateAzureCPUParity proves the pricing estimator returns the SAME VM
// size (and a real rate) the azure backend will actually launch, across the
// whole memory range. Same guarantee TestEstimateLinodeCPUParity gives for
// Linode: the two ladders are written out separately, so only a test keeps
// them from drifting and silently mis-reporting accrued spend — which is also
// the budget cap central enforces on the burst.
func TestEstimateAzureCPUParity(t *testing.T) {
	// Every threshold, the value just past it, and well beyond the ladder top.
	cases := []int64{
		0,
		512,  // B1ms / B2s_v2 boundary
		513,  // first B2s_v2
		1536, // B2s_v2 / D2s_v5 boundary
		1537, // first D2s_v5
		3072, // D2s_v5 / D4s_v5 boundary
		3073, // first D4s_v5
		6144, // D4s_v5 / D8s_v5 boundary
		6145, // first D8s_v5
		32768,
		1 << 30,
	}
	for _, mb := range cases {
		launched := azure.MapType(backends.ResourceRequirements{MemoryMB: mb})
		rate, estimated := EstimateAzureCPU(mb)
		if estimated != launched {
			t.Errorf("memory=%d MiB: estimator picked %q, backend launches %q — ladders disagree",
				mb, estimated, launched)
			continue
		}
		if rate <= 0 {
			t.Errorf("memory=%d MiB: size %q priced at %v (want positive rate)", mb, estimated, rate)
		}
	}
}

// TestAzureRatesCoverEveryLaunchableSize catches the other half of the drift:
// a size added to the backend's mapType but never given a rate would price at
// zero, and a zero rate means a burst that bills real money reports as free.
func TestAzureRatesCoverEveryLaunchableSize(t *testing.T) {
	seen := map[string]bool{}
	for mb := int64(0); mb <= 65536; mb += 64 {
		seen[azure.MapType(backends.ResourceRequirements{MemoryMB: mb})] = true
	}
	for size := range seen {
		if rate, ok := AzureInstance(size); !ok || rate <= 0 {
			t.Errorf("size %q is launchable but has no positive rate (got %v, ok=%v)", size, rate, ok)
		}
	}
}
