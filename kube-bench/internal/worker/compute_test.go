package worker

import (
	"testing"
	"time"
)

func TestBenchmarkComputeRejectsUnknownModeBeforeWork(t *testing.T) {
	start := time.Now()
	result := benchmarkCompute("node-a", time.Second, "unknown")
	if result.Status != "failed" || result.Error == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("invalid mode should fail before running benchmarks")
	}
	if result.AllIntegerOpsPerSec != 0 || result.AllSHA256MiBPerSec != 0 {
		t.Fatalf("invalid mode unexpectedly ran compute: %+v", result)
	}
}
