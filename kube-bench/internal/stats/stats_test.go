package stats

import (
	"math"
	"testing"
)

func TestDistribution(t *testing.T) {
	result := Distribution([]float64{5, 1, 3, 2, 4}, "ms")
	if result.Count != 5 || result.Min != 1 || result.Max != 5 || result.P50 != 3 {
		t.Fatalf("unexpected distribution: %+v", result)
	}
	if math.Abs(result.Mean-3) > 0.0001 {
		t.Fatalf("unexpected mean: %f", result.Mean)
	}
}

func TestGeometricMean(t *testing.T) {
	result := GeometricMean([]float64{1, 4})
	if math.Abs(result-2) > 0.0001 {
		t.Fatalf("unexpected mean: %f", result)
	}
}
