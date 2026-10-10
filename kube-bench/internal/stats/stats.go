package stats

import (
	"math"
	"sort"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

func Distribution(values []float64, unit string) model.Distribution {
	clean := make([]float64, 0, len(values))
	for _, value := range values {
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			clean = append(clean, value)
		}
	}
	if len(clean) == 0 {
		return model.Distribution{Unit: unit}
	}
	sort.Float64s(clean)
	var sum float64
	for _, value := range clean {
		sum += value
	}
	return model.Distribution{
		Count: len(clean),
		Min:   clean[0],
		Mean:  sum / float64(len(clean)),
		P50:   percentile(clean, 0.50),
		P95:   percentile(clean, 0.95),
		P99:   percentile(clean, 0.99),
		Max:   clean[len(clean)-1],
		Unit:  unit,
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	position := p * float64(len(sorted)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return sorted[lower]
	}
	weight := position - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

func GeometricMean(values []float64) float64 {
	var sum float64
	var count int
	for _, value := range values {
		if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		sum += math.Log(value)
		count++
	}
	if count == 0 {
		return 0
	}
	return math.Exp(sum / float64(count))
}
