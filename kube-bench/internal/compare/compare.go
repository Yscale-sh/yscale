package compare

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

type Metric struct {
	Name           string
	Unit           string
	Before         float64
	After          float64
	HigherIsBetter bool
}

func Load(path string) (model.Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return model.Result{}, fmt.Errorf("read result %s: %w", path, err)
	}
	var result model.Result
	if err := json.Unmarshal(data, &result); err != nil {
		return model.Result{}, fmt.Errorf("decode result %s: %w", path, err)
	}
	if result.SchemaVersion == "" {
		return model.Result{}, fmt.Errorf("%s is not a yscale-kube-bench result", path)
	}
	return result, nil
}

func Markdown(before, after model.Result) string {
	metrics := metrics(before, after)
	var out strings.Builder
	fmt.Fprintf(&out, "# Yscale benchmark comparison\n\n`%s` → `%s`\n\n", before.RunID, after.RunID)
	out.WriteString("| Metric | Before | After | Change | Assessment |\n")
	out.WriteString("|---|---:|---:|---:|---|\n")
	for _, metric := range metrics {
		change := percentChange(metric.Before, metric.After)
		assessment := "unchanged"
		if change != 0 {
			improved := change > 0
			if !metric.HigherIsBetter {
				improved = change < 0
			}
			if improved {
				assessment = "improved"
			} else {
				assessment = "regressed"
			}
		}
		fmt.Fprintf(&out, "| %s | %s | %s | %+.1f%% | %s |\n", metric.Name, format(metric.Before, metric.Unit), format(metric.After, metric.Unit), change, assessment)
	}
	return out.String()
}

func metrics(before, after model.Result) []Metric {
	definitions := []struct {
		name, unit string
		higher     bool
		get        func(model.Result) float64
	}{
		{"Overall score", "index", true, func(value model.Result) float64 { return dereference(value.Scores.Overall) }},
		{"Cached pod ready p95", "ms", false, func(value model.Result) float64 {
			if value.Reaction != nil {
				return value.Reaction.Summary.CachedReadyMS.P95
			}
			return 0
		}},
		{"Cluster integer throughput", "ops/s", true, func(value model.Result) float64 {
			if value.Compute != nil {
				return value.Compute.PeakAggregate
			}
			return 0
		}},
		{"Cluster scaling efficiency", "ratio", true, func(value model.Result) float64 {
			if value.Compute != nil {
				return value.Compute.ScalingEfficiency
			}
			return 0
		}},
		{"Cross-node TCP median", "Mbit/s", true, func(value model.Result) float64 {
			if value.Network != nil {
				return value.Network.Summary.CrossNodeTCPMbps.P50
			}
			return 0
		}},
		{"Cross-node RTT p95-of-p99", "ms", false, func(value model.Result) float64 {
			if value.Network != nil {
				return value.Network.Summary.CrossNodeRTTMS.P95
			}
			return 0
		}},
		{"Service DNS p95-of-p99", "ms", false, func(value model.Result) float64 {
			if value.DNS != nil {
				return value.DNS.Summary.ServiceP99MS.P95
			}
			return 0
		}},
		{"Sequential read median", "MB/s", true, func(value model.Result) float64 {
			if value.Storage != nil {
				return value.Storage.Summary.SequentialReadMBps.P50
			}
			return 0
		}},
		{"Random read median", "IOPS", true, func(value model.Result) float64 {
			if value.Storage != nil {
				return value.Storage.Summary.RandomReadIOPS.P50
			}
			return 0
		}},
		{"fsync p95-of-p99", "ms", false, func(value model.Result) float64 {
			if value.Storage != nil {
				return value.Storage.Summary.FsyncP99MS.P95
			}
			return 0
		}},
		{"CPU transcode median", "x", true, func(value model.Result) float64 {
			if value.Transcode != nil {
				return value.Transcode.Summary.CPURealtimeFactor.P50
			}
			return 0
		}},
		{"GPU transcode median", "x", true, func(value model.Result) float64 {
			if value.Transcode != nil {
				return value.Transcode.Summary.GPURealtimeFactor.P50
			}
			return 0
		}},
	}
	var result []Metric
	for _, definition := range definitions {
		oldValue := definition.get(before)
		newValue := definition.get(after)
		if oldValue == 0 && newValue == 0 {
			continue
		}
		result = append(result, Metric{Name: definition.name, Unit: definition.unit, Before: oldValue, After: newValue, HigherIsBetter: definition.higher})
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func percentChange(before, after float64) float64 {
	if before == 0 {
		if after == 0 {
			return 0
		}
		return 100
	}
	return 100 * (after - before) / before
}

func format(value float64, unit string) string {
	if value == 0 {
		return "n/a"
	}
	switch unit {
	case "ops/s", "IOPS":
		return fmt.Sprintf("%.0f %s", value, unit)
	case "ratio":
		return fmt.Sprintf("%.1f%%", value*100)
	case "index":
		return fmt.Sprintf("%.1f", value)
	default:
		return fmt.Sprintf("%.2f %s", value, unit)
	}
}

func dereference(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}
