package scoring

import (
	"fmt"
	"math"

	"github.com/JakeNesler/yscale-kube-bench/internal/config"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/stats"
)

const Version = "yscale-index/v1"

func Calculate(result model.Result, cfg config.ScoringConfig) model.ScoreCard {
	card := model.ScoreCard{Version: Version}
	if !cfg.Enabled {
		card.Explanation = []string{"scoring is disabled; raw measurements remain available"}
		return card
	}
	baseline := cfg.Baselines

	reactionRatios := []float64{}
	if result.Reaction != nil && result.Reaction.Summary.CachedReadyMS.P95 > 0 && baseline.PodReadyP95MS > 0 {
		reactionRatios = append(reactionRatios, baseline.PodReadyP95MS/result.Reaction.Summary.CachedReadyMS.P95)
	}
	if result.DNS != nil && result.DNS.Summary.ServiceP99MS.P95 > 0 && baseline.DNSP99MS > 0 {
		reactionRatios = append(reactionRatios, baseline.DNSP99MS/result.DNS.Summary.ServiceP99MS.P95)
	}
	if result.Network != nil && result.Network.Summary.CrossNodeRTTMS.P95 > 0 && baseline.CrossNodeRTTP99MS > 0 {
		reactionRatios = append(reactionRatios, baseline.CrossNodeRTTP99MS/result.Network.Summary.CrossNodeRTTMS.P95)
	}
	if result.Storage != nil && result.Storage.Summary.FsyncP99MS.P95 > 0 && baseline.FsyncP99MS > 0 {
		reactionRatios = append(reactionRatios, baseline.FsyncP99MS/result.Storage.Summary.FsyncP99MS.P95)
	}
	if score := indexScore(reactionRatios); score > 0 {
		card.Reaction = pointer(score)
	}

	throughputRatios := []float64{}
	if result.Compute != nil && result.Compute.PeakAggregate > 0 && baseline.ClusterIntegerOpsPerSecond > 0 {
		throughputRatios = append(throughputRatios, result.Compute.PeakAggregate/baseline.ClusterIntegerOpsPerSecond)
	}
	if result.Network != nil && result.Network.Summary.CrossNodeTCPMbps.P50 > 0 && baseline.CrossNodeTCPMbps > 0 {
		throughputRatios = append(throughputRatios, result.Network.Summary.CrossNodeTCPMbps.P50/baseline.CrossNodeTCPMbps)
	}
	if result.Storage != nil && result.Storage.Summary.SequentialReadMBps.P50 > 0 && baseline.SequentialReadMBps > 0 {
		throughputRatios = append(throughputRatios, result.Storage.Summary.SequentialReadMBps.P50/baseline.SequentialReadMBps)
	}
	if result.Transcode != nil && baseline.TranscodeRealtimeFactor > 0 {
		transcode := math.Max(result.Transcode.Summary.CPURealtimeFactor.P50, result.Transcode.Summary.GPURealtimeFactor.P50)
		if transcode > 0 {
			throughputRatios = append(throughputRatios, transcode/baseline.TranscodeRealtimeFactor)
		}
	}
	if score := indexScore(throughputRatios); score > 0 {
		card.Throughput = pointer(score)
	}

	if result.Compute != nil && result.Compute.PeakAggregate > 0 {
		var peakWatts, purchasePrice float64
		for _, node := range result.Nodes {
			peakWatts += node.Effective.Economics.PeakWatts
			purchasePrice += node.Effective.Economics.PurchasePriceUSD
		}
		efficiencyRatios := []float64{}
		if peakWatts > 0 && baseline.ComputePerPeakWatt > 0 {
			efficiencyRatios = append(efficiencyRatios, (result.Compute.PeakAggregate/peakWatts)/baseline.ComputePerPeakWatt)
		}
		if purchasePrice > 0 && baseline.ComputePerDollar > 0 {
			efficiencyRatios = append(efficiencyRatios, (result.Compute.PeakAggregate/purchasePrice)/baseline.ComputePerDollar)
		}
		if score := indexScore(efficiencyRatios); score > 0 {
			card.Efficiency = pointer(score)
		}
	}

	var dimensions []float64
	for _, value := range []*float64{card.Reaction, card.Throughput, card.Efficiency} {
		if value != nil && *value > 0 {
			dimensions = append(dimensions, *value)
		}
	}
	if len(dimensions) > 0 {
		overall := stats.GeometricMean(dimensions)
		card.Overall = pointer(round(overall, 1))
	}
	card.Explanation = []string{
		"1000 equals the configured reference baseline; scores scale proportionally and use a geometric mean of available measurements",
		"reaction uses inverse latency, throughput uses direct throughput, and efficiency is omitted unless price or peak-watt metadata is supplied",
		"raw measurements and score baselines are embedded in the result for reproducibility",
	}
	return card
}

func indexScore(ratios []float64) float64 {
	value := stats.GeometricMean(ratios)
	if value <= 0 {
		return 0
	}
	score := value * 1000
	if score > 100000 {
		score = 100000
	}
	return round(score, 1)
}

func round(value float64, places int) float64 {
	factor := math.Pow(10, float64(places))
	return math.Round(value*factor) / factor
}

func pointer(value float64) *float64 {
	return &value
}

func Format(value *float64) string {
	if value == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.1f", *value)
}
