package scoring

import (
	"testing"

	"github.com/JakeNesler/yscale-kube-bench/internal/config"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

func TestCalculateAtBaseline(t *testing.T) {
	cfg := config.Default().Spec.Scoring
	result := model.Result{
		Reaction: &model.ReactionSuite{Summary: model.ReactionSummary{CachedReadyMS: model.Distribution{P95: cfg.Baselines.PodReadyP95MS}}},
		Compute:  &model.ComputeSuite{PeakAggregate: cfg.Baselines.ClusterIntegerOpsPerSecond},
	}
	card := Calculate(result, cfg)
	if card.Reaction == nil || *card.Reaction != 1000 {
		t.Fatalf("unexpected reaction score: %+v", card)
	}
	if card.Throughput == nil || *card.Throughput != 1000 {
		t.Fatalf("unexpected throughput score: %+v", card)
	}
}
