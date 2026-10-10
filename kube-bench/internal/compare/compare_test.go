package compare

import (
	"strings"
	"testing"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

func TestMarkdown(t *testing.T) {
	before := model.Result{RunID: "before", Compute: &model.ComputeSuite{PeakAggregate: 100}}
	after := model.Result{RunID: "after", Compute: &model.ComputeSuite{PeakAggregate: 120}}
	output := Markdown(before, after)
	if !strings.Contains(output, "+20.0%") || !strings.Contains(output, "improved") {
		t.Fatalf("unexpected comparison: %s", output)
	}
}
