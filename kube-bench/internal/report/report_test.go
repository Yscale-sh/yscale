package report

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

func TestWrite(t *testing.T) {
	start := time.Now().UTC()
	result := model.Result{
		RunID: "test-run", SchemaVersion: model.SchemaVersion, ToolVersion: "test",
		StartedAt: start, FinishedAt: start.Add(time.Second),
		Cluster: model.ClusterSummary{NodeCount: 1, Namespace: "bench", Architectures: map[string]int{"amd64": 1}},
	}
	paths, err := Write(result, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{paths.JSON, paths.Markdown, paths.HTML} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
	markdown, _ := os.ReadFile(paths.Markdown)
	if !strings.Contains(string(markdown), "Yscale Kubernetes Benchmark") {
		t.Fatalf("unexpected markdown: %s", markdown)
	}
}
