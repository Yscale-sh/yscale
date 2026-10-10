package runner

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/kubectl"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

func (r *Runner) runCompute(ctx context.Context, nodes []kubectl.Node) *model.ComputeSuite {
	suite := &model.ComputeSuite{Status: "ok"}
	suite.Isolated = r.runIsolatedCompute(ctx, nodes)
	expected := map[string]float64{}
	for _, result := range suite.Isolated {
		if result.Status == "ok" {
			expected[result.Node] = result.AllIntegerOpsPerSec
		}
	}
	for _, step := range computeSteps(r.config.Spec.Compute.ClusterSteps, len(nodes)) {
		waveNodes := nodes[:step]
		wave := r.runComputeWave(ctx, waveNodes, expected)
		suite.ClusterWaves = append(suite.ClusterWaves, wave)
		if wave.AggregateIntegerOpsPerSec > suite.PeakAggregate {
			suite.PeakAggregate = wave.AggregateIntegerOpsPerSec
		}
		if step == len(nodes) {
			suite.ScalingEfficiency = wave.ScalingEfficiency
		}
	}
	failed := 0
	total := len(suite.Isolated) + len(suite.ClusterWaves)
	for _, result := range suite.Isolated {
		if result.Status != "ok" {
			failed++
		}
	}
	for _, wave := range suite.ClusterWaves {
		if wave.Status != "ok" {
			failed++
		}
	}
	if failed == total && total > 0 {
		suite.Status = "failed"
		suite.Error = "all compute measurements failed"
	} else if failed > 0 {
		suite.Status = "partial"
		suite.Error = fmt.Sprintf("%d of %d compute measurements failed", failed, total)
	}
	return suite
}

func (r *Runner) runIsolatedCompute(ctx context.Context, nodes []kubectl.Node) []model.NodeComputeResult {
	results := make([]model.NodeComputeResult, len(nodes))
	parallelism := r.config.Spec.Compute.IsolatedParallelism
	if parallelism < 1 {
		parallelism = 1
	}
	semaphore := make(chan struct{}, parallelism)
	var wait sync.WaitGroup
	for index, node := range nodes {
		index, node := index, node
		wait.Add(1)
		go func() {
			defer wait.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			results[index] = r.computeNode(ctx, node.Metadata.Name, "isolated")
		}()
	}
	wait.Wait()
	return results
}

func (r *Runner) runComputeWave(ctx context.Context, nodes []kubectl.Node, expected map[string]float64) model.ComputeWave {
	wave := model.ComputeWave{NodeCount: len(nodes), Status: "ok"}
	wave.Results = make([]model.NodeComputeResult, len(nodes))
	for _, node := range nodes {
		wave.Nodes = append(wave.Nodes, node.Metadata.Name)
		wave.ExpectedIntegerOpsPerSec += expected[node.Metadata.Name]
	}
	start := time.Now()
	var wait sync.WaitGroup
	for index, node := range nodes {
		index, node := index, node
		wait.Add(1)
		go func() {
			defer wait.Done()
			wave.Results[index] = r.computeNode(ctx, node.Metadata.Name, "all")
		}()
	}
	wait.Wait()
	wave.WallSeconds = time.Since(start).Seconds()
	var failures []string
	for _, result := range wave.Results {
		if result.Status == "ok" {
			wave.AggregateIntegerOpsPerSec += result.AllIntegerOpsPerSec
		} else {
			failures = append(failures, result.Node+": "+result.Error)
		}
	}
	if wave.ExpectedIntegerOpsPerSec > 0 {
		wave.ScalingEfficiency = wave.AggregateIntegerOpsPerSec / wave.ExpectedIntegerOpsPerSec
	}
	if len(failures) == len(wave.Results) && len(wave.Results) > 0 {
		wave.Status = "failed"
		wave.Error = joinErrors(failures)
	} else if len(failures) > 0 {
		wave.Status = "partial"
		wave.Error = joinErrors(failures)
	}
	return wave
}

func (r *Runner) computeNode(ctx context.Context, node, mode string) model.NodeComputeResult {
	request := jobRequest{
		Name:     fmt.Sprintf("compute-%s-%s-%s", mode, node, r.runID),
		NodeName: node,
		Args: []string{
			"worker", "compute",
			"--node=" + node,
			"--mode=" + mode,
			fmt.Sprintf("--duration=%ds", r.config.Spec.Compute.DurationSeconds),
		},
		Environment: map[string]string{"NODE_NAME": node},
	}
	execution, err := r.runJob(ctx, request)
	if err != nil {
		return model.NodeComputeResult{Node: node, Status: "failed", Error: err.Error()}
	}
	result, err := decodeJobResult[model.NodeComputeResult](execution)
	if err != nil {
		return model.NodeComputeResult{Node: node, Status: "failed", Error: err.Error()}
	}
	return result
}

func computeSteps(configured []int, nodeCount int) []int {
	seen := map[int]struct{}{}
	var result []int
	for _, step := range configured {
		if step == 0 || step > nodeCount {
			step = nodeCount
		}
		if step < 1 {
			continue
		}
		if _, exists := seen[step]; exists {
			continue
		}
		seen[step] = struct{}{}
		result = append(result, step)
	}
	if _, exists := seen[nodeCount]; !exists && nodeCount > 0 {
		result = append(result, nodeCount)
	}
	sort.Ints(result)
	return result
}
