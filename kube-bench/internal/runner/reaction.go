package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/kubectl"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/stats"
)

func (r *Runner) runReaction(ctx context.Context, nodes []kubectl.Node) *model.ReactionSuite {
	suite := &model.ReactionSuite{Status: "ok"}
	if pullImage := r.config.Spec.Reaction.PullImage; pullImage != "" {
		suite.PullPath = r.runReactionRounds(ctx, nodes, "pull-path", pullImage, r.config.Spec.Reaction.PullImagePolicy)
	}
	suite.Cached = r.runReactionRounds(ctx, nodes, "cached", r.config.Spec.Image, "IfNotPresent")
	if r.config.Spec.Reaction.ClusterWave {
		suite.ClusterWaves = append(suite.ClusterWaves, r.runReactionWave(ctx, nodes))
	}
	var cachedReady, pullReady, waveReady []float64
	for _, result := range suite.Cached {
		if result.Status == "ok" && result.ObservedReadyMS > 0 {
			cachedReady = append(cachedReady, result.ObservedReadyMS)
		}
	}
	for _, result := range suite.PullPath {
		if result.Status == "ok" && result.ObservedReadyMS > 0 {
			pullReady = append(pullReady, result.ObservedReadyMS)
		}
	}
	for _, wave := range suite.ClusterWaves {
		if wave.Status == "ok" && wave.AllReadyMS > 0 {
			waveReady = append(waveReady, wave.AllReadyMS)
		}
	}
	suite.Summary = model.ReactionSummary{
		CachedReadyMS:     stats.Distribution(cachedReady, "ms"),
		PullReadyMS:       stats.Distribution(pullReady, "ms"),
		ClusterAllReadyMS: stats.Distribution(waveReady, "ms"),
	}
	failed := 0
	total := len(suite.Cached) + len(suite.PullPath)
	for _, result := range append(append([]model.ReactionResult{}, suite.Cached...), suite.PullPath...) {
		if result.Status != "ok" {
			failed++
		}
	}
	for _, wave := range suite.ClusterWaves {
		total++
		if wave.Status != "ok" {
			failed++
		}
	}
	if failed == total && total > 0 {
		suite.Status = "failed"
		suite.Error = "all reaction measurements failed"
	} else if failed > 0 {
		suite.Status = "partial"
		suite.Error = fmt.Sprintf("%d of %d reaction measurements failed", failed, total)
	}
	return suite
}

func (r *Runner) runReactionRounds(ctx context.Context, nodes []kubectl.Node, mode, image, pullPolicy string) []model.ReactionResult {
	type task struct {
		index int
		node  kubectl.Node
		round int
	}
	var tasks []task
	for round := 1; round <= r.config.Spec.Reaction.Rounds; round++ {
		for _, node := range nodes {
			tasks = append(tasks, task{index: len(tasks), node: node, round: round})
		}
	}
	results := make([]model.ReactionResult, len(tasks))
	parallelism := r.config.Spec.Parallelism
	if parallelism < 1 {
		parallelism = 1
	}
	semaphore := make(chan struct{}, parallelism)
	var wait sync.WaitGroup
	for _, current := range tasks {
		current := current
		wait.Add(1)
		go func() {
			defer wait.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			name := fmt.Sprintf("react-%s-r%d-%s-%s", mode, current.round, current.node.Metadata.Name, r.runID)
			results[current.index] = r.measureReactionPod(ctx, name, current.node.Metadata.Name, mode, current.round, image, pullPolicy)
		}()
	}
	wait.Wait()
	return results
}

func (r *Runner) measureReactionPod(ctx context.Context, name, node, mode string, round int, image, pullPolicy string) model.ReactionResult {
	name = sanitizeName(name)
	result := model.ReactionResult{Node: node, Mode: mode, Round: round, Image: image, Status: "ok"}
	manifest := r.reactionPodManifest(name, node, image, pullPolicy)
	createStart := time.Now().UTC()
	var created kubectl.Pod
	if err := r.client.Create(ctx, manifest, &created); err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	createEnd := time.Now().UTC()
	result.CreateRequestMS = milliseconds(createEnd.Sub(createStart))
	readyTimeout := time.Duration(r.config.Spec.Reaction.ReadyTimeoutSeconds) * time.Second
	readyContext, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	readyPod, err := r.waitForPodReady(readyContext, name)
	observed := time.Now().UTC()
	createdAt := readyPod.Metadata.CreationTimestamp
	if createdAt.IsZero() {
		createdAt = created.Metadata.CreationTimestamp
	}
	if createdAt.IsZero() {
		createdAt = createEnd
	}
	result.ObservedReadyMS = milliseconds(observed.Sub(createdAt))
	if scheduled, exists := readyPod.Condition("PodScheduled"); exists {
		result.CreateToScheduledMS = milliseconds(scheduled.Sub(createdAt))
	}
	if running, exists := readyPod.RunningStartedAt(); exists {
		result.CreateToRunningMS = milliseconds(running.Sub(createdAt))
	}
	if readyAt, exists := readyPod.Condition("Ready"); exists {
		result.CreateToReadyMS = milliseconds(readyAt.Sub(createdAt))
	}
	result.ImagePullMS = r.imagePullDuration(ctx, name)
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
	}
	return result
}

func (r *Runner) runReactionWave(ctx context.Context, nodes []kubectl.Node) model.ReactionWave {
	wave := model.ReactionWave{Mode: "cached-cluster-wave", NodeCount: len(nodes), Status: "ok"}
	start := time.Now()
	results := make([]model.ReactionResult, len(nodes))
	var wait sync.WaitGroup
	for index, node := range nodes {
		index, node := index, node
		wait.Add(1)
		go func() {
			defer wait.Done()
			name := fmt.Sprintf("react-wave-%s-%s", node.Metadata.Name, r.runID)
			results[index] = r.measureReactionPod(ctx, name, node.Metadata.Name, "cluster-wave", 1, r.config.Spec.Image, "IfNotPresent")
		}()
	}
	wait.Wait()
	wave.AllReadyMS = milliseconds(time.Since(start))
	var values []float64
	var failures []string
	for _, result := range results {
		if result.Status == "ok" {
			values = append(values, result.ObservedReadyMS)
		} else {
			failures = append(failures, result.Node+": "+result.Error)
		}
	}
	wave.ReadySpread = stats.Distribution(values, "ms")
	if len(failures) == len(results) && len(results) > 0 {
		wave.Status = "failed"
		wave.Error = joinErrors(failures)
	} else if len(failures) > 0 {
		wave.Status = "partial"
		wave.Error = joinErrors(failures)
	}
	return wave
}

func (r *Runner) imagePullDuration(ctx context.Context, podName string) float64 {
	events, err := r.client.EventsFor(ctx, podName)
	if err != nil {
		return 0
	}
	var pulling, pulled time.Time
	for _, event := range events {
		timestamp := event.Timestamp()
		switch event.Reason {
		case "Pulling":
			if pulling.IsZero() || timestamp.Before(pulling) {
				pulling = timestamp
			}
		case "Pulled":
			if pulled.IsZero() || timestamp.After(pulled) {
				pulled = timestamp
			}
		}
	}
	if pulling.IsZero() || pulled.IsZero() || pulled.Before(pulling) {
		return 0
	}
	return milliseconds(pulled.Sub(pulling))
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration.Microseconds()) / 1000
}
