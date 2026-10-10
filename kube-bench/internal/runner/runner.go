package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/JakeNesler/yscale-kube-bench/internal/config"
	"github.com/JakeNesler/yscale-kube-bench/internal/kubectl"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/scoring"
)

type Options struct {
	KubectlBinary string
	Kubeconfig    string
	Context       string
	Verbose       bool
	LogWriter     io.Writer
	ToolVersion   string
}

type Runner struct {
	config  config.Config
	client  *kubectl.Client
	options Options
	runID   string
	labels  map[string]string
}

func New(cfg config.Config, options Options) *Runner {
	runID := newRunID()
	client := kubectl.New(options.KubectlBinary, options.Kubeconfig, options.Context, cfg.Spec.Namespace, options.Verbose, options.LogWriter)
	return &Runner{
		config:  cfg,
		client:  client,
		options: options,
		runID:   runID,
		labels: map[string]string{
			"app.kubernetes.io/name":       "yscale-kube-bench",
			"app.kubernetes.io/managed-by": "yscale-kube-bench",
			"yscale.dev/bench-run":         runID,
		},
	}
}

func (r *Runner) Run(ctx context.Context) (*model.Result, error) {
	startedAt := time.Now().UTC()
	result := &model.Result{
		SchemaVersion: model.SchemaVersion,
		ToolVersion:   r.options.ToolVersion,
		RunID:         r.runID,
		StartedAt:     startedAt,
		Config:        r.config.JSON(),
		Cluster: model.ClusterSummary{
			Context:   r.options.Context,
			Namespace: r.config.Spec.Namespace,
		},
	}
	if err := r.client.Check(); err != nil {
		return nil, err
	}
	if err := r.client.EnsureNamespace(ctx); err != nil {
		return nil, fmt.Errorf("ensure benchmark namespace: %w", err)
	}
	version, err := r.client.Version(ctx)
	if err == nil {
		result.Cluster.KubernetesServerVersion = version.ServerVersion.GitVersion
	}
	nodes, err := r.selectedNodes(ctx)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no ready schedulable nodes matched the benchmark selection")
	}
	result.Cluster = summarizeCluster(result.Cluster, nodes)

	r.logf("inventory: profiling %d node(s)", len(nodes))
	result.Nodes = r.runInventory(ctx, nodes)
	for _, node := range result.Nodes {
		result.Warnings = append(result.Warnings, node.Warnings...)
	}

	if r.config.Spec.Suites.Reaction {
		r.logf("reaction: measuring pod creation and readiness")
		result.Reaction = r.runReaction(ctx, nodes)
	}
	if r.config.Spec.Suites.Compute {
		r.logf("compute: measuring isolated nodes and cluster scaling")
		result.Compute = r.runCompute(ctx, nodes)
	}
	if r.config.Spec.Suites.Network {
		r.logf("network: measuring same-node, cross-node, and service paths")
		result.Network = r.runNetwork(ctx, nodes)
	}
	if r.config.Spec.Suites.DNS {
		r.logf("dns: measuring cluster resolver latency and throughput")
		result.DNS = r.runDNS(ctx, nodes)
	}
	if r.config.Spec.Suites.Storage {
		r.logf("storage: measuring configured writable targets")
		result.Storage = r.runStorage(ctx, nodes)
	}
	if r.config.Spec.Suites.Transcode {
		r.logf("transcode: measuring configured FFmpeg profiles")
		result.Transcode = r.runTranscode(ctx, nodes)
	}

	result.Scores = scoring.Calculate(*result, r.config.Spec.Scoring)
	result.FinishedAt = time.Now().UTC()
	if !r.config.Spec.KeepResources {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := r.client.DeleteByLabel(cleanupContext, "yscale.dev/bench-run="+r.runID); err != nil {
			result.Warnings = append(result.Warnings, "cleanup: "+err.Error())
		}
	}
	result.Warnings = uniqueSorted(result.Warnings)
	return result, nil
}

func (r *Runner) selectedNodes(ctx context.Context) ([]kubectl.Node, error) {
	nodes, err := r.client.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	selectedNames := make(map[string]struct{}, len(r.config.Spec.Nodes))
	for _, name := range r.config.Spec.Nodes {
		selectedNames[name] = struct{}{}
	}
	filtered := make([]kubectl.Node, 0, len(nodes))
	for _, node := range nodes {
		if !node.Ready() || node.Spec.Unschedulable {
			continue
		}
		if !r.config.Spec.IncludeControlPlane && isControlPlane(node.Metadata.Labels) {
			continue
		}
		if len(selectedNames) > 0 {
			if _, exists := selectedNames[node.Metadata.Name]; !exists {
				continue
			}
		}
		if !matchesLabels(node.Metadata.Labels, r.config.Spec.NodeSelector) {
			continue
		}
		filtered = append(filtered, node)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Metadata.Name < filtered[j].Metadata.Name })
	return filtered, nil
}

func summarizeCluster(base model.ClusterSummary, nodes []kubectl.Node) model.ClusterSummary {
	base.NodeCount = len(nodes)
	base.Architectures = map[string]int{}
	base.OperatingSystems = map[string]int{}
	var cpu float64
	var memory int64
	for _, node := range nodes {
		if node.Ready() {
			base.ReadyNodeCount++
		}
		base.Architectures[node.Status.NodeInfo.Architecture]++
		base.OperatingSystems[node.Status.NodeInfo.OperatingSystem]++
		cpu += parseCPUQuantity(node.Status.Capacity["cpu"])
		memory += parseByteQuantity(node.Status.Capacity["memory"])
	}
	base.TotalCPUCapacity = strconv.FormatFloat(cpu, 'f', -1, 64)
	base.TotalMemoryBytes = memory
	return base
}

func parseCPUQuantity(value string) float64 {
	value = strings.TrimSpace(value)
	if strings.HasSuffix(value, "m") {
		number, _ := strconv.ParseFloat(strings.TrimSuffix(value, "m"), 64)
		return number / 1000
	}
	number, _ := strconv.ParseFloat(value, 64)
	return number
}

func parseByteQuantity(value string) int64 {
	value = strings.TrimSpace(value)
	multipliers := map[string]float64{
		"Ki": 1024,
		"Mi": 1024 * 1024,
		"Gi": 1024 * 1024 * 1024,
		"Ti": 1024 * 1024 * 1024 * 1024,
		"K":  1000,
		"M":  1000 * 1000,
		"G":  1000 * 1000 * 1000,
		"T":  1000 * 1000 * 1000 * 1000,
	}
	for suffix, multiplier := range multipliers {
		if strings.HasSuffix(value, suffix) {
			number, _ := strconv.ParseFloat(strings.TrimSuffix(value, suffix), 64)
			return int64(number * multiplier)
		}
	}
	number, _ := strconv.ParseInt(value, 10, 64)
	return number
}

func (r *Runner) logf(format string, values ...any) {
	if r.options.LogWriter == nil {
		return
	}
	fmt.Fprintf(r.options.LogWriter, format+"\n", values...)
}

func newRunID() string {
	buffer := make([]byte, 4)
	_, _ = rand.Read(buffer)
	return time.Now().UTC().Format("20060102-150405") + "-" + hex.EncodeToString(buffer)
}

func isControlPlane(labels map[string]string) bool {
	for _, key := range []string{"node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master"} {
		if _, exists := labels[key]; exists {
			return true
		}
	}
	return false
}

func matchesLabels(actual, required map[string]string) bool {
	for key, value := range required {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func uniqueSorted(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func joinErrors(values []string) string {
	return strings.Join(uniqueSorted(values), "; ")
}
