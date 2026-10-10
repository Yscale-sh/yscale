package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/JakeNesler/yscale-kube-bench/internal/config"
	"github.com/JakeNesler/yscale-kube-bench/internal/kubectl"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/stats"
)

func (r *Runner) runTranscode(ctx context.Context, nodes []kubectl.Node) *model.TranscodeSuite {
	suite := &model.TranscodeSuite{Status: "ok"}
	for profileIndex, profile := range r.config.Spec.Transcode.Profiles {
		for nodeIndex, node := range transcodeNodes(profile, nodes) {
			if !nodeSupportsResources(node, profile.Resources) {
				suite.Results = append(suite.Results, model.TranscodeResult{
					Node: node.Metadata.Name, Profile: profile.Name, Encoder: profile.Encoder,
					Width: profile.Width, Height: profile.Height, FPS: profile.FPS,
					SourceSeconds: float64(profile.Seconds), Hardware: profile.Hardware,
					Status: "skipped", Error: "node does not advertise the requested resource",
				})
				continue
			}
			suite.Results = append(suite.Results, r.transcodeNode(ctx, node.Metadata.Name, profile, profileIndex, nodeIndex))
		}
	}
	var cpu, gpu []float64
	failed := 0
	completed := 0
	for _, result := range suite.Results {
		if result.Status == "failed" {
			failed++
		}
		if result.Status != "ok" || result.RealtimeFactor <= 0 {
			continue
		}
		completed++
		if result.Hardware {
			gpu = append(gpu, result.RealtimeFactor)
		} else {
			cpu = append(cpu, result.RealtimeFactor)
		}
	}
	suite.Summary = model.TranscodeSummary{
		CPURealtimeFactor: stats.Distribution(cpu, "x realtime"),
		GPURealtimeFactor: stats.Distribution(gpu, "x realtime"),
	}
	if len(suite.Results) == 0 || (completed == 0 && failed == 0) {
		suite.Status = "skipped"
		suite.Error = "no transcode profile could run on the selected nodes"
	} else if failed == len(suite.Results) {
		suite.Status = "failed"
		suite.Error = "all transcode measurements failed"
	} else if failed > 0 {
		suite.Status = "partial"
		suite.Error = fmt.Sprintf("%d of %d transcode measurements failed", failed, len(suite.Results))
	}
	return suite
}

func transcodeNodes(profile config.TranscodeProfile, nodes []kubectl.Node) []kubectl.Node {
	if len(profile.Nodes) == 0 {
		return nodes
	}
	allowed := map[string]struct{}{}
	for _, name := range profile.Nodes {
		allowed[name] = struct{}{}
	}
	var result []kubectl.Node
	for _, node := range nodes {
		if _, exists := allowed[node.Metadata.Name]; exists {
			result = append(result, node)
		}
	}
	return result
}

func nodeSupportsResources(node kubectl.Node, resources map[string]string) bool {
	for name, requested := range resources {
		available, exists := node.Status.Allocatable[name]
		if !exists || quantityIsZero(available) || quantityGreater(requested, available) {
			return false
		}
	}
	return true
}

func quantityIsZero(value string) bool {
	value = strings.TrimSpace(value)
	return value == "" || value == "0" || value == "0m"
}

func quantityGreater(requested, available string) bool {
	return parseCPUQuantity(requested) > parseCPUQuantity(available)
}

func (r *Runner) transcodeNode(ctx context.Context, node string, profile config.TranscodeProfile, profileIndex, nodeIndex int) model.TranscodeResult {
	profileJSON, _ := json.Marshal(profile)
	volumes, mounts := hostPathVolumes(profile.HostPaths)
	var resources map[string]any
	if len(profile.Resources) > 0 {
		resources = map[string]any{"requests": profile.Resources, "limits": profile.Resources}
	}
	request := jobRequest{
		Name:         fmt.Sprintf("transcode-%d-%d-%s-%s", profileIndex, nodeIndex, node, r.runID),
		NodeName:     node,
		Volumes:      volumes,
		VolumeMounts: mounts,
		Resources:    resources,
		Args:         []string{"worker", "transcode", "--node=" + node},
		Environment: map[string]string{
			"NODE_NAME":                node,
			"YSCALE_TRANSCODE_PROFILE": string(profileJSON),
		},
		TimeoutSeconds: profile.Seconds*30 + 180,
	}
	execution, err := r.runJob(ctx, request)
	if err != nil {
		return model.TranscodeResult{Node: node, Profile: profile.Name, Encoder: profile.Encoder, Width: profile.Width, Height: profile.Height, FPS: profile.FPS, SourceSeconds: float64(profile.Seconds), Hardware: profile.Hardware, Status: "failed", Error: err.Error()}
	}
	result, err := decodeJobResult[model.TranscodeResult](execution)
	if err != nil {
		return model.TranscodeResult{Node: node, Profile: profile.Name, Encoder: profile.Encoder, Width: profile.Width, Height: profile.Height, FPS: profile.FPS, SourceSeconds: float64(profile.Seconds), Hardware: profile.Hardware, Status: "failed", Error: err.Error()}
	}
	return result
}
