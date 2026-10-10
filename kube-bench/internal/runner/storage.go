package runner

import (
	"context"
	"fmt"

	"github.com/JakeNesler/yscale-kube-bench/internal/config"
	"github.com/JakeNesler/yscale-kube-bench/internal/kubectl"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
	"github.com/JakeNesler/yscale-kube-bench/internal/stats"
)

func (r *Runner) runStorage(ctx context.Context, nodes []kubectl.Node) *model.StorageSuite {
	suite := &model.StorageSuite{Status: "ok"}
	for targetIndex, target := range r.config.Spec.Storage.Targets {
		selected := storageTargetNodes(target, nodes)
		for nodeIndex, node := range selected {
			result := r.storageNode(ctx, node.Metadata.Name, target, targetIndex, nodeIndex)
			suite.Results = append(suite.Results, result)
		}
	}
	var seqRead, seqWrite, randomRead, randomWrite, fsync []float64
	failed := 0
	partial := 0
	skipped := 0
	for _, result := range suite.Results {
		switch result.Status {
		case "failed":
			failed++
		case "partial":
			partial++
		case "skipped":
			skipped++
		}
		for _, profile := range result.Profiles {
			if profile.Status != "ok" {
				continue
			}
			switch profile.Name {
			case "sequential-read":
				seqRead = append(seqRead, profile.ReadMBps)
			case "sequential-write":
				seqWrite = append(seqWrite, profile.WriteMBps)
			case "random-read-4k":
				randomRead = append(randomRead, profile.ReadIOPS)
			case "random-write-4k":
				randomWrite = append(randomWrite, profile.WriteIOPS)
			case "fsync-4k":
				if profile.FsyncP99MS > 0 {
					fsync = append(fsync, profile.FsyncP99MS)
				}
			}
		}
	}
	suite.Summary = model.StorageSummary{
		SequentialReadMBps:  stats.Distribution(seqRead, "MB/s"),
		SequentialWriteMBps: stats.Distribution(seqWrite, "MB/s"),
		RandomReadIOPS:      stats.Distribution(randomRead, "IOPS"),
		RandomWriteIOPS:     stats.Distribution(randomWrite, "IOPS"),
		FsyncP99MS:          stats.Distribution(fsync, "ms"),
	}
	if len(suite.Results) == 0 {
		suite.Status = "skipped"
		suite.Error = "no storage target matched a selected node"
	} else if skipped == len(suite.Results) {
		suite.Status = "skipped"
		suite.Error = "all storage measurements were skipped"
	} else if failed == len(suite.Results) {
		suite.Status = "failed"
		suite.Error = "all storage measurements failed"
	} else if failed > 0 || partial > 0 || skipped > 0 {
		suite.Status = "partial"
		if failed > 0 {
			suite.Error = fmt.Sprintf("%d of %d storage measurements failed", failed, len(suite.Results))
		}
		if partial > 0 {
			if suite.Error != "" {
				suite.Error += "; "
			}
			suite.Error += fmt.Sprintf("%d of %d storage measurements completed partially", partial, len(suite.Results))
		}
		if skipped > 0 {
			if suite.Error != "" {
				suite.Error += "; "
			}
			suite.Error += fmt.Sprintf("%d of %d storage measurements skipped", skipped, len(suite.Results))
		}
	}
	return suite
}

func storageTargetNodes(target config.StorageTarget, nodes []kubectl.Node) []kubectl.Node {
	if len(target.Nodes) > 0 {
		allowed := map[string]struct{}{}
		for _, name := range target.Nodes {
			allowed[name] = struct{}{}
		}
		var selected []kubectl.Node
		for _, node := range nodes {
			if _, exists := allowed[node.Metadata.Name]; exists {
				selected = append(selected, node)
			}
		}
		return selected
	}
	if target.Kind == "pvc" {
		if len(nodes) == 0 {
			return nil
		}
		return nodes[:1]
	}
	return nodes
}

func (r *Runner) storageNode(ctx context.Context, node string, target config.StorageTarget, targetIndex, nodeIndex int) model.StorageResult {
	volumes, mounts, mountPath := storageVolume(target)
	request := jobRequest{
		Name:         fmt.Sprintf("storage-%d-%d-%s-%s", targetIndex, nodeIndex, node, r.runID),
		NodeName:     node,
		Volumes:      volumes,
		VolumeMounts: mounts,
		Args: []string{
			"worker", "storage",
			"--node=" + node,
			"--target=" + target.Name,
			"--kind=" + target.Kind,
			"--path=" + mountPath,
			"--device-hint=" + target.DeviceHint,
			fmt.Sprintf("--duration=%ds", r.config.Spec.Storage.DurationSeconds),
			fmt.Sprintf("--size-mib=%d", r.config.Spec.Storage.SizeMiB),
			"--engine=" + r.config.Spec.Storage.IOEngine,
		},
		Environment:    map[string]string{"NODE_NAME": node},
		TimeoutSeconds: r.config.Spec.Storage.DurationSeconds*10 + 180,
	}
	execution, err := r.runJob(ctx, request)
	if err != nil {
		return model.StorageResult{Node: node, Target: target.Name, Kind: target.Kind, Path: mountPath, DeviceHint: target.DeviceHint, Status: "failed", Error: err.Error()}
	}
	result, err := decodeJobResult[model.StorageResult](execution)
	if err != nil {
		return model.StorageResult{Node: node, Target: target.Name, Kind: target.Kind, Path: mountPath, DeviceHint: target.DeviceHint, Status: "failed", Error: err.Error()}
	}
	return result
}
