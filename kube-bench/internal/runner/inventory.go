package runner

import (
	"context"
	"fmt"
	"sync"

	"github.com/JakeNesler/yscale-kube-bench/internal/config"
	"github.com/JakeNesler/yscale-kube-bench/internal/kubectl"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

func (r *Runner) runInventory(ctx context.Context, nodes []kubectl.Node) []model.NodeResult {
	results := make([]model.NodeResult, len(nodes))
	parallelism := r.config.Spec.Parallelism
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
			results[index] = r.inventoryNode(ctx, node)
		}()
	}
	wait.Wait()
	return results
}

func (r *Runner) inventoryNode(ctx context.Context, node kubectl.Node) model.NodeResult {
	request := jobRequest{
		Name:     "inv-" + node.Metadata.Name + "-" + r.runID,
		NodeName: node.Metadata.Name,
		Args:     []string{"worker", "inventory", "--node=" + node.Metadata.Name},
		Environment: map[string]string{
			"NODE_NAME": node.Metadata.Name,
		},
		Privileged: r.config.Spec.Inventory.Privileged,
	}
	if r.config.Spec.Inventory.HostInspection {
		request.Volumes, request.VolumeMounts = inventoryHostVolumes()
		request.Args = append(request.Args, "--host-root=/host")
		request.Environment["YSCALE_HOST_ROOT"] = "/host"
	}
	execution, err := r.runJob(ctx, request)
	inventory := model.NodeInventory{Status: "failed", Error: "inventory worker did not return a result", NodeName: node.Metadata.Name}
	var warnings []string
	if err != nil {
		inventory.Error = err.Error()
		warnings = append(warnings, fmt.Sprintf("node %s inventory failed: %v", node.Metadata.Name, err))
	} else if decoded, decodeErr := decodeJobResult[model.NodeInventory](execution); decodeErr != nil {
		inventory.Error = decodeErr.Error()
		warnings = append(warnings, fmt.Sprintf("node %s inventory output was invalid: %v", node.Metadata.Name, decodeErr))
	} else {
		inventory = decoded
	}
	mergeKubernetesInventory(&inventory, node)
	for _, limitation := range inventory.Virtualization.Limitations {
		warnings = append(warnings, node.Metadata.Name+": "+limitation)
	}
	effective := effectiveProfile(inventory, node.Metadata.Labels, r.config.Spec.NodeOverrides)
	return model.NodeResult{
		Name:      node.Metadata.Name,
		Inventory: inventory,
		Effective: effective,
		Warnings:  uniqueSorted(warnings),
	}
}

func mergeKubernetesInventory(inventory *model.NodeInventory, node kubectl.Node) {
	inventory.NodeName = node.Metadata.Name
	inventory.Architecture = choose(node.Status.NodeInfo.Architecture, inventory.Architecture)
	inventory.OperatingSystem = choose(node.Status.NodeInfo.OperatingSystem, inventory.OperatingSystem)
	inventory.OSImage = choose(node.Status.NodeInfo.OSImage, inventory.OSImage)
	inventory.KernelVersion = choose(node.Status.NodeInfo.KernelVersion, inventory.KernelVersion)
	inventory.KubeletVersion = node.Status.NodeInfo.KubeletVersion
	inventory.ContainerRuntime = node.Status.NodeInfo.ContainerRuntimeVersion
	inventory.ProviderID = node.Spec.ProviderID
	inventory.MachineID = node.Status.NodeInfo.MachineID
	inventory.SystemUUID = node.Status.NodeInfo.SystemUUID
	inventory.BootID = node.Status.NodeInfo.BootID
	inventory.Capacity = copyMap(node.Status.Capacity)
	inventory.Allocatable = copyMap(node.Status.Allocatable)
	if inventory.Memory.TotalBytes == 0 {
		inventory.Memory.TotalBytes = parseByteQuantity(node.Status.Capacity["memory"])
	}
	if inventory.CPU.LogicalCPUs == 0 {
		inventory.CPU.LogicalCPUs = int(parseCPUQuantity(node.Status.Capacity["cpu"]))
	}
	inventory.Labels = copyMap(node.Metadata.Labels)
}

func effectiveProfile(inventory model.NodeInventory, labels map[string]string, overrides []config.NodeOverride) model.EffectiveProfile {
	profile := model.EffectiveProfile{Tags: map[string]string{}}
	confidence := inventory.Virtualization.Confidence
	profile.Guest.Type = sourcedString(inventory.Virtualization.GuestType, "detected", confidence)
	profile.Guest.CPUModel = sourcedString(inventory.CPU.VisibleCPUModel, "detected", 0.95)
	profile.Guest.VCPUs = model.SourcedInt{Value: inventory.CPU.LogicalCPUs, Source: "detected", Confidence: 0.95}
	profile.Guest.MemoryBytes = inventory.Memory.TotalBytes
	profile.PhysicalHost.Hypervisor = sourcedString(inventory.Virtualization.Hypervisor, "detected", confidence)
	profile.PhysicalHost.Platform = sourcedString(inventory.Virtualization.Platform, "detected", confidence)
	if inventory.Virtualization.NodeEnvironment == "bare-metal" {
		profile.PhysicalHost.CPUModel = sourcedString(inventory.CPU.VisibleCPUModel, "detected", 0.9)
	}
	for _, override := range overrides {
		if !overrideMatches(override.Match, inventory.NodeName, labels) {
			continue
		}
		applyOverride(&profile, override)
	}
	if len(profile.Tags) == 0 {
		profile.Tags = nil
	}
	return profile
}

func applyOverride(profile *model.EffectiveProfile, override config.NodeOverride) {
	setUserString(&profile.PhysicalHost.Name, override.PhysicalHost.Name)
	setUserString(&profile.PhysicalHost.CPUModel, override.PhysicalHost.CPUModel)
	setUserString(&profile.PhysicalHost.Platform, override.PhysicalHost.Platform)
	setUserString(&profile.PhysicalHost.Hypervisor, override.PhysicalHost.Hypervisor)
	setUserString(&profile.PhysicalHost.StorageBackend, override.PhysicalHost.StorageBackend)
	setUserString(&profile.PhysicalHost.NetworkBackend, override.PhysicalHost.NetworkBackend)
	setUserString(&profile.Guest.Type, override.Guest.Type)
	setUserString(&profile.Guest.CPUModel, override.Guest.CPUModel)
	setUserString(&profile.Guest.CPUMode, override.Guest.CPUMode)
	if override.Guest.VCPUs > 0 {
		profile.Guest.VCPUs = model.SourcedInt{Value: override.Guest.VCPUs, Source: "user", Confidence: 1}
	}
	if override.Guest.MemoryBytes > 0 {
		profile.Guest.MemoryBytes = override.Guest.MemoryBytes
	}
	profile.Economics = model.EconomicsProfile{
		PurchasePriceUSD: chooseFloat(override.Economics.PurchasePriceUSD, profile.Economics.PurchasePriceUSD),
		IdleWatts:        chooseFloat(override.Economics.IdleWatts, profile.Economics.IdleWatts),
		PeakWatts:        chooseFloat(override.Economics.PeakWatts, profile.Economics.PeakWatts),
		HourlyCostUSD:    chooseFloat(override.Economics.HourlyCostUSD, profile.Economics.HourlyCostUSD),
	}
	if profile.Tags == nil {
		profile.Tags = map[string]string{}
	}
	for key, value := range override.Tags {
		profile.Tags[key] = value
	}
}

func overrideMatches(match config.NodeMatch, name string, labels map[string]string) bool {
	if match.Name != "" && match.Name != name {
		return false
	}
	return matchesLabels(labels, match.Labels)
}

func sourcedString(value, source string, confidence float64) model.SourcedString {
	if value == "" {
		return model.SourcedString{}
	}
	return model.SourcedString{Value: value, Source: source, Confidence: confidence}
}

func setUserString(target *model.SourcedString, value string) {
	if value != "" {
		*target = model.SourcedString{Value: value, Source: "user", Confidence: 1}
	}
}

func chooseFloat(value, fallback float64) float64 {
	if value > 0 {
		return value
	}
	return fallback
}

func copyMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
