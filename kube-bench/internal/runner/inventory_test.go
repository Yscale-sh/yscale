package runner

import (
	"testing"

	"github.com/JakeNesler/yscale-kube-bench/internal/config"
	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

func TestEffectiveProfileUserOverrideWins(t *testing.T) {
	inventory := model.NodeInventory{
		NodeName:       "worker-1",
		CPU:            model.CPUInfo{VisibleCPUModel: "QEMU Virtual CPU", LogicalCPUs: 8},
		Virtualization: model.VirtualizationInfo{GuestType: "vm", Hypervisor: "kvm", Platform: "qemu/kvm", Confidence: 0.9},
	}
	overrides := []config.NodeOverride{{
		Match:        config.NodeMatch{Name: "worker-1"},
		PhysicalHost: config.PhysicalHostOverride{Platform: "proxmox", CPUModel: "Intel Xeon"},
		Guest:        config.GuestOverride{CPUMode: "host", VCPUs: 8},
	}}
	profile := effectiveProfile(inventory, nil, overrides)
	if profile.PhysicalHost.Platform.Value != "proxmox" || profile.PhysicalHost.Platform.Source != "user" {
		t.Fatalf("unexpected platform: %+v", profile.PhysicalHost.Platform)
	}
	if profile.Guest.CPUModel.Value != "QEMU Virtual CPU" || profile.Guest.CPUMode.Value != "host" {
		t.Fatalf("unexpected guest: %+v", profile.Guest)
	}
}
