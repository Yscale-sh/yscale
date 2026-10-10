package sysinfo

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCollectDetectsQEMU(t *testing.T) {
	root := t.TempDir()
	write := func(path, value string) {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("proc/cpuinfo", "processor : 0\nmodel name : Virtual CPU\nflags : fpu hypervisor\n\nprocessor : 1\nmodel name : Virtual CPU\nflags : fpu hypervisor\n\n")
	write("proc/meminfo", "MemTotal: 1024 kB\nMemAvailable: 512 kB\n")
	write("proc/sys/kernel/osrelease", "6.8.0")
	write("proc/1/cgroup", "0::/system.slice")
	write("sys/class/dmi/id/sys_vendor", "QEMU")
	write("sys/class/dmi/id/product_name", "Standard PC (Q35 + ICH9, 2009)")
	inventory := Collect(root)
	if inventory.Virtualization.NodeEnvironment != "virtual-machine" || inventory.Virtualization.Hypervisor != "kvm" {
		t.Fatalf("unexpected virtualization: %+v", inventory.Virtualization)
	}
	if len(inventory.Virtualization.Limitations) == 0 {
		t.Fatal("expected Proxmox detection limitation")
	}
	if inventory.CPU.LogicalCPUs != 2 {
		t.Fatalf("host inspection should count host processors, got %d", inventory.CPU.LogicalCPUs)
	}
}

func TestParseCPUList(t *testing.T) {
	values := parseCPUList("0-2,4,6-7")
	if len(values) != 6 || values[3] != 4 || values[5] != 7 {
		t.Fatalf("unexpected values: %v", values)
	}
}
