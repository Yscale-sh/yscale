package sysinfo

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/JakeNesler/yscale-kube-bench/internal/model"
)

func Collect(hostRoot string) model.NodeInventory {
	root := strings.TrimSpace(hostRoot)
	if root != "" {
		if _, err := os.Stat(filepath.Join(root, "proc", "cpuinfo")); err != nil {
			root = ""
		}
	}
	inventory := model.NodeInventory{
		Status:          "ok",
		Architecture:    runtime.GOARCH,
		OperatingSystem: runtime.GOOS,
		HostInspection:  root != "",
		InspectionRoot:  root,
	}
	inventory.CPU = collectCPU(root)
	inventory.Memory = collectMemory(root)
	inventory.DMI = collectDMI(root)
	inventory.Virtualization = detectVirtualization(root, inventory.CPU, inventory.DMI)
	inventory.BlockDevices = collectBlockDevices(root)
	inventory.NetworkInterfaces = collectNetworkInterfaces(root)
	inventory.NUMANodes = collectNUMA(root)
	inventory.KernelVersion = strings.TrimSpace(readFile(rooted(root, "/proc/sys/kernel/osrelease")))
	if osImage := parseOSRelease(readFile(rooted(root, "/etc/os-release"))); osImage != "" {
		inventory.OSImage = osImage
	}
	return inventory
}

func rooted(root, path string) string {
	if root == "" {
		return path
	}
	return filepath.Join(root, strings.TrimPrefix(path, "/"))
}

func readFile(path string) string {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return ""
	}
	return string(data)
}

func readTrim(path string) string {
	return strings.TrimSpace(readFile(path))
}

func parseOSRelease(data string) string {
	values := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		values[key] = strings.Trim(strings.TrimSpace(value), `"`)
	}
	if value := values["PRETTY_NAME"]; value != "" {
		return value
	}
	return strings.TrimSpace(values["NAME"] + " " + values["VERSION"])
}

func collectCPU(root string) model.CPUInfo {
	info := model.CPUInfo{LogicalCPUs: runtime.NumCPU(), CoreTypes: map[string]int{}}
	data := readFile(rooted(root, "/proc/cpuinfo"))
	physicalCores := map[string]struct{}{}
	sockets := map[string]struct{}{}
	var currentPhysicalID, currentCoreID string
	var maxMHz float64
	processorCount := 0
	for _, block := range strings.Split(data, "\n\n") {
		currentPhysicalID = ""
		currentCoreID = ""
		for _, line := range strings.Split(block, "\n") {
			key, value, found := strings.Cut(line, ":")
			if !found {
				continue
			}
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			switch key {
			case "processor":
				processorCount++
			case "model name":
				if info.Model == "" {
					info.Model = value
					info.VisibleCPUModel = value
				}
			case "Hardware", "Processor":
				if info.Model == "" && value != "" && !strings.HasPrefix(value, "0x") {
					info.Model = value
					info.VisibleCPUModel = value
				}
			case "vendor_id", "CPU implementer":
				if info.Vendor == "" {
					info.Vendor = value
				}
			case "physical id":
				currentPhysicalID = value
				sockets[value] = struct{}{}
			case "core id":
				currentCoreID = value
			case "cpu MHz":
				mhz, _ := strconv.ParseFloat(value, 64)
				if mhz > maxMHz {
					maxMHz = mhz
				}
			case "flags", "Features":
				if len(info.Flags) == 0 {
					info.Flags = strings.Fields(value)
				}
			}
		}
		if currentCoreID != "" {
			physicalCores[currentPhysicalID+":"+currentCoreID] = struct{}{}
		}
	}
	info.MaxMHz = maxMHz
	if root != "" && processorCount > 0 {
		info.LogicalCPUs = processorCount
	}
	if len(physicalCores) > 0 {
		info.PhysicalCores = len(physicalCores)
	}
	if len(sockets) > 0 {
		info.Sockets = len(sockets)
	}
	for _, flag := range info.Flags {
		if flag == "hypervisor" {
			info.HypervisorFlag = true
			break
		}
	}
	cpuDirs, _ := filepath.Glob(rooted(root, "/sys/devices/system/cpu/cpu[0-9]*"))
	for _, dir := range cpuDirs {
		coreType := readTrim(filepath.Join(dir, "topology", "core_type"))
		if coreType != "" {
			info.CoreTypes["type-"+coreType]++
		}
		freq := readTrim(filepath.Join(dir, "cpufreq", "cpuinfo_max_freq"))
		if khz, err := strconv.ParseFloat(freq, 64); err == nil && khz/1000 > info.MaxMHz {
			info.MaxMHz = khz / 1000
		}
	}
	if len(info.CoreTypes) == 0 {
		info.CoreTypes = nil
	}
	if info.PhysicalCores == 0 {
		info.PhysicalCores = info.LogicalCPUs
	}
	if info.Sockets == 0 {
		info.Sockets = 1
	}
	return info
}

func collectMemory(root string) model.MemoryInfo {
	values := map[string]int64{}
	scanner := bufio.NewScanner(strings.NewReader(readFile(rooted(root, "/proc/meminfo"))))
	for scanner.Scan() {
		line := scanner.Text()
		key, rest, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			continue
		}
		if len(fields) > 1 && strings.EqualFold(fields[1], "kB") {
			value *= 1024
		}
		values[key] = value
	}
	return model.MemoryInfo{
		TotalBytes:     values["MemTotal"],
		AvailableBytes: values["MemAvailable"],
		SwapTotalBytes: values["SwapTotal"],
		HugePagesBytes: values["HugePages_Total"] * values["Hugepagesize"],
	}
}

func collectDMI(root string) model.DMIInfo {
	base := rooted(root, "/sys/class/dmi/id")
	return model.DMIInfo{
		SystemVendor: readTrim(filepath.Join(base, "sys_vendor")),
		ProductName:  readTrim(filepath.Join(base, "product_name")),
		ProductUUID:  readTrim(filepath.Join(base, "product_uuid")),
		BoardVendor:  readTrim(filepath.Join(base, "board_vendor")),
		BoardName:    readTrim(filepath.Join(base, "board_name")),
		BIOSVendor:   readTrim(filepath.Join(base, "bios_vendor")),
		BIOSVersion:  readTrim(filepath.Join(base, "bios_version")),
	}
}

func detectVirtualization(root string, cpu model.CPUInfo, dmi model.DMIInfo) model.VirtualizationInfo {
	joined := strings.ToLower(strings.Join([]string{dmi.SystemVendor, dmi.ProductName, dmi.BoardVendor, dmi.BoardName, dmi.BIOSVendor, dmi.BIOSVersion}, " "))
	cgroup := strings.ToLower(readFile(rooted(root, "/proc/1/cgroup")))
	environ := strings.ToLower(strings.ReplaceAll(readFile(rooted(root, "/proc/1/environ")), "\x00", " "))
	result := model.VirtualizationInfo{ContainerLayer: "kubernetes-pod", NodeEnvironment: "unknown", Confidence: 0.25}

	if strings.Contains(cgroup, "kubepods") || strings.Contains(cgroup, "containerd") || strings.Contains(cgroup, "cri-containerd") {
		result.Evidence = append(result.Evidence, "benchmark process is running in a Kubernetes container")
	}
	if strings.Contains(cgroup, "lxc") || strings.Contains(environ, "container=lxc") {
		result.NodeEnvironment = "container"
		result.GuestType = "lxc"
		result.Hypervisor = "lxc"
		result.Confidence = 0.9
		result.Evidence = append(result.Evidence, "node PID 1 exposes LXC markers")
	}

	type candidate struct {
		needle     string
		hypervisor string
		platform   string
	}
	candidates := []candidate{
		{"proxmox", "kvm", "proxmox"},
		{"vmware", "vmware", "vmware"},
		{"virtualbox", "virtualbox", "virtualbox"},
		{"microsoft corporation virtual machine", "hyper-v", "hyper-v"},
		{"xen", "xen", "xen"},
		{"amazon ec2", "nitro", "aws"},
		{"google compute engine", "kvm", "gcp"},
		{"openstack", "kvm", "openstack"},
		{"qemu", "kvm", "qemu/kvm"},
		{"kvm", "kvm", "kvm"},
	}
	for _, item := range candidates {
		if strings.Contains(joined, item.needle) {
			result.NodeEnvironment = "virtual-machine"
			result.GuestType = "vm"
			result.Hypervisor = item.hypervisor
			result.Platform = item.platform
			result.Confidence = 0.9
			result.Evidence = append(result.Evidence, fmt.Sprintf("DMI contains %q", item.needle))
			break
		}
	}
	if result.NodeEnvironment == "unknown" && cpu.HypervisorFlag {
		result.NodeEnvironment = "virtual-machine"
		result.GuestType = "vm"
		result.Confidence = 0.7
		result.Evidence = append(result.Evidence, "CPU exposes the hypervisor flag")
	}
	if result.NodeEnvironment == "unknown" {
		result.NodeEnvironment = "bare-metal"
		result.GuestType = "bare-metal"
		result.Platform = "bare-metal"
		result.Confidence = 0.6
		result.Evidence = append(result.Evidence, "no VM or host-container markers were detected")
	}
	if result.Platform == "qemu/kvm" || (result.Hypervisor == "kvm" && result.Platform == "") {
		result.Limitations = append(result.Limitations, "KVM/QEMU is visible, but Proxmox cannot be proven from inside a normal guest; use a node override for the physical platform")
	}
	if root == "" {
		result.Limitations = append(result.Limitations, "host inspection is disabled; sysfs and procfs may be namespace-filtered by the container runtime")
	}
	sort.Strings(result.Evidence)
	return result
}

func collectBlockDevices(root string) []model.BlockDevice {
	paths, _ := filepath.Glob(rooted(root, "/sys/block/*"))
	devices := make([]model.BlockDevice, 0, len(paths))
	for _, path := range paths {
		name := filepath.Base(path)
		if strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "ram") {
			continue
		}
		sectors, _ := strconv.ParseInt(readTrim(filepath.Join(path, "size")), 10, 64)
		logical, _ := strconv.ParseInt(readTrim(filepath.Join(path, "queue", "logical_block_size")), 10, 64)
		physical, _ := strconv.ParseInt(readTrim(filepath.Join(path, "queue", "physical_block_size")), 10, 64)
		rotationalRaw := readTrim(filepath.Join(path, "queue", "rotational"))
		var rotational *bool
		if rotationalRaw == "0" || rotationalRaw == "1" {
			value := rotationalRaw == "1"
			rotational = &value
		}
		deviceType := "block"
		if strings.HasPrefix(name, "nvme") {
			deviceType = "nvme"
		} else if strings.HasPrefix(name, "sd") || strings.HasPrefix(name, "vd") {
			deviceType = "disk"
		}
		devices = append(devices, model.BlockDevice{
			Name:               name,
			Model:              readTrim(filepath.Join(path, "device", "model")),
			Vendor:             readTrim(filepath.Join(path, "device", "vendor")),
			Serial:             readTrim(filepath.Join(path, "device", "serial")),
			Type:               deviceType,
			SizeBytes:          sectors * 512,
			Rotational:         rotational,
			LogicalBlockBytes:  logical,
			PhysicalBlockBytes: physical,
			Scheduler:          selectedScheduler(readTrim(filepath.Join(path, "queue", "scheduler"))),
		})
	}
	sort.Slice(devices, func(i, j int) bool { return devices[i].Name < devices[j].Name })
	return devices
}

func selectedScheduler(value string) string {
	for _, field := range strings.Fields(value) {
		if strings.HasPrefix(field, "[") && strings.HasSuffix(field, "]") {
			return strings.Trim(field, "[]")
		}
	}
	return value
}

func collectNetworkInterfaces(root string) []model.NetworkInterface {
	paths, _ := filepath.Glob(rooted(root, "/sys/class/net/*"))
	interfaces := make([]model.NetworkInterface, 0, len(paths))
	for _, path := range paths {
		name := filepath.Base(path)
		if name == "lo" {
			continue
		}
		mtu, _ := strconv.ParseInt(readTrim(filepath.Join(path, "mtu")), 10, 64)
		speed, _ := strconv.ParseInt(readTrim(filepath.Join(path, "speed")), 10, 64)
		driver := ""
		if target, err := filepath.EvalSymlinks(filepath.Join(path, "device", "driver")); err == nil {
			driver = filepath.Base(target)
		}
		interfaces = append(interfaces, model.NetworkInterface{
			Name:       name,
			State:      readTrim(filepath.Join(path, "operstate")),
			MTU:        mtu,
			SpeedMbps:  speed,
			Driver:     driver,
			MACAddress: readTrim(filepath.Join(path, "address")),
		})
	}
	sort.Slice(interfaces, func(i, j int) bool { return interfaces[i].Name < interfaces[j].Name })
	return interfaces
}

func collectNUMA(root string) []model.NUMANode {
	paths, _ := filepath.Glob(rooted(root, "/sys/devices/system/node/node[0-9]*"))
	nodes := make([]model.NUMANode, 0, len(paths))
	for _, path := range paths {
		id, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(path), "node"))
		if err != nil {
			continue
		}
		cpus := parseCPUList(readTrim(filepath.Join(path, "cpulist")))
		var memoryBytes int64
		scanner := bufio.NewScanner(strings.NewReader(readFile(filepath.Join(path, "meminfo"))))
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.Contains(line, "MemTotal") {
				continue
			}
			fields := strings.Fields(line)
			for index, field := range fields {
				if field == "MemTotal:" && index+1 < len(fields) {
					value, _ := strconv.ParseInt(fields[index+1], 10, 64)
					memoryBytes = value * 1024
				}
			}
		}
		nodes = append(nodes, model.NUMANode{ID: id, CPUs: cpus, Memory: memoryBytes})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

func parseCPUList(value string) []int {
	var result []int
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		startRaw, endRaw, rangeFound := strings.Cut(part, "-")
		start, err := strconv.Atoi(startRaw)
		if err != nil {
			continue
		}
		if !rangeFound {
			result = append(result, start)
			continue
		}
		end, err := strconv.Atoi(endRaw)
		if err != nil || end < start {
			continue
		}
		for value := start; value <= end; value++ {
			result = append(result, value)
		}
	}
	return result
}
