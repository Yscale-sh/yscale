// Package pricing is the central server's price catalog: per-SKU
// USD/hour rates plus selection helpers the decider uses to pick the
// cheapest fit within a workload's budget. Prices are hand-curated;
// the long-term plan is a CI script that regenerates them from the
// upstream catalogs.
//
// Today the priced GPU supply is Linode (RTX-class, dedicated). Fly
// covers CPU. Add a per-backend price table here as each new node
// backend lands.
package pricing

import (
	"strings"
)

// MarginMultiplier is applied to upstream backend cost to derive the
// customer-facing price — a 20% yscale premium that covers the
// abstracted-availability/orchestration value. Customer-facing
// surfaces and workload GPU price admission apply it. Recorded provider cost
// remains raw, so the premium is NOT baked into the catalog.
const MarginMultiplier = 1.20

// CustomerPrice applies the yscale margin to an upstream per-hour cost.
func CustomerPrice(upstreamUSDPerHour float64) float64 {
	return upstreamUSDPerHour * MarginMultiplier
}

// linodeGPUPrices maps the abstract Linode GPU kind to the per-hour
// cost of its smallest (1-GPU) plan — g2-gpu-rtx4000a1-s and
// g1-gpu-rtx6000-1 respectively. Linode GPU instances are dedicated;
// there is no spot tier. Refreshed by hand against the Linode types
// API at each release.
var linodeGPUPrices = map[string]float64{
	"rtx4000ada": 0.52,
	"rtx6000":    1.50,
}

// LookupLinodeGPU returns the per-hour upstream cost for a Linode GPU
// kind, or (0, false) when the kind isn't a Linode GPU offering.
func LookupLinodeGPU(kind string) (float64, bool) {
	p, ok := linodeGPUPrices[strings.ToLower(kind)]
	return p, ok
}

// FlyMachine prices Fly machine sizes. Numbers are baseline shared-cpu
// rates; performance-* costs more per vCPU. Verified against
// fly.io/docs/about/pricing on the last refresh.
//
// Fly GPU is intentionally absent — the offering was deprecated in
// August 2025; GPU workloads route to Linode.
var flyMachineRates = map[string]float64{
	"shared-cpu-1x":   0.0067,
	"shared-cpu-2x":   0.0134,
	"shared-cpu-4x":   0.0268,
	"shared-cpu-8x":   0.0536,
	"performance-1x":  0.0185,
	"performance-2x":  0.0370,
	"performance-4x":  0.0740,
	"performance-8x":  0.1480,
	"performance-16x": 0.2960,
}

// FlyMachine returns the per-hour price for a Fly machine class, or
// (0, false) if the class isn't priced. CPU pricing only — no GPU.
func FlyMachine(machineType string) (float64, bool) {
	p, ok := flyMachineRates[machineType]
	return p, ok
}

// EstimateFlyCPU returns a baseline CPU+memory price for an arbitrary
// (cpuMillis, memoryMB) shape, used when the workload didn't pin a
// machine type.
func EstimateFlyCPU(cpuMillis, memoryMB int64) float64 {
	cpus := float64(cpuMillis) / 1000.0
	memGB := float64(memoryMB) / 1024.0
	if cpus < 1 {
		cpus = 1
	}
	if memGB < 2 {
		memGB = 2
	}
	return 0.0067*cpus + 0.0048*memGB
}

// awsInstanceRates maps EC2 instance types to us-east-1 on-demand Linux
// per-hour rates. The set here is exactly what the aws backend's mapType
// emits. Verified against the AWS Pricing API 2026-06-09 (exact);
// re-verify with scripts/refresh-prices.sh.
var awsInstanceRates = map[string]float64{
	"t3.small":    0.0208,
	"t3.medium":   0.0416,
	"m7i.large":   0.1008,
	"m7i.xlarge":  0.2016,
	"m7i.2xlarge": 0.4032,
}

// awsGPUInstanceRates maps the EC2 GPU instance types the aws backend's
// mapGPUType emits to us-east-1 on-demand Linux per-hour rates.
//
// APPROXIMATE: hand-entered from the public on-demand price list (2026-06)
// and NOT yet machine-verified against the AWS Pricing API the way the CPU
// rates above are — GPU SKUs were unpriced while the backend was CPU-only.
// Re-verify with scripts/refresh-prices.sh before these drive customer
// billing. They are estimates only and MUST NOT enforce customer hard caps;
// hard-cap admission also requires authoritative pricing for the launch region.
var awsGPUInstanceRates = map[string]float64{
	// g4dn — NVIDIA T4
	"g4dn.xlarge":   0.526,
	"g4dn.12xlarge": 3.912,
	"g4dn.metal":    7.824,
	// g5 — NVIDIA A10G
	"g5.xlarge":   1.006,
	"g5.12xlarge": 5.672,
	"g5.48xlarge": 16.288,
	// g6 — NVIDIA L4
	"g6.xlarge":   0.8048,
	"g6.12xlarge": 4.6013,
	"g6.48xlarge": 13.3504,
	// g6e — NVIDIA L40S
	"g6e.xlarge":   1.861,
	"g6e.12xlarge": 10.4926,
	"g6e.48xlarge": 30.1336,
	// p4d/p5/p5e — A100 / H100 / H200 (8-GPU nodes only). p5e (H200) lists
	// higher than p5 (H100) — do not alias them to the same rate.
	"p4d.24xlarge": 32.7726,
	"p5.48xlarge":  98.32,
	"p5e.48xlarge": 108.53,
}

// AWSInstance returns the per-hour price for an EC2 instance type, CPU or
// GPU. The two rate tables are kept separate (CPU is API-verified, GPU is
// approximate) but resolve through this one lookup so callers — the
// decider's SKU pricing and the unified HourlyUSD — need not know which
// family a type belongs to.
func AWSInstance(instanceType string) (float64, bool) {
	if p, ok := awsInstanceRates[instanceType]; ok {
		return p, true
	}
	p, ok := awsGPUInstanceRates[instanceType]
	return p, ok
}

// EstimateAWS maps a memory request to the instance type the aws backend
// would pick (mirrors its mapType thresholds) and returns (rate, type).
func EstimateAWS(memoryMB int64) (float64, string) {
	t := "m7i.2xlarge"
	switch {
	case memoryMB <= 1536:
		t = "t3.small"
	case memoryMB <= 3072:
		t = "t3.medium"
	case memoryMB <= 6144:
		t = "m7i.large"
	case memoryMB <= 14336:
		t = "m7i.xlarge"
	}
	return awsInstanceRates[t], t
}

// azureInstanceRates maps Azure VM sizes to per-hour pay-as-you-go cost,
// Linux, eastus, Consumption price type. Fetched from the Azure Retail
// Prices API — which needs no authentication, so scripts/refresh-prices.sh
// can re-verify these on any machine. Windows and Spot rows share an
// armSkuName and must be filtered out; a Windows rate is roughly double.
//
// Azure GPU is deliberately absent: the backend rejects GPU specs until
// per-subscription GPU quota is approved, so a rate here would price a
// burst that cannot be created.
var azureInstanceRates = map[string]float64{
	"Standard_B1ms":   0.0207, // 1 vCPU / 2 GB
	"Standard_B2s_v2": 0.0832, // 2 vCPU / 4 GB
	"Standard_D2s_v5": 0.0960, // 2 vCPU / 8 GB
	"Standard_D4s_v5": 0.1920, // 4 vCPU / 16 GB
	"Standard_D8s_v5": 0.3840, // 8 vCPU / 32 GB
}

// AzureInstance returns the per-hour price for an Azure VM size.
func AzureInstance(size string) (float64, bool) {
	p, ok := azureInstanceRates[size]
	return p, ok
}

// EstimateAzureCPU maps a memory request to the VM size the azure backend
// would pick (mirrors its mapType thresholds) and returns (rate, size).
func EstimateAzureCPU(memoryMB int64) (float64, string) {
	t := "Standard_D8s_v5"
	switch {
	case memoryMB <= 512:
		t = "Standard_B1ms"
	case memoryMB <= 1536:
		t = "Standard_B2s_v2"
	case memoryMB <= 3072:
		t = "Standard_D2s_v5"
	case memoryMB <= 6144:
		t = "Standard_D4s_v5"
	}
	return azureInstanceRates[t], t
}

// GCP E2 on-demand rates for us-central1, us-east1, and us-west1.
// Shared-core e2-small/e2-medium have fixed prices; standard shapes use
// core SKU CF4E-A0C7-E3BF ($0.02181159/vCPU-hour) plus RAM SKU
// F449-33EC-A5EF ($0.00292353/GiB-hour). Verified 2026-08-12 against
// Google's public SKU list and E2 pricing table.
var gcpMachineTypeRates = map[string]float64{
	"e2-small":      0.016752855,
	"e2-medium":     0.03350571,
	"e2-standard-2": 0.02181159*2 + 0.00292353*8,
	"e2-standard-4": 0.02181159*4 + 0.00292353*16,
	"e2-standard-8": 0.02181159*8 + 0.00292353*32,
}

var gcpPricedRegions = map[string]bool{
	"us-central1": true,
	"us-east1":    true,
	"us-west1":    true,
}

// GCPRegionPriced reports whether region or zone has verified rates.
func GCPRegionPriced(region string) bool {
	if r, ok := regionFromZone(region); ok {
		return gcpPricedRegions[r]
	}
	return gcpPricedRegions[region]
}

// regionFromZone strips a GCP zone's trailing "-<letter>" zone suffix
// to derive its region (e.g. "us-central1-a" -> "us-central1"). Mirrors
// the gcp backend's own regionFromZone so the catalog and the backend
// agree on what counts as "the same region". Returns false for bare
// regions — those need to be looked up directly in gcpPricedRegions.
func regionFromZone(zone string) (string, bool) {
	// A bare region has no trailing single-letter zone. The gcp
	// backend's isZone() treats at least three hyphen-separated parts
	// as a real zone; mirror that here so a name like "us-central1"
	// (two parts) does not get mis-trimmed into "us" + "central1".
	parts := strings.Split(zone, "-")
	if len(parts) < 3 {
		return "", false
	}
	if len(parts[len(parts)-1]) != 1 {
		return "", false
	}
	return strings.Join(parts[:len(parts)-1], "-"), true
}

// GCPMachine returns the upstream hourly rate for a launchable E2 type.
func GCPMachine(machineType string) (float64, bool) {
	p, ok := gcpMachineTypeRates[machineType]
	return p, ok
}

// EstimateGCPCPU maps a memory request to the E2 machine type the gcp
// backend would pick (mirrors its mapType thresholds) and returns
// (rate, type). The threshold ladder MUST mirror pkg/backends/gcp.mapType
// exactly — same boundaries, same default — or the persisted HourlyUSD
// under-reports the real upstream cost (and the budget cap it enforces)
// for the machines the backend actually launches.
// TestEstimateGCPCPUParity (in this package) guards this invariant
// against gcp.MapType, the backend's own selector — same guarantee the
// Azure/Linode parity tests already enforce.
func EstimateGCPCPU(memoryMB int64) (float64, string) {
	t := "e2-standard-8"
	switch {
	case memoryMB <= 1024:
		t = "e2-small"
	case memoryMB <= 3072:
		t = "e2-medium"
	case memoryMB <= 6144:
		t = "e2-standard-2"
	case memoryMB <= 14336:
		t = "e2-standard-4"
	}
	return gcpMachineTypeRates[t], t
}

// linodePlanRates maps Linode plan ids to per-hour cost. Covers the CPU
// burst plans, the g6-nanode-1 used for self-hosted coordination boxes, and
// the dedicated GPU plans. Verified against the Akamai/Linode plan catalog
// 2026-07-18 (exact); re-verify with scripts/refresh-prices.sh.
// (GPU kinds also resolve via LookupLinodeGPU.)
var linodePlanRates = map[string]float64{
	"g6-nanode-1":         0.0075, // 1 GB — coordination box
	"g6-standard-1":       0.018,  // 2 GB
	"g6-standard-2":       0.036,  // 4 GB
	"g6-standard-4":       0.072,  // 8 GB
	"g6-standard-6":       0.144,  // 16 GB
	"g6-standard-8":       0.288,  // 32 GB
	"g2-gpu-rtx4000a1-s":  0.52,   // RTX 4000 Ada, 1 GPU
	"g2-gpu-rtx4000a1-m":  0.67,   // RTX 4000 Ada, 1 GPU, 8 vCPU
	"g2-gpu-rtx4000a1-l":  0.96,   // RTX 4000 Ada, 1 GPU, 16 vCPU
	"g2-gpu-rtx4000a1-xl": 1.53,   // RTX 4000 Ada, 1 GPU, 32 vCPU
	"g2-gpu-rtx4000a2-s":  1.05,   // RTX 4000 Ada, 2 GPUs
	"g2-gpu-rtx4000a2-m":  1.34,   // RTX 4000 Ada, 2 GPUs, 16 vCPU
	"g2-gpu-rtx4000a4-s":  2.96,   // RTX 4000 Ada, 4 GPUs
	"g2-gpu-rtx4000a4-m":  3.57,   // RTX 4000 Ada, 4 GPUs, 48 vCPU
	"g1-gpu-rtx6000-1":    1.50,   // RTX 6000, 1 GPU
	"g1-gpu-rtx6000-2":    3.00,   // RTX 6000, 2 GPUs
	"g1-gpu-rtx6000-3":    4.50,   // RTX 6000, 3 GPUs
	"g1-gpu-rtx6000-4":    6.00,   // RTX 6000, 4 GPUs
}

// LinodePlan returns the per-hour price for a Linode plan id.
func LinodePlan(plan string) (float64, bool) {
	p, ok := linodePlanRates[plan]
	return p, ok
}

// EstimateLinodeCPU maps a memory request to the shared Linode plan the
// linode backend would pick and returns (rate, plan).
//
// The threshold ladder MUST mirror pkg/backends/linode.mapType exactly —
// same boundaries, same default — or the persisted HourlyUSD under-reports
// the real upstream cost (and the budget cap it enforces) for the plans the
// backend actually launches. TestEstimateLinodeCPUParity (in this package)
// guards this invariant against linode.MapType, the backend's own selector.
func EstimateLinodeCPU(memoryMB int64) (float64, string) {
	p := "g6-standard-6"
	switch {
	case memoryMB <= 512:
		p = "g6-nanode-1"
	case memoryMB <= 1536:
		p = "g6-standard-1"
	case memoryMB <= 3072:
		p = "g6-standard-2"
	case memoryMB <= 6144:
		p = "g6-standard-4"
	}
	return linodePlanRates[p], p
}

// FixedMonthlyUSD enumerates always-on (non-burst) costs yscale carries,
// for the cost dashboard's fixed-overhead panel. Per-customer coordination server
// box + the baked-AMI snapshot are the standing line items today.
var FixedMonthlyUSD = map[string]float64{
	"coordination-box": 5.00, // Linode g6-nanode-1 per-customer coordination box
	"aws-ami":          0.40, // 8 GB gp3 EBS snapshot behind the baked burst AMI
}

// HourlyUSD is the unified per-hour upstream cost lookup across backends.
// sku is the backend-specific id: Fly machine class, AWS instance type,
// Azure VM size, GCP E2 machine type, or Linode plan / GPU kind. Returns
// (0, false) when unpriced. The decider's Plan path is the authoritative
// pricing gate; this lookup is a convenience for downstream reporting
// paths that already know the SKU.
func HourlyUSD(backend, sku string) (float64, bool) {
	switch backend {
	case "flyio":
		return FlyMachine(sku)
	case "aws":
		return AWSInstance(sku)
	case "azure":
		return AzureInstance(sku)
	case "gcp":
		return GCPMachine(sku)
	case "linode":
		if p, ok := LinodePlan(sku); ok {
			return p, true
		}
		return LookupLinodeGPU(sku)
	}
	return 0, false
}
