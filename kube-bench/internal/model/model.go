package model

import (
	"encoding/json"
	"time"
)

const SchemaVersion = "yscale-kube-bench/v1alpha1"

type Result struct {
	SchemaVersion string          `json:"schemaVersion"`
	ToolVersion   string          `json:"toolVersion"`
	RunID         string          `json:"runId"`
	StartedAt     time.Time       `json:"startedAt"`
	FinishedAt    time.Time       `json:"finishedAt"`
	Config        json.RawMessage `json:"config,omitempty"`
	Cluster       ClusterSummary  `json:"cluster"`
	Nodes         []NodeResult    `json:"nodes"`
	Reaction      *ReactionSuite  `json:"reaction,omitempty"`
	Compute       *ComputeSuite   `json:"compute,omitempty"`
	Network       *NetworkSuite   `json:"network,omitempty"`
	DNS           *DNSSuite       `json:"dns,omitempty"`
	Storage       *StorageSuite   `json:"storage,omitempty"`
	Transcode     *TranscodeSuite `json:"transcode,omitempty"`
	Scores        ScoreCard       `json:"scores"`
	Warnings      []string        `json:"warnings,omitempty"`
}

type ClusterSummary struct {
	Context                 string         `json:"context,omitempty"`
	Namespace               string         `json:"namespace"`
	KubernetesServerVersion string         `json:"kubernetesServerVersion,omitempty"`
	NodeCount               int            `json:"nodeCount"`
	ReadyNodeCount          int            `json:"readyNodeCount"`
	Architectures           map[string]int `json:"architectures"`
	OperatingSystems        map[string]int `json:"operatingSystems"`
	TotalCPUCapacity        string         `json:"totalCpuCapacity,omitempty"`
	TotalMemoryBytes        int64          `json:"totalMemoryBytes,omitempty"`
}

type NodeResult struct {
	Name      string           `json:"name"`
	Inventory NodeInventory    `json:"inventory"`
	Effective EffectiveProfile `json:"effective"`
	Warnings  []string         `json:"warnings,omitempty"`
}

type NodeInventory struct {
	Status            string             `json:"status"`
	Error             string             `json:"error,omitempty"`
	NodeName          string             `json:"nodeName"`
	Architecture      string             `json:"architecture"`
	OperatingSystem   string             `json:"operatingSystem"`
	OSImage           string             `json:"osImage,omitempty"`
	KernelVersion     string             `json:"kernelVersion,omitempty"`
	KubeletVersion    string             `json:"kubeletVersion,omitempty"`
	ContainerRuntime  string             `json:"containerRuntime,omitempty"`
	ProviderID        string             `json:"providerId,omitempty"`
	MachineID         string             `json:"machineId,omitempty"`
	SystemUUID        string             `json:"systemUuid,omitempty"`
	BootID            string             `json:"bootId,omitempty"`
	Capacity          map[string]string  `json:"capacity,omitempty"`
	Allocatable       map[string]string  `json:"allocatable,omitempty"`
	Labels            map[string]string  `json:"labels,omitempty"`
	CPU               CPUInfo            `json:"cpu"`
	Memory            MemoryInfo         `json:"memory"`
	Virtualization    VirtualizationInfo `json:"virtualization"`
	DMI               DMIInfo            `json:"dmi"`
	BlockDevices      []BlockDevice      `json:"blockDevices,omitempty"`
	NetworkInterfaces []NetworkInterface `json:"networkInterfaces,omitempty"`
	NUMANodes         []NUMANode         `json:"numaNodes,omitempty"`
	HostInspection    bool               `json:"hostInspection"`
	InspectionRoot    string             `json:"inspectionRoot,omitempty"`
}

type CPUInfo struct {
	Model           string         `json:"model,omitempty"`
	Vendor          string         `json:"vendor,omitempty"`
	LogicalCPUs     int            `json:"logicalCpus"`
	PhysicalCores   int            `json:"physicalCores,omitempty"`
	Sockets         int            `json:"sockets,omitempty"`
	MaxMHz          float64        `json:"maxMhz,omitempty"`
	Flags           []string       `json:"flags,omitempty"`
	CoreTypes       map[string]int `json:"coreTypes,omitempty"`
	HypervisorFlag  bool           `json:"hypervisorFlag"`
	VisibleCPUModel string         `json:"visibleCpuModel,omitempty"`
}

type MemoryInfo struct {
	TotalBytes     int64 `json:"totalBytes"`
	AvailableBytes int64 `json:"availableBytes,omitempty"`
	SwapTotalBytes int64 `json:"swapTotalBytes,omitempty"`
	HugePagesBytes int64 `json:"hugePagesBytes,omitempty"`
}

type VirtualizationInfo struct {
	NodeEnvironment string   `json:"nodeEnvironment"`
	Hypervisor      string   `json:"hypervisor,omitempty"`
	Platform        string   `json:"platform,omitempty"`
	GuestType       string   `json:"guestType,omitempty"`
	ContainerLayer  string   `json:"containerLayer,omitempty"`
	Confidence      float64  `json:"confidence"`
	Evidence        []string `json:"evidence,omitempty"`
	Limitations     []string `json:"limitations,omitempty"`
}

type DMIInfo struct {
	SystemVendor string `json:"systemVendor,omitempty"`
	ProductName  string `json:"productName,omitempty"`
	ProductUUID  string `json:"productUuid,omitempty"`
	BoardVendor  string `json:"boardVendor,omitempty"`
	BoardName    string `json:"boardName,omitempty"`
	BIOSVendor   string `json:"biosVendor,omitempty"`
	BIOSVersion  string `json:"biosVersion,omitempty"`
}

type BlockDevice struct {
	Name               string `json:"name"`
	Model              string `json:"model,omitempty"`
	Vendor             string `json:"vendor,omitempty"`
	Serial             string `json:"serial,omitempty"`
	Type               string `json:"type,omitempty"`
	SizeBytes          int64  `json:"sizeBytes,omitempty"`
	Rotational         *bool  `json:"rotational,omitempty"`
	LogicalBlockBytes  int64  `json:"logicalBlockBytes,omitempty"`
	PhysicalBlockBytes int64  `json:"physicalBlockBytes,omitempty"`
	Scheduler          string `json:"scheduler,omitempty"`
}

type NetworkInterface struct {
	Name       string `json:"name"`
	State      string `json:"state,omitempty"`
	MTU        int64  `json:"mtu,omitempty"`
	SpeedMbps  int64  `json:"speedMbps,omitempty"`
	Driver     string `json:"driver,omitempty"`
	MACAddress string `json:"macAddress,omitempty"`
}

type NUMANode struct {
	ID     int   `json:"id"`
	CPUs   []int `json:"cpus,omitempty"`
	Memory int64 `json:"memoryBytes,omitempty"`
}

type EffectiveProfile struct {
	PhysicalHost PhysicalHostProfile `json:"physicalHost"`
	Guest        GuestProfile        `json:"guest"`
	Economics    EconomicsProfile    `json:"economics"`
	Tags         map[string]string   `json:"tags,omitempty"`
}

type SourcedString struct {
	Value      string  `json:"value,omitempty"`
	Source     string  `json:"source,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
}

type SourcedInt struct {
	Value      int     `json:"value,omitempty"`
	Source     string  `json:"source,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
}

type PhysicalHostProfile struct {
	Name           SourcedString `json:"name"`
	CPUModel       SourcedString `json:"cpuModel"`
	Platform       SourcedString `json:"platform"`
	Hypervisor     SourcedString `json:"hypervisor"`
	StorageBackend SourcedString `json:"storageBackend"`
	NetworkBackend SourcedString `json:"networkBackend"`
}

type GuestProfile struct {
	Type        SourcedString `json:"type"`
	CPUModel    SourcedString `json:"cpuModel"`
	CPUMode     SourcedString `json:"cpuMode"`
	VCPUs       SourcedInt    `json:"vcpus"`
	MemoryBytes int64         `json:"memoryBytes,omitempty"`
}

type EconomicsProfile struct {
	PurchasePriceUSD float64 `json:"purchasePriceUsd,omitempty"`
	IdleWatts        float64 `json:"idleWatts,omitempty"`
	PeakWatts        float64 `json:"peakWatts,omitempty"`
	HourlyCostUSD    float64 `json:"hourlyCostUsd,omitempty"`
}

type Distribution struct {
	Count int     `json:"count"`
	Min   float64 `json:"min"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
	Unit  string  `json:"unit"`
}

type ReactionSuite struct {
	Status       string           `json:"status"`
	Error        string           `json:"error,omitempty"`
	Cached       []ReactionResult `json:"cached,omitempty"`
	PullPath     []ReactionResult `json:"pullPath,omitempty"`
	ClusterWaves []ReactionWave   `json:"clusterWaves,omitempty"`
	Summary      ReactionSummary  `json:"summary"`
}

type ReactionResult struct {
	Node                string  `json:"node"`
	Mode                string  `json:"mode"`
	Round               int     `json:"round"`
	Image               string  `json:"image"`
	CreateRequestMS     float64 `json:"createRequestMs"`
	CreateToScheduledMS float64 `json:"createToScheduledMs,omitempty"`
	CreateToRunningMS   float64 `json:"createToRunningMs,omitempty"`
	CreateToReadyMS     float64 `json:"createToReadyMs,omitempty"`
	ObservedReadyMS     float64 `json:"observedReadyMs,omitempty"`
	ImagePullMS         float64 `json:"imagePullMs,omitempty"`
	Status              string  `json:"status"`
	Error               string  `json:"error,omitempty"`
}

type ReactionWave struct {
	Mode        string       `json:"mode"`
	NodeCount   int          `json:"nodeCount"`
	AllReadyMS  float64      `json:"allReadyMs"`
	ReadySpread Distribution `json:"readySpread"`
	Status      string       `json:"status"`
	Error       string       `json:"error,omitempty"`
}

type ReactionSummary struct {
	CachedReadyMS     Distribution `json:"cachedReadyMs"`
	PullReadyMS       Distribution `json:"pullReadyMs"`
	ClusterAllReadyMS Distribution `json:"clusterAllReadyMs"`
}

type ComputeSuite struct {
	Status            string              `json:"status"`
	Error             string              `json:"error,omitempty"`
	Isolated          []NodeComputeResult `json:"isolated,omitempty"`
	ClusterWaves      []ComputeWave       `json:"clusterWaves,omitempty"`
	PeakAggregate     float64             `json:"peakAggregateIntegerOpsPerSecond,omitempty"`
	ScalingEfficiency float64             `json:"scalingEfficiency,omitempty"`
}

type NodeComputeResult struct {
	Node                   string  `json:"node"`
	Status                 string  `json:"status"`
	Error                  string  `json:"error,omitempty"`
	LogicalCPUs            int     `json:"logicalCpus"`
	SingleIntegerOpsPerSec float64 `json:"singleIntegerOpsPerSecond,omitempty"`
	AllIntegerOpsPerSec    float64 `json:"allIntegerOpsPerSecond,omitempty"`
	SingleSHA256MiBPerSec  float64 `json:"singleSha256MibPerSecond,omitempty"`
	AllSHA256MiBPerSec     float64 `json:"allSha256MibPerSecond,omitempty"`
	MemoryCopyMiBPerSec    float64 `json:"memoryCopyMibPerSecond,omitempty"`
	DurationSeconds        float64 `json:"durationSeconds"`
	StealPercentBefore     float64 `json:"stealPercentBefore,omitempty"`
	StealPercentAfter      float64 `json:"stealPercentAfter,omitempty"`
}

type ComputeWave struct {
	Nodes                     []string            `json:"nodes"`
	NodeCount                 int                 `json:"nodeCount"`
	Results                   []NodeComputeResult `json:"results"`
	AggregateIntegerOpsPerSec float64             `json:"aggregateIntegerOpsPerSecond"`
	ExpectedIntegerOpsPerSec  float64             `json:"expectedIntegerOpsPerSecond"`
	ScalingEfficiency         float64             `json:"scalingEfficiency"`
	WallSeconds               float64             `json:"wallSeconds"`
	Status                    string              `json:"status"`
	Error                     string              `json:"error,omitempty"`
}

type NetworkSuite struct {
	Status  string          `json:"status"`
	Error   string          `json:"error,omitempty"`
	Results []NetworkResult `json:"results,omitempty"`
	Summary NetworkSummary  `json:"summary"`
}

type NetworkResult struct {
	SourceNode      string       `json:"sourceNode"`
	DestinationNode string       `json:"destinationNode,omitempty"`
	Path            string       `json:"path"`
	Target          string       `json:"target"`
	TCPRTTMS        Distribution `json:"tcpRttMs"`
	TCPUploadMbps   float64      `json:"tcpUploadMbps,omitempty"`
	TCPDownloadMbps float64      `json:"tcpDownloadMbps,omitempty"`
	UDPRTTMS        Distribution `json:"udpRttMs"`
	UDPLossPercent  float64      `json:"udpLossPercent,omitempty"`
	UDPJitterMS     float64      `json:"udpJitterMs,omitempty"`
	Status          string       `json:"status"`
	Error           string       `json:"error,omitempty"`
}

type NetworkSummary struct {
	SameNodeTCPMbps  Distribution `json:"sameNodeTcpMbps"`
	CrossNodeTCPMbps Distribution `json:"crossNodeTcpMbps"`
	ServiceTCPMbps   Distribution `json:"serviceTcpMbps"`
	CrossNodeRTTMS   Distribution `json:"crossNodeRttMs"`
}

type DNSSuite struct {
	Status  string      `json:"status"`
	Error   string      `json:"error,omitempty"`
	Results []DNSResult `json:"results,omitempty"`
	Summary DNSSummary  `json:"summary"`
}

type DNSResult struct {
	Node          string       `json:"node"`
	Name          string       `json:"name"`
	Protocol      string       `json:"protocol"`
	Class         string       `json:"class,omitempty"`
	LatencyMS     Distribution `json:"latencyMs"`
	QueriesPerSec float64      `json:"queriesPerSecond,omitempty"`
	Failures      int          `json:"failures"`
	Status        string       `json:"status"`
	Error         string       `json:"error,omitempty"`
}

type DNSSummary struct {
	ServiceP99MS  Distribution `json:"serviceP99Ms"`
	ExternalP99MS Distribution `json:"externalP99Ms"`
	FailureCount  int          `json:"failureCount"`
}

type StorageSuite struct {
	Status  string          `json:"status"`
	Error   string          `json:"error,omitempty"`
	Results []StorageResult `json:"results,omitempty"`
	Summary StorageSummary  `json:"summary"`
}

type StorageResult struct {
	Node       string             `json:"node"`
	Target     string             `json:"target"`
	Kind       string             `json:"kind"`
	Path       string             `json:"path"`
	DeviceHint string             `json:"deviceHint,omitempty"`
	Profiles   []FIOProfileResult `json:"profiles,omitempty"`
	Status     string             `json:"status"`
	Error      string             `json:"error,omitempty"`
}

type FIOProfileResult struct {
	Name       string  `json:"name"`
	RW         string  `json:"rw"`
	Engine     string  `json:"engine"`
	Direct     bool    `json:"direct"`
	ReadMBps   float64 `json:"readMbps,omitempty"`
	WriteMBps  float64 `json:"writeMbps,omitempty"`
	ReadIOPS   float64 `json:"readIops,omitempty"`
	WriteIOPS  float64 `json:"writeIops,omitempty"`
	ReadP50MS  float64 `json:"readP50Ms,omitempty"`
	ReadP95MS  float64 `json:"readP95Ms,omitempty"`
	ReadP99MS  float64 `json:"readP99Ms,omitempty"`
	WriteP50MS float64 `json:"writeP50Ms,omitempty"`
	WriteP95MS float64 `json:"writeP95Ms,omitempty"`
	WriteP99MS float64 `json:"writeP99Ms,omitempty"`
	FsyncP99MS float64 `json:"fsyncP99Ms,omitempty"`
	Status     string  `json:"status"`
	Error      string  `json:"error,omitempty"`
}

type StorageSummary struct {
	SequentialReadMBps  Distribution `json:"sequentialReadMbps"`
	SequentialWriteMBps Distribution `json:"sequentialWriteMbps"`
	RandomReadIOPS      Distribution `json:"randomReadIops"`
	RandomWriteIOPS     Distribution `json:"randomWriteIops"`
	FsyncP99MS          Distribution `json:"fsyncP99Ms"`
}

type TranscodeSuite struct {
	Status  string            `json:"status"`
	Error   string            `json:"error,omitempty"`
	Results []TranscodeResult `json:"results,omitempty"`
	Summary TranscodeSummary  `json:"summary"`
}

type TranscodeResult struct {
	Node           string  `json:"node"`
	Profile        string  `json:"profile"`
	Encoder        string  `json:"encoder"`
	Width          int     `json:"width"`
	Height         int     `json:"height"`
	FPS            int     `json:"fps"`
	SourceSeconds  float64 `json:"sourceSeconds"`
	WallSeconds    float64 `json:"wallSeconds,omitempty"`
	Frames         int     `json:"frames,omitempty"`
	EncodeFPS      float64 `json:"encodeFps,omitempty"`
	RealtimeFactor float64 `json:"realtimeFactor,omitempty"`
	Hardware       bool    `json:"hardware"`
	Status         string  `json:"status"`
	Error          string  `json:"error,omitempty"`
}

type TranscodeSummary struct {
	CPURealtimeFactor Distribution `json:"cpuRealtimeFactor"`
	GPURealtimeFactor Distribution `json:"gpuRealtimeFactor"`
}

type ScoreCard struct {
	Version     string   `json:"version"`
	Reaction    *float64 `json:"reaction,omitempty"`
	Throughput  *float64 `json:"throughput,omitempty"`
	Efficiency  *float64 `json:"efficiency,omitempty"`
	Overall     *float64 `json:"overall,omitempty"`
	Explanation []string `json:"explanation,omitempty"`
}
