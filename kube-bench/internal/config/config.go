package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Config struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   Metadata `json:"metadata"`
	Spec       Spec     `json:"spec"`
}

type Metadata struct {
	Name string `json:"name"`
}

type Spec struct {
	Namespace           string            `json:"namespace"`
	Image               string            `json:"image"`
	ImagePullPolicy     string            `json:"imagePullPolicy"`
	TimeoutSeconds      int               `json:"timeoutSeconds"`
	KeepResources       bool              `json:"keepResources"`
	IncludeControlPlane bool              `json:"includeControlPlane"`
	Nodes               []string          `json:"nodes,omitempty"`
	NodeSelector        map[string]string `json:"nodeSelector,omitempty"`
	Parallelism         int               `json:"parallelism"`
	Suites              Suites            `json:"suites"`
	Inventory           InventoryConfig   `json:"inventory"`
	Reaction            ReactionConfig    `json:"reaction"`
	Compute             ComputeConfig     `json:"compute"`
	Network             NetworkConfig     `json:"network"`
	DNS                 DNSConfig         `json:"dns"`
	Storage             StorageConfig     `json:"storage"`
	Transcode           TranscodeConfig   `json:"transcode"`
	NodeOverrides       []NodeOverride    `json:"nodeOverrides,omitempty"`
	Scoring             ScoringConfig     `json:"scoring"`
}

type Suites struct {
	Inventory bool `json:"inventory"`
	Reaction  bool `json:"reaction"`
	Compute   bool `json:"compute"`
	Network   bool `json:"network"`
	DNS       bool `json:"dns"`
	Storage   bool `json:"storage"`
	Transcode bool `json:"transcode"`
}

type InventoryConfig struct {
	HostInspection bool `json:"hostInspection"`
	Privileged     bool `json:"privileged"`
}

type ReactionConfig struct {
	Rounds              int    `json:"rounds"`
	ReadyTimeoutSeconds int    `json:"readyTimeoutSeconds"`
	PullImage           string `json:"pullImage,omitempty"`
	PullImagePolicy     string `json:"pullImagePolicy"`
	ClusterWave         bool   `json:"clusterWave"`
}

type ComputeConfig struct {
	DurationSeconds     int   `json:"durationSeconds"`
	IsolatedParallelism int   `json:"isolatedParallelism"`
	ClusterSteps        []int `json:"clusterSteps"`
}

type NetworkConfig struct {
	DurationSeconds int    `json:"durationSeconds"`
	RTTSamples      int    `json:"rttSamples"`
	UDPSamples      int    `json:"udpSamples"`
	Parallelism     int    `json:"parallelism"`
	Matrix          string `json:"matrix"`
	ServicePath     bool   `json:"servicePath"`
}

type DNSConfig struct {
	Queries     int         `json:"queries"`
	Concurrency int         `json:"concurrency"`
	Targets     []DNSTarget `json:"targets"`
}

type DNSTarget struct {
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
	Class    string `json:"class"`
}

type StorageConfig struct {
	DurationSeconds int             `json:"durationSeconds"`
	SizeMiB         int             `json:"sizeMiB"`
	IOEngine        string          `json:"ioEngine"`
	Targets         []StorageTarget `json:"targets"`
}

type StorageTarget struct {
	Name       string   `json:"name"`
	Kind       string   `json:"kind"`
	HostPath   string   `json:"hostPath,omitempty"`
	ClaimName  string   `json:"claimName,omitempty"`
	MountPath  string   `json:"mountPath,omitempty"`
	DeviceHint string   `json:"deviceHint,omitempty"`
	Nodes      []string `json:"nodes,omitempty"`
	ReadOnly   bool     `json:"readOnly,omitempty"`
}

type TranscodeConfig struct {
	Profiles []TranscodeProfile `json:"profiles"`
}

type TranscodeProfile struct {
	Name      string            `json:"name"`
	Encoder   string            `json:"encoder"`
	Width     int               `json:"width"`
	Height    int               `json:"height"`
	FPS       int               `json:"fps"`
	Seconds   int               `json:"seconds"`
	Arguments []string          `json:"arguments,omitempty"`
	Hardware  bool              `json:"hardware"`
	Resources map[string]string `json:"resources,omitempty"`
	HostPaths []HostPathMount   `json:"hostPaths,omitempty"`
	Nodes     []string          `json:"nodes,omitempty"`
}

type HostPathMount struct {
	Name      string `json:"name"`
	HostPath  string `json:"hostPath"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type NodeOverride struct {
	Match        NodeMatch            `json:"match"`
	PhysicalHost PhysicalHostOverride `json:"physicalHost,omitempty"`
	Guest        GuestOverride        `json:"guest,omitempty"`
	Economics    EconomicsOverride    `json:"economics,omitempty"`
	Tags         map[string]string    `json:"tags,omitempty"`
}

type NodeMatch struct {
	Name   string            `json:"name,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

type PhysicalHostOverride struct {
	Name           string `json:"name,omitempty"`
	CPUModel       string `json:"cpuModel,omitempty"`
	Platform       string `json:"platform,omitempty"`
	Hypervisor     string `json:"hypervisor,omitempty"`
	StorageBackend string `json:"storageBackend,omitempty"`
	NetworkBackend string `json:"networkBackend,omitempty"`
}

type GuestOverride struct {
	Type        string `json:"type,omitempty"`
	CPUModel    string `json:"cpuModel,omitempty"`
	CPUMode     string `json:"cpuMode,omitempty"`
	VCPUs       int    `json:"vcpus,omitempty"`
	MemoryBytes int64  `json:"memoryBytes,omitempty"`
}

type EconomicsOverride struct {
	PurchasePriceUSD float64 `json:"purchasePriceUsd,omitempty"`
	IdleWatts        float64 `json:"idleWatts,omitempty"`
	PeakWatts        float64 `json:"peakWatts,omitempty"`
	HourlyCostUSD    float64 `json:"hourlyCostUsd,omitempty"`
}

type ScoringConfig struct {
	Enabled   bool           `json:"enabled"`
	Baselines ScoreBaselines `json:"baselines"`
}

type ScoreBaselines struct {
	PodReadyP95MS              float64 `json:"podReadyP95Ms"`
	DNSP99MS                   float64 `json:"dnsP99Ms"`
	CrossNodeRTTP99MS          float64 `json:"crossNodeRttP99Ms"`
	FsyncP99MS                 float64 `json:"fsyncP99Ms"`
	ClusterIntegerOpsPerSecond float64 `json:"clusterIntegerOpsPerSecond"`
	CrossNodeTCPMbps           float64 `json:"crossNodeTcpMbps"`
	SequentialReadMBps         float64 `json:"sequentialReadMbps"`
	TranscodeRealtimeFactor    float64 `json:"transcodeRealtimeFactor"`
	ComputePerPeakWatt         float64 `json:"computePerPeakWatt"`
	ComputePerDollar           float64 `json:"computePerDollar"`
}

func Default() Config {
	return Config{
		APIVersion: "kube-bench.yscale.dev/v1alpha1",
		Kind:       "BenchmarkConfig",
		Metadata:   Metadata{Name: "default"},
		Spec: Spec{
			Namespace:       "yscale-kube-bench",
			Image:           "ghcr.io/jakenesler/yscale-kube-bench:latest",
			ImagePullPolicy: "IfNotPresent",
			TimeoutSeconds:  900,
			Parallelism:     4,
			Suites: Suites{
				Inventory: true,
				Reaction:  true,
				Compute:   true,
				Network:   true,
				DNS:       true,
				Storage:   true,
				Transcode: true,
			},
			Inventory: InventoryConfig{},
			Reaction: ReactionConfig{
				Rounds:              3,
				ReadyTimeoutSeconds: 120,
				PullImagePolicy:     "Always",
				ClusterWave:         true,
			},
			Compute: ComputeConfig{
				DurationSeconds:     5,
				IsolatedParallelism: 1,
				ClusterSteps:        []int{1, 2, 4, 0},
			},
			Network: NetworkConfig{
				DurationSeconds: 5,
				RTTSamples:      200,
				UDPSamples:      200,
				Parallelism:     1,
				Matrix:          "sample",
				ServicePath:     true,
			},
			DNS: DNSConfig{
				Queries:     100,
				Concurrency: 4,
				Targets: []DNSTarget{
					{Name: "kubernetes.default.svc.cluster.local", Protocol: "udp", Class: "service"},
					{Name: "example.com", Protocol: "udp", Class: "external"},
				},
			},
			Storage: StorageConfig{
				DurationSeconds: 5,
				SizeMiB:         256,
				IOEngine:        "io_uring",
				Targets: []StorageTarget{
					{Name: "ephemeral", Kind: "emptyDir", MountPath: "/bench/ephemeral"},
				},
			},
			Transcode: TranscodeConfig{
				Profiles: []TranscodeProfile{
					{Name: "h264-1080p", Encoder: "libx264", Width: 1920, Height: 1080, FPS: 30, Seconds: 10, Arguments: []string{"-preset", "medium"}},
					{Name: "h265-1080p", Encoder: "libx265", Width: 1920, Height: 1080, FPS: 30, Seconds: 10, Arguments: []string{"-preset", "medium"}},
					{Name: "av1-1080p", Encoder: "libsvtav1", Width: 1920, Height: 1080, FPS: 30, Seconds: 10, Arguments: []string{"-preset", "8"}},
				},
			},
			Scoring: ScoringConfig{
				Enabled: true,
				Baselines: ScoreBaselines{
					PodReadyP95MS:              1000,
					DNSP99MS:                   2,
					CrossNodeRTTP99MS:          1,
					FsyncP99MS:                 2,
					ClusterIntegerOpsPerSecond: 1_000_000_000,
					CrossNodeTCPMbps:           1000,
					SequentialReadMBps:         1000,
					TranscodeRealtimeFactor:    1,
					ComputePerPeakWatt:         2_000_000,
					ComputePerDollar:           1_000_000,
				},
			},
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if strings.TrimSpace(path) == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config as JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return Config{}, fmt.Errorf("config contains multiple JSON values")
	} else if !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("decode trailing config data: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var problems []string
	if c.APIVersion != "kube-bench.yscale.dev/v1alpha1" {
		problems = append(problems, "apiVersion must be kube-bench.yscale.dev/v1alpha1")
	}
	if c.Kind != "BenchmarkConfig" {
		problems = append(problems, "kind must be BenchmarkConfig")
	}
	if strings.TrimSpace(c.Spec.Namespace) == "" {
		problems = append(problems, "spec.namespace is required")
	}
	if strings.TrimSpace(c.Spec.Image) == "" {
		problems = append(problems, "spec.image is required")
	}
	if c.Spec.TimeoutSeconds < 30 {
		problems = append(problems, "spec.timeoutSeconds must be at least 30")
	}
	if c.Spec.Parallelism < 1 {
		problems = append(problems, "spec.parallelism must be at least 1")
	}
	if c.Spec.Reaction.Rounds < 1 {
		problems = append(problems, "spec.reaction.rounds must be at least 1")
	}
	if c.Spec.Compute.DurationSeconds < 1 {
		problems = append(problems, "spec.compute.durationSeconds must be at least 1")
	}
	if c.Spec.Network.DurationSeconds < 1 {
		problems = append(problems, "spec.network.durationSeconds must be at least 1")
	}
	if c.Spec.Network.Parallelism < 1 {
		problems = append(problems, "spec.network.parallelism must be at least 1")
	}
	if c.Spec.Network.Matrix != "sample" && c.Spec.Network.Matrix != "full" {
		problems = append(problems, "spec.network.matrix must be sample or full")
	}
	if c.Spec.DNS.Queries < 1 || c.Spec.DNS.Concurrency < 1 {
		problems = append(problems, "spec.dns queries and concurrency must be at least 1")
	}
	if c.Spec.Storage.DurationSeconds < 1 || c.Spec.Storage.SizeMiB < 16 {
		problems = append(problems, "spec.storage duration must be at least 1 and sizeMiB at least 16")
	}
	seenTargets := map[string]struct{}{}
	for index, target := range c.Spec.Storage.Targets {
		if target.Name == "" {
			problems = append(problems, fmt.Sprintf("spec.storage.targets[%d].name is required", index))
		}
		if _, exists := seenTargets[target.Name]; exists {
			problems = append(problems, fmt.Sprintf("duplicate storage target %q", target.Name))
		}
		seenTargets[target.Name] = struct{}{}
		switch target.Kind {
		case "emptyDir":
		case "hostPath":
			if target.HostPath == "" {
				problems = append(problems, fmt.Sprintf("storage target %q requires hostPath", target.Name))
			}
		case "pvc":
			if target.ClaimName == "" {
				problems = append(problems, fmt.Sprintf("storage target %q requires claimName", target.Name))
			}
		default:
			problems = append(problems, fmt.Sprintf("storage target %q has unsupported kind %q", target.Name, target.Kind))
		}
		if target.ReadOnly {
			problems = append(problems, fmt.Sprintf("storage target %q cannot be readOnly because fio writes test data", target.Name))
		}
	}
	for index, profile := range c.Spec.Transcode.Profiles {
		if profile.Name == "" || profile.Encoder == "" {
			problems = append(problems, fmt.Sprintf("transcode profile %d requires name and encoder", index))
		}
		if profile.Width < 16 || profile.Height < 16 || profile.FPS < 1 || profile.Seconds < 1 {
			problems = append(problems, fmt.Sprintf("transcode profile %q has invalid dimensions, fps, or duration", profile.Name))
		}
	}
	for index, override := range c.Spec.NodeOverrides {
		if override.Match.Name == "" && len(override.Match.Labels) == 0 {
			problems = append(problems, fmt.Sprintf("node override %d requires match.name or match.labels", index))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("invalid config: %s", strings.Join(problems, "; "))
	}
	return nil
}

func (c Config) JSON() json.RawMessage {
	data, _ := json.Marshal(c)
	return data
}

func (c *Config) SetSuites(names []string) error {
	if len(names) == 0 {
		return nil
	}
	c.Spec.Suites = Suites{}
	for _, name := range names {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "inventory":
			c.Spec.Suites.Inventory = true
		case "reaction":
			c.Spec.Suites.Reaction = true
		case "compute":
			c.Spec.Suites.Compute = true
		case "network":
			c.Spec.Suites.Network = true
		case "dns":
			c.Spec.Suites.DNS = true
		case "storage":
			c.Spec.Suites.Storage = true
		case "transcode":
			c.Spec.Suites.Transcode = true
		case "all":
			c.Spec.Suites = Suites{Inventory: true, Reaction: true, Compute: true, Network: true, DNS: true, Storage: true, Transcode: true}
		default:
			return fmt.Errorf("unknown suite %q", name)
		}
	}
	return nil
}
