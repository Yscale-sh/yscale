package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/yscale-sh/yscale/pkg/backends"
)

type Config struct {
	Backend   BackendConfig   `yaml:"backend"`
	Join      JoinConfig      `yaml:"join"`
	Tailscale TailscaleConfig `yaml:"tailscale"`
	Scaling   ScalingConfig   `yaml:"scaling"`
	Cluster   ClusterConfig   `yaml:"cluster"`
}

type BackendConfig struct {
	Type  string      `yaml:"type"` // "flyio"
	FlyIO FlyIOConfig `yaml:"flyio"`
	GCP   GCPConfig   `yaml:"gcp"`
	Azure AzureConfig `yaml:"azure"`
}

// FlyIOConfig holds Fly.io-specific backend settings. Fly is the cheap-CPU
// backend; their GPU SKUs are deprecated as of July 31 2026 and won't be
// available after that date — route GPU workloads to Linode instead.
type FlyIOConfig struct {
	Org        string `yaml:"org"`
	Region     string `yaml:"region"`
	APIToken   string `yaml:"-"` // FLYIO_TOKEN env var
	AgentImage string `yaml:"agentImage"`
}

// GCPConfig holds Google Compute Engine backend settings (CPU only — see
// pkg/backends/gcp). Unlike FlyIOConfig's APIToken, CredentialsFile is
// optional: an empty value defers to Application Default Credentials
// (gcloud login, the GCE metadata server), mirroring the AWS backend's
// default credential chain rather than requiring a static secret.
type GCPConfig struct {
	ProjectID string `yaml:"projectID"`
	Zone      string `yaml:"zone"`
	Region    string `yaml:"region"`
	// CredentialsFile is GOOGLE_APPLICATION_CREDENTIALS — a path to a
	// service-account JSON key, not a secret value itself, but env-only
	// like the other credential fields so it can't be checked into YAML.
	CredentialsFile string `yaml:"-"`
	AgentImage      string `yaml:"agentImage"`
}

// AzureConfig holds Azure VM backend settings (CPU only — see
// pkg/backends/azure; GPU is a marked seam, not yet implemented since
// Azure GPU quota is unapproved). ClientSecret is env-only like
// FlyIOConfig's APIToken so it can't be checked into YAML.
type AzureConfig struct {
	TenantID       string `yaml:"tenantID"`
	ClientID       string `yaml:"clientID"`
	ClientSecret   string `yaml:"-"` // AZURE_CLIENT_SECRET env var
	SubscriptionID string `yaml:"subscriptionID"`
	Location       string `yaml:"location"`
	ResourceGroup  string `yaml:"resourceGroup"`
	AgentImage     string `yaml:"agentImage"`
}

// Re-exported so existing call sites (cmd/yscale, pkg/controller, etc.)
// don't need to migrate to importing pkg/backends. Single source of
// truth lives in pkg/backends.
const (
	JoinModeK3s     = backends.JoinModeK3s
	JoinModeKubelet = backends.JoinModeKubelet
)

// JoinConfig controls how burst nodes join the cluster.
type JoinConfig struct {
	// Mode is "k3s" or "kubelet".
	// k3s: uses k3s agent binary with --server and --token
	// kubelet: uses standard kubelet with bootstrap token (for LKE, EKS, GKE, etc.)
	Mode string `yaml:"mode"`

	// K3s-specific settings (mode: k3s)
	K3sServerURL string `yaml:"k3sServerURL"` // e.g. "https://100.64.0.1:6443"
	K3sToken     string `yaml:"-"`            // K3S_TOKEN env var

	// Kubelet-specific settings (mode: kubelet)
	// APIServer is the K8s API server URL. For LKE this is the public endpoint.
	APIServer string `yaml:"apiServer"`
	// ClusterCA is the base64-encoded cluster CA certificate.
	// Extract with: kubectl config view --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}'
	ClusterCA string `yaml:"clusterCA"`
}

type TailscaleConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Tailnet   string `yaml:"tailnet"`
	TagPrefix string `yaml:"tagPrefix"`
	AuthKey   string `yaml:"-"` // TS_AUTHKEY env var
}

type ScalingConfig struct {
	MaxNodes                int           `yaml:"maxNodes"`
	ScaleDownDelay          time.Duration `yaml:"scaleDownDelay"`
	PollInterval            time.Duration `yaml:"pollInterval"`
	PendingThreshold        time.Duration `yaml:"pendingThreshold"`
	NodeReadyTimeout        time.Duration `yaml:"nodeReadyTimeout"`
	PrewarmPool             int           `yaml:"prewarmPool"`
	MaxConcurrentProvisions int           `yaml:"maxConcurrentProvisions"`
	NodeResources           NodeResources `yaml:"nodeResources"`

	// PrewarmSuspend selects suspend-based pooling: prewarm machines boot,
	// join the cluster, and are then provider-suspended so they resume in
	// sub-second time with the kubelet still up. A pointer distinguishes
	// "unset" (defaults to true when prewarmPool > 0) from an explicit
	// `prewarmSuspend: false`. Suspend pooling is the ONLY supported form of
	// prewarming, and it additionally requires the backend to implement
	// provider suspend (currently Fly.io only); every other combination with
	// prewarmPool > 0 is rejected by validation rather than degraded to a
	// cold-stopped pool.
	PrewarmSuspend *bool `yaml:"prewarmSuspend"`
	// PrewarmMaxAge bounds how long a suspended prewarm snapshot may sit in
	// the pool before it is destroyed and replaced (bounds rootfs storage
	// cost and stale mesh devices). Default 6h; 0 keeps the default.
	PrewarmMaxAge time.Duration `yaml:"prewarmMaxAge"`

	// MonthlyCapUSD is a hard ceiling on accrued spend per calendar month.
	// 0 disables the cap. When the cap is reached, scaleUp is suppressed
	// until the calendar month rolls over.
	MonthlyCapUSD float64     `yaml:"monthlyCapUSD"`
	Costs         CostsConfig `yaml:"costs"`
}

// PrewarmSuspendEnabled reports whether suspend-based prewarm pooling is
// requested by config. Callers must additionally gate on the backend's
// provider-suspend capability (backends.WarmCapability).
func (s *ScalingConfig) PrewarmSuspendEnabled() bool {
	return s.PrewarmSuspend != nil && *s.PrewarmSuspend
}

// backendSupportsPrewarmSuspend reports whether a backend type implements the
// provider-suspend prewarm state machine (backends.WarmCapability). This is a
// static allow-list rather than a capability probe because validation runs
// before any backend is constructed; keep it in step with the backends that
// implement SuspendNode/WaitNodeState.
func backendSupportsPrewarmSuspend(backendType string) bool {
	return backendType == backends.TypeFlyIO
}

// CostsConfig is the YAML form of cost.Rates. Keys in Backends should match
// the value returned by Backend.Name() (e.g. "flyio").
type CostsConfig struct {
	Backends map[string]BackendCost `yaml:"backends"`
}

type BackendCost struct {
	BaseUSDPerHour   float64            `yaml:"baseUSDPerHour"`
	PerCPUUSDPerHour float64            `yaml:"perCPUUSDPerHour"`
	PerGBUSDPerHour  float64            `yaml:"perGBUSDPerHour"`
	GPURates         map[string]float64 `yaml:"gpuRates"`
}

type NodeResources struct {
	CPUMillis int64 `yaml:"cpuMillis"` // 1000 = 1 CPU
	MemoryMB  int64 `yaml:"memoryMB"`
}

type ClusterConfig struct {
	PodCIDR     string `yaml:"podCIDR"`
	ServiceCIDR string `yaml:"serviceCIDR"`
	CoreDNSIP   string `yaml:"coreDNSIP"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %s: %w", path, err)
	}
	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("parsing config: multiple YAML documents are not supported")
		}
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	cfg.applyDefaults()
	cfg.applyEnvOverrides()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Backend.Type == "" {
		c.Backend.Type = backends.TypeFlyIO
	}
	if c.Backend.FlyIO.Region == "" {
		c.Backend.FlyIO.Region = "ord"
	}
	if c.Backend.FlyIO.AgentImage == "" {
		c.Backend.FlyIO.AgentImage = backends.AgentImage()
	}
	if c.Backend.GCP.AgentImage == "" {
		c.Backend.GCP.AgentImage = backends.AgentImage()
	}
	if c.Backend.Azure.AgentImage == "" {
		c.Backend.Azure.AgentImage = backends.AgentImage()
	}
	if c.Join.Mode == "" {
		c.Join.Mode = JoinModeK3s
	}
	if c.Tailscale.TagPrefix == "" {
		c.Tailscale.TagPrefix = "yscale-burst"
	}
	if c.Scaling.MaxNodes == 0 {
		c.Scaling.MaxNodes = 10
	}
	if c.Scaling.ScaleDownDelay == 0 {
		c.Scaling.ScaleDownDelay = 5 * time.Minute
	}
	if c.Scaling.PollInterval == 0 {
		c.Scaling.PollInterval = 10 * time.Second
	}
	if c.Scaling.PendingThreshold == 0 {
		c.Scaling.PendingThreshold = 10 * time.Second
	}
	if c.Scaling.NodeReadyTimeout == 0 {
		c.Scaling.NodeReadyTimeout = 2 * time.Minute
	}
	if c.Scaling.MaxConcurrentProvisions == 0 {
		c.Scaling.MaxConcurrentProvisions = 1
	}
	if c.Scaling.NodeResources.CPUMillis == 0 {
		c.Scaling.NodeResources.CPUMillis = 2000
	}
	if c.Scaling.NodeResources.MemoryMB == 0 {
		c.Scaling.NodeResources.MemoryMB = 4096
	}
	if c.Scaling.PrewarmSuspend == nil {
		enabled := c.Scaling.PrewarmPool > 0
		c.Scaling.PrewarmSuspend = &enabled
	}
	if c.Scaling.PrewarmMaxAge == 0 {
		c.Scaling.PrewarmMaxAge = 6 * time.Hour
	}

	// Cost defaults: ensure at least Fly.io shared-cpu pricing is available
	// so that homelab users with no costs block still get a spend readout.
	// Numbers are derived from Fly.io shared-cpu-1x list pricing
	// (~$0.0000022/sec/CPU and ~$5/GB/month). Users can override per-backend.
	if c.Scaling.Costs.Backends == nil {
		c.Scaling.Costs.Backends = make(map[string]BackendCost)
	}
	if _, ok := c.Scaling.Costs.Backends[backends.TypeFlyIO]; !ok {
		// Fly is CPU-only after July 31, 2026 (their GPU SKUs deprecate).
		// Numbers from shared-cpu-1x list pricing.
		c.Scaling.Costs.Backends[backends.TypeFlyIO] = BackendCost{
			BaseUSDPerHour:   0,
			PerCPUUSDPerHour: 0.008,
			PerGBUSDPerHour:  0.007,
			GPURates:         map[string]float64{},
		}
	}
	if _, ok := c.Scaling.Costs.Backends[backends.TypeLinode]; !ok {
		// Linode dedicated-GPU list pricing (smallest 1-GPU plans).
		// Linode is currently the GPU primary; no spot tier.
		c.Scaling.Costs.Backends[backends.TypeLinode] = BackendCost{
			BaseUSDPerHour:   0,
			PerCPUUSDPerHour: 0.004,
			PerGBUSDPerHour:  0.002,
			GPURates: map[string]float64{
				"rtx4000ada": 0.52,
				"rtx6000":    1.50,
			},
		}
	}
}

// AgentImage returns the OCI image for the agent on the active backend.
// Used by the controller when building NodeSpecs so it doesn't need to
// know which backend is configured.
func (c *Config) AgentImage() string {
	switch c.Backend.Type {
	case backends.TypeFlyIO:
		return c.Backend.FlyIO.AgentImage
	case backends.TypeGCP:
		return c.Backend.GCP.AgentImage
	case backends.TypeAzure:
		return c.Backend.Azure.AgentImage
	default:
		return backends.AgentImage()
	}
}

func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("FLYIO_TOKEN"); v != "" {
		c.Backend.FlyIO.APIToken = v
	}
	if v := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); v != "" {
		c.Backend.GCP.CredentialsFile = v
	}
	if v := os.Getenv("AZURE_CLIENT_SECRET"); v != "" {
		c.Backend.Azure.ClientSecret = v
	}
	if v := os.Getenv("TS_AUTHKEY"); v != "" {
		c.Tailscale.AuthKey = v
	}
	if v := os.Getenv("K3S_TOKEN"); v != "" {
		c.Join.K3sToken = v
	}
}

func (c *Config) Validate() error {
	if err := c.validateBackend(); err != nil {
		return err
	}
	if err := c.validateJoin(); err != nil {
		return err
	}
	if c.Tailscale.Enabled && c.Tailscale.AuthKey == "" {
		return fmt.Errorf("TS_AUTHKEY env var is required when tailscale is enabled")
	}
	return c.validateScaling()
}

func (c *Config) validateBackend() error {
	switch c.Backend.Type {
	case backends.TypeFlyIO:
		if c.Backend.FlyIO.APIToken == "" {
			return fmt.Errorf("FLYIO_TOKEN env var is required for flyio backend")
		}
		if c.Backend.FlyIO.Org == "" {
			return fmt.Errorf("backend.flyio.org is required")
		}
	default:
		return fmt.Errorf("unsupported backend type: %s", c.Backend.Type)
	}
	return nil
}

func (c *Config) validateJoin() error {
	switch c.Join.Mode {
	case JoinModeK3s:
		if c.Join.K3sServerURL == "" {
			return fmt.Errorf("join.k3sServerURL is required for k3s mode")
		}
		if c.Join.K3sToken == "" {
			return fmt.Errorf("K3S_TOKEN env var is required for k3s mode")
		}
	case JoinModeKubelet:
		if c.Join.APIServer == "" {
			return fmt.Errorf("join.apiServer is required for kubelet mode")
		}
		if c.Join.ClusterCA == "" {
			return fmt.Errorf("join.clusterCA is required for kubelet mode")
		}
	default:
		return fmt.Errorf("invalid join mode: %s (must be k3s or kubelet)", c.Join.Mode)
	}
	return nil
}

func (c *Config) validateScaling() error {
	s := &c.Scaling
	if s.MaxConcurrentProvisions < 1 {
		return fmt.Errorf("scaling.maxConcurrentProvisions must be >= 1")
	}
	if s.PrewarmPool < 0 {
		return fmt.Errorf("scaling.prewarmPool must be >= 0")
	}
	if s.PrewarmPool >= s.MaxNodes {
		return fmt.Errorf("scaling.prewarmPool must be less than scaling.maxNodes")
	}
	if s.PrewarmMaxAge < 0 {
		return fmt.Errorf("scaling.prewarmMaxAge must be >= 0")
	}
	// A cold-stopped pool is unsafe on every backend: it leaves powered-off
	// machines that can keep incurring provider costs (and, on Fly, are created with
	// auto_destroy=true so the pool can silently self-destruct) outside the
	// controller's cost tracking and spend cap. Only the suspend-based
	// prewarm state machine is supported, and only on a backend that
	// implements provider suspend.
	if s.PrewarmPool > 0 {
		if !s.PrewarmSuspendEnabled() {
			return fmt.Errorf("scaling.prewarmPool must be 0 unless scaling.prewarmSuspend is true: cold-stop pooling is not supported")
		}
		if !backendSupportsPrewarmSuspend(c.Backend.Type) {
			return fmt.Errorf("scaling.prewarmPool must be 0 for %s: prewarmSuspend requires a backend with provider suspend (currently %s only)",
				c.Backend.Type, backends.TypeFlyIO)
		}
	}
	if s.NodeResources.CPUMillis < 1000 {
		return fmt.Errorf("scaling.nodeResources.cpuMillis must be >= 1000")
	}
	if s.NodeResources.MemoryMB < 256 {
		return fmt.Errorf("scaling.nodeResources.memoryMB must be >= 256")
	}
	return nil
}
