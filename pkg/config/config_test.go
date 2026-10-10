package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/backends"
)

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "yscale.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("writing config fixture: %v", err)
	}
	return path
}

// withEnv sets env var k to v for the duration of the test.
func withEnv(t *testing.T, k, v string) {
	t.Helper()
	prev, had := os.LookupEnv(k)
	if err := os.Setenv(k, v); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(k, prev)
		} else {
			_ = os.Unsetenv(k)
		}
	})
}

func TestLoadAppliesDefaults(t *testing.T) {
	path := writeConfig(t, `
backend:
  type: flyio
  flyio:
    org: personal
join:
  mode: k3s
  k3sServerURL: https://1.2.3.4:6443
`)
	withEnv(t, "FLYIO_TOKEN", "f")
	withEnv(t, "K3S_TOKEN", "k")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Backend.FlyIO.Region != "ord" {
		t.Errorf("default region: got %q want ord", cfg.Backend.FlyIO.Region)
	}
	if cfg.Backend.FlyIO.AgentImage != backends.AgentImage() {
		t.Errorf("default agent image not applied: got %q", cfg.Backend.FlyIO.AgentImage)
	}
	if cfg.Scaling.MaxNodes != 10 {
		t.Errorf("default maxNodes: got %d want 10", cfg.Scaling.MaxNodes)
	}
	if cfg.Scaling.NodeResources.CPUMillis != 2000 {
		t.Errorf("default cpu: got %d want 2000", cfg.Scaling.NodeResources.CPUMillis)
	}
	if cfg.Tailscale.TagPrefix != "yscale-burst" {
		t.Errorf("default tag prefix: got %q want yscale-burst", cfg.Tailscale.TagPrefix)
	}
}

func TestLoadFillsDefaultBackendType(t *testing.T) {
	path := writeConfig(t, `
backend:
  flyio:
    org: personal
join:
  mode: k3s
  k3sServerURL: https://1.2.3.4:6443
`)
	withEnv(t, "FLYIO_TOKEN", "f")
	withEnv(t, "K3S_TOKEN", "k")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Backend.Type != backends.TypeFlyIO {
		t.Errorf("default backend.type: got %q want %q", cfg.Backend.Type, backends.TypeFlyIO)
	}
}

func TestValidateMissingTokens(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		envs    map[string]string
		wantErr string
	}{
		{
			name:    "flyio without token",
			yaml:    "backend:\n  type: flyio\n  flyio:\n    org: personal\njoin:\n  mode: k3s\n  k3sServerURL: https://x:6443\n",
			envs:    map[string]string{"K3S_TOKEN": "k"},
			wantErr: "FLYIO_TOKEN",
		},
		{
			name:    "flyio without org",
			yaml:    "backend:\n  type: flyio\n  flyio: {}\njoin:\n  mode: k3s\n  k3sServerURL: https://x:6443\n",
			envs:    map[string]string{"FLYIO_TOKEN": "f", "K3S_TOKEN": "k"},
			wantErr: "backend.flyio.org",
		},
		{
			name:    "k3s mode without token",
			yaml:    "backend:\n  type: flyio\n  flyio:\n    org: personal\njoin:\n  mode: k3s\n  k3sServerURL: https://x:6443\n",
			envs:    map[string]string{"FLYIO_TOKEN": "f"},
			wantErr: "K3S_TOKEN",
		},
		{
			name:    "kubelet mode without apiServer",
			yaml:    "backend:\n  type: flyio\n  flyio:\n    org: personal\njoin:\n  mode: kubelet\n  clusterCA: x\n",
			envs:    map[string]string{"FLYIO_TOKEN": "f"},
			wantErr: "join.apiServer",
		},
		{
			name:    "kubelet mode without clusterCA",
			yaml:    "backend:\n  type: flyio\n  flyio:\n    org: personal\njoin:\n  mode: kubelet\n  apiServer: https://eks\n",
			envs:    map[string]string{"FLYIO_TOKEN": "f"},
			wantErr: "join.clusterCA",
		},
		{
			name:    "unknown backend type",
			yaml:    "backend:\n  type: aws-bare\njoin:\n  mode: k3s\n  k3sServerURL: https://x:6443\n",
			envs:    map[string]string{"K3S_TOKEN": "k"},
			wantErr: "unsupported backend type",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := writeConfig(t, c.yaml)
			for k, v := range c.envs {
				withEnv(t, k, v)
			}
			// Defensively unset envs we don't want leaking in.
			for _, k := range []string{"FLYIO_TOKEN", "K3S_TOKEN", "TS_AUTHKEY"} {
				if _, set := c.envs[k]; !set {
					_ = os.Unsetenv(k)
				}
			}

			_, err := Load(path)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("error %q does not contain %q", err, c.wantErr)
			}
		})
	}
}

func TestAgentImageDispatch(t *testing.T) {
	cfg := &Config{}
	cfg.Backend.FlyIO.AgentImage = "fly-img"
	cfg.Backend.GCP.AgentImage = "gcp-img"

	cases := []struct {
		typ  string
		want string
	}{
		{backends.TypeFlyIO, "fly-img"},
		{backends.TypeGCP, "gcp-img"},
		{"unknown", backends.AgentImage()},
	}
	for _, c := range cases {
		cfg.Backend.Type = c.typ
		if got := cfg.AgentImage(); got != c.want {
			t.Errorf("AgentImage type=%q: got %q want %q", c.typ, got, c.want)
		}
	}
}

func TestFlyPrewarmPool(t *testing.T) {
	tests := []struct {
		name    string
		scaling string
		wantErr string
	}{
		{
			name:    "prewarmPool defaults to suspend mode and validates",
			scaling: "scaling:\n  prewarmPool: 1\n",
		},
		{
			name:    "explicit prewarmSuspend false fails closed on flyio",
			scaling: "scaling:\n  prewarmPool: 1\n  prewarmSuspend: false\n",
			wantErr: "cold-stop pooling is not supported",
		},
		{
			name:    "negative prewarmMaxAge rejected",
			scaling: "scaling:\n  prewarmPool: 1\n  prewarmMaxAge: -1h\n",
			wantErr: "prewarmMaxAge must be >= 0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, `
backend:
  type: flyio
  flyio:
    org: personal
join:
  mode: k3s
  k3sServerURL: https://1.2.3.4:6443
`+tc.scaling)
			withEnv(t, "FLYIO_TOKEN", "f")
			withEnv(t, "K3S_TOKEN", "k")

			cfg, err := Load(path)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load error = %v, want nil", err)
			}
			if !cfg.Scaling.PrewarmSuspendEnabled() {
				t.Fatal("PrewarmSuspendEnabled = false, want default true when prewarmPool > 0")
			}
			if cfg.Scaling.PrewarmMaxAge != 6*time.Hour {
				t.Fatalf("PrewarmMaxAge = %v, want default 6h", cfg.Scaling.PrewarmMaxAge)
			}
		})
	}
}

// Prewarming is only safe as provider suspend. A cold-stopped pool creates
// powered-off but still-billing machines that sit outside cost tracking and
// the monthly cap, so every combination except a suspend-capable backend with
// prewarmSuspend on must be rejected at configuration time rather than
// silently degraded.
func TestPrewarmPoolRequiresSuspendCapableBackend(t *testing.T) {
	tests := []struct {
		name        string
		backendType string
		prewarmPool int
		suspend     *bool // nil exercises the applyDefaults path
		wantErr     string
	}{
		{
			name:        "flyio with suspend is the supported combination",
			backendType: backends.TypeFlyIO,
			prewarmPool: 1,
			suspend:     boolPtr(true),
		},
		{
			name:        "flyio defaults to suspend and validates",
			backendType: backends.TypeFlyIO,
			prewarmPool: 1,
		},
		{
			name:        "flyio with explicit cold mode rejected",
			backendType: backends.TypeFlyIO,
			prewarmPool: 1,
			suspend:     boolPtr(false),
			wantErr:     "cold-stop pooling is not supported",
		},
		{
			name:        "linode cannot prewarm even with suspend requested",
			backendType: backends.TypeLinode,
			prewarmPool: 1,
			suspend:     boolPtr(true),
			wantErr:     "must be 0 for linode",
		},
		{
			name:        "linode cannot prewarm under the suspend default",
			backendType: backends.TypeLinode,
			prewarmPool: 2,
			wantErr:     "must be 0 for linode",
		},
		{
			name:        "linode with explicit cold mode rejected",
			backendType: backends.TypeLinode,
			prewarmPool: 1,
			suspend:     boolPtr(false),
			wantErr:     "cold-stop pooling is not supported",
		},
		{
			name:        "aws cannot prewarm",
			backendType: backends.TypeAWS,
			prewarmPool: 1,
			suspend:     boolPtr(true),
			wantErr:     "must be 0 for aws",
		},
		{
			name:        "linode without a pool is unaffected",
			backendType: backends.TypeLinode,
			prewarmPool: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				Backend: BackendConfig{Type: tc.backendType},
				Scaling: ScalingConfig{PrewarmPool: tc.prewarmPool, PrewarmSuspend: tc.suspend},
			}
			cfg.applyDefaults()

			err := cfg.validateScaling()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateScaling error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validateScaling error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func boolPtr(v bool) *bool { return &v }

func TestLoadRejectsUnknownAndRemovedWarmFields(t *testing.T) {
	tests := []struct {
		name  string
		field string
	}{
		{name: "removed backend pool scope", field: "  poolScope: old-scope\n"},
		{name: "removed prewarm max idle", field: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			yml := "backend:\n  type: flyio\n" + tc.field + `  flyio:
    org: personal
join:
  mode: k3s
  k3sServerURL: https://1.2.3.4:6443
scaling:
  prewarmPool: 0
`
			if tc.name == "removed prewarm max idle" {
				yml += "  prewarmMaxIdle: 20m\n"
			}
			path := writeConfig(t, yml)
			withEnv(t, "FLYIO_TOKEN", "f")
			withEnv(t, "K3S_TOKEN", "k")

			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), "field") {
				t.Fatalf("Load error = %v, want unknown-field rejection", err)
			}
		})
	}
}

func TestCostDefaultsPopulatedForEachBackend(t *testing.T) {
	path := writeConfig(t, `
backend:
  type: flyio
  flyio:
    org: personal
join:
  mode: k3s
  k3sServerURL: https://x:6443
`)
	withEnv(t, "FLYIO_TOKEN", "f")
	withEnv(t, "K3S_TOKEN", "k")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, name := range []string{backends.TypeFlyIO, backends.TypeLinode} {
		bc, ok := cfg.Scaling.Costs.Backends[name]
		if !ok {
			t.Errorf("cost defaults missing for %q", name)
			continue
		}
		if bc.PerCPUUSDPerHour <= 0 {
			t.Errorf("%q PerCPUUSDPerHour should be > 0, got %v", name, bc.PerCPUUSDPerHour)
		}
	}
	// Linode is the GPU primary; it must ship with GPU rate defaults.
	// Fly is CPU-only after the July 2026 GPU deprecation, so it
	// legitimately ships with empty GPURates.
	if rates := cfg.Scaling.Costs.Backends[backends.TypeLinode].GPURates; len(rates) == 0 {
		t.Errorf("linode should have GPU rate defaults, got %v", rates)
	}
}
