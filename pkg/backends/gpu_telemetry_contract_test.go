package backends

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

type gpuBootstrapScript struct {
	name string
	path string
}

var gpuCapableScripts = []gpuBootstrapScript{
	{"fly-entrypoint", "../../burst/image/entrypoint-kubelet.sh"},
	{"linode-baked", "../backends/linode/bootstrap-baked.sh"},
	{"linode-fallback", "../backends/linode/bootstrap.sh"},
	{"aws-fallback", "../backends/aws/bootstrap.sh"},
	{"aws-baked", "../backends/aws/bootstrap-baked.sh"},
}

func readScript(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func TestGPUReporterInstalledInAllGPUCapablePaths(t *testing.T) {
	for _, s := range gpuCapableScripts {
		t.Run(s.name, func(t *testing.T) {
			body := readScript(t, s.path)
			if !strings.Contains(body, "gpu-telemetry") {
				t.Fatalf("%s does not install the GPU telemetry reporter", s.name)
			}
		})
	}
}

func TestGPUReporterUsesCorrectEndpointAndPayload(t *testing.T) {
	for _, s := range gpuCapableScripts {
		t.Run(s.name, func(t *testing.T) {
			body := readScript(t, s.path)
			if !strings.Contains(body, `/gpu-telemetry`) {
				t.Fatalf("%s missing /gpu-telemetry endpoint", s.name)
			}
			if !strings.Contains(body, `burst_id`) {
				t.Fatalf("%s missing burst_id in payload", s.name)
			}
			if !strings.Contains(body, `node_name`) {
				t.Fatalf("%s missing node_name in payload", s.name)
			}
			if !strings.Contains(body, `utilization`) {
				t.Fatalf("%s missing utilization in payload", s.name)
			}
			if !strings.Contains(body, `product`) || !strings.Contains(body, `--query-gpu=name`) {
				t.Fatalf("%s missing hardware product observation in payload", s.name)
			}
		})
	}
}

func TestGPUReporterRejectsMixedProductObservation(t *testing.T) {
	for _, s := range gpuCapableScripts {
		t.Run(s.name, func(t *testing.T) {
			body := readScript(t, s.path)
			if !strings.Contains(body, `product_mixed`) || !strings.Contains(body, `product=""`) {
				t.Fatalf("%s does not omit a mixed-model product observation", s.name)
			}
		})
	}
}

func extractGPUReporterBlock(body string) string {
	start := strings.Index(body, "gpu_telemetry_reporter")
	if start < 0 {
		start = strings.Index(body, "yscale-gpu-telemetry")
	}
	if start < 0 {
		return ""
	}
	end := start + 600
	if end > len(body) {
		end = len(body)
	}
	return body[start:end]
}

func TestGPUReporterAggregatesAllGPUs(t *testing.T) {
	for _, s := range gpuCapableScripts {
		t.Run(s.name, func(t *testing.T) {
			body := readScript(t, s.path)
			block := extractGPUReporterBlock(body)
			if block == "" {
				t.Fatalf("%s has no GPU reporter block", s.name)
			}
			if strings.Contains(block, "head -1") {
				t.Fatalf("%s GPU reporter uses head -1 (only reports first GPU)", s.name)
			}
			if !strings.Contains(block, "max_util") {
				t.Fatalf("%s does not aggregate across GPUs (no max_util variable)", s.name)
			}
		})
	}
}

func TestGPUReporterBoundedCurlTimeout(t *testing.T) {
	for _, s := range gpuCapableScripts {
		t.Run(s.name, func(t *testing.T) {
			body := readScript(t, s.path)
			if !strings.Contains(body, "--max-time") {
				t.Fatalf("%s missing --max-time on curl", s.name)
			}
		})
	}
}

func TestGPUReporterNoSecretsInPayload(t *testing.T) {
	for _, s := range gpuCapableScripts {
		t.Run(s.name, func(t *testing.T) {
			body := readScript(t, s.path)
			lines := strings.Split(body, "\n")
			for _, line := range lines {
				lower := strings.ToLower(line)
				if strings.Contains(lower, "gpu-telemetry") || strings.Contains(lower, "gpu_telemetry") {
					if strings.Contains(lower, "ts_authkey") || strings.Contains(lower, "token") {
						if !strings.Contains(lower, "metadata-token") {
							t.Fatalf("%s: GPU telemetry line may leak secrets: %s", s.name, line)
						}
					}
				}
			}
		})
	}
}

func TestGPUCapableScriptsSyntacticallyValid(t *testing.T) {
	for _, s := range gpuCapableScripts {
		t.Run(s.name, func(t *testing.T) {
			cmd := exec.Command("bash", "-n", s.path)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("bash -n %s failed: %v\n%s", s.path, err, out)
			}
		})
	}
}

func TestGPUReporterGatedOnNvidiaSmi(t *testing.T) {
	for _, s := range gpuCapableScripts {
		t.Run(s.name, func(t *testing.T) {
			body := readScript(t, s.path)
			if !strings.Contains(body, "nvidia-smi") {
				t.Fatalf("%s missing nvidia-smi gate", s.name)
			}
		})
	}
}

func TestGPUReporterSystemdManagedInProductionPaths(t *testing.T) {
	productionScripts := []gpuBootstrapScript{
		{"linode-baked", "../backends/linode/bootstrap-baked.sh"},
		{"linode-fallback", "../backends/linode/bootstrap.sh"},
		{"aws-fallback", "../backends/aws/bootstrap.sh"},
		{"aws-baked", "../backends/aws/bootstrap-baked.sh"},
	}
	for _, s := range productionScripts {
		t.Run(s.name, func(t *testing.T) {
			body := readScript(t, s.path)
			if !strings.Contains(body, "yscale-gpu-telemetry.service") {
				t.Fatalf("%s missing systemd service for GPU telemetry", s.name)
			}
		})
	}
}
