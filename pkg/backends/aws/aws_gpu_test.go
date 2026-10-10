package aws

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/smithy-go"

	"github.com/yscale-sh/yscale/pkg/backends"
)

// A GPU burst's bootstrap must be told it is a GPU node so it configures
// the nvidia containerd runtime (the DLAMI ships the driver, but
// containerd has to be pointed at the nvidia runtime before GPU pods can
// see the device). BURST_GPU=1 is that switch; GPU_KIND records the
// resolved card. Empty/any normalises to the default card so the export
// is never blank.
func TestGPUBootstrapEnv(t *testing.T) {
	env := gpuBootstrapEnv("L4")
	if env["BURST_GPU"] != "1" {
		t.Errorf("BURST_GPU = %q, want \"1\"", env["BURST_GPU"])
	}
	if env["GPU_KIND"] != "l4" {
		t.Errorf("GPU_KIND = %q, want \"l4\"", env["GPU_KIND"])
	}
	if got := gpuBootstrapEnv("")["GPU_KIND"]; got != "l4" {
		t.Errorf("empty kind GPU_KIND = %q, want default \"l4\"", got)
	}
}

// An unmappable GPU kind must fail at plan time, before any EC2 API call —
// otherwise a typo'd kind would launch (and bill) an instance, or hang on
// a credential/network error that obscures the real mistake. This runs
// with no AWS credentials precisely to prove no API call happens first.
func TestCreateNode_GPUUnknownKindFailsFast(t *testing.T) {
	b := New("", "", "", "us-east-1")
	_, err := b.CreateNode(context.Background(), &backends.NodeSpec{
		Name:    "ys-burst-x",
		BurstID: "burst_x",
		Resources: backends.ResourceRequirements{
			GPU: &backends.GPUSpec{Kind: "rtx4000ada", Count: 1},
		},
	})
	if err == nil {
		t.Fatal("expected CreateNode to reject unknown GPU kind, got nil")
	}
	if !strings.Contains(err.Error(), "rtx4000ada") {
		t.Errorf("error should name the bad kind, got: %v", err)
	}
	if backends.CreateOutcomeAmbiguous(err) {
		t.Fatalf("pre-dispatch GPU validation must prove zero resource: %v", err)
	}
}

func TestAWSCreateRequestRefusedClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "client refusal", err: &smithy.GenericAPIError{Code: "InvalidParameterValue", Fault: smithy.FaultClient}, want: true},
		{name: "server failure", err: &smithy.GenericAPIError{Code: "InternalError", Fault: smithy.FaultServer}, want: false},
		{name: "transport failure", err: context.DeadlineExceeded, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := awsCreateRequestRefused(tc.err); got != tc.want {
				t.Fatalf("awsCreateRequestRefused = %v, want %v", got, tc.want)
			}
		})
	}
}

// MapGPUType is the EC2 analogue of Linode's MapGPUType: it resolves an
// abstract GPUSpec to the smallest EC2 instance type carrying the
// requested GPU count. The mapping is drawn from docs/backends/aws-plan.md
// §7. These tests pin the contract: a wrong SKU silently bills the
// customer for the wrong (often far more expensive) instance.
func TestMapGPUType(t *testing.T) {
	cases := []struct {
		name string
		kind string
		// count of 0 must default to 1, mirroring Linode.
		count int
		want  string
	}{
		{"default empty kind = cheapest current-gen L4", "", 0, "g6.xlarge"},
		{"any = cheapest current-gen L4", "any", 1, "g6.xlarge"},
		{"t4 single", "t4", 1, "g4dn.xlarge"},
		{"a10g single", "a10g", 1, "g5.xlarge"},
		{"l4 single", "l4", 1, "g6.xlarge"},
		{"l40s single", "l40s", 1, "g6e.xlarge"},
		{"kind is case-insensitive", "L4", 1, "g6.xlarge"},
		{"l4 quad", "l4", 4, "g6.12xlarge"},
		{"l4 octa", "l4", 8, "g6.48xlarge"},
		{"a10g quad", "a10g", 4, "g5.12xlarge"},
		{"a10g octa", "a10g", 8, "g5.48xlarge"},
		{"l40s quad", "l40s", 4, "g6e.12xlarge"},
		{"l40s octa", "l40s", 8, "g6e.48xlarge"},
		{"t4 quad", "t4", 4, "g4dn.12xlarge"},
		{"t4 octa is bare-metal", "t4", 8, "g4dn.metal"},
		{"a100 octa", "a100", 8, "p4d.24xlarge"},
		{"h100 octa", "h100", 8, "p5.48xlarge"},
		{"h200 octa", "h200", 8, "p5e.48xlarge"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := MapGPUType(&backends.GPUSpec{Kind: c.kind, Count: c.count})
			if err != nil {
				t.Fatalf("MapGPUType(%q,%d) unexpected error: %v", c.kind, c.count, err)
			}
			if got != c.want {
				t.Errorf("MapGPUType(%q,%d) = %q, want %q", c.kind, c.count, got, c.want)
			}
		})
	}
}

// P-family (A100/H100/H200) ships ONLY the full 8-GPU node. A request for
// any other count must error loudly rather than silently rounding up to a
// $30-100/hr 8-GPU instance the customer didn't ask for.
func TestMapGPUType_PFamilyRejectsNonOcta(t *testing.T) {
	for _, kind := range []string{"a100", "h100", "h200"} {
		for _, count := range []int{1, 2, 4} {
			if _, err := MapGPUType(&backends.GPUSpec{Kind: kind, Count: count}); err == nil {
				t.Errorf("MapGPUType(%q,%d): expected error (P-family is 8-GPU only), got nil", kind, count)
			}
		}
	}
}

// G-family counts other than 1/4/8 have no single-instance SKU. Error
// rather than guess.
func TestMapGPUType_GFamilyRejectsOddCounts(t *testing.T) {
	for _, count := range []int{2, 3, 5, 16} {
		if _, err := MapGPUType(&backends.GPUSpec{Kind: "l4", Count: count}); err == nil {
			t.Errorf("MapGPUType(l4,%d): expected error for unsupported count, got nil", count)
		}
	}
}

// The full-install bootstrap (the one GPU bursts run on the DLAMI) must
// configure containerd's nvidia runtime when BURST_GPU=1 — the DLAMI ships
// the driver + toolkit, but containerd won't expose the GPU to pods until
// it's pointed at /usr/bin/nvidia-container-runtime and that runtime is
// made the default. A silent drop of this block yields GPU nodes whose
// pods can't see the GPU.
func TestBootstrap_GPUNvidiaRuntimeBlock(t *testing.T) {
	want := []string{
		"BURST_GPU",
		"nvidia-container-runtime",
		`runtimes.nvidia`,
		`default_runtime_name = "nvidia"`,
	}
	for _, w := range want {
		if !strings.Contains(bootstrapScript, w) {
			t.Errorf("aws bootstrap.sh missing GPU runtime directive %q", w)
		}
	}
}

// The containerd nvidia runtime BinaryName must be resolved from PATH (not
// a hard-coded path that silently breaks if a future DLAMI relocates the
// toolkit), and the burst must fail loudly if the toolkit is absent.
func TestBootstrap_GPUResolvesRuntimeBinary(t *testing.T) {
	if !strings.Contains(bootstrapScript, "command -v nvidia-container-runtime") {
		t.Error("GPU bootstrap should resolve nvidia-container-runtime from PATH, not hard-code its location")
	}
}

// CRITICAL: the nvidia containerd runtime only grants a container GPU
// access — it does NOT make the kubelet advertise nvidia.com/gpu. Without
// the NVIDIA device plugin the node reports 0 GPU capacity and every GPU
// pod (which requests nvidia.com/gpu) hangs Pending forever while the burst
// bills. Mirror Linode: a static pod + --pod-manifest-path.
func TestBootstrap_GPUDevicePluginStaticPod(t *testing.T) {
	want := []string{
		"/etc/kubernetes/manifests/nvidia-device-plugin.yaml",
		"nvcr.io/nvidia/k8s-device-plugin",
		"operator: Exists", // must tolerate the burst NoSchedule taint
	}
	for _, w := range want {
		if !strings.Contains(bootstrapScript, w) {
			t.Errorf("aws bootstrap.sh missing GPU device-plugin directive %q", w)
		}
	}
}

func TestBootstrap_KubeletPodManifestPath(t *testing.T) {
	if !strings.Contains(bootstrapScript, "--pod-manifest-path=/etc/kubernetes/manifests") {
		t.Error("kubelet must set --pod-manifest-path so the GPU device-plugin static pod runs")
	}
}

// The nvidia runtime must be CONDITIONAL on BURST_GPU=1 so a CPU burst
// (BURST_GPU unset) gets a plain runc-only containerd and never references
// a runtime binary that isn't on a stock Debian AMI.
func TestBootstrap_GPURuntimeGatedOnBurstGPU(t *testing.T) {
	if !strings.Contains(bootstrapScript, `"${BURST_GPU:-0}" = "1"`) {
		t.Error("nvidia runtime block must be gated on [ \"${BURST_GPU:-0}\" = \"1\" ]")
	}
}

// A GPU burst should fail loudly if the AMI somehow lacks the driver,
// rather than registering a node whose GPU pods mysteriously can't init.
func TestBootstrap_GPUVerifiesDriver(t *testing.T) {
	if !strings.Contains(bootstrapScript, "nvidia-smi") {
		t.Error("GPU bootstrap should verify the NVIDIA driver via nvidia-smi")
	}
}

// The GPU DLAMI may already ship containerd.io (pulled in by Docker);
// apt-installing the distro `containerd` package on top of it conflicts
// and aborts the bootstrap. The install must be guarded on containerd
// actually being absent (always true on the stock-Debian CPU path).
func TestBootstrap_ContainerdInstallConditional(t *testing.T) {
	if !strings.Contains(bootstrapScript, "command -v containerd") {
		t.Error("containerd install should be guarded (command -v containerd) so it doesn't conflict with a DLAMI's pre-installed containerd.io")
	}
}

func TestMapGPUType_UnknownKind(t *testing.T) {
	_, err := MapGPUType(&backends.GPUSpec{Kind: "rtx4000ada", Count: 1})
	if err == nil {
		t.Fatal("expected error for unknown AWS gpu kind, got nil")
	}
	// The error should name the offending kind so the operator can fix the spec.
	if !strings.Contains(err.Error(), "rtx4000ada") {
		t.Errorf("error should mention the unknown kind, got: %v", err)
	}
}
