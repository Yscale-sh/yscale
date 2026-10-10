package workload

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/backends"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestValidateRejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		w    *Workload
		want string
	}{
		{
			name: "nil",
			w:    nil,
			want: "is nil",
		},
		{
			name: "no name",
			w: &Workload{
				Spec: Spec{Image: "x", Size: "small"},
			},
			want: "metadata.name",
		},
		{
			name: "no image",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Size: "small"},
			},
			want: "spec.image",
		},
		{
			name: "no sizing",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x"},
			},
			want: "size, cpu+memory, or machine",
		},
		{
			name: "size and cpu both set",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x", Size: "small", CPU: "2"},
			},
			want: "mutually exclusive",
		},
		{
			name: "memory without cpu",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x", Memory: "1Gi"},
			},
			want: "spec.cpu and spec.memory must be set together",
		},
		{
			name: "cpu without memory",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x", CPU: "1"},
			},
			want: "spec.cpu and spec.memory must be set together",
		},
		{
			name: "unknown size",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x", Size: "huge"},
			},
			want: "unknown size",
		},
		{
			name: "wrong apiVersion",
			w: &Workload{
				APIVersion: "v0",
				Metadata:   Metadata{Name: "w"},
				Spec:       Spec{Image: "x", Size: "small"},
			},
			want: "unsupported apiVersion",
		},
		{
			name: "capitalized networking tier",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x", Size: "small", Networking: &NetworkingSpec{Tier: "Full"}},
			},
			want: "unknown spec.networking.tier",
		},
		{
			name: "unknown networking tier",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x", Size: "small", Networking: &NetworkingSpec{Tier: "bogus"}},
			},
			want: "valid: full",
		},
		{
			// The lite tier is permanently unsupported. Refusal must
			// happen in Validate — which central runs before touching
			// provider, mesh or PodCIDR — so a legacy request cannot
			// silently be run as full capacity.
			name: "explicit lite tier is rejected",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x", Size: "small", Networking: &NetworkingSpec{Tier: "lite"}},
			},
			want: "spec.networking.tier \"lite\" is not supported",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := Validate(c.w)
			if err == nil {
				t.Fatalf("expected error %q, got nil", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not contain %q", err.Error(), c.want)
			}
		})
	}
}

// TestValidateAcceptsEmptyAndFullNetworkingTier pins the "empty tier defaults
// to full" invariant that the rest of the stack (translate, agent preflight,
// burst env) leans on. Without an explicit passing test, a future edit that
// tightens validateNetworking — say, requiring an explicit tier value — would
// silently regress every workload that omits spec.networking and reject them
// with the same wording lite gets, which is a very different failure mode.
func TestValidateAcceptsEmptyAndFullNetworkingTier(t *testing.T) {
	cases := []struct {
		name string
		w    *Workload
	}{
		{
			name: "no networking block",
			w:    &Workload{Metadata: Metadata{Name: "w"}, Spec: Spec{Image: "x", Size: "small"}},
		},
		{
			name: "empty networking block",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x", Size: "small", Networking: &NetworkingSpec{}},
			},
		},
		{
			name: "empty tier string",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x", Size: "small", Networking: &NetworkingSpec{Tier: ""}},
			},
		},
		{
			name: "explicit full tier",
			w: &Workload{
				Metadata: Metadata{Name: "w"},
				Spec:     Spec{Image: "x", Size: "small", Networking: &NetworkingSpec{Tier: NetworkingTierFull}},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := Validate(c.w); err != nil {
				t.Fatalf("Validate = %v, want nil (empty/full must be accepted)", err)
			}
			if got := ResolveNetworkingTier(c.w); got != NetworkingTierFull {
				t.Fatalf("ResolveNetworkingTier = %q, want %q", got, NetworkingTierFull)
			}
		})
	}
}

func TestValidateRegionRequiresExplicitLinode(t *testing.T) {
	// Region is honored only by Linode; every other backend (incl. "" / auto,
	// which can route to Fly or AWS) must be rejected rather than silently
	// ignore the pin.
	for _, backend := range []string{"aws", "flyio", ""} {
		err := Validate(&Workload{
			Metadata: Metadata{Name: "region-pin"},
			Spec:     Spec{Image: "x", Size: "small", Backend: backend, Region: "us-east-1"},
		})
		if err == nil || !strings.Contains(err.Error(), "spec.region is honored only by backend=linode") {
			t.Fatalf("backend=%q: Validate error = %v, want region rejection", backend, err)
		}
	}
	// Explicit Linode is allowed.
	if err := Validate(&Workload{
		Metadata: Metadata{Name: "region-pin-linode"},
		Spec:     Spec{Image: "x", Size: "small", Backend: "linode", Region: "us-ord"},
	}); err != nil {
		t.Fatalf("backend=linode + region: Validate error = %v, want ok", err)
	}
}

func TestValidateGPUHourlyCapFromYAMLRequiresFiniteNumber(t *testing.T) {
	tests := []struct {
		name    string
		yamlCap string
		wantErr bool
	}{
		{name: "NaN", yamlCap: ".nan", wantErr: true},
		{name: "positive infinity", yamlCap: ".inf", wantErr: true},
		{name: "negative infinity", yamlCap: "-.inf", wantErr: true},
		{name: "zero is unlimited", yamlCap: "0"},
		{name: "finite cap", yamlCap: "0.624"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := fmt.Sprintf(`apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: finite-cap-test
spec:
  image: busybox
  size: small
  backend: linode
  gpu:
    kind: rtx4000ada
    count: 1
    maxHourlyUSD: %s
`, tt.yamlCap)
			var wl Workload
			if err := yaml.Unmarshal([]byte(doc), &wl); err != nil {
				t.Fatalf("yaml.Unmarshal(%s): %v", tt.yamlCap, err)
			}
			err := Validate(&wl)
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "maxHourlyUSD must be finite") {
					t.Fatalf("Validate(%s) error = %v, want finite-number rejection", tt.yamlCap, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate(%s): %v", tt.yamlCap, err)
			}
		})
	}
}

// TestValidateReliabilityFailsClosedOnPreemptibleTiers pins the live contract:
// nothing downstream reads spec.gpu.reliability, so accepting spot/any would
// launch — and bill — reliable capacity under an interruptible label. Refuse in
// Validate, which central runs before it authorizes a namespace or plans a node.
func TestValidateReliabilityFailsClosedOnPreemptibleTiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		reliability string
		wantErr     string
	}{
		{name: "empty means on-demand", reliability: ""},
		{name: "reliable means on-demand", reliability: ReliabilityReliable},
		{name: "spot", reliability: ReliabilitySpot, wantErr: "not supported"},
		{name: "any", reliability: ReliabilityAny, wantErr: "not supported"},
		{name: "unknown", reliability: "preemptible", wantErr: "unknown spec.gpu.reliability"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Workload{
				APIVersion: APIVersion,
				Kind:       Kind,
				Metadata:   Metadata{Name: "reliability-test"},
				Spec: Spec{
					Image: "busybox",
					Size:  "small",
					GPU:   &GPURequest{Kind: "rtx4000ada", Count: 1, Reliability: tt.reliability},
				},
			}
			err := Validate(w)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate(%q): %v", tt.reliability, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate(%q) error = %v, want one containing %q", tt.reliability, err, tt.wantErr)
			}
			// The refusal has to say what the platform will actually do, not
			// just that the value is unwelcome.
			if tt.reliability == ReliabilitySpot || tt.reliability == ReliabilityAny {
				if !strings.Contains(err.Error(), ReliabilityReliable) {
					t.Errorf("refusal does not name the supported tier: %v", err)
				}
			}
		})
	}
}

func TestToJobSizePresetAppliesResources(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "whisper-batch"},
		Spec: Spec{
			Image: "ghcr.io/example/whisper:v1",
			Size:  "large",
		},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]

	gotCPU := c.Resources.Requests[corev1.ResourceCPU]
	wantCPU := resource.NewMilliQuantity(4000, resource.DecimalSI)
	if !gotCPU.Equal(*wantCPU) {
		t.Errorf("CPU request: got %s, want %s", gotCPU.String(), wantCPU.String())
	}
	gotMem := c.Resources.Requests[corev1.ResourceMemory]
	wantMem := resource.NewQuantity(16384*1024*1024, resource.BinarySI)
	if !gotMem.Equal(*wantMem) {
		t.Errorf("Memory request: got %s, want %s", gotMem.String(), wantMem.String())
	}
}

func TestToJobExplicitCPUMemory(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "x"},
		Spec: Spec{
			Image:  "x",
			CPU:    "750m",
			Memory: "1.5Gi",
		},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if c.Resources.Requests.Cpu().MilliValue() != 750 {
		t.Errorf("CPU: got %s want 750m", c.Resources.Requests.Cpu().String())
	}
	if c.Resources.Requests.Memory().Value() != 1610612736 {
		t.Errorf("Memory: got %s want 1.5Gi", c.Resources.Requests.Memory().String())
	}
}

func TestToJobAddsBurstNodeTolerationAndSelector(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "x"},
		Spec:       Spec{Image: "x", Size: "small"},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	pod := job.Spec.Template.Spec
	if pod.NodeSelector["yscale.sh/burst-node"] != "true" {
		t.Errorf("missing burst-node nodeSelector: %v", pod.NodeSelector)
	}
	if len(pod.Tolerations) == 0 || pod.Tolerations[0].Key != "yscale.sh/burst-node" {
		t.Errorf("missing burst-node toleration: %v", pod.Tolerations)
	}
}

func TestToJobSetsHardenedSecurityContext(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "x"},
		Spec:       Spec{Image: "x", Size: "small"},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	sc := job.Spec.Template.Spec.Containers[0].SecurityContext
	if sc == nil {
		t.Fatal("workload container has no SecurityContext")
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Errorf("AllowPrivilegeEscalation = %v, want false", sc.AllowPrivilegeEscalation)
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("SeccompProfile = %v, want RuntimeDefault", sc.SeccompProfile)
	}
}

// TestToJobNeverTolerateCiliumAgentNotReady pins the pod-scheduling contract
// now that full is the only supported networking tier: the pod must never
// tolerate node.cilium.io/agent-not-ready. If it did, the pod would schedule
// before host-mode Cilium is up on the burst, ClusterIP + pod-to-pod traffic
// would fail, and with backoffLimit 0 the whole Job would fail. The removed
// lite tier used to tolerate the taint (there was no Cilium to wait for);
// reintroducing that toleration would regress every full-tier launch.
func TestToJobNeverTolerateCiliumAgentNotReady(t *testing.T) {
	tests := []struct {
		name       string
		networking *NetworkingSpec
	}{
		{name: "no networking block"},
		{name: "empty tier defaults full", networking: &NetworkingSpec{}},
		{name: "explicit full", networking: &NetworkingSpec{Tier: "full"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Workload{
				APIVersion: APIVersion,
				Kind:       Kind,
				Metadata:   Metadata{Name: "x"},
				Spec: Spec{
					Image:      "x",
					Size:       "small",
					Networking: tt.networking,
				},
			}
			job, err := ToJob(w)
			if err != nil {
				t.Fatalf("ToJob: %v", err)
			}
			if hasToleration(job.Spec.Template.Spec.Tolerations, "node.cilium.io/agent-not-ready") {
				t.Fatalf("full-tier pod must not tolerate cilium-agent-not-ready; tolerations=%v",
					job.Spec.Template.Spec.Tolerations)
			}
		})
	}
}

func TestToJobGpuNotReadyTolerationByGpu(t *testing.T) {
	// Mirrors the Cilium agent-not-ready tier gate, but for GPU readiness: a GPU
	// workload must NOT tolerate nvidia.com/gpu-not-ready (applied at kubelet
	// registration, cleared by the controller once nvidia.com/gpu is allocatable),
	// so it stays Pending instead of scheduling before the plugin is up and
	// burning its backoffLimit on FailedScheduling. Non-GPU workloads tolerate it
	// as a harmless no-op (they never request nvidia.com/gpu).
	tests := []struct {
		name              string
		size              string
		gpu               *GPURequest
		wantGpuToleration bool
	}{
		{name: "non-GPU tolerates gpu-not-ready (harmless no-op)", size: "small", wantGpuToleration: true},
		{name: "GPU does NOT tolerate gpu-not-ready (waits for plugin)", size: "large", gpu: &GPURequest{Kind: "l4", Count: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Workload{
				APIVersion: APIVersion,
				Kind:       Kind,
				Metadata:   Metadata{Name: "x"},
				Spec: Spec{
					Image: "x",
					Size:  tt.size,
					GPU:   tt.gpu,
				},
			}
			job, err := ToJob(w)
			if err != nil {
				t.Fatalf("ToJob: %v", err)
			}
			got := hasToleration(job.Spec.Template.Spec.Tolerations, "nvidia.com/gpu-not-ready")
			if got != tt.wantGpuToleration {
				t.Fatalf("gpu-not-ready toleration = %v, want %v; tolerations=%v",
					got, tt.wantGpuToleration, job.Spec.Template.Spec.Tolerations)
			}
		})
	}
}

func TestToJobOmitsCloudProviderUninitializedToleration(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "x"},
		Spec:       Spec{Image: "x", Size: "small"},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	if hasToleration(job.Spec.Template.Spec.Tolerations, "node.cloudprovider.kubernetes.io/uninitialized") {
		t.Fatalf("cloud-provider uninitialized toleration must not be present (taint removed); tolerations=%v",
			job.Spec.Template.Spec.Tolerations)
	}
}

func TestToJobAddsGPULimit(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "x"},
		Spec: Spec{
			Image: "x",
			Size:  "large",
			GPU:   &GPURequest{Kind: "l4", Count: 1},
		},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	gpu, ok := c.Resources.Limits[corev1.ResourceName("nvidia.com/gpu")]
	if !ok {
		t.Fatal("nvidia.com/gpu limit not set")
	}
	if gpu.Value() != 1 {
		t.Errorf("gpu count: got %d want 1", gpu.Value())
	}

	// Annotations should also carry the GPU info for the controller.
	if job.ObjectMeta.Annotations[AnnotationGPUKind] != "l4" {
		t.Errorf("missing gpu-kind annotation")
	}
	if job.ObjectMeta.Annotations[AnnotationGPUCount] != "1" {
		t.Errorf("missing gpu-count annotation")
	}
}

func TestToJobDefaultsGPUCountToOne(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "x"},
		Spec: Spec{
			Image: "x",
			Size:  "large",
			GPU:   &GPURequest{Kind: "l4"},
		},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	gpu := job.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceName("nvidia.com/gpu")]
	if gpu.Value() != 1 {
		t.Fatalf("default GPU limit = %d, want 1", gpu.Value())
	}
	if got := job.ObjectMeta.Annotations[AnnotationGPUCount]; got != "1" {
		t.Fatalf("default gpu-count annotation = %q, want 1", got)
	}
}

func hasToleration(tols []corev1.Toleration, key string) bool {
	for _, tol := range tols {
		if tol.Key == key {
			return true
		}
	}
	return false
}

func TestToJobBudgetAnnotations(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "x"},
		Spec: Spec{
			Image: "x",
			Size:  "small",
			Budget: &Budget{
				MaxUSD:   2.50,
				Deadline: 30 * time.Minute,
			},
		},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	if got := job.ObjectMeta.Annotations[AnnotationBudgetUSD]; got != "2.5000" {
		t.Errorf("budget annotation: got %q want 2.5000", got)
	}
	if got := job.ObjectMeta.Annotations[AnnotationDeadline]; got != "30m0s" {
		t.Errorf("deadline annotation: got %q want 30m0s", got)
	}
}

func TestToJobEnvSecretRef(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "x"},
		Spec: Spec{
			Image: "x",
			Size:  "small",
			Env: []EnvVar{
				{Name: "PLAIN", Value: "literal"},
				{Name: "FROM_SECRET", ValueFrom: &EnvVarFromRef{
					SecretKeyRef: &KeyRef{Name: "creds", Key: "token"},
				}},
			},
		},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	env := job.Spec.Template.Spec.Containers[0].Env
	if len(env) != 2 {
		t.Fatalf("env: got %d entries, want 2", len(env))
	}
	if env[0].Name != "PLAIN" || env[0].Value != "literal" {
		t.Errorf("plain env: %+v", env[0])
	}
	if env[1].ValueFrom == nil || env[1].ValueFrom.SecretKeyRef == nil {
		t.Errorf("secret ref env: %+v", env[1])
	} else {
		if env[1].ValueFrom.SecretKeyRef.Name != "creds" {
			t.Errorf("secret ref name: %s", env[1].ValueFrom.SecretKeyRef.Name)
		}
		if env[1].ValueFrom.SecretKeyRef.Key != "token" {
			t.Errorf("secret ref key: %s", env[1].ValueFrom.SecretKeyRef.Key)
		}
	}
}

func TestToJobNamespaceDefault(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "x"}, // no Namespace
		Spec:       Spec{Image: "x", Size: "small"},
	}
	job, _ := ToJob(w)
	if job.Namespace != "default" {
		t.Errorf("namespace: got %q want default", job.Namespace)
	}

	w.Metadata.Namespace = "ml-jobs"
	job, _ = ToJob(w)
	if job.Namespace != "ml-jobs" {
		t.Errorf("namespace: got %q want ml-jobs", job.Namespace)
	}
}

func TestToJobOwnerProjectAnnotations(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata: Metadata{
			Name: "x",
			Tags: map[string]string{"owner": "ml-team", "project": "phoenix"},
		},
		Spec: Spec{Image: "x", Size: "small"},
	}
	job, _ := ToJob(w)
	if job.Annotations[AnnotationOwner] != "ml-team" {
		t.Errorf("owner annotation: got %q", job.Annotations[AnnotationOwner])
	}
	if job.Annotations[AnnotationProject] != "phoenix" {
		t.Errorf("project annotation: got %q", job.Annotations[AnnotationProject])
	}
}

func TestToJobModelVolumeMountsAndEnv(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "flux-gen"},
		Spec: Spec{
			Image:       "x",
			Size:        "small",
			Backend:     "linode", // modelVolume now requires an explicit Linode backend
			ModelVolume: "kubagachi-flux",
			Env:         []EnvVar{{Name: "FOO", Value: "bar"}},
		},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	if got := job.Annotations[AnnotationModelVol]; got != "kubagachi-flux" {
		t.Errorf("model-volume annotation: got %q", got)
	}

	// Pod gets a hostPath volume at the burst's model-cache mount.
	vols := job.Spec.Template.Spec.Volumes
	if len(vols) != 1 || vols[0].HostPath == nil || vols[0].HostPath.Path != modelCacheHostPath {
		t.Fatalf("expected one hostPath volume at %s, got %+v", modelCacheHostPath, vols)
	}

	c := job.Spec.Template.Spec.Containers[0]
	// Read-only mount into the pod.
	var mount *corev1.VolumeMount
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].Name == "model-cache" {
			mount = &c.VolumeMounts[i]
		}
	}
	if mount == nil || mount.MountPath != modelCachePodPath || !mount.ReadOnly {
		t.Fatalf("expected ro model-cache mount at %s, got %+v", modelCachePodPath, c.VolumeMounts)
	}
	// HF_HOME points at the cache; user env is preserved.
	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	if env["HF_HOME"] != modelCachePodPath+"/hf" {
		t.Errorf("HF_HOME: got %q, want %q", env["HF_HOME"], modelCachePodPath+"/hf")
	}
	if env["HF_HUB_OFFLINE"] != "1" {
		t.Errorf("HF_HUB_OFFLINE: got %q, want 1", env["HF_HUB_OFFLINE"])
	}
	if env["FOO"] != "bar" {
		t.Errorf("user env FOO dropped: got %q", env["FOO"])
	}
}

func TestToJobNoModelVolumeByDefault(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "x"},
		Spec:       Spec{Image: "x", Size: "small"},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	if len(job.Spec.Template.Spec.Volumes) != 0 {
		t.Errorf("expected no volumes without ModelVolume, got %+v", job.Spec.Template.Spec.Volumes)
	}
	if _, ok := job.Annotations[AnnotationModelVol]; ok {
		t.Errorf("model-volume annotation set without ModelVolume")
	}
}

func TestToJobCacheAddsSignerInitContainerAndSharedVolume(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "cache-test"},
		Spec: Spec{
			Image: "x",
			Size:  "small",
			Storage: &Storage{Cache: []CacheSpec{{
				Name:       "weights",
				Source:     BucketRef{Bucket: "models", Prefix: "llama/", Endpoint: "https://r2.example", Region: "auto", CredentialsSecret: "r2-creds"},
				Target:     "/models/llama",
				SizeHintGB: 32,
			}}},
		},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	pod := job.Spec.Template.Spec
	if len(pod.InitContainers) != 1 {
		t.Fatalf("init containers: got %d, want 1", len(pod.InitContainers))
	}
	init := pod.InitContainers[0]
	if init.Image != cacheInitImage || !strings.Contains(init.Image, "@sha256:") || !strings.Contains(init.Command[2], "/storage/sign-urls") || !strings.Contains(init.Command[2], `--url "$url"`) {
		t.Fatalf("cache init does not use a pinned image and signer-backed HTTP download: %+v", init)
	}
	if len(pod.Volumes) != 1 || pod.Volumes[0].EmptyDir == nil {
		t.Fatalf("cache volume: got %+v, want one emptyDir", pod.Volumes)
	}
	if got := pod.Volumes[0].EmptyDir.SizeLimit; got == nil || got.Cmp(resource.MustParse("32Gi")) != 0 {
		t.Fatalf("cache volume size limit: got %v, want 32Gi", got)
	}
	if len(init.VolumeMounts) != 1 || init.VolumeMounts[0].MountPath != "/models/llama" || init.VolumeMounts[0].ReadOnly {
		t.Fatalf("init cache mount: %+v", init.VolumeMounts)
	}
	main := pod.Containers[0]
	if len(main.VolumeMounts) != 1 || main.VolumeMounts[0].Name != init.VolumeMounts[0].Name || main.VolumeMounts[0].MountPath != "/models/llama" || !main.VolumeMounts[0].ReadOnly {
		t.Fatalf("main cache mount: %+v", main.VolumeMounts)
	}
	env := map[string]corev1.EnvVar{}
	for _, e := range init.Env {
		env[e.Name] = e
	}
	if env["CACHE_SOURCE"].Value == "" || env["BURST_ID"].ValueFrom == nil || env["BOOTSTRAP_ENDPOINT"].ValueFrom == nil {
		t.Fatalf("cache init signer inputs: %+v", init.Env)
	}
}

func TestToJobArtifactStagesWritableOutput(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion, Kind: Kind,
		Metadata: Metadata{Name: "train", Namespace: "ml-team"},
		Spec: Spec{Image: "trainer", Size: "small", Storage: &Storage{Artifacts: []ArtifactSpec{{
			Name: "checkpoints", Target: "/outputs", MaxFiles: 25, MaxSizeGB: 8,
			To: BucketRef{Bucket: "runs", Prefix: "checkpoints/", Endpoint: "https://r2.example", Region: "auto", CredentialsSecret: "r2-creds"},
		}}}},
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	if got := job.Annotations[AnnotationArtifacts]; !strings.Contains(got, `"name":"checkpoints"`) || job.Spec.Template.Annotations[AnnotationArtifacts] != got {
		t.Fatalf("artifact annotation missing from Job and pod: %q", got)
	}
	pod := job.Spec.Template.Spec
	if len(pod.Volumes) != 1 || pod.Volumes[0].HostPath == nil || !strings.HasPrefix(pod.Volumes[0].HostPath.Path, artifactStagingRootForTest()) {
		t.Fatalf("artifact staging volume = %+v", pod.Volumes)
	}
	if got := pod.Containers[0].VolumeMounts; len(got) != 1 || got[0].MountPath != "/outputs" || got[0].ReadOnly {
		t.Fatalf("workload artifact mount = %+v", got)
	}
	if len(pod.InitContainers) != 1 || pod.InitContainers[0].Image != artifactPrepareImage || pod.InitContainers[0].SecurityContext == nil || pod.InitContainers[0].SecurityContext.ReadOnlyRootFilesystem == nil || !*pod.InitContainers[0].SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("artifact prepare container is not pinned and hardened: %+v", pod.InitContainers)
	}
}

func artifactStagingRootForTest() string { return "/var/lib/yscale/artifacts/" }

func TestSizeNamesIsCompleteAndStable(t *testing.T) {
	names := SizeNames()
	if len(names) != 6 {
		t.Errorf("expected 6 size presets, got %d", len(names))
	}
	for _, n := range names {
		if _, ok := LookupSize(n); !ok {
			t.Errorf("SizeNames returned %q but LookupSize doesn't recognize it", n)
		}
	}
}

func TestValidateStorage(t *testing.T) {
	// Helper for the common shape — image/size always set so we don't
	// trip earlier validation rules.
	wlWithStorage := func(s *Storage) *Workload {
		return &Workload{
			APIVersion: APIVersion, Kind: Kind,
			Metadata: Metadata{Name: "w"},
			Spec:     Spec{Image: "x", Size: "small", Storage: s},
		}
	}

	cases := []struct {
		name        string
		storage     *Storage
		backend     string
		modelVolume string
		wantErr     string
	}{
		{
			name: "single cache + persistent valid",
			storage: &Storage{
				Cache: []CacheSpec{{
					Name:   "weights",
					Source: BucketRef{Bucket: "b", CredentialsSecret: "s"},
					Target: "/m",
				}},
				Persistent: []PersistentSpec{{
					Name: "ckpt", SizeGB: 100, Target: "/w",
				}},
			},
		},
		{
			name: "explicit ephemeral cache valid",
			storage: &Storage{Cache: []CacheSpec{{
				Name: "weights", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"},
				Target: "/m", Retention: "ephemeral",
			}}},
		},
		{
			name: "durable cache retention rejected",
			storage: &Storage{Cache: []CacheSpec{{
				Name: "weights", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"},
				Target: "/m", Retention: "keep",
			}}},
			wantErr: "cache storage is pod-local and ephemeral",
		},
		{
			name: "valid cache ttl still rejected as unsupported",
			storage: &Storage{Cache: []CacheSpec{{
				Name: "weights", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"},
				Target: "/m", Retention: "ttl=24h",
			}}},
			wantErr: "cache storage is pod-local and ephemeral",
		},
		{
			name:        "model volume rejected on aws",
			backend:     backends.TypeAWS,
			modelVolume: "weights",
			wantErr:     "supported only by backend=linode",
		},
		{
			name:        "model volume rejected on flyio",
			backend:     backends.TypeFlyIO,
			modelVolume: "weights",
			wantErr:     "supported only by backend=linode",
		},
		{
			name: "duplicate cache name",
			storage: &Storage{Cache: []CacheSpec{
				{Name: "x", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"}, Target: "/a"},
				{Name: "x", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"}, Target: "/b"},
			}},
			wantErr: "duplicated",
		},
		{
			name: "missing credentialsSecret",
			storage: &Storage{Cache: []CacheSpec{
				{Name: "x", Source: BucketRef{Bucket: "b"}, Target: "/a"},
			}},
			wantErr: "credentialsSecret is required",
		},
		{
			// The agent reads this Secret only from the workload's own
			// namespace, so a cross-namespace reference can't resolve.
			name: "namespaced credentialsSecret",
			storage: &Storage{Cache: []CacheSpec{
				{Name: "x", Source: BucketRef{Bucket: "b", CredentialsSecret: "other-team/s"}, Target: "/a"},
			}},
			wantErr: "must be a bare Secret name",
		},
		{
			name: "namespaced credentialsSecret on snapshot target",
			storage: &Storage{Persistent: []PersistentSpec{
				{Name: "ckpt", SizeGB: 100, Target: "/w", Snapshot: &SnapshotSpec{
					To: BucketRef{Bucket: "b", CredentialsSecret: "other-team/s"},
				}},
			}},
			wantErr: "must be a bare Secret name",
		},
		{
			name: "valid persistent retentions",
			storage: &Storage{Persistent: []PersistentSpec{
				{Name: "a", SizeGB: 1, Target: "/a", Retention: "ttl=24h"},
				{Name: "b", SizeGB: 1, Target: "/b", Retention: "keep"},
				{Name: "c", SizeGB: 1, Target: "/c", Retention: "until=2026-12-31T00:00:00Z"},
				{Name: "d", SizeGB: 1, Target: "/d", Retention: "ephemeral"},
			}},
		},
		{
			name: "invalid retention",
			storage: &Storage{Cache: []CacheSpec{
				{Name: "x", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"}, Target: "/a", Retention: "forever"},
			}},
			wantErr: "unknown retention",
		},
		{
			name: "persistent missing sizeGB",
			storage: &Storage{Persistent: []PersistentSpec{
				{Name: "x", Target: "/w"},
			}},
			wantErr: "sizeGB must be > 0",
		},
		{
			name: "persistent relative target rejected",
			storage: &Storage{Persistent: []PersistentSpec{
				{Name: "x", SizeGB: 100, Target: "workspace"},
			}},
			wantErr: "absolute path",
		},
		{
			name: "persistent with snapshot validates To",
			storage: &Storage{Persistent: []PersistentSpec{
				{Name: "x", SizeGB: 100, Target: "/w", Snapshot: &SnapshotSpec{
					To: BucketRef{}, // missing bucket+creds
				}},
			}},
			wantErr: "bucket is required",
		},
		{
			name: "cache sizeHintGB over the max rejected",
			storage: &Storage{Cache: []CacheSpec{
				{Name: "x", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"}, Target: "/m", SizeHintGB: 1 << 40},
			}},
			wantErr: "sizeHintGB must be between",
		},
		{
			name:        "model volume rejected on auto backend",
			modelVolume: "weights",
			wantErr:     "supported only by backend=linode",
		},
		{
			name:        "model volume allowed on explicit linode",
			backend:     backends.TypeLinode,
			modelVolume: "weights",
		},
		{
			name: "cache relative target rejected",
			storage: &Storage{Cache: []CacheSpec{
				{Name: "x", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"}, Target: "models"},
			}},
			wantErr: "absolute path",
		},
		{
			name: "cache duplicate target rejected",
			storage: &Storage{Cache: []CacheSpec{
				{Name: "a", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"}, Target: "/m"},
				{Name: "b", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"}, Target: "/m"},
			}},
			wantErr: "collides",
		},
		{
			name: "artifact output valid",
			storage: &Storage{Artifacts: []ArtifactSpec{{
				Name: "results", Target: "/outputs", MaxFiles: 100, MaxSizeGB: 4,
				To: BucketRef{Bucket: "runs", CredentialsSecret: "r2-creds"},
			}}},
		},
		{
			name: "artifact overlaps cache mount",
			storage: &Storage{
				Cache:     []CacheSpec{{Name: "models", Source: BucketRef{Bucket: "b", CredentialsSecret: "s"}, Target: "/data"}},
				Artifacts: []ArtifactSpec{{Name: "results", Target: "/data/out", MaxFiles: 10, MaxSizeGB: 1, To: BucketRef{Bucket: "b", CredentialsSecret: "s"}}},
			},
			wantErr: "overlaps storage mount",
		},
		{
			name: "artifact traversal target rejected",
			storage: &Storage{Artifacts: []ArtifactSpec{{
				Name: "results", Target: "/outputs/../secret", MaxFiles: 10, MaxSizeGB: 1,
				To: BucketRef{Bucket: "b", CredentialsSecret: "s"},
			}}},
			wantErr: "canonical absolute directory",
		},
		{
			name: "artifact non-canonical target rejected",
			storage: &Storage{Artifacts: []ArtifactSpec{{
				Name: "results", Target: "/outputs//nested", MaxFiles: 10, MaxSizeGB: 1,
				To: BucketRef{Bucket: "b", CredentialsSecret: "s"},
			}}},
			wantErr: "canonical absolute directory",
		},
		{
			name: "artifact requires bounded files",
			storage: &Storage{Artifacts: []ArtifactSpec{{
				Name: "results", Target: "/outputs", MaxSizeGB: 1,
				To: BucketRef{Bucket: "b", CredentialsSecret: "s"},
			}}},
			wantErr: "maxFiles must be between",
		},
		{
			name: "artifact configuration is bounded",
			storage: &Storage{Artifacts: []ArtifactSpec{{
				Name: "results", Target: "/outputs", MaxFiles: 10, MaxSizeGB: 1,
				To: BucketRef{Bucket: "b", Prefix: strings.Repeat("x", maxArtifactConfigBytes), CredentialsSecret: "s"},
			}}},
			wantErr: "configuration limit",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wl := wlWithStorage(c.storage)
			wl.Spec.Backend = c.backend
			wl.Spec.ModelVolume = c.modelVolume
			err := Validate(wl)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("expected ok, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, c.wantErr)
			}
		})
	}
}

func TestBucketRefURI(t *testing.T) {
	cases := []struct {
		name string
		ref  BucketRef
		want string
	}{
		{"R2", BucketRef{Bucket: "b", Prefix: "p/", Endpoint: "https://abc.r2.cloudflarestorage.com"}, "https://abc.r2.cloudflarestorage.com/b/p/"},
		{"native S3", BucketRef{Bucket: "b", Prefix: "p/"}, "s3://b/p/"},
		{"no prefix", BucketRef{Bucket: "b"}, "s3://b/"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.ref.URI(); got != c.want {
				t.Errorf("URI = %q, want %q", got, c.want)
			}
		})
	}
}

// --- Example YAML validation ---

func TestExampleWorkloadsValidate(t *testing.T) {
	for _, name := range []string{"gpu-l4.yaml", "gpu-l4-cache-artifact.yaml", "node-only.yaml", "hello.yaml", "cpu-medium.yaml"} {
		t.Run(name, func(t *testing.T) {
			w := loadExampleWorkload(t, name)
			if err := Validate(w); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestExampleGPUWorkloadTranslates(t *testing.T) {
	w := loadExampleWorkload(t, "gpu-l4.yaml")
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	gpuLim := job.Spec.Template.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"]
	if gpuLim.Value() != 1 {
		t.Errorf("GPU limit = %v, want 1", gpuLim.Value())
	}
	if job.Spec.Template.Spec.NodeSelector["yscale.sh/burst-node"] != "true" {
		t.Error("missing burst-node nodeSelector")
	}
	if job.Annotations["yscale.sh/gpu-kind"] != "l4" {
		t.Errorf("gpu-kind annotation = %q, want %q", job.Annotations["yscale.sh/gpu-kind"], "l4")
	}
}

func TestExampleCacheArtifactWorkloadTranslates(t *testing.T) {
	w := loadExampleWorkload(t, "gpu-l4-cache-artifact.yaml")
	if w.Spec.Backend != backends.TypeAWS {
		t.Fatalf("backend = %q, want %q for the L4 example", w.Spec.Backend, backends.TypeAWS)
	}
	job, err := ToJob(w)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	if job.Namespace != "ml-team" {
		t.Fatalf("Job namespace = %q, want ml-team so the signer reads the colocated Secret", job.Namespace)
	}

	// GPU limit and selector.
	gpuLim := job.Spec.Template.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"]
	if gpuLim.Value() != 1 {
		t.Errorf("GPU limit = %v, want 1", gpuLim.Value())
	}
	if job.Spec.Template.Spec.NodeSelector["yscale.sh/burst-node"] != "true" {
		t.Error("missing burst-node nodeSelector")
	}
	if job.Annotations["yscale.sh/gpu-kind"] != "l4" {
		t.Errorf("gpu-kind annotation = %q, want l4", job.Annotations["yscale.sh/gpu-kind"])
	}

	// Budget and deadline annotations.
	if got := job.Annotations[AnnotationBudgetUSD]; got != "5.0000" {
		t.Errorf("budget annotation = %q, want 5.0000", got)
	}
	if got := job.Annotations[AnnotationDeadline]; got != "2h0m0s" {
		t.Errorf("deadline annotation = %q, want 2h0m0s", got)
	}

	pod := job.Spec.Template.Spec

	// Cache: one cache-sync init container with signer/downward-API inputs
	// and a read-only mount in the main container.
	var cacheInit *corev1.Container
	for i := range pod.InitContainers {
		if strings.HasPrefix(pod.InitContainers[i].Name, "cache-sync-") {
			cacheInit = &pod.InitContainers[i]
			break
		}
	}
	if cacheInit == nil {
		t.Fatal("no cache-sync init container found")
	}
	if cacheInit.Image != cacheInitImage {
		t.Errorf("cache init image = %q, want %q", cacheInit.Image, cacheInitImage)
	}
	if len(cacheInit.Command) != 3 || !strings.Contains(cacheInit.Command[2], `--url "$url"`) {
		t.Error("cache init command does not download signed URLs over HTTP")
	}
	if len(cacheInit.VolumeMounts) != 1 || cacheInit.VolumeMounts[0].MountPath != "/data/models" {
		t.Errorf("cache init mount = %+v, want /data/models", cacheInit.VolumeMounts)
	}

	// Signer inputs: CACHE_SOURCE, BURST_ID (downward API), BOOTSTRAP_ENDPOINT (downward API).
	cacheEnv := map[string]corev1.EnvVar{}
	for _, e := range cacheInit.Env {
		cacheEnv[e.Name] = e
	}
	var cacheSource map[string]string
	if err := json.Unmarshal([]byte(cacheEnv["CACHE_SOURCE"].Value), &cacheSource); err != nil {
		t.Fatalf("decode CACHE_SOURCE: %v", err)
	}
	wantSource := map[string]string{
		"bucket":             "acme-models",
		"prefix":             "llama-3/base/",
		"endpoint":           "https://0123456789abcdef.r2.cloudflarestorage.com",
		"region":             "auto",
		"credentials_secret": "r2-creds",
	}
	for key, want := range wantSource {
		if got := cacheSource[key]; got != want {
			t.Errorf("CACHE_SOURCE[%q] = %q, want %q", key, got, want)
		}
	}
	assertFieldPath := func(name, want string) {
		t.Helper()
		env := cacheEnv[name]
		if env.ValueFrom == nil || env.ValueFrom.FieldRef == nil {
			t.Errorf("cache init missing %s downward-API ref", name)
			return
		}
		if got := env.ValueFrom.FieldRef.FieldPath; got != want {
			t.Errorf("%s fieldPath = %q, want %q", name, got, want)
		}
	}
	assertFieldPath("BURST_ID", "metadata.annotations['"+AnnotationCacheBurstID+"']")
	assertFieldPath("BOOTSTRAP_ENDPOINT", "metadata.annotations['"+AnnotationCacheBootstrapEndpoint+"']")

	// Cache volume: emptyDir with size limit from sizeHintGB (16 GiB).
	var cacheVol *corev1.Volume
	for i := range pod.Volumes {
		if strings.HasPrefix(pod.Volumes[i].Name, "cache-") {
			cacheVol = &pod.Volumes[i]
			break
		}
	}
	if cacheVol == nil || cacheVol.EmptyDir == nil {
		t.Fatal("no cache emptyDir volume found")
	}
	if got := cacheVol.EmptyDir.SizeLimit; got == nil || got.Cmp(resource.MustParse("16Gi")) != 0 {
		t.Errorf("cache volume size limit = %v, want 16Gi", got)
	}

	// Main container: cache mounted read-only at /data/models.
	main := pod.Containers[0]
	var cacheMount *corev1.VolumeMount
	for i := range main.VolumeMounts {
		if main.VolumeMounts[i].MountPath == "/data/models" {
			cacheMount = &main.VolumeMounts[i]
			break
		}
	}
	if cacheMount == nil {
		t.Fatal("main container missing /data/models mount")
	}
	if !cacheMount.ReadOnly {
		t.Error("cache mount in main container should be read-only")
	}

	// Artifact: writable staging mount at /outputs/checkpoints.
	var artifactMount *corev1.VolumeMount
	for i := range main.VolumeMounts {
		if main.VolumeMounts[i].MountPath == "/outputs/checkpoints" {
			artifactMount = &main.VolumeMounts[i]
			break
		}
	}
	if artifactMount == nil {
		t.Fatal("main container missing /outputs/checkpoints mount")
	}
	if artifactMount.ReadOnly {
		t.Error("artifact mount should be writable")
	}

	// Artifact staging volume: hostPath under /var/lib/yscale/artifacts/.
	var artifactVol *corev1.Volume
	for i := range pod.Volumes {
		if strings.HasPrefix(pod.Volumes[i].Name, "artifact-") {
			artifactVol = &pod.Volumes[i]
			break
		}
	}
	if artifactVol == nil || artifactVol.HostPath == nil {
		t.Fatal("no artifact hostPath volume found")
	}
	if !strings.HasPrefix(artifactVol.HostPath.Path, "/var/lib/yscale/artifacts/") {
		t.Errorf("artifact hostPath = %q, want prefix /var/lib/yscale/artifacts/", artifactVol.HostPath.Path)
	}

	// Artifact prepare init container: pinned image, hardened.
	var prepareInit *corev1.Container
	for i := range pod.InitContainers {
		if pod.InitContainers[i].Name == "artifact-prepare" {
			prepareInit = &pod.InitContainers[i]
			break
		}
	}
	if prepareInit == nil {
		t.Fatal("no artifact-prepare init container found")
	}
	if prepareInit.Image != artifactPrepareImage {
		t.Errorf("artifact-prepare image = %q, want %q", prepareInit.Image, artifactPrepareImage)
	}
	if prepareInit.SecurityContext == nil || prepareInit.SecurityContext.ReadOnlyRootFilesystem == nil || !*prepareInit.SecurityContext.ReadOnlyRootFilesystem {
		t.Error("artifact-prepare init container should have readOnlyRootFilesystem=true")
	}

	// Artifact metadata annotation on both Job and pod template.
	artAnno := job.Annotations[AnnotationArtifacts]
	var artifacts []ArtifactSpec
	if err := json.Unmarshal([]byte(artAnno), &artifacts); err != nil {
		t.Fatalf("decode artifact annotation: %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("artifact annotation count = %d, want 1", len(artifacts))
	}
	artifact := artifacts[0]
	if artifact.Name != "checkpoints" || artifact.Target != "/outputs/checkpoints" || artifact.MaxFiles != 50 || artifact.MaxSizeGB != 10 {
		t.Errorf("artifact annotation = %+v, want bounded checkpoints output", artifact)
	}
	if artifact.To.Bucket != "acme-runs" ||
		artifact.To.Prefix != "finetune/checkpoints/" ||
		artifact.To.Endpoint != "https://0123456789abcdef.r2.cloudflarestorage.com" ||
		artifact.To.Region != "auto" ||
		artifact.To.CredentialsSecret != "r2-creds" {
		t.Errorf("artifact destination = %+v, want the example R2 destination", artifact.To)
	}
	if job.Spec.Template.Annotations[AnnotationArtifacts] != artAnno {
		t.Error("artifact annotation should be identical on Job and pod template")
	}
}

func TestExampleNodeOnlyWorkloadValidates(t *testing.T) {
	w := loadExampleWorkload(t, "node-only.yaml")
	if err := Validate(w); err != nil {
		t.Fatalf("Validate nodeOnly: %v", err)
	}
}

func loadExampleWorkload(t *testing.T, name string) *Workload {
	t.Helper()
	path := filepath.Join("..", "..", "examples", "workloads", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var w Workload
	if err := yaml.Unmarshal(data, &w); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return &w
}

func TestExampleNodeOnlySkipsImageRequirement(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "node-pool"},
		Spec: Spec{
			NodeOnly: true,
			Size:     "small",
		},
	}
	if err := Validate(w); err != nil {
		t.Fatalf("nodeOnly without image should be valid: %v", err)
	}
}

func TestExampleGPUWorkloadRejectsWithoutKindOrSKU(t *testing.T) {
	w := &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "bad-gpu"},
		Spec: Spec{
			Image: "nvidia/cuda:latest",
			GPU:   &GPURequest{Count: 1},
			Size:  "small",
		},
	}
	err := Validate(w)
	if err == nil {
		t.Fatal("GPU without kind or sku should fail validation")
	}
	if !strings.Contains(err.Error(), "kind or sku") {
		t.Errorf("error should mention kind or sku: %v", err)
	}
}
