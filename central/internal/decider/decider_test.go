package decider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/central/internal/pricing"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/central/internal/tailscale"
	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/workload"
	corev1 "k8s.io/api/core/v1"
)

func TestPlanReturnsFabricProvisioningWhenInjected(t *testing.T) {
	d := New(Config{})
	d.SetFabricProvisioning(func(customerID string) bool { return customerID == "cust_onboarding" })

	_, err := d.Plan(context.Background(), &workload.Workload{Spec: workload.Spec{Image: "busybox", Size: "small"}}, handlers.PlanOptions{CustomerID: "cust_onboarding"})
	if !errors.Is(err, ErrFabricProvisioning) {
		t.Fatalf("Plan error = %v, want ErrFabricProvisioning", err)
	}

	d.SetFabricProvisioning(nil)
	_, err = d.Plan(context.Background(), &workload.Workload{Spec: workload.Spec{Image: "busybox", Size: "small"}}, handlers.PlanOptions{CustomerID: "cust_onboarding"})
	if err == nil || err.Error() != "no mesh provider configured" {
		t.Fatalf("nil provisioning lookup changed OSS behavior: %v", err)
	}

	d = New(Config{})
	d.WithBoxProvider(func(string, string, string) mesh.Provider { return nil })
	d.SetFabricProvisioning(func(customerID string) bool { return customerID == "cust_onboarding" })
	_, err = d.Plan(context.Background(), &workload.Workload{Spec: workload.Spec{Image: "busybox", Size: "small"}}, handlers.PlanOptions{CustomerID: "cust_onboarding"})
	if !errors.Is(err, ErrFabricProvisioning) {
		t.Fatalf("Plan with enterprise provider error = %v, want ErrFabricProvisioning", err)
	}
}

func TestRouteCustomBackendOverridesIntelligent(t *testing.T) {
	d := New(Config{})
	cases := []struct {
		name    string
		spec    workload.Spec
		want    string
		wantErr string
	}{
		{
			name: "auto + RTX GPU → linode",
			spec: workload.Spec{Image: "x", Size: "small", GPU: &workload.GPURequest{Kind: "rtx4000ada"}},
			want: "linode",
		},
		{
			name: "auto + bare/default GPU → linode (cheapest RTX)",
			spec: workload.Spec{Image: "x", Size: "small", GPU: &workload.GPURequest{}},
			want: "linode",
		},
		{
			name: "auto + datacenter GPU (h100) → aws (Linode can't serve it)",
			spec: workload.Spec{Image: "x", Size: "small", GPU: &workload.GPURequest{Kind: "h100"}},
			want: "aws",
		},
		{
			name: "auto + L4 GPU → aws",
			spec: workload.Spec{Image: "x", Size: "small", GPU: &workload.GPURequest{Kind: "l4"}},
			want: "aws",
		},
		{
			name: "explicit aws + GPU → aws (now supported)",
			spec: workload.Spec{Image: "x", Size: "small", Backend: "aws", GPU: &workload.GPURequest{Kind: "l4"}},
			want: "aws",
		},
		{
			name: "auto + no GPU → flyio",
			spec: workload.Spec{Image: "x", Size: "small"},
			want: "flyio",
		},
		{
			name: "explicit flyio + no GPU → flyio",
			spec: workload.Spec{Image: "x", Size: "small", Backend: "flyio"},
			want: "flyio",
		},
		{
			name: "explicit linode + GPU → linode",
			spec: workload.Spec{Image: "x", Size: "small", Backend: "linode", GPU: &workload.GPURequest{Kind: "rtx6000"}},
			want: "linode",
		},
		{
			name: "explicit aws + no GPU → aws",
			spec: workload.Spec{Image: "x", Size: "small", Backend: "aws"},
			want: "aws",
		},
		{
			name: "explicit azure + no GPU → azure",
			spec: workload.Spec{Image: "x", Size: "small", Backend: "azure"},
			want: "azure",
		},
		{
			name: "explicit gcp + no GPU → gcp",
			spec: workload.Spec{Image: "x", Size: "small", Backend: "gcp"},
			want: "gcp",
		},
		{
			name:    "explicit flyio + GPU → error",
			spec:    workload.Spec{Image: "x", Size: "small", Backend: "flyio", GPU: &workload.GPURequest{Kind: "rtx6000"}},
			wantErr: "does not support GPU",
		},
		{
			name:    "explicit azure + GPU → error (azure is CPU-only)",
			spec:    workload.Spec{Image: "x", Size: "small", Backend: "azure", GPU: &workload.GPURequest{Kind: "a100"}},
			wantErr: "does not support GPU",
		},
		{
			name:    "explicit gcp + GPU → error (gcp is CPU-only)",
			spec:    workload.Spec{Image: "x", Size: "small", Backend: "gcp", GPU: &workload.GPURequest{Kind: "l4"}},
			wantErr: "does not support GPU",
		},
		{
			name:    "unknown backend → error",
			spec:    workload.Spec{Image: "x", Size: "small", Backend: "digitalocean"},
			wantErr: "unknown backend",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := d.route(&workload.Workload{Spec: c.spec})
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("got err=%v, want substring %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != c.want {
				t.Errorf("route = %q, want %q", got, c.want)
			}
		})
	}
}

// TestResolveNetworkingTierAlwaysFull pins the central-side invariant that
// there is only one supported tier and every spec that reaches here — even
// one that somehow bypassed workload.Validate — resolves to it. A regression
// that let "lite" pass through would re-open a paid-side-effect path (mesh /
// PodCIDR / provider) for a tier the product no longer offers.
func TestResolveNetworkingTierAlwaysFull(t *testing.T) {
	tests := []struct {
		name       string
		networking *workload.NetworkingSpec
	}{
		{name: "no networking block"},
		{name: "empty tier", networking: &workload.NetworkingSpec{}},
		{name: "explicit full", networking: &workload.NetworkingSpec{Tier: "full"}},
		{name: "legacy lite still resolves to full", networking: &workload.NetworkingSpec{Tier: "lite"}},
		{name: "unknown resolves to full", networking: &workload.NetworkingSpec{Tier: "bogus"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wl := &workload.Workload{Spec: workload.Spec{Networking: tt.networking}}
			if got := resolveNetworkingTier(wl); got != workload.NetworkingTierFull {
				t.Fatalf("resolveNetworkingTier = %q, want %q", got, workload.NetworkingTierFull)
			}
		})
	}
}

func TestResourcesForGPUKindPassthrough(t *testing.T) {
	wl := &workload.Workload{
		Spec: workload.Spec{
			Image: "x",
			Size:  "medium",
			GPU:   &workload.GPURequest{Kind: "rtx6000", Count: 1, Reliability: workload.ReliabilityReliable},
		},
	}
	r, err := resourcesFor(wl)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if r.GPU == nil {
		t.Fatal("GPU is nil")
	}
	// The backend (Linode) resolves Kind against its own plan catalog;
	// the decider passes Kind through without a SKU list.
	if r.GPU.Kind != "rtx6000" {
		t.Errorf("Kind = %q, want rtx6000", r.GPU.Kind)
	}
	if len(r.GPU.SKUs) != 0 {
		t.Errorf("expected no pre-resolved SKUs for the Kind path; got %v", r.GPU.SKUs)
	}
	if r.GPU.Count != 1 {
		t.Errorf("Count = %d, want 1", r.GPU.Count)
	}
}

func TestResourcesForExplicitCPUMemoryMatchPodAndDriveLinodeTier(t *testing.T) {
	wl := &workload.Workload{
		Metadata: workload.Metadata{Name: "explicit-resources"},
		Spec: workload.Spec{
			Image: "busybox", Backend: "linode", Region: "us-sea", CPU: "6", Memory: "16Gi",
			GPU: &workload.GPURequest{Kind: "rtx4000ada", Count: 1},
		},
	}
	resources, err := resourcesFor(wl)
	if err != nil {
		t.Fatalf("resourcesFor: %v", err)
	}
	if resources.Region != "us-sea" || resources.CPUMillis != 6000 || resources.MemoryMB != 16384 {
		t.Fatalf("resources = %+v, want us-sea / 6000m / 16384MB", resources)
	}
	if resources.GPU == nil || resources.GPU.CPUMillis != resources.CPUMillis || resources.GPU.MemoryMB != resources.MemoryMB {
		t.Fatalf("GPU resources = %+v, want copied CPU/memory", resources.GPU)
	}
	job, err := workload.ToJob(wl)
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	podResources := job.Spec.Template.Spec.Containers[0].Resources.Requests
	podCPU := podResources[corev1.ResourceCPU]
	if got := podCPU.MilliValue(); got != resources.CPUMillis {
		t.Fatalf("pod CPU = %dm, resourcesFor = %dm", got, resources.CPUMillis)
	}
	podMemory := podResources[corev1.ResourceMemory]
	if got := podMemory.Value() / (1024 * 1024); got != resources.MemoryMB {
		t.Fatalf("pod memory = %dMB, resourcesFor = %dMB", got, resources.MemoryMB)
	}
	quote, err := resolveGPUPriceQuote(backends.TypeLinode, resources.GPU)
	if err != nil || quote.sku != "g2-gpu-rtx4000a1-m" || quote.upstreamHourly != 0.67 {
		t.Fatalf("Linode quote = %+v, %v; want medium tier at $0.67/hour", quote, err)
	}
	wl.Spec.GPU.MaxHourlyUSD = pricing.CustomerPrice(0.67)
	if err := enforceGPUPriceAdmission(wl, backends.TypeLinode, resources); err != nil {
		t.Fatalf("exact medium-tier cap rejected: %v", err)
	}
	wl.Spec.GPU.MaxHourlyUSD -= 0.001
	if err := enforceGPUPriceAdmission(wl, backends.TypeLinode, resources); err == nil {
		t.Fatal("below medium-tier cap unexpectedly admitted")
	}
}

func TestPlanThreadsWorkloadRegionToNodeSpec(t *testing.T) {
	d, backend, _ := newRecordingPlanDecider(t, backends.TypeLinode)
	wl := minimalPlanWorkload()
	wl.Spec.Backend = backends.TypeLinode
	wl.Spec.Region = "us-sea"
	if _, err := d.Plan(context.Background(), wl, handlers.PlanOptions{}); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if backend.lastSpec == nil || backend.lastSpec.Region != "us-sea" || backend.lastSpec.Resources.Region != "us-sea" {
		t.Fatalf("backend NodeSpec = %+v, want workload region us-sea", backend.lastSpec)
	}
}

func TestResourcesForReplicaScalesGPUCount(t *testing.T) {
	wl := &workload.Workload{
		Spec: workload.Spec{
			Image:    "x",
			Size:     "small",
			Replicas: 2,
			GPU:      &workload.GPURequest{Kind: "rtx6000", Count: 1},
		},
	}
	r, err := resourcesFor(wl)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if r.GPU.Count != 2 {
		t.Errorf("Count = %d, want 2 (1 GPU × 2 replicas)", r.GPU.Count)
	}
}

func TestResourcesForCarriesExplicitSKUToAdmission(t *testing.T) {
	wl := &workload.Workload{
		Spec: workload.Spec{
			Image: "x", Size: "small",
			GPU: &workload.GPURequest{Kind: "rtx6000", SKU: "g1-gpu-rtx6000-2", Count: 2},
		},
	}
	r, err := resourcesFor(wl)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(r.GPU.SKUs) != 1 || r.GPU.SKUs[0] != "g1-gpu-rtx6000-2" {
		t.Errorf("explicit SKU admission marker = %v, want requested SKU", r.GPU.SKUs)
	}
	if r.GPU.Count != 2 {
		t.Errorf("Count = %d, want 2", r.GPU.Count)
	}
}

func TestEstimateCostUSDUsesLinodeCatalog(t *testing.T) {
	r := backends.ResourceRequirements{
		CPUMillis: 1000, MemoryMB: 2048,
		GPU: &backends.GPUSpec{Kind: "rtx4000ada", Count: 1},
	}
	wl := &workload.Workload{Spec: workload.Spec{Image: "x"}}
	got := estimateCostUSD(wl, "linode", r)
	if got < 0.50 || got > 0.55 { // catalog says $0.52/hr upstream
		t.Errorf("estimate = %v, want ~$0.52/hr", got)
	}
}

// An AWS GPU burst must price by the resolved EC2 instance type, not fall
// through to the memory-based CPU estimate (which would record ~$0 and let
// an L4 burst run unmetered against its budget). The recorded SKU should be
// the instance type so the burst record is meaningful.
func TestHourlyAndSKU_AWSGPU(t *testing.T) {
	wl := &workload.Workload{Spec: workload.Spec{
		Image: "x", Size: "small", Backend: "aws",
		GPU: &workload.GPURequest{Kind: "l4", Count: 1},
	}}
	r, err := resourcesFor(wl)
	if err != nil {
		t.Fatalf("resourcesFor: %v", err)
	}
	rate, sku := hourlyAndSKU(wl, backends.TypeAWS, r)
	if sku != "g6.xlarge" {
		t.Errorf("sku = %q, want g6.xlarge (the resolved L4 instance type)", sku)
	}
	if rate <= 0 {
		t.Errorf("rate = %v, want a positive GPU rate, not the $0 CPU fallthrough", rate)
	}
}

// Multi-GPU AWS bursts must price the multi-GPU instance type, not Nx the
// 1-GPU rate — the SKU resolves to the larger instance whose price the
// catalog already carries.
func TestHourlyAndSKU_AWSGPUMultiGPU(t *testing.T) {
	wl := &workload.Workload{Spec: workload.Spec{
		Image: "x", Size: "small", Backend: "aws",
		GPU: &workload.GPURequest{Kind: "l4", Count: 4},
	}}
	r, err := resourcesFor(wl)
	if err != nil {
		t.Fatalf("resourcesFor: %v", err)
	}
	_, sku := hourlyAndSKU(wl, backends.TypeAWS, r)
	if sku != "g6.12xlarge" {
		t.Errorf("sku = %q, want g6.12xlarge (the 4-GPU L4 instance)", sku)
	}
}

func TestPlanLinodeGPUCapAllowsAffordableAutomaticShape(t *testing.T) {
	tests := []struct {
		name string
		cap  float64
	}{
		{name: "below cap", cap: 0.70},
		{name: "equal cap", cap: pricing.CustomerPrice(0.52)},
		{name: "zero cap remains unlimited", cap: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeLinode)
			wl := gpuPlanWorkload(backends.TypeLinode, "rtx4000ada", 1, 1, tt.cap)

			plan, err := d.Plan(context.Background(), wl, handlers.PlanOptions{})
			if err != nil {
				t.Fatalf("Plan: %v", err)
			}
			if backend.createCalls != 1 {
				t.Fatalf("backend creates = %d, want 1", backend.createCalls)
			}
			if got := meshCalls.mint.Load(); got != 1 {
				t.Fatalf("mesh mints = %d, want 1", got)
			}
			if plan.SKU != "g2-gpu-rtx4000a1-s" {
				t.Fatalf("plan SKU = %q, want exact Linode GPU plan", plan.SKU)
			}
			if plan.HourlyUSD != 0.52 {
				t.Fatalf("plan upstream hourly USD = %v, want 0.52", plan.HourlyUSD)
			}
		})
	}
}

func TestLinodeGPUQuoteUsesExactMultiGPUPlanPrices(t *testing.T) {
	tests := []struct {
		count              int
		wantSKU            string
		wantUpstreamHourly float64
		wantCustomerPerGPU float64
	}{
		{count: 2, wantSKU: "g2-gpu-rtx4000a2-s", wantUpstreamHourly: 1.05, wantCustomerPerGPU: 0.63},
		{count: 4, wantSKU: "g2-gpu-rtx4000a4-s", wantUpstreamHourly: 2.96, wantCustomerPerGPU: 0.888},
	}
	for _, tt := range tests {
		t.Run(tt.wantSKU, func(t *testing.T) {
			wl := gpuPlanWorkload(backends.TypeLinode, "rtx4000ada", tt.count, 1, tt.wantCustomerPerGPU)
			resources, err := resourcesFor(wl)
			if err != nil {
				t.Fatalf("resourcesFor: %v", err)
			}
			quote, err := resolveGPUPriceQuote(backends.TypeLinode, resources.GPU)
			if err != nil {
				t.Fatalf("resolveGPUPriceQuote: %v", err)
			}
			if quote.sku != tt.wantSKU || quote.upstreamHourly != tt.wantUpstreamHourly {
				t.Fatalf("quote = %+v, want SKU %q at $%.2f/hour", quote, tt.wantSKU, tt.wantUpstreamHourly)
			}
			customerPerGPU := pricing.CustomerPrice(quote.upstreamHourly) / float64(quote.effectiveCount)
			if math.Abs(customerPerGPU-tt.wantCustomerPerGPU) > 0.000001 {
				t.Fatalf("customer per-GPU = %.6f, want %.6f", customerPerGPU, tt.wantCustomerPerGPU)
			}
			if err := enforceGPUPriceAdmission(wl, backends.TypeLinode, resources); err != nil {
				t.Fatalf("equal exact-plan cap rejected: %v", err)
			}
			wl.Spec.GPU.MaxHourlyUSD = tt.wantCustomerPerGPU - 0.001
			if err := enforceGPUPriceAdmission(wl, backends.TypeLinode, resources); err == nil {
				t.Fatal("below exact-plan customer cap unexpectedly admitted")
			}
		})
	}
}

func TestPlanGPUCapRejectsBeforeAllSideEffects(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeLinode)
	store := state.New()
	lastUsed := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	store.PutPersistentVolume(&state.PersistentVolume{
		ID:         "pv-model",
		TenantID:   state.DevCustomerID,
		Type:       "persistent",
		Name:       "model",
		Backend:    backends.TypeLinode,
		DCRegion:   "us-ord",
		State:      "active",
		LastUsedAt: lastUsed,
	})
	d.store = store
	wl := gpuPlanWorkload(backends.TypeLinode, "rtx4000ada", 1, 2, 0.55)
	wl.Spec.Storage = &workload.Storage{Persistent: []workload.PersistentSpec{{
		Name: "model", SizeGB: 20, Target: "/models",
	}}}

	_, err := d.Plan(context.Background(), wl, handlers.PlanOptions{CustomerID: state.DevCustomerID})
	if err == nil {
		t.Fatal("Plan succeeded, want customer-price cap rejection")
	}
	for _, want := range []string{"g2-gpu-rtx4000a2-s", "$0.6300/GPU/hour", "2 effective GPU(s)", "cap $0.5500"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
	if backend.createCalls != 0 {
		t.Fatalf("backend creates = %d, want 0", backend.createCalls)
	}
	if got := meshCalls.mint.Load(); got != 0 {
		t.Fatalf("mesh mints = %d, want 0", got)
	}
	if got := len(d.reservedSlots); got != 0 {
		t.Fatalf("PodCIDR reservations = %d, want 0", got)
	}
	pv, err := store.GetPersistentVolume("pv-model")
	if err != nil {
		t.Fatalf("get persistent volume: %v", err)
	}
	if !pv.LastUsedAt.Equal(lastUsed) {
		t.Fatalf("volume LastUsedAt mutated from %v to %v", lastUsed, pv.LastUsedAt)
	}
}

func TestAWSMultiGPUQuoteComputesNonlinearPerGPUEstimate(t *testing.T) {
	wl := gpuPlanWorkload(backends.TypeAWS, "l4", 4, 1, 0)
	resources, err := resourcesFor(wl)
	if err != nil {
		t.Fatalf("resourcesFor: %v", err)
	}
	quote, err := resolveGPUPriceQuote(backends.TypeAWS, resources.GPU)
	if err != nil {
		t.Fatalf("resolveGPUPriceQuote: %v", err)
	}
	if quote.sku != "g6.12xlarge" || quote.effectiveCount != 4 {
		t.Fatalf("quote = %+v, want g6.12xlarge / 4 GPUs", quote)
	}
	got := pricing.CustomerPrice(quote.upstreamHourly) / float64(quote.effectiveCount)
	if math.Abs(got-1.38039) > 0.000001 {
		t.Fatalf("customer per-GPU estimate = %.6f, want 1.380390", got)
	}
}

func TestPlanCappedAWSRejectsBeforeAllSideEffectsWithoutRegionalPricing(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeAWS)
	store := state.New()
	lastUsed := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	store.PutPersistentVolume(&state.PersistentVolume{
		ID:         "pv-aws-model",
		TenantID:   state.DevCustomerID,
		Type:       "persistent",
		Name:       "aws-model",
		Backend:    backends.TypeAWS,
		DCRegion:   "us-west-2",
		State:      "active",
		LastUsedAt: lastUsed,
	})
	d.store = store
	wl := gpuPlanWorkload(backends.TypeAWS, "l4", 4, 1, 100)
	wl.Spec.Storage = &workload.Storage{Persistent: []workload.PersistentSpec{{
		Name: "aws-model", SizeGB: 20, Target: "/models",
	}}}

	_, err := d.Plan(context.Background(), wl, handlers.PlanOptions{CustomerID: state.DevCustomerID})
	if err == nil {
		t.Fatal("Plan succeeded, want fail-closed AWS regional-pricing rejection")
	}
	for _, want := range []string{"g6.12xlarge", "4 effective GPU(s)", "cap $100.0000", "authoritative region-aware AWS GPU pricing is unavailable"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
	if backend.createCalls != 0 || meshCalls.mint.Load() != 0 || len(d.reservedSlots) != 0 {
		t.Fatalf("side effects on rejection: creates=%d mints=%d CIDRs=%d",
			backend.createCalls, meshCalls.mint.Load(), len(d.reservedSlots))
	}
	pv, err := store.GetPersistentVolume("pv-aws-model")
	if err != nil {
		t.Fatalf("get persistent volume: %v", err)
	}
	if !pv.LastUsedAt.Equal(lastUsed) {
		t.Fatalf("volume LastUsedAt mutated from %v to %v", lastUsed, pv.LastUsedAt)
	}
}

func TestPlanUncappedAWSGPUBehaviorPreserved(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeAWS)
	wl := gpuPlanWorkload(backends.TypeAWS, "l4", 1, 1, 0)
	plan, err := d.Plan(context.Background(), wl, handlers.PlanOptions{})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if backend.createCalls != 1 || meshCalls.mint.Load() != 1 {
		t.Fatalf("uncapped AWS side effects: creates=%d mints=%d, want 1/1", backend.createCalls, meshCalls.mint.Load())
	}
	if plan.SKU != "g6.xlarge" || plan.HourlyUSD != 0.8048 {
		t.Fatalf("uncapped AWS quote = %q / %v, want g6.xlarge / 0.8048", plan.SKU, plan.HourlyUSD)
	}
}

func TestResourcesForGPUCountMultiplicationBoundary(t *testing.T) {
	safe := gpuPlanWorkload(backends.TypeLinode, "rtx4000ada", math.MaxInt/2, 2, 0)
	resources, err := resourcesFor(safe)
	if err != nil {
		t.Fatalf("largest safe multiplication: %v", err)
	}
	if resources.GPU.Count != math.MaxInt-1 {
		t.Fatalf("effective GPU count = %d, want %d", resources.GPU.Count, math.MaxInt-1)
	}

	overflow := gpuPlanWorkload(backends.TypeLinode, "rtx4000ada", math.MaxInt/2+1, 2, 0)
	if _, err := resourcesFor(overflow); err == nil || !strings.Contains(err.Error(), "overflows int") {
		t.Fatalf("overflow resourcesFor error = %v, want checked overflow", err)
	}
}

func TestPlanPinnedGPUAndUnsupportedCappedShapeFailBeforeSideEffects(t *testing.T) {
	tests := []struct {
		name     string
		gpu      *workload.GPURequest
		replicas int32
		wantErr  string
	}{
		{
			name:    "pinned SKU is not silently substituted",
			gpu:     &workload.GPURequest{Kind: "rtx4000ada", Count: 1, SKU: "unknown-gpu-plan", MaxHourlyUSD: 100},
			wantErr: "unknown-gpu-plan",
		},
		{
			name:    "zero cap does not permit an unlaunchable pinned SKU",
			gpu:     &workload.GPURequest{Kind: "rtx4000ada", Count: 1, SKU: "unknown-gpu-plan"},
			wantErr: "unknown-gpu-plan",
		},
		{
			name:    "unsupported automatic shape",
			gpu:     &workload.GPURequest{Kind: "not-a-linode-gpu", Count: 3, MaxHourlyUSD: 100},
			wantErr: "unknown gpu kind",
		},
		{
			name:     "effective GPU count overflow",
			gpu:      &workload.GPURequest{Kind: "rtx4000ada", Count: math.MaxInt/2 + 1, MaxHourlyUSD: 100},
			replicas: 2,
			wantErr:  "overflows int",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeLinode)
			replicas := tt.replicas
			if replicas == 0 {
				replicas = 1
			}
			wl := gpuPlanWorkload(backends.TypeLinode, tt.gpu.Kind, tt.gpu.Count, replicas, tt.gpu.MaxHourlyUSD)
			wl.Spec.GPU.SKU = tt.gpu.SKU

			_, err := d.Plan(context.Background(), wl, handlers.PlanOptions{})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Plan error = %v, want substring %q", err, tt.wantErr)
			}
			if tt.gpu.SKU != "" && !strings.Contains(err.Error(), "does not honor explicitly pinned GPU SKUs exactly") {
				t.Fatalf("pinned SKU error = %v, want exact-launch rejection", err)
			}
			if backend.createCalls != 0 || meshCalls.mint.Load() != 0 || len(d.reservedSlots) != 0 {
				t.Fatalf("side effects on rejection: creates=%d mints=%d CIDRs=%d",
					backend.createCalls, meshCalls.mint.Load(), len(d.reservedSlots))
			}
		})
	}
}

func TestPlanWithoutMeshProviderReturnsGenericError(t *testing.T) {
	d := New(Config{})
	_, err := d.Plan(context.Background(), minimalPlanWorkload(), handlers.PlanOptions{})
	if err == nil || err.Error() != "no mesh provider configured" {
		t.Fatalf("Plan error = %v, want %q", err, "no mesh provider configured")
	}
}

func TestPlanCPUWorkloadUnaffectedByGPUAdmission(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeFlyIO)
	plan, err := d.Plan(context.Background(), minimalPlanWorkload(), handlers.PlanOptions{})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if backend.createCalls != 1 || meshCalls.mint.Load() != 1 {
		t.Fatalf("CPU plan side effects: creates=%d mints=%d, want 1/1", backend.createCalls, meshCalls.mint.Load())
	}
	if plan.Backend != backends.TypeFlyIO {
		t.Fatalf("CPU plan backend = %q, want flyio", plan.Backend)
	}
}

func TestQuoteIsSideEffectFreeAndPlanQuotedRefusesShapeDrift(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeFlyIO)
	wl := minimalPlanWorkload()
	quote, err := d.Quote(context.Background(), wl, handlers.PlanOptions{})
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if quote.Price.MaximumChargeMicroUSD <= 0 || quote.Price.MaximumDurationSeconds <= 0 {
		t.Fatalf("quote is unbounded: %+v", quote.Price)
	}
	if backend.createCalls != 0 || meshCalls.mint.Load() != 0 || len(d.reservedSlots) != 0 {
		t.Fatalf("quote side effects: creates=%d mints=%d CIDRs=%d", backend.createCalls, meshCalls.mint.Load(), len(d.reservedSlots))
	}
	wl.Spec.Size = "large"
	if _, err := d.PlanQuoted(context.Background(), wl, handlers.PlanOptions{}, quote); err == nil || !strings.Contains(err.Error(), "quote no longer matches") {
		t.Fatalf("shape drift error = %v", err)
	}
	if backend.createCalls != 0 || meshCalls.mint.Load() != 0 || len(d.reservedSlots) != 0 {
		t.Fatalf("shape drift reached side effects: creates=%d mints=%d CIDRs=%d", backend.createCalls, meshCalls.mint.Load(), len(d.reservedSlots))
	}
}

type recordingBackend struct {
	name        string
	createID    string
	createCalls int
	lastSpec    *backends.NodeSpec
	createErr   error
	deleteCalls int
	deleteID    string

	cleanupCalls     int
	cleanupTracked   map[string]bool
	cleanupDestroyed int
	cleanupErr       error
}

func (b *recordingBackend) CreateNode(_ context.Context, spec *backends.NodeSpec) (string, error) {
	b.createCalls++
	cp := *spec
	b.lastSpec = &cp
	if b.createErr != nil {
		return "", b.createErr
	}
	if b.createID != "" {
		return b.createID, nil
	}
	return "backend-node-1", nil
}

func (b *recordingBackend) StartNode(context.Context, string) error { return nil }
func (b *recordingBackend) StopNode(context.Context, string) error  { return nil }

func (b *recordingBackend) DeleteNode(_ context.Context, backendID string) error {
	b.deleteCalls++
	b.deleteID = backendID
	return nil
}

func (b *recordingBackend) GetNodeStatus(context.Context, string) (*backends.NodeStatus, error) {
	return &backends.NodeStatus{Phase: backends.NodeRunning}, nil
}

func (b *recordingBackend) ListPooledNodes(context.Context) ([]backends.PooledNode, error) {
	return nil, nil
}

func (b *recordingBackend) CleanupOrphans(_ context.Context, tracked map[string]bool) (int, error) {
	b.cleanupCalls++
	b.cleanupTracked = tracked
	return b.cleanupDestroyed, b.cleanupErr
}

func (b *recordingBackend) Name() string { return b.name }

func minimalPlanWorkload() *workload.Workload {
	return &workload.Workload{
		APIVersion: workload.APIVersion,
		Kind:       workload.Kind,
		Metadata:   workload.Metadata{Name: "wl-test"},
		Spec: workload.Spec{
			Image:   "busybox",
			Size:    "small",
			Backend: backends.TypeFlyIO,
			Command: []string{"true"},
		},
	}
}

func gpuPlanWorkload(backend, kind string, count int, replicas int32, capUSD float64) *workload.Workload {
	return &workload.Workload{
		APIVersion: workload.APIVersion,
		Kind:       workload.Kind,
		Metadata:   workload.Metadata{Name: "gpu-plan-test"},
		Spec: workload.Spec{
			Image:    "busybox",
			Size:     "small",
			Backend:  backend,
			Replicas: replicas,
			GPU: &workload.GPURequest{
				Kind: kind, Count: count, MaxHourlyUSD: capUSD,
			},
			Command: []string{"true"},
		},
	}
}

// SweepOrphans runs CleanupOrphans on every CONFIGURED backend with the tracked
// set, sums destroyed counts, skips nil backends, and joins per-backend errors
// so one failure doesn't strand the rest.
func TestSweepOrphansRunsConfiguredBackendsWithTrackedSet(t *testing.T) {
	d := New(Config{})
	fly := &recordingBackend{name: backends.TypeFlyIO, cleanupDestroyed: 1}
	lin := &recordingBackend{name: backends.TypeLinode, cleanupDestroyed: 2}
	d.fly = fly
	d.linode = lin
	// d.aws stays nil — must be skipped, not panic.

	tracked := map[string]bool{"node-live": true}
	destroyed, err := d.SweepOrphans(context.Background(), tracked)
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	if destroyed != 3 {
		t.Fatalf("destroyed = %d, want 3 (1 fly + 2 linode)", destroyed)
	}
	if fly.cleanupCalls != 1 || lin.cleanupCalls != 1 {
		t.Fatalf("cleanup calls: fly=%d linode=%d, want 1 each", fly.cleanupCalls, lin.cleanupCalls)
	}
	if !lin.cleanupTracked["node-live"] {
		t.Fatal("tracked set not passed through to backend CleanupOrphans")
	}
}

func TestSweepOrphansJoinsErrorsButRunsAll(t *testing.T) {
	d := New(Config{})
	fly := &recordingBackend{name: backends.TypeFlyIO, cleanupErr: errors.New("fly api down")}
	lin := &recordingBackend{name: backends.TypeLinode, cleanupDestroyed: 5}
	d.fly = fly
	d.linode = lin

	destroyed, err := d.SweepOrphans(context.Background(), nil)
	if err == nil {
		t.Fatal("expected a joined error from the failing backend")
	}
	// The healthy backend still ran and its deletes still counted.
	if destroyed != 5 || lin.cleanupCalls != 1 {
		t.Fatalf("destroyed=%d linodeCalls=%d, want 5 and 1 (one failure must not strand the rest)", destroyed, lin.cleanupCalls)
	}
}

func newRecordingPlanDecider(t *testing.T, backendName string) (*Decider, *recordingBackend, *providerCalls) {
	t.Helper()
	ts, meshCalls, closeTS := newTestTailscale(t, "")
	t.Cleanup(closeTS)
	backend := &recordingBackend{name: backendName, createID: "node-1"}
	d := New(Config{})
	d.ts = ts
	switch backendName {
	case backends.TypeFlyIO:
		d.fly = backend
	case backends.TypeLinode:
		d.linode = backend
	case backends.TypeAWS:
		d.aws = backend
	case backends.TypeGCP:
		d.gcp = backend
		d.gcpRegion = "us-central1"
	case backends.TypeAzure:
		d.azure = backend
	default:
		t.Fatalf("unsupported recording backend %q", backendName)
	}
	return d, backend, meshCalls
}

func backendLoginServer(b *recordingBackend) string {
	if b.lastSpec == nil {
		return "<nil spec>"
	}
	return b.lastSpec.LoginServer
}

func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

type providerCalls struct {
	oauth   atomic.Int32
	mint    atomic.Int32
	list    atomic.Int32
	delete  atomic.Int32
	users   atomic.Int32
	preauth atomic.Int32
}

func newTestTailscale(t *testing.T, hostname string) (*tailscale.Client, *providerCalls, func()) {
	t.Helper()
	calls := &providerCalls{}
	var deleted atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/oauth/token":
			calls.oauth.Add(1)
			writeJSONTest(t, w, map[string]any{"access_token": "test-token", "expires_in": 3600})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/tailnet/tailnet/keys":
			calls.mint.Add(1)
			writeJSONTest(t, w, map[string]string{"key": "tskey-auth-test"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/tailnet/tailnet/devices":
			calls.list.Add(1)
			devices := []map[string]string{}
			if hostname != "" && !deleted.Load() {
				devices = append(devices, map[string]string{"id": "dev-1", "hostname": hostname})
			}
			writeJSONTest(t, w, map[string]any{"devices": devices})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v2/device/dev-1":
			calls.delete.Add(1)
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	client, err := tailscale.New(tailscale.Config{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Tailnet:      "tailnet",
		APIBase:      srv.URL,
	})
	if err != nil {
		srv.Close()
		t.Fatalf("tailscale client: %v", err)
	}
	return client, calls, srv.Close
}

func writeJSONTest(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

type mockMeshProvider struct {
	findFunc   func(hostname string) (string, error)
	deleteFunc func(deviceID string) error
}

func TestCleanupMeshRejectsUnknownProvider(t *testing.T) {
	d := New(Config{})
	err := d.CleanupMesh(context.Background(), &state.Burst{MeshProvider: "unknown"})
	if err == nil || !strings.Contains(err.Error(), "unknown burst mesh provider") {
		t.Fatalf("CleanupMesh error = %v, want unknown-provider error", err)
	}
}

func TestCleanupMeshRequiresProviderForRecordedHostname(t *testing.T) {
	for _, tc := range []struct {
		name         string
		meshProvider string
	}{
		{name: "legacy", meshProvider: ""},
		{name: "tailscale", meshProvider: "tailscale"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := New(Config{})
			err := d.CleanupMesh(context.Background(), &state.Burst{
				CustomerID:   "cust_test",
				MeshProvider: tc.meshProvider,
				TSHostname:   "yscale-burst",
			})
			if !errors.Is(err, errMeshProviderUnavailable) {
				t.Fatalf("CleanupMesh error = %v, want unavailable-provider error", err)
			}
		})
	}
}

func TestTeardownWithoutResolvedMeshProviderPreservesLegacyBestEffort(t *testing.T) {
	d := New(Config{})
	err := d.Teardown(context.Background(), &state.Burst{
		Backend:    backends.TypeFlyIO,
		BackendID:  "fly-node-1",
		TSHostname: "yscale-burst",
	})
	if err == nil || !strings.Contains(err.Error(), "flyio backend not configured") {
		t.Fatalf("Teardown error = %v, want provider deletion error", err)
	}
	if errors.Is(err, errMeshProviderUnavailable) {
		t.Fatalf("legacy Teardown exposed mesh-provider sentinel: %v", err)
	}
}

func (m *mockMeshProvider) MintAuthKey(context.Context, []string, time.Duration) (string, error) {
	return "", nil
}
func (m *mockMeshProvider) MintAuthKeyEphemeral(context.Context, []string, time.Duration, bool) (string, error) {
	return "", nil
}
func (m *mockMeshProvider) FindDeviceByHostname(_ context.Context, hostname string) (string, error) {
	return m.findFunc(hostname)
}
func (m *mockMeshProvider) DeleteDevice(_ context.Context, deviceID string) error {
	return m.deleteFunc(deviceID)
}
func (m *mockMeshProvider) LoginServer() string { return "" }

func newDeciderWithMockMesh(t *testing.T, prov mesh.Provider) *Decider {
	t.Helper()
	store := state.New()
	if err := store.SetCustomerMesh("cust_test", &state.MeshEndpoint{
		Provider:    "box",
		LoginServer: "http://127.0.0.1:1",
	}); err != nil {
		t.Fatalf("set customer mesh: %v", err)
	}
	d := New(Config{})
	d.store = store
	d.boxProvider = func(string, string, string) mesh.Provider { return prov }
	return d
}

func TestCleanupMeshVerificationFailsPresent(t *testing.T) {
	d := newDeciderWithMockMesh(t, &mockMeshProvider{
		findFunc:   func(string) (string, error) { return "dev-1", nil },
		deleteFunc: func(string) error { return nil },
	})

	err := d.CleanupMesh(context.Background(), &state.Burst{
		CustomerID:      "cust_test",
		MeshProvider:    "box",
		MeshLoginServer: "http://127.0.0.1:1",
		TSHostname:      "yscale-burst",
	})
	if err == nil || !strings.Contains(err.Error(), "still present after delete") {
		t.Fatalf("expected still present error, got %v", err)
	}
}

func TestCleanupMeshVerificationErrors(t *testing.T) {
	calls := 0
	d := newDeciderWithMockMesh(t, &mockMeshProvider{
		findFunc: func(string) (string, error) {
			calls++
			if calls == 1 {
				return "dev-1", nil
			}
			return "", errors.New("lookup failed")
		},
		deleteFunc: func(string) error { return nil },
	})
	err := d.CleanupMesh(context.Background(), &state.Burst{
		CustomerID:      "cust_test",
		MeshProvider:    "box",
		MeshLoginServer: "http://127.0.0.1:1",
		TSHostname:      "yscale-burst",
	})
	if err == nil || !strings.Contains(err.Error(), "verify mesh device yscale-burst absence: lookup failed") {
		t.Fatalf("expected verify error, got %v", err)
	}
}

func TestCleanupMeshInitiallyAbsent(t *testing.T) {
	d := newDeciderWithMockMesh(t, &mockMeshProvider{
		findFunc:   func(string) (string, error) { return "", nil },
		deleteFunc: func(string) error { return errors.New("should not be called") },
	})
	err := d.CleanupMesh(context.Background(), &state.Burst{
		CustomerID:      "cust_test",
		MeshProvider:    "box",
		MeshLoginServer: "http://127.0.0.1:1",
		TSHostname:      "yscale-burst",
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
