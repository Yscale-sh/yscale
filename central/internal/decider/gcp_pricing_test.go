package decider

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/pricing"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/gcp"
	"github.com/yscale-sh/yscale/pkg/workload"
)

func gcpCPUPlanWorkload(region string) *workload.Workload {
	return &workload.Workload{
		APIVersion: workload.APIVersion,
		Kind:       workload.Kind,
		Metadata:   workload.Metadata{Name: "gcp-cpu-test"},
		Spec: workload.Spec{
			Image:    "busybox",
			Size:     "small",
			Backend:  backends.TypeGCP,
			Region:   region,
			Replicas: 1,
			Command:  []string{"true"},
		},
	}
}

func TestPlanGCPDefaultRegionPricesPositive(t *testing.T) {
	d, backend, _ := newRecordingPlanDecider(t, backends.TypeGCP)
	wl := gcpCPUPlanWorkload("")
	plan, err := d.Plan(context.Background(), wl, handlers.PlanOptions{})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if backend.createCalls != 1 {
		t.Fatalf("backend creates = %d, want 1", backend.createCalls)
	}
	if plan.HourlyUSD <= 0 {
		t.Errorf("plan HourlyUSD = %v, want positive rate (default-region us-central1 is in the priced list)", plan.HourlyUSD)
	}
	// Cross-check: the plan's SKU should be exactly the type the
	// backend's MapType picked for the workload's memory, with a rate
	// the catalog carries. The estimator and the backend's selector
	// must agree — the parity test covers this for every boundary, but
	// locking the shape here makes the failure mode read cleanly.
	resources := backends.ResourceRequirements{
		CPUMillis: 1000,
		MemoryMB:  2048,
		Region:    "",
	}
	estimateRate, estimateSKU := pricing.EstimateGCPCPU(resources.MemoryMB)
	backendSKU := gcp.MapType(resources)
	if plan.SKU != estimateSKU {
		t.Errorf("plan SKU = %q, estimator picked %q — decider-recorded SKU disagrees with catalog estimator", plan.SKU, estimateSKU)
	}
	if plan.SKU != backendSKU {
		t.Errorf("plan SKU = %q, backend MapType picked %q — recorded SKU disagrees with backend's actual launch selection", plan.SKU, backendSKU)
	}
	if plan.HourlyUSD != estimateRate {
		t.Errorf("plan HourlyUSD = %v, estimator rate = %v — recorded rate disagrees with catalog", plan.HourlyUSD, estimateRate)
	}
	if mt, ok := pricing.GCPMachine(plan.SKU); !ok || mt <= 0 || mt != plan.HourlyUSD {
		t.Errorf("GCPMachine(%q) = (%v, %v), want (%v, true)", plan.SKU, mt, ok, plan.HourlyUSD)
	}
}

func TestPlanGCPUnpricedRegionRejectsBeforeAllSideEffects(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeGCP)
	d.gcpRegion = "europe-west1"
	// Storage is in play so the test can also verify storage timestamp
	// isn't bumped — a priced-vs-unpriced region test that mutated
	// storage on rejection would still let a typed error look fine.
	store := state.New()
	lastUsed := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	store.PutPersistentVolume(&state.PersistentVolume{
		ID:         "pv-gcp-model",
		TenantID:   state.DevCustomerID,
		Type:       "persistent",
		Name:       "gcp-model",
		Backend:    backends.TypeGCP,
		DCRegion:   "europe-west1",
		State:      "active",
		LastUsedAt: lastUsed,
	})
	d.store = store
	wl := gcpCPUPlanWorkload("")
	wl.Spec.Storage = &workload.Storage{Persistent: []workload.PersistentSpec{{
		Name: "gcp-model", SizeGB: 20, Target: "/models",
	}}}

	_, err := d.Plan(context.Background(), wl, handlers.PlanOptions{CustomerID: state.DevCustomerID})
	if err == nil {
		t.Fatal("Plan succeeded, want fail-closed GCP regional pricing rejection")
	}
	for _, want := range []string{
		"has no verified E2 price",
		"europe-west1",
		"us-central1",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
	if backend.createCalls != 0 {
		t.Fatalf("backend creates = %d, want 0 (fail closed BEFORE CreateNode)", backend.createCalls)
	}
	if got := meshCalls.mint.Load(); got != 0 {
		t.Fatalf("mesh mints = %d, want 0", got)
	}
	if got := len(d.reservedSlots); got != 0 {
		t.Fatalf("reserved slots = %d, want 0 (no CIDR reservation on rejection)", got)
	}
	pv, err := store.GetPersistentVolume("pv-gcp-model")
	if err != nil {
		t.Fatalf("get persistent volume: %v", err)
	}
	if !pv.LastUsedAt.Equal(lastUsed) {
		t.Fatalf("volume LastUsedAt mutated from %v to %v", lastUsed, pv.LastUsedAt)
	}
}

func TestPlanGCPPricedRegionIsAccepted(t *testing.T) {
	for _, region := range []string{"us-central1", "us-east1", "us-west1"} {
		region := region
		t.Run(region, func(t *testing.T) {
			d, backend, _ := newRecordingPlanDecider(t, backends.TypeGCP)
			d.gcpRegion = region
			wl := gcpCPUPlanWorkload("")
			plan, err := d.Plan(context.Background(), wl, handlers.PlanOptions{})
			if err != nil {
				t.Fatalf("Plan for priced region %q: %v", region, err)
			}
			if backend.createCalls != 1 {
				t.Fatalf("backend creates = %d, want 1", backend.createCalls)
			}
			if plan.HourlyUSD <= 0 {
				t.Errorf("plan HourlyUSD = %v, want positive rate for priced region %q", plan.HourlyUSD, region)
			}
			if _, ok := pricing.GCPMachine(plan.SKU); !ok {
				t.Errorf("plan SKU %q is not in the GCP catalog", plan.SKU)
			}
		})
	}
}

func TestHourlyAndSKU_GCPUseCatalogEstimator(t *testing.T) {
	cases := []int64{512, 2048, 4096, 8192, 16384, 65536}
	for _, mb := range cases {
		resources := backends.ResourceRequirements{
			CPUMillis: 1000,
			MemoryMB:  mb,
			Region:    "us-central1",
		}
		estRate, estSKU := pricing.EstimateGCPCPU(mb)
		rate, sku := hourlyAndSKU(&workload.Workload{}, backends.TypeGCP, resources)
		if sku != estSKU {
			t.Errorf("memory=%d: hourlyAndSKU SKU = %q, estimator SKU = %q", mb, sku, estSKU)
		}
		if rate != estRate {
			t.Errorf("memory=%d: hourlyAndSKU rate = %v, estimator rate = %v", mb, rate, estRate)
		}
		if sku != gcp.MapType(resources) {
			t.Errorf("memory=%d: hourlyAndSKU SKU %q disagrees with backend MapType %q", mb, sku, gcp.MapType(resources))
		}
	}
}

func TestConfiguredGCPRegion(t *testing.T) {
	for _, tc := range []struct{ zone, region, want string }{
		{"", "", "us-central1"},
		{"us-east1-b", "", "us-east1"},
		{"us-east1-b", "us-west1", "us-west1"},
		{"invalid", "", ""},
	} {
		if got := configuredGCPRegion(tc.zone, tc.region); got != tc.want {
			t.Errorf("configuredGCPRegion(%q, %q) = %q, want %q", tc.zone, tc.region, got, tc.want)
		}
	}
}
