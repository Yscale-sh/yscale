// yscale:proprietary

// Box-path tests: the self-hosted coordination box (Headscale) plan/teardown
// behavior behind the mesh.BoxProviderFunc seam. This file is stripped from
// the public OSS release along with the rest of the box code
// (scripts/oss-exclude.txt); the OSS decider has no box provider wired, so
// these paths are unreachable there.
package decider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/factory/client"
	"github.com/yscale-sh/yscale/pkg/backends"
)

// headscaleBoxProvider wires the self-hosted-box seam to the concrete Headscale
// client for the box-path tests. (Enterprise test helper; this test file is
// stripped from the OSS build along with the rest of the box code.)
func headscaleBoxProvider(loginServer, apiKey, user string) mesh.Provider {
	return client.NewHeadscale(loginServer, apiKey, user)
}

func TestTeardownHeadscaleStampClearedMeshSkipsSaaSDeviceDelete(t *testing.T) {
	store := state.New()
	if err := store.SetCustomerMesh("cust_test", nil); err != nil {
		t.Fatalf("clear mesh: %v", err)
	}
	ts, tsCalls, closeTS := newTestTailscale(t, "yscale-burst")
	defer closeTS()
	logs := captureSlog(t)
	d := New(Config{})
	d.store = store
	d.boxProvider = headscaleBoxProvider
	d.ts = ts

	err := d.Teardown(context.Background(), &state.Burst{
		ID:              "burst_headscale",
		CustomerID:      "cust_test",
		Backend:         backends.TypeFlyIO,
		BackendID:       "fly-node-1",
		TSHostname:      "yscale-burst",
		MeshProvider:    "box",
		MeshLoginServer: "http://127.0.0.1:1",
	})
	if err == nil || !strings.Contains(err.Error(), "flyio backend not configured") {
		t.Fatalf("Teardown err = %v, want backend delete attempt error", err)
	}
	if got := tsCalls.list.Load(); got != 0 {
		t.Fatalf("SaaS FindDeviceByHostname calls = %d, want 0", got)
	}
	if got := tsCalls.delete.Load(); got != 0 {
		t.Fatalf("SaaS DeleteDevice calls = %d, want 0", got)
	}
	if !strings.Contains(logs.String(), "legacy teardown keeps its best-effort behavior") {
		t.Fatalf("best-effort warning was not logged; logs:\n%s", logs.String())
	}
}

func TestTeardownTailscaleStampUsesSaaSWhenCustomerNowHeadscale(t *testing.T) {
	store := state.New()
	hsURL, hsCalls, closeHS := newHeadscaleAPIServer(t, headscaleServerOptions{DeviceHostname: "yscale-burst"})
	defer closeHS()
	if err := store.SetCustomerMesh("cust_test", &state.MeshEndpoint{
		Provider:    "headscale",
		LoginServer: hsURL,
		APIKey:      "hs-key",
		User:        "tenant-acme",
	}); err != nil {
		t.Fatalf("set mesh: %v", err)
	}
	ts, tsCalls, closeTS := newTestTailscale(t, "yscale-burst")
	defer closeTS()

	d := New(Config{})
	d.store = store
	d.boxProvider = headscaleBoxProvider
	d.ts = ts

	err := d.Teardown(context.Background(), &state.Burst{
		ID:           "burst_tailscale",
		CustomerID:   "cust_test",
		Backend:      backends.TypeFlyIO,
		BackendID:    "fly-node-1",
		TSHostname:   "yscale-burst",
		MeshProvider: "tailscale",
	})
	if err == nil || !strings.Contains(err.Error(), "flyio backend not configured") {
		t.Fatalf("Teardown err = %v, want backend delete attempt error", err)
	}
	if got := tsCalls.list.Load(); got != 2 {
		t.Fatalf("SaaS FindDeviceByHostname calls = %d, want 2", got)
	}
	if got := tsCalls.delete.Load(); got != 1 {
		t.Fatalf("SaaS DeleteDevice calls = %d, want 1", got)
	}
	if got := hsCalls.list.Load(); got != 0 {
		t.Fatalf("Headscale FindDeviceByHostname calls = %d, want 0", got)
	}
	if got := hsCalls.delete.Load(); got != 0 {
		t.Fatalf("Headscale DeleteDevice calls = %d, want 0", got)
	}
}

func TestTeardownLegacyBurstKeepsCurrentCustomerMeshBehavior(t *testing.T) {
	store := state.New()
	hsURL, hsCalls, closeHS := newHeadscaleAPIServer(t, headscaleServerOptions{DeviceHostname: "yscale-burst"})
	defer closeHS()
	if err := store.SetCustomerMesh("cust_test", &state.MeshEndpoint{
		Provider:    "headscale",
		LoginServer: hsURL,
		APIKey:      "hs-key",
		User:        "tenant-acme",
	}); err != nil {
		t.Fatalf("set mesh: %v", err)
	}
	ts, tsCalls, closeTS := newTestTailscale(t, "yscale-burst")
	defer closeTS()

	d := New(Config{})
	d.store = store
	d.boxProvider = headscaleBoxProvider
	d.ts = ts

	err := d.Teardown(context.Background(), &state.Burst{
		ID:         "burst_legacy",
		CustomerID: "cust_test",
		Backend:    backends.TypeFlyIO,
		BackendID:  "fly-node-1",
		TSHostname: "yscale-burst",
	})
	if err == nil || !strings.Contains(err.Error(), "flyio backend not configured") {
		t.Fatalf("Teardown err = %v, want backend delete attempt error", err)
	}
	if got := hsCalls.list.Load(); got != 2 {
		t.Fatalf("Headscale FindDeviceByHostname calls = %d, want 2", got)
	}
	if got := hsCalls.delete.Load(); got != 1 {
		t.Fatalf("Headscale DeleteDevice calls = %d, want 1", got)
	}
	if got := tsCalls.list.Load(); got != 0 {
		t.Fatalf("SaaS FindDeviceByHostname calls = %d, want 0", got)
	}
}

func TestPlanProvisioningTenantWithBoxProviderDoesNotRequireTailscale(t *testing.T) {
	store := state.New() // cust_test intentionally has no Mesh ref yet.
	backend := &recordingBackend{name: backends.TypeFlyIO, createID: "fly-node-1"}
	d := New(Config{})
	d.store = store
	d.boxProvider = headscaleBoxProvider
	d.fly = backend

	_, err := d.Plan(context.Background(), minimalPlanWorkload(), handlers.PlanOptions{
		CustomerID: "cust_test",
		ClusterID:  "cluster_test",
	})
	if err == nil {
		t.Fatal("Plan succeeded without the provisioning tenant's mesh ref")
	}
	if strings.Contains(err.Error(), "tailscale not configured") {
		t.Fatalf("Plan error = %v, must not require shared Tailscale when a box provider is wired", err)
	}
	if !strings.Contains(err.Error(), "mesh: no coordination provider configured") {
		t.Fatalf("Plan error = %v, want mint-path no-provider error", err)
	}
}

func TestPlanBoxMintFailsClosedNoSharedTailnetFallback(t *testing.T) {
	store := state.New()
	hsURL, _, closeHS := newHeadscaleAPIServer(t, headscaleServerOptions{FailMint: true})
	defer closeHS()
	if err := store.SetCustomerMesh("cust_test", &state.MeshEndpoint{
		Provider:    "headscale",
		LoginServer: hsURL,
		APIKey:      "hs-key",
		User:        "tenant-acme",
	}); err != nil {
		t.Fatalf("set mesh: %v", err)
	}
	// A shared Tailscale client is configured, but a box (SaaS) tenant must NEVER
	// fall back onto it — that would demote a paid-for isolated tenant onto shared
	// multi-tenant infrastructure (#58). The box-mint failure must fail closed.
	ts, tsCalls, closeTS := newTestTailscale(t, "")
	defer closeTS()
	backend := &recordingBackend{name: backends.TypeFlyIO, createID: "fly-node-1"}

	d := New(Config{})
	d.store = store
	d.boxProvider = headscaleBoxProvider
	d.ts = ts
	d.fly = backend

	_, err := d.Plan(context.Background(), minimalPlanWorkload(), handlers.PlanOptions{
		CustomerID: "cust_test",
		ClusterID:  "cluster_test",
	})
	if err == nil {
		t.Fatal("Plan succeeded, want a fail-closed error on box mint failure")
	}
	if !strings.Contains(err.Error(), "mint mesh auth key") {
		t.Fatalf("error = %v, want a mesh mint failure", err)
	}
	// The burst must NOT have been minted on the shared Tailscale tailnet.
	if got := tsCalls.mint.Load(); got != 0 {
		t.Fatalf("Tailscale mint calls = %d, want 0 (no shared-tailnet fallback)", got)
	}
	// No backend node provisioned once the mint fails closed.
	if backend.createCalls != 0 {
		t.Fatalf("backend creates = %d, want 0", backend.createCalls)
	}
	// The customer's mesh ref is retained (teardown + startup box-validation own
	// it), not cleared by a transient mint failure.
	cust, err := store.CustomerByID("cust_test")
	if err != nil {
		t.Fatalf("customer: %v", err)
	}
	if cust.Mesh == nil {
		t.Fatal("customer mesh was cleared by a box-mint failure")
	}
}

func TestPlanPrimaryHeadscaleStampsMintingLoginServer(t *testing.T) {
	store := state.New()
	hsURL, hsCalls, closeHS := newHeadscaleAPIServer(t, headscaleServerOptions{})
	defer closeHS()
	if err := store.SetCustomerMesh("cust_test", &state.MeshEndpoint{
		Provider:    "headscale",
		LoginServer: hsURL,
		APIKey:      "hs-key",
		User:        "tenant-acme",
	}); err != nil {
		t.Fatalf("set mesh: %v", err)
	}
	ts, tsCalls, closeTS := newTestTailscale(t, "")
	defer closeTS()
	backend := &recordingBackend{name: backends.TypeFlyIO, createID: "fly-node-1"}

	d := New(Config{})
	d.store = store
	d.boxProvider = headscaleBoxProvider
	d.ts = ts
	d.fly = backend

	plan, err := d.Plan(context.Background(), minimalPlanWorkload(), handlers.PlanOptions{
		CustomerID: "cust_test",
		ClusterID:  "cluster_test",
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.MeshProvider != "box" {
		t.Fatalf("MeshProvider = %q, want box", plan.MeshProvider)
	}
	if plan.MeshLoginServer != hsURL {
		t.Fatalf("MeshLoginServer = %q, want %q", plan.MeshLoginServer, hsURL)
	}
	if backend.lastSpec == nil || backend.lastSpec.LoginServer != hsURL {
		t.Fatalf("backend LoginServer = %q, want %q", backendLoginServer(backend), hsURL)
	}
	if got := hsCalls.preauth.Load(); got != 1 {
		t.Fatalf("Headscale preauth calls = %d, want 1", got)
	}
	if got := tsCalls.mint.Load(); got != 0 {
		t.Fatalf("Tailscale mint calls = %d, want 0", got)
	}
}

type headscaleServerOptions struct {
	DeviceHostname string
	FailMint       bool
}

func newHeadscaleAPIServer(t *testing.T, opts headscaleServerOptions) (string, *providerCalls, func()) {
	t.Helper()
	calls := &providerCalls{}
	var deleted atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case opts.FailMint && r.Method == http.MethodGet && r.URL.Path == "/api/v1/user":
			calls.users.Add(1)
			http.Error(w, "headscale unavailable", http.StatusServiceUnavailable)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/user":
			calls.users.Add(1)
			writeJSONTest(t, w, map[string]any{
				"users": []map[string]string{{"id": "1", "name": "tenant-acme"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/preauthkey":
			calls.preauth.Add(1)
			writeJSONTest(t, w, map[string]any{
				"preAuthKey": map[string]string{"key": "hskey-auth-test"},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/node":
			calls.list.Add(1)
			nodes := []map[string]any{}
			if opts.DeviceHostname != "" && !deleted.Load() {
				nodes = append(nodes, map[string]any{
					"id":        json.Number("1"),
					"givenName": opts.DeviceHostname,
					"name":      opts.DeviceHostname,
				})
			}
			writeJSONTest(t, w, map[string]any{"nodes": nodes})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/node/1":
			calls.delete.Add(1)
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	return srv.URL, calls, srv.Close
}
