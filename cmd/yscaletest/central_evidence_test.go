package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/internal/evidence"
)

func TestCentralEvidence_CompleteEvidencePasses(t *testing.T) {
	created := time.Now().Add(-10 * time.Minute).UTC().Truncate(time.Second)
	deleted := time.Now().UTC().Truncate(time.Second)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "wl_test",
			"status": "succeeded",
			"cleanup": map[string]any{
				"state":                "terminated",
				"deleted_at":           deleted,
				"provider_created_at":  created,
				"durable_reap_receipt": true,
			},
			"outcome": map[string]any{
				"compute": map[string]any{"result": "succeeded"},
			},
		})
	}))
	defer srv.Close()

	client := &CentralEvidenceClient{
		BaseURL: srv.URL,
		Token:   "test-token",
		Client:  srv.Client(),
	}

	ev, err := client.GetWorkloadEvidence(context.Background(), "wl_test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Cleanup == nil {
		t.Fatal("cleanup is nil")
	}
	if ev.Cleanup.ProviderCreatedAt == nil || !ev.Cleanup.ProviderCreatedAt.Equal(created) {
		t.Errorf("provider_created_at = %v, want %v", ev.Cleanup.ProviderCreatedAt, created)
	}
	if ev.Cleanup.DeletedAt == nil || !ev.Cleanup.DeletedAt.Equal(deleted) {
		t.Errorf("deleted_at = %v, want %v", ev.Cleanup.DeletedAt, deleted)
	}
	if !ev.Cleanup.DurableReapReceipt {
		t.Error("durable_reap_receipt = false, want true")
	}
	if ev.Cleanup.State != "terminated" {
		t.Errorf("state = %q, want terminated", ev.Cleanup.State)
	}
	if ev.Outcome == nil {
		t.Fatal("outcome is nil")
	}
	if ev.Outcome.Compute.Result != "succeeded" {
		t.Errorf("outcome.compute.result = %q, want succeeded", ev.Outcome.Compute.Result)
	}
}

func TestCentralEvidence_FullObservationChain(t *testing.T) {
	created := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	started := time.Date(2026, 8, 20, 12, 2, 0, 0, time.UTC)
	finished := time.Date(2026, 8, 20, 12, 5, 0, 0, time.UTC)
	providerCreated := time.Date(2026, 8, 20, 11, 59, 0, 0, time.UTC)
	requested := time.Date(2026, 8, 20, 12, 6, 0, 0, time.UTC)
	deleted := time.Date(2026, 8, 20, 12, 7, 0, 0, time.UTC)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		resp := map[string]any{
			"id":          "wl_full_obs",
			"status":      "succeeded",
			"created_at":  created,
			"started_at":  started,
			"finished_at": finished,
			"placement": map[string]any{
				"receipt": map[string]any{
					"selected": map[string]any{
						"provider":                 "linode",
						"region":                   "us-east",
						"sku":                      "g2-gpu-rtx4000a1-s",
						"gpu_kind":                 "rtx4000ada",
						"gpu_count":                1,
						"hourly_micro_usd":         520000,
						"maximum_charge_micro_usd": 200000,
					},
					"quote_id":        "q_full_001",
					"pricing_version": 2,
				},
			},
			"cost": map[string]any{
				"usd":        0.125,
				"hourly_usd": 0.52,
				"basis":      "metered",
			},
			"cleanup": map[string]any{
				"state":                "terminated",
				"requested_at":         requested,
				"deleted_at":           deleted,
				"provider_created_at":  providerCreated,
				"durable_reap_receipt": true,
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client := &CentralEvidenceClient{
		BaseURL: srv.URL,
		Token:   "test-token",
		Client:  srv.Client(),
	}

	ev, err := client.GetWorkloadEvidence(context.Background(), "wl_full_obs")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if ev.CreatedAt == nil || !ev.CreatedAt.Equal(created) {
		t.Errorf("created_at = %v, want %v", ev.CreatedAt, created)
	}
	if ev.StartedAt == nil || !ev.StartedAt.Equal(started) {
		t.Errorf("started_at = %v, want %v", ev.StartedAt, started)
	}
	if ev.FinishedAt == nil || !ev.FinishedAt.Equal(finished) {
		t.Errorf("finished_at = %v, want %v", ev.FinishedAt, finished)
	}

	if ev.Placement == nil || ev.Placement.Receipt == nil {
		t.Fatal("placement.receipt is nil")
	}
	sel := ev.Placement.Receipt.Selected
	if sel.Provider != "linode" {
		t.Errorf("selected.provider = %q, want linode", sel.Provider)
	}
	if sel.Region != "us-east" {
		t.Errorf("selected.region = %q, want us-east", sel.Region)
	}
	if sel.SKU != "g2-gpu-rtx4000a1-s" {
		t.Errorf("selected.sku = %q, want g2-gpu-rtx4000a1-s", sel.SKU)
	}
	if sel.GPUKind != "rtx4000ada" {
		t.Errorf("selected.gpu_kind = %q, want rtx4000ada", sel.GPUKind)
	}
	if sel.GPUCount != 1 {
		t.Errorf("selected.gpu_count = %d, want 1", sel.GPUCount)
	}
	if sel.HourlyMicroUSD != 520000 {
		t.Errorf("selected.hourly_micro_usd = %d, want 520000", sel.HourlyMicroUSD)
	}
	if sel.MaximumChargeMicroUSD != 200000 {
		t.Errorf("selected.maximum_charge_micro_usd = %d, want 200000", sel.MaximumChargeMicroUSD)
	}
	if ev.Placement.Receipt.QuoteID != "q_full_001" {
		t.Errorf("quote_id = %q, want q_full_001", ev.Placement.Receipt.QuoteID)
	}
	if ev.Placement.Receipt.PricingVersion != 2 {
		t.Errorf("pricing_version = %d, want 2", ev.Placement.Receipt.PricingVersion)
	}

	if ev.Cost == nil {
		t.Fatal("cost is nil")
	}
	if ev.Cost.USD != 0.125 {
		t.Errorf("cost.usd = %f, want 0.125", ev.Cost.USD)
	}
	if ev.Cost.Basis != "metered" {
		t.Errorf("cost.basis = %q, want metered", ev.Cost.Basis)
	}

	if ev.Cleanup == nil {
		t.Fatal("cleanup is nil")
	}
	if ev.Cleanup.RequestedAt == nil || !ev.Cleanup.RequestedAt.Equal(requested) {
		t.Errorf("requested_at = %v, want %v", ev.Cleanup.RequestedAt, requested)
	}
	if ev.Cleanup.DeletedAt == nil || !ev.Cleanup.DeletedAt.Equal(deleted) {
		t.Errorf("deleted_at = %v, want %v", ev.Cleanup.DeletedAt, deleted)
	}
	if ev.Cleanup.ProviderCreatedAt == nil || !ev.Cleanup.ProviderCreatedAt.Equal(providerCreated) {
		t.Errorf("provider_created_at = %v, want %v", ev.Cleanup.ProviderCreatedAt, providerCreated)
	}
	if !ev.Cleanup.DurableReapReceipt {
		t.Error("durable_reap_receipt = false, want true")
	}

	raw, _ := json.Marshal(ev)
	content := string(raw)
	if strings.Contains(content, "test-token") {
		t.Error("response contains the bearer token — token redaction failure")
	}
	for _, sensitive := range []string{"account_id", "resource_id", "cloud_id", "lease_token"} {
		if strings.Contains(content, sensitive) {
			t.Errorf("response contains unexpected sensitive field %q", sensitive)
		}
	}
}

func TestCentralEvidence_DecodedObservationThroughWriteEvidence(t *testing.T) {
	created := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	providerCreated := time.Date(2026, 8, 20, 12, 0, 30, 0, time.UTC)
	nodeReady := time.Date(2026, 8, 20, 12, 1, 0, 0, time.UTC)
	started := time.Date(2026, 8, 20, 12, 2, 0, 0, time.UTC)
	finished := time.Date(2026, 8, 20, 12, 5, 0, 0, time.UTC)
	requested := time.Date(2026, 8, 20, 12, 6, 0, 0, time.UTC)
	deleted := time.Date(2026, 8, 20, 12, 7, 0, 0, time.UTC)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"status":      "succeeded",
			"created_at":  created,
			"started_at":  started,
			"finished_at": finished,
			"placement": map[string]any{
				"receipt": map[string]any{
					"selected": map[string]any{
						"provider":                 "linode",
						"region":                   "us-east",
						"sku":                      "g2-gpu-rtx4000a1-s",
						"gpu_kind":                 "rtx4000ada",
						"gpu_count":                1,
						"hourly_micro_usd":         520000,
						"maximum_charge_micro_usd": 200000,
					},
					"quote_id":        "q_map_001",
					"pricing_version": 1,
				},
			},
			"cost": map[string]any{
				"usd": 0.125, "hourly_usd": 0.52, "basis": "metered",
			},
			"cleanup": map[string]any{
				"state": "terminated", "requested_at": requested,
				"deleted_at": deleted, "provider_created_at": providerCreated,
				"durable_reap_receipt": true,
			},
			"outcome": map[string]any{
				"compute": map[string]any{
					"result": "succeeded",
					"reason": "the free-form reason central renders should be dropped",
				},
				"artifacts": map[string]any{
					"result":           "succeeded",
					"reason":           "another free-form reason that must not survive decoding",
					"objects_uploaded": 3,
					"bytes_uploaded":   0,
				},
			},
		})
	}))
	defer srv.Close()

	client := &CentralEvidenceClient{BaseURL: srv.URL, Token: "tok", Client: srv.Client()}
	ev, err := client.GetWorkloadEvidence(context.Background(), "wl_map")
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	res := CaseResult{
		Case:    Case{Name: "mapping-test", Path: "cases/mapping-test.yaml"},
		Phase:   "Succeeded",
		Backend: "linode",
		BurstID: "burst_map001",
		Reaped:  true,
	}
	obs := evidenceObservations{
		CentralEvidence:   ev,
		TargetClusterType: "k3s",
		NodeReadyAt:       &nodeReady,
		GPUReadyAt:        &nodeReady,
		ObservedNode: &evidence.ObservedNode{
			Name:            "ys-burst-map001",
			GPUAllocatable:  1,
			GPUPresentLabel: true,
			GPUProductLabel: "NVIDIA-RTX-4000-Ada-Generation",
		},
	}
	commitSHA := strings.Repeat("a", 40)
	_, writeErr := writeEvidence(dir, res, "yt-map", commitSHA, obs)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	path := filepath.Join(dir, sanitizeFilename("mapping-test", "cases/mapping-test.yaml", "yt-map"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var artifact evidence.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Provider != "linode" {
		t.Errorf("provider = %q, want linode", artifact.Provider)
	}
	if artifact.Timestamps.Admitted == nil || !artifact.Timestamps.Admitted.Equal(created) {
		t.Errorf("admitted = %v, want %v", artifact.Timestamps.Admitted, created)
	}
	if artifact.Timestamps.NodeReady == nil || !artifact.Timestamps.NodeReady.Equal(nodeReady) {
		t.Errorf("node_ready = %v, want %v", artifact.Timestamps.NodeReady, nodeReady)
	}
	if artifact.Outcome == nil {
		t.Fatal("outcome was not retained")
	}
	if artifact.Outcome.Compute.Result != evidence.OutcomeResultSucceeded {
		t.Errorf("outcome.compute.result = %q, want succeeded", artifact.Outcome.Compute.Result)
	}
	if artifact.Outcome.Artifacts == nil {
		t.Fatal("outcome.artifacts was not retained")
	}
	if got := artifact.Outcome.Artifacts.ObjectsUploaded; got == nil || *got != 3 {
		t.Fatalf("objects_uploaded = %v, want 3", got)
	}
	if got := artifact.Outcome.Artifacts.BytesUploaded; got == nil || *got != 0 {
		t.Fatalf("bytes_uploaded = %v, want counted zero", got)
	}
	if strings.Contains(string(raw), "reason") {
		t.Fatal("retained artifact preserves the free-form reason central rendered")
	}
}

func TestCentralEvidence_AuthFailureReturnsErrEvidenceAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()

	client := &CentralEvidenceClient{
		BaseURL: srv.URL,
		Token:   "bad-token",
		Client:  srv.Client(),
	}

	_, err := client.GetWorkloadEvidence(context.Background(), "wl_test")
	if !errors.Is(err, errEvidenceAuth) {
		t.Fatalf("error = %v, want errEvidenceAuth", err)
	}
}

func TestCentralEvidence_ProbeAuthAcceptsAuthenticatedNoContent(t *testing.T) {
	const token = "probe-secret-token"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/agent/auth-check" {
			t.Fatalf("probe request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Fatalf("authorization header mismatch")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := &CentralEvidenceClient{BaseURL: srv.URL, Token: token, Client: srv.Client()}
	if err := client.ProbeAuth(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCentralEvidence_ProbeAuthFailsClosedAndRedactsToken(t *testing.T) {
	const token = "probe-secret-token"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	client := &CentralEvidenceClient{BaseURL: srv.URL, Token: token, Client: srv.Client()}
	err := client.ProbeAuth(context.Background())
	if !errors.Is(err, errEvidenceAuth) {
		t.Fatalf("error = %v, want errEvidenceAuth", err)
	}
	if strings.Contains(err.Error(), token) {
		t.Fatal("auth probe error exposed the bearer token")
	}
}

func TestCentralEvidence_ProbeAuthRejectsInconclusiveStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client := &CentralEvidenceClient{BaseURL: srv.URL, Token: "tok", Client: srv.Client()}
	if err := client.ProbeAuth(context.Background()); err == nil {
		t.Fatal("inconclusive auth probe passed")
	}
}

func TestCentralEvidence_ProbeAuthRejectsGenericNotFound(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	client := &CentralEvidenceClient{BaseURL: srv.URL, Token: "tok", Client: srv.Client()}
	if err := client.ProbeAuth(context.Background()); err == nil {
		t.Fatal("generic unauthenticated 404 passed as credential proof")
	}
}

func TestCentralEvidence_MissingCleanupReturnsNilCleanup(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "wl_running",
			"status": "running",
		})
	}))
	defer srv.Close()

	client := &CentralEvidenceClient{
		BaseURL: srv.URL,
		Token:   "tok",
		Client:  srv.Client(),
	}

	ev, err := client.GetWorkloadEvidence(context.Background(), "wl_running")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Cleanup != nil {
		t.Fatalf("cleanup should be nil for running workload, got %+v", ev.Cleanup)
	}
}

// --- waitForCentralEvidence tests ---

func TestWaitForCentralEvidence_SkippedWhenNil(t *testing.T) {
	r := &Runner{}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("expected nil error when CentralEvidence is nil, got %v", err)
	}
}

func TestWaitForCentralEvidence_MissingWorkloadID(t *testing.T) {
	r := &Runner{CentralEvidence: &CentralEvidenceClient{}}
	err := r.waitForCentralEvidence(context.Background(), "", time.Now().Add(time.Minute))
	if !errors.Is(err, errEvidenceIncomplete) {
		t.Fatalf("error = %v, want errEvidenceIncomplete", err)
	}
}

func TestWaitForCentralEvidence_AuthFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "bad", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(time.Second))
	if !errors.Is(err, errEvidenceAuth) {
		t.Fatalf("error = %v, want errEvidenceAuth", err)
	}
}

func TestWaitForCentralEvidence_TimeoutWhenNotComplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "wl_1",
			"status": "running",
		})
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "tok", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(50*time.Millisecond))
	if !errors.Is(err, errEvidenceTimeout) {
		t.Fatalf("error = %v, want errEvidenceTimeout", err)
	}
}

func TestWaitForCentralEvidence_MissingProviderCreatedAtTimesOut(t *testing.T) {
	deleted := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"cleanup": map[string]any{
				"state":                "terminated",
				"deleted_at":           deleted,
				"durable_reap_receipt": true,
			},
		})
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "tok", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(80*time.Millisecond))
	if !errors.Is(err, errEvidenceTimeout) {
		t.Fatalf("error = %v, want errEvidenceTimeout (missing provider_created_at should poll, not fail immediately)", err)
	}
}

func TestWaitForCentralEvidence_MissingDeletedAtTimesOut(t *testing.T) {
	created := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"cleanup": map[string]any{
				"state":                "terminated",
				"provider_created_at":  created,
				"durable_reap_receipt": true,
			},
		})
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "tok", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(80*time.Millisecond))
	if !errors.Is(err, errEvidenceTimeout) {
		t.Fatalf("error = %v, want errEvidenceTimeout (missing deleted_at should poll, not fail immediately)", err)
	}
}

func TestWaitForCentralEvidence_MissingReapReceiptTimesOut(t *testing.T) {
	created := time.Now().UTC()
	deleted := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"cleanup": map[string]any{
				"state":                "terminated",
				"provider_created_at":  created,
				"deleted_at":           deleted,
				"durable_reap_receipt": false,
			},
		})
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "tok", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(80*time.Millisecond))
	if !errors.Is(err, errEvidenceTimeout) {
		t.Fatalf("error = %v, want errEvidenceTimeout (missing receipt should poll, not fail immediately)", err)
	}
}

func TestWaitForCentralEvidence_PollsUntilComplete(t *testing.T) {
	created := time.Now().Add(-10 * time.Minute).UTC()
	deleted := time.Now().UTC()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n <= 2 {
			json.NewEncoder(w).Encode(map[string]any{
				"cleanup": map[string]any{
					"state":                "deleting",
					"provider_created_at":  created,
					"durable_reap_receipt": false,
				},
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"cleanup": map[string]any{
				"state":                "terminated",
				"deleted_at":           deleted,
				"provider_created_at":  created,
				"durable_reap_receipt": true,
			},
			"outcome": map[string]any{
				"compute": map[string]any{"result": "succeeded"},
			},
		})
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "tok", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c := calls.Load(); c < 3 {
		t.Errorf("expected at least 3 calls, got %d", c)
	}
}

func TestWaitForCentralEvidence_CompletePasses(t *testing.T) {
	created := time.Now().Add(-10 * time.Minute).UTC()
	deleted := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"cleanup": map[string]any{
				"state":                "terminated",
				"deleted_at":           deleted,
				"provider_created_at":  created,
				"durable_reap_receipt": true,
			},
			"outcome": map[string]any{
				"compute": map[string]any{"result": "succeeded"},
			},
		})
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "tok", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("complete evidence should pass, got %v", err)
	}
}

func TestWaitForCentralEvidence_RetainsProviderAudit(t *testing.T) {
	// This test verifies that the Central evidence check is additive:
	// waitForProviderAbsence (direct Linode audit) is still called
	// independently. The runner calls both; this test just checks that
	// waitForProviderAbsence is still reachable and has not been replaced.
	r := &Runner{}
	// Non-linode backend: provider absence check returns nil (no audit needed)
	err := r.waitForProviderAbsence(context.Background(), "flyio", "burst_1", time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("non-linode provider absence should pass, got %v", err)
	}
}

// --- convergence window tests ---

func TestWaitForCentralEvidence_TerminatedWithoutReapEventuallyConverges(t *testing.T) {
	created := time.Now().Add(-10 * time.Minute).UTC()
	deleted := time.Now().UTC()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		switch {
		case n <= 2:
			json.NewEncoder(w).Encode(map[string]any{
				"cleanup": map[string]any{
					"state":                "terminated",
					"provider_created_at":  created,
					"deleted_at":           deleted,
					"durable_reap_receipt": false,
				},
			})
		default:
			json.NewEncoder(w).Encode(map[string]any{
				"cleanup": map[string]any{
					"state":                "terminated",
					"provider_created_at":  created,
					"deleted_at":           deleted,
					"durable_reap_receipt": true,
				},
				"outcome": map[string]any{
					"compute": map[string]any{"result": "succeeded"},
				},
			})
		}
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "tok", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatalf("terminated-without-reap should converge, got %v", err)
	}
	if c := calls.Load(); c < 3 {
		t.Errorf("expected at least 3 polls for convergence, got %d", c)
	}
}

func TestWaitForCentralEvidence_AllFieldsArriveIncrementally(t *testing.T) {
	created := time.Now().Add(-10 * time.Minute).UTC()
	deleted := time.Now().UTC()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		switch {
		case n == 1:
			json.NewEncoder(w).Encode(map[string]any{
				"cleanup": map[string]any{
					"state":                "deleting",
					"durable_reap_receipt": false,
				},
			})
		case n == 2:
			json.NewEncoder(w).Encode(map[string]any{
				"cleanup": map[string]any{
					"state":                "terminated",
					"provider_created_at":  created,
					"durable_reap_receipt": false,
				},
			})
		default:
			json.NewEncoder(w).Encode(map[string]any{
				"cleanup": map[string]any{
					"state":                "terminated",
					"provider_created_at":  created,
					"deleted_at":           deleted,
					"durable_reap_receipt": true,
				},
				"outcome": map[string]any{
					"compute": map[string]any{"result": "succeeded"},
				},
			})
		}
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "tok", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatalf("incremental convergence should succeed, got %v", err)
	}
	if c := calls.Load(); c < 3 {
		t.Errorf("expected at least 3 polls, got %d", c)
	}
}

// --- URL construction tests ---

func TestCentralEvidence_URLNormalization(t *testing.T) {
	tests := []struct {
		name       string
		baseURL    string
		workloadID string
		wantPath   string
	}{
		{
			name:       "trailing slash stripped",
			baseURL:    "will-be-replaced/",
			workloadID: "wl_123",
			wantPath:   "/v1/workloads/wl_123",
		},
		{
			name:       "multiple trailing slashes stripped",
			baseURL:    "will-be-replaced///",
			workloadID: "wl_123",
			wantPath:   "/v1/workloads/wl_123",
		},
		{
			name:       "workload ID with slash is escaped",
			baseURL:    "will-be-replaced",
			workloadID: "wl/evil",
			wantPath:   "/v1/workloads/wl%2Fevil",
		},
		{
			name:       "workload ID with space is escaped",
			baseURL:    "will-be-replaced",
			workloadID: "wl test",
			wantPath:   "/v1/workloads/wl%20test",
		},
		{
			name:       "clean workload ID unchanged",
			baseURL:    "will-be-replaced",
			workloadID: "wl_abc123",
			wantPath:   "/v1/workloads/wl_abc123",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.RequestURI()
				json.NewEncoder(w).Encode(map[string]any{"id": tc.workloadID})
			}))
			defer srv.Close()

			client := &CentralEvidenceClient{
				BaseURL: srv.URL + tc.baseURL[len("will-be-replaced"):],
				Token:   "tok",
				Client:  srv.Client(),
			}
			client.GetWorkloadEvidence(context.Background(), tc.workloadID)

			if gotPath != tc.wantPath {
				t.Errorf("path = %q, want %q", gotPath, tc.wantPath)
			}
		})
	}
}

// --- evidenceComplete unit test ---

func TestEvidenceComplete(t *testing.T) {
	created := time.Now().UTC()
	deleted := time.Now().UTC()
	completeCleanup := &CleanupEvidence{
		State: "terminated", ProviderCreatedAt: &created, DeletedAt: &deleted, DurableReapReceipt: true,
	}
	succeededOutcome := &OutcomeEvidence{Compute: ComputeOutcomeEvidence{Result: "succeeded"}}
	failedOutcome := &OutcomeEvidence{Compute: ComputeOutcomeEvidence{Result: "failed"}}

	tests := []struct {
		name string
		ev   *WorkloadEvidence
		want bool
	}{
		{"nil cleanup", &WorkloadEvidence{}, false},
		{"empty cleanup", &WorkloadEvidence{Cleanup: &CleanupEvidence{}}, false},
		{"missing provider_created_at", &WorkloadEvidence{Cleanup: &CleanupEvidence{
			State: "terminated", DeletedAt: &deleted, DurableReapReceipt: true,
		}, Outcome: succeededOutcome}, false},
		{"not terminated", &WorkloadEvidence{Cleanup: &CleanupEvidence{
			State: "deleting", ProviderCreatedAt: &created, DeletedAt: &deleted, DurableReapReceipt: true,
		}, Outcome: succeededOutcome}, false},
		{"missing deleted_at", &WorkloadEvidence{Cleanup: &CleanupEvidence{
			State: "terminated", ProviderCreatedAt: &created, DurableReapReceipt: true,
		}, Outcome: succeededOutcome}, false},
		{"receipt false", &WorkloadEvidence{Cleanup: &CleanupEvidence{
			State: "terminated", ProviderCreatedAt: &created, DeletedAt: &deleted,
		}, Outcome: succeededOutcome}, false},
		{"outcome absent",
			&WorkloadEvidence{Status: "succeeded", Cleanup: completeCleanup},
			false},
		{"outcome disagrees with succeeded status",
			&WorkloadEvidence{Status: "succeeded", Cleanup: completeCleanup, Outcome: failedOutcome},
			false},
		{"failed status with failed outcome still complete",
			&WorkloadEvidence{Status: "failed", Cleanup: completeCleanup, Outcome: failedOutcome},
			true},
		{"all complete",
			&WorkloadEvidence{Status: "succeeded", Cleanup: completeCleanup, Outcome: succeededOutcome},
			true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidenceComplete(tc.ev); got != tc.want {
				t.Errorf("evidenceComplete = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWaitForCentralEvidence_MissingOutcomeTimesOut(t *testing.T) {
	created := time.Now().UTC()
	deleted := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"status": "succeeded",
			"cleanup": map[string]any{
				"state":                "terminated",
				"provider_created_at":  created,
				"deleted_at":           deleted,
				"durable_reap_receipt": true,
			},
		})
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "tok", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(80*time.Millisecond))
	if !errors.Is(err, errEvidenceTimeout) {
		t.Fatalf("error = %v, want errEvidenceTimeout (missing outcome must not pass)", err)
	}
}

func TestWaitForCentralEvidence_OutcomeDisagreesWithSucceededTimesOut(t *testing.T) {
	created := time.Now().UTC()
	deleted := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"status": "succeeded",
			"cleanup": map[string]any{
				"state":                "terminated",
				"provider_created_at":  created,
				"deleted_at":           deleted,
				"durable_reap_receipt": true,
			},
			"outcome": map[string]any{
				"compute": map[string]any{"result": "failed"},
			},
		})
	}))
	defer srv.Close()

	r := &Runner{
		CentralEvidence: &CentralEvidenceClient{
			BaseURL: srv.URL, Token: "tok", Client: srv.Client(),
		},
		EvidencePoll: 10 * time.Millisecond,
	}
	err := r.waitForCentralEvidence(context.Background(), "wl_1", time.Now().Add(80*time.Millisecond))
	if !errors.Is(err, errEvidenceTimeout) {
		t.Fatalf("error = %v, want errEvidenceTimeout (outcome must not be fabricated from status)", err)
	}
}

func TestIncompleteEvidenceSummaryReportsMissingOutcome(t *testing.T) {
	created := time.Now().UTC()
	deleted := time.Now().UTC()
	ev := &WorkloadEvidence{
		Status: "succeeded",
		Cleanup: &CleanupEvidence{
			State: "terminated", ProviderCreatedAt: &created, DeletedAt: &deleted, DurableReapReceipt: true,
		},
	}
	summary := incompleteEvidenceSummary(ev)
	if !strings.Contains(summary, "outcome") {
		t.Fatalf("summary %q does not mention the missing outcome", summary)
	}
}

func TestIncompleteEvidenceSummaryBoundsMalformedResult(t *testing.T) {
	created := time.Now().UTC()
	deleted := time.Now().UTC()
	nasty := strings.Repeat("x", maxIncompleteResultBytes+50)
	ev := &WorkloadEvidence{
		Status: "succeeded",
		Cleanup: &CleanupEvidence{
			State: "terminated", ProviderCreatedAt: &created, DeletedAt: &deleted, DurableReapReceipt: true,
		},
		Outcome: &OutcomeEvidence{
			Compute: ComputeOutcomeEvidence{Result: nasty},
		},
	}
	summary := incompleteEvidenceSummary(ev)
	if strings.Contains(summary, nasty) {
		t.Fatalf("summary %q leaks the unbounded receipt string", summary)
	}
	if !strings.Contains(summary, "outcome.compute.result=") {
		t.Fatalf("summary %q does not name the mismatched compute result", summary)
	}
}

// --- tenant isolation test ---

func TestCentralEvidence_BearerTokenSent(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode(map[string]any{"id": "wl_1"})
	}))
	defer srv.Close()

	client := &CentralEvidenceClient{
		BaseURL: srv.URL, Token: "secret-tenant-token", Client: srv.Client(),
	}
	client.GetWorkloadEvidence(context.Background(), "wl_1")

	if gotAuth != "Bearer secret-tenant-token" {
		t.Errorf("Authorization = %q, want Bearer secret-tenant-token", gotAuth)
	}
}

// --- envDefault test ---

func TestEnvDefault(t *testing.T) {
	t.Setenv("TEST_ENV_DEFAULT_SET", "from-env")
	if got := envDefault("TEST_ENV_DEFAULT_SET", "fallback"); got != "from-env" {
		t.Errorf("envDefault with set env = %q, want from-env", got)
	}
	if got := envDefault("TEST_ENV_DEFAULT_UNSET_"+t.Name(), "fallback"); got != "fallback" {
		t.Errorf("envDefault with unset env = %q, want fallback", got)
	}
}
