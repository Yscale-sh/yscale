package handlers

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/state"
)

// stubProviderDeleteGetter satisfies ProviderDeleteGetter as an optional reader
// interface layered on top of the existing ProviderDeletes, exactly as
// lifecycle.Store does in production. Test fakes that do NOT implement it
// are intentionally excluded, verifying the type-assertion gate.
type stubProviderDeleteGetter struct {
	records map[string]lifecycle.ProviderDeleteRecord
}

func (s *stubProviderDeleteGetter) GetProviderDelete(_ context.Context, customerID, clusterID, burstID string) (lifecycle.ProviderDeleteRecord, error) {
	key := customerID + "/" + clusterID + "/" + burstID
	if r, ok := s.records[key]; ok {
		return r, nil
	}
	return lifecycle.ProviderDeleteRecord{}, lifecycle.ErrNotFound
}

// stubProviderDeletesWithGetter wraps a fakeProviderDeletes with a
// GetProviderDelete method so the Get handler's type assertion succeeds.
type stubProviderDeletesWithGetter struct {
	ProviderDeletes
	getter *stubProviderDeleteGetter
}

func (s *stubProviderDeletesWithGetter) GetProviderDelete(ctx context.Context, customerID, clusterID, burstID string) (lifecycle.ProviderDeleteRecord, error) {
	return s.getter.GetProviderDelete(ctx, customerID, clusterID, burstID)
}

func TestGetWorkload_ExposesFinishedAt(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	now := time.Now().UTC().Truncate(time.Second)
	f.store.PutWorkload(&state.Workload{
		ID: "wl_finished", CustomerID: f.cust.ID, Status: "succeeded",
		BurstID: "burst_finished", FinishedAt: &now, SpecYAML: []byte("kind: Workload"),
	})

	body := getWorkload(t, f, "wl_finished")
	raw, present := body["finished_at"]
	if !present {
		t.Fatal("finished_at absent on terminal workload")
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw.(string))
	if err != nil {
		t.Fatalf("parse finished_at: %v", err)
	}
	if !parsed.Equal(now) {
		t.Errorf("finished_at = %v, want %v", parsed, now)
	}
}

func TestGetWorkload_OmitsFinishedAtOnRunning(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	f.store.PutWorkload(&state.Workload{
		ID: "wl_running", CustomerID: f.cust.ID, Status: "running",
		BurstID: "burst_running", SpecYAML: []byte("kind: Workload"),
	})

	body := getWorkload(t, f, "wl_running")
	if _, present := body["finished_at"]; present {
		t.Fatal("finished_at present on running workload")
	}
}

func TestGetWorkload_ExposesFrozenCost(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	frozenAt := time.Now().UTC().Truncate(time.Second)
	f.store.PutWorkload(&state.Workload{
		ID: "wl_cost", CustomerID: f.cust.ID, Status: "succeeded",
		BurstID: "burst_cost", SpecYAML: []byte("kind: Workload"),
		Cost: &state.WorkloadCost{
			EstimatedUSD: 1.234,
			HourlyUSD:    2.50,
			Runtime:      30 * time.Minute,
			FrozenAt:     frozenAt,
			Backend:      "linode",
			BurstID:      "burst_cost",
			Basis:        state.WorkloadCostBasisRateRuntime,
		},
	})

	body := getWorkload(t, f, "wl_cost")
	raw, present := body["cost"]
	if !present {
		t.Fatal("cost absent on workload with frozen cost")
	}
	costMap := raw.(map[string]any)
	if usd, ok := costMap["usd"].(float64); !ok || usd != 1.234 {
		t.Errorf("cost.usd = %v, want 1.234", costMap["usd"])
	}
	if hourly, ok := costMap["hourly_usd"].(float64); !ok || hourly != 2.50 {
		t.Errorf("cost.hourly_usd = %v, want 2.50", costMap["hourly_usd"])
	}
	if basis, ok := costMap["basis"].(string); !ok || basis != state.WorkloadCostBasisRateRuntime {
		t.Errorf("cost.basis = %v, want %s", costMap["basis"], state.WorkloadCostBasisRateRuntime)
	}
}

func TestGetWorkload_OmitsCostOnRunning(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	f.store.PutWorkload(&state.Workload{
		ID: "wl_no_cost", CustomerID: f.cust.ID, Status: "running",
		BurstID: "burst_no_cost", SpecYAML: []byte("kind: Workload"),
	})

	body := getWorkload(t, f, "wl_no_cost")
	if _, present := body["cost"]; present {
		t.Fatal("cost present on running workload without frozen cost")
	}
}

func TestGetWorkload_ExposesCleanupWhenGetterPresent(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	now := time.Now().UTC().Truncate(time.Second)
	deletedAt := now.Add(5 * time.Minute)
	f.store.PutWorkload(&state.Workload{
		ID: "wl_cleanup", CustomerID: f.cust.ID, ClusterID: "cluster_idem",
		Status: "succeeded", BurstID: "burst_cleanup", SpecYAML: []byte("kind: Workload"),
	})

	getter := &stubProviderDeleteGetter{
		records: map[string]lifecycle.ProviderDeleteRecord{
			f.cust.ID + "/cluster_idem/burst_cleanup": {
				State:       "terminated",
				Provider:    "linode",
				Region:      "us-east",
				SKU:         "g6-nanode-1",
				Reason:      "workload_complete",
				RequestedAt: now,
				UpdatedAt:   now.Add(3 * time.Minute),
				DeletedAt:   &deletedAt,
			},
		},
	}
	f.h.Deletes = &stubProviderDeletesWithGetter{getter: getter}

	body := getWorkload(t, f, "wl_cleanup")
	raw, present := body["cleanup"]
	if !present {
		t.Fatal("cleanup absent when getter is present and record exists")
	}
	cleanup := raw.(map[string]any)
	if cleanup["state"] != "terminated" {
		t.Errorf("cleanup.state = %v, want terminated", cleanup["state"])
	}
	if cleanup["provider"] != "linode" {
		t.Errorf("cleanup.provider = %v, want linode", cleanup["provider"])
	}
	if cleanup["sku"] != "g6-nanode-1" {
		t.Errorf("cleanup.sku = %v, want g6-nanode-1", cleanup["sku"])
	}
	if cleanup["reason"] != "workload_complete" {
		t.Errorf("cleanup.reason = %v, want workload_complete", cleanup["reason"])
	}
	if _, hasDeletedAt := cleanup["deleted_at"]; !hasDeletedAt {
		t.Error("cleanup.deleted_at absent when provider deletion is proven")
	}
}

func TestGetWorkload_OmitsCleanupWhenNoGetterInterface(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	f.store.PutWorkload(&state.Workload{
		ID: "wl_no_getter", CustomerID: f.cust.ID, ClusterID: "cluster_idem",
		Status: "succeeded", BurstID: "burst_no_getter", SpecYAML: []byte("kind: Workload"),
	})
	// h.Deletes is nil (default idemFixture) — no ProviderDeleteGetter interface

	body := getWorkload(t, f, "wl_no_getter")
	if _, present := body["cleanup"]; present {
		t.Fatal("cleanup present when Deletes does not implement ProviderDeleteGetter")
	}
}

func TestGetWorkload_OmitsCleanupWhenNoRecord(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	f.store.PutWorkload(&state.Workload{
		ID: "wl_no_record", CustomerID: f.cust.ID, ClusterID: "cluster_idem",
		Status: "succeeded", BurstID: "burst_no_record", SpecYAML: []byte("kind: Workload"),
	})

	getter := &stubProviderDeleteGetter{records: map[string]lifecycle.ProviderDeleteRecord{}}
	f.h.Deletes = &stubProviderDeletesWithGetter{getter: getter}

	body := getWorkload(t, f, "wl_no_record")
	if _, present := body["cleanup"]; present {
		t.Fatal("cleanup present when no provider-delete record exists")
	}
}

func TestGetWorkload_CleanupOmitsCloudAccountAndResourceIDs(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	f.store.PutWorkload(&state.Workload{
		ID: "wl_redact", CustomerID: f.cust.ID, ClusterID: "cluster_idem",
		Status: "succeeded", BurstID: "burst_redact", SpecYAML: []byte("kind: Workload"),
	})

	now := time.Now().UTC()
	getter := &stubProviderDeleteGetter{
		records: map[string]lifecycle.ProviderDeleteRecord{
			f.cust.ID + "/cluster_idem/burst_redact": {
				ID:                 42,
				CustomerID:         f.cust.ID,
				ClusterID:          "cluster_idem",
				WorkloadID:         "wl_redact",
				BurstID:            "burst_redact",
				Provider:           "linode",
				Region:             "us-east",
				CloudAccountID:     "secret_account_123",
				SKU:                "g6-standard-2",
				ProviderResourceID: "linode_vm_99999",
				State:              "deleting",
				Reason:             "teardown",
				RequestedAt:        now,
				UpdatedAt:          now,
			},
		},
	}
	f.h.Deletes = &stubProviderDeletesWithGetter{getter: getter}

	body := getWorkload(t, f, "wl_redact")
	raw, present := body["cleanup"]
	if !present {
		t.Fatal("cleanup absent")
	}
	data, _ := json.Marshal(raw)
	var cleanup map[string]any
	_ = json.Unmarshal(data, &cleanup)

	for _, forbidden := range []string{"cloud_account_id", "provider_resource_id", "payload", "lease_token"} {
		if _, found := cleanup[forbidden]; found {
			t.Errorf("cleanup response leaks %s", forbidden)
		}
	}
}

// --- Issue #93 durable evidence tests ---

type stubBurstEvidenceReader struct {
	providerCreatedAt map[string]*time.Time // key: customerID/clusterID/burstID
	reapReceipted     map[string]bool       // key: burstID/customerID
}

func (s *stubBurstEvidenceReader) GetBurstProviderCreatedAt(_ context.Context, customerID, clusterID, burstID string) (*time.Time, error) {
	key := customerID + "/" + clusterID + "/" + burstID
	if t, ok := s.providerCreatedAt[key]; ok {
		return t, nil
	}
	return nil, lifecycle.ErrNotFound
}

func (s *stubBurstEvidenceReader) BurstReapRecorded(_ context.Context, burstID, customerID string) (bool, error) {
	return s.reapReceipted[burstID+"/"+customerID], nil
}

func TestGetWorkload_CleanupIncludesProviderCreatedAt(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	now := time.Now().UTC().Truncate(time.Second)
	created := now.Add(-10 * time.Minute)
	deletedAt := now.Add(5 * time.Minute)
	f.store.PutWorkload(&state.Workload{
		ID: "wl_evidence", CustomerID: f.cust.ID, ClusterID: "cluster_idem",
		Status: "succeeded", BurstID: "burst_evidence", SpecYAML: []byte("kind: Workload"),
	})

	getter := &stubProviderDeleteGetter{
		records: map[string]lifecycle.ProviderDeleteRecord{
			f.cust.ID + "/cluster_idem/burst_evidence": {
				State: "terminated", Provider: "linode", Region: "us-east",
				SKU: "g6-nanode-1", Reason: "workload_complete",
				RequestedAt: now, UpdatedAt: now, DeletedAt: &deletedAt,
			},
		},
	}
	evidence := &stubBurstEvidenceReader{
		providerCreatedAt: map[string]*time.Time{
			f.cust.ID + "/cluster_idem/burst_evidence": &created,
		},
		reapReceipted: map[string]bool{
			"burst_evidence/" + f.cust.ID: true,
		},
	}
	f.h.Deletes = &stubProviderDeletesWithGetter{getter: getter}
	f.h.Evidence = evidence

	body := getWorkload(t, f, "wl_evidence")
	raw, present := body["cleanup"]
	if !present {
		t.Fatal("cleanup absent with evidence reader wired")
	}
	cleanup := raw.(map[string]any)
	if cleanup["provider_created_at"] == nil {
		t.Fatal("provider_created_at absent in cleanup")
	}
	parsed, err := time.Parse(time.RFC3339Nano, cleanup["provider_created_at"].(string))
	if err != nil {
		t.Fatalf("parse provider_created_at: %v", err)
	}
	if !parsed.Equal(created) {
		t.Errorf("provider_created_at = %v, want %v", parsed, created)
	}
	if receipt, ok := cleanup["durable_reap_receipt"].(bool); !ok || !receipt {
		t.Errorf("durable_reap_receipt = %v, want true", cleanup["durable_reap_receipt"])
	}
}

func TestGetWorkload_CleanupOmitsProviderCreatedAtWhenNull(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	now := time.Now().UTC().Truncate(time.Second)
	f.store.PutWorkload(&state.Workload{
		ID: "wl_no_create", CustomerID: f.cust.ID, ClusterID: "cluster_idem",
		Status: "succeeded", BurstID: "burst_no_create", SpecYAML: []byte("kind: Workload"),
	})

	getter := &stubProviderDeleteGetter{
		records: map[string]lifecycle.ProviderDeleteRecord{
			f.cust.ID + "/cluster_idem/burst_no_create": {
				State: "queued", Provider: "linode", RequestedAt: now, UpdatedAt: now,
			},
		},
	}
	evidence := &stubBurstEvidenceReader{
		providerCreatedAt: map[string]*time.Time{},
		reapReceipted:     map[string]bool{},
	}
	f.h.Deletes = &stubProviderDeletesWithGetter{getter: getter}
	f.h.Evidence = evidence

	body := getWorkload(t, f, "wl_no_create")
	cleanup := body["cleanup"].(map[string]any)
	if cleanup["provider_created_at"] != nil {
		t.Error("provider_created_at should be omitted when NULL")
	}
	if receipt, ok := cleanup["durable_reap_receipt"].(bool); !ok || receipt {
		t.Errorf("durable_reap_receipt = %v, want false", cleanup["durable_reap_receipt"])
	}
}

func TestGetWorkload_CleanupWithoutEvidenceReaderOmitsNewFields(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	now := time.Now().UTC().Truncate(time.Second)
	deletedAt := now.Add(5 * time.Minute)
	f.store.PutWorkload(&state.Workload{
		ID: "wl_no_ev", CustomerID: f.cust.ID, ClusterID: "cluster_idem",
		Status: "succeeded", BurstID: "burst_no_ev", SpecYAML: []byte("kind: Workload"),
	})

	getter := &stubProviderDeleteGetter{
		records: map[string]lifecycle.ProviderDeleteRecord{
			f.cust.ID + "/cluster_idem/burst_no_ev": {
				State: "terminated", Provider: "linode", Region: "us-east",
				RequestedAt: now, UpdatedAt: now, DeletedAt: &deletedAt,
			},
		},
	}
	f.h.Deletes = &stubProviderDeletesWithGetter{getter: getter}
	// h.Evidence is nil

	body := getWorkload(t, f, "wl_no_ev")
	cleanup := body["cleanup"].(map[string]any)
	if cleanup["provider_created_at"] != nil {
		t.Error("provider_created_at should be omitted without evidence reader")
	}
	if receipt, ok := cleanup["durable_reap_receipt"].(bool); !ok || receipt {
		t.Errorf("durable_reap_receipt = %v, want false (no evidence reader)", cleanup["durable_reap_receipt"])
	}
}

func TestGetWorkload_ForeignTenantCannotReadEvidence(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	now := time.Now().UTC().Truncate(time.Second)
	created := now.Add(-10 * time.Minute)
	deletedAt := now.Add(5 * time.Minute)
	f.store.PutWorkload(&state.Workload{
		ID: "wl_own", CustomerID: f.cust.ID, ClusterID: "cluster_idem",
		Status: "succeeded", BurstID: "burst_own", SpecYAML: []byte("kind: Workload"),
	})
	f.store.PutWorkload(&state.Workload{
		ID: "wl_foreign", CustomerID: "other_tenant", ClusterID: "cluster_other",
		Status: "succeeded", BurstID: "burst_foreign", SpecYAML: []byte("kind: Workload"),
	})

	getter := &stubProviderDeleteGetter{
		records: map[string]lifecycle.ProviderDeleteRecord{
			f.cust.ID + "/cluster_idem/burst_own": {
				State: "terminated", Provider: "linode",
				RequestedAt: now, UpdatedAt: now, DeletedAt: &deletedAt,
			},
		},
	}
	evidence := &stubBurstEvidenceReader{
		providerCreatedAt: map[string]*time.Time{
			f.cust.ID + "/cluster_idem/burst_own": &created,
		},
		reapReceipted: map[string]bool{
			"burst_own/" + f.cust.ID: true,
		},
	}
	f.h.Deletes = &stubProviderDeletesWithGetter{getter: getter}
	f.h.Evidence = evidence

	// Own workload: should see evidence
	body := getWorkload(t, f, "wl_own")
	if _, present := body["cleanup"]; !present {
		t.Fatal("own workload cleanup absent")
	}

	// Foreign workload: should get 404 (not found due to tenant mismatch in Get)
	req := workloadGetReq(f.cust, "wl_foreign")
	rec := httptest.NewRecorder()
	f.h.Get(rec, req)
	if rec.Code != 404 {
		t.Errorf("foreign workload returned %d, want 404", rec.Code)
	}
}

func TestGetWorkload_CleanupEvidenceNeverLeaksSensitiveFields(t *testing.T) {
	f := newIdemFixture(t, &fakeDecider{})
	now := time.Now().UTC().Truncate(time.Second)
	created := now.Add(-10 * time.Minute)
	deletedAt := now.Add(5 * time.Minute)
	f.store.PutWorkload(&state.Workload{
		ID: "wl_leak_check", CustomerID: f.cust.ID, ClusterID: "cluster_idem",
		Status: "succeeded", BurstID: "burst_leak_check", SpecYAML: []byte("kind: Workload"),
	})

	getter := &stubProviderDeleteGetter{
		records: map[string]lifecycle.ProviderDeleteRecord{
			f.cust.ID + "/cluster_idem/burst_leak_check": {
				ID: 42, CustomerID: f.cust.ID, ClusterID: "cluster_idem",
				WorkloadID: "wl_leak_check", BurstID: "burst_leak_check",
				Provider: "linode", Region: "us-east",
				CloudAccountID: "secret_account_999", SKU: "g6-standard-2",
				ProviderResourceID: "linode_vm_secret_77777",
				Payload:            []byte(`{"secret":"data"}`), LeaseToken: "lease_secret_tok",
				State: "terminated", Reason: "teardown",
				RequestedAt: now, UpdatedAt: now, DeletedAt: &deletedAt,
			},
		},
	}
	evidence := &stubBurstEvidenceReader{
		providerCreatedAt: map[string]*time.Time{
			f.cust.ID + "/cluster_idem/burst_leak_check": &created,
		},
		reapReceipted: map[string]bool{
			"burst_leak_check/" + f.cust.ID: true,
		},
	}
	f.h.Deletes = &stubProviderDeletesWithGetter{getter: getter}
	f.h.Evidence = evidence

	body := getWorkload(t, f, "wl_leak_check")
	raw, present := body["cleanup"]
	if !present {
		t.Fatal("cleanup absent")
	}
	data, _ := json.Marshal(raw)
	s := string(data)
	for _, secret := range []string{
		"secret_account_999", "linode_vm_secret_77777",
		"lease_secret_tok", `"secret":"data"`,
		"cloud_account_id", "provider_resource_id", "payload", "lease_token",
	} {
		if strings.Contains(s, secret) {
			t.Errorf("cleanup with evidence leaks %q: %s", secret, s)
		}
	}
}
