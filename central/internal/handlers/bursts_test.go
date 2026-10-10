package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// GET /v1/bursts returns only the calling tenant's bursts, projected to safe
// fields (no MeshLoginServer / BackendID), newest first.
func TestBurstsListTenantScopedAndSafe(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_a", Token: "ta"})
	store.AddCustomer(&state.Customer{ID: "cust_b", Token: "tb"})

	older := time.Now().Add(-30 * time.Minute)
	newer := time.Now().Add(-5 * time.Minute)
	store.PutBurst(&state.Burst{
		ID: "burst_a1", CustomerID: "cust_a", Backend: "flyio", NodeName: "ys-burst-a1",
		Status: "running", SKU: "shared-1x", HourlyUSD: 0.10, CreatedAt: older,
		MeshProvider: "box", MeshLoginServer: "https://box.internal", PodCIDR: "10.244.10.0/24",
	})
	store.PutBurst(&state.Burst{
		ID: "burst_a2", CustomerID: "cust_a", Backend: "linode", NodeName: "ys-burst-a2",
		Status: "provisioning", HourlyUSD: 0.20, CreatedAt: newer,
	})
	store.PutBurst(&state.Burst{ID: "burst_b1", CustomerID: "cust_b", Backend: "aws"})

	h := &Workloads{Store: store, Log: quietLog()}
	req := httptest.NewRequest(http.MethodGet, "/v1/bursts", nil).
		WithContext(context.WithValue(context.Background(), ctxCustomer, &state.Customer{ID: "cust_a"}))
	rec := httptest.NewRecorder()
	h.Bursts(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		Count  int         `json:"count"`
		Bursts []BurstView `json:"bursts"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Count != 2 || len(body.Bursts) != 2 {
		t.Fatalf("count = %d, want 2 (cust_a only, never cust_b)", body.Count)
	}
	// Newest first.
	if body.Bursts[0].ID != "burst_a2" || body.Bursts[1].ID != "burst_a1" {
		t.Fatalf("order = [%s,%s], want [burst_a2,burst_a1]", body.Bursts[0].ID, body.Bursts[1].ID)
	}
	// No cust_b leakage.
	for _, b := range body.Bursts {
		if b.ID == "burst_b1" {
			t.Fatal("cross-tenant burst leaked into the list")
		}
	}
	// Live accrual: 30-min-old burst at $0.10/hr ~ $0.05.
	if got := body.Bursts[1].AccruedUSD; got < 0.04 || got > 0.06 {
		t.Errorf("accrued for 30m@$0.10 = %.4f, want ~0.05", got)
	}
	// Safe projection: BurstView has no field carrying the box URL.
	raw, _ := json.Marshal(body.Bursts[1])
	if got := string(raw); contains(got, "box.internal") {
		t.Errorf("MeshLoginServer leaked into the projection: %s", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestBurstViewGPUTelemetryAbsent(t *testing.T) {
	b := &state.Burst{
		ID: "burst_no_gpu", CustomerID: "c", Backend: "linode",
		Status: "running", CreatedAt: time.Now(),
	}
	v := burstView(b, time.Now())
	if v.GPUUtilPercent != nil {
		t.Fatal("absent telemetry should produce nil GPUUtilPercent")
	}
	if v.LastHeartbeatAt != nil {
		t.Fatal("absent telemetry should produce nil LastHeartbeatAt")
	}
	raw, _ := json.Marshal(v)
	if contains(string(raw), "gpu_util_percent") {
		t.Fatalf("absent telemetry should not appear in JSON: %s", raw)
	}
}

func TestBurstViewGPUTelemetryZero(t *testing.T) {
	ts := time.Now().UTC()
	b := &state.Burst{
		ID: "burst_idle", CustomerID: "c", Backend: "linode",
		Status: "running", CreatedAt: time.Now(),
		GPUUtilPercent: 0, LastHeartbeatAt: &ts,
	}
	v := burstView(b, time.Now())
	if v.GPUUtilPercent == nil {
		t.Fatal("0% observed should produce non-nil GPUUtilPercent")
	}
	if *v.GPUUtilPercent != 0 {
		t.Fatalf("got %f, want 0", *v.GPUUtilPercent)
	}
	raw, _ := json.Marshal(v)
	if !contains(string(raw), `"gpu_util_percent":0`) {
		t.Fatalf("0%% should appear in JSON: %s", raw)
	}
}

func TestBurstViewGPUTelemetryNonzero(t *testing.T) {
	ts := time.Now().UTC()
	b := &state.Burst{
		ID: "burst_active", CustomerID: "c", Backend: "linode",
		Status: "running", CreatedAt: time.Now(),
		GPUUtilPercent: 87.5, LastHeartbeatAt: &ts,
	}
	v := burstView(b, time.Now())
	if v.GPUUtilPercent == nil || *v.GPUUtilPercent != 87.5 {
		t.Fatalf("got %v, want 87.5", v.GPUUtilPercent)
	}
	if v.LastHeartbeatAt == nil {
		t.Fatal("LastHeartbeatAt should be set")
	}
}
