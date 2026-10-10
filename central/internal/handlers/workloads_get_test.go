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

func workloadGetReq(cust *state.Customer, id string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/workloads/"+id, nil)
	r.SetPathValue("id", id)
	return r.WithContext(context.WithValue(r.Context(), ctxCustomer, cust))
}

// GET /v1/workloads/{id} surfaces a live spent_usd computed on the same
// HourlyUSD*age basis as burstView.AccruedUSD (central/internal/handlers/bursts.go).
func TestWorkloadGetLiveSpentUSD(t *testing.T) {
	store := state.New()
	cust, err := store.CustomerByID(state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Now().Add(-30 * time.Minute)
	store.PutWorkload(&state.Workload{ID: "wl1", CustomerID: cust.ID, Status: "running", BurstID: "b1"})
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: cust.ID, HourlyUSD: 1.50, CreatedAt: created})
	h := &Workloads{Store: store, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.Get(rec, workloadGetReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Get status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	spent, ok := resp["spent_usd"].(float64)
	if !ok {
		t.Fatalf("spent_usd missing/not a number in %v", resp)
	}
	// Parity with burstView.AccruedUSD = HourlyUSD * age.Hours() (~$0.75 for 30m @ $1.50/hr).
	want := 1.50 * time.Since(created).Hours()
	if spent < want-0.02 || spent > want+0.02 {
		t.Fatalf("spent_usd = %.4f, want ~%.4f (HourlyUSD*age parity)", spent, want)
	}
}

// GET /v1/workloads/{id} surfaces node_observation when the workload carries one.
func TestWorkloadGetIncludesNodeObservation(t *testing.T) {
	store := state.New()
	cust, err := store.CustomerByID(state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	observed := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	store.PutWorkload(&state.Workload{
		ID: "wl1", CustomerID: cust.ID, Status: "running", BurstID: "b1",
		NodeObservation: &state.NodeObservation{
			NodeName: "ys-burst-b1", Phase: "Ready", Reason: "KubeletReady",
			ObservedAt: observed,
		},
	})
	h := &Workloads{Store: store, Log: quietLog()}
	rec := httptest.NewRecorder()
	h.Get(rec, workloadGetReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Get status = %d; body=%q", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	obs, ok := resp["node_observation"].(map[string]any)
	if !ok {
		t.Fatalf("node_observation missing or not an object in %v", resp)
	}
	if obs["node_name"] != "ys-burst-b1" || obs["phase"] != "Ready" || obs["reason"] != "KubeletReady" {
		t.Fatalf("node_observation = %v", obs)
	}
}

// node_observation is omitted on a workload with no observation, so legacy
// records and ones whose burst has not registered say nothing.
func TestWorkloadGetOmitsNodeObservationWhenAbsent(t *testing.T) {
	store := state.New()
	cust, err := store.CustomerByID(state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	store.PutWorkload(&state.Workload{ID: "wl1", CustomerID: cust.ID, Status: "running", BurstID: "b1"})
	h := &Workloads{Store: store, Log: quietLog()}
	rec := httptest.NewRecorder()
	h.Get(rec, workloadGetReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Get status = %d; body=%q", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if _, present := resp["node_observation"]; present {
		t.Fatal("node_observation should be omitted when absent")
	}
}

// spent_usd is omitted once the burst is reaped (GetBurst → not found), so the
// field never lingers as a stale zero.
func TestWorkloadGetOmitsSpentWhenBurstReaped(t *testing.T) {
	store := state.New()
	cust, err := store.CustomerByID(state.DevCustomerID)
	if err != nil {
		t.Fatal(err)
	}
	store.PutWorkload(&state.Workload{ID: "wl1", CustomerID: cust.ID, Status: "done", BurstID: "gone"})
	h := &Workloads{Store: store, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.Get(rec, workloadGetReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Get status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, present := resp["spent_usd"]; present {
		t.Fatalf("spent_usd should be omitted for a reaped burst, got %v", resp["spent_usd"])
	}
}
