package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// startedReq builds a POST /v1/workloads/{id}/started request authenticated as
// the given customer, the same shape the Auth middleware would produce.
func startedReq(cust *state.Customer, id string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/workloads/"+id+"/started", nil)
	r.SetPathValue("id", id)
	return r.WithContext(context.WithValue(r.Context(), ctxCustomer, cust))
}

// TestStartedStampsWorkload proves POST /started records StartedAt and flips
// Status to "running", and that a repeat report is an idempotent 200 that
// leaves the original stamp intact.
func TestStartedStampsWorkload(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_a", Token: "ta"}
	store.AddCustomer(cust)
	store.PutWorkload(&state.Workload{
		ID: "wl1", CustomerID: "cust_a", Status: "provisioning",
		BurstID: "b1", CreatedAt: time.Now().UTC(),
	})

	h := &Workloads{Store: store, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.Started(rec, startedReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Started = %d, want 200", rec.Code)
	}
	wl, _ := store.GetWorkload("wl1")
	if wl.Status != "running" || wl.StartedAt == nil {
		t.Fatalf("status=%q startedAt=%v, want running + set", wl.Status, wl.StartedAt)
	}
	first := *wl.StartedAt

	// Repeat report (agent re-delivery on watch resync): 200, no re-stamp.
	rec = httptest.NewRecorder()
	h.Started(rec, startedReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("repeat Started = %d, want 200", rec.Code)
	}
	wl, _ = store.GetWorkload("wl1")
	if !wl.StartedAt.Equal(first) {
		t.Errorf("startedAt = %v, want original %v", wl.StartedAt, first)
	}
}

// A started report that lands after the workload finished (the completion beat
// it) is a 200 no-op — the terminal status must not be clobbered.
func TestStartedAfterFinishIsNoop(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_a", Token: "ta"}
	store.AddCustomer(cust)
	store.PutWorkload(&state.Workload{ID: "wl1", CustomerID: "cust_a", Status: "provisioning", BurstID: "b1"})
	store.FinishWorkload("wl1", "succeeded", time.Now().UTC(), false)

	h := &Workloads{Store: store, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.Started(rec, startedReq(cust, "wl1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Started after finish = %d, want 200", rec.Code)
	}
	wl, _ := store.GetWorkload("wl1")
	if wl.Status != "succeeded" || wl.StartedAt != nil {
		t.Errorf("status=%q startedAt=%v, want succeeded + nil", wl.Status, wl.StartedAt)
	}
}

// Cross-tenant started report → 404, no stamp (same scoping as Get/Complete).
func TestStartedCrossTenantDenied(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_a", Token: "ta"})
	store.AddCustomer(&state.Customer{ID: "cust_b", Token: "tb"})
	store.PutWorkload(&state.Workload{ID: "wl_b", CustomerID: "cust_b", Status: "provisioning", BurstID: "b1"})

	h := &Workloads{Store: store, Log: quietLog()}
	attacker := &state.Customer{ID: "cust_a", Token: "ta"}

	rec := httptest.NewRecorder()
	h.Started(rec, startedReq(attacker, "wl_b"))
	if rec.Code != http.StatusNotFound {
		t.Errorf("cross-tenant Started = %d, want 404", rec.Code)
	}
	wl, _ := store.GetWorkload("wl_b")
	if wl.StartedAt != nil || wl.Status != "provisioning" {
		t.Errorf("tenant B's workload must be untouched, got status=%q startedAt=%v", wl.Status, wl.StartedAt)
	}
}
