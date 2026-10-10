package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// TestCrossTenantWorkloadAccessDenied is the API-layer isolation proof: one
// tenant's token can neither READ nor MUTATE another tenant's workload, and
// cannot reap another tenant's burst. Every workload handler scopes on
// wl.CustomerID == cust.ID and returns 404 (not 403 — it doesn't even confirm
// the resource exists) on a mismatch.
func TestCrossTenantWorkloadAccessDenied(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_a", Token: "ta"})
	store.AddCustomer(&state.Customer{ID: "cust_b", Token: "tb"})
	store.PutWorkload(&state.Workload{ID: "wl_b", CustomerID: "cust_b", Status: "running", BurstID: "burst_b"})
	store.PutBurst(&state.Burst{ID: "burst_b", CustomerID: "cust_b"})

	reaper := &fakeReaper{}
	h := &Workloads{Store: store, Log: quietLog(), Reaper: reaper}
	custA := &state.Customer{ID: "cust_a", Token: "ta"} // the attacker

	withA := func(r *http.Request) *http.Request {
		r.SetPathValue("id", "wl_b")
		return r.WithContext(context.WithValue(r.Context(), ctxCustomer, custA))
	}

	// READ another tenant's workload → 404.
	rec := httptest.NewRecorder()
	h.Get(rec, withA(httptest.NewRequest(http.MethodGet, "/v1/workloads/wl_b", nil)))
	if rec.Code != http.StatusNotFound {
		t.Errorf("cross-tenant Get = %d, want 404", rec.Code)
	}

	// CANCEL another tenant's workload → 404, and B's burst is NOT reaped.
	rec = httptest.NewRecorder()
	h.Cancel(rec, withA(httptest.NewRequest(http.MethodDelete, "/v1/workloads/wl_b", nil)))
	if rec.Code != http.StatusNotFound {
		t.Errorf("cross-tenant Cancel = %d, want 404", rec.Code)
	}
	if reaper.reaped("burst_b") {
		t.Fatal("ISOLATION BREACH: tenant A's cancel reaped tenant B's burst")
	}
	if _, err := store.GetBurst("burst_b"); err != nil {
		t.Fatal("ISOLATION BREACH: tenant B's burst record was removed by tenant A")
	}
	if _, err := store.GetWorkload("wl_b"); err != nil {
		t.Fatal("tenant B's workload should be untouched")
	}
}

// A tenant's spend view reflects only its own bursts — it can't observe another
// tenant's footprint.
func TestSpendIsPerTenant(t *testing.T) {
	store := state.New()
	custA := &state.Customer{ID: "cust_a", Token: "ta", MaxHourlyUSD: 10}
	store.AddCustomer(custA)
	store.AddCustomer(&state.Customer{ID: "cust_b", Token: "tb"})
	store.PutBurst(&state.Burst{ID: "a1", CustomerID: "cust_a", HourlyUSD: 1})
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_b", HourlyUSD: 5})

	s := computeSpend(store, custA)
	if s.RunningBursts != 1 || s.HourlyUSD != 1 {
		t.Errorf("tenant A spend leaked tenant B: %+v", s)
	}
}
