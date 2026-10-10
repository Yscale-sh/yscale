package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// The /v1/spend endpoint returns the authed tenant's live spend snapshot.
func TestSpendEndpoint(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_a", Token: "t", MaxConcurrentBursts: 5, MaxHourlyUSD: 10}
	store.AddCustomer(cust)
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_a", HourlyUSD: 2})
	h := &Workloads{Store: store, Log: quietLog()}

	req := httptest.NewRequest(http.MethodGet, "/v1/spend", nil).
		WithContext(context.WithValue(context.Background(), ctxCustomer, cust))
	rec := httptest.NewRecorder()
	h.Spend(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var s TenantSpend
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if s.RunningBursts != 1 || s.HourlyUSD != 2 || s.Limits.MaxHourlyUSD != 10 {
		t.Errorf("spend = %+v", s)
	}
}

func TestComputeSpend(t *testing.T) {
	store := state.New()
	cust := &state.Customer{ID: "cust_a", Token: "t", MaxConcurrentBursts: 5, MaxHourlyUSD: 10}
	store.AddCustomer(cust)
	store.PutBurst(&state.Burst{ID: "b1", CustomerID: "cust_a", HourlyUSD: 0.62})
	store.PutBurst(&state.Burst{ID: "b2", CustomerID: "cust_a", HourlyUSD: 1.50})
	store.PutBurst(&state.Burst{ID: "other", CustomerID: "cust_b", HourlyUSD: 99}) // not counted

	s := computeSpend(store, cust)
	if s.RunningBursts != 2 {
		t.Errorf("RunningBursts = %d, want 2", s.RunningBursts)
	}
	if s.HourlyUSD != 2.12 {
		t.Errorf("HourlyUSD = %v, want 2.12", s.HourlyUSD)
	}
	if s.ProjectedDailyUSD != 2.12*24 {
		t.Errorf("ProjectedDailyUSD = %v, want %v", s.ProjectedDailyUSD, 2.12*24)
	}
	if s.Limits.MaxConcurrentBursts != 5 || s.Limits.MaxHourlyUSD != 10 {
		t.Errorf("limits not surfaced: %+v", s.Limits)
	}
}

// admitBurst evaluates current + candidate against limits; 0 limits = unlimited.
func TestAdmitBurst(t *testing.T) {
	cases := []struct {
		name      string
		spend     TenantSpend
		candidate float64
		want      int // 0 = admit
	}{
		{"under both caps", TenantSpend{RunningBursts: 2, HourlyUSD: 3, Limits: TenantLimits{5, 10}}, 1.0, 0},
		{"at concurrency cap", TenantSpend{RunningBursts: 5, HourlyUSD: 3, Limits: TenantLimits{5, 10}}, 1.0, http.StatusTooManyRequests},
		{"candidate pushes over hourly cap", TenantSpend{RunningBursts: 2, HourlyUSD: 8, Limits: TenantLimits{5, 10}}, 3.0, http.StatusPaymentRequired},
		{"already over hourly cap", TenantSpend{RunningBursts: 2, HourlyUSD: 12, Limits: TenantLimits{5, 10}}, 1.0, http.StatusPaymentRequired},
		{"exactly at hourly cap with candidate", TenantSpend{RunningBursts: 2, HourlyUSD: 9, Limits: TenantLimits{5, 10}}, 1.0, 0},
		{"unlimited (zero limits)", TenantSpend{RunningBursts: 99, HourlyUSD: 999, Limits: TenantLimits{0, 0}}, 100.0, 0},
		{"zero candidate rate admitted", TenantSpend{RunningBursts: 4, HourlyUSD: 9.5, Limits: TenantLimits{5, 10}}, 0, 0},
		{"slot limit with one slot left", TenantSpend{RunningBursts: 4, HourlyUSD: 3, Limits: TenantLimits{5, 10}}, 1.0, 0},
		{"slot limit exactly full", TenantSpend{RunningBursts: 5, HourlyUSD: 3, Limits: TenantLimits{5, 10}}, 1.0, http.StatusTooManyRequests},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, msg := admitBurst(c.spend, c.candidate)
			if status != c.want {
				t.Errorf("status = %d, want %d (msg %q)", status, c.want, msg)
			}
			if c.want != 0 && msg == "" {
				t.Error("a rejection must carry an explanatory message")
			}
		})
	}
}
