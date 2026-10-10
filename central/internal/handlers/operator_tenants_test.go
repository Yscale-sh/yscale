package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func operatorTenantRequest(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	return req.WithContext(context.WithValue(req.Context(), ctxOperatorAccount, "acct_operator_human"))
}

type limitCapturingStore struct {
	tenantStore
	actor state.Actor
	err   error
}

func (s *limitCapturingStore) SetCustomerLimits(customerID string, maxConcurrentBursts int, maxHourlyUSD float64, by state.Actor) (*state.Customer, bool, error) {
	s.actor = by
	if s.err != nil {
		return nil, false, s.err
	}
	return s.tenantStore.SetCustomerLimits(customerID, maxConcurrentBursts, maxHourlyUSD, by)
}

func TestHandleListOperatorTenantsSafePagination(t *testing.T) {
	store := state.New()
	if err := store.RevokeCustomer(state.DevCustomerID); err != nil {
		t.Fatal(err)
	}
	store.AddCustomer(&state.Customer{ID: "cust_b", Name: "Bravo", Plan: "pro", Token: "tok_b", Email: "b@example.com"})
	store.AddCustomer(&state.Customer{ID: "cust_a", Name: "Alpha", Plan: "free", Token: "tok_a", Email: "a@example.com", MaxConcurrentBursts: 3, MaxHourlyUSD: 8})
	if err := store.PutBurst(&state.Burst{ID: "burst_a", CustomerID: "cust_a", HourlyUSD: 2.5}); err != nil {
		t.Fatal(err)
	}
	h := &Tenants{Store: store}

	rec := httptest.NewRecorder()
	h.HandleListOperatorTenants(rec, httptest.NewRequest(http.MethodGet, "/v1/operator/tenants?limit=1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var page OperatorTenantsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Tenants) != 1 || page.Tenants[0].TenantID != "cust_a" || page.NextAfter != "cust_a" {
		t.Fatalf("page = %+v", page)
	}
	row := page.Tenants[0]
	if row.RunningBursts != 1 || row.HourlyUSD != 2.5 || row.ProjectedDailyUSD != 60 ||
		row.Limits.MaxConcurrentBursts != 3 || row.Limits.MaxHourlyUSD != 8 {
		t.Fatalf("row = %+v", row)
	}
	for _, secret := range []string{"tok_a", "a@example.com", "tok_b", "b@example.com"} {
		if strings.Contains(rec.Body.String(), secret) {
			t.Fatalf("response leaked %q: %s", secret, rec.Body.String())
		}
	}

	rec = httptest.NewRecorder()
	h.HandleListOperatorTenants(rec, httptest.NewRequest(http.MethodGet, "/v1/operator/tenants?after=cust_a&limit=1", nil))
	page = OperatorTenantsResponse{}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Tenants) != 1 || page.Tenants[0].TenantID != "cust_b" || page.NextAfter != "" {
		t.Fatalf("second page = %+v", page)
	}
}

func TestHandleListOperatorTenantsRejectsMalformedBounds(t *testing.T) {
	h := &Tenants{Store: state.New()}
	for _, query := range []string{"?limit=0", "?limit=101", "?limit=nope", "?limit=1&limit=2", "?after=", "?cursor=x"} {
		rec := httptest.NewRecorder()
		h.HandleListOperatorTenants(rec, httptest.NewRequest(http.MethodGet, "/v1/operator/tenants"+query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400", query, rec.Code)
		}
	}
}

func TestHandlePatchTenantLimitsStrictBodyFailures(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_patch", MaxConcurrentBursts: 2, MaxHourlyUSD: 5})
	h := &Tenants{Store: store}
	bad := []string{
		`{"max_concurrent_bursts":5,"max_hourly_usd":10,"unknown":1}`,
		`{"max_concurrent_bursts":5,"max_hourly_usd":10} {}`,
		`{"max_concurrent_bursts":"five","max_hourly_usd":10}`,
		`{"max_hourly_usd":10}`,
		`{"max_concurrent_bursts":5}`,
		`{"max_concurrent_bursts":-1,"max_hourly_usd":10}`,
		`{"max_concurrent_bursts":5,"max_hourly_usd":-1}`,
		strings.Repeat(" ", maxTenantLimitsRequestBytes) + `{"max_concurrent_bursts":5,"max_hourly_usd":10}`,
	}
	for _, body := range bad {
		req := operatorTenantRequest(http.MethodPatch, "/v1/operator/tenants/cust_patch/limits", body)
		req.SetPathValue("tenant_id", "cust_patch")
		rec := httptest.NewRecorder()
		h.HandlePatchTenantLimits(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q = %d, want 400", body[:min(len(body), 80)], rec.Code)
		}
	}
}

func TestHandlePatchTenantLimitsNotFoundRevokedAndVerifiedActor(t *testing.T) {
	store := state.New()
	revokedAt := time.Now().UTC()
	store.AddCustomer(&state.Customer{ID: "cust_revoked", RevokedAt: &revokedAt})
	store.AddCustomer(&state.Customer{ID: "cust_live", MaxConcurrentBursts: 2, MaxHourlyUSD: 5})
	h := &Tenants{Store: store}
	body := `{"max_concurrent_bursts":5,"max_hourly_usd":10}`
	for _, id := range []string{"cust_missing", "cust_revoked"} {
		req := operatorTenantRequest(http.MethodPatch, "/v1/operator/tenants/"+id+"/limits", body)
		req.SetPathValue("tenant_id", id)
		rec := httptest.NewRecorder()
		h.HandlePatchTenantLimits(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s = %d, want 404", id, rec.Code)
		}
	}
	unauthenticated := httptest.NewRequest(http.MethodPatch, "/v1/operator/tenants/cust_live/limits", strings.NewReader(body))
	unauthenticated.SetPathValue("tenant_id", "cust_live")
	rec := httptest.NewRecorder()
	h.HandlePatchTenantLimits(rec, unauthenticated)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing verified actor = %d, want 401", rec.Code)
	}
}

func TestHandlePatchTenantLimitsMapsPersistenceFailureToSafe503(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_fail", MaxConcurrentBursts: 2, MaxHourlyUSD: 5})
	failing := &limitCapturingStore{tenantStore: store, err: fmt.Errorf("%w: database password secret", state.ErrPersistence)}
	h := &Tenants{Store: failing}
	req := operatorTenantRequest(http.MethodPatch, "/v1/operator/tenants/cust_fail/limits", `{"max_concurrent_bursts":4,"max_hourly_usd":10}`)
	req.SetPathValue("tenant_id", "cust_fail")
	rec := httptest.NewRecorder()
	h.HandlePatchTenantLimits(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "password") || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("backend error leaked: %s", rec.Body.String())
	}
}

func TestHandlePatchTenantLimitsChangesFutureAdmissionOnly(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_admit"})
	if err := store.PutBurst(&state.Burst{ID: "burst_existing", CustomerID: "cust_admit", HourlyUSD: 1}); err != nil {
		t.Fatal(err)
	}
	h := &Tenants{Store: store}
	req := operatorTenantRequest(http.MethodPatch, "/v1/operator/tenants/cust_admit/limits", `{"max_concurrent_bursts":1,"max_hourly_usd":10}`)
	req.SetPathValue("tenant_id", "cust_admit")
	rec := httptest.NewRecorder()
	h.HandlePatchTenantLimits(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("patch = %d (body %q)", rec.Code, rec.Body.String())
	}
	if bursts := store.BurstsForCustomer("cust_admit"); len(bursts) != 1 || bursts[0].ID != "burst_existing" {
		t.Fatalf("existing burst changed: %+v", bursts)
	}
	cust, err := store.CustomerByID("cust_admit")
	if err != nil {
		t.Fatal(err)
	}
	gate := newAdmissionGate(store)
	_, status, _ := gate.reserve(context.Background(), cust, "wl_probe", 0)
	if status != http.StatusTooManyRequests {
		t.Fatalf("future admission = %d, want 429", status)
	}
}

func TestHandlePatchTenantLimitsResponseAndNoOp(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_response", Name: "Response", Plan: "pro", MaxConcurrentBursts: 2, MaxHourlyUSD: 5})
	capture := &limitCapturingStore{tenantStore: store}
	h := &Tenants{Store: capture}
	body := `{"max_concurrent_bursts":4,"max_hourly_usd":12.5}`
	for i, wantChanged := range []bool{true, false} {
		req := operatorTenantRequest(http.MethodPatch, "/v1/operator/tenants/cust_response/limits", body)
		req.SetPathValue("tenant_id", "cust_response")
		rec := httptest.NewRecorder()
		h.HandlePatchTenantLimits(rec, req)
		var response PatchTenantLimitsResponse
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &response) != nil {
			t.Fatalf("call %d = %d %q", i, rec.Code, rec.Body.String())
		}
		if response.Changed != wantChanged || response.Tenant.TenantID != "cust_response" ||
			response.Tenant.Limits.MaxConcurrentBursts != 4 || response.Tenant.Limits.MaxHourlyUSD != 12.5 {
			t.Fatalf("call %d response = %+v", i, response)
		}
	}
	if capture.actor.Kind != state.ActorHuman || capture.actor.AccountID != "acct_operator_human" || capture.actor.CustomerID != "cust_response" {
		t.Fatalf("actor = %+v, want verified human account scoped to tenant", capture.actor)
	}
}
