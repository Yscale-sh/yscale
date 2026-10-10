package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const safeBillingJSON = `{"tenant_id":"cust_42","currency":"USD","balance_micro_usd":42000000,"held_micro_usd":8000000,"spendable_micro_usd":34000000,"debt_micro_usd":0,"frozen":false,"updated_at":"2026-08-16T10:00:00Z","open_holds":[{"id":41,"workload_id":"wl_1","amount_micro_usd":8000000,"expires_at":"2026-08-16T11:00:00Z","created_at":"2026-08-16T09:00:00Z"}]}`

func TestBillingRoutesForwardExactTransport(t *testing.T) {
	tests := []struct{ method, path, body, upstream, response string }{
		{http.MethodGet, "/api/tenants/cust_42/billing", "", "/v1/tenants/cust_42/billing", safeBillingJSON},
		{http.MethodPost, "/api/operator/tenants/cust_42/billing/service-credits", `{"amount_micro_usd":1250000,"idempotency_key":"svc-credit-test"}`, "/v1/operator/tenants/cust_42/billing/service-credits", `{"tenant_id":"cust_42","amount_micro_usd":1250000,"currency":"USD","idempotency_key":"svc-credit-test","granted":true}`},
	}
	for _, tt := range tests {
		up, got := upstreamRecorder(t, http.StatusOK, tt.response)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
		req.Header.Set("Authorization", "Bearer caller")
		req.Header.Set("Cookie", "browser=session")
		if tt.method == http.MethodPost {
			req.Header.Set("Content-Type", "application/json")
		}
		rr := do(h, req)
		if rr.Code != http.StatusOK || got.method != tt.method || got.path != tt.upstream {
			t.Fatalf("response %d %s; upstream %s %s", rr.Code, rr.Body.String(), got.method, got.path)
		}
		if got.header.Get("Authorization") != "Bearer caller" || got.header.Get("Cookie") != "" {
			t.Fatalf("upstream headers = %v", got.header)
		}
		if tt.method == http.MethodPost && got.body != tt.body {
			t.Fatalf("upstream body = %q", got.body)
		}
	}
}

func TestBillingRoutesRejectUnsafeInputsAndResponses(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, safeBillingJSON)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	bad := []struct{ method, path, contentType, body string }{
		{http.MethodGet, "/api/tenants/cust_42/billing?debug=1", "", ""},
		{http.MethodGet, "/api/tenants/cust_42/billing", "", `{}`},
		{http.MethodPost, "/api/operator/tenants/cust_42/billing/service-credits", "text/plain", `{}`},
		{http.MethodPost, "/api/operator/tenants/cust_42/billing/service-credits", "application/json", `{"amount_micro_usd":0,"idempotency_key":"svc-credit-test"}`},
		{http.MethodPost, "/api/operator/tenants/cust_42/billing/service-credits", "application/json", `{"amount_micro_usd":1,"idempotency_key":"svc-credit-test","provider":"x"}`},
	}
	for _, tc := range bad {
		got.path = ""
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer caller")
		if tc.contentType != "" {
			req.Header.Set("Content-Type", tc.contentType)
		}
		if rr := do(h, req); rr.Code < 400 || rr.Code >= 500 {
			t.Fatalf("%s %s = %d", tc.method, tc.path, rr.Code)
		}
		if got.path != "" {
			t.Fatalf("invalid request reached upstream")
		}
	}
	for _, response := range []string{
		strings.TrimSuffix(safeBillingJSON, "}") + `,"provider_customer_id":"leak"}`,
		`{"tenant_id":"cust_42","currency":"USD","balance_micro_usd":1,"held_micro_usd":0,"spendable_micro_usd":1,"debt_micro_usd":0,"frozen":false,"open_holds":[],"invoice_id":"leak"}`,
		`{"tenant_id":"cust_42","currency":"USD","balance_micro_usd":1,"held_micro_usd":1,"spendable_micro_usd":0,"debt_micro_usd":0,"frozen":false,"open_holds":[{"id":"hold_41","workload_id":"wl_1","amount_micro_usd":1,"expires_at":"2026-08-16T11:00:00Z","created_at":"2026-08-16T09:00:00Z"}]}`,
		`{"tenant_id":"cust_42","currency":"USD","balance_micro_usd":1,"held_micro_usd":1,"spendable_micro_usd":0,"debt_micro_usd":0,"frozen":false,"open_holds":[{"id":0,"workload_id":"wl_1","amount_micro_usd":1,"expires_at":"2026-08-16T11:00:00Z","created_at":"2026-08-16T09:00:00Z"}]}`,
	} {
		unsafe, _ := upstreamRecorder(t, http.StatusOK, response)
		proxy, _ := newTestAppWithProxy(t, "", newAccountProxy(unsafe.URL, unsafe.URL, unsafe.Client()))
		req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/billing", nil)
		req.Header.Set("Authorization", "Bearer caller")
		if rr := do(proxy, req); rr.Code != http.StatusBadGateway {
			t.Fatalf("unsafe response = %d %s", rr.Code, rr.Body.String())
		}
	}
}
