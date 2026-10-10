package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const safeRuntimeBindingJSON = `{"id":"rb_1","key":"DATABASE_URL","name":"Primary database","revision":"3","sync_state":"synced","created_at":"2026-08-16T09:00:00Z","updated_at":"2026-08-16T10:00:00Z"}`

func TestRuntimeBindingRoutesForwardExactTransport(t *testing.T) {
	tests := []struct{ method, path, body, upstream, response string }{
		{http.MethodGet, "/api/tenants/cust_42/runtime-bindings", "", "/v1/tenants/cust_42/runtime-bindings", `{"tenant_id":"cust_42","role":"owner","bindings":[` + safeRuntimeBindingJSON + `]}`},
		{http.MethodPut, "/api/tenants/cust_42/runtime-bindings/DATABASE_URL", ` {"value":"write-only","name":"Primary database"} `, "/v1/tenants/cust_42/runtime-bindings/DATABASE_URL", `{"tenant_id":"cust_42","role":"owner","binding":` + safeRuntimeBindingJSON + `}`},
		{http.MethodDelete, "/api/tenants/cust_42/runtime-bindings/DATABASE_URL", "", "/v1/tenants/cust_42/runtime-bindings/DATABASE_URL", `{"key":"DATABASE_URL","deleted":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			up, got := upstreamRecorder(t, http.StatusOK, tt.response)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer caller")
			req.Header.Set("Cookie", "browser=session")
			if tt.method == http.MethodPut {
				req.Header.Set("Content-Type", "application/json")
			}
			rr := do(h, req)
			if rr.Code != http.StatusOK || got.method != tt.method || got.path != tt.upstream {
				t.Fatalf("response %d %s; upstream %s %s", rr.Code, rr.Body.String(), got.method, got.path)
			}
			if tt.method == http.MethodPut && got.body != `{"name":"Primary database","value":"write-only"}` {
				t.Fatalf("upstream body = %q", got.body)
			}
			if got.header.Get("Authorization") != "Bearer caller" || got.header.Get("Cookie") != "" {
				t.Fatalf("upstream headers = %v", got.header)
			}
		})
	}
}

func TestRuntimeBindingRoutesRejectInvalidRequestsLocally(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"tenant_id":"cust_42","role":"owner","bindings":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	cases := []struct{ method, path, contentType, body string }{
		{http.MethodGet, "/api/tenants/cust_42/runtime-bindings?debug=1", "", ""},
		{http.MethodGet, "/api/tenants/cust_42/runtime-bindings", "application/json", `{}`},
		{http.MethodPut, "/api/tenants/cust_42/runtime-bindings/lowercase", "application/json", `{"name":"N","value":"x"}`},
		{http.MethodPut, "/api/tenants/cust_42/runtime-bindings/KEY", "text/plain", `{"name":"N","value":"x"}`},
		{http.MethodPut, "/api/tenants/cust_42/runtime-bindings/KEY", "application/json; charset=utf-8", `{"name":"N","value":"x"}`},
		{http.MethodPut, "/api/tenants/cust_42/runtime-bindings/KEY", "application/json", `{"name":"N","value":"x","extra":true}`},
		{http.MethodPut, "/api/tenants/cust_42/runtime-bindings/KEY", "application/json", `{"name":"N","value":"x","value":"y"}`},
		{http.MethodPut, "/api/tenants/cust_42/runtime-bindings/KEY", "application/json", `{"name":"N","value":""}`},
		{http.MethodPut, "/api/tenants/cust_42/runtime-bindings/KEY", "application/json", `{"name":"N","value":"a\u0000b"}`},
		{http.MethodPut, "/api/tenants/cust_42/runtime-bindings/KEY", "application/json", `{"name":"` + strings.Repeat("é", 51) + `","value":"x"}`},
		{http.MethodPut, "/api/tenants/cust_42/runtime-bindings/KEY", "application/json", `{"name":"N","value":"` + strings.Repeat("x", 8193) + `"}`},
		{http.MethodDelete, "/api/tenants/cust_42/runtime-bindings/KEY", "", `{}`},
	}
	for _, tc := range cases {
		got.path = ""
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer caller")
		if tc.contentType != "" {
			req.Header.Set("Content-Type", tc.contentType)
		}
		if rr := do(h, req); rr.Code < 400 || rr.Code >= 500 {
			t.Fatalf("%s %s = %d, want local 4xx", tc.method, tc.path, rr.Code)
		}
		if got.path != "" {
			t.Fatalf("rejected request reached upstream as %q", got.path)
		}
	}
}

func TestRuntimeBindingProxyRejectsSecretBearingResponses(t *testing.T) {
	unsafe := []string{
		`{"tenant_id":"cust_42","role":"owner","bindings":[],"value":"leak"}`,
		`{"tenant_id":"cust_42","role":"owner","bindings":[{"id":"rb","key":"KEY","name":"N","revision":"1","sync_state":"synced","created_at":"2026-08-16T09:00:00Z","updated_at":"2026-08-16T09:00:00Z","ciphertext":"sealed"}]}`,
		`{"tenant_id":"cust_42","role":"owner","bindings":[],"nonce":"n"}`,
		`{"tenant_id":"cust_42","role":"owner","bindings":[],"hash":"h"}`,
		`{"error":"failed","secret_detail":"leak"}`,
		`{"tenant_id":"cust_42","role":"owner","bindings":[{"id":"rb","key":"KEY","name":"N","revision":"01","sync_state":"synced","created_at":"2026-08-16T09:00:00Z","updated_at":"2026-08-16T09:00:00Z"}]}`,
	}
	for _, body := range unsafe {
		status := http.StatusOK
		if strings.Contains(body, `"error"`) {
			status = http.StatusConflict
		}
		up, _ := upstreamRecorder(t, status, body)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/runtime-bindings", nil)
		req.Header.Set("Authorization", "Bearer caller")
		rr := do(h, req)
		if rr.Code != http.StatusBadGateway || strings.Contains(rr.Body.String(), "leak") || strings.Contains(rr.Body.String(), "sealed") {
			t.Fatalf("unsafe response became %d %q", rr.Code, rr.Body.String())
		}
	}
}
