package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOperatorTenantRoutesForwardExactTransportAndStripHeaders(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusAccepted, `{"changed":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	list := httptest.NewRequest(http.MethodGet, "/api/operator/tenants?limit=25&after=cust_42", nil)
	list.Header.Set("Authorization", "Bearer operator-token")
	list.Header.Set("Cookie", "browser=session")
	list.Header.Set("X-Admin-Token", "must-not-travel")
	list.Header.Set("Content-Type", "text/plain")
	rr := do(h, list)
	if rr.Code != http.StatusAccepted || rr.Body.String() != `{"changed":true}` {
		t.Fatalf("list response = %d %q", rr.Code, rr.Body.String())
	}
	if got.method != http.MethodGet || got.path != "/v1/operator/tenants" || got.query.Encode() != "after=cust_42&limit=25" || got.body != "" {
		t.Fatalf("list upstream = %s %s?%s body %q", got.method, got.path, got.query.Encode(), got.body)
	}
	if got.header.Get("Authorization") != "Bearer operator-token" {
		t.Fatalf("list Authorization = %q", got.header.Get("Authorization"))
	}
	for _, name := range []string{"Cookie", "X-Admin-Token", "Content-Type", "X-Forwarded-For"} {
		if got.header.Get(name) != "" {
			t.Fatalf("list upstream received %s = %q", name, got.header.Get(name))
		}
	}

	patch := httptest.NewRequest(http.MethodPatch, "/api/operator/tenants/cust_42/limits", strings.NewReader(` { "max_hourly_usd": 12.5, "max_concurrent_bursts": 0 } `))
	patch.Header.Set("Authorization", "Bearer operator-token")
	patch.Header.Set("Content-Type", "application/json")
	patch.Header.Set("Cookie", "browser=session")
	patch.Header.Set("X-Tenant-ID", "must-not-travel")
	rr = do(h, patch)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("patch response = %d %q", rr.Code, rr.Body.String())
	}
	if got.method != http.MethodPatch || got.path != "/v1/operator/tenants/cust_42/limits" || len(got.query) != 0 {
		t.Fatalf("patch upstream = %s %s?%s", got.method, got.path, got.query.Encode())
	}
	if got.body != `{"max_concurrent_bursts":0,"max_hourly_usd":12.5}` {
		t.Fatalf("patch rebuilt body = %q", got.body)
	}
	if got.header.Get("Authorization") != "Bearer operator-token" || got.header.Get("Content-Type") != "application/json" {
		t.Fatalf("patch upstream headers = %v", got.header)
	}
	for _, name := range []string{"Cookie", "X-Tenant-ID", "X-Forwarded-For"} {
		if got.header.Get(name) != "" {
			t.Fatalf("patch upstream received %s = %q", name, got.header.Get(name))
		}
	}
}

func TestOperatorTenantInventoryRejectsNonCanonicalQueriesAndBodies(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"tenants":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	tests := []string{
		"/api/operator/tenants?unknown=1",
		"/api/operator/tenants?after=",
		"/api/operator/tenants?after=-bad",
		"/api/operator/tenants?after=cust_42&after=cust_43",
		"/api/operator/tenants?limit=",
		"/api/operator/tenants?limit=0",
		"/api/operator/tenants?limit=101",
		"/api/operator/tenants?limit=01",
		"/api/operator/tenants?limit=5&limit=6",
	}
	for _, path := range tests {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer operator-token")
		if rr := do(h, req); rr.Code != http.StatusBadRequest {
			t.Errorf("GET %s status = %d, want 400; body %s", path, rr.Code, rr.Body.String())
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/api/operator/tenants", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer operator-token")
	if rr := do(h, req); rr.Code != http.StatusBadRequest {
		t.Errorf("GET body status = %d, want 400", rr.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/api/operator/tenants", nil)
	req.URL.RawQuery = "after=%zz"
	req.Header.Set("Authorization", "Bearer operator-token")
	if rr := do(h, req); rr.Code != http.StatusBadRequest {
		t.Errorf("malformed query status = %d, want 400", rr.Code)
	}
	if got.path != "" {
		t.Fatalf("invalid inventory request reached upstream as %q", got.path)
	}
}

func TestOperatorTenantLimitsRejectInvalidTransportAndJSON(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"changed":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	tests := []struct {
		name        string
		path        string
		contentType string
		body        string
		duplicateCT bool
		want        int
	}{
		{name: "invalid tenant", path: "/api/operator/tenants/-bad/limits", contentType: "application/json", body: `{"max_concurrent_bursts":1,"max_hourly_usd":2}`, want: http.StatusBadRequest},
		{name: "query", path: "/api/operator/tenants/cust_42/limits?force=1", contentType: "application/json", body: `{"max_concurrent_bursts":1,"max_hourly_usd":2}`, want: http.StatusBadRequest},
		{name: "missing content type", path: "/api/operator/tenants/cust_42/limits", body: `{}`, want: http.StatusUnsupportedMediaType},
		{name: "charset content type", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json; charset=utf-8", body: `{}`, want: http.StatusUnsupportedMediaType},
		{name: "duplicate content type", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", duplicateCT: true, body: `{}`, want: http.StatusUnsupportedMediaType},
		{name: "empty", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", want: http.StatusBadRequest},
		{name: "null", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `null`, want: http.StatusBadRequest},
		{name: "array", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `[]`, want: http.StatusBadRequest},
		{name: "missing field", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":1}`, want: http.StatusBadRequest},
		{name: "unknown field", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":1,"max_hourly_usd":2,"role":"owner"}`, want: http.StatusBadRequest},
		{name: "duplicate field", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":1,"max_concurrent_bursts":2,"max_hourly_usd":2}`, want: http.StatusBadRequest},
		{name: "trailing JSON", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":1,"max_hourly_usd":2} {}`, want: http.StatusBadRequest},
		{name: "string number", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":"1","max_hourly_usd":2}`, want: http.StatusBadRequest},
		{name: "fractional concurrent limit", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":1.5,"max_hourly_usd":2}`, want: http.StatusBadRequest},
		{name: "null number", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":null,"max_hourly_usd":2}`, want: http.StatusBadRequest},
		{name: "negative number", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":-1,"max_hourly_usd":2}`, want: http.StatusBadRequest},
		{name: "non finite exponent", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":1e999,"max_hourly_usd":2}`, want: http.StatusBadRequest},
		{name: "NaN token", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":NaN,"max_hourly_usd":2}`, want: http.StatusBadRequest},
		{name: "oversize", path: "/api/operator/tenants/cust_42/limits", contentType: "application/json", body: `{"max_concurrent_bursts":1,"max_hourly_usd":2,"padding":"` + strings.Repeat("x", maxOperatorLimitsJSONBytes) + `"}`, want: http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPatch, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer operator-token")
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			if tt.duplicateCT {
				req.Header.Add("Content-Type", "application/json")
			}
			if rr := do(h, req); rr.Code != tt.want {
				t.Fatalf("status = %d, want %d; body %s", rr.Code, tt.want, rr.Body.String())
			}
		})
	}
	if got.path != "" {
		t.Fatalf("invalid limit request reached upstream as %q", got.path)
	}
}

func TestOperatorTenantRoutesRequireBearerAndPassThroughStatus(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusConflict, `{"error":"central-conflict"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/operator/tenants", nil),
		httptest.NewRequest(http.MethodPatch, "/api/operator/tenants/cust_42/limits", strings.NewReader(`{"max_concurrent_bursts":1,"max_hourly_usd":2}`)),
	} {
		if req.Method == http.MethodPatch {
			req.Header.Set("Content-Type", "application/json")
		}
		if rr := do(h, req); rr.Code != http.StatusUnauthorized {
			t.Fatalf("%s status = %d, want 401", req.URL.Path, rr.Code)
		}
	}
	if got.path != "" {
		t.Fatalf("unauthenticated request reached upstream as %q", got.path)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/operator/tenants?limit=10", nil)
	req.Header.Set("Authorization", "Bearer operator-token")
	rr := do(h, req)
	if rr.Code != http.StatusConflict || rr.Body.String() != `{"error":"central-conflict"}` {
		t.Fatalf("passthrough response = %d %q", rr.Code, rr.Body.String())
	}
}
