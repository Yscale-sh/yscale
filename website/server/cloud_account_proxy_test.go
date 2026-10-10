package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const safeLinodeAccount = `{"tenant_id":"cust_42","role":"owner","account":{"id":"ca_1","provider":"linode","provider_account_id":"linode-acct-1","region":"us-east","cpu_image_ready":true,"gpu_image_ready":false,"updated_at":"2026-08-16T12:00:00Z"},"changed":true}`

func TestLinodeCloudAccountProxyContracts(t *testing.T) {
	tests := []struct {
		method, body, contentType string
	}{
		{method: http.MethodGet},
		{method: http.MethodPut, body: `{"token":"write-only-value","region":"us-east","cpu_image":"linode/ubuntu24.04","gpu_image":"private/yscale-gpu"}`, contentType: "application/json"},
		{method: http.MethodDelete},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			up, got := upstreamRecorder(t, http.StatusOK, safeLinodeAccount)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(tt.method, "/api/tenants/cust_42/cloud-accounts/linode", strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer caller-value")
			req.Header.Set("Cookie", "session=browser-cookie")
			req.Header.Set("X-Forwarded-For", "10.0.0.1")
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType+"; charset=utf-8")
			}
			rr := do(h, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body %s", rr.Code, rr.Body.String())
			}
			if got.method != tt.method || got.path != "/v1/tenants/cust_42/cloud-accounts/linode" || got.body != tt.body {
				t.Fatalf("upstream saw %s %s body %q", got.method, got.path, got.body)
			}
			if got.header.Get("Authorization") != "Bearer caller-value" || got.header.Get("Accept") != "application/json" {
				t.Fatalf("allowlisted headers = %v", got.header)
			}
			for _, banned := range []string{"Cookie", "X-Forwarded-For"} {
				if got.header.Get(banned) != "" {
					t.Fatalf("upstream received %s", banned)
				}
			}
		})
	}
}

func TestLinodeCloudAccountProxyRejectsBadWritesLocally(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, safeLinodeAccount)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	cases := []struct {
		method, path, body, contentType string
	}{
		{http.MethodGet, "/api/tenants/cust_42/cloud-accounts/linode?debug=1", "", ""},
		{http.MethodPut, "/api/tenants/cust_42/cloud-accounts/linode", `{"token":"x","region":"us-east","extra":true}`, "application/json"},
		{http.MethodPut, "/api/tenants/cust_42/cloud-accounts/linode", `{"token":"x","region":4}`, "application/json"},
		{http.MethodPut, "/api/tenants/cust_42/cloud-accounts/linode", `{"token":" x ","region":"us-east"}`, "application/json"},
		{http.MethodPut, "/api/tenants/cust_42/cloud-accounts/linode", `{"token":"x","region":"us-east"}`, "text/plain"},
		{http.MethodDelete, "/api/tenants/cust_42/cloud-accounts/linode", `{}`, "application/json"},
	}
	for _, tc := range cases {
		got.path = ""
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer caller-value")
		if tc.contentType != "" {
			req.Header.Set("Content-Type", tc.contentType)
		}
		if rr := do(h, req); rr.Code < 400 || rr.Code >= 500 {
			t.Fatalf("%s %s status = %d, want local 4xx", tc.method, tc.path, rr.Code)
		}
		if got.path != "" {
			t.Fatalf("rejected request reached upstream as %q", got.path)
		}
	}
}

func TestLinodeCloudAccountProxyRefusesSecretBearingOrMalformedSuccess(t *testing.T) {
	unsafe := []string{
		`{"tenant_id":"cust_42","role":"owner","account":null,"token":"reflected"}`,
		`{"tenant_id":"cust_42","role":"owner","account":{"id":"ca_1","provider":"linode","provider_account_id":"acct","region":"us-east","cpu_image_ready":true,"gpu_image_ready":false,"updated_at":"now","ciphertext":"sealed"}}`,
		`{"tenant_id":"other","role":"owner","account":null}`,
		`{"tenant_id":"cust_42","role":"owner","account":{"provider":"linode"}}`,
		`{"tenant_id":"cust_42","role":"owner","account":{"id":"ca_1","provider":"linode","provider_account_id":"acct","region":"us-east","cpu_image_ready":true,"gpu_image_ready":false,"updated_at":"today"}}`,
	}
	for _, body := range unsafe {
		up, _ := upstreamRecorder(t, http.StatusOK, body)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/cloud-accounts/linode", nil)
		req.Header.Set("Authorization", "Bearer caller-value")
		rr := do(h, req)
		if rr.Code != http.StatusBadGateway || strings.Contains(rr.Body.String(), "reflected") || strings.Contains(rr.Body.String(), "sealed") {
			t.Fatalf("unsafe upstream response became %d %q", rr.Code, rr.Body.String())
		}
	}
}

func TestLinodeCloudAccountProxyRefusesAnEmptySuccess(t *testing.T) {
	up, _ := upstreamRecorder(t, http.StatusNoContent, "")
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_42/cloud-accounts/linode", nil)
	req.Header.Set("Authorization", "Bearer caller-value")
	if rr := do(h, req); rr.Code != http.StatusBadGateway {
		t.Fatalf("empty success status = %d, want 502", rr.Code)
	}
}

func TestLinodeCloudAccountProxyPreservesSafeConflict(t *testing.T) {
	up, _ := upstreamRecorder(t, http.StatusConflict, `{"error":"cloud_account_in_use","message":"Disconnect is blocked while live bursts or leases exist."}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_42/cloud-accounts/linode", nil)
	req.Header.Set("Authorization", "Bearer caller-value")
	rr := do(h, req)
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), "live bursts") {
		t.Fatalf("status/body = %d %q", rr.Code, rr.Body.String())
	}
}
