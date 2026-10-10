package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func hostedClusterRequest(path, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	return req
}

func TestOperatorHostedCapacityRoutesForwardBearerAndExactPaths(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"requests":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	list := httptest.NewRequest(http.MethodGet, "/api/operator/hosted-capacity/requests", nil)
	list.Header.Set("Authorization", "Bearer operator-token")
	list.Header.Set("Cookie", "browser=session")
	list.Header.Set("X-Admin-Token", "must-not-travel")
	if rr := do(h, list); rr.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200; body %s", rr.Code, rr.Body.String())
	}
	if got.method != http.MethodGet || got.path != "/v1/operator/hosted-capacity/requests" || len(got.query) != 0 {
		t.Fatalf("list upstream = %s %s?%s", got.method, got.path, got.query.Encode())
	}
	if got.header.Get("Authorization") != "Bearer operator-token" {
		t.Fatalf("list Authorization = %q", got.header.Get("Authorization"))
	}
	for _, name := range []string{"Cookie", "X-Admin-Token", "X-Forwarded-For"} {
		if got.header.Get(name) != "" {
			t.Fatalf("list upstream received %s = %q", name, got.header.Get(name))
		}
	}

	create := hostedClusterRequest("/api/operator/tenants/cust_42/hosted-clusters", `{"namespace":"yscale-system","name":"edge one","cluster_id":"hc_1"}`)
	create.Header.Set("Authorization", "Bearer operator-token")
	create.Header.Set("Cookie", "browser=session")
	if rr := do(h, create); rr.Code != http.StatusOK {
		t.Fatalf("create status = %d, want 200; body %s", rr.Code, rr.Body.String())
	}
	if got.method != http.MethodPost || got.path != "/v1/operator/tenants/cust_42/hosted-clusters" || len(got.query) != 0 {
		t.Fatalf("create upstream = %s %s?%s", got.method, got.path, got.query.Encode())
	}
	if got.body != `{"cluster_id":"hc_1","name":"edge one","namespace":"yscale-system"}` {
		t.Fatalf("rebuilt body = %q", got.body)
	}
	if got.header.Get("Authorization") != "Bearer operator-token" || got.header.Get("Content-Type") != "application/json" {
		t.Fatalf("create upstream headers = %v", got.header)
	}
	if got.header.Get("Cookie") != "" {
		t.Fatalf("create upstream received Cookie = %q", got.header.Get("Cookie"))
	}
}

func TestOperatorHostedCapacityRoutesRejectMethodAndQuery(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	tests := []struct {
		method string
		path   string
		body   string
		want   int
	}{
		{method: http.MethodPost, path: "/api/operator/hosted-capacity/requests", want: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/api/operator/tenants/cust_42/hosted-clusters", want: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/api/operator/hosted-capacity/requests?limit=1", want: http.StatusBadRequest},
		{method: http.MethodPost, path: "/api/operator/tenants/cust_42/hosted-clusters?dry_run=true", body: `{}`, want: http.StatusBadRequest},
		{method: http.MethodPost, path: "/api/operator/tenants", want: http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
		req.Header.Set("Authorization", "Bearer operator-token")
		if tt.method == http.MethodPost {
			req.Header.Set("Content-Type", "application/json")
		}
		if rr := do(h, req); rr.Code != tt.want && !(tt.want == http.StatusMethodNotAllowed && rr.Code == http.StatusNotFound) {
			t.Fatalf("%s %s: status = %d, want %d (or 404 for an unrouted method); body %s", tt.method, tt.path, rr.Code, tt.want, rr.Body.String())
		}
	}
	if got.path != "" {
		t.Fatalf("refused request reached upstream as %q", got.path)
	}
}

func TestOperatorHostedClusterRejectsInvalidTransportAndJSON(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusCreated, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	tests := []struct {
		name        string
		path        string
		contentType string
		body        string
		want        int
	}{
		{name: "invalid tenant", path: "/api/operator/tenants/-bad/hosted-clusters", contentType: "application/json", body: `{}`, want: http.StatusBadRequest},
		{name: "missing content type", path: "/api/operator/tenants/cust_42/hosted-clusters", body: `{}`, want: http.StatusUnsupportedMediaType},
		{name: "wrong content type", path: "/api/operator/tenants/cust_42/hosted-clusters", contentType: "text/plain", body: `{}`, want: http.StatusUnsupportedMediaType},
		{name: "empty body", path: "/api/operator/tenants/cust_42/hosted-clusters", contentType: "application/json", want: http.StatusBadRequest},
		{name: "null body", path: "/api/operator/tenants/cust_42/hosted-clusters", contentType: "application/json", body: `null`, want: http.StatusBadRequest},
		{name: "malformed JSON", path: "/api/operator/tenants/cust_42/hosted-clusters", contentType: "application/json", body: `{"name":`, want: http.StatusBadRequest},
		{name: "trailing JSON", path: "/api/operator/tenants/cust_42/hosted-clusters", contentType: "application/json", body: `{} {}`, want: http.StatusBadRequest},
		{name: "unknown field", path: "/api/operator/tenants/cust_42/hosted-clusters", contentType: "application/json", body: `{"provider":"admin"}`, want: http.StatusBadRequest},
		{name: "non-string field", path: "/api/operator/tenants/cust_42/hosted-clusters", contentType: "application/json", body: `{"cluster_id":7}`, want: http.StatusBadRequest},
		{name: "null field", path: "/api/operator/tenants/cust_42/hosted-clusters", contentType: "application/json", body: `{"name":null}`, want: http.StatusBadRequest},
		{name: "oversize", path: "/api/operator/tenants/cust_42/hosted-clusters", contentType: "application/json", body: `{"name":"` + strings.Repeat("x", maxHostedClusterJSONBytes) + `"}`, want: http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer operator-token")
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			if rr := do(h, req); rr.Code != tt.want {
				t.Fatalf("status = %d, want %d; body %s", rr.Code, tt.want, rr.Body.String())
			}
		})
	}
	if got.path != "" {
		t.Fatalf("invalid request reached upstream as %q", got.path)
	}
}

func TestOperatorRoutesRequireCallerBearer(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/operator/hosted-capacity/requests", nil),
		hostedClusterRequest("/api/operator/tenants/cust_42/hosted-clusters", `{}`),
	} {
		if rr := do(h, req); rr.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d, want 401", req.URL.Path, rr.Code)
		}
	}
	if got.path != "" {
		t.Fatalf("unauthenticated request reached upstream as %q", got.path)
	}
}

func TestOperatorRouteRefusesUpstreamRedirect(t *testing.T) {
	reached := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		reached = true
	}))
	t.Cleanup(target.Close)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL+"/credential")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirect.Close)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(redirect.URL, redirect.URL, redirect.Client()))

	req := httptest.NewRequest(http.MethodGet, "/api/operator/hosted-capacity/requests", nil)
	req.Header.Set("Authorization", "Bearer operator-token")
	if rr := do(h, req); rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body %s", rr.Code, rr.Body.String())
	}
	if reached {
		t.Fatal("redirect target was reached with the operator request")
	}
}

func TestOperatorRouteRelaysSafeStatuses(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			up, _ := upstreamRecorder(t, status, `{"status":"central"}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(http.MethodGet, "/api/operator/hosted-capacity/requests", nil)
			req.Header.Set("Authorization", "Bearer operator-token")
			rr := do(h, req)
			if rr.Code != status || rr.Body.String() != `{"status":"central"}` {
				t.Fatalf("response = %d %q, want %d with upstream JSON", rr.Code, rr.Body.String(), status)
			}
		})
	}
}

func TestOperatorHostedClusterCredentialResponsePassesThroughOnce(t *testing.T) {
	const credential = `{"cluster_id":"hc_1","credential":{"token":"one-time-secret","server":"https://connect.example"}}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Set-Cookie", "admin=secret")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(credential)) //nolint:errcheck
	}))
	t.Cleanup(up.Close)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := hostedClusterRequest("/api/operator/tenants/cust_42/hosted-clusters", `{"cluster_id":"hc_1"}`)
	req.Header.Set("Authorization", "Bearer operator-token")
	rr := do(h, req)
	if rr.Code != http.StatusCreated || rr.Body.String() != credential {
		t.Fatalf("credential response = %d %q", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Set-Cookie") != "" || rr.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("response headers = %v", rr.Header())
	}
}

func TestOperatorHostedFleetRoutesForwardExactPathsAndStripCallerHeaders(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"clusters":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	tests := []struct {
		method string
		path   string
		want   string
	}{
		{http.MethodGet, "/api/operator/hosted-clusters", "/v1/operator/hosted-clusters"},
		{http.MethodPost, "/api/operator/tenants/cust_42/hosted-clusters/hc_1/credential", "/v1/operator/tenants/cust_42/hosted-clusters/hc_1/credential"},
		{http.MethodDelete, "/api/operator/tenants/cust_42/hosted-clusters/hc_1", "/v1/operator/tenants/cust_42/hosted-clusters/hc_1"},
	}
	for _, tt := range tests {
		req := httptest.NewRequest(tt.method, tt.path, nil)
		req.Header.Set("Authorization", "Bearer operator-token")
		req.Header.Set("Cookie", "browser=session")
		req.Header.Set("X-Admin-Token", "must-not-travel")
		req.Header.Set("X-Tenant-ID", "must-not-travel")
		if tt.method != http.MethodPost {
			req.Header.Set("Content-Type", "text/plain")
		}
		if rr := do(h, req); rr.Code != http.StatusOK {
			t.Fatalf("%s %s: status = %d, body %s", tt.method, tt.path, rr.Code, rr.Body.String())
		}
		if got.method != tt.method || got.path != tt.want || got.body != "" || len(got.query) != 0 {
			t.Fatalf("upstream = %s %s?%s body %q", got.method, got.path, got.query.Encode(), got.body)
		}
		if got.header.Get("Authorization") != "Bearer operator-token" {
			t.Fatalf("Authorization = %q", got.header.Get("Authorization"))
		}
		for _, name := range []string{"Cookie", "X-Admin-Token", "X-Tenant-ID", "Content-Type", "X-Forwarded-For"} {
			if got.header.Get(name) != "" {
				t.Fatalf("%s upstream received %s = %q", tt.path, name, got.header.Get(name))
			}
		}
	}
}

func TestOperatorHostedFleetRoutesRejectInvalidTransport(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{"inventory query", http.MethodGet, "/api/operator/hosted-clusters?limit=1", "", http.StatusBadRequest},
		{"inventory body", http.MethodGet, "/api/operator/hosted-clusters", `{}`, http.StatusBadRequest},
		{"rotate query", http.MethodPost, "/api/operator/tenants/cust_42/hosted-clusters/hc_1/credential?force=1", "", http.StatusBadRequest},
		{"rotate body", http.MethodPost, "/api/operator/tenants/cust_42/hosted-clusters/hc_1/credential", `{}`, http.StatusBadRequest},
		{"rotate content type", http.MethodPost, "/api/operator/tenants/cust_42/hosted-clusters/hc_1/credential", "", http.StatusUnsupportedMediaType},
		{"delete query", http.MethodDelete, "/api/operator/tenants/cust_42/hosted-clusters/hc_1?force=1", "", http.StatusBadRequest},
		{"delete body", http.MethodDelete, "/api/operator/tenants/cust_42/hosted-clusters/hc_1", `{}`, http.StatusBadRequest},
		{"invalid tenant", http.MethodDelete, "/api/operator/tenants/-bad/hosted-clusters/hc_1", "", http.StatusBadRequest},
		{"invalid cluster", http.MethodPost, "/api/operator/tenants/cust_42/hosted-clusters/-bad/credential", "", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer operator-token")
			if tt.name == "rotate content type" {
				req.Header.Set("Content-Type", "application/json")
			}
			if rr := do(h, req); rr.Code != tt.want {
				t.Fatalf("status = %d, want %d; body %s", rr.Code, tt.want, rr.Body.String())
			}
		})
	}
	if got.path != "" {
		t.Fatalf("refused request reached upstream as %q", got.path)
	}
}

func TestOperatorHostedFleetRoutesRequireBearerAndRelayCredential(t *testing.T) {
	const credential = `{"tenant_id":"cust_42","cluster":{"cluster_id":"hc_1"},"connector_token":"one-time"}`
	up, got := upstreamRecorder(t, http.StatusCreated, credential)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/operator/hosted-clusters", nil),
		httptest.NewRequest(http.MethodPost, "/api/operator/tenants/cust_42/hosted-clusters/hc_1/credential", nil),
		httptest.NewRequest(http.MethodDelete, "/api/operator/tenants/cust_42/hosted-clusters/hc_1", nil),
	} {
		if rr := do(h, req); rr.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d, want 401", req.URL.Path, rr.Code)
		}
	}
	if got.path != "" {
		t.Fatalf("unauthenticated request reached upstream as %q", got.path)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/operator/tenants/cust_42/hosted-clusters/hc_1/credential", nil)
	req.Header.Set("Authorization", "Bearer operator-token")
	rr := do(h, req)
	if rr.Code != http.StatusCreated || rr.Body.String() != credential {
		t.Fatalf("credential response = %d %q", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q", rr.Header().Get("Cache-Control"))
	}
}

func TestOperatorHostedFleetRelaysSafeStatuses(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			up, _ := upstreamRecorder(t, status, `{"status":"central"}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(http.MethodDelete, "/api/operator/tenants/cust_42/hosted-clusters/hc_1", nil)
			req.Header.Set("Authorization", "Bearer operator-token")
			rr := do(h, req)
			if rr.Code != status || rr.Body.String() != `{"status":"central"}` {
				t.Fatalf("response = %d %q, want %d with upstream JSON", rr.Code, rr.Body.String(), status)
			}
		})
	}
}
