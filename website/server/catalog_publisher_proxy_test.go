package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const safePublisher = `{"id":"ci-main","name":"Main CI","created_at":"2026-08-16T10:00:00Z","updated_at":"2026-08-16T10:00:00Z"}`

func TestCatalogPublisherRoutesForwardExactTransport(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"publishers":[`+safePublisher+`]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/catalog-publishers", nil)
	req.Header.Set("Authorization", "Bearer publisher-token")
	req.Header.Set("Cookie", "browser=session")
	rr := do(h, req)
	if rr.Code != http.StatusOK || got.method != http.MethodGet || got.path != "/v1/tenants/cust_42/catalog-publishers" {
		t.Fatalf("list = %d, upstream %s %s, body %s", rr.Code, got.method, got.path, rr.Body.String())
	}
	if got.header.Get("Authorization") != "Bearer publisher-token" || got.header.Get("Cookie") != "" {
		t.Fatalf("upstream headers = %v", got.header)
	}

	up.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := `{"publisher":` + safePublisher + `,"token":"once-token"}`
		got.method, got.path, got.header = r.Method, r.URL.Path, r.Header.Clone()
		body, _ := io.ReadAll(r.Body)
		got.body = string(body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(raw))
	})
	create := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/catalog-publishers", strings.NewReader(` {"name":" Main CI "} `))
	create.Header.Set("Authorization", "Bearer publisher-token")
	create.Header.Set("Content-Type", "application/json")
	rr = do(h, create)
	if rr.Code != http.StatusCreated || got.path != "/v1/tenants/cust_42/catalog-publishers" || got.body != `{"name":"Main CI"}` {
		t.Fatalf("create = %d, upstream %s body %q; response %s", rr.Code, got.path, got.body, rr.Body.String())
	}

	rotate := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/catalog-publishers/ci-main/credential", nil)
	rotate.Header.Set("Authorization", "Bearer publisher-token")
	rr = do(h, rotate)
	if rr.Code != http.StatusCreated || got.path != "/v1/tenants/cust_42/catalog-publishers/ci-main/credential" || got.body != "" {
		t.Fatalf("rotate = %d, upstream %s body %q", rr.Code, got.path, got.body)
	}

	up.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.method, got.path, got.header = r.Method, r.URL.Path, r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"publisher_id":"ci-main","deleted":true}`))
	})
	del := httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_42/catalog-publishers/ci-main", nil)
	del.Header.Set("Authorization", "Bearer publisher-token")
	rr = do(h, del)
	if rr.Code != http.StatusOK || got.method != http.MethodDelete || got.path != "/v1/tenants/cust_42/catalog-publishers/ci-main" {
		t.Fatalf("delete = %d, upstream %s %s", rr.Code, got.method, got.path)
	}
}

func TestCatalogPublisherRoutesRejectInvalidRequests(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"publishers":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	tests := []struct {
		name, method, path, contentType, body string
		want                                  int
	}{
		{"bad tenant", http.MethodGet, "/api/tenants/-bad/catalog-publishers", "", "", 400},
		{"query", http.MethodGet, "/api/tenants/cust_42/catalog-publishers?all=1", "", "", 400},
		{"missing bearer", http.MethodGet, "/api/tenants/cust_42/catalog-publishers", "", "", 401},
		{"bad publisher", http.MethodPost, "/api/tenants/cust_42/catalog-publishers/bad%2Fid/credential", "", "", 400},
		{"missing content type", http.MethodPost, "/api/tenants/cust_42/catalog-publishers", "", `{"name":"CI"}`, 415},
		{"charset content type", http.MethodPost, "/api/tenants/cust_42/catalog-publishers", "application/json; charset=utf-8", `{"name":"CI"}`, 415},
		{"unknown field", http.MethodPost, "/api/tenants/cust_42/catalog-publishers", "application/json", `{"name":"CI","role":"owner"}`, 400},
		{"duplicate field", http.MethodPost, "/api/tenants/cust_42/catalog-publishers", "application/json", `{"name":"CI","name":"other"}`, 400},
		{"trailing", http.MethodPost, "/api/tenants/cust_42/catalog-publishers", "application/json", `{"name":"CI"}{}`, 400},
		{"empty name", http.MethodPost, "/api/tenants/cust_42/catalog-publishers", "application/json", `{"name":" "}`, 400},
		{"name over 100 bytes", http.MethodPost, "/api/tenants/cust_42/catalog-publishers", "application/json", `{"name":"` + strings.Repeat("é", 51) + `"}`, 400},
		{"oversize", http.MethodPost, "/api/tenants/cust_42/catalog-publishers", "application/json", `{"name":"` + strings.Repeat("x", maxPublisherJSONBytes) + `"}`, 413},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			if tt.name != "missing bearer" {
				req.Header.Set("Authorization", "Bearer token")
			}
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			if rr := do(h, req); rr.Code != tt.want {
				t.Fatalf("status = %d, want %d; %s", rr.Code, tt.want, rr.Body.String())
			}
		})
	}
	if got.path != "" {
		t.Fatalf("invalid request reached upstream as %q", got.path)
	}
}

func TestCatalogPublisherProxyRejectsUnsafeSuccessAndRedirect(t *testing.T) {
	unsafe := []string{
		`{"publishers":[{"id":"ci","name":"CI","created_at":"2026-08-16T10:00:00Z","updated_at":"2026-08-16T10:00:00Z","token":"leak"}]}`,
		`{"publishers":[],"token":"leak"}`,
		`{"publisher":` + safePublisher + `,"token":"once","command":"curl bad.example"}`,
		`{"publisher_id":"ci","deleted":true,"token":"leak"}`,
	}
	for i, body := range unsafe {
		up, _ := upstreamRecorder(t, http.StatusOK, body)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		path, method := "/api/tenants/cust/catalog-publishers", http.MethodGet
		if i == 2 {
			path, method = "/api/tenants/cust/catalog-publishers/ci/credential", http.MethodPost
		} else if i == 3 {
			path, method = "/api/tenants/cust/catalog-publishers/ci", http.MethodDelete
		}
		req := httptest.NewRequest(method, path, nil)
		req.Header.Set("Authorization", "Bearer token")
		if rr := do(h, req); rr.Code != http.StatusBadGateway {
			t.Fatalf("unsafe %d status = %d; %s", i, rr.Code, rr.Body.String())
		}
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/stolen", http.StatusFound) }))
	defer redirect.Close()
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(redirect.URL, redirect.URL, redirect.Client()))
	req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust/catalog-publishers", nil)
	req.Header.Set("Authorization", "Bearer token")
	if rr := do(h, req); rr.Code != http.StatusBadGateway {
		t.Fatalf("redirect status = %d", rr.Code)
	}
}

func TestAutomationCatalogRoutesAreExactAndStrict(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"templates":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	get := httptest.NewRequest(http.MethodGet, "/v1/automation/tenants/cust_42/templates", nil)
	get.Header.Set("Authorization", "Bearer publisher-token")
	if rr := do(h, get); rr.Code != http.StatusOK || got.path != "/v1/automation/tenants/cust_42/templates" {
		t.Fatalf("GET = %d path %q", rr.Code, got.path)
	}

	put := httptest.NewRequest(http.MethodPut, "/v1/automation/tenants/cust_42/templates", strings.NewReader(` {"templates":[],"catalog_revision":"4"} `))
	put.Header.Set("Authorization", "Bearer publisher-token")
	put.Header.Set("Content-Type", "application/json")
	if rr := do(h, put); rr.Code != http.StatusOK || got.method != http.MethodPut || got.body != `{"catalog_revision":"4","templates":[]}` {
		t.Fatalf("PUT = %d upstream %s body %q", rr.Code, got.method, got.body)
	}

	for _, body := range []string{`{}`, `{"catalog_revision":"4"}`, `{"catalog_revision":"4","templates":[],"role":"owner"}`, `{"catalog_revision":"4","catalog_revision":"5","templates":[]}`, `{"catalog_revision":"4","templates":{}}`} {
		req := httptest.NewRequest(http.MethodPut, "/v1/automation/tenants/cust_42/templates", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer publisher-token")
		req.Header.Set("Content-Type", "application/json")
		if rr := do(h, req); rr.Code != http.StatusBadRequest {
			t.Errorf("body %s status = %d", body, rr.Code)
		}
	}
}
