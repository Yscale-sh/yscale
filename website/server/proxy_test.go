package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// A key the browser could plausibly have minted: opaque, printable, well inside
// central's 8..255 bounds.
const validIdempotencyKey = "wl_9f2c1a7b4e6d80315c2a9b7e4f1d6038" // gitleaks:allow -- deterministic test fixture

// capture records what an upstream actually received, so the tests can assert
// on the far side of the seam rather than on our own intent.
type capture struct {
	method string
	path   string
	query  url.Values
	header http.Header
	form   url.Values
	body   string
}

type denyLimiter struct{}

func (denyLimiter) allow(string) bool { return false }

func upstreamRecorder(t *testing.T, status int, body string) (*httptest.Server, *capture) {
	t.Helper()
	got := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		got.method = r.Method
		got.path = r.URL.Path
		got.query = r.URL.Query()
		got.header = r.Header.Clone()
		got.form, _ = url.ParseQuery(string(raw))
		got.body = string(raw)
		if body == "" {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write([]byte(body)) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)
	return srv, got
}

func do(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func postForm(path string, form url.Values) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req
}

func memberJSONRequest(method, path, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	return req
}

func tenantJSONRequest(path, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	return req
}

func policyJSONRequest(method, path, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	return req
}

// createRequest builds a workload create the way the console does: text/yaml
// under one key the caller already owns.
func createRequest(path, yaml string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(yaml))
	req.Header.Set("Content-Type", "text/yaml")
	req.Header.Set("Idempotency-Key", validIdempotencyKey)
	return req
}

func validTokenForm() url.Values {
	return url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {"auth-code-123"},
		"redirect_uri":  {"https://yscale.sh/callback"},
		"client_id":     {"yscale-platform"},
		"code_verifier": {"verifier-abc"},
	}
}

// The SPA owns /account and /callback: they must fall through to index.html
// instead of bouncing to Yscale ID, while the other identity paths still 302.
func TestSPAOwnsAccountRoutes(t *testing.T) {
	h, _ := newTestApp(t, "")
	for _, path := range []string{"/account", "/callback", "/workloads", "/workloads/new/pytorch-training", "/workloads/wl_1"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Host = "yscale.sh"
			rr := do(h, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (SPA fallback)", rr.Code)
			}
			if !strings.Contains(rr.Body.String(), "yscale spa") {
				t.Fatalf("body = %q, want the SPA index", rr.Body.String())
			}
		})
	}

	for _, path := range []string{"/access", "/login", "/signup", "/waitlist"} {
		t.Run("still redirects "+path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Host = "yscale.sh"
			if rr := do(h, req); rr.Code != http.StatusFound {
				t.Fatalf("status = %d, want 302", rr.Code)
			}
		})
	}
}

func TestAuthTokenForwardsOnlyAllowlistedFields(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"access_token":"at","id_token":"it","expires_in":3600}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	form := validTokenForm()
	form.Set("client_secret", "should-not-travel")
	form.Set("scope", "openid admin")
	req := postForm("/api/auth/token?refresh_token=nope", form)
	req.Header.Set("Cookie", "session=browser-cookie")

	rr := do(h, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rr.Code, rr.Body.String())
	}
	if got.path != "/token" {
		t.Fatalf("upstream path = %q, want /token", got.path)
	}
	if len(got.form) != 5 {
		t.Fatalf("upstream form = %v, want exactly the 5 allowlisted fields", got.form)
	}
	for _, banned := range []string{"client_secret", "scope", "refresh_token"} {
		if got.form.Get(banned) != "" {
			t.Fatalf("upstream received %q = %q, want it dropped", banned, got.form.Get(banned))
		}
	}
	if got.form.Get("code") != "auth-code-123" || got.form.Get("code_verifier") != "verifier-abc" {
		t.Fatalf("upstream form lost a required field: %v", got.form)
	}
	if got.header.Get("Cookie") != "" {
		t.Fatalf("upstream received a cookie: %q", got.header.Get("Cookie"))
	}
}

func TestAuthTokenRejectsUnsupportedRequests(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"access_token":"at"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	mutate := map[string]func(url.Values){
		"refresh grant":         func(f url.Values) { f.Set("grant_type", "refresh_token") },
		"foreign client":        func(f url.Values) { f.Set("client_id", "another-client") },
		"missing verifier":      func(f url.Values) { f.Del("code_verifier") },
		"missing code":          func(f url.Values) { f.Del("code") },
		"foreign redirect path": func(f url.Values) { f.Set("redirect_uri", "https://evil.example/steal") },
		"foreign callback host": func(f url.Values) { f.Set("redirect_uri", "https://evil.example/callback") },
		"relative redirect":     func(f url.Values) { f.Set("redirect_uri", "/callback") },
		"redirect with query":   func(f url.Values) { f.Set("redirect_uri", "https://yscale.sh/callback?next=/admin") },
		"oversized field":       func(f url.Values) { f.Set("code", strings.Repeat("a", maxFormFieldLen+1)) },
	}
	for name, fn := range mutate {
		t.Run(name, func(t *testing.T) {
			form := validTokenForm()
			fn(form)
			if rr := do(h, postForm("/api/auth/token", form)); rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rr.Code)
			}
			if got.path != "" {
				t.Fatalf("upstream was called for a rejected request")
			}
		})
	}
}

func TestAuthTokenCapsRequestBody(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"access_token":"at"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	form := validTokenForm()
	form.Set("code", strings.Repeat("a", maxTokenReqBytes))
	rr := do(h, postForm("/api/auth/token", form))
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body %s", rr.Code, rr.Body.String())
	}
	if got.path != "" {
		t.Fatalf("upstream was called with an oversized body")
	}
}

func TestAuthTokenUpstreamFailures(t *testing.T) {
	t.Run("unreachable", func(t *testing.T) {
		down := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		client := down.Client()
		down.Close() // nothing is listening now
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(down.URL, down.URL, client))
		if rr := do(h, postForm("/api/auth/token", validTokenForm())); rr.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", rr.Code)
		}
	})

	t.Run("upstream error passes through", func(t *testing.T) {
		up, _ := upstreamRecorder(t, http.StatusBadRequest, `{"error":"invalid_grant"}`)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		rr := do(h, postForm("/api/auth/token", validTokenForm()))
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "invalid_grant") {
			t.Fatalf("body = %q, want the upstream JSON error", rr.Body.String())
		}
	})

	t.Run("non-JSON upstream is not relayed", func(t *testing.T) {
		up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("<html>gateway timeout</html>")) //nolint:errcheck
		}))
		t.Cleanup(up.Close)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		rr := do(h, postForm("/api/auth/token", validTokenForm()))
		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want the upstream 500", rr.Code)
		}
		if strings.Contains(rr.Body.String(), "<html>") {
			t.Fatalf("body = %q, want the HTML dropped", rr.Body.String())
		}
	})
}

func TestUnconfiguredUpstreamsReturn503(t *testing.T) {
	h, _ := newTestApp(t, "") // proxy built with empty URLs
	cases := []struct {
		name string
		req  *http.Request
	}{
		{"token", postForm("/api/auth/token", validTokenForm())},
		{"account", httptest.NewRequest(http.MethodGet, "/api/account", nil)},
		{"create tenant", tenantJSONRequest("/api/account/tenants", `{"name":"First Workspace"}`)},
		{"members", httptest.NewRequest(http.MethodGet, "/api/tenants/t1/members", nil)},
		{"add member", memberJSONRequest(http.MethodPost, "/api/tenants/t1/members", `{"email":"person@example.com","role":"member"}`)},
		{"update member", memberJSONRequest(http.MethodPatch, "/api/tenants/t1/members/a1", `{"role":"admin"}`)},
		{"audit", httptest.NewRequest(http.MethodGet, "/api/tenants/t1/audit", nil)},
		{"usage", httptest.NewRequest(http.MethodGet, "/api/tenants/t1/usage", nil)},
		{"remove", httptest.NewRequest(http.MethodDelete, "/api/tenants/t1/members/a1", nil)},
		{"clusters", httptest.NewRequest(http.MethodGet, "/api/tenants/t1/clusters", nil)},
		{"register cluster", policyJSONRequest(http.MethodPost, "/api/tenants/t1/clusters", `{"name":"prod"}`)},
		{"rotate cluster credential", httptest.NewRequest(http.MethodPost, "/api/tenants/t1/clusters/c1/credential", nil)},
		{"delete cluster", httptest.NewRequest(http.MethodDelete, "/api/tenants/t1/clusters/c1", nil)},
		{"cluster policy", httptest.NewRequest(http.MethodGet, "/api/tenants/t1/cluster-policy", nil)},
		{"put cluster policy", policyJSONRequest(http.MethodPut, "/api/tenants/t1/cluster-policy", `{"allow":[],"deny":[],"auto":"require_pin"}`)},
		{"templates", httptest.NewRequest(http.MethodGet, "/api/tenants/t1/templates", nil)},
		{"put templates", policyJSONRequest(http.MethodPut, "/api/tenants/t1/templates", `{"templates":[]}`)},
		{"gitops sources", httptest.NewRequest(http.MethodGet, "/api/tenants/t1/gitops/sources", nil)},
		{"put gitops sources", policyJSONRequest(http.MethodPut, "/api/tenants/t1/gitops/sources", `{"sources_revision":"1","sources":[]}`)},
		{"workloads", httptest.NewRequest(http.MethodGet, "/api/tenants/t1/workloads", nil)},
		{"create workload", createRequest("/api/tenants/t1/workloads", "kind: Workload\n")},
		{"workload", httptest.NewRequest(http.MethodGet, "/api/tenants/t1/workloads/w1", nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name != "token" {
				tc.req.Header.Set("Authorization", "Bearer token-value")
			}
			rr := do(h, tc.req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503; body %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "not configured") {
				t.Fatalf("body = %q, want a clear configuration error", rr.Body.String())
			}
		})
	}
}

func TestUnconfiguredAccountDoesNotMasqueradeAsMissingAuth(t *testing.T) {
	h, _ := newTestApp(t, "")
	req := httptest.NewRequest(http.MethodGet, "/api/account", nil)
	rr := do(h, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body %s", rr.Code, rr.Body.String())
	}
}

func TestAccountRoutesAreRateLimitedBeforeUpstream(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxyLimiter(t, "", newAccountProxy(up.URL, up.URL, up.Client()), denyLimiter{})

	cases := []*http.Request{
		postForm("/api/auth/token", validTokenForm()),
		httptest.NewRequest(http.MethodGet, "/api/account", nil),
		tenantJSONRequest("/api/account/tenants", `{"name":"First Workspace"}`),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_1/members", nil),
		memberJSONRequest(http.MethodPost, "/api/tenants/cust_1/members", `{"email":"person@example.com","role":"member"}`),
		memberJSONRequest(http.MethodPatch, "/api/tenants/cust_1/members/acct_1", `{"role":"admin"}`),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_1/audit", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_1/usage", nil),
		httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_1/members/acct_1", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_1/clusters", nil),
		policyJSONRequest(http.MethodPost, "/api/tenants/cust_1/clusters", `{"name":"prod"}`),
		httptest.NewRequest(http.MethodPost, "/api/tenants/cust_1/clusters/c1/credential", nil),
		httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_1/clusters/c1", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_1/cluster-policy", nil),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_1/cluster-policy", `{"allow":[],"deny":[],"auto":"require_pin"}`),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_1/templates", nil),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_1/templates", `{"templates":[]}`),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_1/gitops/sources", nil),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_1/gitops/sources", `{"sources_revision":"1","sources":[]}`),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_1/workloads", nil),
		httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_1/workloads/wl_1", nil),
	}
	for _, req := range cases {
		req.Header.Set("Authorization", "Bearer token-value")
		rr := do(h, req)
		if rr.Code != http.StatusTooManyRequests {
			t.Fatalf("%s %s: status = %d, want 429", req.Method, req.URL.Path, rr.Code)
		}
		if rr.Header().Get("Retry-After") != "60" {
			t.Fatalf("%s %s: Retry-After = %q", req.Method, req.URL.Path, rr.Header().Get("Retry-After"))
		}
	}
	if got.path != "" {
		t.Fatalf("rate-limited request reached upstream as %q", got.path)
	}
}

func TestAccountProxyForwardsOnlyTheBearer(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"account_id":"acct_1","tenants":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodGet, "/api/account", nil)
	req.Header.Set("Authorization", "Bearer token-value")
	req.Header.Set("Cookie", "session=browser-cookie")
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	req.Header.Set("X-Admin", "1")

	rr := do(h, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rr.Code, rr.Body.String())
	}
	if got.path != "/v1/account" {
		t.Fatalf("upstream path = %q, want /v1/account", got.path)
	}
	if got.header.Get("Authorization") != "Bearer token-value" {
		t.Fatalf("Authorization = %q, want it forwarded verbatim", got.header.Get("Authorization"))
	}
	for _, banned := range []string{"Cookie", "X-Forwarded-For", "X-Admin"} {
		if got.header.Get(banned) != "" {
			t.Fatalf("upstream received %s = %q, want it dropped", banned, got.header.Get(banned))
		}
	}
	if rr.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", rr.Header().Get("Cache-Control"))
	}
}

func TestCreateTenantRebuildsNameOnly(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusCreated, `{"account_id":"acct_1","tenants":[{"customer_id":"tenant_1","role":"owner"}]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := tenantJSONRequest("/api/account/tenants", `{"name":"  First Workspace  "}`)
	req.Header.Set("Authorization", "Bearer token-value")
	req.Header.Set("Cookie", "session=browser-cookie")
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	req.Header.Set("X-Admin", "1")

	rr := do(h, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body %s", rr.Code, rr.Body.String())
	}
	if got.method != http.MethodPost || got.path != "/v1/account/tenants" {
		t.Fatalf("upstream saw %s %s, want POST /v1/account/tenants", got.method, got.path)
	}
	if got.body != `{"name":"First Workspace"}` {
		t.Fatalf("upstream body = %q, want trimmed exact name JSON", got.body)
	}
	if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Accept") != "application/json" || got.header.Get("Content-Type") != "application/json" {
		t.Fatalf("allowlisted headers = %v", got.header)
	}
	for _, banned := range []string{"Cookie", "X-Forwarded-For", "X-Admin"} {
		if got.header.Get(banned) != "" {
			t.Fatalf("upstream received %s = %q, want it dropped", banned, got.header.Get(banned))
		}
	}
}

func TestCreateTenantRejectsInvalidLocalShape(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusCreated, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	cases := []*http.Request{
		tenantJSONRequest("/api/account/tenants?debug=1", `{"name":"First Workspace"}`),
		func() *http.Request {
			req := httptest.NewRequest(http.MethodPost, "/api/account/tenants", strings.NewReader(`{"name":"First Workspace"}`))
			req.Header.Set("Content-Type", "text/plain")
			return req
		}(),
		tenantJSONRequest("/api/account/tenants", `{"name":""}`),
		tenantJSONRequest("/api/account/tenants", `{"name":"`+strings.Repeat("a", 65)+`"}`),
		tenantJSONRequest("/api/account/tenants", "{\"name\":\"bad\\u0001name\"}"),
		tenantJSONRequest("/api/account/tenants", `{"name":"First Workspace","token":"nope"}`),
		tenantJSONRequest("/api/account/tenants", `{"name":"First Workspace"} {}`),
	}
	for _, req := range cases {
		got.path = ""
		req.Header.Set("Authorization", "Bearer token-value")
		rr := do(h, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusUnsupportedMediaType && rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s: status = %d, want local validation refusal", req.URL.String(), rr.Code)
		}
		if got.path != "" {
			t.Fatalf("%s reached upstream as %q", req.URL.String(), got.path)
		}
	}

	oversized := tenantJSONRequest("/api/account/tenants", `{"name":"`+strings.Repeat("x", maxTenantJSONBytes)+`"}`)
	oversized.Header.Set("Authorization", "Bearer token-value")
	got.path = ""
	if rr := do(h, oversized); rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized tenant JSON status = %d, want 413; body %s", rr.Code, rr.Body.String())
	}
	if got.path != "" {
		t.Fatalf("oversized tenant JSON reached upstream as %q", got.path)
	}
}

func TestAccountProxyRequiresBearer(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"account_id":"acct_1"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	for _, header := range []string{"", "Basic abc", "Bearer ", "Bearer to ken"} {
		req := httptest.NewRequest(http.MethodGet, "/api/account", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		if rr := do(h, req); rr.Code != http.StatusUnauthorized {
			t.Fatalf("Authorization %q: status = %d, want 401", header, rr.Code)
		}
	}
	if got.path != "" {
		t.Fatalf("upstream was called without a usable bearer")
	}
}

func TestMembersQueryAllowlist(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"members":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/members?limit=50&after=acct_9&role=owner&include=secrets", nil)
	req.Header.Set("Authorization", "Bearer token-value")
	if rr := do(h, req); rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got.path != "/v1/tenants/cust_42/members" {
		t.Fatalf("upstream path = %q", got.path)
	}
	if len(got.query) != 2 || got.query.Get("limit") != "50" || got.query.Get("after") != "acct_9" {
		t.Fatalf("upstream query = %v, want only limit and after", got.query)
	}

	for _, bad := range []string{"?limit=0", "?limit=101", "?limit=all", "?after=" + url.QueryEscape("a b")} {
		req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/members"+bad, nil)
		req.Header.Set("Authorization", "Bearer token-value")
		if rr := do(h, req); rr.Code != http.StatusBadRequest {
			t.Fatalf("query %q: status = %d, want 400", bad, rr.Code)
		}
	}
}

func TestMemberMutationAndUsageProxyContracts(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		status      int
		wantPath    string
		wantBody    string
		contentType string
	}{
		{
			name: "add member", method: http.MethodPost, path: "/api/tenants/cust_42/members?admin=1",
			body: `{"email":" person@example.com ","role":"member"}`, status: http.StatusCreated,
			wantPath: "/v1/tenants/cust_42/members", wantBody: `{"email":"person@example.com","role":"member"}`, contentType: "application/json",
		},
		{
			name: "update role", method: http.MethodPatch, path: "/api/tenants/cust_42/members/acct_9",
			body: `{"role":"admin"}`, status: http.StatusOK,
			wantPath: "/v1/tenants/cust_42/members/acct_9", wantBody: `{"role":"admin"}`, contentType: "application/json",
		},
		{
			name: "usage", method: http.MethodGet, path: "/api/tenants/cust_42/usage",
			status: http.StatusOK, wantPath: "/v1/tenants/cust_42/usage",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, tt.status, `{"ok":true}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer token-value")
			req.Header.Set("Cookie", "session=browser-cookie")
			req.Header.Set("X-Forwarded-For", "10.0.0.1")
			if tt.body != "" {
				req.Header.Set("Content-Type", "application/json; charset=utf-8")
			}
			rr := do(h, req)
			if tt.name == "add member" {
				if rr.Code != http.StatusBadRequest {
					t.Fatalf("query-bearing add status = %d, want 400", rr.Code)
				}
				if got.path != "" {
					t.Fatalf("query-bearing add reached upstream as %q", got.path)
				}
				req = httptest.NewRequest(tt.method, "/api/tenants/cust_42/members", strings.NewReader(tt.body))
				req.Header.Set("Authorization", "Bearer token-value")
				req.Header.Set("Content-Type", "application/json; charset=utf-8")
				rr = do(h, req)
			}
			if rr.Code != tt.status {
				t.Fatalf("status = %d, want %d; body %s", rr.Code, tt.status, rr.Body.String())
			}
			if got.method != tt.method || got.path != tt.wantPath || got.body != tt.wantBody {
				t.Fatalf("upstream saw %s %s body %q, want %s %s body %q", got.method, got.path, got.body, tt.method, tt.wantPath, tt.wantBody)
			}
			if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Accept") != "application/json" {
				t.Fatalf("allowlisted headers = %v", got.header)
			}
			if got.header.Get("Content-Type") != tt.contentType {
				t.Fatalf("Content-Type = %q, want %q", got.header.Get("Content-Type"), tt.contentType)
			}
			for _, banned := range []string{"Cookie", "X-Forwarded-For"} {
				if got.header.Get(banned) != "" {
					t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
				}
			}
		})
	}
}

func TestMemberMutationJSONIsBoundedAndRebuilt(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusCreated, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	cases := []*http.Request{
		func() *http.Request {
			req := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/members", strings.NewReader(`{"email":"person@example.com","role":"member"}`))
			req.Header.Set("Content-Type", "text/plain")
			return req
		}(),
		memberJSONRequest(http.MethodPost, "/api/tenants/cust_42/members", `{"email":"person@example.com","role":"member","admin":true}`),
		memberJSONRequest(http.MethodPost, "/api/tenants/cust_42/members", `{"email":"not-an-email","role":"member"}`),
		memberJSONRequest(http.MethodPatch, "/api/tenants/cust_42/members/acct_9", `{"role":"root"}`),
		memberJSONRequest(http.MethodPatch, "/api/tenants/cust_42/members/acct_9?debug=1", `{"role":"admin"}`),
	}
	for _, req := range cases {
		got.path = ""
		req.Header.Set("Authorization", "Bearer token-value")
		rr := do(h, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusUnsupportedMediaType && rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s %s: status = %d, want local validation refusal", req.Method, req.URL.String(), rr.Code)
		}
		if got.path != "" {
			t.Fatalf("%s %s reached upstream as %q", req.Method, req.URL.String(), got.path)
		}
	}

	oversized := memberJSONRequest(http.MethodPost, "/api/tenants/cust_42/members",
		`{"email":"`+strings.Repeat("x", maxMemberJSONBytes)+`","role":"member"}`)
	oversized.Header.Set("Authorization", "Bearer token-value")
	got.path = ""
	if rr := do(h, oversized); rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized member JSON status = %d, want 413; body %s", rr.Code, rr.Body.String())
	}
	if got.path != "" {
		t.Fatalf("oversized member JSON reached upstream as %q", got.path)
	}
}

func TestUsageAndMemberMutationPathValidation(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	for _, req := range []*http.Request{
		memberJSONRequest(http.MethodPost, "/api/tenants/-bad/members", `{"email":"person@example.com","role":"member"}`),
		memberJSONRequest(http.MethodPatch, "/api/tenants/cust_42/members/-bad", `{"role":"admin"}`),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust%2042/usage", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/usage?include=history", nil),
	} {
		got.path = ""
		req.Header.Set("Authorization", "Bearer token-value")
		rr := do(h, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusNotFound {
			t.Fatalf("%s %s: status = %d, want 400 or 404", req.Method, req.URL.String(), rr.Code)
		}
		if got.path != "" {
			t.Fatalf("%s %s reached upstream as %q", req.Method, req.URL.String(), got.path)
		}
	}
}

func TestMemberMutationUpstreamStatusPassesThrough(t *testing.T) {
	up, _ := upstreamRecorder(t, http.StatusForbidden, `{"error":"owner required"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := memberJSONRequest(http.MethodPatch, "/api/tenants/cust_42/members/acct_9", `{"role":"owner"}`)
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "owner required") {
		t.Fatalf("status/body = %d %q, want upstream 403 JSON", rr.Code, rr.Body.String())
	}
}

func TestRemoveMemberPathValidation(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusNoContent, "")
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_42/members/acct_9", nil)
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body %s", rr.Code, rr.Body.String())
	}
	if got.method != http.MethodDelete || got.path != "/v1/tenants/cust_42/members/acct_9" {
		t.Fatalf("upstream saw %s %s", got.method, got.path)
	}

	// Traversal and admin-route reach are rejected before any upstream call.
	// Encoded slashes never reach the handler as one segment; the escaping
	// cases below are what a hand-built URL can actually deliver.
	got.path = ""
	for _, bad := range []string{
		"/api/tenants/..%2Fadmin/members/acct_9",
		"/api/tenants/cust_42/members/..%2F..%2Fv1%2Fadmin",
		"/api/tenants/cust%2042/members/acct_9",
		"/api/tenants/-bad/members/acct_9",
	} {
		req := httptest.NewRequest(http.MethodDelete, bad, nil)
		req.Header.Set("Authorization", "Bearer token-value")
		rr := do(h, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusNotFound {
			t.Fatalf("path %q: status = %d, want 400 or 404", bad, rr.Code)
		}
		if got.path != "" {
			t.Fatalf("path %q reached the upstream as %q", bad, got.path)
		}
	}
}

func TestWorkloadProxyMethodsPathsBodiesAndHeaders(t *testing.T) {
	const yamlBody = "apiVersion: yscale.sh/v1\nkind: Workload\nmetadata:\n  name: test\nspec:\n  image: busybox\n  size: nano\n"
	tests := []struct {
		name        string
		method      string
		path        string
		wantPath    string
		body        string
		status      int
		wantBody    string
		contentType string
		idempotency string
	}{
		{name: "list", method: http.MethodGet, path: "/api/tenants/cust_42/workloads", wantPath: "/v1/tenants/cust_42/workloads", status: http.StatusOK},
		{name: "create", method: http.MethodPost, path: "/api/tenants/cust_42/workloads", wantPath: "/v1/tenants/cust_42/workloads", body: yamlBody, status: http.StatusAccepted, wantBody: yamlBody, contentType: "text/yaml", idempotency: validIdempotencyKey},
		{name: "get", method: http.MethodGet, path: "/api/tenants/cust_42/workloads/wl_9", wantPath: "/v1/tenants/cust_42/workloads/wl_9", status: http.StatusOK},
		{name: "retry", method: http.MethodPost, path: "/api/tenants/cust_42/workloads/wl_9/retry", wantPath: "/v1/tenants/cust_42/workloads/wl_9/retry", status: http.StatusAccepted, idempotency: validIdempotencyKey},
		{name: "cancel", method: http.MethodDelete, path: "/api/tenants/cust_42/workloads/wl_9", wantPath: "/v1/tenants/cust_42/workloads/wl_9", status: http.StatusNoContent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, tt.status, func() string {
				if tt.status == http.StatusNoContent {
					return ""
				}
				return `{"ok":true}`
			}())
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer token-value")
			req.Header.Set("Accept", "text/html")
			req.Header.Set("Cookie", "session=browser-cookie")
			req.Header.Set("X-Forwarded-For", "10.0.0.1")
			req.Header.Set("X-Admin", "1")
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			if tt.idempotency != "" {
				req.Header.Set("Idempotency-Key", tt.idempotency)
			}
			rr := do(h, req)
			if rr.Code != tt.status {
				t.Fatalf("status = %d, want %d; body %s", rr.Code, tt.status, rr.Body.String())
			}
			if got.method != tt.method || got.path != tt.wantPath || got.body != tt.wantBody {
				t.Fatalf("upstream saw %s %s body %q, want %s %s body %q", got.method, got.path, got.body, tt.method, tt.wantPath, tt.wantBody)
			}
			if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Accept") != "application/json" {
				t.Fatalf("allowlisted headers = %v", got.header)
			}
			if got.header.Get("Content-Type") != tt.contentType {
				t.Fatalf("Content-Type = %q, want %q", got.header.Get("Content-Type"), tt.contentType)
			}
			// Only workload submission routes carry a key, and they carry it verbatim.
			if got.header.Get("Idempotency-Key") != tt.idempotency {
				t.Fatalf("upstream Idempotency-Key = %q, want %q", got.header.Get("Idempotency-Key"), tt.idempotency)
			}
			for _, banned := range []string{"Cookie", "X-Forwarded-For", "X-Admin"} {
				if got.header.Get(banned) != "" {
					t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
				}
			}
		})
	}
}

func TestRetryWorkloadForwardsEmptyBodyAndRejectsSmuggledCluster(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_retry","status":"provisioning"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/workloads/wl_9/retry", nil)
	req.Header.Set("Authorization", "Bearer token-value")
	req.Header.Set("Cookie", "browser-session=1")
	req.Header.Set("Idempotency-Key", validIdempotencyKey)
	rr := do(h, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("retry status = %d, want 202: %s", rr.Code, rr.Body.String())
	}
	if got.method != http.MethodPost || got.path != "/v1/tenants/cust_42/workloads/wl_9/retry" || got.body != "" {
		t.Fatalf("upstream saw %s %s body %q", got.method, got.path, got.body)
	}
	if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Idempotency-Key") != validIdempotencyKey {
		t.Fatalf("forwarded headers = %v", got.header)
	}
	for _, banned := range []string{"Cookie", "X-Cluster-ID", "Content-Type"} {
		if got.header.Get(banned) != "" {
			t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
		}
	}

	blocked := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/workloads/wl_9/retry", nil)
	blocked.Header.Set("Authorization", "Bearer token-value")
	blocked.Header.Set("Idempotency-Key", validIdempotencyKey)
	blocked.Header.Set("X-Cluster-ID", "cluster_console")
	if rr := do(h, blocked); rr.Code != http.StatusBadRequest {
		t.Fatalf("smuggled cluster retry = %d, want 400", rr.Code)
	}
}

func TestWorkloadLogsForwardsOnlyBoundedTailAndBearer(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"observed_at":"2026-08-14T00:00:00Z","streams":[{"pod":"job-abc","container":"main","output":"ready\n"}],"truncated":false}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/workloads/wl_9/logs?tail=40", nil)
	req.Header.Set("Authorization", "Bearer token-value")
	req.Header.Set("Cookie", "browser-session=1")
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	rr := do(h, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"output":"ready\n"`) {
		t.Fatalf("logs = %d %q", rr.Code, rr.Body.String())
	}
	if got.method != http.MethodGet || got.path != "/v1/tenants/cust_42/workloads/wl_9/logs" || got.query.Get("tail") != "40" {
		t.Fatalf("upstream = %s %s?%s", got.method, got.path, got.query.Encode())
	}
	if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Accept") != "application/json" {
		t.Fatalf("forwarded headers = %v", got.header)
	}
	for _, banned := range []string{"Cookie", "X-Forwarded-For", "X-Cluster-ID", "X-Namespace", "X-Container"} {
		if got.header.Get(banned) != "" {
			t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
		}
	}
}

func TestWorkloadLogsRejectsSmuggledSelectorsBodyAndBadTail(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"streams":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	tests := []struct {
		name, path, header, body string
	}{
		{name: "zero tail", path: "/api/tenants/cust_42/workloads/wl_9/logs?tail=0"},
		{name: "large tail", path: "/api/tenants/cust_42/workloads/wl_9/logs?tail=1001"},
		{name: "duplicate tail", path: "/api/tenants/cust_42/workloads/wl_9/logs?tail=20&tail=21"},
		{name: "container query", path: "/api/tenants/cust_42/workloads/wl_9/logs?container=main"},
		{name: "cluster header", path: "/api/tenants/cust_42/workloads/wl_9/logs", header: "X-Cluster-ID"},
		{name: "namespace header", path: "/api/tenants/cust_42/workloads/wl_9/logs", header: "X-Namespace"},
		{name: "container header", path: "/api/tenants/cust_42/workloads/wl_9/logs", header: "X-Container"},
		{name: "body", path: "/api/tenants/cust_42/workloads/wl_9/logs", body: "x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got.path = ""
			req := httptest.NewRequest(http.MethodGet, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer token-value")
			if tc.header != "" {
				req.Header.Set(tc.header, "smuggled")
			}
			if rr := do(h, req); rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rr.Code, rr.Body.String())
			}
			if got.path != "" {
				t.Fatalf("invalid request reached upstream as %q", got.path)
			}
		})
	}
}

// The cluster observation is a plain tenant-scoped read: one path, no
// parameters, the caller's bearer and nothing else of the browser's.
func TestClusterReadForwardsOnlyTheBearer(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"tenant_id":"cust_42","clusters":[],"partial":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/clusters", nil)
	req.Header.Set("Authorization", "Bearer token-value")
	req.Header.Set("Accept", "text/html")
	req.Header.Set("Cookie", "session=browser-cookie")
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	req.Header.Set("X-Cluster-ID", "smuggled-cluster")

	rr := do(h, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"partial":true`) {
		t.Fatalf("status/body = %d %q, want the upstream observation", rr.Code, rr.Body.String())
	}
	if got.method != http.MethodGet || got.path != "/v1/tenants/cust_42/clusters" {
		t.Fatalf("upstream saw %s %s, want GET /v1/tenants/cust_42/clusters", got.method, got.path)
	}
	if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Accept") != "application/json" {
		t.Fatalf("allowlisted headers = %v", got.header)
	}
	// A read has no target, so the create route's one extra header is not a
	// header this route forwards either.
	for _, banned := range []string{"Cookie", "X-Forwarded-For", "X-Cluster-ID"} {
		if got.header.Get(banned) != "" {
			t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
		}
	}
	if rr.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", rr.Header().Get("Cache-Control"))
	}
}

func TestClusterReadRejectsParametersAndBadTenants(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"clusters":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	for _, path := range []string{
		"/api/tenants/cust_42/clusters?limit=10",
		"/api/tenants/cust_42/clusters?after=abc",
		"/api/tenants/-bad/clusters",
		"/api/tenants/cust%2042/clusters",
		"/api/tenants/..%2Fadmin/clusters",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer token-value")
		if rr := do(h, req); rr.Code != http.StatusBadRequest && rr.Code != http.StatusNotFound {
			t.Fatalf("path %q: status = %d, want 400 or 404", path, rr.Code)
		}
		if got.path != "" {
			t.Fatalf("path %q reached upstream as %q", path, got.path)
		}
	}

	unauth := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/clusters", nil)
	if rr := do(h, unauth); rr.Code != http.StatusUnauthorized {
		t.Fatalf("missing bearer status = %d, want 401", rr.Code)
	}
	if got.path != "" {
		t.Fatalf("unauthenticated read reached upstream as %q", got.path)
	}
}

func TestHostedCapacityReadAndRequestContracts(t *testing.T) {
	tests := []struct {
		name, method string
		status       int
	}{
		{name: "read", method: http.MethodGet, status: http.StatusOK},
		{name: "first request", method: http.MethodPost, status: http.StatusAccepted},
		{name: "duplicate request", method: http.MethodPost, status: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, tt.status, `{"tenant_id":"cust_42","role":"owner","status":"requested","requested_at":"2026-08-15T10:20:30Z"}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(tt.method, "/api/tenants/cust_42/hosted-capacity", nil)
			req.Header.Set("Authorization", "Bearer token-value")
			req.Header.Set("Cookie", "browser-session=1")
			req.Header.Set("X-Forwarded-For", "10.0.0.1")
			req.Header.Set("X-Cluster-ID", "smuggled-cluster")

			rr := do(h, req)
			if rr.Code != tt.status || !strings.Contains(rr.Body.String(), `"status":"requested"`) {
				t.Fatalf("status/body = %d %q", rr.Code, rr.Body.String())
			}
			if got.method != tt.method || got.path != "/v1/tenants/cust_42/hosted-capacity" || got.body != "" {
				t.Fatalf("upstream = %s %s body %q", got.method, got.path, got.body)
			}
			if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Accept") != "application/json" {
				t.Fatalf("allowlisted headers = %v", got.header)
			}
			for _, banned := range []string{"Cookie", "X-Forwarded-For", "X-Cluster-ID", "Content-Type"} {
				if got.header.Get(banned) != "" {
					t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
				}
			}
			if rr.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", rr.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestHostedCapacityRejectsParametersBodiesBadTenantsAndMissingAuth(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"tenant_id":"cust_42","role":"owner","status":"not_requested"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{name: "read query", method: http.MethodGet, path: "/api/tenants/cust_42/hosted-capacity?status=1", want: http.StatusBadRequest},
		{name: "post query", method: http.MethodPost, path: "/api/tenants/cust_42/hosted-capacity?request=1", want: http.StatusBadRequest},
		{name: "post body", method: http.MethodPost, path: "/api/tenants/cust_42/hosted-capacity", body: `{}`, want: http.StatusBadRequest},
		{name: "bad tenant", method: http.MethodGet, path: "/api/tenants/-bad/hosted-capacity", want: http.StatusBadRequest},
		{name: "missing auth", method: http.MethodGet, path: "/api/tenants/cust_42/hosted-capacity", want: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got.path = ""
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			if tt.name != "missing auth" {
				req.Header.Set("Authorization", "Bearer token-value")
			}
			if rr := do(h, req); rr.Code != tt.want {
				t.Fatalf("status = %d, want %d: %s", rr.Code, tt.want, rr.Body.String())
			}
			if got.path != "" {
				t.Fatalf("invalid request reached upstream as %q", got.path)
			}
		})
	}
}

func TestHostedCapacityUsesProxyRateLimitAndResponseCeiling(t *testing.T) {
	t.Run("rate limit", func(t *testing.T) {
		up, got := upstreamRecorder(t, http.StatusOK, `{"tenant_id":"cust_42","role":"owner","status":"not_requested"}`)
		h, _ := newTestAppWithProxyLimiter(t, "", newAccountProxy(up.URL, up.URL, up.Client()), denyLimiter{})
		req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/hosted-capacity", nil)
		req.Header.Set("Authorization", "Bearer token-value")
		if rr := do(h, req); rr.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429", rr.Code)
		}
		if got.path != "" {
			t.Fatalf("rate-limited request reached upstream as %q", got.path)
		}
	})

	t.Run("response ceiling", func(t *testing.T) {
		up, _ := upstreamRecorder(t, http.StatusOK, strings.Repeat("x", maxUpstreamBytes+1))
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/hosted-capacity", nil)
		req.Header.Set("Authorization", "Bearer token-value")
		if rr := do(h, req); rr.Code != http.StatusBadGateway || !strings.Contains(rr.Body.String(), "upstream response too large") {
			t.Fatalf("status/body = %d %q, want bounded 502", rr.Code, rr.Body.String())
		}
	})
}

// The registry writes are the same allowlisted relay: the caller's bearer, a
// bounded JSON body on register, nothing else of the browser's, and the
// upstream answer — one-time connector credential included — handed back as it
// came.
func TestClusterRegistryWriteContracts(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		status      int
		wantPath    string
		wantBody    string
		contentType string
	}{
		{name: "register", method: http.MethodPost, path: "/api/tenants/cust_42/clusters", body: `{"name":"Prod US East","cluster_id":"edge-1"}`, status: http.StatusCreated, wantPath: "/v1/tenants/cust_42/clusters", wantBody: `{"name":"Prod US East","cluster_id":"edge-1"}`, contentType: "application/json"},
		{name: "rotate", method: http.MethodPost, path: "/api/tenants/cust_42/clusters/edge-1/credential", status: http.StatusOK, wantPath: "/v1/tenants/cust_42/clusters/edge-1/credential"},
		{name: "delete", method: http.MethodDelete, path: "/api/tenants/cust_42/clusters/edge-1", status: http.StatusOK, wantPath: "/v1/tenants/cust_42/clusters/edge-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, tt.status, `{"tenant_id":"cust_42","cluster":{"cluster_id":"edge-1"},"connector_token":"one-time-value","helm_install":"helm install"}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer token-value")
			req.Header.Set("Accept", "text/html")
			req.Header.Set("Cookie", "session=browser-cookie")
			req.Header.Set("X-Forwarded-For", "10.0.0.1")
			req.Header.Set("X-Cluster-ID", "smuggled-cluster")
			req.Header.Set("Idempotency-Key", validIdempotencyKey)
			if tt.contentType != "" {
				req.Header.Set("Content-Type", "application/json; charset=utf-8")
			}
			rr := do(h, req)
			if rr.Code != tt.status {
				t.Fatalf("status = %d, want %d; body %s", rr.Code, tt.status, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), `"connector_token":"one-time-value"`) {
				t.Fatalf("body = %q, want the upstream answer relayed whole", rr.Body.String())
			}
			if got.method != tt.method || got.path != tt.wantPath || got.body != tt.wantBody {
				t.Fatalf("upstream saw %s %s body %q", got.method, got.path, got.body)
			}
			if len(got.query) != 0 {
				t.Fatalf("upstream query = %v, want none", got.query)
			}
			if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Accept") != "application/json" {
				t.Fatalf("allowlisted headers = %v", got.header)
			}
			if got.header.Get("Content-Type") != tt.contentType {
				t.Fatalf("Content-Type = %q, want %q", got.header.Get("Content-Type"), tt.contentType)
			}
			for _, banned := range []string{"Cookie", "X-Forwarded-For", "X-Cluster-ID", "Idempotency-Key"} {
				if got.header.Get(banned) != "" {
					t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
				}
			}
			if rr.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", rr.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestClusterRegistryWriteRejectsUnsupportedRequests(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	wrongType := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/clusters", strings.NewReader(`{"name":"x"}`))
	wrongType.Header.Set("Content-Type", "text/plain")

	cases := []*http.Request{
		policyJSONRequest(http.MethodPost, "/api/tenants/cust_42/clusters?debug=1", `{"name":"x"}`),
		policyJSONRequest(http.MethodPost, "/api/tenants/-bad/clusters", `{"name":"x"}`),
		policyJSONRequest(http.MethodPost, "/api/tenants/cust_42/clusters", `{not-json`),
		policyJSONRequest(http.MethodPost, "/api/tenants/cust_42/clusters", strings.Repeat("x", maxClusterJSONBytes+1)),
		wrongType,
		httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/clusters/-bad/credential", nil),
		httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/clusters/..%2Fadmin/credential", nil),
		httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/clusters/edge-1/credential?debug=1", nil),
		httptest.NewRequest(http.MethodPost, "/api/tenants/-bad/clusters/edge-1/credential", nil),
		httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_42/clusters/-bad", nil),
		httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_42/clusters/..%2Fadmin", nil),
		httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_42/clusters/edge-1?debug=1", nil),
	}
	for _, req := range cases {
		req.Header.Set("Authorization", "Bearer token-value")
		rr := do(h, req)
		switch rr.Code {
		case http.StatusBadRequest, http.StatusNotFound, http.StatusUnsupportedMediaType, http.StatusRequestEntityTooLarge:
		default:
			t.Fatalf("%s %s: status = %d, want a local refusal", req.Method, req.URL.Path, rr.Code)
		}
		if got.path != "" {
			t.Fatalf("%s %s reached upstream as %q", req.Method, req.URL.Path, got.path)
		}
	}

	unauth := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/clusters/edge-1/credential", nil)
	if rr := do(h, unauth); rr.Code != http.StatusUnauthorized {
		t.Fatalf("missing bearer status = %d, want 401", rr.Code)
	}
	if got.path != "" {
		t.Fatalf("unauthenticated write reached upstream as %q", got.path)
	}
}

// A conflict — a name already registered, a policy still naming the cluster —
// is central's answer to relay, not this server's to soften.
func TestClusterRegistryUpstreamStatusPassesThrough(t *testing.T) {
	up, _ := upstreamRecorder(t, http.StatusConflict, `{"error":"policy_conflict","message":"the placement policy names this cluster"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_42/clusters/edge-1", nil)
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want the upstream 409", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "policy_conflict") {
		t.Fatalf("body = %q, want the upstream conflict relayed", rr.Body.String())
	}
}

func TestClusterPolicyProxyContracts(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		status      int
		wantBody    string
		contentType string
	}{
		{name: "get", method: http.MethodGet, path: "/api/tenants/cust_42/cluster-policy", status: http.StatusOK},
		{name: "put", method: http.MethodPut, path: "/api/tenants/cust_42/cluster-policy", body: `{"allow":["b","a"],"deny":["blocked"],"auto":"ordered"}`, status: http.StatusOK, wantBody: `{"allow":["b","a"],"deny":["blocked"],"auto":"ordered"}`, contentType: "application/json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, tt.status, `{"tenant_id":"cust_42","policy":{"allow":[],"deny":[],"auto":"require_pin"},"role":"owner","changed":true}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer token-value")
			req.Header.Set("Accept", "text/html")
			req.Header.Set("Cookie", "session=browser-cookie")
			req.Header.Set("X-Forwarded-For", "10.0.0.1")
			req.Header.Set("X-Cluster-ID", "smuggled-cluster")
			req.Header.Set("Idempotency-Key", validIdempotencyKey)
			if tt.contentType != "" {
				req.Header.Set("Content-Type", "application/json; charset=utf-8")
			}
			rr := do(h, req)
			if rr.Code != tt.status {
				t.Fatalf("status = %d, want %d; body %s", rr.Code, tt.status, rr.Body.String())
			}
			if got.method != tt.method || got.path != "/v1/tenants/cust_42/cluster-policy" || got.body != tt.wantBody {
				t.Fatalf("upstream saw %s %s body %q", got.method, got.path, got.body)
			}
			if len(got.query) != 0 {
				t.Fatalf("upstream query = %v, want none", got.query)
			}
			if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Accept") != "application/json" {
				t.Fatalf("allowlisted headers = %v", got.header)
			}
			if got.header.Get("Content-Type") != tt.contentType {
				t.Fatalf("Content-Type = %q, want %q", got.header.Get("Content-Type"), tt.contentType)
			}
			for _, banned := range []string{"Cookie", "X-Forwarded-For", "X-Cluster-ID", "Idempotency-Key"} {
				if got.header.Get(banned) != "" {
					t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
				}
			}
		})
	}
}

func TestClusterPolicyProxyRejectsUnsupportedRequests(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	cases := []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/cluster-policy?debug=1", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/-bad/cluster-policy", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust%2042/cluster-policy", nil),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/cluster-policy?debug=1", `{"allow":[],"deny":[],"auto":"require_pin"}`),
		func() *http.Request {
			req := httptest.NewRequest(http.MethodPut, "/api/tenants/cust_42/cluster-policy", strings.NewReader(`{"auto":"require_pin"}`))
			req.Header.Set("Content-Type", "text/plain")
			return req
		}(),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/cluster-policy", `{not-json`),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/cluster-policy", strings.Repeat("x", maxPolicyJSONBytes+1)),
	}
	for _, req := range cases {
		got.path = ""
		req.Header.Set("Authorization", "Bearer token-value")
		rr := do(h, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusNotFound && rr.Code != http.StatusUnsupportedMediaType && rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s %s: status = %d, want local refusal", req.Method, req.URL.String(), rr.Code)
		}
		if got.path != "" {
			t.Fatalf("%s %s reached upstream as %q", req.Method, req.URL.String(), got.path)
		}
	}

	unauth := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/cluster-policy", nil)
	if rr := do(h, unauth); rr.Code != http.StatusUnauthorized {
		t.Fatalf("missing bearer status = %d, want 401", rr.Code)
	}
	if got.path != "" {
		t.Fatalf("unauthenticated policy read reached upstream as %q", got.path)
	}
}

func TestClusterPolicyUpstreamStatusPassesThrough(t *testing.T) {
	up, _ := upstreamRecorder(t, http.StatusForbidden, `{"error":"owner required"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/cluster-policy", `{"allow":[],"deny":[],"auto":"require_pin"}`)
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "owner required") {
		t.Fatalf("status/body = %d %q, want upstream 403 JSON", rr.Code, rr.Body.String())
	}
}

// The create route widens the seam by one optional header. Absent means central
// decides; present means exactly one id in the same opaque shape every other
// interpolated segment must satisfy.
func TestWorkloadCreateCarriesOneOptionalClusterID(t *testing.T) {
	t.Run("forwarded verbatim", func(t *testing.T) {
		up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
		req.Header.Set("Authorization", "Bearer token-value")
		req.Header.Set("X-Cluster-ID", "acme-prod-us-east")
		if rr := do(h, req); rr.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202; body %s", rr.Code, rr.Body.String())
		}
		if got.header.Get("X-Cluster-ID") != "acme-prod-us-east" {
			t.Fatalf("upstream X-Cluster-ID = %q", got.header.Get("X-Cluster-ID"))
		}
		if got.header.Get("Idempotency-Key") != validIdempotencyKey {
			t.Fatalf("upstream Idempotency-Key = %q", got.header.Get("Idempotency-Key"))
		}
		if strings.Contains(got.body, "acme-prod-us-east") {
			t.Fatalf("the target reached central inside the document: %q", got.body)
		}
	})

	t.Run("absent stays absent", func(t *testing.T) {
		up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
		req.Header.Set("Authorization", "Bearer token-value")
		if rr := do(h, req); rr.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", rr.Code)
		}
		if _, ok := got.header["X-Cluster-Id"]; ok {
			t.Fatalf("an absent target was invented as %q", got.header.Get("X-Cluster-ID"))
		}
	})

	// Anything this server would refuse to interpolate into a URL is refused
	// here too, before it can be handed to another service.
	for _, tc := range []struct {
		name   string
		values []string
	}{
		{name: "empty", values: []string{""}},
		{name: "spaced", values: []string{"acme prod"}},
		{name: "slashed", values: []string{"acme/prod"}},
		{name: "traversal", values: []string{"../admin"}},
		{name: "leading hyphen", values: []string{"-acme"}},
		{name: "control character", values: []string{"acme\x01prod"}},
		{name: "overlong", values: []string{"a" + strings.Repeat("b", 128)}},
		{name: "two targets", values: []string{"acme-prod", "acme-lab"}},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
			req.Header.Set("Authorization", "Bearer token-value")
			for _, value := range tc.values {
				req.Header.Add("X-Cluster-ID", value)
			}
			rr := do(h, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "invalid_cluster_id") {
				t.Fatalf("body = %q, want the cluster-id refusal", rr.Body.String())
			}
			if got.path != "" {
				t.Fatalf("rejected create reached upstream as %q", got.path)
			}
		})
	}
}

// The template a run was composed from is provenance central stores on the
// record, so this seam carries the pair verbatim or refuses it outright.
func TestWorkloadCreateCarriesTheTemplateSelectionPair(t *testing.T) {
	t.Run("forwarded verbatim", func(t *testing.T) {
		up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
		req.Header.Set("Authorization", "Bearer token-value")
		req.Header.Set("X-Template-ID", "pytorch-training")
		req.Header.Set("X-Template-Version", "4")
		if rr := do(h, req); rr.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202; body %s", rr.Code, rr.Body.String())
		}
		if got.header.Get("X-Template-ID") != "pytorch-training" || got.header.Get("X-Template-Version") != "4" {
			t.Fatalf("upstream template pair = %q/%q", got.header.Get("X-Template-ID"), got.header.Get("X-Template-Version"))
		}
		if strings.Contains(got.body, "pytorch-training") {
			t.Fatalf("the template reached central inside the document: %q", got.body)
		}
	})

	t.Run("absent stays absent", func(t *testing.T) {
		up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
		req.Header.Set("Authorization", "Bearer token-value")
		if rr := do(h, req); rr.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", rr.Code)
		}
		if _, ok := got.header["X-Template-Id"]; ok {
			t.Fatalf("an absent template was invented as %q", got.header.Get("X-Template-ID"))
		}
		if _, ok := got.header["X-Template-Version"]; ok {
			t.Fatalf("an absent version was invented as %q", got.header.Get("X-Template-Version"))
		}
	})

	for _, tc := range []struct {
		name     string
		ids      []string
		versions []string
	}{
		{name: "id without version", ids: []string{"container-job"}},
		{name: "version without id", versions: []string{"2"}},
		{name: "empty id", ids: []string{""}, versions: []string{"2"}},
		{name: "slashed id", ids: []string{"acme/job"}, versions: []string{"2"}},
		{name: "traversal id", ids: []string{"../admin"}, versions: []string{"2"}},
		{name: "overlong id", ids: []string{"a" + strings.Repeat("b", 128)}, versions: []string{"2"}},
		{name: "two ids", ids: []string{"container-job", "pytorch-training"}, versions: []string{"2"}},
		{name: "two versions", ids: []string{"container-job"}, versions: []string{"2", "3"}},
		{name: "zero version", ids: []string{"container-job"}, versions: []string{"0"}},
		{name: "negative version", ids: []string{"container-job"}, versions: []string{"-1"}},
		{name: "padded version", ids: []string{"container-job"}, versions: []string{"01"}},
		{name: "word version", ids: []string{"container-job"}, versions: []string{"latest"}},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
			req.Header.Set("Authorization", "Bearer token-value")
			for _, id := range tc.ids {
				req.Header.Add("X-Template-ID", id)
			}
			for _, version := range tc.versions {
				req.Header.Add("X-Template-Version", version)
			}
			rr := do(h, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "invalid_template_selection") {
				t.Fatalf("body = %q, want the template refusal", rr.Body.String())
			}
			if got.path != "" {
				t.Fatalf("rejected create reached upstream as %q", got.path)
			}
		})
	}
}

// A retry carries the source record's provenance forward, so naming a template
// on it would relabel a run this console never composed.
func TestWorkloadRetryRefusesTemplateSelection(t *testing.T) {
	for _, header := range []string{"X-Template-ID", "X-Template-Version"} {
		up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_retry"}`)
		h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
		req := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/workloads/wl_1/retry", nil)
		req.Header.Set("Authorization", "Bearer token-value")
		req.Header.Set("Idempotency-Key", validIdempotencyKey)
		req.Header.Set(header, "container-job")
		if rr := do(h, req); rr.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", header, rr.Code)
		}
		if got.path != "" {
			t.Fatalf("%s: rejected retry reached upstream as %q", header, got.path)
		}
	}
}

func TestTenantTemplateProxyContracts(t *testing.T) {
	const catalog = `{"templates":[{"id":"container-job","version":2,"title":"Generic container job","kind":"Run-once job","description":"","nodeOnly":false,"disabled":false,"defaults":{"name":"container-job","image":"busybox:1.36","size":"small","mode":"cpu"}}]}`
	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		wantBody    string
		contentType string
	}{
		{name: "get", method: http.MethodGet, path: "/api/tenants/cust_42/templates"},
		{name: "put", method: http.MethodPut, path: "/api/tenants/cust_42/templates", body: catalog, wantBody: catalog, contentType: "application/json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, http.StatusOK, `{"tenant_id":"cust_42","catalog_revision":"3","source":"tenant","templates":[],"role":"owner","changed":true}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer token-value")
			req.Header.Set("Accept", "text/html")
			req.Header.Set("Cookie", "session=browser-cookie")
			req.Header.Set("X-Forwarded-For", "10.0.0.1")
			req.Header.Set("X-Template-ID", "smuggled-template")
			req.Header.Set("X-Cluster-ID", "smuggled-cluster")
			req.Header.Set("Idempotency-Key", validIdempotencyKey)
			if tt.contentType != "" {
				req.Header.Set("Content-Type", "application/json; charset=utf-8")
			}
			rr := do(h, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body %s", rr.Code, rr.Body.String())
			}
			if got.method != tt.method || got.path != "/v1/tenants/cust_42/templates" || got.body != tt.wantBody {
				t.Fatalf("upstream saw %s %s body %q", got.method, got.path, got.body)
			}
			if len(got.query) != 0 {
				t.Fatalf("upstream query = %v, want none", got.query)
			}
			if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Accept") != "application/json" {
				t.Fatalf("allowlisted headers = %v", got.header)
			}
			if got.header.Get("Content-Type") != tt.contentType {
				t.Fatalf("Content-Type = %q, want %q", got.header.Get("Content-Type"), tt.contentType)
			}
			for _, banned := range []string{"Cookie", "X-Forwarded-For", "X-Cluster-ID", "X-Template-ID", "Idempotency-Key"} {
				if got.header.Get(banned) != "" {
					t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
				}
			}
		})
	}
}

func TestTenantTemplateProxyRejectsUnsupportedRequests(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	cases := []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/templates?debug=1", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/-bad/templates", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust%2042/templates", nil),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/templates?debug=1", `{"templates":[]}`),
		func() *http.Request {
			req := httptest.NewRequest(http.MethodPut, "/api/tenants/cust_42/templates", strings.NewReader(`{"templates":[]}`))
			req.Header.Set("Content-Type", "text/plain")
			return req
		}(),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/templates", `{not-json`),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/templates", strings.Repeat("x", maxCatalogJSONBytes+1)),
	}
	for _, req := range cases {
		got.path = ""
		req.Header.Set("Authorization", "Bearer token-value")
		rr := do(h, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusNotFound && rr.Code != http.StatusUnsupportedMediaType && rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s %s: status = %d, want local refusal", req.Method, req.URL.String(), rr.Code)
		}
		if got.path != "" {
			t.Fatalf("%s %s reached upstream as %q", req.Method, req.URL.String(), got.path)
		}
	}

	unauth := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/templates", nil)
	if rr := do(h, unauth); rr.Code != http.StatusUnauthorized {
		t.Fatalf("missing bearer status = %d, want 401", rr.Code)
	}
	if got.path != "" {
		t.Fatalf("unauthenticated catalog read reached upstream as %q", got.path)
	}
}

// Whether this role may change the catalog is central's call, so its refusal is
// what the browser sees.
func TestTenantTemplateUpstreamStatusPassesThrough(t *testing.T) {
	up, _ := upstreamRecorder(t, http.StatusForbidden, `{"error":"owner_required"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/templates", `{"templates":[]}`)
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "owner_required") {
		t.Fatalf("status/body = %d %q, want upstream 403 JSON", rr.Code, rr.Body.String())
	}
}

const gitOpsRegistry = `{"sources_revision":"4","sources":[{"id":"prod-infra","name":"Prod infrastructure","reconciler":"flux","repo_url":"https://github.com/acme/infra.git","path":"clusters/prod","ref":"main","cluster_id":"acme-prod-us-east"}]}`

func TestTenantGitOpsProxyContracts(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		wantBody    string
		contentType string
	}{
		{name: "get", method: http.MethodGet, path: "/api/tenants/cust_42/gitops/sources"},
		{name: "put", method: http.MethodPut, path: "/api/tenants/cust_42/gitops/sources", body: gitOpsRegistry, wantBody: gitOpsRegistry, contentType: "application/json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, http.StatusOK, `{"tenant_id":"cust_42","sources_revision":"5","role":"owner","changed":true,"sources":[]}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer token-value")
			req.Header.Set("Accept", "text/html")
			req.Header.Set("Cookie", "session=browser-cookie")
			req.Header.Set("X-Forwarded-For", "10.0.0.1")
			req.Header.Set("X-Cluster-ID", "smuggled-cluster")
			req.Header.Set("X-Template-ID", "smuggled-template")
			req.Header.Set("Idempotency-Key", validIdempotencyKey)
			if tt.contentType != "" {
				req.Header.Set("Content-Type", "application/json; charset=utf-8")
			}
			rr := do(h, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body %s", rr.Code, rr.Body.String())
			}
			if got.method != tt.method || got.path != "/v1/tenants/cust_42/gitops/sources" || got.body != tt.wantBody {
				t.Fatalf("upstream saw %s %s body %q", got.method, got.path, got.body)
			}
			if len(got.query) != 0 {
				t.Fatalf("upstream query = %v, want none", got.query)
			}
			if got.header.Get("Authorization") != "Bearer token-value" || got.header.Get("Accept") != "application/json" {
				t.Fatalf("allowlisted headers = %v", got.header)
			}
			if got.header.Get("Content-Type") != tt.contentType {
				t.Fatalf("Content-Type = %q, want %q", got.header.Get("Content-Type"), tt.contentType)
			}
			for _, banned := range []string{"Cookie", "X-Forwarded-For", "X-Cluster-ID", "X-Template-ID", "Idempotency-Key"} {
				if got.header.Get(banned) != "" {
					t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
				}
			}
		})
	}
}

func TestTenantGitOpsProxyRejectsUnsupportedRequests(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	cases := []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/gitops/sources?debug=1", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/-bad/gitops/sources", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust%2042/gitops/sources", nil),
		httptest.NewRequest(http.MethodGet, "/api/tenants/..%2Fadmin/gitops/sources", nil),
		// Only the registry document is proxied; the path below it is not a seam
		// this server offers, so it must not resolve to the registry route.
		httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/gitops/sources/prod-infra", nil),
		httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_42/gitops/sources", nil),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/gitops/sources?debug=1", `{"sources":[]}`),
		func() *http.Request {
			req := httptest.NewRequest(http.MethodPut, "/api/tenants/cust_42/gitops/sources", strings.NewReader(`{"sources":[]}`))
			req.Header.Set("Content-Type", "text/plain")
			return req
		}(),
		func() *http.Request {
			// No Content-Type at all is as unusable as the wrong one.
			return httptest.NewRequest(http.MethodPut, "/api/tenants/cust_42/gitops/sources", strings.NewReader(`{"sources":[]}`))
		}(),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/gitops/sources", `{not-json`),
		policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/gitops/sources", strings.Repeat("x", maxGitOpsJSONBytes+1)),
	}
	for _, req := range cases {
		got.path = ""
		req.Header.Set("Authorization", "Bearer token-value")
		rr := do(h, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusNotFound && rr.Code != http.StatusMethodNotAllowed && rr.Code != http.StatusUnsupportedMediaType && rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s %s: status = %d, want local refusal", req.Method, req.URL.String(), rr.Code)
		}
		if got.path != "" {
			t.Fatalf("%s %s reached upstream as %q", req.Method, req.URL.String(), got.path)
		}
	}

	unauth := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/gitops/sources", nil)
	if rr := do(h, unauth); rr.Code != http.StatusUnauthorized {
		t.Fatalf("missing bearer status = %d, want 401", rr.Code)
	}
	if got.path != "" {
		t.Fatalf("unauthenticated registry read reached upstream as %q", got.path)
	}
}

// A registry write refused as stale answers with the registry as it stands now.
// The console needs that document to show the reader what moved, so the status
// and the body both have to survive the relay intact.
func TestTenantGitOpsConflictBodyIsRelayed(t *testing.T) {
	const current = `{"tenant_id":"cust_42","sources_revision":"9","role":"owner","changed":false,"sources":[{"id":"prod-infra","name":"Prod infrastructure","reconciler":"argo","repo_url":"https://github.com/acme/infra.git","path":"clusters/prod","ref":"release","cluster_id":"acme-prod-us-east"}]}`
	up, _ := upstreamRecorder(t, http.StatusConflict, current)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/gitops/sources", gitOpsRegistry)
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
	if rr.Body.String() != current {
		t.Fatalf("body = %q, want the upstream envelope verbatim", rr.Body.String())
	}
}

// Whether this role may change the registry is central's call, so its refusal
// is what the browser sees.
func TestTenantGitOpsUpstreamStatusPassesThrough(t *testing.T) {
	up, _ := upstreamRecorder(t, http.StatusForbidden, `{"error":"owner_required"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := policyJSONRequest(http.MethodPut, "/api/tenants/cust_42/gitops/sources", gitOpsRegistry)
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "owner_required") {
		t.Fatalf("status/body = %d %q, want upstream 403 JSON", rr.Code, rr.Body.String())
	}
}

func TestWorkloadProxyValidatesEveryPathSegment(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	for _, path := range []string{
		"/api/tenants/-bad/workloads",
		"/api/tenants/cust%2042/workloads",
		"/api/tenants/cust_42/workloads/-bad",
		"/api/tenants/cust_42/workloads/wl%209",
		"/api/tenants/..%2Fadmin/workloads/wl_9",
		"/api/tenants/cust_42/workloads/..%2Fadmin",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer token-value")
		rr := do(h, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusNotFound {
			t.Fatalf("path %q: status = %d, want 400 or 404", path, rr.Code)
		}
		if got.path != "" {
			t.Fatalf("path %q reached upstream as %q", path, got.path)
		}
	}
}

func TestWorkloadCreateRequiresBoundedYAML(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	wrongType := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/workloads", strings.NewReader("kind: Workload"))
	wrongType.Header.Set("Authorization", "Bearer token-value")
	wrongType.Header.Set("Content-Type", "application/json")
	wrongType.Header.Set("Idempotency-Key", validIdempotencyKey)
	if rr := do(h, wrongType); rr.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong Content-Type status = %d, want 415", rr.Code)
	}

	tooLarge := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/workloads", strings.NewReader(strings.Repeat("x", maxWorkloadBytes+1)))
	tooLarge.Header.Set("Authorization", "Bearer token-value")
	tooLarge.Header.Set("Content-Type", "text/yaml; charset=utf-8")
	tooLarge.Header.Set("Idempotency-Key", validIdempotencyKey)
	if rr := do(h, tooLarge); rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body status = %d, want 413; body %s", rr.Code, rr.Body.String())
	}
	if got.path != "" {
		t.Fatalf("rejected create reached upstream as %q", got.path)
	}
}

func TestWorkloadUpstreamStatusPassesThrough(t *testing.T) {
	up, _ := upstreamRecorder(t, http.StatusConflict, `{"error":"capacity unavailable"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := httptest.NewRequest(http.MethodDelete, "/api/tenants/cust_42/workloads/wl_9", nil)
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), "capacity unavailable") {
		t.Fatalf("status/body = %d %q, want upstream 409 JSON", rr.Code, rr.Body.String())
	}
}

func TestWorkloadPlainTextValidationErrorIsRelayedAsJSON(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("spec.image is required\n")) //nolint:errcheck
	}))
	t.Cleanup(up.Close)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusBadRequest || rr.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status/type = %d %q, want 400 application/json", rr.Code, rr.Header().Get("Content-Type"))
	}
	if !strings.Contains(rr.Body.String(), "spec.image is required") {
		t.Fatalf("body = %q, want the bounded upstream validation detail", rr.Body.String())
	}
}

// A submit is an operation, so the key that lets central deduplicate it is
// part of the request contract, not a nicety. Everything central would refuse
// is refused here, before a run can be started twice.
func TestWorkloadCreateRejectsUnusableIdempotencyKeys(t *testing.T) {
	cases := []struct {
		name string
		keys []string
	}{
		{name: "missing", keys: nil},
		{name: "empty", keys: []string{""}},
		{name: "too short", keys: []string{"wl_1234"}},
		{name: "spaced", keys: []string{"wl_ 0123456789"}},
		{name: "control character", keys: []string{"wl_0123\x0146789"}},
		{name: "non-ascii", keys: []string{"wl_kéy0123456789"}},
		{name: "overlong", keys: []string{"wl_" + strings.Repeat("a", maxIdempotencyKeyLen)}},
		{name: "two keys", keys: []string{validIdempotencyKey, "wl_0000000000000000"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/workloads", strings.NewReader("kind: Workload\n"))
			req.Header.Set("Authorization", "Bearer token-value")
			req.Header.Set("Content-Type", "text/yaml")
			for _, key := range tc.keys {
				req.Header.Add("Idempotency-Key", key)
			}
			rr := do(h, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body %s", rr.Code, rr.Body.String())
			}
			if got.path != "" {
				t.Fatalf("rejected create reached upstream as %q", got.path)
			}
			for _, key := range tc.keys {
				if key != "" && strings.Contains(rr.Body.String(), key) {
					t.Fatalf("refusal echoed the key back: %s", rr.Body.String())
				}
			}
		})
	}
}

// The create route widens the seam by exactly one caller header. A caller that
// asks for more — including the replay header it is only allowed to receive —
// still gets nothing across.
func TestWorkloadCreateForwardsOnlyTheIdempotencyKey(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
	req.Header.Set("Authorization", "Bearer token-value")
	req.Header.Set("Idempotency-Replayed", "true")
	req.Header.Set("Retry-After", "0")
	req.Header.Set("Cookie", "session=browser-cookie")
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	req.Header.Set("X-Admin", "1")
	req.Header.Set("X-Idempotency-Scope", "tenant")

	if rr := do(h, req); rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rr.Code)
	}
	if got.header.Get("Idempotency-Key") != validIdempotencyKey {
		t.Fatalf("upstream Idempotency-Key = %q", got.header.Get("Idempotency-Key"))
	}
	for _, banned := range []string{"Idempotency-Replayed", "Retry-After", "Cookie", "X-Forwarded-For", "X-Admin", "X-Idempotency-Scope"} {
		if got.header.Get(banned) != "" {
			t.Fatalf("upstream received %s = %q", banned, got.header.Get(banned))
		}
	}
}

// The browser has to tell a replay from a fresh admission and has to honour
// central's pacing, so those two headers come back — and nothing else does.
func TestWorkloadCreateAllowsBackOnlyReplayAndRetryHeaders(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Idempotency-Replayed", "true")
		w.Header().Set("Retry-After", "5")
		w.Header().Set("Set-Cookie", "upstream=session")
		w.Header().Set("X-Upstream-Node", "central-7")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"wl_1","status":"queued"}`)) //nolint:errcheck
	}))
	t.Cleanup(up.Close)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
	req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
	req.Header.Set("Authorization", "Bearer token-value")

	rr := do(h, req)
	if rr.Header().Get("Idempotency-Replayed") != "true" || rr.Header().Get("Retry-After") != "5" {
		t.Fatalf("replay headers = %v, want both relayed", rr.Header())
	}
	for _, banned := range []string{"Set-Cookie", "X-Upstream-Node"} {
		if rr.Header().Get(banned) != "" {
			t.Fatalf("browser received %s = %q", banned, rr.Header().Get(banned))
		}
	}

	// A read on the same seam keeps the old contract: no upstream header at all.
	get := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/workloads", nil)
	get.Header.Set("Authorization", "Bearer token-value")
	if rr := do(h, get); rr.Header().Get("Idempotency-Replayed") != "" || rr.Header().Get("Retry-After") != "" {
		t.Fatalf("read relayed upstream headers = %v", rr.Header())
	}
}

// Provisioning a run legitimately outlasts a read, so the create route gets its
// own deadline. The durations are shrunk here; the ordering they prove is the
// same one the constants have.
func TestWorkloadCreateOutlivesTheGenericTimeout(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"id":"wl_1"}`)) //nolint:errcheck
	}))
	t.Cleanup(up.Close)
	px := newAccountProxy(up.URL, up.URL, up.Client())
	px.timeout = 30 * time.Millisecond
	px.createTimeout = 5 * time.Second
	h, _ := newTestAppWithProxy(t, "", px)

	create := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
	create.Header.Set("Authorization", "Bearer token-value")
	if rr := do(h, create); rr.Code != http.StatusAccepted {
		t.Fatalf("slow create status = %d, want 202; body %s", rr.Code, rr.Body.String())
	}

	list := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/workloads", nil)
	list.Header.Set("Authorization", "Bearer token-value")
	if rr := do(h, list); rr.Code != http.StatusBadGateway {
		t.Fatalf("slow read status = %d, want 502 — the generic deadline still binds", rr.Code)
	}
}

// The shipped numbers, asserted where they are easy to get wrong: a client-wide
// Timeout shorter than the create deadline would cap it invisibly.
func TestUpstreamDeadlinesAreTheDocumentedOnes(t *testing.T) {
	px := newAccountProxy("http://id", "http://cloud", nil)
	if px.timeout != 10*time.Second {
		t.Fatalf("generic timeout = %s, want 10s", px.timeout)
	}
	if px.createTimeout != workloadCreateTimeout || px.createTimeout <= px.timeout {
		t.Fatalf("create timeout = %s, want the longer workload deadline", px.createTimeout)
	}
	if px.client.Timeout < px.createTimeout {
		t.Fatalf("client timeout %s silently caps the create deadline %s", px.client.Timeout, px.createTimeout)
	}
	if px.deadline(0) != px.timeout || px.deadline(px.createTimeout) != px.createTimeout {
		t.Fatalf("deadline() ignored the route's own bound")
	}
}

func TestAdminRoutesAreNotProxied(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"ok":true}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	for _, path := range []string{"/api/admin/tenants", "/api/v1/admin/tenants", "/api/tenants"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer token-value")
		if rr := do(h, req); rr.Code != http.StatusNotFound {
			t.Fatalf("path %q: status = %d, want 404", path, rr.Code)
		}
	}
	if got.path != "" {
		t.Fatalf("an unrouted path reached the upstream: %q", got.path)
	}
}

func TestUpstreamResponseIsCapped(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"pad":"` + strings.Repeat("x", maxUpstreamBytes+16) + `"}`)) //nolint:errcheck
	}))
	t.Cleanup(up.Close)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodGet, "/api/account", nil)
	req.Header.Set("Authorization", "Bearer token-value")
	if rr := do(h, req); rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
}

func TestMembersForbiddenPassesThrough(t *testing.T) {
	up, _ := upstreamRecorder(t, http.StatusForbidden, `{"error":"forbidden"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/members", nil)
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "forbidden") {
		t.Fatalf("body = %q, want the upstream JSON error", rr.Body.String())
	}
}

// The audit route is a GET-only read of one tenant's journal: only limit and
// after reach the upstream, and only the caller's own bearer goes with them.
func TestAuditQueryAllowlistAndHeaders(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"events":[],"next_after":""}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/audit?limit=200&after=aud_0123456789abcdef0123456789abcdef&action=membership.remove&account_id=acct_9", nil)
	req.Header.Set("Authorization", "Bearer audit-token")
	req.Header.Set("Cookie", "CF_Authorization=browser-cookie")
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	if rr := do(h, req); rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got.method != http.MethodGet || got.path != "/v1/tenants/cust_42/audit" {
		t.Fatalf("upstream saw %s %s", got.method, got.path)
	}
	if len(got.query) != 2 || got.query.Get("limit") != "200" || got.query.Get("after") != "aud_0123456789abcdef0123456789abcdef" {
		t.Fatalf("upstream query = %v, want only limit and after", got.query)
	}
	if got.header.Get("Authorization") != "Bearer audit-token" {
		t.Fatalf("upstream Authorization = %q", got.header.Get("Authorization"))
	}
	for _, header := range []string{"Cookie", "X-Forwarded-For"} {
		if got.header.Get(header) != "" {
			t.Fatalf("%s crossed the seam as %q", header, got.header.Get(header))
		}
	}
}

// A page bound the upstream would refuse is refused here, before the bearer
// travels. 200 is central's own ceiling for this route, so it must be allowed.
func TestAuditRejectsBadPageBoundsBeforeUpstream(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"events":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	for _, bad := range []string{"?limit=0", "?limit=201", "?limit=fifty", "?after=" + url.QueryEscape("aud_1 aud_2"), "?after=" + url.QueryEscape("../v1/admin")} {
		req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/audit"+bad, nil)
		req.Header.Set("Authorization", "Bearer audit-token")
		if rr := do(h, req); rr.Code != http.StatusBadRequest {
			t.Fatalf("query %q: status = %d, want 400", bad, rr.Code)
		}
		if got.path != "" {
			t.Fatalf("query %q reached the upstream", bad)
		}
	}
}

func TestAuditTenantPathValidation(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"events":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	for _, bad := range []string{
		"/api/tenants/..%2Fadmin/audit",
		"/api/tenants/cust%2042/audit",
		"/api/tenants/-bad/audit",
	} {
		req := httptest.NewRequest(http.MethodGet, bad, nil)
		req.Header.Set("Authorization", "Bearer audit-token")
		rr := do(h, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusNotFound {
			t.Fatalf("path %q: status = %d, want 400 or 404", bad, rr.Code)
		}
		if got.path != "" {
			t.Fatalf("path %q reached the upstream as %q", bad, got.path)
		}
	}
}

// Central decides who may read the journal. A 403 and its body must reach the
// browser intact, or the console cannot tell "your role may not" from "broken".
func TestAuditForbiddenAndCursorPassThrough(t *testing.T) {
	up, _ := upstreamRecorder(t, http.StatusForbidden, `{"error":"forbidden: reading the audit journal requires owner or admin"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/audit", nil)
	req.Header.Set("Authorization", "Bearer audit-token")
	rr := do(h, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "owner or admin") {
		t.Fatalf("body = %q, want the upstream refusal", rr.Body.String())
	}

	// The pagination cursor comes back exactly as central minted it; a rewritten
	// cursor would silently fetch a different page on the next call.
	page, _ := upstreamRecorder(t, http.StatusOK, `{"events":[{"id":"aud_0123456789abcdef0123456789abcdef"}],"next_after":"aud_0123456789abcdef0123456789abcdef"}`)
	h, _ = newTestAppWithProxy(t, "", newAccountProxy(page.URL, page.URL, page.Client()))
	req = httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/audit?limit=1", nil)
	req.Header.Set("Authorization", "Bearer audit-token")
	rr = do(h, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"next_after":"aud_0123456789abcdef0123456789abcdef"`) {
		t.Fatalf("body = %q, want the cursor verbatim", rr.Body.String())
	}
}

// The console renders every journal refusal through one error path keyed on the
// status, so each one has to arrive as JSON with the upstream status intact — a
// refusal flattened to 502 reads as an outage. An ingress in front of central
// answers HTML instead; the status still has to reach the console, the page
// must not.
func TestAuditRefusalsStayStructured(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		wantStatus  int
		wantBody    string
		denyBody    string
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, wantStatus: http.StatusUnauthorized, wantBody: "upstream refused"},
		{name: "forbidden", status: http.StatusForbidden, wantStatus: http.StatusForbidden, wantBody: "upstream refused"},
		{name: "not found", status: http.StatusNotFound, wantStatus: http.StatusNotFound, wantBody: "upstream refused"},
		{name: "rate limited", status: http.StatusTooManyRequests, wantStatus: http.StatusTooManyRequests, wantBody: "upstream refused"},
		{name: "bad gateway", status: http.StatusBadGateway, wantStatus: http.StatusBadGateway, wantBody: "upstream refused"},
		{name: "unavailable", status: http.StatusServiceUnavailable, wantStatus: http.StatusServiceUnavailable, wantBody: "upstream refused"},
		{
			name: "html error page", status: http.StatusServiceUnavailable,
			contentType: "text/html; charset=utf-8",
			body:        "<html><body>503 Service Temporarily Unavailable</body></html>",
			wantStatus:  http.StatusServiceUnavailable, denyBody: "<html>",
		},
		{
			name: "plain text refusal", status: http.StatusBadRequest,
			contentType: "text/plain; charset=utf-8", body: "after must be a cursor returned by this route",
			wantStatus: http.StatusBadRequest, wantBody: "after must be a cursor",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := tt.body
			contentType := tt.contentType
			if body == "" {
				body, contentType = `{"error":"upstream refused"}`, "application/json"
			}
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", contentType)
				w.WriteHeader(tt.status)
				w.Write([]byte(body)) //nolint:errcheck
			}))
			t.Cleanup(up.Close)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

			req := httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/audit", nil)
			req.Header.Set("Authorization", "Bearer audit-token")
			rr := do(h, req)
			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, tt.wantStatus)
			}
			if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", ct)
			}
			if rr.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", rr.Header().Get("Cache-Control"))
			}
			if tt.wantBody != "" && !strings.Contains(rr.Body.String(), tt.wantBody) {
				t.Fatalf("body = %q, want it to carry %q", rr.Body.String(), tt.wantBody)
			}
			if tt.denyBody != "" && strings.Contains(rr.Body.String(), tt.denyBody) {
				t.Fatalf("body = %q, want %q dropped", rr.Body.String(), tt.denyBody)
			}
		})
	}
}

// The journal advertises a 200-row page, so it has to be able to relay one. The
// larger ceiling is this route's alone: every other account route keeps the
// 256 KiB cap, and the journal still refuses a page beyond its own.
func TestAuditResponseCapIsItsOwnAndStillBounded(t *testing.T) {
	if maxAuditRespBytes <= maxUpstreamBytes {
		t.Fatalf("maxAuditRespBytes = %d, want more headroom than maxUpstreamBytes = %d", maxAuditRespBytes, maxUpstreamBytes)
	}
	tests := []struct {
		name       string
		path       string
		padTo      int
		wantStatus int
	}{
		{name: "audit page over the seam default", path: "/api/tenants/cust_42/audit", padTo: maxUpstreamBytes + 4096, wantStatus: http.StatusOK},
		{name: "audit page over its own ceiling", path: "/api/tenants/cust_42/audit", padTo: maxAuditRespBytes + 4096, wantStatus: http.StatusBadGateway},
		{name: "roster keeps the seam default", path: "/api/tenants/cust_42/members", padTo: maxUpstreamBytes + 4096, wantStatus: http.StatusBadGateway},
		{name: "account keeps the seam default", path: "/api/account", padTo: maxUpstreamBytes + 4096, wantStatus: http.StatusBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"events":[],"pad":"` + strings.Repeat("x", tt.padTo) + `"}`)) //nolint:errcheck
			}))
			t.Cleanup(up.Close)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Header.Set("Authorization", "Bearer audit-token")
			rr := do(h, req)
			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusBadGateway && strings.Contains(rr.Body.String(), "xxxx") {
				t.Fatalf("the oversized upstream body reached the browser")
			}
		})
	}
}

func TestAuditRequiresBearerAndIsGETOnly(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"events":[]}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	if rr := do(h, httptest.NewRequest(http.MethodGet, "/api/tenants/cust_42/audit", nil)); rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 without a bearer", rr.Code)
	}
	for _, method := range []string{http.MethodPost, http.MethodDelete, http.MethodPut} {
		req := httptest.NewRequest(method, "/api/tenants/cust_42/audit", nil)
		req.Header.Set("Authorization", "Bearer audit-token")
		if rr := do(h, req); rr.Code != http.StatusMethodNotAllowed && rr.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 405 or 404", method, rr.Code)
		}
	}
	if got.path != "" {
		t.Fatalf("a refused audit request reached the upstream as %q", got.path)
	}
}

// --- Placement preview ---

func TestPlacementPreviewSuccess(t *testing.T) {
	const previewResp = `{"status":"ok","placement":{"granted_cluster_id":"acme-lab"},"launch_token":"tok_opaque"}`
	up, got := upstreamRecorder(t, http.StatusOK, previewResp)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/placement-preview", strings.NewReader("kind: Workload\n"))
	req.Header.Set("Content-Type", "text/yaml")
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rr.Code, rr.Body.String())
	}
	if got.method != http.MethodPost || got.path != "/v1/tenants/cust_42/placement-preview" {
		t.Fatalf("upstream = %s %s", got.method, got.path)
	}
	if got.body != "kind: Workload\n" {
		t.Fatalf("upstream body = %q", got.body)
	}
	if got.header.Get("Content-Type") != "text/yaml" {
		t.Fatalf("upstream Content-Type = %q", got.header.Get("Content-Type"))
	}
	if got.header.Get("Authorization") != "Bearer token-value" {
		t.Fatalf("upstream Authorization = %q", got.header.Get("Authorization"))
	}
	if got.header.Get("Accept") != "application/json" {
		t.Fatalf("upstream Accept = %q", got.header.Get("Accept"))
	}
	for _, banned := range []string{"Cookie", "X-Forwarded-For", "Idempotency-Key"} {
		if got.header.Get(banned) != "" {
			t.Fatalf("upstream received banned header %s = %q", banned, got.header.Get(banned))
		}
	}
	if !strings.Contains(rr.Body.String(), "tok_opaque") {
		t.Fatalf("response body = %q, want launch_token", rr.Body.String())
	}
}

func TestPlacementPreviewWithClusterID(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{"status":"ok","placement":{"granted_cluster_id":"acme-lab"},"launch_token":"tok"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/placement-preview", strings.NewReader("kind: Workload\n"))
	req.Header.Set("Content-Type", "text/yaml")
	req.Header.Set("Authorization", "Bearer token-value")
	req.Header.Set("X-Cluster-ID", "acme-lab")
	rr := do(h, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got.header.Get("X-Cluster-ID") != "acme-lab" {
		t.Fatalf("upstream X-Cluster-ID = %q", got.header.Get("X-Cluster-ID"))
	}
}

func TestPlacementPreviewRefusal(t *testing.T) {
	const refusalResp = `{"status":"rejected","code":"no_capacity","message":"No eligible clusters."}`
	up, _ := upstreamRecorder(t, http.StatusOK, refusalResp)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/placement-preview", strings.NewReader("kind: Workload\n"))
	req.Header.Set("Content-Type", "text/yaml")
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "rejected") {
		t.Fatalf("body = %q, want rejection", rr.Body.String())
	}
}

func TestPlacementPreviewRequiresYAML(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/placement-preview", strings.NewReader(`{"yaml": true}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415; body %s", rr.Code, rr.Body.String())
	}
	if got.path != "" {
		t.Fatalf("rejected preview reached upstream")
	}
}

func TestPlacementPreviewCapsBody(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/placement-preview", strings.NewReader(strings.Repeat("x", 2<<20)))
	req.Header.Set("Content-Type", "text/yaml")
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rr.Code)
	}
	if got.path != "" {
		t.Fatalf("oversized preview reached upstream")
	}
}

func TestPlacementPreviewRejectsBadCluster(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusOK, `{}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := httptest.NewRequest(http.MethodPost, "/api/tenants/cust_42/placement-preview", strings.NewReader("kind: Workload\n"))
	req.Header.Set("Content-Type", "text/yaml")
	req.Header.Set("Authorization", "Bearer token-value")
	req.Header.Add("X-Cluster-ID", "a/b")
	rr := do(h, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if got.path != "" {
		t.Fatalf("bad cluster preview reached upstream")
	}
}

// --- Placement token on workload create ---

func TestWorkloadCreateForwardsPlacementToken(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
	req.Header.Set("Authorization", "Bearer token-value")
	req.Header.Set("X-Yscale-Placement-Token", "tok_signed_opaque_abc123") // gitleaks:allow -- deterministic test fixture
	rr := do(h, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", rr.Code, rr.Body.String())
	}
	if got.header.Get("X-Yscale-Placement-Token") != "tok_signed_opaque_abc123" {
		t.Fatalf("upstream token = %q", got.header.Get("X-Yscale-Placement-Token"))
	}
}

func TestWorkloadCreateAbsentTokenStaysAbsent(t *testing.T) {
	up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
	h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))

	req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
	req.Header.Set("Authorization", "Bearer token-value")
	rr := do(h, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rr.Code)
	}
	if _, ok := got.header["X-Yscale-Placement-Token"]; ok {
		t.Fatalf("an absent token was invented as %q", got.header.Get("X-Yscale-Placement-Token"))
	}
}

func TestWorkloadCreateRejectsBadPlacementTokens(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
	}{
		{name: "empty", values: []string{""}},
		{name: "space inside", values: []string{"tok abc"}},
		{name: "control char", values: []string{"tok\x00abc"}},
		{name: "overlong", values: []string{strings.Repeat("a", 4097)}},
		{name: "two tokens", values: []string{"tok_a", "tok_b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up, got := upstreamRecorder(t, http.StatusAccepted, `{"id":"wl_1"}`)
			h, _ := newTestAppWithProxy(t, "", newAccountProxy(up.URL, up.URL, up.Client()))
			req := createRequest("/api/tenants/cust_42/workloads", "kind: Workload\n")
			req.Header.Set("Authorization", "Bearer token-value")
			for _, v := range tc.values {
				req.Header.Add("X-Yscale-Placement-Token", v)
			}
			rr := do(h, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "invalid_placement_token") {
				t.Fatalf("body = %q, want placement token refusal", rr.Body.String())
			}
			if got.path != "" {
				t.Fatalf("rejected create reached upstream")
			}
		})
	}
}
