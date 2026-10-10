package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestApp(t *testing.T, exportToken string) (http.Handler, string) {
	t.Helper()
	return newTestAppWithProxy(t, exportToken, newAccountProxy("", "", nil))
}

// newTestAppWithProxy builds the handler over a temp static dir holding a stub
// index.html, so SPA-fallback routes (/account, /callback) can be asserted.
func newTestAppWithProxy(t *testing.T, exportToken string, px *accountProxy) (http.Handler, string) {
	return newTestAppWithProxyLimiter(t, exportToken, px, nil)
}

func newTestAppWithProxyLimiter(t *testing.T, exportToken string, px *accountProxy, proxyLimiter limiter) (http.Handler, string) {
	t.Helper()
	dir := t.TempDir()
	signupPath := filepath.Join(dir, "signups.jsonl")
	f, err := openAppend(signupPath)
	if err != nil {
		t.Fatalf("open signup file: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(spaIndexMarker), 0o600); err != nil {
		t.Fatalf("write index.html: %v", err)
	}
	st := &fileStore{path: signupPath, file: f}
	a := newApp(dir, exportToken, st, newRateLimiter(), px)
	if proxyLimiter != nil {
		a.proxyLimiter = proxyLimiter
	}
	return a.routes(), signupPath
}

const spaIndexMarker = "<!doctype html><title>yscale spa</title>"

func postSignup(h http.Handler, body, remoteAddr string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/signup", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "test-agent")
	req.RemoteAddr = remoteAddr
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestVanityRedirect(t *testing.T) {
	h, _ := newTestApp(t, "")
	req := httptest.NewRequest(http.MethodGet, "/kubagachi", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusFound {
		t.Fatalf("want 302, got %d", rr.Code)
	}
	if got := rr.Header().Get("Location"); got != "https://github.com/Yscale-sh/Kubagachi" {
		t.Fatalf("unexpected Location: %q", got)
	}
}

func TestIdentityRedirectUsesWebsiteEnvironment(t *testing.T) {
	h, _ := newTestApp(t, "")
	tests := []struct {
		name string
		host string
		path string
		want string
	}{
		{
			name: "dev preview",
			host: "yscale-dev.yscale.sh",
			path: "/login",
			want: "https://id-dev.yscale.sh/login?return_to=%2Faccount",
		},
		{
			name: "LAN preview",
			host: "10.0.0.218:8080",
			path: "/signup",
			want: "https://id-dev.yscale.sh/signup?return_to=%2Faccount",
		},
		{
			name: "production apex",
			host: "yscale.sh",
			path: "/waitlist",
			want: "https://id.kubagachi.com/signup?return_to=%2Faccount",
		},
		{
			name: "production www",
			host: "www.yscale.sh:443",
			path: "/access",
			want: "https://id.kubagachi.com/signup?return_to=%2Faccount",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Host = tt.host
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusFound {
				t.Fatalf("want 302, got %d", rr.Code)
			}
			if got := rr.Header().Get("Location"); got != tt.want {
				t.Fatalf("Location = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSignupHappyPath(t *testing.T) {
	h, signupPath := newTestApp(t, "")
	rr := postSignup(h, `{"email":"dev@example.com","track":"cloud","company":"Acme"}`, "10.0.0.1:1234")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rr.Code, rr.Body.String())
	}
	var resp map[string]bool
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil || !resp["ok"] {
		t.Fatalf("body = %q, want {\"ok\":true}", rr.Body.String())
	}
	data, err := os.ReadFile(signupPath)
	if err != nil {
		t.Fatalf("read signup file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("signup file has %d lines, want 1", len(lines))
	}
	var rec signupRecord
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("unmarshal stored line: %v", err)
	}
	if rec.Email != "dev@example.com" || rec.Track != "cloud" || rec.Company != "Acme" {
		t.Errorf("stored record = %+v", rec)
	}
	if rec.Ts == "" || rec.IP != "10.0.0.1" || rec.UA != "test-agent" {
		t.Errorf("stored metadata = %+v", rec)
	}
}

func TestSignupDefaultTrack(t *testing.T) {
	h, signupPath := newTestApp(t, "")
	if rr := postSignup(h, `{"email":"a@b.co"}`, "10.0.0.2:1234"); rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	data, _ := os.ReadFile(signupPath)
	var rec signupRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &rec); err != nil {
		t.Fatalf("unmarshal stored line: %v", err)
	}
	if rec.Track != "oss" {
		t.Errorf("track = %q, want oss (default)", rec.Track)
	}
}

func TestSignupHoneypot(t *testing.T) {
	h, signupPath := newTestApp(t, "")
	rr := postSignup(h, `{"email":"bot@spam.io","website":"http://spam"}`, "10.0.0.3:1234")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"ok":true`) {
		t.Errorf("body = %q, want ok:true", rr.Body.String())
	}
	st, err := os.Stat(signupPath)
	if err != nil {
		t.Fatalf("stat signup file: %v", err)
	}
	if st.Size() != 0 {
		t.Errorf("signup file size = %d, want 0 (honeypot must not store)", st.Size())
	}
}

func TestSignupBadEmail(t *testing.T) {
	h, _ := newTestApp(t, "")
	for _, body := range []string{
		`{"email":"not-an-email"}`,
		`{"email":""}`,
		fmt.Sprintf(`{"email":"%s@example.com"}`, strings.Repeat("a", 250)),
	} {
		if rr := postSignup(h, body, "10.0.0.4:1234"); rr.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, rr.Code)
		}
	}
}

func TestSignupBadTrack(t *testing.T) {
	h, _ := newTestApp(t, "")
	rr := postSignup(h, `{"email":"a@b.co","track":"enterprise"}`, "10.0.0.5:1234")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestSignupRateLimit(t *testing.T) {
	h, _ := newTestApp(t, "")
	const addr = "9.9.9.9:1234"
	for i := 0; i < rateLimit; i++ {
		if rr := postSignup(h, `{"email":"a@b.co"}`, addr); rr.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i+1, rr.Code)
		}
	}
	if rr := postSignup(h, `{"email":"a@b.co"}`, addr); rr.Code != http.StatusTooManyRequests {
		t.Errorf("6th request: status = %d, want 429", rr.Code)
	}
	// A different IP is unaffected.
	if rr := postSignup(h, `{"email":"a@b.co"}`, "8.8.8.8:1234"); rr.Code != http.StatusOK {
		t.Errorf("other IP: status = %d, want 200", rr.Code)
	}
}

func postUnsubscribe(h http.Handler, body, remoteAddr string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/unsubscribe", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "test-agent")
	req.RemoteAddr = remoteAddr
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestSignupDropsTrack(t *testing.T) {
	h, signupPath := newTestApp(t, "")
	if rr := postSignup(h, `{"email":"fan@kritters.dev","track":"drops"}`, "10.0.0.7:1234"); rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	data, _ := os.ReadFile(signupPath)
	var rec signupRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &rec); err != nil {
		t.Fatalf("unmarshal stored line: %v", err)
	}
	if rec.Track != "drops" {
		t.Errorf("track = %q, want drops", rec.Track)
	}
}

func TestUnsubscribe(t *testing.T) {
	t.Run("appends tombstone", func(t *testing.T) {
		h, signupPath := newTestApp(t, "")
		postSignup(h, `{"email":"a@b.co","track":"drops"}`, "10.0.1.1:1")
		if rr := postUnsubscribe(h, `{"email":"a@b.co"}`, "10.0.1.1:1"); rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rr.Code)
		}
		data, _ := os.ReadFile(signupPath)
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) != 2 {
			t.Fatalf("file has %d lines, want 2 (append-only)", len(lines))
		}
		var rec signupRecord
		if err := json.Unmarshal([]byte(lines[1]), &rec); err != nil {
			t.Fatalf("unmarshal tombstone: %v", err)
		}
		if rec.Action != "unsubscribe" || rec.Email != "a@b.co" {
			t.Errorf("tombstone = %+v", rec)
		}
	})
	t.Run("ok even when never subscribed (no enumeration)", func(t *testing.T) {
		h, _ := newTestApp(t, "")
		if rr := postUnsubscribe(h, `{"email":"ghost@never.io"}`, "10.0.1.2:1"); rr.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rr.Code)
		}
	})
	t.Run("honeypot stores nothing", func(t *testing.T) {
		h, signupPath := newTestApp(t, "")
		if rr := postUnsubscribe(h, `{"email":"bot@spam.io","website":"x"}`, "10.0.1.3:1"); rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rr.Code)
		}
		if st, _ := os.Stat(signupPath); st.Size() != 0 {
			t.Errorf("file size = %d, want 0", st.Size())
		}
	})
	t.Run("bad email 400", func(t *testing.T) {
		h, _ := newTestApp(t, "")
		if rr := postUnsubscribe(h, `{"email":"nope"}`, "10.0.1.4:1"); rr.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rr.Code)
		}
	})
}

func TestExportEffective(t *testing.T) {
	h, _ := newTestApp(t, "sekret")
	postSignup(h, `{"email":"keep@a.co","track":"drops"}`, "10.0.2.1:1")
	postSignup(h, `{"email":"gone@b.co","track":"drops"}`, "10.0.2.2:1")
	postSignup(h, `{"email":"resub@c.co","track":"drops"}`, "10.0.2.3:1")
	postUnsubscribe(h, `{"email":"gone@b.co"}`, "10.0.2.2:1")
	postUnsubscribe(h, `{"email":"resub@c.co"}`, "10.0.2.3:1")
	postSignup(h, `{"email":"resub@c.co","track":"drops"}`, "10.0.2.3:1") // re-subscribed after

	req := httptest.NewRequest(http.MethodGet, "/api/signups?effective=1", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "keep@a.co") {
		t.Errorf("effective export missing keep@a.co: %q", body)
	}
	if strings.Contains(body, "gone@b.co") {
		t.Errorf("effective export still contains unsubscribed gone@b.co: %q", body)
	}
	if !strings.Contains(body, "resub@c.co") {
		t.Errorf("effective export missing re-subscribed resub@c.co: %q", body)
	}
	if n := len(strings.Split(strings.TrimSpace(body), "\n")); n != 2 {
		t.Errorf("effective export has %d lines, want 2", n)
	}
}

func TestHealthz(t *testing.T) {
	h, _ := newTestApp(t, "")
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || rr.Body.String() != "ok" {
		t.Errorf("healthz = %d %q, want 200 ok", rr.Code, rr.Body.String())
	}
}

func TestExport(t *testing.T) {
	t.Run("404 when token unset", func(t *testing.T) {
		h, _ := newTestApp(t, "")
		req := httptest.NewRequest(http.MethodGet, "/api/signups", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", rr.Code)
		}
	})
	t.Run("401 with wrong bearer", func(t *testing.T) {
		h, _ := newTestApp(t, "sekret")
		req := httptest.NewRequest(http.MethodGet, "/api/signups", nil)
		req.Header.Set("Authorization", "Bearer wrong")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rr.Code)
		}
	})
	t.Run("200 with correct bearer streams file", func(t *testing.T) {
		h, _ := newTestApp(t, "sekret")
		postSignup(h, `{"email":"dev@example.com"}`, "10.0.0.6:1234")
		req := httptest.NewRequest(http.MethodGet, "/api/signups", nil)
		req.Header.Set("Authorization", "Bearer sekret")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "dev@example.com") {
			t.Errorf("export body = %q, want stored signup", rr.Body.String())
		}
	})
}

func TestKritters(t *testing.T) {
	h, _ := newTestApp(t, "")
	routes := []string{
		"/kritters/nori.png",
		"/kritters/nori-sheet.png",
		"/kritters/yscale-000.png",
		"/kritters/yscale-000-sheet.png",
	}
	for _, route := range routes {
		t.Run(route, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, route, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("want 200 OK for %s, got %d", route, rr.Code)
			}
			if got := rr.Header().Get("Content-Type"); got != "image/png" {
				t.Errorf("want image/png, got %q", got)
			}
			if rr.Body.Len() == 0 {
				t.Errorf("expected non-empty image content for %s", route)
			}
		})
	}

	t.Run("nonexistent", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/kritters/missing.png", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Errorf("want 404, got %d", rr.Code)
		}
	})
}
