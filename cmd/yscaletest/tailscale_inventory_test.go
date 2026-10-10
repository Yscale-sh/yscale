package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

const fakeTSClientID = "ts-client-id-do-not-log"
const fakeTSClientSecret = "ts-client-secret-do-not-log"

type tailscaleStub struct {
	oauthStatus   int
	oauthBody     string
	devicesBody   string
	devicesStatus int

	mu         sync.Mutex
	methods    []string
	paths      []string
	authTokens []string
}

func (s *tailscaleStub) observe(method, path, authToken string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.methods = append(s.methods, method)
	s.paths = append(s.paths, path)
	s.authTokens = append(s.authTokens, authToken)
}

func (s *tailscaleStub) seen() (methods, paths, tokens []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.methods...),
		append([]string(nil), s.paths...),
		append([]string(nil), s.authTokens...)
}

func newTailscaleStub(t *testing.T, s *tailscaleStub) *tailscaleInventory {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.observe(r.Method, r.URL.Path, r.Header.Get("Authorization"))

		if strings.HasSuffix(r.URL.Path, "/oauth/token") {
			if s.oauthStatus != 0 {
				w.WriteHeader(s.oauthStatus)
				fmt.Fprint(w, s.oauthBody)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token":"test-bearer-token","expires_in":3600}`)
			return
		}

		if strings.Contains(r.URL.Path, "/devices") {
			if r.Method != http.MethodGet {
				t.Errorf("devices endpoint called with %s, want GET", r.Method)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			if s.devicesStatus != 0 {
				w.WriteHeader(s.devicesStatus)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, s.devicesBody)
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	inv := newTailscaleInventory(fakeTSClientID, fakeTSClientSecret, "test-tailnet")
	inv.apiBase = srv.URL
	return inv
}

func TestTailscaleInventoryFindsExactHostname(t *testing.T) {
	stub := &tailscaleStub{
		devicesBody: `{"devices":[
			{"id":"1","hostname":"yscale-burst-abc123def456"},
			{"id":"2","hostname":"yscale-burst-other"},
			{"id":"3","hostname":"yscale-burst-abc123def456"}
		]}`,
	}
	inv := newTailscaleStub(t, stub)

	count, err := inv.FindDevice(context.Background(), "yscale-burst-abc123def456")
	if err != nil {
		t.Fatalf("FindDevice failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("want 2 matching devices, got %d", count)
	}
}

func TestTailscaleInventoryReportsProviderName(t *testing.T) {
	inv := newTailscaleInventory("client", "secret", "example.com")
	if got := inv.ProviderName(); got != "tailscale" {
		t.Fatalf("ProviderName() = %q, want tailscale", got)
	}
}

func TestTailscaleInventoryReportsAbsence(t *testing.T) {
	stub := &tailscaleStub{
		devicesBody: `{"devices":[{"id":"1","hostname":"yscale-burst-other"}]}`,
	}
	inv := newTailscaleStub(t, stub)

	count, err := inv.FindDevice(context.Background(), "yscale-burst-abc123def456")
	if err != nil {
		t.Fatalf("FindDevice failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("want 0, got %d", count)
	}
}

func TestTailscaleInventoryUsesOnlyGETForDevices(t *testing.T) {
	stub := &tailscaleStub{
		devicesBody: `{"devices":[]}`,
	}
	inv := newTailscaleStub(t, stub)

	_, err := inv.FindDevice(context.Background(), "yscale-burst-test")
	if err != nil {
		t.Fatalf("FindDevice failed: %v", err)
	}

	methods, paths, _ := stub.seen()
	for i, m := range methods {
		if strings.Contains(paths[i], "/devices") && m != http.MethodGet {
			t.Fatalf("device endpoint used %s, want GET-only", m)
		}
	}
	oauthPosts := 0
	for i, m := range methods {
		if strings.Contains(paths[i], "/oauth/token") {
			if m != http.MethodPost {
				t.Fatalf("oauth token endpoint used %s, want POST", m)
			}
			oauthPosts++
		}
	}
	if oauthPosts == 0 {
		t.Fatal("no OAuth token exchange observed")
	}
}

func TestTailscaleInventoryNeverLeaksCredentials(t *testing.T) {
	stub := &tailscaleStub{
		devicesBody: `{"devices":[]}`,
	}
	inv := newTailscaleStub(t, stub)

	_, _ = inv.FindDevice(context.Background(), "yscale-burst-test")

	_, _, tokens := stub.seen()
	for _, tok := range tokens {
		if strings.Contains(tok, fakeTSClientID) {
			t.Fatal("client ID leaked in Authorization header")
		}
		if strings.Contains(tok, fakeTSClientSecret) {
			t.Fatal("client secret leaked in Authorization header")
		}
	}
}

func TestTailscaleInventoryFailsOnOAuthError(t *testing.T) {
	stub := &tailscaleStub{
		oauthStatus: http.StatusUnauthorized,
		oauthBody:   fmt.Sprintf(`{"error":"invalid_client","error_description":"bad credentials: %s"}`, fakeTSClientSecret),
	}
	inv := newTailscaleStub(t, stub)

	_, err := inv.FindDevice(context.Background(), "yscale-burst-test")
	if err == nil {
		t.Fatal("should fail on OAuth error")
	}
	msg := err.Error()
	if strings.Contains(msg, fakeTSClientSecret) {
		t.Fatalf("error leaked client secret: %q", msg)
	}
	if !strings.Contains(msg, "401") {
		t.Fatalf("error should include status code, got %q", msg)
	}
}

func TestTailscaleInventoryFailsOnDeviceListError(t *testing.T) {
	stub := &tailscaleStub{
		devicesStatus: http.StatusForbidden,
	}
	inv := newTailscaleStub(t, stub)

	_, err := inv.FindDevice(context.Background(), "yscale-burst-test")
	if err == nil {
		t.Fatal("should fail on device list error")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("error should include status code, got %q", err.Error())
	}
}

func TestTailscaleInventoryRefusesEmptyHostname(t *testing.T) {
	inv := newTailscaleStub(t, &tailscaleStub{devicesBody: `{"devices":[]}`})

	_, err := inv.FindDevice(context.Background(), "")
	if err == nil {
		t.Fatal("should refuse empty hostname")
	}
}

func TestTailscaleInventoryHonoursContextCancellation(t *testing.T) {
	inv := newTailscaleStub(t, &tailscaleStub{devicesBody: `{"devices":[]}`})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := inv.FindDevice(ctx, "yscale-burst-test")
	if err == nil {
		t.Fatal("should fail on cancelled context")
	}
}

func TestTailscaleInventoryConcurrentRaceSafe(t *testing.T) {
	stub := &tailscaleStub{
		devicesBody: `{"devices":[{"id":"1","hostname":"yscale-burst-abc123def456"}]}`,
	}
	inv := newTailscaleStub(t, stub)

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, errs[n] = inv.FindDevice(context.Background(), "yscale-burst-abc123def456")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
}

func TestTailscaleInventoryMissingDevicesField(t *testing.T) {
	stub := &tailscaleStub{
		devicesBody: `{}`,
	}
	inv := newTailscaleStub(t, stub)

	_, err := inv.FindDevice(context.Background(), "yscale-burst-test")
	if err == nil {
		t.Fatal("missing 'devices' field should fail")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error should mention missing field, got %q", err.Error())
	}
}

func TestBurstTailscaleHostname(t *testing.T) {
	tests := []struct {
		burstID string
		want    string
	}{
		{"burst_abc123def456", "yscale-burst-abc123def456"},
		{"burst_ab_cd", "yscale-burst-ab-cd"},
		{"", ""},
	}
	for _, tc := range tests {
		got := burstTailscaleHostname(tc.burstID)
		if got != tc.want {
			t.Errorf("burstTailscaleHostname(%q) = %q, want %q", tc.burstID, got, tc.want)
		}
	}
}
