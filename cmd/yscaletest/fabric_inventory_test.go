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

const fakeFabricAPIKey = "hs-api-key-do-not-log"

type fabricStub struct {
	nodesBody   string
	nodesStatus int

	mu         sync.Mutex
	methods    []string
	paths      []string
	rawQueries []string
	authTokens []string
}

func (s *fabricStub) observe(method, path, rawQuery, authToken string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.methods = append(s.methods, method)
	s.paths = append(s.paths, path)
	s.rawQueries = append(s.rawQueries, rawQuery)
	s.authTokens = append(s.authTokens, authToken)
}

func (s *fabricStub) seen() (methods, paths, rawQueries, tokens []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.methods...),
		append([]string(nil), s.paths...),
		append([]string(nil), s.rawQueries...),
		append([]string(nil), s.authTokens...)
}

func newFabricStub(t *testing.T, s *fabricStub) *fabricInventory {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.observe(r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"))
		if r.URL.Path == "/api/v1/node" {
			if r.Method != http.MethodGet {
				t.Errorf("nodes endpoint called with %s, want GET", r.Method)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			if s.nodesStatus != 0 {
				w.WriteHeader(s.nodesStatus)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, s.nodesBody)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	return newFabricInventory(srv.URL, fakeFabricAPIKey, "cust_test")
}

func TestFabricInventoryFindsExactHostnameByGivenName(t *testing.T) {
	stub := &fabricStub{
		nodesBody: `{"nodes":[
			{"id":"1","name":"foo","givenName":"yscale-burst-abc123def456"},
			{"id":"2","name":"bar","givenName":"yscale-burst-other"},
			{"id":"3","name":"baz","givenName":"yscale-burst-abc123def456"}
		]}`,
	}
	inv := newFabricStub(t, stub)

	count, err := inv.FindDevice(context.Background(), "yscale-burst-abc123def456")
	if err != nil {
		t.Fatalf("FindDevice: %v", err)
	}
	if count != 2 {
		t.Fatalf("want 2 matching devices, got %d", count)
	}
}

func TestFabricInventoryReportsProviderName(t *testing.T) {
	inv := newFabricInventory("https://fabric.example", fakeFabricAPIKey, "cust_test")
	if got := inv.ProviderName(); got != "fabric" {
		t.Fatalf("ProviderName() = %q, want fabric", got)
	}
}

func TestFabricInventoryFindsExactHostnameByName(t *testing.T) {
	stub := &fabricStub{
		nodesBody: `{"nodes":[
			{"id":"1","name":"yscale-burst-abc123def456","givenName":"something-else"}
		]}`,
	}
	inv := newFabricStub(t, stub)

	count, err := inv.FindDevice(context.Background(), "yscale-burst-abc123def456")
	if err != nil {
		t.Fatalf("FindDevice: %v", err)
	}
	if count != 1 {
		t.Fatalf("want 1 matching device (via name), got %d", count)
	}
}

func TestFabricInventoryTolerantDecoding(t *testing.T) {
	// Fabric versions/proxies emit ids as either string or number, and
	// carry extra fields (createdAt, ipAddresses, machineKey, ...) the audit
	// does not care about. Decoding must ignore them without failing.
	stub := &fabricStub{
		nodesBody: `{"nodes":[
			{"id":1,"name":"yscale-burst-num","createdAt":"2026-08-24T00:00:00Z","ipAddresses":["100.64.0.1"]},
			{"id":"2","name":"yscale-burst-str","machineKey":"mkey:abc"},
			{"id":"3","name":"other","given_name":"yscale-burst-snake"}
		]}`,
	}
	inv := newFabricStub(t, stub)

	if _, err := inv.FindDevice(context.Background(), "yscale-burst-num"); err != nil {
		t.Fatalf("FindDevice numeric id: %v", err)
	}
	if _, err := inv.FindDevice(context.Background(), "yscale-burst-str"); err != nil {
		t.Fatalf("FindDevice string id: %v", err)
	}
	count, err := inv.FindDevice(context.Background(), "yscale-burst-snake")
	if err != nil {
		t.Fatalf("FindDevice snake-case given name: %v", err)
	}
	if count != 1 {
		t.Fatalf("snake-case given name count = %d, want 1", count)
	}
}

func TestFabricInventoryReportsAbsence(t *testing.T) {
	stub := &fabricStub{
		nodesBody: `{"nodes":[{"id":"1","name":"other","givenName":"other-given"}]}`,
	}
	inv := newFabricStub(t, stub)

	count, err := inv.FindDevice(context.Background(), "yscale-burst-abc123def456")
	if err != nil {
		t.Fatalf("FindDevice: %v", err)
	}
	if count != 0 {
		t.Fatalf("want 0, got %d", count)
	}
}

func TestFabricInventoryUsesOnlyGETScopedByUser(t *testing.T) {
	stub := &fabricStub{nodesBody: `{"nodes":[]}`}
	inv := newFabricStub(t, stub)

	if _, err := inv.FindDevice(context.Background(), "yscale-burst-test"); err != nil {
		t.Fatalf("FindDevice: %v", err)
	}
	methods, paths, queries, _ := stub.seen()
	if len(methods) != 1 {
		t.Fatalf("want exactly 1 request, got %d", len(methods))
	}
	if methods[0] != http.MethodGet {
		t.Fatalf("method = %s, want GET-only", methods[0])
	}
	if paths[0] != "/api/v1/node" {
		t.Fatalf("path = %s, want /api/v1/node", paths[0])
	}
	if !strings.Contains(queries[0], "user=cust_test") {
		t.Fatalf("query %q must scope by user", queries[0])
	}
}

func TestFabricInventoryRefusesInsecureBaseURL(t *testing.T) {
	inv := newFabricInventory("http://fabric.example", fakeFabricAPIKey, "cust_test")
	_, err := inv.FindDevice(context.Background(), "yscale-burst-test")
	if err == nil {
		t.Fatal("plain http (non-loopback) must be rejected")
	}
	if !strings.Contains(err.Error(), "https") {
		t.Fatalf("error should mention https requirement, got %q", err.Error())
	}
	if strings.Contains(err.Error(), fakeFabricAPIKey) {
		t.Fatalf("error leaked API key: %q", err.Error())
	}
}

func TestFabricInventoryLoopbackHTTPAllowed(t *testing.T) {
	// httptest.NewServer uses a 127.0.0.1 URL — proves the loopback carveout.
	stub := &fabricStub{nodesBody: `{"nodes":[]}`}
	inv := newFabricStub(t, stub)
	if !strings.HasPrefix(inv.baseURL, "http://127.0.0.1") {
		t.Fatalf("expected loopback http base URL, got %q", inv.baseURL)
	}
	if _, err := inv.FindDevice(context.Background(), "yscale-burst-test"); err != nil {
		t.Fatalf("FindDevice against loopback should be allowed: %v", err)
	}
}

func TestFabricInventoryRefusesMalformedBaseURL(t *testing.T) {
	inv := newFabricInventory("::not a url::", fakeFabricAPIKey, "cust_test")
	if _, err := inv.FindDevice(context.Background(), "yscale-burst-test"); err == nil {
		t.Fatal("malformed base URL must be rejected before any request")
	}
}

func TestFabricInventoryFailsOnListError(t *testing.T) {
	stub := &fabricStub{nodesStatus: http.StatusForbidden}
	inv := newFabricStub(t, stub)

	_, err := inv.FindDevice(context.Background(), "yscale-burst-test")
	if err == nil {
		t.Fatal("should fail on list error")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("error should include status, got %q", err.Error())
	}
	if strings.Contains(err.Error(), fakeFabricAPIKey) {
		t.Fatalf("error leaked API key: %q", err.Error())
	}
}

func TestFabricInventoryFailsOnInvalidJSON(t *testing.T) {
	stub := &fabricStub{nodesBody: `{"nodes":`}
	inv := newFabricStub(t, stub)

	_, err := inv.FindDevice(context.Background(), "yscale-burst-test")
	if err == nil {
		t.Fatal("invalid JSON must fail closed")
	}
	if !strings.Contains(err.Error(), "valid JSON") {
		t.Fatalf("error should mention JSON, got %q", err.Error())
	}
}

func TestFabricInventoryFailsOnMissingNodesEnvelope(t *testing.T) {
	stub := &fabricStub{nodesBody: `{}`}
	inv := newFabricStub(t, stub)

	_, err := inv.FindDevice(context.Background(), "yscale-burst-test")
	if err == nil {
		t.Fatal("missing 'nodes' field must fail closed")
	}
	if !strings.Contains(err.Error(), "missing 'nodes'") {
		t.Fatalf("error should mention missing envelope, got %q", err.Error())
	}
}

func TestFabricInventoryFailsOnTruncatedBody(t *testing.T) {
	// Emit strictly more than fabricMaxListBytes so the LimitReader trips.
	// A single large buffer keeps the test to a single ResponseWriter.Write.
	oversized := make([]byte, fabricMaxListBytes+64)
	for i := range oversized {
		oversized[i] = 'a'
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"nodes":[`))
		_, _ = w.Write(oversized)
	}))
	t.Cleanup(srv.Close)

	inv := newFabricInventory(srv.URL, fakeFabricAPIKey, "cust_test")
	_, err := inv.FindDevice(context.Background(), "yscale-burst-test")
	if err == nil {
		t.Fatal("truncated body must fail closed")
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("error should mention truncation, got %q", err.Error())
	}
}

func TestFabricInventoryRefusesEmptyHostname(t *testing.T) {
	inv := newFabricStub(t, &fabricStub{nodesBody: `{"nodes":[]}`})
	if _, err := inv.FindDevice(context.Background(), ""); err == nil {
		t.Fatal("empty hostname must be refused")
	}
}

func TestFabricInventoryRefusesEmptyUser(t *testing.T) {
	stub := &fabricStub{nodesBody: `{"nodes":[]}`}
	inv := newFabricStub(t, stub)
	inv.user = ""
	if _, err := inv.FindDevice(context.Background(), "yscale-burst-test"); err == nil {
		t.Fatal("empty user must be refused")
	}
}

func TestFabricInventoryHonoursContextCancellation(t *testing.T) {
	inv := newFabricStub(t, &fabricStub{nodesBody: `{"nodes":[]}`})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := inv.FindDevice(ctx, "yscale-burst-test"); err == nil {
		t.Fatal("cancelled context must fail")
	}
}

func TestFabricInventoryNeverLeaksAPIKeyInErrors(t *testing.T) {
	stub := &fabricStub{nodesStatus: http.StatusUnauthorized}
	inv := newFabricStub(t, stub)

	_, err := inv.FindDevice(context.Background(), "yscale-burst-test")
	if err == nil {
		t.Fatal("401 must fail")
	}
	if strings.Contains(err.Error(), fakeFabricAPIKey) {
		t.Fatalf("error leaked API key: %q", err.Error())
	}

	_, _, _, tokens := stub.seen()
	for _, tok := range tokens {
		if !strings.HasPrefix(tok, "Bearer ") {
			t.Fatalf("Authorization must be Bearer scheme, got %q", tok)
		}
	}
}

func TestFabricInventoryTrailingSlashOnBaseURL(t *testing.T) {
	stub := &fabricStub{nodesBody: `{"nodes":[]}`}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stub.observe(r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"nodes":[]}`)
	}))
	t.Cleanup(srv.Close)

	inv := newFabricInventory(srv.URL+"/", fakeFabricAPIKey, "cust_test")
	if _, err := inv.FindDevice(context.Background(), "yscale-burst-test"); err != nil {
		t.Fatalf("FindDevice: %v", err)
	}
	_, paths, _, _ := stub.seen()
	if len(paths) != 1 || paths[0] != "/api/v1/node" {
		t.Fatalf("trailing slash must be trimmed; got path %v", paths)
	}
}
