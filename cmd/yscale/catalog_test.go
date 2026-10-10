package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogEndpointNormalizesServerAndUsesExactAutomationPath(t *testing.T) {
	got, err := catalogEndpoint("https://example.test/control/", "cust_a")
	if err != nil {
		t.Fatal(err)
	}
	if want := "https://example.test/control/v1/automation/tenants/cust_a/templates"; got != want {
		t.Fatalf("endpoint = %q, want %q", got, want)
	}
	if _, err := catalogEndpoint("https://example.test/?token=nope", "cust_a"); err == nil {
		t.Fatal("server URL query was accepted")
	}
}

func TestCatalogApplyYAMLReadsRevisionThenPutsOnce(t *testing.T) {
	t.Setenv("PUBLISHER_TOKEN", "secret-from-env")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/v1/automation/tenants/cust_a/templates" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-from-env" {
			t.Errorf("authorization = %q", got)
		}
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"catalog_revision":"rev-current","templates":[]}`))
		case http.MethodPut:
			var body struct {
				CatalogRevision string            `json:"catalog_revision"`
				Templates       []json.RawMessage `json:"templates"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.CatalogRevision != "rev-current" || len(body.Templates) != 1 {
				t.Errorf("put body = %+v", body)
			}
			_, _ = w.Write([]byte(`{"catalog_revision":"rev-next","templates":[]}`))
		default:
			t.Errorf("method = %s", r.Method)
		}
	}))
	defer server.Close()

	file := filepath.Join(t.TempDir(), "catalog.yaml")
	if err := os.WriteFile(file, []byte("templates:\n  - id: job\n    version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runCatalogApply([]string{"--server", server.URL + "/", "--tenant", "cust_a", "--token-env", "PUBLISHER_TOKEN", "-f", file}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want GET+PUT", calls)
	}
}

func TestCatalogApplyConflictDoesNotRetry(t *testing.T) {
	t.Setenv(defaultCatalogTokenEnv, "publisher-token")
	puts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"catalog_revision":"rev-current","templates":[]}`))
			return
		}
		puts++
		http.Error(w, `{"error":"template catalog changed"}`, http.StatusConflict)
	}))
	defer server.Close()
	file := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(file, []byte(`{"templates":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runCatalogApply([]string{"--server", server.URL, "--tenant", "cust_a", "-f", file})
	if err == nil || !strings.Contains(err.Error(), "did not retry or overwrite") {
		t.Fatalf("conflict error = %v", err)
	}
	if puts != 1 {
		t.Fatalf("PUT attempts = %d, want 1", puts)
	}
}

func TestCatalogApplyAcceptsEmptyTemplates(t *testing.T) {
	t.Setenv(defaultCatalogTokenEnv, "publisher-token")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"catalog_revision":"rev-current","templates":[{"id":"container-job","version":1}]}`))
		case http.MethodPut:
			var body struct {
				CatalogRevision string            `json:"catalog_revision"`
				Templates       []json.RawMessage `json:"templates"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				return
			}
			if body.CatalogRevision != "rev-current" || len(body.Templates) != 0 {
				t.Errorf("put body = %+v", body)
			}
			_, _ = w.Write([]byte(`{"catalog_revision":"rev-next","templates":[]}`))
		default:
			t.Fatalf("method = %s", r.Method)
		}
	}))
	defer server.Close()
	file := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(file, []byte(`{"templates":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runCatalogApply([]string{"--server", server.URL, "--tenant", "cust_a", "-f", file}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want GET+PUT", calls)
	}
}

func TestCatalogCommandDefaultTokenEnvUsedByDefault(t *testing.T) {
	if err := os.Setenv(defaultCatalogTokenEnv, "from-default"); err != nil {
		t.Fatal(err)
	}
	got := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/automation/tenants/cust_a/templates" {
			return
		}
		switch r.Method {
		case http.MethodGet:
			got = r.Header.Get("Authorization")
			_, _ = w.Write([]byte(`{"catalog_revision":"rev-current","templates":[]}`))
		case http.MethodPut:
			got = r.Header.Get("Authorization")
			_, _ = w.Write([]byte(`{"catalog_revision":"rev-next","templates":[]}`))
		}
	}))
	defer server.Close()
	file := filepath.Join(t.TempDir(), "catalog.yaml")
	if err := os.WriteFile(file, []byte("templates:\n  - id: job\n    version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runCatalogApply([]string{"--server", server.URL, "--tenant", "cust_a", "-f", file}); err != nil {
		t.Fatal(err)
	}
	if got != "Bearer from-default" {
		t.Fatalf("authorization = %q, want %q", got, "Bearer from-default")
	}
}

func TestCatalogCommandsNeverAcceptTokenFlag(t *testing.T) {
	if err := runCatalogApply([]string{"--server", "https://example.test", "--tenant", "cust_a", "-f", "catalog.yaml", "--token", "secret"}); err == nil {
		t.Fatal("--token flag was accepted")
	}
}

func TestCatalogRequestRefusesRedirectWithoutLeakingBearer(t *testing.T) {
	t.Setenv(defaultCatalogTokenEnv, "redirect-secret")
	targetCalls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls++
		if r.Header.Get("Authorization") != "" {
			t.Errorf("redirect target received Authorization header")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/stolen", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	_, err := catalogRequest(catalogCLIOptions{
		server: redirect.URL, tenant: "cust_a", tokenEnv: defaultCatalogTokenEnv,
	}, http.MethodGet, nil)
	if err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Fatalf("redirect error = %v, want refused HTTP 307", err)
	}
	if targetCalls != 0 {
		t.Fatalf("redirect target called %d times, want 0", targetCalls)
	}
}
