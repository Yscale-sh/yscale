package workload

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the actual ToJob shell command with real curl/jq and local HTTP(S),
// not a second implementation of its downloader. Only apk is stubbed: these
// tools are already installed on the test host. Missing tools fail the gate.
func runCacheInit(t *testing.T, target, endpoint string, extraEnv ...string) (string, error) {
	t.Helper()
	for _, tool := range []string{"sh", "curl", "jq"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("cache-init execution tests require %s: %v", tool, err)
		}
	}
	job, err := ToJob(&Workload{
		APIVersion: APIVersion, Kind: Kind,
		Metadata: Metadata{Name: "cache-init-execution"},
		Spec: Spec{Image: "trainer", Size: "small", Storage: &Storage{Cache: []CacheSpec{{
			Name: "weights", Target: target,
			Source: BucketRef{Bucket: "models", Prefix: "weights/", CredentialsSecret: "model-creds"},
		}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	init := job.Spec.Template.Spec.InitContainers[0]
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "apk"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, init.Command[0], init.Command[1:]...)
	cmd.Dir = scratch
	cmd.Env = []string{
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"TMPDIR=" + scratch,
		"BURST_ID=fixture-burst",
		"BOOTSTRAP_ENDPOINT=" + endpoint,
		"NO_PROXY=*",
	}
	for _, env := range init.Env {
		if env.ValueFrom == nil {
			cmd.Env = append(cmd.Env, env.Name+"="+env.Value)
		}
	}
	cmd.Env = append(cmd.Env, extraEnv...)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("cache init did not finish: %v", ctx.Err())
	}
	entries, readErr := os.ReadDir(scratch)
	if readErr != nil || len(entries) != 0 {
		t.Errorf("cache init left signer/download scratch files: %v, %v", entries, readErr)
	}
	return string(output), err
}

func TestCacheInitDownloadsSignedHTTPURLs(t *testing.T) {
	for _, tls := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%v", tls), func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "models")
			files := map[string]string{
				"nested/model [1].bin":    "model weights\x00\xff",
				"config with spaces.json": `{"model":"fixture"}`,
				"empty.txt":               "",
			}
			var downloads atomic.Int32
			objects := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.RawQuery != "signature=test-only&selector=[1]&literal={a,b}" {
					t.Errorf("signed request changed: %s %s", r.Method, r.URL)
					http.Error(w, "bad signature", http.StatusForbidden)
					return
				}
				body, ok := files[strings.TrimPrefix(r.URL.Path, "/")]
				if !ok {
					http.NotFound(w, r)
					return
				}
				downloads.Add(1)
				_, _ = w.Write([]byte(body))
			}))
			var extraEnv []string
			if tls {
				objects.StartTLS()
				cert := filepath.Join(t.TempDir(), "ca.pem")
				if err := os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: objects.Certificate().Raw}), 0o600); err != nil {
					t.Fatal(err)
				}
				extraEnv = append(extraEnv, "CURL_CA_BUNDLE="+cert)
			} else {
				objects.Start()
			}
			defer objects.Close()
			signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					BurstID string            `json:"burst_id"`
					Mode    string            `json:"mode"`
					Source  map[string]string `json:"source"`
				}
				if r.Method != http.MethodPost || r.URL.Path != "/storage/sign-urls" || json.NewDecoder(r.Body).Decode(&req) != nil || req.BurstID != "fixture-burst" || req.Mode != "read" || req.Source["bucket"] != "models" || req.Source["prefix"] != "weights/" || req.Source["credentials_secret"] != "model-creds" {
					t.Error("cache init did not send its bound source to the signer")
					http.Error(w, "bad request", http.StatusBadRequest)
					return
				}
				urls := []map[string]string{}
				for key := range files {
					path := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(key, " ", "%20"), "[", "%5B"), "]", "%5D")
					urls = append(urls, map[string]string{"key": key, "url": objects.URL + "/" + path + "?signature=test-only&selector=[1]&literal={a,b}"})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"urls": urls})
			}))
			defer signer.Close()
			output, err := runCacheInit(t, target, signer.URL, extraEnv...)
			if err != nil {
				t.Fatalf("cache init failed: %v; %s", err, output)
			}
			if downloads.Load() != int32(len(files)) {
				t.Errorf("downloads = %d, want %d", downloads.Load(), len(files))
			}
			for key, want := range files {
				path := filepath.Join(target, key)
				got, err := os.ReadFile(path)
				if err != nil || string(got) != want {
					t.Errorf("cached %q = %q, %v; want %q", key, got, err, want)
				}
				if info, err := os.Stat(path); err == nil && info.Mode().Perm()&0o044 != 0o044 {
					t.Errorf("cached %q cannot be read by a non-root workload: %v", key, info.Mode())
				}
			}
			if strings.Contains(output, "signature=test-only") {
				t.Fatal("cache init logged a presigned URL")
			}
		})
	}
}

func TestCacheInitRejectsInvalidSignerResponses(t *testing.T) {
	cases := map[string]string{
		"malformed":           `{"urls":[`,
		"missing":             `{}`,
		"null":                `{"urls":null}`,
		"object":              `{"urls":{}}`,
		"entry type":          `{"urls":[1]}`,
		"missing fields":      `{"urls":[{}]}`,
		"nonstring key":       `{"urls":[{"key":5,"url":"http://example.invalid/model"}]}`,
		"nonstring URL":       `{"urls":[{"key":"model","url":5}]}`,
		"empty key":           `{"urls":[{"key":"","url":"http://example.invalid/model"}]}`,
		"absolute key":        `{"urls":[{"key":"/escape","url":"http://example.invalid/model"}]}`,
		"traversal":           `{"urls":[{"key":"../escape","url":"http://example.invalid/model"}]}`,
		"noncanonical key":    `{"urls":[{"key":"dir//model","url":"http://example.invalid/model"}]}`,
		"dot segment":         `{"urls":[{"key":"dir/./model","url":"http://example.invalid/model"}]}`,
		"backslash":           `{"urls":[{"key":"dir\\model","url":"http://example.invalid/model"}]}`,
		"control key":         `{"urls":[{"key":"model\nother","url":"http://example.invalid/model"}]}`,
		"local URL":           `{"urls":[{"key":"model","url":"file:///etc/passwd"}]}`,
		"duplicate key":       `{"urls":[{"key":"model","url":"http://example.invalid/a"},{"key":"model","url":"http://example.invalid/b"}]}`,
		"extra JSON document": `{"urls":[]} {"urls":[]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer signer.Close()
			target := t.TempDir()
			output, err := runCacheInit(t, target, signer.URL)
			if err == nil {
				t.Fatalf("invalid signer response allowed the workload to start: %s", output)
			}
			if !strings.Contains(output, "cache init failed: invalid signer response") {
				t.Errorf("missing safe diagnostic: %q", output)
			}
			entries, err := os.ReadDir(target)
			if err != nil || len(entries) != 0 {
				t.Errorf("invalid response modified cache: %v, %v", entries, err)
			}
		})
	}
}

func TestCacheInitEmptyPrefix(t *testing.T) {
	signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"urls":[]}`))
	}))
	defer signer.Close()
	if output, err := runCacheInit(t, t.TempDir(), signer.URL); err != nil {
		t.Fatalf("valid empty prefix failed: %v; %s", err, output)
	}
}

func TestCacheInitDownloadFailuresPreserveCache(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusPartialContent, http.StatusNoContent, http.StatusFound, http.StatusOK} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			const privateMarker = "test-only-upstream-bearer-marker"
			var redirected atomic.Bool
			objects := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/redirected" {
					redirected.Store(true)
				}
				w.Header().Set("Location", "/redirected?signature="+privateMarker)
				if status == http.StatusOK {
					// A 200 alone is insufficient: a truncated body must fail too.
					w.Header().Set("Content-Length", "1000")
				}
				w.WriteHeader(status)
				_, _ = w.Write([]byte(privateMarker))
			}))
			defer objects.Close()
			signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"urls": []map[string]string{{
					"key": "model.bin", "url": objects.URL + "/model?signature=" + privateMarker,
				}}})
			}))
			defer signer.Close()
			target := t.TempDir()
			file := filepath.Join(target, "model.bin")
			if err := os.WriteFile(file, []byte("previous complete model"), 0o644); err != nil {
				t.Fatal(err)
			}
			output, err := runCacheInit(t, target, signer.URL)
			if err == nil || !strings.Contains(output, "cache init failed: object download") {
				t.Errorf("failed download must stop init with a safe diagnostic: %v; %q", err, output)
			}
			if strings.Contains(output, privateMarker) || redirected.Load() {
				t.Error("failed download leaked a bearer URL/body or followed a redirect")
			}
			got, err := os.ReadFile(file)
			if err != nil || string(got) != "previous complete model" {
				t.Errorf("failed download overwrote the complete cache file: %q, %v", got, err)
			}
			entries, err := os.ReadDir(target)
			if err != nil || len(entries) != 1 || entries[0].Name() != "model.bin" {
				t.Errorf("failed download left a partial file: %v, %v", entries, err)
			}
		})
	}
}

func TestCacheInitRejectsSignerHTTPFailure(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusFound, http.StatusNoContent} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "/different-source")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"urls":[]}`))
			}))
			defer signer.Close()
			output, err := runCacheInit(t, t.TempDir(), signer.URL)
			if err == nil || !strings.Contains(output, "cache init failed: storage signing") {
				t.Errorf("signer HTTP failure must stop init: %v; %q", err, output)
			}
		})
	}
}

func TestCacheInitValidatesWholeManifestBeforeDownloads(t *testing.T) {
	var downloads atomic.Int32
	var signer *httptest.Server
	signer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/storage/sign-urls" {
			downloads.Add(1)
			_, _ = w.Write([]byte("model"))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"urls": []map[string]string{
			{"key": "model", "url": signer.URL + "/model"},
			{"key": "../escape", "url": signer.URL + "/invalid"},
		}})
	}))
	defer signer.Close()
	output, err := runCacheInit(t, t.TempDir(), signer.URL)
	if err == nil || downloads.Load() != 0 || !strings.Contains(output, "invalid signer response") {
		t.Fatalf("invalid later entry reached download: %v, downloads=%d; %q", err, downloads.Load(), output)
	}
}

func TestCacheInitRetriesTransientDownloadFailure(t *testing.T) {
	var downloads atomic.Int32
	var signer *httptest.Server
	signer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/storage/sign-urls" {
			_ = json.NewEncoder(w).Encode(map[string]any{"urls": []map[string]string{{"key": "model", "url": signer.URL + "/model"}}})
			return
		}
		if downloads.Add(1) == 1 {
			http.Error(w, "temporary upstream failure", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("complete model"))
	}))
	defer signer.Close()
	target := t.TempDir()
	if output, err := runCacheInit(t, target, signer.URL); err != nil {
		t.Fatalf("transient download was not recovered: %v; %s", err, output)
	}
	got, err := os.ReadFile(filepath.Join(target, "model"))
	if err != nil || string(got) != "complete model" || downloads.Load() != 2 {
		t.Fatalf("retry did not produce one complete file: %q, %v, downloads=%d", got, err, downloads.Load())
	}
}

func TestCacheInitRejectsFileDirectoryConflict(t *testing.T) {
	var signer *httptest.Server
	signer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/storage/sign-urls" {
			_, _ = w.Write([]byte("model"))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"urls": []map[string]string{
			{"key": "dir/model", "url": signer.URL + "/nested"},
			{"key": "dir", "url": signer.URL + "/conflict"},
		}})
	}))
	defer signer.Close()
	output, err := runCacheInit(t, t.TempDir(), signer.URL)
	if err == nil || !strings.Contains(output, "cache path conflict") {
		t.Fatalf("file/directory conflict allowed init to succeed: %v; %q", err, output)
	}
}
