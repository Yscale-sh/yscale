package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

func TestArtifactUploaderSignsAttemptPrefixAndUploadsFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "metrics.json"), []byte("metrics"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "model.bin"), []byte("model"), 0o644); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	uploaded := map[string]string{}
	var signerRequest SignURLsRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/storage/sign-urls":
			if err := json.NewDecoder(r.Body).Decode(&signerRequest); err != nil {
				t.Error(err)
			}
			out := SignURLsResponse{}
			for _, key := range signerRequest.Objects {
				out.URLs = append(out.URLs, SignedURL{Key: key, URL: "http://" + r.Host + "/put/" + key})
			}
			_ = json.NewEncoder(w).Encode(out)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/put/"):
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			uploaded[strings.TrimPrefix(r.URL.Path, "/put/")] = string(body)
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	source := protocol.BucketRef{Bucket: "results", Prefix: "team/", CredentialsSecret: "r2-creds"}
	result, err := RunArtifactUploader(context.Background(), server.URL, "burst_x", []ArtifactUpload{{
		LocalPath: root, KeyPrefix: "wl_1/train-pod", Source: source, MaxFiles: 10, MaxBytes: 1024,
	}})
	if err != nil {
		t.Fatalf("RunArtifactUploader: %v", err)
	}
	if result.ObjectsUploaded != 2 || result.BytesUploaded != int64(len("metrics")+len("model")) {
		t.Fatalf("result = %+v, want 2 objects / 12 bytes", result)
	}
	if signerRequest.Mode != "write" || signerRequest.Source != source {
		t.Fatalf("signer request = %+v", signerRequest)
	}
	sort.Strings(signerRequest.Objects)
	want := []string{"wl_1/train-pod/metrics.json", "wl_1/train-pod/nested/model.bin"}
	if strings.Join(signerRequest.Objects, ",") != strings.Join(want, ",") {
		t.Fatalf("signed objects = %v, want %v", signerRequest.Objects, want)
	}
	if uploaded[want[0]] != "metrics" || uploaded[want[1]] != "model" {
		t.Fatalf("uploaded = %+v", uploaded)
	}
}

func TestCollectArtifactFilesRejectsSymlinkAndLimits(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "one"), []byte("1234"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "two"), []byte("5678"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := ArtifactUpload{LocalPath: root, KeyPrefix: "wl/pod", MaxFiles: 1, MaxBytes: 100}
	if _, err := collectArtifactFiles(base); err == nil || !strings.Contains(err.Error(), "file limit") {
		t.Fatalf("file limit error = %v", err)
	}
	base.MaxFiles = 2
	base.MaxBytes = 7
	if _, err := collectArtifactFiles(base); err == nil || !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("byte limit error = %v", err)
	}
	base.MaxBytes = 100
	if err := os.Symlink("one", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	base.MaxFiles = 3
	if _, err := collectArtifactFiles(base); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink error = %v", err)
	}
}

func TestArtifactUploaderRejectsSignerResponseMismatch(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "result"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(SignURLsResponse{URLs: []SignedURL{{Key: "not-requested", URL: "https://example.invalid"}}})
	}))
	defer server.Close()
	result, err := RunArtifactUploader(context.Background(), server.URL, "burst_x", []ArtifactUpload{{
		LocalPath: root, KeyPrefix: "wl/pod", Source: protocol.BucketRef{Bucket: "b"}, MaxFiles: 1, MaxBytes: 10,
	}})
	if err == nil || !strings.Contains(err.Error(), "unrequested object key") {
		t.Fatalf("mismatched signer response error = %v", err)
	}
	if artifactFailureCategory(err) != artifactFailureSigner {
		t.Errorf("category = %q, want signer", artifactFailureCategory(err))
	}
	if result.ObjectsUploaded != 0 || result.BytesUploaded != 0 {
		t.Errorf("result = %+v, want nothing counted", result)
	}
}

// A partial upload is reported as what it moved, not as all-or-nothing: the
// objects that reached the bucket before the failure are still there, and the
// receipt says so.
func TestArtifactUploaderCountsOnlyCompletedPuts(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a-first"), []byte("aaaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b-second"), []byte("bb"), 0o644); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/storage/sign-urls":
			var req SignURLsRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			out := SignURLsResponse{}
			for _, key := range req.Objects {
				out.URLs = append(out.URLs, SignedURL{Key: key, URL: "http://" + r.Host + "/put/" + key})
			}
			_ = json.NewEncoder(w).Encode(out)
		case strings.HasSuffix(r.URL.Path, "/a-first"):
			w.WriteHeader(http.StatusNoContent)
		default:
			// 403 is terminal: the uploader must not retry it.
			http.Error(w, "denied", http.StatusForbidden)
		}
	}))
	defer server.Close()

	result, err := RunArtifactUploader(context.Background(), server.URL, "burst_x", []ArtifactUpload{{
		LocalPath: root, KeyPrefix: "wl/pod", Source: protocol.BucketRef{Bucket: "b"}, MaxFiles: 5, MaxBytes: 1024,
	}})
	if err == nil {
		t.Fatal("partial upload reported success")
	}
	if artifactFailureCategory(err) != artifactFailureUpload {
		t.Errorf("category = %q, want upload", artifactFailureCategory(err))
	}
	if result.ObjectsUploaded != 1 || result.BytesUploaded != 4 {
		t.Fatalf("result = %+v, want the one object that completed (4 bytes)", result)
	}
}

// A retried PUT counts once, and only after the attempt that actually
// succeeded — a 5xx the object store recovered from must not be counted twice,
// and one it never recovered from must not be counted at all.
func TestArtifactUploaderCountsRetriedPutExactlyOnce(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "flaky"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	puts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/storage/sign-urls" {
			var req SignURLsRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			out := SignURLsResponse{}
			for _, key := range req.Objects {
				out.URLs = append(out.URLs, SignedURL{Key: key, URL: "http://" + r.Host + "/put/" + key})
			}
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		mu.Lock()
		puts++
		attempt := puts
		mu.Unlock()
		if attempt < 3 {
			http.Error(w, "try again", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	result, err := RunArtifactUploader(context.Background(), server.URL, "burst_x", []ArtifactUpload{{
		LocalPath: root, KeyPrefix: "wl/pod", Source: protocol.BucketRef{Bucket: "b"}, MaxFiles: 5, MaxBytes: 1024,
	}})
	if err != nil {
		t.Fatalf("RunArtifactUploader: %v", err)
	}
	if result.ObjectsUploaded != 1 || result.BytesUploaded != int64(len("payload")) {
		t.Fatalf("result = %+v, want the retried object counted exactly once", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if puts != 3 {
		t.Fatalf("PUT attempts = %d, want 3", puts)
	}
}

// The uploader leaves a summary behind on both paths: it is the only thing the
// completion watcher will believe about counts.
func TestArtifactUploadSummaryWrittenOnBothPaths(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "result"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	uploads, err := json.Marshal([]ArtifactUpload{{
		LocalPath: root, KeyPrefix: "wl/pod", Source: protocol.BucketRef{Bucket: "b"}, MaxFiles: 5, MaxBytes: 1024,
	}})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/storage/sign-urls" {
				var req SignURLsRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				out := SignURLsResponse{}
				for _, key := range req.Objects {
					out.URLs = append(out.URLs, SignedURL{Key: key, URL: "http://" + r.Host + "/put/" + key})
				}
				_ = json.NewEncoder(w).Encode(out)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		path := filepath.Join(t.TempDir(), "termination-log")
		if err := runArtifactUploadCommand(context.Background(), path, string(uploads), server.URL, "burst_x"); err != nil {
			t.Fatalf("upload: %v", err)
		}
		summary := readSummary(t, path)
		if summary.Result != OutcomeResultSucceeded || summary.Reason != "" {
			t.Fatalf("summary = %+v, want a clean success", summary)
		}
		if summary.ObjectsUploaded != 1 || summary.BytesUploaded != 2 {
			t.Fatalf("summary counts = %+v, want 1 object / 2 bytes", summary)
		}
	})

	t.Run("failure", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termination-log")
		err := runArtifactUploadCommand(context.Background(), path, string(uploads), "not-a-url", "burst_x")
		if err == nil {
			t.Fatal("bad endpoint reported success")
		}
		summary := readSummary(t, path)
		if summary.Result != OutcomeResultFailed || summary.Reason != artifactFailureConfiguration {
			t.Fatalf("summary = %+v, want a categorised failure", summary)
		}
		if summary.ObjectsUploaded != 0 || summary.BytesUploaded != 0 {
			t.Fatalf("summary counts = %+v, want zeros", summary)
		}
	})
}

// The durable half of a failed export is a category token and two counts.
// Nothing the uploader saw on the way — object keys, signed URLs, the endpoint,
// the destination, or the store's own response body — may be in it.
func TestArtifactUploadSummaryCarriesNoSensitiveMaterial(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "model.bin"), []byte("weights"), 0o644); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/storage/sign-urls" {
			var req SignURLsRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			out := SignURLsResponse{}
			for _, key := range req.Objects {
				out.URLs = append(out.URLs, SignedURL{Key: key, URL: "http://" + r.Host + "/put/" + key + "?sig=topsecret"})
			}
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		http.Error(w, "AccessDenied: key AKIAEXAMPLE is not authorized for bucket customer-bucket", http.StatusForbidden)
	}))
	defer server.Close()

	upload := ArtifactUpload{LocalPath: root, KeyPrefix: "wl_9/train-pod", MaxFiles: 5, MaxBytes: 1024}
	upload.Source = protocol.BucketRef{Bucket: "customer-bucket", Endpoint: server.URL, CredentialsSecret: "r2-creds"}
	uploads, err := json.Marshal([]ArtifactUpload{upload})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "termination-log")
	if err := runArtifactUploadCommand(context.Background(), path, string(uploads), server.URL, "burst_x"); err == nil {
		t.Fatal("denied upload reported success")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{
		"model.bin", "wl_9", "topsecret", "customer-bucket", "r2-creds",
		"AKIAEXAMPLE", "AccessDenied", server.URL, "http",
	} {
		if strings.Contains(string(raw), secret) {
			t.Errorf("termination summary leaked %q: %s", secret, raw)
		}
	}
}

func readSummary(t *testing.T, path string) artifactUploadSummary {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("no termination summary was written: %v", err)
	}
	summary, ok := parseArtifactUploadSummary(string(raw))
	if !ok {
		t.Fatalf("termination summary is not the strict shape: %s", raw)
	}
	return summary
}

// A termination message the watcher cannot fully vouch for is not a summary.
// The uploader is the agent's own image, but the message is read back out of
// pod status and turned into a record, so it is validated like any other input.
func TestParseArtifactUploadSummaryRejectsUntrustworthyMessages(t *testing.T) {
	for _, message := range []string{
		"",
		"{not json",
		`{"result":"maybe","objects_uploaded":0,"bytes_uploaded":0}`,
		`{"result":"succeeded","objects_uploaded":-1,"bytes_uploaded":0}`,
		`{"result":"succeeded","objects_uploaded":0,"bytes_uploaded":-1}`,
		`{"result":"succeeded","objects_uploaded":0,"bytes_uploaded":0,"url":"https://x"}`,
		`{"result":"succeeded","objects_uploaded":0,"bytes_uploaded":0}{"result":"failed"}`,
		`{"result":"succeeded","objects_uploaded":0,"bytes_uploaded":0}` + strings.Repeat(" ", maxArtifactSummaryBytes),
	} {
		if _, ok := parseArtifactUploadSummary(message); ok {
			t.Errorf("accepted an untrustworthy termination message: %q", message)
		}
	}

	summary, ok := parseArtifactUploadSummary(
		`{"result":"failed","reason":"upload","objects_uploaded":2,"bytes_uploaded":9}` + "\n")
	if !ok {
		t.Fatal("rejected a well-formed summary")
	}
	if summary.Result != OutcomeResultFailed || summary.Reason != artifactFailureUpload ||
		summary.ObjectsUploaded != 2 || summary.BytesUploaded != 9 {
		t.Fatalf("summary = %+v", summary)
	}
}
