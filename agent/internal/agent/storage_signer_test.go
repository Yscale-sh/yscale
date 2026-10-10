package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

// teamASource is the bucket ref central binds to burst_x in the tests
// below. teamBSecret is an identically-named Secret in another team's
// namespace — the thing a cross-namespace request is reaching for.
var teamASource = protocol.BucketRef{
	Bucket:            "models",
	Prefix:            "llama/",
	Endpoint:          "https://r2.example",
	Region:            "auto",
	CredentialsSecret: "r2-creds",
}

// signerFor wires a signer whose only authorized burst is burst_x,
// bound to namespace and bindings, and returns its test server URL.
func signerFor(t *testing.T, namespace string, bindings []protocol.StorageBinding, objs ...runtime.Object) string {
	t.Helper()
	bs := &BootstrapServer{grants: map[string]*burstGrant{
		"burst_x": {namespace: namespace, storage: bindings},
	}}
	mux := http.NewServeMux()
	NewStorageSigner(fake.NewSimpleClientset(objs...), bs, discardLogger()).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func postSign(t *testing.T, url string, req SignURLsRequest) *http.Response {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url+"/storage/sign-urls", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func credsSecret(namespace, name, id string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data: map[string][]byte{
			"AWS_ACCESS_KEY_ID":     []byte(id),
			"AWS_SECRET_ACCESS_KEY": []byte("secret-for-" + id),
		},
	}
}

func cacheBinding(src protocol.BucketRef) protocol.StorageBinding {
	return protocol.StorageBinding{Name: "models", Type: "cache", Source: &src}
}

func artifactBinding(dst protocol.BucketRef) protocol.StorageBinding {
	return protocol.StorageBinding{Name: "results", Type: "artifact", WriteTo: &dst}
}

func TestSignURLsRejectsUnknownBurst(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	bs := &BootstrapServer{grants: map[string]*burstGrant{}}
	signer := NewStorageSigner(k8s, bs, discardLogger())

	mux := http.NewServeMux()
	signer.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := postSign(t, srv.URL, SignURLsRequest{
		BurstID: "burst_unknown",
		Mode:    "read",
		Source:  protocol.BucketRef{Bucket: "b", CredentialsSecret: "creds"},
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403, got %d", resp.StatusCode)
	}
}

func TestSignURLsRequiresBucket(t *testing.T) {
	url := signerFor(t, "team-a", []protocol.StorageBinding{cacheBinding(teamASource)})

	resp := postSign(t, url, SignURLsRequest{
		BurstID: "burst_x",
		Mode:    "read",
		Source:  protocol.BucketRef{}, // missing bucket
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

// The happy path: the burst asks for exactly the source central bound
// to it, and gets signed URLs. Guards the /storage/sign-urls contract
// against the tightening around it.
func TestSignURLsSignsAuthorizedSource(t *testing.T) {
	url := signerFor(t, "team-a",
		[]protocol.StorageBinding{cacheBinding(teamASource)},
		credsSecret("team-a", "r2-creds", "team-a-key"))

	resp := postSign(t, url, SignURLsRequest{
		BurstID: "burst_x",
		Mode:    "read",
		Source:  teamASource,
		Objects: []string{"weights.bin"},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out SignURLsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.URLs) != 1 || out.URLs[0].Key != "weights.bin" {
		t.Fatalf("urls = %+v, want one entry keyed weights.bin", out.URLs)
	}
	if !strings.Contains(out.URLs[0].URL, "team-a-key") {
		t.Errorf("signed URL not built from the team-a namespace's credentials: %s", out.URLs[0].URL)
	}
}

func TestSignURLsSignsOnlyBoundArtifactSink(t *testing.T) {
	dst := teamASource
	dst.Bucket = "results"
	url := signerFor(t, "team-a", []protocol.StorageBinding{artifactBinding(dst)}, credsSecret("team-a", "r2-creds", "team-a-key"))

	resp := postSign(t, url, SignURLsRequest{BurstID: "burst_x", Mode: "write", Source: dst, Objects: []string{"wl_1/pod/output.bin"}})
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	other := dst
	other.Bucket = "other"
	resp = postSign(t, url, SignURLsRequest{BurstID: "burst_x", Mode: "write", Source: other, Objects: []string{"wl_1/pod/output.bin"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("unbound artifact sink status = %d, want 403", resp.StatusCode)
	}
}

func TestSignURLsRejectsUnsafeAndDuplicateWriteKeys(t *testing.T) {
	dst := teamASource
	dst.Bucket = "results"
	url := signerFor(t, "team-a", []protocol.StorageBinding{artifactBinding(dst)}, credsSecret("team-a", "r2-creds", "team-a-key"))

	for _, objects := range [][]string{{"../secret"}, {"/absolute"}, {"a\\b"}, {"same", "same"}, {}} {
		resp := postSign(t, url, SignURLsRequest{BurstID: "burst_x", Mode: "write", Source: dst, Objects: objects})
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("objects %q status = %d, want 400", objects, resp.StatusCode)
		}
	}
}

func TestSignURLsBoundsRequestAndObjectCount(t *testing.T) {
	dst := teamASource
	dst.Bucket = "results"
	url := signerFor(t, "team-a", []protocol.StorageBinding{artifactBinding(dst)}, credsSecret("team-a", "r2-creds", "team-a-key"))
	objects := make([]string, maxSignedObjects+1)
	for i := range objects {
		objects[i] = fmt.Sprintf("file-%d", i)
	}
	resp := postSign(t, url, SignURLsRequest{BurstID: "burst_x", Mode: "write", Source: dst, Objects: objects})
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("object count status = %d, want 413", resp.StatusCode)
	}

	body := strings.NewReader(`{"padding":"` + strings.Repeat("x", int(maxSignRequestBytes)) + `"}`)
	resp, err := http.Post(url+"/storage/sign-urls", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized body status = %d, want 400", resp.StatusCode)
	}
}

// A "namespace/name" credentials_secret is the cross-namespace escape
// H2 described. It must be refused, not stripped down to the bare name.
func TestSignURLsRejectsNamespacedSecretRef(t *testing.T) {
	src := teamASource
	src.CredentialsSecret = "team-b/r2-creds"
	url := signerFor(t, "team-a",
		[]protocol.StorageBinding{cacheBinding(teamASource)},
		credsSecret("team-b", "r2-creds", "team-b-key"))

	resp := postSign(t, url, SignURLsRequest{
		BurstID: "burst_x",
		Mode:    "read",
		Source:  src,
		Objects: []string{"weights.bin"},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for namespace-bearing secret ref, got %d", resp.StatusCode)
	}
}

// Even a bare Secret name is only ever read from the burst's own
// namespace: team-b's identically-named Secret must stay unreachable.
func TestSignURLsNeverLeavesAuthorizedNamespace(t *testing.T) {
	url := signerFor(t, "team-a",
		[]protocol.StorageBinding{cacheBinding(teamASource)},
		credsSecret("team-b", "r2-creds", "team-b-key"))

	resp := postSign(t, url, SignURLsRequest{
		BurstID: "burst_x",
		Mode:    "read",
		Source:  teamASource,
		Objects: []string{"weights.bin"},
	})
	if resp.StatusCode == http.StatusOK {
		t.Fatal("signed against team-b's Secret from a team-a burst")
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "team-a/r2-creds") {
		t.Errorf("error should name the namespace we looked in, got %q", string(body))
	}
}

// A valid BurstID is not enough: the source must be one central bound
// to that burst. Swapping the bucket while keeping the authorized
// Secret name is the interesting case.
func TestSignURLsRejectsUnboundSource(t *testing.T) {
	other := teamASource
	other.Bucket = "someone-elses-bucket"
	url := signerFor(t, "team-a",
		[]protocol.StorageBinding{cacheBinding(teamASource)},
		credsSecret("team-a", "r2-creds", "team-a-key"))

	resp := postSign(t, url, SignURLsRequest{
		BurstID: "burst_x",
		Mode:    "read",
		Source:  other,
		Objects: []string{"weights.bin"},
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for unbound source, got %d", resp.StatusCode)
	}
}

// Read and write authorize against different halves of a binding: a
// cache Source signs GETs, a snapshot target signs PUTs. Asking to
// write to a read-only cache source is not authorized.
func TestSignURLsModeSelectsAuthorizedRef(t *testing.T) {
	snapTo := protocol.BucketRef{Bucket: "backups", Endpoint: "https://r2.example", Region: "auto", CredentialsSecret: "r2-creds"}
	bindings := []protocol.StorageBinding{
		cacheBinding(teamASource),
		{Name: "state", Type: "persistent", SnapshotTo: &snapTo},
	}
	url := signerFor(t, "team-a", bindings, credsSecret("team-a", "r2-creds", "team-a-key"))

	resp := postSign(t, url, SignURLsRequest{
		BurstID: "burst_x", Mode: "write", Source: snapTo, Objects: []string{"state.tar"},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write to the bound snapshot target: expected 200, got %d", resp.StatusCode)
	}

	resp = postSign(t, url, SignURLsRequest{
		BurstID: "burst_x", Mode: "write", Source: teamASource, Objects: []string{"state.tar"},
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("write to a read-only cache source: expected 403, got %d", resp.StatusCode)
	}
}

// An announce from a central that predates BurstAnnounce.Namespace
// leaves the grant without one. Signing is refused; the burst's other
// paths are untouched.
func TestSignURLsRefusesGrantWithoutNamespace(t *testing.T) {
	url := signerFor(t, "",
		[]protocol.StorageBinding{cacheBinding(teamASource)},
		credsSecret("default", "r2-creds", "default-key"))

	resp := postSign(t, url, SignURLsRequest{
		BurstID: "burst_x",
		Mode:    "read",
		Source:  teamASource,
		Objects: []string{"weights.bin"},
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 without a server-authorized namespace, got %d", resp.StatusCode)
	}
}

func TestSignURLsRejectsUnknownMode(t *testing.T) {
	url := signerFor(t, "team-a", []protocol.StorageBinding{cacheBinding(teamASource)})

	resp := postSign(t, url, SignURLsRequest{
		BurstID: "burst_x",
		Mode:    "delete",
		Source:  teamASource,
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for unknown mode, got %d", resp.StatusCode)
	}
}

func TestFetchCredsReadsAWSStandardKeys(t *testing.T) {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "r2-creds", Namespace: "default"},
		Data: map[string][]byte{
			"AWS_ACCESS_KEY_ID":     []byte("AKIA..."),
			"AWS_SECRET_ACCESS_KEY": []byte("xyz..."),
		},
	}
	k8s := fake.NewSimpleClientset(sec)
	signer := NewStorageSigner(k8s, &BootstrapServer{grants: map[string]*burstGrant{}}, nil)
	creds, err := signer.fetchCreds(context.Background(), "default", "r2-creds")
	if err != nil {
		t.Fatalf("fetchCreds: %v", err)
	}
	if creds.AccessKeyID != "AKIA..." || creds.SecretAccessKey != "xyz..." {
		t.Errorf("creds mismatch: %+v", creds)
	}
}

func TestFetchCredsRejectsIncompleteSecret(t *testing.T) {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "default"},
		Data:       map[string][]byte{"AWS_ACCESS_KEY_ID": []byte("only-id")},
	}
	k8s := fake.NewSimpleClientset(sec)
	signer := NewStorageSigner(k8s, &BootstrapServer{grants: map[string]*burstGrant{}}, nil)
	if _, err := signer.fetchCreds(context.Background(), "default", "broken"); err == nil {
		t.Error("expected error on missing AWS_SECRET_ACCESS_KEY")
	}
}

// The cache is keyed on namespace as well as name, so two tenants'
// same-named Secrets can't alias onto one entry.
func TestFetchCredsCacheIsNamespaceScoped(t *testing.T) {
	k8s := fake.NewSimpleClientset(
		credsSecret("team-a", "r2-creds", "team-a-key"),
		credsSecret("team-b", "r2-creds", "team-b-key"),
	)
	signer := NewStorageSigner(k8s, &BootstrapServer{grants: map[string]*burstGrant{}}, nil)
	for _, ns := range []string{"team-a", "team-b"} {
		creds, err := signer.fetchCreds(context.Background(), ns, "r2-creds")
		if err != nil {
			t.Fatalf("fetchCreds %s: %v", ns, err)
		}
		if creds.AccessKeyID != ns+"-key" {
			t.Errorf("namespace %s served %q", ns, creds.AccessKeyID)
		}
	}
}
