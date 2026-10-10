package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

// StorageSigner serves the agent-side endpoint burst nodes call to
// obtain pre-signed URLs against the customer's S3-compatible bucket.
// Reads K8s Secrets from the customer's cluster (the agent's
// in-cluster ServiceAccount has access), uses the credentials in
// memory only, and returns short-lived URLs the burst can use with
// HTTP. Provider credentials never leave the agent process; signed URLs are
// short-lived bearer capabilities scoped to individual objects.
//
// Nothing in a request selects what gets signed. Both halves of the
// answer — which namespace the credentials Secret is read from, and
// which bucket refs are signable at all — come from the burst's
// server-side grant, recorded when central announced the burst. A
// request only identifies which of those authorized sources it wants.
//
// Listens on the same tailnet address as the bootstrap server. There is
// no source-IP check: the caller is authenticated by BurstID against
// the bootstrap server's grant registry, and the tailnet ACL is what
// bounds who can open the connection at all.
type StorageSigner struct {
	K8s    kubernetes.Interface
	Bursts *BootstrapServer // grant registry: authorizes the BurstID and scopes what it may sign
	Log    *slog.Logger

	// Default URL TTL. Burst rarely needs longer than the workload's
	// cold start window.
	URLTTL time.Duration

	// secretCache memoizes "namespace/name" → AWS creds for the
	// duration of a workload — saves a round-trip to the apiserver
	// on every cache fetch. Keyed on the namespace too, so a hit can
	// never serve one namespace's credentials to another.
	secretCacheMu sync.Mutex
	secretCache   map[string]cachedCreds
}

type cachedCreds struct {
	awsCreds aws.Credentials
	expires  time.Time
}

const defaultURLTTL = 1 * time.Hour
const secretCacheTTL = 10 * time.Minute
const maxSignRequestBytes int64 = 1 << 20
const maxSignedObjects = 1000
const maxObjectKeyBytes = 1024
const maxStorageListPages = 1000

// NewStorageSigner constructs a signer with sensible defaults.
func NewStorageSigner(k8s kubernetes.Interface, bursts *BootstrapServer, log *slog.Logger) *StorageSigner {
	if log == nil {
		log = slog.Default()
	}
	return &StorageSigner{
		K8s:         k8s,
		Bursts:      bursts,
		Log:         log,
		URLTTL:      defaultURLTTL,
		secretCache: make(map[string]cachedCreds),
	}
}

// Register attaches the signer's HTTP handlers to mux. Used by
// cmd/yscale-agent to add the routes alongside the bootstrap server.
func (s *StorageSigner) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /storage/sign-urls", s.handleSign)
}

// SignURLsRequest is what burst init POSTs. Source names WHICH of the
// burst's authorized storage sources to sign against — it does not
// define one. It must equal a source central bound to this burst, and
// its credentials_secret is a bare Secret name: a "namespace/name"
// reference is rejected, not normalized.
type SignURLsRequest struct {
	BurstID string             `json:"burst_id"`
	Mode    string             `json:"mode"` // "read" | "write"
	Source  protocol.BucketRef `json:"source"`
	Objects []string           `json:"objects,omitempty"` // relative to Source.Prefix; empty for read = "list-and-sign"
}

// SignURLsResponse is what we return.
type SignURLsResponse struct {
	URLs []SignedURL `json:"urls"`
}

// SignedURL pairs an object key (relative to source prefix) with a
// pre-signed URL the burst can download directly over HTTP(S).
type SignedURL struct {
	Key string `json:"key"`
	URL string `json:"url"`
}

func (s *StorageSigner) handleSign(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSignRequestBytes)
	var req SignURLsRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "decode: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !s.Bursts.IsAuthorizedSource(req.BurstID, r.RemoteAddr) {
		http.Error(w, "unauthorized burst", http.StatusForbidden)
		return
	}
	if req.Mode != "read" && req.Mode != "write" {
		http.Error(w, fmt.Sprintf("unknown mode %q", req.Mode), http.StatusBadRequest)
		return
	}
	if req.Source.Bucket == "" || req.Source.CredentialsSecret == "" {
		http.Error(w, "source.bucket and source.credentials_secret required", http.StatusBadRequest)
		return
	}
	if len(req.Objects) > maxSignedObjects {
		http.Error(w, fmt.Sprintf("objects exceeds the %d-object signing limit", maxSignedObjects), http.StatusRequestEntityTooLarge)
		return
	}
	if req.Mode == "write" && len(req.Objects) == 0 {
		http.Error(w, "write mode requires at least one object", http.StatusBadRequest)
		return
	}
	seenObjects := make(map[string]struct{}, len(req.Objects))
	for _, object := range req.Objects {
		if !validRelativeObjectKey(object) {
			http.Error(w, "objects must be canonical relative keys without traversal, control characters, or backslashes", http.StatusBadRequest)
			return
		}
		if _, exists := seenObjects[object]; exists {
			http.Error(w, "objects must not contain duplicate keys", http.StatusBadRequest)
			return
		}
		seenObjects[object] = struct{}{}
	}
	// A "namespace/name" reference is the cross-tenant escape this
	// endpoint used to allow. Refuse it outright rather than stripping
	// the namespace: a burst that sends one is asking for a Secret it
	// was never granted, and silently reinterpreting the request would
	// hide that from the operator.
	if indexByteSafe(req.Source.CredentialsSecret, '/') >= 0 {
		http.Error(w, "source.credentials_secret must be a bare Secret name, not namespace/name", http.StatusBadRequest)
		return
	}

	namespace, bindings, ok := s.Bursts.authorizedStorage(req.BurstID)
	if !ok {
		http.Error(w, "unauthorized burst", http.StatusForbidden)
		return
	}
	if namespace == "" {
		// Announce from a central that predates BurstAnnounce.Namespace.
		// Without a server-authorized namespace there is no safe one to
		// guess, so storage signing is off for this burst. The burst's
		// other paths (bootstrap, job execution) are unaffected.
		s.Log.Error("refusing to sign: burst was announced without an authorized namespace; central needs the namespace-scoped storage-signing update",
			"burst", req.BurstID)
		http.Error(w, "storage signing unavailable: burst has no server-authorized namespace", http.StatusForbidden)
		return
	}

	// Sign against the binding central recorded, not the copy the burst
	// sent. They must be equal to get here; using the authorized one
	// keeps that the only reachable outcome.
	src, ok := authorizedSource(bindings, req.Mode, req.Source)
	if !ok {
		s.Log.Warn("refusing to sign: source is not bound to this burst",
			"burst", req.BurstID, "mode", req.Mode, "bucket", req.Source.Bucket, "secret", req.Source.CredentialsSecret)
		http.Error(w, "source is not an authorized storage source for this burst", http.StatusForbidden)
		return
	}
	req.Source = src

	creds, err := s.fetchCreds(r.Context(), namespace, src.CredentialsSecret)
	if err != nil {
		s.Log.Error("fetch creds", "namespace", namespace, "secret", src.CredentialsSecret, "error", err)
		http.Error(w, "fetch creds: "+err.Error(), http.StatusInternalServerError)
		return
	}

	cli := s3ClientFor(creds, req.Source)
	presigner := s3.NewPresignClient(cli, func(o *s3.PresignOptions) {
		o.Expires = s.URLTTL
	})

	objects := req.Objects
	if req.Mode == "read" && len(objects) == 0 {
		// "List the prefix, sign every object." Bounded to keep responses
		// reasonable; very-large prefixes should be paged.
		listed, err := listObjects(r.Context(), cli, req.Source)
		if err != nil {
			http.Error(w, "list: "+err.Error(), http.StatusBadGateway)
			return
		}
		objects = listed
	}

	resp := SignURLsResponse{URLs: make([]SignedURL, 0, len(objects))}
	for _, obj := range objects {
		key := joinKey(req.Source.Prefix, obj)
		var u string
		switch req.Mode {
		case "read":
			pres, err := presigner.PresignGetObject(r.Context(), &s3.GetObjectInput{
				Bucket: aws.String(req.Source.Bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				http.Error(w, "presign get: "+err.Error(), http.StatusInternalServerError)
				return
			}
			u = pres.URL
		case "write":
			pres, err := presigner.PresignPutObject(r.Context(), &s3.PutObjectInput{
				Bucket: aws.String(req.Source.Bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				http.Error(w, "presign put: "+err.Error(), http.StatusInternalServerError)
				return
			}
			u = pres.URL
		default:
			http.Error(w, fmt.Sprintf("unknown mode %q", req.Mode), http.StatusBadRequest)
			return
		}
		resp.URLs = append(resp.URLs, SignedURL{Key: obj, URL: u})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// fetchCreds reads the named K8s Secret from namespace — the workload
// namespace central authorized for this burst. Both arguments come
// from the server side; callers must never pass a namespace or name a
// burst supplied. Cached for secretCacheTTL, keyed on both.
func (s *StorageSigner) fetchCreds(ctx context.Context, namespace, name string) (aws.Credentials, error) {
	cacheKey := namespace + "/" + name
	s.secretCacheMu.Lock()
	if c, ok := s.secretCache[cacheKey]; ok && time.Now().Before(c.expires) {
		s.secretCacheMu.Unlock()
		return c.awsCreds, nil
	}
	s.secretCacheMu.Unlock()

	sec, err := s.K8s.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("get secret %s/%s: %w", namespace, name, err)
	}
	creds, err := credsFromSecret(sec)
	if err != nil {
		return aws.Credentials{}, err
	}
	s.secretCacheMu.Lock()
	s.secretCache[cacheKey] = cachedCreds{awsCreds: creds, expires: time.Now().Add(secretCacheTTL)}
	s.secretCacheMu.Unlock()
	return creds, nil
}

// authorizedSource finds the binding-supplied BucketRef that want
// names, for the given mode: "read" signs GETs against a cache
// binding's Source, "write" signs PUTs against a persistent binding's
// SnapshotTo. Every field must match, so a request cannot keep an
// authorized credentials_secret while swapping in another bucket,
// endpoint or prefix.
func authorizedSource(bindings []protocol.StorageBinding, mode string, want protocol.BucketRef) (protocol.BucketRef, bool) {
	for _, b := range bindings {
		var ref *protocol.BucketRef
		switch mode {
		case "read":
			ref = b.Source
		case "write":
			ref = b.WriteTo
			if ref == nil {
				ref = b.SnapshotTo
			}
		}
		if ref != nil && *ref == want {
			return *ref, true
		}
	}
	return protocol.BucketRef{}, false
}

func credsFromSecret(sec *corev1.Secret) (aws.Credentials, error) {
	id := string(sec.Data["AWS_ACCESS_KEY_ID"])
	key := string(sec.Data["AWS_SECRET_ACCESS_KEY"])
	if id == "" || key == "" {
		return aws.Credentials{}, errors.New("secret missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY")
	}
	return aws.Credentials{
		AccessKeyID:     id,
		SecretAccessKey: key,
		SessionToken:    string(sec.Data["AWS_SESSION_TOKEN"]),
		Source:          "yscale-customer-secret",
	}, nil
}

// s3ClientFor builds an S3 client targeting the (R2 / S3) bucket.
// Uses path-style addressing because R2 doesn't support virtual-host.
func s3ClientFor(creds aws.Credentials, src protocol.BucketRef) *s3.Client {
	region := src.Region
	if region == "" {
		region = "auto"
	}
	cfg := aws.Config{
		Credentials: credentials.StaticCredentialsProvider{Value: creds},
		Region:      region,
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if src.Endpoint != "" {
			o.BaseEndpoint = aws.String(src.Endpoint)
		}
		o.UsePathStyle = true // R2 needs this; S3 tolerates it
	})
}

func listObjects(ctx context.Context, cli *s3.Client, src protocol.BucketRef) ([]string, error) {
	out := []string{}
	// Objects are signed through joinKey, which treats Prefix as a directory.
	// Listing must use that same boundary ("weights/", not "weights-backup").
	prefix := joinKey(src.Prefix, "")
	seenKeys := make(map[string]struct{})
	seenTokens := make(map[string]struct{})
	var token *string
	for pageNumber := 0; pageNumber < maxStorageListPages; pageNumber++ {
		page, err := cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(src.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				return nil, fmt.Errorf("listing contains an object without a key")
			}
			if !strings.HasPrefix(*obj.Key, prefix) {
				return nil, fmt.Errorf("listing contains an object outside the authorized prefix")
			}
			rel := strings.TrimPrefix(*obj.Key, prefix)
			// S3 console folders are zero-byte objects ending in '/'. They
			// are metadata, not downloadable files. Never discard data-bearing
			// trailing-slash objects or accept unsafe marker paths.
			if strings.HasSuffix(*obj.Key, "/") && obj.Size != nil && *obj.Size == 0 {
				if *obj.Key == prefix || validRelativeObjectKey(strings.TrimSuffix(rel, "/")) {
					continue
				}
			}
			if !validRelativeObjectKey(rel) {
				return nil, fmt.Errorf("object key under prefix is not a safe relative path")
			}
			if _, duplicate := seenKeys[rel]; duplicate {
				return nil, fmt.Errorf("listing contains a duplicate object key")
			}
			seenKeys[rel] = struct{}{}
			out = append(out, rel)
			if len(out) > maxSignedObjects {
				return nil, fmt.Errorf("prefix contains more than %d objects", maxSignedObjects)
			}
		}
		if page.IsTruncated == nil {
			return nil, fmt.Errorf("listing is missing its pagination status")
		}
		if !*page.IsTruncated {
			return out, nil
		}
		if page.NextContinuationToken == nil || *page.NextContinuationToken == "" {
			return nil, fmt.Errorf("truncated listing has no continuation token")
		}
		if _, repeated := seenTokens[*page.NextContinuationToken]; repeated {
			return nil, fmt.Errorf("listing repeated a continuation token")
		}
		seenTokens[*page.NextContinuationToken] = struct{}{}
		token = page.NextContinuationToken
	}
	return nil, fmt.Errorf("prefix exceeds the %d-page listing limit", maxStorageListPages)
}

func validRelativeObjectKey(key string) bool {
	if key == "" || len(key) > maxObjectKeyBytes || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") || path.Clean(key) != key {
		return false
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func joinKey(prefix, rel string) string {
	if prefix == "" {
		return rel
	}
	if endsWith(prefix, "/") {
		return prefix + rel
	}
	return prefix + "/" + rel
}

func startsWith(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }
func endsWith(s, suf string) bool { return len(s) >= len(suf) && s[len(s)-len(suf):] == suf }

func indexByteSafe(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// IsAuthorizedSource reports whether a burst-id has been authorized
// for bootstrap. It checks the BurstID and NOTHING ELSE — remoteAddr is
// ignored, kept only so the call sites read as an authorization check
// with the caller's address in hand. Tailnet membership plus the
// tag-based ACL is what bounds the source connection; tailnet IPs are
// dynamically assigned, so pinning one would fail bursts, not attackers.
func (b *BootstrapServer) IsAuthorizedSource(burstID, remoteAddr string) bool {
	b.mu.Lock()
	_, authorized := b.grants[burstID]
	b.mu.Unlock()
	_ = remoteAddr // deliberately unused; see doc comment
	return authorized
}

// Pull in net for SplitHostPort. Imported separately to keep the
// AWS-SDK-heavy import block above readable.
var _ = url.Parse // url kept live for future helpers (signed-URL parsing/validation)
