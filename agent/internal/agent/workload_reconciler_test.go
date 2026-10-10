package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	k8sv1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

func TestMarkStatusPreservesConditionsAndProjectsRequestedShape(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata": map[string]any{
			"name": "train", "namespace": "ml", "generation": int64(7),
		},
		"spec": map[string]any{
			"region": "us-ord",
			"gpu":    map[string]any{"kind": "l4", "count": int64(2)},
		},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type": "NodeReady", "status": "True", "reason": "KubeletReady",
			"lastTransitionTime": "2026-08-20T00:00:00Z",
		}}},
	}}
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), obj.DeepCopy())
	r := &WorkloadReconciler{Dyn: dyn}
	r.markStatus(context.Background(), obj, "Provisioning", "wl_1", "burst_1", "linode", nil)

	got, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get patched workload: %v", err)
	}
	conditions := workloadStatusConditions(got)
	if k8sv1.FindCondition(conditions, k8sv1.ConditionNodeReady) == nil {
		t.Fatal("existing NodeReady condition was discarded")
	}
	accepted := k8sv1.FindCondition(conditions, k8sv1.ConditionAccepted)
	if accepted == nil || accepted.Status != metav1.ConditionTrue || accepted.ObservedGeneration != 7 {
		t.Fatalf("Accepted condition = %+v", accepted)
	}
	for path, want := range map[string]string{
		"reason": k8sv1.ReasonAccepted, "region": "us-ord", "providerClass": "linode",
	} {
		value, _, _ := unstructured.NestedString(got.Object, "status", path)
		if value != want {
			t.Errorf("status.%s = %q, want %q", path, value, want)
		}
	}
	gpu, _, _ := unstructured.NestedMap(got.Object, "status", "gpu")
	if gpu["kind"] != "l4" || gpu["count"] != int64(2) {
		t.Errorf("status.gpu = %#v", gpu)
	}
}

func TestMarkStatusDoesNotCallAmbiguousSubmitARejection(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1", "kind": "Workload",
		"metadata": map[string]any{"name": "train", "namespace": "ml"},
	}}
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), obj.DeepCopy())
	r := &WorkloadReconciler{Dyn: dyn}
	r.markStatus(context.Background(), obj, "Failed", "", "", "", errors.New("connection reset"))

	got, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get patched workload: %v", err)
	}
	if accepted := k8sv1.FindCondition(workloadStatusConditions(got), k8sv1.ConditionAccepted); accepted != nil {
		t.Fatalf("ambiguous submit wrote Accepted=%s", accepted.Status)
	}
	reason, _, _ := unstructured.NestedString(got.Object, "status", "reason")
	if reason != k8sv1.ReasonSubmitOutcomeUnknown {
		t.Fatalf("status.reason = %q", reason)
	}
}

// TestSubmitRejectsExplicitLiteTier pins the ordering of the two submit-time
// preflights: the removed lite tier is unsupported no matter how the gateway
// is wired, so it must be rejected as an unsupported tier BEFORE the gateway
// diagnostic — otherwise a customer on a lite spec would be sent to fix the
// gateway (which was never the problem) or, worse, would see the spec go
// through when the gateway happens to be enabled.
func TestSubmitRejectsExplicitLiteTier(t *testing.T) {
	t.Parallel()
	central := &recordingCentral{}
	cr := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1", "kind": "Workload",
		"metadata": map[string]any{"name": "legacy", "namespace": "ml"},
		"spec": map[string]any{
			"image":      "x",
			"networking": map[string]any{"tier": "lite"},
		},
	}}
	for _, gateway := range []bool{false, true} {
		r := submittingReconciler(central)
		r.GatewayEnabled = gateway
		err := r.submit(context.Background(), cr)
		if err == nil {
			t.Fatalf("gateway=%v: expected rejection, got nil", gateway)
		}
		var rejection *confirmedSubmitRejection
		if !errors.As(err, &rejection) {
			t.Fatalf("gateway=%v: err type = %T, want confirmed rejection", gateway, err)
		}
		if !strings.Contains(err.Error(), `tier="lite"`) || !strings.Contains(err.Error(), "not supported") {
			t.Fatalf("gateway=%v: err = %q, want tier-shaped rejection naming lite", gateway, err.Error())
		}
		if strings.Contains(err.Error(), "gateway") {
			t.Fatalf("gateway=%v: err = %q, lite must be surfaced as a tier problem, not a gateway one", gateway, err.Error())
		}
	}
	if central.submits != 0 {
		t.Fatalf("lite tier must never reach central; got %d submits", central.submits)
	}
}

// TestUnsupportedNetworkingTier keeps the preflight predicate honest — empty
// and "full" pass, everything else (including the removed lite tier and any
// unknown value) is flagged with its literal string so the caller can name it
// in the error message.
func TestUnsupportedNetworkingTier(t *testing.T) {
	t.Parallel()
	mk := func(tier string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"spec": map[string]any{"networking": map[string]any{"tier": tier}},
		}}
	}
	cases := []struct {
		name       string
		obj        *unstructured.Unstructured
		wantTier   string
		wantReject bool
	}{
		{name: "no spec", obj: &unstructured.Unstructured{Object: map[string]any{}}},
		{name: "no networking", obj: &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"image": "x"}}}},
		{name: "empty networking", obj: &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"networking": map[string]any{}}}}},
		{name: "empty tier", obj: mk("")},
		{name: "explicit full", obj: mk("full")},
		{name: "explicit lite", obj: mk("lite"), wantTier: "lite", wantReject: true},
		{name: "unknown", obj: mk("bogus"), wantTier: "bogus", wantReject: true},
		{name: "capitalized full", obj: mk("Full"), wantTier: "Full", wantReject: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tier, reject := unsupportedNetworkingTier(tt.obj)
			if reject != tt.wantReject || tier != tt.wantTier {
				t.Fatalf("got (%q,%v), want (%q,%v)", tier, reject, tt.wantTier, tt.wantReject)
			}
		})
	}
}

func TestWorkloadRequiresGateway(t *testing.T) {
	t.Parallel()

	mk := func(spec map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "yscale.sh/v1",
				"kind":       "Workload",
				"spec":       spec,
			},
		}
	}

	tests := []struct {
		name string
		obj  *unstructured.Unstructured
		want bool
	}{
		{
			name: "tier=full requires gateway",
			obj:  mk(map[string]any{"image": "x", "networking": map[string]any{"tier": "full"}}),
			want: true,
		},
		{
			// The removed lite tier used to skip the gateway; central now
			// refuses it, and the local preflight also treats it as any
			// other spec — the gateway is mandatory for every supported
			// tier.
			name: "legacy tier=lite still requires gateway",
			obj:  mk(map[string]any{"image": "x", "networking": map[string]any{"tier": "lite"}}),
			want: true,
		},
		{
			name: "no networking block - default full requires gateway",
			obj:  mk(map[string]any{"image": "x"}),
			want: true,
		},
		{
			name: "empty networking block - no tier - default full requires gateway",
			obj:  mk(map[string]any{"image": "x", "networking": map[string]any{}}),
			want: true,
		},
		{
			// Case-sensitive on purpose: Validate rejects this, and the
			// raw CR preflight still fails closed to the full-tier path.
			name: "tier=Full (capitalized) requires gateway",
			obj:  mk(map[string]any{"image": "x", "networking": map[string]any{"tier": "Full"}}),
			want: true,
		},
		{
			name: "unknown tier requires gateway",
			obj:  mk(map[string]any{"image": "x", "networking": map[string]any{"tier": "bogus"}}),
			want: true,
		},
		{
			name: "missing spec entirely",
			obj:  &unstructured.Unstructured{Object: map[string]any{"apiVersion": "yscale.sh/v1"}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workloadRequiresGateway(tt.obj); got != tt.want {
				t.Fatalf("got %v want %v", got, tt.want)
			}
		})
	}
}

func TestSpecToYAML(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "yscale.sh/v1",
			"kind":       "Workload",
			"metadata":   map[string]any{"name": "train", "namespace": "ml"},
			"spec": map[string]any{
				"image": "x:1",
				"size":  "small",
				"gpu":   map[string]any{"kind": "a100", "count": int64(1)},
			},
		},
	}
	out, err := specToYAML(obj)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, want := range []string{
		"apiVersion: yscale.sh/v1",
		"kind: Workload",
		"name: train",
		"namespace: ml",
		"image: x:1",
		"kind: a100",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("yaml missing %q:\n%s", want, s)
		}
	}
}

func TestCentralHTTPClientSubmitWorkload(t *testing.T) {
	const key = "workload-0123456789abcdef0123456789abcdef"

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/workloads", func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("Authorization"), "Bearer t"; got != want {
			t.Errorf("auth header = %q, want %q", got, want)
		}
		if got, want := r.Header.Get("Content-Type"), "application/yaml"; got != want {
			t.Errorf("content-type = %q, want %q", got, want)
		}
		// Verbatim, not merely present: central binds the claim to the exact
		// bytes, so a rewritten key is a second paid node on every retry.
		if got := r.Header.Get("Idempotency-Key"); got != key {
			t.Errorf("Idempotency-Key = %q, want %q", got, key)
		}
		if got, want := r.Header.Get("X-Workload-Origin"), "workload-cr"; got != want {
			t.Errorf("X-Workload-Origin = %q, want %q", got, want)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "kind: Workload") {
			t.Errorf("body missing kind: %s", body)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":            "wl_abc",
			"status":        "provisioning",
			"backend":       "linode",
			"burst_id":      "burst_xyz",
			"estimated_usd": 3.58,
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	wlID, burstID, backend, estUSD, err := cli.SubmitWorkload(context.Background(), "ml",
		[]byte("apiVersion: yscale.sh/v1\nkind: Workload\nspec: { image: x }"), key,
		protocol.OriginWorkloadCR)
	if err != nil {
		t.Fatalf("SubmitWorkload: %v", err)
	}
	if wlID != "wl_abc" || burstID != "burst_xyz" || backend != "linode" || estUSD != 3.58 {
		t.Errorf("unexpected response: id=%q burst=%q backend=%q usd=%v", wlID, burstID, backend, estUSD)
	}
}

// TestCentralHTTPClientSubmitOmitsEmptyIdempotencyKey pins the unkeyed path:
// central still accepts a cluster-token submission with no header, and sending
// an empty one would be a different thing than sending none.
func TestCentralHTTPClientSubmitOmitsEmptyIdempotencyKey(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Values("Idempotency-Key")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"wl_abc"}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	if _, _, _, _, err := cli.SubmitWorkload(context.Background(), "ml", []byte("x"), "", protocol.OriginWorkloadCR); err != nil {
		t.Fatalf("SubmitWorkload: %v", err)
	}
	if len(seen) != 0 {
		t.Errorf("Idempotency-Key sent for an empty key: %q", seen)
	}
}

// TestSubmissionKeysSatisfyCentralContract holds the derived keys to central's
// admission rule (handlers/idempotency.go: 8..255 bytes, printable ASCII with
// no spaces) and to the rule that a key never carries the payload or a
// credential — it rides in a header, through every proxy log on the way.
func TestSubmissionKeysSatisfyCentralContract(t *testing.T) {
	t.Parallel()

	const (
		minKeyBytes = 8
		maxKeyBytes = 255
	)
	keys := map[string]string{
		"pod":              podSubmissionKey(newPendingPod("inference-1", "ml", "gpu: {kind: a100}")),
		"pod empty uid":    podSubmissionKey(&corev1.Pod{}),
		"cr":               crSubmissionKey(crWithUID("ml", "train", "0e0f6e8a-1c2d-4e3f-8a9b-0c1d2e3f4a5b", 3)),
		"cr no uid":        crSubmissionKey(crWithStatus("ml", "train", nil)),
		"cr hostile parts": crSubmissionKey(crWithUID("ml", "train", "uid with spaces\nand\ta newline", 1)),
	}
	for name, key := range keys {
		if len(key) < minKeyBytes || len(key) > maxKeyBytes {
			t.Errorf("%s: key length %d outside 8..255: %q", name, len(key), key)
		}
		for i := 0; i < len(key); i++ {
			if key[i] < '!' || key[i] > '~' {
				t.Errorf("%s: key has non-printable or space byte at %d: %q", name, i, key)
				break
			}
		}
	}
}

// The agent names its OWN cluster on every submission, so central routes the
// burst back to the cluster the pod is waiting in rather than to the tenant's
// other one.
func TestCentralHTTPClientSubmitSendsClusterID(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Values("X-Cluster-ID")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"wl_abc"}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster-abc123")
	if _, _, _, _, err := cli.SubmitWorkload(context.Background(), "ml", []byte("x"), "key-with-cluster", protocol.OriginPendingPod); err != nil {
		t.Fatalf("SubmitWorkload: %v", err)
	}
	if len(seen) != 1 || seen[0] != "cluster-abc123" {
		t.Errorf("X-Cluster-ID = %q, want the agent's configured cluster verbatim", seen)
	}
}

// An agent with no configured cluster id sends no header, and central's
// single-connector fallback answers it exactly as it always has. Sending an
// empty value would be a different thing than sending none.
func TestCentralHTTPClientSubmitOmitsEmptyClusterID(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Values("X-Cluster-ID")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"wl_abc"}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "")
	if _, _, _, _, err := cli.SubmitWorkload(context.Background(), "ml", []byte("x"), "key-no-cluster", protocol.OriginPendingPod); err != nil {
		t.Fatalf("SubmitWorkload: %v", err)
	}
	if len(seen) != 0 {
		t.Errorf("X-Cluster-ID sent for an unset cluster: %q", seen)
	}
}

// The origin travels EXACTLY as the calling controller named it. Central holds
// the header to a closed set, so a value this client abbreviated, title-cased or
// invented is a 400 on a submission that was otherwise fine.
func TestCentralHTTPClientSubmitSendsOriginVerbatim(t *testing.T) {
	for _, tc := range []struct {
		name   string
		origin protocol.SubmissionOrigin
		want   []string
	}{
		{"pending pod", protocol.OriginPendingPod, []string{"pending-pod"}},
		{"workload cr", protocol.OriginWorkloadCR, []string{"workload-cr"}},
		// Not reported, for the same reason it is not accepted: "api" is what
		// central concludes when nobody claimed a connector path.
		{"api", protocol.OriginAPI, nil},
		// A connector built before the field, and any caller that names an origin
		// this client does not know: no header, and central reads the submission
		// back as the API path rather than refusing it over a label.
		{"unset", "", nil},
		{"unknown", protocol.SubmissionOrigin("keda"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen = r.Header.Values("X-Workload-Origin")
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(`{"id":"wl_abc"}`))
			}))
			defer srv.Close()

			cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
			if _, _, _, _, err := cli.SubmitWorkload(context.Background(), "ml",
				[]byte("x"), "key-for-origin", tc.origin); err != nil {
				t.Fatalf("SubmitWorkload: %v", err)
			}
			if len(seen) != len(tc.want) {
				t.Fatalf("X-Workload-Origin = %q, want %q", seen, tc.want)
			}
			for i := range tc.want {
				if seen[i] != tc.want[i] {
					t.Errorf("X-Workload-Origin[%d] = %q, want %q", i, seen[i], tc.want[i])
				}
			}
		})
	}
}

func TestCentralHTTPClientErrorBubblesUp(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "bad", "cluster_a")
	_, _, _, _, err := cli.SubmitWorkload(context.Background(), "ml", []byte("x"), "key-for-401", protocol.OriginWorkloadCR)
	if err == nil {
		t.Error("expected error on 401")
	}
	var rejection *confirmedSubmitRejection
	if !errors.As(err, &rejection) {
		t.Fatalf("401 error type = %T, want confirmed rejection", err)
	}
}

func TestCentralHTTPClientServerErrorIsNotConfirmedRejection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	_, _, _, _, err := cli.SubmitWorkload(context.Background(), "ml", []byte("x"), "key-for-503", protocol.OriginWorkloadCR)
	var rejection *confirmedSubmitRejection
	if err == nil || errors.As(err, &rejection) {
		t.Fatalf("503 error = %v, confirmed rejection = %v", err, rejection != nil)
	}
}

// --- Central status sync (GET /v1/workloads/{id} -> CR status) ---

// stubStatusReader stands in for central on the status-sync path. It is
// deliberately NOT a CentralAPI: the loop must reach it through the narrow
// WorkloadStatusReader view, not by growing CentralAPI's fakes.
type stubStatusReader struct {
	snapshots map[string]CentralWorkloadSnapshot
	errs      map[string]error
	fetched   []string
}

func (s *stubStatusReader) GetWorkloadStatus(_ context.Context, workloadID string) (CentralWorkloadSnapshot, error) {
	s.fetched = append(s.fetched, workloadID)
	if err, ok := s.errs[workloadID]; ok {
		return CentralWorkloadSnapshot{}, err
	}
	return s.snapshots[workloadID], nil
}

func statusSyncReconciler(t *testing.T, objs ...*unstructured.Unstructured) (*WorkloadReconciler, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	list := make([]runtime.Object, len(objs))
	for i, o := range objs {
		list[i] = o
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{workloadGVR: "WorkloadList"},
		list...)
	r := &WorkloadReconciler{
		Dyn: dyn,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return r, dyn
}

func patchActions(dyn *dynamicfake.FakeDynamicClient) int {
	n := 0
	for _, a := range dyn.Actions() {
		if a.Matches("patch", "workloads") {
			n++
		}
	}
	return n
}

func TestCentralHTTPClientGetWorkloadStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/workloads/wl_abc", func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("Authorization"), "Bearer t"; got != want {
			t.Errorf("auth header = %q, want %q", got, want)
		}
		_, _ = w.Write([]byte(`{
			"id": "wl_abc",
			"status": "running",
			"node_observation": {"node_name": "ip-192-0-2-4", "phase": "Ready", "observed_at": "2026-08-20T00:00:00Z"},
			"placement": {"gpu_product": "NVIDIA RTX 4000 Ada", "receipt": {"selected": {"provider": "linode", "sku": "g2-gpu-rtx4000a1-s"}}},
			"spent_usd": 1.2345
		}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_abc")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.NodeName != "ip-192-0-2-4" {
		t.Errorf("NodeName = %q, want %q", snap.NodeName, "ip-192-0-2-4")
	}
	if snap.SpentUSD == nil || *snap.SpentUSD != 1.2345 {
		t.Errorf("SpentUSD = %v, want 1.2345", snap.SpentUSD)
	}
	if snap.GPUProduct != "NVIDIA RTX 4000 Ada" {
		t.Errorf("GPUProduct = %q, want NVIDIA RTX 4000 Ada", snap.GPUProduct)
	}
}

// The id is a path segment, not a query value: one carrying separators must
// arrive percent-escaped so a hostile id can't address a sibling route.
func TestCentralHTTPClientGetWorkloadStatusEscapesID(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		_, _ = w.Write([]byte(`{"id":"x"}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	if _, err := cli.GetWorkloadStatus(context.Background(), "wl/a b"); err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if want := "/v1/workloads/wl%2Fa%20b"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
}

// A workload whose burst is reaped answers without node_observation and
// spent_usd; both must decode as absent (nil), not zero.
func TestCentralHTTPClientGetWorkloadStatusOmittedFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"wl_abc","status":"succeeded"}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_abc")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.NodeName != "" {
		t.Errorf("NodeName = %q, want empty", snap.NodeName)
	}
	if snap.SpentUSD != nil {
		t.Errorf("SpentUSD = %v, want nil (omitted, not zero)", *snap.SpentUSD)
	}
}

func TestCentralHTTPClientGetWorkloadStatusDecodesGPUReadyAt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"id": "wl_gpu",
			"status": "running",
			"gpu_observation": {"allocatable_at": "2026-08-21T10:30:00Z"},
			"pod_observation": {"pod_name": "train-pod", "node_name": "burst-node", "scheduled_at": "2026-08-21T10:00:00Z"},
			"cleanup": {"state": "queued", "provider_created_at": "2026-08-21T09:45:00Z"}
		}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_gpu")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.GPUReadyAt == nil {
		t.Fatal("GPUReadyAt = nil, want the allocatable_at value decoded")
	}
	want := time.Date(2026, 8, 21, 10, 30, 0, 0, time.UTC)
	if !snap.GPUReadyAt.Equal(want) {
		t.Fatalf("GPUReadyAt = %v, want %v", snap.GPUReadyAt, want)
	}
	if snap.PodName != "train-pod" {
		t.Errorf("PodName = %q, want %q", snap.PodName, "train-pod")
	}
	providerCreatedAt := time.Date(2026, 8, 21, 9, 45, 0, 0, time.UTC)
	if snap.ProviderCreatedAt == nil || !snap.ProviderCreatedAt.Equal(providerCreatedAt) {
		t.Errorf("ProviderCreatedAt = %v, want %v", snap.ProviderCreatedAt, providerCreatedAt)
	}
}

func TestSyncCentralStatusProjectsProviderCreatedAtWithoutChurnOrErasure(t *testing.T) {
	providerCreatedAt := time.Date(2026, 8, 21, 9, 45, 0, 0, time.UTC)
	cr := crWithStatus("ml", "train", map[string]any{
		"phase": "Succeeded", "workloadID": "wl_abc",
	})
	r, dyn := statusSyncReconciler(t, cr)
	reader := &stubStatusReader{snapshots: map[string]CentralWorkloadSnapshot{
		"wl_abc": {ProviderCreatedAt: &providerCreatedAt},
	}}

	r.syncCentralStatus(context.Background(), reader)
	got, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get workload: %v", err)
	}
	if stamp, _, _ := unstructured.NestedString(got.Object, "status", "providerCreatedAt"); stamp != providerCreatedAt.Format(time.RFC3339) {
		t.Errorf("status.providerCreatedAt = %q, want %q", stamp, providerCreatedAt.Format(time.RFC3339))
	}
	if n := patchActions(dyn); n != 1 {
		t.Fatalf("patches after first sync = %d, want 1", n)
	}

	r.syncCentralStatus(context.Background(), reader)
	if n := patchActions(dyn); n != 1 {
		t.Fatalf("patches after unchanged resync = %d, want 1", n)
	}

	reader.snapshots["wl_abc"] = CentralWorkloadSnapshot{}
	r.syncCentralStatus(context.Background(), reader)
	if n := patchActions(dyn); n != 1 {
		t.Fatalf("patches after omitted timestamp = %d, want 1", n)
	}
}

func TestSyncCentralStatusPatchesNodeNameAndCost(t *testing.T) {
	spent := 1.2345
	cr := crWithStatus("ml", "train", map[string]any{
		"phase": "Provisioning", "workloadID": "wl_abc", "backend": "linode",
		"gpu": map[string]any{"kind": "rtx4000ada", "count": int64(1)},
	})
	r, dyn := statusSyncReconciler(t, cr)
	reader := &stubStatusReader{snapshots: map[string]CentralWorkloadSnapshot{
		"wl_abc": {NodeName: "ip-192-0-2-4", SpentUSD: &spent, GPUProduct: "NVIDIA RTX 4000 Ada"},
	}}

	r.syncCentralStatus(context.Background(), reader)

	got, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get workload: %v", err)
	}
	if nn, _, _ := unstructured.NestedString(got.Object, "status", "nodeName"); nn != "ip-192-0-2-4" {
		t.Errorf("status.nodeName = %q, want %q", nn, "ip-192-0-2-4")
	}
	// Cost lands as a deterministic sub-cent string, not raw float text.
	if c, _, _ := unstructured.NestedString(got.Object, "status", "costSoFarUSD"); c != "1.234500" {
		t.Errorf("status.costSoFarUSD = %q, want %q", c, "1.234500")
	}
	if product, _, _ := unstructured.NestedString(got.Object, "status", "gpu", "product"); product != "NVIDIA RTX 4000 Ada" {
		t.Errorf("status.gpu.product = %q, want NVIDIA RTX 4000 Ada", product)
	}
	if kind, _, _ := unstructured.NestedString(got.Object, "status", "gpu", "kind"); kind != "rtx4000ada" {
		t.Errorf("status.gpu.kind = %q, want preserved rtx4000ada", kind)
	}
	if count, _, _ := unstructured.NestedInt64(got.Object, "status", "gpu", "count"); count != 1 {
		t.Errorf("status.gpu.count = %d, want preserved 1", count)
	}
	// The projection must not disturb the fields other writers own.
	for path, want := range map[string]string{
		"phase": "Provisioning", "workloadID": "wl_abc", "backend": "linode",
	} {
		if v, _, _ := unstructured.NestedString(got.Object, "status", path); v != want {
			t.Errorf("status.%s = %q, want %q (must be preserved)", path, v, want)
		}
	}

	// Second sync with the same central values: no new patch (dedup —
	// every patch is a watch event, and cost text must byte-match).
	if before := patchActions(dyn); before != 1 {
		t.Fatalf("patches after first sync = %d, want 1", before)
	}
	r.syncCentralStatus(context.Background(), reader)
	if after := patchActions(dyn); after != 1 {
		t.Fatalf("patches after unchanged resync = %d, want 1 (values matched, patch skipped)", after)
	}
}

// Central omitting a field is not central reporting empty: a reaped burst
// answers without spent_usd, and the last known cost and node must survive.
func TestSyncCentralStatusDoesNotEraseOmittedValues(t *testing.T) {
	cr := crWithStatus("ml", "train", map[string]any{
		"phase": "Succeeded", "workloadID": "wl_abc",
		"nodeName": "ip-192-0-2-4", "costSoFarUSD": "9.99",
	})
	r, dyn := statusSyncReconciler(t, cr)

	r.syncCentralStatus(context.Background(), &stubStatusReader{
		snapshots: map[string]CentralWorkloadSnapshot{"wl_abc": {}},
	})

	if n := patchActions(dyn); n != 0 {
		t.Fatalf("patches = %d, want 0 when central omits both fields", n)
	}
	got, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get workload: %v", err)
	}
	if nn, _, _ := unstructured.NestedString(got.Object, "status", "nodeName"); nn != "ip-192-0-2-4" {
		t.Errorf("nodeName erased to %q", nn)
	}
	if c, _, _ := unstructured.NestedString(got.Object, "status", "costSoFarUSD"); c != "9.99" {
		t.Errorf("costSoFarUSD erased to %q", c)
	}
}

// One CR's fetch failure must not strand the rest of the batch.
func TestSyncCentralStatusContinuesAfterPerItemErrors(t *testing.T) {
	spent := 0.5
	good := crWithStatus("ml", "good", map[string]any{"phase": "Provisioning", "workloadID": "wl_good"})
	bad := crWithStatus("ml", "bad", map[string]any{"phase": "Provisioning", "workloadID": "wl_bad"})
	unstamped := crWithStatus("ml", "new", map[string]any{"phase": "Pending"})
	r, dyn := statusSyncReconciler(t, good, bad, unstamped)

	r.syncCentralStatus(context.Background(), &stubStatusReader{
		snapshots: map[string]CentralWorkloadSnapshot{
			"wl_good": {NodeName: "node-7", SpentUSD: &spent},
		},
		errs: map[string]error{"wl_bad": errors.New("central 503")},
	})

	got, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "good", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get good: %v", err)
	}
	if nn, _, _ := unstructured.NestedString(got.Object, "status", "nodeName"); nn != "node-7" {
		t.Errorf("good CR not synced after sibling error: nodeName = %q", nn)
	}
	if c, _, _ := unstructured.NestedString(got.Object, "status", "costSoFarUSD"); c != "0.500000" {
		t.Errorf("good CR costSoFarUSD = %q, want %q", c, "0.500000")
	}

	// The unstamped CR is skipped without a central fetch, and the failing
	// one is attempted (then logged and abandoned) exactly once.
}

// TestCentralHTTPClientGetWorkloadStatusDecodesGPUTelemetry pins the
// contract with central's GET /v1/workloads/{id}: gpu_util_percent +
// last_heartbeat_at land on the snapshot as an atomic pair, including an
// observed idle 0%. A reader that dropped the 0 would blank a live idle GPU
// on the CR.
func TestCentralHTTPClientGetWorkloadStatusDecodesGPUTelemetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"id": "wl_gpu",
			"status": "running",
			"gpu_util_percent": 0,
			"last_heartbeat_at": "2026-08-25T14:30:00Z"
		}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_gpu")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.GPUUtilPercent == nil || *snap.GPUUtilPercent != 0 {
		t.Errorf("GPUUtilPercent = %v, want observed 0", snap.GPUUtilPercent)
	}
	want := time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC)
	if snap.LastHeartbeatAt == nil || !snap.LastHeartbeatAt.Equal(want) {
		t.Errorf("LastHeartbeatAt = %v, want %v", snap.LastHeartbeatAt, want)
	}
}

// TestCentralHTTPClientGetWorkloadStatusDecodesNonzeroGPUTelemetry — nonzero
// utilisation must survive the round trip verbatim and get carried onto the
// snapshot alongside its heartbeat.
func TestCentralHTTPClientGetWorkloadStatusDecodesNonzeroGPUTelemetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"id": "wl_gpu",
			"status": "running",
			"gpu_util_percent": 87.5,
			"last_heartbeat_at": "2026-08-25T14:31:00Z"
		}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_gpu")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.GPUUtilPercent == nil || *snap.GPUUtilPercent != 87.5 {
		t.Errorf("GPUUtilPercent = %v, want 87.5", snap.GPUUtilPercent)
	}
	if snap.LastHeartbeatAt == nil {
		t.Fatal("LastHeartbeatAt = nil, want the observation timestamp")
	}
}

// TestCentralHTTPClientGetWorkloadStatusFailsClosedOnPartialOrInvalidGPU —
// central's contract is that the two fields ship as an atomic pair. Anything
// central sent that violates the pair contract — one field missing, a NaN
// utilisation, out-of-range, negative, or a zero timestamp — must fail closed
// so the projection loop cannot mint a nonsense value onto the CR.
func TestCentralHTTPClientGetWorkloadStatusFailsClosedOnPartialOrInvalidGPU(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"util only (no heartbeat)", `{"id":"wl","gpu_util_percent":50}`},
		{"heartbeat only (no util)", `{"id":"wl","last_heartbeat_at":"2026-08-25T14:30:00Z"}`},
		{"zero heartbeat", `{"id":"wl","gpu_util_percent":50,"last_heartbeat_at":"0001-01-01T00:00:00Z"}`},
		{"negative util", `{"id":"wl","gpu_util_percent":-1,"last_heartbeat_at":"2026-08-25T14:30:00Z"}`},
		{"above range", `{"id":"wl","gpu_util_percent":101,"last_heartbeat_at":"2026-08-25T14:30:00Z"}`},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
			snap, err := cli.GetWorkloadStatus(context.Background(), "wl")
			if err != nil {
				t.Fatalf("GetWorkloadStatus: %v", err)
			}
			if snap.GPUUtilPercent != nil || snap.LastHeartbeatAt != nil {
				t.Fatalf("partial/invalid pair leaked onto snapshot: util=%v heartbeat=%v",
					snap.GPUUtilPercent, snap.LastHeartbeatAt)
			}
		})
	}
}

// TestSyncCentralStatusPatchesGPUTelemetry proves the projection loop stamps
// the pair atomically when central emitted a valid observation, and refuses
// to patch a second time when nothing changed (every re-patch is a watch
// event on the customer's apiserver).
func TestSyncCentralStatusPatchesGPUTelemetry(t *testing.T) {
	util := 42.5
	heartbeat := time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC)
	cr := crWithStatus("ml", "train", map[string]any{
		"phase": "Running", "workloadID": "wl_abc",
	})
	r, dyn := statusSyncReconciler(t, cr)
	reader := &stubStatusReader{snapshots: map[string]CentralWorkloadSnapshot{
		"wl_abc": {GPUUtilPercent: &util, LastHeartbeatAt: &heartbeat},
	}}

	r.syncCentralStatus(context.Background(), reader)

	got, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get workload: %v", err)
	}
	if u, _, _ := unstructured.NestedFloat64(got.Object, "status", "gpuUtilPercent"); u != 42.5 {
		t.Errorf("status.gpuUtilPercent = %v, want 42.5", u)
	}
	if h, _, _ := unstructured.NestedString(got.Object, "status", "lastHeartbeatAt"); h != "2026-08-25T14:30:00Z" {
		t.Errorf("status.lastHeartbeatAt = %q, want 2026-08-25T14:30:00Z", h)
	}
	if n := patchActions(dyn); n != 1 {
		t.Fatalf("patches after first sync = %d, want 1", n)
	}

	// Same telemetry a second time must not re-patch: no watch churn on
	// unchanged values, same as the nodeName + spend guards.
	r.syncCentralStatus(context.Background(), reader)
	if n := patchActions(dyn); n != 1 {
		t.Fatalf("patches after unchanged resync = %d, want 1 (dedup)", n)
	}
}

// TestSyncCentralStatusProjectsObservedZeroGPU pins the observed idle case:
// central said 0%, the CR must record 0 rather than treat it as absence.
func TestSyncCentralStatusProjectsObservedZeroGPU(t *testing.T) {
	util := 0.0
	heartbeat := time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC)
	cr := crWithStatus("ml", "train", map[string]any{
		"phase": "Running", "workloadID": "wl_abc",
	})
	r, dyn := statusSyncReconciler(t, cr)
	reader := &stubStatusReader{snapshots: map[string]CentralWorkloadSnapshot{
		"wl_abc": {GPUUtilPercent: &util, LastHeartbeatAt: &heartbeat},
	}}

	r.syncCentralStatus(context.Background(), reader)
	got, _ := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
	u, found := nestedGPUUtilPercent(got)
	if !found || u != 0 {
		t.Fatalf("observed 0%% must land on the CR as 0 (found=%v, u=%v)", found, u)
	}

	// Second sync of the same 0% must not re-patch — the dedup helper must
	// treat a stored int64(0) as float64(0) or every resync would rewrite
	// the same value and burn a watch event.
	before := patchActions(dyn)
	r.syncCentralStatus(context.Background(), reader)
	if got := patchActions(dyn); got != before {
		t.Fatalf("observed-0%% resync re-patched %d times, want 0 (dedup must accept int64(0))", got-before)
	}
}

// TestSyncCentralStatusPreservesLastGoodGPUOnPartialOrOmitted — a central
// answer that omits either field (a reaped burst, a stale sample discarded)
// must never blank the last good stamped values. The pair is atomic on the
// way in AND on the way out.
func TestSyncCentralStatusPreservesLastGoodGPUOnPartialOrOmitted(t *testing.T) {
	util := 42.5
	heartbeat := time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC)
	cases := []struct {
		name string
		snap CentralWorkloadSnapshot
	}{
		{"both absent", CentralWorkloadSnapshot{}},
		{"util only", CentralWorkloadSnapshot{GPUUtilPercent: &util}},
		{"heartbeat only", CentralWorkloadSnapshot{LastHeartbeatAt: &heartbeat}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cr := crWithStatus("ml", "train", map[string]any{
				"phase": "Succeeded", "workloadID": "wl_abc",
				"gpuUtilPercent":  float64(75),
				"lastHeartbeatAt": "2026-08-25T13:00:00Z",
			})
			r, dyn := statusSyncReconciler(t, cr)
			reader := &stubStatusReader{snapshots: map[string]CentralWorkloadSnapshot{"wl_abc": tc.snap}}

			r.syncCentralStatus(context.Background(), reader)

			got, _ := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
			if u, _, _ := unstructured.NestedFloat64(got.Object, "status", "gpuUtilPercent"); u != 75 {
				t.Errorf("gpuUtilPercent erased to %v (case %q)", u, tc.name)
			}
			if h, _, _ := unstructured.NestedString(got.Object, "status", "lastHeartbeatAt"); h != "2026-08-25T13:00:00Z" {
				t.Errorf("lastHeartbeatAt erased to %q (case %q)", h, tc.name)
			}
		})
	}
}

// TestValidGPUTelemetryEdgeCases pins the fail-closed guard the HTTP decoder
// and the projection loop share. The list of shapes central could hand us
// that must NOT reach the CR: nil sides, NaN/±Inf, out-of-range, and the
// zero-value timestamp Go emits for a missing time.
func TestValidGPUTelemetryEdgeCases(t *testing.T) {
	ok := time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC)
	zero := time.Time{}
	nan := math.NaN()
	inf := math.Inf(1)
	neg := -0.01
	over := 100.01
	good := 55.0

	cases := []struct {
		name   string
		util   *float64
		hb     *time.Time
		wantOK bool
	}{
		{"both nil", nil, nil, false},
		{"util nil", nil, &ok, false},
		{"heartbeat nil", &good, nil, false},
		{"zero heartbeat", &good, &zero, false},
		{"NaN util", &nan, &ok, false},
		{"inf util", &inf, &ok, false},
		{"negative util", &neg, &ok, false},
		{"above range", &over, &ok, false},
		{"observed 0", func() *float64 { z := 0.0; return &z }(), &ok, true},
		{"observed 100", func() *float64 { h := 100.0; return &h }(), &ok, true},
		{"observed mid", &good, &ok, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			u, h, ok := validGPUTelemetry(tc.util, tc.hb)
			if ok != tc.wantOK {
				t.Fatalf("validGPUTelemetry ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK && (u != nil || h != nil) {
				t.Fatal("invalid pair must return nil sides")
			}
		})
	}
}

// --- Receipt projection: distinct compute + artifact legs ---

// TestCentralHTTPClientGetWorkloadStatusDecodesReceiptLegs pins the wire
// contract with central's GET /v1/workloads/{id}: the outcome payload carries
// compute.result/reason AND (when export ran) an artifacts leg with
// result/reason/objects_uploaded/bytes_uploaded. All six values must land on
// the snapshot verbatim, with the counts as pointers so a counted-zero is
// distinct from a null central sent.
func TestCentralHTTPClientGetWorkloadStatusDecodesReceiptLegs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"id": "wl_r",
			"status": "succeeded",
			"outcome": {
				"compute": {"result": "succeeded", "reason": "clean exit"},
				"artifacts": {"result": "succeeded", "reason": "", "objects_uploaded": 0, "bytes_uploaded": 4096}
			}
		}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_r")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.OutcomeResult != "succeeded" || snap.ComputeReason != "clean exit" {
		t.Errorf("compute leg = %q/%q, want succeeded/clean exit", snap.OutcomeResult, snap.ComputeReason)
	}
	if snap.ArtifactResult != "succeeded" || snap.ArtifactReason != "" {
		t.Errorf("artifact leg = %q/%q, want succeeded/empty", snap.ArtifactResult, snap.ArtifactReason)
	}
	if snap.ArtifactObjectsUploaded == nil || *snap.ArtifactObjectsUploaded != 0 {
		t.Errorf("ArtifactObjectsUploaded = %v, want counted 0", snap.ArtifactObjectsUploaded)
	}
	if snap.ArtifactBytesUploaded == nil || *snap.ArtifactBytesUploaded != 4096 {
		t.Errorf("ArtifactBytesUploaded = %v, want 4096", snap.ArtifactBytesUploaded)
	}
}

// TestCentralHTTPClientGetWorkloadStatusOmittedArtifactLegStaysNil covers a
// workload with no export configured: central emits only the compute leg,
// and the artifact fields must land as absent — never as a fabricated 0 —
// so the projection loop preserves the CR's last-known artifact state (or
// leaves it absent).
func TestCentralHTTPClientGetWorkloadStatusOmittedArtifactLegStaysNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"id": "wl_c",
			"status": "succeeded",
			"outcome": {"compute": {"result": "succeeded"}}
		}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_c")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.OutcomeResult != "succeeded" {
		t.Errorf("OutcomeResult = %q, want succeeded", snap.OutcomeResult)
	}
	if snap.ArtifactResult != "" || snap.ArtifactReason != "" ||
		snap.ArtifactObjectsUploaded != nil || snap.ArtifactBytesUploaded != nil {
		t.Errorf("artifact leg leaked from a receipt with no export: %q/%q/%v/%v",
			snap.ArtifactResult, snap.ArtifactReason, snap.ArtifactObjectsUploaded, snap.ArtifactBytesUploaded)
	}
}

// TestCentralHTTPClientGetWorkloadStatusNullCountsStayNil pins the pointer
// semantics: central emits `null` for objects_uploaded and bytes_uploaded on
// an export that failed before counting anything. Those must decode as nil
// on the snapshot so the projection loop knows not to overwrite whatever
// count the CR already carries.
func TestCentralHTTPClientGetWorkloadStatusNullCountsStayNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"id": "wl_f",
			"status": "failed",
			"outcome": {
				"compute": {"result": "succeeded"},
				"artifacts": {"result": "failed", "reason": "the artifact export failed", "objects_uploaded": null, "bytes_uploaded": null}
			}
		}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_f")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.ArtifactResult != "failed" || snap.ArtifactReason != "the artifact export failed" {
		t.Errorf("artifact leg = %q/%q", snap.ArtifactResult, snap.ArtifactReason)
	}
	if snap.ArtifactObjectsUploaded != nil || snap.ArtifactBytesUploaded != nil {
		t.Errorf("null counts leaked as %v/%v", snap.ArtifactObjectsUploaded, snap.ArtifactBytesUploaded)
	}
}

// TestSyncCentralStatusProjectsReceiptLegs proves the projection loop
// stamps the additive compute + artifact fields alongside the legacy
// status.outcome string, and does not re-patch on a second sync of the
// same values (every re-patch is a watch event). The counted-zero object
// count must round-trip as int64(0) — the pointer's zero-vs-nil semantics
// are the whole point of these fields.
func TestSyncCentralStatusProjectsReceiptLegs(t *testing.T) {
	var (
		objects int64 = 0
		bytes   int64 = 4096
	)
	cr := crWithStatus("ml", "train", map[string]any{
		"phase": "Succeeded", "workloadID": "wl_abc",
	})
	r, dyn := statusSyncReconciler(t, cr)
	reader := &stubStatusReader{snapshots: map[string]CentralWorkloadSnapshot{
		"wl_abc": {
			OutcomeResult:           "succeeded",
			ComputeReason:           "clean exit",
			ArtifactResult:          "succeeded",
			ArtifactReason:          "",
			ArtifactObjectsUploaded: &objects,
			ArtifactBytesUploaded:   &bytes,
		},
	}}

	r.syncCentralStatus(context.Background(), reader)

	got, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get workload: %v", err)
	}
	// Legacy compute string is preserved on the same field it has always
	// used, so existing tooling (kubectl printcolumn, dashboards) is
	// unchanged.
	if o, _, _ := unstructured.NestedString(got.Object, "status", "outcome"); o != "succeeded" {
		t.Errorf("legacy status.outcome = %q, want succeeded", o)
	}
	for path, want := range map[string]string{
		"computeResult":  "succeeded",
		"computeReason":  "clean exit",
		"artifactResult": "succeeded",
	} {
		if v, _, _ := unstructured.NestedString(got.Object, "status", path); v != want {
			t.Errorf("status.%s = %q, want %q", path, v, want)
		}
	}
	// Empty reason must not be stamped as an empty string; projectStatusString
	// skips empty values so a later central answer with a reason can fill it in.
	if _, found, _ := unstructured.NestedString(got.Object, "status", "artifactReason"); found {
		t.Errorf("empty artifactReason must stay omitted on the CR")
	}
	if v, _, _ := unstructured.NestedInt64(got.Object, "status", "artifactObjectsUploaded"); v != 0 {
		t.Errorf("status.artifactObjectsUploaded = %d, want counted 0", v)
	}
	if v, _, _ := unstructured.NestedInt64(got.Object, "status", "artifactBytesUploaded"); v != 4096 {
		t.Errorf("status.artifactBytesUploaded = %d, want 4096", v)
	}

	if n := patchActions(dyn); n != 1 {
		t.Fatalf("patches after first sync = %d, want 1", n)
	}
	r.syncCentralStatus(context.Background(), reader)
	if n := patchActions(dyn); n != 1 {
		t.Fatalf("patches after unchanged resync = %d, want 1 (dedup — counted-0 must not re-patch)", n)
	}
}

// TestSyncCentralStatusOmittedArtifactLegPreservesLastKnown proves the
// projection preserves whatever artifact fields the CR already carries when
// central's answer omits the artifact leg entirely (e.g. a receipt from a
// central that predates the field, or a workload that never exported).
// This is the twin of the GPU preservation guard.
func TestSyncCentralStatusOmittedArtifactLegPreservesLastKnown(t *testing.T) {
	cr := crWithStatus("ml", "train", map[string]any{
		"phase": "Succeeded", "workloadID": "wl_abc",
		"artifactResult":          "succeeded",
		"artifactReason":          "prior reason",
		"artifactObjectsUploaded": int64(3),
		"artifactBytesUploaded":   int64(1024),
	})
	r, dyn := statusSyncReconciler(t, cr)
	reader := &stubStatusReader{snapshots: map[string]CentralWorkloadSnapshot{
		"wl_abc": {OutcomeResult: "succeeded"}, // no artifact leg from central
	}}

	r.syncCentralStatus(context.Background(), reader)

	got, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get workload: %v", err)
	}
	if v, _, _ := unstructured.NestedString(got.Object, "status", "artifactResult"); v != "succeeded" {
		t.Errorf("artifactResult erased to %q", v)
	}
	if v, _, _ := unstructured.NestedString(got.Object, "status", "artifactReason"); v != "prior reason" {
		t.Errorf("artifactReason erased to %q", v)
	}
	if v, _, _ := unstructured.NestedInt64(got.Object, "status", "artifactObjectsUploaded"); v != 3 {
		t.Errorf("artifactObjectsUploaded erased to %d", v)
	}
	if v, _, _ := unstructured.NestedInt64(got.Object, "status", "artifactBytesUploaded"); v != 1024 {
		t.Errorf("artifactBytesUploaded erased to %d", v)
	}
}

// TestSyncCentralStatusPresentArtifactLegNullCountsPreserveLastKnown pins
// the finer-grained twin of the guard above: central emits an artifact leg
// (result + reason) but leaves the two count pointers null. The result and
// reason must update; the last-known counts must stay put — a null count
// says "not counted", not "counted zero".
func TestSyncCentralStatusPresentArtifactLegNullCountsPreserveLastKnown(t *testing.T) {
	cr := crWithStatus("ml", "train", map[string]any{
		"phase": "Failed", "workloadID": "wl_abc",
		"artifactObjectsUploaded": int64(3),
		"artifactBytesUploaded":   int64(1024),
	})
	r, dyn := statusSyncReconciler(t, cr)
	reader := &stubStatusReader{snapshots: map[string]CentralWorkloadSnapshot{
		"wl_abc": {
			ArtifactResult: "failed",
			ArtifactReason: "the artifact export failed",
			// counts left nil intentionally
		},
	}}

	r.syncCentralStatus(context.Background(), reader)

	got, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "train", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get workload: %v", err)
	}
	if v, _, _ := unstructured.NestedString(got.Object, "status", "artifactResult"); v != "failed" {
		t.Errorf("artifactResult = %q, want failed", v)
	}
	if v, _, _ := unstructured.NestedInt64(got.Object, "status", "artifactObjectsUploaded"); v != 3 {
		t.Errorf("artifactObjectsUploaded erased to %d (nil count must not overwrite)", v)
	}
	if v, _, _ := unstructured.NestedInt64(got.Object, "status", "artifactBytesUploaded"); v != 1024 {
		t.Errorf("artifactBytesUploaded erased to %d (nil count must not overwrite)", v)
	}
}

// Suppress lint for unused import of "bytes" in tests above.
var _ = bytes.NewReader

func TestScheduledConditionWaiting(t *testing.T) {
	snap := CentralWorkloadSnapshot{
		SchedulingState:   protocol.SchedulingStateWaiting,
		SchedulingReason:  protocol.SchedulingReasonInsufficientGPU,
		SchedulingMessage: "insufficient GPU devices available on any node",
	}
	c := scheduledCondition(snap, 1)
	if c == nil {
		t.Fatal("scheduledCondition returned nil for Waiting state")
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("Status = %q, want False", c.Status)
	}
	if c.Reason != protocol.SchedulingReasonInsufficientGPU {
		t.Errorf("Reason = %q, want %q", c.Reason, protocol.SchedulingReasonInsufficientGPU)
	}
	if c.Type != k8sv1.ConditionScheduled {
		t.Errorf("Type = %q, want %q", c.Type, k8sv1.ConditionScheduled)
	}
}

func TestScheduledConditionScheduled(t *testing.T) {
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	snap := CentralWorkloadSnapshot{
		SchedulingState:      protocol.SchedulingStateScheduled,
		SchedulingObservedAt: &at,
	}
	c := scheduledCondition(snap, 1)
	if c == nil {
		t.Fatal("scheduledCondition returned nil for Scheduled state")
	}
	if c.Status != metav1.ConditionTrue {
		t.Errorf("Status = %q, want True", c.Status)
	}
	if c.Reason != k8sv1.ReasonScheduled {
		t.Errorf("Reason = %q, want %q", c.Reason, k8sv1.ReasonScheduled)
	}
}

func TestScheduledConditionLegacy(t *testing.T) {
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	snap := CentralWorkloadSnapshot{
		ScheduledAt: &at,
	}
	c := scheduledCondition(snap, 1)
	if c == nil {
		t.Fatal("scheduledCondition returned nil for legacy positive path")
	}
	if c.Status != metav1.ConditionTrue {
		t.Errorf("Status = %q, want True", c.Status)
	}
}

func TestScheduledConditionNoObservation(t *testing.T) {
	snap := CentralWorkloadSnapshot{}
	c := scheduledCondition(snap, 1)
	if c != nil {
		t.Fatal("scheduledCondition should return nil when no observation exists")
	}
}

func TestWaitingForPodProjectionNonTerminal(t *testing.T) {
	snap := CentralWorkloadSnapshot{
		Status:           "provisioning",
		SchedulingState:  protocol.SchedulingStateWaiting,
		SchedulingReason: protocol.SchedulingReasonInsufficientGPU,
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"phase": "Provisioning"},
	}}
	phase, reason := waitingForPodProjection(snap, obj)
	if phase != "WaitingForPod" {
		t.Errorf("phase = %q, want WaitingForPod", phase)
	}
	if reason != protocol.SchedulingReasonInsufficientGPU {
		t.Errorf("reason = %q, want %q", reason, protocol.SchedulingReasonInsufficientGPU)
	}
}

func TestWaitingForPodProjectionNoRegressionFromRunning(t *testing.T) {
	for _, terminal := range []string{"Running", "Succeeded", "Failed", "Cancelled"} {
		snap := CentralWorkloadSnapshot{
			Status:           strings.ToLower(terminal),
			SchedulingState:  protocol.SchedulingStateWaiting,
			SchedulingReason: protocol.SchedulingReasonInsufficientGPU,
		}
		obj := &unstructured.Unstructured{Object: map[string]any{
			"status": map[string]any{"phase": terminal},
		}}
		phase, _ := waitingForPodProjection(snap, obj)
		if phase != "" {
			t.Errorf("WaitingForPod regressed from %q (phase=%q)", terminal, phase)
		}
	}
}

func TestWaitingForPodProjectionNotWaiting(t *testing.T) {
	snap := CentralWorkloadSnapshot{
		SchedulingState: protocol.SchedulingStateScheduled,
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"phase": "Provisioning"},
	}}
	phase, _ := waitingForPodProjection(snap, obj)
	if phase != "" {
		t.Errorf("should not project WaitingForPod for Scheduled state, got %q", phase)
	}
}

func TestWaitingForPodOverridesProvisioning(t *testing.T) {
	snap := CentralWorkloadSnapshot{
		Status:           "provisioning",
		SchedulingState:  protocol.SchedulingStateWaiting,
		SchedulingReason: protocol.SchedulingReasonInsufficientGPU,
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"phase": "Pending"},
	}}
	phase, reason := waitingForPodProjection(snap, obj)
	if phase != "WaitingForPod" {
		t.Fatalf("WaitingForPod should override Provisioning, got phase=%q", phase)
	}
	if reason != protocol.SchedulingReasonInsufficientGPU {
		t.Fatalf("reason = %q, want InsufficientGPU", reason)
	}
}

func TestWaitingForPodSuppressedByPositiveScheduling(t *testing.T) {
	scheduledAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	snap := CentralWorkloadSnapshot{
		Status:           "provisioning",
		SchedulingState:  protocol.SchedulingStateWaiting,
		SchedulingReason: protocol.SchedulingReasonInsufficientGPU,
		ScheduledAt:      &scheduledAt,
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"phase": "Provisioning"},
	}}
	phase, _ := waitingForPodProjection(snap, obj)
	if phase != "" {
		t.Fatalf("WaitingForPod should be suppressed by positive ScheduledAt, got phase=%q", phase)
	}
}

func TestScheduledConditionPositiveTakesPrecedenceOverWaiting(t *testing.T) {
	scheduledAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	snap := CentralWorkloadSnapshot{
		SchedulingState:  protocol.SchedulingStateWaiting,
		SchedulingReason: protocol.SchedulingReasonInsufficientGPU,
		ScheduledAt:      &scheduledAt,
	}
	c := scheduledCondition(snap, 1)
	if c == nil {
		t.Fatal("scheduledCondition = nil")
	}
	if c.Status != metav1.ConditionTrue {
		t.Fatalf("Scheduled Status = %q, want True (positive proof should win)", c.Status)
	}
}

func TestWaitingForPodDoesNotOverrideRunningStatus(t *testing.T) {
	snap := CentralWorkloadSnapshot{
		Status:           "running",
		SchedulingState:  protocol.SchedulingStateWaiting,
		SchedulingReason: protocol.SchedulingReasonInsufficientGPU,
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"phase": "Running"},
	}}
	phase, _ := waitingForPodProjection(snap, obj)
	if phase != "" {
		t.Fatalf("WaitingForPod should never override Running, got phase=%q", phase)
	}
}

func TestGPUReadyIndependentOfScheduling(t *testing.T) {
	gpuAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	snap := CentralWorkloadSnapshot{
		SchedulingState:  protocol.SchedulingStateWaiting,
		SchedulingReason: protocol.SchedulingReasonInsufficientGPU,
		GPUReadyAt:       &gpuAt,
	}

	gpuC := gpuReadyCondition(snap, 1)
	schedC := scheduledCondition(snap, 1)

	if gpuC == nil {
		t.Fatal("GPU condition should be projected independently of scheduling state")
	}
	if gpuC.Status != metav1.ConditionTrue {
		t.Errorf("GPU Status = %q, want True", gpuC.Status)
	}
	if schedC == nil || schedC.Status != metav1.ConditionFalse {
		t.Error("Scheduled condition should be False while GPU is True")
	}
}
