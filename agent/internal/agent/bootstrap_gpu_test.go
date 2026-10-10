package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	k8sv1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

func bootstrapAndBindBurst(s *BootstrapServer, burstID string) string {
	nodeName := expectedBurstNodeName(burstID)
	s.AuthorizeBurst(burstID, "full", "default", nil)
	s.mu.Lock()
	s.nodes[nodeName] = burstID
	s.mu.Unlock()
	return nodeName
}

func postGPU(t *testing.T, s *BootstrapServer, burstID, nodeName string, util float64) *httptest.ResponseRecorder {
	return postGPUProduct(t, s, burstID, nodeName, util, "")
}

func postGPUProduct(t *testing.T, s *BootstrapServer, burstID, nodeName string, util float64, product string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(gpuTelemetryRequest{
		BurstID: burstID, NodeName: nodeName, Utilization: util, Product: product,
	})
	req := httptest.NewRequest(http.MethodPost, "/gpu-telemetry", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.handleGPUTelemetry(w, req)
	return w
}

func gpuNode(name, allocatable string, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceName("nvidia.com/gpu"): resource.MustParse(allocatable),
		}},
	}
}

func newTestBootstrapServer() *BootstrapServer {
	return &BootstrapServer{
		Log:    slog.Default(),
		grants: make(map[string]*burstGrant),
		nodes:  make(map[string]string),
	}
}

func TestGPUTelemetryEndpointHappyPath(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)

	w := postGPU(t, s, burstID, nodeName, 75)
	if w.Code != http.StatusNoContent {
		t.Fatalf("got status %d, want 204: %s", w.Code, w.Body.String())
	}
	samples := s.GPUTelemetrySamples()
	if len(samples) != 1 {
		t.Fatalf("got %d samples, want 1", len(samples))
	}
	if samples[0].Utilization != 75 {
		t.Fatalf("got utilization %f, want 75", samples[0].Utilization)
	}
}

func TestGPUTelemetryReconcilesHardwareObservedProductAfterGPUReady(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)
	s.K8s = k8sfake.NewSimpleClientset(gpuNode(nodeName, "1", nil))

	w := postGPUProduct(t, s, burstID, nodeName, 75, "NVIDIA RTX 4000 Ada Generation")
	if w.Code != http.StatusNoContent {
		t.Fatalf("got status %d, want 204: %s", w.Code, w.Body.String())
	}
	node, err := s.K8s.CoreV1().Nodes().Get(t.Context(), nodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := node.Labels[k8sv1.LabelNvidiaGPUProduct]; got != "NVIDIA-RTX-4000-Ada-Generation" {
		t.Fatalf("gpu product label = %q, want hardware-observed product", got)
	}
}

func TestGPUTelemetryDoesNotLabelProductBeforeGPUAllocatable(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)
	s.K8s = k8sfake.NewSimpleClientset(gpuNode(nodeName, "0", nil))

	postGPUProduct(t, s, burstID, nodeName, 0, "NVIDIA RTX 4000 Ada Generation")
	node, err := s.K8s.CoreV1().Nodes().Get(t.Context(), nodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := node.Labels[k8sv1.LabelNvidiaGPUProduct]; got != "" {
		t.Fatalf("gpu product label = %q before allocatable, want absent", got)
	}
}

func TestGPUTelemetryPreservesExistingGFDProduct(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)
	s.K8s = k8sfake.NewSimpleClientset(gpuNode(nodeName, "1", map[string]string{
		k8sv1.LabelNvidiaGPUProduct: "NVIDIA-RTX-4000-Ada-Generation-SHARED",
	}))

	postGPUProduct(t, s, burstID, nodeName, 50, "NVIDIA RTX 4000 Ada Generation")
	node, err := s.K8s.CoreV1().Nodes().Get(t.Context(), nodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := node.Labels[k8sv1.LabelNvidiaGPUProduct]; got != "NVIDIA-RTX-4000-Ada-Generation-SHARED" {
		t.Fatalf("existing GFD product was overwritten: %q", got)
	}
}

func TestGPUTelemetryInvalidProductDoesNotDiscardUtilization(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)
	s.K8s = k8sfake.NewSimpleClientset(gpuNode(nodeName, "1", nil))

	w := postGPUProduct(t, s, burstID, nodeName, 25, "invalid/product")
	if w.Code != http.StatusNoContent {
		t.Fatalf("invalid optional product discarded valid telemetry: status %d", w.Code)
	}
	if got := s.GPUTelemetrySamples(); len(got) != 1 || got[0].Utilization != 25 {
		t.Fatalf("utilization sample = %+v, want retained", got)
	}
	node, err := s.K8s.CoreV1().Nodes().Get(t.Context(), nodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := node.Labels[k8sv1.LabelNvidiaGPUProduct]; got != "" {
		t.Fatalf("invalid product created label %q", got)
	}
}

func TestGPUProductLabelValue(t *testing.T) {
	for _, tt := range []struct {
		raw  string
		want string
		ok   bool
	}{
		{"NVIDIA RTX 4000 Ada Generation", "NVIDIA-RTX-4000-Ada-Generation", true},
		{"NVIDIA TITAN X (Pascal)", "NVIDIA-TITAN-X-Pascal", true},
		{" Tesla T4 ", "Tesla-T4", true},
		{"invalid/product", "", false},
		{strings.Repeat("x", k8sv1.MaxLabelValueLen+1), "", false},
		{"", "", false},
	} {
		got, ok := gpuProductLabelValue(tt.raw)
		if got != tt.want || ok != tt.ok {
			t.Errorf("gpuProductLabelValue(%q) = (%q, %v), want (%q, %v)", tt.raw, got, ok, tt.want, tt.ok)
		}
	}
}

func TestGPUTelemetryEndpointRejectsUnauthorizedBurst(t *testing.T) {
	s := newTestBootstrapServer()
	nodeName := expectedBurstNodeName("burst_unknown")
	s.K8s = k8sfake.NewSimpleClientset(gpuNode(nodeName, "1", nil))
	w := postGPUProduct(t, s, "burst_unknown", nodeName, 50, "NVIDIA RTX 4000 Ada Generation")
	if w.Code != http.StatusForbidden {
		t.Fatalf("got status %d, want 403", w.Code)
	}
	node, err := s.K8s.CoreV1().Nodes().Get(t.Context(), nodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := node.Labels[k8sv1.LabelNvidiaGPUProduct]; got != "" {
		t.Fatalf("unauthorized telemetry created product label %q", got)
	}
}

func TestGPUTelemetryEndpointRejectsMismatchedNodeName(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	bootstrapAndBindBurst(s, burstID)
	w := postGPU(t, s, burstID, "wrong-node", 50)
	if w.Code != http.StatusForbidden {
		t.Fatalf("got status %d, want 403", w.Code)
	}
}

func TestGPUTelemetryEndpointRejectsRevokedBurst(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)
	s.Forget(burstID)

	w := postGPU(t, s, burstID, nodeName, 50)
	if w.Code != http.StatusForbidden {
		t.Fatalf("got status %d, want 403", w.Code)
	}
}

func TestGPUTelemetrySnapshotDoesNotClear(t *testing.T) {
	s := newTestBootstrapServer()
	s.mu.Lock()
	s.gpuSamples = map[string]protocol.GPUTelemetrySample{
		"b1": {BurstID: "b1", Utilization: 50},
		"b2": {BurstID: "b2", Utilization: 80},
	}
	s.mu.Unlock()
	first := s.GPUTelemetrySamples()
	if len(first) != 2 {
		t.Fatalf("got %d, want 2", len(first))
	}
	second := s.GPUTelemetrySamples()
	if len(second) != 2 {
		t.Fatalf("snapshot should preserve: got %d, want 2", len(second))
	}
}

func TestGPUTelemetrySnapshotBatchesEventuallyCoverEveryBurst(t *testing.T) {
	s := newTestBootstrapServer()
	total := protocol.MaxGPUTelemetrySamples + 5
	s.mu.Lock()
	s.gpuSamples = make(map[string]protocol.GPUTelemetrySample, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("burst_%03d", i)
		s.gpuSamples[id] = protocol.GPUTelemetrySample{BurstID: id, Utilization: float64(i % 101)}
	}
	s.mu.Unlock()

	seen := make(map[string]bool, total)
	for attempt := 0; attempt < 2; attempt++ {
		batch := s.GPUTelemetrySamples()
		if len(batch) != protocol.MaxGPUTelemetrySamples {
			t.Fatalf("batch %d has %d samples, want %d", attempt, len(batch), protocol.MaxGPUTelemetrySamples)
		}
		for _, sample := range batch {
			seen[sample.BurstID] = true
		}
	}
	if len(seen) != total {
		t.Fatalf("two batches covered %d of %d bursts", len(seen), total)
	}
}

func TestGPUTelemetryReplacesOlderSample(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)

	postGPU(t, s, burstID, nodeName, 50)
	postGPU(t, s, burstID, nodeName, 90)
	samples := s.GPUTelemetrySamples()
	if len(samples) != 1 || samples[0].Utilization != 90 {
		t.Fatalf("should keep latest: %+v", samples)
	}
}

func TestGPUTelemetryForgetClearsCachedSample(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)

	postGPU(t, s, burstID, nodeName, 75)
	if len(s.GPUTelemetrySamples()) != 1 {
		t.Fatal("sample should exist before forget")
	}
	s.Forget(burstID)
	if len(s.GPUTelemetrySamples()) != 0 {
		t.Fatal("forget must clear cached GPU sample")
	}
}

func TestGPUTelemetryForgetNodeClearsCachedSample(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)

	postGPU(t, s, burstID, nodeName, 75)
	s.ForgetNode(nodeName)
	if len(s.GPUTelemetrySamples()) != 0 {
		t.Fatal("ForgetNode must clear cached GPU sample")
	}
}

func TestGPUTelemetryRequiresNodeBinding(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := expectedBurstNodeName(burstID)
	s.AuthorizeBurst(burstID, "full", "default", nil)

	w := postGPU(t, s, burstID, nodeName, 50)
	if w.Code != http.StatusForbidden {
		t.Fatalf("should reject before bootstrap binding: got %d", w.Code)
	}
}

func TestGPUTelemetryRejectsTrailingJSON(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)

	body := `{"burst_id":"` + burstID + `","node_name":"` + nodeName + `","utilization":50}{"extra":true}`
	req := httptest.NewRequest(http.MethodPost, "/gpu-telemetry", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.handleGPUTelemetry(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("trailing JSON should be rejected: got %d", w.Code)
	}
}

func TestGPUTelemetryRejectsUnknownFields(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)

	body := `{"burst_id":"` + burstID + `","node_name":"` + nodeName + `","utilization":50,"evil":"payload"}`
	req := httptest.NewRequest(http.MethodPost, "/gpu-telemetry", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.handleGPUTelemetry(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown fields should be rejected: got %d", w.Code)
	}
}

func TestGPUTelemetryRejectsOversizedBody(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)

	body := `{"burst_id":"` + burstID + `","node_name":"` + nodeName + `","utilization":50,"` + strings.Repeat("x", maxGPUTelemetryBody+100) + `":""}`
	req := httptest.NewRequest(http.MethodPost, "/gpu-telemetry", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.handleGPUTelemetry(w, req)
	if w.Code == http.StatusNoContent {
		t.Fatal("oversized body should be rejected")
	}
}

func TestGPUTelemetryZeroUtilization(t *testing.T) {
	s := newTestBootstrapServer()
	burstID := "burst_abc123"
	nodeName := bootstrapAndBindBurst(s, burstID)

	w := postGPU(t, s, burstID, nodeName, 0)
	if w.Code != http.StatusNoContent {
		t.Fatalf("0%% utilization is valid: got %d", w.Code)
	}
	samples := s.GPUTelemetrySamples()
	if len(samples) != 1 || samples[0].Utilization != 0 {
		t.Fatalf("0%% should be stored: %+v", samples)
	}
}
