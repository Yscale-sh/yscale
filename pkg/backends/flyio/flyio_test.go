package flyio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/workload"
)

// maxMemPerCPU mirrors the constant in flyio.go so we can test the
// Fly-valid invariant without exposing it.
const testMaxMemPerCPU = 2048

// reserveForAllocatable is the fraction of machine memory that remains
// after kubelet/system reserved (~10-15%). We use 85% as a conservative
// floor to express the invariant: allocatable > request * reserveForAllocatable.
const reserveForAllocatable = 0.85

func TestMapResources_Headroom(t *testing.T) {
	tests := []struct {
		name  string
		size  string
		after func(t *testing.T, got GuestConfig, req backends.ResourceRequirements)
	}{
		{
			name: "nano",
			size: "nano",
			after: func(t *testing.T, got GuestConfig, req backends.ResourceRequirements) {
				// Nano: request 512 MiB → with headroom should be > ~600 MB.
				if got.MemoryMB <= int(float64(req.MemoryMB)*reserveForAllocatable) {
					t.Errorf("nano: MemoryMB=%d, want > %.0f (request %d MiB * reserve %.0f)",
						got.MemoryMB, float64(req.MemoryMB)*reserveForAllocatable, req.MemoryMB, reserveForAllocatable)
				}
			},
		},
		{
			name: "small",
			size: "small",
			after: func(t *testing.T, got GuestConfig, req backends.ResourceRequirements) {
				// Small: request 2048 MiB → with headroom should be > ~2663 MB.
				if got.MemoryMB <= int(float64(req.MemoryMB)*reserveForAllocatable) {
					t.Errorf("small: MemoryMB=%d, want > %.0f (request %d MiB * reserve %.0f)",
						got.MemoryMB, float64(req.MemoryMB)*reserveForAllocatable, req.MemoryMB, reserveForAllocatable)
				}
			},
		},
		{
			name: "medium",
			size: "medium",
			after: func(t *testing.T, got GuestConfig, req backends.ResourceRequirements) {
				// Medium: request 4096 MiB → with headroom should be > ~5325 MB.
				if got.MemoryMB <= int(float64(req.MemoryMB)*reserveForAllocatable) {
					t.Errorf("medium: MemoryMB=%d, want > %.0f (request %d MiB * reserve %.0f)",
						got.MemoryMB, float64(req.MemoryMB)*reserveForAllocatable, req.MemoryMB, reserveForAllocatable)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			size, ok := workload.LookupSize(tc.size)
			if !ok {
				t.Fatalf("unknown size %q", tc.size)
			}
			req := backends.ResourceRequirements{
				CPUMillis: size.CPUMillis,
				MemoryMB:  size.MemoryMB,
			}
			got := mapResources(req)

			// Fly-valid invariant: memory must be within 256 MB .. 2048 MB per shared vCPU.
			if got.MemoryMB < 256 {
				t.Errorf("MemoryMB=%d < 256 (below Fly minimum)", got.MemoryMB)
			}
			if got.MemoryMB > got.CPUs*testMaxMemPerCPU {
				t.Errorf("MemoryMB=%d > %d*%d=%d (above Fly max per shared vCPU)",
					got.MemoryMB, got.CPUs, testMaxMemPerCPU, got.CPUs*testMaxMemPerCPU)
			}
			// Fly requires memory to be a multiple of 256.
			if got.MemoryMB%256 != 0 {
				t.Errorf("MemoryMB=%d is not a multiple of 256", got.MemoryMB)
			}
			// Memory must exceed the pod request with headroom.
			tc.after(t, got, req)
		})
	}
}

// TestMapResources_Small_ExactlyFitsWithHeadroom verifies that the small size
// (2048 MiB pod request) results in a machine that can actually accommodate that
// request after kubelet/system reserved. This is the specific bug scenario.
func TestMapResources_Small_ExactlyFitsWithHeadroom(t *testing.T) {
	size, _ := workload.LookupSize("small")
	req := backends.ResourceRequirements{
		CPUMillis: size.CPUMillis,
		MemoryMB:  size.MemoryMB, // 2048 MiB
	}
	got := mapResources(req)

	// Before the fix: machine was exactly 2048 MB with ~1820 MiB allocatable,
	// which is less than the pod's 2048 MiB request → unschedulable.
	// After the fix: machine is ~2664 MB, allocatable ~2264 MiB > 2048 MiB request.
	allocatableMiB := int(float64(got.MemoryMB) * reserveForAllocatable)
	if allocatableMiB <= int(req.MemoryMB) {
		t.Errorf("small: allocatableMiB=%d (from %d MB machine) must exceed request %d MiB",
			allocatableMiB, got.MemoryMB, req.MemoryMB)
	}
}

// TestMapResources_MemoryRounding verifies that mapResources always rounds
// memory to a Fly-valid 256 MB boundary.
func TestMapResources_MemoryRounding(t *testing.T) {
	cases := []struct {
		name          string
		memMB         int64
		wantDivisible bool
	}{
		{"exact 512", 512, true},
		{"prime near 2048", 2053, true},
		{"zero", 0, true},
		{"small floor", 100, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mapResources(backends.ResourceRequirements{CPUMillis: 1000, MemoryMB: c.memMB})
			if c.wantDivisible && got.MemoryMB%256 != 0 {
				t.Errorf("MemoryMB=%d not divisible by 256", got.MemoryMB)
			}
		})
	}
}

type mockTransport struct {
	roundTrip func(*http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

func TestDeleteNode_Idempotent404(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			// All calls to absent machine return 404
			if req.Method == "GET" && strings.Contains(req.URL.Path, "/machines/123") {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader(`{"error":"machine not found"}`)),
				}, nil
			}
			if req.Method == "POST" && strings.Contains(req.URL.Path, "/machines/123/stop") {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader(`{"error":"machine not found"}`)),
				}, nil
			}
			if req.Method == "DELETE" && strings.Contains(req.URL.Path, "/machines/123") {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader(`{"error":"machine not found"}`)),
				}, nil
			}
			t.Errorf("unexpected request: %s %s", req.Method, req.URL)
			return nil, errors.New("unexpected request")
		},
	}

	err := b.DeleteNode(context.Background(), "123")
	if err != nil {
		t.Fatalf("expected nil error on 404, got: %v", err)
	}
}

func TestDeleteNode_Success(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			if req.Method == "GET" && strings.Contains(req.URL.Path, "/machines/123") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"id":"123","config":{"metadata":{"yscale-node":"ys-burst-test","yscale-volume":"vol-456"}}}`)),
				}, nil
			}
			if req.Method == "POST" && strings.Contains(req.URL.Path, "/machines/123/stop") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			}
			if req.Method == "DELETE" && strings.Contains(req.URL.Path, "/machines/123") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			}
			if req.Method == "DELETE" && strings.Contains(req.URL.Path, "/volumes/vol-456") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			}
			t.Errorf("unexpected request: %s %s", req.Method, req.URL)
			return nil, errors.New("unexpected request")
		},
	}

	err := b.DeleteNode(context.Background(), "123")
	if err != nil {
		t.Fatalf("expected nil error on success, got: %v", err)
	}
}

func TestDeleteNode_PreserveErrors(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		respBody   string
		wantErr    string
	}{
		{
			name:       "unauthorized 401",
			statusCode: http.StatusUnauthorized,
			respBody:   "unauthorized",
			wantErr:    "status 401",
		},
		{
			name:       "rate limit 429",
			statusCode: http.StatusTooManyRequests,
			respBody:   "rate limited",
			wantErr:    "status 429",
		},
		{
			name:       "server error 500",
			statusCode: http.StatusInternalServerError,
			respBody:   "internal error",
			wantErr:    "status 500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New("mock-token", "mock-org", "mock-region")
			b.client.Transport = &mockTransport{
				roundTrip: func(req *http.Request) (*http.Response, error) {
					// We let getMachine fail with 500 or just mock destroyMachine failing
					if req.Method == "GET" && strings.Contains(req.URL.Path, "/machines/123") {
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(strings.NewReader(`{"id":"123","config":{"metadata":{"yscale-node":"ys-burst-test"}}}`)),
						}, nil
					}
					if req.Method == "POST" && strings.Contains(req.URL.Path, "/machines/123/stop") {
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(strings.NewReader("{}")),
						}, nil
					}
					if req.Method == "DELETE" && strings.Contains(req.URL.Path, "/machines/123") {
						return &http.Response{
							StatusCode: tt.statusCode,
							Body:       io.NopCloser(strings.NewReader(tt.respBody)),
						}, nil
					}
					return nil, errors.New("unexpected request")
				},
			}

			err := b.DeleteNode(context.Background(), "123")
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got: %v", tt.wantErr, err)
			}

			var httpErr *HTTPError
			if !errors.As(err, &httpErr) {
				t.Fatalf("expected error to be of type *HTTPError, got: %T (%v)", err, err)
			}
			if httpErr.StatusCode != tt.statusCode {
				t.Errorf("expected StatusCode %d, got %d", tt.statusCode, httpErr.StatusCode)
			}
		})
	}
}

func TestDeleteNode_TransportError(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	expectedErr := errors.New("network down")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			if req.Method == "GET" {
				// getMachine succeeds or fails, let's say it succeeds
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"id":"123","config":{"metadata":{"yscale-node":"ys-burst-test"}}}`)),
				}, nil
			}
			if req.Method == "POST" {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			}
			if req.Method == "DELETE" && strings.Contains(req.URL.Path, "/machines/123") {
				return nil, expectedErr
			}
			return nil, errors.New("unexpected request")
		},
	}

	err := b.DeleteNode(context.Background(), "123")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to wrap %q, got: %v", expectedErr, err)
	}
}

// --- Warm capability tests ---

func TestSetPoolScope(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	if b.poolScope != "" {
		t.Errorf("expected empty poolScope, got %q", b.poolScope)
	}
	b.SetPoolScope("hash-123")
	if b.poolScope != "hash-123" {
		t.Errorf("expected poolScope=hash-123, got %q", b.poolScope)
	}
	// Verify ScopedBackend interface compliance.
	var _ backends.ScopedBackend = b
}

// mockAppHandler is a helper that returns mock responses for ensureApp and
// machine creation, capturing the last create request body for inspection.
type createCapturer struct {
	lastBody []byte
}

func (c *createCapturer) RoundTrip(req *http.Request) (*http.Response, error) {
	// ensureApp: app already exists
	if req.Method == "GET" && strings.Contains(req.URL.Path, "/apps/") {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"name":"hs-mock-org-burst"}`)),
		}, nil
	}
	// createMachine: capture body and return success
	if req.Method == "POST" && strings.Contains(req.URL.Path, "/machines") {
		var err error
		c.lastBody, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"id":"m-001","name":"test-node","state":"created"}`)),
		}, nil
	}
	return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
}

func TestCreateNode_AutoDestroyCold(t *testing.T) {
	capt := &createCapturer{}
	b := New("mock-token", "mock-org", "mock-region")
	b.client.Transport = capt

	spec := &backends.NodeSpec{
		Name:       "ys-burst-test",
		AgentImage: backends.AgentImage(),
		Lifecycle:  backends.LifecycleCold,
	}
	_, err := b.CreateNode(context.Background(), spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var req CreateMachineRequest
	if err := json.Unmarshal(capt.lastBody, &req); err != nil {
		t.Fatalf("failed to decode request body: %v", err)
	}
	if !req.Config.AutoDestroy {
		t.Error("cold lifecycle: expected AutoDestroy=true")
	}
	if v := req.Config.Metadata["yscale-lifecycle"]; v != string(backends.LifecycleCold) {
		t.Errorf("cold lifecycle: expected yscale-lifecycle=cold, got %q", v)
	}
}

func TestCreateNode_AutoDestroyPrewarm(t *testing.T) {
	capt := &createCapturer{}
	b := New("mock-token", "mock-org", "mock-region")
	b.SetPoolScope("scope-abc")
	b.client.Transport = capt

	spec := &backends.NodeSpec{
		Name:       "ys-burst-pre",
		AgentImage: "registry.fly.io/yscale-burst-image@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Lifecycle:  backends.LifecyclePrewarm,
		ScopeHash:  "scope-abc",
		ConfigHash: "cfghash12",
	}
	_, err := b.CreateNode(context.Background(), spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var req CreateMachineRequest
	if err := json.Unmarshal(capt.lastBody, &req); err != nil {
		t.Fatalf("failed to decode request body: %v", err)
	}
	if req.Config.AutoDestroy {
		t.Error("prewarm lifecycle: expected AutoDestroy=false")
	}
	if v := req.Config.Metadata["yscale-lifecycle"]; v != string(backends.LifecyclePrewarm) {
		t.Errorf("prewarm lifecycle: expected yscale-lifecycle=prewarm, got %q", v)
	}
	if v := req.Config.Metadata["yscale-scope"]; v != "scope-abc" {
		t.Errorf("expected yscale-scope=scope-abc, got %q", v)
	}
	if v := req.Config.Metadata["yscale-config-hash"]; v != "cfghash12" {
		t.Errorf("expected yscale-config-hash=cfghash12, got %q", v)
	}
}

func TestCreateNode_DefaultLifecycleIsCold(t *testing.T) {
	capt := &createCapturer{}
	b := New("mock-token", "mock-org", "mock-region")
	b.client.Transport = capt

	spec := &backends.NodeSpec{
		Name:       "ys-burst-legacy",
		AgentImage: backends.AgentImage(),
		// Lifecycle not set (zero value).
	}
	_, err := b.CreateNode(context.Background(), spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var req CreateMachineRequest
	if err := json.Unmarshal(capt.lastBody, &req); err != nil {
		t.Fatalf("failed to decode request body: %v", err)
	}
	if !req.Config.AutoDestroy {
		t.Error("default lifecycle: expected AutoDestroy=true")
	}
	if v := req.Config.Metadata["yscale-lifecycle"]; v != "" && v != string(backends.LifecycleCold) {
		t.Errorf("default lifecycle: expected empty or cold, got %q", v)
	}
}

// --- SuspendNode tests ---

type suspendCapturer struct {
	gotSuspendCall bool
}

func (s *suspendCapturer) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet {
		body := `{"id":"m-123","state":"started","config":{"metadata":{"yscale-node":"ys-burst-test","yscale-scope":"scope-a","yscale-lifecycle":"prewarm","yscale-config-hash":"cfg-1"}}}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
	if req.Method == "POST" && strings.Contains(req.URL.Path, "/suspend") {
		s.gotSuspendCall = true
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("{}")),
		}, nil
	}
	return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
}

func TestSuspendNode_Success(t *testing.T) {
	capt := &suspendCapturer{}
	b := New("mock-token", "mock-org", "mock-region")
	b.SetPoolScope("scope-a")
	b.client.Transport = capt

	err := b.SuspendNode(context.Background(), "m-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !capt.gotSuspendCall {
		t.Error("expected suspend call to be made")
	}
}

func TestSuspendNode_AcceptsAsyncResponse(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.SetPoolScope("scope-a")
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet {
			body := `{"id":"m-123","state":"started","config":{"metadata":{"yscale-node":"ys-burst-test","yscale-scope":"scope-a","yscale-lifecycle":"prewarm","yscale-config-hash":"cfg-1"}}}`
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
		}
		if req.Method != http.MethodPost || !strings.HasSuffix(req.URL.Path, "/suspend") {
			return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}}

	if err := b.SuspendNode(context.Background(), "m-123"); err != nil {
		t.Fatalf("SuspendNode returned error for 202 Accepted: %v", err)
	}
}

func TestStartNode_AcceptsAsyncResponse(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.SetPoolScope("scope-a")
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet {
			body := `{"id":"m-123","state":"suspended","config":{"metadata":{"yscale-node":"ys-burst-test","yscale-scope":"scope-a","yscale-lifecycle":"prewarm","yscale-config-hash":"cfg-1"}}}`
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
		}
		if req.Method != http.MethodPost || !strings.HasSuffix(req.URL.Path, "/start") {
			return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}}

	if err := b.StartNode(context.Background(), "m-123"); err != nil {
		t.Fatalf("StartNode returned error for 202 Accepted: %v", err)
	}
}

func TestSuspendNode_Error(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader("internal error")),
			}, nil
		},
	}

	err := b.SuspendNode(context.Background(), "m-123")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Errorf("expected status 500 error, got: %v", err)
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected typed HTTPError(500), got %T: %v", err, err)
	}
}

func TestSuspendNode_SatisfiesWarmCapability(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	var _ backends.WarmCapability = b
}

// --- ListPooledNodes scope and suspended tests ---

func TestListPooledNodes_ScopeFiltering(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.SetPoolScope("scope-a")

	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			body := `[
				{"id":"m-1","state":"suspended","config":{"metadata":{"yscale-node":"ys-burst-node-1","yscale-scope":"scope-a","yscale-lifecycle":"prewarm"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-2","state":"suspended","config":{"metadata":{"yscale-node":"ys-burst-node-2","yscale-scope":"scope-b","yscale-lifecycle":"prewarm"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-3","state":"suspended","config":{"metadata":{"yscale-node":"ys-burst-node-3","yscale-lifecycle":"prewarm"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-4","state":"suspended","config":{"metadata":{"yscale-node":"ys-burst-node-4","yscale-scope":"scope-a","yscale-lifecycle":"prewarm"}},"created_at":"2025-01-01T00:00:00Z"}
			]`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		},
	}

	nodes, err := b.ListPooledNodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes in scope-a, got %d", len(nodes))
	}
	if nodes[0].BackendID != "m-1" || nodes[1].BackendID != "m-4" {
		t.Errorf("unexpected node IDs: %v, %v", nodes[0].BackendID, nodes[1].BackendID)
	}
}

func TestListPooledNodes_DefaultOnlyIncludesColdStopped(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			body := `[
				{"id":"m-1","state":"stopped","config":{"metadata":{"yscale-node":"ys-burst-node-1"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-2","state":"suspended","config":{"metadata":{"yscale-node":"ys-burst-node-2","yscale-lifecycle":"prewarm"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-3","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-3"}},"created_at":"2025-01-01T00:00:00Z"}
			]`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		},
	}

	nodes, err := b.ListPooledNodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected only the cold-stopped node, got %d", len(nodes))
	}
	if nodes[0].BackendID != "m-1" || nodes[0].Lifecycle == backends.LifecyclePrewarm {
		t.Fatalf("unexpected default candidate: %+v", nodes[0])
	}
}

func TestListPooledNodes_PopulatesMetadataFields(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.SetPoolScope("scope-x")
	createdAt := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			body := fmt.Sprintf(`[
				{"id":"m-1","state":"suspended","config":{"metadata":{"yscale-node":"ys-burst-node-1","yscale-lifecycle":"prewarm","yscale-scope":"scope-x","yscale-config-hash":"cfghash12"}},"created_at":"%s"}
			]`, createdAt.Format(time.RFC3339))
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		},
	}

	nodes, err := b.ListPooledNodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	n := nodes[0]
	if n.Name != "ys-burst-node-1" {
		t.Errorf("expected name=ys-burst-node-1, got %q", n.Name)
	}
	if n.Lifecycle != backends.LifecyclePrewarm {
		t.Errorf("expected lifecycle=prewarm, got %q", n.Lifecycle)
	}
	if n.ScopeHash != "scope-x" {
		t.Errorf("expected scope=scope-x, got %q", n.ScopeHash)
	}
	if n.ConfigHash != "cfghash12" {
		t.Errorf("expected configHash=cfghash12, got %q", n.ConfigHash)
	}
	if !n.CreatedAt.Equal(createdAt) {
		t.Errorf("expected CreatedAt=%v, got %v", createdAt, n.CreatedAt)
	}
}

func TestListPooledNodes_UnscopedCannotSeeWarmCandidates(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	// poolScope stays empty (unscoped legacy).

	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			body := `[
				{"id":"m-1","state":"suspended","config":{"metadata":{"yscale-node":"ys-burst-node-1","yscale-scope":"scope-a","yscale-lifecycle":"prewarm"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-2","state":"suspended","config":{"metadata":{"yscale-node":"ys-burst-node-2","yscale-scope":"scope-b","yscale-lifecycle":"prewarm"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-3","state":"suspended","config":{"metadata":{"yscale-node":"ys-burst-node-3","yscale-lifecycle":"prewarm"}},"created_at":"2025-01-01T00:00:00Z"}
			]`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		},
	}

	nodes, err := b.ListPooledNodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("unscoped backend must not see warm candidates, got %+v", nodes)
	}
}

func TestListPooledNodes_FiltersNonYscale(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			body := `[
				{"id":"m-1","state":"stopped","config":{"metadata":{"yscale-node":"ys-burst-node-1"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-2","state":"stopped","config":{"metadata":{}},"created_at":"2025-01-01T00:00:00Z"}
			]`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		},
	}

	nodes, err := b.ListPooledNodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 yscale machine, got %d", len(nodes))
	}
}

func TestListOwnedNodes_PopulatesBurstIDFromExactMetadata(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			body := `[
				{"id":"m-owned","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-1","yscale-burst-id":"burst_exact"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-legacy","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-2"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-foreign","state":"started","config":{"metadata":{"yscale-burst-id":"burst_foreign"}},"created_at":"2025-01-01T00:00:00Z"}
			]`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		},
	}

	nodes, err := b.ListOwnedNodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("owned nodes = %+v, want exactly the two machines with yscale ownership metadata", nodes)
	}
	if nodes[0].BackendID != "m-owned" || nodes[0].BurstID != "burst_exact" {
		t.Fatalf("owned BurstID = %+v, want exact metadata marker", nodes[0])
	}
	if nodes[1].BackendID != "m-legacy" || nodes[1].BurstID != "" {
		t.Fatalf("legacy owned node should not infer BurstID: %+v", nodes[1])
	}
}

func TestListPooledNodes_ExcludesRunningMachines(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			body := `[
				{"id":"m-1","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-1"}},"created_at":"2025-01-01T00:00:00Z"},
				{"id":"m-2","state":"created","config":{"metadata":{"yscale-node":"ys-burst-node-2"}},"created_at":"2025-01-01T00:00:00Z"}
			]`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		},
	}

	nodes, err := b.ListPooledNodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0 pooled nodes (all running), got %d", len(nodes))
	}
}

// --- CleanupOrphans scope tests ---
//
// Every fixture machine below carries a created_at far older than
// backends.OrphanGracePeriod so it is past the create-race window: ownership,
// scope, and trackedness are then the only things deciding its fate. The gate
// itself is exercised in TestCleanupOrphans_GracePeriod.

func TestCleanupOrphans_ScopeFiltering(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.SetPoolScope("scope-a")

	var destroyedIDs []string
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			path := req.URL.Path
			if req.Method == "GET" && strings.Contains(path, "/machines") && !strings.Contains(path, "/volumes") {
				body := `[
					{"id":"m-1","created_at":"2020-01-01T00:00:00Z","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-1","yscale-scope":"scope-a"}}},
					{"id":"m-2","created_at":"2020-01-01T00:00:00Z","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-2","yscale-scope":"scope-b"}}},
					{"id":"m-3","created_at":"2020-01-01T00:00:00Z","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-3"}}}
				]`
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
			}
			if req.Method == "GET" && strings.Contains(path, "/volumes") {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("[]"))}, nil
			}
			if req.Method == "POST" && strings.Contains(path, "/stop") {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			}
			if req.Method == "DELETE" {
				for _, id := range []string{"m-1", "m-2", "m-3"} {
					if strings.Contains(path, id) {
						destroyedIDs = append(destroyedIDs, id)
					}
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			}
			return nil, fmt.Errorf("unexpected request: %s %s", req.Method, path)
		},
	}

	tracked := map[string]bool{} // none tracked → all are orphans
	count, err := b.CleanupOrphans(context.Background(), tracked)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 orphan destroyed (only scope-a), got %d", count)
	}
	if len(destroyedIDs) != 1 || destroyedIDs[0] != "m-1" {
		t.Errorf("expected only m-1 destroyed, got %v", destroyedIDs)
	}
}

func TestCleanupOrphans_UnscopedCannotCleanScoped(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	// poolScope stays empty.

	var destroyedIDs []string
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			path := req.URL.Path
			if req.Method == "GET" && strings.Contains(path, "/machines") && !strings.Contains(path, "/volumes") {
				body := `[
					{"id":"m-1","created_at":"2020-01-01T00:00:00Z","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-1","yscale-scope":"scope-a"}}},
					{"id":"m-2","created_at":"2020-01-01T00:00:00Z","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-2","yscale-scope":"scope-b"}}},
					{"id":"m-3","created_at":"2020-01-01T00:00:00Z","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-3"}}}
				]`
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
			}
			if req.Method == "GET" && strings.Contains(path, "/volumes") {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("[]"))}, nil
			}
			if req.Method == "POST" && strings.Contains(path, "/stop") {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			}
			if req.Method == "DELETE" {
				for _, id := range []string{"m-1", "m-2", "m-3"} {
					if strings.Contains(path, id) {
						destroyedIDs = append(destroyedIDs, id)
					}
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			}
			return nil, fmt.Errorf("unexpected request: %s %s", req.Method, path)
		},
	}

	tracked := map[string]bool{}
	count, err := b.CleanupOrphans(context.Background(), tracked)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 || len(destroyedIDs) != 1 || destroyedIDs[0] != "m-3" {
		t.Fatalf("unscoped backend should destroy only unscoped m-3; count=%d ids=%v", count, destroyedIDs)
	}
}

func TestCleanupOrphans_SkipsTracked(t *testing.T) {
	b := New("mock-token", "mock-org", "mock-region")
	b.SetPoolScope("scope-a")

	var destroyedIDs []string
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			path := req.URL.Path
			if req.Method == "GET" && strings.Contains(path, "/machines") && !strings.Contains(path, "/volumes") {
				body := `[
					{"id":"m-1","created_at":"2020-01-01T00:00:00Z","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-1","yscale-scope":"scope-a"}}},
					{"id":"m-2","created_at":"2020-01-01T00:00:00Z","state":"started","config":{"metadata":{"yscale-node":"ys-burst-node-2","yscale-scope":"scope-a"}}}
				]`
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
			}
			if req.Method == "GET" && strings.Contains(path, "/volumes") {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("[]"))}, nil
			}
			if req.Method == "POST" && strings.Contains(path, "/stop") {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			}
			if req.Method == "DELETE" {
				for _, id := range []string{"m-1", "m-2"} {
					if strings.Contains(path, id) {
						destroyedIDs = append(destroyedIDs, id)
					}
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			}
			return nil, fmt.Errorf("unexpected request: %s %s", req.Method, path)
		},
	}

	tracked := map[string]bool{"m-1": true}
	count, err := b.CleanupOrphans(context.Background(), tracked)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 destroyed (only m-2, m-1 tracked), got %d", count)
	}
	if len(destroyedIDs) != 1 || destroyedIDs[0] != "m-2" {
		t.Errorf("expected only m-2 destroyed, got %v", destroyedIDs)
	}
}

// The create-race gate. CreateNode writes the machine at Fly BEFORE the caller
// persists the record that tracks it, so "owned and untracked" only means
// "leaked" once the machine is older than backends.OrphanGracePeriod — younger
// than that, a sweep on a serving central would destroy a burst mid-create.
// Age is strictly an EXTRA condition: a machine yscale does not own, or one in
// another controller's scope, is untouchable at any age.
func TestCleanupOrphans_GracePeriod(t *testing.T) {
	cases := []struct {
		name      string
		createdAt string // machine created_at; "" omits the field entirely
		metadata  string
		destroy   bool
	}{
		{
			name:      "owned orphan inside the grace period survives",
			createdAt: time.Now().Add(-(backends.OrphanGracePeriod - time.Minute)).UTC().Format(time.RFC3339),
			metadata:  `"yscale-node":"ys-burst-young","yscale-scope":"scope-a"`,
			destroy:   false,
		},
		{
			name:      "owned orphan past the grace period is destroyed",
			createdAt: time.Now().Add(-(backends.OrphanGracePeriod + time.Minute)).UTC().Format(time.RFC3339),
			metadata:  `"yscale-node":"ys-burst-old","yscale-scope":"scope-a"`,
			destroy:   true,
		},
		{
			name:      "owned orphan with no reported creation time survives",
			createdAt: "",
			metadata:  `"yscale-node":"ys-burst-unknown-age","yscale-scope":"scope-a"`,
			destroy:   false,
		},
		{
			name:      "non-yscale machine survives however old",
			createdAt: "2020-01-01T00:00:00Z",
			metadata:  `"yscale-node":"someone-elses-vm","yscale-scope":"scope-a"`,
			destroy:   false,
		},
		{
			name:      "another controller's scope survives however old",
			createdAt: "2020-01-01T00:00:00Z",
			metadata:  `"yscale-node":"ys-burst-elsewhere","yscale-scope":"scope-b"`,
			destroy:   false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := New("mock-token", "mock-org", "mock-region")
			b.SetPoolScope("scope-a")

			created := ""
			if c.createdAt != "" {
				created = fmt.Sprintf(`,"created_at":%q`, c.createdAt)
			}
			body := fmt.Sprintf(`[{"id":"m-1"%s,"state":"started","config":{"metadata":{%s}}}]`,
				created, c.metadata)

			var destroyedIDs []string
			b.client.Transport = &mockTransport{
				roundTrip: func(req *http.Request) (*http.Response, error) {
					path := req.URL.Path
					if req.Method == "GET" && strings.Contains(path, "/machines") && !strings.Contains(path, "/volumes") {
						return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
					}
					if req.Method == "GET" && strings.Contains(path, "/volumes") {
						return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("[]"))}, nil
					}
					if req.Method == "POST" && strings.Contains(path, "/stop") {
						return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
					}
					if req.Method == "DELETE" {
						destroyedIDs = append(destroyedIDs, path)
						return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
					}
					return nil, fmt.Errorf("unexpected request: %s %s", req.Method, path)
				},
			}

			count, err := b.CleanupOrphans(context.Background(), map[string]bool{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := 0
			if c.destroy {
				want = 1
			}
			if count != want || len(destroyedIDs) != want {
				t.Fatalf("destroyed = %d (deletes %v), want %d", count, destroyedIDs, want)
			}
		})
	}
}

// --- NodeLifecycle type tests ---

func TestNodeLifecycle_Constants(t *testing.T) {
	if backends.LifecycleCold != "cold" {
		t.Errorf("LifecycleCold = %q, want \"cold\"", backends.LifecycleCold)
	}
	if backends.LifecyclePrewarm != "prewarm" {
		t.Errorf("LifecyclePrewarm = %q, want \"prewarm\"", backends.LifecyclePrewarm)
	}
	// Zero value should NOT equal LifecyclePrewarm (safety: default is cold).
	var zero backends.NodeLifecycle
	if zero == backends.LifecyclePrewarm {
		t.Error("zero-value NodeLifecycle must not equal LifecyclePrewarm")
	}
}
