package linode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/pkg/backends"
)

type mockTransport struct {
	roundTrip func(*http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

func TestDeleteNode_Idempotent404(t *testing.T) {
	b := New("mock-token", "us-ord", "")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			if req.Method == "DELETE" && strings.HasSuffix(req.URL.Path, "/linode/instances/123") {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader(`{"errors":[{"reason": "Not found"}]}`)),
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
	b := New("mock-token", "us-ord", "")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			if req.Method == "DELETE" && strings.HasSuffix(req.URL.Path, "/linode/instances/123") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			}
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
			b := New("mock-token", "us-ord", "")
			b.client.Transport = &mockTransport{
				roundTrip: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: tt.statusCode,
						Body:       io.NopCloser(strings.NewReader(tt.respBody)),
					}, nil
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
	b := New("mock-token", "us-ord", "")
	expectedErr := errors.New("network down")
	b.client.Transport = &mockTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			return nil, expectedErr
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

func TestCreateNodeTwoGPUsRetriesNextRegionAfterCapacity403(t *testing.T) {
	b := New("mock-token", "us-ord", "")
	b.cpuImage, b.gpuImage = "", "private/gpu-test"
	b.burstFWID = 1
	b.metaRegions = map[string]bool{"us-ord": true, "us-sea": true}
	b.metaExpiry = time.Now().Add(time.Hour)
	var createRegions []string
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == "GET" && req.URL.EscapedPath() == "/v4/images/private%2Fgpu-test":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":"private/gpu-test","status":"available"}`))}, nil
		case req.Method == "GET" && req.URL.Path == "/v4/regions/availability":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"region":"us-ord","available":true},{"region":"us-sea","available":true}]}`))}, nil
		case req.Method == "POST" && req.URL.Path == "/v4/linode/instances":
			body, _ := io.ReadAll(req.Body)
			if !strings.Contains(string(body), `"type":"g2-gpu-rtx4000a2-s"`) {
				t.Fatalf("create body = %s, want exact two-GPU plan", body)
			}
			if strings.Contains(string(body), `"region":"us-ord"`) {
				createRegions = append(createRegions, "us-ord")
				return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(`{"errors":[{"reason":"The Linode plan you chose is not currently available in the selected region"}]}`))}, nil
			}
			if strings.Contains(string(body), `"region":"us-sea"`) {
				createRegions = append(createRegions, "us-sea")
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":42}`))}, nil
			}
		case req.Method == "GET" && req.URL.Path == "/v4/linode/instances/42/configs":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"id":7}]}`))}, nil
		case req.Method == "PUT" && req.URL.Path == "/v4/linode/instances/42/configs/7":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		case req.Method == "POST" && req.URL.Path == "/v4/linode/instances/42/boot":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		}
		return nil, errors.New("unexpected request")
	}}

	id, err := b.CreateNode(context.Background(), &backends.NodeSpec{
		Name: "ys-burst-test", BurstID: "burst_test",
		Resources: backends.ResourceRequirements{GPU: &backends.GPUSpec{Kind: "rtx4000ada", Count: 2}},
	})
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	if id != "42" {
		t.Fatalf("CreateNode id = %q, want 42", id)
	}
	if got := strings.Join(createRegions, ","); got != "us-ord,us-sea" {
		t.Fatalf("create regions = %q, want us-ord,us-sea", got)
	}
}

func TestCreateNodePinnedRegionNoCapacityDoesNotCreate(t *testing.T) {
	b := New("mock-token", "us-ord", "")
	b.cpuImage, b.gpuImage = "", ""
	b.burstFWID = 1
	b.metaRegions = map[string]bool{"us-sea": true}
	b.metaExpiry = time.Now().Add(time.Hour)
	creates := 0
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/v4/regions/availability":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"region":"us-sea","available":false},{"region":"us-ord","available":true}]}`))}, nil
		case req.Method == "POST" && req.URL.Path == "/v4/linode/instances":
			creates++
			return nil, errors.New("instance create must not be reached")
		}
		return nil, errors.New("unexpected request")
	}}

	_, err := b.CreateNode(context.Background(), &backends.NodeSpec{Name: "ys-burst-test", BurstID: "burst_test", Region: "us-sea"})
	if err == nil || !strings.Contains(err.Error(), `pinned region "us-sea"`) {
		t.Fatalf("CreateNode error = %v, want pinned-region capacity error", err)
	}
	if creates != 0 {
		t.Fatalf("instance creates = %d, want 0", creates)
	}
}

func TestCreateNodeModelVolumeRegionNoCapacityDoesNotCreate(t *testing.T) {
	b := New("mock-token", "us-ord", "")
	b.cpuImage, b.gpuImage = "", ""
	b.burstFWID = 1
	b.metaRegions = map[string]bool{"us-sea": true, "us-ord": true}
	b.metaExpiry = time.Now().Add(time.Hour)
	creates := 0
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/v4/volumes":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"id":7,"label":"model","region":"us-sea"}]}`))}, nil
		case req.Method == "GET" && req.URL.Path == "/v4/regions/availability":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"region":"us-ord","available":true},{"region":"us-sea","available":false}]}`))}, nil
		case req.Method == "POST" && req.URL.Path == "/v4/linode/instances":
			creates++
			return nil, errors.New("instance create must not be reached")
		}
		return nil, errors.New("unexpected request")
	}}

	_, err := b.CreateNode(context.Background(), &backends.NodeSpec{Name: "ys-burst-test", BurstID: "burst_test", ModelVolume: "model"})
	if err == nil || !strings.Contains(err.Error(), `pinned region "us-sea"`) {
		t.Fatalf("CreateNode error = %v, want model-volume region capacity error", err)
	}
	if creates != 0 {
		t.Fatalf("instance creates = %d, want 0", creates)
	}
}

func TestCreateNodeAllCapacity403IsSanitized(t *testing.T) {
	b := New("mock-token", "us-ord", "")
	// Pin an explicit GPU image so the preflight below is deterministic —
	// the test's focus is post-preflight capacity exhaustion, so image
	// lookup must succeed to reach the create loop.
	b.gpuImage = "private/gpu-test"
	b.burstFWID = 1
	b.metaRegions = map[string]bool{"us-ord": true, "us-sea": true}
	b.metaExpiry = time.Now().Add(time.Hour)
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == "GET" && req.URL.EscapedPath() == "/v4/images/private%2Fgpu-test":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":"private/gpu-test","status":"available"}`))}, nil
		case req.Method == "GET" && req.URL.Path == "/v4/regions/availability":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"region":"us-ord","available":true},{"region":"us-sea","available":true}]}`))}, nil
		case req.Method == "POST" && req.URL.Path == "/v4/linode/instances":
			return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(`{"errors":[{"reason":"The Linode plan you chose is not currently available in the selected region"}]}`))}, nil
		}
		return nil, errors.New("unexpected request")
	}}

	_, err := b.CreateNode(context.Background(), &backends.NodeSpec{
		Name: "ys-burst-test", BurstID: "burst_test",
		Resources: backends.ResourceRequirements{GPU: &backends.GPUSpec{Kind: "rtx4000ada", Count: 2}},
	})
	if err == nil {
		t.Fatal("CreateNode succeeded, want exhausted capacity error")
	}
	for _, want := range []string{"rtx4000ada x2 has no capacity right now across [us-ord us-sea]"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "The Linode plan you chose") {
		t.Fatalf("customer error leaked upstream response: %v", err)
	}
}

func TestMapGPUTypeSelectsSmallestTierThatFitsResources(t *testing.T) {
	tests := []struct {
		name string
		gpu  backends.GPUSpec
		want string
	}{
		{name: "unspecified defaults small", gpu: backends.GPUSpec{Kind: "rtx4000ada", Count: 1}, want: "g2-gpu-rtx4000a1-s"},
		{name: "six CPUs selects medium", gpu: backends.GPUSpec{Kind: "rtx4000ada", Count: 1, CPUMillis: 6000}, want: "g2-gpu-rtx4000a1-m"},
		{name: "memory selects large", gpu: backends.GPUSpec{Kind: "rtx4000ada", Count: 1, MemoryMB: 40000}, want: "g2-gpu-rtx4000a1-l"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MapGPUType(&tt.gpu)
			if err != nil || got != tt.want {
				t.Fatalf("MapGPUType = %q, %v; want %q, nil", got, err, tt.want)
			}
		})
	}
}

func TestMapGPUTypeRejectsOversizedRTX6000CPU(t *testing.T) {
	_, err := MapGPUType(&backends.GPUSpec{Kind: "rtx6000", Count: 1, CPUMillis: 9000})
	if err == nil || !strings.Contains(err.Error(), "rtx6000 x1 has no plan with at least 9000m CPU") {
		t.Fatalf("MapGPUType error = %v, want oversized CPU rejection", err)
	}
}

func TestMapGPUTypeHeadroomBumpsToNextTier(t *testing.T) {
	// An 8000m/32768Mi request exactly matches the rtx4000ada medium plan's raw
	// capacity. With system headroom (250m/1024Mi), the effective need is
	// 8250m/33792Mi which exceeds medium — the mapper must select large.
	got, err := MapGPUType(&backends.GPUSpec{Kind: "rtx4000ada", Count: 1, CPUMillis: 8000, MemoryMB: 32768})
	if err != nil || got != "g2-gpu-rtx4000a1-l" {
		t.Fatalf("MapGPUType = %q, %v; want g2-gpu-rtx4000a1-l, nil", got, err)
	}
}

func TestMapGPUTypeHeadroomRejectsFixedPlanRTX6000(t *testing.T) {
	// An rtx6000 x1 plan has exactly 8000m/32768Mi. A workload requesting
	// exactly that cannot be scheduled because headroom pushes the effective
	// need to 8250m/33792Mi.
	_, err := MapGPUType(&backends.GPUSpec{Kind: "rtx6000", Count: 1, CPUMillis: 8000, MemoryMB: 32768})
	if err == nil {
		t.Fatal("MapGPUType must reject an exact-capacity rtx6000 request; headroom leaves no room")
	}
	if !strings.Contains(err.Error(), "system headroom") {
		t.Fatalf("rejection message = %q, want it to mention system headroom", err)
	}
}

func TestMapGPUTypeHeadroomFitsWithinPlan(t *testing.T) {
	// 7750m + 250m = 8000m and 31744Mi + 1024Mi = 32768Mi: fits medium exactly.
	got, err := MapGPUType(&backends.GPUSpec{Kind: "rtx4000ada", Count: 1, CPUMillis: 7750, MemoryMB: 31744})
	if err != nil || got != "g2-gpu-rtx4000a1-m" {
		t.Fatalf("MapGPUType = %q, %v; want g2-gpu-rtx4000a1-m, nil", got, err)
	}
}

func TestResolveRegionPrefersConfiguredRegionWhenUnpinned(t *testing.T) {
	b := New("mock-token", "us-sea", "")
	b.metaRegions = map[string]bool{"us-ord": true, "us-sea": true}
	b.metaExpiry = time.Now().Add(time.Hour)
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		if req.Method == "GET" && req.URL.Path == "/v4/regions/availability" {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"region":"us-ord","available":true},{"region":"us-sea","available":true}]}`))}, nil
		}
		return nil, errors.New("unexpected request")
	}}

	regions, err := b.resolveRegion(context.Background(), "g6-standard-1", "")
	if err != nil {
		t.Fatalf("resolveRegion: %v", err)
	}
	if got := strings.Join(regions, ","); got != "us-sea,us-ord" {
		t.Fatalf("regions = %q, want us-sea,us-ord", got)
	}
}

// The create-race gate. CreateNode returns an instance that exists and bills
// at Linode BEFORE the caller persists the record that tracks it, so "tagged
// ours and untracked" only means "leaked" once the instance is older than
// backends.OrphanGracePeriod — younger than that, a sweep on a serving central
// would destroy a burst mid-create. Age is strictly an EXTRA condition: an
// instance without the exact ownerTag is untouchable at any age.
func TestCleanupOrphans_GracePeriod(t *testing.T) {
	// Linode reports timestamps without a zone, implicitly UTC (see linodeTime).
	const linodeLayout = "2006-01-02T15:04:05"

	cases := []struct {
		name    string
		created string // instance created; "" omits the field entirely
		tags    string
		destroy bool
	}{
		{
			name:    "owned orphan inside the grace period survives",
			created: time.Now().UTC().Add(-(backends.OrphanGracePeriod - time.Minute)).Format(linodeLayout),
			tags:    `["` + ownerTag + `"]`,
			destroy: false,
		},
		{
			name:    "owned orphan past the grace period is destroyed",
			created: time.Now().UTC().Add(-(backends.OrphanGracePeriod + time.Minute)).Format(linodeLayout),
			tags:    `["` + ownerTag + `"]`,
			destroy: true,
		},
		{
			name:    "owned orphan with no reported creation time survives",
			created: "",
			tags:    `["` + ownerTag + `"]`,
			destroy: false,
		},
		{
			name:    "untagged instance survives however old",
			created: "2020-01-01T00:00:00",
			tags:    `[]`,
			destroy: false,
		},
		{
			name:    "someone else's tag survives however old",
			created: "2020-01-01T00:00:00",
			tags:    `["not-yscale"]`,
			destroy: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			created := ""
			if c.created != "" {
				created = fmt.Sprintf(`,"created":%q`, c.created)
			}
			body := fmt.Sprintf(`{"data":[{"id":42,"label":"ys-burst-x","status":"running","tags":%s%s}]}`,
				c.tags, created)

			b := New("mock-token", "us-ord", "")
			var deleted []string
			b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
				if req.Method == "GET" && req.URL.Path == "/v4/linode/instances" {
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
				}
				if req.Method == "DELETE" {
					deleted = append(deleted, req.URL.Path)
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
				}
				return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
			}}

			count, err := b.CleanupOrphans(context.Background(), map[string]bool{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := 0
			if c.destroy {
				want = 1
			}
			if count != want || len(deleted) != want {
				t.Fatalf("destroyed = %d (deletes %v), want %d", count, deleted, want)
			}
		})
	}
}

// The image preflight is the create-flow's cheapest guardrail: verify that an
// account-scoped burst image (private/... or shared/...) exists and is
// deployable BEFORE spinning firewall/region work or issuing any paid
// mutation. These tests fix the reachability rules — including that a stored
// replica in a different region must not block deployment (Akamai second-gen
// custom images deploy to any compatible region; replicas only speed up
// placement) — and that failure modes never leak provider response bodies.

// A private image whose sole stored replica lives outside the target region
// must still deploy: replicas are a placement-speed hint, not a regional
// eligibility gate (issue #16 evidence).
func TestCreateNodeAvailableImageWithRemoteReplicaProceeds(t *testing.T) {
	b := New("mock-token", "us-ord", "")
	// Configure a specific baked CPU image so the preflight runs and the
	// test controls exactly which image endpoint is hit.
	b.cpuImage = "private/38851039"
	b.gpuImage = ""
	b.burstFWID = 1
	b.metaRegions = map[string]bool{"us-sea": true}
	b.metaExpiry = time.Now().Add(time.Hour)
	var imageLookups int
	var createRegions []string
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == "GET" && req.URL.EscapedPath() == "/v4/images/private%2F38851039":
			imageLookups++
			// regions[] lists only us-ord even though the create target is
			// us-sea. That must not stop the burst.
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":"private/38851039","status":"available","regions":[{"region":"us-ord","status":"available"}]}`))}, nil
		case req.Method == "GET" && req.URL.Path == "/v4/regions/availability":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"region":"us-sea","available":true}]}`))}, nil
		case req.Method == "POST" && req.URL.Path == "/v4/linode/instances":
			body, _ := io.ReadAll(req.Body)
			if strings.Contains(string(body), `"region":"us-sea"`) {
				createRegions = append(createRegions, "us-sea")
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":42}`))}, nil
			}
		}
		return nil, fmt.Errorf("unexpected request: %s %s (escaped=%s)", req.Method, req.URL.Path, req.URL.EscapedPath())
	}}

	id, err := b.CreateNode(context.Background(), &backends.NodeSpec{Name: "ys-burst-test", BurstID: "burst_test"})
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	if id != "42" {
		t.Fatalf("CreateNode id = %q, want 42", id)
	}
	if imageLookups != 1 {
		t.Fatalf("image lookups = %d, want exactly 1 preflight", imageLookups)
	}
	if got := strings.Join(createRegions, ","); got != "us-sea" {
		t.Fatalf("create regions = %q, want us-sea (remote-only replica must not filter)", got)
	}
}

func TestCreateNodeImagePreflightFailureStopsBeforeMutations(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		want       string
	}{
		{name: "pending", statusCode: http.StatusOK, body: `{"id":"private/38851039","status":"pending_upload"}`, want: "not ready yet"},
		{name: "missing", statusCode: http.StatusNotFound, body: `{"errors":[{"reason":"secret-provider-body"}]}`, want: "not found on this account"},
		{name: "inaccessible", statusCode: http.StatusForbidden, body: `{"errors":[{"reason":"secret-provider-body"}]}`, want: "inaccessible with this token"},
		{name: "mismatched id", statusCode: http.StatusOK, body: `{"id":"private/999999","status":"available"}`, want: "mismatched id"},
		{name: "malformed", statusCode: http.StatusOK, body: `not-json-secret-provider-body`, want: "lookup unavailable"},
		{name: "unknown status", statusCode: http.StatusOK, body: `{"id":"private/38851039","status":"secret-provider-body"}`, want: "not deployable"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := New("mock-token", "us-ord", "")
			b.cpuImage, b.gpuImage = "private/38851039", ""
			var posts, firewallLists int
			b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
				switch {
				case req.Method == "POST":
					posts++
					return nil, errors.New("provider mutation must not run")
				case req.Method == "GET" && req.URL.Path == "/v4/networking/firewalls":
					firewallLists++
					return nil, errors.New("firewall lookup must not run")
				case req.Method == "GET" && req.URL.EscapedPath() == "/v4/images/private%2F38851039":
					return &http.Response{StatusCode: tc.statusCode, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
				default:
					return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
				}
			}}

			_, err := b.CreateNode(context.Background(), &backends.NodeSpec{Name: "ys-burst-test", BurstID: "burst_test"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CreateNode error = %v, want %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "secret-provider-body") {
				t.Fatalf("preflight error leaked provider payload: %v", err)
			}
			if posts != 0 || firewallLists != 0 {
				t.Fatalf("preflight failure reached provider setup: posts=%d firewall_lists=%d", posts, firewallLists)
			}
		})
	}
}

// The public linode/... path is common and always addressable. The preflight
// is a no-op there: it must not issue a GET /images/... on any linode/... ref
// (that would waste an RTT per burst and hit rate-limits for high-fanout
// launches).
func TestCreateNodePublicImageSkipsPreflight(t *testing.T) {
	b := New("mock-token", "us-ord", "")
	b.cpuImage, b.gpuImage = "", "" // stock Debian path
	b.burstFWID = 1
	b.metaRegions = map[string]bool{"us-ord": true}
	b.metaExpiry = time.Now().Add(time.Hour)
	var imageLookups int
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == "GET" && strings.HasPrefix(req.URL.Path, "/v4/images/"):
			imageLookups++
			return nil, errors.New("public image must not trigger preflight lookup")
		case req.Method == "GET" && req.URL.Path == "/v4/regions/availability":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"region":"us-ord","available":true}]}`))}, nil
		case req.Method == "POST" && req.URL.Path == "/v4/linode/instances":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":7}`))}, nil
		}
		return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
	}}

	if _, err := b.CreateNode(context.Background(), &backends.NodeSpec{Name: "ys-burst-test", BurstID: "burst_test"}); err != nil {
		t.Fatalf("CreateNode (public image): %v", err)
	}
	if imageLookups != 0 {
		t.Fatalf("image lookups on public path = %d, want 0", imageLookups)
	}
}

func TestListOwnedNodes_PopulatesBurstIDFromExactTag(t *testing.T) {
	body := `{"data":[
		{"id":42,"label":"ys-burst-owned","status":"running","tags":["` + ownerTag + `","burst_exact"],"created":"2025-01-01T00:00:00"},
		{"id":43,"label":"ys-burst-legacy","status":"running","tags":["` + ownerTag + `"],"created":"2025-01-01T00:00:00"},
		{"id":44,"label":"ys-burst-foreign","status":"running","tags":["burst_exact"],"created":"2025-01-01T00:00:00"}
	]}`
	b := New("mock-token", "us-ord", "")
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		if req.Method != "GET" || req.URL.Path != "/v4/linode/instances" {
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
	}}

	nodes, err := b.ListOwnedNodes(context.Background())
	if err != nil {
		t.Fatalf("list owned nodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("owned nodes = %+v, want exactly owner-tagged instances", nodes)
	}
	if nodes[0].BackendID != "42" || nodes[0].BurstID != "burst_exact" {
		t.Fatalf("owned BurstID = %+v, want exact BurstID tag", nodes[0])
	}
	if nodes[1].BackendID != "43" || nodes[1].BurstID != "" {
		t.Fatalf("legacy owned node should not infer BurstID: %+v", nodes[1])
	}
}

// A pre-dispatch validation failure (image preflight, region resolution,
// model volume lookup) proves no paid create ran, so central can release
// the tenant lease and refund the hold.
func TestCreateNodeImagePreflightFailureIsProvenCleanFailure(t *testing.T) {
	b := New("mock-token", "us-ord", "")
	b.cpuImage, b.gpuImage = "private/38851039", ""
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		if req.Method == "GET" && req.URL.EscapedPath() == "/v4/images/private%2F38851039" {
			return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		}
		return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
	}}
	_, err := b.CreateNode(context.Background(), &backends.NodeSpec{Name: "ys-burst-test", BurstID: "burst_test"})
	if err == nil {
		t.Fatal("CreateNode succeeded despite missing image")
	}
	if backends.CreateOutcomeAmbiguous(err) {
		t.Fatalf("pre-dispatch validation failure must classify as proven zero, got ambiguous: %v", err)
	}
}

// A modeled 4xx from the create POST that isn't the availability 403 proves
// the provider refused. Anything 5xx or transport-level stays ambiguous.
func TestCreateNodePOSTErrorClassification(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		transport  error
		wantAmb    bool
	}{
		{name: "400 refused", statusCode: http.StatusBadRequest, wantAmb: false},
		{name: "409 may already exist", statusCode: http.StatusConflict, wantAmb: true},
		{name: "429 acceptance uncertain", statusCode: http.StatusTooManyRequests, wantAmb: true},
		{name: "500 server", statusCode: http.StatusInternalServerError, wantAmb: true},
		{name: "transport", transport: errors.New("connection reset"), wantAmb: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := New("mock-token", "us-ord", "")
			b.cpuImage, b.gpuImage = "", ""
			b.burstFWID = 1
			b.metaRegions = map[string]bool{"us-ord": true}
			b.metaExpiry = time.Now().Add(time.Hour)
			b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
				switch {
				case req.Method == "GET" && req.URL.Path == "/v4/regions/availability":
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"region":"us-ord","available":true}]}`))}, nil
				case req.Method == "POST" && req.URL.Path == "/v4/linode/instances":
					if tc.transport != nil {
						return nil, tc.transport
					}
					return &http.Response{StatusCode: tc.statusCode, Body: io.NopCloser(strings.NewReader(`{"errors":[{"reason":"nope"}]}`))}, nil
				}
				return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
			}}

			_, err := b.CreateNode(context.Background(), &backends.NodeSpec{Name: "ys-burst-test", BurstID: "burst_test"})
			if err == nil {
				t.Fatal("CreateNode succeeded despite a failed POST")
			}
			if got := backends.CreateOutcomeAmbiguous(err); got != tc.wantAmb {
				t.Fatalf("CreateOutcomeAmbiguous = %v, want %v (err=%v)", got, tc.wantAmb, err)
			}
		})
	}
}

// Post-create setup (kernel switch, volume attach) runs against a machine
// that already exists. Best-effort DELETE cleanup does not prove absence —
// the DELETE may fail or be lost — so the outcome must stay ambiguous.
func TestCreateNodeGPUPostCreateFailureStaysAmbiguous(t *testing.T) {
	b := New("mock-token", "us-ord", "")
	b.cpuImage, b.gpuImage = "", "private/gpu-test"
	b.burstFWID = 1
	b.metaRegions = map[string]bool{"us-ord": true}
	b.metaExpiry = time.Now().Add(time.Hour)
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == "GET" && req.URL.EscapedPath() == "/v4/images/private%2Fgpu-test":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":"private/gpu-test","status":"available"}`))}, nil
		case req.Method == "GET" && req.URL.Path == "/v4/regions/availability":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[{"region":"us-ord","available":true}]}`))}, nil
		case req.Method == "POST" && req.URL.Path == "/v4/linode/instances":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":42}`))}, nil
		case req.Method == "GET" && strings.HasPrefix(req.URL.Path, "/v4/linode/instances/42/configs"):
			// Force the boot-config wait to fail so bootDistroKernel returns
			// an error and cleanupFailedCreate runs.
			return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		case req.Method == "DELETE" && strings.HasSuffix(req.URL.Path, "/linode/instances/42"):
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		}
		return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
	}}
	_, err := b.CreateNode(context.Background(), &backends.NodeSpec{
		Name: "ys-burst-test", BurstID: "burst_test",
		Resources: backends.ResourceRequirements{GPU: &backends.GPUSpec{Kind: "rtx4000ada", Count: 1}},
	})
	if err == nil {
		t.Fatal("CreateNode succeeded despite post-create failure")
	}
	if !backends.CreateOutcomeAmbiguous(err) {
		t.Fatalf("post-create failure must stay ambiguous even after best-effort cleanup: %v", err)
	}
}
