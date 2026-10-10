package flyio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
)

func TestValidateNodeSpecRejectsUnsafeInputs(t *testing.T) {
	b := New("token", "org", "ord")
	b.SetPoolScope("scope-a")
	validCold := backends.NodeSpec{
		Name:       "ys-burst-valid",
		AgentImage: backends.AgentImage(),
		Lifecycle:  backends.LifecycleCold,
	}
	validPrewarm := validCold
	validPrewarm.Lifecycle = backends.LifecyclePrewarm
	validPrewarm.AgentImage = "registry.fly.io/yscale@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	validPrewarm.ScopeHash = "scope-a"
	validPrewarm.ConfigHash = "cfg-a"

	tests := []struct {
		name string
		spec *backends.NodeSpec
	}{
		{name: "nil spec", spec: nil},
		{name: "foreign name", spec: nodeSpecCopy(validCold, func(s *backends.NodeSpec) { s.Name = "foreign" })},
		{name: "empty identifier", spec: nodeSpecCopy(validCold, func(s *backends.NodeSpec) { s.Name = backends.NodeNamePrefix })},
		{name: "missing image", spec: nodeSpecCopy(validCold, func(s *backends.NodeSpec) { s.AgentImage = "" })},
		{name: "unknown lifecycle", spec: nodeSpecCopy(validCold, func(s *backends.NodeSpec) { s.Lifecycle = "recycled" })},
		{name: "prewarm missing hashes", spec: nodeSpecCopy(validPrewarm, func(s *backends.NodeSpec) { s.ConfigHash = "" })},
		{name: "prewarm scope mismatch", spec: nodeSpecCopy(validPrewarm, func(s *backends.NodeSpec) { s.ScopeHash = "scope-b" })},
		{name: "prewarm mutable image", spec: nodeSpecCopy(validPrewarm, func(s *backends.NodeSpec) { s.AgentImage = "registry.fly.io/yscale:latest" })},
		{name: "prewarm short digest", spec: nodeSpecCopy(validPrewarm, func(s *backends.NodeSpec) { s.AgentImage = "registry.fly.io/yscale@sha256:0123" })},
		{name: "prewarm non-hex digest", spec: nodeSpecCopy(validPrewarm, func(s *backends.NodeSpec) { s.AgentImage = "registry.fly.io/yscale@sha256:" + strings.Repeat("z", 64) })},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := b.validateNodeSpec(tc.spec); err == nil {
				t.Fatal("expected fail-closed validation error")
			}
		})
	}
	if err := b.validateNodeSpec(&validPrewarm); err != nil {
		t.Fatalf("valid digest-pinned prewarm spec rejected: %v", err)
	}
}

func TestCreateNodeRejectsScopeMismatchBeforeProvider(t *testing.T) {
	tests := []struct {
		name         string
		backendScope string
		specScope    string
	}{
		{name: "scoped backend rejects unscoped spec", backendScope: "scope-a", specScope: ""},
		{name: "unscoped backend rejects scoped spec", backendScope: "", specScope: "scope-a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := New("token", "org", "ord")
			b.SetPoolScope(tc.backendScope)
			requests := 0
			b.client.Transport = &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
				requests++
				return response(http.StatusOK, `{}`), nil
			}}
			_, err := b.CreateNode(context.Background(), &backends.NodeSpec{
				Name:       "ys-burst-scope-test",
				AgentImage: backends.AgentImage(),
				Lifecycle:  backends.LifecycleCold,
				ScopeHash:  tc.specScope,
			})
			if err == nil {
				t.Fatal("scope mismatch should fail closed")
			}
			if requests != 0 {
				t.Fatalf("provider requests = %d, want zero", requests)
			}
			if backends.CreateOutcomeAmbiguous(err) {
				t.Fatalf("pre-dispatch scope validation must prove zero resource: %v", err)
			}
		})
	}
}

func TestCreateNodeOutcomeClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		response  *http.Response
		transport error
		wantAmb   bool
	}{
		{name: "provider 400", response: response(http.StatusBadRequest, `{"error":"bad request"}`), wantAmb: false},
		{name: "provider 409", response: response(http.StatusConflict, `{"error":"already exists"}`), wantAmb: true},
		{name: "provider 500", response: response(http.StatusInternalServerError, `{"error":"unavailable"}`), wantAmb: true},
		{name: "transport", transport: errors.New("connection reset"), wantAmb: true},
		{name: "accepted without id", response: response(http.StatusOK, `{}`), wantAmb: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := New("token", "org", "ord")
			b.appReady = true
			b.client.Transport = &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
				return tc.response, tc.transport
			}}
			_, err := b.CreateNode(context.Background(), &backends.NodeSpec{
				Name: "ys-burst-outcome", BurstID: "burst_outcome", AgentImage: backends.AgentImage(),
			})
			if err == nil {
				t.Fatal("CreateNode succeeded without a usable machine")
			}
			if got := backends.CreateOutcomeAmbiguous(err); got != tc.wantAmb {
				t.Fatalf("CreateOutcomeAmbiguous = %v, want %v: %v", got, tc.wantAmb, err)
			}
		})
	}
}

func nodeSpecCopy(base backends.NodeSpec, mutate func(*backends.NodeSpec)) *backends.NodeSpec {
	copy := base
	mutate(&copy)
	return &copy
}

func TestEnsureAppRetriesTransientFailure(t *testing.T) {
	b := New("token", "org", "ord")
	requests := 0
	b.client.Transport = &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			return nil, errors.New("temporary network failure")
		}
		return response(http.StatusOK, `{}`), nil
	}}

	if err := b.ensureApp(context.Background()); err == nil {
		t.Fatal("first ensureApp should preserve the transient error")
	}
	if err := b.ensureApp(context.Background()); err != nil {
		t.Fatalf("second ensureApp should retry and recover: %v", err)
	}
	if err := b.ensureApp(context.Background()); err != nil {
		t.Fatalf("cached successful ensureApp failed: %v", err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want one failure + one successful retry", requests)
	}
}

func TestEnsureAppOnlyCreatesOnNotFound(t *testing.T) {
	t.Run("unauthorized does not create", func(t *testing.T) {
		b := New("token", "org", "ord")
		posts := 0
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost {
				posts++
			}
			return response(http.StatusUnauthorized, `denied`), nil
		}}
		if err := b.ensureApp(context.Background()); err == nil || !strings.Contains(err.Error(), "status 401") {
			t.Fatalf("error = %v, want HTTP 401", err)
		}
		if posts != 0 {
			t.Fatalf("POST count = %d, want zero", posts)
		}
	})

	t.Run("not found creates once", func(t *testing.T) {
		b := New("token", "org", "ord")
		gets, posts := 0, 0
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			switch req.Method {
			case http.MethodGet:
				gets++
				return response(http.StatusNotFound, `{}`), nil
			case http.MethodPost:
				posts++
				return response(http.StatusCreated, `{}`), nil
			default:
				return nil, fmt.Errorf("unexpected method %s", req.Method)
			}
		}}
		if err := b.ensureApp(context.Background()); err != nil {
			t.Fatalf("ensureApp: %v", err)
		}
		if err := b.ensureApp(context.Background()); err != nil {
			t.Fatalf("cached ensureApp: %v", err)
		}
		if gets != 1 || posts != 1 {
			t.Fatalf("GET/POST = %d/%d, want 1/1", gets, posts)
		}
	})
}

func TestStopNodeVerifiesOwnership(t *testing.T) {
	t.Run("owned machine accepts async stop", func(t *testing.T) {
		b := New("token", "org", "ord")
		stops := 0
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodGet {
				return ownedMachineResponse("started", ""), nil
			}
			stops++
			return response(http.StatusAccepted, `{}`), nil
		}}
		if err := b.StopNode(context.Background(), "m-1"); err != nil {
			t.Fatalf("StopNode: %v", err)
		}
		if stops != 1 {
			t.Fatalf("stop requests = %d, want 1", stops)
		}
	})

	t.Run("foreign machine is never mutated", func(t *testing.T) {
		b := New("token", "org", "ord")
		mutations := 0
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodGet {
				return response(http.StatusOK, `{"id":"m-1","config":{"metadata":{"yscale-node":"foreign"}}}`), nil
			}
			mutations++
			return response(http.StatusOK, `{}`), nil
		}}
		if err := b.StopNode(context.Background(), "m-1"); err == nil {
			t.Fatal("foreign machine stop should fail closed")
		}
		if mutations != 0 {
			t.Fatalf("foreign mutations = %d, want 0", mutations)
		}
	})
}

func TestDeleteNodeNeverDeletesVolumeAfterMachineFailure(t *testing.T) {
	b := New("token", "org", "ord")
	volumeDeletes := 0
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet {
			body := `{"id":"m-1","config":{"metadata":{"yscale-node":"ys-burst-owned","yscale-volume":"vol-1"}}}`
			return response(http.StatusOK, body), nil
		}
		if strings.Contains(req.URL.Path, "/volumes/") {
			volumeDeletes++
			return response(http.StatusOK, `{}`), nil
		}
		return response(http.StatusInternalServerError, `provider failed`), nil
	}}

	err := b.DeleteNode(context.Background(), "m-1")
	if err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("DeleteNode error = %v, want provider 500", err)
	}
	if volumeDeletes != 0 {
		t.Fatalf("volume deletes = %d, want zero after machine failure", volumeDeletes)
	}
}

func TestDeleteNodeWaitsForAsyncDestroyBeforeVolume(t *testing.T) {
	b := New("token", "org", "ord")
	var sequence []string
	b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/machines/m-1"):
			sequence = append(sequence, "ownership")
			body := `{"id":"m-1","config":{"metadata":{"yscale-node":"ys-burst-owned","yscale-volume":"vol-1"}}}`
			return response(http.StatusOK, body), nil
		case req.Method == http.MethodDelete && strings.Contains(req.URL.Path, "/machines/"):
			sequence = append(sequence, "destroy-accepted")
			return response(http.StatusAccepted, `{}`), nil
		case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/wait"):
			sequence = append(sequence, "destroy-confirmed")
			if req.URL.Query().Get("state") != string(backends.ProviderStateDestroyed) {
				t.Fatalf("wait state = %q, want destroyed", req.URL.Query().Get("state"))
			}
			return response(http.StatusOK, `{"ok":true}`), nil
		case req.Method == http.MethodDelete && strings.Contains(req.URL.Path, "/volumes/"):
			sequence = append(sequence, "volume-deleted")
			return response(http.StatusOK, `{}`), nil
		default:
			return nil, fmt.Errorf("unexpected request %s %s", req.Method, req.URL)
		}
	}}

	if err := b.DeleteNode(context.Background(), "m-1"); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	want := []string{"ownership", "destroy-accepted", "destroy-confirmed", "volume-deleted"}
	if fmt.Sprint(sequence) != fmt.Sprint(want) {
		t.Fatalf("sequence = %v, want %v", sequence, want)
	}
}

func TestDeleteNodeAsyncDestroyTerminalOutcomes(t *testing.T) {
	tests := []struct {
		name             string
		waitStatus       int
		wantErr          bool
		wantVolumeDelete bool
	}{
		{name: "wait 404 means terminally absent", waitStatus: http.StatusNotFound, wantVolumeDelete: true},
		{name: "wait timeout preserves volume", waitStatus: http.StatusRequestTimeout, wantErr: true},
		{name: "wait provider failure preserves volume", waitStatus: http.StatusInternalServerError, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := New("token", "org", "ord")
			volumeDeletes := 0
			b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
				switch {
				case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/machines/m-1"):
					body := `{"id":"m-1","config":{"metadata":{"yscale-node":"ys-burst-owned","yscale-volume":"vol-1"}}}`
					return response(http.StatusOK, body), nil
				case req.Method == http.MethodDelete && strings.Contains(req.URL.Path, "/machines/"):
					return response(http.StatusAccepted, `{}`), nil
				case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/wait"):
					return response(tc.waitStatus, `wait result`), nil
				case req.Method == http.MethodDelete && strings.Contains(req.URL.Path, "/volumes/"):
					volumeDeletes++
					return response(http.StatusOK, `{}`), nil
				default:
					return nil, fmt.Errorf("unexpected request %s %s", req.Method, req.URL)
				}
			}}

			err := b.DeleteNode(context.Background(), "m-1")
			if (err != nil) != tc.wantErr {
				t.Fatalf("DeleteNode error = %v, wantErr=%t", err, tc.wantErr)
			}
			wantDeletes := 0
			if tc.wantVolumeDelete {
				wantDeletes = 1
			}
			if volumeDeletes != wantDeletes {
				t.Fatalf("volume deletes = %d, want %d", volumeDeletes, wantDeletes)
			}
		})
	}
}

func TestWaitNodeState(t *testing.T) {
	t.Run("confirms exact started state", func(t *testing.T) {
		b := New("token", "org", "ord")
		waits := 0
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/wait") {
				waits++
				if req.URL.Query().Get("state") != string(backends.ProviderStateStarted) {
					t.Fatalf("wait state = %q", req.URL.Query().Get("state"))
				}
				if req.URL.Query().Get("timeout") != "25" {
					t.Fatalf("wait timeout = %q, want 25", req.URL.Query().Get("timeout"))
				}
				return response(http.StatusOK, `{"ok":true}`), nil
			}
			return ownedMachineResponse("starting", ""), nil
		}}
		if err := b.WaitNodeState(context.Background(), "m-1", backends.ProviderStateStarted); err != nil {
			t.Fatalf("WaitNodeState: %v", err)
		}
		if waits != 1 {
			t.Fatalf("wait calls = %d, want 1", waits)
		}
	})

	t.Run("stopped wait carries provider instance ID", func(t *testing.T) {
		b := New("token", "org", "ord")
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/wait") {
				if got := req.URL.Query().Get("state"); got != string(backends.ProviderStateStopped) {
					t.Fatalf("wait state = %q", got)
				}
				if got := req.URL.Query().Get("instance_id"); got != "instance-123" {
					t.Fatalf("instance_id = %q, want instance-123", got)
				}
				return response(http.StatusOK, `{"ok":true}`), nil
			}
			body := `{"id":"m-1","state":"stopping","instance_id":"instance-123","config":{"metadata":{"yscale-node":"ys-burst-owned"}}}`
			return response(http.StatusOK, body), nil
		}}
		if err := b.WaitNodeState(context.Background(), "m-1", backends.ProviderStateStopped); err != nil {
			t.Fatalf("WaitNodeState stopped: %v", err)
		}
	})

	t.Run("destroyed is idempotent before or during wait", func(t *testing.T) {
		t.Run("already absent", func(t *testing.T) {
			b := New("token", "org", "ord")
			b.client.Transport = &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
				return response(http.StatusNotFound, `absent`), nil
			}}
			if err := b.WaitNodeState(context.Background(), "m-1", backends.ProviderStateDestroyed); err != nil {
				t.Fatalf("already absent destroyed wait: %v", err)
			}
		})

		t.Run("disappears after ownership check", func(t *testing.T) {
			b := New("token", "org", "ord")
			b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
				if strings.HasSuffix(req.URL.Path, "/wait") {
					return response(http.StatusNotFound, `absent`), nil
				}
				return ownedMachineResponse("destroying", ""), nil
			}}
			if err := b.WaitNodeState(context.Background(), "m-1", backends.ProviderStateDestroyed); err != nil {
				t.Fatalf("disappeared destroyed wait: %v", err)
			}
		})
	})

	t.Run("foreign scope never reaches wait endpoint", func(t *testing.T) {
		b := New("token", "org", "ord")
		b.SetPoolScope("scope-a")
		waits := 0
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/wait") {
				waits++
			}
			return ownedMachineResponse("starting", "scope-b"), nil
		}}
		if err := b.WaitNodeState(context.Background(), "m-1", backends.ProviderStateStarted); err == nil {
			t.Fatal("foreign scope should fail ownership")
		}
		if waits != 0 {
			t.Fatalf("wait calls = %d, want zero", waits)
		}
	})

	t.Run("provider timeout stays typed", func(t *testing.T) {
		b := New("token", "org", "ord")
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/wait") {
				return response(http.StatusRequestTimeout, `timed out`), nil
			}
			return ownedMachineResponse("starting", ""), nil
		}}
		err := b.WaitNodeState(context.Background(), "m-1", backends.ProviderStateStarted)
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusRequestTimeout {
			t.Fatalf("error = %T %v, want typed HTTP 408", err, err)
		}
	})

	t.Run("provider 5xx stays typed", func(t *testing.T) {
		b := New("token", "org", "ord")
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/wait") {
				return response(http.StatusServiceUnavailable, `unavailable`), nil
			}
			return ownedMachineResponse("starting", ""), nil
		}}
		err := b.WaitNodeState(context.Background(), "m-1", backends.ProviderStateStarted)
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("error = %T %v, want typed HTTP 503", err, err)
		}
	})

	t.Run("accepted is not terminal confirmation", func(t *testing.T) {
		b := New("token", "org", "ord")
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/wait") {
				return response(http.StatusAccepted, `still waiting`), nil
			}
			return ownedMachineResponse("starting", ""), nil
		}}
		err := b.WaitNodeState(context.Background(), "m-1", backends.ProviderStateStarted)
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusAccepted {
			t.Fatalf("error = %T %v, want typed HTTP 202", err, err)
		}
	})

	t.Run("cancelled context stops before provider mutation", func(t *testing.T) {
		b := New("token", "org", "ord")
		b.client.Transport = &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			return nil, req.Context().Err()
		}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := b.WaitNodeState(ctx, "m-1", backends.ProviderStateStarted)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	})

	t.Run("rejects unsupported target", func(t *testing.T) {
		b := New("token", "org", "ord")
		if err := b.WaitNodeState(context.Background(), "m-1", "migrated"); err == nil {
			t.Fatal("unsupported provider state should fail before HTTP")
		}
	})
}

func TestGetNodeStatusMapsEveryProviderState(t *testing.T) {
	tests := map[string]backends.NodePhase{
		"started":    backends.NodeRunning,
		"created":    backends.NodeStarting,
		"starting":   backends.NodeStarting,
		"stopped":    backends.NodeStopped,
		"suspended":  backends.NodeStopped,
		"destroyed":  backends.NodeStopped,
		"failed":     backends.NodeFailed,
		"unexpected": backends.NodeUnknown,
	}
	for state, want := range tests {
		t.Run(state, func(t *testing.T) {
			b := New("token", "org", "ord")
			b.client.Transport = &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
				body := fmt.Sprintf(`{"id":"m-1","state":%q,"private_ip":"fdaa::1","created_at":"2026-07-17T00:00:00Z","config":{}}`, state)
				return response(http.StatusOK, body), nil
			}}
			status, err := b.GetNodeStatus(context.Background(), "m-1")
			if err != nil {
				t.Fatalf("GetNodeStatus: %v", err)
			}
			if status.Phase != want || status.IP != "fdaa::1" {
				t.Fatalf("status = %+v, want phase=%s ip=fdaa::1", status, want)
			}
		})
	}
}

func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}
}

func ownedMachineResponse(state, scope string) *http.Response {
	body := fmt.Sprintf(`{"id":"m-1","state":%q,"config":{"metadata":{"yscale-node":"ys-burst-owned","yscale-scope":%q}}}`, state, scope)
	return response(http.StatusOK, body)
}
