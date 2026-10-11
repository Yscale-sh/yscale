package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/central/internal/state"
)

type authMintProvider struct {
	fakeMeshProvider
	mu        sync.Mutex
	calls     []string
	mintErr   error
	deleteErr error
}

func (p *authMintProvider) record(call string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, call)
}

func (p *authMintProvider) MintAuthKeyEphemeral(_ context.Context, _ []string, _ time.Duration, _ bool) (string, error) {
	p.record("mint")
	if p.mintErr != nil {
		return "", p.mintErr
	}
	return "tskey-auth-test", nil
}

func (p *authMintProvider) FindDeviceByHostname(_ context.Context, hostname string) (string, error) {
	p.record("find:" + hostname)
	return "existing-device", nil
}

func (p *authMintProvider) DeleteDevice(_ context.Context, deviceID string) error {
	p.record("delete:" + deviceID)
	return p.deleteErr
}

func TestMintKey_ConcurrentTenantsOnlyWinnerMints(t *testing.T) {
	const tenants = 32
	for round := 0; round < 100; round++ {
		prov := &authMintProvider{}
		h := &AgentAuth{
			Store: state.New(), Log: quietLog(),
			BoxProvider: func(string, string, string) mesh.Provider { return prov },
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		statuses := make([]int, tenants)
		customers := make([]*state.Customer, tenants)
		for i := range customers {
			customers[i] = &state.Customer{ID: fmt.Sprintf("tenant-%d", i), Mesh: &state.MeshEndpoint{}}
			h.Store.AddCustomer(customers[i])
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				statuses[i] = callMint(h, customers[i], "shared-cluster").Code
			}(i)
		}
		close(start)
		wg.Wait()
		owner, ok := h.clusterSeen.Load("shared-cluster")
		if !ok {
			t.Fatal("no tenant claimed the cluster")
		}
		for i, status := range statuses {
			want := http.StatusForbidden
			if customers[i].ID == owner {
				want = http.StatusOK
			}
			if status != want {
				t.Fatalf("round %d: tenant %s status = %d, want %d (owner %s)", round, customers[i].ID, status, want, owner)
			}
		}
		if len(prov.calls) != 3 {
			t.Fatalf("provider calls = %v, want one mint and cleanup", prov.calls)
		}
	}
}

func TestMintKey_MintBeforeDeviceCleanup(t *testing.T) {
	for _, kind := range []string{"agent", "gateway"} {
		for _, outcome := range []string{"mint failure", "success", "delete failure"} {
			t.Run(kind+"/"+outcome, func(t *testing.T) {
				prov := &authMintProvider{}
				wantStatus := http.StatusOK
				wantCalls := []string{"mint", "find:yscale-" + kind + "-cluster", "delete:existing-device"}
				if outcome == "mint failure" {
					prov.mintErr = errors.New("mint unavailable")
					wantStatus = http.StatusBadGateway
					wantCalls = []string{"mint"}
				}
				if outcome == "delete failure" {
					prov.deleteErr = errors.New("delete unavailable")
				}
				h := &AgentAuth{
					Store: state.New(), Log: quietLog(),
					BoxProvider: func(string, string, string) mesh.Provider { return prov },
				}
				cust := &state.Customer{ID: "tenant", Mesh: &state.MeshEndpoint{}}
				req := httptest.NewRequest(http.MethodPost, "/v1/agent/ts-auth-key", strings.NewReader(`{"kind":"`+kind+`"}`))
				req.Header.Set(clusterIDHeader, "cluster")
				rec := httptest.NewRecorder()
				h.MintKey(rec, req.WithContext(context.WithValue(req.Context(), ctxCustomer, cust)))
				if rec.Code != wantStatus {
					t.Fatalf("status = %d, want %d: %s", rec.Code, wantStatus, rec.Body)
				}
				if !slices.Equal(prov.calls, wantCalls) {
					t.Fatalf("provider calls = %v, want %v", prov.calls, wantCalls)
				}
			})
		}
	}
}

func TestDeviceKindSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		kind       string
		clusterID  string
		wantHost   string
		wantTag    string
		wantErrSub string // empty = expect no error
	}{
		{
			// Backcompat path: existing agent init container posts with
			// no body, handler defaults kind="agent". Must produce the
			// exact pre-2026-05-25 hostname+tag so already-deployed
			// agents keep working across the upgrade.
			name:      "agent default backcompat",
			kind:      "agent",
			clusterID: "cl-smoketest",
			wantHost:  "yscale-agent-cl-smoketest",
			wantTag:   "tag:yscale",
		},
		{
			// Gateway sidecar (cilium-gateway-sidecar.md Phase 1). Gets
			// its own hostname so the stale-device delete in MintKey
			// only matches the gateway's device, never the agent's.
			// Gets its own tag so the tailnet ACL can autoApprove
			// route advertisements without granting that authority to
			// the agent device.
			name:      "gateway",
			kind:      "gateway",
			clusterID: "cl-smoketest",
			wantHost:  "yscale-gateway-cl-smoketest",
			wantTag:   "tag:yscale-gateway",
		},
		{
			name:       "unknown kind rejected",
			kind:       "router",
			clusterID:  "cl-x",
			wantErrSub: "unknown device kind",
		},
		{
			name:       "empty kind rejected at this layer",
			kind:       "",
			clusterID:  "cl-x",
			wantErrSub: "unknown device kind",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, tag, err := deviceKindSpec(tt.kind, tt.clusterID)
			if tt.wantErrSub != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (host=%q tag=%q)", tt.wantErrSub, host, tag)
				}
				if !strings.Contains(err.Error(), tt.wantErrSub) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if host != tt.wantHost {
				t.Errorf("hostname mismatch: got %q want %q", host, tt.wantHost)
			}
			if tag != tt.wantTag {
				t.Errorf("tag mismatch: got %q want %q", tag, tt.wantTag)
			}
		})
	}
}

// TestDeviceKindSpec_DistinctHostnames ensures the agent and gateway
// hostnames in the same cluster differ — this is the property that
// makes the stale-device delete safe across kinds (the 2026-05-25
// regression where calling mint for the gateway nuked the agent
// device was caused by hostname collision).
func TestDeviceKindSpec_DistinctHostnames(t *testing.T) {
	t.Parallel()

	agentHost, _, err := deviceKindSpec("agent", "cl-x")
	if err != nil {
		t.Fatal(err)
	}
	gatewayHost, _, err := deviceKindSpec("gateway", "cl-x")
	if err != nil {
		t.Fatal(err)
	}
	if agentHost == gatewayHost {
		t.Fatalf("agent and gateway in same cluster MUST have distinct hostnames; both = %q", agentHost)
	}
}
