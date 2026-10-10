package handlers

import (
	"strings"
	"testing"
)

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
