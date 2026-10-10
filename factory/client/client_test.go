package client_test

import (
	"context"
	"testing"

	"github.com/yscale-sh/yscale/factory/client"
)

// meshBox is the shape central's mesh-policy reconciler holds a *client.Headscale
// by: the policy PUT plus the node-level route approval the policy's
// autoApprovers do not cover. It is restated here rather than imported so the
// check runs from THIS side of the re-export — a method that stops coming
// through the alias should fail in the package that publishes it, not somewhere
// downstream in central.
type meshBox interface {
	EnsurePolicy(ctx context.Context, tagOwners, routeApprovers map[string][]string, extraACLs []client.PolicyACL) error
	FindDeviceByHostname(ctx context.Context, hostname string) (string, error)
	ApproveNodeRoutes(ctx context.Context, nodeID string, routes []string) error
}

var _ meshBox = (*client.Headscale)(nil)

func TestNewHeadscaleReturnsTheReExportedClient(t *testing.T) {
	h := client.NewHeadscale("https://hs.example.com/", "k", "u")
	if h == nil {
		t.Fatalf("NewHeadscale returned nil")
	}
	if got := h.LoginServer(); got != "https://hs.example.com" {
		t.Errorf("LoginServer = %q, want https://hs.example.com", got)
	}
}
