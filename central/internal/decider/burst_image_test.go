package decider

import (
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
)

func TestBurstImageForBackendFlyOverride(t *testing.T) {
	t.Setenv("YSCALE_FLYIO_BURST_IMAGE", "registry.fly.io/example-burst:v1")
	if got := burstImageForBackend(backends.TypeFlyIO); got != "registry.fly.io/example-burst:v1" {
		t.Fatalf("fly image override ignored: %q", got)
	}
	t.Setenv("YSCALE_FLYIO_BURST_IMAGE", "  ")
	if got := burstImageForBackend(backends.TypeFlyIO); got != "registry.fly.io/yscale-burst-image:kubelet-latest" {
		t.Fatalf("fly image default changed: %q", got)
	}
}
