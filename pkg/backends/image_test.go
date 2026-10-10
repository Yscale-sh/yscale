package backends

import "testing"

// The compiled-in burst image lives in a specific Fly organization and is
// pulled credential-free precisely because the machine booting it belongs to
// that same org. Both halves of that are unavailable to anyone else, so
// YSCALE_BURST_IMAGE is the only way a different operator boots a Fly burst.
func TestAgentImage(t *testing.T) {
	t.Run("unset falls back to the compiled-in default", func(t *testing.T) {
		t.Setenv("YSCALE_BURST_IMAGE", "")
		if got := AgentImage(); got != defaultAgentImage {
			t.Errorf("AgentImage() = %q, want the default %q", got, defaultAgentImage)
		}
	})

	t.Run("override wins", func(t *testing.T) {
		t.Setenv("YSCALE_BURST_IMAGE", "ghcr.io/example/burst:v1")
		if got := AgentImage(); got != "ghcr.io/example/burst:v1" {
			t.Errorf("AgentImage() = %q, want the override", got)
		}
	})

	t.Run("whitespace is trimmed", func(t *testing.T) {
		t.Setenv("YSCALE_BURST_IMAGE", "  ghcr.io/example/burst:v1\n")
		if got := AgentImage(); got != "ghcr.io/example/burst:v1" {
			t.Errorf("AgentImage() = %q, want the trimmed override", got)
		}
	})

	// Resolved per call, not memoised at init(): a process that sets its
	// environment after start (or a test) must still see the change.
	t.Run("resolves per call rather than caching", func(t *testing.T) {
		t.Setenv("YSCALE_BURST_IMAGE", "ghcr.io/example/first:v1")
		first := AgentImage()
		t.Setenv("YSCALE_BURST_IMAGE", "ghcr.io/example/second:v2")
		second := AgentImage()
		if first == second {
			t.Fatalf("AgentImage() returned %q twice; the value is cached, not resolved per call", first)
		}
		if second != "ghcr.io/example/second:v2" {
			t.Errorf("AgentImage() = %q, want the updated value", second)
		}
	})
}
