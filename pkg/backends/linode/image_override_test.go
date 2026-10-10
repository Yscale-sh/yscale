package linode

import "testing"

// The compiled-in image IDs are Linode private images, which are scoped to the
// account that baked them — they are unreachable from any other token. The env
// overrides are therefore the only way a fresh operator can boot a burst at
// all, so they are worth pinning: a silently-ignored override reads exactly
// like "this product does not work".
func TestBurstImageRef(t *testing.T) {
	const fallback = "private/000000"

	tests := []struct {
		name string
		env  string
		set  bool
		want string
	}{
		{name: "unset falls back to the baked default", set: false, want: fallback},
		{name: "empty falls back to the baked default", env: "", set: true, want: fallback},
		{name: "override wins", env: "private/12345", set: true, want: "private/12345"},
		{name: "whitespace is trimmed", env: "  private/12345\n", set: true, want: "private/12345"},
		{name: "a stock distro ref is honored", env: "linode/debian12", set: true, want: "linode/debian12"},
		// "-" is how an operator says "I have no baked image", which is NOT the
		// same as "unset". The CPU path treats empty as "fall back to stock
		// Debian + the full-install bootstrap", so this has to round-trip.
		{name: "dash means explicitly none", env: "-", set: true, want: ""},
		{name: "dash with whitespace still means none", env: "  -  ", set: true, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv("YSCALE_TEST_IMAGE", tc.env)
			}
			if got := burstImageRef("YSCALE_TEST_IMAGE", fallback); got != tc.want {
				t.Errorf("burstImageRef() = %q, want %q", got, tc.want)
			}
		})
	}
}

// Guard the wiring, not just the helper: it would be easy to add the resolver
// and forget to call it from CreateNode's selection branch.
func TestBurstImageRefsReadTheirOwnVars(t *testing.T) {
	t.Setenv("YSCALE_LINODE_IMAGE_GPU", "private/gpu-override")
	t.Setenv("YSCALE_LINODE_IMAGE_CPU", "private/cpu-override")

	if got := gpuBurstImageRef(); got != "private/gpu-override" {
		t.Errorf("gpuBurstImageRef() = %q, want the YSCALE_LINODE_IMAGE_GPU value", got)
	}
	if got := cpuBurstImageRef(); got != "private/cpu-override" {
		t.Errorf("cpuBurstImageRef() = %q, want the YSCALE_LINODE_IMAGE_CPU value", got)
	}
}
