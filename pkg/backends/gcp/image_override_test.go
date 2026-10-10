package gcp

import "testing"

// YSCALE_GCP_IMAGE is the ONLY way to boot a baked GCP image today — unlike
// AWS's embedded us-east-1 AMI or Linode's compiled-in private image IDs,
// this backend ships no default at all (a baked image is built by a human
// running image/build-image.sh in their own project). Unset or "-" must
// fall back to the exact stock debian-12 + full-install path this backend
// has always used, byte for byte — baking is unverified live here (no GCP
// credentials in CI), so a silent regression on the default path would ship
// unnoticed.
func TestSelectImage(t *testing.T) {
	t.Run("unset stays on stock debian-12", func(t *testing.T) {
		t.Setenv("YSCALE_GCP_IMAGE", "")
		image, script, mode := selectImage()
		if image != burstImage {
			t.Errorf("image = %q, want the stock burstImage %q", image, burstImage)
		}
		if script != bootstrapScript {
			t.Error("unset must use the full-install bootstrap.sh, not the join-only baked variant")
		}
		if mode != "stock-debian-12" {
			t.Errorf("mode = %q, want stock-debian-12", mode)
		}
	})

	t.Run(`dash explicitly selects no baked image, same as unset`, func(t *testing.T) {
		t.Setenv("YSCALE_GCP_IMAGE", "-")
		image, script, mode := selectImage()
		if image != burstImage || script != bootstrapScript {
			t.Errorf("selectImage() = (%q, ..), want the stock path", image)
		}
		if mode != "stock-debian-12" {
			t.Errorf("mode = %q, want stock-debian-12", mode)
		}
	})

	t.Run("override switches to the baked image + join-only bootstrap", func(t *testing.T) {
		const ref = "projects/my-proj/global/images/yscale-burst-20260801"
		t.Setenv("YSCALE_GCP_IMAGE", ref)
		image, script, mode := selectImage()
		if image != ref {
			t.Errorf("image = %q, want the override %q", image, ref)
		}
		if script != bootstrapBakedScript {
			t.Error("an overridden image is baked; it must get the join-only bootstrap")
		}
		if mode != "baked-override" {
			t.Errorf("mode = %q, want baked-override", mode)
		}
	})

	t.Run("whitespace is trimmed", func(t *testing.T) {
		const ref = "projects/my-proj/global/images/yscale-burst-20260801"
		t.Setenv("YSCALE_GCP_IMAGE", "  "+ref+"\n")
		if got := imageOverride(); got != ref {
			t.Errorf("imageOverride() = %q, want %q", got, ref)
		}
	})
}
