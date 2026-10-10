package aws

import (
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// The embedded AMI ID is baked in the maintainer's account. An AMI is
// region-scoped and, unless explicitly shared, account-scoped — so without an
// override a fresh operator's burst boots an AMI they cannot see. This pins the
// override so it cannot silently regress into "the product does not work".
func TestSelectAMI_Override(t *testing.T) {
	t.Run("override wins over the embedded region AMI", func(t *testing.T) {
		t.Setenv("YSCALE_AWS_AMI", "ami-0123456789abcdef0")
		b := &Backend{region: "us-east-1"}
		id, script, mode := b.selectAMI(t.Context())
		if id != "ami-0123456789abcdef0" {
			t.Errorf("amiID = %q, want the override", id)
		}
		if script != bootstrapBakedScript {
			t.Error("an overridden AMI is a baked image; it must get the join-only bootstrap")
		}
		if mode != "override-us-east-1" {
			t.Errorf("mode = %q, want override-us-east-1", mode)
		}
	})

	// The embedded set only covers us-east-1, so before this override a
	// second region silently fell back to stock Debian + the slow full
	// install. The override is what makes another region usable at all.
	t.Run("override enables a region with no embedded AMI", func(t *testing.T) {
		t.Setenv("YSCALE_AWS_AMI", "ami-0aaaaaaaaaaaaaaaa")
		b := &Backend{region: "eu-west-2"}
		id, _, mode := b.selectAMI(t.Context())
		if id != "ami-0aaaaaaaaaaaaaaaa" {
			t.Errorf("amiID = %q, want the override in a non-embedded region", id)
		}
		if mode != "override-eu-west-2" {
			t.Errorf("mode = %q, want override-eu-west-2", mode)
		}
	})

	// Fail safe rather than passing junk to RunInstances: a malformed value
	// falls through to the normal selection path.
	t.Run("a malformed override is ignored", func(t *testing.T) {
		t.Setenv("YSCALE_AWS_AMI", "not-an-ami")
		b := &Backend{region: "eu-west-2"}
		if id, _, _ := b.selectAMI(t.Context()); id != "" {
			t.Errorf("amiID = %q, want empty so the caller falls back to resolveAMI", id)
		}
	})

	t.Run("unset keeps the embedded default", func(t *testing.T) {
		t.Setenv("YSCALE_AWS_AMI", "")
		b := &Backend{region: "us-east-1"}
		id, _, mode := b.selectAMI(t.Context())
		if id == "" || mode != "baked-us-east-1" {
			t.Errorf("selectAMI() = (%q, _, %q), want the embedded us-east-1 AMI", id, mode)
		}
	})
}

func TestOwnedNodeBurstIDComesFromExactTag(t *testing.T) {
	tags := []ec2types.Tag{
		{Key: awssdk.String(ownerTagKey), Value: awssdk.String(ownerTagValue)},
		{Key: awssdk.String(nameTagKey), Value: awssdk.String("ys-burst-node")},
		{Key: awssdk.String(burstIDTagKey), Value: awssdk.String("burst_exact")},
	}
	if got := tagValue(tags, burstIDTagKey); got != "burst_exact" {
		t.Fatalf("BurstID tag = %q, want exact marker", got)
	}
	if got := tagValue(tags, "yscale-node"); got != "" {
		t.Fatalf("unexpected BurstID inference from non-marker tag: %q", got)
	}
}
