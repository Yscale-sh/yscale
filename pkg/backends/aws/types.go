package aws

// Most of this backend's data lives in the official aws-sdk-go-v2 EC2
// types (RunInstancesInput, Instance, Filter, …) rather than in
// hand-rolled structs the way Linode's types.go does — the SDK already
// gives us tagged structs for every API surface we touch. This file
// holds only the small pieces of yscale-internal configuration that
// don't have a 1:1 SDK equivalent.

// ownerTag is the EC2 tag key=value pair stamped on every instance
// CreateNode provisions. It is the *only* way CleanupOrphans identifies
// a yscale instance — an exact tag yscale alone applies, never a name
// pattern — so cleanup can never touch the customer's own workloads
// sharing the AWS account.
const (
	ownerTagKey   = "yscale-owner"
	ownerTagValue = "burst"
	// burstIDTagKey holds the central-assigned BurstID for traceability
	// and cost reporting; mirrors Linode's per-burst tag.
	burstIDTagKey = "yscale-burst-id"
	// nameTagKey is EC2's well-known display name, set so instances
	// surface their yscale name in the EC2 console.
	nameTagKey = "Name"
	// burstSecurityGroupName is the dedicated, reused security group every
	// burst is launched into (idempotent by name in the default VPC). It
	// exists for ONE reason: open inbound UDP 41641 so Tailscale can
	// establish a DIRECT WireGuard path instead of falling back to the tiny
	// per-customer DERP relay (which caps bulk transfer at ~tens of Mbps).
	// Without it, EC2's default SG blocks all inbound and every burst is
	// DERP-relayed. NOT the default SG — a dedicated group so we never touch
	// other tenants' instances.
	burstSecurityGroupName = "yscale-burst-fw"
	// tailscaleDirectPort is Tailscale's default WireGuard listen port. WG
	// traffic is encrypted + authenticated (Noise), so exposing this port to
	// 0.0.0.0/0 is safe — unauthenticated packets are dropped by WireGuard
	// itself. This is the ONLY inbound rule we add; SSH stays closed and
	// debug access is over the tailnet (the kubelet is reached the same way).
	tailscaleDirectPort = 41641
)

// debianOwnerID is the Amazon-issued AWS account number that owns the
// official Debian AMIs. Hard-coded because Debian's account is stable;
// DescribeImages filters by `owner-id` to scope the lookup to authentic
// images and rule out the dozens of unofficial Debian-named community
// AMIs. (Cite: cloud.debian.org/images/cloud — "AWS account ID of the
// publisher".)
const debianOwnerID = "136693071363"

// debianNamePattern matches the latest cloud images for Debian 12
// (bookworm) on x86_64, HVM, EBS-backed. DescribeImages returns the
// matching set; resolveAMI picks the newest by CreationDate.
const debianNamePattern = "debian-12-amd64-*"

// GPU bursts boot an AWS Deep Learning Base AMI: Ubuntu 22.04 with the
// NVIDIA driver, CUDA, and the NVIDIA Container Toolkit pre-installed but
// no ML frameworks (the "Base" line is the slim driver-only DLAMI). This
// replaces Linode's hand-baked private GPU image — AWS/NVIDIA ship the
// driver layer, so yscale never bakes a GPU AMI (aws-plan.md §7). The
// burst's full-install bootstrap layers kubelet/tailscale/CNI on top and
// points containerd at the pre-installed nvidia runtime.
//
// Resolved by owner alias + name pattern (newest by CreationDate), the
// same shape as resolveAMI. The "amazon" owner alias is used rather than
// a hard-coded publisher account because the DLAMI publisher account id
// is not stable across all regions; the name pattern is specific enough
// that only the genuine Base GPU DLAMI matches.
// The name pattern leaves the Ubuntu version open (`Ubuntu *`) rather than
// pinning 22.04 — when AWS moves the Base line to a newer LTS, a pinned
// pattern would match nothing and strand every GPU burst. "Deep Learning
// Base OSS Nvidia Driver GPU AMI" is specific enough that only the genuine
// driver-only Base DLAMI matches.
const (
	dlamiOwnerAlias     = "amazon"
	dlamiGPUNamePattern = "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu *)*"
)

// defaultRegion is the AWS region used when no explicit region is
// supplied to New(). Mirrors Linode's us-ord default — pick a US-East
// region that has both broad SKU coverage and a default VPC by default.
const defaultRegion = "us-east-1"
