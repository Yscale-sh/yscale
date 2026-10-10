// Package aws implements the Backend interface against the AWS EC2
// API v2 (aws-sdk-go-v2). AWS is yscale's second real-VM backend after
// Linode and the path to frontier GPU silicon (A100/H100/H200) Linode
// cannot reach. Each burst is a single EC2 instance that joins the
// customer's cluster as a native kubelet node.
//
// Like Linode, an EC2 instance boots a full distro image and the
// burst's join logic rides in cloud-init user-data: a bootstrap script
// EC2's IMDS hands cloud-init on first boot, with the per-burst values
// injected as shell exports prepended ahead of it. v0 installs
// containerd/kubelet/tailscale at boot (~3-5 min cold). The fast path
// bakes a per-region AMI so user-data only does the join.
//
// Networking note: every EC2 instance is launched into the chosen
// region's *default* VPC and gets an auto-assigned public IPv4 via the
// IGW (we set AssociatePublicIpAddress=true unconditionally). This is
// enough for the bootstrap's `apt-get` and for `tailscale up` to reach
// Tailscale's control plane; the kubelet itself is reached over the
// tailnet, not the public IP. See docs/backends/aws-plan.md §11.
package aws

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/internal/agentenv"
)

//go:embed bootstrap.sh
var bootstrapScript string

//go:embed bootstrap-baked.sh
var bootstrapBakedScript string

//go:embed ami/ami-id.us-east-1
var bakedAMIUSEast1 string

// Backend drives the EC2 instances API. Single-region per instance:
// callers wanting multi-region failover construct one Backend per
// region. AMI IDs are region-scoped (every region has its own Debian
// AMI), so the ami cache is region-bound to this Backend.
type Backend struct {
	client *ec2.Client
	region string

	// amiCache memoises the latest Debian 12 AMI ID for this Backend's
	// region (CPU bursts) and the latest GPU DLAMI ID (GPU bursts).
	// Resolved on first CreateNode of each kind; the AMI lists rarely
	// change within a process lifetime so a single lookup is enough. Both
	// share amiMu — they are independent fields, the mutex just serialises
	// the lazy lookups.
	amiMu     sync.Mutex
	amiID     string
	amiErr    error
	gpuAMIID  string
	gpuAMIErr error

	// sgID memoises the burst security group for this Backend's region.
	// Ensuring it costs two Describe round-trips (VPC + SG) on the
	// burst create critical path, and the group is stable once created.
	// Invalidated when RunInstances fails, which self-heals the cache
	// if the group was deleted out-of-band.
	sgMu sync.Mutex
	sgID string
}

// New constructs an AWS backend. accessKeyID/secretAccessKey/sessionToken
// are optional — when accessKeyID is empty the SDK's default credential
// chain (env / shared config / IAM role) is used instead, which is the
// recommended shape for production. region is the AWS region slug (e.g.
// "us-east-1"); empty defaults to us-east-1.
func New(accessKeyID, secretAccessKey, sessionToken, region string) *Backend {
	if region == "" {
		region = defaultRegion
	}
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}
	if accessKeyID != "" {
		opts = append(opts,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				accessKeyID, secretAccessKey, sessionToken,
			)),
		)
	}
	// LoadDefaultConfig only fails on bad option types, not on missing
	// creds — credential resolution is deferred to first API call. So
	// New() can stay synchronous and not return an error, matching
	// Linode's New().
	cfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		// Fall back to an empty config; the first API call will fail
		// loudly. Mirrors Linode's "construct now, fail on use" shape.
		cfg = awssdk.Config{Region: region}
	}
	return &Backend{
		client: ec2.NewFromConfig(cfg),
		region: region,
	}
}

func (b *Backend) Name() string { return backends.TypeAWS }

func (b *Backend) CreateNode(ctx context.Context, spec *backends.NodeSpec) (string, error) {
	env := agentenv.AsMap(agentenv.Build(spec))

	var instType, amiID, script string
	if spec.Resources.GPU != nil {
		// GPU path. MapGPUType is resolved FIRST so a bad kind/count fails
		// at plan time, before any EC2 API call — a typo'd spec must not
		// launch (and bill) an instance or hang on a network error.
		var err error
		instType, err = MapGPUType(spec.Resources.GPU)
		if err != nil {
			return "", backends.MarkCreateProvenZeroResource(err)
		}
		// GPU bursts boot the AWS Deep Learning Base AMI (driver + CUDA +
		// container toolkit pre-installed) and run the FULL-install
		// bootstrap — the DLAMI carries only the driver layer, not
		// kubelet/tailscale/CNI, so there is nothing to "join-only" against
		// the way a fully-baked image allows. BURST_GPU=1 switches the
		// bootstrap into its nvidia-containerd-runtime path.
		amiID, err = b.resolveGPUAMI(ctx)
		if err != nil {
			// A read-only preflight failure proves no launch happened.
			return "", backends.MarkCreateProvenZeroResource(fmt.Errorf("aws: resolving GPU AMI in %s: %w", b.region, err))
		}
		script = bootstrapScript
		for k, v := range gpuBootstrapEnv(spec.Resources.GPU.Kind) {
			env[k] = v
		}
	} else {
		instType = mapType(spec.Resources)
		// AMI + bootstrap-script selection:
		//   1. Prefer the baked AMI for this region (//go:embedded under
		//      ami/ami-id.<region>). Boots in ~60-100 s — everything's
		//      pre-installed, bootstrap is just join.
		//   2. Fall back to stock Debian 12 + the full install bootstrap
		//      when no baked AMI is recorded for this region. ~230 s cold.
		var mode string
		amiID, script, mode = b.selectAMI(ctx)
		if amiID == "" {
			var err error
			amiID, err = b.resolveAMI(ctx)
			if err != nil {
				return "", backends.MarkCreateProvenZeroResource(fmt.Errorf("aws: resolving AMI in %s: %w", b.region, err))
			}
			script = bootstrapScript
			mode = "stock-debian-12"
		}
		_ = mode // useful for caller logging if it threads a logger
	}

	// user-data must be base64-encoded by the caller for the SDK's
	// RunInstances path (the CLI auto-encodes, the SDK does not). A
	// raw script silently fails to run — aws-plan.md §13 gotcha 6.
	userData := base64.StdEncoding.EncodeToString(buildUserData(env, script))

	// Attach the dedicated burst security group (opens inbound UDP 41641 so
	// Tailscale can go DIRECT instead of relaying through the per-customer
	// DERP nanode — see ensureBurstSecurityGroup). Best-effort: if it can't
	// be ensured, launch into the default SG anyway — that's still SECURE
	// (default SG blocks all inbound), the burst just falls back to the
	// slower DERP-relayed mesh path rather than failing to provision.
	var sgGroups []string
	if sgID, sgErr := b.ensureBurstSecurityGroup(ctx); sgErr == nil && sgID != "" {
		sgGroups = []string{sgID}
	}

	input := &ec2.RunInstancesInput{
		ImageId:      awssdk.String(amiID),
		InstanceType: ec2types.InstanceType(instType),
		MinCount:     awssdk.Int32(1),
		MaxCount:     awssdk.Int32(1),
		UserData:     awssdk.String(userData),
		// Require IMDSv2 (token-authenticated metadata). UserData carries the
		// burst's provisioning secrets, and IMDSv1 lets any process on the box
		// (incl. a customer workload pod) read it with a single unauthenticated
		// GET. HttpTokens=required forces the PUT-token handshake; the default
		// hop limit of 1 already keeps the token PUT from crossing the pod->host
		// L3 hop. Belt-and-suspenders with the entrypoint's pod->metadata DROP.
		// Safe: the burst's own boot never reads IMDSv1 at runtime (providerID
		// comes from a PROVIDER_ID env fallback), and the AMI build already uses
		// the IMDSv2 token flow.
		MetadataOptions: &ec2types.InstanceMetadataOptionsRequest{
			HttpTokens:   ec2types.HttpTokensStateRequired,
			HttpEndpoint: ec2types.InstanceMetadataEndpointStateEnabled,
		},
		// Tag at launch so the owner tag is atomic with creation —
		// CleanupOrphans cannot race a create-then-tag window. The
		// Name tag also shows in the EC2 console; the burst-id tag
		// matches Linode's per-burst tag for traceability.
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeInstance,
				Tags: []ec2types.Tag{
					{Key: awssdk.String(ownerTagKey), Value: awssdk.String(ownerTagValue)},
					{Key: awssdk.String(burstIDTagKey), Value: awssdk.String(spec.BurstID)},
					{Key: awssdk.String(nameTagKey), Value: awssdk.String(spec.Name)},
				},
			},
		},
		// Force a public IPv4 + IGW egress regardless of the chosen
		// subnet's MapPublicIpOnLaunch — the bootstrap's apt-get and
		// `tailscale up` need outbound internet, and a subnet with no
		// public-IP default would hang the burst at boot. See
		// aws-plan.md §11 + §13 gotcha 4.
		NetworkInterfaces: []ec2types.InstanceNetworkInterfaceSpecification{
			{
				DeviceIndex:              awssdk.Int32(0),
				AssociatePublicIpAddress: awssdk.Bool(true),
				DeleteOnTermination:      awssdk.Bool(true),
				// Burst SG (UDP 41641 only) for direct WireGuard. Empty =
				// default SG (secure, but DERP-relayed) when ensure failed.
				Groups: sgGroups,
			},
		},
	}

	out, err := b.client.RunInstances(ctx, input)
	if err != nil {
		// A stale cached SG ID (group deleted out-of-band) fails the
		// launch; drop the cache so the next attempt re-ensures it.
		b.sgMu.Lock()
		b.sgID = ""
		b.sgMu.Unlock()
		// GPU launches hit two AWS-specific failure modes that a bare
		// "RunInstances failed" hides: a vCPU quota of 0 (the default on
		// new accounts for G/P families — an operational prerequisite, not
		// a code bug) and a region/AZ physically out of that GPU SKU. Spell
		// both out so the operator knows whether to request quota or retry
		// elsewhere, rather than re-reading EC2 error codes. See
		// aws-plan.md §13 gotchas 1-2.
		wrapped := fmt.Errorf("aws: RunInstances %s: %w", spec.Name, err)
		if spec.Resources.GPU != nil {
			if hint := gpuLaunchHint(err, instType, b.region); hint != "" {
				wrapped = fmt.Errorf("aws: RunInstances %s (%s): %w — %s", spec.Name, instType, err, hint)
			}
		}
		// A modeled client fault from RunInstances (EC2's smithy APIError
		// with FaultClient) proves the provider refused the launch — quota
		// exceeded, InvalidParameter, InsufficientInstanceCapacity, etc.
		// Server faults and transport errors stay ambiguous by default.
		if awsCreateRequestRefused(err) {
			return "", backends.MarkCreateProvenZeroResource(wrapped)
		}
		return "", wrapped
	}
	if len(out.Instances) == 0 || out.Instances[0].InstanceId == nil {
		// The API succeeded but returned no instance id — treat as
		// ambiguous: a launch may have started even though we cannot name it.
		return "", backends.MarkCreateAmbiguous(fmt.Errorf("aws: empty instance id in RunInstances response"))
	}
	return *out.Instances[0].InstanceId, nil
}

func awsCreateRequestRefused(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorFault() == smithy.FaultClient
}

// ensureBurstSecurityGroup returns the ID of the dedicated, reused burst
// security group, creating it idempotently (by name in the region's default
// VPC) on first use. Its ONLY inbound rule is UDP 41641 (Tailscale WireGuard),
// which lets bursts establish a DIRECT mesh path instead of falling back to the
// per-customer DERP relay. Security posture: WireGuard authenticates every
// packet, so an open 41641 is safe; SSH and everything else inbound stay closed
// (debug is over the tailnet). Egress is allow-all by SG default. The default
// VPC SG is never modified — a dedicated group keeps other tenants untouched.
func (b *Backend) ensureBurstSecurityGroup(ctx context.Context) (string, error) {
	b.sgMu.Lock()
	cached := b.sgID
	b.sgMu.Unlock()
	if cached != "" {
		return cached, nil
	}

	vpcID, err := b.defaultVPCID(ctx)
	if err != nil {
		return "", err
	}
	if id, err := b.findSecurityGroup(ctx, vpcID); err != nil {
		return "", err
	} else if id != "" {
		b.sgMu.Lock()
		b.sgID = id
		b.sgMu.Unlock()
		return id, nil // reuse the existing group
	}

	created, err := b.client.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   awssdk.String(burstSecurityGroupName),
		Description: awssdk.String("yscale burst: inbound UDP 41641 (Tailscale WireGuard direct) only"),
		VpcId:       awssdk.String(vpcID),
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeSecurityGroup,
			Tags:         []ec2types.Tag{{Key: awssdk.String(ownerTagKey), Value: awssdk.String(ownerTagValue)}},
		}},
	})
	if err != nil {
		// A concurrent CreateNode may have won the create race — re-look it up.
		if id, lerr := b.findSecurityGroup(ctx, vpcID); lerr == nil && id != "" {
			b.sgMu.Lock()
			b.sgID = id
			b.sgMu.Unlock()
			return id, nil
		}
		return "", fmt.Errorf("create security group: %w", err)
	}
	sgID := *created.GroupId

	if _, err := b.client.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{
		GroupId: awssdk.String(sgID),
		IpPermissions: []ec2types.IpPermission{{
			IpProtocol: awssdk.String("udp"),
			FromPort:   awssdk.Int32(tailscaleDirectPort),
			ToPort:     awssdk.Int32(tailscaleDirectPort),
			IpRanges: []ec2types.IpRange{{
				CidrIp:      awssdk.String("0.0.0.0/0"),
				Description: awssdk.String("tailscale-direct (WireGuard, authenticated)"),
			}},
		}},
	}); err != nil {
		return "", fmt.Errorf("authorize udp %d ingress: %w", tailscaleDirectPort, err)
	}
	b.sgMu.Lock()
	b.sgID = sgID
	b.sgMu.Unlock()
	return sgID, nil
}

// defaultVPCID resolves the region's default VPC, where CreateNode launches.
func (b *Backend) defaultVPCID(ctx context.Context) (string, error) {
	out, err := b.client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
		Filters: []ec2types.Filter{{Name: awssdk.String("isDefault"), Values: []string{"true"}}},
	})
	if err != nil {
		return "", fmt.Errorf("describe default vpc: %w", err)
	}
	if len(out.Vpcs) == 0 || out.Vpcs[0].VpcId == nil {
		return "", fmt.Errorf("no default vpc in %s", b.region)
	}
	return *out.Vpcs[0].VpcId, nil
}

// findSecurityGroup returns the burst SG's ID in the given VPC, or "" if absent.
func (b *Backend) findSecurityGroup(ctx context.Context, vpcID string) (string, error) {
	out, err := b.client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
		Filters: []ec2types.Filter{
			{Name: awssdk.String("group-name"), Values: []string{burstSecurityGroupName}},
			{Name: awssdk.String("vpc-id"), Values: []string{vpcID}},
		},
	})
	if err != nil {
		return "", fmt.Errorf("describe security groups: %w", err)
	}
	if len(out.SecurityGroups) > 0 && out.SecurityGroups[0].GroupId != nil {
		return *out.SecurityGroups[0].GroupId, nil
	}
	return "", nil
}

func (b *Backend) StartNode(ctx context.Context, backendID string) error {
	_, err := b.client.StartInstances(ctx, &ec2.StartInstancesInput{
		InstanceIds: []string{backendID},
	})
	if err != nil {
		return fmt.Errorf("aws: StartInstances %s: %w", backendID, err)
	}
	return nil
}

func (b *Backend) StopNode(ctx context.Context, backendID string) error {
	_, err := b.client.StopInstances(ctx, &ec2.StopInstancesInput{
		InstanceIds: []string{backendID},
	})
	if err != nil {
		return fmt.Errorf("aws: StopInstances %s: %w", backendID, err)
	}
	return nil
}

// DeleteNode permanently terminates an instance. Idempotent: if the
// instance is already gone (terminated and aged out of EC2's list, or
// never existed), DeleteNode returns nil so reap retries never wedge.
// The root EBS volume is DeleteOnTermination=true by default, so no
// extra cleanup is needed.
func (b *Backend) DeleteNode(ctx context.Context, backendID string) error {
	_, err := b.client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
		InstanceIds: []string{backendID},
	})
	if err == nil {
		return nil
	}
	if isNotFoundErr(err) {
		return nil
	}
	return fmt.Errorf("aws: TerminateInstances %s: %w", backendID, err)
}

func (b *Backend) GetNodeStatus(ctx context.Context, backendID string) (*backends.NodeStatus, error) {
	out, err := b.client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{backendID},
	})
	if err != nil {
		return nil, fmt.Errorf("aws: DescribeInstances %s: %w", backendID, err)
	}
	inst, ok := firstInstance(out)
	if !ok {
		return nil, fmt.Errorf("aws: instance %s not found", backendID)
	}
	// Prefer the public IPv4 (the tailnet needs egress through it).
	// Fall back to the private IPv4 for the rare case the instance is
	// in a private subnet — Tailscale still works as long as outbound
	// reaches the control plane.
	ip := ""
	if inst.PublicIpAddress != nil {
		ip = *inst.PublicIpAddress
	} else if inst.PrivateIpAddress != nil {
		ip = *inst.PrivateIpAddress
	}
	started := time.Time{}
	if inst.LaunchTime != nil {
		started = *inst.LaunchTime
	}
	return &backends.NodeStatus{
		Phase:     mapStatus(inst.State),
		IP:        ip,
		StartedAt: started,
	}, nil
}

// ListPooledNodes returns yscale-owned instances currently in the
// `stopped` state — instances StopNode parked for fast restart. Uses
// server-side tag filtering, faster than Linode's client-side loop.
func (b *Backend) ListPooledNodes(ctx context.Context) ([]backends.PooledNode, error) {
	insts, err := b.listOwnedInstances(ctx, ownerTagValue,
		&ec2types.Filter{
			Name:   awssdk.String("instance-state-name"),
			Values: []string{string(ec2types.InstanceStateNameStopped)},
		},
	)
	if err != nil {
		return nil, err
	}
	pooled := make([]backends.PooledNode, 0, len(insts))
	for _, in := range insts {
		if in.InstanceId == nil {
			continue
		}
		pooled = append(pooled, backends.PooledNode{
			BackendID: *in.InstanceId,
			Name:      instanceName(in),
		})
	}
	return pooled, nil
}

// ListOwnedNodes returns only nonterminal EC2 instances carrying the exact
// Yscale owner tag. It is read-only provider inventory for central
// reconciliation.
func (b *Backend) ListOwnedNodes(ctx context.Context) ([]backends.OwnedNode, error) {
	insts, err := b.listOwnedInstances(ctx, ownerTagValue)
	if err != nil {
		return nil, err
	}
	owned := make([]backends.OwnedNode, 0)
	for _, in := range insts {
		if in.InstanceId == nil || !hasOwnerTag(in.Tags) {
			continue
		}
		switch in.State.Name {
		case ec2types.InstanceStateNameTerminated, ec2types.InstanceStateNameShuttingDown:
			continue
		}
		var launched time.Time
		if in.LaunchTime != nil {
			launched = *in.LaunchTime
		}
		owned = append(owned, backends.OwnedNode{
			BackendID: *in.InstanceId,
			Name:      instanceName(in),
			BurstID:   tagValue(in.Tags, burstIDTagKey),
			CreatedAt: launched,
		})
	}
	return owned, nil
}

// CleanupOrphans destroys yscale-created EC2 instances the controller
// is no longer tracking. It identifies a yscale instance solely by the
// exact yscale-owner tag CreateNode stamps — never a name pattern — so
// it can only ever touch instances yscale itself provisioned. Already-
// terminated instances are ignored (they bill nothing and EC2 will GC
// them on its own clock). Instances younger than
// backends.OrphanGracePeriod are ignored too: untracked is
// indistinguishable from mid-create until the caller's record write lands.
func (b *Backend) CleanupOrphans(ctx context.Context, trackedIDs map[string]bool) (int, error) {
	insts, err := b.listOwnedInstances(ctx, ownerTagValue)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	destroyed := 0
	for _, in := range insts {
		if in.InstanceId == nil {
			continue
		}
		// Belt-and-braces: only act on instances whose tags we re-verify
		// carry our exact owner pair. The tag-filtered DescribeInstances
		// already guarantees this, but a defensive check keeps the
		// safety invariant local + readable.
		if !hasOwnerTag(in.Tags) {
			continue
		}
		// Ignore already-gone states — terminating/terminated instances
		// are not orphans, just stragglers in EC2's listing.
		switch in.State.Name {
		case ec2types.InstanceStateNameTerminated, ec2types.InstanceStateNameShuttingDown:
			continue
		}
		id := *in.InstanceId
		if trackedIDs[id] {
			continue
		}
		// A nil LaunchTime reads as the zero time, which OrphanTooYoung
		// treats as too young — fail safe toward keeping the instance.
		var launched time.Time
		if in.LaunchTime != nil {
			launched = *in.LaunchTime
		}
		if backends.OrphanTooYoung(launched, now) {
			continue
		}
		if err := b.DeleteNode(ctx, id); err == nil {
			destroyed++
		}
	}
	return destroyed, nil
}

// listOwnedInstances returns every EC2 instance carrying the yscale
// owner tag with the given value, plus any extra caller-supplied
// filters. Tag-filtering happens server-side — the EC2 API filter
// `tag:<key>` matches against the tag value, the precise primitive
// CleanupOrphans needs to scope itself to yscale-only resources.
func (b *Backend) listOwnedInstances(ctx context.Context, ownerValue string, extra ...*ec2types.Filter) ([]ec2types.Instance, error) {
	filters := []ec2types.Filter{
		{
			Name:   awssdk.String("tag:" + ownerTagKey),
			Values: []string{ownerValue},
		},
	}
	for _, f := range extra {
		if f != nil {
			filters = append(filters, *f)
		}
	}
	out := make([]ec2types.Instance, 0)
	pager := ec2.NewDescribeInstancesPaginator(b.client, &ec2.DescribeInstancesInput{
		Filters: filters,
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("aws: DescribeInstances (tag:%s=%s): %w", ownerTagKey, ownerValue, err)
		}
		for _, r := range page.Reservations {
			out = append(out, r.Instances...)
		}
	}
	return out, nil
}

// selectAMI returns a pre-baked yscale AMI + the slim join-only
// bootstrap when one is recorded for this region under
// pkg/backends/aws/ami/ami-id.<region>. Returns ("", "", "") when no
// baked AMI is recorded; caller falls back to resolveAMI + the full
// install bootstrap. The ami-id files are //go:embedded so the
// recipe + AMI ID ship together in the binary.
//
// YSCALE_AWS_AMI overrides the embedded ID. An AMI is region-scoped and, unless
// its owner has shared it, account-scoped too — so the embedded default only
// works for the account that baked it. An operator who has run build-ami.sh in
// their own account points yscale at the result here. The override is honored
// for ANY region (the embedded set only covers us-east-1), which is also how a
// second region becomes usable without a rebuild.
func (b *Backend) selectAMI(ctx context.Context) (amiID, script, mode string) {
	_ = ctx
	if id := strings.TrimSpace(os.Getenv("YSCALE_AWS_AMI")); strings.HasPrefix(id, "ami-") {
		return id, bootstrapBakedScript, "override-" + b.region
	}
	switch b.region {
	case "us-east-1":
		id := strings.TrimSpace(bakedAMIUSEast1)
		if strings.HasPrefix(id, "ami-") {
			return id, bootstrapBakedScript, "baked-" + b.region
		}
	}
	return "", "", ""
}

// resolveAMI looks up the latest Debian 12 (bookworm) cloud AMI in
// this Backend's region and caches the result. AMI IDs are
// region-scoped; the cache is local to this Backend instance and is
// only invalidated by process restart, which is acceptable — Debian
// publishes new AMIs ~monthly, and a yscale process restart picks
// them up.
func (b *Backend) resolveAMI(ctx context.Context) (string, error) {
	b.amiMu.Lock()
	defer b.amiMu.Unlock()
	if b.amiID != "" {
		return b.amiID, nil
	}
	if b.amiErr != nil {
		// Don't cache resolution errors permanently — a transient API
		// failure shouldn't poison the backend for its lifetime.
		// Re-attempt below.
		b.amiErr = nil
	}
	out, err := b.client.DescribeImages(ctx, &ec2.DescribeImagesInput{
		Owners: []string{debianOwnerID},
		Filters: []ec2types.Filter{
			{Name: awssdk.String("name"), Values: []string{debianNamePattern}},
			{Name: awssdk.String("architecture"), Values: []string{"x86_64"}},
			{Name: awssdk.String("virtualization-type"), Values: []string{"hvm"}},
			{Name: awssdk.String("root-device-type"), Values: []string{"ebs"}},
			{Name: awssdk.String("state"), Values: []string{"available"}},
		},
	})
	if err != nil {
		b.amiErr = err
		return "", err
	}
	if len(out.Images) == 0 {
		return "", fmt.Errorf("aws: no Debian 12 AMIs found in %s (owner=%s, pattern=%s)", b.region, debianOwnerID, debianNamePattern)
	}
	// Sort by CreationDate descending; pick the newest.
	images := out.Images
	sort.Slice(images, func(i, j int) bool {
		ci, cj := "", ""
		if images[i].CreationDate != nil {
			ci = *images[i].CreationDate
		}
		if images[j].CreationDate != nil {
			cj = *images[j].CreationDate
		}
		return ci > cj
	})
	if images[0].ImageId == nil {
		return "", fmt.Errorf("aws: latest Debian 12 AMI has no ImageId")
	}
	b.amiID = *images[0].ImageId
	return b.amiID, nil
}

// resolveGPUAMI looks up the latest AWS Deep Learning Base GPU AMI in
// this Backend's region and caches it. Mirrors resolveAMI but filters on
// the DLAMI owner alias + GPU name pattern (§7). The DLAMI ships the
// NVIDIA driver/CUDA/container-toolkit, so a GPU burst boots it and the
// full-install bootstrap only adds kubelet/tailscale/CNI + the nvidia
// containerd runtime config — yscale never bakes a GPU AMI.
func (b *Backend) resolveGPUAMI(ctx context.Context) (string, error) {
	b.amiMu.Lock()
	defer b.amiMu.Unlock()
	if b.gpuAMIID != "" {
		return b.gpuAMIID, nil
	}
	if b.gpuAMIErr != nil {
		// Don't poison the backend on a transient lookup failure — clear the
		// remembered error and re-attempt, matching resolveAMI.
		b.gpuAMIErr = nil
	}
	out, err := b.client.DescribeImages(ctx, &ec2.DescribeImagesInput{
		Owners: []string{dlamiOwnerAlias},
		Filters: []ec2types.Filter{
			{Name: awssdk.String("name"), Values: []string{dlamiGPUNamePattern}},
			{Name: awssdk.String("architecture"), Values: []string{"x86_64"}},
			{Name: awssdk.String("virtualization-type"), Values: []string{"hvm"}},
			{Name: awssdk.String("root-device-type"), Values: []string{"ebs"}},
			{Name: awssdk.String("state"), Values: []string{"available"}},
		},
	})
	if err != nil {
		b.gpuAMIErr = err
		return "", err
	}
	if len(out.Images) == 0 {
		return "", fmt.Errorf("aws: no Deep Learning Base GPU AMI found in %s (owner=%s, pattern=%q) — GPU bursts need the DLAMI; verify it is published in this region", b.region, dlamiOwnerAlias, dlamiGPUNamePattern)
	}
	images := out.Images
	sort.Slice(images, func(i, j int) bool {
		ci, cj := "", ""
		if images[i].CreationDate != nil {
			ci = *images[i].CreationDate
		}
		if images[j].CreationDate != nil {
			cj = *images[j].CreationDate
		}
		return ci > cj
	})
	if images[0].ImageId == nil {
		return "", fmt.Errorf("aws: latest GPU DLAMI in %s has no ImageId", b.region)
	}
	b.gpuAMIID = *images[0].ImageId
	return b.gpuAMIID, nil
}

// gpuBootstrapEnv returns the extra cloud-init exports a GPU burst needs
// on top of the standard agent env. BURST_GPU=1 switches the bootstrap
// into its nvidia-containerd-runtime path; GPU_KIND records the resolved
// card for the bootstrap log. An empty/"any" kind normalises to the
// default card so GPU_KIND is never blank.
func gpuBootstrapEnv(kind string) map[string]string {
	k := strings.ToLower(kind)
	if k == "" || k == "any" {
		k = "l4"
	}
	return map[string]string{
		"BURST_GPU": "1",
		"GPU_KIND":  k,
	}
}

// gpuLaunchHint returns operator-facing guidance when a GPU RunInstances
// fails for a quota or capacity reason, or "" for any other error (where
// the raw EC2 message is already informative). Kept separate from the
// wrap so the hint text lives in one place.
func gpuLaunchHint(err error, instType, region string) string {
	var apiErr smithy.APIError
	code := ""
	if errors.As(err, &apiErr) {
		code = apiErr.ErrorCode()
	}
	switch {
	case code == "VcpuLimitExceeded" || strings.Contains(err.Error(), "VcpuLimitExceeded"):
		return fmt.Sprintf("the %s vCPU quota looks unset (new AWS accounts default G/P GPU quota to 0); request a Service Quotas increase for the instance family before bursting GPU in %s", instType, region)
	case code == "InsufficientInstanceCapacity" || strings.Contains(err.Error(), "InsufficientInstanceCapacity"):
		return fmt.Sprintf("%s has no spare %s capacity right now; retry in another AZ/region (no AWS API predicts GPU capacity — see aws-plan.md §9)", region, instType)
	}
	return ""
}

// firstInstance pulls the first instance out of a DescribeInstances
// response (which wraps instances inside Reservations). Returns
// (instance, true) when found, (zero, false) otherwise.
func firstInstance(out *ec2.DescribeInstancesOutput) (ec2types.Instance, bool) {
	if out == nil {
		return ec2types.Instance{}, false
	}
	for _, r := range out.Reservations {
		if len(r.Instances) > 0 {
			return r.Instances[0], true
		}
	}
	return ec2types.Instance{}, false
}

// hasOwnerTag reports whether the given tag list carries the exact
// yscale-owner=burst pair. CleanupOrphans uses it as a defensive
// re-check of the server-side tag filter.
func hasOwnerTag(tags []ec2types.Tag) bool {
	for _, t := range tags {
		if t.Key == nil || t.Value == nil {
			continue
		}
		if *t.Key == ownerTagKey && *t.Value == ownerTagValue {
			return true
		}
	}
	return false
}

// instanceName returns the EC2 Name tag value, or empty if unset.
func instanceName(in ec2types.Instance) string {
	return tagValue(in.Tags, nameTagKey)
}

func tagValue(tags []ec2types.Tag, key string) string {
	for _, t := range tags {
		if t.Key == nil || t.Value == nil {
			continue
		}
		if *t.Key == key {
			return *t.Value
		}
	}
	return ""
}

// isNotFoundErr reports whether an EC2 error indicates the instance
// does not exist — InvalidInstanceID.NotFound (the documented code
// the EC2 SDK itself special-cases for the same purpose, see
// api_op_DescribeInstances.go in aws-sdk-go-v2).
func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed":
			return true
		}
	}
	// Some EC2 errors surface only as a string in the operation error;
	// substring match is a safe last-line fallback.
	return strings.Contains(err.Error(), "InvalidInstanceID.NotFound")
}

// mapStatus collapses an EC2 InstanceState into our NodePhase. The
// table mirrors aws-plan.md §4; terminating/terminated are treated as
// Stopped (gone) so callers don't need an extra "terminated" phase.
func mapStatus(state *ec2types.InstanceState) backends.NodePhase {
	if state == nil {
		return backends.NodeUnknown
	}
	switch state.Name {
	case ec2types.InstanceStateNameRunning:
		return backends.NodeRunning
	case ec2types.InstanceStateNamePending:
		return backends.NodeStarting
	case ec2types.InstanceStateNameStopping,
		ec2types.InstanceStateNameStopped,
		ec2types.InstanceStateNameShuttingDown,
		ec2types.InstanceStateNameTerminated:
		return backends.NodeStopped
	}
	return backends.NodeUnknown
}

// awsGPUInstances maps an abstract GPU kind to the smallest EC2 instance
// type carrying each supported GPU count. Drawn from aws-plan.md §7.
//
// G-family (g4dn/g5/g6/g6e) offers true 1-GPU SKUs (the `.xlarge` size);
// 4-GPU and 8-GPU counts step up to the family's multi-GPU sizes (g4dn's
// 8-GPU node is bare-metal `.metal`, the others are `.48xlarge`). The
// P-family (p4d/p5/p5e) ships ONLY the full 8-GPU node — there is no
// 1-GPU A100/H100/H200 instance — so those kinds carry a single 8-count
// entry and any other count is an error rather than a silent (and very
// expensive) round-up. A burst rarely needs more CPU/RAM than the
// smallest size carrying the requested GPU count, so we always pick the
// smallest, exactly as Linode's MapGPUType does.
var awsGPUInstances = map[string]map[int]string{
	"t4":   {1: "g4dn.xlarge", 4: "g4dn.12xlarge", 8: "g4dn.metal"},
	"a10g": {1: "g5.xlarge", 4: "g5.12xlarge", 8: "g5.48xlarge"},
	"l4":   {1: "g6.xlarge", 4: "g6.12xlarge", 8: "g6.48xlarge"},
	"l40s": {1: "g6e.xlarge", 4: "g6e.12xlarge", 8: "g6e.48xlarge"},
	"a100": {8: "p4d.24xlarge"},
	"h100": {8: "p5.48xlarge"},
	"h200": {8: "p5e.48xlarge"},
}

// MapGPUType resolves an abstract GPUSpec to the EC2 instance type that
// carries the requested GPU count. An empty or "any" kind defaults to L4
// (`g6`, the cheapest current-gen inference card), mirroring Linode's
// default-to-cheapest behaviour. A count of 0 is treated as 1. Returns a
// loud error for an unknown kind or a count the family has no SKU for, so
// a bad spec fails at plan time rather than launching the wrong instance.
func MapGPUType(gpu *backends.GPUSpec) (string, error) {
	kind := strings.ToLower(gpu.Kind)
	if kind == "" || kind == "any" {
		kind = "l4"
	}
	count := max(gpu.Count, 1)
	sizes, ok := awsGPUInstances[kind]
	if !ok {
		return "", fmt.Errorf("aws: unknown gpu kind %q (supported: t4, a10g, l4, l40s, a100, h100, h200)", gpu.Kind)
	}
	it, ok := sizes[count]
	if !ok {
		return "", fmt.Errorf("aws: gpu kind %q has no %d-GPU instance (supported counts: %s)", kind, count, supportedGPUCounts(sizes))
	}
	return it, nil
}

// supportedGPUCounts renders a family's available GPU counts in ascending
// order for an error message (e.g. "1, 4, 8").
func supportedGPUCounts(sizes map[int]string) string {
	counts := make([]int, 0, len(sizes))
	for c := range sizes {
		counts = append(counts, c)
	}
	sort.Ints(counts)
	parts := make([]string, len(counts))
	for i, c := range counts {
		parts[i] = fmt.Sprint(c)
	}
	return strings.Join(parts, ", ")
}

// mapType picks the smallest EC2 instance type that fits the workload's
// memory request, mirroring Linode's mapType shape. Thresholds leave
// ~1GB+ headroom for the OS + kubelet + containerd — the node must fit
// the workload's request *plus* system overhead, or the pod stays
// Pending. T3 is x86_64 burstable (cheap, fine for short bursts); M7i
// is x86_64 steady-state. Per aws-plan.md §6.
//
// GPU sizing is handled separately by MapGPUType.
func mapType(r backends.ResourceRequirements) string {
	switch {
	case r.MemoryMB <= 1536:
		return string(ec2types.InstanceTypeT3Small) // 2 vCPU / 2 GB
	case r.MemoryMB <= 3072:
		return string(ec2types.InstanceTypeT3Medium) // 2 vCPU / 4 GB
	case r.MemoryMB <= 6144:
		return string(ec2types.InstanceTypeM7iLarge) // 2 vCPU / 8 GB
	case r.MemoryMB <= 14336:
		return string(ec2types.InstanceTypeM7iXlarge) // 4 vCPU / 16 GB
	default:
		return string(ec2types.InstanceTypeM7i2xlarge) // 8 vCPU / 32 GB
	}
}

// buildUserData renders the cloud-init user-data: a #!/bin/bash header,
// the per-burst values as shell exports (sorted for determinism, empties
// skipped), then the given bootstrap body which consumes them as env
// vars. Ported verbatim from Linode — EC2 user-data takes the same
// shape (a #!/bin/bash script run as root on first boot), so the
// bootstrap body lifts across cleanly.
func buildUserData(env map[string]string, body string) []byte {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b bytes.Buffer
	b.WriteString("#!/bin/bash\n")
	for _, k := range keys {
		if env[k] == "" {
			continue
		}
		fmt.Fprintf(&b, "export %s=%s\n", k, shellQuote(env[k]))
	}
	b.WriteString(body)
	return b.Bytes()
}

// shellQuote single-quote-wraps a value so it is safe in an export
// line. Ported verbatim from Linode.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
