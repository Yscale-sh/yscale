// Package gcp implements the Backend interface against the Google
// Compute Engine API v1 (cloud.google.com/go/compute/apiv1). GCP is
// yscale's third real-VM backend after Linode and AWS. Each burst is a
// single Compute Engine instance that joins the customer's cluster as
// a native kubelet node.
//
// Like Linode and AWS, a GCE instance boots a full distro image and the
// burst's join logic rides in the `startup-script` instance metadata
// key: the GCE guest agent runs it as root on first boot, with the
// per-burst values injected as shell exports prepended ahead of it. By
// default this installs containerd/kubelet/tailscale at boot (~3-5 min
// cold, bootstrap.sh) with no Cilium in the rootfs at all — so on a
// full-tier burst the node.cilium.io/agent-not-ready taint registered
// at kubelet start is never cleared and full-tier pods stay Pending
// forever (verified live 2026-08-01). YSCALE_GCP_IMAGE points a burst
// at a pre-baked image instead (pkg/backends/gcp/image) that carries
// host-mode Cilium binaries, and runs the join-only
// bootstrap-baked.sh. That path never registers the taint in the first
// place: it starts cilium-agent and waits for it BEFORE kubelet starts, so
// there is nothing to gate on — see selectImage.
//
// Structural difference from Linode/AWS: `instances.insert` returns a
// long-running zonal Operation, not a ready instance (gcp-plan.md §3).
// CreateNode uses the instance name it chose as backendID (deterministic,
// per gcp-plan.md §3) and only bounds its wait on the operation long
// enough to catch a synchronous rejection (bad machine type, quota
// denial); a real cold boot is left for GetNodeStatus to observe.
// Start/Stop/Delete are also async operations but fire-and-forget —
// GetNodeStatus is the source of truth for all of them (gcp-plan.md §4).
//
// GPU is NOT implemented: GCP's GPU ceiling (H100/H200) is the whole
// reason this backend is worth having, but GPU quota is unapproved on
// every yscale GCP project today (gcp-plan.md §13), so a sizing catalog
// here would be an unverifiable guess baked into provisioning code —
// worse than admitting the gap. See mapGPUType.
//
// Networking note: every burst is launched into the project's default
// VPC with an ephemeral external IP (gcp-plan.md §11) — enough for the
// bootstrap's `apt-get` and for `tailscale up` to reach the tailnet; the
// kubelet itself is reached over the tailnet, not the public IP.
package gcp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	_ "embed"

	compute "cloud.google.com/go/compute/apiv1"
	computepb "cloud.google.com/go/compute/apiv1/computepb"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/internal/agentenv"
)

const (
	// defaultRegion is used when neither a zone nor a region is configured.
	defaultRegion = "us-central1"
	// burstImage is the stock CPU image every burst boots absent a
	// YSCALE_GCP_IMAGE override — the always-latest Debian 12 family
	// image (gcp-plan.md §8). Unlike a Linode private Image or an AWS
	// AMI, a baked GCP image is a global resource needing no per-zone
	// or per-region replication — see image/build-image.sh and
	// selectImage.
	burstImage = "projects/debian-cloud/global/images/family/debian-12"

	// ownerLabelKey/ownerLabelValue are stamped on every instance
	// CreateNode provisions. Together they are the ONLY way cleanup
	// identifies a yscale instance — an exact label pair yscale alone
	// applies, never a name pattern — so CleanupOrphans can never touch
	// the customer's own resources sharing the project (porting
	// methodology §10, gcp-plan.md §10).
	ownerLabelKey   = "ys-owner"
	ownerLabelValue = "yscale-burst"
	// scopeLabelKey carries ScopeHash so multiple controllers sharing one
	// GCP project stay isolated: ownsInstance requires an EXACT match,
	// including the empty string, so an unscoped controller can never
	// adopt another controller's scoped bursts (interface.go's
	// ScopedBackend contract; mirrors flyio's "yscale-scope" metadata key).
	scopeLabelKey = "ys-scope"
	// burstIDLabelKey carries the central-assigned BurstID for
	// traceability; it plays no role in ownership decisions.
	burstIDLabelKey = "ys-burst-id"

	// createOperationWaitTimeout bounds how long CreateNode waits on the
	// insert operation before treating it as "still provisioning, ask
	// GetNodeStatus later." Quota/capacity/malformed-request errors surface
	// in operation.error almost immediately; a real cold boot can take
	// minutes, which CreateNode must not block on (gcp-plan.md §3).
	createOperationWaitTimeout = 20 * time.Second

	// zoneStatusUp is the status GCE reports for a zone that is accepting
	// work. DOWN marks a decommissioned or scheduled-down zone, not one
	// that is merely full — being full surfaces per-insert as
	// ZONE_RESOURCE_POOL_EXHAUSTED, which CreateNode already retries past.
	zoneStatusUp = "UP"
)

// errInstanceNotFound is returned by findOwnedInstance when no instance
// with the given name carries the exact owner label + current pool scope.
var errInstanceNotFound = errors.New("gcp: instance not found")

// bootstrapScript is the full-install bootstrap for stock Debian 12;
// bootstrapBakedScript is the join-only variant for a yscale baked
// image (image/install.sh — install layer already present). Both are
// delivered via the `startup-script` metadata key.
//
//go:embed bootstrap.sh
var bootstrapScript string

//go:embed bootstrap-baked.sh
var bootstrapBakedScript string

// Backend drives the Compute Engine instances API.
type Backend struct {
	project string
	// zone is the "home" zone bursts default to when a workload doesn't
	// pin a placement; region is derived from it (or vice versa) in New.
	zone   string
	region string
	client *compute.InstancesClient
	// zones discovers a region's real zone list. It is nil when the zones
	// client could not be constructed (see New): zone resolution degrades
	// to the guessed suffixes rather than failing the burst.
	zones zoneLister
	// newErr carries a credential/transport resolution failure from New.
	// NewInstancesRESTClient resolves credentials eagerly (unlike
	// Linode's bearer-token New(), which never touches the network), so
	// — matching AWS's "construct now, fail on use" shape — every method
	// checks newErr and fails loudly on first use instead of New()
	// returning an error or panicking.
	newErr error

	// poolScope scopes every instance op to an exact label match. Empty
	// owns only legacy unscoped instances; it is not a wildcard.
	poolScopeMu sync.RWMutex
	poolScope   string

	// zoneMu/zoneCache memoise per-region zone discovery: it sits on the
	// burst-create path and the answer is effectively static.
	zoneMu    sync.Mutex
	zoneCache map[string]zoneCacheEntry
}

// New constructs a GCP backend. zone is the GCP zone bursts default to
// when a workload doesn't pin one (e.g. "us-central1-a"); region, when
// zone is empty, resolves to that region's "-a" zone. Both empty default
// to defaultRegion. credentialsFile, when set, points the client at a
// service-account JSON key; empty defers to Application Default
// Credentials (gcloud login, GOOGLE_APPLICATION_CREDENTIALS, or the GCE
// metadata server), mirroring the AWS backend's default credential chain.
func New(projectID, zone, region, credentialsFile string) *Backend {
	zone, region = resolveZoneRegion(zone, region)

	var opts []option.ClientOption
	if credentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(credentialsFile))
	}
	client, err := compute.NewInstancesRESTClient(context.Background(), opts...)
	// The zones client is built the same way, but its failure deliberately
	// does NOT become newErr: zone discovery is a capacity optimization, so
	// losing it must degrade to the guessed suffix list rather than fail
	// every burst. Left nil on error so a nil *ZonesClient can never be
	// called through a non-nil interface.
	var zones zoneLister
	if zc, zerr := compute.NewZonesRESTClient(context.Background(), opts...); zerr == nil {
		zones = computeZoneLister{client: zc}
	}
	return &Backend{
		project:   projectID,
		zone:      zone,
		region:    region,
		client:    client,
		zones:     zones,
		newErr:    err,
		zoneCache: make(map[string]zoneCacheEntry),
	}
}

// resolveZoneRegion applies New's defaulting: an empty zone derives from
// region's "-a" zone (region itself defaulting to defaultRegion); an
// empty region derives from a given zone. Split out from New so the
// defaulting logic is testable without touching credentials/network.
func resolveZoneRegion(zone, region string) (string, string) {
	if zone == "" {
		if region == "" {
			region = defaultRegion
		}
		return region + "-a", region
	}
	if region == "" {
		region = regionFromZone(zone)
	}
	return zone, region
}

func (b *Backend) Name() string { return backends.TypeGCP }

// SetPoolScope sets the exact pool scope owned by this backend instance.
// An empty scope owns only legacy unscoped instances; it never sees
// scoped ones.
func (b *Backend) SetPoolScope(hash string) {
	b.poolScopeMu.Lock()
	defer b.poolScopeMu.Unlock()
	b.poolScope = hash
}

func (b *Backend) currentPoolScope() string {
	b.poolScopeMu.RLock()
	defer b.poolScopeMu.RUnlock()
	return b.poolScope
}

// mapGPUType is the deliberately unimplemented GPU sizing seam. GCP's
// GPU ceiling (H100/H200, gcp-plan.md §7) is the whole point of this
// backend, but GPU quota is unapproved on every yscale GCP project today
// (gcp-plan.md §13) — a Kind+Count -> (machineType, guestAccelerators)
// catalog that has never been exercised against a live quota grant would
// be an unverifiable guess baked into provisioning code, worse than
// admitting the gap here. Wire the real catalog once a project has
// approved on-demand + Spot GPU quota to test against.
func mapGPUType(gpu *backends.GPUSpec) (string, error) {
	return "", fmt.Errorf("gcp: GPU not yet supported on gcp")
}

// imageOverride resolves YSCALE_GCP_IMAGE, mirroring the "-" vs unset idiom
// Linode's burstImageRef and AWS's selectAMI use: unset means "no override
// configured", "-" means the same thing but explicitly. The two currently
// collapse to the same result — this backend ships no compiled-in default
// the way AWS's embedded us-east-1 AMI or Linode's private image IDs do,
// because a baked GCP image is built by a human running
// image/build-image.sh in their own project (gcp-plan.md has no "shared"
// image concept for GCP) — but the distinction is preserved so a future
// compiled-in default can't silently override an operator's explicit "-".
func imageOverride() string {
	switch v := strings.TrimSpace(os.Getenv("YSCALE_GCP_IMAGE")); v {
	case "", "-":
		return ""
	default:
		return v
	}
}

// selectImage returns the burst image + bootstrap script CreateNode uses,
// and a mode string for future caller logging. Absent YSCALE_GCP_IMAGE,
// this MUST stay the stock debian-12 image + the full-install
// bootstrap.sh — byte-identical to before baked GCP images existed — so
// an existing deployment sees no behavior change until it opts in.
func selectImage() (image, script, mode string) {
	if id := imageOverride(); id != "" {
		return id, bootstrapBakedScript, "baked-override"
	}
	return burstImage, bootstrapScript, "stock-debian-12"
}

func (b *Backend) CreateNode(ctx context.Context, spec *backends.NodeSpec) (string, error) {
	if b.newErr != nil {
		// Client-init failure happens before we can dispatch anything.
		return "", backends.MarkCreateProvenZeroResource(fmt.Errorf("gcp: client not initialized: %w", b.newErr))
	}
	if spec.Resources.GPU != nil {
		_, err := mapGPUType(spec.Resources.GPU)
		return "", backends.MarkCreateProvenZeroResource(err)
	}
	if spec.ScopeHash != b.currentPoolScope() {
		return "", backends.MarkCreateProvenZeroResource(
			fmt.Errorf("gcp: creating instance %s: scopeHash does not match backend pool scope", spec.Name))
	}

	machineType := mapType(spec.Resources)

	pinned := spec.Region
	if pinned == "" {
		pinned = spec.Resources.Region
	}
	zones, err := b.resolveZones(ctx, pinned)
	if err != nil {
		return "", backends.MarkCreateProvenZeroResource(err)
	}

	env := agentenv.AsMap(agentenv.Build(spec))
	image, script, mode := selectImage()
	_ = mode // useful for caller logging if it threads a logger
	startupScript := buildStartupScript(env, script)

	labels := map[string]string{
		ownerLabelKey: ownerLabelValue,
		scopeLabelKey: sanitizeLabelValue(spec.ScopeHash),
	}
	if spec.BurstID != "" {
		labels[burstIDLabelKey] = sanitizeLabelValue(spec.BurstID)
	}

	var lastErr error
	for _, zone := range zones {
		req := &computepb.InsertInstanceRequest{
			Project: b.project,
			Zone:    zone,
			InstanceResource: &computepb.Instance{
				Name:        proto.String(spec.Name),
				MachineType: proto.String(fmt.Sprintf("zones/%s/machineTypes/%s", zone, machineType)),
				Labels:      labels,
				Disks: []*computepb.AttachedDisk{{
					Boot:       proto.Bool(true),
					AutoDelete: proto.Bool(true),
					InitializeParams: &computepb.AttachedDiskInitializeParams{
						SourceImage: proto.String(image),
					},
				}},
				// Default VPC + an ephemeral external IP: zero scaffolding,
				// enough egress for apt-get and the tailnet join
				// (gcp-plan.md §11). Tailscale rides on top regardless.
				NetworkInterfaces: []*computepb.NetworkInterface{{
					Network: proto.String("global/networks/default"),
					AccessConfigs: []*computepb.AccessConfig{{
						Type: proto.String("ONE_TO_ONE_NAT"),
						Name: proto.String("external-nat"),
					}},
				}},
				Metadata: &computepb.Metadata{
					Items: []*computepb.Items{{
						Key:   proto.String("startup-script"),
						Value: proto.String(startupScript),
					}},
				},
			},
		}
		op, err := b.client.Insert(ctx, req)
		if err != nil {
			lastErr = err
			if isZoneExhausted(err) {
				continue
			}
			wrapped := fmt.Errorf("gcp: creating instance %s in %s: %w", spec.Name, zone, err)
			// A modeled 4xx from Insert proves the API refused the request
			// before an operation was created; server-side errors and
			// transport failures leave the outcome ambiguous.
			if gcpCreateRequestRefused(err) {
				return "", backends.MarkCreateProvenZeroResource(wrapped)
			}
			return "", wrapped
		}
		// Bound the wait: quota/capacity/malformed-request errors surface in
		// operation.error almost immediately, but a real cold boot can take
		// minutes. If our sub-wait times out while the caller's own ctx is
		// still alive, the operation is still running, not failed — treat it
		// as accepted and let GetNodeStatus reflect provisioning from here,
		// mirroring Linode's fire-and-return CreateNode shape.
		waitCtx, cancel := context.WithTimeout(ctx, createOperationWaitTimeout)
		waitErr := op.Wait(waitCtx)
		cancel()
		if waitErr != nil {
			if errors.Is(waitErr, context.DeadlineExceeded) && ctx.Err() == nil {
				return spec.Name, nil
			}
			lastErr = waitErr
			if isZoneExhausted(waitErr) {
				continue
			}
			wrapped := fmt.Errorf("gcp: creating instance %s in %s: %w", spec.Name, zone, waitErr)
			// A terminal (Done) operation error proves no instance exists:
			// GCE only bills the resource once the insert operation succeeds.
			// A non-terminal error (context cancellation observed by Wait
			// while the operation is still running) stays ambiguous.
			if op.Done() && gcpCreateRequestRefused(waitErr) {
				return "", backends.MarkCreateProvenZeroResource(wrapped)
			}
			return "", wrapped
		}
		return spec.Name, nil
	}
	// Every zone reported ZONE_RESOURCE_POOL_EXHAUSTED (a modeled 4xx). No
	// operation was accepted, so nothing bills.
	return "", backends.MarkCreateProvenZeroResource(
		fmt.Errorf("gcp: %s has no capacity right now across zones %v: %w", machineType, zones, lastErr))
}

func gcpCreateRequestRefused(err error) bool {
	var apiErr *apierror.APIError
	return errors.As(err, &apiErr) && backends.HTTPCreateResponseProvesNoResource(apiErr.HTTPCode())
}

// StartNode and StopNode fire-and-forget the async operation: per
// gcp-plan.md §4, GetNodeStatus is the source of truth for all of
// start/stop/delete, so there is nothing useful an in-line wait buys
// here beyond the immediate synchronous-rejection check.
func (b *Backend) StartNode(ctx context.Context, backendID string) error {
	if b.newErr != nil {
		return fmt.Errorf("gcp: client not initialized: %w", b.newErr)
	}
	zone, _, err := b.findOwnedInstance(ctx, backendID)
	if err != nil {
		return fmt.Errorf("gcp: authorizing start of instance %s: %w", backendID, err)
	}
	if _, err := b.client.Start(ctx, &computepb.StartInstanceRequest{
		Project: b.project, Zone: zone, Instance: backendID,
	}); err != nil {
		return fmt.Errorf("gcp: Start %s: %w", backendID, err)
	}
	return nil
}

func (b *Backend) StopNode(ctx context.Context, backendID string) error {
	if b.newErr != nil {
		return fmt.Errorf("gcp: client not initialized: %w", b.newErr)
	}
	zone, _, err := b.findOwnedInstance(ctx, backendID)
	if err != nil {
		return fmt.Errorf("gcp: authorizing stop of instance %s: %w", backendID, err)
	}
	if _, err := b.client.Stop(ctx, &computepb.StopInstanceRequest{
		Project: b.project, Zone: zone, Instance: backendID,
	}); err != nil {
		return fmt.Errorf("gcp: Stop %s: %w", backendID, err)
	}
	return nil
}

// DeleteNode destroys a GCE instance. It is idempotent to honor the
// at-least-once teardown contract (which can redeliver teardown
// requests): if the instance is already absent, it returns nil. Fails
// closed on unknown ownership — a backendID that resolves to a real
// instance outside our owner label + pool scope is never touched.
func (b *Backend) DeleteNode(ctx context.Context, backendID string) error {
	if b.newErr != nil {
		return fmt.Errorf("gcp: client not initialized: %w", b.newErr)
	}
	zone, _, err := b.findOwnedInstance(ctx, backendID)
	if errors.Is(err, errInstanceNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("gcp: authorizing delete of instance %s: %w", backendID, err)
	}
	return b.deleteInstanceInZone(ctx, zone, backendID)
}

// deleteInstanceInZone issues the delete once the caller has already
// resolved the zone (and, where required, verified ownership). Shared by
// DeleteNode and CleanupOrphans so the idempotent-404 handling lives in
// one place.
func (b *Backend) deleteInstanceInZone(ctx context.Context, zone, name string) error {
	_, err := b.client.Delete(ctx, &computepb.DeleteInstanceRequest{
		Project: b.project, Zone: zone, Instance: name,
	})
	if err != nil {
		if isNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("gcp: Delete %s: %w", name, err)
	}
	return nil
}

func (b *Backend) GetNodeStatus(ctx context.Context, backendID string) (*backends.NodeStatus, error) {
	if b.newErr != nil {
		return nil, fmt.Errorf("gcp: client not initialized: %w", b.newErr)
	}
	_, inst, err := b.findOwnedInstance(ctx, backendID)
	if err != nil {
		return nil, fmt.Errorf("gcp: instance %s: %w", backendID, err)
	}
	return &backends.NodeStatus{
		Phase:     mapStatus(inst.GetStatus()),
		IP:        instancePublicIP(inst),
		StartedAt: parseGCPTimestamp(inst.GetCreationTimestamp()),
	}, nil
}

// ListPooledNodes returns yscale-owned instances in the current pool
// scope that are stopped or (Spot-preempted) terminated — instances
// StopNode parked for fast restart. Per gcp-plan.md §4, a preempted Spot
// VM lands in the same TERMINATED state as a deliberate stop; the
// controller reconciles against its own intent.
func (b *Backend) ListPooledNodes(ctx context.Context) ([]backends.PooledNode, error) {
	if b.newErr != nil {
		return nil, fmt.Errorf("gcp: client not initialized: %w", b.newErr)
	}
	refs, err := b.listOwnedInstances(ctx)
	if err != nil {
		return nil, err
	}
	scope := b.currentPoolScope()
	pooled := make([]backends.PooledNode, 0)
	for _, ref := range refs {
		if !ownsInstance(ref.inst, scope) {
			continue
		}
		switch ref.inst.GetStatus() {
		case "TERMINATED", "STOPPED":
		default:
			continue
		}
		pooled = append(pooled, backends.PooledNode{
			BackendID: ref.inst.GetName(),
			Name:      ref.inst.GetName(),
			ScopeHash: ref.inst.GetLabels()[scopeLabelKey],
			CreatedAt: parseGCPTimestamp(ref.inst.GetCreationTimestamp()),
		})
	}
	return pooled, nil
}

// ListOwnedNodes returns only GCE instances carrying exact Yscale ownership
// labels in this backend's current scope. It is read-only provider inventory
// for central reconciliation.
func (b *Backend) ListOwnedNodes(ctx context.Context) ([]backends.OwnedNode, error) {
	if b.newErr != nil {
		return nil, fmt.Errorf("gcp: client not initialized: %w", b.newErr)
	}
	refs, err := b.listOwnedInstances(ctx)
	if err != nil {
		return nil, err
	}
	scope := b.currentPoolScope()
	owned := make([]backends.OwnedNode, 0)
	for _, ref := range refs {
		if !ownsInstance(ref.inst, scope) {
			continue
		}
		owned = append(owned, backends.OwnedNode{
			BackendID: ref.inst.GetName(),
			Name:      ref.inst.GetName(),
			BurstID:   ref.inst.GetLabels()[burstIDLabelKey],
			CreatedAt: parseGCPTimestamp(ref.inst.GetCreationTimestamp()),
		})
	}
	return owned, nil
}

// CleanupOrphans destroys yscale-created GCE instances in the current
// pool scope that the controller is no longer tracking. It identifies a
// yscale instance solely by the exact owner label CreateNode stamps —
// never a name pattern — so it can only ever touch instances yscale
// itself provisioned in this exact scope, never the customer's own
// resources sharing the project (porting methodology §10). Instances
// younger than backends.OrphanGracePeriod are left alone: untracked is
// indistinguishable from mid-create until the caller's record write lands.
func (b *Backend) CleanupOrphans(ctx context.Context, trackedIDs map[string]bool) (int, error) {
	if b.newErr != nil {
		return 0, fmt.Errorf("gcp: client not initialized: %w", b.newErr)
	}
	refs, err := b.listOwnedInstances(ctx)
	if err != nil {
		return 0, err
	}
	scope := b.currentPoolScope()
	now := time.Now()
	destroyed := 0
	for _, ref := range refs {
		if !ownsInstance(ref.inst, scope) {
			continue
		}
		name := ref.inst.GetName()
		if trackedIDs[name] {
			continue
		}
		// An absent or unparseable creationTimestamp yields the zero time,
		// which OrphanTooYoung treats as too young — fail safe toward
		// keeping the instance.
		if backends.OrphanTooYoung(parseGCPTimestamp(ref.inst.GetCreationTimestamp()), now) {
			continue
		}
		if err := b.deleteInstanceInZone(ctx, ref.zone, name); err == nil {
			destroyed++
		}
	}
	return destroyed, nil
}

// instanceRef pairs an instance with the zone aggregatedList found it
// in — instances.get/start/stop/delete are all zone-scoped, and an
// instance carries no zone field a caller can act on directly without
// first resolving it this way (gcp-plan.md §4).
type instanceRef struct {
	zone string
	inst *computepb.Instance
}

// listOwnedInstances returns every instance across every zone carrying
// the exact yscale owner label, regardless of pool scope — callers
// filter by scope themselves via ownsInstance. The owner-label filter is
// applied server-side (gcp-plan.md §10's suggested aggregatedList
// filter); scope and status matching stay client-side so a subtly wrong
// filter expression can only under-select, never mis-claim a resource.
func (b *Backend) listOwnedInstances(ctx context.Context) ([]instanceRef, error) {
	filter := fmt.Sprintf("labels.%s=%s", ownerLabelKey, ownerLabelValue)
	it := b.client.AggregatedList(ctx, &computepb.AggregatedListInstancesRequest{
		Project: b.project,
		Filter:  proto.String(filter),
	})
	var out []instanceRef
	for {
		pair, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcp: aggregatedList instances: %w", err)
		}
		if pair.Value == nil {
			continue
		}
		zone := strings.TrimPrefix(pair.Key, "zones/")
		for _, inst := range pair.Value.GetInstances() {
			out = append(out, instanceRef{zone: zone, inst: inst})
		}
	}
	return out, nil
}

// findOwnedInstance resolves an instance's zone by name among the
// caller's owned+scoped instances. Every zone-scoped mutating op
// (Start/Stop/Delete/GetNodeStatus) goes through this: since GCP gives
// no way to act on an instance without knowing its zone, the lookup
// this forces doubles as an ownership+scope re-check for free — a
// backendID that resolves to a real instance outside our label/scope is
// never acted on.
func (b *Backend) findOwnedInstance(ctx context.Context, name string) (string, *computepb.Instance, error) {
	refs, err := b.listOwnedInstances(ctx)
	if err != nil {
		return "", nil, err
	}
	scope := b.currentPoolScope()
	for _, ref := range refs {
		if ref.inst.GetName() != name {
			continue
		}
		if !ownsInstance(ref.inst, scope) {
			return "", nil, fmt.Errorf("instance %s is outside owned scope %q", name, scope)
		}
		return ref.zone, ref.inst, nil
	}
	return "", nil, errInstanceNotFound
}

// ownsInstance reports whether inst carries the exact yscale owner label
// AND the exact scope. Both checks are exact-match, including the empty
// scope: an unscoped backend (scope=="") claims only instances whose
// scope label is also empty, and can never adopt a scoped controller's
// bursts or vice versa (interface.go's ScopedBackend contract).
func ownsInstance(inst *computepb.Instance, scope string) bool {
	labels := inst.GetLabels()
	if labels[ownerLabelKey] != ownerLabelValue {
		return false
	}
	return labels[scopeLabelKey] == scope
}

// instancePublicIP prefers the ephemeral external NAT IP (the tailnet
// needs egress through it, gcp-plan.md §11) and falls back to the
// internal IP for the rare case a burst landed on a subnet with no
// external address.
func instancePublicIP(inst *computepb.Instance) string {
	for _, ni := range inst.GetNetworkInterfaces() {
		for _, ac := range ni.GetAccessConfigs() {
			if ip := ac.GetNatIP(); ip != "" {
				return ip
			}
		}
	}
	for _, ni := range inst.GetNetworkInterfaces() {
		if ip := ni.GetNetworkIP(); ip != "" {
			return ip
		}
	}
	return ""
}

// parseGCPTimestamp parses an RFC3339 creationTimestamp, returning the
// zero time for an empty or unparseable value rather than erroring —
// StartedAt is best-effort telemetry, not load-bearing.
func parseGCPTimestamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// mapStatus collapses a GCE instance status into our NodePhase, per the
// table in gcp-plan.md §4. A preempted Spot VM and a deliberately
// stopped one both land in TERMINATED — the caller (ListPooledNodes,
// the controller) reconciles against its own intent, not this mapping.
func mapStatus(status string) backends.NodePhase {
	switch status {
	case "RUNNING":
		return backends.NodeRunning
	case "PROVISIONING", "STAGING":
		return backends.NodeStarting
	case "STOPPING", "SUSPENDING", "SUSPENDED", "TERMINATED", "STOPPED":
		return backends.NodeStopped
	}
	return backends.NodeUnknown
}

// mapType picks the smallest GCP E2 machine type that fits the
// workload's memory, mirroring Linode's mapType shape (gcp-plan.md §6).
// Thresholds leave ~1GB+ headroom for the OS + kubelet + containerd —
// the node must fit the workload's request *plus* system overhead, or
// the pod stays Pending. e2-small (2GB) is the floor: e2-micro (1GB)
// leaves no workload headroom.
func mapType(r backends.ResourceRequirements) string {
	switch {
	case r.MemoryMB <= 1024:
		return "e2-small" // 2 GB
	case r.MemoryMB <= 3072:
		return "e2-medium" // 4 GB
	case r.MemoryMB <= 6144:
		return "e2-standard-2" // 8 GB
	case r.MemoryMB <= 14336:
		return "e2-standard-4" // 16 GB
	default:
		return "e2-standard-8" // 32 GB
	}
}

// MapType exposes the exact memory-to-machine-type mapping used by
// CreateNode. Central's pricing estimator (pricing.EstimateGCPCPU) mirrors
// these thresholds to predict per-hour cost before the VM is launched;
// keeping this as a thin wrapper ensures the estimator and the provider's
// actual launch selection cannot diverge silently (mirrors
// pkg/backends/azure.MapType, pkg/backends/linode.MapType).
func MapType(r backends.ResourceRequirements) string {
	return mapType(r)
}

// isZone reports whether s looks like a full GCP zone ("us-central1-a":
// a region plus a single-letter suffix) rather than a bare region
// ("us-central1"). Used to tell a hard zone pin from a region hint.
func isZone(s string) bool {
	parts := strings.Split(s, "-")
	return len(parts) >= 3 && len(parts[len(parts)-1]) == 1
}

// regionFromZone trims a zone's trailing "-<letter>" to derive its region.
func regionFromZone(zone string) string {
	i := strings.LastIndex(zone, "-")
	if i < 0 {
		return zone
	}
	return zone[:i]
}

// zoneLister is the zone-discovery seam: the single Compute call zone
// resolution makes. *compute.ZonesClient is the only real implementation;
// it is named as an interface so zone resolution is testable with no
// credentials and no network (the shape podSlotArbiter uses in
// central/internal/decider). It hands back the raw zones so every
// decision about them — UP/DOWN, region membership, ordering — stays on
// the tested side of the seam.
type zoneLister interface {
	listZones(ctx context.Context, project string) ([]*computepb.Zone, error)
}

// computeZoneLister is the live lister. It lists the project's zones
// unfiltered: the whole list is ~100 small objects on a single page, and
// a subtly wrong server-side filter expression silently under-selects,
// which here means dropping real capacity — the exact bug this discovery
// exists to fix.
type computeZoneLister struct{ client *compute.ZonesClient }

func (l computeZoneLister) listZones(ctx context.Context, project string) ([]*computepb.Zone, error) {
	it := l.client.List(ctx, &computepb.ListZonesRequest{Project: project})
	var out []*computepb.Zone
	for {
		zone, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcp: list zones: %w", err)
		}
		out = append(out, zone)
	}
	return out, nil
}

// zoneCacheEntry is one region's resolved candidate list and the moment
// it stops being reusable.
type zoneCacheEntry struct {
	zones   []string
	expires time.Time
}

const (
	// zoneCacheTTL bounds how long a region's discovered zone list is
	// reused. A region's zones change on the order of years and new ones
	// are announced months ahead, so this is not a correctness window: it
	// only decides how long a long-running central keeps a stale view
	// after a real change. Half a day costs nothing there and cuts the
	// lookup to twice a day per region on the burst-create path. A zone
	// flipping to DOWN does not argue for less either — DOWN is a
	// decommission signal, not an outage one.
	zoneCacheTTL = 12 * time.Hour
	// zoneCacheFailureTTL caches the fallback list after a failed lookup.
	// Without it the realistic permanent failure — a service account
	// lacking compute.zones.list — costs every burst a doomed round trip
	// on the hot path; with it, one a minute.
	zoneCacheFailureTTL = time.Minute
)

// fallbackZoneSuffixes is the pre-discovery guess, kept only for the
// fail-soft path. It is a wrong guess: GCP zone suffixes are not
// contiguous letters (us-central1 has a, b, c and f — no d or e;
// us-east1 has b, c, d and no a), so it can both miss real capacity and
// name zones that do not exist. It stays because three plausible zones
// beat provisioning nothing when discovery is unavailable.
var fallbackZoneSuffixes = []string{"a", "b", "c"}

func fallbackZones(region string) []string {
	out := make([]string, 0, len(fallbackZoneSuffixes))
	for _, suffix := range fallbackZoneSuffixes {
		out = append(out, region+"-"+suffix)
	}
	return out
}

// resolveZones returns the ordered zone candidates CreateNode should
// try. GCP has no Linode-style "is this plan placeable" pre-check
// (gcp-plan.md §9); the only signal is ZONE_RESOURCE_POOL_EXHAUSTED on
// the insert operation itself, so this returns a candidate list for
// CreateNode to retry down rather than pre-validating. A pinned zone is
// a hard constraint with no fallback, matching Linode's pinnedRegion
// contract. A pinned bare region, or nothing at all, expands to the
// zones GCP reports UP for that region, with the backend's configured
// zone tried first.
func (b *Backend) resolveZones(ctx context.Context, pinned string) ([]string, error) {
	if pinned != "" && isZone(pinned) {
		return []string{pinned}, nil
	}
	region := pinned
	if region == "" {
		region = b.region
	}
	if region == "" {
		return nil, fmt.Errorf("gcp: no region or zone configured")
	}

	candidates := b.zonesInRegion(ctx, region)
	ordered := make([]string, 0, len(candidates)+1)
	seen := make(map[string]bool, len(candidates)+1)
	// The configured zone leads even if discovery never reported it: an
	// operator pinning a zone outranks what we learned about the region.
	if b.zone != "" && regionFromZone(b.zone) == region {
		ordered = append(ordered, b.zone)
		seen[b.zone] = true
	}
	for _, z := range candidates {
		if !seen[z] {
			ordered = append(ordered, z)
			seen[z] = true
		}
	}
	return ordered, nil
}

// zonesInRegion returns the region's UP zones, sorted for a deterministic
// try order, memoised per region. It never errors: an unavailable lookup
// falls back to the guessed suffixes, because this is a capacity and
// latency optimization, not a safety boundary — trying three plausible
// zones is strictly better than provisioning nothing. (Contrast central's
// durable-state reads, which fail closed: acting on a partial view there
// hands two live bursts the same /24.)
func (b *Backend) zonesInRegion(ctx context.Context, region string) []string {
	b.zoneMu.Lock()
	entry, ok := b.zoneCache[region]
	b.zoneMu.Unlock()
	if ok && time.Now().Before(entry.expires) {
		return entry.zones
	}

	zones, err := b.discoverZones(ctx, region)
	ttl := zoneCacheTTL
	// An empty answer counts as a failed lookup, not as "this region has
	// no zones": caching it would leave CreateNode nothing to iterate and
	// no provider error to report for the rejection.
	if err != nil || len(zones) == 0 {
		zones = fallbackZones(region)
		ttl = zoneCacheFailureTTL
	}
	sort.Strings(zones)

	b.zoneMu.Lock()
	if b.zoneCache == nil {
		b.zoneCache = make(map[string]zoneCacheEntry, 1)
	}
	b.zoneCache[region] = zoneCacheEntry{zones: zones, expires: time.Now().Add(ttl)}
	b.zoneMu.Unlock()
	return zones
}

// discoverZones returns the zones GCP reports UP for region. A nil
// zoneLister (the zones client failed to construct in New) yields no
// zones and no error: the caller treats an empty list and a failure
// identically, so there is nothing for a sentinel to add.
func (b *Backend) discoverZones(ctx context.Context, region string) ([]string, error) {
	if b.zones == nil {
		return nil, nil
	}
	all, err := b.zones.listZones(ctx, b.project)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(all))
	for _, zone := range all {
		if zone.GetStatus() != zoneStatusUp {
			continue
		}
		// Match on the name's own region prefix rather than the zone's
		// region field: that field is a full resource URL, and deriving
		// the region here is the same trim every other caller does.
		if regionFromZone(zone.GetName()) != region {
			continue
		}
		out = append(out, zone.GetName())
	}
	return out, nil
}

// sanitizeLabelValue coerces s into a valid GCP label value: lowercase
// letters, digits, underscore, hyphen, at most 63 characters. Label
// values yscale stamps (ScopeHash, BurstID) already satisfy this, but a
// provider-facing field must not depend on an upstream format guarantee
// — sanitize defensively (gcp-plan.md §10).
func sanitizeLabelValue(s string) string {
	s = strings.ToLower(s)
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_', c == '-':
			out = append(out, c)
		default:
			out = append(out, '-')
		}
	}
	if len(out) > 63 {
		out = out[:63]
	}
	return string(out)
}

// isZoneExhausted reports whether err is GCP's signal that a zone has no
// spare capacity for the requested shape right now — the fallback-to-
// next-zone trigger (gcp-plan.md §9), not a hard failure.
func isZoneExhausted(err error) bool {
	var apiErr *apierror.APIError
	if errors.As(err, &apiErr) && apiErr.Reason() == "ZONE_RESOURCE_POOL_EXHAUSTED" {
		return true
	}
	return strings.Contains(err.Error(), "ZONE_RESOURCE_POOL_EXHAUSTED")
}

// isNotFoundErr reports whether a GCP error indicates the resource does
// not exist, for DeleteNode's idempotent-teardown contract.
func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *apierror.APIError
	if errors.As(err, &apiErr) {
		return apiErr.HTTPCode() == http.StatusNotFound
	}
	return strings.Contains(err.Error(), "notFound") || strings.Contains(err.Error(), "404")
}

// buildStartupScript renders the GCE startup-script metadata value: a
// #!/bin/bash header, the per-burst values as shell exports (sorted for
// determinism, empties skipped), then the given bootstrap body which
// consumes them as env vars. Ported from Linode's buildUserData; the
// `startup-script` metadata key takes plain text (no base64/gzip
// encoding step, unlike Linode/AWS's user-data field — gcp-plan.md §5),
// so this returns a string directly.
func buildStartupScript(env map[string]string, body string) string {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("#!/bin/bash\n")
	for _, k := range keys {
		if env[k] == "" {
			continue
		}
		fmt.Fprintf(&b, "export %s=%s\n", k, shellQuote(env[k]))
	}
	b.WriteString(body)
	return b.String()
}

// shellQuote single-quote-wraps a value so it is safe in an export
// line. Ported verbatim from Linode.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
