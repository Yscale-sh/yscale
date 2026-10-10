// Package linode implements the Backend interface against the Linode
// (Akamai Cloud) API v4. Linode is yscale's primary real-VM backend:
// each burst is a Linode instance that joins the customer's cluster as
// a native kubelet node.
//
// Unlike Fly (which boots an OCI image directly as the microVM), a
// Linode instance boots a full distro image. The burst's join logic
// rides in cloud-init user-data: a bootstrap script Linode's Metadata
// service runs on first boot, with the per-burst values injected as
// shell exports prepended ahead of it. (StackScripts were tried first
// but Linode's StackScript-deploy path proved unreliable.)
//
// v0 installs containerd/kubelet/tailscale at boot (~3-5 min cold).
// The fast path bakes a private Image so user-data only does the join.
package linode

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/internal/agentenv"
)

const (
	apiBase = "https://api.linode.com/v4"
	// burstImage is the stock distro a CPU burst boots. It must support
	// Linode's Metadata service so cloud-init runs our user-data. Stock
	// images get the full-install bootstrap (bootstrapScript).
	burstImage = "linode/debian12"
	// defaultGPUBurstImage is a baked private Image GPU bursts boot:
	// Debian 12 with the NVIDIA driver + container-toolkit, containerd
	// (nvidia runtime), and kubelet/CNI/tailscale pre-installed. Built by
	// gpu-setup.sh. Bursts on it get the join-only bootstrap
	// (bootstrapBakedScript) since the install layer is already present.
	//
	// A Linode "private/<id>" image is scoped to the ACCOUNT THAT BAKED IT, so
	// this default is unreachable from anyone else's token. Bake your own and
	// set YSCALE_LINODE_IMAGE_GPU — see gpuBurstImageRef.
	defaultGPUBurstImage = "private/38851039"
	// defaultCPUBurstImage is the baked private Image CPU bursts boot: Debian
	// 12 with containerd (runc), kubelet/CNI/tailscale, and the host-mode
	// Cilium binaries + eBPF assets baked in (built by cpu-setup.sh). CPU
	// bursts on it run host-mode cilium-agent that lifts their own
	// cilium-not-ready taint — instead of stock Debian + the in-cluster Cilium
	// DaemonSet, which crashloops in init on a Linode burst and never readies.
	// EMPTY until the image is baked + snapshotted; while empty, a CPU burst
	// falls back to stock Debian + full bootstrap (the old, DS-dependent path).
	// Set this to the "private/<id>" snapshot id once cpu-setup.sh has been run.
	// Baked 2026-06-03 via bake-cpu-image.sh (containerd+kubelet+host-mode cilium),
	// disk shrunk before imagize so it's ~5GB, not the 81GB raw plan disk. NOTE: an
	// in-VM `poweroff` triggers Linode's Lassie watchdog reboot, which corrupts the
	// bake (the prior image private/39152465 hung in "creating" 25h+ and was
	// deleted); the script now shuts down via the API instead. See bake-cpu-image.sh.
	// Account-scoped like the GPU image: override with YSCALE_LINODE_IMAGE_CPU.
	defaultCPUBurstImage = "private/39191583"
	// distroKernel boots an instance via the distribution's own
	// bootloader (and thus the distro's kernel) rather than a
	// Linode-supplied kernel. GPU bursts MUST use it: the baked NVIDIA
	// DKMS module is built against the Debian kernel, and Linode's own
	// kernels neither carry that module nor ship headers to build one.
	// Private-image instances otherwise default to a Linode kernel.
	distroKernel = "linode/grub2"
	// configWaitTimeout bounds the wait for a new instance's
	// auto-created boot config to appear.
	configWaitTimeout = 60 * time.Second
	// ownerTag is stamped on every instance CreateNode provisions. It is
	// the *only* way cleanup identifies a yscale instance — an exact tag
	// yscale alone applies, never a name pattern — so cleanup can never
	// touch the customer's own workloads sharing the account.
	ownerTag = "yscale-burst"
)

// gpuBurstImageRef and cpuBurstImageRef resolve which baked image a burst
// boots. Linode private images are scoped to the account that created them, so
// the compiled-in defaults only work for the account that baked them; every
// other operator bakes their own (gpu-setup.sh / bake-cpu-image.sh) and points
// yscale at it here. Env-first with the default as fallback keeps existing
// deployments byte-identical while making a fresh install possible at all.
//
// Setting the variable to "-" explicitly selects "no baked image", which for
// the CPU path falls back to stock Debian + the full-install bootstrap (the
// pre-baked-image behavior). It is not a "lite" mode: the removed lite tier
// used to imply bridge-CNI-only bursts, which is refused during workload
// admission. An empty/unset variable means "use the default", so "-" is the
// only way to express the difference.
func gpuBurstImageRef() string { return burstImageRef("YSCALE_LINODE_IMAGE_GPU", defaultGPUBurstImage) }
func cpuBurstImageRef() string { return burstImageRef("YSCALE_LINODE_IMAGE_CPU", defaultCPUBurstImage) }

func burstImageRef(envVar, fallback string) string {
	switch v := strings.TrimSpace(os.Getenv(envVar)); v {
	case "":
		return fallback
	case "-":
		return ""
	default:
		return v
	}
}

// bootstrapScript is the full-install bootstrap for stock Debian 12;
// bootstrapBakedScript is the join-only variant for yscale baked
// Images (install layer already present).
//
//go:embed bootstrap.sh
var bootstrapScript string

//go:embed bootstrap-baked.sh
var bootstrapBakedScript string

// Backend drives the Linode instances API.
type Backend struct {
	token    string
	region   string
	client   *http.Client
	cpuImage string
	gpuImage string
	// sshKey, when set, is added to root authorized_keys on every burst.
	sshKey string

	// CreateNode sits on the burst's pod-start critical path, so the
	// stable lookups it needs are cached: the Metadata-capable region
	// set (capabilities change ~never; TTL guards against the rare
	// rollout) and the burst firewall ID (created once per account).
	// Both caches self-heal: the firewall ID is invalidated when an
	// instance create fails (covers out-of-band deletion), the region
	// set simply expires.
	cacheMu     sync.Mutex
	metaRegions map[string]bool
	metaExpiry  time.Time
	burstFWID   int
}

// New constructs a Linode backend. region is the Linode region slug
// (e.g. "us-ord"); empty defaults to us-ord. sshKey (optional) is an SSH
// public key added to root on every burst for ops/debugging.
func New(token, region, sshKey string) *Backend {
	return NewWithConfig(token, Config{
		Region: region, SSHKey: sshKey,
		CPUImage: cpuBurstImageRef(), GPUImage: gpuBurstImageRef(),
	})
}

// Config is an account-scoped Linode configuration. Images are explicit: a
// blank CPU image selects stock Debian and a blank GPU image disables GPU
// creation. This prevents tenant tokens from inheriting platform-private image
// ids through process-global environment variables.
type Config struct {
	Region   string
	SSHKey   string
	CPUImage string
	GPUImage string
}

func NewWithConfig(token string, cfg Config) *Backend {
	region := cfg.Region
	if region == "" {
		region = "us-ord"
	}
	return &Backend{
		token:    token,
		region:   region,
		sshKey:   cfg.SSHKey,
		cpuImage: strings.TrimSpace(cfg.CPUImage),
		gpuImage: strings.TrimSpace(cfg.GPUImage),
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (b *Backend) Name() string { return backends.TypeLinode }

// ValidateAccount establishes the stable Linode account UUID and verifies the
// configured region supports Metadata, without exposing provider response
// bodies through its errors.
func (b *Backend) ValidateAccount(ctx context.Context) (string, error) {
	var account Account
	if err := b.doJSON(ctx, "GET", "/account", nil, &account); err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && (httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
			return "", errors.New("linode: credentials rejected")
		}
		return "", errors.New("linode: account validation unavailable")
	}
	if strings.TrimSpace(account.UUID) == "" {
		return "", errors.New("linode: account has no stable identity")
	}
	var region Region
	if err := b.doJSON(ctx, "GET", "/regions/"+b.region, nil, &region); err != nil {
		return "", errors.New("linode: region validation failed")
	}
	if region.ID != b.region || !hasTag(region.Capabilities, "Metadata") {
		return "", errors.New("linode: region does not support burst metadata")
	}
	return account.UUID, nil
}

func (b *Backend) CreateNode(ctx context.Context, spec *backends.NodeSpec) (string, error) {
	// Image + bootstrap selection:
	//   GPU                       → baked GPU Image + join-only bootstrap.
	//   CPU with baked image      → baked CPU Image + join-only bootstrap, so
	//                               host-mode Cilium lifts its own taint (no
	//                               crashlooping in-cluster DaemonSet dep).
	//   CPU without baked image   → stock Debian 12 + full-install bootstrap.
	//
	// The removed "lite" tier used to bypass Cilium at boot; workload admission
	// now refuses it, so every CPU spec that reaches here is full-tier and the
	// stock-image fallback only fires when no baked CPU image is configured.
	planType := mapType(spec.Resources)
	image := burstImage
	script := bootstrapScript
	gpu := spec.Resources.GPU != nil
	if gpu {
		gt, err := mapGPUType(spec.Resources.GPU)
		if err != nil {
			return "", backends.MarkCreateProvenZeroResource(err)
		}
		planType = gt
		image = b.gpuImage
		if image == "" {
			return "", backends.MarkCreateProvenZeroResource(errors.New("linode: tenant GPU bursts require a configured GPU image"))
		}
		script = bootstrapBakedScript
	} else if cpuImage := b.cpuImage; cpuImage != "" {
		image = cpuImage
		script = bootstrapBakedScript
	}
	// Prove the selected account-scoped image exists and is available before
	// starting shared firewall setup or any provider mutation. Public
	// linode/... images skip this lookup. Read-only pre-dispatch: any failure
	// here proves the paid create was never attempted.
	if err := b.preflightAccountImage(ctx, image); err != nil {
		return "", backends.MarkCreateProvenZeroResource(err)
	}
	// The firewall ensure is independent of region resolution; run it
	// concurrently so the pre-create API latency is max(the two), not
	// the sum. Both gate the create POST below.
	type fwResult struct {
		id  int
		err error
	}
	fwCh := make(chan fwResult, 1)
	go func() {
		id, ferr := b.ensureBurstFirewall(ctx)
		fwCh <- fwResult{id: id, err: ferr}
	}()

	pinnedRegion := spec.Region
	if pinnedRegion == "" {
		pinnedRegion = spec.Resources.Region
	}
	if spec.ModelVolume != "" {
		volume, err := b.volumeByLabel(ctx, spec.ModelVolume)
		if err != nil {
			return "", backends.MarkCreateProvenZeroResource(err)
		}
		if pinnedRegion != "" && pinnedRegion != volume.Region {
			return "", backends.MarkCreateProvenZeroResource(fmt.Errorf("linode: model volume %q is in region %q, but workload is pinned to %q", spec.ModelVolume, volume.Region, pinnedRegion))
		}
		pinnedRegion = volume.Region
	}
	regions, err := b.resolveRegion(ctx, planType, pinnedRegion)
	if err != nil {
		return "", backends.MarkCreateProvenZeroResource(err)
	}
	pass, err := randPassword()
	if err != nil {
		return "", backends.MarkCreateProvenZeroResource(err)
	}

	// Linode caps metadata.user_data at 16 KB (base64-DECODED). The bootstrap
	// cloud-init outgrew that, 400-ing every burst at creation. cloud-init
	// transparently gunzips gzip-compressed user-data, and the cap applies to
	// the gzipped bytes — so gzip first for ~3-5x headroom. See
	// LINODE_BURST_KNOWN_ISSUES.md.
	// BURST_GPU=1 is the signal bootstrap-baked.sh gates its GPU paths on
	// (the nvidia.com/gpu-not-ready device-plugin readiness taint — issue #39).
	// The baked GPU image carries the driver/plugin stack, so this is the only
	// env it needs to know it is on a GPU node. Mirrors aws.gpuBootstrapEnv.
	env := agentenv.AsMap(agentenv.Build(spec))
	if gpu {
		env["BURST_GPU"] = "1"
	}
	userData, err := encodeUserData(buildUserData(env, script))
	if err != nil {
		return "", backends.MarkCreateProvenZeroResource(err)
	}

	req := CreateInstanceRequest{
		Label:    spec.Name,
		Type:     planType,
		Image:    image,
		RootPass: pass,
		// GPU bursts are created unbooted so their boot config can be
		// switched to the distro kernel before first boot; CPU bursts
		// boot straight away.
		Booted: !gpu,
		// ownerTag marks this as yscale's; BurstID ties it to the
		// central record for traceability and cost reporting.
		Tags: []string{ownerTag, spec.BurstID},
		Metadata: &InstanceMetadata{
			UserData: userData,
		},
	}
	if b.sshKey != "" {
		req.AuthorizedKeys = []string{b.sshKey}
	}

	// Attach a Cloud Firewall at creation so the burst is protected from first
	// boot. Best-effort: a firewall API hiccup must not block bursting (the
	// instance is still mesh-only by design), but log loudly if it's missing.
	if fw := <-fwCh; fw.err != nil {
		fmt.Printf("[linode] WARN: burst firewall unavailable, creating UNFIREWALLED burst: %v\n", fw.err)
	} else {
		req.FirewallID = fw.id
	}

	var inst Instance
	for _, region := range regions {
		req.Region = region
		if err := b.doJSON(ctx, "POST", "/linode/instances", req, &inst); err == nil {
			break
		} else if isPlanRegionUnavailable(err) {
			// Linode's availability preflight can be stale. Keep the raw
			// provider response in logs, but never expose it to customers.
			fmt.Printf("[linode] create unavailable in %s for %s: %v\n", region, planType, err)
			continue
		} else {
			// A stale cached firewall ID (deleted out-of-band) 400s the create;
			// drop the cache so the next attempt re-ensures it.
			b.cacheMu.Lock()
			b.burstFWID = 0
			b.cacheMu.Unlock()
			// A modeled 4xx from the create endpoint proves the provider
			// refused it; anything else (5xx, transport failure) is ambiguous
			// because the provider may have committed the create before the
			// response reached us.
			wrapped := fmt.Errorf("creating linode instance %s: %w", spec.Name, err)
			var httpErr *HTTPError
			if errors.As(err, &httpErr) && backends.HTTPCreateResponseProvesNoResource(httpErr.StatusCode) {
				return "", backends.MarkCreateProvenZeroResource(wrapped)
			}
			return "", backends.MarkCreateAmbiguous(wrapped)
		}
	}
	if inst.ID == 0 {
		// Every candidate region was refused with a modeled availability 4xx;
		// no POST landed, so nothing exists to bill.
		if spec.Resources.GPU != nil {
			return "", backends.MarkCreateProvenZeroResource(
				fmt.Errorf("linode: %s has no capacity right now across %v", gpuCapacityDescription(spec.Resources.GPU), regions))
		}
		return "", backends.MarkCreateProvenZeroResource(
			fmt.Errorf("linode: %s has no capacity right now across %v", planType, regions))
	}
	if gpu {
		if err := b.bootDistroKernel(ctx, inst.ID, spec.ModelVolume); err != nil {
			// A machine EXISTS at this point. Best-effort cleanup does not
			// prove absence: the DELETE may itself have failed or been lost,
			// and the reaper is the backstop. Leave the outcome ambiguous.
			b.cleanupFailedCreate(inst.ID)
			return "", fmt.Errorf("linode: GPU burst %d: %w", inst.ID, err)
		}
	} else if spec.ModelVolume != "" {
		// CPU bursts boot immediately (Booted:true above), so there is no
		// pre-boot config to attach into — hotplug the volume onto the
		// running instance and let the bootstrap's device-wait loop catch it.
		if err := b.attachModelVolume(ctx, inst.ID, 0, spec.ModelVolume); err != nil {
			b.cleanupFailedCreate(inst.ID)
			return "", fmt.Errorf("linode: burst %d model volume: %w", inst.ID, err)
		}
	}
	return fmt.Sprint(inst.ID), nil
}

// CreateOutcomeAmbiguous is retained for compatibility with earlier callers
// (state, tests) that reach for the Linode-package classifier. It delegates
// to the provider-neutral shared classifier so central and every adapter
// derive the same boolean.
func CreateOutcomeAmbiguous(err error) bool { return backends.CreateOutcomeAmbiguous(err) }

// MarkCreateOutcomeAmbiguous is retained for compatibility with the same
// callers. It delegates to the shared explicit ambiguous marker.
func MarkCreateOutcomeAmbiguous(err error) error { return backends.MarkCreateAmbiguous(err) }

// cleanupFailedCreate best-effort destroys an instance whose post-create
// setup (kernel switch, volume attach, boot) failed, so a half-provisioned
// burst doesn't leak as an untracked, billing instance. Uses a fresh
// background context so it still runs if the caller's ctx was canceled;
// the reaper's CleanupOrphans is the longer-term backstop.
func (b *Backend) cleanupFailedCreate(id int) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := b.doJSON(ctx, "DELETE", fmt.Sprintf("/linode/instances/%d", id), nil, nil); err != nil {
		fmt.Printf("[linode] WARN: failed to clean up half-provisioned instance %d: %v\n", id, err)
	}
}

// bootDistroKernel switches a freshly-created (unbooted) instance's
// boot config to the distro kernel, then boots it. The boot job queues
// behind the still-running disk provisioning, so this returns without
// waiting for the burst to finish provisioning.
func (b *Backend) bootDistroKernel(ctx context.Context, id int, modelVolume string) error {
	cfgID, err := b.waitForConfigID(ctx, id)
	if err != nil {
		return err
	}
	if err := b.doJSON(ctx, "PUT",
		fmt.Sprintf("/linode/instances/%d/configs/%d", id, cfgID),
		map[string]string{"kernel": distroKernel}, nil); err != nil {
		return fmt.Errorf("setting distro kernel: %w", err)
	}
	// Attach the pre-seeded model volume into this boot config (after the
	// kernel PUT so the device addition isn't clobbered) so it is present
	// as a block device on first boot. The attach job queues behind disk
	// provisioning and ahead of the boot job, so the device is ready when
	// the bootstrap runs.
	if modelVolume != "" {
		if err := b.attachModelVolume(ctx, id, cfgID, modelVolume); err != nil {
			return fmt.Errorf("attaching model volume: %w", err)
		}
	}
	if err := b.doJSON(ctx, "POST",
		fmt.Sprintf("/linode/instances/%d/boot", id),
		map[string]int{"config_id": cfgID}, nil); err != nil {
		return fmt.Errorf("booting: %w", err)
	}
	return nil
}

// attachModelVolume looks up a Block Storage volume by label and attaches
// it to the instance. When configID > 0 the volume is added to that boot
// config's device map (present before first boot); otherwise it is
// hotplugged onto the running instance. The volume must already exist —
// operators seed it once — and live in the burst's region. It is RWO, so
// a workload using it must not run concurrently with another that does.
func (b *Backend) attachModelVolume(ctx context.Context, instID, configID int, label string) error {
	volID, err := b.volumeIDByLabel(ctx, label)
	if err != nil {
		return err
	}
	body := map[string]any{"linode_id": instID, "persist_across_boots": true}
	if configID > 0 {
		body["config_id"] = configID
	}
	if err := b.doJSON(ctx, "POST",
		fmt.Sprintf("/volumes/%d/attach", volID), body, nil); err != nil {
		return fmt.Errorf("attaching volume %q (%d) to instance %d: %w", label, volID, instID, err)
	}
	return nil
}

// volumeIDByLabel resolves a Block Storage volume's numeric id from its
// label via a server-side X-Filter query.
func (b *Backend) volumeIDByLabel(ctx context.Context, label string) (int, error) {
	volume, err := b.volumeByLabel(ctx, label)
	if err != nil {
		return 0, err
	}
	return volume.ID, nil
}

// volumeByLabel resolves a Block Storage volume by label, including the
// region needed to preflight an instance create before it can be billed.
func (b *Backend) volumeByLabel(ctx context.Context, label string) (Volume, error) {
	filter := fmt.Sprintf(`{"label":%q}`, label)
	var out listResponse[Volume]
	if err := b.getFiltered(ctx, "/volumes", filter, &out); err != nil {
		return Volume{}, fmt.Errorf("listing volumes: %w", err)
	}
	for _, v := range out.Data {
		if v.Label == label {
			return v, nil
		}
	}
	return Volume{}, fmt.Errorf("no Linode volume labeled %q (seed it first)", label)
}

// waitForConfigID polls until the instance's auto-created boot config
// exists and returns its id. The config appears within seconds of
// create — well before disk provisioning finishes — so poll at sub-
// second granularity: every tick shaved here is GPU-burst boot time.
func (b *Backend) waitForConfigID(ctx context.Context, id int) (int, error) {
	deadline := time.Now().Add(configWaitTimeout)
	for {
		var out listResponse[InstanceConfig]
		if err := b.doJSON(ctx, "GET",
			fmt.Sprintf("/linode/instances/%d/configs", id), nil, &out); err != nil {
			return 0, fmt.Errorf("listing configs: %w", err)
		}
		if len(out.Data) > 0 {
			return out.Data[0].ID, nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("boot config did not appear within %s", configWaitTimeout)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (b *Backend) StartNode(ctx context.Context, backendID string) error {
	return b.doJSON(ctx, "POST", "/linode/instances/"+backendID+"/boot", nil, nil)
}

func (b *Backend) StopNode(ctx context.Context, backendID string) error {
	return b.doJSON(ctx, "POST", "/linode/instances/"+backendID+"/shutdown", nil, nil)
}

// DeleteNode destroys a Linode instance. It is idempotent to honor the
// at-least-once teardown contract (which can redeliver teardown requests):
// if the instance is already absent (HTTP 404), it returns nil.
func (b *Backend) DeleteNode(ctx context.Context, backendID string) error {
	err := b.doJSON(ctx, "DELETE", "/linode/instances/"+backendID, nil, nil)
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
			return nil
		}
		return err
	}
	return nil
}

func (b *Backend) GetNodeStatus(ctx context.Context, backendID string) (*backends.NodeStatus, error) {
	var inst Instance
	if err := b.doJSON(ctx, "GET", "/linode/instances/"+backendID, nil, &inst); err != nil {
		return nil, err
	}
	ip := ""
	if len(inst.IPv4) > 0 {
		ip = inst.IPv4[0]
	}
	return &backends.NodeStatus{
		Phase:     mapStatus(inst.Status),
		IP:        ip,
		StartedAt: inst.Created.Time,
	}, nil
}

func (b *Backend) ListPooledNodes(ctx context.Context) ([]backends.PooledNode, error) {
	insts, err := b.listInstances(ctx)
	if err != nil {
		return nil, err
	}
	pooled := make([]backends.PooledNode, 0)
	for _, in := range insts {
		if !hasTag(in.Tags, ownerTag) {
			continue
		}
		if in.Status == "offline" {
			pooled = append(pooled, backends.PooledNode{BackendID: fmt.Sprint(in.ID), Name: in.Label})
		}
	}
	return pooled, nil
}

// ListOwnedNodes returns only Linode instances carrying the exact Yscale owner
// tag. It is read-only provider inventory for central reconciliation.
func (b *Backend) ListOwnedNodes(ctx context.Context) ([]backends.OwnedNode, error) {
	insts, err := b.listInstances(ctx)
	if err != nil {
		return nil, err
	}
	owned := make([]backends.OwnedNode, 0)
	for _, in := range insts {
		if !hasTag(in.Tags, ownerTag) {
			continue
		}
		owned = append(owned, backends.OwnedNode{
			BackendID: fmt.Sprint(in.ID),
			Name:      in.Label,
			BurstID:   burstIDFromTags(in.Tags),
			CreatedAt: in.Created.Time,
		})
	}
	return owned, nil
}

// CleanupOrphans destroys yscale-created Linode instances the controller
// is no longer tracking. It identifies a yscale instance solely by the
// exact ownerTag CreateNode stamps — never a name pattern — so it can
// only ever touch instances yscale itself provisioned, never the
// customer's own workloads sharing the account. Instances younger than
// backends.OrphanGracePeriod are left alone: untracked is
// indistinguishable from mid-create until the caller's record write lands.
func (b *Backend) CleanupOrphans(ctx context.Context, trackedIDs map[string]bool) (int, error) {
	insts, err := b.listInstances(ctx)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	destroyed := 0
	for _, in := range insts {
		id := fmt.Sprint(in.ID)
		if trackedIDs[id] || !hasTag(in.Tags, ownerTag) {
			continue
		}
		if backends.OrphanTooYoung(in.Created.Time, now) {
			continue
		}
		if err := b.DeleteNode(ctx, id); err == nil {
			destroyed++
		}
	}
	return destroyed, nil
}

// hasTag reports whether tags contains an exact match for want.
func hasTag(tags []string, want string) bool {
	return slices.Contains(tags, want)
}

func burstIDFromTags(tags []string) string {
	for _, tag := range tags {
		if tag != "" && tag != ownerTag {
			return tag
		}
	}
	return ""
}

// buildUserData renders the cloud-init user-data: a #!/bin/bash header,
// the per-burst values as shell exports (sorted for determinism, empties
// skipped), then the given bootstrap body which consumes them as env
// vars.
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

// shellQuote single-quote-wraps a value so it is safe in an export line.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// maxLinodeUserData is Linode's hard cap on metadata.user_data, measured on the
// base64-DECODED bytes. cloud-init transparently gunzips gzip-compressed
// user-data, so we gzip before base64 and the cap applies to the gzipped bytes.
const maxLinodeUserData = 16384

// encodeUserData gzips then base64-encodes cloud-init user-data. cloud-init
// detects the gzip magic bytes and decompresses transparently, so the script
// runs unchanged while the 16 KB Linode limit applies to the (much smaller)
// gzipped payload. Returns a clear error if even gzipped it exceeds the cap,
// rather than letting Linode 400 the create. See LINODE_BURST_KNOWN_ISSUES.md.
func encodeUserData(raw []byte) (string, error) {
	var gz bytes.Buffer
	w, _ := gzip.NewWriterLevel(&gz, gzip.BestCompression)
	if _, err := w.Write(raw); err != nil {
		return "", fmt.Errorf("gzip user_data: %w", err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("gzip user_data: %w", err)
	}
	if gz.Len() > maxLinodeUserData {
		return "", fmt.Errorf("cloud-init user_data is %d bytes gzipped, over Linode's %d-byte limit; slim bootstrap-baked.sh or switch to a curl-stub bootstrap", gz.Len(), maxLinodeUserData)
	}
	return base64.StdEncoding.EncodeToString(gz.Bytes()), nil
}

// burstFirewallLabel is the Cloud Firewall yscale attaches to every burst.
const burstFirewallLabel = "yscale-burst-fw"

// ensureBurstFirewall returns the id of the yscale burst Cloud Firewall,
// creating it once (idempotent by label) if absent. Rules: default-DROP inbound
// except SSH (key-only — the image disables password auth) and tailscale's
// direct-connection UDP port; ACCEPT all outbound (tailscale, image pulls, and
// segment uploads egress freely). All burst control traffic (kubelet, cilium)
// rides the tailnet overlay, not the public interface, so a tight public
// firewall doesn't impede the mesh — it just removes the public attack surface.
func (b *Backend) ensureBurstFirewall(ctx context.Context) (int, error) {
	b.cacheMu.Lock()
	cached := b.burstFWID
	b.cacheMu.Unlock()
	if cached != 0 {
		return cached, nil
	}

	var list listResponse[Firewall]
	if err := b.doJSON(ctx, "GET", "/networking/firewalls", nil, &list); err != nil {
		return 0, fmt.Errorf("list firewalls: %w", err)
	}
	for _, f := range list.Data {
		if f.Label == burstFirewallLabel {
			b.cacheMu.Lock()
			b.burstFWID = f.ID
			b.cacheMu.Unlock()
			return f.ID, nil
		}
	}
	anyV4, anyV6 := []string{"0.0.0.0/0"}, []string{"::/0"}
	req := CreateFirewallRequest{
		Label: burstFirewallLabel,
		Tags:  []string{ownerTag},
		Rules: FirewallRules{
			InboundPolicy:  "DROP",
			OutboundPolicy: "ACCEPT",
			Inbound: []FirewallRule{
				{Label: "ssh", Action: "ACCEPT", Protocol: "TCP", Ports: "22",
					Addresses: FirewallAddrs{IPv4: anyV4, IPv6: anyV6}},
				{Label: "tailscale-direct", Action: "ACCEPT", Protocol: "UDP", Ports: "41641",
					Addresses: FirewallAddrs{IPv4: anyV4, IPv6: anyV6}},
			},
		},
	}
	var fw Firewall
	if err := b.doJSON(ctx, "POST", "/networking/firewalls", req, &fw); err != nil {
		return 0, fmt.Errorf("create firewall: %w", err)
	}
	b.cacheMu.Lock()
	b.burstFWID = fw.ID
	b.cacheMu.Unlock()
	return fw.ID, nil
}

// preflightAccountImage confirms an account-scoped burst image (a private/...
// or shared/... ref) is deploy-ready before any paid mutation. Public
// linode/... images are always addressable and short-circuit as a no-op.
//
// Errors are sanitized: they name the image reference (which the operator
// configured and therefore already knows) but never wrap the provider's
// HTTPError, whose Body may echo tokens, error correlation ids, or account
// internals into customer-facing logs.
//
// Deliberately does NOT filter on the image's regions array or trigger a
// replication: Akamai's second-generation custom images deploy to any
// compatible region and replicas only affect placement speed, so consuming
// the regions field here would strand valid deployments (issue #16).
func (b *Backend) preflightAccountImage(ctx context.Context, imageRef string) error {
	if !strings.HasPrefix(imageRef, "private/") && !strings.HasPrefix(imageRef, "shared/") {
		return nil
	}
	var img image
	// The "/" in "private/38851039" is part of the single {imageId} path
	// parameter, not a path separator; %2F-escape it so the API routes the
	// GET to the exact image rather than a nested collection.
	if err := b.doJSON(ctx, "GET", "/images/"+url.PathEscape(imageRef), nil, &img); err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) {
			switch httpErr.StatusCode {
			case http.StatusNotFound:
				return fmt.Errorf("linode: burst image %q not found on this account", imageRef)
			case http.StatusUnauthorized, http.StatusForbidden:
				return fmt.Errorf("linode: burst image %q inaccessible with this token", imageRef)
			}
			return fmt.Errorf("linode: burst image %q lookup failed (status %d)", imageRef, httpErr.StatusCode)
		}
		return fmt.Errorf("linode: burst image %q lookup unavailable", imageRef)
	}
	if img.ID != imageRef {
		return fmt.Errorf("linode: burst image %q lookup returned mismatched id", imageRef)
	}
	if img.Status != "available" {
		switch img.Status {
		case "creating", "pending_upload":
			return fmt.Errorf("linode: burst image %q is not ready yet", imageRef)
		default:
			// Status is provider payload. Do not echo an unknown value: a
			// malformed response must not become a log-injection or secret-
			// reflection path.
			return fmt.Errorf("linode: burst image %q is not deployable", imageRef)
		}
	}
	return nil
}

func (b *Backend) listInstances(ctx context.Context) ([]Instance, error) {
	var out listResponse[Instance]
	if err := b.doJSON(ctx, "GET", "/linode/instances", nil, &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// mapStatus collapses Linode instance status into our NodePhase.
func mapStatus(status string) backends.NodePhase {
	switch status {
	case "running":
		return backends.NodeRunning
	case "provisioning", "booting", "rebooting", "migrating":
		return backends.NodeStarting
	case "offline", "shutting_down", "deleting":
		return backends.NodeStopped
	}
	return backends.NodeUnknown
}

// mapType picks the smallest Linode shared-CPU plan that fits the
// workload's memory. Thresholds leave ~1GB+ headroom for the OS,
// kubelet, and containerd — the node must fit the workload's request
// *plus* system overhead, or the pod stays Pending.
func mapType(r backends.ResourceRequirements) string {
	switch {
	case r.MemoryMB <= 512:
		return "g6-nanode-1" // 1GB
	case r.MemoryMB <= 1536:
		return "g6-standard-1" // 2GB
	case r.MemoryMB <= 3072:
		return "g6-standard-2" // 4GB
	case r.MemoryMB <= 6144:
		return "g6-standard-4" // 8GB
	default:
		return "g6-standard-6" // 16GB
	}
}

// MapType exposes the exact memory-to-plan mapping used by CreateNode.
// Central's pricing estimator (pricing.EstimateLinodeCPU) mirrors these
// thresholds to predict the per-hour cost before the node is launched;
// keeping this as a thin wrapper ensures the estimator and the provider's
// actual launch selection cannot diverge silently (see MapGPUType, which
// serves the same purpose for GPU plans).
func MapType(r backends.ResourceRequirements) string {
	return mapType(r)
}

// gpuNodeSystemCPUMillis and gpuNodeSystemMemoryMB are the conservative
// system headroom reserved on every GPU burst node for the kubelet,
// container runtime, device plugin, and OS. A workload is only placed
// on a plan whose raw capacity exceeds the request by at least this
// much; without it an exact-capacity match remains permanently Pending
// because the kubelet's allocatable is lower than the plan ceiling.
// The values match the ~1 GiB headroom policy used by the repository's
// VM backends.
const (
	gpuNodeSystemCPUMillis int64 = 250
	gpuNodeSystemMemoryMB  int64 = 1024
)

// mapGPUType resolves an abstract GPUSpec to a Linode GPU plan ID.
// Linode offers RTX-class GPUs only (no A100/H100): "rtx4000ada" (Ada
// generation, the cheaper inference card) and "rtx6000" (Quadro RTX
// 6000). An empty or "any" kind defaults to rtx4000ada, the cheapest
// GPU entry. For each GPU count the smallest plan is chosen — larger
// sizes only add CPU/RAM the burst rarely needs. When a workload asks for
// more CPU or memory, the smallest matching tier is selected. The workload
// request is inflated by GPU-node system headroom before comparison so the
// kubelet's allocatable can satisfy the pod. Note: GPU plans are
// region-limited; provisioning fails before create if no candidate has them.
func mapGPUType(gpu *backends.GPUSpec) (string, error) {
	kind := strings.ToLower(gpu.Kind)
	if kind == "" || kind == "any" {
		kind = "rtx4000ada"
	}
	count := max(gpu.Count, 1)

	needCPU := gpu.CPUMillis + gpuNodeSystemCPUMillis
	needMem := gpu.MemoryMB + gpuNodeSystemMemoryMB
	if needCPU < gpu.CPUMillis || needMem < gpu.MemoryMB {
		return "", fmt.Errorf("linode: workload request overflows with system headroom (%dm CPU, %dMB memory)", gpu.CPUMillis, gpu.MemoryMB)
	}

	switch kind {
	case "rtx4000ada", "rtx4000":
		variants := map[int][]gpuPlanVariant{
			1: {{"g2-gpu-rtx4000a1-s", 4000, 16384}, {"g2-gpu-rtx4000a1-m", 8000, 32768}, {"g2-gpu-rtx4000a1-l", 16000, 65536}, {"g2-gpu-rtx4000a1-xl", 32000, 131072}},
			2: {{"g2-gpu-rtx4000a2-s", 8000, 32768}, {"g2-gpu-rtx4000a2-m", 16000, 65536}},
			4: {{"g2-gpu-rtx4000a4-s", 32000, 131072}, {"g2-gpu-rtx4000a4-m", 48000, 200704}},
		}
		for _, variant := range variants[count] {
			if variant.cpuMillis >= needCPU && variant.memoryMB >= needMem {
				return variant.plan, nil
			}
		}
		if _, ok := variants[count]; !ok {
			return "", fmt.Errorf("linode: rtx4000ada supports 1, 2, or 4 GPUs, not %d", count)
		}
		return "", fmt.Errorf("linode: rtx4000ada x%d has no plan with at least %dm CPU and %dMB memory (including %dm/%dMB system headroom)", count, gpu.CPUMillis, gpu.MemoryMB, gpuNodeSystemCPUMillis, gpuNodeSystemMemoryMB)
	case "rtx6000":
		variants := map[int]gpuPlanVariant{
			1: {"g1-gpu-rtx6000-1", 8000, 32768},
			2: {"g1-gpu-rtx6000-2", 16000, 65536},
			3: {"g1-gpu-rtx6000-3", 20000, 98304},
			4: {"g1-gpu-rtx6000-4", 24000, 131072},
		}
		variant, ok := variants[count]
		if !ok {
			return "", fmt.Errorf("linode: rtx6000 supports 1-4 GPUs, not %d", count)
		}
		if variant.cpuMillis < needCPU || variant.memoryMB < needMem {
			return "", fmt.Errorf("linode: rtx6000 x%d has no plan with at least %dm CPU and %dMB memory (including %dm/%dMB system headroom)", count, gpu.CPUMillis, gpu.MemoryMB, gpuNodeSystemCPUMillis, gpuNodeSystemMemoryMB)
		}
		return variant.plan, nil
	}
	return "", fmt.Errorf("linode: unknown gpu kind %q (supported: rtx4000ada, rtx6000)", kind)
}

// MapGPUType exposes the exact Kind+Count to plan mapping used by CreateNode.
// Central uses it for pre-side-effect price admission; keeping this as a thin
// wrapper ensures admission and provider launch selection cannot diverge.
func MapGPUType(gpu *backends.GPUSpec) (string, error) {
	return mapGPUType(gpu)
}

type gpuPlanVariant struct {
	plan      string
	cpuMillis int64
	memoryMB  int64
}

func gpuCapacityDescription(gpu *backends.GPUSpec) string {
	kind := strings.ToLower(gpu.Kind)
	if kind == "" || kind == "any" {
		kind = "rtx4000ada"
	}
	return fmt.Sprintf("%s x%d", kind, max(gpu.Count, 1))
}

// resolveRegion returns every Linode region that can deploy planType right
// now, sorted with the configured region and its continent first. A non-empty
// pinnedRegion is a hard constraint. Every candidate must support the Metadata
// service required for the cloud-init burst bootstrap. It returns an error when
// no eligible region can currently place the plan, before any create request.
func (b *Backend) resolveRegion(ctx context.Context, planType, pinnedRegion string) ([]string, error) {
	var avail listResponse[RegionAvailability]
	if err := b.getFiltered(ctx, "/regions/availability?page_size=500",
		fmt.Sprintf(`{"plan":%q}`, planType), &avail); err != nil {
		return nil, fmt.Errorf("linode: checking region availability for %s: %w", planType, err)
	}
	meta, err := b.metadataRegions(ctx)
	if err != nil {
		return nil, err
	}
	candidates := make([]string, 0)
	for _, r := range avail.Data {
		if !r.Available || !meta[r.Region] || (pinnedRegion != "" && r.Region != pinnedRegion) {
			continue
		}
		candidates = append(candidates, r.Region)
	}
	if len(candidates) == 0 {
		if pinnedRegion != "" {
			return nil, fmt.Errorf("linode: plan %s has no capacity in pinned region %q", planType, pinnedRegion)
		}
		return nil, fmt.Errorf("linode: plan %s is not currently available in any Metadata-capable region", planType)
	}
	// Prefer the configured region when it has capacity. Then prefer a
	// region on the same continent; break ties deterministically by region id.
	sort.Slice(candidates, func(i, j int) bool {
		if pinnedRegion == "" {
			ci := candidates[i] == b.region
			cj := candidates[j] == b.region
			if ci != cj {
				return ci
			}
		}
		si := regionContinent(candidates[i]) == regionContinent(b.region)
		sj := regionContinent(candidates[j]) == regionContinent(b.region)
		if si != sj {
			return si
		}
		return candidates[i] < candidates[j]
	})
	return candidates, nil
}

func isPlanRegionUnavailable(err error) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusForbidden &&
		strings.Contains(strings.ToLower(string(httpErr.Body)), "plan you chose is not currently available in the selected region")
}

// metaRegionsTTL bounds the metadata-capable region cache. Region
// capabilities change on Linode's infrastructure rollout timescale
// (months), so an hour is conservative.
const metaRegionsTTL = time.Hour

// metadataRegions returns the set of region ids that support the
// Metadata service. cloud-init user-data only runs in these regions, so
// a burst placed elsewhere would never bootstrap. Cached: this sits on
// the burst create critical path and the answer is effectively static.
func (b *Backend) metadataRegions(ctx context.Context) (map[string]bool, error) {
	b.cacheMu.Lock()
	if b.metaRegions != nil && time.Now().Before(b.metaExpiry) {
		set := b.metaRegions
		b.cacheMu.Unlock()
		return set, nil
	}
	b.cacheMu.Unlock()

	var out listResponse[Region]
	if err := b.doJSON(ctx, "GET", "/regions?page_size=500", nil, &out); err != nil {
		return nil, fmt.Errorf("linode: listing regions: %w", err)
	}
	set := make(map[string]bool, len(out.Data))
	for _, r := range out.Data {
		if hasTag(r.Capabilities, "Metadata") {
			set[r.ID] = true
		}
	}
	b.cacheMu.Lock()
	b.metaRegions = set
	b.metaExpiry = time.Now().Add(metaRegionsTTL)
	b.cacheMu.Unlock()
	return set, nil
}

// regionContinent returns the leading token of a region id (e.g. "us"
// from "us-ord"), a rough continent grouping used to prefer a nearby
// fallback region.
func regionContinent(region string) string {
	if i := strings.IndexByte(region, '-'); i > 0 {
		return region[:i]
	}
	return region
}

// randPassword generates a Linode-acceptable root password (>=11 chars,
// 3+ character classes). The burst is never logged into with it — access
// is via the tailnet and the SSH key — but the API requires a password.
func randPassword() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	// "Ys1!" guarantees upper+lower+digit+symbol; the hex adds entropy.
	return "Ys1!" + hex.EncodeToString(b[:]), nil
}

// HTTPError represents an error returned by the Linode API.
type HTTPError struct {
	Op         string
	StatusCode int
	Body       []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("linode: %s: status %d: %s", e.Op, e.StatusCode, strings.TrimSpace(string(e.Body)))
}

func apiError(op string, resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	return &HTTPError{
		Op:         op,
		StatusCode: resp.StatusCode,
		Body:       body,
	}
}

func (b *Backend) newRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var r io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshaling request: %w", err)
		}
		r = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, apiBase+path, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+b.token)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

// doJSON performs a request and, when out is non-nil, decodes the JSON
// response into it.
func (b *Backend) doJSON(ctx context.Context, method, path string, body, out any) error {
	req, err := b.newRequest(ctx, method, path, body)
	if err != nil {
		return err
	}
	return b.execute(req, method+" "+path, out)
}

// getFiltered performs a GET carrying Linode's X-Filter header (a JSON
// query object) so the API returns only matching rows.
func (b *Backend) getFiltered(ctx context.Context, path, filter string, out any) error {
	req, err := b.newRequest(ctx, "GET", path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Filter", filter)
	return b.execute(req, "GET "+path, out)
}

// execute sends req and, when out is non-nil, decodes the JSON response
// into it. op is a "METHOD /path" label used only in error messages.
func (b *Backend) execute(req *http.Request, op string, out any) error {
	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("linode: %s: %w", op, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return apiError(op, resp)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("linode: decoding %s response: %w", op, err)
	}
	return nil
}
