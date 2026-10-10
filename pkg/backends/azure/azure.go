// Package azure implements the Backend interface against the Azure
// Resource Manager compute/network APIs (github.com/Azure/azure-sdk-for-go/sdk).
// Azure is yscale's fourth real-VM backend after Linode, AWS, and GCP.
// Each burst is a single Azure VM that joins the customer's cluster as
// a native kubelet node.
//
// Structural differences from Linode/AWS/GCP, per azure-plan.md:
//
//   - Every mutating call (VM/NIC/subnet/etc. create, start, deallocate,
//     delete) is a long-running ARM operation returned as a *runtime.Poller;
//     the SDK's PollUntilDone drives it, unlike GCP's zonal Operation
//     handle or Linode/AWS's synchronous create. CreateNode bounds its
//     wait the same way gcp.CreateNode does: a sub-wait deadline with the
//     caller's ctx still alive means "still provisioning," not failure
//     (azure-plan.md §3).
//   - A VM cannot exist without a NIC, and a NIC cannot exist without a
//     VNet/subnet — unlike Linode/AWS/GCP, which hand out a default
//     network for free. ensureNetwork (network.go) lazily provisions a
//     shared per-region VNet/subnet/NSG/NAT-Gateway once; only the NIC is
//     per-burst (azure-plan.md §11).
//   - StopNode calls `deallocate`, never `powerOff`: a powerOff'd VM still
//     bills full compute (azure-plan.md §12) — the single most expensive
//     mistake available in this port.
//   - DeleteNode/CleanupOrphans wait for the VM delete to finish, then
//     explicitly delete its NIC and OS disk. Both already carry
//     DeleteOption=Delete so Azure cascades them automatically, but an
//     explicit delete is defense in depth against a half-applied cascade
//     (azure-plan.md §10) — and the wait is required because Azure refuses
//     to delete a NIC/disk still attached to a live VM.
//
// GPU is NOT implemented: Azure reaches the A100/H100 ceiling (the whole
// reason this backend is worth having), but GPU quota is unapproved and
// unexercisable today (azure-plan.md §9/§13), so a sizing catalog here
// would be an unverifiable guess baked into provisioning code — worse
// than admitting the gap. See mapGPUType. CreateNode rejects a GPU spec
// before any API call, exactly like the GCP backend.
//
// Networking note: CPU bursts get no public IP. Egress (apt-get, the
// tailnet dial-out) rides a shared per-region NAT Gateway; the kubelet
// itself is reached over the tailnet, never a public address
// (azure-plan.md §11).
package azure

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	_ "embed"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	armcompute "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5"
	armnetwork "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/internal/agentenv"
)

const (
	// defaultLocation is used when no location is configured or pinned.
	defaultLocation = "eastus"
	// burstAdminUsername is the local admin account every burst VM gets.
	// Never logged into — access is via the tailnet — but OSProfile
	// requires one for a Linux VM without an SSH-key-only config, so this
	// mirrors Linode's randPassword() rationale: a required-but-unused
	// credential.
	burstAdminUsername = "ysadmin"

	// ownerTagKey/ownerTagValue are stamped on every VM (and its NIC)
	// CreateNode provisions. Together they are the ONLY way cleanup
	// identifies a yscale resource — an exact tag pair yscale alone
	// applies, never a name pattern — so CleanupOrphans can never touch
	// the customer's own resources sharing the resource group (porting
	// methodology §10, azure-plan.md §10).
	ownerTagKey   = "ys-owner"
	ownerTagValue = "yscale-burst"
	// scopeTagKey carries ScopeHash so multiple controllers sharing one
	// Azure resource group stay isolated: ownsVM requires an EXACT match,
	// including the empty string, so an unscoped controller can never
	// adopt another controller's scoped bursts (interface.go's
	// ScopedBackend contract; mirrors gcp's scopeLabelKey).
	scopeTagKey = "ys-scope"
	// burstIDTagKey carries the central-assigned BurstID for
	// traceability; it plays no role in ownership decisions.
	burstIDTagKey = "ys-burst-id"

	// createOperationWaitTimeout bounds how long CreateNode waits on the
	// VM PUT's long-running operation before treating it as "still
	// provisioning, ask GetNodeStatus later." Quota/capacity/malformed-
	// request errors surface almost immediately; a real cold boot can
	// take minutes, which CreateNode must not block on (azure-plan.md §3,
	// mirrors gcp.createOperationWaitTimeout).
	createOperationWaitTimeout = 20 * time.Second
)

// burstImagePublisher/Offer/SKU/Version identify the stock CPU image
// every burst boots: Canonical's Ubuntu 24.04 LTS, always-latest
// (azure-plan.md §8). No baked-image fast path yet — see linode/aws/gcp
// for that pattern once an Azure Compute Gallery image is baked.
const (
	burstImagePublisher = "Canonical"
	burstImageOffer     = "ubuntu-24_04-lts"
	burstImageSKU       = "server"
	burstImageVersion   = "latest"
)

// errVMNotFound is returned by getOwnedVM when no VM with the given name
// carries the exact owner tag + current pool scope.
var errVMNotFound = errors.New("azure: VM not found")

// bootstrapScript is the full-install bootstrap for stock Ubuntu 24.04,
// delivered via the VM's customData field.
//
//go:embed bootstrap.sh
var bootstrapScript string

// Backend drives the Azure Resource Manager compute + network APIs.
type Backend struct {
	subscriptionID string
	resourceGroup  string
	// location is the "home" Azure region bursts default to when a
	// workload doesn't pin one.
	location string

	vmClient     *armcompute.VirtualMachinesClient
	diskClient   *armcompute.DisksClient
	vnetClient   *armnetwork.VirtualNetworksClient
	subnetClient *armnetwork.SubnetsClient
	nsgClient    *armnetwork.SecurityGroupsClient
	nicClient    *armnetwork.InterfacesClient
	pipClient    *armnetwork.PublicIPAddressesClient
	natClient    *armnetwork.NatGatewaysClient

	// newErr carries a credential/client construction failure from New.
	// ClientSecretCredential resolves eagerly, so — matching AWS/GCP's
	// "construct now, fail on use" shape — every method checks newErr and
	// fails loudly on first use instead of New() returning an error.
	newErr error

	// poolScope scopes every VM op to an exact tag match. Empty owns only
	// legacy unscoped VMs; it is not a wildcard.
	poolScopeMu sync.RWMutex
	poolScope   string

	// netMu/netCache memoise the per-region shared network scaffolding
	// (VNet/subnet/NSG/NAT Gateway) ensureNetwork provisions. Resource
	// names are deterministic and every PUT is create-or-update, so a
	// cache miss racing another goroutine just re-verifies/re-creates
	// harmlessly — this cache is purely a latency optimization, not a
	// correctness lock (mirrors linode.ensureBurstFirewall's cacheMu).
	netMu    sync.Mutex
	netCache map[string]string
}

// New constructs an Azure backend from a service-principal secret
// (azure-plan.md §2). location is the Azure region bursts default to
// when a workload doesn't pin one (e.g. "eastus"); empty defaults to
// defaultLocation. resourceGroup is the yscale-owned resource group all
// bursts (and their network scaffolding) live in — it must already
// exist; this backend never creates or deletes it, mirroring how the
// GCP backend never creates its project.
func New(tenantID, clientID, clientSecret, subscriptionID, location, resourceGroup string) *Backend {
	if location == "" {
		location = defaultLocation
	}
	b := &Backend{
		subscriptionID: subscriptionID,
		resourceGroup:  resourceGroup,
		location:       location,
		netCache:       make(map[string]string),
	}

	cred, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
	if err != nil {
		b.newErr = fmt.Errorf("azure: building credential: %w", err)
		return b
	}

	var errs []error
	b.vmClient, err = armcompute.NewVirtualMachinesClient(subscriptionID, cred, nil)
	errs = append(errs, err)
	b.diskClient, err = armcompute.NewDisksClient(subscriptionID, cred, nil)
	errs = append(errs, err)
	b.vnetClient, err = armnetwork.NewVirtualNetworksClient(subscriptionID, cred, nil)
	errs = append(errs, err)
	b.subnetClient, err = armnetwork.NewSubnetsClient(subscriptionID, cred, nil)
	errs = append(errs, err)
	b.nsgClient, err = armnetwork.NewSecurityGroupsClient(subscriptionID, cred, nil)
	errs = append(errs, err)
	b.nicClient, err = armnetwork.NewInterfacesClient(subscriptionID, cred, nil)
	errs = append(errs, err)
	b.pipClient, err = armnetwork.NewPublicIPAddressesClient(subscriptionID, cred, nil)
	errs = append(errs, err)
	b.natClient, err = armnetwork.NewNatGatewaysClient(subscriptionID, cred, nil)
	errs = append(errs, err)
	if joined := errors.Join(errs...); joined != nil {
		b.newErr = fmt.Errorf("azure: building clients: %w", joined)
	}
	return b
}

func (b *Backend) Name() string { return backends.TypeAzure }

// SetPoolScope sets the exact pool scope owned by this backend instance.
// An empty scope owns only legacy unscoped VMs; it never sees scoped ones.
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

// mapGPUType is the deliberately unimplemented GPU sizing seam. Azure's
// GPU ceiling (A100/H100, azure-plan.md §7) is the best of any Tier B
// backend evaluated, but GPU quota defaults to zero on a fresh
// subscription and needs a support ticket to raise (azure-plan.md §9) —
// unapproved and unexercisable on every yscale Azure subscription today.
// A Kind+Count -> VM-size catalog that has never been exercised against
// a live quota grant would be an unverifiable guess baked into
// provisioning code — worse than admitting the gap here. Wire the real
// catalog once a subscription has approved GPU quota to test against.
func mapGPUType(gpu *backends.GPUSpec) (string, error) {
	return "", fmt.Errorf("azure: GPU not yet supported on azure")
}

func (b *Backend) CreateNode(ctx context.Context, spec *backends.NodeSpec) (string, error) {
	if b.newErr != nil {
		return "", backends.MarkCreateProvenZeroResource(fmt.Errorf("azure: client not initialized: %w", b.newErr))
	}
	// GPU is rejected before any API call — no network scaffolding, no
	// NIC, no VM PUT. Mirrors gcp.CreateNode exactly.
	if spec.Resources.GPU != nil {
		_, err := mapGPUType(spec.Resources.GPU)
		return "", backends.MarkCreateProvenZeroResource(err)
	}
	if spec.ScopeHash != b.currentPoolScope() {
		return "", backends.MarkCreateProvenZeroResource(
			fmt.Errorf("azure: creating VM %s: scopeHash does not match backend pool scope", spec.Name))
	}

	vmSize := mapType(spec.Resources)

	location := spec.Region
	if location == "" {
		location = spec.Resources.Region
	}
	if location == "" {
		location = b.location
	}
	if location == "" {
		return "", backends.MarkCreateProvenZeroResource(fmt.Errorf("azure: no location configured"))
	}

	subnetID, err := b.ensureNetwork(ctx, location)
	if err != nil {
		// No VM request has been dispatched. Shared network scaffolding may be
		// partially present, but there is no billable burst node to retain a
		// workload hold for.
		return "", backends.MarkCreateProvenZeroResource(
			fmt.Errorf("azure: ensuring network in %s: %w", location, err))
	}

	tags := map[string]*string{
		ownerTagKey: to.Ptr(ownerTagValue),
		scopeTagKey: to.Ptr(spec.ScopeHash),
	}
	if spec.BurstID != "" {
		tags[burstIDTagKey] = to.Ptr(spec.BurstID)
	}

	nicID, err := b.createNIC(ctx, spec.Name, location, subnetID, tags)
	if err != nil {
		return "", backends.MarkCreateProvenZeroResource(
			fmt.Errorf("azure: creating NIC for %s: %w", spec.Name, err))
	}

	password, err := randAdminPassword()
	if err != nil {
		b.deleteNICByID(context.WithoutCancel(ctx), nicID)
		return "", backends.MarkCreateProvenZeroResource(
			fmt.Errorf("azure: generating admin password: %w", err))
	}

	env := agentenv.AsMap(agentenv.Build(spec))
	// OSProfile.CustomData must already be base64-encoded (its own doc
	// comment: "a base-64 encoded string of custom data") — cloud-init
	// decodes it, unlike GCE's plain-text startup-script metadata key.
	customData := base64.StdEncoding.EncodeToString([]byte(buildCustomData(env, bootstrapScript)))

	vm := armcompute.VirtualMachine{
		Location: to.Ptr(location),
		Tags:     tags,
		Properties: &armcompute.VirtualMachineProperties{
			HardwareProfile: &armcompute.HardwareProfile{
				VMSize: to.Ptr(armcompute.VirtualMachineSizeTypes(vmSize)),
			},
			StorageProfile: &armcompute.StorageProfile{
				ImageReference: &armcompute.ImageReference{
					Publisher: to.Ptr(burstImagePublisher),
					Offer:     to.Ptr(burstImageOffer),
					SKU:       to.Ptr(burstImageSKU),
					Version:   to.Ptr(burstImageVersion),
				},
				OSDisk: &armcompute.OSDisk{
					CreateOption: to.Ptr(armcompute.DiskCreateOptionTypesFromImage),
					// Delete cascades the OS disk when the VM is deleted
					// (azure-plan.md §4/§10) — DeleteNode/CleanupOrphans
					// additionally delete it explicitly as defense in depth.
					DeleteOption: to.Ptr(armcompute.DiskDeleteOptionTypesDelete),
					ManagedDisk: &armcompute.ManagedDiskParameters{
						// The chosen VM sizes are all 's'-suffixed
						// (Premium-storage capable, azure-plan.md §6).
						StorageAccountType: to.Ptr(armcompute.StorageAccountTypesPremiumLRS),
					},
				},
			},
			OSProfile: &armcompute.OSProfile{
				ComputerName:  to.Ptr(spec.Name),
				AdminUsername: to.Ptr(burstAdminUsername),
				AdminPassword: to.Ptr(password),
				CustomData:    to.Ptr(customData),
			},
			NetworkProfile: &armcompute.NetworkProfile{
				NetworkInterfaces: []*armcompute.NetworkInterfaceReference{{
					ID: to.Ptr(nicID),
					Properties: &armcompute.NetworkInterfaceReferenceProperties{
						Primary: to.Ptr(true),
						// Delete cascades the NIC when the VM is deleted
						// (azure-plan.md §4/§10/§11).
						DeleteOption: to.Ptr(armcompute.DeleteOptionsDelete),
					},
				}},
			},
		},
	}

	poller, err := b.vmClient.BeginCreateOrUpdate(ctx, b.resourceGroup, spec.Name, vm, nil)
	if err != nil {
		wrapped := fmt.Errorf("azure: creating VM %s: %w", spec.Name, err)
		if azureCreateRequestRefused(err) {
			b.deleteNICByID(context.WithoutCancel(ctx), nicID)
			return "", backends.MarkCreateProvenZeroResource(wrapped)
		}
		// Transport, 5xx, and response-decode failures may follow an accepted
		// PUT. Keep both the classification and NIC for reconciliation.
		return "", wrapped
	}
	// Bound the wait: quota/capacity/malformed-request errors surface
	// almost immediately, but a real cold boot can take minutes. If our
	// sub-wait times out while the caller's own ctx is still alive, the
	// operation is still running, not failed — treat it as accepted and
	// let GetNodeStatus reflect provisioning from here (mirrors
	// gcp.CreateNode's bounded-wait shape exactly).
	waitCtx, cancel := context.WithTimeout(ctx, createOperationWaitTimeout)
	_, waitErr := poller.PollUntilDone(waitCtx, nil)
	cancel()
	if waitErr != nil {
		if errors.Is(waitErr, context.DeadlineExceeded) && ctx.Err() == nil {
			return spec.Name, nil
		}
		wrapped := fmt.Errorf("azure: creating VM %s: %w", spec.Name, waitErr)
		if poller.Done() && azureCreateRequestRefused(waitErr) {
			b.deleteNICByID(context.WithoutCancel(ctx), nicID)
			return "", backends.MarkCreateProvenZeroResource(wrapped)
		}
		// A polling/transport/cancellation error before a terminal ARM state
		// leaves the accepted PUT unresolved. Do not delete its NIC out from
		// under a VM that may still be provisioning.
		return "", wrapped
	}
	return spec.Name, nil
}

func azureCreateRequestRefused(err error) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && backends.HTTPCreateResponseProvesNoResource(responseErr.StatusCode)
}

// StartNode fires the async start operation and returns without waiting
// for it to complete — GetNodeStatus is the source of truth, mirroring
// gcp.StartNode's rationale (azure-plan.md §4).
func (b *Backend) StartNode(ctx context.Context, backendID string) error {
	if b.newErr != nil {
		return fmt.Errorf("azure: client not initialized: %w", b.newErr)
	}
	if _, err := b.getOwnedVM(ctx, backendID); err != nil {
		return fmt.Errorf("azure: authorizing start of VM %s: %w", backendID, err)
	}
	if _, err := b.vmClient.BeginStart(ctx, b.resourceGroup, backendID, nil); err != nil {
		return fmt.Errorf("azure: starting VM %s: %w", backendID, err)
	}
	return nil
}

// StopNode calls deallocate, never powerOff: a powerOff'd VM is billed
// full compute even while "stopped" (azure-plan.md §12) — the single
// most expensive mistake available in this port. Fire-and-forget like
// StartNode; GetNodeStatus is the source of truth.
func (b *Backend) StopNode(ctx context.Context, backendID string) error {
	if b.newErr != nil {
		return fmt.Errorf("azure: client not initialized: %w", b.newErr)
	}
	if _, err := b.getOwnedVM(ctx, backendID); err != nil {
		return fmt.Errorf("azure: authorizing stop of VM %s: %w", backendID, err)
	}
	if _, err := b.vmClient.BeginDeallocate(ctx, b.resourceGroup, backendID, nil); err != nil {
		return fmt.Errorf("azure: deallocating VM %s: %w", backendID, err)
	}
	return nil
}

// DeleteNode destroys an Azure VM and its NIC + OS disk. It is
// idempotent to honor the at-least-once teardown contract (which can
// redeliver teardown requests): if the VM is already absent, it returns
// nil. Fails closed on unknown ownership — a backendID that resolves to
// a real VM outside our owner tag + pool scope is never touched.
func (b *Backend) DeleteNode(ctx context.Context, backendID string) error {
	if b.newErr != nil {
		return fmt.Errorf("azure: client not initialized: %w", b.newErr)
	}
	vm, err := b.getOwnedVM(ctx, backendID)
	if errors.Is(err, errVMNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("azure: authorizing delete of VM %s: %w", backendID, err)
	}
	return b.deleteVMAndResources(ctx, backendID, vm)
}

// deleteVMAndResources deletes a VM the caller has already authorized,
// then its NIC(s) and OS disk. Unlike Start/Stop/GetNodeStatus, this
// WAITS for the VM delete to finish before touching the NIC/disk: Azure
// refuses to delete a network interface or managed disk still attached
// to a live VM, so the ordering is load-bearing, not stylistic — this is
// the one place this backend departs from GCP's fire-and-forget
// mutating-call shape. NIC/disk carry DeleteOption=Delete so Azure
// cascades them on its own, but deleting them explicitly too is defense
// in depth against a half-applied cascade (azure-plan.md §10) — the
// single named hard requirement for this port besides deallocate.
func (b *Backend) deleteVMAndResources(ctx context.Context, name string, vm armcompute.VirtualMachine) error {
	nicIDs := nicIDsOf(vm)
	diskID := osDiskIDOf(vm)

	poller, err := b.vmClient.BeginDelete(ctx, b.resourceGroup, name, nil)
	if err != nil {
		if isNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("azure: deleting VM %s: %w", name, err)
	}
	if _, err := poller.PollUntilDone(ctx, nil); err != nil {
		return fmt.Errorf("azure: waiting for VM %s to delete: %w", name, err)
	}

	for _, nicID := range nicIDs {
		b.deleteNICByID(ctx, nicID)
	}
	if diskID != "" {
		b.deleteDiskByID(ctx, diskID)
	}
	return nil
}

func (b *Backend) GetNodeStatus(ctx context.Context, backendID string) (*backends.NodeStatus, error) {
	if b.newErr != nil {
		return nil, fmt.Errorf("azure: client not initialized: %w", b.newErr)
	}
	vm, err := b.getOwnedVM(ctx, backendID)
	if err != nil {
		return nil, fmt.Errorf("azure: VM %s: %w", backendID, err)
	}
	var provisioningState string
	var statuses []*armcompute.InstanceViewStatus
	var createdAt time.Time
	if vm.Properties != nil {
		provisioningState = stringValue(vm.Properties.ProvisioningState)
		if vm.Properties.InstanceView != nil {
			statuses = vm.Properties.InstanceView.Statuses
		}
		if vm.Properties.TimeCreated != nil {
			createdAt = *vm.Properties.TimeCreated
		}
	}
	return &backends.NodeStatus{
		Phase:     mapStatus(provisioningState, extractPowerState(statuses)),
		IP:        b.vmPrivateIP(ctx, vm),
		StartedAt: createdAt,
	}, nil
}

// ListPooledNodes returns yscale-owned VMs in the current pool scope
// that are deallocated — VMs StopNode parked for fast restart
// (azure-plan.md §4). The plain VM list doesn't carry power state (Azure
// only allows the instanceView $expand alongside a VMSS $filter, per the
// SDK's own VirtualMachinesClientListOptions.Filter doc comment — not
// useful for standalone bursts), so each tag-owned candidate needs one
// extra Get to check it.
func (b *Backend) ListPooledNodes(ctx context.Context) ([]backends.PooledNode, error) {
	if b.newErr != nil {
		return nil, fmt.Errorf("azure: client not initialized: %w", b.newErr)
	}
	vms, err := b.listOwnedVMs(ctx)
	if err != nil {
		return nil, err
	}
	scope := b.currentPoolScope()
	pooled := make([]backends.PooledNode, 0)
	for _, vm := range vms {
		if !ownsVM(vm.Tags, scope) {
			continue
		}
		name := stringValue(vm.Name)
		full, err := b.getOwnedVM(ctx, name)
		if err != nil {
			continue
		}
		var statuses []*armcompute.InstanceViewStatus
		if full.Properties != nil && full.Properties.InstanceView != nil {
			statuses = full.Properties.InstanceView.Statuses
		}
		if extractPowerState(statuses) != "deallocated" {
			continue
		}
		var createdAt time.Time
		if full.Properties != nil && full.Properties.TimeCreated != nil {
			createdAt = *full.Properties.TimeCreated
		}
		pooled = append(pooled, backends.PooledNode{
			BackendID: name,
			Name:      name,
			ScopeHash: tagValue(vm.Tags, scopeTagKey),
			CreatedAt: createdAt,
		})
	}
	return pooled, nil
}

// ListOwnedNodes returns only Azure VMs carrying exact Yscale ownership tags in
// this backend's current scope. It is read-only provider inventory for central
// reconciliation.
func (b *Backend) ListOwnedNodes(ctx context.Context) ([]backends.OwnedNode, error) {
	if b.newErr != nil {
		return nil, fmt.Errorf("azure: client not initialized: %w", b.newErr)
	}
	vms, err := b.listOwnedVMs(ctx)
	if err != nil {
		return nil, err
	}
	scope := b.currentPoolScope()
	owned := make([]backends.OwnedNode, 0)
	for _, vm := range vms {
		if !ownsVM(vm.Tags, scope) {
			continue
		}
		var createdAt time.Time
		if vm.Properties != nil && vm.Properties.TimeCreated != nil {
			createdAt = *vm.Properties.TimeCreated
		}
		name := stringValue(vm.Name)
		owned = append(owned, backends.OwnedNode{
			BackendID: name,
			Name:      name,
			BurstID:   tagValue(vm.Tags, burstIDTagKey),
			CreatedAt: createdAt,
		})
	}
	return owned, nil
}

// CleanupOrphans destroys yscale-created Azure VMs (and their NIC + OS
// disk) in the current pool scope that the controller is no longer
// tracking. It identifies a yscale VM solely by the exact owner tag
// CreateNode stamps — never a name pattern — so it can only ever touch
// VMs yscale itself provisioned in this exact scope, never the
// customer's own resources sharing the resource group (porting
// methodology §10). VMs younger than backends.OrphanGracePeriod are left
// alone: untracked is indistinguishable from mid-create until the
// caller's record write lands.
func (b *Backend) CleanupOrphans(ctx context.Context, trackedIDs map[string]bool) (int, error) {
	if b.newErr != nil {
		return 0, fmt.Errorf("azure: client not initialized: %w", b.newErr)
	}
	vms, err := b.listOwnedVMs(ctx)
	if err != nil {
		return 0, err
	}
	scope := b.currentPoolScope()
	now := time.Now()
	destroyed := 0
	for _, vm := range vms {
		if !ownsVM(vm.Tags, scope) {
			continue
		}
		name := stringValue(vm.Name)
		if trackedIDs[name] {
			continue
		}
		// Properties.TimeCreated needs api-version >= 2021-11-01 (the SDK
		// pins 2024-03-01), so a nil here means a VM predating that, not a
		// projection this listing omits. Nil reads as the zero time, which
		// OrphanTooYoung treats as too young — fail safe toward keeping it.
		var createdAt time.Time
		if vm.Properties != nil && vm.Properties.TimeCreated != nil {
			createdAt = *vm.Properties.TimeCreated
		}
		if backends.OrphanTooYoung(createdAt, now) {
			continue
		}
		if err := b.deleteVMAndResources(ctx, name, vm); err == nil {
			destroyed++
		}
	}
	return destroyed, nil
}

// listOwnedVMs returns every VM in the resource group, full properties
// included (Tags, NetworkProfile, StorageProfile — everything except
// InstanceView, which needs a per-VM $expand). Unlike gcp's
// aggregatedList, armcompute's list Filter only accepts a VMSS-id
// expression (see VirtualMachinesClientListOptions), so there is no
// server-side tag filter to apply here — every RG VM is fetched and
// ownsVM does the filtering client-side. Listing is already RG-scoped,
// which narrows the blast radius (azure-plan.md §10); the tag check
// remains the hard gate.
func (b *Backend) listOwnedVMs(ctx context.Context) ([]armcompute.VirtualMachine, error) {
	var out []armcompute.VirtualMachine
	pager := b.vmClient.NewListPager(b.resourceGroup, nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azure: listing VMs in %s: %w", b.resourceGroup, err)
		}
		for _, vm := range page.Value {
			if vm != nil {
				out = append(out, *vm)
			}
		}
	}
	return out, nil
}

// getOwnedVM resolves a VM by name, with its instance view expanded, and
// verifies it carries the exact owner tag + current pool scope. Every
// mutating op (Start/Stop/Delete/GetNodeStatus) goes through this: a
// backendID that resolves to a real VM outside our tag/scope is never
// acted on (mirrors gcp.findOwnedInstance's authorize-then-act shape).
func (b *Backend) getOwnedVM(ctx context.Context, name string) (armcompute.VirtualMachine, error) {
	resp, err := b.vmClient.Get(ctx, b.resourceGroup, name, &armcompute.VirtualMachinesClientGetOptions{
		Expand: to.Ptr(armcompute.InstanceViewTypesInstanceView),
	})
	if err != nil {
		if isNotFoundErr(err) {
			return armcompute.VirtualMachine{}, errVMNotFound
		}
		return armcompute.VirtualMachine{}, fmt.Errorf("getting VM %s: %w", name, err)
	}
	scope := b.currentPoolScope()
	if !ownsVM(resp.Tags, scope) {
		return armcompute.VirtualMachine{}, fmt.Errorf("VM %s is outside owned scope %q", name, scope)
	}
	return resp.VirtualMachine, nil
}

// ownsVM reports whether a VM's tags carry the exact yscale owner tag
// AND the exact scope. Both checks are exact-match, including the empty
// scope: an unscoped backend (scope=="") claims only VMs whose scope tag
// is also empty, and can never adopt a scoped controller's bursts or
// vice versa (interface.go's ScopedBackend contract; mirrors
// gcp.ownsInstance).
func ownsVM(tags map[string]*string, scope string) bool {
	if tagValue(tags, ownerTagKey) != ownerTagValue {
		return false
	}
	return tagValue(tags, scopeTagKey) == scope
}

// tagValue safely dereferences an Azure tag value, treating a missing or
// nil entry as empty.
func tagValue(tags map[string]*string, key string) string {
	if v, ok := tags[key]; ok && v != nil {
		return *v
	}
	return ""
}

func stringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// nicIDsOf returns the resource IDs of every NIC attached to vm.
func nicIDsOf(vm armcompute.VirtualMachine) []string {
	if vm.Properties == nil || vm.Properties.NetworkProfile == nil {
		return nil
	}
	var ids []string
	for _, ref := range vm.Properties.NetworkProfile.NetworkInterfaces {
		if ref != nil && ref.ID != nil {
			ids = append(ids, *ref.ID)
		}
	}
	return ids
}

// osDiskIDOf returns vm's managed OS disk resource ID, or "" if unset.
func osDiskIDOf(vm armcompute.VirtualMachine) string {
	if vm.Properties == nil || vm.Properties.StorageProfile == nil {
		return ""
	}
	disk := vm.Properties.StorageProfile.OSDisk
	if disk == nil || disk.ManagedDisk == nil {
		return ""
	}
	return stringValue(disk.ManagedDisk.ID)
}

// vmPrivateIP resolves the private IP of a VM's primary NIC, best-effort
// (an extra NIC Get). Every burst in the v0 network model (azure-plan.md
// §11) has only a private address — egress rides the shared NAT
// Gateway, not a per-burst public IP — so this is the only address there
// is to report. A lookup failure returns "": StartedAt/IP are best-effort
// telemetry, not load-bearing (mirrors gcp.parseGCPTimestamp's rationale).
func (b *Backend) vmPrivateIP(ctx context.Context, vm armcompute.VirtualMachine) string {
	ids := nicIDsOf(vm)
	if len(ids) == 0 {
		return ""
	}
	nicName := resourceNameFromID(ids[0])
	if nicName == "" {
		return ""
	}
	resp, err := b.nicClient.Get(ctx, b.resourceGroup, nicName, nil)
	if err != nil || resp.Properties == nil {
		return ""
	}
	for _, cfg := range resp.Properties.IPConfigurations {
		if cfg == nil || cfg.Properties == nil {
			continue
		}
		if ip := stringValue(cfg.Properties.PrivateIPAddress); ip != "" {
			return ip
		}
	}
	return ""
}

// extractPowerState returns the "PowerState/*" suffix from a VM's
// instance-view statuses (e.g. "running", "deallocated"), or "" if none
// is present.
func extractPowerState(statuses []*armcompute.InstanceViewStatus) string {
	for _, s := range statuses {
		if s == nil || s.Code == nil {
			continue
		}
		if state, ok := strings.CutPrefix(*s.Code, "PowerState/"); ok {
			return state
		}
	}
	return ""
}

// mapStatus collapses Azure's provisioningState + PowerState signals
// into our NodePhase, per the table in azure-plan.md §4.
func mapStatus(provisioningState, powerState string) backends.NodePhase {
	switch powerState {
	case "running":
		return backends.NodeRunning
	case "starting":
		return backends.NodeStarting
	case "deallocated", "deallocating", "stopped", "stopping":
		return backends.NodeStopped
	}
	switch provisioningState {
	case "Creating", "Updating":
		return backends.NodeStarting
	case "Failed":
		return backends.NodeFailed
	}
	return backends.NodeUnknown
}

// mapType picks the smallest Azure VM size that fits the workload's
// memory, mirroring Linode/GCP's mapType shape (azure-plan.md §6).
// Thresholds leave ~1GB+ headroom for the OS + kubelet + containerd — the
// node must fit the workload's request *plus* system overhead, or the
// pod stays Pending. Standard_B1ms (2GB) is the floor: Azure's smaller
// B-series burstable sizes leave no workload headroom.
func mapType(r backends.ResourceRequirements) string {
	switch {
	case r.MemoryMB <= 512:
		return "Standard_B1ms" // 1 vCPU / 2 GB
	case r.MemoryMB <= 1536:
		return "Standard_B2s_v2" // 2 vCPU / 4 GB
	case r.MemoryMB <= 3072:
		return "Standard_D2s_v5" // 2 vCPU / 8 GB
	case r.MemoryMB <= 6144:
		return "Standard_D4s_v5" // 4 vCPU / 16 GB
	default:
		return "Standard_D8s_v5" // 8 vCPU / 32 GB
	}
}

// MapType exposes the exact memory-to-size mapping used by CreateNode.
// Central's pricing estimator (pricing.EstimateAzureCPU) mirrors these
// thresholds to predict per-hour cost before the VM is launched; keeping
// this as a thin wrapper ensures the estimator and the provider's actual
// launch selection cannot diverge silently.
func MapType(r backends.ResourceRequirements) string { return mapType(r) }

// resourceNameFromID trims an ARM resource ID down to its trailing name
// segment (e.g. ".../networkInterfaces/ys-burst-x-nic" -> "ys-burst-x-nic").
func resourceNameFromID(id string) string {
	i := strings.LastIndex(id, "/")
	if i < 0 || i == len(id)-1 {
		return ""
	}
	return id[i+1:]
}

// isNotFoundErr reports whether an Azure error indicates the resource
// does not exist, for DeleteNode's idempotent-teardown contract.
func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == http.StatusNotFound
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "statuscode: 404") || strings.Contains(msg, "notfound")
}

// randAdminPassword generates an Azure-acceptable local admin password
// (12+ chars, 3+ of the 4 character classes). The burst is never logged
// into with it — access is via the tailnet — but OSProfile requires one
// absent an SSH-key-only config. Mirrors linode.randPassword.
func randAdminPassword() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	// "Ys1!" guarantees upper+lower+digit+symbol; the hex adds entropy.
	return "Ys1!" + hex.EncodeToString(buf[:]), nil
}

// buildCustomData renders the VM's customData: a #!/bin/bash header, the
// per-burst values as shell exports (sorted for determinism, empties
// skipped), then the given bootstrap body which consumes them as env
// vars. Unlike Linode's user_data, Azure's customData has no documented
// hard size cap this backend needs to defend against at v0's script size
// (azure-plan.md §5's 64KB ceiling is generous), so no gzip step.
// Base64-encoding happens in armcompute.OSProfile.CustomData's *string
// contract, not here — the field doc requires the value already encoded,
// so CreateNode base64-encodes buildCustomData's output before setting it.
func buildCustomData(env map[string]string, body string) string {
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

// shellQuote single-quote-wraps a value so it is safe in an export line.
// Ported verbatim from Linode/GCP.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
