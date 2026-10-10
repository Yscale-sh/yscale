package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	armnetwork "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4"
)

// Network scaffolding naming + sizing (azure-plan.md §11). One VNet per
// region, reused across every burst placed there — the ranges never
// need to talk to each other or to the customer's cluster (bursts are
// island nodes reached over the tailnet), so the same private range is
// safe to reuse in every region's independent, unpeered VNet.
const (
	vnetAddressSpace    = "10.40.0.0/16"
	subnetAddressPrefix = "10.40.0.0/24"
	subnetResourceName  = "ys-subnet"
	// natGatewayIdleTimeoutMinutes is the NAT Gateway default; bursts hold
	// long-lived tailnet/API connections, not bursty short ones, so the
	// default is fine and there's no reason to tune it down.
	natGatewayIdleTimeoutMinutes = int32(4)
)

func vnetNameFor(location string) string          { return "ys-vnet-" + location }
func nsgNameFor(location string) string           { return "ys-nsg-" + location }
func natGatewayNameFor(location string) string    { return "ys-natgw-" + location }
func natGatewayPIPNameFor(location string) string { return "ys-natgw-pip-" + location }
func nicNameFor(vmName string) string             { return vmName + "-nic" }

// ensureNetwork idempotently provisions the shared per-region network
// scaffolding a burst's NIC needs to exist at all: a VNet, a subnet, an
// NSG, and a NAT Gateway (+ its own public IP) for egress, so no burst
// needs a per-VM public IP (azure-plan.md §11's recommended v0 model:
// "provision the RG + per-region VNet/subnet + NSG + NAT Gateway once,
// then per burst only create a NIC"). Every PUT here is create-or-update
// against a deterministic name, so two callers racing on a region's
// first burst converge safely without needing a distributed lock —
// netCache (azure.go) is purely a latency optimization on top of that.
func (b *Backend) ensureNetwork(ctx context.Context, location string) (string, error) {
	b.netMu.Lock()
	if subnetID, ok := b.netCache[location]; ok {
		b.netMu.Unlock()
		return subnetID, nil
	}
	b.netMu.Unlock()

	subnetID, err := b.ensureNetworkUncached(ctx, location)
	if err != nil {
		return "", err
	}
	b.netMu.Lock()
	b.netCache[location] = subnetID
	b.netMu.Unlock()
	return subnetID, nil
}

func (b *Backend) ensureNetworkUncached(ctx context.Context, location string) (string, error) {
	tags := map[string]*string{ownerTagKey: to.Ptr(ownerTagValue)}

	if err := b.ensureVNet(ctx, location, tags); err != nil {
		return "", fmt.Errorf("vnet: %w", err)
	}
	nsgID, err := b.ensureNSG(ctx, location, tags)
	if err != nil {
		return "", fmt.Errorf("nsg: %w", err)
	}
	natGatewayID, err := b.ensureNATGateway(ctx, location, tags)
	if err != nil {
		return "", fmt.Errorf("nat gateway: %w", err)
	}
	subnetID, err := b.ensureSubnet(ctx, location, nsgID, natGatewayID)
	if err != nil {
		return "", fmt.Errorf("subnet: %w", err)
	}
	return subnetID, nil
}

func (b *Backend) ensureVNet(ctx context.Context, location string, tags map[string]*string) error {
	poller, err := b.vnetClient.BeginCreateOrUpdate(ctx, b.resourceGroup, vnetNameFor(location), armnetwork.VirtualNetwork{
		Location: to.Ptr(location),
		Tags:     tags,
		Properties: &armnetwork.VirtualNetworkPropertiesFormat{
			AddressSpace: &armnetwork.AddressSpace{
				AddressPrefixes: []*string{to.Ptr(vnetAddressSpace)},
			},
		},
	}, nil)
	if err != nil {
		return err
	}
	_, err = poller.PollUntilDone(ctx, nil)
	return err
}

// ensureNSG creates the shared per-region NSG with no custom rules: the
// subnet's requirement is "allow outbound all; inbound can be fully
// closed" (azure-plan.md §11), which Azure's built-in default rules
// (DenyAllInBound, AllowVnetOutBound/AllowInternetOutBound) already
// provide with zero rules of our own — all burst control traffic
// (kubelet, cilium) rides the tailnet overlay, not the public interface.
func (b *Backend) ensureNSG(ctx context.Context, location string, tags map[string]*string) (string, error) {
	poller, err := b.nsgClient.BeginCreateOrUpdate(ctx, b.resourceGroup, nsgNameFor(location), armnetwork.SecurityGroup{
		Location:   to.Ptr(location),
		Tags:       tags,
		Properties: &armnetwork.SecurityGroupPropertiesFormat{},
	}, nil)
	if err != nil {
		return "", err
	}
	resp, err := poller.PollUntilDone(ctx, nil)
	if err != nil {
		return "", err
	}
	return stringValue(resp.ID), nil
}

// ensureNATGateway provisions the shared per-region NAT Gateway (+ its
// own Standard public IP) that gives every burst's NIC stable outbound
// egress without a per-VM public IP — the modern Azure-recommended
// egress path now that default outbound access is being retired for new
// VMs (azure-plan.md §11/§13).
func (b *Backend) ensureNATGateway(ctx context.Context, location string, tags map[string]*string) (string, error) {
	pipPoller, err := b.pipClient.BeginCreateOrUpdate(ctx, b.resourceGroup, natGatewayPIPNameFor(location), armnetwork.PublicIPAddress{
		Location: to.Ptr(location),
		Tags:     tags,
		SKU: &armnetwork.PublicIPAddressSKU{
			Name: to.Ptr(armnetwork.PublicIPAddressSKUNameStandard),
			Tier: to.Ptr(armnetwork.PublicIPAddressSKUTierRegional),
		},
		Properties: &armnetwork.PublicIPAddressPropertiesFormat{
			// Standard SKU public IPs require Static allocation.
			PublicIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodStatic),
			PublicIPAddressVersion:   to.Ptr(armnetwork.IPVersionIPv4),
		},
	}, nil)
	if err != nil {
		return "", fmt.Errorf("public ip: %w", err)
	}
	pipResp, err := pipPoller.PollUntilDone(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("public ip: %w", err)
	}

	natPoller, err := b.natClient.BeginCreateOrUpdate(ctx, b.resourceGroup, natGatewayNameFor(location), armnetwork.NatGateway{
		Location: to.Ptr(location),
		Tags:     tags,
		SKU:      &armnetwork.NatGatewaySKU{Name: to.Ptr(armnetwork.NatGatewaySKUNameStandard)},
		Properties: &armnetwork.NatGatewayPropertiesFormat{
			IdleTimeoutInMinutes: to.Ptr(natGatewayIdleTimeoutMinutes),
			PublicIPAddresses:    []*armnetwork.SubResource{{ID: pipResp.ID}},
		},
	}, nil)
	if err != nil {
		return "", err
	}
	natResp, err := natPoller.PollUntilDone(ctx, nil)
	if err != nil {
		return "", err
	}
	return stringValue(natResp.ID), nil
}

func (b *Backend) ensureSubnet(ctx context.Context, location, nsgID, natGatewayID string) (string, error) {
	poller, err := b.subnetClient.BeginCreateOrUpdate(ctx, b.resourceGroup, vnetNameFor(location), subnetResourceName, armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{
			AddressPrefix:        to.Ptr(subnetAddressPrefix),
			NetworkSecurityGroup: &armnetwork.SecurityGroup{ID: to.Ptr(nsgID)},
			NatGateway:           &armnetwork.SubResource{ID: to.Ptr(natGatewayID)},
		},
	}, nil)
	if err != nil {
		return "", err
	}
	resp, err := poller.PollUntilDone(ctx, nil)
	if err != nil {
		return "", err
	}
	return stringValue(resp.ID), nil
}

// createNIC provisions the per-burst NIC (the only per-burst network
// resource — azure-plan.md §11): no public IP, a dynamic private address
// in the shared subnet, egress via that subnet's NAT Gateway. Tagged
// like the VM so CleanupOrphans/an operator audit can find it
// independently of the VM (azure-plan.md §10); its lifecycle is still
// driven by the VM (DeleteOption=Delete on the VM's NetworkProfile
// reference, plus DeleteNode's explicit delete), never discovered by
// scanning NICs on its own.
func (b *Backend) createNIC(ctx context.Context, vmName, location, subnetID string, tags map[string]*string) (string, error) {
	poller, err := b.nicClient.BeginCreateOrUpdate(ctx, b.resourceGroup, nicNameFor(vmName), armnetwork.Interface{
		Location: to.Ptr(location),
		Tags:     tags,
		Properties: &armnetwork.InterfacePropertiesFormat{
			IPConfigurations: []*armnetwork.InterfaceIPConfiguration{{
				Name: to.Ptr("ipconfig1"),
				Properties: &armnetwork.InterfaceIPConfigurationPropertiesFormat{
					Subnet:                    &armnetwork.Subnet{ID: to.Ptr(subnetID)},
					PrivateIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodDynamic),
				},
			}},
		},
	}, nil)
	if err != nil {
		return "", err
	}
	resp, err := poller.PollUntilDone(ctx, nil)
	if err != nil {
		return "", err
	}
	return stringValue(resp.ID), nil
}

// deleteNICByID best-effort deletes a NIC by its full resource ID,
// fire-and-forget like GCP/Linode's delete calls — nothing downstream
// depends on this specific call completing before returning. A 404 (already
// gone, e.g. the VM's own cascade beat us to it) is not logged as a failure.
func (b *Backend) deleteNICByID(ctx context.Context, id string) {
	name := resourceNameFromID(id)
	if name == "" {
		return
	}
	if _, err := b.nicClient.BeginDelete(ctx, b.resourceGroup, name, nil); err != nil && !isNotFoundErr(err) {
		fmt.Printf("[azure] WARN: failed to delete NIC %s: %v\n", name, err)
	}
}

// deleteDiskByID best-effort deletes a managed disk by its full resource
// ID. Same fire-and-forget shape as deleteNICByID.
func (b *Backend) deleteDiskByID(ctx context.Context, id string) {
	name := resourceNameFromID(id)
	if name == "" {
		return
	}
	if _, err := b.diskClient.BeginDelete(ctx, b.resourceGroup, name, nil); err != nil && !isNotFoundErr(err) {
		fmt.Printf("[azure] WARN: failed to delete OS disk %s: %v\n", name, err)
	}
}
