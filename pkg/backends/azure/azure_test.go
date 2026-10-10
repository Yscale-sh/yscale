package azure

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	azfake "github.com/Azure/azure-sdk-for-go/sdk/azcore/fake"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	armcompute "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5"
	armcomputefake "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5/fake"

	"github.com/yscale-sh/yscale/pkg/backends"
)

// mapType picks the smallest Azure VM size that fits the workload's
// memory, mirroring azure-plan.md §6's table. A wrong threshold either
// starves the burst (kubelet/containerd can't fit) or silently
// overbills the customer for a bigger machine than requested.
func TestMapType(t *testing.T) {
	cases := []struct {
		name     string
		memoryMB int64
		want     string
	}{
		{"floor", 1, "Standard_B1ms"},
		{"at 512 boundary", 512, "Standard_B1ms"},
		{"just over 512", 513, "Standard_B2s_v2"},
		{"at 1536 boundary", 1536, "Standard_B2s_v2"},
		{"just over 1536", 1537, "Standard_D2s_v5"},
		{"at 3072 boundary", 3072, "Standard_D2s_v5"},
		{"just over 3072", 3073, "Standard_D4s_v5"},
		{"at 6144 boundary", 6144, "Standard_D4s_v5"},
		{"just over 6144", 6145, "Standard_D8s_v5"},
		{"large request", 65536, "Standard_D8s_v5"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mapType(backends.ResourceRequirements{MemoryMB: c.memoryMB})
			if got != c.want {
				t.Errorf("mapType(%d) = %q, want %q", c.memoryMB, got, c.want)
			}
		})
	}
}

// GPU is a deliberately unimplemented seam (Azure GPU quota is unapproved
// and unexercisable — see azure-plan.md §9/§13): CreateNode must reject a
// GPU request loudly and before touching the network, never guess a
// sizing catalog entry that has never been exercised. A zero-value
// Backend (no clients, no New()) proves no API call happens first.
func TestCreateNode_GPURejectedFast(t *testing.T) {
	b := &Backend{}
	_, err := b.CreateNode(context.Background(), &backends.NodeSpec{
		Name:    "ys-burst-x",
		BurstID: "burst_x",
		Resources: backends.ResourceRequirements{
			GPU: &backends.GPUSpec{Kind: "h100", Count: 8},
		},
	})
	if err == nil {
		t.Fatal("expected CreateNode to reject a GPU request, got nil")
	}
	if !strings.Contains(err.Error(), "GPU not yet supported on azure") {
		t.Errorf("error should name the gap plainly, got: %v", err)
	}
	if backends.CreateOutcomeAmbiguous(err) {
		t.Fatalf("pre-dispatch GPU validation must prove zero resource: %v", err)
	}
}

func TestAzureCreateRequestRefusedClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "client refusal", err: &azcore.ResponseError{StatusCode: http.StatusBadRequest}, want: true},
		{name: "conflict may already exist", err: &azcore.ResponseError{StatusCode: http.StatusConflict}, want: false},
		{name: "server failure", err: &azcore.ResponseError{StatusCode: http.StatusServiceUnavailable}, want: false},
		{name: "transport failure", err: errors.New("connection reset"), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := azureCreateRequestRefused(tc.err); got != tc.want {
				t.Fatalf("azureCreateRequestRefused = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMapGPUType_AlwaysErrors(t *testing.T) {
	_, err := mapGPUType(&backends.GPUSpec{Kind: "a100", Count: 1})
	if err == nil {
		t.Fatal("expected mapGPUType to always error, got nil")
	}
}

// mapStatus mirrors the table in azure-plan.md §4: PowerState takes
// priority over provisioningState wherever both could apply, except for
// the terminal Failed state.
func TestMapStatus(t *testing.T) {
	cases := []struct {
		name              string
		provisioningState string
		powerState        string
		want              backends.NodePhase
	}{
		{"running", "Succeeded", "running", backends.NodeRunning},
		{"starting power state", "Succeeded", "starting", backends.NodeStarting},
		{"provisioning creating", "Creating", "", backends.NodeStarting},
		{"provisioning updating", "Updating", "", backends.NodeStarting},
		{"deallocated", "Succeeded", "deallocated", backends.NodeStopped},
		{"deallocating", "Succeeded", "deallocating", backends.NodeStopped},
		{"stopped", "Succeeded", "stopped", backends.NodeStopped},
		{"stopping", "Succeeded", "stopping", backends.NodeStopped},
		{"provisioning failed", "Failed", "", backends.NodeFailed},
		{"provisioning failed with unknown power state", "Failed", "unknown", backends.NodeFailed},
		{"unknown power state alone", "Succeeded", "unknown", backends.NodeUnknown},
		{"nothing set", "", "", backends.NodeUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := mapStatus(c.provisioningState, c.powerState); got != c.want {
				t.Errorf("mapStatus(%q, %q) = %q, want %q", c.provisioningState, c.powerState, got, c.want)
			}
		})
	}
}

// ownsVM is the hard safety boundary CleanupOrphans/ListPooledNodes/
// getOwnedVM all delegate to: it must claim yscale's own VMs in the
// exact configured scope, and NOTHING else — a non-yscale VM sharing the
// resource group, or a yscale VM in a different controller's scope, must
// never be claimed (porting methodology §10).
func TestOwnsVM(t *testing.T) {
	strPtr := func(s string) *string { return &s }

	cases := []struct {
		name   string
		tags   map[string]*string
		scope  string
		claims bool
	}{
		{
			name:   "yscale VM, matching empty scope",
			tags:   map[string]*string{ownerTagKey: strPtr(ownerTagValue), scopeTagKey: strPtr("")},
			scope:  "",
			claims: true,
		},
		{
			name:   "yscale VM, matching non-empty scope",
			tags:   map[string]*string{ownerTagKey: strPtr(ownerTagValue), scopeTagKey: strPtr("abc123")},
			scope:  "abc123",
			claims: true,
		},
		{
			name:   "non-yscale VM (no owner tag) is never claimed",
			tags:   map[string]*string{"env": strPtr("prod")},
			scope:  "",
			claims: false,
		},
		{
			name:   "non-yscale VM with an unrelated owner value is never claimed",
			tags:   map[string]*string{ownerTagKey: strPtr("someone-else")},
			scope:  "",
			claims: false,
		},
		{
			name:   "wrong ScopeHash is never claimed by an empty-scope backend",
			tags:   map[string]*string{ownerTagKey: strPtr(ownerTagValue), scopeTagKey: strPtr("abc123")},
			scope:  "",
			claims: false,
		},
		{
			name:   "wrong ScopeHash is never claimed by a differently-scoped backend",
			tags:   map[string]*string{ownerTagKey: strPtr(ownerTagValue), scopeTagKey: strPtr("abc123")},
			scope:  "xyz789",
			claims: false,
		},
		{
			name:   "empty scope is not a wildcard over a scoped VM",
			tags:   map[string]*string{ownerTagKey: strPtr(ownerTagValue), scopeTagKey: strPtr("abc123")},
			scope:  "",
			claims: false,
		},
		{
			name:   "no tags at all is never claimed",
			tags:   nil,
			scope:  "",
			claims: false,
		},
		{
			name:   "nil scope tag value is treated as empty",
			tags:   map[string]*string{ownerTagKey: strPtr(ownerTagValue), scopeTagKey: nil},
			scope:  "",
			claims: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ownsVM(c.tags, c.scope); got != c.claims {
				t.Errorf("ownsVM(scope=%q) = %v, want %v", c.scope, got, c.claims)
			}
		})
	}
}

func TestOwnedNodeBurstIDComesFromExactTag(t *testing.T) {
	tags := map[string]*string{
		ownerTagKey:   to.Ptr(ownerTagValue),
		scopeTagKey:   to.Ptr(""),
		burstIDTagKey: to.Ptr("burst_exact"),
	}
	if !ownsVM(tags, "") {
		t.Fatal("fixture must be an owned VM")
	}
	if got := tagValue(tags, burstIDTagKey); got != "burst_exact" {
		t.Fatalf("BurstID tag = %q, want exact marker", got)
	}
	if got := tagValue(tags, "name"); got != "" {
		t.Fatalf("unexpected BurstID inference from non-marker tag: %q", got)
	}
}

func TestResourceNameFromID(t *testing.T) {
	cases := []struct {
		id   string
		want string
	}{
		{"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/networkInterfaces/ys-burst-x-nic", "ys-burst-x-nic"},
		{"no-slashes", ""},
		{"trailing/slash/", ""},
		{"", ""},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			if got := resourceNameFromID(c.id); got != c.want {
				t.Errorf("resourceNameFromID(%q) = %q, want %q", c.id, got, c.want)
			}
		})
	}
}

func TestIsNotFoundErr(t *testing.T) {
	if !isNotFoundErr(errors.New("azure: getting VM x: StatusCode: 404")) {
		t.Error("expected a StatusCode: 404 substring to be detected")
	}
	if !isNotFoundErr(errors.New("ResourceNotFound: the Resource 'x' was not found")) {
		t.Error("expected a notfound substring to be detected")
	}
	if isNotFoundErr(errors.New("StatusCode: 403 forbidden")) {
		t.Error("expected a 403 to not be treated as not-found")
	}
	if isNotFoundErr(nil) {
		t.Error("expected a nil error to not be not-found")
	}
}

func TestBuildCustomData(t *testing.T) {
	got := buildCustomData(map[string]string{
		"B_VAR": "b",
		"A_VAR": "a",
		"EMPTY": "",
	}, "echo done\n")

	if !strings.HasPrefix(got, "#!/bin/bash\n") {
		t.Errorf("script should start with a shebang, got: %q", got)
	}
	if !strings.Contains(got, "export A_VAR='a'\nexport B_VAR='b'\n") {
		t.Errorf("expected sorted, quoted exports, got: %q", got)
	}
	if strings.Contains(got, "EMPTY") {
		t.Errorf("empty-valued vars must be skipped, got: %q", got)
	}
	if !strings.HasSuffix(got, "echo done\n") {
		t.Errorf("expected the bootstrap body appended verbatim, got: %q", got)
	}
}

// StopNode calling deallocate (never powerOff) is the single most
// expensive mistake available in this port: a powerOff'd VM keeps
// billing full compute even while "stopped" (azure-plan.md §12). This
// drives a real armcompute.VirtualMachinesClient through an in-memory
// fake transport (github.com/.../armcompute/v5/fake) — no live API
// calls, no credentials, no network — and asserts BeginDeallocate fires
// and BeginPowerOff never does.
func TestStopNode_CallsDeallocateNotPowerOff(t *testing.T) {
	var deallocateCalled, powerOffCalled bool

	srv := armcomputefake.VirtualMachinesServer{
		Get: func(ctx context.Context, resourceGroupName, vmName string, options *armcompute.VirtualMachinesClientGetOptions) (resp azfake.Responder[armcompute.VirtualMachinesClientGetResponse], errResp azfake.ErrorResponder) {
			vm := armcompute.VirtualMachine{
				Name: to.Ptr(vmName),
				Tags: map[string]*string{
					ownerTagKey: to.Ptr(ownerTagValue),
					scopeTagKey: to.Ptr(""),
				},
			}
			resp.SetResponse(http.StatusOK, armcompute.VirtualMachinesClientGetResponse{VirtualMachine: vm}, nil)
			return
		},
		BeginDeallocate: func(ctx context.Context, resourceGroupName, vmName string, options *armcompute.VirtualMachinesClientBeginDeallocateOptions) (resp azfake.PollerResponder[armcompute.VirtualMachinesClientDeallocateResponse], errResp azfake.ErrorResponder) {
			deallocateCalled = true
			resp.SetTerminalResponse(http.StatusOK, armcompute.VirtualMachinesClientDeallocateResponse{}, nil)
			return
		},
		BeginPowerOff: func(ctx context.Context, resourceGroupName, vmName string, options *armcompute.VirtualMachinesClientBeginPowerOffOptions) (resp azfake.PollerResponder[armcompute.VirtualMachinesClientPowerOffResponse], errResp azfake.ErrorResponder) {
			powerOffCalled = true
			resp.SetTerminalResponse(http.StatusOK, armcompute.VirtualMachinesClientPowerOffResponse{}, nil)
			return
		},
	}

	transport := armcomputefake.NewVirtualMachinesServerTransport(&srv)
	client, err := armcompute.NewVirtualMachinesClient("sub", &azfake.TokenCredential{}, &arm.ClientOptions{
		ClientOptions: policy.ClientOptions{Transport: transport},
	})
	if err != nil {
		t.Fatalf("constructing fake VirtualMachinesClient: %v", err)
	}

	b := &Backend{resourceGroup: "rg", vmClient: client}
	if err := b.StopNode(context.Background(), "ys-burst-x"); err != nil {
		t.Fatalf("StopNode: %v", err)
	}
	if !deallocateCalled {
		t.Error("expected StopNode to call deallocate")
	}
	if powerOffCalled {
		t.Error("StopNode must never call powerOff — a powerOff'd VM keeps billing full compute (azure-plan.md §12)")
	}
}
