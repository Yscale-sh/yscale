package azure

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	azfake "github.com/Azure/azure-sdk-for-go/sdk/azcore/fake"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	armcompute "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5"
	armnetwork "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4"

	"github.com/yscale-sh/yscale/pkg/backends"
)

type ownershipTransport func(*http.Request) (*http.Response, error)

func (f ownershipTransport) Do(req *http.Request) (*http.Response, error) { return f(req) }

// All SDK requests terminate in memory, including token acquisition.
func ownershipResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: req,
	}
}

func TestEnsureNSGChecksOwnership(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		body         string
		transportErr error
		wantErr      string
	}{
		{name: "absent", status: 404, body: `{"error":{"code":"ResourceNotFound"}}`},
		{name: "owned", status: 200, body: `{"tags":{"ys-owner":"yscale-burst"}}`},
		{name: "untagged", status: 200, body: `{}`, wantErr: "not owned"},
		{name: "wrong owner", status: 200, body: `{"tags":{"ys-owner":"someone-else"}}`, wantErr: "not owned"},
		{name: "nil owner", status: 200, body: `{"tags":{"ys-owner":null}}`, wantErr: "not owned"},
		{name: "forbidden", status: 403, body: `{"error":{"code":"AuthorizationFailed"}}`, wantErr: "getting NSG"},
		{name: "unavailable", status: 503, body: `{"error":{"code":"Unavailable"}}`, wantErr: "getting NSG"},
		{name: "transport error", transportErr: errors.New("NotFound in transport"), wantErr: "getting NSG"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			transport := ownershipTransport(func(req *http.Request) (*http.Response, error) {
				calls = append(calls, req.Method)
				if !strings.HasSuffix(req.URL.Path, "/resourceGroups/rg/providers/Microsoft.Network/networkSecurityGroups/ys-nsg-eastus") {
					t.Fatalf("unexpected request: %s", req.URL.Path)
				}
				if req.Method == http.MethodGet {
					if tc.transportErr != nil {
						return nil, tc.transportErr
					}
					return ownershipResponse(req, tc.status, tc.body), nil
				}
				if req.Method != http.MethodPut {
					t.Fatalf("unexpected method: %s", req.Method)
				}
				return ownershipResponse(req, 200, `{"id":"/nsg","properties":{"provisioningState":"Succeeded"}}`), nil
			})
			client, err := armnetwork.NewSecurityGroupsClient("sub", &azfake.TokenCredential{}, &arm.ClientOptions{
				ClientOptions: policy.ClientOptions{Transport: transport, Retry: policy.RetryOptions{MaxRetries: -1}},
			})
			if err != nil {
				t.Fatal(err)
			}
			b := &Backend{resourceGroup: "rg", nsgClient: client}
			id, err := b.ensureNSG(context.Background(), "eastus", map[string]*string{ownerTagKey: to.Ptr(ownerTagValue)})
			wantCalls := []string{http.MethodGet}
			if tc.wantErr == "" {
				wantCalls = append(wantCalls, http.MethodPut)
				if err != nil || id != "/nsg" {
					t.Errorf("ensureNSG = %q, %v", id, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) || id != "" {
				t.Errorf("ensureNSG = %q, %v; want error containing %q", id, err, tc.wantErr)
			}
			if !reflect.DeepEqual(calls, wantCalls) {
				t.Errorf("requests = %v, want %v", calls, wantCalls)
			}
		})
	}
}

func TestCreateNodeChecksVMOwnership(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		body         string
		transportErr error
		wantErr      string
	}{
		{name: "absent", status: 404, body: `{"error":{"code":"ResourceNotFound"}}`},
		{name: "owned", status: 200, body: `{"tags":{"ys-owner":"yscale-burst","ys-scope":"pool"}}`},
		{name: "untagged", status: 200, body: `{}`, wantErr: "outside owned scope"},
		{name: "wrong owner", status: 200, body: `{"tags":{"ys-owner":"someone-else","ys-scope":"pool"}}`, wantErr: "outside owned scope"},
		{name: "other pool", status: 200, body: `{"tags":{"ys-owner":"yscale-burst","ys-scope":"other"}}`, wantErr: "outside owned scope"},
		{name: "unscoped", status: 200, body: `{"tags":{"ys-owner":"yscale-burst"}}`, wantErr: "outside owned scope"},
		{name: "forbidden", status: 403, body: `{"error":{"code":"AuthorizationFailed"}}`, wantErr: "getting VM"},
		{name: "unavailable", status: 503, body: `{"error":{"code":"Unavailable"}}`, wantErr: "getting VM"},
		{name: "transport error", transportErr: errors.New("NotFound in transport"), wantErr: "getting VM"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			transport := ownershipTransport(func(req *http.Request) (*http.Response, error) {
				kind := "VM"
				if strings.HasSuffix(req.URL.Path, "/networkInterfaces/burst-nic") {
					kind = "NIC"
				} else if !strings.HasSuffix(req.URL.Path, "/virtualMachines/burst") {
					t.Fatalf("unexpected request: %s", req.URL.Path)
				}
				calls = append(calls, req.Method+" "+kind)
				if req.Method == http.MethodGet && kind == "VM" {
					if tc.transportErr != nil {
						return nil, tc.transportErr
					}
					return ownershipResponse(req, tc.status, tc.body), nil
				}
				if req.Method != http.MethodPut {
					t.Fatalf("unexpected method: %s", req.Method)
				}
				return ownershipResponse(req, 200, `{"id":"/resource","properties":{"provisioningState":"Succeeded"}}`), nil
			})
			opts := &arm.ClientOptions{ClientOptions: policy.ClientOptions{Transport: transport, Retry: policy.RetryOptions{MaxRetries: -1}}}
			vmClient, err := armcompute.NewVirtualMachinesClient("sub", &azfake.TokenCredential{}, opts)
			if err != nil {
				t.Fatal(err)
			}
			nicClient, err := armnetwork.NewInterfacesClient("sub", &azfake.TokenCredential{}, opts)
			if err != nil {
				t.Fatal(err)
			}
			b := &Backend{resourceGroup: "rg", location: "eastus", poolScope: "pool", vmClient: vmClient, nicClient: nicClient, netCache: map[string]string{"eastus": "/subnet"}}
			id, err := b.CreateNode(context.Background(), &backends.NodeSpec{Name: "burst", ScopeHash: "pool"})
			wantCalls := []string{"GET VM"}
			if tc.wantErr == "" {
				wantCalls = append(wantCalls, "PUT NIC", "PUT VM")
				if err != nil || id != "burst" {
					t.Errorf("CreateNode = %q, %v", id, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) || id != "" {
				t.Errorf("CreateNode = %q, %v; want error containing %q", id, err, tc.wantErr)
			}
			if !reflect.DeepEqual(calls, wantCalls) {
				t.Errorf("requests = %v, want %v", calls, wantCalls)
			}
		})
	}
}
