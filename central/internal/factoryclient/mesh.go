package factoryclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/factory/client"
)

// Tenant mesh operations routed through the factory (central never holds a box's admin key).
// Central never holds a box admin key: every mint, device lookup/delete,
// route approval and policy push for a factory-provisioned tenant is an RPC,
// and the factory uses the box key it alone stores. Every RPC names the box
// central recorded (loginServer), and the factory resolves exactly that
// tenant/box pair, so an operation can never land on a different box.

// MeshLoginServerHeader carries the expected box on every mesh RPC.
const MeshLoginServerHeader = "X-Yscale-Login-Server"

// MeshService is the factory mesh seam used by TenantMesh. Tests can replace
// it with a fake; production uses Client.
type MeshService interface {
	MintKey(ctx context.Context, tenantID, loginServer string, tags []string, expiry time.Duration, ephemeral bool) (key, mintedOn string, err error)
	FindDevice(ctx context.Context, tenantID, loginServer, hostname string) (string, error)
	DeleteDevice(ctx context.Context, tenantID, loginServer, deviceID string) error
	ApproveRoutes(ctx context.Context, tenantID, loginServer, deviceID string, routes []string) error
	EnsurePolicy(ctx context.Context, tenantID, loginServer string, tagOwners, routeApprovers map[string][]string, extraACLs []client.PolicyACL) error
}

var _ MeshService = (*Client)(nil)

// MintKey mints a join key on the tenant's box and reports the box's login
// server. A fabric that is still onboarding returns ErrFabricProvisioning.
func (c *Client) MintKey(ctx context.Context, tenantID, loginServer string, tags []string, expiry time.Duration, ephemeral bool) (string, string, error) {
	in := struct {
		Tags          []string `json:"tags"`
		ExpirySeconds int64    `json:"expiry_seconds"`
		Ephemeral     bool     `json:"ephemeral"`
	}{Tags: tags, ExpirySeconds: int64(expiry / time.Second), Ephemeral: ephemeral}
	var out struct {
		AuthKey     string `json:"auth_key"`
		LoginServer string `json:"login_server"`
	}
	if err := c.meshCall(ctx, http.MethodPost, tenantID, loginServer, "keys", nil, in, &out); err != nil {
		return "", "", err
	}
	if out.AuthKey == "" || out.LoginServer == "" {
		return "", "", fmt.Errorf("factory: mesh keys: incomplete response")
	}
	return out.AuthKey, out.LoginServer, nil
}

// FindDevice returns the box's device ID for hostname, or "" when absent.
func (c *Client) FindDevice(ctx context.Context, tenantID, loginServer, hostname string) (string, error) {
	var out struct {
		ID string `json:"id"`
	}
	err := c.meshCall(ctx, http.MethodGet, tenantID, loginServer, "devices", url.Values{"hostname": {hostname}}, nil, &out)
	return out.ID, err
}

// DeleteDevice removes a device from the tenant's box; absent is not an error.
func (c *Client) DeleteDevice(ctx context.Context, tenantID, loginServer, deviceID string) error {
	return c.meshCall(ctx, http.MethodDelete, tenantID, loginServer, "devices/"+url.PathEscape(deviceID), nil, nil, nil)
}

// ApproveRoutes approves a gateway node's advertised routes on the tenant's box.
func (c *Client) ApproveRoutes(ctx context.Context, tenantID, loginServer, deviceID string, routes []string) error {
	in := struct {
		Routes []string `json:"routes"`
	}{Routes: routes}
	return c.meshCall(ctx, http.MethodPost, tenantID, loginServer, "devices/"+url.PathEscape(deviceID)+"/routes", nil, in, nil)
}

// EnsurePolicy replaces the managed policy on the tenant's box.
func (c *Client) EnsurePolicy(ctx context.Context, tenantID, loginServer string, tagOwners, routeApprovers map[string][]string, extraACLs []client.PolicyACL) error {
	in := struct {
		TagOwners      map[string][]string `json:"tag_owners"`
		RouteApprovers map[string][]string `json:"route_approvers"`
		ExtraACLs      []client.PolicyACL  `json:"extra_acls"`
	}{TagOwners: tagOwners, RouteApprovers: routeApprovers, ExtraACLs: extraACLs}
	return c.meshCall(ctx, http.MethodPut, tenantID, loginServer, "policy", nil, in, nil)
}

// meshCall performs one bounded mesh RPC and decodes the response inside the
// call budget (cancelling the context before reading would abort the body).
func (c *Client) meshCall(ctx context.Context, method, tenantID, loginServer, sub string, query url.Values, in, out any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	target := c.baseURL + "/v1/tenants/" + url.PathEscape(tenantID) + "/mesh/" + sub
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var body io.Reader
	if in != nil {
		encoded, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("factory: mesh %s: encode request", sub)
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return fmt.Errorf("factory: mesh %s: create request: %w", sub, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set(MeshLoginServerHeader, loginServer)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("factory: mesh %s: %w", sub, ctx.Err())
		}
		return fmt.Errorf("factory: mesh %s: request failed", sub)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		if out == nil {
			return nil
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out); err != nil {
			return fmt.Errorf("factory: mesh %s: decode response", sub)
		}
		return nil
	case http.StatusConflict:
		return ErrFabricProvisioning
	case http.StatusNotFound:
		return ErrFabricAbsent
	default:
		return responseError(resp)
	}
}

// TenantMesh is the mesh.Provider (and meshpolicy ensurer) for one
// factory-provisioned tenant. It is keyed by tenant ID and pinned to the login
// server central recorded when the fabric became ready.
type TenantMesh struct {
	svc         MeshService
	tenantID    string
	loginServer string
}

var _ mesh.Provider = (*TenantMesh)(nil)

// NewTenantMesh returns a factory-routed provider for tenantID.
func NewTenantMesh(svc MeshService, tenantID, loginServer string) *TenantMesh {
	return &TenantMesh{svc: svc, tenantID: tenantID, loginServer: loginServer}
}

// MintAuthKey mints an ephemeral key, matching the other providers' default.
func (m *TenantMesh) MintAuthKey(ctx context.Context, tags []string, expiry time.Duration) (string, error) {
	return m.MintAuthKeyEphemeral(ctx, tags, expiry, true)
}

// MintAuthKeyEphemeral mints a key on the tenant's box. A key from a different
// box than the one central recorded is refused: joining it would strand the
// device on a coordinator central's teardown no longer resolves.
func (m *TenantMesh) MintAuthKeyEphemeral(ctx context.Context, tags []string, expiry time.Duration, ephemeral bool) (string, error) {
	key, loginServer, err := m.svc.MintKey(ctx, m.tenantID, m.loginServer, tags, expiry, ephemeral)
	if err != nil {
		return "", err
	}
	if loginServer != m.loginServer {
		return "", fmt.Errorf("factory: tenant %s mesh moved from %s to %s; reattach before minting", m.tenantID, m.loginServer, loginServer)
	}
	return key, nil
}

func (m *TenantMesh) FindDeviceByHostname(ctx context.Context, hostname string) (string, error) {
	return m.svc.FindDevice(ctx, m.tenantID, m.loginServer, hostname)
}

func (m *TenantMesh) DeleteDevice(ctx context.Context, deviceID string) error {
	return m.svc.DeleteDevice(ctx, m.tenantID, m.loginServer, deviceID)
}

func (m *TenantMesh) ApproveNodeRoutes(ctx context.Context, nodeID string, routes []string) error {
	return m.svc.ApproveRoutes(ctx, m.tenantID, m.loginServer, nodeID, routes)
}

func (m *TenantMesh) EnsurePolicy(ctx context.Context, tagOwners, routeApprovers map[string][]string, extraACLs []client.PolicyACL) error {
	return m.svc.EnsurePolicy(ctx, m.tenantID, m.loginServer, tagOwners, routeApprovers, extraACLs)
}

// LoginServer is the tenant box URL joining nodes must use.
func (m *TenantMesh) LoginServer() string { return m.loginServer }
