// Package hostedcontroller reconciles Yscale-hosted tenant capacity from
// central's admin API into the shared Kubernetes cluster.
package hostedcontroller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxResponseBytes = 1 << 20

type HostedCluster struct {
	ClusterID       string    `json:"cluster_id"`
	Name            string    `json:"name,omitempty"`
	Source          string    `json:"source"`
	State           string    `json:"state"`
	HostedNamespace string    `json:"hosted_namespace"`
	RegisteredAt    time.Time `json:"registered_at"`
}

type InventoryRow struct {
	TenantID string        `json:"tenant_id"`
	Name     string        `json:"name,omitempty"`
	Plan     string        `json:"plan"`
	Cluster  HostedCluster `json:"cluster"`
}

type CapacityRequest struct {
	TenantID    string    `json:"tenant_id"`
	Name        string    `json:"name,omitempty"`
	Plan        string    `json:"plan"`
	RequestedAt time.Time `json:"requested_at"`
}

type Credential struct {
	TenantID         string        `json:"tenant_id"`
	Cluster          HostedCluster `json:"cluster"`
	ConnectorToken   string        `json:"connector_token"`
	HelmRelease      string        `json:"helm_release"`
	ConnectorRBACSet string        `json:"connector_rbac_set"`
	HelmCommand      string        `json:"helm_command"`
}

type Central interface {
	ListInventory(context.Context) ([]InventoryRow, error)
	ListRequests(context.Context) ([]CapacityRequest, error)
	Assign(context.Context, string) (Credential, error)
	Rotate(context.Context, string, string) (Credential, error)
}

type Client struct {
	base  *url.URL
	token string
	http  *http.Client
}

func NewClient(rawURL, token string, timeout time.Duration) (*Client, error) {
	base, err := url.Parse(strings.TrimRight(rawURL, "/"))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil {
		return nil, errors.New("invalid central URL")
	}
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("admin token is required")
	}
	if timeout <= 0 || timeout > time.Minute {
		return nil, errors.New("central timeout must be between 1ns and 1m")
	}
	return &Client{base: base, token: token, http: &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (c *Client) ListInventory(ctx context.Context) ([]InventoryRow, error) {
	var out struct {
		Clusters []InventoryRow `json:"clusters"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/admin/hosted-clusters", nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	if out.Clusters == nil {
		return nil, errors.New("central inventory response has null clusters")
	}
	for _, row := range out.Clusters {
		if err := validateRow(row); err != nil {
			return nil, err
		}
	}
	return out.Clusters, nil
}

func (c *Client) ListRequests(ctx context.Context) ([]CapacityRequest, error) {
	var out struct {
		Requests []CapacityRequest `json:"requests"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/admin/hosted-capacity/requests", nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	if out.Requests == nil {
		return nil, errors.New("central requests response has null requests")
	}
	for _, row := range out.Requests {
		if row.TenantID == "" || row.Plan == "" || row.RequestedAt.IsZero() {
			return nil, errors.New("central requests response is invalid")
		}
	}
	return out.Requests, nil
}

func (c *Client) Assign(ctx context.Context, tenantID string) (Credential, error) {
	var out Credential
	path := "/v1/admin/tenants/" + url.PathEscape(tenantID) + "/hosted-clusters"
	if err := c.do(ctx, http.MethodPost, path, strings.NewReader("{}"), &out, http.StatusCreated); err != nil {
		return Credential{}, err
	}
	return out, validateCredential(out, tenantID, "")
}

func (c *Client) Rotate(ctx context.Context, tenantID, clusterID string) (Credential, error) {
	var out Credential
	path := "/v1/admin/tenants/" + url.PathEscape(tenantID) + "/hosted-clusters/" + url.PathEscape(clusterID) + "/credential"
	if err := c.do(ctx, http.MethodPost, path, strings.NewReader("{}"), &out, http.StatusOK); err != nil {
		return Credential{}, err
	}
	return out, validateCredential(out, tenantID, clusterID)
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader, out any, want int) error {
	u := *c.base
	u.Path = strings.TrimRight(c.base.Path, "/") + path
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return errors.New("build central request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("central request failed: %w", redact(err, c.token))
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		return fmt.Errorf("central returned HTTP %d", resp.StatusCode)
	}
	limited := io.LimitReader(resp.Body, maxResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return errors.New("read central response")
	}
	if len(data) > maxResponseBytes {
		return errors.New("central response exceeds limit")
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return errors.New("central response is invalid")
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return errors.New("central response has trailing JSON")
	}
	return nil
}

func validateRow(row InventoryRow) error {
	if row.TenantID == "" || row.Plan == "" || row.Cluster.ClusterID == "" || row.Cluster.Source != "hosted" || row.Cluster.State == "" || row.Cluster.HostedNamespace == "" || row.Cluster.RegisteredAt.IsZero() {
		return errors.New("central inventory response is invalid")
	}
	return nil
}

func validateCredential(c Credential, tenantID, clusterID string) error {
	if c.TenantID != tenantID || c.ConnectorToken == "" || c.Cluster.ClusterID == "" || c.Cluster.Source != "hosted" || c.Cluster.State == "" || c.Cluster.HostedNamespace == "" || c.HelmRelease == "" || c.ConnectorRBACSet == "" || c.HelmCommand == "" {
		return errors.New("central credential response is invalid")
	}
	if clusterID != "" && c.Cluster.ClusterID != clusterID {
		return errors.New("central credential response mismatches assignment")
	}
	return nil
}

func redact(err error, secret string) error {
	return errors.New(strings.ReplaceAll(err.Error(), secret, "[redacted]"))
}
