// yscale:proprietary

// Package factoryclient speaks the yscale-factory tenant lifecycle API.
package factoryclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	// ErrFabricProvisioning means the factory accepted the tenant but its
	// fabric is not ready yet.
	ErrFabricProvisioning = errors.New("fabric provisioning")
	// ErrFabricAbsent means the factory has no fabric record for the tenant.
	ErrFabricAbsent = errors.New("fabric absent")
)

// Fabric is the factory's public lifecycle view. The factory never exposes an
// API key, so this deliberately has no credential field.
type Fabric struct {
	Status              string `json:"status"`
	LoginServer         string `json:"login_server"`
	User                string `json:"user"`
	BackendID           string `json:"backend_id"`
	CreatedAt           string `json:"created_at"`
	LastValidatedAt     string `json:"last_validated_at"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
}

// FabricService is the lifecycle seam used by central. Tests can replace it
// with a fake; production uses Client.
type FabricService interface {
	EnsureFabric(ctx context.Context, tenantID, idempotencyKey string) error
	GetFabric(ctx context.Context, tenantID string) (Fabric, error)
	DeleteFabric(ctx context.Context, tenantID string) error
}

// HTTPError reports an unexpected factory response without retaining or
// exposing its body.
type HTTPError struct {
	StatusCode int
	Reason     string
}

func (e *HTTPError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("factory: HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("factory: HTTP %d (%s)", e.StatusCode, e.Reason)
}

// Client is a bounded HTTP client for the factory lifecycle API.
type Client struct {
	baseURL string
	token   string
	httpc   *http.Client
	timeout time.Duration
}

// Option adjusts a Client.
type Option func(*Client)

// WithHTTPClient replaces the HTTP transport, primarily for tests.
func WithHTTPClient(httpc *http.Client) Option {
	return func(c *Client) {
		if httpc != nil {
			c.httpc = httpc
		}
	}
}

// WithTimeout sets the total budget for one factory call.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		if timeout > 0 {
			c.timeout = timeout
		}
	}
}

// New constructs a factory API client with a 10-second call budget.
func New(baseURL, token string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		timeout: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.httpc == nil {
		c.httpc = &http.Client{Timeout: c.timeout}
	}
	return c
}

// EnsureFabric starts provisioning a tenant fabric. The tenant ID is also the
// stable idempotency key used by central on lifecycle retries.
func (c *Client) EnsureFabric(ctx context.Context, tenantID, idempotencyKey string) error {
	resp, err := c.do(ctx, http.MethodPost, tenantID, idempotencyKey)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
		return nil
	case http.StatusConflict:
		return ErrFabricProvisioning
	default:
		return responseError(resp)
	}
}

// GetFabric retrieves the public fabric state for a tenant.
func (c *Client) GetFabric(ctx context.Context, tenantID string) (Fabric, error) {
	resp, err := c.do(ctx, http.MethodGet, tenantID, "")
	if err != nil {
		return Fabric{}, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		return Fabric{}, ErrFabricAbsent
	case http.StatusConflict:
		return Fabric{}, ErrFabricProvisioning
	case http.StatusOK:
		var fabric Fabric
		if err := json.NewDecoder(resp.Body).Decode(&fabric); err != nil {
			return Fabric{}, fmt.Errorf("factory: decode fabric response: %w", err)
		}
		return fabric, nil
	default:
		return Fabric{}, responseError(resp)
	}
}

// DeleteFabric starts idempotent fabric teardown.
func (c *Client) DeleteFabric(ctx context.Context, tenantID string) error {
	resp, err := c.do(ctx, http.MethodDelete, tenantID, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted, http.StatusNotFound:
		return nil
	default:
		return responseError(resp)
	}
}

func (c *Client) do(ctx context.Context, method, tenantID, idempotencyKey string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	path := "/v1/tenants/" + url.PathEscape(tenantID) + "/fabric"
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("factory: create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return c.httpc.Do(req)
}

func responseError(resp *http.Response) error {
	// Keep errors safe even if a proxy returns secrets in a body. The status
	// code is enough for callers; use the standard short reason only.
	return &HTTPError{StatusCode: resp.StatusCode, Reason: http.StatusText(resp.StatusCode)}
}
