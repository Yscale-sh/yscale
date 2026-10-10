package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/internal/evidence"
)

const (
	tailscaleAPIBase        = "https://api.tailscale.com"
	tailscaleRequestTimeout = 20 * time.Second
)

// tailscaleInventory is a read-only audit client that proves a burst's
// mesh device is absent from the tailnet. OAuth token exchange POSTs to
// the token endpoint; all account/device operations are GET-only.
//
// It deliberately does NOT import central/internal/tailscale, which
// owns key minting and device deletion. The evidence path must not
// hold a capability to mutate the mesh.
type tailscaleInventory struct {
	clientID     string
	clientSecret string
	tailnet      string
	apiBase      string
	client       *http.Client

	mu          sync.Mutex
	bearer      string
	bearerUntil time.Time
}

func newTailscaleInventory(clientID, clientSecret, tailnet string) *tailscaleInventory {
	return &tailscaleInventory{
		clientID:     clientID,
		clientSecret: clientSecret,
		tailnet:      tailnet,
		apiBase:      tailscaleAPIBase,
		client:       &http.Client{Timeout: tailscaleRequestTimeout},
	}
}

func (t *tailscaleInventory) ProviderName() string { return evidence.MeshProviderTailscale }

// tailscaleDevice is the redacted subset of a device row this audit
// needs. Nothing else is decoded.
type tailscaleDevice struct {
	ID       string `json:"id"`
	Hostname string `json:"hostname"`
}

// bearerToken obtains an OAuth bearer token via the client-credentials
// flow. This is the only POST the audit client makes.
func (t *tailscaleInventory) bearerToken(ctx context.Context) (string, error) {
	t.mu.Lock()
	if t.bearer != "" && time.Until(t.bearerUntil) > 60*time.Second {
		tok := t.bearer
		t.mu.Unlock()
		return tok, nil
	}
	t.mu.Unlock()

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", t.clientID)
	form.Set("client_secret", t.clientSecret)

	endpoint := t.apiBase + "/api/v2/oauth/token"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("tailscale inventory: build oauth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("tailscale inventory: oauth request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("tailscale inventory: oauth token exchange: status %d (body redacted)", resp.StatusCode)
	}

	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("tailscale inventory: decode oauth response: invalid JSON")
	}
	if out.AccessToken == "" {
		return "", fmt.Errorf("tailscale inventory: oauth response missing access_token")
	}

	t.mu.Lock()
	t.bearer = out.AccessToken
	if out.ExpiresIn > 0 {
		t.bearerUntil = time.Now().Add(time.Duration(out.ExpiresIn) * time.Second)
	} else {
		t.bearerUntil = time.Now().Add(30 * time.Minute)
	}
	t.mu.Unlock()
	return out.AccessToken, nil
}

// FindDevice searches the tailnet for a device with the exact hostname.
// Returns the count of matching devices (0 or 1+). Any error means the
// tailnet could not be listed and the result is inconclusive.
func (t *tailscaleInventory) FindDevice(ctx context.Context, hostname string) (int, error) {
	if hostname == "" {
		return 0, fmt.Errorf("tailscale inventory: empty hostname; refusing to audit")
	}

	token, err := t.bearerToken(ctx)
	if err != nil {
		return 0, err
	}

	endpoint := fmt.Sprintf("%s/api/v2/tailnet/%s/devices", t.apiBase, url.PathEscape(t.tailnet))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("tailscale inventory: build list request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := t.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("tailscale inventory: list request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return 0, fmt.Errorf("tailscale inventory: list devices: status %d (body redacted)", resp.StatusCode)
	}

	const maxDeviceListBytes = 8 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDeviceListBytes+1))
	if err != nil {
		return 0, fmt.Errorf("tailscale inventory: read device list: %w", err)
	}
	if len(body) > maxDeviceListBytes {
		return 0, fmt.Errorf("tailscale inventory: device list exceeds %d bytes; refusing to report absence from a truncated read", maxDeviceListBytes)
	}

	var out struct {
		Devices []tailscaleDevice `json:"devices"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return 0, fmt.Errorf("tailscale inventory: list devices response was not valid JSON")
	}
	if out.Devices == nil {
		return 0, fmt.Errorf("tailscale inventory: device list missing 'devices' field; cannot prove absence from an empty envelope")
	}

	count := 0
	for _, d := range out.Devices {
		if d.Hostname == hostname {
			count++
		}
	}
	return count, nil
}

// MeshInventory is the interface the runner uses to audit mesh device
// absence. It is intentionally read-only.
type MeshInventory interface {
	ProviderName() string
	FindDevice(ctx context.Context, hostname string) (int, error)
}
