package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/internal/evidence"
)

const (
	fabricRequestTimeout = 20 * time.Second
	fabricMaxListBytes   = 8 << 20
)

// fabricInventory is a read-only audit client for a per-customer Fabric
// coordination box. It proves that a burst's mesh node is absent from the
// customer's own tailnet — not from Tailscale SaaS.
//
// This client deliberately has no key-minting, route-approval, user-creation,
// or device-deletion methods, so the evidence path cannot mutate the mesh.
// Only GET /api/v1/node is issued.
type fabricInventory struct {
	baseURL string
	apiKey  string
	user    string
	client  *http.Client
}

// Compile-time proof that fabricInventory satisfies the read-only
// MeshInventory contract. The contract exposes only provider identity and
// device lookup, keeping mutation capabilities out of the evidence path.
var _ MeshInventory = (*fabricInventory)(nil)

// newFabricInventory constructs a Fabric audit client bound to the given
// box base URL, API key, and Fabric user (namespace that owns the nodes).
// Any trailing slash on baseURL is trimmed so endpoint paths concatenate
// cleanly.
func newFabricInventory(baseURL, apiKey, user string) *fabricInventory {
	return &fabricInventory{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		user:    user,
		client: &http.Client{
			Timeout: fabricRequestTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (h *fabricInventory) ProviderName() string { return evidence.MeshProviderFabric }

// fabricNode tolerates Fabric's node listing across versions: field
// names have drifted between givenName/name for the display identifier this
// audit needs. Only the fields required for hostname-exact matching are
// decoded; the node id is deliberately NOT retained because this client has
// no capability to delete a device — the id would only ever tempt one.
type fabricNode struct {
	Name           string `json:"name"`
	GivenName      string `json:"givenName"`
	GivenNameSnake string `json:"given_name"`
}

type fabricNodeList struct {
	Nodes []fabricNode `json:"nodes"`
}

// FindDevice lists the configured user's Fabric nodes via
// GET /api/v1/node?user=<user> and returns the exact count of nodes whose
// givenName OR name equals hostname. Any error (bad URL, transport failure,
// non-2xx, truncated body, invalid JSON, missing "nodes" field) means the box
// could not be listed and the caller must treat the result as inconclusive.
func (h *fabricInventory) FindDevice(ctx context.Context, hostname string) (int, error) {
	if hostname == "" {
		return 0, fmt.Errorf("fabric inventory: empty hostname; refusing to audit")
	}
	if strings.TrimSpace(h.user) == "" {
		return 0, fmt.Errorf("fabric inventory: empty user; refusing to audit")
	}
	if err := evidence.ValidateFabricAuditURL(h.baseURL); err != nil {
		return 0, fmt.Errorf("fabric inventory: %w", err)
	}

	q := url.Values{}
	q.Set("user", h.user)
	endpoint := h.baseURL + "/api/v1/node?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("fabric inventory: build list request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+h.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fabric inventory: list request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return 0, fmt.Errorf("fabric inventory: list nodes: status %d (body redacted)", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, fabricMaxListBytes+1))
	if err != nil {
		return 0, fmt.Errorf("fabric inventory: read node list: %w", err)
	}
	if len(body) > fabricMaxListBytes {
		return 0, fmt.Errorf("fabric inventory: node list exceeds %d bytes; refusing to report absence from a truncated read", fabricMaxListBytes)
	}

	var out fabricNodeList
	if err := json.Unmarshal(body, &out); err != nil {
		return 0, fmt.Errorf("fabric inventory: list nodes response was not valid JSON")
	}
	if out.Nodes == nil {
		return 0, fmt.Errorf("fabric inventory: node list missing 'nodes' field; cannot prove absence from an empty envelope")
	}

	count := 0
	for _, n := range out.Nodes {
		if n.GivenName == hostname || n.GivenNameSnake == hostname || n.Name == hostname {
			count++
		}
	}
	return count, nil
}
