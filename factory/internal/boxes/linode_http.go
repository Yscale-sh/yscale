package boxes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const defaultLinodeBaseURL = "https://api.linode.com/v4"

// LinodeHTTP implements the Linode v4 REST boundary used by LinodeWorker.
type LinodeHTTP struct {
	token   string
	baseURL string
	httpc   *http.Client
}

// HTTPOption configures the HTTP implementation.
type HTTPOption func(*LinodeHTTP)

// WithBaseURL overrides the Linode API base URL. It is intended for tests and
// private API proxies.
func WithBaseURL(baseURL string) HTTPOption {
	return func(c *LinodeHTTP) { c.baseURL = strings.TrimRight(baseURL, "/") }
}

// WithHTTPClient overrides the HTTP client used for Linode API requests.
func WithHTTPClient(httpc *http.Client) HTTPOption {
	return func(c *LinodeHTTP) { c.httpc = httpc }
}

// NewLinodeHTTP constructs the production Linode v4 client.
func NewLinodeHTTP(token string, opts ...HTTPOption) *LinodeHTTP {
	c := &LinodeHTTP{
		token:   token,
		baseURL: defaultLinodeBaseURL,
		httpc:   &http.Client{Timeout: 30 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.httpc == nil {
		c.httpc = &http.Client{Timeout: 30 * time.Second}
	}
	return c
}

type linodeFirewall struct {
	ID    json.RawMessage `json:"id"`
	Label string          `json:"label"`
}

type linodeInstance struct {
	ID     json.RawMessage `json:"id"`
	Status string          `json:"status"`
	IPv4   []string        `json:"ipv4"`
	Tags   []string        `json:"tags"`
	Label  string          `json:"label"`
}

func (i linodeInstance) instance() (Instance, error) {
	id, err := linodeID(i.ID)
	if err != nil {
		return Instance{}, err
	}
	return Instance{ID: id, Status: i.Status, IPv4: i.IPv4, Tags: i.Tags, Label: i.Label}, nil
}

func linodeID(raw json.RawMessage) (string, error) {
	var id string
	if err := json.Unmarshal(raw, &id); err == nil && id != "" {
		return id, nil
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil && number.String() != "" {
		return number.String(), nil
	}
	return "", fmt.Errorf("linode: invalid response id")
}

func (c *LinodeHTTP) EnsureFirewall(ctx context.Context, label string, tags []string) (string, error) {
	resp, err := c.do(ctx, http.MethodGet, "/networking/firewalls", nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if err := c.checkStatus(resp, http.MethodGet, "/networking/firewalls"); err != nil {
		return "", err
	}
	var firewalls struct {
		Data []linodeFirewall `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&firewalls); err != nil {
		return "", fmt.Errorf("linode: GET /networking/firewalls: decode response")
	}
	for _, firewall := range firewalls.Data {
		if firewall.Label == label {
			return linodeID(firewall.ID)
		}
	}

	body := struct {
		Label string   `json:"label"`
		Tags  []string `json:"tags"`
		Rules struct {
			InboundPolicy  string `json:"inbound_policy"`
			OutboundPolicy string `json:"outbound_policy"`
			Inbound        []struct {
				Label     string `json:"label"`
				Action    string `json:"action"`
				Protocol  string `json:"protocol"`
				Ports     string `json:"ports"`
				Addresses struct {
					IPv4 []string `json:"ipv4"`
					IPv6 []string `json:"ipv6"`
				} `json:"addresses"`
			} `json:"inbound"`
		} `json:"rules"`
	}{Label: label, Tags: tags}
	body.Rules.InboundPolicy = "DROP"
	body.Rules.OutboundPolicy = "ACCEPT"
	for _, rule := range []struct {
		label, protocol, ports string
	}{
		{"https", "TCP", "443"},
		{"http-le", "TCP", "80"},
		{"derp-udp", "UDP", "3478,41641"},
		{"ssh", "TCP", "22"},
	} {
		inbound := struct {
			Label     string `json:"label"`
			Action    string `json:"action"`
			Protocol  string `json:"protocol"`
			Ports     string `json:"ports"`
			Addresses struct {
				IPv4 []string `json:"ipv4"`
				IPv6 []string `json:"ipv6"`
			} `json:"addresses"`
		}{Label: rule.label, Action: "ACCEPT", Protocol: rule.protocol, Ports: rule.ports}
		inbound.Addresses.IPv4 = []string{"0.0.0.0/0"}
		inbound.Addresses.IPv6 = []string{"::/0"}
		body.Rules.Inbound = append(body.Rules.Inbound, inbound)
	}

	resp, err = c.do(ctx, http.MethodPost, "/networking/firewalls", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if err := c.checkStatus(resp, http.MethodPost, "/networking/firewalls"); err != nil {
		return "", err
	}
	var firewall linodeFirewall
	if err := json.NewDecoder(resp.Body).Decode(&firewall); err != nil {
		return "", fmt.Errorf("linode: POST /networking/firewalls: decode response")
	}
	return linodeID(firewall.ID)
}

func (c *LinodeHTTP) CreateInstance(ctx context.Context, spec InstanceSpec) (Instance, error) {
	body := struct {
		Label      string   `json:"label"`
		Region     string   `json:"region"`
		Type       string   `json:"type"`
		Image      string   `json:"image"`
		RootPass   string   `json:"root_pass"`
		FirewallID int64    `json:"firewall_id,omitempty"`
		Tags       []string `json:"tags"`
		Metadata   struct {
			UserData string `json:"user_data"`
		} `json:"metadata"`
	}{
		Label: spec.Label, Region: spec.Region, Type: spec.Type, Image: spec.Image,
		RootPass: spec.RootPass, Tags: spec.Tags,
	}
	body.Metadata.UserData = spec.UserDataB64
	// Linode requires an integer firewall_id; a quoted ID is rejected with
	// HTTP 400 and no instance. Empty means "no firewall" (omitted).
	if spec.FirewallID != "" {
		id, err := strconv.ParseInt(spec.FirewallID, 10, 64)
		if err != nil || id <= 0 {
			return Instance{}, fmt.Errorf("linode: POST /linode/instances: invalid firewall ID %q", spec.FirewallID)
		}
		body.FirewallID = id
	}

	resp, err := c.do(ctx, http.MethodPost, "/linode/instances", body)
	if err != nil {
		return Instance{}, err
	}
	defer resp.Body.Close()
	if err := c.checkStatus(resp, http.MethodPost, "/linode/instances"); err != nil {
		return Instance{}, err
	}
	var instance linodeInstance
	if err := json.NewDecoder(resp.Body).Decode(&instance); err != nil {
		return Instance{}, fmt.Errorf("linode: POST /linode/instances: decode response")
	}
	return instance.instance()
}

func (c *LinodeHTTP) GetInstance(ctx context.Context, id string) (Instance, error) {
	path := "/linode/instances/" + id
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return Instance{}, err
	}
	defer resp.Body.Close()
	if err := c.checkStatus(resp, http.MethodGet, path); err != nil {
		return Instance{}, err
	}
	var instance linodeInstance
	if err := json.NewDecoder(resp.Body).Decode(&instance); err != nil {
		return Instance{}, fmt.Errorf("linode: GET %s: decode response", path)
	}
	return instance.instance()
}

func (c *LinodeHTTP) ListInstancesByTag(ctx context.Context, tag string) ([]Instance, error) {
	var instances []Instance
	for page, pages := 1, 1; page <= pages; page++ {
		path := fmt.Sprintf("/linode/instances?page_size=500&page=%d", page)
		resp, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		if err := c.checkStatus(resp, http.MethodGet, path); err != nil {
			resp.Body.Close()
			return nil, err
		}
		var result struct {
			Data  []linodeInstance `json:"data"`
			Pages int              `json:"pages"`
		}
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("linode: GET %s: decode response", path)
		}
		if result.Pages > 0 {
			pages = result.Pages
		}
		for _, instance := range result.Data {
			if !hasExactTag(instance.Tags, tag) {
				continue
			}
			mapped, err := instance.instance()
			if err != nil {
				return nil, err
			}
			instances = append(instances, mapped)
		}
	}
	return instances, nil
}

func (c *LinodeHTTP) DeleteInstance(ctx context.Context, id string) error {
	path := "/linode/instances/" + id
	resp, err := c.do(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return c.checkStatus(resp, http.MethodDelete, path)
}

func (c *LinodeHTTP) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("linode: %s %s: marshal request", method, path)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, fmt.Errorf("linode: %s %s: build request", method, path)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("linode: %s %s: request failed", method, path)
	}
	return resp, nil
}

func (c *LinodeHTTP) checkStatus(resp *http.Response, method, path string) error {
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if reasons := linodeErrorReasons(resp.Body); reasons != "" {
			return fmt.Errorf("linode: %s %s: unexpected HTTP status %d: %s", method, path, resp.StatusCode, reasons)
		}
		return fmt.Errorf("linode: %s %s: unexpected HTTP status %d", method, path, resp.StatusCode)
	}
	return nil
}

// linodeErrorReasons extracts only Linode's structured field/reason pairs, so
// an operator can act on a rejected request without the raw body (which is
// never logged) leaking anything else.
func linodeErrorReasons(body io.Reader) string {
	var parsed struct {
		Errors []struct {
			Field  string `json:"field"`
			Reason string `json:"reason"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(io.LimitReader(body, 8<<10)).Decode(&parsed); err != nil {
		return ""
	}
	reasons := make([]string, 0, len(parsed.Errors))
	for _, e := range parsed.Errors {
		if e.Field != "" {
			reasons = append(reasons, e.Field+": "+e.Reason)
		} else {
			reasons = append(reasons, e.Reason)
		}
	}
	return strings.Join(reasons, "; ")
}

var _ linodeAPI = (*LinodeHTTP)(nil)
