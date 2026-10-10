package hsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Headscale is a hand-rolled REST client for a single customer's Headscale v1
// HTTP API. It deliberately avoids any Headscale/Tailscale SDK and speaks the
// raw API with net/http to match the project's existing HTTP client style.
//
// Auth is a Headscale API key passed as an HTTP Bearer token.
//
// IMPORTANT: the exact request/response shapes below were written against the
// Headscale v1 API as pinned by the deploy (deploy/headscale/cloud-init.sh,
// HEADSCALE_VERSION=0.23.0). Headscale's JSON field names have drifted across
// releases, so decoding here is intentionally tolerant (multiple fallbacks).
// Re-verify these endpoint shapes against the pinned server version whenever the
// deploy bumps HEADSCALE_VERSION.
type Headscale struct {
	baseURL string
	apiKey  string
	user    string
	http    *http.Client
}

// NewHeadscale constructs a Headscale client for the given box base URL, API key
// and user (the Headscale "user"/namespace that owns minted keys and nodes). Any
// trailing slash on baseURL is trimmed so endpoint paths can be concatenated
// cleanly.
func NewHeadscale(baseURL, apiKey, user string) *Headscale {
	return &Headscale{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		user:    user,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// LoginServer returns the Headscale box base URL. Nodes joining this mesh must
// be pointed at it via --login-server.
func (h *Headscale) LoginServer() string { return h.baseURL }

// requireSecureBaseURL rejects any base URL that would send the API key over an
// unencrypted channel. https is required; a loopback host (localhost / 127.0.0.0/8
// / ::1) is allowed over http only for tests and local debugging.
func requireSecureBaseURL(baseURL string) error {
	u, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("mesh: headscale invalid base URL %q: %w", baseURL, err)
	}
	if u.Scheme == "https" {
		return nil
	}
	if u.Scheme == "http" && isLoopbackHost(u.Hostname()) {
		return nil
	}
	return fmt.Errorf("mesh: headscale refuses to send API key over insecure base URL %q (scheme must be https)", baseURL)
}

// isLoopbackHost reports whether host is a loopback name or address.
func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// MintAuthKey mints an ephemeral pre-auth key. See MintAuthKeyEphemeral.
func (h *Headscale) MintAuthKey(ctx context.Context, tags []string, expiry time.Duration) (string, error) {
	return h.MintAuthKeyEphemeral(ctx, tags, expiry, true)
}

// preAuthKeyRequest is the body POSTed to /api/v1/preauthkey.
//
// NOTE: the "user" field is a uint64 USER ID in Headscale 0.26.x — NOT the
// username. proto3 JSON encodes uint64 as a decimal string, so this stays a
// Go string but MUST carry the numeric id (e.g. "1"). Sending the username
// here ("cust_test") makes the gateway reject the whole request with
// `proto: invalid value for uint64 field user`, so the key is never minted
// (verified live against headscale 0.26.1, 2026-05-31). resolveUserID() maps
// h.user (the name) to that numeric id.
type preAuthKeyRequest struct {
	User       string   `json:"user"`
	Reusable   bool     `json:"reusable"`
	Ephemeral  bool     `json:"ephemeral"`
	Expiration string   `json:"expiration"`
	ACLTags    []string `json:"aclTags,omitempty"`
}

// userListResponse decodes GET /api/v1/user. The id is a string-encoded
// uint64 in the 0.26.x JSON gateway.
type userListResponse struct {
	Users []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"users"`
}

// resolveUserID maps the configured username (h.user) to its numeric Headscale
// user id via GET /api/v1/user. Headscale 0.26's POST /api/v1/preauthkey takes
// a uint64 user id in the "user" field, not the name, so every mint must
// resolve the id first. Returns the id as a decimal string.
func (h *Headscale) resolveUserID(ctx context.Context) (string, error) {
	var resp userListResponse
	if err := h.do(ctx, http.MethodGet, "/api/v1/user", nil, &resp); err != nil {
		return "", err
	}
	for _, u := range resp.Users {
		if u.Name == h.user {
			if u.ID == "" || u.ID == "0" {
				return "", fmt.Errorf("mesh: headscale user %q has empty id", h.user)
			}
			return u.ID, nil
		}
	}
	return "", fmt.Errorf("mesh: headscale user %q not found on box", h.user)
}

// preAuthKeyResponse tolerates both the nested shape ({"preAuthKey":{"key":...}})
// and a flat fallback ({"key":...}) that some Headscale versions/proxies emit.
type preAuthKeyResponse struct {
	PreAuthKey *struct {
		Key string `json:"key"`
	} `json:"preAuthKey"`
	Key string `json:"key"`
}

// MintAuthKeyEphemeral mints a reusable pre-auth key against
// POST /api/v1/preauthkey carrying the given ACL tags, expiring at now+expiry
// (encoded RFC3339). When ephemeral is true the node self-removes after it goes
// offline. It returns the key string. The response is decoded tolerantly: it
// prefers .preAuthKey.key and falls back to a flat .key.
func (h *Headscale) MintAuthKeyEphemeral(ctx context.Context, tags []string, expiry time.Duration, ephemeral bool) (string, error) {
	uid, err := h.resolveUserID(ctx)
	if err != nil {
		return "", err
	}
	req := preAuthKeyRequest{
		User:       uid, // numeric user id (uint64), NOT the name — see preAuthKeyRequest
		Reusable:   true,
		Ephemeral:  ephemeral,
		Expiration: time.Now().UTC().Add(expiry).Format(time.RFC3339),
		ACLTags:    tags,
	}
	var resp preAuthKeyResponse
	if err := h.do(ctx, http.MethodPost, "/api/v1/preauthkey", req, &resp); err != nil {
		return "", err
	}
	if resp.PreAuthKey != nil && resp.PreAuthKey.Key != "" {
		return resp.PreAuthKey.Key, nil
	}
	if resp.Key != "" {
		return resp.Key, nil
	}
	return "", fmt.Errorf("mesh: headscale preauthkey response had no key")
}

// nodeListResponse tolerates Headscale's node listing. Field names vary across
// versions, so both givenName/given_name and name are decoded and the id is
// accepted as either string or number.
type nodeListResponse struct {
	Nodes []headscaleNode `json:"nodes"`
}

type headscaleNode struct {
	ID        json.Number `json:"id"`
	IDAlt     json.Number `json:"nodeId"`
	Name      string      `json:"name"`
	GivenName string      `json:"givenName"`
}

// FindDeviceByHostname lists nodes for the configured user via
// GET /api/v1/node?user=<user> and returns the id of the first node whose
// givenName OR name equals hostname. It returns "" if none match.
func (h *Headscale) FindDeviceByHostname(ctx context.Context, hostname string) (string, error) {
	q := url.Values{}
	q.Set("user", h.user)
	path := "/api/v1/node?" + q.Encode()

	var resp nodeListResponse
	if err := h.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return "", err
	}
	for _, n := range resp.Nodes {
		if n.GivenName == hostname || n.Name == hostname {
			if id := n.ID.String(); id != "" && id != "0" {
				return id, nil
			}
			if id := n.IDAlt.String(); id != "" && id != "0" {
				return id, nil
			}
		}
	}
	return "", nil
}

// approveRoutesRequest is the body POSTed to
// /api/v1/node/{node_id}/approve_routes (proto SetApprovedRoutesRequest).
//
// Routes carries no omitempty and is never left nil: the endpoint REPLACES a
// node's approved set with exactly what is sent, so `{"routes":[]}` is how the
// last approval is withdrawn. A missing/null field would leave stale approvals
// standing on the node.
type approveRoutesRequest struct {
	Routes []string `json:"routes"`
}

// ApproveNodeRoutes replaces the approved subnet routes on one node with
// exactly routes, via POST /api/v1/node/{node_id}/approve_routes.
//
// The policy autoApprovers (EnsurePolicy) only cover routes a node advertises
// while the policy already names them; a node that registered first, or one
// whose advertised set changed, keeps its routes pending until they are
// approved here. This is that approval, and it is a REPLACE, not a merge:
// passing the desired set removes approvals that are no longer desired, and
// passing an empty set clears them all. Nothing the node merely advertises is
// approved — only what the caller asks for.
//
// routes are trimmed, de-duplicated and sorted so a caller's ordering or
// whitespace can't turn an unchanged set into a different request body.
func (h *Headscale) ApproveNodeRoutes(ctx context.Context, nodeID string, routes []string) error {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return fmt.Errorf("mesh: headscale approve routes requires a node id")
	}
	clean := uniquePolicyStrings(routes)
	if clean == nil {
		clean = []string{}
	}
	path := "/api/v1/node/" + url.PathEscape(nodeID) + "/approve_routes"
	return h.do(ctx, http.MethodPost, path, approveRoutesRequest{Routes: clean}, nil)
}

// DeleteDevice removes a node via DELETE /api/v1/node/{id}. A 404 is treated as
// success so the call is idempotent.
func (h *Headscale) DeleteDevice(ctx context.Context, deviceID string) error {
	path := "/api/v1/node/" + url.PathEscape(deviceID)
	if err := h.do(ctx, http.MethodDelete, path, nil, nil); err != nil {
		if isHTTPStatus(err, http.StatusNotFound) {
			return nil
		}
		return err
	}
	return nil
}

// userRequest is the body POSTed to /api/v1/user.
type userRequest struct {
	Name string `json:"name"`
}

// EnsureUser makes sure the configured Headscale user/namespace exists before
// keys are minted. It is NOT part of Provider; registration calls it. It is
// idempotent + version-tolerant: it looks the user up first (the box's
// cloud-init pre-creates it, so "already there" is the common case) and only
// POSTs /api/v1/user when absent. Headscale 0.26.1 returns HTTP 500 carrying
// "UNIQUE constraint failed: users.name" for a duplicate (older versions
// 400/409); relying on the status code alone made registration spuriously
// fail, so that body is also treated as "already exists".
func (h *Headscale) EnsureUser(ctx context.Context) error {
	if id, err := h.resolveUserID(ctx); err == nil && id != "" {
		return nil
	}
	req := userRequest{Name: h.user}
	if err := h.do(ctx, http.MethodPost, "/api/v1/user", req, nil); err != nil {
		if isHTTPStatus(err, http.StatusBadRequest) || isHTTPStatus(err, http.StatusConflict) {
			return nil
		}
		if he, ok := err.(*httpError); ok && strings.Contains(he.body, "UNIQUE constraint failed: users.name") {
			return nil
		}
		return err
	}
	return nil
}

// policySetRequest is the 0.26.x REST-gateway body for PUT /api/v1/policy.
// The API does not accept the policy object as the top-level JSON body; it
// accepts a JSON object with a string field containing the HuJSON/JSON policy
// text, matching proto SetPolicyRequest{policy:string}.
type policySetRequest struct {
	Policy string `json:"policy"`
}

type headscalePolicy struct {
	TagOwners     map[string][]string  `json:"tagOwners"`
	AutoApprovers *policyAutoApprovers `json:"autoApprovers,omitempty"`
	ACLs          []headscalePolicyACL `json:"acls"`
}

type policyAutoApprovers struct {
	Routes map[string][]string `json:"routes"`
}

type headscalePolicyACL struct {
	Action string   `json:"action"`
	Proto  string   `json:"proto,omitempty"`
	Src    []string `json:"src"`
	Dst    []string `json:"dst"`
}

// PolicyACL is an exported ACL rule a caller can add to the generated Headscale
// policy alongside the central-authored mesh and route-distribution rules. Src
// and Dst are cleaned (trimmed, de-duped, sorted) at marshal time and a rule
// with an empty Src or Dst is dropped, so a caller may pass a route-derived set
// that happens to be empty without producing an empty/invalid ACL. Action
// defaults to "accept" when blank. Proto is omitted when blank.
type PolicyACL struct {
	Action string
	Proto  string
	Src    []string
	Dst    []string
}

// EnsurePolicy pushes the desired ACL policy into Headscale's database policy
// API. It is idempotent by construction: repeating the call overwrites the box
// with the same desired policy. extraACLs are appended after the mesh and
// route-distribution rules (used for the narrow kubelet-transparency rule).
func (h *Headscale) EnsurePolicy(ctx context.Context, tagOwners map[string][]string, routeApprovers map[string][]string, extraACLs []PolicyACL) error {
	cleanTagOwners := clonePolicyStringMap(tagOwners)
	tags := sortedPolicyKeys(cleanTagOwners)
	if len(tags) == 0 {
		return fmt.Errorf("mesh: headscale policy requires at least one tag owner")
	}

	tagPeers := make([]string, 0, len(tags))
	for _, tag := range tags {
		tagPeers = append(tagPeers, tag+":*")
	}
	policy := headscalePolicy{
		TagOwners: cleanTagOwners,
		ACLs: []headscalePolicyACL{
			{
				Action: "accept",
				Src:    tags,
				Dst:    tagPeers,
			},
		},
	}

	cleanRouteApprovers := clonePolicyStringMap(routeApprovers)
	routes := sortedPolicyKeys(cleanRouteApprovers)
	if len(routes) > 0 {
		routePeers := make([]string, 0, len(routes))
		for _, route := range routes {
			routePeers = append(routePeers, route+":*")
		}
		policy.AutoApprovers = &policyAutoApprovers{Routes: cleanRouteApprovers}
		policy.ACLs = append(policy.ACLs, headscalePolicyACL{
			Action: "accept",
			Src:    tags,
			Dst:    routePeers,
		})
	}

	for _, extra := range extraACLs {
		src := uniquePolicyStrings(extra.Src)
		dst := uniquePolicyStrings(extra.Dst)
		if len(src) == 0 || len(dst) == 0 {
			continue // drop a rule with no source or destination (e.g. empty routes)
		}
		action := strings.TrimSpace(extra.Action)
		if action == "" {
			action = "accept"
		}
		policy.ACLs = append(policy.ACLs, headscalePolicyACL{
			Action: action,
			Proto:  strings.TrimSpace(extra.Proto),
			Src:    src,
			Dst:    dst,
		})
	}

	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("mesh: headscale marshal policy: %w", err)
	}
	return h.do(ctx, http.MethodPut, "/api/v1/policy", policySetRequest{Policy: string(policyJSON)}, nil)
}

func clonePolicyStringMap(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for key, values := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		cleanValues := uniquePolicyStrings(values)
		if len(cleanValues) == 0 {
			continue
		}
		out[key] = cleanValues
	}
	return out
}

func uniquePolicyStrings(values []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func sortedPolicyKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// do issues an authenticated JSON request to the Headscale API. body, if
// non-nil, is JSON-encoded; out, if non-nil, receives the decoded JSON
// response. Non-2xx responses are returned as *httpError carrying the status
// and body.
func (h *Headscale) do(ctx context.Context, method, path string, body, out any) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("mesh: headscale marshal request: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}

	// Refuse to attach the Headscale API key to anything but a TLS-protected
	// URL. The base URL ultimately comes from customer-controlled state, so a
	// misconfigured/downgraded/typo'd http:// value must never leak the bearer
	// token in cleartext. A loopback carve-out keeps httptest-based tests (and
	// local-only debugging) working.
	if err := requireSecureBaseURL(h.baseURL); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, method, h.baseURL+path, reqBody)
	if err != nil {
		return fmt.Errorf("mesh: headscale build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+h.apiKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := h.http.Do(req)
	if err != nil {
		return fmt.Errorf("mesh: headscale %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &httpError{status: resp.StatusCode, body: string(respBody)}
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("mesh: headscale decode response: %w", err)
		}
	}
	return nil
}

// httpError is returned for non-2xx Headscale responses; it carries the HTTP
// status code and the (truncated-by-caller-if-needed) response body.
type httpError struct {
	status int
	body   string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("mesh: headscale http %d: %s", e.status, e.body)
}

// isHTTPStatus reports whether err is an *httpError with the given status code.
func isHTTPStatus(err error, status int) bool {
	he, ok := err.(*httpError)
	return ok && he.status == status
}
