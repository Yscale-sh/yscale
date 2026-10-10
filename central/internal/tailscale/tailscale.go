// Package tailscale is the central server's Tailscale API client. It
// mints ephemeral, pre-authorized, tag-scoped auth keys that burst
// nodes use to join yscale's tailnet at boot.
//
// One tailnet per yscale deployment; per-customer isolation is via
// ACL tags ("tag:customer-<id>-burst" can only reach
// "tag:customer-<id>-agent"). yscale owns the tailnet — customers
// don't bring their own.
//
// Auth flow:
//  1. POST https://api.tailscale.com/api/v2/oauth/token with
//     client_id + client_secret -> bearer access_token (~1hr TTL).
//  2. POST /api/v2/tailnet/{tailnet}/keys with the bearer token to
//     mint an ephemeral, reusable=true, preauthorized=true key
//     scoped to the requested tags. Reusable=true so a burst whose
//     tailscaled restarts mid-boot (image-pull retry, kubelet crash
//     before `tailscale up` completes, network blip) can re-auth
//     with the same key value; otherwise the second `tailscale up`
//     fails with "invalid key" and the burst can never finish
//     joining. Ephemeral=true keeps the security tradeoff bounded:
//     each device disappears from the tailnet on disconnect.
//
// The bearer token is cached and refreshed on demand. Mints are
// idempotent on the caller side (we generate a fresh key per burst).
package tailscale

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// Default API endpoint. Overridable for tests.
const defaultAPIBase = "https://api.tailscale.com"

// Config carries the OAuth client credentials and target tailnet.
// Populated by the central server from env vars at startup.
type Config struct {
	ClientID     string // TS_OAUTH_CLIENT_ID
	ClientSecret string // TS_OAUTH_CLIENT_SECRET
	Tailnet      string // TS_TAILNET — e.g. "yscale.github" or "-" for the default
	// APIBase is the API root. Empty -> defaultAPIBase.
	APIBase string
}

// Client mints Tailscale auth keys. Safe for concurrent use; the
// bearer token cache is protected by an internal mutex.
type Client struct {
	cfg  Config
	http *http.Client

	mu          sync.Mutex
	bearer      string
	bearerUntil time.Time
}

// New returns a Client. Returns an error if required fields are empty.
func New(cfg Config) (*Client, error) {
	if cfg.ClientID == "" {
		return nil, fmt.Errorf("tailscale: TS_OAUTH_CLIENT_ID is required")
	}
	if cfg.ClientSecret == "" {
		return nil, fmt.Errorf("tailscale: TS_OAUTH_CLIENT_SECRET is required")
	}
	if cfg.Tailnet == "" {
		return nil, fmt.Errorf("tailscale: TS_TAILNET is required")
	}
	if cfg.APIBase == "" {
		cfg.APIBase = defaultAPIBase
	}
	return &Client{
		cfg:  cfg,
		http: &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// MintAuthKey requests a new ephemeral auth key for a burst node.
// Equivalent to MintAuthKeyEphemeral(ctx, tags, expiry, true) — kept
// for backcompat with callers that don't care about the ephemeral
// distinction.
func (c *Client) MintAuthKey(ctx context.Context, tags []string, expiry time.Duration) (string, error) {
	return c.MintAuthKeyEphemeral(ctx, tags, expiry, true)
}

// MintAuthKeyEphemeral requests a new auth key with an explicit
// ephemeral flag.
//
//	ephemeral=true  — Tailscale GC's the device when it's offline
//	                 for >5 min. Right for bursts (short-lived) and
//	                 ANY device where 5min disconnect == dead.
//	ephemeral=false — Device persists until manually deleted. Right
//	                 for the gateway sidecar: its tailscaled inside
//	                 pod-network can lose DERP for >5min during
//	                 transient network blips (homelab observed
//	                 2026-05-25), and ephemeral GC would orphan the
//	                 pod's tailscaled with "404: node not found"
//	                 forever. With ephemeral=false the device stays
//	                 alive across blips; the mint endpoint's stale-
//	                 device delete on next pod start handles
//	                 cleanup of dangling entries.
//
// Returns the auth key (`tskey-auth-...`).
func (c *Client) MintAuthKeyEphemeral(ctx context.Context, tags []string, expiry time.Duration, ephemeral bool) (string, error) {
	if len(tags) == 0 {
		return "", fmt.Errorf("tailscale: at least one tag is required (ACL enforcement depends on it)")
	}
	if expiry <= 0 {
		expiry = 24 * time.Hour
	}
	token, err := c.bearerToken(ctx)
	if err != nil {
		return "", err
	}

	body := mintKeyRequest{
		Capabilities: capabilities{
			Devices: devicesCap{
				Create: createCap{
					// Reusable=true: a burst that has to retry
					// `tailscale up` during boot (kubelet restart,
					// image-pull retry, transient network) MUST be
					// able to reauth with the same key value. With
					// reusable=false the second attempt gets
					// "invalid key" and the burst is bricked.
					Reusable:      true,
					Ephemeral:     ephemeral,
					Preauthorized: true,
					Tags:          tags,
				},
			},
		},
		ExpirySeconds: int64(expiry.Seconds()),
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("tailscale: encode mint request: %w", err)
	}
	endpoint := fmt.Sprintf("%s/api/v2/tailnet/%s/keys", c.cfg.APIBase, url.PathEscape(c.cfg.Tailnet))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return "", fmt.Errorf("tailscale: build mint request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("tailscale: mint request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("tailscale: mint failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out mintKeyResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return "", fmt.Errorf("tailscale: decode mint response: %w", err)
	}
	if out.Key == "" {
		return "", fmt.Errorf("tailscale: mint response missing key")
	}
	return out.Key, nil
}

// bearerToken returns a cached bearer token, refreshing via the
// OAuth client-credentials flow if expired or near expiry.
func (c *Client) bearerToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	if c.bearer != "" && time.Until(c.bearerUntil) > 60*time.Second {
		tok := c.bearer
		c.mu.Unlock()
		return tok, nil
	}
	c.mu.Unlock()

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", c.cfg.ClientID)
	form.Set("client_secret", c.cfg.ClientSecret)
	endpoint := c.cfg.APIBase + "/api/v2/oauth/token"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("tailscale: build oauth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("tailscale: oauth request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("tailscale: oauth failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out oauthResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return "", fmt.Errorf("tailscale: decode oauth response: %w", err)
	}
	if out.AccessToken == "" {
		return "", fmt.Errorf("tailscale: oauth response missing access_token")
	}

	c.mu.Lock()
	c.bearer = out.AccessToken
	ttl := time.Duration(out.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = time.Hour
	}
	c.bearerUntil = time.Now().Add(ttl)
	c.mu.Unlock()
	return out.AccessToken, nil
}

// DeleteDevice removes a device from the tailnet by its TS device ID.
// Idempotent — a 404 from TS (already gone) is treated as success.
//
// Used by central's burst-destroy path. Without this, ephemeral devices
// pile up: TS only auto-cleans them after a *graceful* disconnect (clean
// `tailscale logout`), but Fly machine destruction is abrupt and
// tailscaled never gets to send a logout.
func (c *Client) DeleteDevice(ctx context.Context, deviceID string) error {
	if deviceID == "" {
		return nil
	}
	token, err := c.bearerToken(ctx)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/api/v2/device/%s", c.cfg.APIBase, url.PathEscape(deviceID))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("tailscale: build delete request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("tailscale: delete request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("tailscale: delete failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

// FindDeviceByHostname looks up a device's ID by its TS hostname.
// Used by burst-destroy when only the hostname is known (mint response
// contains the key but not the device ID until the device first
// connects). Returns "" if not found.
func (c *Client) FindDeviceByHostname(ctx context.Context, hostname string) (string, error) {
	if hostname == "" {
		return "", nil
	}
	token, err := c.bearerToken(ctx)
	if err != nil {
		return "", err
	}
	endpoint := fmt.Sprintf("%s/api/v2/tailnet/%s/devices", c.cfg.APIBase, url.PathEscape(c.cfg.Tailnet))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("tailscale: build list request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("tailscale: list request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("tailscale: list failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		Devices []struct {
			ID       string `json:"id"`
			Hostname string `json:"hostname"`
		} `json:"devices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("tailscale: decode list response: %w", err)
	}
	for _, d := range out.Devices {
		if d.Hostname == hostname {
			return d.ID, nil
		}
	}
	return "", nil
}

// EnsurePolicy pushes the tailnet ACL policy via the Tailscale hosted-tailnet
// ACL API. It GETs the current policy, MERGES the provided tagOwners,
// routeApprovers, and rules in additively, and POSTs the result back with the
// fetched Etag (If-Match) for optimistic-concurrency safety.
//
// Unlike the per-customer self-hosted-coordination-server path, which owns a
// whole box and can safely full-replace its policy, the shared tailnet is
// MULTI-TENANT: every yscale customer without their own mesh box rides it. An
// additive merge — never removing an entry another tenant established — is what
// keeps that isolation from regressing. A rule already present (by action/proto
// and Src/Dst set equality) is not duplicated, so the call is idempotent for an
// unchanged input.
//
// It cannot REMOVE anything, which is why the kubelet rule central manages is
// converged through ReplaceManagedKubeletACL instead. This stays the merge form
// for policy content that is only ever added to.
func (c *Client) EnsurePolicy(ctx context.Context, tagOwners, routeApprovers map[string][]string, rules []ACLRule) error {
	token, err := c.bearerToken(ctx)
	if err != nil {
		return err
	}
	current, etag, err := c.fetchACL(ctx, token)
	if err != nil {
		return err
	}
	merged := mergeTailnetACL(current, tagOwners, routeApprovers, rules)
	return c.putACL(ctx, token, etag, merged)
}

// ReplaceManagedKubeletACL converges the ONE ACL rule central manages on this
// tailnet to exactly `accept tcp <src> -> dst`, and REMOVES it outright when src
// is empty. It is the revocation half the additive EnsurePolicy above cannot do:
// a merge can only grow the rule's Src, so a tenant whose gateway CIDRs shrank —
// or whose last cluster was deleted — kept :10250 granted to CIDRs that are not
// gateways any more, indefinitely.
//
// owned is every Src central can prove it wrote here and has not yet proven it
// removed — the union recorded after the last successful write, plus the
// write-ahead claims covering writes whose receipts are still in doubt (see
// state.SharedTailnetIntent.Proof). It is the whole of the ownership proof: the
// rules replaced are the ones still carrying one of them. The destination cannot
// do that job on its own — an operator may author `accept tcp ... ->
// tag:yscale:10250` for their own reasons, and a write that took the destination
// as a marker would delete it. An EMPTY proof means central has nothing to prove
// ownership WITH (a tailnet it has never written, or a lost record), so the write
// ADDS its rule and removes none: a duplicate grant is visible and recoverable, a
// deleted operator rule is neither.
//
// src is the caller's CROSS-TENANT union, not one customer's routes. The shared
// tailnet holds a single policy document for every tenant on it, so a write
// built from one tenant's view revokes the rest; recomputing the whole union per
// call is also what makes a 412 retry converge rather than flap.
//
// An EMPTY src is a real write, not a skip, whenever there is a rule central can
// prove is its own. "No tenant has a gateway route" and "central has not looked"
// are the same thing to a merge and opposite things here: the first has to take
// the rule away. Only a call that would change NOTHING skips the POST — which is
// what keeps a forced re-assert, or a tailnet with no policy at all and nothing
// to grant, from writing a document for no reason.
//
// Everything else in the document is preserved — tag owners, route approvers,
// other tenants' and the operator's ACL rules, tests, SSH rules, node attributes
// — see fetchACLDoc and replaceManagedACLRule for how. The write is
// Etag-guarded, so a concurrent editor is a 412 the caller retries rather than a
// silent overwrite.
func (c *Client) ReplaceManagedKubeletACL(ctx context.Context, dst string, owned [][]string, src []string) error {
	if strings.TrimSpace(dst) == "" {
		return fmt.Errorf("tailscale: the managed kubelet ACL needs a destination")
	}
	token, err := c.bearerToken(ctx)
	if err != nil {
		return err
	}
	doc, etag, err := c.fetchACLDoc(ctx, token)
	if err != nil {
		return err
	}
	next, changed, err := replaceManagedACLRule(doc, dst, owned, src)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("tailscale: encode acl policy: %w", err)
	}
	return c.postACL(ctx, token, etag, raw)
}

// maxCollapseAttempts bounds the collapse's own 412 loop. A retry here is not
// the caller's ladder — it is the SAME recovery re-reading a document an
// operator moved under it — so it has to end somewhere: an editor writing the
// policy in a tight loop would otherwise hold the shared worker forever. Running
// out surfaces as an error the caller retries on its ladder, which is where a
// tailnet that busy belongs.
const maxCollapseAttempts = 4

// collapseRetryBase is the pause before the first 412 retry, doubled per attempt
// after it. Looping straight back into the GET spends every attempt inside one
// round trip of the write that just lost, which is the one pattern guaranteed to
// keep losing: an operator saving the policy from a UI, or a second central on
// its own ladder, is still mid-edit when the retry re-reads. Waiting first is
// what lets the other writer's version settle before this one selects against it.
const collapseRetryBase = 25 * time.Millisecond

// collapseRetryDelay is how long attempt waits before re-reading. Exponential
// from collapseRetryBase, plus up to half of itself as jitter so two writers that
// collided once do not line up to collide again on every attempt after it.
func collapseRetryDelay(attempt int) time.Duration {
	d := collapseRetryBase << (attempt - 1)
	return d + rand.N(d/2)
}

// CollapseManagedKubeletACL reduces the central-owned copies of the managed
// kubelet rule on this tailnet to ONE that is already there, and returns the Src
// that write left behind. It is the exceptional-recovery half of the exact
// replace: what the shared-tailnet reconciler runs, holding a FULL ownership
// proof set, to get back to a document it can name in one union again.
//
// pushed is the union central recorded after its last completed write; claims
// are the write-ahead ownership claims still outstanding, oldest first. Together
// they are the durable proof set, exactly as ReplaceManagedKubeletACL's owned —
// but here their ORDER matters, because one of them is about to be installed.
//
// SELECTION IS MADE FROM THE DOCUMENT, not from the proof set, and that is the
// whole point of this call existing. A claim is durable BEFORE its POST, so a
// claim can name a union that never reached the tailnet at all: a recovery that
// installed the newest claim on the strength of the record alone would GRANT
// :10250 to CIDRs the document has never carried, and the proof set gives it no
// way to notice. So the target is picked from the unions this GET can see —
// pushed first, since it is the one central has a completed write for, then the
// newest claim actually present — and when no proven rule is on the document at
// all the target is EMPTY, because removing central's own rules is the only
// honest thing left to write.
//
// The selection and the replace share ONE Etag. A 412 means the document moved
// between them, so the whole loop restarts against what the other writer landed
// — re-reading is what keeps an external edit from turning a stale selection
// into a re-grant.
//
// The result may REDUCE duplicate historical grants; it can never introduce one.
// Every CIDR the returned union carries was on the document in the exact version
// this replaced, which is what makes it safe for the caller to record as pushed.
func (c *Client) CollapseManagedKubeletACL(ctx context.Context, dst string, pushed []string, claims [][]string) ([]string, error) {
	if strings.TrimSpace(dst) == "" {
		return nil, fmt.Errorf("tailscale: the managed kubelet ACL needs a destination")
	}
	token, err := c.bearerToken(ctx)
	if err != nil {
		return nil, err
	}
	owned := provenUnions(pushed, claims)
	for attempt := 1; ; attempt++ {
		doc, etag, err := c.fetchACLDoc(ctx, token)
		if err != nil {
			return nil, err
		}
		target, err := liveManagedUnion(doc, dst, pushed, claims)
		if err != nil {
			return nil, err
		}
		next, changed, err := replaceManagedACLRule(doc, dst, owned, target)
		if err != nil {
			return nil, err
		}
		if !changed {
			// The document already holds exactly the selected target and nothing
			// else of central's. The GET is the evidence, and there is nothing to
			// write for it.
			return target, nil
		}
		raw, err := json.Marshal(next)
		if err != nil {
			return nil, fmt.Errorf("tailscale: encode acl policy: %w", err)
		}
		err = c.postACL(ctx, token, etag, raw)
		if err == nil {
			return target, nil
		}
		if !errors.Is(err, errACLPrecondition) || attempt >= maxCollapseAttempts {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("tailscale: acl collapse abandoned between retries: %w", ctx.Err())
		case <-time.After(collapseRetryDelay(attempt)):
		}
	}
}

// liveManagedUnion picks the union the collapse installs: the one central can
// prove it owns AND this document is carrying right now.
//
// The recorded PUSHED union comes first. It is the only union central has a
// completed write for, so preferring it leaves the document on the grant central
// last knew it had landed. A claim is preferred only when pushed is not there —
// a lost receipt is exactly that shape — and then the NEWEST present one, which
// is the closest to what the tenants were last asking for.
//
// Nothing present means an empty target. There is no union to hold, so the
// collapse can only take central's own rules away; inventing one from the record
// would be the re-grant this selection exists to prevent.
func liveManagedUnion(doc aclDoc, dst string, pushed []string, claims [][]string) ([]string, error) {
	var rules []json.RawMessage
	if raw, ok := doc["acls"]; ok {
		if err := json.Unmarshal(raw, &rules); err != nil {
			return nil, fmt.Errorf("tailscale: decode acls: %w", err)
		}
	}
	present := func(union []string) (bool, error) {
		if len(union) == 0 {
			return false, nil
		}
		for _, rule := range rules {
			// One candidate at a time: the answer needed here is WHICH union is
			// on the document, not whether any of them is.
			managed, err := isManagedACLRule(rule, dst, [][]string{union}, nil)
			if err != nil || managed {
				return managed, err
			}
		}
		return false, nil
	}
	if live, err := present(pushed); err != nil {
		return nil, err
	} else if live {
		return slices.Clone(pushed), nil
	}
	for i := len(claims) - 1; i >= 0; i-- {
		live, err := present(claims[i])
		if err != nil {
			return nil, err
		}
		if live {
			return slices.Clone(claims[i]), nil
		}
	}
	return nil, nil
}

// provenUnions is the proof set the collapse replaces against: the recorded
// pushed union plus every outstanding claim, empties and duplicates dropped. It
// is the same evidence ReplaceManagedKubeletACL takes as owned — mirrored here
// so this call can take the pushed union and the claims in the order the
// selection needs them.
func provenUnions(pushed []string, claims [][]string) [][]string {
	out := make([][]string, 0, len(claims)+1)
	for _, union := range append([][]string{pushed}, claims...) {
		if len(union) == 0 || slices.ContainsFunc(out, func(u []string) bool { return equalStringSet(u, union) }) {
			continue
		}
		out = append(out, slices.Clone(union))
	}
	return out
}

// aclDoc is the tailnet ACL policy held as its RAW top-level members. The
// exact-replace path reads and writes the whole document, so every key it does
// not model — ssh, tests, groups, hosts, nodeAttrs, postures, whatever the API
// grows next — has to survive the round trip; a struct silently drops each one,
// and dropping an operator's SSH rules on a kubelet-ACL write is not a tradeoff
// anyone chose.
type aclDoc map[string]json.RawMessage

// fetchACLDoc GETs the current tailnet ACL as a raw JSON object plus its Etag
// (Accept: application/json so the API returns parseable JSON, not HuJSON). A
// 404 — no policy yet — yields an empty document and no Etag; the caller then
// POSTs without If-Match.
func (c *Client) fetchACLDoc(ctx context.Context, token string) (aclDoc, string, error) {
	endpoint := fmt.Sprintf("%s/api/v2/tailnet/%s/acl", c.cfg.APIBase, url.PathEscape(c.cfg.Tailnet))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, "", fmt.Errorf("tailscale: build acl get request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("tailscale: acl get request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return aclDoc{}, "", nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("tailscale: acl get failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	doc := aclDoc{}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, "", fmt.Errorf("tailscale: decode acl: %w", err)
	}
	return doc, resp.Header.Get("Etag"), nil
}

// replaceManagedACLRule returns doc with every rule central can PROVE it wrote
// for dst replaced by exactly one `accept tcp <src> -> dst`, or with all of them
// gone when src is empty, plus whether the document actually moved.
//
// MANAGED means action accept, proto tcp, a destination of exactly {dst}, AND a
// Src equal as a set to one of the owned unions OR to src. The first three narrow
// the search; the Src match is the part that makes it central's OWN rule rather
// than any rule shaped like it. An operator is free to write `accept tcp <their
// sources> -> tag:yscale:10250`, and nothing may delete it on the strength of a
// destination central does not have exclusive use of.
//
// Owned is a SET, not the single last-recorded union, because of the gap between
// the two writes this converges: the ACL POST lands, and the record of what it
// wrote does not. The union that write installed is durable as a claim made
// BEFORE the POST, so it is still in this list when the retry runs — even after
// the desired union has moved on twice and the rule matches neither the recorded
// pushed union nor the new src. Every union central may have installed is
// therefore removable in one pass, which is what leaves exactly the rule it wants
// behind.
//
// src counts as proof alongside them for the same reason one notch further back:
// a central that predates the claims wrote receipts only, so a rule carrying
// exactly the Src being written is recognised and replaced with itself — a no-op
// the caller can safely record. The only rule this widens to is one whose Src is
// already exactly what central wants: replacing it grants precisely what it
// granted.
//
// An empty proof proves nothing, so — with an empty src — it deletes nothing: a
// tailnet central has never written, or one whose record was lost, keeps every
// rule on it. Where the two options are a duplicate grant an operator can see
// and remove or deleting a rule that may not be central's, this takes the first.
//
// Historical copies the additive merge left behind carry DIFFERENT Srcs (one per
// distinct union it ever merged), so they are indistinguishable from an
// operator's and are deliberately not collapsed. Only ones central can name are —
// and a tailnet that somehow holds two rules central owns converges back to one.
//
// Every rule that is NOT central's is carried over as the raw JSON it arrived
// as. Re-encoding it through ACLRule would quietly drop the fields this client
// does not model — srcPosture, ipProto, a comment — off another tenant's or the
// operator's policy. A rule that cannot be decoded at all fails the whole write:
// there is no safe guess about a rule central cannot read.
func replaceManagedACLRule(doc aclDoc, dst string, owned [][]string, src []string) (aclDoc, bool, error) {
	var current []json.RawMessage
	if raw, ok := doc["acls"]; ok {
		if err := json.Unmarshal(raw, &current); err != nil {
			return nil, false, fmt.Errorf("tailscale: decode acls: %w", err)
		}
	}
	// Non-nil so an emptied list marshals as [] rather than null, which is what
	// the API wants for "no rules".
	kept := make([]json.RawMessage, 0, len(current)+1)
	var removed []json.RawMessage
	at := -1
	for _, rule := range current {
		managed, err := isManagedACLRule(rule, dst, owned, src)
		if err != nil {
			return nil, false, err
		}
		if managed {
			// The replacement lands where the FIRST managed rule was, so a
			// converged tailnet's document keeps its ordering and the diff a
			// human reads is the Src alone.
			if at < 0 {
				at = len(kept)
			}
			removed = append(removed, rule)
			continue
		}
		kept = append(kept, rule)
	}
	var inserted json.RawMessage
	if len(src) > 0 {
		encoded, err := json.Marshal(ACLRule{
			Action: "accept",
			Proto:  "tcp",
			Src:    slices.Clone(src),
			Dst:    []string{dst},
		})
		if err != nil {
			return nil, false, fmt.Errorf("tailscale: encode managed acl rule: %w", err)
		}
		if at < 0 {
			at = len(kept)
		}
		kept = append(kept, nil)
		copy(kept[at+1:], kept[at:])
		kept[at] = encoded
		inserted = encoded
	}
	// Nothing removed and nothing added leaves the document exactly as it came —
	// and so does replacing one rule with what it already says, which is what a
	// forced re-assert of an already-converged tailnet does. Neither earns a
	// POST: an unconditional write would burn the tailnet's Etag (making a
	// concurrent operator edit a 412 for no reason) and would create an
	// `{"acls":[]}` policy on a tailnet that has none.
	changed := true
	switch {
	case len(removed) == 0 && inserted == nil:
		changed = false
	case len(removed) == 1 && inserted != nil && isExactManagedRule(removed[0], dst, src):
		changed = false
	}
	if !changed {
		return doc, false, nil
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return nil, false, fmt.Errorf("tailscale: encode acls: %w", err)
	}
	next := make(aclDoc, len(doc)+1)
	maps.Copy(next, doc)
	next["acls"] = encoded
	return next, true, nil
}

// isExactManagedRule reports whether one raw rule ALREADY says exactly what the
// managed rule for dst/src would say — same four members, same values, same
// order. A rule that does is one this write would replace with itself, so the
// document does not move and there is nothing to POST.
//
// Compared field by field rather than byte by byte because the document comes
// back as the operator (or the API) formatted it: whitespace and key order carry
// no meaning here, and treating them as a difference would rewrite the policy on
// every boot.
func isExactManagedRule(raw json.RawMessage, dst string, src []string) bool {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 4 {
		// Extra members are ones this client does not model, and leaving the
		// rule alone would keep them. Normalising once is the predictable
		// outcome, so it is reported as a change.
		return false
	}
	var probe ACLRule
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	return probe.Action == "accept" && probe.Proto == "tcp" &&
		slices.Equal(probe.Dst, []string{dst}) && slices.Equal(probe.Src, src)
}

// isManagedACLRule reports whether one raw rule is one central wrote for dst:
// the accept/tcp/dst shape, carrying exactly a Src central can prove is its own —
// one of the owned unions, or the one it is about to write. It decodes only the
// four fields the identity is made of, so a rule carrying anything else still
// answers. Every candidate Src being empty matches nothing — see
// replaceManagedACLRule for why that is the safe answer and not a widening.
func isManagedACLRule(raw json.RawMessage, dst string, owned [][]string, src []string) (bool, error) {
	if len(src) == 0 && !slices.ContainsFunc(owned, func(u []string) bool { return len(u) > 0 }) {
		return false, nil
	}
	var probe struct {
		Action string   `json:"action"`
		Proto  string   `json:"proto"`
		Src    []string `json:"src"`
		Dst    []string `json:"dst"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false, fmt.Errorf("tailscale: decode acl rule %s: %w", raw, err)
	}
	if probe.Action != "accept" || probe.Proto != "tcp" ||
		len(probe.Dst) != 1 || probe.Dst[0] != dst {
		return false, nil
	}
	if len(probe.Src) == 0 {
		return false, nil
	}
	if equalStringSet(probe.Src, src) {
		return true, nil
	}
	return slices.ContainsFunc(owned, func(u []string) bool {
		return len(u) > 0 && equalStringSet(probe.Src, u)
	}), nil
}

// fetchACL GETs the current tailnet ACL as JSON (Accept: application/json so the
// API returns parseable JSON, not HuJSON). A 404 (no policy yet) yields an empty
// policy with no Etag; the caller then POSTs without If-Match. The returned
// Etag is "" when the API didn't supply one.
func (c *Client) fetchACL(ctx context.Context, token string) (tailnetACL, string, error) {
	endpoint := fmt.Sprintf("%s/api/v2/tailnet/%s/acl", c.cfg.APIBase, url.PathEscape(c.cfg.Tailnet))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return tailnetACL{}, "", fmt.Errorf("tailscale: build acl get request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return tailnetACL{}, "", fmt.Errorf("tailscale: acl get request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		// No policy set yet: seed an empty one (acls must be non-nil so json.Marshal
		// emits "acls":[] rather than omitting it).
		return tailnetACL{ACLs: []ACLRule{}}, "", nil
	}
	if resp.StatusCode != http.StatusOK {
		return tailnetACL{}, "", fmt.Errorf("tailscale: acl get failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var acl tailnetACL
	if err := json.Unmarshal(body, &acl); err != nil {
		return tailnetACL{}, "", fmt.Errorf("tailscale: decode acl: %w", err)
	}
	if acl.ACLs == nil {
		acl.ACLs = []ACLRule{}
	}
	return acl, resp.Header.Get("Etag"), nil
}

// putACL POSTs the (merged) ACL for the additive path.
func (c *Client) putACL(ctx context.Context, token, etag string, acl tailnetACL) error {
	raw, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("tailscale: encode acl policy: %w", err)
	}
	return c.postACL(ctx, token, etag, raw)
}

// errACLPrecondition marks the 412 an If-Match write is refused with. The status
// is still in the message the caller reads; this is what lets the collapse's own
// loop tell "another writer got there first, re-read and re-select" apart from
// every other refusal, which it must not retry into.
var errACLPrecondition = errors.New("tailscale: acl precondition failed")

// postACL POSTs a policy document with an If-Match Etag when one was observed. A
// 412 Precondition Failed means a concurrent writer changed the policy first;
// the resulting error lets the reconciler's worker retry the whole read-modify-
// write, which is the safe outcome — and on the exact-replace path the retry
// recomputes the cross-tenant union, so the two writers converge instead of
// taking turns clobbering each other.
func (c *Client) postACL(ctx context.Context, token, etag string, raw []byte) error {
	endpoint := fmt.Sprintf("%s/api/v2/tailnet/%s/acl", c.cfg.APIBase, url.PathEscape(c.cfg.Tailnet))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("tailscale: build acl post request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("tailscale: acl post request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusPreconditionFailed {
		return fmt.Errorf("%w: acl post failed: status %d: %s", errACLPrecondition, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("tailscale: acl post failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// mergeTailnetACL folds the caller-supplied maps and rules into cur additively.
// Existing owners/approvers/rules are preserved and never removed; provided
// values are unioned in (de-duplicated). This is the multi-tenant safety
// invariant: one customer's reconcile cannot drop another's policy.
func mergeTailnetACL(cur tailnetACL, tagOwners, routeApprovers map[string][]string, rules []ACLRule) tailnetACL {
	out := cur
	if out.TagOwners == nil {
		out.TagOwners = map[string][]string{}
	}
	for tag, owners := range tagOwners {
		out.TagOwners[tag] = unionStrings(out.TagOwners[tag], owners)
	}
	for route, approvers := range routeApprovers {
		if out.AutoApprovers == nil {
			out.AutoApprovers = &tailnetAutoApprovers{Routes: map[string][]string{}}
		} else if out.AutoApprovers.Routes == nil {
			out.AutoApprovers.Routes = map[string][]string{}
		}
		out.AutoApprovers.Routes[route] = unionStrings(out.AutoApprovers.Routes[route], approvers)
	}
	for _, rule := range rules {
		if !containsACLRule(out.ACLs, rule) {
			out.ACLs = append(out.ACLs, rule)
		}
	}
	return out
}

// unionStrings returns the de-duplicated, trimmed union of a and b, preserving
// first-seen order. Empty strings are dropped.
func unionStrings(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	for _, s := range a {
		add(s)
	}
	for _, s := range b {
		add(s)
	}
	return out
}

// containsACLRule reports whether acls already holds a rule equal to rule, where
// Src/Dst compare as sets (order-independent) so a caller that lists the same
// CIDRs in a different order still dedupes.
func containsACLRule(acls []ACLRule, rule ACLRule) bool {
	for _, a := range acls {
		if a.Action == rule.Action && a.Proto == rule.Proto &&
			equalStringSet(a.Src, rule.Src) && equalStringSet(a.Dst, rule.Dst) {
			return true
		}
	}
	return false
}

// equalStringSet reports whether a and b contain the same members regardless of
// order, without mutating the caller's slices.
func equalStringSet(a, b []string) bool {
	ca := append([]string(nil), a...)
	cb := append([]string(nil), b...)
	slices.Sort(ca)
	slices.Sort(cb)
	return slices.Equal(ca, cb)
}

// LoginServer satisfies mesh.Provider. Tailscale SaaS is the default coordination
// server, so clients need no --login-server flag — return "".
func (c *Client) LoginServer() string { return "" }

// --- wire types ---

type oauthResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

type mintKeyRequest struct {
	Capabilities  capabilities `json:"capabilities"`
	ExpirySeconds int64        `json:"expirySeconds,omitempty"`
}

type capabilities struct {
	Devices devicesCap `json:"devices"`
}

type devicesCap struct {
	Create createCap `json:"create"`
}

type createCap struct {
	Reusable      bool     `json:"reusable"`
	Ephemeral     bool     `json:"ephemeral"`
	Preauthorized bool     `json:"preauthorized"`
	Tags          []string `json:"tags,omitempty"`
}

type mintKeyResponse struct {
	ID           string    `json:"id"`
	Key          string    `json:"key"`
	Created      time.Time `json:"created"`
	Expires      time.Time `json:"expires"`
	Capabilities any       `json:"capabilities"`
}

// ACLRule is one accept/deny rule in the hosted-tailnet ACL. Its shape mirrors
// the policy ACL values BuildPolicy emits, so the policy reconciler can reuse
// the kubelet-transparency rule verbatim. It is defined here so the tailscale
// HTTP client stays a leaf package with no dependency on the policy client.
type ACLRule struct {
	Action string   `json:"action"`
	Proto  string   `json:"proto,omitempty"`
	Src    []string `json:"src"`
	Dst    []string `json:"dst"`
}

// tailnetACL is the JSON body of the Tailscale ACL policy
// (/api/v2/tailnet/{tailnet}/acl). It round-trips the subset EnsurePolicy reads
// and writes; unknown fields are ignored on GET.
type tailnetACL struct {
	TagOwners     map[string][]string   `json:"tagOwners,omitempty"`
	AutoApprovers *tailnetAutoApprovers `json:"autoApprovers,omitempty"`
	ACLs          []ACLRule             `json:"acls"`
}

// tailnetAutoApprovers mirrors the autoApprovers block; only routes are pushed.
type tailnetAutoApprovers struct {
	Routes map[string][]string `json:"routes,omitempty"`
}
