package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	// linodeAPIBase mirrors pkg/backends/linode's apiBase.
	linodeAPIBase = "https://api.linode.com/v4"
	// linodePageSize is the largest page the Linode API will serve.
	linodePageSize = 100
	// linodeMaxPages bounds pagination. Hitting it is an error, not a
	// stopping point: a truncated listing cannot prove absence.
	linodeMaxPages = 100
	// linodeRequestTimeout bounds each individual page request so one hung
	// socket can't eat the whole reap budget.
	linodeRequestTimeout = 20 * time.Second
)

// linodeInventory is a read-only, token-scoped view of a Linode account,
// used for exactly one thing: proving that no instance still carries a
// burst's ownership tags.
//
// It re-implements the one call it needs with net/http instead of
// importing pkg/backends/linode, whose Backend also owns provisioning,
// firewalls and deletion. Keeping the evidence path free of that type
// means the harness cannot mutate the account it is auditing, by
// construction rather than by discipline.
type linodeInventory struct {
	token    string
	baseURL  string
	client   *http.Client
	maxPages int
}

func newLinodeInventory(token string) *linodeInventory {
	return &linodeInventory{
		token:    token,
		baseURL:  linodeAPIBase,
		client:   &http.Client{Timeout: linodeRequestTimeout},
		maxPages: linodeMaxPages,
	}
}

func (l *linodeInventory) Name() string { return backendLinode }

// linodeInstance is the redacted subset of an instance row this audit
// needs. Nothing else is decoded, so nothing else can reach a report.
type linodeInstance struct {
	ID   int      `json:"id"`
	Tags []string `json:"tags"`
}

// linodeInstancePage is the pagination envelope Linode wraps collections
// in (mirrors listResponse in pkg/backends/linode).
type linodeInstancePage struct {
	Data  []linodeInstance `json:"data"`
	Page  int              `json:"page"`
	Pages int              `json:"pages"`
}

// CountBurstInstances walks every page of /linode/instances and counts the
// instances whose tag set contains BOTH yscaleBurstTag and the full
// burstID — the exact pair CreateNode stamps.
//
// Any error aborts with zero results rather than a partial count: a
// half-read account is not evidence of absence.
func (l *linodeInventory) CountBurstInstances(ctx context.Context, burstID string) (OwnedInstances, error) {
	if burstID == "" {
		return OwnedInstances{}, errors.New("linode inventory: empty burstID; refusing to audit without an ownership tag to match")
	}
	pageCap := l.maxPages
	if pageCap <= 0 {
		pageCap = linodeMaxPages
	}

	var out OwnedInstances
	for page := 1; ; page++ {
		if page > pageCap {
			return OwnedInstances{}, fmt.Errorf(
				"linode inventory: account listing exceeds %d pages; refusing to report absence from a truncated read", pageCap)
		}
		body, err := l.getInstancePage(ctx, page)
		if err != nil {
			return OwnedInstances{}, err
		}
		for _, in := range body.Data {
			if hasExactTag(in.Tags, yscaleBurstTag) && hasExactTag(in.Tags, burstID) {
				out.Count++
				out.IDs = append(out.IDs, strconv.Itoa(in.ID))
			}
		}
		if body.Pages <= page {
			return out, nil
		}
	}
}

// getInstancePage fetches one page under its own bounded context.
//
// Errors are redacted. The status code separates 401 (bad token) from 429
// (rate limited) from 5xx, which is all an operator needs; the response
// body is drained and dropped because Linode error payloads echo request
// context, and this string lands in a report. The token lives in a header
// and is never formatted into a message.
func (l *linodeInventory) getInstancePage(ctx context.Context, page int) (*linodeInstancePage, error) {
	reqCtx, cancel := context.WithTimeout(ctx, linodeRequestTimeout)
	defer cancel()

	url := fmt.Sprintf("%s/linode/instances?page=%d&page_size=%d", l.baseURL, page, linodePageSize)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("linode inventory: build request for page %d: %w", page, err)
	}
	req.Header.Set("Authorization", "Bearer "+l.token)
	req.Header.Set("Accept", "application/json")

	resp, err := l.client.Do(req)
	if err != nil {
		// A transport error carries the URL and the network cause, never
		// the Authorization header, so it is safe to surface as-is.
		return nil, fmt.Errorf("linode inventory: GET instances page %d: %w", page, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil, fmt.Errorf("linode inventory: GET instances page %d: status %d (body redacted)", page, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20+1))
	if err != nil {
		return nil, fmt.Errorf("linode inventory: read page %d: %w", page, err)
	}
	if len(body) > 1<<20 {
		return nil, fmt.Errorf("linode inventory: page %d exceeds 1MB; refusing to parse a truncated body", page)
	}
	var out linodeInstancePage
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("linode inventory: page %d response was not valid instance JSON", page)
	}
	if out.Page != page || out.Pages < out.Page {
		return nil, fmt.Errorf("linode inventory: page %d has invalid pagination metadata (page=%d pages=%d)", page, out.Page, out.Pages)
	}
	return &out, nil
}

// linodeVolume is the redacted subset of a volume row this audit needs.
type linodeVolume struct {
	ID       int  `json:"id"`
	LinodeID *int `json:"linode_id"`
}

type linodeVolumePage struct {
	Data  []linodeVolume `json:"data"`
	Page  int            `json:"page"`
	Pages int            `json:"pages"`
}

// linodeFirewallDevice is the redacted subset of a firewall device.
type linodeFirewallDevice struct {
	ID     int `json:"id"`
	Entity struct {
		ID   int    `json:"id"`
		Type string `json:"type"`
	} `json:"entity"`
}

type linodeFirewallDevicePage struct {
	Data  []linodeFirewallDevice `json:"data"`
	Page  int                    `json:"page"`
	Pages int                    `json:"pages"`
}

type linodeFirewallListPage struct {
	Data []struct {
		ID    int    `json:"id"`
		Label string `json:"label"`
	} `json:"data"`
	Page  int `json:"page"`
	Pages int `json:"pages"`
}

// yscaleBurstFirewallLabel is the shared firewall label from
// pkg/backends/linode.
const yscaleBurstFirewallLabel = "yscale-burst-fw"

// CountExactInstance checks whether the specific provider instance ID
// still exists. Returns 1 if present, 0 if 404, error on list failure.
func (l *linodeInventory) CountExactInstance(ctx context.Context, providerID int) (int, error) {
	if providerID <= 0 {
		return 0, fmt.Errorf("linode inventory: invalid provider ID %d", providerID)
	}

	reqCtx, cancel := context.WithTimeout(ctx, linodeRequestTimeout)
	defer cancel()

	url := fmt.Sprintf("%s/linode/instances/%d", l.baseURL, providerID)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("linode inventory: build instance request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+l.token)
	req.Header.Set("Accept", "application/json")

	resp, err := l.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("linode inventory: GET instance %d: %w", providerID, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return 0, nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return 1, nil
	default:
		return 0, fmt.Errorf("linode inventory: GET instance %d: status %d (body redacted)", providerID, resp.StatusCode)
	}
}

// CountAttachedVolumes walks all volumes and counts those attached to
// the given provider instance ID.
func (l *linodeInventory) CountAttachedVolumes(ctx context.Context, providerID int) (int, error) {
	if providerID <= 0 {
		return 0, fmt.Errorf("linode inventory: invalid provider ID %d for volume audit", providerID)
	}

	pageCap := l.maxPages
	if pageCap <= 0 {
		pageCap = linodeMaxPages
	}

	count := 0
	for page := 1; ; page++ {
		if page > pageCap {
			return 0, fmt.Errorf("linode inventory: volume listing exceeds %d pages; refusing to report absence from a truncated read", pageCap)
		}
		body, err := l.getPage(ctx, fmt.Sprintf("%s/volumes?page=%d&page_size=%d", l.baseURL, page, linodePageSize))
		if err != nil {
			return 0, fmt.Errorf("linode inventory: volume page %d: %w", page, err)
		}
		var vp linodeVolumePage
		if err := json.Unmarshal(body, &vp); err != nil {
			return 0, fmt.Errorf("linode inventory: volume page %d: invalid JSON", page)
		}
		if vp.Page != page || vp.Pages < vp.Page {
			return 0, fmt.Errorf("linode inventory: volume page %d: invalid pagination metadata (page=%d pages=%d)", page, vp.Page, vp.Pages)
		}
		for _, v := range vp.Data {
			if v.LinodeID != nil && *v.LinodeID == providerID {
				count++
			}
		}
		if vp.Pages <= page {
			return count, nil
		}
	}
}

// CountFirewallDevices finds the yscale-burst-fw firewall and counts
// devices attached to the given provider instance ID.
func (l *linodeInventory) CountFirewallDevices(ctx context.Context, providerID int) (int, error) {
	if providerID <= 0 {
		return 0, fmt.Errorf("linode inventory: invalid provider ID %d for firewall audit", providerID)
	}

	fwID, err := l.findBurstFirewall(ctx)
	if err != nil {
		return 0, err
	}
	if fwID == 0 {
		return 0, nil
	}

	pageCap := l.maxPages
	if pageCap <= 0 {
		pageCap = linodeMaxPages
	}

	count := 0
	for page := 1; ; page++ {
		if page > pageCap {
			return 0, fmt.Errorf("linode inventory: firewall device listing exceeds %d pages; refusing to report absence from a truncated read", pageCap)
		}
		body, err := l.getPage(ctx, fmt.Sprintf("%s/networking/firewalls/%d/devices?page=%d&page_size=%d", l.baseURL, fwID, page, linodePageSize))
		if err != nil {
			return 0, fmt.Errorf("linode inventory: firewall device page %d: %w", page, err)
		}
		var dp linodeFirewallDevicePage
		if err := json.Unmarshal(body, &dp); err != nil {
			return 0, fmt.Errorf("linode inventory: firewall device page %d: invalid JSON", page)
		}
		if dp.Page != page || dp.Pages < dp.Page {
			return 0, fmt.Errorf("linode inventory: firewall device page %d: invalid pagination metadata (page=%d pages=%d)", page, dp.Page, dp.Pages)
		}
		for _, d := range dp.Data {
			if d.Entity.Type == "linode" && d.Entity.ID == providerID {
				count++
			}
		}
		if dp.Pages <= page {
			return count, nil
		}
	}
}

func (l *linodeInventory) findBurstFirewall(ctx context.Context) (int, error) {
	pageCap := l.maxPages
	if pageCap <= 0 {
		pageCap = linodeMaxPages
	}
	for page := 1; ; page++ {
		if page > pageCap {
			return 0, fmt.Errorf("linode inventory: firewall listing exceeds %d pages; refusing to report absence from a truncated read", pageCap)
		}
		body, err := l.getPage(ctx, fmt.Sprintf("%s/networking/firewalls?page=%d&page_size=%d", l.baseURL, page, linodePageSize))
		if err != nil {
			return 0, fmt.Errorf("linode inventory: firewall page %d: %w", page, err)
		}
		var fp linodeFirewallListPage
		if err := json.Unmarshal(body, &fp); err != nil {
			return 0, fmt.Errorf("linode inventory: firewall page %d: invalid JSON", page)
		}
		if fp.Page != page || fp.Pages < fp.Page {
			return 0, fmt.Errorf("linode inventory: firewall page %d: invalid pagination metadata (page=%d pages=%d)", page, fp.Page, fp.Pages)
		}
		for _, fw := range fp.Data {
			if fw.Label == yscaleBurstFirewallLabel {
				return fw.ID, nil
			}
		}
		if fp.Pages <= page {
			return 0, nil
		}
	}
}

// getPage fetches a single paginated endpoint page. Errors are redacted.
func (l *linodeInventory) getPage(ctx context.Context, endpoint string) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, linodeRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+l.token)
	req.Header.Set("Accept", "application/json")

	resp, err := l.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil, fmt.Errorf("status %d (body redacted)", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if len(body) > 1<<20 {
		return nil, fmt.Errorf("response exceeds 1MB; refusing to parse a truncated body")
	}
	return body, nil
}

// AuditBurstResidue performs a granular audit covering tagged instances,
// exact provider instance ID, volumes attached to that instance, and
// firewall device attachments.
func (l *linodeInventory) AuditBurstResidue(ctx context.Context, burstID string, providerID int) (ProviderResidue, error) {
	owned, err := l.CountBurstInstances(ctx, burstID)
	if err != nil {
		return ProviderResidue{}, fmt.Errorf("tagged instance audit: %w", err)
	}

	exactCount, err := l.CountExactInstance(ctx, providerID)
	if err != nil {
		return ProviderResidue{}, fmt.Errorf("exact instance audit: %w", err)
	}

	volCount, err := l.CountAttachedVolumes(ctx, providerID)
	if err != nil {
		return ProviderResidue{}, fmt.Errorf("volume audit: %w", err)
	}

	fwCount, err := l.CountFirewallDevices(ctx, providerID)
	if err != nil {
		return ProviderResidue{}, fmt.Errorf("firewall device audit: %w", err)
	}

	exactIDTagged := false
	providerIDText := strconv.Itoa(providerID)
	for _, id := range owned.IDs {
		if id == providerIDText {
			exactIDTagged = true
			break
		}
	}

	return ProviderResidue{
		TaggedInstances: owned.Count,
		ExactIDInstance: exactCount,
		ExactIDTagged:   exactIDTagged,
		AttachedVolumes: volCount,
		FirewallDevices: fwCount,
	}, nil
}

// hasExactTag reports exact membership.
//
// Exactness is the whole point. "burst_ab" is a substring of
// "burst_abcdef", so a Contains-based match would let one burst's residue
// clear a different burst's audit — and the ownership tag "yscale-burst"
// is a substring of plenty of plausible user tags.
func hasExactTag(tags []string, want string) bool {
	if want == "" {
		return false
	}
	for _, t := range tags {
		if t == want {
			return true
		}
	}
	return false
}
