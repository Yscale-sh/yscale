package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/pricefeed/internal/feed"
)

const linodeAPIBase = "https://api.linode.com/v4"

// Linode fetches Akamai/Linode plan prices and per-region availability.
// It joins GET /v4/linode/types (pricing + specs) with
// GET /v4/regions/availability (which plans are provisionable where).
// Linode has no spot tier, so every Offering is OnDemand.
type Linode struct {
	token string // optional; /linode/types is public, availability accepts a token
	http  *http.Client
}

// NewLinode constructs the Linode source, reading LINODE_TOKEN from the
// environment if present.
func NewLinode() *Linode {
	return &Linode{
		token: os.Getenv("LINODE_TOKEN"),
		http:  &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *Linode) Name() string            { return "linode" }
func (s *Linode) Interval() time.Duration { return 30 * time.Minute }

// Fetch joins the type catalog with regional availability and emits one
// Offering per (plan, region) that is currently provisionable.
func (s *Linode) Fetch(ctx context.Context) ([]feed.Offering, error) {
	typeList, err := fetchAllPages[linodeType](ctx, s, "/linode/types")
	if err != nil {
		return nil, fmt.Errorf("linode types: %w", err)
	}
	byID := make(map[string]linodeType, len(typeList))
	for _, t := range typeList {
		byID[t.ID] = t
	}

	avail, err := fetchAllPages[linodeAvailability](ctx, s, "/regions/availability")
	if err != nil {
		return nil, fmt.Errorf("linode availability: %w", err)
	}

	now := time.Now().UTC()
	out := make([]feed.Offering, 0, len(avail))
	for _, a := range avail {
		if !a.Available {
			continue
		}
		t, ok := byID[a.Plan]
		if !ok {
			continue // a plan in availability but not in /types — skip
		}
		out = append(out, feed.Offering{
			Provider:    "linode",
			SKU:         t.ID,
			Kind:        linodeKind(t),
			Region:      a.Region,
			GPU:         t.GPUs > 0,
			GPUCount:    t.GPUs,
			VCPU:        t.VCPUs,
			MemoryMB:    t.Memory,
			Reliability: feed.OnDemand,
			USDPerHour:  priceForRegion(t, a.Region),
			Available:   true,
			ObservedAt:  now,
		})
	}
	return out, nil
}

// --- Linode API shapes ---------------------------------------------------

type linodeType struct {
	ID           string              `json:"id"`
	Class        string              `json:"class"`
	VCPUs        int                 `json:"vcpus"`
	Memory       int                 `json:"memory"` // MB
	GPUs         int                 `json:"gpus"`
	Price        linodePrice         `json:"price"`
	RegionPrices []linodeRegionPrice `json:"region_prices"`
}

type linodePrice struct {
	Hourly  float64 `json:"hourly"`
	Monthly float64 `json:"monthly"`
}

type linodeRegionPrice struct {
	ID     string  `json:"id"`
	Hourly float64 `json:"hourly"`
}

type linodeAvailability struct {
	Region    string `json:"region"`
	Plan      string `json:"plan"`
	Available bool   `json:"available"`
}

type linodeListResp[T any] struct {
	Data  []T `json:"data"`
	Page  int `json:"page"`
	Pages int `json:"pages"`
}

// --- helpers -------------------------------------------------------------

// fetchAllPages pages through a Linode list endpoint (page_size=500)
// and returns every row.
func fetchAllPages[T any](ctx context.Context, s *Linode, path string) ([]T, error) {
	var all []T
	for page := 1; ; page++ {
		var resp linodeListResp[T]
		url := fmt.Sprintf("%s%s?page=%d&page_size=500", linodeAPIBase, path, page)
		if err := s.getJSON(ctx, url, &resp); err != nil {
			return nil, err
		}
		all = append(all, resp.Data...)
		if resp.Pages <= 1 || page >= resp.Pages {
			return all, nil
		}
	}
}

func (s *Linode) getJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GET %s: status %d: %s", url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// linodeKind maps a Linode type to a yscale abstract kind. GPU plans
// resolve to their RTX class; everything else is "cpu".
func linodeKind(t linodeType) string {
	if t.GPUs == 0 {
		return "cpu"
	}
	switch {
	case strings.Contains(t.ID, "rtx6000"):
		return "rtx6000"
	case strings.Contains(t.ID, "rtx4000a"):
		return "rtx4000ada"
	default:
		return "gpu"
	}
}

// priceForRegion returns the region-specific hourly price when Linode
// publishes one for that region, else the plan's base hourly price.
func priceForRegion(t linodeType, region string) float64 {
	for _, rp := range t.RegionPrices {
		if rp.ID == region {
			return rp.Hourly
		}
	}
	return t.Price.Hourly
}
