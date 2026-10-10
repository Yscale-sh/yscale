package sources

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"

	"github.com/yscale-sh/yscale/pricefeed/internal/feed"
)

// flyPricingURL is Fly's published Machine pricing page. Fly has no
// pricing API; this HTML page is the only source. NOTE: Fly serves the
// unrendered ERB template unless a browser User-Agent is sent — see
// fetchPage.
const flyPricingURL = "https://fly.io/docs/about/pricing/"

// Flyio scrapes the Fly.io Machine pricing page weekly. The page
// renders one table per region (id="started-machines-pricing-matrix-
// <region>"); each preset's first row gives the canonical config. On
// any fetch/parse failure Fetch returns an error — the aggregator then
// keeps the last good Fly data instead of clearing it. All Fly
// Machines are CPU; Fly GPU is deprecated.
type Flyio struct {
	url  string
	http *http.Client
}

// NewFlyio constructs the Fly source.
func NewFlyio() *Flyio {
	return &Flyio{
		url:  flyPricingURL,
		http: &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *Flyio) Name() string            { return "flyio" }
func (s *Flyio) Interval() time.Duration { return 7 * 24 * time.Hour } // weekly

// Fetch scrapes and parses the Fly pricing page. Any failure returns
// an error so the aggregator preserves the previous good data.
func (s *Flyio) Fetch(ctx context.Context) ([]feed.Offering, error) {
	page, err := s.fetchPage(ctx)
	if err != nil {
		return nil, fmt.Errorf("flyio: fetch pricing page: %w", err)
	}
	offerings, err := parseFlyPricing(page)
	if err != nil {
		return nil, fmt.Errorf("flyio: parse pricing page: %w", err)
	}
	return offerings, nil
}

func (s *Flyio) fetchPage(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, err
	}
	// Without a browser UA, Fly returns the raw, unrendered ERB
	// template (no price tables). These headers get the rendered page.
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "+
		"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
	req.Header.Set("Accept", "text/html")
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 8<<20))
}

const flyMatrixIDPrefix = "started-machines-pricing-matrix-"

// flyPresetRE matches a Fly machine-preset name (shared-cpu-2x,
// performance-16x, ...). It gates which table rows become Offerings,
// so stray tables elsewhere on the page can't be misparsed.
var flyPresetRE = regexp.MustCompile(`^(?:shared-cpu|performance)-\d+x$`)

// parseFlyPricing walks the pricing page's per-region tables and emits
// one Offering per (preset, region) — the preset's canonical first row.
func parseFlyPricing(page []byte) ([]feed.Offering, error) {
	z := html.NewTokenizer(bytes.NewReader(page))
	now := time.Now().UTC()

	var (
		out    []feed.Offering
		region string // current matrix region
		preset string // preset name from the current row's rowspan <th>
		cells  []string
		inTH   bool // capturing a rowspan <th>
		inTD   bool
		buf    strings.Builder
	)

	for {
		switch z.Next() {
		case html.ErrorToken:
			if errors.Is(z.Err(), io.EOF) {
				if len(out) == 0 {
					return nil, errors.New("no offerings parsed (page structure changed?)")
				}
				return out, nil
			}
			return nil, z.Err()

		case html.StartTagToken:
			t := z.Token()
			switch t.Data {
			case "div":
				if id := attrVal(t, "id"); strings.HasPrefix(id, flyMatrixIDPrefix) {
					region = strings.TrimPrefix(id, flyMatrixIDPrefix)
				}
			case "tr":
				preset, cells = "", nil
			case "th":
				if attrVal(t, "rowspan") != "" {
					inTH, buf = true, strings.Builder{}
				}
			case "td":
				inTD, buf = true, strings.Builder{}
			}

		case html.TextToken:
			if inTH || inTD {
				buf.WriteString(z.Token().Data)
			}

		case html.EndTagToken:
			switch z.Token().Data {
			case "th":
				if inTH {
					preset, inTH = strings.TrimSpace(buf.String()), false
				}
			case "td":
				if inTD {
					cells, inTD = append(cells, strings.TrimSpace(buf.String())), false
				}
			case "tr":
				if region != "" && flyPresetRE.MatchString(preset) {
					if o, ok := flyOffering(preset, region, cells, now); ok {
						out = append(out, o)
					}
				}
			}
		}
	}
}

// flyOffering builds an Offering from a preset's first table row. The
// cells are [cpuDesc, RAM, $/sec, $/hour, $/month].
func flyOffering(preset, region string, cells []string, now time.Time) (feed.Offering, bool) {
	if len(cells) < 4 {
		return feed.Offering{}, false
	}
	cpus := atoiFirstField(cells[0])
	ramMB := parseRAM(cells[1])
	hourly := parseUSD(cells[3])
	if cpus == 0 || ramMB == 0 || hourly == 0 {
		return feed.Offering{}, false
	}
	return feed.Offering{
		Provider:    "flyio",
		SKU:         preset,
		Kind:        "cpu",
		Region:      region,
		VCPU:        cpus,
		MemoryMB:    ramMB,
		Reliability: feed.OnDemand,
		USDPerHour:  hourly,
		Available:   true,
		ObservedAt:  now,
	}, true
}

func attrVal(t html.Token, key string) string {
	for _, a := range t.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

// atoiFirstField parses the leading integer of e.g. "8 performance".
func atoiFirstField(s string) int {
	f := strings.Fields(s)
	if len(f) == 0 {
		return 0
	}
	n, _ := strconv.Atoi(f[0])
	return n
}

// parseRAM converts "256MB" / "1GB" / "1.5GB" to megabytes.
func parseRAM(s string) int {
	s = strings.TrimSpace(s)
	switch {
	case strings.HasSuffix(s, "GB"):
		v, err := strconv.ParseFloat(strings.TrimSuffix(s, "GB"), 64)
		if err != nil {
			return 0
		}
		return int(v * 1024)
	case strings.HasSuffix(s, "MB"):
		v, _ := strconv.Atoi(strings.TrimSuffix(s, "MB"))
		return v
	}
	return 0
}

// parseUSD converts "$0.0028" to 0.0028.
func parseUSD(s string) float64 {
	s = strings.TrimPrefix(strings.TrimSpace(s), "$")
	v, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return v
}
