package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/yscale-sh/yscale/central/internal/pricing"
	"github.com/yscale-sh/yscale/pkg/workload"
)

// GPUPrices handles GET /v1/price/gpu — returns the yscale GPU price
// index in a form the CLI can display. Prices are customer-facing:
// upstream backend cost with the yscale margin already applied.
// Provider names are intentionally omitted — customers see abstract
// yscale-branded kinds, not which neocloud backs each one.
type GPUPrices struct {
	Log *slog.Logger
}

// GPUPrice is one row the CLI renders. USDPerHour/USDPerMinute already
// include the yscale margin (pricing.MarginMultiplier).
type GPUPrice struct {
	Kind         string  `json:"kind"` // "a100", "rtx4000ada", ...
	VRAMGiB      int     `json:"vram_gib"`
	Reliability  string  `json:"reliability"` // "reliable" | "spot"
	USDPerHour   float64 `json:"usd_per_hour"`
	USDPerMinute float64 `json:"usd_per_minute"`
	Note         string  `json:"note,omitempty"`
}

// gpuKind is a curated abstract GPU kind exposed in the index, with a
// representative VRAM number and a one-line note for the CLI display.
type gpuKind struct {
	kind string
	vram int
	note string
}

// linodeKinds are the RTX-class kinds backed by Linode. Linode GPU
// instances are dedicated — reliable tier only, no spot. This is the
// full priced GPU supply today; add training-class kinds here as the
// hyperscaler (AWS/GCP) GPU backends land.
var linodeKinds = []gpuKind{
	{"rtx4000ada", 20, "Ada inference / light training"},
	{"rtx6000", 24, "general-purpose ML"},
}

// ServeHTTP renders the GPU price index: every priced (kind,
// reliability) pair, customer price (margin applied) included.
func (h *GPUPrices) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	out := []GPUPrice{}

	for _, k := range linodeKinds {
		if cost, ok := pricing.LookupLinodeGPU(k.kind); ok {
			out = append(out, row(k, workload.ReliabilityReliable, cost, k.note))
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"gpus": out})
}

// row builds a GPUPrice, applying the yscale margin to the upstream
// per-hour cost.
func row(k gpuKind, reliability string, upstreamUSDPerHour float64, note string) GPUPrice {
	price := pricing.CustomerPrice(upstreamUSDPerHour)
	return GPUPrice{
		Kind:         k.kind,
		VRAMGiB:      k.vram,
		Reliability:  reliability,
		USDPerHour:   price,
		USDPerMinute: price / 60.0,
		Note:         note,
	}
}
