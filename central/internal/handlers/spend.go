package handlers

import (
	"fmt"
	"net/http"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// TenantLimits are a tenant's per-tenant spend guardrails (0 = unlimited).
type TenantLimits struct {
	MaxConcurrentBursts int     `json:"max_concurrent_bursts"`
	MaxHourlyUSD        float64 `json:"max_hourly_usd"`
}

// TenantSpend is a tenant's current burst spend, derived live from its running
// bursts — no ledger. ProjectedDailyUSD is the worst case if everything keeps
// running for a day.
type TenantSpend struct {
	RunningBursts     int          `json:"running_bursts"`
	HourlyUSD         float64      `json:"hourly_usd"`
	ProjectedDailyUSD float64      `json:"projected_daily_usd"`
	Limits            TenantLimits `json:"limits"`
}

// computeSpend sums a tenant's running bursts into a spend snapshot.
func computeSpend(store *state.Store, cust *state.Customer) TenantSpend {
	return spendFromBursts(store.BurstsForCustomer(cust.ID),
		TenantLimits{cust.MaxConcurrentBursts, cust.MaxHourlyUSD})
}

// spendFromBursts is the arithmetic itself, split from the lookup so the human
// tenant surface computes usage the SAME way this route does. It has to be one
// implementation: a console showing a number that disagrees with the cap the
// admission path enforces reads as a billing bug, and the two would drift the
// first time either side changed.
func spendFromBursts(running []*state.Burst, limits TenantLimits) TenantSpend {
	var hourly float64
	for _, b := range running {
		hourly += b.HourlyUSD
	}
	return TenantSpend{
		RunningBursts:     len(running),
		HourlyUSD:         hourly,
		ProjectedDailyUSD: hourly * 24,
		Limits:            limits,
	}
}

// admitBurst is the admission decision for one new burst. It returns
// (0, "") to admit, or an HTTP status + operator/customer-facing message when a
// tenant is at a ceiling. The candidate's slot and rate are evaluated as part of
// the total: running + reserved + candidate must not exceed the limit. Zero
// limits are unlimited; each burst's deadline bounds the daily bill to
// MaxHourlyUSD × 24.
func admitBurst(s TenantSpend, candidateHourlyUSD float64) (int, string) {
	if s.Limits.MaxConcurrentBursts > 0 && s.RunningBursts+1 > s.Limits.MaxConcurrentBursts {
		return http.StatusTooManyRequests, fmt.Sprintf(
			"concurrent burst limit reached (%d running, max %d) — let some finish first",
			s.RunningBursts, s.Limits.MaxConcurrentBursts)
	}
	if s.Limits.MaxHourlyUSD > 0 && s.HourlyUSD+candidateHourlyUSD > s.Limits.MaxHourlyUSD {
		return http.StatusPaymentRequired, fmt.Sprintf(
			"hourly spend cap reached ($%.2f/hr running + $%.2f/hr candidate, cap $%.2f/hr) — existing bursts must finish first",
			s.HourlyUSD, candidateHourlyUSD, s.Limits.MaxHourlyUSD)
	}
	return 0, ""
}

// Spend serves GET /v1/spend: a tenant's live burst spend + its limits.
func (h *Workloads) Spend(w http.ResponseWriter, r *http.Request) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, computeSpend(h.Store, cust))
}
