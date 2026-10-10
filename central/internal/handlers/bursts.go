package handlers

import (
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// liveBurstSpendUSD is the one live-spend calculation used by every tenant
// read surface. It fails closed when a corrupted rate or future creation time
// would otherwise make a console show NaN, infinity, or a negative charge.
func liveBurstSpendUSD(b *state.Burst, now time.Time) (float64, bool) {
	if b == nil || b.HourlyUSD < 0 || math.IsNaN(b.HourlyUSD) || math.IsInf(b.HourlyUSD, 0) || now.Before(b.CreatedAt) {
		return 0, false
	}
	spent := b.HourlyUSD * now.Sub(b.CreatedAt).Hours()
	if spent < 0 || math.IsNaN(spent) || math.IsInf(spent, 0) {
		return 0, false
	}
	return spent, true
}

// liveBurstGPUTelemetry preserves the important distinction between an
// observed idle GPU (0%) and no trustworthy sample. The state writer already
// validates this range; checking again keeps corrupted legacy rows off the
// human API instead of turning them into product truth.
func liveBurstGPUTelemetry(b *state.Burst) (*float64, *time.Time) {
	if b == nil || b.LastHeartbeatAt == nil || b.LastHeartbeatAt.IsZero() ||
		b.GPUUtilPercent < 0 || b.GPUUtilPercent > 100 ||
		math.IsNaN(b.GPUUtilPercent) || math.IsInf(b.GPUUtilPercent, 0) {
		return nil, nil
	}
	util := b.GPUUtilPercent
	heartbeat := *b.LastHeartbeatAt
	return &util, &heartbeat
}

// BurstView is the dashboard-safe projection of a burst: display fields only,
// no credentials or internal box URLs (MeshLoginServer, BackendID, AgentID are
// intentionally omitted). Consumed by read-only cockpits (e.g. the kubagachi
// yscale tab) via GET /v1/bursts.
type BurstView struct {
	ID           string    `json:"id"`
	Backend      string    `json:"backend"`     // flyio | linode | aws
	NodeName     string    `json:"node_name"`   // ys-burst-...
	TSHostname   string    `json:"ts_hostname"` // mesh device hostname
	Status       string    `json:"status"`      // provisioning | running | ...
	SKU          string    `json:"sku"`         // backend machine class
	HourlyUSD    float64   `json:"hourly_usd"`  // upstream per-hour rate
	AccruedUSD   float64   `json:"accrued_usd"` // hourly x age (live estimate)
	CreatedAt    time.Time `json:"created_at"`
	AgeSeconds   int64     `json:"age_seconds"`
	MeshProvider string    `json:"mesh_provider"`      // tailscale | box | "" legacy
	PodCIDR      string    `json:"pod_cidr"`           // 10.244.N.0/24
	Deadline     string    `json:"deadline,omitempty"` // spec.budget.deadline, human ("30m")
	MaxUSD       float64   `json:"max_usd,omitempty"`  // spec.budget.maxUSD

	// GPU utilisation from the burst node's nvidia-smi heartbeat, carried on
	// the connector's authenticated websocket heartbeat. Pointer so JSON
	// distinguishes absent telemetry (null/omitted) from an observed 0%.
	GPUUtilPercent *float64 `json:"gpu_util_percent,omitempty"`

	// LastHeartbeatAt is the timestamp of the most recent GPU telemetry
	// observation from the burst node.
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`

	// The last burst-node lifecycle phase the customer's connector reported,
	// alongside Status rather than folded into it. Status is what central claims
	// about the burst; the phase is what was actually observed in the cluster,
	// and a reader chasing "why does this say degraded" needs the observation,
	// its short reason, and when it landed. All omitted until a connector
	// reports one, so a burst whose node has not registered yet says nothing
	// rather than claiming a phase nobody saw.
	NodePhase       string     `json:"node_phase,omitempty"`
	NodePhaseReason string     `json:"node_phase_reason,omitempty"`
	NodePhaseAt     *time.Time `json:"node_phase_at,omitempty"`
}

// burstView projects a stored burst into its dashboard-safe form.
func burstView(b *state.Burst, now time.Time) BurstView {
	age := now.Sub(b.CreatedAt)
	accrued, _ := liveBurstSpendUSD(b, now)
	util, heartbeat := liveBurstGPUTelemetry(b)
	v := BurstView{
		ID:              b.ID,
		Backend:         b.Backend,
		NodeName:        b.NodeName,
		TSHostname:      b.TSHostname,
		Status:          b.Status,
		SKU:             b.SKU,
		HourlyUSD:       b.HourlyUSD,
		AccruedUSD:      accrued,
		CreatedAt:       b.CreatedAt,
		AgeSeconds:      int64(age.Seconds()),
		MeshProvider:    b.MeshProvider,
		PodCIDR:         b.PodCIDR,
		MaxUSD:          b.MaxUSD,
		GPUUtilPercent:  util,
		LastHeartbeatAt: heartbeat,
		NodePhase:       b.NodePhase,
		NodePhaseReason: b.NodePhaseReason,
		NodePhaseAt:     b.NodePhaseAt,
	}
	if b.Deadline > 0 {
		v.Deadline = b.Deadline.String()
	}
	return v
}

// Bursts serves GET /v1/bursts: the calling tenant's live bursts, newest first.
// Read-only and tenant-scoped (BurstsForCustomer keys on the authenticated
// customer), so it never leaks another tenant's fleet.
func (h *Workloads) Bursts(w http.ResponseWriter, r *http.Request) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	now := time.Now()
	bursts := h.Store.BurstsForCustomer(cust.ID)
	out := make([]BurstView, 0, len(bursts))
	for _, b := range bursts {
		out = append(out, burstView(b, now))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	writeJSON(w, http.StatusOK, map[string]any{
		"bursts": out,
		"count":  len(out),
	})
}
