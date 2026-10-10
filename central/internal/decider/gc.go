package decider

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/cache"
)

// CacheGC is the daemon that evicts stale, expired, or
// budget-exceeding cache and persistent volumes. Runs as a
// background loop in central; tick interval is configurable so
// integration tests can drive it deterministically.
//
// Eviction tiers (in priority order):
//
//  1. Explicit retention. ephemeral / ttl=Xh / until=<rfc3339>
//     volumes whose retention has elapsed get evicted unconditionally.
//
//  2. Source freshness (cache only). When the source bucket's
//     content-hash drifts from what we last saw, mark stale; lazy
//     refetch on next workload submit. (Implemented in resolveStorage,
//     not here — tier 2 is best-effort, not a separate loop.)
//
//  3. Tenant budget. Per-tenant maxCacheGB cap; over budget evict
//     LRU, dropping ephemeral first, then ttl, then keep last.
//     (TenantLimits not yet wired; this tier is a TODO; left in the
//     design so the structure makes sense.)
//
//  4. Cost-aware. monthly_storage_cost > 1.5 × expected_refetch_cost
//     → suggest eviction. (Same TODO as tier 3.)
type CacheGC struct {
	Store    *state.Store
	Log      *slog.Logger
	Interval time.Duration

	// SoftDeleteWindow is how long a "evicting" volume sits before
	// hard delete — gives a grace period if a workload submitted
	// during eviction wants the same volume back.
	SoftDeleteWindow time.Duration
}

// NewCacheGC builds a GC with sane defaults. Caller passes the store
// (required) and a logger.
func NewCacheGC(store *state.Store, log *slog.Logger) *CacheGC {
	if log == nil {
		log = slog.Default()
	}
	return &CacheGC{
		Store:            store,
		Log:              log,
		Interval:         1 * time.Hour,
		SoftDeleteWindow: 6 * time.Hour,
	}
}

// Run loops until ctx is cancelled. Each tick scans all PVs and
// applies the eviction policy.
func (g *CacheGC) Run(ctx context.Context) error {
	t := time.NewTicker(g.Interval)
	defer t.Stop()

	g.Log.Info("cache GC running", "interval", g.Interval)
	// Run once on startup so a long-running outage doesn't accumulate
	// expired volumes.
	g.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			g.tick(ctx)
		}
	}
}

// tick walks every volume; soft-deletes the eligible ones; hard-
// deletes anything past the soft-delete window.
func (g *CacheGC) tick(ctx context.Context) {
	now := nowUTC()
	pvs := g.Store.ListPersistentVolumes()

	stats := tickStats{}
	for _, v := range pvs {
		switch v.State {
		case "active":
			if g.shouldEvict(v, now) {
				g.softDelete(v)
				stats.softDeleted++
			}
		case "evicting":
			if v.LastUsedAt.Add(g.SoftDeleteWindow).Before(now) {
				g.hardDelete(v)
				stats.hardDeleted++
			}
		}
	}
	if stats.softDeleted+stats.hardDeleted > 0 {
		g.Log.Info("cache GC tick",
			"soft_deleted", stats.softDeleted,
			"hard_deleted", stats.hardDeleted,
			"total_pvs", len(pvs))
	}
}

type tickStats struct {
	softDeleted, hardDeleted int
}

// shouldEvict applies tier 1 (explicit retention). Tier 3/4 land
// once TenantLimits wires up.
func (g *CacheGC) shouldEvict(v *state.PersistentVolume, now time.Time) bool {
	r, err := cache.ParseRetention(v.Retention)
	if err != nil {
		// Bad retention string in state — leave it alone. This should
		// never happen since Validate runs server-side, but defensive.
		g.Log.Warn("invalid retention string in state", "pv", v.ID, "retention", v.Retention)
		return false
	}
	return r.Expired(v.LastUsedAt, now)
}

// softDelete moves v to "evicting" state — tier 4 grace-period
// behavior. Backend volume itself is left alone for now; the hard
// delete tear-down will call the backend.
func (g *CacheGC) softDelete(v *state.PersistentVolume) {
	g.Log.Info("soft-deleting volume",
		"pv", v.ID, "tenant", v.TenantID, "type", v.Type,
		"retention", v.Retention, "last_used", v.LastUsedAt)
	g.Store.MarkPersistentVolumeState(v.ID, "evicting")
}

// hardDelete removes the volume from state. The actual backend tear-
// down (Linode block-storage delete, etc.) will
// hook in here once per-backend volume APIs are wrapped — for now
// we drop the state record and the backend volume gets cleaned up by
// the per-backend CleanupOrphans pass on the next decider boot.
func (g *CacheGC) hardDelete(v *state.PersistentVolume) {
	g.Log.Info("hard-deleting volume",
		"pv", v.ID, "tenant", v.TenantID, "type", v.Type)
	g.Store.DeletePersistentVolume(v.ID)
}

// sortedByLastUsed is a small helper for tier-3 LRU eviction once
// budget caps land. Stable sort so tests are deterministic.
func sortedByLastUsed(pvs []*state.PersistentVolume) []*state.PersistentVolume {
	out := make([]*state.PersistentVolume, len(pvs))
	copy(out, pvs)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].LastUsedAt.Before(out[j].LastUsedAt)
	})
	return out
}
