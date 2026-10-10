package decider

import (
	"context"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestGCSoftDeletesExpiredCacheVolumes(t *testing.T) {
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	nowUTC = func() time.Time { return now }
	defer func() { nowUTC = func() time.Time { return time.Now().UTC() } }()

	s := state.New()
	// Volume last used 8 days ago with default retention (= ttl=7d).
	// Should be evicted.
	expired := &state.PersistentVolume{
		ID: "pv_expired", TenantID: "cust_x", CacheKey: "ck_e", Type: "cache",
		Retention: "", State: "active",
		LastUsedAt: now.Add(-8 * 24 * time.Hour),
	}
	s.PutPersistentVolume(expired)

	// Volume marked keep — never evict.
	keeper := &state.PersistentVolume{
		ID: "pv_keep", TenantID: "cust_x", CacheKey: "ck_k", Type: "cache",
		Retention: "keep", State: "active",
		LastUsedAt: now.Add(-30 * 24 * time.Hour),
	}
	s.PutPersistentVolume(keeper)

	// Recent volume — leave alone.
	fresh := &state.PersistentVolume{
		ID: "pv_fresh", TenantID: "cust_x", CacheKey: "ck_f", Type: "cache",
		Retention: "ttl=7d", State: "active",
		LastUsedAt: now.Add(-1 * time.Hour),
	}
	s.PutPersistentVolume(fresh)

	gc := NewCacheGC(s, nil)
	gc.tick(context.Background())

	if got, _ := s.GetPersistentVolume("pv_expired"); got.State != "evicting" {
		t.Errorf("expired pv state = %q, want evicting", got.State)
	}
	if got, _ := s.GetPersistentVolume("pv_keep"); got.State != "active" {
		t.Errorf("keep pv state = %q, want active (never evict)", got.State)
	}
	if got, _ := s.GetPersistentVolume("pv_fresh"); got.State != "active" {
		t.Errorf("fresh pv state = %q, want active", got.State)
	}
}

func TestGCHardDeletesAfterSoftDeleteWindow(t *testing.T) {
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	nowUTC = func() time.Time { return now }
	defer func() { nowUTC = func() time.Time { return time.Now().UTC() } }()

	s := state.New()
	// Volume that's been "evicting" for 7h (past the 6h soft-delete window).
	s.PutPersistentVolume(&state.PersistentVolume{
		ID: "pv_old", TenantID: "cust_x", CacheKey: "ck_o", State: "evicting",
		LastUsedAt: now.Add(-7 * time.Hour),
	})

	gc := NewCacheGC(s, nil)
	gc.tick(context.Background())

	if _, err := s.GetPersistentVolume("pv_old"); err == nil {
		t.Error("expected pv_old hard-deleted, but it's still in state")
	}
}

func TestGCRespectsKeepUnderBudget(t *testing.T) {
	// Pure smoke test: a keep volume after the entire-tenant budget
	// would be exceeded should still survive at this stage (budget
	// eviction = tier 3 / future).
	s := state.New()
	s.PutPersistentVolume(&state.PersistentVolume{
		ID: "pv_keep_huge", TenantID: "cust_x", CacheKey: "ck_h", Type: "cache",
		Retention: "keep", State: "active", SizeGB: 10000,
		LastUsedAt: time.Now().UTC(),
	})
	NewCacheGC(s, nil).tick(context.Background())
	if got, _ := s.GetPersistentVolume("pv_keep_huge"); got.State != "active" {
		t.Errorf("keep volume should not be evicted in tier 1 GC, got state %q", got.State)
	}
}
