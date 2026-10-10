package state

import (
	"testing"
	"time"
)

func TestPersistentVolumeCacheLookup(t *testing.T) {
	s := New()
	v := &PersistentVolume{
		ID:       "pv_1",
		TenantID: "cust_x",
		Type:     "cache",
		CacheKey: "ck_abc",
		Backend:  "linode",
		DCRegion: "EU-RO-1",
		State:    "active",
	}
	s.PutPersistentVolume(v)

	got, err := s.GetCacheVolume("cust_x", "ck_abc")
	if err != nil {
		t.Fatalf("GetCacheVolume: %v", err)
	}
	if got.ID != "pv_1" {
		t.Errorf("got %s, want pv_1", got.ID)
	}

	if _, err := s.GetCacheVolume("cust_y", "ck_abc"); err != ErrNotFound {
		t.Errorf("cross-tenant lookup should miss, got %v", err)
	}
}

func TestPersistentVolumeNamedLookup(t *testing.T) {
	s := New()
	v := &PersistentVolume{
		ID:       "pv_2",
		TenantID: "cust_x",
		Type:     "persistent",
		Name:     "training-state",
		Backend:  "linode",
		State:    "active",
	}
	s.PutPersistentVolume(v)
	got, err := s.GetNamedVolume("cust_x", "training-state")
	if err != nil {
		t.Fatalf("GetNamedVolume: %v", err)
	}
	if got.ID != "pv_2" {
		t.Errorf("got %s, want pv_2", got.ID)
	}
}

func TestNonActiveCacheVolumeNotFound(t *testing.T) {
	s := New()
	s.PutPersistentVolume(&PersistentVolume{
		ID: "pv_3", TenantID: "cust_x", CacheKey: "ck_xyz", State: "evicting",
	})
	if _, err := s.GetCacheVolume("cust_x", "ck_xyz"); err != ErrNotFound {
		t.Errorf("evicting volumes should be invisible to lookup, got %v", err)
	}
}

func TestTouchPersistentVolume(t *testing.T) {
	s := New()
	old := time.Now().UTC().Add(-1 * time.Hour)
	s.PutPersistentVolume(&PersistentVolume{
		ID: "pv_t", TenantID: "cust_x", CacheKey: "ck_t", State: "active", LastUsedAt: old,
	})
	s.TouchPersistentVolume("pv_t")
	got, _ := s.GetPersistentVolume("pv_t")
	if !got.LastUsedAt.After(old) {
		t.Errorf("LastUsedAt not advanced: %v", got.LastUsedAt)
	}
}

func TestDeletePersistentVolume(t *testing.T) {
	s := New()
	s.PutPersistentVolume(&PersistentVolume{
		ID: "pv_d", TenantID: "cust_x", CacheKey: "ck_d", Name: "dn", State: "active",
	})
	s.DeletePersistentVolume("pv_d")
	if _, err := s.GetPersistentVolume("pv_d"); err == nil {
		t.Errorf("PV still present after delete")
	}
	if _, err := s.GetCacheVolume("cust_x", "ck_d"); err == nil {
		t.Errorf("cache index not cleaned")
	}
	if _, err := s.GetNamedVolume("cust_x", "dn"); err == nil {
		t.Errorf("name index not cleaned")
	}
}
