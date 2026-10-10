package cache

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDeriveKeyDeterministic(t *testing.T) {
	a := DeriveKey("cust_x", "s3://my-bucket/llama/")
	b := DeriveKey("cust_x", "s3://my-bucket/llama/")
	if a != b {
		t.Errorf("not deterministic: %s != %s", a, b)
	}
	if !strings.HasPrefix(string(a), "ck_") {
		t.Errorf("want ck_ prefix, got %s", a)
	}
}

func TestDeriveKeyTenantIsolation(t *testing.T) {
	a := DeriveKey("cust_x", "s3://b/p/")
	b := DeriveKey("cust_y", "s3://b/p/")
	if a == b {
		t.Errorf("same source, different tenants must hash differently: both %s", a)
	}
}

func TestDeriveKeyTrailingSlashIgnored(t *testing.T) {
	with := DeriveKey("cust_x", "s3://b/p/")
	without := DeriveKey("cust_x", "s3://b/p")
	if with != without {
		t.Errorf("trailing slash should be normalized: %s vs %s", with, without)
	}
}

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := &Manifest{
		Key:         Key("ck_abc123"),
		SourceURI:   "s3://b/p/",
		SizeBytes:   1024 * 1024 * 1024,
		FileCount:   42,
		PopulatedAt: time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
		SourceETags: map[string]string{"a/b.bin": "etag1", "c.bin": "etag2"},
	}
	if err := Write(dir, want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := Read(dir)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got == nil {
		t.Fatal("Read returned nil after Write")
	}
	if got.Version != ManifestVersion {
		t.Errorf("Version not auto-set: %d", got.Version)
	}
	if got.Key != want.Key || got.FileCount != want.FileCount {
		t.Errorf("round-trip mismatch: got %+v", got)
	}
	if got.SourceETags["a/b.bin"] != "etag1" {
		t.Errorf("etags lost in round trip: %v", got.SourceETags)
	}
}

func TestManifestReadAbsentReturnsNilNil(t *testing.T) {
	dir := t.TempDir()
	got, err := Read(dir)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for absent manifest, got %+v", got)
	}
}

func TestManifestRejectsHigherVersion(t *testing.T) {
	dir := t.TempDir()
	high := &Manifest{Version: ManifestVersion + 1, Key: "ck"}
	// Write directly bypassing Write() since Write resets Version.
	data := `{"version":99,"key":"ck"}`
	mustWrite(t, filepath.Join(dir, ManifestFilename), data)
	if _, err := Read(dir); err == nil {
		t.Errorf("expected error on higher version, got %+v", high)
	}
}

func TestParseRetention(t *testing.T) {
	cases := []struct {
		in   string
		mode string
		err  bool
	}{
		{"", "ttl", false},
		{"ephemeral", "ephemeral", false},
		{"keep", "keep", false},
		{"ttl=24h", "ttl", false},
		{"ttl=15m", "ttl", false},
		{"until=2026-12-31T00:00:00Z", "until", false},
		{"forever", "", true},
		{"ttl=", "", true},
		{"until=not-a-time", "", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			r, err := ParseRetention(c.in)
			if c.err {
				if err == nil {
					t.Errorf("expected error, got %+v", r)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if r.Mode != c.mode {
				t.Errorf("mode = %s, want %s", r.Mode, c.mode)
			}
		})
	}
}

func TestRetentionExpired(t *testing.T) {
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	lastUsed := now.Add(-3 * 24 * time.Hour)

	r, _ := ParseRetention("ttl=24h")
	if !r.Expired(lastUsed, now) {
		t.Error("ttl=24h, last used 3d ago, should be expired")
	}

	r, _ = ParseRetention("ttl=7d")
	if r.Expired(lastUsed, now) {
		t.Error("ttl=7d, last used 3d ago, should not be expired")
	}

	r, _ = ParseRetention("keep")
	if r.Expired(lastUsed, now) {
		t.Error("keep should never expire")
	}

	r, _ = ParseRetention("ephemeral")
	if !r.Expired(lastUsed, now) {
		t.Error("ephemeral expires immediately at lastUsed")
	}
}

func mustWrite(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}
