package cache

import (
	"testing"
	"time"
)

func TestParseRetentionExtendedUnits(t *testing.T) {
	cases := []struct {
		in      string
		wantTTL time.Duration
	}{
		{"ttl=7d", 7 * 24 * time.Hour},
		{"ttl=2w", 14 * 24 * time.Hour},
		{"ttl=30d", 30 * 24 * time.Hour},
		{"ttl=24h", 24 * time.Hour}, // standard units still work
		{"ttl=15m", 15 * time.Minute},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			r, err := ParseRetention(c.in)
			if err != nil {
				t.Fatalf("ParseRetention(%q): %v", c.in, err)
			}
			if r.TTL != c.wantTTL {
				t.Errorf("TTL = %v, want %v", r.TTL, c.wantTTL)
			}
		})
	}
}
