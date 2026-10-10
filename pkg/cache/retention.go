package cache

import (
	"fmt"
	"strings"
	"time"
)

// Retention encodes how long a backend volume should be kept. Parsed
// from the workload-spec strings: "ephemeral" | "ttl=24h" | "until=<RFC3339>" | "keep" | "" (= "ttl=7d" default).
type Retention struct {
	Mode  string // "ephemeral" | "ttl" | "until" | "keep"
	TTL   time.Duration
	Until time.Time
}

// Default retention when none is specified — mid-of-the-road TTL
// matches the customer expectation that an unmarked cache is a
// short-lived helper, not a permanent commitment.
const defaultTTL = 7 * 24 * time.Hour

// ParseRetention turns a spec string into a Retention. Empty string
// is treated as "ttl=7d" (the platform default).
func ParseRetention(s string) (Retention, error) {
	s = strings.TrimSpace(s)
	switch {
	case s == "":
		return Retention{Mode: "ttl", TTL: defaultTTL}, nil
	case s == "ephemeral":
		return Retention{Mode: "ephemeral"}, nil
	case s == "keep":
		return Retention{Mode: "keep"}, nil
	case strings.HasPrefix(s, "ttl="):
		d, err := parseDurationExtended(strings.TrimPrefix(s, "ttl="))
		if err != nil {
			return Retention{}, fmt.Errorf("parse ttl: %w", err)
		}
		return Retention{Mode: "ttl", TTL: d}, nil
	case strings.HasPrefix(s, "until="):
		t, err := time.Parse(time.RFC3339, strings.TrimPrefix(s, "until="))
		if err != nil {
			return Retention{}, fmt.Errorf("parse until: %w", err)
		}
		return Retention{Mode: "until", Until: t}, nil
	}
	return Retention{}, fmt.Errorf("unknown retention %q (valid: ephemeral, ttl=Xh, until=<rfc3339>, keep)", s)
}

// parseDurationExtended accepts time.ParseDuration units plus "d"
// (24h) and "w" (7d) suffixes. Standard time.ParseDuration rejects
// "7d" — but operators write that all the time.
func parseDurationExtended(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	last := s[len(s)-1]
	if last == 'd' || last == 'w' {
		body := s[:len(s)-1]
		if body == "" {
			return 0, fmt.Errorf("missing number before %q", last)
		}
		var n int
		_, err := fmt.Sscanf(body, "%d", &n)
		if err != nil {
			return 0, fmt.Errorf("parse %s: %w", s, err)
		}
		mult := 24 * time.Hour
		if last == 'w' {
			mult = 7 * 24 * time.Hour
		}
		return time.Duration(n) * mult, nil
	}
	return time.ParseDuration(s)
}

// ExpiresAt returns the time the volume becomes eligible for eviction
// based on lastUsedAt + retention. For "keep", returns time.Time{}
// (zero) — caller treats zero as "never auto-evicts."
func (r Retention) ExpiresAt(lastUsedAt time.Time) time.Time {
	switch r.Mode {
	case "ephemeral":
		return lastUsedAt // expires immediately
	case "ttl":
		return lastUsedAt.Add(r.TTL)
	case "until":
		return r.Until
	case "keep":
		return time.Time{}
	}
	return time.Time{}
}

// Expired returns true when the retention has elapsed at the given
// time. "keep" mode is never Expired.
func (r Retention) Expired(lastUsedAt, now time.Time) bool {
	exp := r.ExpiresAt(lastUsedAt)
	if exp.IsZero() {
		return false
	}
	return !now.Before(exp)
}
