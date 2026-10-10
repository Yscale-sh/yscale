package backends

import (
	"testing"
	"time"
)

// OrphanTooYoung is the single shared rule every backend's CleanupOrphans
// consults before destroying an owned-but-untracked instance. It is what makes
// a periodic sweep against a serving caller safe, so its edges matter: the
// boundary must not delete an instance that has only just reached the grace
// period, and an unknown creation time must resolve to "keep", never "delete".
func TestOrphanTooYoung(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		createdAt time.Time
		want      bool
	}{
		{"created this instant", now, true},
		{"one second old", now.Add(-time.Second), true},
		{"one second short of the grace period", now.Add(-OrphanGracePeriod + time.Second), true},
		{"exactly the grace period old", now.Add(-OrphanGracePeriod), false},
		{"one second past the grace period", now.Add(-OrphanGracePeriod - time.Second), false},
		{"days old", now.Add(-72 * time.Hour), false},
		{"unknown creation time is treated as too young", time.Time{}, true},
		// A provider clock running ahead of ours reports a future creation
		// time; that must read as too young, not as an ancient orphan.
		{"future creation time is treated as too young", now.Add(time.Hour), true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := OrphanTooYoung(c.createdAt, now); got != c.want {
				t.Errorf("OrphanTooYoung(%v, %v) = %v, want %v", c.createdAt, now, got, c.want)
			}
		})
	}
}

// The grace period has to outlast the slowest create-to-persist window (a
// long-running ARM/GCE create plus the caller's record write). A value trimmed
// down to seconds would silently reopen the create-race this whole gate exists
// to close.
func TestOrphanGracePeriodIsGenerous(t *testing.T) {
	if OrphanGracePeriod < 10*time.Minute {
		t.Errorf("OrphanGracePeriod = %v, too short to outlast a slow provider create", OrphanGracePeriod)
	}
}
