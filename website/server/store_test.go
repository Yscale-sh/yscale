package main

import (
	"os"
	"testing"
	"time"
)

func TestFoldEffective(t *testing.T) {
	sub := func(email, track string) signupRecord {
		return signupRecord{Ts: "2026-07-03T00:00:00Z", Email: email, Track: track}
	}
	unsub := func(email string) signupRecord {
		return signupRecord{Ts: "2026-07-03T00:00:00Z", Email: email, Action: "unsubscribe"}
	}

	t.Run("empty input", func(t *testing.T) {
		if got := foldEffective(nil); len(got) != 0 {
			t.Errorf("fold(nil) = %v, want empty", got)
		}
	})
	t.Run("last record per email wins", func(t *testing.T) {
		got := foldEffective([]signupRecord{sub("a@b.co", "oss"), sub("a@b.co", "cloud")})
		if len(got) != 1 || got[0].Track != "cloud" {
			t.Errorf("fold = %v, want single cloud record", got)
		}
	})
	t.Run("unsubscribe removes", func(t *testing.T) {
		got := foldEffective([]signupRecord{sub("a@b.co", "oss"), sub("b@c.co", "oss"), unsub("a@b.co")})
		if len(got) != 1 || got[0].Email != "b@c.co" {
			t.Errorf("fold = %v, want only b@c.co", got)
		}
	})
	t.Run("resubscribe after unsubscribe survives", func(t *testing.T) {
		got := foldEffective([]signupRecord{sub("a@b.co", "oss"), unsub("a@b.co"), sub("a@b.co", "drops")})
		if len(got) != 1 || got[0].Track != "drops" {
			t.Errorf("fold = %v, want single drops record", got)
		}
	})
	t.Run("unsubscribe of unknown address is a no-op", func(t *testing.T) {
		got := foldEffective([]signupRecord{unsub("ghost@never.io"), sub("a@b.co", "oss")})
		if len(got) != 1 || got[0].Email != "a@b.co" {
			t.Errorf("fold = %v, want only a@b.co", got)
		}
	})
	t.Run("survivors ordered by winning record", func(t *testing.T) {
		got := foldEffective([]signupRecord{
			sub("a@b.co", "oss"), sub("b@c.co", "oss"), sub("c@d.co", "oss"), sub("a@b.co", "cloud"),
		})
		want := []string{"b@c.co", "c@d.co", "a@b.co"}
		if len(got) != len(want) {
			t.Fatalf("fold has %d records, want %d", len(got), len(want))
		}
		for i, email := range want {
			if got[i].Email != email {
				t.Errorf("fold[%d].Email = %q, want %q", i, got[i].Email, email)
			}
		}
	})
}

// TestPGStore exercises the Postgres backend against a live database. Unit
// CI has no services, so it is opt-in via TEST_DATABASE_URL.
func TestPGStore(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping live Postgres test")
	}
	st, err := newPGStore(dsn)
	if err != nil {
		t.Fatalf("newPGStore: %v", err)
	}
	defer st.db.Close()
	rec := signupRecord{
		Ts:    time.Now().UTC().Format(time.RFC3339),
		Email: "pgtest@example.com",
		Track: "cloud",
		IP:    "10.0.0.1",
		UA:    "test-agent",
	}
	if err := st.append(rec); err != nil {
		t.Fatalf("append: %v", err)
	}
	recs, err := st.readAll()
	if err != nil {
		t.Fatalf("readAll: %v", err)
	}
	last := recs[len(recs)-1]
	if last.Email != rec.Email || last.Track != rec.Track || last.Ts != rec.Ts {
		t.Errorf("last record = %+v, want %+v", last, rec)
	}
}

// TestRedisLimiter exercises the Redis fixed-window limiter against a live
// server. Opt-in via TEST_REDIS_URL.
func TestRedisLimiter(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL not set; skipping live Redis test")
	}
	rl, err := newRedisLimiter(url)
	if err != nil {
		t.Fatalf("newRedisLimiter: %v", err)
	}
	defer rl.client.Close()
	ip := "203.0.113.7" // TEST-NET; unlikely to collide with real keys
	for i := 0; i < rateLimit; i++ {
		if !rl.allow(ip) {
			t.Fatalf("request %d denied, want allowed", i+1)
		}
	}
	if rl.allow(ip) {
		t.Errorf("request %d allowed, want denied", rateLimit+1)
	}
}
