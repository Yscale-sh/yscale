package broker

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// TestRedisAgainstMiniredis runs the real Redis broker implementation (XAdd /
// XGroupCreate / XReadGroup / XAutoClaim / XAck / XPending) against an in-process
// miniredis, so the production code path is exercised in CI without a Redis
// server. miniredis's fake clock (SetTime) drives the idle-based redelivery
// instead of wall-clock sleeps.
func TestRedisAgainstMiniredis(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	base := time.Now()
	mr.SetTime(base) // stream PEL idle is measured against this clock

	b, err := NewRedis(ctx, "redis://"+mr.Addr(), time.Second)
	if err != nil {
		t.Fatalf("NewRedis(miniredis): %v", err)
	}
	defer b.Close()
	// Keep the poll window tiny so the "nothing left" checks return fast.
	b.pollBlock = 50 * time.Millisecond

	const stream, group, consumer = "tstream", "g", "c"

	if err := b.Publish(ctx, stream, []byte("job1")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	m := mustConsume(t, b, stream, group, consumer)
	if string(m.Payload) != "job1" {
		t.Fatalf("consume payload = %q, want job1", m.Payload)
	}

	// Un-Ack'd: once idle passes retryAfter (1s), it must be redelivered.
	mr.SetTime(base.Add(2 * time.Second))
	m2 := mustConsume(t, b, stream, group, consumer)
	if string(m2.Payload) != "job1" {
		t.Fatalf("redelivery payload = %q, want job1", m2.Payload)
	}
	if m2.Deliveries < 2 {
		t.Errorf("redelivery count = %d, want >= 2", m2.Deliveries)
	}

	// Ack → no further redelivery even after the window passes.
	mustAck(t, b, stream, group, m2.ID)
	mr.SetTime(base.Add(4 * time.Second))
	if got, _ := b.Consume(ctx, stream, group, consumer); got != nil {
		t.Fatalf("acked message redelivered: %q", got.Payload)
	}

	// A second new message still flows after the first is done.
	if err := b.Publish(ctx, stream, []byte("job2")); err != nil {
		t.Fatalf("publish job2: %v", err)
	}
	m3 := mustConsume(t, b, stream, group, consumer)
	if string(m3.Payload) != "job2" {
		t.Fatalf("second job payload = %q, want job2", m3.Payload)
	}
}

// TestRedisAckCompactsOnlyAcknowledgedHistory proves the retention contract:
// acknowledged history is bounded, while an older pending entry survives both
// trimming and a later worker reclaim.
func TestRedisAckCompactsOnlyAcknowledgedHistory(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	base := time.Now()
	mr.SetTime(base)

	b, err := NewRedis(ctx, "redis://"+mr.Addr(), time.Second)
	if err != nil {
		t.Fatalf("NewRedis(miniredis): %v", err)
	}
	defer b.Close()
	b.pollBlock = 20 * time.Millisecond

	const stream, group, consumer = "retention", "workers", "worker-a"
	for _, payload := range []string{"one", "two", "three"} {
		if err := b.Publish(ctx, stream, []byte(payload)); err != nil {
			t.Fatalf("publish %q: %v", payload, err)
		}
	}

	first := mustConsume(t, b, stream, group, consumer)
	second := mustConsume(t, b, stream, group, consumer)
	mustAckAndTrim(t, b, stream, group, second.ID)

	stats, err := b.StreamStats(ctx, stream, group)
	if err != nil {
		t.Fatalf("stats with pending work: %v", err)
	}
	if stats.Length != 3 || stats.Pending != 1 {
		t.Fatalf("stats with older pending = %+v, want length 3 pending 1", stats)
	}

	// The first entry was deliberately left pending. It must still be present
	// and reclaimable after the idle window, even though a newer entry was acked.
	mr.SetTime(base.Add(2 * time.Second))
	reclaimed := mustConsume(t, b, stream, group, "worker-b")
	if reclaimed.ID != first.ID || string(reclaimed.Payload) != "one" {
		t.Fatalf("reclaimed = %+v, want pending message %s", reclaimed, first.ID)
	}
	mustAckAndTrim(t, b, stream, group, reclaimed.ID)

	third := mustConsume(t, b, stream, group, consumer)
	if string(third.Payload) != "three" {
		t.Fatalf("third payload = %q, want three", third.Payload)
	}
	mustAckAndTrim(t, b, stream, group, third.ID)

	stats, err = b.StreamStats(ctx, stream, group)
	if err != nil {
		t.Fatalf("final stats: %v", err)
	}
	if stats.Length != 1 || stats.Pending != 0 {
		t.Fatalf("final stats = %+v, want one replay checkpoint and no pending work", stats)
	}
	if stats.MemoryBytes <= 0 {
		t.Fatalf("memory usage = %d, want a positive stream allocation", stats.MemoryBytes)
	}
}

// TestRedisAckRespectsLaggingConsumerGroups prevents one worker group from
// deleting entries another registered group has not safely passed.
func TestRedisAckRespectsLaggingConsumerGroups(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	b, err := NewRedis(ctx, "redis://"+mr.Addr(), time.Hour)
	if err != nil {
		t.Fatalf("NewRedis(miniredis): %v", err)
	}
	defer b.Close()
	b.pollBlock = 20 * time.Millisecond

	const stream = "multi-group"
	for _, payload := range []string{"one", "two", "three"} {
		if err := b.Publish(ctx, stream, []byte(payload)); err != nil {
			t.Fatalf("publish %q: %v", payload, err)
		}
	}

	lagging := mustConsume(t, b, stream, "slow", "slow-1")
	for i := 0; i < 3; i++ {
		msg := mustConsume(t, b, stream, "fast", "fast-1")
		mustAckAndTrim(t, b, stream, "fast", msg.ID)
	}
	if got := b.rdb.XLen(ctx, stream).Val(); got != 3 {
		t.Fatalf("length with lagging group = %d, want 3", got)
	}

	mustAckAndTrim(t, b, stream, "slow", lagging.ID)
	for i := 0; i < 2; i++ {
		msg := mustConsume(t, b, stream, "slow", "slow-1")
		mustAckAndTrim(t, b, stream, "slow", msg.ID)
	}
	if got := b.rdb.XLen(ctx, stream).Val(); got != 1 {
		t.Fatalf("length after all groups catch up = %d, want 1", got)
	}
}

func TestRedisTrimPreservesHistoryForIdleGroup(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	b, err := NewRedis(ctx, "redis://"+mr.Addr(), time.Hour)
	if err != nil {
		t.Fatalf("NewRedis(miniredis): %v", err)
	}
	defer b.Close()

	const stream, group = "idle-group", "not-started"
	if err := b.ensureGroup(ctx, stream, group); err != nil {
		t.Fatalf("ensure idle group: %v", err)
	}
	for _, payload := range []string{"one", "two"} {
		if err := b.Publish(ctx, stream, []byte(payload)); err != nil {
			t.Fatalf("publish %q: %v", payload, err)
		}
	}
	if err := b.TrimAckedHistory(ctx, stream); err != nil {
		t.Fatalf("trim with idle group: %v", err)
	}
	if got := b.rdb.XLen(ctx, stream).Val(); got != 2 {
		t.Fatalf("length with idle group = %d, want 2", got)
	}
}

func mustAckAndTrim(t *testing.T, b *Redis, stream, group, id string) {
	t.Helper()
	if err := b.Ack(context.Background(), stream, group, id); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := b.TrimAckedHistory(context.Background(), stream); err != nil {
		t.Fatalf("trim acknowledged history: %v", err)
	}
}
