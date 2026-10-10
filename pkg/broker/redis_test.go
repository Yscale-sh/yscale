package broker

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestRedisRoundTrip exercises the Redis Streams broker against a real Redis:
// publish → consume → ack, and un-Ack'd → redelivered (the durable-retry
// property). Gated on YSCALE_TEST_REDIS_URL so the default `go test` run needs
// no Redis. Uses a unique stream per run so reruns don't collide.
func TestRedisRoundTrip(t *testing.T) {
	url := os.Getenv("YSCALE_TEST_REDIS_URL")
	if url == "" {
		t.Skip("set YSCALE_TEST_REDIS_URL to a throwaway Redis to run this integration test")
	}
	ctx := context.Background()
	// Short retry window so the redelivery assertion doesn't wait long.
	b, err := NewRedis(ctx, url, 150*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer b.Close()

	stream := "yscale_test_" + time.Now().UTC().Format("150405.000000")
	const group, consumer = "g", "c"

	if err := b.Publish(ctx, stream, []byte("job1")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	m := mustConsume(t, b, stream, group, consumer)
	if string(m.Payload) != "job1" {
		t.Fatalf("consume payload = %q, want job1", m.Payload)
	}

	// Don't ack → after the retry window it must be redelivered.
	time.Sleep(250 * time.Millisecond)
	m2 := mustConsume(t, b, stream, group, consumer)
	if string(m2.Payload) != "job1" {
		t.Fatalf("redelivery payload = %q, want job1", m2.Payload)
	}
	if m2.Deliveries < 2 {
		t.Errorf("redelivery count = %d, want >= 2", m2.Deliveries)
	}

	// Ack → no further redelivery within the window.
	mustAck(t, b, stream, group, m2.ID)
	time.Sleep(250 * time.Millisecond)
	if got, _ := b.Consume(ctx, stream, group, consumer); got != nil {
		t.Fatalf("acked message redelivered: %q", got.Payload)
	}
}

// TestRedisRetentionAndStats exercises the Redis commands that miniredis can
// model functionally but cannot validate against Redis's real stream encoding
// and memory accounting.
func TestRedisRetentionAndStats(t *testing.T) {
	url := os.Getenv("YSCALE_TEST_REDIS_URL")
	if url == "" {
		t.Skip("set YSCALE_TEST_REDIS_URL to a throwaway Redis to run this integration test")
	}
	ctx := context.Background()
	b, err := NewRedis(ctx, url, time.Second)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer b.Close()
	b.pollBlock = 20 * time.Millisecond

	stream := "yscale_retention_test_" + time.Now().UTC().Format("150405.000000")
	defer func() { _ = b.rdb.Del(ctx, stream).Err() }()
	const group, consumer = "workers", "worker-1"
	for _, payload := range []string{"one", "two", "three"} {
		if err := b.Publish(ctx, stream, []byte(payload)); err != nil {
			t.Fatalf("publish %q: %v", payload, err)
		}
	}
	for i := 0; i < 3; i++ {
		msg := mustConsume(t, b, stream, group, consumer)
		mustAckAndTrim(t, b, stream, group, msg.ID)
	}

	stats, err := b.StreamStats(ctx, stream, group)
	if err != nil {
		t.Fatalf("StreamStats: %v", err)
	}
	if stats.Length != 1 || stats.Pending != 0 {
		t.Fatalf("stats after drain = %+v, want one checkpoint and no pending entries", stats)
	}
	if stats.MemoryBytes <= 0 {
		t.Fatalf("Redis-reported memory = %d, want positive bytes", stats.MemoryBytes)
	}
}
