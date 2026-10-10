package cost

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/yscale-sh/yscale/pkg/broker"
)

type fakeTeardownStreamSource struct {
	mu      sync.Mutex
	stats   broker.StreamStats
	err     error
	calls   int
	stream  string
	group   string
	bounded bool
}

func (f *fakeTeardownStreamSource) StreamStats(ctx context.Context, stream, group string) (broker.StreamStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.stream = stream
	f.group = group
	_, f.bounded = ctx.Deadline()
	return f.stats, f.err
}

func TestTeardownStreamCollectorPublishesAggregateView(t *testing.T) {
	m := NewMeter()
	src := &fakeTeardownStreamSource{stats: broker.StreamStats{
		Length:      11,
		Pending:     3,
		MemoryBytes: 4096,
	}}
	m.SetTeardownStreamSource(src, "queue", "workers")

	body := scrapeMetrics(t, m)
	for _, want := range []string{
		"yscale_teardown_stream_length 11",
		"yscale_teardown_stream_pending 3",
		"yscale_teardown_stream_memory_bytes 4096",
		"yscale_teardown_stream_source_up 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in /metrics output:\n%s", want, body)
		}
	}

	src.mu.Lock()
	defer src.mu.Unlock()
	if src.calls != 1 || src.stream != "queue" || src.group != "workers" {
		t.Errorf("source call = %d %q/%q, want one queue/workers read", src.calls, src.stream, src.group)
	}
	if !src.bounded {
		t.Error("collector passed an unbounded context")
	}
}

func TestTeardownStreamCollectorFailsClosed(t *testing.T) {
	m := NewMeter()
	m.SetTeardownStreamSource(&fakeTeardownStreamSource{
		stats: broker.StreamStats{Length: 99, Pending: 8, MemoryBytes: 8192},
		err:   errors.New("redis unavailable"),
	}, "queue", "workers")

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "yscale_teardown_stream_source_up 0") {
		t.Errorf("missing failed source health in /metrics output:\n%s", body)
	}
	for _, name := range []string{
		"yscale_teardown_stream_length",
		"yscale_teardown_stream_pending",
		"yscale_teardown_stream_memory_bytes",
	} {
		mustNotContain(t, body, name, "a failed Redis read must not publish a healthy zero or stale value")
	}
}

func TestTeardownStreamCollectorUnwiredPublishesNothing(t *testing.T) {
	body := scrapeMetrics(t, NewMeter())
	for _, name := range []string{
		"yscale_teardown_stream_length",
		"yscale_teardown_stream_pending",
		"yscale_teardown_stream_memory_bytes",
		"yscale_teardown_stream_source_up",
	} {
		mustNotContain(t, body, name, "an inline deployment has no durable teardown stream")
	}
}

func TestSetTeardownStreamSourceNilSafety(t *testing.T) {
	var m *Meter
	m.SetTeardownStreamSource(&fakeTeardownStreamSource{}, "queue", "workers")
}
