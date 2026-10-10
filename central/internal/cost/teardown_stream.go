package cost

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/yscale-sh/yscale/pkg/broker"
)

// TeardownStreamSource is the aggregate operational view exposed by the
// durable teardown broker. Redis implements it without exposing its client.
type TeardownStreamSource interface {
	StreamStats(context.Context, string, string) (broker.StreamStats, error)
}

const teardownStreamReadTimeout = 5 * time.Second

var (
	teardownStreamLengthDesc = prometheus.NewDesc(
		"yscale_teardown_stream_length",
		"Entries retained in the durable teardown Redis stream, including pending work and the safe replay checkpoint.",
		nil, nil,
	)
	teardownStreamPendingDesc = prometheus.NewDesc(
		"yscale_teardown_stream_pending",
		"Delivered teardown entries awaiting acknowledgement in the worker consumer group.",
		nil, nil,
	)
	teardownStreamMemoryDesc = prometheus.NewDesc(
		"yscale_teardown_stream_memory_bytes",
		"Redis memory allocated to the durable teardown stream in bytes.",
		nil, nil,
	)
	teardownStreamSourceUpDesc = prometheus.NewDesc(
		"yscale_teardown_stream_source_up",
		"1 when this scrape read the durable teardown stream and consumer group, 0 when the read failed.",
		nil, nil,
	)
)

type teardownStreamCollector struct {
	mu     sync.RWMutex
	src    TeardownStreamSource
	stream string
	group  string
}

func (c *teardownStreamCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- teardownStreamLengthDesc
	ch <- teardownStreamPendingDesc
	ch <- teardownStreamMemoryDesc
	ch <- teardownStreamSourceUpDesc
}

func (c *teardownStreamCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	src, stream, group := c.src, c.stream, c.group
	c.mu.RUnlock()
	if src == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), teardownStreamReadTimeout)
	defer cancel()
	stats, err := src.StreamStats(ctx, stream, group)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(teardownStreamSourceUpDesc, prometheus.GaugeValue, 0)
		return
	}

	ch <- prometheus.MustNewConstMetric(teardownStreamSourceUpDesc, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(teardownStreamLengthDesc, prometheus.GaugeValue, float64(stats.Length))
	ch <- prometheus.MustNewConstMetric(teardownStreamPendingDesc, prometheus.GaugeValue, float64(stats.Pending))
	ch <- prometheus.MustNewConstMetric(teardownStreamMemoryDesc, prometheus.GaugeValue, float64(stats.MemoryBytes))
}

// SetTeardownStreamSource wires the fixed teardown queue into scrape-time
// metrics. Until Redis is configured the collector emits no series, so an OSS
// inline deployment does not claim to have a healthy durable queue.
func (m *Meter) SetTeardownStreamSource(src TeardownStreamSource, stream, group string) {
	if m == nil {
		return
	}
	m.teardownStream.mu.Lock()
	m.teardownStream.src = src
	m.teardownStream.stream = stream
	m.teardownStream.group = group
	m.teardownStream.mu.Unlock()
}
