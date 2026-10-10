package cost

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ProvisioningSource is the authoritative live-provisioning view, read at
// scrape time. Satisfied by *CompositeSource (which combines state and
// lifecycle) or by *state.Store alone for OSS.
//
// active is how many bursts are in provider/node provisioning; oldestAge is how
// long the oldest of them has been there, and is 0 when none can be aged. An
// error means the view could not be read AT ALL — never a partial or empty one
// — and the collector publishes that as unknown rather than as zero.
type ProvisioningSource interface {
	LiveProvisioning(ctx context.Context, now time.Time) (active int, oldestAge time.Duration, err error)
}

// provisioningReadTimeout bounds the durable read a scrape triggers. A scrape
// must not hang on a database that has stopped answering: the timeout turns
// that into a read failure, which is published as source_up 0 and is exactly
// the honest answer.
const provisioningReadTimeout = 5 * time.Second

// Deliberately unlabelled. These are the aggregate the stuck-provisioning alert
// reads, and burst or customer ids here would be unbounded cardinality for a
// series nothing queries per-burst. A backend label was considered and left
// out: the alert wants "is anything stuck", which max() over a backend-split
// gauge answers identically, and the split would only add series that the
// per-burst dashboards already cover from the burst records themselves.
var (
	provisioningActiveDesc = prometheus.NewDesc(
		"yscale_burst_provisioning_active",
		"Bursts currently in provider/node provisioning, read from authoritative lifecycle and state/workload views at scrape time.",
		nil, nil,
	)
	provisioningOldestAgeDesc = prometheus.NewDesc(
		"yscale_burst_provisioning_oldest_age_seconds",
		"Age in seconds of the oldest burst still in provider/node provisioning; 0 when nothing is provisioning or nothing can be aged.",
		nil, nil,
	)
	provisioningSourceUpDesc = prometheus.NewDesc(
		"yscale_burst_provisioning_source_up",
		"1 when this scrape read the authoritative lifecycle and state/workload views, 0 when either read failed.",
		nil, nil,
	)
)

// provisioningCollector publishes the live provisioning view at scrape time.
//
// It is a Collector rather than gauges some writer keeps up to date because
// there is no such writer: the interesting state is a burst that nothing is
// happening to, so nothing would fire the update. Reading at scrape time also
// means the age is the age at scrape, not at the last event.
//
// A failed read publishes ONLY source_up 0, and deliberately omits the count
// and the age. Publishing a zero there would be indistinguishable from a
// healthy idle fleet, which is the failure mode this whole change exists to
// remove; leaving them absent makes the alert go quiet — and makes the
// companion visibility alert the thing that fires instead.
type provisioningCollector struct {
	mu  sync.RWMutex
	src ProvisioningSource
	now func() time.Time
}

// Describe declares the three series so registration still catches a name
// collision, even though Collect emits a subset of them on a failed read.
func (c *provisioningCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- provisioningActiveDesc
	ch <- provisioningOldestAgeDesc
	ch <- provisioningSourceUpDesc
}

func (c *provisioningCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	src, now := c.src, c.now
	c.mu.RUnlock()
	if src == nil {
		// No source wired (a Meter built by a test or a caller that has no
		// store). Emit nothing at all: an unwired collector knows less than a
		// failed read, and must not claim either health or a count.
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), provisioningReadTimeout)
	defer cancel()
	active, oldestAge, err := src.LiveProvisioning(ctx, now())
	if err != nil {
		ch <- prometheus.MustNewConstMetric(provisioningSourceUpDesc, prometheus.GaugeValue, 0)
		return
	}

	seconds := oldestAge.Seconds()
	if seconds < 0 {
		// The source is documented not to return one, and a negative age is
		// meaningless in the alert's comparison, so it is floored rather than
		// exported and reasoned about downstream.
		seconds = 0
	}
	ch <- prometheus.MustNewConstMetric(provisioningSourceUpDesc, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(provisioningActiveDesc, prometheus.GaugeValue, float64(active))
	ch <- prometheus.MustNewConstMetric(provisioningOldestAgeDesc, prometheus.GaugeValue, seconds)
}

// SetProvisioningSource points the live-provisioning collector at the
// authoritative composite view. Until it is called the collector publishes nothing.
//
// The collector is registered by NewMeter, so this only swaps the source —
// wiring it late (main builds the meter before it can hand over the store) can
// never fail on a duplicate registration.
func (m *Meter) SetProvisioningSource(src ProvisioningSource) {
	if m == nil {
		return
	}
	m.liveProvisioning.mu.Lock()
	m.liveProvisioning.src = src
	m.liveProvisioning.mu.Unlock()
}
