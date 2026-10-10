package cost

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeProvisioningSource stands in for *state.Store. It records the scrape time
// it was handed so the collector's clock ownership is testable, and it can fail
// so the fail-closed path is reachable without a database.
type fakeProvisioningSource struct {
	mu     sync.Mutex
	active int
	oldest time.Duration
	err    error

	calls  int
	gotNow time.Time
	hadCtx bool
}

func (f *fakeProvisioningSource) LiveProvisioning(ctx context.Context, now time.Time) (int, time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.gotNow = now
	_, f.hadCtx = ctx.Deadline()
	return f.active, f.oldest, f.err
}

// observed reports what the collector did, under the same lock the collect
// goroutine writes through: the registry gathers collectors concurrently.
func (f *fakeProvisioningSource) observed() (calls int, now time.Time, bounded bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.gotNow, f.hadCtx
}

// mustNotContain is the fail-closed assertion: a series that is absent is the
// point, so its NAME must not appear anywhere in the exposition — not as a
// sample, not as a HELP or TYPE line.
func mustNotContain(t *testing.T, body, name, why string) {
	t.Helper()
	if strings.Contains(body, name) {
		t.Errorf("%s: %q is present in /metrics output:\n%s", why, name, body)
	}
}

// TestProvisioningCollectorPublishesLiveView proves the three series are
// derived from the source at scrape time, which is what lets the alert see a
// burst that nothing is happening to.
func TestProvisioningCollectorPublishesLiveView(t *testing.T) {
	m := NewMeter()
	src := &fakeProvisioningSource{active: 3, oldest: 400 * time.Second}
	m.SetProvisioningSource(src)

	body := scrapeMetrics(t, m)
	for _, want := range []string{
		"yscale_burst_provisioning_active 3",
		"yscale_burst_provisioning_oldest_age_seconds 400",
		"yscale_burst_provisioning_source_up 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in /metrics output:\n%s", want, body)
		}
	}
	calls, stamped, bounded := src.observed()
	if calls != 1 {
		t.Errorf("source read %d times for one scrape, want 1 — the series must be derived per scrape", calls)
	}
	if stamped.IsZero() {
		t.Error("collector did not stamp a scrape time; the age would be measured from an unknown clock")
	}
	if !bounded {
		t.Error("collector passed an unbounded context — a hung durable read would hang the scrape")
	}

	// A second scrape must re-read: this is live evidence, not a cached value.
	_ = scrapeMetrics(t, m)
	if calls, _, _ := src.observed(); calls != 2 {
		t.Errorf("source read %d times for two scrapes, want 2", calls)
	}
}

// TestProvisioningCollectorFailsClosedOnReadError is the honesty contract. A
// failed read must publish source_up 0 and NOTHING else: a zero count or a zero
// age is indistinguishable from a healthy idle fleet, which is precisely the
// blindness the completed-latency alert had.
func TestProvisioningCollectorFailsClosedOnReadError(t *testing.T) {
	m := NewMeter()
	m.SetProvisioningSource(&fakeProvisioningSource{
		active: 9,
		oldest: time.Hour,
		err:    errors.New("postgres is down"),
	})

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "yscale_burst_provisioning_source_up 0") {
		t.Errorf("read failure did not publish source_up 0; body:\n%s", body)
	}
	mustNotContain(t, body, "yscale_burst_provisioning_active",
		"a failed read must not publish a count")
	mustNotContain(t, body, "yscale_burst_provisioning_oldest_age_seconds",
		"a failed read must not publish a healthy zero age")
}

// TestProvisioningCollectorUnwiredPublishesNothing covers a Meter with no store
// behind it. It knows less than a failed read does, so it claims nothing at all
// — including no health.
func TestProvisioningCollectorUnwiredPublishesNothing(t *testing.T) {
	body := scrapeMetrics(t, NewMeter())
	for _, name := range []string{
		"yscale_burst_provisioning_active",
		"yscale_burst_provisioning_oldest_age_seconds",
		"yscale_burst_provisioning_source_up",
	} {
		mustNotContain(t, body, name, "an unwired collector must not claim health or a count")
	}
}

// TestProvisioningCollectorFloorsNegativeAge is defence in depth: the source is
// documented never to return one, and if it ever did, a negative age compared
// against the alert threshold would silence the alert forever.
func TestProvisioningCollectorFloorsNegativeAge(t *testing.T) {
	m := NewMeter()
	m.SetProvisioningSource(&fakeProvisioningSource{active: 1, oldest: -5 * time.Minute})

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "yscale_burst_provisioning_oldest_age_seconds 0") {
		t.Errorf("negative age was not floored to 0; body:\n%s", body)
	}
}

// TestProvisioningCollectorEmptyFleet proves the honest zero survives: a
// successful read with nothing provisioning is a real, published zero, and is
// distinguishable from the failed read above only by source_up.
func TestProvisioningCollectorEmptyFleet(t *testing.T) {
	m := NewMeter()
	m.SetProvisioningSource(&fakeProvisioningSource{})

	body := scrapeMetrics(t, m)
	for _, want := range []string{
		"yscale_burst_provisioning_active 0",
		"yscale_burst_provisioning_oldest_age_seconds 0",
		"yscale_burst_provisioning_source_up 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in /metrics output:\n%s", want, body)
		}
	}
}

// TestSetProvisioningSourceNilSafety keeps the package's nil-receiver contract:
// handlers and tests that never wire a Meter must not panic.
func TestSetProvisioningSourceNilSafety(t *testing.T) {
	var m *Meter
	m.SetProvisioningSource(&fakeProvisioningSource{})
}

// TestObserveProvisionLatencyIsUnaffectedByLiveMetrics pins the split the issue
// turns on: the completed-start histogram remains the latency SLI and is not
// touched by the live collector, and the live series are not fed by it.
func TestObserveProvisionLatencyIsUnaffectedByLiveMetrics(t *testing.T) {
	m := NewMeter()
	m.SetProvisioningSource(&fakeProvisioningSource{}) // nothing provisioning
	m.ObserveProvisionLatency(45 * time.Second)

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "yscale_burst_provision_seconds_count 1") {
		t.Errorf("completed-start histogram regressed; body:\n%s", body)
	}
	if !strings.Contains(body, "yscale_burst_provisioning_active 0") {
		t.Errorf("a completed start must not raise the live provisioning count; body:\n%s", body)
	}
}
