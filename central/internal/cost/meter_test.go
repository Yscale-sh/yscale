package cost

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// hijackableRecorder is a ResponseRecorder that also implements http.Hijacker,
// standing in for the real server's connection (which gorilla/websocket hijacks).
type hijackableRecorder struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (h *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	return nil, nil, nil
}

// The RED middleware must not break the agent-stream WebSocket upgrade: the
// wrapper it hands the handler has to remain an http.Hijacker that passes
// through. Without this, every agent's connection to central would fail.
func TestHTTPMiddlewarePassesThroughHijack(t *testing.T) {
	m := NewMeter()
	rec := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}

	var sawHijacker bool
	h := m.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("wrapped ResponseWriter is not an http.Hijacker — agent WS upgrade would break")
			return
		}
		sawHijacker = true
		_, _, _ = hj.Hijack()
	}))
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/agent/stream", nil))

	if !sawHijacker || !rec.hijacked {
		t.Fatal("Hijack did not pass through the RED middleware to the underlying ResponseWriter")
	}
}

// scrapeMetrics calls the meter's /metrics handler and returns the body as a
// string for name-presence assertions. Uses the package-internal Meter type
// directly (same package test) so the private reg is accessible via Handler().
func scrapeMetrics(t *testing.T, m *Meter) string {
	t.Helper()
	h := m.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics handler returned %d", rec.Code)
	}
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	return string(body)
}

// hasMetric checks that name appears somewhere in the exposition output.
func hasMetric(t *testing.T, body, name string) {
	t.Helper()
	if !strings.Contains(body, name) {
		t.Errorf("metric %q not found in /metrics output", name)
	}
}

// TestNewMeterRegistersAllMetrics proves that NewMeter registers every new
// reliability metric and the existing cost metrics in a single registry, so the
// existing ServiceMonitor scrape at /metrics picks up everything without
// configuration changes.
//
// Prometheus Vecs only emit output once at least one WithLabelValues call has
// been made; this test exercises every method once so all families are present.
func TestNewMeterRegistersAllMetrics(t *testing.T) {
	m := NewMeter()

	// Trigger Vec families so they appear in the /metrics output.
	m.BurstCreated("flyio", 0.10)
	m.BurstReaped("flyio", 0.10, time.Minute)
	m.SetFixedMonthly(map[string]float64{"box": 5.00})
	m.ObserveProvisionLatency(30 * time.Second)
	m.RecordMeshMint("tailscale", "ok")
	m.RecordReap("ok")
	m.RecordPersistenceFailure("burst", "upsert")
	m.SetPodCIDRFree(200)
	m.AgentConnected()
	// The live provisioning series are collected from a source at scrape time,
	// so they only exist once one is wired.
	m.SetProvisioningSource(&fakeProvisioningSource{active: 1, oldest: time.Minute})
	// Fire the HTTP middleware to populate the httpRequests / httpDuration vecs.
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	req := httptest.NewRequest(http.MethodGet, "/v1/spend", nil)
	req.Pattern = "GET /v1/spend"
	rec := httptest.NewRecorder()
	m.HTTPMiddleware(inner).ServeHTTP(rec, req)

	body := scrapeMetrics(t, m)

	// Existing cost metrics — must not regress.
	for _, name := range []string{
		"yscale_burst_active",
		"yscale_burst_hourly_usd",
		"yscale_cloud_cost_usd_total",
		"yscale_bursts_created_total",
		"yscale_bursts_reaped_total",
		"yscale_burst_lifetime_seconds",
		"yscale_fixed_cost_usd_per_month",
	} {
		hasMetric(t, body, name)
	}

	// New reliability + RED metrics.
	for _, name := range []string{
		"yscale_burst_provision_seconds",
		"yscale_burst_provisioning_active",
		"yscale_burst_provisioning_oldest_age_seconds",
		"yscale_burst_provisioning_source_up",
		"yscale_mesh_mint_total",
		"yscale_mesh_fallback_total",
		"yscale_reap_total",
		"yscale_state_persistence_failures_total",
		"yscale_podcidr_pool_free",
		"yscale_agents_connected",
		"yscale_http_requests_total",
		"yscale_http_request_duration_seconds",
	} {
		hasMetric(t, body, name)
	}
}

func TestRecordPersistenceFailureUsesBoundedLabels(t *testing.T) {
	m := NewMeter()
	m.RecordPersistenceFailure("workload", "upsert")
	m.RecordPersistenceFailure("workload", "upsert")
	m.RecordPersistenceFailure("customer", "delete")

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `yscale_state_persistence_failures_total{entity="workload",operation="upsert"} 2`) {
		t.Errorf("workload persistence failure count wrong; body:\n%s", body)
	}
	if !strings.Contains(body, `yscale_state_persistence_failures_total{entity="customer",operation="delete"} 1`) {
		t.Errorf("customer persistence failure count wrong; body:\n%s", body)
	}
}

// TestObserveProvisionLatency proves the histogram is observable and the bucket
// count increments for a value that falls within the configured buckets.
func TestObserveProvisionLatency(t *testing.T) {
	m := NewMeter()
	m.ObserveProvisionLatency(45 * time.Second) // 45s falls in the 30–60s bucket

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "yscale_burst_provision_seconds_count 1") {
		t.Errorf("provision histogram count not incremented; body:\n%s", body)
	}
}

// TestRecordMeshMint proves the counter increments with the correct labels.
// Prometheus text format sorts label names alphabetically: provider < result.
func TestRecordMeshMint(t *testing.T) {
	m := NewMeter()
	m.RecordMeshMint("tailscale", "ok")
	m.RecordMeshMint("tailscale", "ok")
	m.RecordMeshMint("box", "fail")

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `provider="tailscale",result="ok"} 2`) {
		t.Errorf("tailscale ok count wrong; body:\n%s", body)
	}
	if !strings.Contains(body, `provider="box",result="fail"} 1`) {
		t.Errorf("box fail count wrong; body:\n%s", body)
	}
}

// TestRecordMeshFallback proves the fallback counter increments.
func TestRecordMeshFallback(t *testing.T) {
	m := NewMeter()
	m.RecordMeshFallback()
	m.RecordMeshFallback()

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "yscale_mesh_fallback_total 2") {
		t.Errorf("fallback counter wrong; body:\n%s", body)
	}
}

// TestRecordReap proves ok and fail counters are independent.
func TestRecordReap(t *testing.T) {
	m := NewMeter()
	m.RecordReap("ok")
	m.RecordReap("ok")
	m.RecordReap("fail")

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `result="ok"} 2`) {
		t.Errorf("reap ok count wrong; body:\n%s", body)
	}
	if !strings.Contains(body, `result="fail"} 1`) {
		t.Errorf("reap fail count wrong; body:\n%s", body)
	}
}

// TestSetPodCIDRFree proves the gauge reflects the last Set call.
func TestSetPodCIDRFree(t *testing.T) {
	m := NewMeter()
	m.SetPodCIDRFree(200)

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "yscale_podcidr_pool_free 200") {
		t.Errorf("pod cidr free gauge wrong; body:\n%s", body)
	}
}

func TestNewMeterInitializesPodCIDRFreeUnknown(t *testing.T) {
	body := scrapeMetrics(t, NewMeter())
	if !strings.Contains(body, "yscale_podcidr_pool_free -1") {
		t.Errorf("initial pod cidr gauge missing unknown value; body:\n%s", body)
	}
}

// TestAgentGauge proves AgentConnected and AgentDisconnected track the gauge.
func TestAgentGauge(t *testing.T) {
	m := NewMeter()
	m.AgentConnected()
	m.AgentConnected()
	m.AgentDisconnected()

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "yscale_agents_connected 1") {
		t.Errorf("agents_connected gauge wrong; body:\n%s", body)
	}
}

// TestNilMeterSafety proves all new methods are safe when the receiver is nil
// (same pattern the existing BurstCreated/BurstReaped methods use).
func TestNilMeterSafety(t *testing.T) {
	var m *Meter
	// None of these should panic.
	m.ObserveProvisionLatency(time.Second)
	m.RecordMeshMint("tailscale", "ok")
	m.RecordMeshFallback()
	m.RecordReap("ok")
	m.SetPodCIDRFree(100)
	m.AgentConnected()
	m.AgentDisconnected()

	// HTTPMiddleware with nil meter must pass through to the next handler unchanged.
	called := false
	handler := m.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if !called {
		t.Error("nil HTTPMiddleware did not call next handler")
	}
}

// TestHTTPMiddlewareRecordsREDMetrics proves the RED middleware captures status,
// route (from r.Pattern), and method, and that both counter and histogram
// increment for a handled request.
//
// Prometheus text format sorts label names alphabetically in the output:
// for httpRequests{route,method,status}: m < r < s → method,route,status.
// for httpDuration{route,method}: m < r → method,route.
func TestHTTPMiddlewareRecordsREDMetrics(t *testing.T) {
	m := NewMeter()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	wrapped := m.HTTPMiddleware(inner)

	req := httptest.NewRequest(http.MethodPost, "/v1/workloads", nil)
	// Simulate the ServeMux setting the matched pattern — the middleware reads r.Pattern.
	req.Pattern = "POST /v1/workloads"
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	body := scrapeMetrics(t, m)
	// Labels sorted alphabetically in prometheus text: method < route < status.
	if !strings.Contains(body, `method="POST",route="POST /v1/workloads",status="201"} 1`) {
		t.Errorf("http_requests_total counter wrong; body:\n%s", body)
	}
	// Duration histogram: method < route.
	if !strings.Contains(body, `yscale_http_request_duration_seconds_count{method="POST",route="POST /v1/workloads"} 1`) {
		t.Errorf("http_request_duration_seconds histogram not incremented; body:\n%s", body)
	}
}

// TestHTTPMiddlewareUnmatchedRoute proves "unmatched" is used when r.Pattern is empty
// (the mux didn't match any registered route, e.g. a 404).
func TestHTTPMiddlewareUnmatchedRoute(t *testing.T) {
	m := NewMeter()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	wrapped := m.HTTPMiddleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/no-such-path", nil)
	// r.Pattern is intentionally left empty — the mux didn't match any route.
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `route="unmatched"`) {
		t.Errorf("unmatched route label not applied; body:\n%s", body)
	}
}
