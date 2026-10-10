// Package cost is central's burst-cost accounting + Prometheus export.
//
// It accrues yscale's own upstream spend on burst nodes — tagged by
// backend — as bursts are created and reaped, and serves it at /metrics
// for the Grafana cost dashboard. The numbers are yscale's accounting
// view (pricing catalog × actual burst lifetime), not provider invoices;
// a provider-billing reconciliation layer can be added later.
//
// All methods are nil-receiver-safe so handlers/tests that don't wire a
// Meter simply no-op.
package cost

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Meter holds the cost collectors on a private registry (so we export
// exactly these series, no Go-runtime noise) and serves them at /metrics.
type Meter struct {
	reg *prometheus.Registry

	active    *prometheus.GaugeVec     // yscale_burst_active{backend}
	hourly    *prometheus.GaugeVec     // yscale_burst_hourly_usd{backend} — current burn rate
	costTotal *prometheus.CounterVec   // yscale_cloud_cost_usd_total{backend} — cumulative accrued
	created   *prometheus.CounterVec   // yscale_bursts_created_total{backend}
	reaped    *prometheus.CounterVec   // yscale_bursts_reaped_total{backend}
	lifetime  *prometheus.HistogramVec // yscale_burst_lifetime_seconds{backend}
	fixed     *prometheus.GaugeVec     // yscale_fixed_cost_usd_per_month{component}

	// Reliability metrics — registered on the same registry so the existing
	// ServiceMonitor scrape at /metrics picks them up without config changes.
	provision                   *prometheus.HistogramVec // yscale_burst_provision_seconds — created→started
	meshMint                    *prometheus.CounterVec   // yscale_mesh_mint_total{provider,result}
	meshFallback                prometheus.Counter       // yscale_mesh_fallback_total
	reapTotal                   *prometheus.CounterVec   // yscale_reap_total{result}
	persistenceFailures         *prometheus.CounterVec   // yscale_state_persistence_failures_total{entity,operation}
	podCIDRFree                 prometheus.Gauge         // yscale_podcidr_pool_free
	agents                      prometheus.Gauge         // yscale_agents_connected
	billingReconcileEnabled     prometheus.Gauge         // yscale_billing_reconciliation_enabled
	billingReconcileHealthy     prometheus.Gauge         // yscale_billing_reconciliation_healthy
	billingReconcileDifferences *prometheus.GaugeVec     // yscale_billing_reconciliation_differences{category}
	billingReconcileRuns        *prometheus.CounterVec   // yscale_billing_reconciliation_runs_total{result}
	billingReconcileLastSuccess prometheus.Gauge         // yscale_billing_reconciliation_last_success_timestamp_seconds

	// liveProvisioning derives yscale_burst_provisioning_{active,oldest_age_seconds}
	// and _source_up from authoritative lifecycle and state/workload views at
	// scrape time. It is the live counterpart to the provision histogram above,
	// which can only ever
	// describe starts that finished. See provisioning.go.
	liveProvisioning *provisioningCollector
	teardownStream   *teardownStreamCollector

	// RED (Rate/Errors/Duration) HTTP metrics.
	httpRequests *prometheus.CounterVec   // yscale_http_requests_total{route,method,status}
	httpDuration *prometheus.HistogramVec // yscale_http_request_duration_seconds{route,method}
}

// NewMeter registers the cost collectors on a fresh registry.
func NewMeter() *Meter {
	reg := prometheus.NewRegistry()
	m := &Meter{
		reg: reg,
		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "yscale_burst_active",
			Help: "Burst nodes currently live, by backend.",
		}, []string{"backend"}),
		hourly: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "yscale_burst_hourly_usd",
			Help: "Current burst burn rate in upstream USD/hour, by backend.",
		}, []string{"backend"}),
		costTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_cloud_cost_usd_total",
			Help: "Cumulative upstream USD accrued by reaped bursts (rate x lifetime), by backend.",
		}, []string{"backend"}),
		created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_bursts_created_total",
			Help: "Bursts provisioned, by backend.",
		}, []string{"backend"}),
		reaped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_bursts_reaped_total",
			Help: "Bursts reaped, by backend.",
		}, []string{"backend"}),
		lifetime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "yscale_burst_lifetime_seconds",
			Help:    "Burst lifetime (create to reap) in seconds, by backend.",
			Buckets: prometheus.ExponentialBuckets(30, 2, 10), // 30s .. ~4h
		}, []string{"backend"}),
		fixed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "yscale_fixed_cost_usd_per_month",
			Help: "Always-on (non-burst) monthly USD costs, by component.",
		}, []string{"component"}),

		// Reliability metrics.
		provision: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "yscale_burst_provision_seconds",
			Help:    "Time from workload created to the agent reporting the burst is running (created→started).",
			Buckets: []float64{5, 10, 30, 60, 90, 120, 180, 300, 600},
		}, []string{}),
		meshMint: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_mesh_mint_total",
			Help: "Mesh auth-key mint attempts, by coordination-server provider and result.",
		}, []string{"provider", "result"}),
		meshFallback: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "yscale_mesh_fallback_total",
			Help: "Times a self-hosted coordination mint fell back to shared Tailscale SaaS.",
		}),
		reapTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_reap_total",
			Help: "Burst reap attempts, by result (ok|fail).",
		}, []string{"result"}),
		persistenceFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_state_persistence_failures_total",
			Help: "State write-through failures, by bounded entity and operation.",
		}, []string{"entity", "operation"}),
		podCIDRFree: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "yscale_podcidr_pool_free",
			Help: "Free /24 pod-CIDR slots remaining in the 10.244.10-250.0/24 pool.",
		}),
		agents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "yscale_agents_connected",
			Help: "Cluster Connectors currently connected via WebSocket.",
		}),
		billingReconcileEnabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "yscale_billing_reconciliation_enabled",
			Help: "Whether this central replica has the managed billing reconciliation worker enabled.",
		}),
		billingReconcileHealthy: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "yscale_billing_reconciliation_healthy",
			Help: "Whether the latest billing-ledger reconciliation is healthy (1) or paid admission is fail-closed (0).",
		}),
		billingReconcileDifferences: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "yscale_billing_reconciliation_differences",
			Help: "Aggregate billing-ledger differences by bounded category; never labeled with tenant or object identifiers.",
		}, []string{"category"}),
		billingReconcileRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_billing_reconciliation_runs_total",
			Help: "Billing-ledger reconciliation attempts by bounded result (healthy, difference, or error).",
		}, []string{"result"}),
		billingReconcileLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "yscale_billing_reconciliation_last_success_timestamp_seconds",
			Help: "Unix timestamp of the most recent healthy billing-ledger reconciliation.",
		}),

		// RED HTTP metrics.
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_http_requests_total",
			Help: "Total HTTP requests handled by central, by route, method, and status code.",
		}, []string{"route", "method", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "yscale_http_request_duration_seconds",
			Help:    "HTTP request latency in seconds, by route and method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),

		// Registered with no source: it publishes nothing until
		// SetProvisioningSource hands it the store.
		liveProvisioning: &provisioningCollector{now: time.Now},
		teardownStream:   &teardownStreamCollector{},
	}
	m.podCIDRFree.Set(-1) // capacity is unknown until the first successful refresh
	for _, category := range billingReconciliationCategories {
		m.billingReconcileDifferences.WithLabelValues(category).Set(0)
	}
	reg.MustRegister(
		m.active, m.hourly, m.costTotal, m.created, m.reaped, m.lifetime, m.fixed,
		m.provision, m.meshMint, m.meshFallback, m.reapTotal, m.persistenceFailures, m.podCIDRFree, m.agents,
		m.billingReconcileEnabled, m.billingReconcileHealthy, m.billingReconcileDifferences, m.billingReconcileRuns,
		m.billingReconcileLastSuccess, m.httpRequests, m.httpDuration, m.liveProvisioning,
		m.teardownStream,
	)
	return m
}

// Handler serves the cost metrics for Prometheus to scrape. Mount it
// UNAUTHENTICATED (like /healthz) so the in-cluster Prometheus can poll it.
func (m *Meter) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// HTTPMiddleware wraps next so every request is measured: request count
// (yscale_http_requests_total) and duration (yscale_http_request_duration_seconds).
// Route label is r.Pattern (the mux's matched pattern), which is low-cardinality
// by construction in Go 1.22+ ServeMux. Falls back to "unmatched" when the pattern
// is empty (no route matched, e.g. 404 from the mux's default handler).
// Nil-safe: when the meter is nil the handler is called unchanged.
func (m *Meter) HTTPMiddleware(next http.Handler) http.Handler {
	if m == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		elapsed := time.Since(start).Seconds()

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		method := r.Method
		status := strconv.Itoa(rw.status)

		m.httpRequests.WithLabelValues(route, method, status).Inc()
		m.httpDuration.WithLabelValues(route, method).Observe(elapsed)
	})
}

// statusWriter is a minimal ResponseWriter wrapper that captures the HTTP
// status code written by the handler.
type statusWriter struct {
	http.ResponseWriter
	status  int
	written bool
}

// Compile-time guard: statusWriter MUST stay an http.Hijacker or the RED
// middleware silently breaks the agent-stream WebSocket upgrade (fleet outage).
var (
	_ http.Hijacker = (*statusWriter)(nil)
	_ http.Flusher  = (*statusWriter)(nil)
)

func (sw *statusWriter) WriteHeader(code int) {
	if !sw.written {
		sw.status = code
		sw.written = true
	}
	sw.ResponseWriter.WriteHeader(code)
}

func (sw *statusWriter) Write(b []byte) (int, error) {
	if !sw.written {
		sw.status = http.StatusOK
		sw.written = true
	}
	return sw.ResponseWriter.Write(b)
}

// Hijack passes through to the underlying ResponseWriter so connection-takeover
// handlers keep working through this wrapper. This is REQUIRED: central's agent
// stream (GET /v1/agent/stream) upgrades to a WebSocket via gorilla/websocket,
// which type-asserts http.Hijacker — without this passthrough the middleware
// would break every agent's connection to central.
func (sw *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := sw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("cost: underlying ResponseWriter is not an http.Hijacker")
	}
	return hj.Hijack()
}

// Flush passes through for streaming/chunked responses.
func (sw *statusWriter) Flush() {
	if f, ok := sw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func backendLabel(backend string) string {
	if backend == "" {
		return "unknown"
	}
	return backend
}

// BurstCreated records a newly-provisioned burst: bumps the live count
// and adds its rate to the current burn for its backend.
func (m *Meter) BurstCreated(backend string, hourlyUSD float64) {
	if m == nil {
		return
	}
	b := backendLabel(backend)
	m.active.WithLabelValues(b).Inc()
	m.hourly.WithLabelValues(b).Add(hourlyUSD)
	m.created.WithLabelValues(b).Inc()
}

// BurstReaped records a torn-down burst: accrues its cost (rate x
// lifetime), drops the live count + burn rate, and observes its lifetime.
func (m *Meter) BurstReaped(backend string, hourlyUSD float64, lifetime time.Duration) {
	if m == nil {
		return
	}
	b := backendLabel(backend)
	m.active.WithLabelValues(b).Dec()
	m.hourly.WithLabelValues(b).Sub(hourlyUSD)
	m.reaped.WithLabelValues(b).Inc()
	if lifetime > 0 {
		m.lifetime.WithLabelValues(b).Observe(lifetime.Seconds())
		m.costTotal.WithLabelValues(b).Add(hourlyUSD * lifetime.Hours())
	}
}

// SetFixedMonthly publishes the always-on monthly costs (coordination box,
// baked-AMI snapshot, …). Called once at startup.
func (m *Meter) SetFixedMonthly(costs map[string]float64) {
	if m == nil {
		return
	}
	for component, usd := range costs {
		m.fixed.WithLabelValues(component).Set(usd)
	}
}

// ObserveProvisionLatency records the created→started elapsed time for one burst.
//
// This is the COMPLETED-start latency SLI and nothing else. It can only ever
// describe starts that finished, so it must not be the stuck-provisioning
// signal — a burst that never starts is never observed here. That question is
// answered by the live provisioning collector in provisioning.go.
func (m *Meter) ObserveProvisionLatency(d time.Duration) {
	if m == nil {
		return
	}
	m.provision.WithLabelValues().Observe(d.Seconds())
}

// RecordMeshMint increments yscale_mesh_mint_total for one mint attempt.
// provider is "tailscale" or "box"; result is "ok" or "fail".
func (m *Meter) RecordMeshMint(provider, result string) {
	if m == nil {
		return
	}
	if provider == "" {
		provider = "unknown"
	}
	m.meshMint.WithLabelValues(provider, result).Inc()
}

// RecordMeshFallback increments yscale_mesh_fallback_total (a self-hosted-box mint
// fell back to shared Tailscale SaaS).
func (m *Meter) RecordMeshFallback() {
	if m == nil {
		return
	}
	m.meshFallback.Inc()
}

// RecordReap increments yscale_reap_total. result must be "ok" or "fail".
func (m *Meter) RecordReap(result string) {
	if m == nil {
		return
	}
	m.reapTotal.WithLabelValues(result).Inc()
}

// RecordPersistenceFailure increments the bounded write-through failure
// counter. Callers pass closed entity/operation constants, never identifiers or
// error text, so this remains low-cardinality and cannot export secrets.
func (m *Meter) RecordPersistenceFailure(entity, operation string) {
	if m == nil {
		return
	}
	m.persistenceFailures.WithLabelValues(entity, operation).Inc()
}

// SetPodCIDRFree sets yscale_podcidr_pool_free to the number of unallocated /24 slots.
func (m *Meter) SetPodCIDRFree(n int) {
	if m == nil {
		return
	}
	m.podCIDRFree.Set(float64(n))
}

// AgentConnected increments yscale_agents_connected.
func (m *Meter) AgentConnected() {
	if m == nil {
		return
	}
	m.agents.Inc()
}

// AgentDisconnected decrements yscale_agents_connected.
func (m *Meter) AgentDisconnected() {
	if m == nil {
		return
	}
	m.agents.Dec()
}

var billingReconciliationCategories = [...]string{
	"account_balance", "held_balance", "funding_reversal", "operation_ledger", "economic_object", "external_cash", "usage_capture",
}

// EnableBillingReconciliation marks this replica as subject to the managed
// billing alert. OSS and unpaid deployments keep the default zero and quiet.
func (m *Meter) EnableBillingReconciliation() {
	if m != nil {
		m.billingReconcileEnabled.Set(1)
	}
}

// RecordBillingReconciliation publishes one read-only reconciliation result.
// Its fixed arguments and normalized result keep every metric label bounded.
func (m *Meter) RecordBillingReconciliation(result string, account, held, funding, operations, objects, externalCash, usageCapture int64) {
	if m == nil {
		return
	}
	values := [...]int64{account, held, funding, operations, objects, externalCash, usageCapture}
	for i, category := range billingReconciliationCategories {
		m.billingReconcileDifferences.WithLabelValues(category).Set(float64(values[i]))
	}
	switch result {
	case "healthy":
		m.billingReconcileHealthy.Set(1)
		m.billingReconcileLastSuccess.SetToCurrentTime()
	case "difference":
		m.billingReconcileHealthy.Set(0)
	default:
		result = "error"
		m.billingReconcileHealthy.Set(0)
	}
	m.billingReconcileRuns.WithLabelValues(result).Inc()
}

// MarkBillingReconciliationStale closes the observable health signal without
// pretending the watchdog performed another database reconciliation.
func (m *Meter) MarkBillingReconciliationStale() {
	if m != nil {
		m.billingReconcileHealthy.Set(0)
	}
}
