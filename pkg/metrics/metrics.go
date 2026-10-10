// Package metrics defines the Prometheus metrics exposed by the yscale
// controller. All metrics are registered with the Prometheus default registry
// and are namespaced under the "yscale_" prefix.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// provisionDurationBuckets are the histogram buckets (in seconds) for
// burst-node provisioning latencies, sized to cover sub-second resume
// times through multi-minute cold boots.
var provisionDurationBuckets = []float64{0.5, 1, 2, 5, 10, 30, 60, 120}

// Metrics is the collection of Prometheus metrics exported by the
// yscale controller. A nil *Metrics is a valid no-op receiver:
// callers must guard against nil before recording values.
type Metrics struct {
	// NodesProvisioned counts burst-node provisioning attempts by backend,
	// mode (cold or resume), and result (success or fail).
	NodesProvisioned *prometheus.CounterVec
	// NodesPaused counts successful pause/stop operations that returned a
	// burst node to the prewarm pool.
	NodesPaused prometheus.Counter
	// NodesDestroyed counts burst nodes destroyed, labelled by reason
	// (idle, stuck, orphan).
	NodesDestroyed *prometheus.CounterVec
	// FailedScaleUps counts scale-up attempts where the new node came up
	// but the unschedulable pods could not be placed on it.
	FailedScaleUps prometheus.Counter
	// OrphansDestroyed counts orphaned backend resources cleaned up by
	// the periodic orphan sweep.
	OrphansDestroyed prometheus.Counter
	// PodsEvicted counts pods evicted during node drain, labelled by
	// result (success or delete-fallback).
	PodsEvicted *prometheus.CounterVec

	// NodesActive is the current number of burst nodes tracked by the
	// controller (ready or provisioning).
	NodesActive prometheus.Gauge
	// NodesProvisioning is the current number of in-flight provision
	// operations.
	NodesProvisioning prometheus.Gauge
	// NodesReady is the current number of burst nodes that have joined
	// the cluster and are Ready.
	NodesReady prometheus.Gauge
	// PoolSize is the current number of paused machines available for
	// fast resume.
	PoolSize prometheus.Gauge
	// UnschedulablePods is the current number of pending pods that the
	// scheduler reports as Unschedulable.
	UnschedulablePods prometheus.Gauge

	// ProvisionDurationSec measures the wall-clock time of a successful
	// provisionNode call, labelled by backend and mode (cold or resume).
	ProvisionDurationSec *prometheus.HistogramVec
	// NodeReadyDurationSec measures the time from burst-node creation
	// until the kubelet first reports Ready.
	NodeReadyDurationSec prometheus.Histogram
	// ReconcileDurationSec measures the wall-clock time of a single
	// controller reconcile loop iteration.
	ReconcileDurationSec prometheus.Histogram
}

// New creates a Metrics, registering all collectors with the Prometheus
// default registry via promauto. Calling New twice will panic on
// duplicate registration.
func New() *Metrics {
	return &Metrics{
		NodesProvisioned: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_nodes_provisioned_total",
			Help: "Total burst-node provisioning attempts, by backend, mode, and result.",
		}, []string{"backend", "mode", "result"}),

		NodesPaused: promauto.NewCounter(prometheus.CounterOpts{
			Name: "yscale_nodes_paused_total",
			Help: "Total burst nodes successfully paused (returned to the prewarm pool).",
		}),

		NodesDestroyed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_nodes_destroyed_total",
			Help: "Total burst nodes destroyed, by reason (idle, stuck, orphan).",
		}, []string{"reason"}),

		FailedScaleUps: promauto.NewCounter(prometheus.CounterOpts{
			Name: "yscale_failed_scale_ups_total",
			Help: "Total scale-ups where the provisioned node could not satisfy pending pods.",
		}),

		OrphansDestroyed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "yscale_orphans_destroyed_total",
			Help: "Total orphaned backend resources destroyed by the periodic sweep.",
		}),

		PodsEvicted: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "yscale_pods_evicted_total",
			Help: "Total pods evicted during node drain, by result (success or delete-fallback).",
		}, []string{"result"}),

		NodesActive: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "yscale_nodes_active",
			Help: "Current number of burst nodes tracked by the controller.",
		}),

		NodesProvisioning: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "yscale_nodes_provisioning",
			Help: "Current number of in-flight burst-node provision operations.",
		}),

		NodesReady: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "yscale_nodes_ready",
			Help: "Current number of burst nodes that have joined the cluster and are Ready.",
		}),

		PoolSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "yscale_pool_size",
			Help: "Current number of paused machines available for fast resume.",
		}),

		UnschedulablePods: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "yscale_unschedulable_pods",
			Help: "Current number of pending pods reported as Unschedulable.",
		}),

		ProvisionDurationSec: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "yscale_provision_duration_seconds",
			Help:    "Duration of successful provisionNode calls, by backend and mode.",
			Buckets: provisionDurationBuckets,
		}, []string{"backend", "mode"}),

		NodeReadyDurationSec: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "yscale_node_ready_duration_seconds",
			Help:    "Time from burst-node creation until the node first reports Ready.",
			Buckets: provisionDurationBuckets,
		}),

		ReconcileDurationSec: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "yscale_reconcile_duration_seconds",
			Help:    "Duration of a single controller reconcile loop iteration.",
			Buckets: prometheus.DefBuckets,
		}),
	}
}

// Handler returns an http.Handler that serves the Prometheus default
// registry in the standard text exposition format.
func Handler() http.Handler {
	return promhttp.Handler()
}
