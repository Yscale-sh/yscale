package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/flyio"
	"github.com/yscale-sh/yscale/pkg/config"
	"github.com/yscale-sh/yscale/pkg/controller"
	"github.com/yscale-sh/yscale/pkg/cost"
	"github.com/yscale-sh/yscale/pkg/logger"
	"github.com/yscale-sh/yscale/pkg/metrics"
	"github.com/yscale-sh/yscale/pkg/preflight"
)

func main() {
	// Subcommand routing: yscale <subcommand> [flags]. Default (no subcommand
	// or a leading -flag) runs the controller.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "init":
			if err := runInit(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "yscale init:", err)
				os.Exit(1)
			}
			return
		case "apply":
			if err := runApply(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "yscale apply:", err)
				os.Exit(1)
			}
			return
		case "price":
			if err := runPrice(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "yscale price:", err)
				os.Exit(1)
			}
			return
		case "catalog":
			if err := runCatalog(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "yscale catalog:", err)
				os.Exit(1)
			}
			return
		case "inject":
			if err := runInject(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "yscale inject:", err)
				os.Exit(1)
			}
			return
		case "help", "-h", "--help":
			printUsage()
			return
		}
	}
	runController()
}

func printUsage() {
	fmt.Fprint(os.Stderr, `yscale — Kubernetes burst autoscaler (CLI + legacy OSS controller)

Usage:
  yscale [flags]                    run the OSS controller (default, legacy)
  yscale init [flags]               generate a Helm values fragment from a kubeconfig
  yscale apply [-f FILE]            submit a Workload YAML
  yscale price gpu                  show the yscale GPU price index
  yscale catalog get [flags]        fetch a tenant template catalog
  yscale catalog apply [flags]      replace a tenant template catalog
  yscale inject -f FILE             inject burst nodeSelector/toleration into
                                    annotated Pod templates (never applies)
  yscale help                       show this message

For the Yscale Cluster Connector, see the separate yscale-agent binary
(yscale-agent remains the compatibility artifact name).
For the Yscale Control Plane API, see yscale-cloud.

Run 'yscale init -h', 'yscale apply -h', 'yscale inject -h', or 'yscale -h' for flags.
`)
}

func runController() {
	var (
		configPath  string
		kubeconfig  string
		metricsAddr string
	)
	flag.StringVar(&configPath, "config", "/etc/yscale/config.yaml", "path to yscale config file")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig (uses in-cluster config if empty)")
	flag.StringVar(&metricsAddr, "metrics-addr", ":9090", "address for the Prometheus /metrics endpoint")
	flag.Parse()

	// Structured logging via pkg/logger: JSON to stderr + best-effort Loki shipping
	// (LOKI_URL/ENVIRONMENT injected by the platform). SetDefault so package-level
	// slog.* across yscale also ships.
	log, closeLog := logger.New(logger.Options{Job: "yscale"})
	slog.SetDefault(log)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = closeLog(ctx)
	}()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Error("loading config", "error", err)
		os.Exit(1)
	}

	var restConfig *rest.Config
	if kubeconfig != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		restConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		log.Error("building kube config", "error", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Error("creating kube client", "error", err)
		os.Exit(1)
	}

	// Preflight: in kubelet mode, fail loud if the configured apiServer
	// + clusterCA can't establish TLS. Saves a debugging nightmare where
	// the controller runs happily but every burst node silently fails to
	// join. No-op in k3s mode.
	preflightCtx, preflightCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := preflight.VerifyAPIServer(preflightCtx, cfg); err != nil {
		preflightCancel()
		log.Error("api server preflight failed", "error", err)
		os.Exit(1)
	}
	preflightCancel()
	if cfg.Join.Mode == config.JoinModeKubelet {
		log.Info("api server preflight ok", "apiServer", cfg.Join.APIServer)
	}

	// For kubelet join mode, ensure bootstrap RBAC is configured.
	if cfg.Join.Mode == config.JoinModeKubelet {
		ctx := context.Background()
		if err := controller.EnsureBootstrapRBAC(ctx, clientset); err != nil {
			log.Error("setting up bootstrap RBAC", "error", err)
			os.Exit(1)
		}
		log.Info("bootstrap RBAC configured")
	}

	var backend backends.Backend
	switch cfg.Backend.Type {
	case backends.TypeFlyIO:
		backend = flyio.New(cfg.Backend.FlyIO.APIToken, cfg.Backend.FlyIO.Org, cfg.Backend.FlyIO.Region)
	default:
		log.Error("unsupported backend type", "type", cfg.Backend.Type)
		os.Exit(1)
	}

	m := metrics.New()
	costTracker := cost.New(buildCostRates(cfg), cfg.Scaling.MonthlyCapUSD)

	factory := informers.NewSharedInformerFactory(clientset, 0)
	ctrl := controller.New(clientset, factory, backend, cfg, log, m, costTracker)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	factory.Start(ctx.Done())

	// Serve Prometheus metrics. Shut down gracefully when ctx is done.
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	metricsServer := &http.Server{
		Addr:              metricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("metrics server listening", "addr", metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	go logSpend(ctx, log, costTracker)

	log.Info("yscale starting",
		"backend", cfg.Backend.Type,
		"joinMode", cfg.Join.Mode,
		"maxNodes", cfg.Scaling.MaxNodes,
		"tailscale", cfg.Tailscale.Enabled,
		"metricsAddr", metricsAddr,
	)

	if err := ctrl.Run(ctx); err != nil {
		log.Error("controller failed", "error", err)
		os.Exit(1)
	}
}

// buildCostRates translates the YAML costs block into the package-internal
// cost.Rates structure used by the tracker.
func buildCostRates(cfg *config.Config) cost.Rates {
	out := cost.Rates{Backends: make(map[string]cost.BackendRate, len(cfg.Scaling.Costs.Backends))}
	for name, bc := range cfg.Scaling.Costs.Backends {
		out.Backends[name] = cost.BackendRate{
			BaseUSDPerHour:   bc.BaseUSDPerHour,
			PerCPUUSDPerHour: bc.PerCPUUSDPerHour,
			PerGBUSDPerHour:  bc.PerGBUSDPerHour,
			GPURates:         bc.GPURates,
		}
	}
	return out
}

// logSpend emits a once-per-minute snapshot of accrued spend so operators
// can see what the autoscaler is costing them in near real time.
func logSpend(ctx context.Context, log *slog.Logger, tracker *cost.Tracker) {
	if tracker == nil {
		return
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap := tracker.Snapshot()
			log.Info("cost snapshot",
				"monthSpentUSD", snap.MonthSpentUSD,
				"monthCapUSD", snap.MonthCapUSD,
				"activeNodes", snap.ActiveCount,
				"activeUSDPerHour", snap.ActiveCostUSDHour,
				"completedRuns", snap.CompletedRuns,
			)
		}
	}
}
