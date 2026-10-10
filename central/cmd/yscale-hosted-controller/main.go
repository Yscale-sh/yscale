package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yscale-sh/yscale/central/internal/hostedcontroller"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type envConfig struct {
	CentralURL string
	AdminToken string
	Controller hostedcontroller.Config
}

func loadConfig(getenv func(string) string) (envConfig, error) {
	value := func(key, fallback string) string {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return v
		}
		return fallback
	}
	adminToken := strings.TrimSpace(getenv("YSCALE_ADMIN_TOKEN"))
	if strings.TrimSpace(adminToken) == "" {
		return envConfig{}, errors.New("YSCALE_ADMIN_TOKEN is required")
	}
	maxAssignments, err := strconv.Atoi(value("YSCALE_HOSTED_MAX_ASSIGNMENTS", "8"))
	if err != nil {
		return envConfig{}, errors.New("YSCALE_HOSTED_MAX_ASSIGNMENTS must be an integer")
	}
	poll, err := time.ParseDuration(value("YSCALE_HOSTED_POLL_INTERVAL", "30s"))
	if err != nil {
		return envConfig{}, errors.New("YSCALE_HOSTED_POLL_INTERVAL must be a duration")
	}
	routes := splitCSV(value("YSCALE_HOSTED_ADVERTISED_ROUTES", "10.42.0.0/16,10.43.0.0/16,10.0.0.0/24"))
	excluded := make(map[string]struct{})
	for _, clusterID := range splitCSV(getenv("YSCALE_HOSTED_EXCLUDED_CLUSTER_IDS")) {
		excluded[clusterID] = struct{}{}
	}
	cfg := envConfig{CentralURL: value("YSCALE_CENTRAL_URL", "http://yscale-cloud.yscale:8443"), AdminToken: adminToken, Controller: hostedcontroller.Config{
		MaxAssignments: maxAssignments, ExcludedClusterIDs: excluded, ConnectorNamespace: value("YSCALE_HOSTED_CONNECTOR_NAMESPACE", "yscale-system"),
		CentralEndpoint: value("YSCALE_CENTRAL_ENDPOINT", "ws://yscale-cloud.yscale:8443"), CentralServiceNamespace: value("YSCALE_HOSTED_CENTRAL_SERVICE_NAMESPACE", "yscale"), SourceName: value("YSCALE_HOSTED_SOURCE_NAME", "yscale-agent"), SourceNamespace: value("YSCALE_HOSTED_SOURCE_NAMESPACE", "flux-system"),
		BootstrapAPIServer: value("YSCALE_HOSTED_BOOTSTRAP_APISERVER_URL", "https://node0:6443"), AgentImageRepo: value("YSCALE_HOSTED_AGENT_IMAGE_REPOSITORY", "ghcr.io/jakenesler/yscale-cluster-agent"), AgentImageTag: value("YSCALE_HOSTED_AGENT_IMAGE_TAG", "latest"), AgentImageDigest: strings.TrimSpace(getenv("YSCALE_HOSTED_AGENT_IMAGE_DIGEST")),
		AdvertisedRoutes: routes, PollInterval: poll,
	}}
	if err := cfg.Controller.Validate(); err != nil {
		return envConfig{}, err
	}
	return cfg, nil
}

func splitCSV(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func run(ctx context.Context, cfg envConfig, log *slog.Logger) error {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load in-cluster Kubernetes config: %w", err)
	}
	kube, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("create dynamic Kubernetes client: %w", err)
	}
	central, err := hostedcontroller.NewClient(cfg.CentralURL, cfg.AdminToken, 10*time.Second)
	if err != nil {
		return err
	}
	r := &hostedcontroller.Reconciler{Central: central, Kube: kube, Dynamic: dyn, Config: cfg.Controller, Log: log}
	return r.Run(ctx)
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log); err != nil {
		log.Error("hosted controller stopped", "error", err)
		os.Exit(1)
	}
}
