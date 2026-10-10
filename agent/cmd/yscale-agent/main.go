// Command yscale-agent is the customer-cluster-side daemon. It
// connects to the central yscale.sh server over WebSocket, sends
// Hello with this cluster's identity, and runs the command/status
// loop. Holds no backend creds — only YSCALE_TOKEN.
//
// Wires the production handler stack:
//   - in-cluster Kubernetes clientset (rest.InClusterConfig)
//   - BootstrapServer that mints kubelet tokens for bursts that ask
//     for them over the yscale tailnet
//
// Deployed in the customer's cluster via Helm. The pod also runs
// tailscaled as a sidecar so the agent is reachable from burst nodes
// by MagicDNS at hostname `yscale-agent-<cluster-id>`.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/yscale-sh/yscale/agent/internal/agent"
	"github.com/yscale-sh/yscale/agent/internal/clusterroutes"
	"github.com/yscale-sh/yscale/pkg/logger"
)

const (
	gatewayRoutesConfigMapName  = "yscale-gateway-routes"
	gatewayRoutesConfigMapKey   = "routes"
	serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "yscale-agent:", err)
		os.Exit(1)
	}
}

// runOptions holds parsed-and-resolved CLI flags. Values flow:
// fs.Parse -> env-var fallback -> ensureClusterID. Built by parseFlags.
type runOptions struct {
	endpoint            string
	token               string
	clusterID           string
	stateDir            string
	bootstrapListenAddr string // host:port the bootstrap-token endpoint binds to
	disableBootstrap    bool
	bootstrapAuthMode   string // kubernetes-token | eks | gke
	eksClusterName      string
	eksRegion           string
	eksBootstrapRoleARN string

	// GKE bootstrap namespace — the connector's own release namespace,
	// resolved from POD_NAMESPACE when bootstrap-auth-mode=gke. Passed to
	// GKETokenIssuer and CSRApprover so per-burst SAs live in the connector's
	// namespace, not kube-system.
	gkeNamespace string

	// Customer cluster facts. Currently logged at startup so they're
	// visible in `kubectl describe pod`; in a future PR the agent
	// forwards them to central in the WS Hello frame so central can
	// stamp the right CLUSTER_DNS / providerID on burst NodeSpecs.
	// The Helm chart computes defaults from .Values.cloudProvider; see
	// deploy/helm/yscale-agent/templates/_helpers.tpl.
	cloudProvider         string // linode | aws | gcp | azure | k3s | self-managed
	clusterDNS            string // e.g. 10.96.0.10 (LKE), 172.20.0.10 (EKS)
	clusterDomain         string // e.g. cluster.local
	burstProviderIDFormat string // e.g. linode://yscale-burst-{BURST_ID}

	// gatewayEnabled mirrors the Helm value `gateway.enabled`. When the
	// gateway sidecar IS deployed, the agent permits networking.tier=
	// full submissions; otherwise it rejects them at submit time with
	// a clear "set gateway.enabled" error. See
	// docs/architecture/cilium-gateway-sidecar.md.
	gatewayEnabled bool
	// gatewayHostname is the tailnet hostname of the gateway sidecar
	// (`yscale-gateway-${clusterID}`). Bursts receive this in their
	// bootstrap-kubeconfig response and resolve it via Tailscale
	// MagicDNS to install a static kernel route. Empty when gateway
	// not enabled.
	gatewayHostname string
	// gatewayRoutes is a comma-separated list of CIDRs that bursts
	// should reach via the gateway (e.g. "10.42.0.0/16,10.43.0.0/16").
	// Mirrors Helm `.Values.gateway.customerPodCIDRs`.
	gatewayRoutes string
	// workloadNamespace scopes which namespace's Workload CRs this agent
	// reconciles. Empty (default) = all namespaces (the normal single-agent
	// install). Set it when multiple agents share ONE physical cluster (teams
	// or simulated multi-tenant) so each agent owns only its namespace's CRs
	// and they don't both submit the same CR. Mirrors Helm
	// `.Values.workloadNamespace`.
	workloadNamespace string
	// podInventoryNamespaces is the explicit namespace allowlist whose Pending
	// Pods this connector may count when it lacks cluster-wide Pod LIST. The
	// Helm chart derives it from rbac.allowedNamespaces.
	podInventoryNamespaces string
	// idleNodeGrace is how long a burst node must be continuously observed
	// carrying no schedulable pods before the connector asks central to tear it
	// down. Mirrors Helm `.Values.idleTeardown.grace`. Zero or negative disables
	// the watcher, and with it the claim in the Hello — see
	// authoritativeOccupancy. nodeOnly bursts then live until a declared budget or
	// central's admission deadline ends them.
	idleNodeGrace time.Duration
	// occupancyObserveInterval is how often the idle watcher re-states that it
	// could still read the whole cluster's pods for a burst node. Mirrors Helm
	// `.Values.occupancyObserveInterval`. It is central's ONLY liveness signal
	// for nodeOnly capacity, which is expired on SILENCE rather than on age — so
	// it must stay well under YSCALE_NODE_ONLY_MAX_LIFETIME. Zero or negative
	// sends no observations, and therefore withdraws the claim in the Hello too:
	// a ceiling with nothing to measure from never expires anything.
	occupancyObserveInterval time.Duration
	// authoritativePodVisibility asserts that this connector's RBAC really does
	// allow the cluster-wide pod LIST the idle watcher's every claim rests on.
	// Mirrors the Helm-computed `rbac.scope == cluster && !workloadNamespace`.
	// Default FALSE for direct and manual installs: an unset -workload-namespace
	// says nobody scoped this connector, not that it was granted cluster-wide
	// reads, and the shipped chart's RBAC is namespaced.
	authoritativePodVisibility bool
}

// authoritativeOccupancy is what this connector may claim to central in its
// Hello: that it can read the whole cluster's pods AND will actually say so, and
// so is equipped to end a nodeOnly burst by reporting its node idle.
//
// The claim is what central prices the capacity against. Believing it, central
// admits a nodeOnly burst with no deadline of its own and bounds it on SILENCE
// instead — expiring it only once the observations stop. So the claim has to
// cover every condition under which an observation is sent, not just the RBAC
// half, because a connector that can read the pods but never reports holds that
// ceiling open forever on a burst nothing will ever end.
//
// Four facts, and all four are required:
//
//   - The pod-visibility GRANT and an UNSCOPED connector. This is the pair
//     IdleNodeWatcher.Run refuses to start without, and it has to be the same
//     pair, because the two answer one question from opposite ends. The watcher
//     decides whether an observation may be sent; the Hello tells central at
//     SUBMIT time whether one is ever coming.
//   - A POSITIVE idle grace. Zero or negative means startBackgroundServices never
//     starts the watcher at all, and the watcher is the only sender of both the
//     idle report and the observations.
//   - A POSITIVE observation interval. Zero or negative means occupancyDue
//     refuses every observation, so the watcher runs and can still ask for a
//     teardown — but central's silence ceiling has nothing to measure from.
//
// A connector that claims nothing is not penalised for it; its nodeOnly bursts
// are given a finite deadline at admission rather than an observation ceiling
// they could never satisfy. That is the safe side of this, and it is where a
// disabled grace or a disabled observation interval belongs.
func (o *runOptions) authoritativeOccupancy() bool {
	return o.authoritativePodVisibility &&
		o.workloadNamespace == "" &&
		o.idleNodeGrace > 0 &&
		o.occupancyObserveInterval > 0
}

func run(args []string) error {
	if len(args) == 1 && args[0] == "artifact-upload" {
		return runArtifactUpload()
	}
	opts, err := parseFlags(args)
	if err != nil || opts == nil {
		return err
	}

	log, closeLog := logger.New(logger.Options{Job: "yscale-agent"})
	slog.SetDefault(log)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = closeLog(ctx)
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Some background services are load-bearing enough that losing one
	// has to take the process with it. fail() records why; run returns
	// that cause so the container exits non-zero, the pod restarts, and
	// central sees the connector drop instead of holding a session it
	// can no longer honour.
	ctx, fail := context.WithCancelCause(sigCtx)
	defer fail(nil)

	// In-cluster K8s client. Required for CreateJob / DeleteJob /
	// DrainNode handlers and for minting bootstrap-token Secrets.
	k8sClient, err := buildK8sClient()
	if err != nil {
		log.Warn("kubernetes client unavailable; CreateJob/DeleteJob/DrainNode/Bootstrap disabled", "error", err)
	}

	var dynClient dynamic.Interface
	if k8sClient != nil {
		dynClient, err = buildDynamicClient()
		if err != nil {
			log.Warn("dynamic client unavailable; Workload CRD status updates and ServiceCIDR API detection disabled", "error", err)
		}
	}

	// Bootstrap server. Optional — disabled in non-cluster envs or
	// when the operator opts out.
	var boot *agent.BootstrapServer
	if !opts.disableBootstrap && k8sClient != nil {
		boot, err = agent.NewBootstrapServer(k8sClient, log)
		if err != nil {
			log.Warn("bootstrap server unavailable; bursts will not be able to fetch kubeconfigs", "error", err)
			boot = nil
		} else if boot != nil {
			switch opts.bootstrapAuthMode {
			case "eks":
				issuer, issuerErr := agent.NewEKSTokenIssuer(ctx, opts.eksClusterName, opts.eksBootstrapRoleARN, opts.eksRegion)
				if issuerErr != nil {
					return fmt.Errorf("configure EKS bootstrap authentication: %w", issuerErr)
				}
				boot.TokenIssuer = issuer
			case "gke":
				ns, nsErr := podNamespace()
				if nsErr != nil {
					return fmt.Errorf("GKE bootstrap requires POD_NAMESPACE: %w", nsErr)
				}
				opts.gkeNamespace = ns
				gkeIssuer := agent.NewGKETokenIssuer(k8sClient, ns, log)
				boot.TokenIssuer = gkeIssuer
				boot.GKECleaner = gkeIssuer
			}
			boot.CloudProvider = opts.cloudProvider
			// Inject gateway config the bootstrap server stamps into
			// every burst's response. Empty when the gateway sidecar
			// isn't deployed — a misconfiguration now that the removed
			// lite tier no longer offers a gateway-free path.
			boot.GatewayHostname = opts.gatewayHostname
			boot.GatewayRoutes = splitGatewayRoutes(opts.gatewayRoutes)
			if opts.gatewayHostname != "" || len(boot.GatewayRoutes) > 0 {
				log.Info("gateway routing configured", "hostname", opts.gatewayHostname, "routes", boot.GatewayRoutes)
			}
		}
	}

	handler := agent.NewRealHandler(k8sClient, boot, log)
	handler.WorkloadNamespace = opts.workloadNamespace
	handler.PodInventoryNamespaces = splitCommaList(opts.podInventoryNamespaces)

	cli, err := agent.New(agent.Config{
		Endpoint:          opts.endpoint,
		Token:             opts.token,
		ClusterID:         opts.clusterID,
		AgentVersion:      "yscale-agent/v0",
		WorkloadNamespace: opts.workloadNamespace,
		// Stated in the handshake, not just enforced locally: central has to
		// decide at submit time whether a nodeOnly burst placed here will ever
		// get a teardown signal.
		AuthoritativeOccupancy: opts.authoritativeOccupancy(),
	}, handler, log)
	if err != nil {
		return err
	}

	// Detect + publish gateway routes: write the masked set to the ConfigMap
	// the gateway sidecar reads, then report the IDENTICAL set to central so
	// the coordination-server approver matches what the gateway advertises byte-for-byte.
	// Runs before cli.Run dials; the client caches the set and flushes it on
	// connect (and re-flushes after each reconnect).
	if opts.gatewayEnabled {
		publishCtx, publishCancel := context.WithTimeout(ctx, 30*time.Second)
		publishGatewayRoutes(publishCtx, k8sClient, dynClient, opts, cli, log)
		publishCancel()
	}

	startBackgroundServices(ctx, fail, opts, k8sClient, dynClient, boot, handler, cli, cli, log)

	log.Info("yscale-agent starting",
		"endpoint", opts.endpoint,
		"cluster", opts.clusterID,
		"cloud_provider", opts.cloudProvider,
		"cluster_dns", opts.clusterDNS,
		"cluster_domain", opts.clusterDomain,
		"burst_provider_id_format", opts.burstProviderIDFormat,
	)
	runErr := cli.Run(ctx)
	// A fatal background service beats whatever cli.Run reported: the
	// client only ever sees the cancellation that service triggered.
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return nil
}

// parseFlags parses argv and resolves env-var fallbacks. Returns
// (nil, nil) on -help so the caller can exit cleanly without an error.
func parseFlags(args []string) (*runOptions, error) {
	fs := flag.NewFlagSet("yscale-agent", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	opts := &runOptions{}
	var clusterIDArg string
	fs.StringVar(&opts.endpoint, "endpoint", "", "your central server URL (default $YSCALE_ENDPOINT, then http://127.0.0.1:8443 for local development)")
	fs.StringVar(&opts.token, "token", "", "customer API token (default $YSCALE_TOKEN)")
	fs.StringVar(&clusterIDArg, "cluster-id", "", "stable cluster UUID (default $YSCALE_CLUSTER_ID, then read/create from --state-dir)")
	fs.StringVar(&opts.stateDir, "state-dir", "/var/lib/yscale", "directory for cluster-id and other persistent state")
	fs.StringVar(&opts.bootstrapListenAddr, "bootstrap-listen", ":8080", "where the bootstrap-token endpoint binds. ':port' = the agent's tailnet IPv4, or loopback when tailscaled has none (never all interfaces); 'tailnet:port' = require the tailnet IPv4, fail closed without it; 'host:port' = bind that host verbatim")
	fs.BoolVar(&opts.disableBootstrap, "disable-bootstrap", false, "skip the bootstrap-token endpoint (useful for tests / k8s-less envs)")
	fs.StringVar(&opts.bootstrapAuthMode, "bootstrap-auth-mode", "kubernetes-token", "kubelet bootstrap authenticator (kubernetes-token|eks|gke)")
	fs.StringVar(&opts.eksClusterName, "eks-cluster-name", "", "EKS cluster name used to sign kubelet bootstrap credentials")
	fs.StringVar(&opts.eksRegion, "eks-region", "", "AWS region of the EKS cluster")
	fs.StringVar(&opts.eksBootstrapRoleARN, "eks-bootstrap-role-arn", "", "zero-permission IAM role registered as an EKS hybrid node identity")

	// Customer cluster facts. Accepted as flags but not yet plumbed
	// to central — flags exist so the Helm chart can pass them without
	// breaking the agent binary. Logged at startup for operator
	// visibility. Central-side consumption lands in a future PR.
	fs.StringVar(&opts.cloudProvider, "cloud-provider", "", "customer cluster's cloud provider (linode|aws|gcp|azure|k3s|self-managed)")
	fs.StringVar(&opts.clusterDNS, "cluster-dns", "", "customer cluster's coredns/kube-dns ClusterIP (e.g. 10.96.0.10)")
	fs.StringVar(&opts.clusterDomain, "cluster-domain", "cluster.local", "customer cluster's service-DNS suffix")
	fs.StringVar(&opts.burstProviderIDFormat, "burst-provider-id-format", "", "Node providerID template stamped on bursts (e.g. linode://yscale-burst-{BURST_ID}); used to bypass managed-CCM Node-delete loops")
	fs.BoolVar(&opts.gatewayEnabled, "gateway-enabled", true, "this cluster's yscale-agent install includes the cilium-gateway sidecar; required for networking.tier=full submissions to succeed (default $YSCALE_GATEWAY_ENABLED, then true)")
	fs.StringVar(&opts.gatewayHostname, "gateway-hostname", "", "tailnet hostname of the gateway sidecar (e.g. yscale-gateway-cl-smoketest); sent to bursts so they can install static routes via MagicDNS")
	fs.StringVar(&opts.gatewayRoutes, "gateway-routes", "", "comma-separated CIDRs bursts should route via the gateway (e.g. 10.42.0.0/16,10.43.0.0/16)")
	fs.StringVar(&opts.workloadNamespace, "workload-namespace", "", "limit Workload-CR reconciliation to this namespace (default all; set when multiple agents share one cluster)")
	fs.StringVar(&opts.podInventoryNamespaces, "pod-inventory-namespaces", "", "comma-separated namespaces the connector may read (namespaced RBAC); its Job, pod and drain watchers run in each when -workload-namespace is unset")
	fs.DurationVar(&opts.idleNodeGrace, "idle-node-grace", 10*time.Minute, "how long a nodeOnly burst node must carry no schedulable pods before its teardown is requested (0 disables the watcher and the occupancy claim with it; ignored without -authoritative-pod-visibility or when -workload-namespace is set)")
	fs.DurationVar(&opts.occupancyObserveInterval, "occupancy-observe-interval", 5*time.Minute, "how often the idle watcher re-states that it could still read the whole cluster's pods for a burst node; it is what keeps a live nodeOnly burst from expiring on silence (0 sends none, and withdraws the occupancy claim)")
	fs.BoolVar(&opts.authoritativePodVisibility, "authoritative-pod-visibility", false, "this connector's RBAC really does allow a cluster-wide pod LIST; required (with no -workload-namespace) before the idle watcher will run or observe occupancy at all")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, nil
		}
		return nil, err
	}

	if opts.endpoint == "" {
		opts.endpoint = os.Getenv("YSCALE_ENDPOINT")
	}
	if opts.endpoint == "" {
		opts.endpoint = "http://127.0.0.1:8443"
	}
	if opts.token == "" {
		opts.token = os.Getenv("YSCALE_TOKEN")
	}
	if opts.token == "" {
		return nil, fmt.Errorf("YSCALE_TOKEN env var (or -token) required")
	}
	switch opts.bootstrapAuthMode {
	case "kubernetes-token":
	case "eks":
		if opts.disableBootstrap {
			return nil, fmt.Errorf("bootstrap-auth-mode=eks cannot be used with disable-bootstrap")
		}
		if opts.cloudProvider != "aws" {
			return nil, fmt.Errorf("bootstrap-auth-mode=eks requires cloud-provider=aws")
		}
		if strings.TrimSpace(opts.eksClusterName) == "" || strings.TrimSpace(opts.eksRegion) == "" || strings.TrimSpace(opts.eksBootstrapRoleARN) == "" {
			return nil, fmt.Errorf("bootstrap-auth-mode=eks requires eks-cluster-name, eks-region, and eks-bootstrap-role-arn")
		}
	case "gke":
		if opts.disableBootstrap {
			return nil, fmt.Errorf("bootstrap-auth-mode=gke cannot be used with disable-bootstrap")
		}
		if opts.cloudProvider != "gcp" {
			return nil, fmt.Errorf("bootstrap-auth-mode=gke requires cloud-provider=gcp")
		}
	default:
		return nil, fmt.Errorf("unsupported bootstrap-auth-mode %q", opts.bootstrapAuthMode)
	}

	opts.clusterID = clusterIDArg
	if opts.clusterID == "" {
		opts.clusterID = os.Getenv("YSCALE_CLUSTER_ID")
	}
	if opts.clusterID == "" {
		id, err := ensureClusterID(opts.stateDir)
		if err != nil {
			return nil, fmt.Errorf("acquiring cluster id: %w", err)
		}
		opts.clusterID = id
	}

	// gatewayEnabled: flag wins, then env, then default true. The env parser
	// stays conservative: only `1`, `true`, `yes`, and `on` enable it; any
	// other non-empty value disables it.
	gatewayFlagSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "gateway-enabled" {
			gatewayFlagSet = true
		}
	})
	if !gatewayFlagSet {
		if v := strings.ToLower(strings.TrimSpace(os.Getenv("YSCALE_GATEWAY_ENABLED"))); v != "" {
			switch v {
			case "1", "true", "yes", "on":
				opts.gatewayEnabled = true
			default:
				opts.gatewayEnabled = false
			}
		}
	}

	return opts, nil
}

// startBackgroundServices launches the bootstrap-token HTTP server,
// CSR approver, Workload CRD reconciler, pending-pod watcher, burst
// node watcher, and NodeGC. Each is independently gated: bootstrap
// needs boot != nil, the node watcher additionally needs a central
// client to report through, the rest need k8sClient != nil. All run
// for the lifetime of ctx.
//
// fail terminates the agent. Only the bootstrap listener uses it: the
// others degrade a feature, that one silently removes the endpoint
// every burst has to reach.
func startBackgroundServices(
	ctx context.Context,
	fail context.CancelCauseFunc,
	opts *runOptions,
	k8sClient kubernetes.Interface,
	dynCli dynamic.Interface,
	boot *agent.BootstrapServer,
	handler *agent.RealHandler,
	nodeEvents agent.IdleNodeEventReporter,
	podEvents agent.PodEventReporter,
	log *slog.Logger,
) {
	if boot != nil {
		signer := agent.NewStorageSigner(k8sClient, boot, log)
		go func() {
			err := boot.Listen(ctx, opts.bootstrapListenAddr, signer)
			if err == nil || errors.Is(err, context.Canceled) {
				return
			}
			// Without this listener no burst can fetch a kubeconfig and
			// no bucket URL can be signed, so every workload central
			// schedules here would hang at bring-up. Exit instead, and
			// let the pod restart / central's connection drop be the
			// visible failure.
			log.Error("bootstrap server exited; shutting the connector down so central stops scheduling here", "error", err)
			fail(fmt.Errorf("bootstrap server: %w", err))
		}()
	}
	if k8sClient == nil {
		return
	}

	// CSR approver: auto-approves kubelet-bootstrap CSRs from the
	// yscale bursts group so kubelets don't sit Pending after
	// authenticating with the bootstrap token.
	approver := agent.NewCSRApprover(k8sClient, log)
	if opts.bootstrapAuthMode == "gke" {
		approver.GKEMode = true
		approver.GKENamespace = opts.gkeNamespace
	}
	go func() {
		if err := approver.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("csr approver exited", "error", err)
		}
	}()

	centralCli := agent.NewCentralHTTPClient(opts.endpoint, opts.token, opts.clusterID)

	// Workload CRD reconciler: kubectl apply -f workload.yaml is the
	// primary user-facing entry point. Watches Workload CRs, proxies
	// to central, mirrors status.
	if dynCli != nil {
		reconciler := agent.NewWorkloadReconciler(k8sClient, dynCli, centralCli, log)
		reconciler.GatewayEnabled = opts.gatewayEnabled
		reconciler.Namespace = opts.workloadNamespace
		go func() {
			if err := reconciler.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("workload reconciler exited", "error", err)
			}
		}()
	}

	// Pending-pod watcher: KEDA / Deployments / Argo / etc create
	// pods with the burst-node selector; we provision matching
	// capacity behind the scenes so the customer's tools "just work."
	//
	// This and the Job and pod-scheduling watchers below run once per readable
	// namespace. Under namespaced RBAC an unscoped watch is forbidden, and a
	// completion watcher that sees no Jobs never reports a workload finished,
	// so its burst bills until the safety net fires (seen live 2026-10-04).
	readable := agent.ReadableNamespaces(opts.workloadNamespace, splitCommaList(opts.podInventoryNamespaces))
	for _, namespace := range readable {
		ppw := agent.NewPendingPodWatcher(k8sClient, centralCli, log)
		ppw.Namespace = namespace
		go func() {
			if err := ppw.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("pending-pod watcher exited", "namespace", namespace, "error", err)
			}
		}()
	}

	// Completion watcher: when a workload's Job reaches a terminal
	// state, report it so central reaps the burst behind it (destroys
	// the backend VM, the tailnet device, and this Node object).
	var artifactPullSecrets []corev1.LocalObjectReference
	if encoded := os.Getenv("YSCALE_AGENT_PULL_SECRETS"); encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &artifactPullSecrets); err != nil {
			log.Error("artifact uploader image pull secrets are invalid", "error", err)
		}
	}
	for _, namespace := range readable {
		cw := agent.NewCompletionWatcher(k8sClient, dynCli, centralCli, log)
		cw.Namespace = namespace
		cw.ArtifactImage = os.Getenv("YSCALE_AGENT_IMAGE")
		cw.ArtifactNamespace = os.Getenv("POD_NAMESPACE")
		cw.ArtifactPullSecrets = artifactPullSecrets
		go func() {
			if err := cw.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("completion watcher exited", "namespace", namespace, "error", err)
			}
		}()
	}

	// Burst node watcher: central has no other view of what happened to
	// the nodes behind its bursts, so a node that never joins, one whose
	// kubelet dies, and one someone deleted all look like "provisioning"
	// there until a budget runs out. Gated on the WS client because that
	// is the seam it publishes through — there is nowhere to report
	// without it.
	if nodeEvents != nil {
		nw := agent.NewNodeWatcher(k8sClient, nodeEvents, opts.clusterID, log)
		go func() {
			if err := nw.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("burst node watcher exited", "error", err)
			}
		}()
	}

	// Idle burst node watcher: a nodeOnly burst backs pods central never
	// created, so nothing reports it finished when KEDA scales the Deployment
	// back to zero. This is the only signal that says the capacity is spare.
	// It declines to run without BOTH an unscoped connector and an explicit
	// cluster-wide pod-visibility grant — see IdleNodeWatcher.Run — and the
	// connector passes both in rather than deciding here so the refusal stays the
	// watcher's own contract.
	//
	// It is also the only sender of the occupancy observations central's nodeOnly
	// ceiling measures silence from. A connector that does not run it — scoped to
	// a namespace, without the visibility grant, or with idle teardown switched
	// off — sends none, and central's automatic silence ceiling therefore never
	// applies to its nodeOnly bursts at all. Those bursts are bounded by their
	// declared budget and nothing else, which is why a namespaced or shared
	// install must give nodeOnly workloads an explicit budget.
	if nodeEvents != nil && opts.idleNodeGrace > 0 {
		iw := agent.NewIdleNodeWatcher(k8sClient, nodeEvents, opts.clusterID, log)
		iw.IdleGrace = opts.idleNodeGrace
		iw.OccupancyInterval = opts.occupancyObserveInterval
		iw.WorkloadNamespace = opts.workloadNamespace
		iw.AuthoritativePodVisibility = opts.authoritativePodVisibility
		go func() {
			if err := iw.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("idle burst node watcher exited", "error", err)
			}
		}()
	}

	// Pod scheduling watcher: tells central when a workload pod becomes
	// Scheduled with a non-empty nodeName. The observation is cached on
	// the WebSocket client so it survives disconnects.
	if podEvents != nil {
		for _, namespace := range readable {
			psw := agent.NewPodSchedulingWatcher(k8sClient, podEvents, log)
			psw.Namespace = namespace
			go func() {
				if err := psw.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Error("pod scheduling watcher exited", "namespace", namespace, "error", err)
				}
			}()
		}
	}

	// NodeGC: periodically reaps ghost burst Node objects that linger
	// NotReady when central's DrainNode push was dropped (WS blip, etc.).
	gc := agent.NewNodeGC(k8sClient, handler, log)
	go func() {
		if err := gc.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("node gc exited", "error", err)
		}
	}()
}

func runArtifactUpload() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return agent.RunArtifactUploadCommand(ctx,
		os.Getenv("YSCALE_ARTIFACT_UPLOADS"), os.Getenv("BOOTSTRAP_ENDPOINT"), os.Getenv("BURST_ID"))
}

// routeReporter is the slice of *agent.Client publishGatewayRoutes needs:
// report the masked route set to central. Kept as an interface so the publish
// path is testable without a live WS connection.
type routeReporter interface {
	ReportClusterRoutes(routes []string)
}

func publishGatewayRoutes(
	ctx context.Context,
	kube kubernetes.Interface,
	dyn dynamic.Interface,
	opts *runOptions,
	reporter routeReporter,
	log *slog.Logger,
) {
	if kube == nil {
		log.Error("gateway route detection failed; kubernetes client unavailable; leaving ConfigMap unchanged")
		return
	}

	namespace, err := podNamespace()
	if err != nil {
		log.Error("gateway route detection failed; agent namespace unavailable; leaving ConfigMap unchanged", "error", err)
		return
	}

	overrides := splitGatewayRoutes(opts.gatewayRoutes)
	routes, err := clusterroutes.Resolve(ctx, kube, dyn, clusterroutes.Options{
		CloudProvider:         opts.cloudProvider,
		Override:              overrides,
		ServiceProbeNamespace: namespace,
	})
	if err != nil {
		log.Error("gateway route detection failed; leaving ConfigMap unchanged", "error", err, "cloud_provider", opts.cloudProvider, "override", len(overrides) > 0)
		return
	}

	resolved := routes.All()
	if len(resolved) == 0 {
		log.Error("gateway route detection returned no routes; leaving ConfigMap unchanged", "cloud_provider", opts.cloudProvider)
		return
	}

	if err := upsertGatewayRoutesConfigMap(ctx, kube, namespace, strings.Join(resolved, "\n")); err != nil {
		log.Error("gateway routes ConfigMap update failed; leaving previous content unchanged", "error", err, "namespace", namespace, "configmap", gatewayRoutesConfigMapName)
		return
	}
	log.Info("gateway routes ConfigMap updated", "namespace", namespace, "configmap", gatewayRoutesConfigMapName, "routes", resolved)

	// Report the IDENTICAL masked slice to central so the advertised set and
	// the approved set are one canonical form. The client caches it and
	// re-asserts after every reconnect.
	if reporter != nil {
		reporter.ReportClusterRoutes(resolved)
	}
}

func upsertGatewayRoutesConfigMap(ctx context.Context, kube kubernetes.Interface, namespace, routes string) error {
	configMaps := kube.CoreV1().ConfigMaps(namespace)
	existing, err := configMaps.Get(ctx, gatewayRoutesConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = configMaps.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      gatewayRoutesConfigMapName,
				Namespace: namespace,
			},
			Data: map[string]string{
				gatewayRoutesConfigMapKey: routes,
			},
		}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if existing.Data != nil && existing.Data[gatewayRoutesConfigMapKey] == routes {
		return nil
	}

	updated := existing.DeepCopy()
	if updated.Data == nil {
		updated.Data = map[string]string{}
	}
	updated.Data[gatewayRoutesConfigMapKey] = routes
	_, err = configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

func podNamespace() (string, error) {
	if namespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); namespace != "" {
		return namespace, nil
	}
	data, err := os.ReadFile(serviceAccountNamespaceFile)
	if err != nil {
		return "", err
	}
	namespace := strings.TrimSpace(string(data))
	if namespace == "" {
		return "", fmt.Errorf("%s is empty", serviceAccountNamespaceFile)
	}
	return namespace, nil
}

func splitGatewayRoutes(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n'
	})
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		if field = strings.TrimSpace(field); field != "" {
			out = append(out, field)
		}
	}
	return out
}

func splitCommaList(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == '\n' })
	seen := make(map[string]struct{}, len(fields))
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if _, exists := seen[field]; exists {
			continue
		}
		seen[field] = struct{}{}
		out = append(out, field)
	}
	return out
}

// buildK8sClient builds a clientset from the in-cluster service-
// account mount. Returns (nil, err) when not running in a Pod —
// callers can degrade gracefully.
func buildK8sClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

// buildDynamicClient builds a dynamic client for CRD operations.
func buildDynamicClient() (dynamic.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(cfg)
}

// ensureClusterID reads stateDir/cluster-id, generates and persists
// one if absent. Idempotent across restarts so the same cluster
// reconnects as itself.
func ensureClusterID(stateDir string) (string, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(stateDir, "cluster-id")
	if data, err := os.ReadFile(path); err == nil {
		id := string(data)
		if len(id) >= 16 {
			return id, nil
		}
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	id := "cl_" + hex.EncodeToString(b[:])
	if err := os.WriteFile(path, []byte(id), 0o600); err != nil {
		return "", err
	}
	return id, nil
}
