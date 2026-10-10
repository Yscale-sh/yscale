// yscaletest — end-to-end test runner that drives the canonical
// customer flow: kubectl apply Workload CR → yscale-agent → yscale-
// cloud (central) → backend → burst node joins → pod runs → burst
// reaped. Lives outside agent/central code so it has no special
// access — it talks to the cluster the same way a real customer's
// kubectl would.
//
// Per cleanup_safety_directive: this runner ONLY deletes Workload
// CRs whose label `yscale.sh/yscaletest-run=<runID>` matches the
// current invocation. It never sweeps cluster-wide.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yscale-sh/yscale/internal/evidence"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

func main() {
	var (
		caseDir                   = flag.String("dir", "test/cases", "directory of Workload CR YAMLs to run")
		parallel                  = flag.Int("parallel", 4, "max test cases running concurrently")
		caseTO                    = flag.Duration("timeout", 12*time.Minute, "per-case wall-clock budget")
		nodeReadyTO               = flag.Duration("node-ready-timeout", defaultNodeReadyTimeout, "budget for the exact burst node to appear and become Kubernetes NodeReady=True after CR creation; independent of -timeout")
		kubeconfig                = flag.String("kubeconfig", defaultKubeconfig(), "path to kubeconfig (defaults to $KUBECONFIG or ~/.kube/config)")
		namespace                 = flag.String("namespace", "default", "namespace to apply Workload CRs in")
		dryRun                    = flag.Bool("dry-run", false, "print rendered CRs and exit; don't apply")
		filter                    = flag.String("filter", "", "comma-separated substrings; run cases whose filename contains ANY of them (e.g. \"flyio\" or \"flyio,linode\"). Empty = all.")
		watchPort                 = flag.Int("watch", 0, "if non-zero, serve a live dashboard at http://localhost:<port> (suggested: 7890)")
		centralSvc                = flag.String("central-service", "yscale-cloud", "central Service whose ClusterIP replaces "+clusterIPPlaceholder+" in case YAML")
		centralNS                 = flag.String("central-namespace", "yscale", "namespace of the central Service")
		evidenceURL               = flag.String("central-evidence-url", envDefault("YSCALE_EVIDENCE_URL", ""), "Central base URL for durable evidence (e.g. https://api.yscale.sh); env YSCALE_EVIDENCE_URL")
		evidenceToken             = flag.String("central-evidence-token", envDefault("YSCALE_EVIDENCE_TOKEN", ""), "scoped connector/tenant token for the evidence endpoint; env YSCALE_EVIDENCE_TOKEN")
		evidenceDir               = flag.String("evidence-dir", "", "if set, write one JSON evidence artifact per case to this directory (mode 0600)")
		evidenceTargetClusterType = flag.String("evidence-target-cluster-type", "", "target cluster type for evidence artifacts (required when -evidence-dir is set)")
		evidenceMeshProvider      = flag.String("evidence-mesh-provider", envDefault("YSCALE_EVIDENCE_MESH_PROVIDER", ""), "coordination plane to audit for mesh device absence; required when -evidence-dir is set; exactly \"tailscale\" or \"fabric\"; env YSCALE_EVIDENCE_MESH_PROVIDER")
		tsTailnet                 = flag.String("ts-tailnet", envDefault("TS_TAILNET", ""), "Tailscale tailnet for mesh audit; env TS_TAILNET (used only when -evidence-mesh-provider=tailscale)")
		tsClientID                = flag.String("ts-client-id", envDefault("TS_OAUTH_CLIENT_ID", ""), "Tailscale OAuth client ID for mesh audit; env TS_OAUTH_CLIENT_ID (used only when -evidence-mesh-provider=tailscale)")
		tsClientSecret            = flag.String("ts-client-secret", envDefault("TS_OAUTH_CLIENT_SECRET", ""), "Tailscale OAuth client secret for mesh audit; env TS_OAUTH_CLIENT_SECRET (used only when -evidence-mesh-provider=tailscale)")
		fabricURL                 = flag.String("fabric-url", envDefault("YSCALE_EVIDENCE_FABRIC_URL", ""), "Fabric box base URL (https required except loopback) for mesh audit; env YSCALE_EVIDENCE_FABRIC_URL (used only when -evidence-mesh-provider=fabric)")
		fabricAPIKey              = flag.String("fabric-api-key", envDefault("YSCALE_EVIDENCE_FABRIC_API_KEY", ""), "Fabric API key for mesh audit; env YSCALE_EVIDENCE_FABRIC_API_KEY (used only when -evidence-mesh-provider=fabric)")
		fabricUser                = flag.String("fabric-user", envDefault("YSCALE_EVIDENCE_FABRIC_USER", ""), "Fabric user (namespace) that owns burst nodes; env YSCALE_EVIDENCE_FABRIC_USER (used only when -evidence-mesh-provider=fabric)")
	)
	flag.Parse()

	// Evidence preflight: validate all evidence-mode preconditions BEFORE
	// any Kubernetes client, Service read, provider inventory construction,
	// or workload mutation. Evidence-disabled behavior remains unchanged.
	evCfg, err := evidence.ValidatePreflight(*evidenceDir, *evidenceURL, *evidenceToken, *evidenceTargetClusterType, *evidenceMeshProvider, nil)
	if err != nil {
		fail("%v", err)
	}
	var commitSHA string
	if evCfg != nil {
		commitSHA = evCfg.CommitSHA
		fmt.Printf("evidence: dir=%s commit=%s target-cluster-type=%s mesh-provider=%s\n",
			evCfg.ArtifactDir, commitSHA, evCfg.TargetClusterType, evCfg.MeshProvider)
	}

	cases, err := loadCases(*caseDir, *filter)
	if err != nil {
		fail("load cases: %v", err)
	}
	if len(cases) == 0 {
		fail("no test cases found in %s (filter=%q)", *caseDir, *filter)
	}

	if *dryRun {
		for _, c := range cases {
			fmt.Printf("=== %s ===\n%s\n", c.Name, string(c.RawYAML))
		}
		return
	}

	// Evidence case preflight: validate that every selected case uses an
	// explicit supported backend, and that only the selected mesh-provider's
	// audit credentials are required. Runs after dry-run handling but before
	// Central auth, Kubernetes construction, provider/mesh construction, or
	// workload mutation.
	if evCfg != nil {
		var caseBackends []evidence.CaseBackend
		for _, c := range cases {
			backend := extractSpecBackend(c.RawYAML)
			caseBackends = append(caseBackends, evidence.CaseBackend{
				Name:    c.Name,
				Backend: backend,
			})
		}
		if err := evidence.ValidateEvidenceCases(
			caseBackends,
			os.Getenv("LINODE_TOKEN"),
			evCfg.MeshProvider,
			evidence.MeshAuditCredentials{
				TailscaleClientID:     *tsClientID,
				TailscaleClientSecret: *tsClientSecret,
				TailscaleTailnet:      *tsTailnet,
				FabricURL:             *fabricURL,
				FabricAPIKey:          *fabricAPIKey,
				FabricUser:            *fabricUser,
			},
		); err != nil {
			fail("%v", err)
		}
	}

	// Authenticate Central before constructing a cluster or provider client.
	// A valid credential reaches the deliberately absent workload and returns
	// 404; invalid/expired credentials fail here, before paid mutation.
	var evidenceClient *CentralEvidenceClient
	if strings.TrimSpace(*evidenceURL) != "" {
		evidenceClient = &CentralEvidenceClient{
			BaseURL: strings.TrimSpace(*evidenceURL),
			Token:   *evidenceToken,
			Client:  &http.Client{Timeout: 30 * time.Second},
		}
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := evidenceClient.ProbeAuth(probeCtx)
		probeCancel()
		if err != nil {
			fail("central evidence authentication: %v", err)
		}
		fmt.Println("central evidence: authenticated endpoint configured (token=<redacted>)")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		fail("kubeconfig: %v", err)
	}
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fail("k8s client: %v", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		fail("dynamic client: %v", err)
	}
	var accessProber AccessProber
	if evCfg != nil {
		accessProber = NewKubernetesAccessProber(k8s, cfg)
	}

	centralRef := *centralNS + "/" + *centralSvc
	centralIP, centralErr := resolveCentralClusterIP(context.Background(), k8s, *centralNS, *centralSvc)
	if centralErr == nil {
		fmt.Printf("central %s clusterIP=%s (fills %s)\n", centralRef, centralIP, clusterIPPlaceholder)
	} else {
		fmt.Fprintf(os.Stderr, "WARN: could not resolve %s ClusterIP: %v — cases using %s will fail\n", centralRef, centralErr, clusterIPPlaceholder)
	}

	var provider ProviderInventory
	if os.Getenv("LINODE_TOKEN") != "" {
		provider = newLinodeInventory(os.Getenv("LINODE_TOKEN"))
		fmt.Printf("linode provider audit enabled (tag %q + per-case burstID)\n", yscaleBurstTag)
	} else {
		fmt.Fprintf(os.Stderr, "WARN: LINODE_TOKEN unset — %s cases cannot be proven reaped and will fail their provider audit\n", backendLinode)
	}

	var mesh MeshInventory
	switch {
	case evCfg != nil && evCfg.MeshProvider == evidence.MeshProviderFabric:
		mesh = newFabricInventory(*fabricURL, *fabricAPIKey, *fabricUser)
		fmt.Println("fabric mesh audit enabled (per-customer box, hostname-exact match per case)")
	case evCfg != nil && evCfg.MeshProvider == evidence.MeshProviderTailscale:
		mesh = newTailscaleInventory(*tsClientID, *tsClientSecret, *tsTailnet)
		fmt.Println("tailscale mesh audit enabled (hostname-exact match per case)")
	case *tsTailnet != "" && *tsClientID != "" && *tsClientSecret != "":
		// Evidence-disabled legacy behaviour: keep the ambient Tailscale
		// audit available so non-evidence runs are unchanged.
		mesh = newTailscaleInventory(*tsClientID, *tsClientSecret, *tsTailnet)
		fmt.Println("tailscale mesh audit enabled (hostname-exact match per case)")
	}

	runID, err := evidence.GenerateRunID(time.Now())
	if err != nil {
		fail("run ID: %v", err)
	}
	fmt.Printf("yscaletest: run=%s kubeconfig=%s namespace=%s cases=%d parallel=%d per-case-timeout=%s node-ready-timeout=%s\n",
		runID, *kubeconfig, *namespace, len(cases), *parallel, *caseTO, *nodeReadyTO)

	// Optional live dashboard. If -watch was set, this opens an HTTP
	// server on the requested port and serves /events as a stream of
	// state updates. The Runner emits events into the dashboard on
	// every state change.
	dash := NewDashboard(*watchPort, runID)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Sanity-check agent reachability before we apply anything — a
	// missing agent is the #1 reason workloads stay Pending forever.
	if err := checkAgentConnected(ctx, k8s); err != nil {
		fmt.Fprintf(os.Stderr, "WARN: agent preflight: %v (continuing anyway)\n", err)
	}

	sem := make(chan struct{}, *parallel)
	results := make([]CaseResult, len(cases))
	var wg sync.WaitGroup

	for i, c := range cases {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			caseCtx, caseCancel := context.WithTimeout(ctx, *caseTO)
			defer caseCancel()

			r := Runner{
				K8s:               k8s,
				Dyn:               dyn,
				Namespace:         *namespace,
				RunID:             runID,
				Dash:              dash,
				CentralClusterIP:  centralIP,
				CentralRef:        centralRef,
				CentralResolveErr: centralErr,
				Provider:          provider,
				Mesh:              mesh,
				CentralEvidence:   evidenceClient,
				EvidenceEnabled:   evCfg != nil,
				AccessProber:      accessProber,
				NodeReadyTimeout:  *nodeReadyTO,
			}
			results[i] = r.Run(caseCtx, c)
			if evCfg != nil {
				obs := evidenceObservations{
					CentralEvidence:         r.centralEvidence,
					TargetClusterType:       evCfg.TargetClusterType,
					NodeReadyAt:             r.nodeReadyAt,
					GPUReadyAt:              r.gpuReadyAt,
					ObservedNode:            observedNodeFromSnapshot(r.gpuNodeSnapshot),
					AccessProbe:             r.accessProbe,
					AccessProbeFailureStage: r.accessProbeFailureStage,
				}
				if results[i].Reaped {
					inv, err := r.observeInventory(caseCtx, results[i])
					if err != nil {
						fmt.Fprintf(os.Stderr, "WARN: inventory observation for %s: %v\n", c.Name, err)
					} else {
						obs.Inventory = inv
					}
				}
				artifactResult, err := writeEvidence(evCfg.ArtifactDir, results[i], runID, commitSHA, obs)
				if err != nil {
					fmt.Fprintf(os.Stderr, "ERROR: write evidence for %s: %v\n", c.Name, err)
					results[i].ArtifactResult = string(evidence.ResultFailed)
				} else {
					results[i].ArtifactResult = string(artifactResult)
				}
			}
		}()
	}
	wg.Wait()

	printReport(results)
	if anyFailed(results) {
		os.Exit(1)
	}
}

// resolveCentralClusterIP looks up the central Service's ClusterIP. Cases
// target central by IP rather than name because burst-local DNS can't
// resolve cluster.local from a Fly/Linode burst pod (see
// flyio-full-podtopod.yaml), so a Service name would never resolve there.
// Injecting the live IP keeps the fixture portable across clusters.
func resolveCentralClusterIP(ctx context.Context, k8s kubernetes.Interface, ns, name string) (string, error) {
	svc, err := k8s.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	ip := svc.Spec.ClusterIP
	if ip == "" || ip == "None" {
		return "", fmt.Errorf("service %s/%s has no ClusterIP (got %q)", ns, name, ip)
	}
	return ip, nil
}

func defaultKubeconfig() string {
	if v := os.Getenv("KUBECONFIG"); v != "" {
		return v
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".kube", "config")
	}
	return ""
}

func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(2)
}

func decodeWorkloadSpec(raw []byte) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(raw, &obj.Object); err != nil {
		return nil, err
	}
	return obj, nil
}

// observedNodeFromSnapshot translates the runner's fail-closed snapshot into
// the retained evidence receipt. Nil in → nil out, so a runner that never
// captured a valid observation never fabricates one; a captured snapshot
// carries name, observed allocatable count, and the label observation
// verbatim.
func observedNodeFromSnapshot(snap *GPUNodeSnapshot) *evidence.ObservedNode {
	if snap == nil {
		return nil
	}
	return &evidence.ObservedNode{
		Name:            snap.NodeName,
		GPUAllocatable:  snap.GPUAllocatable,
		GPUPresentLabel: snap.GPUPresentLabel,
		GPUProductLabel: snap.GPUProductLabel,
	}
}

func extractSpecBackend(raw []byte) string {
	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(raw, &obj.Object); err != nil {
		return ""
	}
	backend, _, _ := unstructured.NestedString(obj.Object, "spec", "backend")
	return backend
}
