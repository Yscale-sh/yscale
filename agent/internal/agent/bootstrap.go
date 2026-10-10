package agent

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/yscale-sh/yscale/pkg/backends"
	k8sv1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// BootstrapServer is the agent-side endpoint burst nodes hit, over
// yscale's tailnet, to obtain a kubelet bootstrap kubeconfig. The
// kubelet uses the returned token via standard k8s bootstrap-tokens-auth
// flow (the CSR approver in cmd/yscale-agent auto-approves matching
// CSRs).
//
// Security model:
//   - Reachable only inside the tailnet (MagicDNS hostname
//     `yscale-agent-<cluster-id>`). That is a property of the BIND
//     ADDRESS, not of the tailnet ACL alone: the agent is a Pod, so a
//     wildcard bind would also publish these endpoints on its cluster
//     pod IP, where the ACL cannot reach. Listen therefore resolves an
//     address that only the tailnet can deliver to — tailscaled's own
//     100.64.0.0/10 IPv4 in kernel mode, loopback behind `tailscale
//     serve` in userspace mode. A wildcard is reachable only by an
//     operator who spells one out (see resolveListenAddr).
//   - Only burst-IDs that have been authorized via AuthorizeBurst
//     (called by the BurstAnnounce handler) can mint a token. The
//     BurstID is a 12-hex-char random token central generates per
//     burst — treat it as a single-use secret. Source-IP checks aren't
//     used: tailnet IPs are dynamically assigned, and tailnet
//     membership + tag-based ACL is the trust boundary.
//   - Tokens have a 15-minute TTL via the standard "expiration"
//     bootstrap-token field; kubelet swaps to a real client cert on
//     first CSR approval (typically <30s)
type BootstrapServer struct {
	K8s       kubernetes.Interface
	Log       *slog.Logger
	APIServer string // https://<kubernetes-service-clusterip>:443
	ClusterCA []byte // PEM bytes of the cluster's API server CA
	// TokenIssuer overrides the standard Kubernetes bootstrap-token Secret
	// flow. Managed EKS does not enable that authenticator, so EKS installs
	// inject an IAM-backed issuer instead. GKE mode also uses a TokenIssuer
	// (gkeTokenIssuer) that creates per-node SAs and mints SA tokens.
	TokenIssuer BootstrapTokenIssuer

	// GKECleaner, when non-nil, cleans up per-node ServiceAccounts and
	// ClusterRoleBindings created by the GKE bootstrap mode on Forget/ForgetNode.
	GKECleaner GKEBootstrapCleaner

	// GatewayHostname is the tailnet MagicDNS name of the customer
	// cluster's gateway sidecar (yscale-gateway-${clusterID}). Sent in
	// every bootstrap response so bursts can resolve it via Tailscale
	// and install a static route. Empty when the gateway sidecar isn't
	// deployed in the customer cluster (a misconfiguration now that the
	// removed lite tier no longer offers a gateway-free path).
	GatewayHostname string
	// GatewayRoutes lists the customer-cluster CIDRs bursts should reach
	// via the gateway. Each entry installed as a separate static kernel
	// route on the burst: `ip route add <cidr> via <gateway-ip> dev
	// tailscale0`. Typically the customer's pod CIDR; may include the
	// cluster Service CIDR. Empty when gateway not configured.
	GatewayRoutes []string

	// CloudProvider is the customer cluster's cloud provider
	// (linode|aws|gcp|azure|k3s|self-managed). Sent in the bootstrap
	// response so the burst can adjust providerID behaviour — GKE
	// admission rejects non-GCE provider IDs, so a burst joining a GKE
	// cluster must omit --provider-id entirely.
	CloudProvider string

	// ifaceLookup and tailnetWait drive Listen's tailnet-address
	// resolution. Zero values mean the real interfaces and the
	// production wait window; tests substitute their own.
	ifaceLookup ifaceLister
	tailnetWait time.Duration

	mu     sync.Mutex
	grants map[string]*burstGrant // authorized burst-ids
	nodes  map[string]string      // node name -> burst id, learned at bootstrap time

	gpuSamples      map[string]protocol.GPUTelemetrySample // burst id -> latest sample (guarded by mu)
	gpuSampleCursor string                                 // last burst id included in a heartbeat batch
}

// burstGrant is everything the agent remembers about an authorized
// burst between its announce and its teardown: the networking tier
// (recorded for logging/observability now that full is the only
// supported tier), the workload namespace central authorized the burst
// for, and the storage bindings served in the bootstrap response. Fields
// are set once in AuthorizeBurst and read-only afterwards.
type burstGrant struct {
	// tier is the announced networking tier. Only "full" (or "", from an
	// older central) is reachable in production: workload admission refuses
	// the removed "lite" tier before central ever authors an announce. Kept
	// on the grant so a stale announce from an older central logs
	// coherently and the field can be inspected in ops tooling.
	tier      string
	namespace string
	storage   []protocol.StorageBinding
}

// NewBootstrapServer reads the API server URL and CA bundle from the
// in-cluster service-account mount. Errors out if either is missing —
// without them we can't issue valid bootstrap kubeconfigs.
//
// The APIServer URL the kubelet sees can be overridden via
// BOOTSTRAP_APISERVER_URL — point that at a tailscale-hosted proxy
// (e.g. https://yscale-agent-<id>:6443) so the burst can reach the
// API server over the tailnet without subnet routing. The
// tls-server-name in the kubeconfig is hardcoded to
// kubernetes.default.svc.cluster.local so cert validation still works.
func NewBootstrapServer(k8s kubernetes.Interface, log *slog.Logger) (*BootstrapServer, error) {
	if log == nil {
		log = slog.Default()
	}
	apiServer := os.Getenv("BOOTSTRAP_APISERVER_URL")
	if apiServer == "" {
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
		if host == "" || port == "" {
			return nil, errors.New("KUBERNETES_SERVICE_HOST/PORT not set; agent must run in a Pod")
		}
		apiServer = fmt.Sprintf("https://%s:%s", host, port)
	}
	caPath := "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", caPath, err)
	}
	return &BootstrapServer{
		K8s:       k8s,
		Log:       log,
		APIServer: apiServer,
		ClusterCA: caPEM,
		grants:    make(map[string]*burstGrant),
		nodes:     make(map[string]string),
	}, nil
}

// expectedBurstNodeName returns the Kubernetes node name a given BurstID is
// allowed to register as. It mirrors central's derivation (decider.go:
// backends.NodeNamePrefix + the BurstID with its "burst_" prefix stripped),
// so the agent can verify a bootstrap request's node_name belongs to the
// authenticated BurstID without central having to send the name separately.
func expectedBurstNodeName(burstID string) string {
	return backends.NodeNamePrefix + strings.TrimPrefix(burstID, "burst_")
}

// AuthorizeBurst records that a burst with this ID is allowed to
// fetch a kubeconfig from this agent, the burst's networking tier, the
// workload namespace central authorized it for, and the storage
// bindings the burst should receive in its bootstrap response. Auth is
// by BurstID secrecy — central must keep BurstIDs unguessable (12 hex
// chars from rand.Read).
func (s *BootstrapServer) AuthorizeBurst(burstID, tier, namespace string, bindings []protocol.StorageBinding) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants[burstID] = &burstGrant{tier: tier, namespace: namespace, storage: bindings}
}

// authorizedStorage returns what the agent may sign for a burst: the
// namespace its credentials Secrets live in and the bindings central
// authorized. ok is false for an unknown BurstID. Both values come
// from the server side of the protocol; nothing a burst sends can
// widen them.
func (s *BootstrapServer) authorizedStorage(burstID string) (namespace string, bindings []protocol.StorageBinding, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	grant, ok := s.grants[burstID]
	if !ok {
		return "", nil, false
	}
	return grant.namespace, grant.storage, true
}

// Forget drops a burst from the authorized set. Called when a burst
// is torn down so a leaked BurstID can't keep minting tokens.
// Also cleans up per-node GKE bootstrap resources (SA + CRB) when
// the GKE cleaner is configured.
func (s *BootstrapServer) Forget(burstID string) {
	s.mu.Lock()
	var nodesToClean []string
	delete(s.grants, burstID)
	delete(s.gpuSamples, burstID)
	for node, id := range s.nodes {
		if id == burstID {
			nodesToClean = append(nodesToClean, node)
			delete(s.nodes, node)
		}
	}
	cleaner := s.GKECleaner
	s.mu.Unlock()

	if cleaner != nil {
		for _, node := range nodesToClean {
			cleaner.CleanupNode(context.Background(), node)
		}
	}
}

// ForgetNode drops the burst behind the given node name from the
// authorized set. Teardown flows know the k8s Node name (DrainNode
// carries it) but not the BurstID; the node→burst mapping is learned
// when the burst fetches its bootstrap kubeconfig. No-op for unknown
// nodes (e.g. a burst that died before ever bootstrapping — its grant
// is only droppable by BurstID, and expires with the agent process).
// Also cleans up per-node GKE bootstrap resources when configured.
func (s *BootstrapServer) ForgetNode(nodeName string) {
	s.mu.Lock()
	burstID, ok := s.nodes[nodeName]
	cleaner := s.GKECleaner
	s.mu.Unlock()

	if ok {
		s.Forget(burstID)
	} else if cleaner != nil {
		// Even without a known grant, clean up GKE resources for this node.
		cleaner.CleanupNode(context.Background(), nodeName)
	}
}

// Listen serves the endpoint on listenAddr, resolved by
// resolveListenAddr — an explicit host binds verbatim, a bare ":port"
// binds the tailnet IPv4 or loopback, and neither spelling ever expands
// to all interfaces. Resolution failure is fatal and the caller must
// treat it as fatal for the process: the endpoint mints kubelet
// bootstrap tokens and pre-signed bucket URLs, so a connector that
// keeps its central session while serving neither is worse than one
// that exits and lets central schedule elsewhere. Optional handlers
// (e.g. StorageSigner) can register extra routes on the same mux via
// Register().
func (s *BootstrapServer) Listen(ctx context.Context, listenAddr string, extras ...interface{ Register(*http.ServeMux) }) error {
	lookup, wait := s.ifaceLookup, s.tailnetWait
	if lookup == nil {
		lookup = hostInterfaces
	}
	if wait == 0 {
		wait = tailnetWaitTimeout
	}
	addr, err := waitForListenAddr(ctx, listenAddr, lookup, wait, tailnetPollInterval)
	if err != nil {
		return fmt.Errorf("resolve bootstrap listen address: %w", err)
	}
	if isWildcardListen(addr) {
		s.Log.Warn("bootstrap server bound to all interfaces by explicit -bootstrap-listen; "+
			"burst-token minting and bucket-URL signing are reachable from every pod in this cluster, not just the tailnet",
			"addr", addr)
	} else if fellBackToLoopback(listenAddr, addr) {
		s.Log.Info("no tailnet address on this pod; bootstrap server bound loopback. Inbound tailnet traffic "+
			"has to arrive via `tailscale serve` (the shipped chart configures it). Pass "+
			"-bootstrap-listen=tailnet:<port> to require tailscaled's own address and fail closed without it",
			"addr", addr)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /bootstrap-kubeconfig", s.handleBootstrap)
	mux.HandleFunc("POST /gpu-telemetry", s.handleGPUTelemetry)
	for _, ex := range extras {
		ex.Register(mux)
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	s.Log.Info("bootstrap server listening", "addr", addr)

	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

type bootstrapRequest struct {
	BurstID  string `json:"burst_id"`
	NodeName string `json:"node_name"`
}

// BootstrapResponse is what the burst's init binary receives from the
// agent. Kubeconfig is the kubelet bootstrap kubeconfig (YAML);
// CiliumKubeconfig (optional) is a kubeconfig with the cilium SA's
// token from kube-system, used by burst-side cilium-agent (which
// runs as a HOST process on the burst, not a K8s pod, so it can't
// auto-mount the SA token like a normal in-cluster Cilium DS would).
// Storage describes the volumes the burst should wire up.
type BootstrapResponse struct {
	Kubeconfig       []byte                    `json:"kubeconfig"`
	CiliumKubeconfig []byte                    `json:"cilium_kubeconfig,omitempty"`
	Storage          []protocol.StorageBinding `json:"storage,omitempty"`

	// GatewayHostname is the tailnet MagicDNS name of the customer
	// cluster's gateway sidecar. Burst entrypoint resolves it via
	// Tailscale and installs static routes (see GatewayRoutes). Empty
	// when the cluster has no gateway sidecar — burst is in
	// island-mode and won't reach customer cluster Services.
	GatewayHostname string `json:"gateway_hostname,omitempty"`
	// GatewayRoutes is the set of CIDRs the burst should reach via the
	// gateway. The burst installs NO static routes for these: the gateway
	// advertises them as Tailscale subnet routes and the burst's
	// --accept-routes turns them into kernel routes on its own. This is the
	// inverse of what an earlier version of this comment claimed — the
	// static `ip route add` approach is the one that was abandoned, and the
	// gateway template records why. The field is carried so the burst can
	// log and sanity-check what it is expected to reach.
	GatewayRoutes []string `json:"gateway_routes,omitempty"`

	// CloudProvider is the customer cluster's cloud provider. The burst
	// uses it to select the right --provider-id strategy: GKE rejects
	// non-GCE provider IDs at admission, so bursts joining a GKE cluster
	// omit --provider-id entirely. Empty/absent = legacy behaviour (the
	// burst self-fetches from cloud metadata).
	CloudProvider string `json:"cloud_provider,omitempty"`
}

func (s *BootstrapServer) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	var req bootstrapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.BurstID == "" || req.NodeName == "" {
		http.Error(w, "burst_id and node_name required", http.StatusBadRequest)
		return
	}

	// Bind the requested node identity to the authenticated BurstID. The node
	// name is a deterministic function of the BurstID, so a burst can only
	// ever register under its OWN node name. Without this check a burst that
	// holds a valid grant for burst_A could request a kubeconfig for node
	// ys-burst-B and bring up a kubelet under another burst's node identity
	// (the client-CSR approver gates on node-name prefix + group, not on the
	// specific name). Checked before recording the node→burst mapping so a
	// rejected request can't pollute it.
	if req.NodeName != expectedBurstNodeName(req.BurstID) {
		http.Error(w, "node_name does not match burst id", http.StatusForbidden)
		return
	}

	s.mu.Lock()
	grant := s.grants[req.BurstID]
	if grant != nil {
		// Remember which Node this burst registers as, so teardown
		// (DrainNode carries only the node name) can revoke the grant.
		s.nodes[req.NodeName] = req.BurstID
	}
	s.mu.Unlock()
	if grant == nil {
		http.Error(w, "unauthorized burst id", http.StatusForbidden)
		return
	}
	// Log the source IP for audit; tailnet ACL is the trust boundary,
	// not the IP itself (tailnet IPs are dynamically assigned).
	if remoteHost, _, _ := net.SplitHostPort(r.RemoteAddr); remoteHost != "" {
		s.Log.Debug("bootstrap request", "burst", req.BurstID, "from", remoteHost)
	}

	credentialMode := "kubernetes-bootstrap-token"
	tokenID := ""
	var token string
	var err error
	if s.TokenIssuer != nil {
		switch s.TokenIssuer.(type) {
		case *GKETokenIssuer:
			credentialMode = "gke-sa"
		default:
			credentialMode = "eks-iam"
		}
		token, err = s.TokenIssuer.Issue(r.Context(), req.NodeName)
		if err != nil {
			s.Log.Error("issue bootstrap credential", "mode", credentialMode, "node", req.NodeName, "error", err)
			http.Error(w, "issue bootstrap credential", http.StatusServiceUnavailable)
			return
		}
	} else {
		var tokenSecret string
		tokenID, tokenSecret, err = generateBootstrapToken()
		if err != nil {
			http.Error(w, "token gen: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := s.createBootstrapTokenSecret(r.Context(), tokenID, tokenSecret, req.NodeName); err != nil {
			s.Log.Error("create bootstrap-token secret", "error", err)
			http.Error(w, "k8s api: "+err.Error(), http.StatusInternalServerError)
			return
		}
		token = fmt.Sprintf("%s.%s", tokenID, tokenSecret)
	}

	kubeconfig := buildBootstrapKubeconfig(s.APIServer, s.ClusterCA, token, req.NodeName)

	// Best-effort: mint a cilium SA token + kubeconfig for the burst's
	// host-mode cilium-agent. Failures are NOT fatal — the burst can
	// still fall back to the K8s Cilium DaemonSet path (which is
	// what's deployed today as the customer's chart). Log + continue.
	// The removed lite tier used to skip this to save a TokenRequest call
	// on bursts that never ran Cilium; every supported tier now runs
	// host-mode Cilium, so mint unconditionally.
	var ciliumKC []byte
	if ciliumKC, err = s.issueCiliumKubeconfig(r.Context()); err != nil {
		s.Log.Warn("issue cilium kubeconfig (continuing without)", "error", err)
	}

	bindings := grant.storage

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(BootstrapResponse{
		Kubeconfig:       kubeconfig,
		CiliumKubeconfig: ciliumKC,
		Storage:          bindings,
		GatewayHostname:  s.GatewayHostname,
		GatewayRoutes:    s.GatewayRoutes,
		CloudProvider:    s.CloudProvider,
	})
	s.Log.Info("issued bootstrap kubeconfig",
		"burst", req.BurstID, "node", req.NodeName, "credential_mode", credentialMode, "token_id", tokenID,
		"cilium_kubeconfig_bytes", len(ciliumKC),
		"storage_bindings", len(bindings),
		"gateway_hostname", s.GatewayHostname,
		"gateway_routes", s.GatewayRoutes)
}

// issueCiliumKubeconfig mints a JWT for the cilium ServiceAccount in
// kube-system via the TokenRequest API (k8s 1.24+ supported) and
// wraps it in a kubeconfig YAML the burst's host-mode cilium-agent
// can use directly. Returns nil if the cilium SA doesn't exist
// (customer cluster has no Cilium installed) or the agent lacks RBAC
// — neither case should fail the bootstrap of a non-Cilium-tier
// burst.
func (s *BootstrapServer) issueCiliumKubeconfig(ctx context.Context) ([]byte, error) {
	const sa = "cilium"
	const ns = "kube-system"
	if s.K8s == nil {
		return nil, fmt.Errorf("k8s client unavailable")
	}
	// This cluster-scoped cilium SA token is written to disk on a VM running
	// the customer's image as root, so scope its lifetime to the burst's, not
	// Cilium's 24h default. 2h safely covers the 90m max burst lifetime
	// (YSCALE_BURST_MAX_LIFETIME) plus boot/margin while shrinking the window a
	// leaked token is usable. (Real fix: prefer the DaemonSet cilium path so the
	// SA token is never handed to the burst — see JoinMode notes.)
	expSec := int64(2 * 60 * 60)
	tr, err := s.K8s.CoreV1().ServiceAccounts(ns).CreateToken(ctx, sa,
		&authenticationv1.TokenRequest{
			Spec: authenticationv1.TokenRequestSpec{
				ExpirationSeconds: &expSec,
				Audiences:         []string{"https://kubernetes.default.svc"},
			},
		}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("token request: %w", err)
	}
	if tr.Status.Token == "" {
		return nil, fmt.Errorf("empty token in TokenRequest response")
	}
	return buildCiliumKubeconfig(s.APIServer, s.ClusterCA, tr.Status.Token), nil
}

// buildCiliumKubeconfig is the host-mode-cilium-agent equivalent of
// buildBootstrapKubeconfig — but with a bearer token (SA JWT) instead
// of a bootstrap token, and pointing at the cluster's apiserver
// directly. Cilium reads kubeconfig from --kube-config-path.
func buildCiliumKubeconfig(apiServer string, caPEM []byte, token string) []byte {
	caB64 := base64.StdEncoding.EncodeToString(caPEM)
	return []byte(fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: yscale
  cluster:
    server: %s
    certificate-authority-data: %s
users:
- name: cilium
  user:
    token: %s
contexts:
- name: cilium@yscale
  context:
    cluster: yscale
    user: cilium
current-context: cilium@yscale
`, apiServer, caB64, token))
}

// createBootstrapTokenSecret writes the standard
// `bootstrap.kubernetes.io/token` Secret in kube-system that the
// apiserver's bootstrap-token authenticator picks up automatically.
func (s *BootstrapServer) createBootstrapTokenSecret(ctx context.Context, tokenID, tokenSecret, nodeName string) error {
	expiration := time.Now().Add(15 * time.Minute).UTC().Format(time.RFC3339)
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bootstrap-token-" + tokenID,
			Namespace: "kube-system",
		},
		Type: corev1.SecretTypeBootstrapToken,
		StringData: map[string]string{
			"token-id":                       tokenID,
			"token-secret":                   tokenSecret,
			"usage-bootstrap-authentication": "true",
			"usage-bootstrap-signing":        "true",
			// Per-node bind group included so the CSR approver can pin the
			// earned client cert to THIS node (see nodeBindGroupPrefix).
			"auth-extra-groups": bootstrapAuthExtraGroups(nodeName),
			"description":       "yscale burst-node kubelet bootstrap (auto-expires)",
			"expiration":        expiration,
		},
	}
	_, err := s.K8s.CoreV1().Secrets("kube-system").Create(ctx, sec, metav1.CreateOptions{})
	return err
}

// bootstrapAuthExtraGroups returns the comma-separated auth-extra-groups a
// burst's bootstrap token carries: the shared bursts group (coarse gate)
// plus a per-node group (nodeBindGroupPrefix+nodeName) that binds the token
// to exactly this node, enforced by the CSR approver. Both surface in the
// client CSR's Spec.Groups when the kubelet authenticates with the token.
func bootstrapAuthExtraGroups(nodeName string) string {
	return burstsBootstrapGroup + "," + nodeBindGroupPrefix + nodeName
}

// generateBootstrapToken returns (token_id, token_secret) per the
// k8s spec: 6 hex chars + 16 hex chars.
func generateBootstrapToken() (id, secret string, err error) {
	var idBytes [3]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return "", "", err
	}
	var secretBytes [8]byte
	if _, err := rand.Read(secretBytes[:]); err != nil {
		return "", "", err
	}
	return hex.EncodeToString(idBytes[:]), hex.EncodeToString(secretBytes[:]), nil
}

// buildBootstrapKubeconfig produces the YAML the kubelet expects at
// --bootstrap-kubeconfig. Format follows the standard kubeadm shape.
//
// `server` should be a URL whose hostname matches the API server
// cert's SAN list — e.g. the k3s control-plane's tailnet hostname,
// since k3s adds the node name to the cert by default. We don't set
// tls-server-name because kubelet's auto-generated post-bootstrap
// kubeconfig loses it, and the URL hostname must validate strictly
// from then on.
func buildBootstrapKubeconfig(apiServer string, caPEM []byte, token, nodeName string) []byte {
	return []byte(fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: yscale
  cluster:
    server: %s
    certificate-authority-data: %s
contexts:
- name: bootstrap
  context:
    cluster: yscale
    user: kubelet-bootstrap-%s
current-context: bootstrap
users:
- name: kubelet-bootstrap-%s
  user:
    token: %s
`, apiServer, base64.StdEncoding.EncodeToString(caPEM), nodeName, nodeName, token))
}

// gpuTelemetryRequest is the body a burst node posts to /gpu-telemetry.
type gpuTelemetryRequest struct {
	BurstID     string  `json:"burst_id"`
	NodeName    string  `json:"node_name"`
	Utilization float64 `json:"utilization"`
	// Product is the model observed by the root-owned host reporter through
	// nvidia-smi. It is optional for compatibility with already-running nodes.
	Product string `json:"product,omitempty"`
}

const maxGPUTelemetryBody = 1024

func (s *BootstrapServer) handleGPUTelemetry(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxGPUTelemetryBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req gpuTelemetryRequest
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "decode: "+err.Error(), http.StatusBadRequest)
		return
	}
	if dec.More() {
		http.Error(w, "trailing data after JSON object", http.StatusBadRequest)
		return
	}
	if req.BurstID == "" || req.NodeName == "" {
		http.Error(w, "burst_id and node_name required", http.StatusBadRequest)
		return
	}
	if req.NodeName != expectedBurstNodeName(req.BurstID) {
		http.Error(w, "node_name does not match burst id", http.StatusForbidden)
		return
	}

	now := time.Now().UTC()
	sample := protocol.GPUTelemetrySample{
		BurstID:     req.BurstID,
		NodeName:    req.NodeName,
		Utilization: req.Utilization,
		ObservedAt:  now,
	}
	if !protocol.ValidGPUTelemetrySample(sample) {
		http.Error(w, "invalid telemetry sample", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	grant := s.grants[req.BurstID]
	if grant == nil || s.nodes[req.NodeName] != req.BurstID {
		s.mu.Unlock()
		http.Error(w, "unauthorized burst id", http.StatusForbidden)
		return
	}
	if s.gpuSamples == nil {
		s.gpuSamples = make(map[string]protocol.GPUTelemetrySample)
	}
	s.gpuSamples[req.BurstID] = sample
	s.mu.Unlock()

	if product, ok := gpuProductLabelValue(req.Product); ok {
		s.reconcileGPUProductLabel(r.Context(), req.NodeName, product)
	} else if req.Product != "" {
		s.Log.Warn("gpu telemetry: invalid product observation omitted",
			"burst", req.BurstID, "node", req.NodeName)
	}

	w.WriteHeader(http.StatusNoContent)
}

// gpuProductLabelValue mirrors NVIDIA GPU Feature Discovery's conservative
// product-label shape: spaces become dashes and parentheses are removed. Any
// other invalid or overlong Kubernetes label is rejected rather than guessed.
func gpuProductLabelValue(raw string) (string, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", false
	}
	value = strings.NewReplacer(" ", "-", "\t", "-", "(", "", ")", "").Replace(value)
	if err := k8sv1.ValidateLabelValue(value); err != nil || value == "" {
		return "", false
	}
	return value, true
}

// reconcileGPUProductLabel publishes only a live host observation and only
// after Kubernetes reports positive GPU allocatable. An existing value is
// treated as owned by customer-installed GFD and is never overwritten.
// Failures are best-effort: the host reporter retries every 30 seconds.
func (s *BootstrapServer) reconcileGPUProductLabel(parent context.Context, nodeName, product string) {
	if s.K8s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	node, err := s.K8s.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		s.Log.Warn("gpu product reconciliation: get failed", "node", nodeName, "error", err)
		return
	}
	if !gpuAllocatable(node) {
		return
	}
	if current := node.Labels[k8sv1.LabelNvidiaGPUProduct]; current != "" {
		if current != product {
			s.Log.Warn("gpu product reconciliation: preserving existing label",
				"node", nodeName, "observed", product, "existing", current)
		}
		return
	}
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"labels": map[string]string{k8sv1.LabelNvidiaGPUProduct: product}},
	})
	if err != nil {
		return
	}
	if _, err := s.K8s.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		s.Log.Warn("gpu product reconciliation: patch failed", "node", nodeName, "error", err)
		return
	}
	s.Log.Info("reconciled hardware-observed gpu product", "node", nodeName, "product", product)
}

// GPUTelemetrySamples returns a bounded, deterministic round-robin snapshot of
// cached GPU telemetry without clearing it. The heartbeat loop attaches one
// batch to each heartbeat; when more than MaxGPUTelemetrySamples bursts are
// active, the cursor guarantees every burst appears in a later batch instead
// of truncating arbitrary Go map order forever. Samples persist until
// superseded by a newer report or cleared by Forget.
func (s *BootstrapServer) GPUTelemetrySamples() []protocol.GPUTelemetrySample {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.gpuSamples) == 0 {
		s.gpuSampleCursor = ""
		return nil
	}
	ids := make([]string, 0, len(s.gpuSamples))
	for id := range s.gpuSamples {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	start := sort.Search(len(ids), func(i int) bool { return ids[i] > s.gpuSampleCursor })
	if start == len(ids) {
		start = 0
	}
	count := min(len(ids), protocol.MaxGPUTelemetrySamples)
	out := make([]protocol.GPUTelemetrySample, 0, count)
	for i := 0; i < count; i++ {
		id := ids[(start+i)%len(ids)]
		out = append(out, s.gpuSamples[id])
		s.gpuSampleCursor = id
	}
	return out
}
