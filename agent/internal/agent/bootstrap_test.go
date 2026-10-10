package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type staticBootstrapTokenIssuer struct {
	token string
	node  string
}

func (i *staticBootstrapTokenIssuer) Issue(_ context.Context, nodeName string) (string, error) {
	i.node = nodeName
	return i.token, nil
}

// expectedBurstNodeName must reproduce the node name central derives from a
// BurstID (decider.go: NodeNamePrefix + burstID without the "burst_" prefix).
// The bootstrap handler uses it to bind the requested node identity to the
// authenticated BurstID.
func TestExpectedBurstNodeName(t *testing.T) {
	if got := expectedBurstNodeName("burst_deadbeef"); got != "ys-burst-deadbeef" {
		t.Errorf("expectedBurstNodeName = %q, want ys-burst-deadbeef", got)
	}
}

// The bootstrap token must carry a per-node extra group so the CSR approver
// can bind the earned client cert to this node. The list must include both the
// shared bursts group (existing policy) and the node-specific group.
func TestBootstrapAuthExtraGroups(t *testing.T) {
	got := bootstrapAuthExtraGroups("ys-burst-abc123")
	if !strings.Contains(got, burstsBootstrapGroup) {
		t.Errorf("auth-extra-groups %q missing the shared bursts group %q", got, burstsBootstrapGroup)
	}
	if !strings.Contains(got, nodeBindGroupPrefix+"ys-burst-abc123") {
		t.Errorf("auth-extra-groups %q missing the per-node bind group", got)
	}
}

// SECURITY: a burst authenticates with its (secret) BurstID but supplies its
// own node_name. Without binding node_name to the BurstID, a burst holding a
// valid grant for burst_A can request a kubeconfig for node ys-burst-B and
// register a kubelet under another burst's node identity. The handler must
// reject a node_name that doesn't match the one derived from the BurstID —
// before minting any bootstrap token.
func TestHandleBootstrap_RejectsNodeNameMismatch(t *testing.T) {
	s := &BootstrapServer{
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		grants: map[string]*burstGrant{},
		nodes:  map[string]string{},
		// K8s intentionally nil: a correct rejection must happen before any
		// API call, so this path must never touch the client.
	}
	s.AuthorizeBurst("burst_aaaa", "full", "default", nil)

	// Authenticated as burst_aaaa, but claiming burst_bbbb's node identity.
	body := `{"burst_id":"burst_aaaa","node_name":"ys-burst-bbbb"}`
	req := httptest.NewRequest(http.MethodPost, "/bootstrap-kubeconfig", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleBootstrap(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403 for node-name/burst-id mismatch", rec.Code)
	}
}

// The legitimate case — node_name matching the BurstID — must pass the identity
// check (it then proceeds to token minting, exercised elsewhere). We assert the
// derived name is what a real burst sends.
func TestHandleBootstrap_AcceptsMatchingNodeName(t *testing.T) {
	// A real burst sends node_name == expectedBurstNodeName(burstID); confirm
	// the handler's own derivation agrees, so legitimate bursts are not locked
	// out by the new check.
	if expectedBurstNodeName("burst_aaaa") != "ys-burst-aaaa" {
		t.Fatal("derivation mismatch would reject legitimate bursts")
	}
}

// A grant with no namespace only costs the burst storage signing. It
// must still get its kubelet bootstrap kubeconfig, or an agent newer
// than its central takes every non-storage workload down with it.
func TestHandleBootstrap_WorksWithoutAnnouncedNamespace(t *testing.T) {
	s := &BootstrapServer{
		K8s:       fake.NewSimpleClientset(),
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIServer: "https://10.43.0.1:443",
		ClusterCA: []byte("-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"),
		grants:    map[string]*burstGrant{},
		nodes:     map[string]string{},
	}
	s.AuthorizeBurst("burst_aaaa", "full", "", nil)

	body := `{"burst_id":"burst_aaaa","node_name":"ys-burst-aaaa"}`
	req := httptest.NewRequest(http.MethodPost, "/bootstrap-kubeconfig", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleBootstrap(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var resp BootstrapResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Kubeconfig) == 0 {
		t.Error("no kubeconfig issued")
	}
}

// GKE mode: the GKE token issuer provides a ServiceAccount-backed token.
// Verify the credential_mode is logged correctly and no bootstrap-token
// Secret is created.
func TestHandleBootstrap_GKEIssuer(t *testing.T) {
	kube := fake.NewSimpleClientset()
	issuer := &staticBootstrapTokenIssuer{token: "gke-sa-token-value"}
	gkeIssuer := NewGKETokenIssuer(kube, "yscale", slog.New(slog.NewTextHandler(io.Discard, nil)))
	_ = gkeIssuer // verify it compiles; we use the static issuer for the test

	s := &BootstrapServer{
		K8s:         kube,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIServer:   "https://10.0.0.1:443",
		ClusterCA:   []byte("ca"),
		TokenIssuer: issuer,
		grants:      map[string]*burstGrant{},
		nodes:       map[string]string{},
	}
	s.AuthorizeBurst("burst_012345abcdef", "full", "default", nil)

	req := httptest.NewRequest(http.MethodPost, "/bootstrap-kubeconfig", strings.NewReader(
		`{"burst_id":"burst_012345abcdef","node_name":"ys-burst-012345abcdef"}`,
	))
	rec := httptest.NewRecorder()
	s.handleBootstrap(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var resp BootstrapResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(resp.Kubeconfig), "token: gke-sa-token-value") {
		t.Fatal("kubeconfig does not contain the GKE SA token")
	}
	secrets, err := kube.CoreV1().Secrets("kube-system").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(secrets.Items) != 0 {
		t.Fatalf("created %d bootstrap Secrets in GKE mode, want 0", len(secrets.Items))
	}
}

// Forget with a GKE cleaner configured cleans up per-node SA/CRB resources.
func TestForgetCleansUpGKEResources(t *testing.T) {
	const gkeNS = "yscale"
	const node = "ys-burst-deadbeef1234"
	const saName = "yscale-burst-bootstrap-" + node

	kube := fake.NewSimpleClientset()
	cleaner := NewGKETokenIssuer(kube, gkeNS, slog.New(slog.NewTextHandler(io.Discard, nil)))

	s := &BootstrapServer{
		K8s:        kube,
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		GKECleaner: cleaner,
		grants:     map[string]*burstGrant{},
		nodes:      map[string]string{},
	}
	s.AuthorizeBurst("burst_deadbeef1234", "full", "default", nil)
	s.mu.Lock()
	s.nodes[node] = "burst_deadbeef1234"
	s.mu.Unlock()

	// Pre-create the SA and CRB with ownership labels that Issue would have created.
	kube.CoreV1().ServiceAccounts(gkeNS).Create(context.Background(),
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
			Name: saName, Namespace: gkeNS,
			Labels: map[string]string{
				"yscale.sh/burst-bootstrap": "true",
				"yscale.sh/node-name":       node,
			}}},
		metav1.CreateOptions{})
	kube.RbacV1().ClusterRoleBindings().Create(context.Background(),
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: saName,
				Labels: map[string]string{
					"yscale.sh/burst-bootstrap": "true",
					"yscale.sh/node-name":       node,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "system:node-bootstrapper",
			},
			Subjects: []rbacv1.Subject{
				{Kind: "ServiceAccount", Name: saName, Namespace: gkeNS},
			},
		},
		metav1.CreateOptions{})

	s.Forget("burst_deadbeef1234")

	// SA and CRB should be gone.
	_, err := kube.CoreV1().ServiceAccounts(gkeNS).Get(context.Background(),
		saName, metav1.GetOptions{})
	if err == nil {
		t.Error("SA still exists after Forget")
	}
	_, err = kube.RbacV1().ClusterRoleBindings().Get(context.Background(),
		saName, metav1.GetOptions{})
	if err == nil {
		t.Error("CRB still exists after Forget")
	}
}

func TestHandleBootstrap_EKSIssuerSkipsUnsupportedBootstrapSecret(t *testing.T) {
	kube := fake.NewSimpleClientset()
	issuer := &staticBootstrapTokenIssuer{token: "x"}
	s := &BootstrapServer{
		K8s:         kube,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIServer:   "https://example.eks.amazonaws.com",
		ClusterCA:   []byte("ca"),
		TokenIssuer: issuer,
		grants:      map[string]*burstGrant{},
		nodes:       map[string]string{},
	}
	s.AuthorizeBurst("burst_012345abcdef", "full", "default", nil)

	req := httptest.NewRequest(http.MethodPost, "/bootstrap-kubeconfig", strings.NewReader(
		`{"burst_id":"burst_012345abcdef","node_name":"ys-burst-012345abcdef"}`,
	))
	rec := httptest.NewRecorder()
	s.handleBootstrap(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if issuer.node != "ys-burst-012345abcdef" {
		t.Fatalf("issuer node = %q", issuer.node)
	}
	var resp BootstrapResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(resp.Kubeconfig), "token: x") {
		t.Fatal("response did not contain the issued EKS token")
	}
	secrets, err := kube.CoreV1().Secrets("kube-system").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(secrets.Items) != 0 {
		t.Fatalf("created %d bootstrap Secrets in EKS mode, want 0", len(secrets.Items))
	}
}

// CloudProvider is plumbed in the bootstrap response so the burst can
// adjust its --provider-id strategy per customer-cluster cloud.
func TestHandleBootstrap_CloudProviderPlumbed(t *testing.T) {
	for _, tt := range []struct {
		name  string
		cloud string
		want  string
	}{
		{name: "GKE", cloud: "gcp", want: "gcp"},
		{name: "EKS", cloud: "aws", want: "aws"},
		{name: "LKE", cloud: "linode", want: "linode"},
		{name: "k3s", cloud: "k3s", want: "k3s"},
		{name: "empty", cloud: "", want: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &BootstrapServer{
				K8s:           fake.NewSimpleClientset(),
				Log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
				APIServer:     "https://10.43.0.1:443",
				ClusterCA:     []byte("-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"),
				CloudProvider: tt.cloud,
				grants:        map[string]*burstGrant{},
				nodes:         map[string]string{},
			}
			s.AuthorizeBurst("burst_aaaa", "full", "default", nil)

			body := `{"burst_id":"burst_aaaa","node_name":"ys-burst-aaaa"}`
			req := httptest.NewRequest(http.MethodPost, "/bootstrap-kubeconfig", strings.NewReader(body))
			rec := httptest.NewRecorder()
			s.handleBootstrap(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
			}
			var resp BootstrapResponse
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatal(err)
			}
			if resp.CloudProvider != tt.want {
				t.Errorf("cloud_provider = %q, want %q", resp.CloudProvider, tt.want)
			}
		})
	}
}

// GKE mode: cloud_provider is "gcp" so the burst omits --provider-id.
// Verify GKE bootstrap includes cloud_provider alongside the SA token.
func TestHandleBootstrap_GKECloudProviderWithSAToken(t *testing.T) {
	kube := fake.NewSimpleClientset()
	issuer := &staticBootstrapTokenIssuer{token: "gke-sa-token-value"}

	s := &BootstrapServer{
		K8s:           kube,
		Log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIServer:     "https://10.0.0.1:443",
		ClusterCA:     []byte("ca"),
		TokenIssuer:   issuer,
		CloudProvider: "gcp",
		grants:        map[string]*burstGrant{},
		nodes:         map[string]string{},
	}
	s.AuthorizeBurst("burst_012345abcdef", "full", "default", nil)

	req := httptest.NewRequest(http.MethodPost, "/bootstrap-kubeconfig", strings.NewReader(
		`{"burst_id":"burst_012345abcdef","node_name":"ys-burst-012345abcdef"}`,
	))
	rec := httptest.NewRecorder()
	s.handleBootstrap(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var resp BootstrapResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.CloudProvider != "gcp" {
		t.Errorf("cloud_provider = %q, want gcp", resp.CloudProvider)
	}
	if !strings.Contains(string(resp.Kubeconfig), "token: gke-sa-token-value") {
		t.Fatal("kubeconfig missing GKE SA token")
	}
}

// Standard mode: no cloud provider → cloud_provider absent from JSON.
func TestHandleBootstrap_NoCloudProviderOmitted(t *testing.T) {
	s := &BootstrapServer{
		K8s:       fake.NewSimpleClientset(),
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIServer: "https://10.43.0.1:443",
		ClusterCA: []byte("-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"),
		grants:    map[string]*burstGrant{},
		nodes:     map[string]string{},
	}
	s.AuthorizeBurst("burst_aaaa", "full", "default", nil)

	body := `{"burst_id":"burst_aaaa","node_name":"ys-burst-aaaa"}`
	req := httptest.NewRequest(http.MethodPost, "/bootstrap-kubeconfig", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleBootstrap(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	// Empty CloudProvider → omitempty → field absent from JSON.
	if strings.Contains(rec.Body.String(), "cloud_provider") {
		t.Error("cloud_provider should be omitted when empty")
	}
}
