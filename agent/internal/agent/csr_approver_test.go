package agent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"net"
	"net/url"
	"testing"

	certv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// TestEligibilityPolicy pins the only policy decision the auto-
// approver makes. Wrong on this and we either rubber-stamp arbitrary
// CSRs (security blast) or refuse to approve anything (every burst
// node sits in NotReady forever).
func TestEligibilityPolicy(t *testing.T) {
	const (
		yscaleGroup = "system:bootstrappers:yscale:bursts"
		nodePrefix  = "system:node:ys-burst-"
	)
	// nodeGroup is the per-node bootstrap group minted into the burst's
	// token (auth-extra-groups), which surfaces in csr.Spec.Groups. The
	// client-CSR policy requires it to match the requested node CN so a
	// token can only ever earn a client cert for its OWN node.
	const nodeGroup = "system:bootstrappers:yscale:nodes:ys-burst-abc123"
	makeCSR := func(cn, username, group, signer string, extraGroups ...string) *certv1.CertificateSigningRequest {
		groups := append([]string{"system:authenticated", group}, extraGroups...)
		return &certv1.CertificateSigningRequest{
			Spec: certv1.CertificateSigningRequestSpec{
				Request:    mustCSRPEM(t, cn),
				Username:   username,
				Groups:     groups,
				SignerName: signer,
			},
		}
	}

	approver := &CSRApprover{Group: yscaleGroup, NodePrefix: nodePrefix, K8s: fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ys-burst-abc123"}})}

	cases := []struct {
		name string
		csr  *certv1.CertificateSigningRequest
		want bool
	}{
		{
			name: "yscale burst client cert with matching node-bind group — approve",
			csr:  makeCSR("system:node:ys-burst-abc123", "system:bootstrap:005180", yscaleGroup, certv1.KubeAPIServerClientKubeletSignerName, nodeGroup),
			want: true,
		},
		{
			name: "client cert without node-bind group — refuse (un-bound token can register as any burst node)",
			csr:  makeCSR("system:node:ys-burst-abc123", "system:bootstrap:005180", yscaleGroup, certv1.KubeAPIServerClientKubeletSignerName),
			want: false,
		},
		{
			name: "client cert with mismatched node-bind group — refuse (burst impersonating another node)",
			csr:  makeCSR("system:node:ys-burst-abc123", "system:bootstrap:005180", yscaleGroup, certv1.KubeAPIServerClientKubeletSignerName, "system:bootstrappers:yscale:nodes:ys-burst-xyz999"),
			want: false,
		},
		{
			name: "yscale burst serving cert (system:nodes group) — approve",
			csr:  makeCSR("system:node:ys-burst-abc123", "system:node:ys-burst-abc123", "system:nodes", certv1.KubeletServingSignerName),
			want: true,
		},
		{
			name: "serving cert in bootstrap group — refuse (real serving CSRs come from system:nodes)",
			csr:  makeCSR("system:node:ys-burst-abc123", "system:node:ys-burst-abc123", yscaleGroup, certv1.KubeletServingSignerName),
			want: false,
		},
		{
			name: "serving cert with username != CN — refuse (node requesting a cert for another identity)",
			csr:  makeCSR("system:node:ys-burst-abc123", "system:node:ys-burst-xyz999", "system:nodes", certv1.KubeletServingSignerName),
			want: false,
		},
		{
			name: "client cert in system:nodes group — refuse (bootstrap CSR must be in the bursts group)",
			csr:  makeCSR("system:node:ys-burst-abc123", "system:node:ys-burst-abc123", "system:nodes", certv1.KubeAPIServerClientKubeletSignerName),
			want: false,
		},
		{
			name: "wrong group — refuse",
			csr:  makeCSR("system:node:ys-burst-abc123", "system:bootstrap:005180", "system:bootstrappers:kubeadm:default-node-token", certv1.KubeAPIServerClientKubeletSignerName),
			want: false,
		},
		{
			name: "wrong CN prefix — refuse (someone trying to impersonate a non-burst node)",
			csr:  makeCSR("system:node:worker-1", "system:bootstrap:005180", yscaleGroup, certv1.KubeAPIServerClientKubeletSignerName),
			want: false,
		},
		{
			name: "non-kubelet signer — refuse",
			csr:  makeCSR("system:node:ys-burst-abc123", "system:bootstrap:005180", yscaleGroup, "kubernetes.io/legacy-unknown"),
			want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := approver.eligible(context.Background(), c.csr); got != c.want {
				t.Errorf("eligible(%s) = %v, want %v", c.name, got, c.want)
			}
		})
	}
}

// GKE bootstrap mode: the kubelet authenticates as a per-node ServiceAccount
// instead of a bootstrap token. The CSR approver must accept these when the
// username is deterministically bound to the exact node CN, and reject them
// when the username doesn't match.
func TestEligibilityPolicy_GKEMode(t *testing.T) {
	const nodePrefix = "system:node:ys-burst-"
	const gkeNS = "yscale-system"

	gkeUsername := func(node string) string {
		return gkeExpectedUsername(node, gkeNS)
	}

	makeCSR := func(cn, username string, groups []string, signer string) *certv1.CertificateSigningRequest {
		return &certv1.CertificateSigningRequest{
			Spec: certv1.CertificateSigningRequestSpec{
				Request:    mustCSRPEM(t, cn),
				Username:   username,
				Groups:     groups,
				SignerName: signer,
			},
		}
	}

	approver := &CSRApprover{Group: burstsBootstrapGroup, NodePrefix: nodePrefix, GKEMode: true, GKENamespace: gkeNS}

	cases := []struct {
		name string
		csr  *certv1.CertificateSigningRequest
		want bool
	}{
		{
			name: "GKE SA username matching node CN — approve",
			csr: makeCSR("system:node:ys-burst-deadbeef1234",
				gkeUsername("ys-burst-deadbeef1234"),
				[]string{"system:authenticated", "system:serviceaccounts", "system:serviceaccounts:" + gkeNS},
				certv1.KubeAPIServerClientKubeletSignerName),
			want: true,
		},
		{
			name: "GKE SA username for different node — refuse",
			csr: makeCSR("system:node:ys-burst-deadbeef1234",
				gkeUsername("ys-burst-aabbccddee00"),
				[]string{"system:authenticated", "system:serviceaccounts", "system:serviceaccounts:" + gkeNS},
				certv1.KubeAPIServerClientKubeletSignerName),
			want: false,
		},
		{
			name: "GKE SA username for non-burst CN — refuse",
			csr: makeCSR("system:node:worker-1",
				gkeUsername("ys-burst-deadbeef1234"),
				[]string{"system:authenticated", "system:serviceaccounts"},
				certv1.KubeAPIServerClientKubeletSignerName),
			want: false,
		},
		{
			name: "GKE SA username with serving signer — follow existing serving policy",
			csr: makeCSR("system:node:ys-burst-deadbeef1234",
				gkeUsername("ys-burst-deadbeef1234"),
				[]string{"system:nodes"},
				certv1.KubeletServingSignerName),
			want: false, // serving CSR requires username == CN, not SA username
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := approver.eligible(context.Background(), c.csr); got != c.want {
				t.Errorf("eligible(%s) = %v, want %v", c.name, got, c.want)
			}
		})
	}
}

// Default mode (GKEMode=false) must NOT approve GKE-style SA usernames.
// Without this guard, a SA with the right prefix in any namespace could
// get a client cert approved through the GKE path even on non-GKE clusters.
func TestEligibilityPolicy_DefaultModeRejectsGKEUsernames(t *testing.T) {
	const nodePrefix = "system:node:ys-burst-"
	const gkeNS = "yscale-system"

	approver := &CSRApprover{Group: burstsBootstrapGroup, NodePrefix: nodePrefix}
	// GKEMode defaults to false

	csr := &certv1.CertificateSigningRequest{
		Spec: certv1.CertificateSigningRequestSpec{
			Request:    mustCSRPEM(t, "system:node:ys-burst-deadbeef1234"),
			Username:   gkeExpectedUsername("ys-burst-deadbeef1234", gkeNS),
			Groups:     []string{"system:authenticated", "system:serviceaccounts", "system:serviceaccounts:" + gkeNS},
			SignerName: certv1.KubeAPIServerClientKubeletSignerName,
		},
	}
	if approver.eligible(context.Background(), csr) {
		t.Error("default mode (GKEMode=false) should not approve GKE-style SA usernames")
	}
}

func TestAlreadyDecided(t *testing.T) {
	if alreadyDecided(&certv1.CertificateSigningRequest{}) {
		t.Error("empty CSR should not be decided")
	}
	approved := &certv1.CertificateSigningRequest{}
	approved.Status.Conditions = []certv1.CertificateSigningRequestCondition{
		{Type: certv1.CertificateApproved},
	}
	if !alreadyDecided(approved) {
		t.Error("approved CSR should be decided")
	}
}

func mustCSRPEM(t *testing.T, cn string) []byte {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: cn},
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, tmpl, priv)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
}

func TestServingCSRNodeSANs(t *testing.T) {
	const nodeName = "ys-burst-abc123"
	for _, tc := range []struct {
		name        string
		dns         []string
		ips         []net.IP
		emails      []string
		uris        []*url.URL
		missingNode bool
		want        bool
	}{
		{name: "node name", dns: []string{nodeName}, want: true},
		{name: "known addresses", dns: []string{"host.internal", "host.example"}, ips: []net.IP{net.ParseIP("100.64.0.2"), net.ParseIP("203.0.113.2"), net.ParseIP("2001:db8::2")}, want: true},
		{name: "unrelated DNS", dns: []string{nodeName, "kubernetes.default.svc"}},
		{name: "unrelated IP", ips: []net.IP{net.ParseIP("100.64.0.2"), net.ParseIP("10.43.0.1")}},
		{name: "missing node", dns: []string{nodeName}, missingNode: true},
		{name: "email SAN", emails: []string{"admin@example.com"}},
		{name: "URI SAN", uris: []*url.URL{{Scheme: "spiffe", Host: "other-node"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var objects []runtime.Object
			if !tc.missingNode {
				objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
					{Type: corev1.NodeHostName, Address: "host.internal"},
					{Type: corev1.NodeExternalDNS, Address: "host.example"},
					{Type: corev1.NodeInternalIP, Address: "100.64.0.2"},
					{Type: corev1.NodeExternalIP, Address: "203.0.113.2"},
					{Type: corev1.NodeInternalIP, Address: "2001:0db8:0:0:0:0:0:2"},
				}}})
			}
			priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
				Subject: pkix.Name{CommonName: "system:node:" + nodeName}, DNSNames: tc.dns, IPAddresses: tc.ips, EmailAddresses: tc.emails, URIs: tc.uris,
			}, priv)
			if err != nil {
				t.Fatal(err)
			}
			csr := &certv1.CertificateSigningRequest{ObjectMeta: metav1.ObjectMeta{Name: "serving"}, Spec: certv1.CertificateSigningRequestSpec{
				Request:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}),
				SignerName: certv1.KubeletServingSignerName, Username: "system:node:" + nodeName, Groups: []string{nodesGroup},
			}}
			objects = append(objects, csr)
			kube := fake.NewSimpleClientset(objects...)
			approver := NewCSRApprover(kube, nil)
			if err := approver.tick(context.Background()); err != nil {
				t.Fatal(err)
			}
			got, err := kube.CertificatesV1().CertificateSigningRequests().Get(context.Background(), csr.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if approved := alreadyDecided(got); approved != tc.want {
				t.Fatalf("approved = %v, want %v", approved, tc.want)
			}
		})
	}
}
