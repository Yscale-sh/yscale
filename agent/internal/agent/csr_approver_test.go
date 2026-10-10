package agent

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"testing"

	certv1 "k8s.io/api/certificates/v1"
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

	approver := &CSRApprover{Group: yscaleGroup, NodePrefix: nodePrefix}

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
			if got := approver.eligible(c.csr); got != c.want {
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
			if got := approver.eligible(c.csr); got != c.want {
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
	if approver.eligible(csr) {
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
