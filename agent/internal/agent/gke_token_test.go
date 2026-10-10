package agent

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const gkeTestNamespace = "yscale-system"

func TestGKESAName_ValidBurstNode(t *testing.T) {
	name, err := gkeSAName("ys-burst-deadbeef1234")
	if err != nil {
		t.Fatal(err)
	}
	if want := "yscale-burst-bootstrap-ys-burst-deadbeef1234"; name != want {
		t.Fatalf("gkeSAName = %q, want %q", name, want)
	}
}

func TestGKESAName_RejectsNonBurstNode(t *testing.T) {
	for _, name := range []string{
		"worker-1",
		"ys-burst-",
		"ys-burst-SHORT",
		"ys-burst-ZZZZZZZZZZZZ",
		"",
	} {
		if _, err := gkeSAName(name); err == nil {
			t.Errorf("gkeSAName(%q) accepted non-burst node name", name)
		}
	}
}

func TestGKEExpectedUsername(t *testing.T) {
	got := gkeExpectedUsername("ys-burst-deadbeef1234", gkeTestNamespace)
	want := "system:serviceaccount:" + gkeTestNamespace + ":yscale-burst-bootstrap-ys-burst-deadbeef1234"
	if got != want {
		t.Fatalf("gkeExpectedUsername = %q, want %q", got, want)
	}
}

func TestGKEExpectedUsername_InvalidNodeReturnsEmpty(t *testing.T) {
	if got := gkeExpectedUsername("not-a-burst-node", gkeTestNamespace); got != "" {
		t.Fatalf("gkeExpectedUsername for invalid node = %q, want empty", got)
	}
}

func TestGKEUsernameMatchesNode(t *testing.T) {
	node := "ys-burst-deadbeef1234"
	username := gkeExpectedUsername(node, gkeTestNamespace)
	if !gkeUsernameMatchesNode(username, node, gkeTestNamespace) {
		t.Fatal("gkeUsernameMatchesNode returned false for matching pair")
	}
}

func TestGKEUsernameMatchesNode_RejectsMismatch(t *testing.T) {
	username := gkeExpectedUsername("ys-burst-deadbeef1234", gkeTestNamespace)
	if gkeUsernameMatchesNode(username, "ys-burst-aabbccddee00", gkeTestNamespace) {
		t.Fatal("gkeUsernameMatchesNode accepted mismatched node")
	}
}

func TestGKEUsernameMatchesNode_RejectsWrongNamespace(t *testing.T) {
	node := "ys-burst-deadbeef1234"
	username := gkeExpectedUsername(node, gkeTestNamespace)
	if gkeUsernameMatchesNode(username, node, "kube-system") {
		t.Fatal("gkeUsernameMatchesNode accepted wrong namespace")
	}
}

func TestIsGKEBootstrapUsername(t *testing.T) {
	for _, tt := range []struct {
		name     string
		username string
		ns       string
		want     bool
	}{
		{"valid GKE SA username", "system:serviceaccount:" + gkeTestNamespace + ":yscale-burst-bootstrap-ys-burst-deadbeef1234", gkeTestNamespace, true},
		{"standard bootstrap token username", "system:bootstrap:005180", gkeTestNamespace, false},
		{"random node username", "system:node:worker-1", gkeTestNamespace, false},
		{"empty", "", gkeTestNamespace, false},
		{"wrong namespace", "system:serviceaccount:default:yscale-burst-bootstrap-ys-burst-deadbeef1234", gkeTestNamespace, false},
		{"kube-system namespace mismatch", "system:serviceaccount:kube-system:yscale-burst-bootstrap-ys-burst-deadbeef1234", gkeTestNamespace, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := isGKEBootstrapUsername(tt.username, tt.ns); got != tt.want {
				t.Errorf("isGKEBootstrapUsername(%q, %q) = %v, want %v", tt.username, tt.ns, got, tt.want)
			}
		})
	}
}

func TestGKETokenIssuer_IssueCreatesResources(t *testing.T) {
	kube := fake.NewSimpleClientset()
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// fake clientset does not implement TokenRequest, so Issue will fail at
	// the token mint step. We verify the SA and CRB were created.
	_, _ = issuer.Issue(context.Background(), "ys-burst-deadbeef1234")

	saName := "yscale-burst-bootstrap-ys-burst-deadbeef1234"
	sa, err := kube.CoreV1().ServiceAccounts(gkeTestNamespace).Get(context.Background(), saName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("SA not created: %v", err)
	}
	if sa.Labels[gkeOwnerLabel] != "true" {
		t.Error("SA missing burst-bootstrap label")
	}
	if sa.Labels[gkeNodeNameLabel] != "ys-burst-deadbeef1234" {
		t.Error("SA missing node-name label")
	}

	crbName := "yscale-burst-bootstrap-ys-burst-deadbeef1234"
	crb, err := kube.RbacV1().ClusterRoleBindings().Get(context.Background(), crbName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("CRB not created: %v", err)
	}
	if crb.RoleRef.Name != "system:node-bootstrapper" {
		t.Errorf("CRB roleRef = %q, want system:node-bootstrapper", crb.RoleRef.Name)
	}
	if len(crb.Subjects) != 1 || crb.Subjects[0].Name != saName || crb.Subjects[0].Namespace != gkeTestNamespace {
		t.Errorf("CRB subject = %+v, want exactly the per-node SA in %s", crb.Subjects, gkeTestNamespace)
	}
}

func TestGKETokenIssuer_IssueIsIdempotent(t *testing.T) {
	kube := fake.NewSimpleClientset()
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))
	node := "ys-burst-deadbeef1234"

	// First call creates resources.
	_, _ = issuer.Issue(context.Background(), node)

	// Second call (retry) must not fail on AlreadyExists — ownership matches.
	_, _ = issuer.Issue(context.Background(), node)

	// Verify only one SA exists.
	sas, err := kube.CoreV1().ServiceAccounts(gkeTestNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, sa := range sas.Items {
		if sa.Name == "yscale-burst-bootstrap-"+node {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("got %d SAs, want exactly 1 after retry", count)
	}
}

func TestGKETokenIssuer_IssueRejectsInvalidNode(t *testing.T) {
	kube := fake.NewSimpleClientset()
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, err := issuer.Issue(context.Background(), "not-a-burst-node")
	if err == nil {
		t.Fatal("Issue accepted invalid node name")
	}
}

// Collision fail-closed: SA exists but is NOT owned by this agent.
func TestGKETokenIssuer_IssueFailsClosedOnSACollision(t *testing.T) {
	node := "ys-burst-deadbeef1234"
	saName := "yscale-burst-bootstrap-" + node

	// Pre-create an SA with wrong labels (simulates a collision).
	kube := fake.NewSimpleClientset(
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      saName,
				Namespace: gkeTestNamespace,
				Labels:    map[string]string{"some-other-owner": "true"},
			},
		},
	)
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, err := issuer.Issue(context.Background(), node)
	if err == nil {
		t.Fatal("Issue should fail closed on SA collision with wrong labels")
	}
	if got := err.Error(); !strings.Contains(got, "not owned by this agent") {
		t.Fatalf("error = %q, want 'not owned by this agent'", got)
	}
}

// Collision fail-closed: SA exists with right labels but CRB has wrong roleRef.
func TestGKETokenIssuer_IssueFailsClosedOnCRBCollision(t *testing.T) {
	node := "ys-burst-deadbeef1234"
	saName := "yscale-burst-bootstrap-" + node
	crbName := "yscale-burst-bootstrap-" + node

	kube := fake.NewSimpleClientset(
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      saName,
				Namespace: gkeTestNamespace,
				Labels: map[string]string{
					gkeOwnerLabel:    "true",
					gkeNodeNameLabel: node,
				},
			},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: crbName,
				Labels: map[string]string{
					gkeOwnerLabel:    "true",
					gkeNodeNameLabel: node,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "cluster-admin", // wrong roleRef
			},
			Subjects: []rbacv1.Subject{
				{Kind: "ServiceAccount", Name: saName, Namespace: gkeTestNamespace},
			},
		},
	)
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, err := issuer.Issue(context.Background(), node)
	if err == nil {
		t.Fatal("Issue should fail closed on CRB collision with wrong roleRef")
	}
	if got := err.Error(); !strings.Contains(got, "unexpected owner/roleRef/subject") {
		t.Fatalf("error = %q, want 'unexpected owner/roleRef/subject'", got)
	}
}

// Collision fail-closed: CRB exists with extra subjects.
func TestGKETokenIssuer_IssueFailsClosedOnCRBExtraSubjects(t *testing.T) {
	node := "ys-burst-deadbeef1234"
	saName := "yscale-burst-bootstrap-" + node
	crbName := "yscale-burst-bootstrap-" + node

	kube := fake.NewSimpleClientset(
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      saName,
				Namespace: gkeTestNamespace,
				Labels: map[string]string{
					gkeOwnerLabel:    "true",
					gkeNodeNameLabel: node,
				},
			},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: crbName,
				Labels: map[string]string{
					gkeOwnerLabel:    "true",
					gkeNodeNameLabel: node,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "system:node-bootstrapper",
			},
			Subjects: []rbacv1.Subject{
				{Kind: "ServiceAccount", Name: saName, Namespace: gkeTestNamespace},
				{Kind: "ServiceAccount", Name: "intruder", Namespace: "kube-system"},
			},
		},
	)
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, err := issuer.Issue(context.Background(), node)
	if err == nil {
		t.Fatal("Issue should fail closed on CRB with extra subjects")
	}
}

func TestGKETokenIssuer_CleanupRemovesOwnedResources(t *testing.T) {
	node := "ys-burst-deadbeef1234"
	saName := "yscale-burst-bootstrap-" + node
	crbName := "yscale-burst-bootstrap-" + node

	kube := fake.NewSimpleClientset(
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      saName,
				Namespace: gkeTestNamespace,
				Labels: map[string]string{
					gkeOwnerLabel:    "true",
					gkeNodeNameLabel: node,
				},
			},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: crbName,
				Labels: map[string]string{
					gkeOwnerLabel:    "true",
					gkeNodeNameLabel: node,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "system:node-bootstrapper",
			},
			Subjects: []rbacv1.Subject{
				{Kind: "ServiceAccount", Name: saName, Namespace: gkeTestNamespace},
			},
		},
	)
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))

	issuer.CleanupNode(context.Background(), node)

	_, err := kube.CoreV1().ServiceAccounts(gkeTestNamespace).Get(context.Background(), saName, metav1.GetOptions{})
	if err == nil {
		t.Error("SA still exists after cleanup")
	}
	_, err = kube.RbacV1().ClusterRoleBindings().Get(context.Background(), crbName, metav1.GetOptions{})
	if err == nil {
		t.Error("CRB still exists after cleanup")
	}
}

// Cleanup must refuse to delete resources not owned by this agent.
func TestGKETokenIssuer_CleanupRefusesToDeleteUnownedSA(t *testing.T) {
	node := "ys-burst-deadbeef1234"
	saName := "yscale-burst-bootstrap-" + node

	kube := fake.NewSimpleClientset(
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      saName,
				Namespace: gkeTestNamespace,
				Labels:    map[string]string{"unrelated-owner": "true"},
			},
		},
	)
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))

	issuer.CleanupNode(context.Background(), node)

	// SA must still exist — cleanup refused to delete it.
	_, err := kube.CoreV1().ServiceAccounts(gkeTestNamespace).Get(context.Background(), saName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unowned SA was deleted: %v", err)
	}
}

func TestGKETokenIssuer_CleanupRefusesToDeleteUnownedCRB(t *testing.T) {
	node := "ys-burst-deadbeef1234"
	saName := "yscale-burst-bootstrap-" + node
	crbName := "yscale-burst-bootstrap-" + node

	kube := fake.NewSimpleClientset(
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: crbName,
				Labels: map[string]string{
					gkeOwnerLabel:    "true",
					gkeNodeNameLabel: node,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "cluster-admin", // wrong roleRef — not ours
			},
			Subjects: []rbacv1.Subject{
				{Kind: "ServiceAccount", Name: saName, Namespace: gkeTestNamespace},
			},
		},
	)
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))

	issuer.CleanupNode(context.Background(), node)

	// CRB must still exist — cleanup refused to delete it.
	_, err := kube.RbacV1().ClusterRoleBindings().Get(context.Background(), crbName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unowned CRB was deleted: %v", err)
	}
}

func TestGKETokenIssuer_CleanupIdempotent(t *testing.T) {
	kube := fake.NewSimpleClientset()
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// Cleanup on a node that was never issued should not panic or error.
	issuer.CleanupNode(context.Background(), "ys-burst-deadbeef1234")
}

func TestGKETokenIssuer_CleanupIgnoresInvalidNode(t *testing.T) {
	kube := fake.NewSimpleClientset()
	issuer := NewGKETokenIssuer(kube, gkeTestNamespace, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// Should silently return, not panic.
	issuer.CleanupNode(context.Background(), "not-a-burst-node")
}
