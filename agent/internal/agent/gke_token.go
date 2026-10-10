package agent

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	gkeBootstrapSAPrefix  = "yscale-burst-bootstrap-"
	gkeTokenExpirationSec = 900 // 15 minutes — same window as bootstrap-token Secrets

	gkeOwnerLabel    = "yscale.sh/burst-bootstrap"
	gkeNodeNameLabel = "yscale.sh/node-name"
)

// GKETokenIssuer creates a unique per-burst ServiceAccount in the connector's
// own namespace and an exact per-node ClusterRoleBinding to
// system:node-bootstrapper, then mints a short-lived token via TokenRequest.
// GKE 1.35+ rejects legacy bootstrap.kubernetes.io/token bearer auth; this
// mode uses SA tokens that GKE's apiserver accepts natively.
//
// Security invariant: the ClusterRoleBinding's subject is the EXACT
// ServiceAccount in the connector's namespace, and the CSR approver verifies
// the requesting username matches
// system:serviceaccount:<namespace>:yscale-burst-bootstrap-<id> which is
// deterministically derived from the burst node CN. No shared SA and no
// sibling-node privilege.
//
// Implements both BootstrapTokenIssuer and GKEBootstrapCleaner.
type GKETokenIssuer struct {
	k8s       kubernetes.Interface
	namespace string
	log       *slog.Logger
}

// NewGKETokenIssuer returns a GKETokenIssuer that places per-burst
// ServiceAccounts in the given namespace (the connector's own release
// namespace, derived from POD_NAMESPACE at runtime).
func NewGKETokenIssuer(k8s kubernetes.Interface, namespace string, log *slog.Logger) *GKETokenIssuer {
	if log == nil {
		log = slog.Default()
	}
	return &GKETokenIssuer{k8s: k8s, namespace: namespace, log: log}
}

// gkeSAName returns the deterministic ServiceAccount name for a burst node.
// The node name is validated to be a canonical burst node name.
func gkeSAName(nodeName string) (string, error) {
	if _, ok := burstIDForNodeName(nodeName); !ok {
		return "", fmt.Errorf("invalid GKE bootstrap node name %q", nodeName)
	}
	return gkeBootstrapSAPrefix + nodeName, nil
}

// gkeCRBName returns the deterministic ClusterRoleBinding name for a burst node.
func gkeCRBName(nodeName string) string {
	return gkeBootstrapSAPrefix + nodeName
}

// gkeExpectedUsername returns the Kubernetes username a kubelet authenticates
// as when using the SA token minted for this node. The CSR approver checks
// this to bind the CSR to exactly the right node.
func gkeExpectedUsername(nodeName, namespace string) string {
	sa, err := gkeSAName(nodeName)
	if err != nil {
		return ""
	}
	return "system:serviceaccount:" + namespace + ":" + sa
}

func (i *GKETokenIssuer) Issue(ctx context.Context, nodeName string) (string, error) {
	saName, err := gkeSAName(nodeName)
	if err != nil {
		return "", err
	}

	// Create or verify the per-node ServiceAccount. Fail closed on collision.
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: i.namespace,
			Labels: map[string]string{
				gkeOwnerLabel:    "true",
				gkeNodeNameLabel: nodeName,
			},
		},
	}
	_, err = i.k8s.CoreV1().ServiceAccounts(i.namespace).Create(ctx, sa, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := i.k8s.CoreV1().ServiceAccounts(i.namespace).Get(ctx, saName, metav1.GetOptions{})
		if getErr != nil {
			return "", fmt.Errorf("verify existing GKE bootstrap SA: %w", getErr)
		}
		if !isOwnedGKEBootstrapSA(existing, nodeName) {
			return "", fmt.Errorf("GKE bootstrap SA %s/%s exists but is not owned by this agent (missing or mismatched labels)", i.namespace, saName)
		}
	} else if err != nil {
		return "", fmt.Errorf("create GKE bootstrap SA: %w", err)
	}

	// Create or verify the per-node ClusterRoleBinding. Fail closed on collision.
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: gkeCRBName(nodeName),
			Labels: map[string]string{
				gkeOwnerLabel:    "true",
				gkeNodeNameLabel: nodeName,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "system:node-bootstrapper",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      saName,
				Namespace: i.namespace,
			},
		},
	}
	_, err = i.k8s.RbacV1().ClusterRoleBindings().Create(ctx, crb, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := i.k8s.RbacV1().ClusterRoleBindings().Get(ctx, gkeCRBName(nodeName), metav1.GetOptions{})
		if getErr != nil {
			return "", fmt.Errorf("verify existing GKE bootstrap CRB: %w", getErr)
		}
		if !isOwnedGKEBootstrapCRB(existing, saName, i.namespace, nodeName) {
			return "", fmt.Errorf("GKE bootstrap CRB %s exists but has unexpected owner/roleRef/subject", gkeCRBName(nodeName))
		}
	} else if err != nil {
		return "", fmt.Errorf("create GKE bootstrap CRB: %w", err)
	}

	// Mint a short-lived token via TokenRequest.
	expSec := int64(gkeTokenExpirationSec)
	tr, err := i.k8s.CoreV1().ServiceAccounts(i.namespace).CreateToken(ctx, saName,
		&authenticationv1.TokenRequest{
			Spec: authenticationv1.TokenRequestSpec{
				ExpirationSeconds: &expSec,
			},
		}, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("mint GKE bootstrap token: %w", err)
	}
	if tr.Status.Token == "" {
		return "", fmt.Errorf("empty token in GKE TokenRequest response")
	}

	i.log.Info("issued GKE bootstrap SA token",
		"sa", saName, "namespace", i.namespace, "node", nodeName, "crb", gkeCRBName(nodeName))
	return tr.Status.Token, nil
}

// CleanupNode removes the per-node ServiceAccount and ClusterRoleBinding
// created for a burst node. Refuses to delete resources it does not own
// (missing or mismatched ownership labels). Idempotent: not-found errors
// are ignored.
func (i *GKETokenIssuer) CleanupNode(ctx context.Context, nodeName string) {
	saName, err := gkeSAName(nodeName)
	if err != nil {
		return
	}
	crbName := gkeCRBName(nodeName)

	crb, err := i.k8s.RbacV1().ClusterRoleBindings().Get(ctx, crbName, metav1.GetOptions{})
	if err == nil {
		if isOwnedGKEBootstrapCRB(crb, saName, i.namespace, nodeName) {
			if delErr := i.k8s.RbacV1().ClusterRoleBindings().Delete(ctx, crbName, metav1.DeleteOptions{}); delErr != nil && !apierrors.IsNotFound(delErr) {
				i.log.Warn("cleanup GKE bootstrap CRB", "name", crbName, "error", delErr)
			}
		} else {
			i.log.Warn("refusing to delete GKE bootstrap CRB not owned by this agent", "name", crbName)
		}
	} else if !apierrors.IsNotFound(err) {
		i.log.Warn("cleanup GKE bootstrap CRB lookup", "name", crbName, "error", err)
	}

	sa, err := i.k8s.CoreV1().ServiceAccounts(i.namespace).Get(ctx, saName, metav1.GetOptions{})
	if err == nil {
		if isOwnedGKEBootstrapSA(sa, nodeName) {
			if delErr := i.k8s.CoreV1().ServiceAccounts(i.namespace).Delete(ctx, saName, metav1.DeleteOptions{}); delErr != nil && !apierrors.IsNotFound(delErr) {
				i.log.Warn("cleanup GKE bootstrap SA", "name", saName, "error", delErr)
			}
		} else {
			i.log.Warn("refusing to delete GKE bootstrap SA not owned by this agent", "name", saName, "namespace", i.namespace)
		}
	} else if !apierrors.IsNotFound(err) {
		i.log.Warn("cleanup GKE bootstrap SA lookup", "name", saName, "namespace", i.namespace, "error", err)
	}

	i.log.Info("cleaned up GKE bootstrap resources", "node", nodeName, "sa", saName, "namespace", i.namespace, "crb", crbName)
}

// GKEBootstrapCleaner is the interface Forget/ForgetNode calls to clean up
// GKE-specific per-node resources.
type GKEBootstrapCleaner interface {
	CleanupNode(ctx context.Context, nodeName string)
}

// isGKEBootstrapUsername reports whether a CSR requesting username is a
// GKE-mode per-node SA in the given namespace. The approver uses this to
// accept GKE-mode CSRs where the username is
// system:serviceaccount:<namespace>:yscale-burst-bootstrap-<node> instead
// of system:bootstrap:<token-id>.
func isGKEBootstrapUsername(username, namespace string) bool {
	return strings.HasPrefix(username, "system:serviceaccount:"+namespace+":"+gkeBootstrapSAPrefix)
}

// gkeUsernameMatchesNode checks that the SA-based username is deterministically
// bound to the requested node CN. Returns true only when the username encodes
// the exact node name that the CN claims.
func gkeUsernameMatchesNode(username, nodeName, namespace string) bool {
	return username == gkeExpectedUsername(nodeName, namespace)
}

// isOwnedGKEBootstrapSA verifies that a ServiceAccount carries the required
// ownership labels for the given node.
func isOwnedGKEBootstrapSA(sa *corev1.ServiceAccount, nodeName string) bool {
	return sa.Labels[gkeOwnerLabel] == "true" && sa.Labels[gkeNodeNameLabel] == nodeName
}

// isOwnedGKEBootstrapCRB verifies that a ClusterRoleBinding carries the
// required ownership labels, the exact roleRef to system:node-bootstrapper,
// and the exact sole subject pointing at the given SA in the given namespace.
func isOwnedGKEBootstrapCRB(crb *rbacv1.ClusterRoleBinding, saName, namespace, nodeName string) bool {
	if crb.Labels[gkeOwnerLabel] != "true" || crb.Labels[gkeNodeNameLabel] != nodeName {
		return false
	}
	if crb.RoleRef.APIGroup != "rbac.authorization.k8s.io" || crb.RoleRef.Kind != "ClusterRole" || crb.RoleRef.Name != "system:node-bootstrapper" {
		return false
	}
	if len(crb.Subjects) != 1 {
		return false
	}
	s := crb.Subjects[0]
	return s.Kind == "ServiceAccount" && s.Name == saName && s.Namespace == namespace
}

var (
	_ BootstrapTokenIssuer = (*GKETokenIssuer)(nil)
	_ GKEBootstrapCleaner  = (*GKETokenIssuer)(nil)
)
