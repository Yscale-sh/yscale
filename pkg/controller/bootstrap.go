package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// bootstrapTokenTTL is how long the bootstrap token is valid.
// The kubelet must use it within this window to TLS-bootstrap.
const bootstrapTokenTTL = 15 * time.Minute

// EnsureBootstrapRBAC creates the ClusterRoleBinding needed for bootstrap
// token authentication and CSR auto-approval. Idempotent.
func EnsureBootstrapRBAC(ctx context.Context, clientset kubernetes.Interface) error {
	// Allow bootstrap tokens to create CSRs.
	crbName := "yscale:node-bootstrapper"
	_, err := clientset.RbacV1().ClusterRoleBindings().Get(ctx, crbName, metav1.GetOptions{})
	if err != nil {
		_, err = clientset.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: crbName},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "system:node-bootstrapper",
			},
			Subjects: []rbacv1.Subject{
				{Kind: "Group", Name: "system:bootstrappers", APIGroup: "rbac.authorization.k8s.io"},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("creating bootstrap CRB: %w", err)
		}
	}

	// Auto-approve CSRs from bootstrap tokens.
	csrCRBName := "yscale:node-autoapprove-bootstrap"
	_, err = clientset.RbacV1().ClusterRoleBindings().Get(ctx, csrCRBName, metav1.GetOptions{})
	if err != nil {
		_, err = clientset.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: csrCRBName},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "system:certificates.k8s.io:certificatesigningrequests:nodeclient",
			},
			Subjects: []rbacv1.Subject{
				{Kind: "Group", Name: "system:bootstrappers", APIGroup: "rbac.authorization.k8s.io"},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("creating CSR auto-approve CRB: %w", err)
		}
	}

	// Auto-approve CSR renewals from nodes.
	renewCRBName := "yscale:node-autoapprove-certificate-rotation"
	_, err = clientset.RbacV1().ClusterRoleBindings().Get(ctx, renewCRBName, metav1.GetOptions{})
	if err != nil {
		_, err = clientset.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: renewCRBName},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "system:certificates.k8s.io:certificatesigningrequests:selfnodeclient",
			},
			Subjects: []rbacv1.Subject{
				{Kind: "Group", Name: "system:nodes", APIGroup: "rbac.authorization.k8s.io"},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("creating cert rotation CRB: %w", err)
		}
	}

	return nil
}

// CreateBootstrapToken creates a short-lived bootstrap token that a kubelet
// can use to TLS-bootstrap and join the cluster. Returns "tokenID.tokenSecret".
func CreateBootstrapToken(ctx context.Context, clientset kubernetes.Interface) (string, error) {
	tokenID, err := randomHex(3)
	if err != nil {
		return "", err
	}
	tokenSecret, err := randomHex(8)
	if err != nil {
		return "", err
	}

	expiration := time.Now().Add(bootstrapTokenTTL)

	secret := &corev1.Secret{
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
			// The bare group "system:bootstrappers" fails the authenticator's
			// BootstrapGroupPattern (extra groups MUST carry a suffix:
			// system:bootstrappers:<name>), which silently invalidates the
			// whole token: every join dies with 401 Unauthorized. The default
			// group is always added implicitly; this suffixed extra only tags
			// yscale-minted tokens. Live-validated against k3s v1.31.5.
			"auth-extra-groups": "system:bootstrappers:yscale",
			"expiration":        expiration.Format(time.RFC3339),
		},
	}

	_, err = clientset.CoreV1().Secrets("kube-system").Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("creating bootstrap token secret: %w", err)
	}

	return tokenID + "." + tokenSecret, nil
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
