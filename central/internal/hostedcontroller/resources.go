package hostedcontroller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	managedByLabel = "app.kubernetes.io/managed-by"
	tenantLabel    = "yscale.sh/tenant-id"
	clusterLabel   = "yscale.sh/cluster-id"
	capacityLabel  = "yscale.sh/capacity-source"
)

var helmReleaseGVR = schema.GroupVersionResource{Group: "helm.toolkit.fluxcd.io", Version: "v2", Resource: "helmreleases"}

func (r *Reconciler) reconcileAssignment(ctx context.Context, row InventoryRow, token string) error {
	if err := validateRow(row); err != nil {
		return err
	}
	secretName := connectorSecretName(row.Cluster.ClusterID)
	secret, err := r.Kube.CoreV1().Secrets(r.Config.ConnectorNamespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get connector secret for tenant %s cluster %s: %w", row.TenantID, row.Cluster.ClusterID, err)
	}
	if err == nil {
		if err := validateOwnership(secret.Labels, row); err != nil {
			return fmt.Errorf("connector Secret conflict: %w", err)
		}
		if strings.TrimSpace(string(secret.Data["YSCALE_TOKEN"])) == "" {
			credential, rotateErr := r.Central.Rotate(ctx, row.TenantID, row.Cluster.ClusterID)
			if rotateErr != nil {
				return fmt.Errorf("recover credential for tenant %s cluster %s: %w", row.TenantID, row.Cluster.ClusterID, rotateErr)
			}
			token = credential.ConnectorToken
		}
	} else if token == "" {
		credential, rotateErr := r.Central.Rotate(ctx, row.TenantID, row.Cluster.ClusterID)
		if rotateErr != nil {
			return fmt.Errorf("recover credential for tenant %s cluster %s: %w", row.TenantID, row.Cluster.ClusterID, rotateErr)
		}
		token = credential.ConnectorToken
	}
	if token != "" {
		if err := r.upsertSecret(ctx, row, secretName, token); err != nil {
			return err
		}
	}
	if err := r.upsertNamespace(ctx, row); err != nil {
		return err
	}
	if err := r.upsertRuntimeBindingsSecret(ctx, row); err != nil {
		return err
	}
	if err := r.upsertQuota(ctx, row); err != nil {
		return err
	}
	if err := r.upsertLimitRange(ctx, row); err != nil {
		return err
	}
	if err := r.upsertNetworkPolicy(ctx, row); err != nil {
		return err
	}
	if err := r.upsertHelmRelease(ctx, row, secretName); err != nil {
		return err
	}
	r.logger().Info("hosted assignment reconciled", "tenant", row.TenantID, "cluster", row.Cluster.ClusterID)
	return nil
}

func connectorSecretName(clusterID string) string { return "yscale-" + clusterID + "-token" }

func labelsFor(row InventoryRow) map[string]string {
	return map[string]string{
		managedByLabel:              "yscale-hosted-controller",
		tenantLabel:                 row.TenantID,
		clusterLabel:                row.Cluster.ClusterID,
		capacityLabel:               "hosted",
		"app.kubernetes.io/part-of": "yscale",
	}
}

func mergeLabels(dst, src map[string]string) map[string]string {
	if dst == nil {
		dst = map[string]string{}
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func validateOwnership(labels map[string]string, row InventoryRow) error {
	if got := labels[tenantLabel]; got != "" && got != row.TenantID {
		return fmt.Errorf("tenant label is %q, want %q", got, row.TenantID)
	}
	if got := labels[clusterLabel]; got != "" && got != row.Cluster.ClusterID {
		return fmt.Errorf("cluster label is %q, want %q", got, row.Cluster.ClusterID)
	}
	return nil
}

func (r *Reconciler) upsertSecret(ctx context.Context, row InventoryRow, name, token string) error {
	api := r.Kube.CoreV1().Secrets(r.Config.ConnectorNamespace)
	current, err := api.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.Config.ConnectorNamespace, Labels: labelsFor(row)}, Data: map[string][]byte{"YSCALE_TOKEN": []byte(token)}}, metav1.CreateOptions{})
	} else if err == nil {
		if ownErr := validateOwnership(current.Labels, row); ownErr != nil {
			return ownErr
		}
		current.Labels = mergeLabels(current.Labels, labelsFor(row))
		current.StringData = nil
		current.Data = map[string][]byte{"YSCALE_TOKEN": []byte(token)}
		_, err = api.Update(ctx, current, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("persist connector credential for tenant %s cluster %s: %w", row.TenantID, row.Cluster.ClusterID, err)
	}
	return nil
}

func (r *Reconciler) upsertNamespace(ctx context.Context, row InventoryRow) error {
	api := r.Kube.CoreV1().Namespaces()
	current, err := api.Get(ctx, row.Cluster.HostedNamespace, metav1.GetOptions{})
	desired := labelsFor(row)
	desired["pod-security.kubernetes.io/enforce"] = "baseline"
	desired["pod-security.kubernetes.io/enforce-version"] = "v1.35"
	desired["pod-security.kubernetes.io/audit"] = "restricted"
	desired["pod-security.kubernetes.io/audit-version"] = "latest"
	desired["pod-security.kubernetes.io/warn"] = "restricted"
	desired["pod-security.kubernetes.io/warn-version"] = "latest"
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: row.Cluster.HostedNamespace, Labels: desired}}, metav1.CreateOptions{})
	} else if err == nil {
		if ownErr := validateOwnership(current.Labels, row); ownErr != nil {
			return fmt.Errorf("Namespace conflict: %w", ownErr)
		}
		current.Labels = mergeLabels(current.Labels, desired)
		_, err = api.Update(ctx, current, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("reconcile Namespace for tenant %s cluster %s: %w", row.TenantID, row.Cluster.ClusterID, err)
	}
	return nil
}

func (r *Reconciler) upsertRuntimeBindingsSecret(ctx context.Context, row InventoryRow) error {
	api := r.Kube.CoreV1().Secrets(row.Cluster.HostedNamespace)
	current, err := api.Get(ctx, "yscale-runtime-bindings", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: "yscale-runtime-bindings", Namespace: row.Cluster.HostedNamespace, Labels: labelsFor(row),
		}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{}}, metav1.CreateOptions{})
	} else if err == nil {
		if ownErr := validateOwnership(current.Labels, row); ownErr != nil {
			return fmt.Errorf("runtime bindings Secret conflict: %w", ownErr)
		}
		current.Labels = mergeLabels(current.Labels, labelsFor(row))
		_, err = api.Update(ctx, current, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("reconcile runtime bindings Secret for tenant %s cluster %s: %w", row.TenantID, row.Cluster.ClusterID, err)
	}
	return nil
}

func (r *Reconciler) upsertQuota(ctx context.Context, row InventoryRow) error {
	api := r.Kube.CoreV1().ResourceQuotas(row.Cluster.HostedNamespace)
	hard := corev1.ResourceList{
		corev1.ResourceName("count/jobs.batch"): resource.MustParse("25"),
		corev1.ResourceName("count/pods"):       resource.MustParse("50"),
		corev1.ResourceRequestsCPU:              resource.MustParse("64"),
		corev1.ResourceRequestsMemory:           resource.MustParse("256Gi"),
		corev1.ResourceLimitsCPU:                resource.MustParse("128"),
		corev1.ResourceLimitsMemory:             resource.MustParse("512Gi"),
	}
	cur, err := api.Get(ctx, "tenant-capacity", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "tenant-capacity", Namespace: row.Cluster.HostedNamespace, Labels: labelsFor(row)}, Spec: corev1.ResourceQuotaSpec{Hard: hard}}, metav1.CreateOptions{})
	} else if err == nil {
		if ownErr := validateOwnership(cur.Labels, row); ownErr != nil {
			return fmt.Errorf("ResourceQuota conflict: %w", ownErr)
		}
		cur.Labels = mergeLabels(cur.Labels, labelsFor(row))
		cur.Spec.Hard = hard
		_, err = api.Update(ctx, cur, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("reconcile ResourceQuota for tenant %s cluster %s: %w", row.TenantID, row.Cluster.ClusterID, err)
	}
	return nil
}

func (r *Reconciler) upsertLimitRange(ctx context.Context, row InventoryRow) error {
	api := r.Kube.CoreV1().LimitRanges(row.Cluster.HostedNamespace)
	limits := []corev1.LimitRangeItem{{Type: corev1.LimitTypeContainer, DefaultRequest: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")}, Default: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")}}}
	cur, err := api.Get(ctx, "tenant-defaults", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, &corev1.LimitRange{ObjectMeta: metav1.ObjectMeta{Name: "tenant-defaults", Namespace: row.Cluster.HostedNamespace, Labels: labelsFor(row)}, Spec: corev1.LimitRangeSpec{Limits: limits}}, metav1.CreateOptions{})
	} else if err == nil {
		if ownErr := validateOwnership(cur.Labels, row); ownErr != nil {
			return fmt.Errorf("LimitRange conflict: %w", ownErr)
		}
		cur.Labels = mergeLabels(cur.Labels, labelsFor(row))
		cur.Spec.Limits = limits
		_, err = api.Update(ctx, cur, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("reconcile LimitRange for tenant %s cluster %s: %w", row.TenantID, row.Cluster.ClusterID, err)
	}
	return nil
}

func (r *Reconciler) upsertNetworkPolicy(ctx context.Context, row InventoryRow) error {
	api := r.Kube.NetworkingV1().NetworkPolicies(row.Cluster.HostedNamespace)
	desired := networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{}, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}}
	cur, err := api.Get(ctx, "default-deny-ingress", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "default-deny-ingress", Namespace: row.Cluster.HostedNamespace, Labels: labelsFor(row)}, Spec: desired}, metav1.CreateOptions{})
	} else if err == nil {
		if ownErr := validateOwnership(cur.Labels, row); ownErr != nil {
			return fmt.Errorf("NetworkPolicy conflict: %w", ownErr)
		}
		cur.Labels = mergeLabels(cur.Labels, labelsFor(row))
		cur.Spec = desired
		_, err = api.Update(ctx, cur, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("reconcile NetworkPolicy for tenant %s cluster %s: %w", row.TenantID, row.Cluster.ClusterID, err)
	}
	return nil
}

func (r *Reconciler) upsertHelmRelease(ctx context.Context, row InventoryRow, secretName string) error {
	name := "yscale-agent-" + row.Cluster.ClusterID
	api := r.Dynamic.Resource(helmReleaseGVR).Namespace(r.Config.ConnectorNamespace)
	cur, err := api.Get(ctx, name, metav1.GetOptions{})
	values := map[string]any{
		"endpoint": r.Config.CentralEndpoint, "clusterID": row.Cluster.ClusterID, "workloadNamespace": row.Cluster.HostedNamespace,
		"existingSecret": secretName, "installCRD": false, "cloudProvider": "k3s", "imagePullSecrets": []any{map[string]any{"name": "ghcr-pull"}},
		"rbac":      map[string]any{"scope": "namespaced", "allowedNamespaces": []any{row.Cluster.HostedNamespace}},
		"bootstrap": map[string]any{"apiserverURL": r.Config.BootstrapAPIServer},
		"agent": map[string]any{"image": map[string]any{
			"repository": r.Config.AgentImageRepo, "tag": r.Config.AgentImageTag,
			"digest": r.Config.AgentImageDigest, "requireDigest": true, "pullPolicy": "Always",
		}},
		"gateway": map[string]any{
			"enabled":                          true,
			"centralServiceNamespace":          r.Config.CentralServiceNamespace,
			"advertiseRoutes":                  stringSliceAny(r.Config.AdvertisedRoutes),
			"enableForwardingViaInitContainer": true,
			"kubeletProxyRouting":              map[string]any{"enabled": false},
		},
		"cilium": map[string]any{"crds": map[string]any{"enabled": false}, "scaffold": map[string]any{"enabled": false}},
	}
	spec := map[string]any{
		"releaseName":      name,
		"targetNamespace":  r.Config.ConnectorNamespace,
		"storageNamespace": r.Config.ConnectorNamespace,
		"interval":         "30m",
		"timeout":          "10m",
		"chart": map[string]any{"spec": map[string]any{
			"chart":             "./deploy/helm/yscale-agent",
			"reconcileStrategy": "Revision",
			"sourceRef":         map[string]any{"kind": "GitRepository", "name": r.Config.SourceName, "namespace": r.Config.SourceNamespace},
		}},
		"install": map[string]any{"disableWait": false, "remediation": map[string]any{"retries": int64(3), "remediateLastFailure": true}},
		"upgrade": map[string]any{"disableWait": false, "cleanupOnFail": true, "remediation": map[string]any{"retries": int64(3), "remediateLastFailure": true, "strategy": "rollback"}},
		"values":  values,
	}
	if apierrors.IsNotFound(err) {
		obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease", "metadata": map[string]any{"name": name, "namespace": r.Config.ConnectorNamespace, "labels": stringMapAny(labelsFor(row))}, "spec": spec}}
		_, err = api.Create(ctx, obj, metav1.CreateOptions{FieldManager: "yscale-hosted-controller"})
	} else if err == nil {
		if ownErr := validateOwnership(cur.GetLabels(), row); ownErr != nil {
			return fmt.Errorf("HelmRelease conflict: %w", ownErr)
		}
		cur.SetLabels(mergeLabels(cur.GetLabels(), labelsFor(row)))
		if setErr := unstructured.SetNestedMap(cur.Object, spec, "spec"); setErr != nil {
			return setErr
		}
		_, err = api.Update(ctx, cur, metav1.UpdateOptions{FieldManager: "yscale-hosted-controller"})
	}
	if err != nil {
		return fmt.Errorf("reconcile HelmRelease for tenant %s cluster %s: %w", row.TenantID, row.Cluster.ClusterID, err)
	}
	return nil
}

func stringSliceAny(in []string) []any {
	out := make([]any, len(in))
	for i := range in {
		out[i] = in[i]
	}
	return out
}
func stringMapAny(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
