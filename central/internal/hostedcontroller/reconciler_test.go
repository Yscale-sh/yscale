package hostedcontroller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type fakeCentral struct {
	inventory                []InventoryRow
	requests                 []CapacityRequest
	assigned                 Credential
	rotated                  Credential
	assignCalls, rotateCalls int
}

func (f *fakeCentral) ListInventory(context.Context) ([]InventoryRow, error) { return f.inventory, nil }
func (f *fakeCentral) ListRequests(context.Context) ([]CapacityRequest, error) {
	return f.requests, nil
}
func (f *fakeCentral) Assign(context.Context, string) (Credential, error) {
	f.assignCalls++
	return f.assigned, nil
}
func (f *fakeCentral) Rotate(context.Context, string, string) (Credential, error) {
	f.rotateCalls++
	return f.rotated, nil
}

func testRow() InventoryRow {
	return InventoryRow{TenantID: "cust-a", Plan: "pro", Cluster: HostedCluster{ClusterID: "hosted-a", Source: "hosted", State: "connected", HostedNamespace: "ys-a", RegisteredAt: time.Now()}}
}
func testCredential(token string) Credential {
	r := testRow()
	return Credential{TenantID: r.TenantID, Cluster: r.Cluster, ConnectorToken: token, HelmRelease: "yscale-agent-hosted-a", ConnectorRBACSet: "x", HelmCommand: "x"}
}
func testConfig() Config {
	return Config{MaxAssignments: 8, ConnectorNamespace: "yscale-system", CentralEndpoint: "ws://yscale-cloud.yscale:8443", CentralServiceNamespace: "yscale", SourceName: "yscale-agent", SourceNamespace: "flux-system", BootstrapAPIServer: "https://node0:6443", AgentImageRepo: "ghcr.io/jakenesler/agent", AgentImageTag: "v1", AgentImageDigest: "sha256:" + strings.Repeat("a", 64), AdvertisedRoutes: []string{"10.42.0.0/16"}, PollInterval: time.Second}
}
func newTestReconciler(c *fakeCentral, objects ...runtime.Object) (*Reconciler, *k8sfake.Clientset) {
	k := k8sfake.NewSimpleClientset(objects...)
	d := fake.NewSimpleDynamicClient(runtime.NewScheme())
	return &Reconciler{Central: c, Kube: k, Dynamic: d, Config: testConfig(), Log: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}, k
}

func TestConfigRequiresImmutableAgentDigest(t *testing.T) {
	for _, digest := range []string{"", "sha256:short", "sha256:" + strings.Repeat("A", 64), "sha512:" + strings.Repeat("a", 64)} {
		cfg := testConfig()
		cfg.AgentImageDigest = digest
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "agent image digest") {
			t.Fatalf("Validate digest %q = %v, want digest error", digest, err)
		}
	}
	if err := testConfig().Validate(); err != nil {
		t.Fatalf("valid immutable config: %v", err)
	}
}

func TestPendingAssignmentPersistsSecretAndResources(t *testing.T) {
	c := &fakeCentral{requests: []CapacityRequest{{TenantID: "cust-a", Plan: "pro", RequestedAt: time.Now()}}, assigned: testCredential("token-value")}
	r, k := newTestReconciler(c)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.assignCalls != 1 || c.rotateCalls != 0 {
		t.Fatalf("calls assign=%d rotate=%d", c.assignCalls, c.rotateCalls)
	}
	s, err := k.CoreV1().Secrets("yscale-system").Get(context.Background(), connectorSecretName("hosted-a"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(s.Data["YSCALE_TOKEN"]) != "token-value" || len(s.Data) != 1 {
		t.Fatalf("secret data = %v", s.Data)
	}
	ns, err := k.CoreV1().Namespaces().Get(context.Background(), "ys-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"yscale.sh/tenant-id": "cust-a", "yscale.sh/capacity-source": "hosted", "pod-security.kubernetes.io/enforce": "baseline", "pod-security.kubernetes.io/enforce-version": "v1.35", "pod-security.kubernetes.io/audit": "restricted", "pod-security.kubernetes.io/audit-version": "latest", "pod-security.kubernetes.io/warn": "restricted", "pod-security.kubernetes.io/warn-version": "latest"} {
		if ns.Labels[key] != want {
			t.Errorf("namespace label %s = %q, want %q", key, ns.Labels[key], want)
		}
	}
	quota, err := k.CoreV1().ResourceQuotas("ys-a").Get(context.Background(), "tenant-capacity", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(quota.Spec.Hard) != 6 {
		t.Fatalf("quota hard = %v, want exact six limits", quota.Spec.Hard)
	}
	for key, want := range map[corev1.ResourceName]string{"count/jobs.batch": "25", "count/pods": "50", corev1.ResourceRequestsCPU: "64", corev1.ResourceRequestsMemory: "256Gi", corev1.ResourceLimitsCPU: "128", corev1.ResourceLimitsMemory: "512Gi"} {
		if got := quota.Spec.Hard[key]; got.String() != want {
			t.Errorf("quota %s = %s, want %s", key, got.String(), want)
		}
	}
	limits, err := k.CoreV1().LimitRanges("ys-a").Get(context.Background(), "tenant-defaults", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := limits.Spec.Limits[0].Default[corev1.ResourceCPU]; got.String() != "1" {
		t.Errorf("default cpu = %s", got.String())
	}
	if _, err := k.NetworkingV1().NetworkPolicies("ys-a").Get(context.Background(), "default-deny-ingress", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	hr, err := r.Dynamic.Resource(helmReleaseGVR).Namespace("yscale-system").Get(context.Background(), "yscale-agent-hosted-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		path []string
		want string
	}{
		{[]string{"spec", "releaseName"}, "yscale-agent-hosted-a"},
		{[]string{"spec", "targetNamespace"}, "yscale-system"},
		{[]string{"spec", "storageNamespace"}, "yscale-system"},
		{[]string{"spec", "interval"}, "30m"}, {[]string{"spec", "timeout"}, "10m"},
		{[]string{"spec", "chart", "spec", "reconcileStrategy"}, "Revision"},
		{[]string{"spec", "chart", "spec", "sourceRef", "name"}, "yscale-agent"},
		{[]string{"spec", "upgrade", "remediation", "strategy"}, "rollback"},
		{[]string{"spec", "values", "endpoint"}, "ws://yscale-cloud.yscale:8443"},
		{[]string{"spec", "values", "existingSecret"}, connectorSecretName("hosted-a")},
		{[]string{"spec", "values", "gateway", "centralServiceNamespace"}, "yscale"},
		{[]string{"spec", "values", "agent", "image", "digest"}, testConfig().AgentImageDigest},
		{[]string{"spec", "values", "agent", "image", "pullPolicy"}, "Always"},
	} {
		got, ok, getErr := unstructured.NestedString(hr.Object, check.path...)
		if getErr != nil || !ok || got != check.want {
			t.Errorf("HelmRelease %v = %q ok=%v err=%v, want %q", check.path, got, ok, getErr, check.want)
		}
	}
	if required, ok, _ := unstructured.NestedBool(hr.Object, "spec", "values", "agent", "image", "requireDigest"); !ok || !required {
		t.Error("HelmRelease does not require an immutable agent digest")
	}
	if enabled, ok, _ := unstructured.NestedBool(hr.Object, "spec", "values", "gateway", "enableForwardingViaInitContainer"); !ok || !enabled {
		t.Error("gateway init forwarding not enabled")
	}
	if enabled, ok, _ := unstructured.NestedBool(hr.Object, "spec", "values", "gateway", "kubeletProxyRouting", "enabled"); !ok || enabled {
		t.Error("hosted connector must not enable kubelet proxy routing")
	}
	if _, found, _ := unstructured.NestedString(hr.Object, "spec", "values", "gateway", "kubeletProxyRouting", "destinationCIDR"); found {
		t.Error("hosted connector must not set kubeletProxyRouting.destinationCIDR")
	}
	if retries, ok, _ := unstructured.NestedInt64(hr.Object, "spec", "install", "remediation", "retries"); !ok || retries != 3 {
		t.Errorf("install remediation retries = %d ok=%v", retries, ok)
	}
	if retries, ok, _ := unstructured.NestedInt64(hr.Object, "spec", "upgrade", "remediation", "retries"); !ok || retries != 3 {
		t.Errorf("upgrade remediation retries = %d ok=%v", retries, ok)
	}
}

func TestMissingSecretRotatesOnceAndExistingSecretNeverRotates(t *testing.T) {
	c := &fakeCentral{inventory: []InventoryRow{testRow()}, rotated: testCredential("recovered")}
	r, k := newTestReconciler(c)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.rotateCalls != 1 {
		t.Fatalf("rotations = %d, want 1", c.rotateCalls)
	}
	if _, err := k.CoreV1().Secrets("yscale-system").Get(context.Background(), connectorSecretName("hosted-a"), metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestExistingSecretRepairsMissingResourcesWithoutRotation(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: connectorSecretName("hosted-a"), Namespace: "yscale-system", Labels: labelsFor(testRow())}, Data: map[string][]byte{"YSCALE_TOKEN": []byte("kept")}}
	c := &fakeCentral{inventory: []InventoryRow{testRow()}, rotated: testCredential("must-not-use")}
	r, k := newTestReconciler(c, secret)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.rotateCalls != 0 {
		t.Fatalf("rotations = %d", c.rotateCalls)
	}
	if _, err := k.CoreV1().ResourceQuotas("ys-a").Get(context.Background(), "tenant-capacity", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyExistingSecretRotatesOnceBeforeResources(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: connectorSecretName("hosted-a"), Namespace: "yscale-system", Labels: labelsFor(testRow())}, Data: map[string][]byte{"YSCALE_TOKEN": nil}}
	c := &fakeCentral{inventory: []InventoryRow{testRow()}, rotated: testCredential("recovered-empty")}
	r, k := newTestReconciler(c, secret)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.rotateCalls != 1 {
		t.Fatalf("rotations = %d, want 1", c.rotateCalls)
	}
	got, err := k.CoreV1().Secrets("yscale-system").Get(context.Background(), connectorSecretName("hosted-a"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Data["YSCALE_TOKEN"]) != "recovered-empty" {
		t.Fatalf("token was not recovered")
	}
}

func TestAtCapacityDoesNotAssign(t *testing.T) {
	c := &fakeCentral{inventory: []InventoryRow{testRow()}, requests: []CapacityRequest{{TenantID: "cust-b", Plan: "pro", RequestedAt: time.Now()}}, rotated: testCredential("x")}
	r, _ := newTestReconciler(c)
	r.Config.MaxAssignments = 1
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.assignCalls != 0 {
		t.Fatalf("assignments = %d", c.assignCalls)
	}
}

func TestExcludedLegacyClusterIsSkippedButCountsAgainstCapacity(t *testing.T) {
	c := &fakeCentral{inventory: []InventoryRow{testRow()}, requests: []CapacityRequest{{TenantID: "cust-b", Plan: "pro", RequestedAt: time.Now()}}}
	r, k := newTestReconciler(c)
	r.Config.MaxAssignments = 1
	r.Config.ExcludedClusterIDs = map[string]struct{}{"hosted-a": {}}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.assignCalls != 0 || c.rotateCalls != 0 {
		t.Fatalf("assign=%d rotate=%d", c.assignCalls, c.rotateCalls)
	}
	if _, err := k.CoreV1().Namespaces().Get(context.Background(), "ys-a", metav1.GetOptions{}); err == nil {
		t.Fatal("excluded cluster namespace was reconciled")
	}
}

func TestSecretOwnershipMismatchFailsBeforeRotation(t *testing.T) {
	labels := labelsFor(testRow())
	labels[clusterLabel] = "hosted-other"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: connectorSecretName("hosted-a"), Namespace: "yscale-system", Labels: labels}}
	c := &fakeCentral{inventory: []InventoryRow{testRow()}, rotated: testCredential("must-not-use")}
	r, _ := newTestReconciler(c, secret)
	err := r.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "connector Secret conflict") {
		t.Fatalf("error = %v", err)
	}
	if c.rotateCalls != 0 {
		t.Fatalf("rotations = %d", c.rotateCalls)
	}
}

func TestConflictingOwnershipFailsClosed(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ys-a", Labels: map[string]string{tenantLabel: "cust-other"}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: connectorSecretName("hosted-a"), Namespace: "yscale-system", Labels: labelsFor(testRow())}, Data: map[string][]byte{"YSCALE_TOKEN": []byte("kept")}}
	c := &fakeCentral{inventory: []InventoryRow{testRow()}}
	r, _ := newTestReconciler(c, ns, secret)
	err := r.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("error = %v", err)
	}
	if c.rotateCalls != 0 {
		t.Fatalf("rotations = %d", c.rotateCalls)
	}
}

func TestTransientFailureKeepsCredentialAndRetryDoesNotRotate(t *testing.T) {
	c := &fakeCentral{requests: []CapacityRequest{{TenantID: "cust-a", Plan: "pro", RequestedAt: time.Now()}}, assigned: testCredential("durable-token"), inventory: nil}
	r, k := newTestReconciler(c)
	fail := true
	k.PrependReactor("create", "resourcequotas", func(ktesting.Action) (bool, runtime.Object, error) {
		if fail {
			fail = false
			return true, nil, errors.New("transient")
		}
		return false, nil, nil
	})
	if err := r.Reconcile(context.Background()); err == nil {
		t.Fatal("expected transient error")
	}
	if _, err := k.CoreV1().Secrets("yscale-system").Get(context.Background(), connectorSecretName("hosted-a"), metav1.GetOptions{}); err != nil {
		t.Fatalf("credential was not persisted first: %v", err)
	}
	c.requests = nil
	c.inventory = []InventoryRow{testRow()}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.rotateCalls != 0 {
		t.Fatalf("rotations = %d", c.rotateCalls)
	}
}

func TestLogsNeverContainConnectorToken(t *testing.T) {
	var logs bytes.Buffer
	c := &fakeCentral{requests: []CapacityRequest{{TenantID: "cust-a", Plan: "pro", RequestedAt: time.Now()}}, assigned: testCredential("super-secret-connector")}
	r, _ := newTestReconciler(c)
	r.Log = slog.New(slog.NewTextHandler(&logs, nil))
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "super-secret-connector") {
		t.Fatalf("logs leaked token: %s", logs.String())
	}
}
