package clusterroutes

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
)

func TestRoutesAllAndEmpty(t *testing.T) {
	var empty Routes
	if !empty.Empty() {
		t.Fatal("empty routes should report Empty")
	}

	routes := Routes{
		OverrideCIDRs: []string{"10.99.0.0/16"},
		PodCIDRs:      []string{"10.42.0.0/16", "10.1.0.0/16"},
		ServiceCIDRs:  []string{"10.43.0.0/16", "10.42.0.0/16"},
		NodeCIDRs:     []string{"10.0.0.0/24"},
	}
	want := []string{"10.0.0.0/24", "10.1.0.0/16", "10.42.0.0/16", "10.43.0.0/16", "10.99.0.0/16"}
	if got := routes.All(); !reflect.DeepEqual(got, want) {
		t.Fatalf("All() = %v, want %v", got, want)
	}
	if routes.Empty() {
		t.Fatal("non-empty routes should not report Empty")
	}
}

func TestResolveOverrideWins(t *testing.T) {
	override := []string{"10.98.0.0/16", "10.99.0.0/16"}
	got, err := Resolve(context.Background(), fake.NewSimpleClientset(node("n1", []string{"192.168.1.0/24"}, "192.168.1.10")), emptyDynamic(), Options{
		CloudProvider: "k3s",
		Override:      override,
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !reflect.DeepEqual(got.OverrideCIDRs, override) || len(got.PodCIDRs) != 0 || len(got.ServiceCIDRs) != 0 || len(got.NodeCIDRs) != 0 {
		t.Fatalf("override routes = %+v, want override verbatim in OverrideCIDRs only", got)
	}
	if gotAll := got.All(); !reflect.DeepEqual(gotAll, override) {
		t.Fatalf("All() = %v, want override %v", gotAll, override)
	}
}

func TestResolveProviderTableAggregatesAfterRuntimeValidation(t *testing.T) {
	kube := fake.NewSimpleClientset(
		node("n1", []string{"10.42.2.0/24", "10.42.1.0/24"}, "10.0.0.10"),
		node("n2", []string{"10.42.1.0/24"}, "10.0.0.250"),
	)

	got, err := Resolve(context.Background(), kube, emptyDynamic(), Options{CloudProvider: "k3s"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if want := []string{"10.42.0.0/16"}; !reflect.DeepEqual(got.PodCIDRs, want) {
		t.Fatalf("PodCIDRs = %v, want provider table aggregate %v", got.PodCIDRs, want)
	}
	if want := []string{"10.43.0.0/16"}; !reflect.DeepEqual(got.ServiceCIDRs, want) {
		t.Fatalf("ServiceCIDRs = %v, want provider fallback %v", got.ServiceCIDRs, want)
	}
	if want := []string{"10.0.0.0/24"}; !reflect.DeepEqual(got.NodeCIDRs, want) {
		t.Fatalf("NodeCIDRs = %v, want %v", got.NodeCIDRs, want)
	}
}

func TestResolveProviderTableStableAcrossScaleOut(t *testing.T) {
	before, err := Resolve(context.Background(), fake.NewSimpleClientset(
		node("n1", []string{"10.42.1.0/24"}, "10.0.0.10"),
	), emptyDynamic(), Options{CloudProvider: "k3s"})
	if err != nil {
		t.Fatalf("Resolve before scale-out: %v", err)
	}

	after, err := Resolve(context.Background(), fake.NewSimpleClientset(
		node("n1", []string{"10.42.1.0/24"}, "10.0.0.10"),
		node("n2", []string{"10.42.99.0/24"}, "10.0.0.11"),
		burstNode("burst-1", []string{"172.30.0.0/24"}, "100.64.0.10"),
	), emptyDynamic(), Options{CloudProvider: "k3s"})
	if err != nil {
		t.Fatalf("Resolve after scale-out: %v", err)
	}

	if !reflect.DeepEqual(after.PodCIDRs, before.PodCIDRs) {
		t.Fatalf("PodCIDRs changed across scale-out: before=%v after=%v", before.PodCIDRs, after.PodCIDRs)
	}
	if !reflect.DeepEqual(after.NodeCIDRs, before.NodeCIDRs) {
		t.Fatalf("NodeCIDRs changed across scale-out: before=%v after=%v", before.NodeCIDRs, after.NodeCIDRs)
	}
}

func TestResolveProviderTableFallback(t *testing.T) {
	tests := []struct {
		provider string
		want     Routes
	}{
		{
			provider: "k3s",
			want: Routes{
				PodCIDRs:     []string{"10.42.0.0/16"},
				ServiceCIDRs: []string{"10.43.0.0/16"},
				NodeCIDRs:    []string{"10.0.0.0/24"},
			},
		},
		{
			provider: "linode",
			want: Routes{
				PodCIDRs:     []string{"10.42.0.0/16"},
				ServiceCIDRs: []string{"10.96.0.0/12"},
				NodeCIDRs:    []string{"10.0.0.0/24"},
			},
		},
		{
			provider: "self-managed",
			want: Routes{
				PodCIDRs:     []string{"10.42.0.0/16"},
				ServiceCIDRs: []string{"10.96.0.0/12"},
				NodeCIDRs:    []string{"10.0.0.0/24"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.provider, func(t *testing.T) {
			got, err := Resolve(context.Background(), fake.NewSimpleClientset(), emptyDynamic(), Options{CloudProvider: tt.provider})
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("routes = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestResolveUsesRuntimePodCIDRsOnlyWhenProviderClassMissing(t *testing.T) {
	providerDefaults["test-pod-runtime"] = providerDefault{
		ServiceCIDRs: []string{"10.55.0.0/16"},
		NodeCIDRs:    []string{"10.0.0.0/24"},
	}
	t.Cleanup(func() { delete(providerDefaults, "test-pod-runtime") })

	kube := fake.NewSimpleClientset(node("n1", []string{"10.2.0.0/24"}, "10.0.0.10"))
	got, err := Resolve(context.Background(), kube, emptyDynamic(), Options{CloudProvider: "test-pod-runtime"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if want := []string{"10.2.0.0/24"}; !reflect.DeepEqual(got.PodCIDRs, want) {
		t.Fatalf("PodCIDRs = %v, want %v", got.PodCIDRs, want)
	}
}

func TestResolveServiceCIDRAPI(t *testing.T) {
	kube := fake.NewSimpleClientset(node("n1", []string{"10.42.1.0/24"}, "10.0.0.10"))
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			serviceCIDRGVRs[0]: "ServiceCIDRList",
		},
		serviceCIDR("kubernetes", "10.43.0.0/16"),
	)

	got, err := Resolve(context.Background(), kube, dyn, Options{CloudProvider: "k3s"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if want := []string{"10.43.0.0/16"}; !reflect.DeepEqual(got.ServiceCIDRs, want) {
		t.Fatalf("ServiceCIDRs = %v, want ServiceCIDR API %v", got.ServiceCIDRs, want)
	}
	if want := []string{"10.42.0.0/16"}; !reflect.DeepEqual(got.PodCIDRs, want) {
		t.Fatalf("PodCIDRs = %v, want provider table aggregate %v", got.PodCIDRs, want)
	}
}

func TestResolveRejectsProviderDefaultMismatches(t *testing.T) {
	tests := []struct {
		name  string
		kube  *fake.Clientset
		dyn   *dynamicfake.FakeDynamicClient
		want  string
		class string
	}{
		{
			name:  "pod CIDR",
			kube:  fake.NewSimpleClientset(node("n1", []string{"10.99.0.0/24"}, "10.0.0.10")),
			dyn:   emptyDynamic(),
			want:  "10.99.0.0/24",
			class: "pod CIDRs",
		},
		{
			name:  "node InternalIP",
			kube:  fake.NewSimpleClientset(node("n1", []string{"10.42.1.0/24"}, "10.0.1.10")),
			dyn:   emptyDynamic(),
			want:  "10.0.1.10",
			class: "node CIDRs",
		},
		{
			name: "service CIDR",
			kube: fake.NewSimpleClientset(node("n1", []string{"10.42.1.0/24"}, "10.0.0.10")),
			dyn: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
				runtime.NewScheme(),
				map[schema.GroupVersionResource]string{serviceCIDRGVRs[0]: "ServiceCIDRList"},
				serviceCIDR("kubernetes", "10.55.0.0/16"),
			),
			want:  "10.55.0.0/16",
			class: "service CIDRs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Resolve(context.Background(), tt.kube, tt.dyn, Options{CloudProvider: "k3s"})
			if err == nil {
				t.Fatal("expected mismatch error")
			}
			for _, want := range []string{tt.class, tt.want, `cloudProvider "k3s"`, "gateway.advertiseRoutes"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q missing %q", err.Error(), want)
				}
			}
		})
	}
}

func TestServiceCIDRDryRunProbeUsesConfiguredNamespace(t *testing.T) {
	kube := fake.NewSimpleClientset()
	var gotNamespace string
	kube.Fake.PrependReactor("create", "services", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		gotNamespace = action.GetNamespace()
		return true, nil, fmt.Errorf(`Service "x" is invalid: spec.clusterIPs[0]: Invalid value: "240.0.0.1": failed to allocate IP 240.0.0.1: the provided IP is not in the valid range. The range of valid IPs is 10.96.0.0/12`)
	})

	got, err := serviceCIDRsFromDryRunProbe(context.Background(), kube, "yscale")
	if err != nil {
		t.Fatalf("serviceCIDRsFromDryRunProbe: %v", err)
	}
	if want := []string{"10.96.0.0/12"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cidrs = %v, want %v", got, want)
	}
	if gotNamespace != "yscale" {
		t.Fatalf("probe namespace = %q, want yscale", gotNamespace)
	}
}

func TestResolveFailsWhenPodClassUnresolved(t *testing.T) {
	providerDefaults["test-no-pods"] = providerDefault{
		ServiceCIDRs: []string{"10.55.0.0/16"},
		NodeCIDRs:    []string{"10.0.0.0/24"},
	}
	t.Cleanup(func() { delete(providerDefaults, "test-no-pods") })

	_, err := Resolve(context.Background(), fake.NewSimpleClientset(), emptyDynamic(), Options{CloudProvider: "test-no-pods"})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"pod CIDRs", "cloudProvider \"test-no-pods\"", "gateway.advertiseRoutes"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err.Error(), want)
		}
	}
}

func TestResolveFailsWhenNodeClassUnresolved(t *testing.T) {
	providerDefaults["test-no-nodes"] = providerDefault{
		PodCIDRs:     []string{"10.42.0.0/16"},
		ServiceCIDRs: []string{"10.55.0.0/16"},
	}
	t.Cleanup(func() { delete(providerDefaults, "test-no-nodes") })

	_, err := Resolve(context.Background(), fake.NewSimpleClientset(
		node("n1", []string{"10.42.1.0/24"}, "10.0.0.10"),
		node("n2", []string{"10.42.2.0/24"}, "10.0.0.250"),
	), emptyDynamic(), Options{CloudProvider: "test-no-nodes"})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"node CIDRs", "cloudProvider \"test-no-nodes\"", "gateway.advertiseRoutes"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err.Error(), want)
		}
	}
}

func TestParseServiceCIDRFromErrorMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want string
		ok   bool
	}{
		{
			name: "apiserver range",
			msg:  `Service "x" is invalid: spec.clusterIPs[0]: Invalid value: "240.0.0.1": failed to allocate IP 240.0.0.1: the provided IP is not in the valid range. The range of valid IPs is 10.96.0.0/12`,
			want: "10.96.0.0/12",
			ok:   true,
		},
		{
			name: "lowercase message",
			msg:  "spec.clusterIP: Invalid value: the range of valid IPs is fd00:10:96::/112",
			want: "fd00:10:96::/112",
			ok:   true,
		},
		{
			name: "no cidr",
			msg:  "some other validation error",
			ok:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseServiceCIDRFromErrorMessage(tt.msg)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("parse = (%q, %v), want (%q, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestResolveErrorsWhenClassCannotResolve(t *testing.T) {
	_, err := Resolve(context.Background(), fake.NewSimpleClientset(), emptyDynamic(), Options{CloudProvider: "mystery"})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"service CIDRs", "cloudProvider \"mystery\"", "gateway.advertiseRoutes"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err.Error(), want)
		}
	}
}

func node(name string, podCIDRs []string, internalIP string) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.NodeSpec{
			PodCIDRs: podCIDRs,
		},
	}
	if internalIP != "" {
		n.Status.Addresses = []corev1.NodeAddress{{
			Type:    corev1.NodeInternalIP,
			Address: internalIP,
		}}
	}
	return n
}

func burstNode(name string, podCIDRs []string, internalIP string) *corev1.Node {
	n := node(name, podCIDRs, internalIP)
	n.Labels = map[string]string{burstNodeLabel: "true"}
	return n
}

func serviceCIDR(name string, cidrs ...string) *unstructured.Unstructured {
	cidrValues := make([]any, 0, len(cidrs))
	for _, cidr := range cidrs {
		cidrValues = append(cidrValues, cidr)
	}
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "networking.k8s.io/v1",
			"kind":       "ServiceCIDR",
			"metadata": map[string]any{
				"name": name,
			},
			"spec": map[string]any{
				"cidrs": cidrValues,
			},
		},
	}
}

func emptyDynamic() *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			serviceCIDRGVRs[0]: "ServiceCIDRList",
			serviceCIDRGVRs[1]: "ServiceCIDRList",
		},
	)
}
