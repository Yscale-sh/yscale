package clusterroutes

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// Routes are the customer-cluster CIDRs that full-tier bursts must route
// through the gateway.
type Routes struct {
	// OverrideCIDRs are explicit operator-provided routes. They are kept out of
	// the detected class fields so All() can preserve the "override wins"
	// contract without misclassifying them as pod, service, or node routes.
	OverrideCIDRs []string
	PodCIDRs      []string
	ServiceCIDRs  []string
	NodeCIDRs     []string
}

// All returns the deduplicated, sorted, MASKED union of all route classes.
//
// Every path is masked here, including the operator Override path: central keys
// autoApprovers.routes by exact masked string, so an unmasked advertised prefix
// (e.g. "10.42.0.1/16") would never match central's masked approver key
// ("10.42.0.0/16") and would be silently un-approved. The detected pod/service/
// node paths are already masked by normalizeCIDRs; masking again here is
// idempotent and yields one canonical form the caller uses for BOTH the
// ConfigMap write and the central report (byte-for-byte identical).
func (r Routes) All() []string {
	all := make([]string, 0, len(r.OverrideCIDRs)+len(r.PodCIDRs)+len(r.ServiceCIDRs)+len(r.NodeCIDRs))
	all = append(all, r.OverrideCIDRs...)
	all = append(all, r.PodCIDRs...)
	all = append(all, r.ServiceCIDRs...)
	all = append(all, r.NodeCIDRs...)
	return maskSorted(all)
}

// maskSorted parses, masks, dedups, and sorts CIDRs. Entries that fail to parse
// are preserved verbatim (rather than silently dropped) so a malformed operator
// override surfaces downstream — central's fail-closed validator will reject the
// whole report, which is the intended safe behavior.
func maskSorted(in []string) []string {
	out := make([]string, 0, len(in))
	for _, raw := range in {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(raw)
		if err != nil {
			out = append(out, raw)
			continue
		}
		out = append(out, prefix.Masked().String())
	}
	return dedupSorted(out)
}

func (r Routes) Empty() bool {
	return len(r.OverrideCIDRs) == 0 && len(r.PodCIDRs) == 0 && len(r.ServiceCIDRs) == 0 && len(r.NodeCIDRs) == 0
}

type Options struct {
	CloudProvider         string
	Override              []string
	ServiceProbeNamespace string
}

type providerDefault struct {
	PodCIDRs     []string
	ServiceCIDRs []string
	NodeCIDRs    []string
}

// providerDefaults mirrors defaults already encoded elsewhere in this repo:
// deploy/helm/yscale-agent/templates/_helpers.tpl maps cloudProvider to the
// default CoreDNS ClusterIP, which anchors the service CIDRs below; and
// central/cmd/yscale-cloud/main.go's defaultcoordination serverPolicy* CIDRs provide the
// current k3s/LKE-style pod/node baseline (10.42.0.0/16, 10.0.0.0/24).
// Providers whose pod/node CIDRs are VPC- or cluster-specific intentionally do
// not get guessed here; Resolve will fail loud unless runtime detection finds
// them or Options.Override is set.
var providerDefaults = map[string]providerDefault{
	"k3s": {
		PodCIDRs:     []string{"10.42.0.0/16"},
		ServiceCIDRs: []string{"10.43.0.0/16"},
		NodeCIDRs:    []string{"10.0.0.0/24"},
	},
	"linode": {
		PodCIDRs:     []string{"10.42.0.0/16"},
		ServiceCIDRs: []string{"10.96.0.0/12"},
		NodeCIDRs:    []string{"10.0.0.0/24"},
	},
	"self-managed": {
		PodCIDRs:     []string{"10.42.0.0/16"},
		ServiceCIDRs: []string{"10.96.0.0/12"},
		NodeCIDRs:    []string{"10.0.0.0/24"},
	},
	"aws": {
		ServiceCIDRs: []string{"172.20.0.0/16"},
	},
}

var serviceCIDRGVRs = []schema.GroupVersionResource{
	{Group: "networking.k8s.io", Version: "v1", Resource: "servicecidrs"},
	{Group: "networking.k8s.io", Version: "v1beta1", Resource: "servicecidrs"},
}

const burstNodeLabel = "yscale.sh/burst-node"

// Resolve implements provider-anchored hybrid route detection. It never returns
// a partially covered route set: each required CIDR class must come from exact
// runtime detection, an explicit provider-table default, or Options.Override.
func Resolve(ctx context.Context, kube kubernetes.Interface, dyn dynamic.Interface, opts Options) (Routes, error) {
	if len(opts.Override) > 0 {
		return Routes{OverrideCIDRs: append([]string(nil), opts.Override...)}, nil
	}

	provider := strings.ToLower(strings.TrimSpace(opts.CloudProvider))
	table, hasTable := providerDefaults[provider]

	detectedServiceCIDRs, serviceErr := resolveServiceCIDRs(ctx, kube, dyn, opts.ServiceProbeNamespace)
	serviceCIDRs := detectedServiceCIDRs
	if len(serviceCIDRs) > 0 && len(table.ServiceCIDRs) > 0 {
		if err := validateCIDRsCovered("service CIDRs", provider, serviceCIDRs, table.ServiceCIDRs); err != nil {
			return Routes{}, err
		}
	} else if len(serviceCIDRs) == 0 && hasTable {
		serviceCIDRs = copyStrings(table.ServiceCIDRs)
	}
	if len(serviceCIDRs) == 0 {
		return Routes{}, classError("service CIDRs", provider, serviceErr)
	}

	detectedPodCIDRs, nodeIPs, nodeObservationErr := resolveNodeNetworkObservations(ctx, kube)
	if nodeObservationErr != nil {
		return Routes{}, classError("pod CIDRs and node InternalIPs", provider, nodeObservationErr)
	}
	var podCIDRs []string
	if len(table.PodCIDRs) > 0 {
		if err := validateCIDRsCovered("pod CIDRs", provider, detectedPodCIDRs, table.PodCIDRs); err != nil {
			return Routes{}, err
		}
		// The observed PodCIDRs are per-node allocations. Once validated, keep
		// advertising the provider aggregate so later scale-out remains routed.
		podCIDRs = copyStrings(table.PodCIDRs)
	} else {
		podCIDRs = detectedPodCIDRs
	}
	if len(podCIDRs) == 0 {
		return Routes{}, classError("pod CIDRs", provider, nil)
	}

	var nodeCIDRs []string
	var nodeErr error
	if len(table.NodeCIDRs) > 0 {
		if err := validateIPsCovered("node CIDRs", provider, nodeIPs, table.NodeCIDRs); err != nil {
			return Routes{}, err
		}
		nodeCIDRs = copyStrings(table.NodeCIDRs)
	} else {
		nodeCIDRs, nodeErr = resolveNodeCIDRs(ctx, kube)
	}
	if len(nodeCIDRs) == 0 {
		return Routes{}, classError("node CIDRs", provider, nodeErr)
	}

	return Routes{
		PodCIDRs:     podCIDRs,
		ServiceCIDRs: serviceCIDRs,
		NodeCIDRs:    nodeCIDRs,
	}, nil
}

func resolveServiceCIDRs(ctx context.Context, kube kubernetes.Interface, dyn dynamic.Interface, probeNamespace string) ([]string, error) {
	var errs []error
	if dyn != nil {
		cidrs, err := serviceCIDRsFromAPI(ctx, dyn)
		if len(cidrs) > 0 {
			return cidrs, nil
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	if kube != nil {
		cidrs, err := serviceCIDRsFromDryRunProbe(ctx, kube, probeNamespace)
		if len(cidrs) > 0 {
			return cidrs, nil
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return nil, errors.Join(errs...)
}

func serviceCIDRsFromAPI(ctx context.Context, dyn dynamic.Interface) ([]string, error) {
	var errs []error
	for _, gvr := range serviceCIDRGVRs {
		list, err := dyn.Resource(gvr).List(ctx, metav1.ListOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			errs = append(errs, fmt.Errorf("%s: %w", gvr.GroupVersion().String(), err))
			continue
		}
		var cidrs []string
		for _, item := range list.Items {
			itemCIDRs, found, err := unstructured.NestedStringSlice(item.Object, "spec", "cidrs")
			if err != nil {
				return nil, fmt.Errorf("read %s ServiceCIDR %q spec.cidrs: %w", gvr.GroupVersion().String(), item.GetName(), err)
			}
			if found {
				cidrs = append(cidrs, itemCIDRs...)
			}
		}
		if len(cidrs) > 0 {
			return normalizeCIDRs(cidrs)
		}
	}
	return nil, errors.Join(errs...)
}

func serviceCIDRsFromDryRunProbe(ctx context.Context, kube kubernetes.Interface, namespace string) ([]string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "yscale-service-cidr-probe-",
		},
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: "240.0.0.1",
			Ports: []corev1.ServicePort{{
				Port:     1,
				Protocol: corev1.ProtocolTCP,
			}},
		},
	}
	_, err := kube.CoreV1().Services(namespace).Create(ctx, svc, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err == nil {
		return nil, nil
	}
	if cidr, ok := parseServiceCIDRFromErrorMessage(err.Error()); ok {
		return []string{cidr}, nil
	}
	return nil, err
}

func resolveNodeNetworkObservations(ctx context.Context, kube kubernetes.Interface) ([]string, []string, error) {
	if kube == nil {
		return nil, nil, nil
	}
	nodes, err := kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, err
	}
	var podCIDRs []string
	var nodeIPs []string
	for _, node := range nodes.Items {
		// Burst nodes are remote consumers of these routes, not part of the
		// customer-side network being advertised through the gateway.
		if strings.EqualFold(strings.TrimSpace(node.Labels[burstNodeLabel]), "true") {
			continue
		}
		podCIDRs = append(podCIDRs, node.Spec.PodCIDRs...)
		if len(node.Spec.PodCIDRs) == 0 && node.Spec.PodCIDR != "" {
			podCIDRs = append(podCIDRs, node.Spec.PodCIDR)
		}
		for _, address := range node.Status.Addresses {
			if address.Type != corev1.NodeInternalIP || strings.TrimSpace(address.Address) == "" {
				continue
			}
			ip, err := netip.ParseAddr(strings.TrimSpace(address.Address))
			if err != nil {
				return nil, nil, fmt.Errorf("node %q has invalid InternalIP %q: %w", node.Name, address.Address, err)
			}
			nodeIPs = append(nodeIPs, ip.String())
		}
	}
	if len(podCIDRs) == 0 {
		return nil, dedupSorted(nodeIPs), nil
	}
	normalizedPodCIDRs, err := normalizeCIDRs(podCIDRs)
	if err != nil {
		return nil, nil, err
	}
	return normalizedPodCIDRs, dedupSorted(nodeIPs), nil
}

func resolveNodeCIDRs(ctx context.Context, kube kubernetes.Interface) ([]string, error) {
	if kube == nil {
		return nil, nil
	}
	return nil, errors.New("no exact runtime node CIDR source is available")
}

func validateCIDRsCovered(class, provider string, observed, defaults []string) error {
	for _, raw := range observed {
		observedPrefix, err := netip.ParsePrefix(raw)
		if err != nil {
			return fmt.Errorf("resolve %s: invalid detected CIDR %q: %w", class, raw, err)
		}
		covered := false
		for _, defaultRaw := range defaults {
			defaultPrefix, err := netip.ParsePrefix(defaultRaw)
			if err != nil {
				return fmt.Errorf("resolve %s: invalid cloudProvider %q default %q: %w", class, provider, defaultRaw, err)
			}
			if defaultPrefix.Addr().BitLen() == observedPrefix.Addr().BitLen() &&
				defaultPrefix.Bits() <= observedPrefix.Bits() &&
				defaultPrefix.Contains(observedPrefix.Addr()) {
				covered = true
				break
			}
		}
		if !covered {
			return routeMismatchError(class, provider, raw, defaults)
		}
	}
	return nil
}

func validateIPsCovered(class, provider string, observed, defaults []string) error {
	for _, raw := range observed {
		ip, err := netip.ParseAddr(raw)
		if err != nil {
			return fmt.Errorf("resolve %s: invalid detected InternalIP %q: %w", class, raw, err)
		}
		covered := false
		for _, defaultRaw := range defaults {
			defaultPrefix, err := netip.ParsePrefix(defaultRaw)
			if err != nil {
				return fmt.Errorf("resolve %s: invalid cloudProvider %q default %q: %w", class, provider, defaultRaw, err)
			}
			if defaultPrefix.Contains(ip) {
				covered = true
				break
			}
		}
		if !covered {
			return routeMismatchError(class, provider, raw, defaults)
		}
	}
	return nil
}

func routeMismatchError(class, provider, observed string, defaults []string) error {
	return fmt.Errorf(
		"resolve %s: detected %q is outside cloudProvider %q defaults %v; set gateway.advertiseRoutes explicitly for custom cluster CIDRs",
		class,
		observed,
		provider,
		defaults,
	)
}

var validIPsRE = regexp.MustCompile(`(?i)valid IPs is\s+([0-9A-Fa-f:.]+/\d{1,3})`)

func parseServiceCIDRFromErrorMessage(msg string) (string, bool) {
	match := validIPsRE.FindStringSubmatch(msg)
	if len(match) != 2 {
		return "", false
	}
	prefix, err := netip.ParsePrefix(match[1])
	if err != nil {
		return "", false
	}
	return prefix.Masked().String(), true
}

func normalizeCIDRs(in []string) ([]string, error) {
	out := make([]string, 0, len(in))
	for _, raw := range in {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q: %w", raw, err)
		}
		out = append(out, prefix.Masked().String())
	}
	return dedupSorted(out), nil
}

func dedupSorted(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, raw := range in {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if _, ok := seen[raw]; ok {
			continue
		}
		seen[raw] = struct{}{}
		out = append(out, raw)
	}
	sort.Strings(out)
	return out
}

func copyStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	return append([]string(nil), in...)
}

func classError(class, provider string, cause error) error {
	providerMsg := "cloudProvider is empty or unknown"
	if provider != "" {
		providerMsg = fmt.Sprintf("cloudProvider %q has no provider-table default for this class", provider)
	}
	msg := fmt.Sprintf("resolve %s: runtime detection found no exact CIDRs and %s; set gateway.advertiseRoutes", class, providerMsg)
	if cause != nil {
		return fmt.Errorf("%s: %w", msg, cause)
	}
	return errors.New(msg)
}
