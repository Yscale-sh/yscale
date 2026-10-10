package agent

import (
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	k8sv1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/workload"
)

const (
	pendingPodRejectMissingBurstToleration = "missing_burst_toleration"
	pendingPodRejectUnsupportedNodeName    = "unsupported_node_name"
	pendingPodRejectUnsupportedAffinity    = "unsupported_required_affinity"
	pendingPodRejectUnsupportedSelector    = "unsupported_node_selector"
	pendingPodRejectSelectorConflict       = "node_selector_conflicts_with_template"
)

// pendingPodConstraintRejection returns a stable reason when hard Pod
// scheduling constraints cannot be honored by the capacity request derived
// from spec. An empty result means the supported constraints are compatible.
func pendingPodConstraintRejection(pod *corev1.Pod, spec workload.Spec) string {
	if pod.Spec.NodeName != "" {
		return pendingPodRejectUnsupportedNodeName
	}
	if !toleratesBurstNode(pod.Spec.Tolerations) {
		return pendingPodRejectMissingBurstToleration
	}
	if hasRequiredAffinity(pod.Spec.Affinity) {
		return pendingPodRejectUnsupportedAffinity
	}
	keys := make([]string, 0, len(pod.Spec.NodeSelector))
	for key := range pod.Spec.NodeSelector {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := pod.Spec.NodeSelector[key]
		if !supportedBurstSelector(key, value, spec) {
			if knownBurstSelector(key) {
				return pendingPodRejectSelectorConflict
			}
			return pendingPodRejectUnsupportedSelector
		}
	}
	return ""
}

func toleratesBurstNode(tolerations []corev1.Toleration) bool {
	for _, toleration := range tolerations {
		if toleration.Key != workload.LabelBurstNode ||
			(toleration.Effect != "" && toleration.Effect != corev1.TaintEffectNoSchedule) {
			continue
		}
		switch toleration.Operator {
		case corev1.TolerationOpExists:
			return true
		case "", corev1.TolerationOpEqual:
			if toleration.Value == "true" {
				return true
			}
		}
	}
	return false
}

func hasRequiredAffinity(affinity *corev1.Affinity) bool {
	if affinity == nil {
		return false
	}
	if affinity.NodeAffinity != nil && affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		for _, term := range affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			if len(term.MatchExpressions) > 0 || len(term.MatchFields) > 0 {
				return true
			}
		}
	}
	if affinity.PodAffinity != nil && len(affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0 {
		return true
	}
	return affinity.PodAntiAffinity != nil && len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0
}

func knownBurstSelector(key string) bool {
	switch key {
	case workload.LabelBurstNode,
		k8sv1.LabelNvidiaGPUPresent,
		k8sv1.LabelGPUKind,
		k8sv1.LabelGPUCount,
		k8sv1.LabelProviderClass,
		k8sv1.LabelRegion,
		corev1.LabelOSStable,
		corev1.LabelArchStable:
		return true
	default:
		return false
	}
}

func supportedBurstSelector(key, value string, spec workload.Spec) bool {
	switch key {
	case workload.LabelBurstNode:
		return value == "true"
	case k8sv1.LabelNvidiaGPUPresent:
		return value == "true" && spec.GPU != nil
	case k8sv1.LabelGPUKind:
		return spec.GPU != nil && spec.GPU.Kind != "" && value == spec.GPU.Kind
	case k8sv1.LabelGPUCount:
		if spec.GPU == nil {
			return false
		}
		return value == k8sv1.GPUCountLabelValue(spec.GPU.Count)
	case k8sv1.LabelProviderClass:
		backend := strings.TrimSpace(spec.Backend)
		return backend != "" && backend != "auto" && value == backend
	case k8sv1.LabelRegion:
		return spec.Region != "" && value == spec.Region
	case corev1.LabelOSStable:
		return value == "linux"
	case corev1.LabelArchStable:
		return value == "amd64"
	default:
		return false
	}
}
