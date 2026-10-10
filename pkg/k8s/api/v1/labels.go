package v1

import (
	"fmt"
	"strconv"

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

// Stable yscale.sh label keys for node and workload identity. These are
// the contract kubectl, kstatus, and customer selectors are written
// against — values are bounded, safe for K8s labels, and never carry
// provider account IDs, secrets, or raw upstream payloads.
const (
	// LabelBurstID identifies which burst a node or workload belongs to.
	LabelBurstID = "yscale.sh/burst-id"

	// LabelClusterID identifies the customer cluster a burst node joined. The
	// connector uses it to avoid reporting another installation's nodes when
	// several connectors share one Kubernetes API server.
	LabelClusterID = "yscale.sh/cluster-id"

	// LabelProviderClass is the stable backend family
	// (linode/aws/gcp/azure/flyio). Never an account ID.
	LabelProviderClass = "yscale.sh/provider-class"

	// LabelRegion is the provider region the burst was placed in.
	LabelRegion = "yscale.sh/region"

	// LabelGPUKind is the admitted GPU shape (l4, a100, h100, etc.).
	// GPUReady, not this label, proves that hardware was observed.
	LabelGPUKind = "yscale.sh/gpu-kind"

	// LabelGPUCount is the admitted GPU count, as a decimal string.
	LabelGPUCount = "yscale.sh/gpu-count"

	// LabelNvidiaGPUPresent marks a node as carrying an NVIDIA GPU.
	LabelNvidiaGPUPresent = "nvidia.com/gpu.present"

	// LabelNvidiaGPUProduct is the hardware-observed NVIDIA model using the
	// standard GPU Feature Discovery label key. Placement metadata must never
	// populate this label.
	LabelNvidiaGPUProduct = "nvidia.com/gpu.product"

	// LabelWorkloadName is the workload CR name. Already used by
	// translate.go on the rendered Job/Pod.
	LabelWorkloadName = "yscale.sh/workload"

	// LabelWorkloadID is the central-assigned workload identifier.
	LabelWorkloadID = "yscale.sh/workload-id"
)

// MaxLabelValueLen is the K8s maximum for a label value.
const MaxLabelValueLen = 63

// MaxGPUCount bounds the gpu-count label to a sensible maximum.
// Current providers top out at 8 GPUs per node.
const MaxGPUCount = 64

// ValidateLabelKey checks whether k is a legal qualified Kubernetes label key.
func ValidateLabelKey(k string) error {
	if errs := k8svalidation.IsQualifiedName(k); len(errs) > 0 {
		return fmt.Errorf("label key %q: %s", k, errs[0])
	}
	return nil
}

// ValidateLabelValue checks whether v is a legal K8s label value using
// the canonical Kubernetes validation. Empty is valid per the K8s spec.
func ValidateLabelValue(v string) error {
	if errs := k8svalidation.IsValidLabelValue(v); len(errs) > 0 {
		return fmt.Errorf("label value %q: %s", v, errs[0])
	}
	return nil
}

// ValidateGPUCountLabel checks that v is a valid gpu-count label: a
// decimal integer in [1, MaxGPUCount].
func ValidateGPUCountLabel(v string) error {
	n, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("gpu-count label %q is not an integer", v)
	}
	if n < 1 || n > MaxGPUCount {
		return fmt.Errorf("gpu-count label %q out of range [1, %d]", v, MaxGPUCount)
	}
	return nil
}

// GPUCountLabelValue formats a GPU count as the label value string.
func GPUCountLabelValue(count int) string {
	if count == 0 {
		return "1"
	}
	return strconv.Itoa(count)
}
