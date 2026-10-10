package backends

import "strings"

// ResolvedGPUProduct returns the customer-facing GPU model guaranteed by an
// exact provider SKU. It is placement metadata, not a hardware observation;
// unknown and CPU SKUs intentionally return empty rather than guessing.
func ResolvedGPUProduct(provider, sku string) string {
	sku = strings.ToLower(strings.TrimSpace(sku))
	switch provider {
	case TypeLinode:
		switch {
		case strings.HasPrefix(sku, "g2-gpu-rtx4000a"):
			return "NVIDIA RTX 4000 Ada"
		case strings.HasPrefix(sku, "g1-gpu-rtx6000-"):
			return "NVIDIA Quadro RTX 6000"
		}
	case TypeAWS:
		switch {
		case strings.HasPrefix(sku, "g4dn."):
			return "NVIDIA T4"
		case strings.HasPrefix(sku, "g5."):
			return "NVIDIA A10G"
		case strings.HasPrefix(sku, "g6."):
			return "NVIDIA L4"
		case strings.HasPrefix(sku, "g6e."):
			return "NVIDIA L40S"
		case strings.HasPrefix(sku, "p4d."):
			return "NVIDIA A100 40GB"
		case strings.HasPrefix(sku, "p5."):
			return "NVIDIA H100 80GB"
		case strings.HasPrefix(sku, "p5e."):
			return "NVIDIA H200 141GB"
		}
	}
	return ""
}
