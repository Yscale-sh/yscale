package backends

import "testing"

func TestResolvedGPUProduct(t *testing.T) {
	tests := []struct {
		provider string
		sku      string
		want     string
	}{
		{TypeLinode, "g2-gpu-rtx4000a1-s", "NVIDIA RTX 4000 Ada"},
		{TypeLinode, "g2-gpu-rtx4000a4-m", "NVIDIA RTX 4000 Ada"},
		{TypeLinode, "g1-gpu-rtx6000-2", "NVIDIA Quadro RTX 6000"},
		{TypeAWS, "g4dn.xlarge", "NVIDIA T4"},
		{TypeAWS, "g5.48xlarge", "NVIDIA A10G"},
		{TypeAWS, "g6.12xlarge", "NVIDIA L4"},
		{TypeAWS, "g6e.xlarge", "NVIDIA L40S"},
		{TypeAWS, "p4d.24xlarge", "NVIDIA A100 40GB"},
		{TypeAWS, "p5.48xlarge", "NVIDIA H100 80GB"},
		{TypeAWS, "p5e.48xlarge", "NVIDIA H200 141GB"},
		{TypeLinode, "g6-standard-2", ""},
		{TypeAWS, "t3.medium", ""},
		{TypeGCP, "n1-standard-4", ""},
		{"unknown", "g6.xlarge", ""},
	}
	for _, tt := range tests {
		t.Run(tt.provider+"/"+tt.sku, func(t *testing.T) {
			if got := ResolvedGPUProduct(tt.provider, tt.sku); got != tt.want {
				t.Fatalf("ResolvedGPUProduct(%q, %q) = %q, want %q", tt.provider, tt.sku, got, tt.want)
			}
		})
	}
}
