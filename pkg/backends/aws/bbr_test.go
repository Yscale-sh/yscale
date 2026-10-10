package aws

import (
	"strings"
	"testing"
)

// Mirror of the linode BBR guard: the AWS baked bootstrap must also install BBR + fq + large
// TCP windows so burst↔cluster throughput isn't capped by cubic on the high-BDP WG path.
// A silent drop of this block quietly regresses source-fetch/segment-write performance.

func TestBootstrapBaked_BBRBlockPresent(t *testing.T) {
	want := []string{
		"modprobe tcp_bbr",
		"net.core.default_qdisc=fq",
		"net.ipv4.tcp_congestion_control=bbr",
		"net.core.rmem_max=67108864",
		"net.core.wmem_max=67108864",
		"net.ipv4.tcp_rmem=",
		"net.ipv4.tcp_wmem=",
		"/etc/sysctl.d/99-yscale-bbr.conf",
	}
	for _, w := range want {
		if !strings.Contains(bootstrapBakedScript, w) {
			t.Errorf("aws bootstrap-baked.sh missing BBR directive %q", w)
		}
	}
}

func TestBootstrapBaked_BBRModprobeBestEffort(t *testing.T) {
	if !strings.Contains(bootstrapBakedScript, "modprobe tcp_bbr 2>/dev/null || true") {
		t.Error("modprobe tcp_bbr must be best-effort (`2>/dev/null || true`)")
	}
}

// BBR block should be applied before the kubelet unit so the node registers with the tuned
// stack.
func TestBootstrapBaked_BBRBeforeKubelet(t *testing.T) {
	bbr := strings.Index(bootstrapBakedScript, "tcp_congestion_control=bbr")
	kubelet := strings.LastIndex(bootstrapBakedScript, "kubelet")
	if bbr < 0 || kubelet < 0 {
		t.Skip("markers not found")
	}
	if bbr > kubelet {
		t.Errorf("BBR block (idx %d) should precede kubelet setup (idx %d)", bbr, kubelet)
	}
}
