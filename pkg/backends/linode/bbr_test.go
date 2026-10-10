package linode

import (
	"strings"
	"testing"
)

// The burst↔cluster data path is high-BDP TCP over a ~32ms WireGuard tunnel; stock cubic +
// small buffers collapse on path loss and cap single-flow throughput (~330 Mbps measured).
// bootstrap-baked.sh installs BBR + fq + large windows at boot. These tests guard that the
// block stays present and well-formed in the embedded script (a silent drop would quietly
// regress source-fetch/segment-write throughput back to the cubic ceiling).

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
			t.Errorf("bootstrap-baked.sh missing BBR directive %q", w)
		}
	}
}

// modprobe must be best-effort (older kernels lack tcp_bbr) so it never aborts the
// `set -euo pipefail` bootstrap.
func TestBootstrapBaked_BBRModprobeIsBestEffort(t *testing.T) {
	if !strings.Contains(bootstrapBakedScript, "modprobe tcp_bbr 2>/dev/null || true") {
		t.Error("modprobe tcp_bbr must be guarded with `2>/dev/null || true` so it can't abort the pipefail bootstrap on kernels without the module")
	}
}

// The BBR block must be applied BEFORE kubelet/cilium start so the node comes up with the
// tuned stack — i.e. it should appear before the kubelet systemd unit is written.
func TestBootstrapBaked_BBRBeforeKubelet(t *testing.T) {
	bbr := strings.Index(bootstrapBakedScript, "tcp_congestion_control=bbr")
	kubelet := strings.Index(bootstrapBakedScript, "yscale-kubelet.service")
	if bbr < 0 || kubelet < 0 {
		t.Skip("markers not found; covered by other tests")
	}
	if bbr > kubelet {
		t.Errorf("BBR sysctl block (idx %d) should be applied before the kubelet unit (idx %d)", bbr, kubelet)
	}
}

// Adding the BBR block must not push the real baked bootstrap's gzipped user_data over
// Linode's cap (regression guard for the size budget).
func TestBootstrapBaked_RealUserDataUnderLimit(t *testing.T) {
	ud := buildUserData(map[string]string{
		"TS_AUTHKEY":         "tskey-auth-xxxxxxxxxxxxxxxxxxxx",
		"TS_HOSTNAME":        "yscale-burst-abc123",
		"NODE_NAME":          "ys-burst-abc123",
		"BURST_ID":           "burst_abc123",
		"POD_CIDR":           "10.244.7.0/24",
		"BOOTSTRAP_ENDPOINT": "http://yscale-agent-cust-test-1:8080",
		"BURST_TIER":         "full",
	}, bootstrapBakedScript)
	enc, err := encodeUserData(ud)
	if err != nil {
		t.Fatalf("real baked bootstrap user_data over limit (BBR block too big?): %v", err)
	}
	if enc == "" {
		t.Fatal("empty encoded user_data")
	}
}
