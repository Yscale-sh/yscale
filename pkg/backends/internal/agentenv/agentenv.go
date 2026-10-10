// Package agentenv builds the env-var set the burst-node agent image
// consumes. Centralised so adding a new env var (or changing the K3s vs
// kubelet contract) is a single edit instead of three.
package agentenv

import (
	"sort"
	"strings"

	"github.com/yscale-sh/yscale/pkg/backends"
)

// KV is one env var. Backends translate to their own native shape (a map,
// a struct, a slice) with a trivial loop.
type KV struct {
	Key, Value string
}

// NetworkingTierFull is the only supported BURST_TIER value. Duplicated
// here rather than imported from pkg/workload to avoid an import cycle
// (workload → backends → agentenv, and workload also defines the same
// constant for spec-side callers).
const NetworkingTierFull = "full"

// Build returns the canonical env vars the agent image consumes for this
// NodeSpec. Order is deterministic so backend-side hashing/diffing is
// stable across calls.
func Build(spec *backends.NodeSpec) []KV {
	env := []KV{
		{"TS_AUTHKEY", spec.TSAuthKey},
		{"TS_HOSTNAME", spec.TSHostname},
		{"NODE_NAME", spec.Name},
		{"BURST_ID", spec.BurstID},
	}

	switch spec.JoinMode {
	case backends.JoinModeKubelet:
		env = append(env,
			KV{"API_SERVER", spec.APIServer},
			KV{"CLUSTER_CA", spec.ClusterCA},
			KV{"BOOTSTRAP_TOKEN", spec.BootstrapToken},
			KV{"BOOTSTRAP_ENDPOINT", spec.BootstrapEndpoint},
		)
	default: // k3s
		env = append(env,
			KV{"K3S_URL", spec.K3sServerURL},
			KV{"K3S_TOKEN", spec.K3sToken},
			KV{"K3S_NODE_NAME", spec.Name},
		)
	}

	if len(spec.TSTags) > 0 {
		// Sorted for deterministic output.
		tags := append([]string(nil), spec.TSTags...)
		sort.Strings(tags)
		env = append(env, KV{"TS_TAGS", strings.Join(tags, ",")})
	}
	// LoginServer points 'tailscale up' at a self-hosted coordination box.
	// Only emit when set so existing SaaS bursts get an identical env set
	// (preserves the deterministic ordering/contents above).
	if spec.LoginServer != "" {
		env = append(env, KV{"TS_LOGIN_SERVER", spec.LoginServer})
	}
	if spec.PodCIDR != "" {
		env = append(env, KV{"POD_CIDR", spec.PodCIDR})
	}
	// BURST_TIER tells the burst entrypoint which CNI path to take. Full
	// (host-mode cilium-agent) is the only supported tier — the "lite"
	// bridge-CNI path has been removed and workload admission refuses it —
	// so we always emit "full". The variable stays on the wire so an older
	// burst rootfs that still branches on it stays deterministic instead of
	// depending on its own default.
	env = append(env, KV{"BURST_TIER", NetworkingTierFull})

	// MODEL_VOLUME tells the bootstrap to wait for and mount a pre-seeded
	// Block Storage volume (attached by linode.CreateNode). Emitted only
	// when requested so non-model bursts keep an identical, faster boot
	// path with no device-wait.
	if spec.ModelVolume != "" {
		env = append(env, KV{"MODEL_VOLUME", spec.ModelVolume})
	}

	if len(spec.NodeLabels) > 0 {
		// Sort label pairs so the resulting comma-separated string is
		// deterministic regardless of map iteration order.
		pairs := make([]string, 0, len(spec.NodeLabels))
		for k, v := range spec.NodeLabels {
			pairs = append(pairs, k+"="+v)
		}
		sort.Strings(pairs)

		labelKey := "K3S_NODE_LABELS"
		if spec.JoinMode == backends.JoinModeKubelet {
			labelKey = "EXTRA_NODE_LABELS"
		}
		env = append(env, KV{labelKey, strings.Join(pairs, ",")})
	}

	return env
}

// AsMap is a convenience for backends whose API takes a map[string]string.
func AsMap(kvs []KV) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}
	return m
}
