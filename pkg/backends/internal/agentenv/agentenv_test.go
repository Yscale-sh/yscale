package agentenv

import (
	"reflect"
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
)

func TestBuildK3sMode(t *testing.T) {
	got := Build(&backends.NodeSpec{
		Name:         "ys-burst-abcd",
		BurstID:      "burst_abcd",
		JoinMode:     backends.JoinModeK3s,
		K3sServerURL: "https://k3s.tail-scale.ts.net:6443",
		K3sToken:     "K10::token",
		TSAuthKey:    "tskey-auth-x",
		TSHostname:   "ys-ys-burst-abcd",
	})

	want := []KV{
		{"TS_AUTHKEY", "tskey-auth-x"},
		{"TS_HOSTNAME", "ys-ys-burst-abcd"},
		{"NODE_NAME", "ys-burst-abcd"},
		{"BURST_ID", "burst_abcd"},
		{"K3S_URL", "https://k3s.tail-scale.ts.net:6443"},
		{"K3S_TOKEN", "K10::token"},
		{"K3S_NODE_NAME", "ys-burst-abcd"},
		{"BURST_TIER", "full"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("k3s env mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestBuildKubeletMode(t *testing.T) {
	got := Build(&backends.NodeSpec{
		Name:              "ys-burst-1",
		BurstID:           "burst_1",
		JoinMode:          backends.JoinModeKubelet,
		APIServer:         "https://eks.example.com",
		ClusterCA:         "BASE64==",
		BootstrapToken:    "abcdef.0123456789abcdef", // fake fixture, kubeadm token format — gitleaks:allow
		BootstrapEndpoint: "http://yscale-agent-cl1.tail-xyz.ts.net:8080",
		TSAuthKey:         "k",
		TSHostname:        "h",
	})

	want := []KV{
		{"TS_AUTHKEY", "k"},
		{"TS_HOSTNAME", "h"},
		{"NODE_NAME", "ys-burst-1"},
		{"BURST_ID", "burst_1"},
		{"API_SERVER", "https://eks.example.com"},
		{"CLUSTER_CA", "BASE64=="},
		{"BOOTSTRAP_TOKEN", "abcdef.0123456789abcdef"}, // gitleaks:allow
		{"BOOTSTRAP_ENDPOINT", "http://yscale-agent-cl1.tail-xyz.ts.net:8080"},
		{"BURST_TIER", "full"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("kubelet env mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestBuildTierFull(t *testing.T) {
	got := Build(&backends.NodeSpec{
		Name:       "ys-burst-x",
		BurstID:    "burst_x",
		JoinMode:   backends.JoinModeK3s,
		TSAuthKey:  "k",
		TSHostname: "h",
		Tier:       "full",
	})
	// Tier=full should emit BURST_TIER=full at the end.
	last := got[len(got)-1]
	if last.Key != "BURST_TIER" || last.Value != "full" {
		t.Fatalf("expected BURST_TIER=full at tail, got %#v", last)
	}
}

func TestBuildTierUnsetDefaultsFull(t *testing.T) {
	got := Build(&backends.NodeSpec{
		Name:       "ys-burst-x",
		BurstID:    "burst_x",
		JoinMode:   backends.JoinModeK3s,
		TSAuthKey:  "k",
		TSHostname: "h",
		// Tier intentionally unset
	})
	last := got[len(got)-1]
	if last.Key != "BURST_TIER" || last.Value != "full" {
		t.Fatalf("expected BURST_TIER=full default at tail, got %#v", last)
	}
}

func TestBuildNodeLabelsDeterministic(t *testing.T) {
	// Map iteration order is non-deterministic; the helper must sort
	// label pairs so the resulting string is stable.
	spec := &backends.NodeSpec{
		Name:     "ys-burst-x",
		JoinMode: backends.JoinModeK3s,
		NodeLabels: map[string]string{
			"yscale.sh/burst-node": "true",
			"a":                    "1",
			"z":                    "9",
			"m":                    "5",
		},
	}
	first := Build(spec)
	for range 50 {
		got := Build(spec)
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("Build is non-deterministic across calls\n got: %#v\nfirst: %#v", got, first)
		}
	}

	// And the sort is alphabetical.
	last := first[len(first)-1]
	if last.Key != "K3S_NODE_LABELS" {
		t.Fatalf("expected last KV to be the labels entry, got %q", last.Key)
	}
	want := "a=1,m=5,yscale.sh/burst-node=true,z=9"
	if last.Value != want {
		t.Fatalf("label sort wrong\n got: %s\nwant: %s", last.Value, want)
	}
}

func TestBuildKubeletLabelsUseExtraNodeLabels(t *testing.T) {
	got := Build(&backends.NodeSpec{
		Name:     "ys-burst-x",
		JoinMode: backends.JoinModeKubelet,
		NodeLabels: map[string]string{
			"yscale.sh/burst-node": "true",
		},
	})
	last := got[len(got)-1]
	if last.Key != "EXTRA_NODE_LABELS" {
		t.Fatalf("kubelet mode should use EXTRA_NODE_LABELS, got %q", last.Key)
	}
}

func TestBuildEmptyLabels(t *testing.T) {
	got := Build(&backends.NodeSpec{
		Name:     "ys-burst-x",
		JoinMode: backends.JoinModeK3s,
	})
	for _, kv := range got {
		if kv.Key == "K3S_NODE_LABELS" || kv.Key == "EXTRA_NODE_LABELS" {
			t.Fatalf("no labels were configured but found a labels env var: %#v", kv)
		}
	}
}

func TestBuildTSTagsSortedAndJoined(t *testing.T) {
	got := Build(&backends.NodeSpec{
		Name:     "ys-burst-1",
		JoinMode: backends.JoinModeKubelet,
		TSTags:   []string{"tag:yscale-burst", "tag:customer-acme-burst"},
	})
	// TS_TAGS sits after the kubelet block; find it.
	var found *KV
	for i := range got {
		if got[i].Key == "TS_TAGS" {
			found = &got[i]
		}
	}
	if found == nil {
		t.Fatalf("TS_TAGS missing from env: %#v", got)
	}
	if found.Value != "tag:customer-acme-burst,tag:yscale-burst" {
		t.Fatalf("TS_TAGS not sorted: %q", found.Value)
	}
}

func TestBuildTSTagsAbsentWhenEmpty(t *testing.T) {
	got := Build(&backends.NodeSpec{
		Name:     "ys-burst-1",
		JoinMode: backends.JoinModeK3s,
	})
	for _, kv := range got {
		if kv.Key == "TS_TAGS" {
			t.Fatalf("TS_TAGS present with no tags: %#v", got)
		}
	}
}

func TestBuildLoginServerPresent(t *testing.T) {
	got := Build(&backends.NodeSpec{
		Name:        "ys-burst-1",
		JoinMode:    backends.JoinModeKubelet,
		LoginServer: "https://box.customer.example",
	})
	var found *KV
	for i := range got {
		if got[i].Key == "TS_LOGIN_SERVER" {
			found = &got[i]
		}
	}
	if found == nil || found.Value != "https://box.customer.example" {
		t.Fatalf("TS_LOGIN_SERVER missing or wrong: %#v", got)
	}
}

func TestBuildLoginServerAbsentWhenEmpty(t *testing.T) {
	// SaaS default (no LoginServer): the env set must be byte-for-byte
	// identical to existing bursts, so TS_LOGIN_SERVER must not appear.
	got := Build(&backends.NodeSpec{
		Name:       "ys-burst-1",
		JoinMode:   backends.JoinModeK3s,
		TSAuthKey:  "k",
		TSHostname: "h",
		// LoginServer intentionally unset.
	})
	for _, kv := range got {
		if kv.Key == "TS_LOGIN_SERVER" {
			t.Fatalf("TS_LOGIN_SERVER present with no login server: %#v", got)
		}
	}
}

func TestBuildPodCIDRPresent(t *testing.T) {
	got := Build(&backends.NodeSpec{
		Name:     "ys-burst-1",
		JoinMode: backends.JoinModeKubelet,
		PodCIDR:  "10.244.42.0/24",
	})
	var found *KV
	for i := range got {
		if got[i].Key == "POD_CIDR" {
			found = &got[i]
		}
	}
	if found == nil || found.Value != "10.244.42.0/24" {
		t.Fatalf("POD_CIDR missing or wrong: %#v", got)
	}
}

func TestAsMap(t *testing.T) {
	m := AsMap([]KV{{"A", "1"}, {"B", "2"}})
	if got, want := len(m), 2; got != want {
		t.Fatalf("len: got %d want %d", got, want)
	}
	if m["A"] != "1" || m["B"] != "2" {
		t.Fatalf("AsMap lost data: %#v", m)
	}
}
