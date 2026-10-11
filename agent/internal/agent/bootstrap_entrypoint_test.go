package agent

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBootstrapEntrypointReboot(t *testing.T) {
	raw, err := os.ReadFile("../../../burst/image/entrypoint-kubelet.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	start := strings.Index(script, "mkdir -p /etc/kubernetes")
	end := strings.Index(script, "# Write a bridge CNI conflist")
	if start < 0 || end <= start {
		t.Fatal("bootstrap section not found")
	}
	dir := t.TempDir()
	section := strings.NewReplacer("/etc/kubernetes", dir+"/kubernetes", "/etc/cilium", dir+"/cilium", "/var/lib/kubelet", dir+"/kubelet").Replace(script[start:end])
	response, err := json.Marshal(map[string]string{
		"kubeconfig":        base64.StdEncoding.EncodeToString([]byte("kubelet-config")),
		"cilium_kubeconfig": base64.StdEncoding.EncodeToString([]byte("cilium-config")),
		"cloud_provider":    "gcp",
	})
	if err != nil {
		t.Fatal(err)
	}
	run := func(joined bool, standalone bool) {
		t.Helper()
		cmd := exec.Command("bash", "-c", `set -euo pipefail
log_t() { :; }
curl() {
 if [ "$JOINED" = 1 ] || [ "$STANDALONE" = 1 ]; then echo unexpected-bootstrap >&2; return 1; fi
 printf '%s' "$RESPONSE"
}
`+section+`
if [ "$STANDALONE" = 0 ]; then
 test "$PROVIDER_ID" = "linode://ys-burst-aaaa"
 test "$HOST_CILIUM" = 1
else
 test "$HOST_CILIUM" = 0
fi
`)
		joinedFlag, standaloneFlag := "0", "0"
		if joined {
			joinedFlag = "1"
		}
		if standalone {
			standaloneFlag = "1"
		}
		cmd.Env = append(os.Environ(), "JOINED="+joinedFlag, "STANDALONE="+standaloneFlag, "RESPONSE="+string(response), "BOOTSTRAP_ENDPOINT=http://unused", "BURST_ID=burst_aaaa", "NODE_NAME=ys-burst-aaaa", "BURST_TIER=full", "API_SERVER=https://api", "CLUSTER_CA="+base64.StdEncoding.EncodeToString([]byte("ca")), "BOOTSTRAP_TOKEN=token")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("joined=%v standalone=%v: %v: %s", joined, standalone, err, out)
		}
	}
	run(false, false)
	if err := os.MkdirAll(filepath.Join(dir, "kubelet/pki"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "kubelet/pki/kubelet-client-current.pem"), []byte("client-cert"), 0600); err != nil {
		t.Fatal(err)
	}
	run(true, false)
	for path, want := range map[string]string{"kubernetes/bootstrap-kubeconfig.yaml": "kubelet-config", "cilium/kubeconfig": "cilium-config"} {
		data, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil || string(data) != want {
			t.Fatalf("%s = %q, %v", path, data, err)
		}
	}
	info, err := os.Stat(filepath.Join(dir, "kubernetes/bootstrap-response.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("response permissions = %v, want 0600", info.Mode().Perm())
	}
	run(true, true)
}
