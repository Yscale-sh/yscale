package image_test

import (
	"encoding/base64"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestCiliumConfigMapAuthenticatesServer(t *testing.T) {
	for _, name := range []string{"trusted", "untrusted", "missing_ca", "malformed_ca"} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Header.Get("Authorization") != "Bearer test-token" {
					t.Error("missing bearer token")
				}
				if r.URL.Path != "/api/v1/namespaces/kube-system/configmaps/cilium-config" {
					t.Errorf("unexpected path: %s", r.URL.Path)
				}
				io.WriteString(w, `{"data":{"routing-mode":"tunnel"}}`)
			}))
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			server.StartTLS()
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			endpoint := server.URL
			if name == "untrusted" {
				// The certificate covers example.com, but not localhost.
				endpoint = strings.Replace(endpoint, "127.0.0.1", "localhost", 1)
			}
			caData := base64.StdEncoding.EncodeToString(ca)
			if name == "missing_ca" {
				caData = ""
			} else if name == "malformed_ca" {
				caData = "!invalid-base64!"
			}
			dir := t.TempDir()
			bootstrap := filepath.Join(dir, "bootstrap-kubeconfig.yaml")
			kubeconfig := filepath.Join(dir, "kubeconfig")
			for path, contents := range map[string]string{
				bootstrap:  "clusters:\n- cluster:\n    certificate-authority-data: " + caData + "\n",
				kubeconfig: "clusters:\n- cluster:\n    server: " + endpoint + "\nusers:\n- user:\n    token: test-token\n",
			} {
				if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			source, err := os.ReadFile("entrypoint-kubelet.sh")
			if err != nil {
				t.Fatal(err)
			}
			start := strings.Index(string(source), "  APISERVER=")
			end := strings.Index(string(source), "  # Parse + write each ConfigMap")
			if start < 0 || end <= start {
				t.Fatal("ConfigMap request block not found")
			}
			block := strings.NewReplacer(
				"/etc/kubernetes/bootstrap-kubeconfig.yaml", bootstrap,
				"/etc/cilium/kubeconfig", kubeconfig,
				"/etc/cilium/ca.crt", filepath.Join(dir, "ca.crt"),
			).Replace(string(source[start:end]))
			cmd := exec.Command("bash", "-c", "set -euo pipefail\n"+block+"\nprintf '%s' \"$CM_JSON\"\n")
			cmd.Env = append(os.Environ(), "NO_PROXY=*", "no_proxy=*")
			output, err := cmd.CombinedOutput()
			if name == "trusted" {
				if err != nil || requests.Load() != 1 || !strings.Contains(string(output), `"routing-mode":"tunnel"`) {
					t.Fatalf("trusted request failed: requests=%d err=%v output=%s", requests.Load(), err, output)
				}
			} else if requests.Load() != 0 {
				t.Fatalf("sent bearer token without authenticating the server (%d requests)", requests.Load())
			}
		})
	}
}
