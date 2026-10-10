package boxes

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpsLoginServerRequiredBeforeCloudCalls(t *testing.T) {
	s := testStore(t)
	api := &fakeLinodeAPI{instances: map[string][]Instance{}}
	reg := &fakeRegistrar{key: "box-api-key"}
	cloudInitPath, aclPath := testWorkerFiles(t)
	w := NewLinodeWorker(s, api, reg, WithCloudInitPath(cloudInitPath), WithACLTemplatePath(aclPath),
		WithOpsHandoff("ops-key", "http://100.64.0.1:8081"), withHeadscaleRegistration(fakeHeadscaleRegistration))
	_, err := w.Provision(context.Background(), "tenant-a", "missing-login")
	if err == nil || !strings.Contains(err.Error(), "FACTORY_OPS_LOGIN_SERVER") {
		t.Fatalf("expected explicit ops login-server error, got %v", err)
	}
	if api.ensureCalls != 0 || len(api.createCalls) != 0 || len(reg.newCalls) != 0 {
		t.Fatal("invalid ops configuration reached registrar or cloud")
	}
}

func TestValidateOpsLoginServer(t *testing.T) {
	for _, value := range []string{"", " ", "http://ops.example.com", "https://", "https://user:secret@ops.example.com", "https://ops.example.com/api", "https://ops.example.com?secret=key", "https://ops.example.com#fragment"} {
		if err := ValidateOpsLoginServer(value); err == nil {
			t.Errorf("accepted invalid ops login URL %q", value)
		}
	}
	for _, value := range []string{"https://ops.example.com", "https://ops.example.com/", "https://ops.example.com:8443", "https://[fd00::1]:443"} {
		if err := ValidateOpsLoginServer(value); err != nil {
			t.Errorf("rejected valid URL %q: %v", value, err)
		}
	}
}

// Execute the actual bootstrap's join function with a local fake client. This
// checks argv and failure behavior without modifying the host or using a cloud.
func TestBootstrapOpsJoinUsesExplicitCoordinator(t *testing.T) {
	body, err := os.ReadFile("../../../deploy/headscale/cloud-init.sh")
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(body), "yscale_ops_join() {")
	if !ok {
		t.Fatal("ops join function missing")
	}
	functionBody, _, ok := strings.Cut(rest, "\n}\n")
	if !ok {
		t.Fatal("ops join function not terminated")
	}
	for _, tc := range []struct {
		name, login, key string
		wantOK           bool
	}{
		{"headscale", "https://ops.example.com", "fixture-key", true},
		{"missing coordinator", "", "fixture-key", false},
		{"insecure coordinator", "http://ops.example.com", "fixture-key", false},
		{"missing key", "https://ops.example.com", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			argsFile := filepath.Join(dir, "args")
			client := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$OPS_TEST_ARGS\"\n"
			if err := os.WriteFile(filepath.Join(dir, "tailscale"), []byte(client), 0700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", "-c", "yscale_ops_join() {"+functionBody+"\n}\nyscale_ops_join")
			cmd.Env = []string{"PATH=" + dir + ":/usr/bin:/bin", "HS_OPS_LOGIN_SERVER=" + tc.login, "HS_OPS_AUTHKEY=" + tc.key, "OPS_TEST_ARGS=" + argsFile}
			out, runErr := cmd.CombinedOutput()
			if (runErr == nil) != tc.wantOK {
				t.Fatalf("join status %v: %s", runErr, out)
			}
			args, err := os.ReadFile(argsFile)
			if !tc.wantOK {
				if !os.IsNotExist(err) {
					t.Fatal("invalid config invoked client, risking hosted fallback")
				}
				return
			}
			want := "up\n--login-server=https://ops.example.com\n--auth-key=fixture-key\n--accept-dns=false\n--timeout=60s\n"
			if err != nil || string(args) != want {
				t.Fatalf("join argv = %q, %v", args, err)
			}
		})
	}
}
