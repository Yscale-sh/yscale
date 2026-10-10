package sourcefastpath_test

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// Wraps the shell test suite (grant_test.sh) so it runs under `go test ./...`. The suite
// exercises scripts/source-grant.sh + source-revoke.sh via --dry-run (no cluster needed)
// and asserts the source-fastpath security model: deny-by-default, IP-allowlist, bearer
// token, read-only, jailed-to-named-mounts, LoadBalancer+Local exposure, arg validation,
// and (if shellcheck is installed) lint-clean.
func TestSourceFastpathScripts(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	_, here, _, _ := runtime.Caller(0)
	suite := filepath.Join(filepath.Dir(here), "grant_test.sh")
	out, err := exec.Command("bash", suite).CombinedOutput()
	t.Logf("\n%s", out)
	if err != nil {
		t.Fatalf("source-fastpath shell suite failed: %v", err)
	}
}
