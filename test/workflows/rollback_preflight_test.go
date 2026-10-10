package workflows

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// These mocks never contact a cluster. Unexpected commands and unbounded reads
// fail, so a mutating addition cannot quietly turn this into a live test.
const mockKubectl = `#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >> "$PREFLIGHT_CALLS"
[[ "$*" == *'--request-timeout=10s'* ]] || exit 71
case "$*" in
  *' version') exit 0 ;;
  *' get '*) ;;
  *) exit 72 ;;
esac
if [[ "$PREFLIGHT_CASE" == 'unreadable' ]]; then exit 73; fi
case "$*" in
  *lastAppliedRevision*) echo 'master@sha1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' ;;
  *conditions*) echo True ;;
  *'containers[*]'*'.image'*)
    if [[ "$PREFLIGHT_CASE" == 'malformed-image' ]]; then echo 'example.invalid/cloud@sha256:123';
    else printf 'example.invalid/cloud@sha256:%064d\n' 0; fi ;;
  *observedGeneration*)
    case "$PREFLIGHT_CASE" in
      zero-ready) echo '3|3|1|1|0' ;;
      zero-desired) echo '3|3|0|0|0' ;;
      stale-generation) echo '3|2|1|1|1' ;;
      stale-replicas) echo '3|3|2|1|2' ;;
      missing-health) echo '3||1|1|' ;;
      *) echo '3|3|1|1|1' ;;
    esac ;;
  *spec.replicas*) echo 1 ;;
  *strategy.type*) echo Recreate ;;
  *) exit 74 ;;
esac
`

func TestRollbackPreflightFailsClosed(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"healthy", "zero-ready", "zero-desired", "stale-generation", "stale-replicas", "missing-health", "malformed-image", "unreadable", "mutation-flag"} {
		t.Run(scenario, func(t *testing.T) {
			bin := t.TempDir()
			for name, content := range map[string]string{
				"kubectl": mockKubectl,
				"flux":    "#!/usr/bin/env bash\nexit 0\n",
			} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(content), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			calls := filepath.Join(bin, "calls")
			args := []string{filepath.Join(root, "scripts", "rollback-preflight.sh"),
				"--context", "mock-only", "--hosted-controller-deployment", "hosted",
				"--target-cloud-digest", "sha256:" + strings.Repeat("0", 64),
				"--target-hosted-controller-digest", "sha256:" + strings.Repeat("0", 64),
				"--target-agent-digest", "sha256:" + strings.Repeat("0", 64),
				"--target-connector-source", strings.Repeat("a", 40)}
			if scenario == "mutation-flag" {
				args = append(args, "--apply")
			}
			cmd := exec.Command("bash", args...)
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"PREFLIGHT_CASE="+scenario, "PREFLIGHT_CALLS="+calls)
			out, err := cmd.CombinedOutput()
			if scenario == "healthy" {
				if err != nil || !strings.Contains(string(out), "PREFLIGHT OK") {
					t.Fatalf("healthy preflight failed: %v\n%s", err, out)
				}
			} else if err == nil || strings.Contains(string(out), "PREFLIGHT OK") {
				t.Fatalf("unsafe preflight passed: %v\n%s", err, out)
			}
			if scenario == "mutation-flag" {
				if _, err := os.Stat(calls); !os.IsNotExist(err) {
					t.Fatal("mutation flag must be rejected before cluster reads")
				}
			}
		})
	}
}
