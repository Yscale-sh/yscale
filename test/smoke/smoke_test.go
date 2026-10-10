package smoke_test

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func between(t *testing.T, text, start, end string) string {
	t.Helper()
	_, rest, ok := strings.Cut(text, start)
	if !ok {
		t.Fatalf("missing start boundary %q", start)
	}
	part, _, ok := strings.Cut(rest, end)
	if !ok {
		t.Fatalf("missing end boundary %q", end)
	}
	return part
}

// Exercise the actual fixture and driver commands together. The fake kubelet
// deliberately loses logs as soon as the container exits, as a fast natural
// reap does in production. A successful phase must still be observed after the
// marker: neither holding the fixture nor printing a marker alone is a pass.
func TestSmokeCompletionEvidenceSurvivesImmediateReap(t *testing.T) {
	type harness struct{ name, shell, fixture, driver string }
	var job struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct{ Command []string }
				}
			}
		}
	}
	if err := yaml.Unmarshal([]byte(readFile(t, "workload-cpu.yaml.tpl")), &job); err != nil {
		t.Fatal(err)
	}
	local := readFile(t, "../../scripts/smoke-test.sh")
	harnesses := []harness{{
		name:    "local",
		shell:   "bash",
		fixture: job.Spec.Template.Spec.Containers[0].Command[2],
		driver:  between(t, local, "# Stage 4:", "# Stage 6:"),
	}}
	// The first boundary includes prose on its own line; keep it a comment.
	harnesses[0].driver = "#" + harnesses[0].driver
	decoder := yaml.NewDecoder(strings.NewReader(readFile(t, "smoke-test.yaml")))
	for {
		var doc struct {
			Kind string
			Data map[string]string
		}
		if err := decoder.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if doc.Kind != "ConfigMap" || doc.Data["run.sh"] == "" {
			continue
		}
		script := doc.Data["run.sh"]
		workload := between(t, script, "WL_YAML=\"$(cat <<EOF\n", "\nEOF\n)")
		var spec struct{ Spec struct{ Args []string } }
		if err := yaml.Unmarshal([]byte(strings.ReplaceAll(workload, `\$`, `$`)), &spec); err != nil {
			t.Fatal(err)
		}
		harnesses = append(harnesses, harness{
			name:    "scheduled",
			shell:   "sh",
			fixture: strings.ReplaceAll(spec.Spec.Args[0], "sleep 30", "sleep 0.05"),
			driver:  between(t, script, `log "OK: live kubelet logs + exec verified"`, "# Stage 5:"),
		})
	}
	if len(harnesses) != 2 {
		t.Fatal("scheduled runner ConfigMap not found")
	}
	for _, h := range harnesses {
		for _, scenario := range []string{"success", "missing_marker", "logs_unavailable", "failed_exit", "ack_failed"} {
			t.Run(h.name+"/"+scenario, func(t *testing.T) {
				dir := t.TempDir()
				fixture := strings.ReplaceAll(h.fixture, "/tmp/yscale-smoke-", dir+"/yscale-smoke-")
				if scenario == "missing_marker" {
					fixture = strings.ReplaceAll(fixture, "MARKER-OK", "MARKER-MISSING")
				}
				if scenario == "failed_exit" {
					fixture += "\nexit 7\n"
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, h.shell, "-c", fakeKubelet+"\n"+h.driver+"\n[ \"$(cat \"$SMOKE_DIR/finished\")\" = 0 ]")
				cmd.Env = append(os.Environ(), "SMOKE_DIR="+dir, "SMOKE_FIXTURE="+fixture, "SCENARIO="+scenario)
				out, err := cmd.CombinedOutput()
				if ctx.Err() != nil {
					t.Fatalf("driver hung: %v\n%s", ctx.Err(), out)
				}
				if scenario == "success" && err != nil {
					t.Fatalf("lost completion evidence during immediate reap: %v\n%s", err, out)
				}
				if scenario != "success" && err == nil {
					t.Fatalf("driver falsely passed %s\n%s", scenario, out)
				}
				if scenario == "missing_marker" || scenario == "logs_unavailable" {
					if _, err := os.Stat(filepath.Join(dir, "yscale-smoke-marker-read")); err == nil {
						t.Fatal("driver acknowledged unread marker")
					}
				}
			})
		}
	}
}

const fakeKubelet = `
set -eu
if [ -n "${BASH_VERSION:-}" ]; then set -o pipefail; fi
NS=default
POD=smoke
log() { printf '%s\n' "$*"; }
elapsed() { echo 0s; }
fatal() { log "FATAL: $*"; exit 1; }
wait_for() {
  attempt=0
  while [ "$attempt" -lt 100 ]; do
    if eval "$2"; then return 0; fi
    sleep 0.01
    attempt=$((attempt + 1))
  done
  fatal "timed out: $1"
}
(
  set +e
  sh -c "$SMOKE_FIXTURE" >"$SMOKE_DIR/logs" 2>&1 &
  child=$!
  trap 'kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true' TERM
  wait "$child"
  printf '%s' "$?" >"$SMOKE_DIR/finished"
) &
fixture_pid=$!
trap 'kill "$fixture_pid" 2>/dev/null || true; wait "$fixture_pid" 2>/dev/null || true' EXIT
kubectl() {
  [ "$1" = -n ] && shift 2
  case "$1" in
    logs)
      [ "$SCENARIO" != logs_unavailable ] || return 1
      [ ! -f "$SMOKE_DIR/finished" ] || return 1
      cat "$SMOKE_DIR/logs"
      ;;
    exec)
      [ "$2" = "$POD" ] && [ "$3" = -- ] && [ "$4" = touch ] || return 1
      if [ "$SCENARIO" = ack_failed ] && [ "$5" = /tmp/yscale-smoke-marker-read ]; then return 1; fi
      touch "$SMOKE_DIR/${5##*/}"
      ;;
    get)
      if [ -f "$SMOKE_DIR/finished" ]; then
        if [ "$(cat "$SMOKE_DIR/finished")" = 0 ]; then echo Succeeded; else echo Failed; fi
      else
        echo Running
      fi
      ;;
    *) return 1 ;;
  esac
}
kctl() { kubectl "$@"; }
`
