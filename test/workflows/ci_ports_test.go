// Package workflows guards the GitHub Actions service-container port contract.
//
// The native CI jobs share self-hosted runners, so two jobs that both publish
// a fixed host port cannot run concurrently: post-merge run 35113859170
// attempt 2 failed before tests with "Bind for 0.0.0.0:5432 failed: port is
// already allocated" because the state-integration job published 5432:5432
// while another job's PostgreSQL container held the port. The lifecycle and
// factory jobs already avoid this by declaring container-only
// ports ("5432/tcp") and dialing the dynamically allocated host port through
// ${{ job.services.<name>.ports['<port>'] }}.
//
// These guards pin that pattern for every workflow: no service may publish a
// fixed host port, and no workflow may hardcode a loopback address with a
// literal port. Nothing in the Go build type-checks workflow YAML, so without
// this the regression is invisible until a runner happens to collide again.
package workflows

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const workflowDir = "../../.github/workflows"

// containerOnlyPort matches a service port declaration that lets the runner
// allocate the host port dynamically, e.g. "5432/tcp" or "6379/udp". A bare
// "8080" also publishes dynamically, so it is allowed; "host:container"
// mappings are not.
var containerOnlyPort = regexp.MustCompile(`^[0-9]+(/(tcp|udp))?$`)

// hardcodedLoopback matches a literal loopback port such as
// "localhost:5432". The dynamic form "localhost:${{ job.services... }}" does
// not match because the port position is an expression, not digits.
var hardcodedLoopback = regexp.MustCompile(`(localhost|127\.0\.0\.1):[0-9]+`)

func workflowFiles(t *testing.T) []string {
	t.Helper()
	var files []string
	for _, pattern := range []string{"*.yaml", "*.yml"} {
		matches, err := filepath.Glob(filepath.Join(workflowDir, pattern))
		if err != nil {
			t.Fatalf("glob %s: %v", pattern, err)
		}
		files = append(files, matches...)
	}
	if len(files) == 0 {
		t.Fatalf("no workflow files found under %s", workflowDir)
	}
	return files
}

// TestServicePortsAreDynamicallyAllocated fails if any workflow service
// publishes a fixed host port.
func TestServicePortsAreDynamicallyAllocated(t *testing.T) {
	for _, file := range workflowFiles(t) {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		var workflow struct {
			Jobs map[string]struct {
				Services map[string]struct {
					Ports []string `yaml:"ports"`
				} `yaml:"services"`
			} `yaml:"jobs"`
		}
		if err := yaml.Unmarshal(raw, &workflow); err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for jobName, job := range workflow.Jobs {
			for serviceName, service := range job.Services {
				for _, port := range service.Ports {
					if !containerOnlyPort.MatchString(port) {
						t.Errorf("%s: job %q service %q publishes fixed port %q; declare a container-only port (e.g. %q) and dial ${{ job.services.%s.ports['<port>'] }}",
							filepath.Base(file), jobName, serviceName, port,
							"5432/tcp", serviceName)
					}
				}
			}
		}
	}
}

// TestNoHardcodedLoopbackPorts fails if any workflow bakes a literal
// loopback host:port into an env value, run block, or comment. Service
// connections must go through the dynamically allocated port expression.
func TestNoHardcodedLoopbackPorts(t *testing.T) {
	for _, file := range workflowFiles(t) {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if loc := hardcodedLoopback.Find(raw); loc != nil {
			t.Errorf("%s: hardcoded loopback address %q; use ${{ job.services.<name>.ports['<port>'] }} for the port",
				filepath.Base(file), string(loc))
		}
	}
}

// TestGuardCatchesFixedPort proves the port guard actually rejects the exact
// shape that broke run 35113859170, so a refactor of the guard cannot
// silently stop matching it.
func TestGuardCatchesFixedPort(t *testing.T) {
	for port, wantOK := range map[string]bool{
		"5432/tcp":          true,
		"6379/udp":          true,
		"8080":              true,
		"5432:5432":         false,
		"0.0.0.0:5432:5432": false,
	} {
		if got := containerOnlyPort.MatchString(port); got != wantOK {
			t.Errorf("port %q: allowed=%v, want %v", port, got, wantOK)
		}
	}
	bad := "postgres://yscale:yscale@localhost:5432/db"
	good := "postgres://yscale:yscale@localhost:${{ job.services.postgres.ports['5432'] }}/db"
	if !hardcodedLoopback.MatchString(bad) {
		t.Errorf("loopback guard missed %q", bad)
	}
	if hardcodedLoopback.MatchString(good) {
		t.Errorf("loopback guard false-positive on %q", good)
	}
}

func TestAllocatedPostgresPortIsRequiredBeforeTests(t *testing.T) {
	for _, name := range []string{"ci.yaml", "ci-oss.yaml"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(workflowDir, name))
			if name == "ci-oss.yaml" && errors.Is(err, os.ErrNotExist) {
				t.Skip("legacy split-edition workflow is not part of this tree")
			}
			if err != nil {
				t.Fatal(err)
			}
			var workflow struct {
				Jobs map[string]struct {
					Steps []struct {
						Name string            `yaml:"name"`
						Env  map[string]string `yaml:"env"`
						Run  string            `yaml:"run"`
					} `yaml:"steps"`
				} `yaml:"jobs"`
			}
			if err := yaml.Unmarshal(raw, &workflow); err != nil {
				t.Fatal(err)
			}
			steps := workflow.Jobs["connector-ledger-postgres"].Steps
			if len(steps) == 0 || steps[0].Name != "Verify allocated PostgreSQL port" ||
				!strings.Contains(steps[0].Env["PG_SERVICE_PORT"], "job.services.postgres.ports['5432']") {
				t.Fatal("state job lacks its first-step allocated-port guard")
			}
			for _, port := range []string{"", "0", "invalid", "65536", "-1", "1", "49152", "65535"} {
				cmd := exec.Command("bash", "-e", "-c", steps[0].Run)
				cmd.Env = append(os.Environ(), "PG_SERVICE_PORT="+port)
				out, err := cmd.CombinedOutput()
				wantOK := port == "1" || port == "49152" || port == "65535"
				if (err == nil) != wantOK {
					t.Errorf("port %q: err=%v wantOK=%v output=%s", port, err, wantOK, out)
				}
			}
		})
	}
}
