package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/yscale-sh/yscale/internal/rbaccontract"
)

func main() {
	chart := flag.String("chart", "deploy/helm/yscale-agent", "path to the yscale-agent Helm chart")
	release := flag.String("release", "yscale-rbac-audit", "stable Helm release name used in the contract")
	namespace := flag.String("namespace", "yscale", "agent release namespace")
	allowed := flag.String("allowed-namespaces", "team-a,team-b", "comma-separated workload and Secret namespaces")
	flag.Parse()

	namespaces := splitNamespaces(*allowed)
	if len(namespaces) == 0 {
		fatalf("at least one allowed namespace is required")
	}
	var inputs []rbaccontract.ProfileInput
	for _, scope := range []string{"namespaced", "cluster"} {
		for _, authMode := range []string{"kubernetes-token", "gke"} {
			name := scope + "-" + authMode
			rendered, err := render(*chart, *release, *namespace, scope, authMode, namespaces)
			if err != nil {
				fatalf("render %s profile: %v", name, err)
			}
			inputs = append(inputs, rbaccontract.ProfileInput{
				Name: name, Scope: scope, BootstrapAuthMode: authMode,
				AllowedNamespaces: namespaces, Rendered: rendered,
			})
		}
	}
	contract, err := rbaccontract.Build("yscale-agent", *release, *namespace, inputs)
	if err != nil {
		fatalf("build contract: %v", err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(contract); err != nil {
		fatalf("encode contract: %v", err)
	}
}

func splitNamespaces(raw string) []string {
	var out []string
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func render(chart, release, namespace, scope, authMode string, allowed []string) (string, error) {
	bin, err := exec.LookPath("helm")
	if err != nil {
		return "", fmt.Errorf("find helm: %w", err)
	}
	args := []string{
		"template", release, chart,
		"--namespace", namespace,
		"--set", "rbac.scope=" + scope,
		"--set", "bootstrap.authMode=" + authMode,
		"--set", "rbac.allowedNamespaces={" + strings.Join(allowed, ",") + "}",
	}
	out, err := exec.Command(bin, args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("helm template: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "yscale-rbac-contract: "+format+"\n", args...)
	os.Exit(1)
}
