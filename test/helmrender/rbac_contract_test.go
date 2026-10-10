package helmrender

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/yscale-sh/yscale/internal/rbaccontract"
)

func TestRenderedRBACMatchesReviewedMachineContract(t *testing.T) {
	const (
		release   = "yscale-rbac-audit"
		namespace = "yscale"
	)
	allowed := []string{"team-a", "team-b"}
	var inputs []rbaccontract.ProfileInput
	for _, scope := range []string{"namespaced", "cluster"} {
		for _, authMode := range []string{"kubernetes-token", "gke"} {
			rendered := render(t,
				"--name-template", release,
				"--namespace", namespace,
				"--set", "rbac.scope="+scope,
				"--set", "bootstrap.authMode="+authMode,
				"--set", "rbac.allowedNamespaces={"+strings.Join(allowed, ",")+"}")
			inputs = append(inputs, rbaccontract.ProfileInput{
				Name: scope + "-" + authMode, Scope: scope, BootstrapAuthMode: authMode,
				AllowedNamespaces: allowed, Rendered: rendered,
			})
		}
	}
	contract, err := rbaccontract.Build("yscale-agent", release, namespace, inputs)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.MarshalIndent(contract, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	want, err := os.ReadFile("testdata/yscale-agent-rbac-contract.json")
	if err != nil {
		t.Fatalf("read reviewed RBAC contract: %v", err)
	}
	if diff := cmp.Diff(string(want), string(got)); diff != "" {
		const maxDiff = 32 << 10
		if len(diff) > maxDiff {
			diff = diff[:maxDiff] + "\n... diff truncated ..."
		}
		t.Fatalf("rendered RBAC changed; review the privilege diff and regenerate with go run ./cmd/yscale-rbac-contract (-want +got):\n%s", diff)
	}
}
