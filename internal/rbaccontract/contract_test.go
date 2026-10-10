package rbaccontract

import (
	"strings"
	"testing"
)

func TestBuildCanonicalizesAndRejectsPrivilegeExpansion(t *testing.T) {
	valid := `apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: workloads, namespace: team-a}
rules:
  - apiGroups: [batch, ""]
    resources: [pods, jobs]
    verbs: [watch, get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: workloads, namespace: team-a}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: workloads}
subjects:
  - {kind: ServiceAccount, name: agent, namespace: yscale}
`
	contract, err := Build("chart", "release", "yscale", []ProfileInput{{
		Name: "namespaced-kubernetes-token", Scope: "namespaced", BootstrapAuthMode: "kubernetes-token",
		AllowedNamespaces: []string{"team-b", "team-a"}, Rendered: valid,
	}})
	if err != nil {
		t.Fatal(err)
	}
	profile := contract.Profiles[0]
	if got := strings.Join(profile.AllowedNamespaces, ","); got != "team-a,team-b" {
		t.Fatalf("allowed namespaces = %q", got)
	}
	var role Object
	for _, object := range profile.Objects {
		if object.Kind == "Role" {
			role = object
			break
		}
	}
	if len(role.Rules) != 1 {
		t.Fatalf("role rules = %+v", role.Rules)
	}
	if got := strings.Join(role.Rules[0].Resources, ","); got != "jobs,pods" {
		t.Fatalf("resources = %q", got)
	}

	for name, rendered := range map[string]string{
		"cluster secret": strings.Replace(strings.Replace(valid, "kind: Role\n", "kind: ClusterRole\n", 1), "resources: [pods, jobs]", "resources: [secrets]", 1),
		"dangling role":  strings.Replace(valid, "name: workloads}", "name: absent}", 1),
		"wildcard verb":  strings.Replace(valid, "verbs: [watch, get]", "verbs: ['*']", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Build("chart", "release", "yscale", []ProfileInput{{
				Name: "bad", Scope: "cluster", BootstrapAuthMode: "kubernetes-token", Rendered: rendered,
			}}); err == nil {
				t.Fatal("privilege expansion was accepted")
			}
		})
	}

	if _, err := Build("chart", "release", "yscale", []ProfileInput{{
		Name: "duplicate-namespace", Scope: "namespaced", BootstrapAuthMode: "kubernetes-token",
		AllowedNamespaces: []string{"team-a", "team-a"}, Rendered: valid,
	}}); err == nil {
		t.Fatal("duplicate allowed namespace was accepted")
	}
}
