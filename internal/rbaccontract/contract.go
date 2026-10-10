// Package rbaccontract turns rendered Kubernetes RBAC into a stable,
// machine-readable permission contract.
package rbaccontract

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const SchemaVersion = 1

type Contract struct {
	SchemaVersion int       `json:"schema_version"`
	Chart         string    `json:"chart"`
	Release       string    `json:"release"`
	Namespace     string    `json:"namespace"`
	Profiles      []Profile `json:"profiles"`
}

type ProfileInput struct {
	Name              string
	Scope             string
	BootstrapAuthMode string
	AllowedNamespaces []string
	Rendered          string
}

type Profile struct {
	Name              string   `json:"name"`
	Scope             string   `json:"scope"`
	BootstrapAuthMode string   `json:"bootstrap_auth_mode"`
	AllowedNamespaces []string `json:"allowed_namespaces"`
	Objects           []Object `json:"objects"`
}

type Object struct {
	Kind      string       `json:"kind"`
	Namespace string       `json:"namespace,omitempty"`
	Name      string       `json:"name"`
	Rules     []PolicyRule `json:"rules,omitempty"`
	RoleRef   *RoleRef     `json:"role_ref,omitempty"`
	Subjects  []Subject    `json:"subjects,omitempty"`
}

type PolicyRule struct {
	APIGroups       []string `json:"api_groups,omitempty" yaml:"apiGroups"`
	Resources       []string `json:"resources,omitempty" yaml:"resources"`
	ResourceNames   []string `json:"resource_names,omitempty" yaml:"resourceNames"`
	NonResourceURLs []string `json:"non_resource_urls,omitempty" yaml:"nonResourceURLs"`
	Verbs           []string `json:"verbs" yaml:"verbs"`
}

type RoleRef struct {
	APIGroup string `json:"api_group" yaml:"apiGroup"`
	Kind     string `json:"kind" yaml:"kind"`
	Name     string `json:"name" yaml:"name"`
}

type Subject struct {
	APIGroup  string `json:"api_group,omitempty" yaml:"apiGroup"`
	Kind      string `json:"kind" yaml:"kind"`
	Name      string `json:"name" yaml:"name"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace"`
}

type manifest struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Rules    []PolicyRule `yaml:"rules"`
	RoleRef  RoleRef      `yaml:"roleRef"`
	Subjects []Subject    `yaml:"subjects"`
}

func Build(chart, release, namespace string, inputs []ProfileInput) (Contract, error) {
	if strings.TrimSpace(chart) == "" || strings.TrimSpace(release) == "" || strings.TrimSpace(namespace) == "" {
		return Contract{}, errors.New("rbac contract: chart, release, and namespace are required")
	}
	if len(inputs) == 0 {
		return Contract{}, errors.New("rbac contract: at least one profile is required")
	}
	contract := Contract{SchemaVersion: SchemaVersion, Chart: chart, Release: release, Namespace: namespace}
	seen := map[string]bool{}
	for _, input := range inputs {
		if input.Name == "" || (input.Scope != "namespaced" && input.Scope != "cluster") ||
			(input.BootstrapAuthMode != "kubernetes-token" && input.BootstrapAuthMode != "gke") {
			return Contract{}, fmt.Errorf("rbac contract: profile name, scope, and bootstrap auth mode are required: %q/%q/%q", input.Name, input.Scope, input.BootstrapAuthMode)
		}
		if seen[input.Name] {
			return Contract{}, fmt.Errorf("rbac contract: duplicate profile %q", input.Name)
		}
		seen[input.Name] = true
		objects, err := parseObjects(input.Rendered)
		if err != nil {
			return Contract{}, fmt.Errorf("rbac contract: profile %s: %w", input.Name, err)
		}
		allowed := append([]string(nil), input.AllowedNamespaces...)
		sort.Strings(allowed)
		if duplicate := firstDuplicate(allowed); duplicate != "" {
			return Contract{}, fmt.Errorf("rbac contract: profile %s: duplicate allowed namespace %q", input.Name, duplicate)
		}
		contract.Profiles = append(contract.Profiles, Profile{
			Name: input.Name, Scope: input.Scope, BootstrapAuthMode: input.BootstrapAuthMode,
			AllowedNamespaces: allowed, Objects: objects,
		})
	}
	sort.Slice(contract.Profiles, func(i, j int) bool { return contract.Profiles[i].Name < contract.Profiles[j].Name })
	return contract, nil
}

func parseObjects(rendered string) ([]Object, error) {
	dec := yaml.NewDecoder(strings.NewReader(rendered))
	var objects []Object
	seen := map[string]bool{}
	roles := 0
	for {
		var doc manifest
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode rendered YAML: %w", err)
		}
		if !rbacKind(doc.Kind) {
			continue
		}
		if doc.APIVersion != "rbac.authorization.k8s.io/v1" || doc.Metadata.Name == "" {
			return nil, fmt.Errorf("invalid %s %q apiVersion/name", doc.Kind, doc.Metadata.Name)
		}
		key := doc.Kind + "/" + doc.Metadata.Namespace + "/" + doc.Metadata.Name
		if seen[key] {
			return nil, fmt.Errorf("duplicate RBAC object %s", key)
		}
		seen[key] = true
		object := Object{Kind: doc.Kind, Namespace: doc.Metadata.Namespace, Name: doc.Metadata.Name}
		switch doc.Kind {
		case "Role", "ClusterRole":
			roles++
			for _, rule := range doc.Rules {
				normalizeRule(&rule)
				if err := validateRule(object, rule); err != nil {
					return nil, err
				}
				object.Rules = append(object.Rules, rule)
			}
			sort.Slice(object.Rules, func(i, j int) bool { return ruleKey(object.Rules[i]) < ruleKey(object.Rules[j]) })
		case "RoleBinding", "ClusterRoleBinding":
			if doc.RoleRef.APIGroup == "" || doc.RoleRef.Kind == "" || doc.RoleRef.Name == "" || len(doc.Subjects) == 0 {
				return nil, fmt.Errorf("incomplete binding %s", key)
			}
			object.RoleRef = &doc.RoleRef
			object.Subjects = append([]Subject(nil), doc.Subjects...)
			sort.Slice(object.Subjects, func(i, j int) bool { return subjectKey(object.Subjects[i]) < subjectKey(object.Subjects[j]) })
		}
		objects = append(objects, object)
	}
	if roles == 0 {
		return nil, errors.New("rendered chart contains no Role or ClusterRole")
	}
	if err := validateBindings(objects); err != nil {
		return nil, err
	}
	sort.Slice(objects, func(i, j int) bool {
		return objectKey(objects[i]) < objectKey(objects[j])
	})
	return objects, nil
}

func validateBindings(objects []Object) error {
	roles := make(map[string]bool)
	for _, object := range objects {
		if object.Kind == "Role" || object.Kind == "ClusterRole" {
			roles[objectKey(object)] = true
		}
	}
	for _, object := range objects {
		if object.RoleRef == nil {
			continue
		}
		ref := object.RoleRef
		if ref.APIGroup != "rbac.authorization.k8s.io" {
			return fmt.Errorf("%s/%s references unexpected role API group %q", object.Kind, object.Name, ref.APIGroup)
		}
		if object.Kind == "ClusterRoleBinding" && ref.Kind != "ClusterRole" {
			return fmt.Errorf("ClusterRoleBinding/%s must reference a ClusterRole", object.Name)
		}
		if ref.Kind != "Role" && ref.Kind != "ClusterRole" {
			return fmt.Errorf("%s/%s references unexpected role kind %q", object.Kind, object.Name, ref.Kind)
		}
		refNamespace := ""
		if ref.Kind == "Role" {
			refNamespace = object.Namespace
		}
		key := objectKey(Object{Kind: ref.Kind, Namespace: refNamespace, Name: ref.Name})
		if !roles[key] && !(ref.Kind == "ClusterRole" && ref.Name == "system:node-bootstrapper") {
			return fmt.Errorf("%s/%s references role absent from the rendered contract: %s/%s", object.Kind, object.Name, ref.Kind, ref.Name)
		}
	}
	return nil
}

func firstDuplicate(values []string) string {
	for i := 1; i < len(values); i++ {
		if values[i] == values[i-1] {
			return values[i]
		}
	}
	return ""
}

func rbacKind(kind string) bool {
	switch kind {
	case "Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding":
		return true
	default:
		return false
	}
}

func normalizeRule(rule *PolicyRule) {
	for _, values := range []*[]string{&rule.APIGroups, &rule.Resources, &rule.ResourceNames, &rule.NonResourceURLs, &rule.Verbs} {
		sort.Strings(*values)
	}
}

func validateRule(object Object, rule PolicyRule) error {
	if len(rule.Verbs) == 0 || (len(rule.Resources) == 0 && len(rule.NonResourceURLs) == 0) {
		return fmt.Errorf("%s/%s has an empty RBAC rule", object.Kind, object.Name)
	}
	if slices.Contains(rule.APIGroups, "*") || slices.Contains(rule.Resources, "*") || slices.Contains(rule.NonResourceURLs, "*") {
		return fmt.Errorf("%s/%s grants wildcard API groups, resources, or URLs", object.Kind, object.Name)
	}
	if slices.Contains(rule.Verbs, "*") {
		return fmt.Errorf("%s/%s grants a wildcard verb", object.Kind, object.Name)
	}
	if object.Kind == "ClusterRole" && slices.Contains(rule.Resources, "secrets") {
		return fmt.Errorf("ClusterRole/%s grants cluster-wide Secret access", object.Name)
	}
	return nil
}

func objectKey(object Object) string {
	return object.Kind + "\x00" + object.Namespace + "\x00" + object.Name
}

func ruleKey(rule PolicyRule) string {
	return strings.Join(rule.APIGroups, ",") + "\x00" + strings.Join(rule.Resources, ",") + "\x00" +
		strings.Join(rule.ResourceNames, ",") + "\x00" + strings.Join(rule.NonResourceURLs, ",") + "\x00" + strings.Join(rule.Verbs, ",")
}

func subjectKey(subject Subject) string {
	return subject.Kind + "\x00" + subject.Namespace + "\x00" + subject.Name + "\x00" + subject.APIGroup
}
