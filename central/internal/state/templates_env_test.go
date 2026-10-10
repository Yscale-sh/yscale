package state

import (
	"errors"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/pkg/workload"
)

func templateCatalogWithEnv(env []workload.EnvVar) WorkloadTemplateCatalog {
	return WorkloadTemplateCatalog{Templates: []WorkloadTemplate{{
		ID: "env-job", Version: 1, Title: "Environment job", Kind: "Run-once job",
		Defaults: WorkloadTemplateDefaults{
			Name: "env-job", Image: "busybox:1.36", Size: "small", Mode: WorkloadTemplateModeCPU, Env: env,
		},
	}}}
}

func TestTemplateCatalogAcceptsReferenceOnlyEnvironment(t *testing.T) {
	env := []workload.EnvVar{
		{Name: "API_TOKEN", ValueFrom: &workload.EnvVarFromRef{SecretKeyRef: &workload.KeyRef{Name: "app-secrets", Key: "api_token"}}},
		{Name: "LOG_LEVEL", ValueFrom: &workload.EnvVarFromRef{ConfigMapKeyRef: &workload.KeyRef{Name: "app-config", Key: "log.level"}}},
	}
	catalog := templateCatalogWithEnv(env)
	before := templateCatalogWithEnv(env[:1])
	beforeRevision := before.Revision()
	if sameTemplateCatalog(&before, &catalog) {
		t.Fatal("catalog equality ignored an environment-only edit")
	}
	stored, err := ValidateWorkloadTemplateCatalog(catalog)
	if err != nil {
		t.Fatalf("reference environment rejected: %v", err)
	}
	if stored.Revision() == beforeRevision {
		t.Fatal("adding an environment reference did not change the catalog revision")
	}

	stored.Templates[0].Defaults.Env[0].ValueFrom.SecretKeyRef.Name = "stored-mutated"
	if catalog.Templates[0].Defaults.Env[0].ValueFrom.SecretKeyRef.Name != "app-secrets" {
		t.Fatal("validated catalog aliases caller-owned env references")
	}
	snapshot := catalog.Effective()
	snapshot.Templates[0].Defaults.Env[1].ValueFrom.ConfigMapKeyRef.Key = "snapshot-mutated"
	if catalog.Templates[0].Defaults.Env[1].ValueFrom.ConfigMapKeyRef.Key != "log.level" {
		t.Fatal("effective catalog aliases stored env references")
	}
}

func TestTemplateCatalogRejectsUnsafeEnvironmentDefaults(t *testing.T) {
	tests := map[string]WorkloadTemplateCatalog{
		"literal value": templateCatalogWithEnv([]workload.EnvVar{{Name: "TOKEN", Value: "secret-value"}}),
		"empty literal": templateCatalogWithEnv([]workload.EnvVar{{Name: "TOKEN", Value: ""}}),
		"duplicate": templateCatalogWithEnv([]workload.EnvVar{
			{Name: "TOKEN", ValueFrom: &workload.EnvVarFromRef{SecretKeyRef: &workload.KeyRef{Name: "secret", Key: "one"}}},
			{Name: "TOKEN", ValueFrom: &workload.EnvVarFromRef{ConfigMapKeyRef: &workload.KeyRef{Name: "config", Key: "two"}}},
		}),
	}
	nodeOnly := templateCatalogWithEnv([]workload.EnvVar{{Name: "TOKEN", ValueFrom: &workload.EnvVarFromRef{SecretKeyRef: &workload.KeyRef{Name: "secret", Key: "token"}}}})
	nodeOnly.Templates[0].NodeOnly = true
	nodeOnly.Templates[0].Defaults.Image = ""
	tests["node-only"] = nodeOnly

	large := make([]workload.EnvVar, 40)
	for i := range large {
		large[i] = workload.EnvVar{
			Name: "ENV_" + strings.Repeat("A", i),
			ValueFrom: &workload.EnvVarFromRef{SecretKeyRef: &workload.KeyRef{
				Name: strings.Repeat("a", 253), Key: strings.Repeat("k", 253),
			}},
		}
	}
	tests["encoded size"] = templateCatalogWithEnv(large)

	for name, catalog := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ValidateWorkloadTemplateCatalog(catalog); !errors.Is(err, ErrInvalidTemplateCatalog) {
				t.Fatalf("error = %v, want ErrInvalidTemplateCatalog", err)
			}
		})
	}
}
