package workload

import (
	"strings"
	"testing"
)

func validEnvWorkload(env []EnvVar) *Workload {
	return &Workload{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "env-test"},
		Spec:       Spec{Image: "busybox:1.36", Size: "small", Env: env},
	}
}

func TestValidateEnvAcceptsLiteralsAndReferences(t *testing.T) {
	env := []EnvVar{
		{Name: "EMPTY", Value: ""},
		{Name: "LITERAL", Value: "ordinary-value"},
		{Name: "API_TOKEN", ValueFrom: &EnvVarFromRef{SecretKeyRef: &KeyRef{Name: "app-secrets", Key: "api_token"}}},
		{Name: "LOG_LEVEL", ValueFrom: &EnvVarFromRef{ConfigMapKeyRef: &KeyRef{Name: "app-config", Key: "log.level"}}},
	}
	if err := Validate(validEnvWorkload(env)); err != nil {
		t.Fatalf("valid environment rejected: %v", err)
	}

	job, err := ToJob(validEnvWorkload(env))
	if err != nil {
		t.Fatalf("ToJob: %v", err)
	}
	got := job.Spec.Template.Spec.Containers[0].Env
	if len(got) != 4 || got[0].Name != "EMPTY" || got[0].Value != "" || got[0].ValueFrom != nil ||
		got[1].Value != "ordinary-value" || got[1].ValueFrom != nil ||
		got[2].Value != "" || got[2].ValueFrom == nil || got[2].ValueFrom.SecretKeyRef == nil ||
		got[2].ValueFrom.SecretKeyRef.Name != "app-secrets" || got[2].ValueFrom.SecretKeyRef.Key != "api_token" ||
		got[3].Value != "" || got[3].ValueFrom == nil || got[3].ValueFrom.ConfigMapKeyRef == nil ||
		got[3].ValueFrom.ConfigMapKeyRef.Name != "app-config" || got[3].ValueFrom.ConfigMapKeyRef.Key != "log.level" {
		t.Fatalf("translated env = %#v", got)
	}
}

func TestValidateEnvRejectsMalformedDuplicateAndOversizedEntries(t *testing.T) {
	validSecret := func() *EnvVarFromRef {
		return &EnvVarFromRef{SecretKeyRef: &KeyRef{Name: "app-secrets", Key: "token"}}
	}
	tests := map[string][]EnvVar{
		"invalid name":        {{Name: "NOT-AN-ENV", Value: "x"}},
		"duplicate name":      {{Name: "TOKEN", Value: "x"}, {Name: "TOKEN", Value: "y"}},
		"literal plus source": {{Name: "TOKEN", Value: "x", ValueFrom: validSecret()}},
		"empty valueFrom":     {{Name: "TOKEN", ValueFrom: &EnvVarFromRef{}}},
		"two sources": {{Name: "TOKEN", ValueFrom: &EnvVarFromRef{
			SecretKeyRef: &KeyRef{Name: "secret", Key: "token"}, ConfigMapKeyRef: &KeyRef{Name: "config", Key: "token"},
		}}},
		"qualified object name": {{Name: "TOKEN", ValueFrom: &EnvVarFromRef{SecretKeyRef: &KeyRef{Name: "other/secret", Key: "token"}}}},
		"empty key":             {{Name: "TOKEN", ValueFrom: &EnvVarFromRef{SecretKeyRef: &KeyRef{Name: "secret", Key: ""}}}},
		"invalid key":           {{Name: "TOKEN", ValueFrom: &EnvVarFromRef{SecretKeyRef: &KeyRef{Name: "secret", Key: "not/a/key"}}}},
		"oversized literal":     {{Name: "TOKEN", Value: strings.Repeat("x", maxEnvLiteralBytes+1)}},
	}
	tooMany := make([]EnvVar, maxEnvVars+1)
	for i := range tooMany {
		tooMany[i] = EnvVar{Name: "ENV_" + strings.Repeat("A", i), Value: "x"}
	}
	tests["too many"] = tooMany

	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			if err := Validate(validEnvWorkload(env)); err == nil || !strings.Contains(err.Error(), "spec.env") {
				t.Fatalf("Validate error = %v, want spec.env refusal", err)
			}
		})
	}
}

func TestCloneEnvDoesNotAliasNestedReferences(t *testing.T) {
	original := []EnvVar{{Name: "TOKEN", ValueFrom: &EnvVarFromRef{SecretKeyRef: &KeyRef{Name: "secret", Key: "token"}}}}
	cloned := CloneEnv(original)
	cloned[0].Name = "CHANGED"
	cloned[0].ValueFrom.SecretKeyRef.Name = "other"
	cloned[0].ValueFrom.SecretKeyRef.Key = "changed"
	if original[0].Name != "TOKEN" || original[0].ValueFrom.SecretKeyRef.Name != "secret" || original[0].ValueFrom.SecretKeyRef.Key != "token" {
		t.Fatalf("clone aliases original: %#v", original)
	}
}
