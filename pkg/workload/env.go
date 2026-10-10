package workload

import (
	"fmt"
	"regexp"

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	maxEnvVars           = 64
	maxEnvNameBytes      = 253
	maxEnvLiteralBytes   = 32 * 1024
	maxEnvReferenceBytes = 253
)

var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidateEnv checks the environment subset Yscale can translate without
// ambiguity. Literal values remain supported for hand-authored workloads;
// callers publishing templates may layer a stricter reference-only policy on
// top of this common grammar.
func ValidateEnv(env []EnvVar) error {
	if len(env) > maxEnvVars {
		return fmt.Errorf("spec.env may contain at most %d entries", maxEnvVars)
	}
	seen := make(map[string]struct{}, len(env))
	for i, entry := range env {
		if len(entry.Name) == 0 || len(entry.Name) > maxEnvNameBytes || !envNamePattern.MatchString(entry.Name) {
			return fmt.Errorf("spec.env[%d].name must be a valid environment variable name of at most %d bytes", i, maxEnvNameBytes)
		}
		if _, ok := seen[entry.Name]; ok {
			return fmt.Errorf("spec.env contains duplicate name %q", entry.Name)
		}
		seen[entry.Name] = struct{}{}

		if len(entry.Value) > maxEnvLiteralBytes {
			return fmt.Errorf("spec.env[%d].value may be at most %d bytes", i, maxEnvLiteralBytes)
		}
		if entry.ValueFrom == nil {
			continue
		}
		if entry.Value != "" {
			return fmt.Errorf("spec.env[%d] may not set both value and valueFrom", i)
		}
		sources := 0
		if entry.ValueFrom.SecretKeyRef != nil {
			sources++
			if err := validateKeyRef(i, "secretKeyRef", entry.ValueFrom.SecretKeyRef); err != nil {
				return err
			}
		}
		if entry.ValueFrom.ConfigMapKeyRef != nil {
			sources++
			if err := validateKeyRef(i, "configMapKeyRef", entry.ValueFrom.ConfigMapKeyRef); err != nil {
				return err
			}
		}
		if sources != 1 {
			return fmt.Errorf("spec.env[%d].valueFrom must set exactly one of secretKeyRef or configMapKeyRef", i)
		}
	}
	return nil
}

func validateKeyRef(index int, source string, ref *KeyRef) error {
	if len(ref.Name) == 0 || len(ref.Name) > maxEnvReferenceBytes || len(k8svalidation.IsDNS1123Subdomain(ref.Name)) > 0 {
		return fmt.Errorf("spec.env[%d].valueFrom.%s.name must be a bare Kubernetes object name of at most %d bytes", index, source, maxEnvReferenceBytes)
	}
	if len(ref.Key) == 0 || len(ref.Key) > maxEnvReferenceBytes || len(k8svalidation.IsConfigMapKey(ref.Key)) > 0 {
		return fmt.Errorf("spec.env[%d].valueFrom.%s.key must be a valid key of at most %d bytes", index, source, maxEnvReferenceBytes)
	}
	return nil
}

// CloneEnv returns a deep copy of environment entries and their nested
// references so catalog snapshots cannot alias caller-owned mutable data.
func CloneEnv(env []EnvVar) []EnvVar {
	if env == nil {
		return nil
	}
	out := make([]EnvVar, len(env))
	for i, entry := range env {
		out[i] = entry
		if entry.ValueFrom == nil {
			continue
		}
		out[i].ValueFrom = &EnvVarFromRef{}
		if entry.ValueFrom.SecretKeyRef != nil {
			ref := *entry.ValueFrom.SecretKeyRef
			out[i].ValueFrom.SecretKeyRef = &ref
		}
		if entry.ValueFrom.ConfigMapKeyRef != nil {
			ref := *entry.ValueFrom.ConfigMapKeyRef
			out[i].ValueFrom.ConfigMapKeyRef = &ref
		}
	}
	return out
}
