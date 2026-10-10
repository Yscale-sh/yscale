package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMergesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := []byte(`{"apiVersion":"kube-bench.yscale.dev/v1alpha1","kind":"BenchmarkConfig","spec":{"namespace":"custom","reaction":{"rounds":1}}}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Spec.Namespace != "custom" || cfg.Spec.Image == "" || cfg.Spec.Reaction.Rounds != 1 || cfg.Spec.Compute.DurationSeconds == 0 {
		t.Fatalf("defaults were not merged: %+v", cfg.Spec)
	}
}

func TestSetSuites(t *testing.T) {
	cfg := Default()
	if err := cfg.SetSuites([]string{"inventory", "compute"}); err != nil {
		t.Fatal(err)
	}
	if !cfg.Spec.Suites.Inventory || !cfg.Spec.Suites.Compute || cfg.Spec.Suites.Network {
		t.Fatalf("unexpected suites: %+v", cfg.Spec.Suites)
	}
}

func TestLoadRejectsTrailingJSONValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := []byte(`{"apiVersion":"kube-bench.yscale.dev/v1alpha1","kind":"BenchmarkConfig"} {}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected multiple JSON values to fail")
	}
}

func TestLoadRejectsTrailingGarbage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := []byte(`{"apiVersion":"kube-bench.yscale.dev/v1alpha1","kind":"BenchmarkConfig"} nope`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected trailing garbage to fail")
	}
}
