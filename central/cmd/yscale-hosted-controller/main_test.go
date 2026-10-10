package main

import (
	"strings"
	"testing"
)

func TestLoadConfigRequiresTokenAndValidatesBounds(t *testing.T) {
	if _, err := loadConfig(func(string) string { return "" }); err == nil || !strings.Contains(err.Error(), "YSCALE_ADMIN_TOKEN") {
		t.Fatalf("missing token error = %v", err)
	}
	env := map[string]string{
		"YSCALE_ADMIN_TOKEN": "adm", "YSCALE_HOSTED_MAX_ASSIGNMENTS": "0",
		"YSCALE_HOSTED_AGENT_IMAGE_DIGEST": "sha256:" + strings.Repeat("a", 64),
	}
	if _, err := loadConfig(func(k string) string { return env[k] }); err == nil {
		t.Fatal("zero cap accepted")
	}
	env["YSCALE_HOSTED_MAX_ASSIGNMENTS"] = "8"
	env["YSCALE_HOSTED_POLL_INTERVAL"] = "500ms"
	if _, err := loadConfig(func(k string) string { return env[k] }); err == nil {
		t.Fatal("too-fast poll accepted")
	}
	delete(env, "YSCALE_HOSTED_POLL_INTERVAL")
	env["YSCALE_HOSTED_EXCLUDED_CLUSTER_IDS"] = " hosted-demo, hosted-old,hosted-demo "
	cfg, err := loadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Controller.MaxAssignments != 8 || cfg.Controller.ConnectorNamespace != "yscale-system" {
		t.Fatalf("defaults = %+v", cfg.Controller)
	}
	if len(cfg.Controller.ExcludedClusterIDs) != 2 {
		t.Fatalf("excluded cluster ids = %v", cfg.Controller.ExcludedClusterIDs)
	}
	if _, ok := cfg.Controller.ExcludedClusterIDs["hosted-demo"]; !ok {
		t.Fatal("hosted-demo exclusion was not parsed")
	}
	if cfg.Controller.SourceName != "yscale-agent" || cfg.Controller.CentralServiceNamespace != "yscale" {
		t.Fatalf("proven defaults = %+v", cfg.Controller)
	}
	if cfg.Controller.AgentImageDigest != env["YSCALE_HOSTED_AGENT_IMAGE_DIGEST"] {
		t.Fatalf("agent image digest = %q", cfg.Controller.AgentImageDigest)
	}
}
