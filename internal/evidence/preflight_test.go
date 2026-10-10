package evidence

import (
	"runtime/debug"
	"strings"
	"testing"
)

func cleanResolver() func() (*debug.BuildInfo, bool) {
	return func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: strings.Repeat("a", 40)},
				{Key: "vcs.modified", Value: "false"},
			},
		}, true
	}
}

func tsMeshCreds() MeshAuditCredentials {
	return MeshAuditCredentials{
		TailscaleClientID:     "id",
		TailscaleClientSecret: "secret",
		TailscaleTailnet:      "tailnet",
	}
}

func fabricMeshCreds() MeshAuditCredentials {
	return MeshAuditCredentials{
		FabricURL:    "https://fabric.example",
		FabricAPIKey: "hs-key",
		FabricUser:   "cust_test",
	}
}

func TestValidatePreflight_DisabledReturnsNil(t *testing.T) {
	cfg, err := ValidatePreflight("", "url", "tok", "k3s", MeshProviderTailscale, cleanResolver())
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		t.Fatal("expected nil config when evidence is disabled")
	}
}

func TestValidatePreflight_DisabledStillRejectsHalfCentralConfig(t *testing.T) {
	if _, err := ValidatePreflight("", "https://api.yscale.sh", "", "", MeshProviderTailscale, cleanResolver()); err == nil {
		t.Fatal("expected existing Central half-configuration to remain fail-closed")
	}
}

func TestValidatePreflight_ValidTailscaleConfig(t *testing.T) {
	cfg, err := ValidatePreflight("/tmp/ev", "https://api.yscale.sh", "tok", "k3s", MeshProviderTailscale, cleanResolver())
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.CommitSHA != strings.Repeat("a", 40) {
		t.Fatalf("commit SHA = %q", cfg.CommitSHA)
	}
	if cfg.ArtifactDir != "/tmp/ev" {
		t.Fatalf("artifact dir = %q", cfg.ArtifactDir)
	}
	if cfg.TargetClusterType != "k3s" {
		t.Fatalf("target cluster type = %q", cfg.TargetClusterType)
	}
	if cfg.MeshProvider != MeshProviderTailscale {
		t.Fatalf("mesh provider = %q, want %q", cfg.MeshProvider, MeshProviderTailscale)
	}
}

func TestValidatePreflight_ValidFabricConfig(t *testing.T) {
	cfg, err := ValidatePreflight("/tmp/ev", "https://api.yscale.sh", "tok", "k3s", MeshProviderFabric, cleanResolver())
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.MeshProvider != MeshProviderFabric {
		t.Fatalf("mesh provider = %q, want %q", cfg.MeshProvider, MeshProviderFabric)
	}
}

func TestValidatePreflight_MissingMeshProviderRejected(t *testing.T) {
	if _, err := ValidatePreflight("/tmp/ev", "https://api.yscale.sh", "tok", "k3s", "", cleanResolver()); err == nil {
		t.Fatal("expected evidence to require an explicit mesh provider")
	}
}

func TestValidatePreflight_UnknownMeshProviderRejected(t *testing.T) {
	for _, name := range []string{"auto", "TAILSCALE", "tailscale ", "netmaker"} {
		t.Run(name, func(t *testing.T) {
			_, err := ValidatePreflight("/tmp/ev", "https://api.yscale.sh", "tok", "k3s", name, cleanResolver())
			if err == nil {
				t.Fatalf("expected mesh provider %q to be rejected", name)
			}
		})
	}
}

func TestValidatePreflight_MissingCommitSHA(t *testing.T) {
	noSHA := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{}, true
	}
	_, err := ValidatePreflight("/tmp/ev", "url", "tok", "k3s", MeshProviderTailscale, noSHA)
	if err == nil {
		t.Fatal("expected error for missing commit SHA")
	}
}

func TestValidatePreflight_MissingTargetClusterType(t *testing.T) {
	_, err := ValidatePreflight("/tmp/ev", "url", "tok", "", MeshProviderTailscale, cleanResolver())
	if err == nil {
		t.Fatal("expected error for missing target cluster type")
	}
}

func TestValidatePreflight_HalfCentralConfig(t *testing.T) {
	_, err := ValidatePreflight("/tmp/ev", "https://api.yscale.sh", "", "k3s", MeshProviderTailscale, cleanResolver())
	if err == nil {
		t.Fatal("expected error for half central config (URL without token)")
	}
	_, err = ValidatePreflight("/tmp/ev", "", "tok", "k3s", MeshProviderTailscale, cleanResolver())
	if err == nil {
		t.Fatal("expected error for half central config (token without URL)")
	}
}

func TestValidatePreflight_NoCentralIsRejected(t *testing.T) {
	if _, err := ValidatePreflight("/tmp/ev", "", "", "k3s", MeshProviderTailscale, cleanResolver()); err == nil {
		t.Fatal("expected evidence mode to require authenticated Central configuration")
	}
}

func TestValidatePreflight_WhitespaceDirectoryIsDisabled(t *testing.T) {
	cfg, err := ValidatePreflight("   ", "https://api.yscale.sh", "tok", "", MeshProviderTailscale, cleanResolver())
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		t.Fatal("whitespace-only evidence directory must not enable artifacts")
	}
}

func TestValidatePreflight_DirtyBuildFails(t *testing.T) {
	dirty := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: strings.Repeat("a", 40)},
				{Key: "vcs.modified", Value: "true"},
			},
		}, true
	}
	_, err := ValidatePreflight("/tmp/ev", "url", "tok", "k3s", MeshProviderTailscale, dirty)
	if err == nil {
		t.Fatal("expected error for dirty build")
	}
}

func TestValidateEvidenceCases_TailscaleAllLinode(t *testing.T) {
	cases := []CaseBackend{
		{Name: "linode-gpu-1", Backend: "linode"},
		{Name: "linode-gpu-2", Backend: "linode"},
	}
	if err := ValidateEvidenceCases(cases, "tok", MeshProviderTailscale, tsMeshCreds()); err != nil {
		t.Fatalf("all-linode with tailscale should pass: %v", err)
	}
}

func TestValidateEvidenceCases_FabricAllLinode(t *testing.T) {
	cases := []CaseBackend{
		{Name: "linode-gpu-1", Backend: "linode"},
	}
	if err := ValidateEvidenceCases(cases, "tok", MeshProviderFabric, fabricMeshCreds()); err != nil {
		t.Fatalf("all-linode with fabric should pass: %v", err)
	}
}

func TestValidateEvidenceCases_FabricIgnoresTailscaleCreds(t *testing.T) {
	cases := []CaseBackend{
		{Name: "linode-gpu-1", Backend: "linode"},
	}
	// Only Fabric creds supplied; Tailscale fields intentionally empty.
	if err := ValidateEvidenceCases(cases, "tok", MeshProviderFabric, fabricMeshCreds()); err != nil {
		t.Fatalf("fabric audit should not require tailscale creds: %v", err)
	}
}

func TestValidateEvidenceCases_TailscaleIgnoresFabricCreds(t *testing.T) {
	cases := []CaseBackend{
		{Name: "linode-gpu-1", Backend: "linode"},
	}
	if err := ValidateEvidenceCases(cases, "tok", MeshProviderTailscale, tsMeshCreds()); err != nil {
		t.Fatalf("tailscale audit should not require fabric creds: %v", err)
	}
}

func TestValidateEvidenceCases_EmptyBackend(t *testing.T) {
	cases := []CaseBackend{
		{Name: "no-backend", Backend: ""},
	}
	err := ValidateEvidenceCases(cases, "tok", MeshProviderTailscale, tsMeshCreds())
	if err == nil {
		t.Fatal("empty backend should fail")
	}
	if !strings.Contains(err.Error(), "no spec.backend") {
		t.Fatalf("error should mention missing backend, got %q", err.Error())
	}
}

func TestValidateEvidenceCases_AutoBackend(t *testing.T) {
	cases := []CaseBackend{
		{Name: "auto-case", Backend: "auto"},
	}
	err := ValidateEvidenceCases(cases, "tok", MeshProviderTailscale, tsMeshCreds())
	if err == nil {
		t.Fatal("auto backend should fail")
	}
	if !strings.Contains(err.Error(), "auto") {
		t.Fatalf("error should mention auto, got %q", err.Error())
	}
}

func TestValidateEvidenceCases_UnsupportedProvider(t *testing.T) {
	cases := []CaseBackend{
		{Name: "flyio-case", Backend: "flyio"},
	}
	err := ValidateEvidenceCases(cases, "tok", MeshProviderTailscale, tsMeshCreds())
	if err == nil {
		t.Fatal("non-linode should fail")
	}
	if !strings.Contains(err.Error(), "flyio") {
		t.Fatalf("error should mention flyio, got %q", err.Error())
	}
}

func TestValidateEvidenceCases_MissingLinodeToken(t *testing.T) {
	cases := []CaseBackend{
		{Name: "linode-gpu", Backend: "linode"},
	}
	err := ValidateEvidenceCases(cases, "", MeshProviderTailscale, tsMeshCreds())
	if err == nil {
		t.Fatal("missing LINODE_TOKEN should fail")
	}
	if !strings.Contains(err.Error(), "LINODE_TOKEN") {
		t.Fatalf("error should mention LINODE_TOKEN, got %q", err.Error())
	}
}

func TestValidateEvidenceCases_MissingTailscale(t *testing.T) {
	cases := []CaseBackend{
		{Name: "linode-gpu", Backend: "linode"},
	}
	creds := tsMeshCreds()
	creds.TailscaleClientID = ""
	err := ValidateEvidenceCases(cases, "tok", MeshProviderTailscale, creds)
	if err == nil {
		t.Fatal("missing TS_OAUTH_CLIENT_ID should fail")
	}
	if !strings.Contains(err.Error(), "TS_OAUTH_CLIENT_ID") {
		t.Fatalf("error should mention TS_OAUTH_CLIENT_ID, got %q", err.Error())
	}
}

func TestValidateEvidenceCases_MissingFabric(t *testing.T) {
	cases := []CaseBackend{
		{Name: "linode-gpu", Backend: "linode"},
	}
	for _, tc := range []struct {
		name   string
		mutate func(*MeshAuditCredentials)
		wants  string
	}{
		{"missing URL", func(c *MeshAuditCredentials) { c.FabricURL = "" }, "YSCALE_EVIDENCE_FABRIC_URL"},
		{"missing API key", func(c *MeshAuditCredentials) { c.FabricAPIKey = "" }, "YSCALE_EVIDENCE_FABRIC_API_KEY"},
		{"missing user", func(c *MeshAuditCredentials) { c.FabricUser = "" }, "YSCALE_EVIDENCE_FABRIC_USER"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creds := fabricMeshCreds()
			tc.mutate(&creds)
			err := ValidateEvidenceCases(cases, "tok", MeshProviderFabric, creds)
			if err == nil {
				t.Fatalf("should fail: %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wants) {
				t.Fatalf("error should mention %s, got %q", tc.wants, err.Error())
			}
		})
	}
}

func TestValidateEvidenceCases_RejectsUnsafeFabricURL(t *testing.T) {
	cases := []CaseBackend{{Name: "linode-gpu", Backend: "linode"}}
	for _, raw := range []string{
		"http://fabric.example",
		"https://",
		"https://user@fabric.example",
		"https://fabric.example/prefix",
		"https://fabric.example?box=other",
		"https://fabric.example#fragment",
	} {
		t.Run(raw, func(t *testing.T) {
			creds := fabricMeshCreds()
			creds.FabricURL = raw
			if err := ValidateEvidenceCases(cases, "tok", MeshProviderFabric, creds); err == nil {
				t.Fatalf("unsafe YSCALE_EVIDENCE_FABRIC_URL %q should fail before mutation", raw)
			}
		})
	}
}

func TestValidateFabricAuditURLAllowsHTTPSAndLoopbackHTTP(t *testing.T) {
	for _, raw := range []string{
		"https://fabric.example",
		"https://fabric.example:8443/",
		"http://localhost:8080",
		"http://127.0.0.1:8080",
		"http://[::1]:8080",
	} {
		t.Run(raw, func(t *testing.T) {
			if err := ValidateFabricAuditURL(raw); err != nil {
				t.Fatalf("ValidateFabricAuditURL(%q): %v", raw, err)
			}
		})
	}
}

func TestValidateEvidenceCases_UnknownMeshProvider(t *testing.T) {
	cases := []CaseBackend{
		{Name: "linode-gpu", Backend: "linode"},
	}
	err := ValidateEvidenceCases(cases, "tok", "auto", tsMeshCreds())
	if err == nil {
		t.Fatal("unknown mesh provider should be rejected")
	}
}

func TestValidateEvidenceCases_NoCases(t *testing.T) {
	err := ValidateEvidenceCases(nil, "tok", MeshProviderTailscale, tsMeshCreds())
	if err == nil {
		t.Fatal("no cases should fail")
	}
}

func TestValidateEvidenceCases_NeverLogsValues(t *testing.T) {
	cases := []CaseBackend{
		{Name: "linode-gpu", Backend: "linode"},
	}
	creds := MeshAuditCredentials{
		TailscaleClientID:     "secret-id",
		TailscaleClientSecret: "secret-secret",
		TailscaleTailnet:      "my-tailnet",
		FabricURL:             "https://box.example",
		FabricAPIKey:          "hs-secret",
		FabricUser:            "hs-user",
	}
	err := ValidateEvidenceCases(cases, "", MeshProviderTailscale, creds)
	if err == nil {
		t.Fatal("should fail")
	}
	msg := err.Error()
	for _, secret := range []string{"secret-id", "secret-secret", "my-tailnet", "hs-secret", "hs-user"} {
		if strings.Contains(msg, secret) {
			t.Fatalf("error leaked credential value %q: %s", secret, msg)
		}
	}
}
