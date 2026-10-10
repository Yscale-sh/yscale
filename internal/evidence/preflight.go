package evidence

import (
	"fmt"
	"net"
	"net/url"
	"runtime/debug"
	"strings"
)

// EvidenceConfig holds the validated evidence preflight configuration.
type EvidenceConfig struct {
	ArtifactDir       string
	CommitSHA         string
	TargetClusterType string
	CentralURL        string
	CentralToken      string
	// MeshProvider is the coordination plane the caller elected to audit —
	// exactly "tailscale" or "fabric". Selection is required whenever
	// evidence is enabled so the retained artifact proves which plane was
	// checked instead of defaulting to Tailscale SaaS.
	MeshProvider string
}

// ValidatePreflight checks all evidence-mode preconditions before any
// Kubernetes client, Service read, provider inventory construction, or
// workload mutation. Returns a validated config or an error that should
// abort the process.
//
// When evidenceDir is empty, evidence is disabled — returns nil config.
//
// meshProvider names the coordination plane the evidence path will audit.
// It is required whenever evidence is enabled and must be exactly
// "tailscale" or "fabric"; any other value (including "auto", empty, or
// a legacy synonym) is rejected before any Kubernetes or provider mutation.
func ValidatePreflight(evidenceDir, centralURL, centralToken, targetClusterType, meshProvider string, buildInfoResolver func() (*debug.BuildInfo, bool)) (*EvidenceConfig, error) {
	dir := strings.TrimSpace(evidenceDir)
	baseURL := strings.TrimSpace(centralURL)
	haveURL := baseURL != ""
	haveTok := strings.TrimSpace(centralToken) != ""
	if haveURL != haveTok {
		return nil, fmt.Errorf("central evidence: both central-evidence-url and central-evidence-token must be set (got url=%v token=%v)", haveURL, haveTok)
	}
	if dir == "" {
		return nil, nil
	}
	if !haveURL {
		return nil, fmt.Errorf("evidence: both central-evidence-url and central-evidence-token are required")
	}

	sha, err := ResolveCommitSHA(buildInfoResolver)
	if err != nil {
		return nil, fmt.Errorf("evidence requires a full commit SHA: %w", err)
	}

	if err := ValidateBoundedMetadata("evidence-target-cluster-type", targetClusterType); err != nil {
		return nil, fmt.Errorf("evidence: %w", err)
	}

	mp := meshProvider
	if !IsValidMeshProvider(mp) {
		return nil, fmt.Errorf("evidence: -evidence-mesh-provider must be %q or %q, got %q",
			MeshProviderTailscale, MeshProviderFabric, meshProvider)
	}

	return &EvidenceConfig{
		ArtifactDir:       dir,
		CommitSHA:         sha,
		TargetClusterType: targetClusterType,
		CentralURL:        baseURL,
		CentralToken:      centralToken,
		MeshProvider:      mp,
	}, nil
}

// CaseBackend extracts the spec.backend value from raw Workload YAML.
// Returns "" if not found or not a string.
type CaseBackend struct {
	Name    string
	Backend string
}

// MeshAuditCredentials carries the audit credentials for every supported mesh
// coordination plane. ValidateEvidenceCases requires only the credentials for
// the selected provider — the unselected provider's fields may be empty. Only
// the selected provider's credentials are validated so a caller cannot
// accidentally satisfy the audit gate by supplying credentials for a plane
// that will never be queried.
type MeshAuditCredentials struct {
	// Tailscale SaaS OAuth (used when meshProvider == "tailscale").
	TailscaleClientID     string
	TailscaleClientSecret string
	TailscaleTailnet      string
	// Per-customer Fabric box (used when meshProvider == "fabric").
	FabricURL    string
	FabricAPIKey string
	FabricUser   string
}

// ValidateEvidenceCases checks that every selected case is explicitly
// backend: linode and that all required audit credentials are present for
// the selected mesh provider. meshProvider must already have been validated
// with IsValidMeshProvider by ValidatePreflight; this function rejects an
// unknown provider as a defence-in-depth against a caller that skipped
// ValidatePreflight. This runs after dry-run handling but before Central
// network auth, Kubernetes construction, or any workload mutation.
func ValidateEvidenceCases(cases []CaseBackend, linodeToken, meshProvider string, mesh MeshAuditCredentials) error {
	if len(cases) == 0 {
		return fmt.Errorf("evidence preflight: no cases selected")
	}
	for _, c := range cases {
		if c.Backend == "" {
			return fmt.Errorf("evidence preflight: case %q has no spec.backend; evidence mode requires explicit backend (got empty)", c.Name)
		}
		if c.Backend == "auto" {
			return fmt.Errorf("evidence preflight: case %q uses spec.backend %q; evidence mode requires an explicit provider", c.Name, c.Backend)
		}
		if c.Backend != "linode" {
			return fmt.Errorf("evidence preflight: case %q uses spec.backend %q; only %q is supported for retained evidence", c.Name, c.Backend, "linode")
		}
	}
	if strings.TrimSpace(linodeToken) == "" {
		return fmt.Errorf("evidence preflight: LINODE_TOKEN is required for provider audit")
	}
	switch meshProvider {
	case MeshProviderTailscale:
		if strings.TrimSpace(mesh.TailscaleClientID) == "" ||
			strings.TrimSpace(mesh.TailscaleClientSecret) == "" ||
			strings.TrimSpace(mesh.TailscaleTailnet) == "" {
			return fmt.Errorf("evidence preflight: TS_OAUTH_CLIENT_ID, TS_OAUTH_CLIENT_SECRET, and TS_TAILNET are all required for tailscale mesh audit")
		}
	case MeshProviderFabric:
		if strings.TrimSpace(mesh.FabricURL) == "" ||
			strings.TrimSpace(mesh.FabricAPIKey) == "" ||
			strings.TrimSpace(mesh.FabricUser) == "" {
			return fmt.Errorf("evidence preflight: YSCALE_EVIDENCE_FABRIC_URL, YSCALE_EVIDENCE_FABRIC_API_KEY, and YSCALE_EVIDENCE_FABRIC_USER are all required for fabric mesh audit")
		}
		if err := ValidateFabricAuditURL(mesh.FabricURL); err != nil {
			return fmt.Errorf("evidence preflight: %w", err)
		}
	default:
		return fmt.Errorf("evidence preflight: unknown mesh provider %q; expected %q or %q",
			meshProvider, MeshProviderTailscale, MeshProviderFabric)
	}
	return nil
}

// ValidateFabricAuditURL rejects endpoints that could send an audit API key
// over plaintext or to an ambiguous URL. Production boxes must use an HTTPS
// origin with no userinfo, query, fragment, or path prefix. HTTP is allowed
// only for loopback test servers.
func ValidateFabricAuditURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("YSCALE_EVIDENCE_FABRIC_URL is invalid: %w", err)
	}
	if u.Hostname() == "" || u.Host == "" {
		return fmt.Errorf("YSCALE_EVIDENCE_FABRIC_URL must include a host")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return fmt.Errorf("YSCALE_EVIDENCE_FABRIC_URL must be an origin URL without userinfo, path, query, or fragment")
	}
	if u.Scheme == "https" {
		return nil
	}
	if u.Scheme == "http" && isLoopbackHost(u.Hostname()) {
		return nil
	}
	return fmt.Errorf("YSCALE_EVIDENCE_FABRIC_URL must use https except for loopback tests")
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
