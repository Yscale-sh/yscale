package tenantadversarial

import (
	"bufio"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// seam is one authoritative negative control retained as evidence. Each entry
// names an existing test in its own package; nothing here re-implements the
// check, and none of these fields are runtime-derived.
type seam struct {
	Seam    string   `json:"seam"`
	Package string   `json:"package"`
	Tests   []string `json:"tests"`
	Outcome string   `json:"outcome"`
}

// seams is the retained control-plane denial evidence. Order is load-bearing:
// it fixes the golden artifact's shape.
var seams = []seam{
	{
		Seam:    "api_cross_tenant_workload_access",
		Package: "github.com/yscale-sh/yscale/central/internal/handlers",
		Tests:   []string{"TestCrossTenantWorkloadAccessDenied"},
		Outcome: "cross_tenant_read_and_mutation_denied_without_side_effect",
	},
	{
		Seam:    "tenant_workload_log_isolation",
		Package: "github.com/yscale-sh/yscale/central/internal/handlers",
		Tests:   []string{"TestTenantWorkloadRoutesHideUnknownRevokedAndNonMemberIdentically"},
		Outcome: "cross_tenant_workload_logs_return_uniform_not_found",
	},
	{
		Seam:    "tenant_billing_customer_facing_authorization",
		Package: "github.com/yscale-sh/yscale/central/internal/handlers",
		Tests: []string{
			"TestTenantBillingSummaryIsMemberSafeAndTenantIsolated",
			"TestTenantBillingStatementJSONAndCSVAreBoundedTenantSafeSnapshots",
			"TestTenantCheckoutPersistsBeforeProviderAndIsExactlyReplayable",
		},
		Outcome: "cross_tenant_billing_summary_statement_and_checkout_denied_before_billing_or_checkout_dispatch",
	},
	{
		Seam:    "connector_credential_scope",
		Package: "github.com/yscale-sh/yscale/central/internal/handlers",
		Tests:   []string{"TestConnectorCredentialIsScopedToAgentRoutes"},
		Outcome: "connector_credential_refused_off_its_agent_surface",
	},
	{
		Seam:    "connector_scoped_dispatch_binding",
		Package: "github.com/yscale-sh/yscale/central/internal/handlers",
		Tests: []string{
			"TestConnectorSubmitAuthRefusesASiblingClusterHeader",
			"TestConnectorWorkloadAuthRefusesWorkloadsOutsideItsCluster",
		},
		Outcome: "cross_cluster_submit_and_workload_dispatch_denied",
	},
	{
		Seam:    "agent_websocket_hello_binding",
		Package: "github.com/yscale-sh/yscale/central/internal/handlers",
		Tests: []string{
			"TestAgentStreamScopedCredentialEnforcesHelloBinding",
			"TestAgentStreamRefusesCrossTenantHello",
		},
		Outcome: "wrong_or_cross_tenant_hello_closes_socket_and_registers_no_agent",
	},
	{
		Seam:    "central_cluster_ownership",
		Package: "github.com/yscale-sh/yscale/central/internal/handlers",
		Tests: []string{
			"TestClusterOwner_CrossTenantRejected",
			"TestMintKey_ExplicitClusterOwnershipUnchanged",
		},
		Outcome: "foreign_cluster_id_rejected_at_ownership_check_and_at_mint",
	},
	{
		Seam:    "state_route_and_command_ownership",
		Package: "github.com/yscale-sh/yscale/central/internal/state",
		Tests: []string{
			"TestClaimAgentCluster",
			"TestConnectorCommandScopeConflictAndDeadLetter",
			"TestReapUnheldClusterGatewayRoutesRefusesAnUnknownTenant",
			"TestSetCustomerGatewayRoutesRefusesAnotherTenantsCluster",
		},
		Outcome: "cross_tenant_cluster_claim_route_write_command_ack_and_unknown_tenant_route_reap_all_refused",
	},
	{
		Seam:    "agent_bootstrap_node_identity",
		Package: "github.com/yscale-sh/yscale/agent/internal/agent",
		Tests: []string{
			"TestHandleBootstrap_RejectsNodeNameMismatch",
			"TestBootstrapAuthExtraGroups",
			"TestEligibilityPolicy",
		},
		Outcome: "mismatched_or_unbound_node_name_refused_at_bootstrap_and_csr_approver",
	},
	{
		Seam:    "foreign_node_ownership_filtering",
		Package: "github.com/yscale-sh/yscale/agent/internal/agent",
		Tests: []string{
			"TestNodeWatcherReportsOnlyItsClusterNodes",
			"TestNodeWatcherIgnoresForeignAndMalformedNodeNames",
			"TestIdleWatcherReportsOnlyItsClusterNodes",
		},
		Outcome: "foreign_or_malformed_burst_nodes_dropped_at_source",
	},
	{
		Seam:    "provider_reconciliation_ownership_filtering",
		Package: "github.com/yscale-sh/yscale/central/internal/decider",
		Tests:   []string{"TestProviderReconciliationFailurePathsAndOwnershipFiltering"},
		Outcome: "provider_resources_lacking_our_owner_tag_never_enter_inventory",
	},
}

// artifact is the exact shape of testdata/two-tenant-denial.json — no
// timestamps, no commit SHA, no random ids, no logs, no provider payload.
type artifact struct {
	SchemaVersion         int       `json:"schema_version"`
	Result                string    `json:"result"`
	Scope                 string    `json:"scope"`
	Seams                 []seam    `json:"seams"`
	Postures              []posture `json:"postures"`
	RemainingLiveEvidence []string  `json:"remaining_live_evidence"`
}

type posture struct {
	Seam         string `json:"seam"`
	Package      string `json:"-"`
	Verification string `json:"verification"`
	Outcome      string `json:"outcome"`
}

var postures = []posture{{
	Seam:         "central_operator_telemetry",
	Package:      "github.com/yscale-sh/yscale/test/tenantadversarial",
	Verification: "TestCentralTelemetryPostureIsOperatorOnly",
	Outcome:      "metrics_and_system_logs_operator_only_tenant_workload_logs_scoped_separately",
}}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller: no source position")
	}
	dir := filepath.Dir(file)
	for i := 0; i < 12; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("repo root (go.mod) not found from %s", file)
	return ""
}

// TestSeamsPassInTheirOwnPackages shells out to `go test -json` per package,
// runs only the named tests, and refuses to pass unless every one of them
// APPEARS AND PASSES. A missing name, a skip, or a fail all fail here — this
// suite cannot silently drift as the authoritative tests are renamed.
func TestSeamsPassInTheirOwnPackages(t *testing.T) {
	root := repoRoot(t)

	byPkg := map[string]map[string]struct{}{}
	for _, s := range seams {
		if byPkg[s.Package] == nil {
			byPkg[s.Package] = map[string]struct{}{}
		}
		for _, name := range s.Tests {
			byPkg[s.Package][name] = struct{}{}
		}
	}
	for _, p := range postures {
		if byPkg[p.Package] == nil {
			byPkg[p.Package] = map[string]struct{}{}
		}
		byPkg[p.Package][p.Verification] = struct{}{}
	}

	for pkg, wantSet := range byPkg {
		names := make([]string, 0, len(wantSet))
		for n := range wantSet {
			names = append(names, n)
		}
		sort.Strings(names)
		runPattern := "^(" + strings.Join(names, "|") + ")$"

		cmd := exec.Command("go", "test", "-count=1", "-run", runPattern, "-json", pkg)
		cmd.Dir = root
		cmd.Env = os.Environ()
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("stdout pipe %s: %v", pkg, err)
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start go test %s: %v", pkg, err)
		}

		passed := map[string]struct{}{}
		failed := map[string]struct{}{}
		skipped := map[string]struct{}{}
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 1<<16), 1<<22)
		for scanner.Scan() {
			var ev struct {
				Action string
				Test   string
			}
			if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
				continue
			}
			if ev.Test == "" || strings.Contains(ev.Test, "/") {
				continue
			}
			switch ev.Action {
			case "pass":
				passed[ev.Test] = struct{}{}
			case "fail":
				failed[ev.Test] = struct{}{}
			case "skip":
				skipped[ev.Test] = struct{}{}
			}
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", pkg, err)
		}
		if err := cmd.Wait(); err != nil {
			t.Errorf("%s: package test command failed: %v", pkg, err)
		}

		for _, name := range names {
			if _, ok := failed[name]; ok {
				t.Errorf("%s: %s FAILED", pkg, name)
				continue
			}
			if _, ok := skipped[name]; ok {
				t.Errorf("%s: %s SKIPPED — this suite refuses skips of authoritative tests", pkg, name)
				continue
			}
			if _, ok := passed[name]; !ok {
				t.Errorf("%s: %s not found in package events — a rename or a build failure", pkg, name)
			}
		}
	}
}

// TestEvidenceGoldenMatchesInMemory rebuilds the artifact in memory and
// byte-diffs it against testdata/two-tenant-denial.json, so `go test ./...`
// gates the checked-in artifact against the retained seam list.
func TestEvidenceGoldenMatchesInMemory(t *testing.T) {
	root := repoRoot(t)
	art := artifact{
		SchemaVersion: 1,
		Result:        "passed",
		Scope:         "deterministic_control_plane",
		Seams:         seams,
		Postures:      postures,
		RemainingLiveEvidence: []string{
			"mesh_network_reachability",
			"paid_flow_activation",
		},
	}
	got, err := json.MarshalIndent(art, "", "  ")
	if err != nil {
		t.Fatalf("marshal artifact: %v", err)
	}
	got = append(got, '\n')

	path := filepath.Join(root, "test", "tenantadversarial", "testdata", "two-tenant-denial.json")
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	if string(got) != string(want) {
		t.Fatalf("evidence drift at %s: regenerate from the in-memory artifact\n--- want ---\n%s\n--- got ---\n%s", path, want, got)
	}
}
