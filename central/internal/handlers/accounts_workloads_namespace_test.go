// yscale:proprietary

package handlers

import (
	"net/http"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// The console rebuilds a Customer from a TenantSummary before submitting.
// If the tenant's authorized namespaces don't survive that hop, every
// console submission gets judged against the fail-closed set instead of
// the tenant's own — and the namespace check silently stops meaning
// anything for this entry point.
func TestTenantWorkloadNamespaceAuthorizationSurvivesTheSummary(t *testing.T) {
	store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"owner": state.RoleOwner})

	got := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", "human_owner", tenantWorkloadYAML)
	if got.Code != http.StatusAccepted {
		t.Fatalf("authorized namespace = %d, want 202 (body %q)", got.Code, got.Body.String())
	}

	// Same tenant, same role, a namespace it was never authorized for.
	elsewhere := strings.Replace(tenantWorkloadYAML, "namespace: jobs", "namespace: kube-system", 1)
	planned := decider.calls
	rejected := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", "human_owner", elsewhere)
	if rejected.Code != http.StatusForbidden {
		t.Fatalf("unauthorized namespace = %d, want 403 (body %q)", rejected.Code, rejected.Body.String())
	}
	if decider.calls != planned {
		t.Error("planned a burst for an unauthorized namespace")
	}

	summary, err := store.TenantSummaryByID("cust_console")
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.WorkloadNamespaces) != 1 || summary.WorkloadNamespaces[0] != "jobs" {
		t.Errorf("summary namespaces = %v, want [jobs]", summary.WorkloadNamespaces)
	}
}

// The console reads the stored spec back. It has to show the namespace the
// workload actually ran in — the one central pinned — not the one the
// submission asked for or left out. A list that reports "" for every
// unqualified submission is a list that cannot answer "where did this run".
func TestTenantWorkloadListShowsThePinnedNamespace(t *testing.T) {
	_, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"owner": state.RoleOwner})

	// Submitted with no namespace at all: it lands in the tenant's first
	// authorized one, and the record has to say so.
	unqualified := strings.Replace(tenantWorkloadYAML, "  namespace: jobs\n", "", 1)
	if strings.Contains(unqualified, "namespace:") {
		t.Fatal("fixture no longer carries the namespace line this test strips")
	}
	created := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", "human_owner", unqualified)
	if created.Code != http.StatusAccepted {
		t.Fatalf("create = %d, want 202 (body %q)", created.Code, created.Body.String())
	}

	listed := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_owner", "")
	if listed.Code != http.StatusOK {
		t.Fatalf("list = %d, want 200 (body %q)", listed.Code, listed.Body.String())
	}
	response := decodeTenantWorkloads(t, listed)
	if len(response.Workloads) != 1 {
		t.Fatalf("listed %d workloads, want 1", len(response.Workloads))
	}
	got := response.Workloads[0].Spec
	if got.Metadata.Namespace != "jobs" {
		t.Errorf("listed namespace = %q, want %q (the namespace the Job ran in)", got.Metadata.Namespace, "jobs")
	}
	// Still the same workload otherwise.
	if got.Metadata.Name != "console-job" || got.Spec.Image != "busybox:latest" || got.Spec.Size != "small" {
		t.Errorf("listed spec lost fields: %+v", got)
	}
	if got.Spec.Budget == nil || got.Spec.Budget.MaxUSD != 2.5 || got.Spec.Budget.Deadline != "5m0s" {
		t.Errorf("listed budget = %+v", got.Spec.Budget)
	}
}
