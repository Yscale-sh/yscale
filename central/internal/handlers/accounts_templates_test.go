// yscale:proprietary

package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func templatesMux(a *Accounts) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/templates", http.HandlerFunc(a.HandleGetTemplates))
	mux.Handle("PUT /v1/tenants/{tenant_id}/templates", http.HandlerFunc(a.HandlePutTemplates))
	return mux
}

func callTemplates(a *Accounts, method, token, tenantID, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/v1/tenants/"+tenantID+"/templates", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	templatesMux(a).ServeHTTP(rec, req)
	return rec
}

func decodeTemplates(t *testing.T, rec *httptest.ResponseRecorder) TenantTemplateCatalogResponse {
	t.Helper()
	var response TenantTemplateCatalogResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode template catalog: %v (body %q)", err, rec.Body.String())
	}
	return response
}

func templateCatalogRequest(revision, templates string) string {
	rev, _ := json.Marshal(revision)
	return `{"catalog_revision":` + string(rev) + `,"templates":` + templates + `}`
}

func TestTenantTemplatesGETPUTRBACValidationAndIsolation(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_bob", Token: "ysk_bob", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "adm": state.RoleAdmin, "mem": state.RoleMember, "view": state.RoleViewer,
	})
	rosterFixture(t, store, "cust_bob", map[string]string{"bob": state.RoleOwner})

	defaults := callTemplates(accounts, http.MethodGet, "human_view", "cust_alice", "")
	if defaults.Code != http.StatusOK {
		t.Fatalf("viewer get templates = %d, want 200: %s", defaults.Code, defaults.Body)
	}
	defaultCatalog := decodeTemplates(t, defaults)
	if defaultCatalog.Source != templateSourceDefault || defaultCatalog.Role != state.RoleViewer || len(defaultCatalog.Templates) != 3 || defaultCatalog.CatalogRevision == "" {
		t.Fatalf("default catalog response = %+v", defaultCatalog)
	}

	validTemplates := `[{"id":"approved-cpu","version":7,"mark":"A1","title":"Approved CPU","kind":"Run-once job","description":"The approved base image.","defaults":{"name":"approved-cpu","image":"registry.example.invalid/platform/cpu:v7","size":"medium","mode":"cpu"}}]`
	valid := templateCatalogRequest(defaultCatalog.CatalogRevision, validTemplates)
	if rec := callTemplates(accounts, http.MethodPut, "human_mem", "cust_alice", valid); rec.Code != http.StatusForbidden {
		t.Fatalf("member put templates = %d, want 403: %s", rec.Code, rec.Body)
	}
	if rec := callTemplates(accounts, http.MethodPut, "human_alice", "cust_bob", valid); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant put templates = %d, want 404: %s", rec.Code, rec.Body)
	}
	if rec := callTemplates(accounts, http.MethodGet, "human_alice", "cust_bob", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant get templates = %d, want 404: %s", rec.Code, rec.Body)
	}
	if rec := callTemplates(accounts, http.MethodPut, "human_adm", "cust_alice", `{"catalog_revision":"`+defaultCatalog.CatalogRevision+`","templates":[],"unknown":true}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field = %d, want 400: %s", rec.Code, rec.Body)
	}
	if rec := callTemplates(accounts, http.MethodPut, "human_adm", "cust_alice", templateCatalogRequest(defaultCatalog.CatalogRevision, `[{"id":"BAD","version":1,"title":"Bad","kind":"Job","defaults":{"name":"bad","image":"busybox","size":"small","mode":"cpu"}}]`)); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid catalog = %d, want 400: %s", rec.Code, rec.Body)
	}
	if rec := callTemplates(accounts, http.MethodPut, "human_adm", "cust_alice", `{"templates":[]}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing catalog revision = %d, want 400: %s", rec.Code, rec.Body)
	}

	written := callTemplates(accounts, http.MethodPut, "human_adm", "cust_alice", valid)
	if written.Code != http.StatusOK {
		t.Fatalf("admin put templates = %d, want 200: %s", written.Code, written.Body)
	}
	custom := decodeTemplates(t, written)
	if !custom.Changed || custom.Source != templateSourceTenant || custom.Role != state.RoleAdmin || len(custom.Templates) != 1 || custom.Templates[0].ID != "approved-cpu" {
		t.Fatalf("written catalog response = %+v", custom)
	}
	readBack := decodeTemplates(t, callTemplates(accounts, http.MethodGet, "human_view", "cust_alice", ""))
	if readBack.Source != templateSourceTenant || readBack.CatalogRevision != custom.CatalogRevision || len(readBack.Templates) != 1 || readBack.Templates[0].Version != 7 {
		t.Fatalf("catalog did not persist: %+v", readBack)
	}

	// Replacing a catalog with itself is not a change, so the response says so
	// rather than reporting an edit nobody made.
	same := templateCatalogRequest(custom.CatalogRevision, validTemplates)
	if again := decodeTemplates(t, callTemplates(accounts, http.MethodPut, "human_alice", "cust_alice", same)); again.Changed {
		t.Fatalf("identical replacement reported changed: %+v", again)
	}
	if rec := callTemplates(accounts, http.MethodPut, "human_alice", "cust_alice", valid); rec.Code != http.StatusConflict {
		t.Fatalf("stale catalog replacement = %d, want 409: %s", rec.Code, rec.Body)
	}

	// Publishing nothing is a tenant's answer to make, and it is not the same
	// answer as never having published: the catalog is theirs and empty, not
	// central's three defaults.
	emptied := callTemplates(accounts, http.MethodPut, "human_alice", "cust_alice", templateCatalogRequest(custom.CatalogRevision, `[]`))
	if emptied.Code != http.StatusOK {
		t.Fatalf("emptying the catalog = %d, want 200: %s", emptied.Code, emptied.Body)
	}
	if got := decodeTemplates(t, emptied); got.Source != templateSourceTenant || len(got.Templates) != 0 {
		t.Fatalf("emptied catalog = %+v, want a tenant-owned catalog with no entries", got)
	}
	if body := emptied.Body.String(); !strings.Contains(body, `"templates":[]`) {
		t.Fatalf("empty catalog rendered as null rather than an empty gallery: %s", body)
	}

	// The body is capped, so one caller cannot decide how much memory this
	// route spends.
	huge := `{"templates":[{"id":"big","version":1,"title":"` + strings.Repeat("t", maxTemplateCatalogBytes) + `","kind":"Job","defaults":{"name":"big","image":"busybox","size":"small","mode":"cpu"}}]}`
	if rec := callTemplates(accounts, http.MethodPut, "human_alice", "cust_alice", huge); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body = %d, want 413: %s", rec.Code, rec.Body)
	}

	// The two credential types do not cross here either, and without Yscale ID
	// the route does not exist on this deployment at all.
	if code := callTemplates(accounts, http.MethodGet, "ysk_alice", "cust_alice", "").Code; code != http.StatusUnauthorized {
		t.Errorf("cluster token on the template route = %d, want 401", code)
	}
	if code := callTemplates(accounts, http.MethodGet, "", "cust_alice", "").Code; code != http.StatusUnauthorized {
		t.Errorf("unauthenticated read = %d, want 401", code)
	}
	disabled := &Accounts{Store: store, Issuer: testIssuer, Log: quietLog()}
	if code := callTemplates(disabled, http.MethodGet, "human_alice", "cust_alice", "").Code; code != http.StatusNotFound {
		t.Errorf("template route on an unconfigured deployment = %d, want 404", code)
	}
}

func callTemplatedWorkload(a *Accounts, method, path, token, body, key, templateID, templateVersion string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	if key != "" {
		req.Header.Set(idempotencyHeader, key)
	}
	if templateID != "" {
		req.Header.Set(templateIDHeader, templateID)
	}
	if templateVersion != "" {
		req.Header.Set(templateVersionHeader, templateVersion)
	}
	rec := httptest.NewRecorder()
	tenantWorkloadMux(a).ServeHTTP(rec, req)
	return rec
}

func TestTenantTemplateSubmissionProvenanceValidationAndRetryPreservation(t *testing.T) {
	store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	path := "/v1/tenants/cust_console/workloads"

	if rec := callTemplatedWorkload(accounts, http.MethodPost, path, "human_alice", tenantWorkloadYAML, "template-invalid-pair", "container-job", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("half template reference = %d, want 400: %s", rec.Code, rec.Body)
	}
	if rec := callTemplatedWorkload(accounts, http.MethodPost, path, "human_alice", tenantWorkloadYAML, "template-stale-version", "container-job", "1"); rec.Code != http.StatusConflict {
		t.Fatalf("stale template version = %d, want 409: %s", rec.Code, rec.Body)
	}
	if rec := callTemplatedWorkload(accounts, http.MethodPost, path, "human_alice", tenantWorkloadYAML, "template-shape-mismatch", "pytorch-training", fmt.Sprintf("%d", state.DefaultWorkloadTemplateVersion)); rec.Code != http.StatusBadRequest {
		t.Fatalf("template shape mismatch = %d, want 400: %s", rec.Code, rec.Body)
	}
	if decider.calls != 0 {
		t.Fatalf("invalid template references reached provisioning %d times", decider.calls)
	}

	created := callTemplatedWorkload(accounts, http.MethodPost, path, "human_alice", tenantWorkloadYAML, "template-valid-submit", "container-job", fmt.Sprintf("%d", state.DefaultWorkloadTemplateVersion))
	if created.Code != http.StatusAccepted {
		t.Fatalf("templated submit = %d, want 202: %s", created.Code, created.Body)
	}
	var createResponse CreateWorkloadResponse
	if err := json.Unmarshal(created.Body.Bytes(), &createResponse); err != nil {
		t.Fatal(err)
	}
	record, err := store.GetWorkload(createResponse.ID)
	if err != nil || record.TemplateRef == nil || record.TemplateRef.ID != "container-job" || record.TemplateRef.Version != state.DefaultWorkloadTemplateVersion || record.TemplateRef.CatalogRevision == "" {
		t.Fatalf("stored template provenance = %+v err=%v", record, err)
	}
	originalRef := *record.TemplateRef

	listed := decodeTenantWorkloads(t, callTenantWorkload(accounts, http.MethodGet, path, "human_alice", ""))
	if len(listed.Workloads) != 1 || listed.Workloads[0].Template == nil || *listed.Workloads[0].Template != originalRef {
		t.Fatalf("list template provenance = %+v", listed.Workloads)
	}
	detail := callTenantWorkload(accounts, http.MethodGet, path+"/"+createResponse.ID, "human_alice", "")
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), fmt.Sprintf(`"template":{"id":"container-job","version":%d,"catalog_revision":"`, state.DefaultWorkloadTemplateVersion)) {
		t.Fatalf("detail template provenance = %d: %s", detail.Code, detail.Body)
	}

	if !store.FinishWorkload(createResponse.ID, "succeeded", time.Now().UTC(), true) {
		t.Fatal("could not make source workload terminal")
	}
	alice, err := store.AccountByIdentity(testIssuer, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetTenantTemplateCatalog("cust_console", alice.ID, state.WorkloadTemplateCatalog{Templates: []state.WorkloadTemplate{}}, state.HumanActor(alice.ID, "cust_console")); err != nil {
		t.Fatalf("retire template catalog: %v", err)
	}
	retry := callTemplatedWorkload(accounts, http.MethodPost, path+"/"+createResponse.ID+"/retry", "human_alice", "", "template-preserved-retry", "", "")
	if retry.Code != http.StatusAccepted {
		t.Fatalf("retry after template retirement = %d, want 202: %s", retry.Code, retry.Body)
	}
	var retryResponse CreateWorkloadResponse
	if err := json.Unmarshal(retry.Body.Bytes(), &retryResponse); err != nil {
		t.Fatal(err)
	}
	retried, err := store.GetWorkload(retryResponse.ID)
	if err != nil || retried.TemplateRef == nil || *retried.TemplateRef != originalRef || retried.RetryOfWorkloadID != createResponse.ID {
		t.Fatalf("retry did not preserve template provenance: %+v err=%v", retried, err)
	}
	if decider.calls != 2 {
		t.Fatalf("Plan calls = %d, want valid submit plus retry", decider.calls)
	}
}
