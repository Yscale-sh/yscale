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

func catalogPublisherMux(store *state.Store, accounts *Accounts) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/catalog-publishers", http.HandlerFunc(accounts.HandleListCatalogPublishers))
	mux.Handle("POST /v1/tenants/{tenant_id}/catalog-publishers", http.HandlerFunc(accounts.HandleCreateCatalogPublisher))
	mux.Handle("POST /v1/tenants/{tenant_id}/catalog-publishers/{publisher_id}/credential", http.HandlerFunc(accounts.HandleRotateCatalogPublisherCredential))
	mux.Handle("DELETE /v1/tenants/{tenant_id}/catalog-publishers/{publisher_id}", http.HandlerFunc(accounts.HandleDeleteCatalogPublisher))
	mux.Handle("GET /v1/automation/tenants/{tenant_id}/templates", CatalogPublisherAuth(store, http.HandlerFunc(accounts.HandleAutomationGetTemplates)))
	mux.Handle("PUT /v1/automation/tenants/{tenant_id}/templates", CatalogPublisherAuth(store, http.HandlerFunc(accounts.HandleAutomationPutTemplates)))
	return mux
}

func callCatalogPublisher(mux http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestCatalogPublisherRoutesLifecycleIsolationAndStrictShapes(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_pub", Token: "tenant-token", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "other-token", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_pub", map[string]string{"owner": state.RoleOwner, "member": state.RoleMember})
	mux := catalogPublisherMux(store, accounts)

	if rec := callCatalogPublisher(mux, http.MethodPost, "/v1/tenants/cust_pub/catalog-publishers", "human_member", `{"name":"nope"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("member create = %d: %s", rec.Code, rec.Body)
	}
	if rec := callCatalogPublisher(mux, http.MethodPost, "/v1/tenants/cust_pub/catalog-publishers", "human_owner", `{"name":"prod","unknown":true}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown create field = %d: %s", rec.Code, rec.Body)
	}
	created := callCatalogPublisher(mux, http.MethodPost, "/v1/tenants/cust_pub/catalog-publishers", "human_owner", `{"name":"prod"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", created.Code, created.Body)
	}
	var credential catalogPublisherCredentialResponse
	if err := json.Unmarshal(created.Body.Bytes(), &credential); err != nil || credential.Token == "" || credential.Publisher.Name != "prod" {
		t.Fatalf("create response = %+v err=%v", credential, err)
	}
	if strings.Contains(created.Body.String(), "CredentialHash") || strings.Contains(created.Body.String(), state.HashCatalogPublisherCredential(credential.Token)) {
		t.Fatalf("create response leaked digest: %s", created.Body)
	}
	memberList := callCatalogPublisher(mux, http.MethodGet, "/v1/tenants/cust_pub/catalog-publishers", "human_member", "")
	if memberList.Code != http.StatusOK || !strings.Contains(memberList.Body.String(), `"name":"prod"`) {
		t.Fatalf("member safe-summary list = %d: %s", memberList.Code, memberList.Body)
	}
	listed := callCatalogPublisher(mux, http.MethodGet, "/v1/tenants/cust_pub/catalog-publishers", "human_owner", "")
	if listed.Code != http.StatusOK || strings.Contains(listed.Body.String(), credential.Token) || strings.Contains(listed.Body.String(), "credential") {
		t.Fatalf("unsafe list = %d: %s", listed.Code, listed.Body)
	}

	get := callCatalogPublisher(mux, http.MethodGet, "/v1/automation/tenants/cust_pub/templates", credential.Token, "")
	if get.Code != http.StatusOK {
		t.Fatalf("automation get = %d: %s", get.Code, get.Body)
	}
	current := decodeTemplates(t, get)
	putBody := templateCatalogRequest(current.CatalogRevision, `[]`)
	if rec := callCatalogPublisher(mux, http.MethodPut, "/v1/automation/tenants/cust_pub/templates", credential.Token, putBody); rec.Code != http.StatusOK {
		t.Fatalf("automation put = %d: %s", rec.Code, rec.Body)
	}
	if rec := callCatalogPublisher(mux, http.MethodPut, "/v1/automation/tenants/cust_pub/templates", credential.Token, putBody); rec.Code != http.StatusConflict {
		t.Fatalf("stale automation put = %d: %s", rec.Code, rec.Body)
	}
	if rec := callCatalogPublisher(mux, http.MethodGet, "/v1/automation/tenants/cust_other/templates", credential.Token, ""); rec.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant automation get = %d: %s", rec.Code, rec.Body)
	}
	if rec := callCatalogPublisher(mux, http.MethodGet, "/v1/automation/tenants/cust_pub/templates", "tenant-token", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("tenant token on automation = %d: %s", rec.Code, rec.Body)
	}

	rotatePath := "/v1/tenants/cust_pub/catalog-publishers/" + credential.Publisher.ID + "/credential"
	if rec := callCatalogPublisher(mux, http.MethodPost, rotatePath, "human_owner", `{}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("rotate body accepted = %d: %s", rec.Code, rec.Body)
	}
	rotated := callCatalogPublisher(mux, http.MethodPost, rotatePath, "human_owner", "")
	if rotated.Code != http.StatusOK {
		t.Fatalf("rotate = %d: %s", rotated.Code, rotated.Body)
	}
	var next catalogPublisherCredentialResponse
	if err := json.Unmarshal(rotated.Body.Bytes(), &next); err != nil || next.Token == credential.Token {
		t.Fatalf("rotate response = %+v err=%v", next, err)
	}
	if rec := callCatalogPublisher(mux, http.MethodGet, "/v1/automation/tenants/cust_pub/templates", credential.Token, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("old token after rotate = %d", rec.Code)
	}
	deletePath := "/v1/tenants/cust_pub/catalog-publishers/" + credential.Publisher.ID
	if rec := callCatalogPublisher(mux, http.MethodDelete, deletePath, "human_owner", ""); rec.Code != http.StatusOK {
		t.Fatalf("delete = %d: %s", rec.Code, rec.Body)
	}
	if rec := callCatalogPublisher(mux, http.MethodGet, "/v1/automation/tenants/cust_pub/templates", next.Token, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("token after revoke = %d", rec.Code)
	}
}

func TestCatalogAutomationTemplateRoutesValidateShapesAndNoop(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_pub", Token: "tenant-token", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_pub", map[string]string{"owner": state.RoleOwner})
	mux := catalogPublisherMux(store, accounts)

	created := callCatalogPublisher(mux, http.MethodPost, "/v1/tenants/cust_pub/catalog-publishers", "human_owner", `{"name":"ci-publisher"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", created.Code, created.Body)
	}
	var credential catalogPublisherCredentialResponse
	if err := json.Unmarshal(created.Body.Bytes(), &credential); err != nil {
		t.Fatalf("create response: %v: %s", err, created.Body)
	}

	get := callCatalogPublisher(mux, http.MethodGet, "/v1/automation/tenants/cust_pub/templates", credential.Token, "")
	if get.Code != http.StatusOK {
		t.Fatalf("automation get = %d: %s", get.Code, get.Body)
	}
	current := decodeTemplates(t, get)

	templatesBody, err := json.Marshal(current.Templates)
	if err != nil {
		t.Fatalf("marshal templates: %v", err)
	}

	validPut := templateCatalogRequest(current.CatalogRevision, string(templatesBody))
	if rec := callCatalogPublisher(mux, http.MethodPut, "/v1/automation/tenants/cust_pub/templates", credential.Token, validPut); rec.Code != http.StatusOK {
		t.Fatalf("automation put = %d: %s", rec.Code, rec.Body)
	}
	noop := decodeTemplates(t, callCatalogPublisher(mux, http.MethodPut, "/v1/automation/tenants/cust_pub/templates", credential.Token, validPut))
	if noop.Changed {
		t.Fatalf("automation no-op put reported change: %+v", noop)
	}
	if noop.CatalogRevision != current.CatalogRevision {
		t.Fatalf("automation no-op put changed revision: %s -> %s", current.CatalogRevision, noop.CatalogRevision)
	}

	if rec := callCatalogPublisher(mux, http.MethodPut, "/v1/automation/tenants/cust_pub/templates", credential.Token, `{"templates":[]}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("automation put without revision = %d: %s", rec.Code, rec.Body)
	}
	if rec := callCatalogPublisher(mux, http.MethodPut, "/v1/automation/tenants/cust_pub/templates", credential.Token, `{"catalog_revision":"`+current.CatalogRevision+`","templates":{"id":"bad"}}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("automation put with wrong templates type = %d: %s", rec.Code, rec.Body)
	}
	if rec := callCatalogPublisher(mux, http.MethodPut, "/v1/automation/tenants/cust_pub/templates", credential.Token, `{"catalog_revision":"`+current.CatalogRevision+`","templates":[],"unknown":true}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("automation put with unknown field = %d: %s", rec.Code, rec.Body)
	}
	if rec := callCatalogPublisher(mux, http.MethodPut, "/v1/automation/tenants/cust_pub/templates", credential.Token, templateCatalogRequest(current.CatalogRevision+"-stale", `[]`)); rec.Code != http.StatusConflict {
		t.Fatalf("automation put stale revision = %d: %s", rec.Code, rec.Body)
	}

	// Payloads larger than 128 KiB cannot reach application decoding.
	huge := `{"catalog_revision":"` + current.CatalogRevision + `","templates":[{"id":"big","version":1,"title":"` + strings.Repeat("t", maxTemplateCatalogBytes) + `","kind":"Run-once job","defaults":{"name":"big","image":"busybox","size":"small","mode":"cpu"}}]}`
	if rec := callCatalogPublisher(mux, http.MethodPut, "/v1/automation/tenants/cust_pub/templates", credential.Token, huge); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("automation put oversized body = %d: %s", rec.Code, rec.Body)
	}
}
