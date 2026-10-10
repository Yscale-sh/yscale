// yscale:proprietary

package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/credentialcipher"
	"github.com/yscale-sh/yscale/central/internal/state"
)

func cloudAccountCall(a *Accounts, method, token, tenantID, body string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/cloud-accounts/linode", http.HandlerFunc(a.HandleGetLinodeCloudAccount))
	mux.Handle("PUT /v1/tenants/{tenant_id}/cloud-accounts/linode", http.HandlerFunc(a.HandlePutLinodeCloudAccount))
	mux.Handle("DELETE /v1/tenants/{tenant_id}/cloud-accounts/linode", http.HandlerFunc(a.HandleDeleteLinodeCloudAccount))
	req := httptest.NewRequest(method, "/v1/tenants/"+tenantID+"/cloud-accounts/linode", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestLinodeCloudAccountRoutesRBACValidationAndNoTokenLeak(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_cloud", Token: "cluster-token", Plan: "pro"})
	a, _ := rosterFixture(t, store, "cust_cloud", map[string]string{"owner": state.RoleOwner, "admin": state.RoleAdmin, "member": state.RoleMember, "viewer": state.RoleViewer})
	cipher, err := credentialcipher.New(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	a.CredentialCipher = cipher
	a.ValidateLinodeAccount = func(_ context.Context, token, region string) (string, error) {
		if token != "provider-secret" || region != "us-ord" {
			t.Fatalf("validator input = %q/%q", token, region)
		}
		return "provider-uuid", nil
	}
	empty := cloudAccountCall(a, http.MethodGet, "human_viewer", "cust_cloud", "")
	if empty.Code != http.StatusOK || !strings.Contains(empty.Body.String(), `"account":null`) {
		t.Fatalf("empty account GET = %d: %s", empty.Code, empty.Body)
	}
	if cross := cloudAccountCall(a, http.MethodGet, "human_owner", "cust_bob", ""); cross.Code != http.StatusNotFound {
		t.Fatalf("unknown/cross-tenant GET = %d, want 404", cross.Code)
	}
	valid := `{"token":"provider-secret","region":"us-ord","cpu_image":"","gpu_image":"private/7"}`
	for _, role := range []string{"member", "viewer"} {
		if rec := cloudAccountCall(a, http.MethodPut, "human_"+role, "cust_cloud", valid); rec.Code != http.StatusForbidden {
			t.Fatalf("%s PUT = %d", role, rec.Code)
		}
	}
	if rec := cloudAccountCall(a, http.MethodPut, "human_admin", "cust_cloud", `{"token":"provider-secret","region":"us-ord","extra":1}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field = %d", rec.Code)
	}
	created := cloudAccountCall(a, http.MethodPut, "human_admin", "cust_cloud", valid)
	if created.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", created.Code, created.Body)
	}
	if strings.Contains(created.Body.String(), "provider-secret") || strings.Contains(created.Body.String(), "CredentialCiphertext") {
		t.Fatalf("response leaked secret: %s", created.Body)
	}
	var createdEnvelope tenantCloudAccountResponse
	if err := json.Unmarshal(created.Body.Bytes(), &createdEnvelope); err != nil {
		t.Fatal(err)
	}
	if !createdEnvelope.Changed || createdEnvelope.Account == nil || createdEnvelope.Account.ProviderAccountID != "provider-uuid" ||
		!createdEnvelope.Account.CPUImageReady || !createdEnvelope.Account.GPUImageReady {
		t.Fatalf("created envelope = %+v", createdEnvelope)
	}
	var raw map[string]any
	if err := json.Unmarshal(created.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	accountJSON := raw["account"].(map[string]any)
	wantKeys := map[string]bool{"id": true, "provider": true, "provider_account_id": true, "region": true, "cpu_image_ready": true, "gpu_image_ready": true, "updated_at": true}
	if len(accountJSON) != len(wantKeys) {
		t.Fatalf("safe account keys = %v", accountJSON)
	}
	for key := range accountJSON {
		if !wantKeys[key] {
			t.Fatalf("unexpected safe account key %q", key)
		}
	}
	for _, role := range []string{"owner", "admin", "member", "viewer"} {
		rec := cloudAccountCall(a, http.MethodGet, "human_"+role, "cust_cloud", "")
		if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "provider-secret") {
			t.Fatalf("%s GET = %d %s", role, rec.Code, rec.Body)
		}
	}
	stored, err := store.LinodeCloudAccount("cust_cloud")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := cipher.Decrypt(stored.CredentialCiphertext, credentialcipher.AdditionalData("cust_cloud", stored.ID, state.CloudProviderLinode))
	if err != nil || plain != "provider-secret" {
		t.Fatalf("stored credential = %q, %v", plain, err)
	}
	serialized, _ := json.Marshal(mustCustomer(t, store, "cust_cloud"))
	if strings.Contains(string(serialized), "provider-secret") {
		t.Fatal("customer JSON leaked token")
	}
	if rec := cloudAccountCall(a, http.MethodDelete, "human_member", "cust_cloud", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("member DELETE = %d", rec.Code)
	}
	if rec := cloudAccountCall(a, http.MethodDelete, "human_owner", "cust_cloud", ""); rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"account":null`) || !strings.Contains(rec.Body.String(), `"changed":true`) {
		t.Fatalf("owner DELETE = %d: %s", rec.Code, rec.Body)
	}
}

func mustCustomer(t *testing.T, s *state.Store, id string) *state.Customer {
	t.Helper()
	c, err := s.CustomerByID(id)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
