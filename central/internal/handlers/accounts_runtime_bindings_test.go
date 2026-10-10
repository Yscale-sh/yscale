// yscale:proprietary

package handlers

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/credentialcipher"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

func runtimeBindingCall(a *Accounts, method, token, tenantID, key, body string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/runtime-bindings", http.HandlerFunc(a.HandleListRuntimeBindings))
	mux.Handle("PUT /v1/tenants/{tenant_id}/runtime-bindings/{key}", http.HandlerFunc(a.HandlePutRuntimeBinding))
	mux.Handle("DELETE /v1/tenants/{tenant_id}/runtime-bindings/{key}", http.HandlerFunc(a.HandleDeleteRuntimeBinding))
	path := "/v1/tenants/" + tenantID + "/runtime-bindings"
	if key != "" {
		path += "/" + key
	}
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestRuntimeBindingRoutesRBACStrictJSONAndNoDisclosure(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_rt", Token: "cluster-token", Plan: "pro"})
	a, _ := rosterFixture(t, store, "cust_rt", map[string]string{"owner": state.RoleOwner, "member": state.RoleMember})
	cipher, err := credentialcipher.New(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	a.CredentialCipher = cipher

	if rec := runtimeBindingCall(a, http.MethodPut, "human_member", "cust_rt", "OPENAI_API_KEY", `{"name":"OpenAI","value":"sk-secret"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("member PUT = %d", rec.Code)
	}
	if rec := runtimeBindingCall(a, http.MethodPut, "human_owner", "cust_rt", "bad-key", `{"name":"OpenAI","value":"sk-secret"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad key PUT = %d", rec.Code)
	}
	if rec := runtimeBindingCall(a, http.MethodPut, "human_owner", "cust_rt", "OPENAI_API_KEY", `{"name":"OpenAI","value":"sk-secret","extra":1}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field PUT = %d", rec.Code)
	}

	created := runtimeBindingCall(a, http.MethodPut, "human_owner", "cust_rt", "OPENAI_API_KEY", `{"name":"OpenAI","value":"sk-secret"}`)
	if created.Code != http.StatusOK {
		t.Fatalf("create = %d: %s", created.Code, created.Body)
	}
	if body := created.Body.String(); strings.Contains(body, "sk-secret") || strings.Contains(body, "CredentialCiphertext") || strings.Contains(body, "cipher") {
		t.Fatalf("response leaked secret material: %s", body)
	}
	var envelope runtimeBindingResponse
	if err := json.Unmarshal(created.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Binding.Key != "OPENAI_API_KEY" || envelope.Binding.Name != "OpenAI" || envelope.Binding.SyncState != "pending" {
		t.Fatalf("safe binding response = %+v", envelope.Binding)
	}
	rows, err := store.RuntimeBindingRows("cust_rt")
	if err != nil || len(rows) != 1 {
		t.Fatalf("stored rows = %d, %v", len(rows), err)
	}
	plain, err := cipher.Decrypt(rows[0].CredentialCiphertext, state.RuntimeBindingAdditionalData("cust_rt", rows[0].ID, rows[0].Key))
	if err != nil || plain != "sk-secret" {
		t.Fatalf("decrypt with correct aad = %q, %v", plain, err)
	}
	if _, err := cipher.Decrypt(rows[0].CredentialCiphertext, state.RuntimeBindingAdditionalData("cust_other", rows[0].ID, rows[0].Key)); err == nil {
		t.Fatal("cross-tenant AAD decrypted")
	}
	if _, err := cipher.Decrypt(rows[0].CredentialCiphertext, state.RuntimeBindingAdditionalData("cust_rt", rows[0].ID, "OTHER_KEY")); err == nil {
		t.Fatal("cross-key AAD decrypted")
	}
	serialized, _ := json.Marshal(mustCustomer(t, store, "cust_rt"))
	if strings.Contains(string(serialized), "sk-secret") {
		t.Fatal("customer JSON leaked plaintext")
	}

	listed := runtimeBindingCall(a, http.MethodGet, "human_member", "cust_rt", "", "")
	if listed.Code != http.StatusOK || strings.Contains(listed.Body.String(), "sk-secret") {
		t.Fatalf("member GET = %d: %s", listed.Code, listed.Body)
	}
	if rec := runtimeBindingCall(a, http.MethodDelete, "human_owner", "cust_rt", "OPENAI_API_KEY", "x"); rec.Code != http.StatusBadRequest {
		t.Fatalf("DELETE body = %d", rec.Code)
	}
	if rec := runtimeBindingCall(a, http.MethodDelete, "human_owner", "cust_rt", "OPENAI_API_KEY", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"deleted":true`) {
		t.Fatalf("DELETE = %d: %s", rec.Code, rec.Body)
	}
}

func TestRuntimeBindingSyncDispatchesOnlyAuthorizedConnectorNamespace(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_sync", Token: "cluster-token", Plan: "pro", WorkloadNamespaces: []string{"team-a", "team-b"}})
	a, _ := rosterFixture(t, store, "cust_sync", map[string]string{"owner": state.RoleOwner})
	cipher, _ := credentialcipher.New(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	a.CredentialCipher = cipher

	inScope := &state.Agent{ID: "agent_a", CustomerID: "cust_sync", ClusterID: "cluster-a", WorkloadNamespace: "team-a", ConnectedAt: time.Now(), Send: make(chan protocol.Envelope, 8)}
	outScope := &state.Agent{ID: "agent_z", CustomerID: "cust_sync", ClusterID: "cluster-z", WorkloadNamespace: "team-z", ConnectedAt: time.Now(), Send: make(chan protocol.Envelope, 8)}
	store.AddAgent(inScope)
	store.AddAgent(outScope)

	rec := runtimeBindingCall(a, http.MethodPut, "human_owner", "cust_sync", "API_TOKEN", `{"name":"API","value":"secret-value"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT = %d: %s", rec.Code, rec.Body)
	}
	select {
	case env := <-inScope.Send:
		if env.Type != protocol.TypeSyncRuntimeBindings {
			t.Fatalf("type = %s", env.Type)
		}
		var cmd protocol.SyncRuntimeBindings
		if err := json.Unmarshal(env.Body, &cmd); err != nil {
			t.Fatal(err)
		}
		if cmd.Namespace != "team-a" || cmd.Bindings["API_TOKEN"] != "secret-value" || cmd.Revision <= 0 {
			t.Fatalf("sync command = %+v", cmd)
		}
		createdRevision := cmd.Revision
		deleted := runtimeBindingCall(a, http.MethodDelete, "human_owner", "cust_sync", "API_TOKEN", "")
		if deleted.Code != http.StatusOK {
			t.Fatalf("DELETE = %d: %s", deleted.Code, deleted.Body)
		}
		select {
		case deleteEnv := <-inScope.Send:
			var deleteCmd protocol.SyncRuntimeBindings
			if err := json.Unmarshal(deleteEnv.Body, &deleteCmd); err != nil {
				t.Fatal(err)
			}
			if deleteCmd.Revision <= createdRevision || len(deleteCmd.Bindings) != 0 {
				t.Fatalf("delete sync = %+v, created revision %d", deleteCmd, createdRevision)
			}
			customer, err := store.CustomerByID("cust_sync")
			if err != nil || customer.RuntimeBindingsRevision != deleteCmd.Revision {
				t.Fatalf("durable desired revision = %d err=%v, sync revision %d", customer.RuntimeBindingsRevision, err, deleteCmd.Revision)
			}
		case <-time.After(time.Second):
			t.Fatal("no deletion sync enqueued")
		}
	case <-time.After(time.Second):
		t.Fatal("no sync command enqueued for authorized namespace")
	}
	select {
	case env := <-outScope.Send:
		t.Fatalf("unauthorized namespace received %s", env.Type)
	default:
	}
}
