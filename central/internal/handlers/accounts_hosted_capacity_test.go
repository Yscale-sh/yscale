// yscale:proprietary

package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func hostedCapacityMux(a *Accounts) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/hosted-capacity", http.HandlerFunc(a.HandleGetHostedCapacity))
	mux.Handle("POST /v1/tenants/{tenant_id}/hosted-capacity", http.HandlerFunc(a.HandleRequestHostedCapacity))
	return mux
}

func callHostedCapacity(a *Accounts, method, token, tenantID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/v1/tenants/"+tenantID+"/hosted-capacity", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	hostedCapacityMux(a).ServeHTTP(rec, req)
	return rec
}

func decodeHostedCapacity(t *testing.T, rec *httptest.ResponseRecorder) TenantHostedCapacityResponse {
	t.Helper()
	var response TenantHostedCapacityResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode hosted capacity: %v (body %q)", err, rec.Body.String())
	}
	return response
}

func TestHostedCapacityRoutesRBACIdempotencyAndAssignment(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_h", Token: "ysk_h", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_h", map[string]string{
		"owner": state.RoleOwner, "admin": state.RoleAdmin,
		"member": state.RoleMember, "viewer": state.RoleViewer,
	})
	outsider(t, store, "outsider")
	accounts.Resolver = idProvider(t, map[string]string{
		"human_owner": "owner", "human_admin": "admin", "human_member": "member",
		"human_viewer": "viewer", "human_outsider": "outsider",
	})

	initial := callHostedCapacity(accounts, http.MethodGet, "human_viewer", "cust_h")
	if initial.Code != http.StatusOK {
		t.Fatalf("viewer GET = %d: %s", initial.Code, initial.Body)
	}
	if got := decodeHostedCapacity(t, initial); got.TenantID != "cust_h" || got.Role != state.RoleViewer ||
		got.Status != state.HostedCapacityNotRequested || got.RequestedAt != nil {
		t.Fatalf("initial response = %+v", got)
	}
	for _, token := range []string{"human_member", "human_viewer"} {
		if rec := callHostedCapacity(accounts, http.MethodPost, token, "cust_h"); rec.Code != http.StatusForbidden {
			t.Fatalf("%s POST = %d, want 403: %s", token, rec.Code, rec.Body)
		}
	}

	firstRec := callHostedCapacity(accounts, http.MethodPost, "human_owner", "cust_h")
	if firstRec.Code != http.StatusAccepted {
		t.Fatalf("first POST = %d, want 202: %s", firstRec.Code, firstRec.Body)
	}
	first := decodeHostedCapacity(t, firstRec)
	if first.Status != state.HostedCapacityRequested || first.Role != state.RoleOwner || first.RequestedAt == nil {
		t.Fatalf("first response = %+v", first)
	}
	duplicateRec := callHostedCapacity(accounts, http.MethodPost, "human_admin", "cust_h")
	if duplicateRec.Code != http.StatusOK {
		t.Fatalf("duplicate POST = %d, want 200: %s", duplicateRec.Code, duplicateRec.Body)
	}
	duplicate := decodeHostedCapacity(t, duplicateRec)
	if duplicate.Role != state.RoleAdmin || duplicate.RequestedAt == nil || !duplicate.RequestedAt.Equal(*first.RequestedAt) {
		t.Fatalf("duplicate response = %+v, want original timestamp", duplicate)
	}

	if _, _, err := store.AssignHostedCluster("cust_h", "hosted-h", "", "ys-h", state.OperatorActor()); err != nil {
		t.Fatalf("assign: %v", err)
	}
	assignedRec := callHostedCapacity(accounts, http.MethodPost, "human_owner", "cust_h")
	if assignedRec.Code != http.StatusOK {
		t.Fatalf("assigned POST = %d: %s", assignedRec.Code, assignedRec.Body)
	}
	if assigned := decodeHostedCapacity(t, assignedRec); assigned.Status != state.HostedCapacityAssigned || assigned.RequestedAt != nil {
		t.Fatalf("assigned response = %+v", assigned)
	}

	for _, tenantID := range []string{"cust_h", "cust_missing"} {
		if rec := callHostedCapacity(accounts, http.MethodGet, "human_outsider", tenantID); rec.Code != http.StatusNotFound {
			t.Fatalf("outsider GET %s = %d, want 404", tenantID, rec.Code)
		}
		if rec := callHostedCapacity(accounts, http.MethodPost, "human_outsider", tenantID); rec.Code != http.StatusNotFound {
			t.Fatalf("outsider POST %s = %d, want 404", tenantID, rec.Code)
		}
	}
	if rec := callHostedCapacity(accounts, http.MethodGet, "", "cust_h"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET = %d, want 401", rec.Code)
	}
	if rec := callHostedCapacity(accounts, http.MethodPost, "ysk_h", "cust_h"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("cluster credential POST = %d, want 401", rec.Code)
	}

	if err := store.RevokeCustomer("cust_h"); err != nil {
		t.Fatalf("revoke tenant: %v", err)
	}
	if rec := callHostedCapacity(accounts, http.MethodGet, "human_owner", "cust_h"); rec.Code != http.StatusNotFound {
		t.Fatalf("revoked tenant GET = %d, want 404", rec.Code)
	}
	if rec := callHostedCapacity(accounts, http.MethodPost, "human_admin", "cust_h"); rec.Code != http.StatusNotFound {
		t.Fatalf("revoked tenant POST = %d, want 404", rec.Code)
	}
}

func TestHostedCapacityRoutesDisabledWithoutIdentityProvider(t *testing.T) {
	disabled := &Accounts{Store: state.New(), Issuer: testIssuer, Log: quietLog()}
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		if rec := callHostedCapacity(disabled, method, "human_owner", "cust_h"); rec.Code != http.StatusNotFound {
			t.Fatalf("%s without identity provider = %d, want 404", method, rec.Code)
		}
	}
}
