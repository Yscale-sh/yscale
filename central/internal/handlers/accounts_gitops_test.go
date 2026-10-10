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

func gitOpsMux(a *Accounts) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/gitops/sources", http.HandlerFunc(a.HandleGetGitOpsSources))
	mux.Handle("PUT /v1/tenants/{tenant_id}/gitops/sources", http.HandlerFunc(a.HandlePutGitOpsSources))
	return mux
}

func callGitOps(a *Accounts, method, token, tenantID, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/v1/tenants/"+tenantID+"/gitops/sources", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	gitOpsMux(a).ServeHTTP(rec, req)
	return rec
}

func decodeGitOps(t *testing.T, rec *httptest.ResponseRecorder) TenantGitOpsSourcesResponse {
	t.Helper()
	var response TenantGitOpsSourcesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode gitops sources: %v (body %q)", err, rec.Body.String())
	}
	return response
}

func gitOpsRequest(revision, sources string) string {
	rev, _ := json.Marshal(revision)
	return `{"sources_revision":` + string(rev) + `,"sources":` + sources + `}`
}

const validGitOpsSources = `[{"id":"platform","name":"Platform","reconciler":"flux",` +
	`"repo_url":"https://github.example.invalid/acme/platform","path":"clusters/prod",` +
	`"ref":"main","cluster_id":"cl-prod"}]`

func TestTenantGitOpsSourcesGETPUTRBACValidationAndIsolation(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_bob", Token: "ysk_bob", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{
		"alice": state.RoleOwner, "adm": state.RoleAdmin, "mem": state.RoleMember, "view": state.RoleViewer,
	})
	rosterFixture(t, store, "cust_bob", map[string]string{"bob": state.RoleOwner})

	// A tenant that has registered nothing answers an empty list and a revision,
	// not a 404: "we have configured no sources" is an answer.
	empty := callGitOps(accounts, http.MethodGet, "human_view", "cust_alice", "")
	if empty.Code != http.StatusOK {
		t.Fatalf("viewer get sources = %d, want 200: %s", empty.Code, empty.Body)
	}
	start := decodeGitOps(t, empty)
	if start.Role != state.RoleViewer || len(start.Sources) != 0 || start.SourcesRevision == "" {
		t.Fatalf("empty registry response = %+v", start)
	}
	if body := empty.Body.String(); !strings.Contains(body, `"sources":[]`) {
		t.Fatalf("empty registry rendered as null rather than an empty list: %s", body)
	}

	valid := gitOpsRequest(start.SourcesRevision, validGitOpsSources)
	if rec := callGitOps(accounts, http.MethodPut, "human_mem", "cust_alice", valid); rec.Code != http.StatusForbidden {
		t.Fatalf("member put sources = %d, want 403: %s", rec.Code, rec.Body)
	}
	if rec := callGitOps(accounts, http.MethodPut, "human_view", "cust_alice", valid); rec.Code != http.StatusForbidden {
		t.Fatalf("viewer put sources = %d, want 403: %s", rec.Code, rec.Body)
	}
	if rec := callGitOps(accounts, http.MethodPut, "human_alice", "cust_bob", valid); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant put sources = %d, want 404: %s", rec.Code, rec.Body)
	}
	if rec := callGitOps(accounts, http.MethodGet, "human_alice", "cust_bob", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant get sources = %d, want 404: %s", rec.Code, rec.Body)
	}

	malformed := map[string]string{
		"unknown field":     `{"sources_revision":"` + start.SourcesRevision + `","sources":[],"webhook":"https://evil.invalid"}`,
		"trailing document": gitOpsRequest(start.SourcesRevision, `[]`) + `{"sources":[]}`,
		"not json":          `{`,
		"missing revision":  `{"sources":[]}`,
		"missing sources":   `{"sources_revision":"` + start.SourcesRevision + `"}`,
		"null sources":      `{"sources_revision":"` + start.SourcesRevision + `","sources":null}`,
		"bad reconciler":    gitOpsRequest(start.SourcesRevision, `[{"id":"p","name":"P","reconciler":"kustomize","repo_url":"https://h.invalid/a/b","path":"","ref":"main","cluster_id":"cl-prod"}]`),
		"credential in url": gitOpsRequest(start.SourcesRevision, `[{"id":"p","name":"P","reconciler":"flux","repo_url":"https://ghp_deadbeef@h.invalid/a/b","path":"","ref":"main","cluster_id":"cl-prod"}]`),
		"escaping path":     gitOpsRequest(start.SourcesRevision, `[{"id":"p","name":"P","reconciler":"flux","repo_url":"https://h.invalid/a/b","path":"../../etc","ref":"main","cluster_id":"cl-prod"}]`),
	}
	for name, body := range malformed {
		if rec := callGitOps(accounts, http.MethodPut, "human_adm", "cust_alice", body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400: %s", name, rec.Code, rec.Body)
		}
	}

	written := callGitOps(accounts, http.MethodPut, "human_adm", "cust_alice", valid)
	if written.Code != http.StatusOK {
		t.Fatalf("admin put sources = %d, want 200: %s", written.Code, written.Body)
	}
	registered := decodeGitOps(t, written)
	if !registered.Changed || registered.Role != state.RoleAdmin || len(registered.Sources) != 1 ||
		registered.Sources[0].ID != "platform" || registered.Sources[0].Reconciler != state.GitOpsReconcilerFlux {
		t.Fatalf("written registry response = %+v", registered)
	}
	if registered.SourcesRevision == start.SourcesRevision {
		t.Fatal("registering a source did not move the revision")
	}
	readBack := decodeGitOps(t, callGitOps(accounts, http.MethodGet, "human_view", "cust_alice", ""))
	if readBack.SourcesRevision != registered.SourcesRevision || len(readBack.Sources) != 1 ||
		readBack.Sources[0].RepoURL != "https://github.example.invalid/acme/platform" || readBack.Sources[0].Ref != "main" {
		t.Fatalf("registry did not persist: %+v", readBack)
	}

	// Replacing a registry with itself is not a change, so the response says so
	// rather than reporting an edit nobody made.
	same := gitOpsRequest(registered.SourcesRevision, validGitOpsSources)
	if again := decodeGitOps(t, callGitOps(accounts, http.MethodPut, "human_alice", "cust_alice", same)); again.Changed {
		t.Fatalf("identical replacement reported changed: %+v", again)
	}

	// The stale write loses, and the 409 carries the registry that actually won
	// so the loser can render it instead of guessing.
	stale := callGitOps(accounts, http.MethodPut, "human_alice", "cust_alice", valid)
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale replacement = %d, want 409: %s", stale.Code, stale.Body)
	}
	conflict := decodeGitOps(t, stale)
	if conflict.SourcesRevision != registered.SourcesRevision || len(conflict.Sources) != 1 {
		t.Fatalf("conflict body = %+v, want the current registry", conflict)
	}

	// Removing the last source is a manager's answer to make, and it comes back
	// as an empty list rather than as the registry they started with.
	emptied := callGitOps(accounts, http.MethodPut, "human_alice", "cust_alice", gitOpsRequest(registered.SourcesRevision, `[]`))
	if emptied.Code != http.StatusOK {
		t.Fatalf("emptying the registry = %d, want 200: %s", emptied.Code, emptied.Body)
	}
	if got := decodeGitOps(t, emptied); len(got.Sources) != 0 || got.SourcesRevision != start.SourcesRevision {
		t.Fatalf("emptied registry = %+v, want the empty registry back", got)
	}

	// The body is capped, so one caller cannot decide how much memory this route
	// spends.
	huge := `{"sources":[{"id":"big","name":"` + strings.Repeat("n", maxGitOpsSourcesBytes) + `"}]}`
	if rec := callGitOps(accounts, http.MethodPut, "human_alice", "cust_alice", huge); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body = %d, want 413: %s", rec.Code, rec.Body)
	}

	// The two credential types do not cross here either, and without Yscale ID
	// the route does not exist on this deployment at all.
	if code := callGitOps(accounts, http.MethodGet, "ysk_alice", "cust_alice", "").Code; code != http.StatusUnauthorized {
		t.Errorf("cluster token on the gitops route = %d, want 401", code)
	}
	if code := callGitOps(accounts, http.MethodGet, "", "cust_alice", "").Code; code != http.StatusUnauthorized {
		t.Errorf("unauthenticated read = %d, want 401", code)
	}
	disabled := &Accounts{Store: store, Issuer: testIssuer, Log: quietLog()}
	if code := callGitOps(disabled, http.MethodGet, "human_alice", "cust_alice", "").Code; code != http.StatusNotFound {
		t.Errorf("gitops route on an unconfigured deployment = %d, want 404", code)
	}
}

// Whatever the reconciler authenticates with lives in the cluster. This route
// has nowhere to accept a credential and nothing to hand one back, and that is
// asserted rather than assumed: a future field added to GitOpsSource that
// carried one would fail here.
func TestTenantGitOpsSourcesCarryNoCredentialField(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_alice", Token: "ysk_alice", Plan: "pro"})
	accounts, _ := rosterFixture(t, store, "cust_alice", map[string]string{"alice": state.RoleOwner})

	start := decodeGitOps(t, callGitOps(accounts, http.MethodGet, "human_alice", "cust_alice", ""))
	for _, field := range []string{"token", "password", "secret", "ssh_key", "credential"} {
		body := gitOpsRequest(start.SourcesRevision, `[{"id":"p","name":"P","reconciler":"flux",`+
			`"repo_url":"https://h.invalid/a/b","path":"","ref":"main","cluster_id":"cl-prod","`+field+`":"s3cret"}]`)
		if rec := callGitOps(accounts, http.MethodPut, "human_alice", "cust_alice", body); rec.Code != http.StatusBadRequest {
			t.Errorf("source carrying %q = %d, want 400", field, rec.Code)
		}
	}
	written := callGitOps(accounts, http.MethodPut, "human_alice", "cust_alice", gitOpsRequest(start.SourcesRevision, validGitOpsSources))
	if written.Code != http.StatusOK {
		t.Fatalf("put sources = %d: %s", written.Code, written.Body)
	}
	if strings.Contains(written.Body.String(), "s3cret") {
		t.Fatalf("a refused credential survived into the registry: %s", written.Body)
	}
}
