// yscale:proprietary

package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// maxGitOpsSourcesBytes bounds the replacement body. Generous against a full
// registry of bounded sources and small enough that a caller cannot make one
// request the reason this route needs a memory budget.
const maxGitOpsSourcesBytes = 64 << 10

// TenantGitOpsSourcesResponse is the stable envelope both verbs answer.
//
// SourcesRevision is the field a browser client carries back into its next PUT:
// it names the exact registry the client rendered, so two managers editing the
// same tenant find out at write time rather than silently overwriting one
// another. It is NOT a git revision — each source carries its own ref.
type TenantGitOpsSourcesResponse struct {
	TenantID        string               `json:"tenant_id"`
	SourcesRevision string               `json:"sources_revision"`
	Sources         []state.GitOpsSource `json:"sources"`
	Role            string               `json:"role,omitempty"`
	Changed         bool                 `json:"changed,omitempty"`
}

func gitOpsSourcesResponse(tenantID string, view state.TenantGitOpsSourcesView) TenantGitOpsSourcesResponse {
	sources := view.Sources
	if sources == nil {
		// A tenant with no sources answers an empty array, not null: the console
		// reads this as a list, and null is a shape it would have to special-case
		// into the same empty list anyway.
		sources = []state.GitOpsSource{}
	}
	return TenantGitOpsSourcesResponse{
		TenantID:        tenantID,
		SourcesRevision: view.Revision,
		Sources:         sources,
		Role:            view.Role,
		Changed:         view.Changed,
	}
}

// HandleGetGitOpsSources serves GET /v1/tenants/{tenant_id}/gitops/sources.
//
// Every member reads it, viewers included, for the template catalog's reason: a
// seat that cannot see which repositories its clusters reconcile cannot tell
// what is deployed. No role sees another tenant's, which is why the store
// authorizes the read against the caller's own grant in the lock it takes the
// record in — an unknown tenant, a revoked one and a non-member are one answer,
// so this route cannot enumerate tenant ids either.
func (a *Accounts) HandleGetGitOpsSources(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	view, err := a.Store.TenantGitOpsSourcesFor(tenantID, caller.ID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			tenantNotFound(w)
			return
		}
		a.Log.Error("account: read tenant gitops sources", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, gitOpsSourcesResponse(tenantID, view))
}

// HandlePutGitOpsSources serves PUT /v1/tenants/{tenant_id}/gitops/sources.
//
// A whole-registry replacement rather than a per-source edit, matching the
// template catalog and the cluster policy: the registry is one set, and a
// partial edit surface would need a merge rule that two managers editing at once
// could disagree about. Owner and admin may write it; the store decides that, so
// a refusal is journaled with the same authority the write would have been.
//
// The body is capped and decoded with unknown fields refused, because this is
// durable tenant state a browser renders: a field central does not understand is
// one a console believes it saved — and on this route, a rejected unknown field
// is also what keeps a client from inventing a place to put a credential.
func (a *Accounts) HandlePutGitOpsSources(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxGitOpsSourcesBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req struct {
		SourcesRevision string                `json:"sources_revision"`
		Sources         *[]state.GitOpsSource `json:"sources"`
	}
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid gitops sources JSON"})
		}
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid gitops sources JSON"})
		return
	}
	if req.Sources == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sources must be an array"})
		return
	}
	view, err := a.Store.SetTenantGitOpsSourcesIfRevision(
		tenantID,
		caller.ID,
		req.SourcesRevision,
		*req.Sources,
		state.HumanActor(caller.ID, tenantID),
	)
	if err != nil {
		switch {
		case errors.Is(err, state.ErrNotFound):
			tenantNotFound(w)
		case errors.Is(err, state.ErrNotAuthorized):
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing gitops sources requires owner or admin"})
		case errors.Is(err, state.ErrInvalidGitOpsSources):
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		case errors.Is(err, state.ErrGitOpsSourcesConflict):
			// The conflict answer carries the CURRENT registry, so a console that
			// lost the race can show what is actually stored rather than asking
			// the manager to reload and guess what changed.
			writeJSON(w, http.StatusConflict, gitOpsSourcesResponse(tenantID, view))
		default:
			a.Log.Error("account: set tenant gitops sources", "tenant", tenantID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		}
		return
	}
	writeJSON(w, http.StatusOK, gitOpsSourcesResponse(tenantID, view))
}
