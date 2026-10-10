// yscale:proprietary

package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// maxTemplateCatalogBytes bounds the replacement body. Generous against a full
// catalog of bounded entries and small enough that a caller cannot make one
// request the reason this route needs a memory budget.
const maxTemplateCatalogBytes = 128 << 10

// TenantTemplateCatalogResponse is the stable envelope both verbs answer.
//
// CatalogRevision is the field a launch client is expected to carry back: it
// names the exact catalog a submission was verified against, so a console
// holding a stale gallery finds out at submit rather than launching yesterday's
// template. Source says whether the tenant published this catalog or inherits
// the server's, because "we have not configured templates" and "we publish
// exactly these" are different answers that would otherwise look identical.
type TenantTemplateCatalogResponse struct {
	TenantID        string                   `json:"tenant_id"`
	CatalogRevision string                   `json:"catalog_revision"`
	Source          string                   `json:"source"`
	Templates       []state.WorkloadTemplate `json:"templates"`
	Role            string                   `json:"role,omitempty"`
	Changed         bool                     `json:"changed,omitempty"`
}

// The two values Source takes. A tenant that has never published a catalog
// tracks the server's own defaults as they move, which is what "default" says.
const (
	templateSourceTenant  = "tenant"
	templateSourceDefault = "default"
)

func templateCatalogResponse(tenantID string, view state.TenantTemplateCatalogView) TenantTemplateCatalogResponse {
	source := templateSourceDefault
	if view.Custom {
		source = templateSourceTenant
	}
	templates := view.Catalog.Templates
	if templates == nil {
		// A tenant that publishes nothing answers an empty array, not null: the
		// console reads this as a gallery, and null is a shape it would have to
		// special-case into the same empty gallery anyway.
		templates = []state.WorkloadTemplate{}
	}
	return TenantTemplateCatalogResponse{
		TenantID:        tenantID,
		CatalogRevision: view.Revision,
		Source:          source,
		Templates:       templates,
		Role:            view.Role,
		Changed:         view.Changed,
	}
}

// HandleGetTemplates serves GET /v1/tenants/{tenant_id}/templates.
//
// Every member reads it, viewers included, and for the cluster read's reason:
// the ids are what a member's own submission has to name, and a seat that
// cannot see the catalog cannot launch from it. No role sees another tenant's,
// which is why the store authorizes the read against the caller's own grant in
// the lock it takes the record in — an unknown tenant, a revoked one and a
// non-member are one answer, so this route cannot enumerate tenant ids either.
func (a *Accounts) HandleGetTemplates(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	view, err := a.Store.TenantTemplateCatalogFor(tenantID, caller.ID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			tenantNotFound(w)
			return
		}
		a.Log.Error("account: read tenant template catalog", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, templateCatalogResponse(tenantID, view))
}

// HandlePutTemplates serves PUT /v1/tenants/{tenant_id}/templates.
//
// A whole-catalog replacement rather than a per-entry edit, matching the
// cluster policy: the catalog is one published set, and a partial edit surface
// would need a merge rule that two managers editing at once could disagree
// about. Owner and admin may write it; the store decides that, so a refusal is
// journaled with the same authority the write would have been.
//
// The body is capped and decoded with unknown fields refused, because this is
// durable tenant state a browser renders: a field central does not understand
// is one a console believes it saved.
func (a *Accounts) HandlePutTemplates(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTemplateCatalogBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req struct {
		CatalogRevision string                   `json:"catalog_revision"`
		Templates       []state.WorkloadTemplate `json:"templates"`
	}
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid template catalog JSON"})
		}
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid template catalog JSON"})
		return
	}
	view, err := a.Store.SetTenantTemplateCatalogIfRevision(
		tenantID,
		caller.ID,
		req.CatalogRevision,
		state.WorkloadTemplateCatalog{Templates: req.Templates},
		state.HumanActor(caller.ID, tenantID),
	)
	if err != nil {
		switch {
		case errors.Is(err, state.ErrNotFound):
			tenantNotFound(w)
		case errors.Is(err, state.ErrNotAuthorized):
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing launch templates requires owner or admin"})
		case errors.Is(err, state.ErrInvalidTemplateCatalog):
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		case errors.Is(err, state.ErrTemplateCatalogConflict):
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		default:
			a.Log.Error("account: set tenant template catalog", "tenant", tenantID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		}
		return
	}
	writeJSON(w, http.StatusOK, templateCatalogResponse(tenantID, view))
}
