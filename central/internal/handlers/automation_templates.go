// yscale:proprietary

package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/yscale-sh/yscale/central/internal/state"
)

func (a *Accounts) HandleAutomationGetTemplates(w http.ResponseWriter, r *http.Request) {
	principal, ok := catalogPublisherFromContext(r.Context())
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	view, err := a.Store.AutomationTenantTemplateCatalogFor(principal.TenantID, principal.PublisherID)
	if err != nil {
		http.Error(w, "unauthorized: credential revoked", http.StatusUnauthorized)
		return
	}
	writeJSON(w, http.StatusOK, templateCatalogResponse(principal.TenantID, view))
}

func (a *Accounts) HandleAutomationPutTemplates(w http.ResponseWriter, r *http.Request) {
	principal, ok := catalogPublisherFromContext(r.Context())
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
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
	view, err := a.Store.SetAutomationTenantTemplateCatalogIfRevision(
		principal.TenantID, principal.PublisherID, req.CatalogRevision,
		state.WorkloadTemplateCatalog{Templates: req.Templates},
	)
	if err != nil {
		switch {
		case errors.Is(err, state.ErrNotFound):
			http.Error(w, "unauthorized: credential revoked", http.StatusUnauthorized)
		case errors.Is(err, state.ErrInvalidTemplateCatalog):
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		case errors.Is(err, state.ErrTemplateCatalogConflict):
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		default:
			a.Log.Error("automation: set tenant template catalog", "tenant", principal.TenantID, "publisher", principal.PublisherID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		}
		return
	}
	writeJSON(w, http.StatusOK, templateCatalogResponse(principal.TenantID, view))
}
