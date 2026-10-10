// yscale:proprietary

package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/yscale-sh/yscale/central/internal/state"
)

const maxCatalogPublisherRequestBytes = 4 << 10

type catalogPublishersResponse struct {
	Publishers []state.CatalogPublisherSummary `json:"publishers"`
}

type catalogPublisherCredentialResponse struct {
	Publisher state.CatalogPublisherSummary `json:"publisher"`
	Token     string                        `json:"token"`
}

func (a *Accounts) HandleListCatalogPublishers(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	rows, _, err := a.Store.CatalogPublishersFor(tenantID, caller.ID)
	if err != nil {
		a.writeCatalogPublisherError(w, tenantID, err)
		return
	}
	if rows == nil {
		rows = []state.CatalogPublisherSummary{}
	}
	writeJSON(w, http.StatusOK, catalogPublishersResponse{Publishers: rows})
}

func (a *Accounts) HandleCreateCatalogPublisher(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxCatalogPublisherRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req struct {
		Name string `json:"name"`
	}
	if err := dec.Decode(&req); err != nil {
		writeCatalogPublisherDecodeError(w, err)
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid catalog publisher JSON"})
		return
	}
	publisher, token, _, err := a.Store.CreateCatalogPublisher(tenantID, caller.ID, req.Name)
	if err != nil {
		a.writeCatalogPublisherError(w, tenantID, err)
		return
	}
	writeJSON(w, http.StatusCreated, catalogPublisherCredentialResponse{Publisher: publisher, Token: token})
}

func (a *Accounts) HandleRotateCatalogPublisherCredential(w http.ResponseWriter, r *http.Request) {
	tenantID, publisherID := r.PathValue("tenant_id"), r.PathValue("publisher_id")
	if tenantID == "" || publisherID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	if !requireEmptyCatalogPublisherBody(w, r) {
		return
	}
	publisher, token, _, err := a.Store.RotateCatalogPublisherCredential(tenantID, caller.ID, publisherID)
	if err != nil {
		a.writeCatalogPublisherError(w, tenantID, err)
		return
	}
	writeJSON(w, http.StatusOK, catalogPublisherCredentialResponse{Publisher: publisher, Token: token})
}

func (a *Accounts) HandleDeleteCatalogPublisher(w http.ResponseWriter, r *http.Request) {
	tenantID, publisherID := r.PathValue("tenant_id"), r.PathValue("publisher_id")
	if tenantID == "" || publisherID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	if !requireEmptyCatalogPublisherBody(w, r) {
		return
	}
	if _, err := a.Store.DeleteCatalogPublisher(tenantID, caller.ID, publisherID); err != nil {
		a.writeCatalogPublisherError(w, tenantID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"publisher_id": publisherID, "deleted": true})
}

func requireEmptyCatalogPublisherBody(w http.ResponseWriter, r *http.Request) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1)
	var one [1]byte
	n, err := r.Body.Read(one[:])
	if n != 0 || (err != nil && err != io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body must be empty"})
		return false
	}
	return true
}

func writeCatalogPublisherDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid catalog publisher JSON"})
}

func (a *Accounts) writeCatalogPublisherError(w http.ResponseWriter, tenantID string, err error) {
	switch {
	case errors.Is(err, state.ErrCatalogPublisherNotFound), errors.Is(err, state.ErrNotFound):
		tenantNotFound(w)
	case errors.Is(err, state.ErrNotAuthorized):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing catalog publishers requires owner or admin"})
	case errors.Is(err, state.ErrInvalidCatalogPublisher):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, state.ErrCatalogPublisherLimit):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	default:
		a.Log.Error("account: catalog publisher", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
	}
}
