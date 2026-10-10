// yscale:proprietary

package handlers

import (
	"errors"
	"net/http"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// TenantHostedCapacityResponse is the complete tenant-facing shared-capacity
// contract. RequestedAt is omitted unless the request is pending.
type TenantHostedCapacityResponse struct {
	TenantID    string     `json:"tenant_id"`
	Role        string     `json:"role"`
	Status      string     `json:"status"`
	RequestedAt *time.Time `json:"requested_at,omitempty"`
}

func hostedCapacityResponse(tenantID string, view state.TenantHostedCapacity) TenantHostedCapacityResponse {
	return TenantHostedCapacityResponse{
		TenantID: tenantID, Role: view.Role, Status: view.Status, RequestedAt: view.RequestedAt,
	}
}

func (a *Accounts) HandleGetHostedCapacity(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	view, err := a.Store.TenantHostedCapacityFor(tenantID, caller.ID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			tenantNotFound(w)
			return
		}
		a.Log.Error("account: read hosted capacity", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, hostedCapacityResponse(tenantID, view))
}

func (a *Accounts) HandleRequestHostedCapacity(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	view, created, err := a.Store.RequestTenantHostedCapacity(
		tenantID, caller.ID, state.HumanActor(caller.ID, tenantID),
	)
	if err != nil {
		switch {
		case errors.Is(err, state.ErrNotFound):
			tenantNotFound(w)
		case errors.Is(err, state.ErrNotAuthorized):
			writeForbidden(w)
		default:
			a.Log.Error("account: request hosted capacity", "tenant", tenantID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		}
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	writeJSON(w, status, hostedCapacityResponse(tenantID, view))
}
