// yscale:proprietary

package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

const maxRuntimeBindingRequestBytes = 10 << 10

type runtimeBindingsResponse struct {
	TenantID string                        `json:"tenant_id"`
	Role     string                        `json:"role"`
	Bindings []state.RuntimeBindingSummary `json:"bindings"`
}

type runtimeBindingResponse struct {
	TenantID string                      `json:"tenant_id"`
	Role     string                      `json:"role"`
	Binding  state.RuntimeBindingSummary `json:"binding"`
}

func (a *Accounts) HandleListRuntimeBindings(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	rows, role, err := a.Store.RuntimeBindingsFor(tenantID, caller.ID, "pending")
	if err != nil {
		a.writeMembershipReadError(w, err)
		return
	}
	if rows == nil {
		rows = []state.RuntimeBindingSummary{}
	}
	writeJSON(w, http.StatusOK, runtimeBindingsResponse{TenantID: tenantID, Role: role, Bindings: rows})
}

func (a *Accounts) HandlePutRuntimeBinding(w http.ResponseWriter, r *http.Request) {
	tenantID, key := r.PathValue("tenant_id"), r.PathValue("key")
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	if !protocol.ValidRuntimeBindingKey(key) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid runtime binding key"})
		return
	}
	if a.CredentialCipher == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "runtime bindings are unavailable"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRuntimeBindingRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := dec.Decode(&req); err != nil {
		writeRuntimeBindingDecodeError(w, err)
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid runtime binding JSON"})
		return
	}
	if err := protocol.ValidateRuntimeBindingValue(req.Value); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid runtime binding value"})
		return
	}
	bindingID, err := a.Store.RuntimeBindingIDForKey(tenantID, key)
	if err != nil {
		a.writeRuntimeBindingError(w, tenantID, err)
		return
	}
	ciphertext, err := a.CredentialCipher.Encrypt(req.Value, state.RuntimeBindingAdditionalData(tenantID, bindingID, key))
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "runtime binding encryption failed"})
		return
	}
	now := time.Now().UTC()
	summary, role, err := a.Store.SetRuntimeBinding(tenantID, caller.ID, state.RuntimeBinding{
		ID: bindingID, Key: key, Name: req.Name, CredentialCiphertext: ciphertext,
		Revision: now.UnixNano(), CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		a.writeRuntimeBindingError(w, tenantID, err)
		return
	}
	a.syncRuntimeBindings(tenantID)
	writeJSON(w, http.StatusOK, runtimeBindingResponse{TenantID: tenantID, Role: role, Binding: summary})
}

func (a *Accounts) HandleDeleteRuntimeBinding(w http.ResponseWriter, r *http.Request) {
	tenantID, key := r.PathValue("tenant_id"), r.PathValue("key")
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	if !protocol.ValidRuntimeBindingKey(key) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid runtime binding key"})
		return
	}
	if !requireEmptyRuntimeBindingBody(w, r) {
		return
	}
	if _, _, err := a.Store.DeleteRuntimeBinding(tenantID, caller.ID, key); err != nil {
		a.writeRuntimeBindingError(w, tenantID, err)
		return
	}
	a.syncRuntimeBindings(tenantID)
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "deleted": true})
}

func requireEmptyRuntimeBindingBody(w http.ResponseWriter, r *http.Request) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1)
	var one [1]byte
	n, err := r.Body.Read(one[:])
	if n != 0 || (err != nil && err != io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body must be empty"})
		return false
	}
	return true
}

func writeRuntimeBindingDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid runtime binding JSON"})
}

func (a *Accounts) writeRuntimeBindingError(w http.ResponseWriter, tenantID string, err error) {
	switch {
	case errors.Is(err, state.ErrRuntimeBindingNotFound), errors.Is(err, state.ErrNotFound):
		tenantNotFound(w)
	case errors.Is(err, state.ErrNotAuthorized):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing runtime bindings requires owner or admin"})
	case errors.Is(err, state.ErrInvalidRuntimeBinding):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid runtime binding"})
	case errors.Is(err, state.ErrRuntimeBindingLimit):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	default:
		a.Log.Error("account: runtime binding", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
	}
}

func (a *Accounts) syncRuntimeBindings(tenantID string) {
	if a.CredentialCipher == nil {
		return
	}
	cust, err := a.Store.CustomerByID(tenantID)
	if err != nil || cust.Revoked() {
		return
	}
	rows, err := a.Store.RuntimeBindingRows(tenantID)
	if err != nil {
		return
	}
	desired := make(map[string]string, len(rows))
	revision := cust.RuntimeBindingsRevision
	for _, row := range rows {
		value, decErr := a.CredentialCipher.Decrypt(row.CredentialCiphertext, state.RuntimeBindingAdditionalData(tenantID, row.ID, row.Key))
		if decErr != nil {
			a.Log.Error("runtime binding decrypt failed", "tenant", tenantID, "binding", row.ID, "key", row.Key)
			return
		}
		desired[row.Key] = value
		if row.Revision > revision {
			revision = row.Revision
		}
	}
	allowed := authorizedWorkloadNamespaces(cust)
	for _, agent := range a.Store.AgentsForCustomer(tenantID) {
		namespaces := allowed
		if agent.WorkloadNamespace != "" {
			if !slices.Contains(allowed, agent.WorkloadNamespace) {
				continue
			}
			namespaces = []string{agent.WorkloadNamespace}
		}
		for _, namespace := range namespaces {
			body, err := json.Marshal(protocol.SyncRuntimeBindings{Namespace: namespace, Revision: revision, Bindings: desired})
			if err != nil {
				continue
			}
			_ = enqueue(agent, protocol.Envelope{
				APIVersion: protocol.APIVersion,
				Type:       protocol.TypeSyncRuntimeBindings,
				ID:         newID("cmd"),
				Timestamp:  time.Now().UTC(),
				Body:       body,
			})
		}
	}
}
