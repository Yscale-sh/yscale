// yscale:proprietary

package handlers

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/central/internal/credentialcipher"
	"github.com/yscale-sh/yscale/central/internal/state"
)

const maxCloudAccountRequestBytes = 16 << 10

type CredentialCipher interface {
	Encrypt(string, []byte) (string, error)
	Decrypt(string, []byte) (string, error)
}

type tenantCloudAccountResponse struct {
	TenantID string                     `json:"tenant_id"`
	Role     string                     `json:"role"`
	Account  *state.CloudAccountSummary `json:"account"`
	Changed  bool                       `json:"changed,omitempty"`
}

func (a *Accounts) HandleGetLinodeCloudAccount(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	summary, role, err := a.Store.TenantLinodeCloudAccountFor(tenantID, caller.ID)
	if err != nil {
		a.writeMembershipReadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tenantCloudAccountResponse{TenantID: tenantID, Role: role, Account: summary})
}

func (a *Accounts) HandlePutLinodeCloudAccount(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	membership, err := a.Store.MembershipFor(caller.ID, tenantID)
	if err != nil {
		a.writeMembershipReadError(w, err)
		return
	}
	if membership.Role != state.RoleOwner && membership.Role != state.RoleAdmin {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing cloud accounts requires owner or admin"})
		return
	}
	if a.CredentialCipher == nil || a.ValidateLinodeAccount == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "cloud account connections are unavailable"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxCloudAccountRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req struct {
		Token    string `json:"token"`
		Region   string `json:"region"`
		CPUImage string `json:"cpu_image"`
		GPUImage string `json:"gpu_image"`
	}
	if err := dec.Decode(&req); err != nil {
		writeCloudAccountDecodeError(w, err)
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cloud account JSON"})
		return
	}
	req.Token, req.Region = strings.TrimSpace(req.Token), strings.TrimSpace(req.Region)
	req.CPUImage, req.GPUImage = strings.TrimSpace(req.CPUImage), strings.TrimSpace(req.GPUImage)
	if req.Token == "" || len(req.Token) > 4096 || req.Region == "" || len(req.Region) > 64 || len(req.CPUImage) > 256 || len(req.GPUImage) > 256 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cloud account configuration"})
		return
	}
	identity, err := a.ValidateLinodeAccount(r.Context(), req.Token, req.Region)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Linode credentials or region could not be validated"})
		return
	}
	accountID := ""
	if current, getErr := a.Store.LinodeCloudAccount(tenantID); getErr == nil {
		accountID = current.ID
		if current.ProviderIdentity != identity {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "credential belongs to a different Linode account"})
			return
		}
	} else if !errors.Is(getErr, state.ErrNotFound) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	if accountID == "" {
		accountID, err = newCloudAccountID()
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "cloud account connection failed"})
			return
		}
	}
	ciphertext, err := a.CredentialCipher.Encrypt(req.Token, credentialcipher.AdditionalData(tenantID, accountID, state.CloudProviderLinode))
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "cloud account connection failed"})
		return
	}
	stored, rotated, err := a.Store.SetLinodeCloudAccount(tenantID, state.CloudAccount{ID: accountID, Provider: state.CloudProviderLinode,
		ProviderIdentity: identity, Region: req.Region, CPUImage: req.CPUImage, GPUImage: req.GPUImage,
		CredentialCiphertext: ciphertext, UpdatedAt: time.Now().UTC()}, state.HumanActor(caller.ID, tenantID))
	if err != nil {
		switch {
		case errors.Is(err, state.ErrNotAuthorized):
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing cloud accounts requires owner or admin"})
		case errors.Is(err, state.ErrCloudAccountMismatch):
			writeJSON(w, http.StatusConflict, map[string]string{"error": "credential belongs to a different Linode account"})
		case errors.Is(err, state.ErrNotFound):
			tenantNotFound(w)
		default:
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		}
		return
	}
	status := http.StatusCreated
	if rotated {
		status = http.StatusOK
	}
	summary := stored.Summary()
	writeJSON(w, status, tenantCloudAccountResponse{TenantID: tenantID, Role: membership.Role, Account: &summary, Changed: true})
}

func (a *Accounts) HandleDeleteLinodeCloudAccount(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	membership, err := a.Store.MembershipFor(caller.ID, tenantID)
	if err != nil {
		a.writeMembershipReadError(w, err)
		return
	}
	if membership.Role != state.RoleOwner && membership.Role != state.RoleAdmin {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing cloud accounts requires owner or admin"})
		return
	}
	current, err := a.Store.LinodeCloudAccount(tenantID)
	if err != nil {
		tenantNotFound(w)
		return
	}
	err = a.Store.DisconnectLinodeCloudAccount(tenantID, current.ID, time.Now().UTC(), state.HumanActor(caller.ID, tenantID))
	if err != nil {
		switch {
		case errors.Is(err, state.ErrNotAuthorized):
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: managing cloud accounts requires owner or admin"})
		case errors.Is(err, state.ErrCloudAccountInUse):
			writeJSON(w, http.StatusConflict, map[string]string{"error": "cloud account is in use by an active burst"})
		case errors.Is(err, state.ErrNotFound):
			tenantNotFound(w)
		default:
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		}
		return
	}
	writeJSON(w, http.StatusOK, tenantCloudAccountResponse{TenantID: tenantID, Role: membership.Role, Account: nil, Changed: true})
}

func writeCloudAccountDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cloud account JSON"})
}

func newCloudAccountID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "ca_" + hex.EncodeToString(b[:]), nil
}
