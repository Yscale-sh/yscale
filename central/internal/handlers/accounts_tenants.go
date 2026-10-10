// yscale:proprietary

package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// SelfServiceTenantRequest is the POST /v1/account/tenants body: the display
// name of the workspace to create.
type SelfServiceTenantRequest struct {
	Name string `json:"name"`
}

const (
	maxSelfServiceTenantNameRunes    = 64
	maxSelfServiceTenantRequestBytes = 1 << 10

	selfServicePlan                = "trial"
	selfServiceMaxConcurrentBursts = 1
	selfServiceMaxHourlyUSD        = 1.0
)

func decodeSelfServiceTenantRequest(w http.ResponseWriter, r *http.Request) (SelfServiceTenantRequest, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSelfServiceTenantRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request SelfServiceTenantRequest
	if err := decoder.Decode(&request); err != nil {
		return SelfServiceTenantRequest{}, errors.New("body must be a JSON object with name")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return SelfServiceTenantRequest{}, errors.New("body must be a single JSON object")
	}
	return request, nil
}

func selfServiceTenantName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if name == "" {
		return "", errors.New("name is required")
	}
	if !utf8.ValidString(name) {
		return "", errors.New("name must be valid UTF-8")
	}
	if utf8.RuneCountInString(name) > maxSelfServiceTenantNameRunes {
		return "", fmt.Errorf("name must be at most %d characters", maxSelfServiceTenantNameRunes)
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "", errors.New("name must not contain control characters")
		}
	}
	return name, nil
}

// HandleCreateTenant serves POST /v1/account/tenants for opt-in first-run SaaS
// onboarding. It creates the caller's one deterministic tenant only while the
// caller has no memberships, so retries and concurrent replicas collide on the
// same durable tenant id rather than buying multiple workspaces.
func (a *Accounts) HandleCreateTenant(w http.ResponseWriter, r *http.Request) {
	if !a.AllowSelfServiceTenants {
		http.NotFound(w, r)
		return
	}
	identity, ok := a.resolveCaller(w, r)
	if !ok {
		return
	}
	account, err := a.Store.UpsertAccount(a.Issuer, identity.Subject, state.AccountProfile{
		Email:         identity.Email,
		EmailVerified: identity.EmailVerified,
		Name:          identity.Name,
	})
	if err != nil {
		if errors.Is(err, state.ErrPersistence) {
			a.Log.Error("account: persist account", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
			return
		}
		a.Log.Warn("account: rejected identity", "error", err)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized: invalid identity"})
		return
	}
	if !account.EmailVerified {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: creating a workspace requires a verified email"})
		return
	}
	request, err := decodeSelfServiceTenantRequest(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	name, err := selfServiceTenantName(request.Name)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	tenant := &state.Customer{
		ID:                  state.SelfServiceTenantID(account.ID),
		Token:               state.NewCustomerToken(),
		Email:               account.Email,
		Plan:                selfServicePlan,
		Name:                name,
		MaxConcurrentBursts: selfServiceMaxConcurrentBursts,
		MaxHourlyUSD:        selfServiceMaxHourlyUSD,
	}
	if _, _, err := a.Store.CreateFirstTenant(tenant, account.ID); err != nil {
		if errors.Is(err, state.ErrAccountEmailUnverified) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: creating a workspace requires a verified email"})
			return
		}
		if errors.Is(err, state.ErrCustomerExists) || errors.Is(err, state.ErrAccountHasTenant) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "conflict: this account already belongs to a tenant"})
			return
		}
		a.Log.Error("account: create self-service tenant", "account", account.ID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	a.Log.Info("self-service tenant created", "tenant", tenant.ID, "account", account.ID, "plan", selfServicePlan)

	tenants, err := a.tenantsFor(r.Context(), account.ID)
	if err != nil {
		a.Log.Error("account: read created tenant", "account", account.ID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account store unavailable"})
		return
	}
	writeJSON(w, http.StatusCreated, AccountResponse{
		AccountID:     account.ID,
		Issuer:        account.Issuer,
		Subject:       account.Subject,
		Email:         account.Email,
		EmailVerified: account.EmailVerified,
		Name:          account.Name,
		CreatedAt:     account.CreatedAt,
		Tenants:       tenants,
	})
}
