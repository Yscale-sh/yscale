// yscale:proprietary

package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// The tenant audit read: GET /v1/tenants/{tenant_id}/audit.
//
// It rides the same human credential and the same hook as the roster, and it is
// scoped to a tenant's own managers for the same reason the roster is — the
// journal names who submitted what and whose access changed, which is the
// tenant's business and no other tenant's. Multi-tenancy is enterprise, so the
// route lives in this build only.

// TenantAuditResponse is one page of a tenant's governance journal, newest
// first.
type TenantAuditResponse struct {
	Events []TenantAuditEvent `json:"events"`
	// NextAfter is present ONLY when there are older rows behind this page, and
	// is what the caller sends back as ?after= to get them. Absent means the
	// journal ends here, so a client that ignores it never mistakes a truncated
	// history for a complete one.
	NextAfter string `json:"next_after,omitempty"`
}

// TenantAuditEvent is one journal row as a tenant's owner or admin may see it.
//
// Every field is a closed constant, an id central minted, a role from the
// closed role set, or a validated namespace label — the shape mirrors
// state.AuditDetail, which is where the "no secrets by construction" property
// actually lives. What is absent here is absent because it is absent THERE:
// there is no access token, no spec, no env value, no command, no storage
// endpoint, no Secret name, no issuer or subject and no error body in an audit
// row to render. The actor's account id is the same handle the roster shows a
// co-member; the identity provider's key for that human is not.
type TenantAuditEvent struct {
	ID      string    `json:"id"`
	At      time.Time `json:"at"`
	Action  string    `json:"action"`
	Outcome string    `json:"outcome"`
	// Actor is who took the action: the kind of principal, plus the account id
	// when that principal is a human.
	Actor      TenantAuditActor  `json:"actor"`
	TargetKind string            `json:"target_kind,omitempty"`
	TargetID   string            `json:"target_id,omitempty"`
	Detail     TenantAuditDetail `json:"detail"`
}

type TenantAuditActor struct {
	Kind      string `json:"kind"`
	AccountID string `json:"account_id,omitempty"`
}

// TenantAuditDetail is the bounded payload. Rendered field by field rather than
// by embedding state.AuditDetail, so a field added to the stored shape for some
// internal reason cannot reach a response by inheriting a JSON tag.
type TenantAuditDetail struct {
	Reason                      string   `json:"reason,omitempty"`
	Role                        string   `json:"role,omitempty"`
	PreviousRole                string   `json:"previous_role,omitempty"`
	Rule                        string   `json:"rule,omitempty"`
	RuleVersion                 string   `json:"rule_version,omitempty"`
	RequestedNamespace          string   `json:"requested_namespace,omitempty"`
	GrantedNamespace            string   `json:"granted_namespace,omitempty"`
	Namespaces                  []string `json:"namespaces,omitempty"`
	Status                      string   `json:"status,omitempty"`
	BurstID                     string   `json:"burst_id,omitempty"`
	RetryWorkloadID             string   `json:"retry_workload_id,omitempty"`
	PreviousMaxConcurrentBursts *int     `json:"previous_max_concurrent_bursts,omitempty"`
	MaxConcurrentBursts         *int     `json:"max_concurrent_bursts,omitempty"`
	PreviousMaxHourlyUSD        *float64 `json:"previous_max_hourly_usd,omitempty"`
	MaxHourlyUSD                *float64 `json:"max_hourly_usd,omitempty"`
}

// parseAuditQuery reads the page bounds off a request, and rejects everything
// it rejects BEFORE the tenant is looked at — so a 400 is the same answer for a
// tenant that exists and one that does not.
//
// The rules are parseRosterQuery's, for parseRosterQuery's reasons: repeated
// and empty values carry two intents or none, an out-of-range limit silently
// clamped reports a truncated page as a complete one, and an unknown parameter
// means a caller who thinks they sent a cursor and got page one. The cursor is
// additionally checked for SHAPE, which the roster's account-id cursor does not
// need: an audit cursor is opaque, so a value that is not an audit id can only
// be a caller guessing at the ordering, and answering the newest page for it
// looks exactly like the page they asked for.
func parseAuditQuery(raw string) (state.AuditQuery, error) {
	values, err := url.ParseQuery(raw)
	if err != nil {
		return state.AuditQuery{}, errors.New("invalid query string")
	}
	var q state.AuditQuery
	for key, vals := range values {
		if key != "limit" && key != "after" {
			return state.AuditQuery{}, fmt.Errorf("unsupported query parameter %q; this route takes limit and after", key)
		}
		if len(vals) != 1 || vals[0] == "" {
			return state.AuditQuery{}, fmt.Errorf("%s must be given exactly once with a value", key)
		}
	}
	if v := values.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > state.MaxAuditLimit {
			return state.AuditQuery{}, fmt.Errorf("limit must be an integer between 1 and %d", state.MaxAuditLimit)
		}
		q.Limit = n
	}
	if v := values.Get("after"); v != "" {
		if !state.ValidAuditCursor(v) {
			return state.AuditQuery{}, errors.New("after must be a cursor returned by this route")
		}
		q.After = v
	}
	return q, nil
}

// HandleListAudit serves GET /v1/tenants/{tenant_id}/audit: one page of one
// tenant's governance journal, for an owner or admin of that tenant.
//
// Authorization belongs to the store, which resolves the tenant and the
// caller's grant together and scopes the durable read to that tenant in the
// statement rather than filtering rows it already fetched. This handler maps
// errors to statuses and renders the page it was handed. An empty journal is a
// 200 with an empty list: a tenant with no recorded history is not a missing
// tenant, and saying so would leak the difference to a manager who is entitled
// to know it either way.
func (a *Accounts) HandleListAudit(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	// After the credential, before the tenant — parseRosterQuery's ordering, and
	// for its reason: answering a malformed page bound first would tell a caller
	// on a deployment with no Yscale ID that the route is there at all.
	query, err := parseAuditQuery(r.URL.RawQuery)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	page, err := a.Store.TenantAuditFor(r.Context(), tenantID, caller.ID, query)
	if err != nil {
		switch {
		case errors.Is(err, state.ErrNotAuthorized):
			// A member or viewer already knows the tenant exists, so there is
			// nothing left to conceal: 403 tells them the truth, that the
			// journal is a management surface and their role is not.
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: reading the audit journal requires owner or admin"})
		case errors.Is(err, state.ErrNotFound):
			// Unknown tenant, revoked tenant and non-member are one answer.
			tenantNotFound(w)
		case errors.Is(err, state.ErrInvalidAudit):
			// A cursor the store refused that parseAuditQuery accepted; still
			// the caller's input, so still a 400.
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "after must be a cursor returned by this route"})
		default:
			// ErrPersistence and anything unrecognised: the journal could not be
			// read. Never reported as "tenant not found", which would tell a
			// manager their tenant's history was deleted and leave nothing in
			// the log to contradict it.
			a.Log.Error("account: read tenant audit", "tenant", tenantID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "audit store unavailable"})
		}
		return
	}

	events := make([]TenantAuditEvent, 0, len(page.Events))
	for _, ev := range page.Events {
		events = append(events, TenantAuditEvent{
			ID:      ev.ID,
			At:      ev.At,
			Action:  ev.Action,
			Outcome: ev.Outcome,
			Actor: TenantAuditActor{
				Kind:      ev.Actor.Kind,
				AccountID: ev.Actor.AccountID,
			},
			TargetKind: ev.TargetKind,
			TargetID:   ev.TargetID,
			Detail: TenantAuditDetail{
				Reason:                      ev.Detail.Reason,
				Role:                        ev.Detail.Role,
				PreviousRole:                ev.Detail.PreviousRole,
				Rule:                        ev.Detail.Rule,
				RuleVersion:                 ev.Detail.RuleVersion,
				RequestedNamespace:          ev.Detail.RequestedNamespace,
				GrantedNamespace:            ev.Detail.GrantedNamespace,
				Namespaces:                  ev.Detail.Namespaces,
				Status:                      ev.Detail.Status,
				BurstID:                     ev.Detail.BurstID,
				RetryWorkloadID:             ev.Detail.RetryWorkloadID,
				PreviousMaxConcurrentBursts: ev.Detail.PreviousMaxConcurrentBursts,
				MaxConcurrentBursts:         ev.Detail.MaxConcurrentBursts,
				PreviousMaxHourlyUSD:        ev.Detail.PreviousMaxHourlyUSD,
				MaxHourlyUSD:                ev.Detail.MaxHourlyUSD,
			},
		})
	}
	writeJSON(w, http.StatusOK, TenantAuditResponse{Events: events, NextAfter: page.NextAfter})
}
