// yscale:proprietary

package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// The operator half of the connector-command recovery surface: putting a
// dead-lettered command back on the delivery path, and the tenant-named
// attention list an operator reads before deciding to.
//
// Both routes exist twice, on the two enterprise recovery credentials this
// repo already has and on nothing else:
//
//	GET  /v1/admin/tenants/{tenant_id}/connector-commands              (AdminAuth)
//	POST /v1/admin/tenants/{tenant_id}/connector-commands/{id}/requeue (AdminAuth)
//	GET  /v1/operator/tenants/{tenant_id}/connector-commands              (OperatorAuth)
//	POST /v1/operator/tenants/{tenant_id}/connector-commands/{id}/requeue (OperatorAuth)
//
// The tenant surface keeps only the read (GET /v1/connector-commands). That is
// the whole point of the split: on the OSS and connector path a "tenant" token
// IS the cluster's own credential, so a requeue there would let a compromised
// connector re-drive the drains and announces central is trying to send it,
// attributed to a cluster. Requeue is an operator decision, made with an
// operator credential, and journaled against an operator.
//
// The tenant a route acts on is the {tenant_id} PATH segment and nothing else.
// No request body is read for it, and neither route consults the customer
// context: an operator credential is not scoped to a tenant, so the URL is the
// only place the scope can honestly come from — and the store then refuses any
// command id that does not belong to it.

// connectorCommandRecoveryLedger is the slice of the connector-command ledger
// the operator recovery routes touch: one bounded read and one explicit,
// single-command requeue that carries its own audit event. It names nothing
// from the delivery loop — no claim, no lease, no acknowledgement — so this
// surface cannot advance a command's delivery state by any path except the
// one an operator explicitly asked for.
type connectorCommandRecoveryLedger interface {
	ListConnectorCommandsNeedingAttention(ctx context.Context, customerID string, limit int) ([]state.ConnectorCommandStatus, error)
	RequeueConnectorCommand(ctx context.Context, customerID, id string, ev *state.AuditEvent) (state.ConnectorCommandStatus, error)
}

// HandleListTenantConnectorCommands serves the operator read of ONE named
// tenant's stalled connector commands: retryable failures and dead letters,
// bounded, in the same reduced status shape the tenant's own view returns —
// no envelope body, no acknowledgement result, no lease token.
//
// A pure read: no write, no audit row, and no cross-tenant listing. An
// operator who wants the whole fleet's stalls asks tenant by tenant, which is
// what keeps this route the same tenant-scoped query the store already
// enforces rather than a new all-tenants one.
func (t *Tenants) HandleListTenantConnectorCommands(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	if t.Commands == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"tenant_id": tenantID, "connector_commands": []state.ConnectorCommandStatus{}, "count": 0,
		})
		return
	}
	commands, err := t.Commands.ListConnectorCommandsNeedingAttention(r.Context(), tenantID, 0)
	switch {
	case errors.Is(err, state.ErrInvalidConnectorCommand):
		// A tenant id the ledger will not accept is a path that names nothing,
		// not a server fault.
		http.NotFound(w, r)
		return
	case err != nil:
		t.Log.Warn("operator list connector commands needing attention", "tenant", tenantID, "error", err)
		http.Error(w, "could not list connector commands", http.StatusServiceUnavailable)
		return
	}
	if commands == nil {
		commands = []state.ConnectorCommandStatus{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id":          tenantID,
		"connector_commands": commands,
		"count":              len(commands),
	})
}

// HandleRequeueTenantConnectorCommand puts ONE dead-lettered command back on
// the delivery path.
//
// Explicit, per command, and never inferred. Nothing in the ledger requeues a
// dead letter on its own, and nothing here requeues more than the one id the
// caller named in the URL — a "retry everything" button is how a connector
// that is rejecting a drain for a good reason gets hammered, and how an
// announce for a burst that no longer exists is re-delivered hours later.
//
// The command's identity is untouched: the store moves delivery state only,
// and the stored envelope is the one that was admitted. A command that is not
// a dead letter is 409 rather than a silent no-op, so an operator who requeued
// the wrong id learns it; a command that does not belong to {tenant_id} is
// 404, the same as one that does not exist, so an operator cannot use one
// tenant's path to probe another tenant's ids.
func (t *Tenants) HandleRequeueTenantConnectorCommand(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	commandID := r.PathValue("id")
	if tenantID == "" || commandID == "" {
		http.NotFound(w, r)
		return
	}
	if t.Commands == nil {
		http.NotFound(w, r)
		return
	}
	// The audit event is built HERE, from closed constants, central's own ids
	// and the actor the MIDDLEWARE verified — never a body, a header or a
	// tenant role. Detail carries a reason code and nothing else: the
	// command's own failure reason is already a closed code on the row, and
	// copying it into the journal would only give a future caller a field to
	// put a connector's text in.
	requeued, err := t.Commands.RequeueConnectorCommand(r.Context(), tenantID, commandID,
		state.NewAuditEvent(state.AuditEvent{
			CustomerID: tenantID,
			Actor:      operatorLifecycleActor(r, tenantID),
			Action:     state.ActionConnectorCommandRequeue,
			Outcome:    state.OutcomeAccepted,
			TargetKind: state.TargetConnectorCommand,
			TargetID:   commandID,
			Detail:     state.AuditDetail{Reason: state.ReasonConnectorCommandRequeued},
		}))
	switch {
	case err == nil:
		t.Log.Info("dead-letter connector command requeued by operator",
			"tenant", tenantID, "command", requeued.ID)
		writeJSON(w, http.StatusOK, map[string]any{
			"status":            "requeued",
			"tenant_id":         tenantID,
			"connector_command": requeued,
		})
	case errors.Is(err, state.ErrConnectorCommandNotFound), errors.Is(err, state.ErrInvalidConnectorCommand):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, state.ErrConnectorCommandNotDeadLettered):
		http.Error(w, "only a dead-letter connector command can be requeued", http.StatusConflict)
	default:
		// A requeue whose audit row would not write is refused, not performed:
		// this is an authorization decision, and the journal takes the same
		// fail-closed trade submit and cancel take.
		t.Log.Error("requeue connector command", "tenant", tenantID, "command", commandID, "error", err)
		http.Error(w, "could not requeue the connector command; please retry", http.StatusServiceUnavailable)
	}
}
