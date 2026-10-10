package handlers

import (
	"net/http"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// The tenant half of the connector-command recovery surface: SEEING what
// automatic delivery could not complete.
//
// It is tenant-scoped like every other read on this handler — the customer
// comes from the authenticated context and is the only tenant it can name —
// and it is read-only on purpose. Putting a dead letter back on the delivery
// path is an operator action: the tenant credential this route accepts is, on
// the OSS and connector path, the CLUSTER's own token, and a credential that
// lives inside the customer's cluster must not be able to re-drive the
// commands central is trying to send it. The requeue therefore lives on the
// operator surfaces (tenants_connector_commands.go), behind the static admin
// token or a signed-in operator, and never here.
//
// The response speaks only ConnectorCommandStatus, which carries no envelope
// body, no acknowledgement result, no lease token and only closed reason
// codes. There is nothing here for a connector's text to travel through.

// ConnectorCommands serves GET /v1/connector-commands: the tenant's connector
// commands that automatic delivery could not complete — retryable failures and
// dead letters — newest first and bounded.
//
// This is the visibility half of "dead letters must be recoverable". Before it,
// a command that exhausted its attempts was a state nothing surfaced: the
// workload detail showed it only if the command carried that workload's id, and
// a burst-scoped drain carries none.
func (h *Workloads) ConnectorCommands(w http.ResponseWriter, r *http.Request) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if h.Commands == nil {
		writeJSON(w, http.StatusOK, map[string]any{"connector_commands": []state.ConnectorCommandStatus{}, "count": 0})
		return
	}
	commands, err := h.Commands.ListConnectorCommandsNeedingAttention(r.Context(), cust.ID, 0)
	if err != nil {
		h.Log.Warn("list connector commands needing attention", "customer", cust.ID, "error", err)
		http.Error(w, "could not list connector commands", http.StatusServiceUnavailable)
		return
	}
	if commands == nil {
		commands = []state.ConnectorCommandStatus{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"connector_commands": commands,
		"count":              len(commands),
	})
}
