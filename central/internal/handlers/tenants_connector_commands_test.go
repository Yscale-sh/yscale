// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// requeueAuditLedger captures the audit event the handler hands the store, so a
// test can assert what would be journaled without a database.
type requeueAuditLedger struct {
	*state.Store
	mu     sync.Mutex
	events []*state.AuditEvent
}

func (l *requeueAuditLedger) RequeueConnectorCommand(ctx context.Context, customerID, id string,
	ev *state.AuditEvent) (state.ConnectorCommandStatus, error) {
	l.mu.Lock()
	l.events = append(l.events, ev)
	l.mu.Unlock()
	return l.Store.RequeueConnectorCommand(ctx, customerID, id, ev)
}

func (l *requeueAuditLedger) auditEvents() []*state.AuditEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*state.AuditEvent(nil), l.events...)
}

// adminRequest is a request as AdminAuth leaves it: the static operator token
// was verified by the middleware, which puts NOTHING in the context — no
// customer, no submitter, no operator account. The tenant is the path.
func adminRequest(t *testing.T, method, target, tenantID, commandID string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req.SetPathValue("tenant_id", tenantID)
	if commandID != "" {
		req.SetPathValue("id", commandID)
	}
	return req
}

// operatorRequest is a request as OperatorAuth leaves it: same path values,
// plus the account id the middleware verified for the signed-in human.
func operatorRequest(t *testing.T, method, target, tenantID, commandID, accountID string) *http.Request {
	t.Helper()
	req := adminRequest(t, method, target, tenantID, commandID)
	return req.WithContext(context.WithValue(req.Context(), ctxOperatorAccount, accountID))
}

func operatorTenants(store *state.Store) (*Tenants, *requeueAuditLedger) {
	ledger := &requeueAuditLedger{Store: store}
	return &Tenants{Store: store, Commands: ledger, Log: quietLog()}, ledger
}

// The requeue is scoped by the {tenant_id} PATH segment and by nothing else.
// A body naming another tenant changes nothing, a customer context left over
// from some other middleware changes nothing, and one tenant's path cannot
// reach another tenant's command — a foreign id is 404, indistinguishable from
// one that never existed, so the route cannot be used to probe for ids.
func TestOperatorRequeueScopesByPathTenantAndNeverByBody(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_owner", Token: "t-owner"})
	store.AddCustomer(&state.Customer{ID: "cust_thief", Token: "t-thief"})
	stuck := connectorCommandFor(t, "cust_owner", "cluster-owner", protocol.TypeBurstAnnounce, "burst-stuck")
	deadLetterCommand(t, store, stuck)

	h, _ := operatorTenants(store)

	// The thief's path, the owner's command id: 404, and the command is
	// untouched.
	rec := httptest.NewRecorder()
	h.HandleRequeueTenantConnectorCommand(rec,
		adminRequest(t, http.MethodPost, "/v1/admin/tenants/cust_thief/connector-commands/"+stuck.ID+"/requeue",
			"cust_thief", stuck.ID))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant requeue = %d, want 404", rec.Code)
	}
	if after := attentionList(t, store, "cust_owner"); len(after) != 1 ||
		after[0].State != state.ConnectorCommandDeadLetter {
		t.Fatalf("cross-tenant requeue moved the command: %+v", after)
	}

	// A body naming the owner, on the thief's path, is still the thief's
	// request: the handler reads no tenant from a body at all.
	rec = httptest.NewRecorder()
	spoofed := httptest.NewRequest(http.MethodPost,
		"/v1/admin/tenants/cust_thief/connector-commands/"+stuck.ID+"/requeue",
		strings.NewReader(`{"tenant_id":"cust_owner","customer_id":"cust_owner"}`))
	spoofed.SetPathValue("tenant_id", "cust_thief")
	spoofed.SetPathValue("id", stuck.ID)
	// Belt and braces: even a customer context (which no operator middleware
	// sets) must not become the scope.
	spoofed = spoofed.WithContext(context.WithValue(spoofed.Context(), ctxCustomer, &state.Customer{ID: "cust_owner"}))
	h.HandleRequeueTenantConnectorCommand(rec, spoofed)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("body-spoofed requeue = %d, want 404", rec.Code)
	}
	if after := attentionList(t, store, "cust_owner"); len(after) != 1 ||
		after[0].State != state.ConnectorCommandDeadLetter {
		t.Fatalf("body-spoofed requeue moved the command: %+v", after)
	}

	// The owner's own path works, and is explicit: exactly the one named id
	// moves, and a second call is refused out loud rather than silently
	// re-running.
	rec = httptest.NewRecorder()
	h.HandleRequeueTenantConnectorCommand(rec,
		adminRequest(t, http.MethodPost, "/v1/admin/tenants/cust_owner/connector-commands/"+stuck.ID+"/requeue",
			"cust_owner", stuck.ID))
	if rec.Code != http.StatusOK {
		t.Fatalf("owner-path requeue = %d, want 200: %s", rec.Code, rec.Body)
	}
	var requeued struct {
		Status   string                       `json:"status"`
		TenantID string                       `json:"tenant_id"`
		Command  state.ConnectorCommandStatus `json:"connector_command"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &requeued); err != nil {
		t.Fatal(err)
	}
	if requeued.Status != "requeued" || requeued.TenantID != "cust_owner" ||
		requeued.Command.ID != stuck.ID || requeued.Command.State != state.ConnectorCommandPending ||
		requeued.Command.Attempts != 0 {
		t.Fatalf("requeue response = %+v", requeued)
	}

	rec = httptest.NewRecorder()
	h.HandleRequeueTenantConnectorCommand(rec,
		adminRequest(t, http.MethodPost, "/v1/admin/tenants/cust_owner/connector-commands/"+stuck.ID+"/requeue",
			"cust_owner", stuck.ID))
	if rec.Code != http.StatusConflict {
		t.Fatalf("requeue of a non-dead-letter = %d, want 409", rec.Code)
	}

	// An id that belongs to nobody is the same 404 as one that belongs to
	// someone else.
	rec = httptest.NewRecorder()
	h.HandleRequeueTenantConnectorCommand(rec,
		adminRequest(t, http.MethodPost, "/v1/admin/tenants/cust_owner/connector-commands/ccmd_nope/requeue",
			"cust_owner", "ccmd_nope"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown-id requeue = %d, want 404", rec.Code)
	}
}

// The audit actor is whichever operator credential the MIDDLEWARE verified:
// the static operator on AdminAuth, the signed-in human's account on
// OperatorAuth. Never a cluster — that fallback belongs to the connector path,
// and no connector may reach this route.
func TestOperatorRequeueAuditsTheOperatorCredentialNeverACluster(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_actor", Token: "t-actor"})
	staticStuck := connectorCommandFor(t, "cust_actor", "cluster-actor", protocol.TypeBurstAnnounce, "burst-static")
	humanStuck := connectorCommandFor(t, "cust_actor", "cluster-actor", protocol.TypeBurstAnnounce, "burst-human")
	deadLetterCommand(t, store, staticStuck)
	deadLetterCommand(t, store, humanStuck)

	h, ledger := operatorTenants(store)

	// Static admin token: no operator account in the context.
	rec := httptest.NewRecorder()
	h.HandleRequeueTenantConnectorCommand(rec,
		adminRequest(t, http.MethodPost, "/v1/admin/tenants/cust_actor/connector-commands/"+staticStuck.ID+"/requeue",
			"cust_actor", staticStuck.ID))
	if rec.Code != http.StatusOK {
		t.Fatalf("admin requeue = %d, want 200: %s", rec.Code, rec.Body)
	}

	// Browser operator: OperatorAuth verified account acct_op into the context.
	rec = httptest.NewRecorder()
	h.HandleRequeueTenantConnectorCommand(rec,
		operatorRequest(t, http.MethodPost, "/v1/operator/tenants/cust_actor/connector-commands/"+humanStuck.ID+"/requeue",
			"cust_actor", humanStuck.ID, "acct_op"))
	if rec.Code != http.StatusOK {
		t.Fatalf("operator requeue = %d, want 200: %s", rec.Code, rec.Body)
	}

	events := ledger.auditEvents()
	if len(events) != 2 {
		t.Fatalf("audit events = %d, want one per accepted requeue", len(events))
	}
	static, human := events[0], events[1]
	for _, ev := range events {
		if ev.Actor.Kind == state.ActorCluster {
			t.Fatalf("requeue attributed to a cluster: %+v", ev.Actor)
		}
		if ev.CustomerID != "cust_actor" || ev.Action != state.ActionConnectorCommandRequeue ||
			ev.Outcome != state.OutcomeAccepted || ev.TargetKind != state.TargetConnectorCommand ||
			ev.Detail.Reason != state.ReasonConnectorCommandRequeued {
			t.Fatalf("requeue audit event = %+v", ev)
		}
	}
	if static.Actor != (state.Actor{Kind: state.ActorOperator}) || static.TargetID != staticStuck.ID {
		t.Fatalf("static admin requeue actor = %+v (target %q), want the bare operator actor",
			static.Actor, static.TargetID)
	}
	if human.Actor != state.HumanActor("acct_op", "cust_actor") || human.TargetID != humanStuck.ID {
		t.Fatalf("browser operator requeue actor = %+v (target %q), want the signed-in account",
			human.Actor, human.TargetID)
	}
	// The detail is a closed reason code and nothing else — no role, no
	// connector-authored text, no identity beyond the account id above.
	blob, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(blob), "connector-authored-sensitive-value") ||
		strings.Contains(string(blob), "user.token") {
		t.Fatalf("requeue audit events carried connector text: %s", blob)
	}
}

// The operator attention list answers for the tenant the path names, reads
// only, and shows one tenant's stalls to a request that names another tenant
// never.
func TestOperatorConnectorCommandListIsTenantScopedAndReadOnly(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_a", Token: "t-a"})
	store.AddCustomer(&state.Customer{ID: "cust_b", Token: "t-b"})
	stuckA := connectorCommandFor(t, "cust_a", "cluster-a", protocol.TypeBurstAnnounce, "burst-a")
	deadLetterCommand(t, store, stuckA)

	h, ledger := operatorTenants(store)

	decode := func(t *testing.T, rec *httptest.ResponseRecorder) struct {
		TenantID string                         `json:"tenant_id"`
		Count    int                            `json:"count"`
		Commands []state.ConnectorCommandStatus `json:"connector_commands"`
	} {
		t.Helper()
		var listed struct {
			TenantID string                         `json:"tenant_id"`
			Count    int                            `json:"count"`
			Commands []state.ConnectorCommandStatus `json:"connector_commands"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
			t.Fatalf("decode %q: %v", rec.Body.String(), err)
		}
		return listed
	}

	rec := httptest.NewRecorder()
	h.HandleListTenantConnectorCommands(rec,
		adminRequest(t, http.MethodGet, "/v1/admin/tenants/cust_a/connector-commands", "cust_a", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("admin list = %d, want 200", rec.Code)
	}
	listed := decode(t, rec)
	if listed.TenantID != "cust_a" || listed.Count != 1 || listed.Commands[0].ID != stuckA.ID {
		t.Fatalf("admin list = %+v, want cust_a's one dead letter", listed)
	}

	// The browser-operator twin is the same handler and the same answer.
	rec = httptest.NewRecorder()
	h.HandleListTenantConnectorCommands(rec,
		operatorRequest(t, http.MethodGet, "/v1/operator/tenants/cust_a/connector-commands", "cust_a", "", "acct_op"))
	if rec.Code != http.StatusOK || decode(t, rec).Count != 1 {
		t.Fatalf("operator list = %d %s", rec.Code, rec.Body)
	}

	// Another tenant's path shows nothing of cust_a's.
	rec = httptest.NewRecorder()
	h.HandleListTenantConnectorCommands(rec,
		adminRequest(t, http.MethodGet, "/v1/admin/tenants/cust_b/connector-commands", "cust_b", ""))
	listed = decode(t, rec)
	if listed.TenantID != "cust_b" || listed.Count != 0 {
		t.Fatalf("cust_b list = %+v, want empty", listed)
	}

	// Reads write nothing: no audit row was offered and the command is still a
	// dead letter.
	if events := ledger.auditEvents(); len(events) != 0 {
		t.Fatalf("the read offered %d audit events, want none", len(events))
	}
	if after := attentionList(t, store, "cust_a"); len(after) != 1 ||
		after[0].State != state.ConnectorCommandDeadLetter {
		t.Fatalf("the read moved the command: %+v", after)
	}
}

// A deployment with no ledger wired answers honestly rather than pretending:
// the read is empty and the requeue is 404, so nothing reports success for a
// command it never touched. A path missing either segment is 404 too.
func TestOperatorConnectorCommandRoutesFailClosedWithoutALedger(t *testing.T) {
	store := state.New()
	store.AddCustomer(&state.Customer{ID: "cust_none", Token: "t-none"})
	h := &Tenants{Store: store, Log: quietLog()}

	rec := httptest.NewRecorder()
	h.HandleListTenantConnectorCommands(rec,
		adminRequest(t, http.MethodGet, "/v1/admin/tenants/cust_none/connector-commands", "cust_none", ""))
	if rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"count":0`) {
		t.Fatalf("list without a ledger = %d %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	h.HandleRequeueTenantConnectorCommand(rec,
		adminRequest(t, http.MethodPost, "/v1/admin/tenants/cust_none/connector-commands/ccmd_x/requeue",
			"cust_none", "ccmd_x"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("requeue without a ledger = %d, want 404", rec.Code)
	}

	wired, _ := operatorTenants(store)
	for name, req := range map[string]*http.Request{
		"no tenant": adminRequest(t, http.MethodPost, "/v1/admin/tenants//connector-commands/ccmd_x/requeue", "", "ccmd_x"),
		"no id":     adminRequest(t, http.MethodPost, "/v1/admin/tenants/cust_none/connector-commands//requeue", "cust_none", ""),
	} {
		rec = httptest.NewRecorder()
		wired.HandleRequeueTenantConnectorCommand(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("requeue with %s = %d, want 404", name, rec.Code)
		}
	}
	rec = httptest.NewRecorder()
	wired.HandleListTenantConnectorCommands(rec,
		adminRequest(t, http.MethodGet, "/v1/admin/tenants//connector-commands", "", ""))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("list with no tenant = %d, want 404", rec.Code)
	}
}
