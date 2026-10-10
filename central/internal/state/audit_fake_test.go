package state

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
)

// auditRecorder is the journal half of every fake persister in this package.
// Embedded rather than copied into each fake because the interesting behaviour
// is shared — rows are appended, never rewritten, and read back tenant-scoped,
// newest-first, exclusive of a cursor — and a per-fake reimplementation is how
// one of them ends up quietly returning another tenant's rows in the exact test
// that is supposed to prove it cannot.
//
// Rows are stored MARSHALLED, like accountSpyPersister's other collections and
// like the JSONB backend, so a detail field that does not survive encoding
// fails here too rather than passing on the in-memory struct.
type auditRecorder struct {
	mu   sync.Mutex
	rows []auditRow
	// appendErr and listErr are the two failure switches. They are what a test
	// flips to prove an authorizing mutation refuses rather than proceeding
	// unaudited, and that a durable read failure is never reported as a missing
	// tenant.
	appendErr error
	listErr   error

	// meshRow is the single global mesh-state row setMeshState wrote, nil until
	// one has been.
	meshRow *meshState
}

type auditRow struct {
	id         string
	customerID string
	data       []byte
}

func (r *auditRecorder) appendAudit(ev *AuditEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.appendLocked(ev)
}

// appendLocked is the append a fake's own transaction calls once it has decided
// its state write succeeds, so a refused state write leaves no journal row —
// the property the real backend gets from sharing one commit.
//
// It stamps the id here, holding r.mu, because that is what the durable backend
// does: insertAuditTx mints one only after it holds the tenant's advisory lock,
// so the id orders the commit. A fake that let the caller stamp would order rows
// by when they were BUILT and quietly pass a pagination test the database fails.
func (r *auditRecorder) appendLocked(ev *AuditEvent) error {
	if r.appendErr != nil {
		return r.appendErr
	}
	if ev == nil {
		return nil
	}
	stampAudit(ev, r.newestLocked(ev.CustomerID))
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	r.rows = append(r.rows, auditRow{id: ev.ID, customerID: ev.CustomerID, data: data})
	return nil
}

// newestLocked is the fake's SELECT max(id): the newest id already recorded for
// one tenant, "" when it has none.
func (r *auditRecorder) newestLocked(customerID string) string {
	var newest string
	for _, row := range r.rows {
		if row.customerID == customerID && row.id > newest {
			newest = row.id
		}
	}
	return newest
}

// withAudit runs a fake's state write and its audit append as one step: either
// both land or neither does. It is the fakes' stand-in for the Postgres
// transaction, and it exists so a test cannot pass because the fake was more
// forgiving than the database.
func (r *auditRecorder) withAudit(ev *AuditEvent, write func() error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.appendErr != nil {
		return r.appendErr
	}
	if err := write(); err != nil {
		return err
	}
	return r.appendLocked(ev)
}

func (r *auditRecorder) listAudit(_ context.Context, customerID, after string, limit int) ([]*AuditEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listErr != nil {
		return nil, r.listErr
	}
	rows := slices.Clone(r.rows)
	// Newest first by id, which is the ordering audit ids encode.
	slices.SortFunc(rows, func(a, b auditRow) int { return strings.Compare(b.id, a.id) })
	out := make([]*AuditEvent, 0, limit)
	for _, row := range rows {
		if row.customerID != customerID {
			continue
		}
		if after != "" && row.id >= after {
			continue
		}
		var ev AuditEvent
		if err := json.Unmarshal(row.data, &ev); err != nil {
			return nil, err
		}
		out = append(out, &ev)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// idsInCommitOrder returns the recorded ids in the order they were appended.
// The recorder appends under r.mu and stamps under it too, so that order IS the
// commit order — which is the whole point of the assertion it serves: ids that
// sort the same way commit order runs.
func (r *auditRecorder) idsInCommitOrder() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.rows))
	for _, row := range r.rows {
		out = append(out, row.id)
	}
	return out
}

// events returns the recorded rows decoded, oldest first, for assertions.
func (r *auditRecorder) events() []AuditEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]AuditEvent, 0, len(r.rows))
	for _, row := range r.rows {
		var ev AuditEvent
		if err := json.Unmarshal(row.data, &ev); err != nil {
			continue
		}
		out = append(out, ev)
	}
	return out
}

// eventsWith returns the recorded rows for one action, which is what most
// assertions actually want: exactly one grant, no cancel, and so on.
func (r *auditRecorder) eventsWith(action string) []AuditEvent {
	var out []AuditEvent
	for _, ev := range r.events() {
		if ev.Action == action {
			out = append(out, ev)
		}
	}
	return out
}

// The default audited writes, for the fakes that hold no customer or workload
// state of their own. A fake that does hold it overrides these — an outer
// method shadows the embedded one — so its own guards still apply.
func (r *auditRecorder) upsertCustomerAudited(_ *Customer, ev *AuditEvent) error {
	return r.appendAudit(ev)
}

func (r *auditRecorder) submitWorkload(_ *Workload, ev *AuditEvent) error {
	return r.appendAudit(ev)
}

// setMeshState accepts the global mesh-state row and keeps it, so a fake
// that never looks at it still satisfies the interface and a fake that does can
// read it back. Overridden by the fakes that need it to fail.
func (r *auditRecorder) setMeshState(st *meshState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *st
	cp.SharedKubeletRoutes = slices.Clone(st.SharedKubeletRoutes)
	cp.SharedKubeletClaims = cloneRouteUnions(st.SharedKubeletClaims)
	r.meshRow = &cp
	return nil
}

// meshState returns the row as a restart would read it back, or nil when none
// has been written.
func (r *auditRecorder) meshState() *meshState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.meshRow == nil {
		return nil
	}
	cp := *r.meshRow
	cp.SharedKubeletRoutes = slices.Clone(r.meshRow.SharedKubeletRoutes)
	cp.SharedKubeletClaims = cloneRouteUnions(r.meshRow.SharedKubeletClaims)
	return &cp
}
