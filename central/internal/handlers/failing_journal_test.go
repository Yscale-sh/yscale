package handlers

import (
	"sync"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// failingJournal is the same refusal on the workload path's audited-write seam,
// where the store is a concrete type. Every write it does not refuse goes
// through to the real store, so a test that scripts one failure still observes
// the true behaviour of everything around it.
type failingJournal struct {
	*state.Store
	submitErr error
	appendErr error
	burstErr  error

	mu     sync.Mutex
	events []state.AuditEvent
}

func (f *failingJournal) SubmitWorkload(w *state.Workload, ev *state.AuditEvent) error {
	if f.submitErr != nil {
		return f.submitErr
	}
	f.record(ev)
	return f.Store.SubmitWorkload(w, ev)
}

func (f *failingJournal) AppendAudit(ev *state.AuditEvent) error {
	if f.appendErr != nil {
		return f.appendErr
	}
	f.record(ev)
	return f.Store.AppendAudit(ev)
}

func (f *failingJournal) PutBurst(b *state.Burst) error {
	if f.burstErr != nil {
		return f.burstErr
	}
	return f.Store.PutBurst(b)
}

func (f *failingJournal) record(ev *state.AuditEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ev != nil {
		f.events = append(f.events, *ev)
	}
}

func (f *failingJournal) recorded(action string) []state.AuditEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []state.AuditEvent
	for _, ev := range f.events {
		if ev.Action == action {
			out = append(out, ev)
		}
	}
	return out
}
