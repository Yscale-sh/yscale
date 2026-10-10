package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// PaidGateState represents the activation state of paid runtime.
type PaidGateState int

const (
	PaidGateDisabled PaidGateState = iota
	PaidGateNotReady
	PaidGateReady
)

func (s PaidGateState) String() string {
	switch s {
	case PaidGateDisabled:
		return "disabled"
	case PaidGateNotReady:
		return "not_ready"
	case PaidGateReady:
		return "ready"
	default:
		return "unknown"
	}
}

// PaidGatePrerequisite names a single prerequisite for paid-runtime activation.
type PaidGatePrerequisite int

const (
	PrereqBillingDB PaidGatePrerequisite = iota
	PrereqBillingMode
	PrereqDurableAdmission
	PrereqLifecycleStore
	PrereqConnectorLedger
	PrereqProviderReconciliation
	PrereqReconciliationMonitor
	PrereqBillingReconciliation
	PrereqOperatorKillSwitch

	prereqCount int = iota
)

var prereqNames = [prereqCount]string{
	PrereqBillingDB:              "billing_db",
	PrereqBillingMode:            "billing_mode",
	PrereqDurableAdmission:       "durable_admission",
	PrereqLifecycleStore:         "lifecycle_store",
	PrereqConnectorLedger:        "connector_ledger",
	PrereqProviderReconciliation: "provider_reconciliation",
	PrereqReconciliationMonitor:  "reconciliation_monitor",
	PrereqBillingReconciliation:  "billing_reconciliation",
	PrereqOperatorKillSwitch:     "operator_kill_switch",
}

func (p PaidGatePrerequisite) String() string {
	if p >= 0 && int(p) < len(prereqNames) {
		return prereqNames[p]
	}
	return fmt.Sprintf("prereq_%d", p)
}

// PaidGate is a concurrency-safe typed gate that controls whether paid-runtime
// admission is allowed. Default state is disabled. Transitions from ready to
// not-ready or disabled are permitted (kill switch / readiness loss).
type PaidGate struct {
	mu     sync.RWMutex
	state  PaidGateState
	ready  [prereqCount]bool
	reason string
}

// NewPaidGate returns a gate in the disabled state.
func NewPaidGate() *PaidGate {
	return &PaidGate{state: PaidGateDisabled, reason: "not configured"}
}

// Enable moves the gate from disabled to not-ready, indicating that paid
// runtime was requested but prerequisites have not been satisfied.
func (g *PaidGate) Enable() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == PaidGateDisabled {
		g.state = PaidGateNotReady
		g.reason = "prerequisites not met"
	}
}

// SetReady marks a single prerequisite as satisfied and recalculates state.
func (g *PaidGate) SetReady(p PaidGatePrerequisite) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == PaidGateDisabled || p < 0 || int(p) >= len(g.ready) {
		return
	}
	g.ready[p] = true
	g.recalcLocked()
}

// ClearReady marks a prerequisite as unsatisfied and recalculates state.
func (g *PaidGate) ClearReady(p PaidGatePrerequisite) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if p < 0 || int(p) >= len(g.ready) {
		return
	}
	g.ready[p] = false
	g.recalcLocked()
}

// Disable moves the gate to disabled regardless of current state.
func (g *PaidGate) Disable() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state = PaidGateDisabled
	g.ready = [prereqCount]bool{}
	g.reason = "disabled"
}

// State returns the current gate state.
func (g *PaidGate) State() PaidGateState {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state
}

// Allowed returns true only when the gate is in the ready state.
func (g *PaidGate) Allowed() bool {
	return g.State() == PaidGateReady
}

// BeginAdmission linearizes the final paid-admission decision with runtime
// closure. The returned release function retains a read lease across the
// provider call; ClearReady and Disable cannot complete while that call is in
// flight, and Go's RWMutex prevents later readers from passing a waiting
// writer. Callers must invoke release exactly once.
func (g *PaidGate) BeginAdmission() (release func(), ok bool) {
	g.mu.RLock()
	if g.state != PaidGateReady {
		g.mu.RUnlock()
		return nil, false
	}
	var once sync.Once
	return func() {
		once.Do(g.mu.RUnlock)
	}, true
}

// Status returns the state and a human-readable reason without leaking
// sensitive configuration values.
func (g *PaidGate) Status() (PaidGateState, string, []string) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var missing []string
	if g.state != PaidGateDisabled {
		for i := 0; i < prereqCount; i++ {
			if !g.ready[i] {
				missing = append(missing, prereqNames[i])
			}
		}
	}
	return g.state, g.reason, missing
}

func (g *PaidGate) recalcLocked() {
	if g.state == PaidGateDisabled {
		return
	}
	for i := 0; i < prereqCount; i++ {
		if !g.ready[i] {
			g.state = PaidGateNotReady
			g.reason = fmt.Sprintf("missing prerequisite: %s", prereqNames[i])
			return
		}
	}
	g.state = PaidGateReady
	g.reason = "all prerequisites met"
}

// ReadyzPaidHandler returns an http.HandlerFunc for /readyz/paid.
func ReadyzPaidHandler(gate *PaidGate) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		st, reason, missing := gate.Status()
		status := http.StatusOK
		if st != PaidGateReady {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		resp := struct {
			State   string   `json:"state"`
			Reason  string   `json:"reason"`
			Missing []string `json:"missing,omitempty"`
		}{
			State:   st.String(),
			Reason:  reason,
			Missing: missing,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}
