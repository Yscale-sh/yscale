package state

import "time"

// TrackConnectorCommandLease associates a durable claim with the envelope the
// agent write pump will send. It is replica-local transport metadata; the
// authoritative lease remains on the command row.
func (a *Agent) TrackConnectorCommandLease(commandID, leaseToken string) {
	a.commandLeaseMu.Lock()
	if a.commandLeases == nil {
		a.commandLeases = make(map[string]string)
	}
	a.commandLeases[commandID] = leaseToken
	a.commandLeaseMu.Unlock()
}

// ConnectorCommandLease returns the fence token for a queued or delivered but
// unacknowledged envelope. It remains tracked until ACK or session cleanup.
func (a *Agent) ConnectorCommandLease(commandID string) (string, bool) {
	a.commandLeaseMu.Lock()
	defer a.commandLeaseMu.Unlock()
	token, ok := a.commandLeases[commandID]
	return token, ok
}

// ForgetConnectorCommandLease removes a claim whose envelope never entered the
// socket queue (for example, queue pressure or eviction).
func (a *Agent) ForgetConnectorCommandLease(commandID string) (string, bool) {
	a.commandLeaseMu.Lock()
	defer a.commandLeaseMu.Unlock()
	token, ok := a.commandLeases[commandID]
	if ok {
		delete(a.commandLeases, commandID)
	}
	return token, ok
}

// DrainConnectorCommandLeases returns transport claims abandoned when a
// session exits before its write pump observed them.
func (a *Agent) DrainConnectorCommandLeases() map[string]string {
	a.commandLeaseMu.Lock()
	defer a.commandLeaseMu.Unlock()
	out := a.commandLeases
	a.commandLeases = nil
	return out
}

// AllowConnectorCommandAckAudit bounds durable journal growth from a connector
// repeatedly submitting unknown or cross-cluster command IDs.
func (a *Agent) AllowConnectorCommandAckAudit(now time.Time, every time.Duration) bool {
	a.commandAckAuditMu.Lock()
	defer a.commandAckAuditMu.Unlock()
	if !a.lastCommandAckAudit.IsZero() && now.Sub(a.lastCommandAckAudit) < every {
		return false
	}
	a.lastCommandAckAudit = now
	return true
}
