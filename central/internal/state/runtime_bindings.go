// yscale:proprietary

package state

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

const MaxRuntimeBindingDisplayNameBytes = 100

var (
	ErrInvalidRuntimeBinding  = errors.New("invalid runtime binding")
	ErrRuntimeBindingLimit    = fmt.Errorf("a tenant may have at most %d runtime bindings", protocol.MaxRuntimeBindings)
	ErrRuntimeBindingNotFound = fmt.Errorf("%w: no such runtime binding", ErrNotFound)
)

type RuntimeBinding struct {
	ID                   string
	Key                  string
	Name                 string
	CredentialCiphertext string
	Revision             int64
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type RuntimeBindingSummary struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	Name      string    `json:"name"`
	Revision  string    `json:"revision"`
	SyncState string    `json:"sync_state,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func RuntimeBindingAdditionalData(tenantID, bindingID, key string) []byte {
	return []byte(tenantID + "\x00" + bindingID + "\x00" + key)
}

func NewRuntimeBindingID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("state: crypto/rand read failed: " + err.Error())
	}
	return "rtb_" + hex.EncodeToString(b[:])
}

func validateRuntimeBindingName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > MaxRuntimeBindingDisplayNameBytes || !utf8.ValidString(name) {
		return "", fmt.Errorf("%w: name must be 1-100 UTF-8 bytes", ErrInvalidRuntimeBinding)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("%w: name must not contain control characters", ErrInvalidRuntimeBinding)
		}
	}
	return name, nil
}

func validateRuntimeBindingRow(row RuntimeBinding) error {
	if strings.TrimSpace(row.ID) == "" || !protocol.ValidRuntimeBindingKey(row.Key) ||
		strings.TrimSpace(row.CredentialCiphertext) == "" || row.CreatedAt.IsZero() ||
		row.UpdatedAt.IsZero() || row.Revision < 0 || row.Revision > protocol.MaxRuntimeBindingRevision {
		return ErrInvalidRuntimeBinding
	}
	if _, err := validateRuntimeBindingName(row.Name); err != nil {
		return err
	}
	return nil
}

func runtimeBindingSummary(row *RuntimeBinding, syncState string) RuntimeBindingSummary {
	return RuntimeBindingSummary{ID: row.ID, Key: row.Key, Name: row.Name, Revision: strconv.FormatInt(row.Revision, 10),
		SyncState: syncState, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}

func runtimeBindingsRevision(c *Customer) int64 {
	revision := c.RuntimeBindingsRevision
	for _, row := range c.RuntimeBindings {
		if row != nil && row.Revision > revision {
			revision = row.Revision
		}
	}
	return revision
}

func copyRuntimeBinding(row *RuntimeBinding) *RuntimeBinding {
	if row == nil {
		return nil
	}
	cp := *row
	return &cp
}

func copyRuntimeBindings(in []*RuntimeBinding) []*RuntimeBinding {
	if in == nil {
		return nil
	}
	out := make([]*RuntimeBinding, 0, len(in))
	for _, row := range in {
		if row == nil {
			continue
		}
		out = append(out, copyRuntimeBinding(row))
	}
	return out
}

func runtimeBindingByKey(rows []*RuntimeBinding, key string) (int, *RuntimeBinding) {
	for i, row := range rows {
		if row != nil && row.Key == key {
			return i, row
		}
	}
	return -1, nil
}

func (s *Store) RuntimeBindingsFor(customerID, callerAccountID, syncState string) ([]RuntimeBindingSummary, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return nil, "", err
	}
	c := s.customers[customerID]
	out := make([]RuntimeBindingSummary, 0, len(c.RuntimeBindings))
	for _, row := range copyRuntimeBindings(c.RuntimeBindings) {
		out = append(out, runtimeBindingSummary(row, syncState))
	}
	slices.SortFunc(out, func(a, b RuntimeBindingSummary) int {
		return strings.Compare(a.Key, b.Key)
	})
	return out, caller.Role, nil
}

func (s *Store) RuntimeBindingIDForKey(customerID, key string) (string, error) {
	if !protocol.ValidRuntimeBindingKey(key) {
		return "", ErrInvalidRuntimeBinding
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.customers[customerID]
	if c == nil || c.Revoked() {
		return "", ErrNotFound
	}
	if _, row := runtimeBindingByKey(c.RuntimeBindings, key); row != nil {
		return row.ID, nil
	}
	return NewRuntimeBindingID(), nil
}

func (s *Store) RuntimeBindingRows(customerID string) ([]*RuntimeBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.customers[customerID]
	if c == nil || c.Revoked() {
		return nil, ErrNotFound
	}
	return copyRuntimeBindings(c.RuntimeBindings), nil
}

func (s *Store) SetRuntimeBinding(customerID, callerAccountID string, next RuntimeBinding) (RuntimeBindingSummary, string, error) {
	name, err := validateRuntimeBindingName(next.Name)
	if err != nil {
		return RuntimeBindingSummary{}, "", err
	}
	next.Name = name
	if err := validateRuntimeBindingRow(next); err != nil {
		return RuntimeBindingSummary{}, "", err
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return RuntimeBindingSummary{}, "", err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		role := caller.Role
		s.mu.Unlock()
		return RuntimeBindingSummary{}, role, fmt.Errorf("%w: managing runtime bindings requires owner or admin", ErrNotAuthorized)
	}
	c := s.customers[customerID]
	rows := copyRuntimeBindings(c.RuntimeBindings)
	idx, existing := runtimeBindingByKey(rows, next.Key)
	if existing == nil && len(rows) >= protocol.MaxRuntimeBindings {
		s.mu.Unlock()
		return RuntimeBindingSummary{}, caller.Role, ErrRuntimeBindingLimit
	}
	if current := runtimeBindingsRevision(c); next.Revision <= current {
		next.Revision = current + 1
	}
	if next.Revision <= 0 || next.Revision > protocol.MaxRuntimeBindingRevision {
		s.mu.Unlock()
		return RuntimeBindingSummary{}, caller.Role, ErrInvalidRuntimeBinding
	}
	now := next.UpdatedAt
	reason := ReasonRuntimeBindingCreated
	if existing != nil {
		if existing.ID != next.ID {
			s.mu.Unlock()
			return RuntimeBindingSummary{}, caller.Role, ErrInvalidRuntimeBinding
		}
		next.CreatedAt = existing.CreatedAt
		reason = ReasonRuntimeBindingRotated
		rows[idx] = copyRuntimeBinding(&next)
	} else {
		next.CreatedAt = now
		rows = append(rows, copyRuntimeBinding(&next))
	}
	snapshot := *c
	snapshot.RuntimeBindings = rows
	snapshot.RuntimeBindingsRevision = next.Revision
	role := caller.Role
	s.mu.Unlock()
	ev := NewAuditEvent(AuditEvent{CustomerID: customerID, Actor: HumanActor(callerAccountID, customerID),
		Action: ActionRuntimeBindingSet, Outcome: OutcomeAccepted, TargetKind: TargetRuntimeBinding, TargetID: next.ID,
		Detail: AuditDetail{Reason: reason, Role: role}})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return RuntimeBindingSummary{}, role, err
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.RuntimeBindings = copyRuntimeBindings(rows)
		live.RuntimeBindingsRevision = next.Revision
	}
	s.mu.Unlock()
	return runtimeBindingSummary(&next, "pending"), role, nil
}

func (s *Store) DeleteRuntimeBinding(customerID, callerAccountID, key string) (string, string, error) {
	if !protocol.ValidRuntimeBindingKey(key) {
		return "", "", ErrInvalidRuntimeBinding
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return "", "", err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		role := caller.Role
		s.mu.Unlock()
		return "", role, fmt.Errorf("%w: managing runtime bindings requires owner or admin", ErrNotAuthorized)
	}
	c := s.customers[customerID]
	rows := copyRuntimeBindings(c.RuntimeBindings)
	idx, existing := runtimeBindingByKey(rows, key)
	if existing == nil {
		s.mu.Unlock()
		return "", caller.Role, ErrRuntimeBindingNotFound
	}
	rows = append(rows[:idx], rows[idx+1:]...)
	revision := runtimeBindingsRevision(c) + 1
	if revision <= 0 || revision > protocol.MaxRuntimeBindingRevision {
		s.mu.Unlock()
		return "", caller.Role, ErrInvalidRuntimeBinding
	}
	snapshot := *c
	snapshot.RuntimeBindings = rows
	snapshot.RuntimeBindingsRevision = revision
	role := caller.Role
	bindingID := existing.ID
	s.mu.Unlock()
	ev := NewAuditEvent(AuditEvent{CustomerID: customerID, Actor: HumanActor(callerAccountID, customerID),
		Action: ActionRuntimeBindingDelete, Outcome: OutcomeAccepted, TargetKind: TargetRuntimeBinding, TargetID: bindingID,
		Detail: AuditDetail{Reason: ReasonRuntimeBindingDeleted, Role: role}})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return "", role, err
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.RuntimeBindings = copyRuntimeBindings(rows)
		live.RuntimeBindingsRevision = revision
	}
	s.mu.Unlock()
	return bindingID, role, nil
}
