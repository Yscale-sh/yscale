// yscale:proprietary

package state

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const MaxCatalogPublishers = 32

var (
	ErrInvalidCatalogPublisher  = errors.New("invalid catalog publisher")
	ErrCatalogPublisherLimit    = fmt.Errorf("a tenant may have at most %d catalog publishers", MaxCatalogPublishers)
	ErrCatalogPublisherNotFound = fmt.Errorf("%w: no such catalog publisher", ErrNotFound)
)

// CatalogPublisher is persisted inside Customer. CredentialHash is never
// serialized by a public handler; those use CatalogPublisherSummary.
type CatalogPublisher struct {
	ID             string
	Name           string
	CredentialHash string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type CatalogPublisherSummary struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type catalogPublisherRef struct {
	customerID  string
	publisherID string
}

func newCatalogPublisherCredential() string {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("state: crypto/rand read failed: " + err.Error())
	}
	return "yscale_catalog_" + hex.EncodeToString(b[:])
}

func newCatalogPublisherID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("state: crypto/rand read failed: " + err.Error())
	}
	return "pub_" + hex.EncodeToString(b[:])
}

func validateCatalogPublisherName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 100 || !utf8.ValidString(name) {
		return "", fmt.Errorf("%w: name must be 1-100 UTF-8 bytes", ErrInvalidCatalogPublisher)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("%w: name must not contain control characters", ErrInvalidCatalogPublisher)
		}
	}
	return name, nil
}

func catalogPublisherSummary(p *CatalogPublisher) CatalogPublisherSummary {
	return CatalogPublisherSummary{ID: p.ID, Name: p.Name, CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt}
}

func copyCatalogPublishers(in []*CatalogPublisher) []*CatalogPublisher {
	if in == nil {
		return nil
	}
	out := make([]*CatalogPublisher, 0, len(in))
	for _, p := range in {
		if p == nil {
			continue
		}
		cp := *p
		out = append(out, &cp)
	}
	return out
}

func (s *Store) indexCatalogPublishersLocked(c *Customer) {
	for _, p := range c.CatalogPublishers {
		if p != nil && p.CredentialHash != "" {
			s.catalogPublisherCreds[p.CredentialHash] = catalogPublisherRef{customerID: c.ID, publisherID: p.ID}
		}
	}
}

func (s *Store) unindexCatalogPublishersLocked(c *Customer) {
	for _, p := range c.CatalogPublishers {
		if p == nil || p.CredentialHash == "" {
			continue
		}
		if ref, ok := s.catalogPublisherCreds[p.CredentialHash]; ok && ref.customerID == c.ID && ref.publisherID == p.ID {
			delete(s.catalogPublisherCreds, p.CredentialHash)
		}
	}
}

func (s *Store) CatalogPublishersFor(customerID, callerAccountID string) ([]CatalogPublisherSummary, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return nil, "", err
	}
	c := s.customers[customerID]
	out := make([]CatalogPublisherSummary, 0, len(c.CatalogPublishers))
	for _, p := range c.CatalogPublishers {
		out = append(out, catalogPublisherSummary(p))
	}
	return out, caller.Role, nil
}

func (s *Store) CreateCatalogPublisher(customerID, callerAccountID, name string) (CatalogPublisherSummary, string, string, error) {
	name, err := validateCatalogPublisherName(name)
	if err != nil {
		return CatalogPublisherSummary{}, "", "", err
	}
	token := newCatalogPublisherCredential()
	now := time.Now().UTC()
	row := &CatalogPublisher{ID: newCatalogPublisherID(), Name: name, CredentialHash: HashCatalogPublisherCredential(token), CreatedAt: now, UpdatedAt: now}

	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return CatalogPublisherSummary{}, "", "", err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		role := caller.Role
		s.mu.Unlock()
		return CatalogPublisherSummary{}, "", role, fmt.Errorf("%w: managing catalog publishers requires owner or admin", ErrNotAuthorized)
	}
	c := s.customers[customerID]
	if len(c.CatalogPublishers) >= MaxCatalogPublishers {
		s.mu.Unlock()
		return CatalogPublisherSummary{}, "", caller.Role, ErrCatalogPublisherLimit
	}
	snapshot := *c
	snapshot.CatalogPublishers = append(copyCatalogPublishers(c.CatalogPublishers), row)
	role := caller.Role
	s.mu.Unlock()
	ev := NewAuditEvent(AuditEvent{
		CustomerID: customerID, Actor: HumanActor(callerAccountID, customerID),
		Action: ActionCatalogPublisherCreate, Outcome: OutcomeAccepted,
		TargetKind: TargetCatalogPublisher, TargetID: row.ID,
		Detail: AuditDetail{Reason: ReasonCatalogPublisherCreated, Role: role},
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return CatalogPublisherSummary{}, "", role, err
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.CatalogPublishers = snapshot.CatalogPublishers
		s.catalogPublisherCreds[row.CredentialHash] = catalogPublisherRef{customerID: customerID, publisherID: row.ID}
	}
	s.mu.Unlock()
	return catalogPublisherSummary(row), token, role, nil
}

func (s *Store) RotateCatalogPublisherCredential(customerID, callerAccountID, publisherID string) (CatalogPublisherSummary, string, string, error) {
	token := newCatalogPublisherCredential()
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return CatalogPublisherSummary{}, "", "", err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		role := caller.Role
		s.mu.Unlock()
		return CatalogPublisherSummary{}, "", role, fmt.Errorf("%w: managing catalog publishers requires owner or admin", ErrNotAuthorized)
	}
	c := s.customers[customerID]
	rows := copyCatalogPublishers(c.CatalogPublishers)
	idx := -1
	for i, p := range rows {
		if p.ID == publisherID {
			idx = i
			break
		}
	}
	if idx < 0 {
		s.mu.Unlock()
		return CatalogPublisherSummary{}, "", caller.Role, ErrCatalogPublisherNotFound
	}
	oldHash := rows[idx].CredentialHash
	rows[idx].CredentialHash = HashCatalogPublisherCredential(token)
	rows[idx].UpdatedAt = time.Now().UTC()
	snapshot := *c
	snapshot.CatalogPublishers = rows
	role := caller.Role
	s.mu.Unlock()
	ev := NewAuditEvent(AuditEvent{
		CustomerID: customerID, Actor: HumanActor(callerAccountID, customerID),
		Action: ActionCatalogPublisherRotate, Outcome: OutcomeAccepted,
		TargetKind: TargetCatalogPublisher, TargetID: publisherID,
		Detail: AuditDetail{Reason: ReasonCatalogPublisherCredentialRotated, Role: role},
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return CatalogPublisherSummary{}, "", role, err
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.CatalogPublishers = rows
		delete(s.catalogPublisherCreds, oldHash)
		s.catalogPublisherCreds[rows[idx].CredentialHash] = catalogPublisherRef{customerID: customerID, publisherID: publisherID}
	}
	s.mu.Unlock()
	return catalogPublisherSummary(rows[idx]), token, role, nil
}

func (s *Store) DeleteCatalogPublisher(customerID, callerAccountID, publisherID string) (string, error) {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return "", err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		role := caller.Role
		s.mu.Unlock()
		return role, fmt.Errorf("%w: managing catalog publishers requires owner or admin", ErrNotAuthorized)
	}
	c := s.customers[customerID]
	rows := copyCatalogPublishers(c.CatalogPublishers)
	idx := -1
	for i, p := range rows {
		if p.ID == publisherID {
			idx = i
			break
		}
	}
	if idx < 0 {
		s.mu.Unlock()
		return caller.Role, ErrCatalogPublisherNotFound
	}
	oldHash := rows[idx].CredentialHash
	rows = append(rows[:idx], rows[idx+1:]...)
	snapshot := *c
	snapshot.CatalogPublishers = rows
	role := caller.Role
	s.mu.Unlock()
	ev := NewAuditEvent(AuditEvent{
		CustomerID: customerID, Actor: HumanActor(callerAccountID, customerID),
		Action: ActionCatalogPublisherDelete, Outcome: OutcomeAccepted,
		TargetKind: TargetCatalogPublisher, TargetID: publisherID,
		Detail: AuditDetail{Reason: ReasonCatalogPublisherDeleted, Role: role},
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return role, err
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.CatalogPublishers = rows
		delete(s.catalogPublisherCreds, oldHash)
	}
	s.mu.Unlock()
	return role, nil
}

func (s *Store) AuthCatalogPublisher(token string) (*Customer, string, error) {
	return s.AuthCatalogPublisherContext(context.Background(), token)
}
