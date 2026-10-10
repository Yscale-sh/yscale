package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const CloudProviderLinode = "linode"

var (
	ErrCloudAccountMismatch = errors.New("cloud account identity does not match")
	ErrCloudAccountInUse    = errors.New("cloud account is in use")
)

// CloudAccount is persisted inside Customer. CredentialCiphertext is excluded
// from all safe projections by construction.
type CloudAccount struct {
	ID                   string
	Provider             string
	ProviderIdentity     string
	Region               string
	CPUImage             string `json:",omitempty"`
	GPUImage             string `json:",omitempty"`
	CredentialCiphertext string
	UpdatedAt            time.Time
}

type CloudAccountSummary struct {
	ID                string    `json:"id"`
	Provider          string    `json:"provider"`
	ProviderAccountID string    `json:"provider_account_id"`
	Region            string    `json:"region"`
	CPUImageReady     bool      `json:"cpu_image_ready"`
	GPUImageReady     bool      `json:"gpu_image_ready"`
	UpdatedAt         time.Time `json:"updated_at"`
}

func (a *CloudAccount) Summary() CloudAccountSummary {
	if a == nil {
		return CloudAccountSummary{}
	}
	return CloudAccountSummary{ID: a.ID, Provider: a.Provider, ProviderAccountID: a.ProviderIdentity,
		Region: a.Region, CPUImageReady: true, GPUImageReady: a.GPUImage != "", UpdatedAt: a.UpdatedAt}
}

type CloudAccountLease struct {
	BurstID        string
	CustomerID     string
	CloudAccountID string
	ExpiresAt      time.Time
}

type TenantCloudAccount struct {
	CustomerID string
	Account    *CloudAccount
}

type cloudAccountPersister interface {
	disconnectCloudAccount(c *Customer, accountID string, now time.Time, ev *AuditEvent) error
	acquireCloudAccountLease(ctx context.Context, lease *CloudAccountLease) (bool, error)
	releaseCloudAccountLease(ctx context.Context, burstID string) error
	extendCloudAccountLease(ctx context.Context, burstID, customerID, accountID string, expiresAt time.Time) (bool, error)
}

func cloneCloudAccount(a *CloudAccount) *CloudAccount {
	if a == nil {
		return nil
	}
	cp := *a
	return &cp
}

func (s *Store) LinodeCloudAccount(customerID string) (*CloudAccount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.customers[customerID]
	if c == nil || c.Revoked() || c.LinodeCloudAccount == nil {
		return nil, ErrNotFound
	}
	return cloneCloudAccount(c.LinodeCloudAccount), nil
}

func (s *Store) LinodeCloudAccounts() []TenantCloudAccount {
	s.mu.RLock()
	defer s.mu.RUnlock()
	accounts := make([]TenantCloudAccount, 0)
	for customerID, c := range s.customers {
		if c == nil || c.Revoked() || c.LinodeCloudAccount == nil {
			continue
		}
		accounts = append(accounts, TenantCloudAccount{
			CustomerID: customerID,
			Account:    cloneCloudAccount(c.LinodeCloudAccount),
		})
	}
	return accounts
}

func (s *Store) HasCloudAccounts() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.customers {
		if c.LinodeCloudAccount != nil {
			return true
		}
	}
	return false
}

func (s *Store) TenantLinodeCloudAccountFor(customerID, callerAccountID string) (*CloudAccountSummary, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return nil, "", err
	}
	c := s.customers[customerID]
	if c.LinodeCloudAccount == nil {
		return nil, m.Role, nil
	}
	summary := c.LinodeCloudAccount.Summary()
	return &summary, m.Role, nil
}

// SetLinodeCloudAccount connects or rotates an already validated and encrypted
// credential. Rotation preserves the stable id and provider identity.
func (s *Store) SetLinodeCloudAccount(customerID string, next CloudAccount, by Actor) (*CloudAccount, bool, error) {
	if strings.TrimSpace(next.ID) == "" || next.Provider != CloudProviderLinode ||
		strings.TrimSpace(next.ProviderIdentity) == "" || strings.TrimSpace(next.Region) == "" ||
		strings.TrimSpace(next.CredentialCiphertext) == "" || next.UpdatedAt.IsZero() {
		return nil, false, ErrCloudAccountMismatch
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.RLock()
	live := s.customers[customerID]
	if live == nil || live.Revoked() {
		s.mu.RUnlock()
		return nil, false, ErrNotFound
	}
	if by.Human() {
		m, err := s.membershipForLocked(by.AccountID, customerID)
		if err != nil {
			s.mu.RUnlock()
			return nil, false, err
		}
		if m.Role != RoleOwner && m.Role != RoleAdmin {
			s.mu.RUnlock()
			return nil, false, ErrNotAuthorized
		}
	}
	snapshot := *live
	previous := cloneCloudAccount(live.LinodeCloudAccount)
	s.mu.RUnlock()
	rotating := previous != nil
	if rotating && (previous.ID != next.ID || previous.Provider != next.Provider || previous.ProviderIdentity != next.ProviderIdentity) {
		return nil, false, ErrCloudAccountMismatch
	}
	snapshot.LinodeCloudAccount = cloneCloudAccount(&next)
	action, reason := ActionCloudAccountConnect, ReasonCloudAccountConnected
	if rotating {
		action, reason = ActionCloudAccountRotate, ReasonCloudAccountRotated
	}
	ev := NewAuditEvent(AuditEvent{CustomerID: customerID, Actor: by, Action: action,
		Outcome: OutcomeAccepted, TargetKind: TargetCloudAccount, TargetID: next.ID,
		Detail: AuditDetail{Reason: reason}})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	if current := s.customers[customerID]; current != nil && !current.Revoked() {
		current.LinodeCloudAccount = cloneCloudAccount(&next)
	}
	s.mu.Unlock()
	return cloneCloudAccount(&next), rotating, nil
}

func (s *Store) DisconnectLinodeCloudAccount(customerID, accountID string, now time.Time, by Actor) error {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.RLock()
	live := s.customers[customerID]
	if live == nil || live.Revoked() || live.LinodeCloudAccount == nil {
		s.mu.RUnlock()
		return ErrNotFound
	}
	if live.LinodeCloudAccount.ID != accountID {
		s.mu.RUnlock()
		return ErrCloudAccountMismatch
	}
	if by.Human() {
		m, err := s.membershipForLocked(by.AccountID, customerID)
		if err != nil {
			s.mu.RUnlock()
			return err
		}
		if m.Role != RoleOwner && m.Role != RoleAdmin {
			s.mu.RUnlock()
			return ErrNotAuthorized
		}
	}
	snapshot := *live
	snapshot.LinodeCloudAccount = nil
	for _, b := range s.bursts {
		if b.CustomerID == customerID && b.CloudAccountID == accountID {
			s.mu.RUnlock()
			return ErrCloudAccountInUse
		}
	}
	for _, lease := range s.cloudAccountLeases {
		if lease.CustomerID == customerID && lease.CloudAccountID == accountID && lease.ExpiresAt.After(now) {
			s.mu.RUnlock()
			return ErrCloudAccountInUse
		}
	}
	p := s.persist
	s.mu.RUnlock()
	ev := NewAuditEvent(AuditEvent{CustomerID: customerID, Actor: by, Action: ActionCloudAccountDisconnect,
		Outcome: OutcomeAccepted, TargetKind: TargetCloudAccount, TargetID: accountID,
		Detail: AuditDetail{Reason: ReasonCloudAccountDisconnected}})
	if p != nil {
		capable, ok := p.(cloudAccountPersister)
		if !ok {
			s.recordPersistenceFailure("cloud_account", "disconnect", ErrPersistence)
			return fmt.Errorf("%w: cloud-account transactions unavailable", ErrPersistence)
		}
		if err := capable.disconnectCloudAccount(&snapshot, accountID, now, ev); err != nil {
			s.refreshCustomerWrite(customerID, err)
			if errors.Is(err, ErrCloudAccountInUse) {
				return err
			}
			s.recordPersistenceFailure("cloud_account", "disconnect", err)
			return fmt.Errorf("%w: disconnect cloud account: %w", ErrPersistence, err)
		}
		s.acceptCustomerWrite(&snapshot)
	} else if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return err
	}
	s.mu.Lock()
	if current := s.customers[customerID]; current != nil && current.LinodeCloudAccount != nil && current.LinodeCloudAccount.ID == accountID {
		current.LinodeCloudAccount = nil
	}
	s.mu.Unlock()
	return nil
}

func (s *Store) AcquireCloudAccountLease(ctx context.Context, lease CloudAccountLease) (bool, error) {
	if lease.BurstID == "" || lease.CustomerID == "" || lease.CloudAccountID == "" || !lease.ExpiresAt.After(time.Now()) {
		return false, errors.New("invalid cloud account lease")
	}
	s.mu.Lock()
	c := s.customers[lease.CustomerID]
	if c == nil || c.Revoked() || c.LinodeCloudAccount == nil || c.LinodeCloudAccount.ID != lease.CloudAccountID {
		s.mu.Unlock()
		return false, ErrNotFound
	}
	if s.persist == nil {
		if old := s.cloudAccountLeases[lease.BurstID]; old != nil && old.ExpiresAt.After(time.Now()) {
			s.mu.Unlock()
			return false, nil
		}
		cp := lease
		s.cloudAccountLeases[lease.BurstID] = &cp
		s.mu.Unlock()
		return true, nil
	}
	p := s.persist
	s.mu.Unlock()
	capable, ok := p.(cloudAccountPersister)
	if !ok {
		s.recordPersistenceFailure("cloud_account_lease", "acquire", ErrPersistence)
		return false, fmt.Errorf("%w: cloud-account leases unavailable", ErrPersistence)
	}
	won, err := capable.acquireCloudAccountLease(ctx, &lease)
	if err != nil {
		s.recordPersistenceFailure("cloud_account_lease", "acquire", err)
		return false, fmt.Errorf("%w: acquire cloud-account lease: %w", ErrPersistence, err)
	}
	return won, nil
}

func (s *Store) ReleaseCloudAccountLease(ctx context.Context, burstID string) error {
	s.mu.Lock()
	if s.persist == nil {
		delete(s.cloudAccountLeases, burstID)
		s.mu.Unlock()
		return nil
	}
	p := s.persist
	s.mu.Unlock()
	capable, ok := p.(cloudAccountPersister)
	if !ok {
		s.recordPersistenceFailure("cloud_account_lease", "release", ErrPersistence)
		return fmt.Errorf("%w: cloud-account leases unavailable", ErrPersistence)
	}
	if err := capable.releaseCloudAccountLease(ctx, burstID); err != nil {
		s.recordPersistenceFailure("cloud_account_lease", "release", err)
		return fmt.Errorf("%w: release cloud-account lease: %w", ErrPersistence, err)
	}
	return nil
}

// ExtendCloudAccountLease lengthens an existing exact tenant/account/burst
// lease. It is deliberately separate from acquisition: an ambiguous provider
// create must retain the proof already held, never create a new claim after the
// original disappeared or overwrite a different account's claim.
func (s *Store) ExtendCloudAccountLease(ctx context.Context, burstID, customerID, accountID string, expiresAt time.Time) error {
	if burstID == "" || customerID == "" || accountID == "" || !expiresAt.After(time.Now()) {
		return errors.New("invalid cloud account lease extension")
	}
	s.mu.Lock()
	if s.persist == nil {
		lease := s.cloudAccountLeases[burstID]
		if lease == nil || lease.CustomerID != customerID || lease.CloudAccountID != accountID {
			s.mu.Unlock()
			return ErrNotFound
		}
		if expiresAt.After(lease.ExpiresAt) {
			lease.ExpiresAt = expiresAt
		}
		s.mu.Unlock()
		return nil
	}
	p := s.persist
	s.mu.Unlock()
	capable, ok := p.(cloudAccountPersister)
	if !ok {
		s.recordPersistenceFailure("cloud_account_lease", "extend", ErrPersistence)
		return fmt.Errorf("%w: cloud-account lease extension unavailable", ErrPersistence)
	}
	extended, err := capable.extendCloudAccountLease(ctx, burstID, customerID, accountID, expiresAt)
	if err != nil {
		s.recordPersistenceFailure("cloud_account_lease", "extend", err)
		return fmt.Errorf("%w: extend cloud-account lease: %w", ErrPersistence, err)
	}
	if !extended {
		return ErrNotFound
	}
	return nil
}
