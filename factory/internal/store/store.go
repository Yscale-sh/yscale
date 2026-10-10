// Package store holds the factory's durable fabric records.
package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"golang.org/x/crypto/nacl/secretbox"
)

var (
	// ErrNotFound is returned when a requested record does not exist.
	ErrNotFound = errors.New("factory store: not found")
	// ErrInvalidKEK means the configured KEK cannot safely encrypt box keys.
	ErrInvalidKEK = errors.New("factory store: FACTORY_KEK must be a base64-encoded 32-byte key")
	// ErrDecrypt means a stored API key could not be authenticated and decrypted.
	ErrDecrypt = errors.New("factory store: API key decryption failed")
)

const (
	kekSize   = 32
	nonceSize = 24

	StatusProvisioning    = "provisioning"
	StatusReady           = "ready"
	StatusDegraded        = "degraded"
	StatusReplacing       = "replacing"
	StatusDecommissioning = "decommissioning"
	StatusDead            = "dead"

	JobPending    = "pending"
	JobInProgress = "in_progress"
)

// Box is a tenant's dedicated coordination-server box. apiKeyEnc is
// intentionally unexported: callers can never receive an API key in plaintext.
type Box struct {
	TenantID            string
	LoginServer         string
	Hostname            string
	Backend             string
	BackendID           string
	HSUser              string
	apiKeyEnc           []byte
	Status              string
	CreatedAt           time.Time
	LastValidatedAt     time.Time
	ConsecutiveFailures int
	ReplacedBy          string
}

// PolicyState records the last policy successfully pushed to a tenant box.
type PolicyState struct {
	TenantID     string
	PushedRoutes []string
	LastPushAt   time.Time
	LastError    string
}

// Job is an asynchronous fabric operation.
type Job struct {
	ID             string
	TenantID       string
	LoginServer    string
	Kind           string // provision | deprovision | replace
	Status         string
	IdempotencyKey string
	RequestHash    string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	LastError      string
}

// Audit is the factory's compliance record for sensitive fabric operations.
type Audit struct {
	At       time.Time
	Actor    string
	TenantID string
	Action   string
}

// Store is the factory's fabric-record persistence seam.
type Store interface {
	SetBox(Box, string) error
	PromoteFabric(stagingLoginServer string, box Box, plaintextAPIKey string) error
	GetBox(loginServer string) (Box, error)
	ListBoxes() []Box
	ListBoxesByStatus(status string) []Box
	GetFabric(tenantID string) (Box, error)
	GetBoxForTenant(tenantID, loginServer string) (Box, error)
	SetBoxStatus(loginServer, status string) error
	BeginDeprovision(tenantID, loginServer string) (Box, bool, error)
	DecryptAPIKey(loginServer string) (string, error)

	SetPolicyState(PolicyState) error
	GetPolicyState(tenantID string) (PolicyState, error)
	SetJob(Job) error
	EnsureFabric(tenantID, idempotencyKey string) (Job, Box, error)
	EnqueueJob(Job) (Job, error)
	ClaimJob() (Job, error)
	GetJob(id string) (Job, error)
	ListJobs(tenantID string) []Job
	AppendAudit(Audit) error
	ListAudit(tenantID string) []Audit
}

// Persister is the durable backend seam behind the in-memory working set.
type Persister interface {
	UpsertBox(Box) error
	DeleteBox(loginServer string) error
	UpsertPolicyState(PolicyState) error
	UpsertJob(Job) error
	AppendAudit(Audit) error
	Close()
}

// MemoryStore keeps factory records in memory and optionally writes through to
// a Persister without changing factory callers' read semantics.
type MemoryStore struct {
	mu sync.RWMutex

	boxes        map[string]Box
	policyStates map[string]PolicyState
	jobs         map[string]Job
	fabricJobs   map[string]string
	idempotency  map[string]string
	audits       map[string][]Audit
	persister    Persister
	kek          [kekSize]byte
}

// New constructs a store from FACTORY_KEK. It fails closed if the environment
// variable is absent, malformed, or does not decode to a 32-byte key.
func New() (*MemoryStore, error) {
	return NewFromEnv()
}

// NewFromEnv constructs a store using the base64-encoded FACTORY_KEK.
func NewFromEnv() (*MemoryStore, error) {
	encoded, ok := os.LookupEnv("FACTORY_KEK")
	if !ok || encoded == "" {
		return nil, ErrInvalidKEK
	}

	kek, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, ErrInvalidKEK
	}
	return NewWithKEK(kek)
}

// NewWithKEK constructs a store with a caller-supplied KEK. It is useful for
// tests and controlled bootstrap wiring; production should use New.
func NewWithKEK(kek []byte) (*MemoryStore, error) {
	return NewWithKEKAndPersister(kek, nil)
}

// NewWithKEKAndPersister constructs a store with an optional durable backend.
// Call LoadFrom after construction to hydrate a durable backend into memory.
func NewWithKEKAndPersister(kek []byte, persister Persister) (*MemoryStore, error) {
	if len(kek) != kekSize {
		return nil, ErrInvalidKEK
	}

	s := &MemoryStore{
		boxes:        make(map[string]Box),
		policyStates: make(map[string]PolicyState),
		jobs:         make(map[string]Job),
		fabricJobs:   make(map[string]string),
		idempotency:  make(map[string]string),
		audits:       make(map[string][]Audit),
		persister:    persister,
	}
	copy(s.kek[:], kek)
	return s, nil
}

// SetBox encrypts plaintextAPIKey before storing the box. No plaintext key is
// retained in the Box value or returned by the ordinary box accessors.
func (s *MemoryStore) SetBox(box Box, plaintextAPIKey string) error {
	if box.LoginServer == "" {
		return errors.New("factory store: box login server is required")
	}

	apiKeyEnc, err := s.encryptAPIKey(plaintextAPIKey)
	if err != nil {
		return err
	}
	box.apiKeyEnc = apiKeyEnc

	s.mu.Lock()
	s.boxes[box.LoginServer] = cloneBox(box)
	s.pUpsertBox(box)
	s.mu.Unlock()
	return nil
}

// PromoteFabric atomically re-keys a staging fabric to its validated endpoint.
// EnsureFabric stages a box under a temporary "<id>.factory.invalid" login
// server before the real VM exists; once the provisioner has created the box
// and validated it, the record moves to the real login server. The move is
// TARGETED on the exact stagingLoginServer the caller holds (never a fuzzy
// per-tenant scan) so promoting one tenant's fabric can't disturb another's,
// and the provision job that pointed at the staging endpoint is repointed so
// GetFabric/EnsureFabric keep resolving the same fabric on retry.
func (s *MemoryStore) PromoteFabric(stagingLoginServer string, box Box, plaintextAPIKey string) error {
	if box.LoginServer == "" {
		return errors.New("factory store: box login server is required")
	}

	apiKeyEnc, err := s.encryptAPIKey(plaintextAPIKey)
	if err != nil {
		return err
	}
	box.apiKeyEnc = apiKeyEnc

	s.mu.Lock()
	defer s.mu.Unlock()
	var repointed []Job
	restage := stagingLoginServer != "" && stagingLoginServer != box.LoginServer
	if restage {
		delete(s.boxes, stagingLoginServer)
		for id, job := range s.jobs {
			if job.LoginServer == stagingLoginServer {
				job.LoginServer = box.LoginServer
				s.jobs[id] = job
				repointed = append(repointed, job)
			}
		}
	}
	s.boxes[box.LoginServer] = cloneBox(box)

	// Durable write-through is SAFE-ORDERED so a crash can never lose the box:
	// its api_key_enc is not recoverable by rebuild (design §187). Persist the
	// real box FIRST (key durable), repoint the job(s), and delete the staging
	// record LAST — a crash between steps leaves at worst a harmless staging
	// orphan (its status is provisioning, no job points to it), never a lost
	// box. Best-effort like central's persister; the worker's subsequent
	// SetBoxStatus re-upserts the box, so a transient write failure self-heals.
	s.pUpsertBox(box)
	for _, job := range repointed {
		s.pUpsertJob(job)
	}
	if restage {
		s.pDeleteBox(stagingLoginServer)
	}
	return nil
}

// GetBox returns a ciphertext-only copy of the requested box.
func (s *MemoryStore) GetBox(loginServer string) (Box, error) {
	s.mu.RLock()
	box, ok := s.boxes[loginServer]
	s.mu.RUnlock()
	if !ok {
		return Box{}, ErrNotFound
	}
	return cloneBox(box), nil
}

// ListBoxes returns ciphertext-only copies of all boxes, ordered by login server.
func (s *MemoryStore) ListBoxes() []Box {
	return s.listBoxes(func(Box) bool { return true })
}

// ListBoxesByStatus returns ciphertext-only boxes with the requested status.
func (s *MemoryStore) ListBoxesByStatus(status string) []Box {
	return s.listBoxes(func(box Box) bool { return box.Status == status })
}

// GetFabric returns a tenant's current fabric, including one in its drain
// window. A dead box is historical and does not count as a current fabric.
func (s *MemoryStore) GetFabric(tenantID string) (Box, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var found Box
	for _, box := range s.boxes {
		if box.TenantID != tenantID || box.Status == StatusDead {
			continue
		}
		if found.LoginServer == "" || found.CreatedAt.Before(box.CreatedAt) {
			found = cloneBox(box)
		}
	}
	if found.LoginServer == "" {
		return Box{}, ErrNotFound
	}
	return found, nil
}

// GetBoxForTenant resolves a box by its immutable tenant/login-server pair.
// It deliberately includes decommissioning boxes so drain-window teardown
// requests cannot be redirected to a replacement fabric.
func (s *MemoryStore) GetBoxForTenant(tenantID, loginServer string) (Box, error) {
	s.mu.RLock()
	box, ok := s.boxes[loginServer]
	s.mu.RUnlock()
	if !ok || box.TenantID != tenantID {
		return Box{}, ErrNotFound
	}
	return cloneBox(box), nil
}

// SetBoxStatus updates only lifecycle state, retaining the encrypted API key.
func (s *MemoryStore) SetBoxStatus(loginServer, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	box, ok := s.boxes[loginServer]
	if !ok {
		return ErrNotFound
	}
	box.Status = status
	s.boxes[loginServer] = cloneBox(box)
	s.pUpsertBox(box)
	return nil
}

// BeginDeprovision atomically transitions an active box to decommissioning and
// reports whether THIS call started the teardown. A box already decommissioning
// or dead returns started=false, so a concurrent or repeated DELETE enqueues no
// second teardown job. The box stays resolvable while decommissioning/dead (the
// drain window) so in-flight burst teardowns keep working.
func (s *MemoryStore) BeginDeprovision(tenantID, loginServer string) (Box, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	box, ok := s.boxes[loginServer]
	if !ok || box.TenantID != tenantID {
		return Box{}, false, ErrNotFound
	}
	if box.Status == StatusDecommissioning || box.Status == StatusDead {
		return cloneBox(box), false, nil
	}
	box.Status = StatusDecommissioning
	s.boxes[loginServer] = cloneBox(box)
	s.pUpsertBox(box)
	return cloneBox(box), true, nil
}

func (s *MemoryStore) listBoxes(include func(Box) bool) []Box {
	s.mu.RLock()
	boxes := make([]Box, 0, len(s.boxes))
	for _, box := range s.boxes {
		if include(box) {
			boxes = append(boxes, cloneBox(box))
		}
	}
	s.mu.RUnlock()
	sort.Slice(boxes, func(i, j int) bool { return boxes[i].LoginServer < boxes[j].LoginServer })
	return boxes
}

// DecryptAPIKey returns a key only for factory-internal mint and teardown RPC
// paths. It is deliberately separate from GetBox and ListBoxes.
func (s *MemoryStore) DecryptAPIKey(loginServer string) (string, error) {
	return s.decryptAPIKey(loginServer)
}

func (s *MemoryStore) decryptAPIKey(loginServer string) (string, error) {
	s.mu.RLock()
	box, ok := s.boxes[loginServer]
	s.mu.RUnlock()
	if !ok {
		return "", ErrNotFound
	}
	if len(box.apiKeyEnc) < nonceSize+secretbox.Overhead {
		return "", ErrDecrypt
	}

	var nonce [nonceSize]byte
	copy(nonce[:], box.apiKeyEnc[:nonceSize])
	plaintext, ok := secretbox.Open(nil, box.apiKeyEnc[nonceSize:], &nonce, &s.kek)
	if !ok {
		return "", ErrDecrypt
	}
	return string(plaintext), nil
}

func (s *MemoryStore) encryptAPIKey(plaintext string) ([]byte, error) {
	var nonce [nonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("factory store: generate API key nonce: %w", err)
	}
	return secretbox.Seal(nonce[:], []byte(plaintext), &nonce, &s.kek), nil
}

// SetPolicyState records a tenant's last policy-push result.
func (s *MemoryStore) SetPolicyState(state PolicyState) error {
	if state.TenantID == "" {
		return errors.New("factory store: policy state tenant ID is required")
	}
	s.mu.Lock()
	s.policyStates[state.TenantID] = clonePolicyState(state)
	s.pUpsertPolicyState(state)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) GetPolicyState(tenantID string) (PolicyState, error) {
	s.mu.RLock()
	state, ok := s.policyStates[tenantID]
	s.mu.RUnlock()
	if !ok {
		return PolicyState{}, ErrNotFound
	}
	return clonePolicyState(state), nil
}

// SetJob records an asynchronous fabric operation.
func (s *MemoryStore) SetJob(job Job) error {
	if job.ID == "" {
		return errors.New("factory store: job ID is required")
	}
	s.mu.Lock()
	s.jobs[job.ID] = job
	s.pUpsertJob(job)
	s.mu.Unlock()
	return nil
}

// EnsureFabric atomically finds or creates the single active fabric for a
// tenant. Its job retains the retry key and request hash for durable
// idempotency, enforced by the Postgres uniqueness constraint as well.
func (s *MemoryStore) EnsureFabric(tenantID, idempotencyKey string) (Job, Box, error) {
	if tenantID == "" {
		return Job{}, Box{}, errors.New("factory store: fabric tenant ID is required")
	}
	if idempotencyKey == "" {
		return Job{}, Box{}, errors.New("factory store: idempotency key is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := tenantID + "\x00" + idempotencyKey
	if jobID, ok := s.idempotency[key]; ok {
		if job, ok := s.jobs[jobID]; ok {
			if box, ok := s.boxes[job.LoginServer]; ok && box.Status != StatusDead {
				return job, cloneBox(box), nil
			}
		}
	}
	if jobID, ok := s.fabricJobs[tenantID]; ok {
		if job, ok := s.jobs[jobID]; ok {
			if box, ok := s.boxes[job.LoginServer]; ok && box.Status != StatusDead {
				s.idempotency[key] = job.ID
				return job, cloneBox(box), nil
			}
		}
	}

	id, err := newID()
	if err != nil {
		return Job{}, Box{}, fmt.Errorf("factory store: generate fabric job ID: %w", err)
	}
	now := time.Now().UTC()
	loginServer := "https://" + id + ".factory.invalid"
	job := Job{
		ID:             id,
		TenantID:       tenantID,
		LoginServer:    loginServer,
		Kind:           "provision",
		Status:         JobPending,
		IdempotencyKey: idempotencyKey,
		RequestHash:    fabricRequestHash(tenantID),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	box := Box{
		TenantID:    tenantID,
		LoginServer: loginServer,
		Status:      StatusProvisioning,
		CreatedAt:   now,
	}
	s.jobs[job.ID] = job
	s.boxes[box.LoginServer] = box
	s.fabricJobs[tenantID] = job.ID
	s.idempotency[key] = job.ID
	s.pUpsertJob(job)
	s.pUpsertBox(box)
	return job, cloneBox(box), nil
}

// EnqueueJob records a pending lifecycle job. Empty IDs are generated by the
// store so callers cannot accidentally collide.
func (s *MemoryStore) EnqueueJob(job Job) (Job, error) {
	if job.TenantID == "" || job.Kind == "" {
		return Job{}, errors.New("factory store: job tenant ID and kind are required")
	}
	if job.ID == "" {
		id, err := newID()
		if err != nil {
			return Job{}, fmt.Errorf("factory store: generate job ID: %w", err)
		}
		job.ID = id
	}
	now := time.Now().UTC()
	if job.Status == "" {
		job.Status = JobPending
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now

	s.mu.Lock()
	s.jobs[job.ID] = job
	s.pUpsertJob(job)
	s.mu.Unlock()
	return job, nil
}

// ClaimJob atomically claims the oldest pending job. It is the in-memory
// analogue of SELECT ... FOR UPDATE SKIP LOCKED.
func (s *MemoryStore) ClaimJob() (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var claimed Job
	for _, job := range s.jobs {
		if job.Status != JobPending {
			continue
		}
		if claimed.ID == "" || job.CreatedAt.Before(claimed.CreatedAt) ||
			(job.CreatedAt.Equal(claimed.CreatedAt) && job.ID < claimed.ID) {
			claimed = job
		}
	}
	if claimed.ID == "" {
		return Job{}, ErrNotFound
	}
	claimed.Status = JobInProgress
	claimed.UpdatedAt = time.Now().UTC()
	s.jobs[claimed.ID] = claimed
	s.pUpsertJob(claimed)
	return claimed, nil
}

func (s *MemoryStore) GetJob(id string) (Job, error) {
	s.mu.RLock()
	job, ok := s.jobs[id]
	s.mu.RUnlock()
	if !ok {
		return Job{}, ErrNotFound
	}
	return job, nil
}

// ListJobs returns a tenant's jobs, ordered by creation time then ID.
func (s *MemoryStore) ListJobs(tenantID string) []Job {
	s.mu.RLock()
	jobs := make([]Job, 0)
	for _, job := range s.jobs {
		if job.TenantID == tenantID {
			jobs = append(jobs, job)
		}
	}
	s.mu.RUnlock()
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].ID < jobs[j].ID
		}
		return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
	})
	return jobs
}

// AppendAudit records a compliance event for a tenant.
func (s *MemoryStore) AppendAudit(audit Audit) error {
	if audit.TenantID == "" {
		return errors.New("factory store: audit tenant ID is required")
	}
	s.mu.Lock()
	s.audits[audit.TenantID] = append(s.audits[audit.TenantID], audit)
	s.pAppendAudit(audit)
	s.mu.Unlock()
	return nil
}

// ListAudit returns a tenant's audit records in append order.
func (s *MemoryStore) ListAudit(tenantID string) []Audit {
	s.mu.RLock()
	audits := append([]Audit(nil), s.audits[tenantID]...)
	s.mu.RUnlock()
	return audits
}

func cloneBox(box Box) Box {
	box.apiKeyEnc = append([]byte(nil), box.apiKeyEnc...)
	return box
}

func clonePolicyState(state PolicyState) PolicyState {
	state.PushedRoutes = append([]string(nil), state.PushedRoutes...)
	return state
}

func fabricRequestHash(tenantID string) string {
	digest := sha256.Sum256([]byte(tenantID))
	return hex.EncodeToString(digest[:])
}

func newID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}

var _ Store = (*MemoryStore)(nil)

// Close releases the optional durable backend. It is safe for in-memory stores.
func (s *MemoryStore) Close() {
	if s.persister != nil {
		s.persister.Close()
	}
}

func (s *MemoryStore) pUpsertBox(box Box) {
	if s.persister == nil {
		return
	}
	if err := s.persister.UpsertBox(box); err != nil {
		slog.Error("factory store: persist box", "login_server", box.LoginServer, "error", err)
	}
}

func (s *MemoryStore) pDeleteBox(loginServer string) {
	if s.persister == nil {
		return
	}
	if err := s.persister.DeleteBox(loginServer); err != nil {
		slog.Error("factory store: persist box delete", "login_server", loginServer, "error", err)
	}
}

func (s *MemoryStore) pUpsertPolicyState(state PolicyState) {
	if s.persister == nil {
		return
	}
	if err := s.persister.UpsertPolicyState(state); err != nil {
		slog.Error("factory store: persist policy state", "tenant_id", state.TenantID, "error", err)
	}
}

func (s *MemoryStore) pUpsertJob(job Job) {
	if s.persister == nil {
		return
	}
	if err := s.persister.UpsertJob(job); err != nil {
		slog.Error("factory store: persist job", "id", job.ID, "error", err)
	}
}

func (s *MemoryStore) pAppendAudit(audit Audit) {
	if s.persister == nil {
		return
	}
	if err := s.persister.AppendAudit(audit); err != nil {
		slog.Error("factory store: persist audit", "tenant_id", audit.TenantID, "error", err)
	}
}
