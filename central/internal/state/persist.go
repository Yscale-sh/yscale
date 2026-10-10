package state

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// CustomerCredentialCipher protects reversible customer credentials before
// they enter the JSONB customer document. Implemented by credentialcipher.Cipher
// in production and deliberately kept as a narrow interface here.
type CustomerCredentialCipher interface {
	Encrypt(string, []byte) (string, error)
	Decrypt(string, []byte) (string, error)
}

// persister is the durability backend behind the in-memory store. The store
// keeps the authoritative working set in memory (fast reads, unchanged
// semantics) and write-throughs each individual changed record to the
// persister, so durable state survives a restart. A nil persister — the New()
// store used by unit tests and OSS-local runs — makes every write-through a
// no-op.
//
// Per-record, not whole-snapshot: the old JSON backend rewrote the entire
// state file on every mutation (including high-frequency PV touches). This
// replaces that — implementations upsert/delete only the one record that
// changed.
//
// This interface (together with the broker interface) is a seam along which
// the control plane decomposes into separately-deployable OSS vs enterprise
// pieces: components depend on the interface, not a concrete store.
type persister interface {
	upsertCustomer(*Customer) error
	// createTenant INSERTs a new customer and, when owner is non-nil, its first
	// owner membership, atomically clearing any membership rows still naming
	// that id first. The insert (rather than an upsert) is what makes the id
	// itself the uniqueness check, so a row this replica never had in memory
	// still refuses the create — as ErrCustomerExists, a conflict, not a
	// backend fault. Clearing first is the other half: an id being reissued is
	// the one moment a stale grant left by an interrupted offboard could attach
	// itself to a brand-new tenant, and doing it in the insert's transaction
	// means either both happen or neither does. The owner grant rides the same
	// transaction because a tenant whose owner write failed separately is a
	// tenant somebody has to undo, and the undo is what fails next when the
	// database is the thing that is down. ErrNotFound when the owner account
	// row is absent: the membership would be dangling the moment it committed.
	createTenant(c *Customer, owner *TenantMembership, requireFirst bool) error
	// revokeCustomer writes the RevokedAt-stamped customer row and deletes
	// EVERY membership naming it, atomically. The two cannot be separate
	// writes: a failed membership delete followed by a successful customer
	// write leaves a grant on a tenant nobody can reach, invisible until the id
	// is provisioned again, whereupon the old human is a member of the new
	// tenant. Scoped by customer id rather than by a caller-supplied list so
	// rows this process never had in memory are removed too.
	revokeCustomer(*Customer) error
	// finalizeRevokedCustomer is the final step of an offboard: it records a
	// permanent tombstone for the id and removes the customer row and any
	// membership still naming it, atomically. The whole thing is one
	// transaction, so a crash cannot land half of it; the tombstone is written
	// BEFORE the deletes because it reads the customer row's RevokedAt to record
	// when access was cut, and after the delete there is no row left to read.
	// Once written, the tombstone refuses every later create/upsert on that id —
	// see upsertCustomerStmt. Nothing removes one.
	finalizeRevokedCustomer(id string) error
	// deleteCustomerAndMemberships removes a customer row and any membership
	// row still naming it, leaving NO tombstone. It backs the best-effort legacy
	// DeleteCustomer path, whose OSS/dev callers re-drive themselves from config
	// and do reuse an id they just removed. The offboard lifecycle uses the
	// tombstoning form above.
	deleteCustomerAndMemberships(id string) error
	// Human SaaS identity. There is no account delete: accounts outlive the
	// tenants they own — a human keeps their identity after their last tenant is
	// offboarded. This legacy cached-envelope write-through is refused by
	// PostgreSQL; Store entry points require the operation-based accountWriter.
	upsertAccount(*Account) error
	// Legacy upsertMembership and deleteMembership each carry the audit row for the
	// change they make, and write BOTH in one transaction. The audit row is a
	// parameter rather than a separate call because a roster change that landed
	// and could not be journaled is an unrecorded grant of access — the one
	// outcome the journal exists to make impossible — and two calls have an
	// ordering that produces it whichever way round they go. Durable Store entry
	// points now require membershipMutator instead; PostgreSQL refuses these
	// cached-decision write-throughs, including direct callers.
	upsertMembership(m *TenantMembership, ev *AuditEvent) error
	// deleteMembership removes ONE grant by id, which is what a roster removal
	// is. The tenant-scoped deletes above stay the only way a tenant's whole
	// membership set goes: they ride the customer's own transaction because the
	// two must move together, and widening either to take a single id would give
	// an offboard a way to leave a grant behind. Absent row is not an error —
	// removal is idempotent, and the caller has already decided the grant should
	// not exist.
	deleteMembership(id string, ev *AuditEvent) error
	// upsertCustomerAudited is upsertCustomer plus the audit row for the change,
	// in one transaction. It exists for operator-set authorization and spend
	// guardrails, not for operational customer writes (a mesh attach, a route
	// report), because those re-derive themselves.
	upsertCustomerAudited(c *Customer, ev *AuditEvent) error
	// submitWorkload inserts a newly submitted workload and the authorization
	// decision that admitted it, in one transaction. Distinct from
	// upsertWorkload, whose write-through is best-effort: a submission central
	// authorized without a durable record of authorizing it is what Create's
	// reap-and-503 path exists to refuse.
	submitWorkload(w *Workload, ev *AuditEvent) error
	// appendAudit writes one journal row on its own — a refusal, or an
	// observation of something that already happened. INSERT only: the table
	// takes no UPDATE and no DELETE (a trigger enforces it), so there is no
	// upsert form of this and no id to conflict on.
	//
	// The implementation STAMPS the event's id and time, here and in the two
	// audited writes above: the id has to order the tenant's commits, and only
	// the durable append knows what has already committed. A caller therefore
	// hands over an event with no id, and must not treat the one it comes back
	// with as meaningful unless the call returned nil.
	appendAudit(ev *AuditEvent) error
	// listAudit reads one tenant's journal newest-first, strictly after the
	// given cursor, at most limit rows. Tenant-scoped in the STATEMENT rather
	// than by filtering what comes back: a tenant's journal names its members
	// and its workloads, and a read that could return another tenant's rows at
	// all is one filter bug away from disclosing them.
	listAudit(ctx context.Context, customerID, after string, limit int) ([]*AuditEvent, error)
	// upsertWorkload writes a whole workload document while preserving two
	// independently observed fields from a newer stored row: frozen Cost and the
	// connector's NodeObservation. Lifecycle writers may hold a document captured
	// before either observation landed on another replica, so an unguarded
	// replacement would permanently erase facts they cannot re-derive.
	upsertWorkload(*Workload) error
	// recordWorkloadCost freezes the cost observation onto the workload row a
	// burst backed, and only onto one that carries none: first write wins, so a
	// redelivered teardown or a second reap of a burst reaped once leaves the
	// original timestamp alone. Keyed by burst id because the reaping replica
	// may never have held the workload — it has the claimed burst and nothing
	// else. Reports whether a row took the write; false covers both "no workload
	// for this burst" and "already observed", neither of which is an error.
	recordWorkloadCost(ctx context.Context, burstID string, c *WorkloadCost) (bool, error)
	// recordBurstReapReceipt durably proves that provider teardown for one
	// tenant-owned burst was secured. It is independent of a workload row:
	// node-only capacity has no workload to carry a cost observation.
	recordBurstReapReceipt(ctx context.Context, burstID, customerID string) (bool, error)
	burstReapRecorded(ctx context.Context, burstID, customerID string) (bool, error)
	// workloadByBurst resolves a burst to its workload in DURABLE state. The
	// in-memory workload map only holds workloads this process created, so a
	// reap that runs on any other replica cannot find the workload it just
	// invalidated, and that workload reads "provisioning" for a job whose node
	// is gone. A burst with no workload row is (nil, nil), not an error.
	workloadByBurst(ctx context.Context, burstID string) (*Workload, error)
	upsertBurst(*Burst) error
	deleteBurst(id string) error
	// updateBurstNodePhase applies one connector-observed node phase to a burst
	// row IN PLACE. It is separate from upsertBurst because an upsert INSERTs:
	// run against a burst claimBurst has just deleted, a whole-row write brings
	// the record back, and a resurrected burst is a VM the reaper watchdog
	// re-bills and re-tears-down forever. This one can only ever modify a row
	// that is still present.
	//
	// The tenancy check and the terminal-phase guard live in the same statement
	// for the same reason the claim is one statement: a caller that read the row
	// first would be deciding against a copy any replica may already have
	// claimed. applied=false covers "no such row", "another tenant's row" and
	// "already Removed" without distinguishing them — the caller must not, and
	// the database has nothing more to say.
	updateBurstNodePhase(ctx context.Context, u BurstNodePhaseUpdate) (*burstNodePhaseResult, error)
	stampWorkloadPodObservation(ctx context.Context, workloadID, customerID, clusterID, podName, nodeName string, scheduledAt time.Time) (int64, error)
	stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error)
	// getWorkloadPodObservation reads the durable PodObservation of a workload,
	// tenant+cluster scoped AND joined against the currently live burst row
	// named by the workload's BurstID. exists=false means the workload row is
	// gone, another tenant's, wrong cluster, OR the burst identity no longer
	// matches (retired burst, wrong NodeName) — all indistinguishable on
	// purpose (an oracle across tenants must not be built here). exists=true
	// with obs=nil means the row is present, the burst identity still matches,
	// but nothing has been stamped yet.
	//
	// Used by StampWorkloadPodObservation when its own UPDATE affects zero
	// rows, to tell an already-applied live observation apart from a retired
	// burst / wrong-owner. Only the former authorises the ACK on a retry —
	// the burst-identity join is what stops a stale replica from ACKing a
	// PodObservation whose burst has since been retired.
	getWorkloadPodObservation(ctx context.Context, workloadID, customerID, clusterID, nodeName string) (obs *PodObservation, exists bool, err error)
	// updateBurstGPUTelemetry merges one GPU utilisation sample into an existing
	// burst row. UPDATE-only: a deleted row stays deleted. Tenant+cluster scoped.
	updateBurstGPUTelemetry(ctx context.Context, u BurstGPUTelemetryUpdate) (bool, error)
	// burstOwnedBy reports whether a burst row exists AND belongs to customerID,
	// touching nothing. It is the check a caller needs when the ACTION it is
	// authorising is not itself tenant-scoped: claimBurst deletes by id alone, so
	// without this a connector could name any id in the fleet and tear down
	// another tenant's node.
	//
	// The two false answers are one answer. "No such burst" and "another
	// tenant's burst" are indistinguishable here on purpose — a caller that
	// could tell them apart would be a burst-id oracle across every tenant.
	burstOwnedBy(ctx context.Context, burstID, customerID string) (bool, error)
	// burstForTenant reads one tenant's burst row whole, touching nothing. It is
	// burstOwnedBy for the caller that has to authorise against the record rather
	// than against ownership alone — an idle teardown request is checked on the
	// burst's own nodeOnly flag, cluster and node name, none of which the request
	// may supply. ok=false is "no such burst" and "another tenant's burst"
	// together, exactly as burstOwnedBy keeps them together.
	burstForTenant(ctx context.Context, burstID, customerID string) (*Burst, bool, error)
	// claimBurst removes one burst row and returns the record it removed, so
	// exactly one caller wins a given burst across every replica. Memory cannot
	// decide this: each central holds the burst in its own map, so a map-only
	// claim lets both replicas win and tear the same VM down twice. Returning
	// the row (rather than just a winner flag) is what lets a replica claim a
	// burst it never saw. ok=false means the row was already gone — another
	// claimer won, or it never existed.
	claimBurst(ctx context.Context, id string) (*Burst, bool, error)
	// listBursts reads the durable burst set at RUNTIME, not just at boot.
	// The orphan sweep needs it: its tracked set decides which cloud VMs get
	// destroyed, and this process's in-memory map only ever reflects writes
	// this process made. Any other central would be invisible to it.
	listBursts(ctx context.Context) ([]*Burst, error)
	// The pod-slot trio is the durable form of the allocator's per-process
	// reservation map, and it exists for the same reason claimBurst does: two
	// centrals each consulting their own map hand the SAME /24 to two live
	// bursts, which is not stale bookkeeping — each burst advertises its /24 as
	// a mesh subnet route, so the mesh gets two peers claiming one prefix and
	// routing breaks for both customers.
	//
	// reservePodSlot returns false when another caller already holds the slot;
	// the allocator moves to the next one rather than failing. releasePodSlot
	// is keyed by burst id because slots are recycled and a stale release must
	// not free a live burst's reservation. reservedPodSlots omits rows older
	// than ttl, so a Plan that died before persisting its burst cannot wedge a
	// slot forever.
	reservePodSlot(ctx context.Context, slot int, burstID string, ttl time.Duration) (bool, error)
	releasePodSlot(ctx context.Context, burstID string) error
	reservedPodSlots(ctx context.Context, ttl time.Duration) (map[int]bool, error)
	// The idempotency trio is the durable form of "this keyed submission has
	// already been made". It is a claim rather than a write-through for the
	// reason claimBurst is: the decision must be made once across every
	// replica, and a per-process map makes it once PER replica — which for this
	// record means one Idempotency-Key buying one paid node per central.
	//
	// claimIdempotency inserts the claim, or returns the one already holding
	// the scope with won=false. It never returns (nil, false, nil): a caller
	// that lost must be able to tell a replay from a conflict from a submission
	// still in flight, and only the existing row says which. ErrIdempotencyRaced
	// when the row was inserted and released underneath the call and the retry
	// could not resolve it either — fail-closed, because there is no answer here
	// that is safe to guess.
	claimIdempotency(ctx context.Context, c *IdempotencyClaim) (*IdempotencyClaim, bool, error)
	// completeIdempotency stamps the terminal status and response on a claim.
	// The body is stored verbatim so a replay is byte-identical to the answer
	// the first caller received. ErrNotFound when the claim is gone.
	completeIdempotency(ctx context.Context, id string, status int, body []byte, at time.Time) error
	// deleteIdempotency frees a claim whose submission bailed out before any
	// provider call. Absent row is not an error — the key is free either way.
	deleteIdempotency(ctx context.Context, id string) error
	upsertPV(*PersistentVolume) error
	deletePV(id string) error
	// setMeshState writes the single global mesh-state row. Unlike the
	// other operational write-throughs it RETURNS its error and its caller does
	// not proceed without it, because the record is the proof of authorship the
	// next shared-tailnet write consults: a central that advanced it in memory
	// only would, after a restart, be unable to recognise the rule it wrote and
	// would leave a stale grant standing beside a new one. There is no delete —
	// the row is a singleton whose empty value is a valid state.
	setMeshState(*meshState) error
	// Close releases the backend's resources (e.g. the pg connection pool).
	Close()
}

// admissionPersister is the optional durable admission capability. Keeping it
// separate from persister means specialized persistence fakes and backends do
// not need unrelated admission stubs; stores without it use the in-memory path.
type admissionPersister interface {
	// reserveAdmission runs the atomic admission transaction: advisory-lock the
	// tenant, delete expired reservations, read the customer's limits from the
	// customers table, count/sum live bursts, count/sum non-expired reservations,
	// evaluate admission, and insert idempotently — all before commit.
	reserveAdmission(ctx context.Context, customerID, workloadID string, candidateMicroUSD int64) (string, error)
	// releaseAdmission deletes a reservation row. Idempotent.
	releaseAdmission(ctx context.Context, id string) error
}

// The p* helpers are the single choke point the store mutators call. Each is a
// no-op when persistence is disabled. Callers hold s.mu.
//
// Customer / workload / PV writes log (best-effort, matching the old JSON
// backend) rather than failing the in-memory mutation — the running process
// stays correct in memory; only a later restart would observe the gap.
//
// The burst helpers, the account/membership writes and the customer-lifecycle
// trio are the exception: they return the error. A burst record is the only thing tying a
// running, billing VM to a reaper, so a silently dropped burst write leaves a VM
// that no restart — and no other replica — can find. The in-memory mutation
// still stands either way, so this process keeps tracking the VM; the error
// tells the caller the durable copy may be missing. The account/membership pair
// is stricter still: their callers roll the in-memory change back, because a
// grant that only exists in one replica's memory is access that disappears at
// the next restart with no path back.

func (s *Store) recordPersistenceFailure(entity, operation string, err error) error {
	switch {
	case err == nil,
		errors.Is(err, ErrCustomerTombstoned),
		errors.Is(err, ErrCustomerExists),
		errors.Is(err, ErrAccountHasTenant),
		errors.Is(err, ErrAccountEmailUnverified),
		errors.Is(err, ErrCustomerActive),
		errors.Is(err, ErrInvalidAudit),
		errors.Is(err, ErrNotFound):
		// These are durable business-rule refusals, not a storage outage.
	default:
		s.Cost.RecordPersistenceFailure(entity, operation)
	}
	return err
}

func (s *Store) pUpsertCustomer(c *Customer) error {
	if s.persist == nil {
		return nil
	}
	// AddCustomer may pass its already-indexed record. The persister may only
	// advance the private copy until we publish its precondition under mu.
	snapshot := *c
	err := s.persist.upsertCustomer(&snapshot)
	refreshed := s.refreshCustomerWrite(c.ID, err)
	switch {
	case err == nil:
		s.acceptCustomerWrite(&snapshot)
	case errors.Is(err, ErrCustomerTombstoned):
		// Not a backend fault: the id was permanently retired by a finished
		// offboard, so the durable layer refused to write it back. This process
		// drops its cached record when the persister supplies conflict state.
		// Some best-effort metadata callers cannot return an error, so log the
		// refusal as well as returning it to callers that can retry.
		slog.Warn("state: customer write refused; id is permanently retired",
			"id", c.ID, "error", err)
	case refreshed:
		slog.Warn("state: stale customer write refused; working copy refreshed", "id", c.ID)
	default:
		s.recordPersistenceFailure("customer", "upsert", err)
		slog.Error("state: persist customer", "id", c.ID, "error", err)
	}
	return err
}

// pSetCustomer is pUpsertCustomer for a caller that HAS an error to hand back —
// operator-set authorization and spend guardrails rather than operational state
// the logging path covers. A retired id maps
// to ErrNotFound: the row is gone, which from the caller's side is the same
// answer as a tenant that was never there.
func (s *Store) pSetCustomer(c *Customer, ev *AuditEvent) error {
	if ev != nil {
		if err := validateAudit(ev); err != nil {
			return err
		}
	}
	if s.persist == nil {
		return nil
	}
	err := s.persist.upsertCustomerAudited(c, ev)
	refreshed := s.refreshCustomerWrite(c.ID, err)
	switch {
	case err == nil:
		s.acceptCustomerWrite(c)
		return nil
	case errors.Is(err, ErrCustomerTombstoned):
		return fmt.Errorf("%w: customer %s is permanently retired", ErrNotFound, c.ID)
	case refreshed:
		return err
	default:
		s.recordPersistenceFailure("customer", "upsert", err)
		return fmt.Errorf("%w: persist customer %s: %w", ErrPersistence, c.ID, err)
	}
}

// The customer-lifecycle trio returns its errors rather than logging them: each
// backs a caller that must not proceed without the row. A provisioned tenant's
// token is handed out once, so a customer that exists only in this process's
// memory is a partner whose cluster stops authenticating at the next restart —
// and a revoke that did not land is a credential the operator was told was dead.
func (s *Store) pCreateTenant(c *Customer, owner *TenantMembership, requireFirst bool) error {
	if s.persist == nil {
		return nil
	}
	return s.recordPersistenceFailure("customer", "create", s.persist.createTenant(c, owner, requireFirst))
}

func (s *Store) pRevokeCustomer(c *Customer) error {
	if s.persist == nil {
		return nil
	}
	return s.recordPersistenceFailure("customer", "revoke", s.persist.revokeCustomer(c))
}

func (s *Store) pFinalizeRevokedCustomer(id string) error {
	if s.persist == nil {
		return nil
	}
	return s.recordPersistenceFailure("customer", "finalize", s.persist.finalizeRevokedCustomer(id))
}

func (s *Store) pDeleteCustomerAndMemberships(id string) error {
	if s.persist == nil {
		return nil
	}
	return s.recordPersistenceFailure("customer", "delete", s.persist.deleteCustomerAndMemberships(id))
}

// pUpsertMembership returns its error for the same reason the burst helpers do:
// a membership is the only record of a human's access to a tenant, and nothing
// re-creates one. A dropped write would be invisible until a restart silently
// removed someone's tenant.
func (s *Store) pUpsertMembership(m *TenantMembership, ev *AuditEvent) error {
	if s.persist == nil {
		return nil
	}
	return s.recordPersistenceFailure("membership", "upsert", s.persist.upsertMembership(m, ev))
}

// pDeleteMembership is pUpsertMembership's inverse and returns its error for the
// mirror-image reason: a delete that only landed in memory is access somebody
// was told was revoked, handed straight back at the next restart. There is no
// best-effort form of that, so the caller rolls the in-memory removal back.
func (s *Store) pDeleteMembership(id string, ev *AuditEvent) error {
	if s.persist == nil {
		return nil
	}
	return s.recordPersistenceFailure("membership", "delete", s.persist.deleteMembership(id, ev))
}

func (s *Store) pUpsertWorkload(w *Workload) {
	if s.persist == nil {
		return
	}
	if err := s.persist.upsertWorkload(w); err != nil {
		s.recordPersistenceFailure("workload", "upsert", err)
		slog.Error("state: persist workload", "id", w.ID, "error", err)
	}
}

func (s *Store) pUpsertBurst(b *Burst) error {
	if s.persist == nil {
		return nil
	}
	return s.recordPersistenceFailure("burst", "upsert", s.persist.upsertBurst(b))
}

func (s *Store) pDeleteBurst(id string) error {
	if s.persist == nil {
		return nil
	}
	return s.recordPersistenceFailure("burst", "delete", s.persist.deleteBurst(id))
}

func (s *Store) pUpsertPV(v *PersistentVolume) {
	if s.persist == nil {
		return
	}
	if err := s.persist.upsertPV(v); err != nil {
		s.recordPersistenceFailure("persistent_volume", "upsert", err)
		slog.Error("state: persist pv", "id", v.ID, "error", err)
	}
}

func (s *Store) pDeletePV(id string) {
	if s.persist == nil {
		return
	}
	if err := s.persist.deletePV(id); err != nil {
		s.recordPersistenceFailure("persistent_volume", "delete", err)
		slog.Error("state: persist pv delete", "id", id, "error", err)
	}
}

// pSetMeshState writes the global mesh-state row and reports the result.
// A store with no persister answers nil: the record only has to outlive a
// process that has somewhere to write it.
func (s *Store) pSetMeshState(st *meshState) error {
	if s.persist == nil {
		return nil
	}
	if err := s.persist.setMeshState(st); err != nil {
		s.recordPersistenceFailure("mesh_state", "set", err)
		return fmt.Errorf("%w: mesh policy state: %w", ErrPersistence, err)
	}
	return nil
}

// Close releases the durable backend, if any. Safe to call on an in-memory
// store.
func (s *Store) Close() {
	if s.persist != nil {
		s.persist.Close()
	}
}
