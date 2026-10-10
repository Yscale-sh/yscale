package state

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"time"
)

// The tenant audit journal is the durable, append-only record of WHO changed
// what on a tenant. It lives beside the working set rather than in it: audit
// rows are never loaded by applySnapshot (the snapshot type carries no audit
// collection, by construction) and are never deleted with the customer rows an
// offboard removes, because evidence about a tenant has to outlive the tenant.
//
// Two properties make it evidence rather than logging. The rows are written in
// the SAME durable transaction as the change they describe wherever the
// persister can express it (memberships, the workload-namespace set, a workload
// submission), so a change cannot land unaudited. And every field is a closed
// constant or an already-validated internal value — see AuditDetail — so no
// caller can put a token, a spec, an env value or an arbitrary error body into
// the journal by writing one into a request.

// Actor kinds. The set is closed and validated on write, exactly as membership
// roles are: an audit row whose actor kind has no defined meaning is evidence
// nobody can read back.
//
// These are genuinely different principals, not labels on one:
// ActorHuman is a person signed in through Yscale ID; ActorCluster is a
// tenant's Customer.Token, which identifies a CLUSTER and never a person;
// ActorOperator is the YSCALE_ADMIN_TOKEN credential, which is neither; and
// ActorSystem is central itself — the reaper watchdog, the sweeps, the
// lifecycle observations no request asked for.
const (
	ActorHuman            = "human"
	ActorCluster          = "cluster"
	ActorOperator         = "operator"
	ActorSystem           = "system"
	ActorCatalogPublisher = "catalog_publisher"
)

var validActorKinds = map[string]bool{
	ActorHuman: true, ActorCluster: true, ActorOperator: true, ActorSystem: true,
	ActorCatalogPublisher: true,
}

// Actor identifies the principal behind a change. It carries an account id and
// a tenant id and NOTHING else — no issuer, no subject, no token, no email.
// The identity provider's key for a human identifies them across every tenant
// on this central, and a journal a co-owner may read is not a place for it; the
// account id is already the durable handle every other tenant-scoped surface
// uses. A cluster credential has no human fields at all to omit.
type Actor struct {
	Kind string `json:",omitempty"`
	// AccountID is set only for ActorHuman: it is the human's account id, the
	// same one the roster renders. Empty for every other kind.
	AccountID string `json:",omitempty"`
	// CustomerID is the tenant the credential was scoped to. Empty for
	// ActorOperator and ActorSystem, which are not tenant-scoped principals.
	CustomerID string `json:",omitempty"`
	// PublisherID is set only for ActorCatalogPublisher. It is the safe,
	// server-minted identifier, never the credential or its digest.
	PublisherID string `json:",omitempty"`
}

// Constructors rather than literals keep each principal in its closed shape,
// so a credential cannot acquire a human account id by filling the wrong field.
func HumanActor(accountID, customerID string) Actor {
	return Actor{Kind: ActorHuman, AccountID: accountID, CustomerID: customerID}
}

func ClusterActor(customerID string) Actor {
	return Actor{Kind: ActorCluster, CustomerID: customerID}
}

func CatalogPublisherActor(customerID, publisherID string) Actor {
	return Actor{Kind: ActorCatalogPublisher, CustomerID: customerID, PublisherID: publisherID}
}

// OperatorActor is the YSCALE_ADMIN_TOKEN credential. It carries no identity
// beyond its kind: this deployment holds ONE operator token, so there is no
// per-operator identity to record and inventing a field for one would suggest
// the journal can tell two operators apart when it cannot.
func OperatorActor() Actor { return Actor{Kind: ActorOperator} }

// SystemActor is central acting on its own — the watchdog, a reap, a sweep.
func SystemActor() Actor { return Actor{Kind: ActorSystem} }

// Human reports whether this actor is a signed-in person, which is the only
// kind an account id may be compared against.
func (a Actor) Human() bool { return a.Kind == ActorHuman && a.AccountID != "" }

// Audited actions. Closed set, one constant per decision or observation the
// journal records, namespaced by the thing they act on so a reader can filter
// on a prefix without parsing.
//
// The submit/cancel pair are AUTHORIZATION decisions — a request was accepted
// or denied — and are written in the same durable step as the change they
// authorize. The rest are OBSERVATIONS of a lifecycle that already happened,
// and are best-effort by design: an audit backend that is down must never
// strand a burst teardown.
const (
	ActionWorkloadSubmit    = "workload.submit"
	ActionWorkloadRetry     = "workload.retry"
	ActionWorkloadCancel    = "workload.cancel"
	ActionWorkloadLogs      = "workload.logs"
	ActionWorkloadStarted   = "workload.started"
	ActionWorkloadCompleted = "workload.completed"
	ActionWorkloadFailed    = "workload.failed"
	ActionWorkloadCancelled = "workload.cancelled"
	ActionWorkloadReaped    = "workload.reaped"
	ActionWorkloadExpired   = "workload.expired"

	ActionMembershipGrant      = "membership.grant"
	ActionMembershipRoleChange = "membership.role_change"
	ActionMembershipRemove     = "membership.remove"

	ActionWorkloadNamespacesSet  = "tenant.workload_namespaces"
	ActionClusterPolicySet       = "tenant.cluster_policy"
	ActionTemplateCatalogSet     = "tenant.workload_templates"
	ActionGitOpsSourcesSet       = "tenant.gitops_sources"
	ActionTenantLimitsSet        = "tenant.limits_set"
	ActionTenantCredentialRotate = "tenant.rotate_credential"

	// The cluster registry lifecycle. Register/rotate/delete are authorization
	// decisions on the tenant surface; claim is an observation — a legacy
	// connector self-announced a cluster and the registry absorbed it. None of
	// the four ever carries a credential, a hash, or the tenant's display
	// name: the target id and the actor's role are the whole story.
	ActionClusterRegister = "cluster.register"
	ActionClusterRotate   = "cluster.rotate_credential"
	ActionClusterDelete   = "cluster.delete"
	ActionClusterClaim    = "cluster.claim"

	// The hosted-capacity lifecycle: platform-managed cluster assignments the
	// OPERATOR makes on the tenant's behalf (hosted.go). Distinct actions
	// rather than the tenant registry's, so the journal never shows an
	// operator's shared-capacity change as something a tenant did. Assign and
	// delete carry the reserved namespace in Detail.Namespaces — they move the
	// tenant's namespace authorization in the same durable write.
	ActionHostedClusterAssign       = "cluster.hosted_assign"
	ActionHostedClusterRotate       = "cluster.hosted_rotate_credential"
	ActionHostedClusterDelete       = "cluster.hosted_delete"
	ActionHostedCapacityRequest     = "tenant.hosted_capacity_request"
	ActionCloudAccountConnect       = "cloud_account.connect"
	ActionCloudAccountRotate        = "cloud_account.rotate"
	ActionCloudAccountDisconnect    = "cloud_account.disconnect"
	ActionRuntimeBindingSet         = "runtime_binding.set"
	ActionRuntimeBindingDelete      = "runtime_binding.delete"
	ActionCatalogPublisherCreate    = "catalog_publisher.create"
	ActionCatalogPublisherRotate    = "catalog_publisher.rotate_credential"
	ActionCatalogPublisherDelete    = "catalog_publisher.delete"
	ActionBillingServiceCreditGrant = "billing.service_credit_grant"
	ActionConnectorCommandAck       = "connector_command.ack"
	// ActionConnectorCommandRequeue is an operator putting a dead-lettered
	// connector command back on the delivery path. It is an authorization
	// decision, not an observation — nothing in the ledger ever requeues on its
	// own — so it is written in the same durable step as the state change.
	ActionConnectorCommandRequeue = "connector_command.requeue"
)

var validAuditActions = map[string]bool{
	ActionWorkloadSubmit: true, ActionWorkloadRetry: true, ActionWorkloadCancel: true, ActionWorkloadLogs: true,
	ActionWorkloadStarted: true, ActionWorkloadCompleted: true,
	ActionWorkloadFailed: true, ActionWorkloadCancelled: true,
	ActionWorkloadReaped: true, ActionWorkloadExpired: true,
	ActionMembershipGrant: true, ActionMembershipRoleChange: true,
	ActionMembershipRemove: true, ActionWorkloadNamespacesSet: true,
	ActionClusterPolicySet: true, ActionTemplateCatalogSet: true,
	ActionGitOpsSourcesSet: true, ActionTenantLimitsSet: true, ActionTenantCredentialRotate: true,
	ActionClusterRegister: true, ActionClusterRotate: true,
	ActionClusterDelete: true, ActionClusterClaim: true,
	ActionHostedClusterAssign: true, ActionHostedClusterRotate: true,
	ActionHostedClusterDelete:   true,
	ActionHostedCapacityRequest: true,
	ActionCloudAccountConnect:   true, ActionCloudAccountRotate: true,
	ActionCloudAccountDisconnect: true,
	ActionRuntimeBindingSet:      true, ActionRuntimeBindingDelete: true,
	ActionCatalogPublisherCreate: true, ActionCatalogPublisherRotate: true,
	ActionCatalogPublisherDelete:    true,
	ActionBillingServiceCreditGrant: true,
	ActionConnectorCommandAck:       true,
	ActionConnectorCommandRequeue:   true,
}

// Outcomes. Accepted and denied are the two halves of an authorization
// decision; observed is what a lifecycle event gets, because nothing authorized
// it — a Job finishing is not a request anyone made.
const (
	OutcomeAccepted = "accepted"
	OutcomeDenied   = "denied"
	OutcomeObserved = "observed"
)

var validAuditOutcomes = map[string]bool{
	OutcomeAccepted: true, OutcomeDenied: true, OutcomeObserved: true,
}

// Target kinds. What the action names, as opposed to who took it.
const (
	TargetWorkload         = "workload"
	TargetMembership       = "membership"
	TargetTenant           = "tenant"
	TargetCluster          = "cluster"
	TargetCloudAccount     = "cloud_account"
	TargetRuntimeBinding   = "runtime_binding"
	TargetCatalogPublisher = "catalog_publisher"
	// TargetConnectorCommand names a durable connector command by its stable,
	// central-minted id. The id is a digest of identity central computed itself,
	// so it carries nothing a connector or a submitter authored.
	TargetConnectorCommand = "connector_command"
)

var validAuditTargets = map[string]bool{
	TargetWorkload: true, TargetMembership: true, TargetTenant: true,
	TargetCluster:          true,
	TargetCloudAccount:     true,
	TargetRuntimeBinding:   true,
	TargetCatalogPublisher: true,
	TargetConnectorCommand: true,
}

// Reason codes. A closed vocabulary rather than a message, for the same reason
// the member routes answer a FIXED body: a free-text reason is where an error
// from some backend ends up, and an audit row is the last place that should be
// able to carry one.
const (
	ReasonNamespaceAuthorized               = "namespace_authorized"
	ReasonNamespaceNotAuthorized            = "namespace_not_authorized"
	ReasonRoleAuthorized                    = "role_authorized"
	ReasonRoleReadOnly                      = "role_read_only"
	ReasonSubmitterMatch                    = "submitter_match"
	ReasonNotSubmitter                      = "not_submitter"
	ReasonWorkloadNotTerminal               = "workload_not_terminal"
	ReasonClusterPolicyUpdated              = "cluster_policy_updated"
	ReasonTemplateCatalogUpdated            = "template_catalog_updated"
	ReasonGitOpsSourcesUpdated              = "gitops_sources_updated"
	ReasonTemplateReferenceInvalid          = "template_reference_invalid"
	ReasonTemplateNotOffered                = "template_not_offered"
	ReasonTemplateVersionStale              = "template_version_stale"
	ReasonTemplateShapeMismatch             = "template_shape_mismatch"
	ReasonClusterRegistered                 = "cluster_registered"
	ReasonClusterCredentialRotated          = "cluster_credential_rotated"
	ReasonClusterDeleted                    = "cluster_deleted"
	ReasonClusterClaimed                    = "cluster_claimed"
	ReasonCloudAccountConnected             = "cloud_account_connected"
	ReasonCloudAccountRotated               = "cloud_account_rotated"
	ReasonCloudAccountDisconnected          = "cloud_account_disconnected"
	ReasonCloudAccountInUse                 = "cloud_account_in_use"
	ReasonRuntimeBindingCreated             = "runtime_binding_created"
	ReasonRuntimeBindingRotated             = "runtime_binding_rotated"
	ReasonRuntimeBindingDeleted             = "runtime_binding_deleted"
	ReasonCatalogPublisherCreated           = "catalog_publisher_created"
	ReasonCatalogPublisherCredentialRotated = "catalog_publisher_credential_rotated"
	ReasonCatalogPublisherDeleted           = "catalog_publisher_deleted"
	ReasonBillingServiceCreditGranted       = "billing_service_credit_granted"

	ReasonHostedClusterAssigned          = "hosted_cluster_assigned"
	ReasonHostedClusterCredentialRotated = "hosted_cluster_credential_rotated"
	ReasonHostedClusterDeleted           = "hosted_cluster_deleted"
	ReasonHostedCapacityRequested        = "hosted_capacity_requested"
	ReasonTenantLimitsUpdated            = "tenant_limits_updated"
	ReasonTenantCredentialRotated        = "tenant_credential_rotated"
	ReasonConnectorCommandAckScope       = "connector_command_ack_scope_mismatch"
	ReasonConnectorCommandAckUnknown     = "connector_command_ack_unknown"
	ReasonConnectorCommandRequeued       = "connector_command_requeued"
	ReasonWorkloadRetryChanged           = "workload_retry_changed"
)

// AuditDetail is the bounded, safe payload of an audit row. Every field is
// either a closed constant from this file, an internal identifier central
// minted itself, a validated DNS-1123 namespace label, or a membership role
// from the closed role set.
//
// What it deliberately CANNOT hold is the list the governance requirement names
// and the reason the type is a struct rather than a map: access tokens, spec
// YAML, environment values, commands, storage endpoints, Secret names, the
// identity provider's issuer/subject, and arbitrary error bodies. There is no
// field to put them in, so no future caller can put them there by accident —
// the same "absent by construction" the human response shapes rely on.
type AuditDetail struct {
	// Reason is one of the Reason* codes. Present on authorization decisions.
	Reason string `json:",omitempty"`
	// Role and PreviousRole are membership roles from the closed role set:
	// the role the actor held (on a workload decision) or the role a membership
	// now has and had (on a roster change).
	Role         string `json:",omitempty"`
	PreviousRole string `json:",omitempty"`
	// Rule and RuleVersion name the policy that decided a submission, so a
	// reader can tell which version of which rule allowed it.
	Rule        string `json:",omitempty"`
	RuleVersion string `json:",omitempty"`
	// RequestedNamespace is what the submitter asked for, bounded through
	// SafeNamespace — a value that is not a DNS-1123 label is recorded as
	// NamespaceRedacted rather than stored. GrantedNamespace is central's own
	// answer, so it is already one of the tenant's authorized labels.
	RequestedNamespace string `json:",omitempty"`
	GrantedNamespace   string `json:",omitempty"`
	// Namespaces is the tenant's replacement authorized set, which the caller
	// has already validated as DNS-1123 labels.
	Namespaces []string `json:",omitempty"`
	// ClusterPolicy is the tenant's replacement placement policy, already
	// validated and normalized by the store.
	ClusterPolicy *ClusterPolicy `json:",omitempty"`
	// RequestedClusterID is the X-Cluster-ID pin, when one was supplied.
	// GrantedClusterID is central's placement decision. PlacementMode is
	// "pinned" or "auto".
	RequestedClusterID string `json:",omitempty"`
	GrantedClusterID   string `json:",omitempty"`
	PlacementMode      string `json:",omitempty"`
	// TemplateID and TemplateVersion name the catalog entry a submission
	// referenced, and TemplateCatalogRevision the catalog it was judged
	// against. Ids and versions only — a template's title, description and
	// image are tenant-authored strings, and this shape has nowhere to put one.
	TemplateID              string `json:",omitempty"`
	TemplateVersion         int    `json:",omitempty"`
	TemplateCatalogRevision string `json:",omitempty"`
	// TemplateCatalog is the tenant's replacement catalog reduced to the same
	// ids and versions, in published order, so a reader can see what a tenant
	// offered without the journal carrying what it said about it.
	TemplateCatalog []AuditTemplateRef `json:",omitempty"`
	// GitOpsSourcesRevision is the registry a replacement produced, and
	// GitOpsSources the same registry reduced to ids and reconciler kinds. A
	// source's repository URL, ref, path and name name systems outside this one,
	// and this shape has nowhere to put one.
	GitOpsSourcesRevision string                 `json:",omitempty"`
	GitOpsSources         []AuditGitOpsSourceRef `json:",omitempty"`
	// Status is a workload lifecycle status ("succeeded", "failed",
	// "cancelled"); BurstID is central's own burst id.
	Status  string `json:",omitempty"`
	BurstID string `json:",omitempty"`
	// RetryWorkloadID links a retry decision to the new internal workload ID
	// whose admission shares this commit. The event target is the source run.
	RetryWorkloadID string `json:"retry_workload_id,omitempty"`
	// Non-secret numeric guardrails for tenant limits changes.
	PreviousMaxConcurrentBursts *int     `json:"previous_max_concurrent_bursts,omitempty"`
	MaxConcurrentBursts         *int     `json:"max_concurrent_bursts,omitempty"`
	PreviousMaxHourlyUSD        *float64 `json:"previous_max_hourly_usd,omitempty"`
	MaxHourlyUSD                *float64 `json:"max_hourly_usd,omitempty"`
	AmountMicroUSD              int64    `json:"amount_micro_usd,omitempty"`
	Currency                    string   `json:"currency,omitempty"`
	// PlacementDigest is the identity of the placement decision a submission
	// launched under — the same value the receipt carries and the billing quote
	// is taken against. A hex digest of central's own decision content: it names
	// no provider account, carries no price and cannot hold submitter input, so
	// it belongs in the closed detail shape. Empty on every event that decided
	// no placement.
	PlacementDigest string `json:"placement_digest,omitempty"`
}

// AuditTemplateRef is the only template shape allowed in catalog-change audit
// details. It intentionally has no catalog revision or tenant-authored fields.
type AuditTemplateRef struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
}

// AuditGitOpsSourceRef is the only GitOps shape allowed in registry-change audit
// details: the source id central validated and the closed reconciler constant.
// It intentionally has no repository, ref, path or tenant-authored field.
type AuditGitOpsSourceRef struct {
	ID         string `json:"id"`
	Reconciler string `json:"reconciler"`
}

// AuditEvent is one row of the journal.
type AuditEvent struct {
	// ID is time-ordered: "aud_" then the append time in nanoseconds and a
	// random tail, both hex. Ordering by id descending is therefore ordering by
	// time descending, with the tail breaking ties deterministically, so one
	// indexed column gives the read a stable total order AND an opaque cursor.
	// A caller cannot compute the next id.
	//
	// It is stamped by the DURABLE APPEND, not by the caller, and only once the
	// per-tenant append lock is held — see stampAudit. Until then it is empty:
	// an id handed out before its row commits is an id a reader's cursor can
	// pass, and the row behind it is then skipped forever.
	ID         string
	CustomerID string
	At         time.Time
	Actor      Actor
	Action     string
	Outcome    string
	TargetKind string `json:",omitempty"`
	TargetID   string `json:",omitempty"`
	Detail     AuditDetail
}

// NewAuditEvent builds an event from a caller's fields. It stamps NOTHING: the
// caller supplies everything that carries meaning, and the two fields that
// decide ordering — the id and the append time — belong to the append itself,
// which is the only place that knows what has already committed for the tenant.
func NewAuditEvent(ev AuditEvent) *AuditEvent { return &ev }

// auditIDLen is the hex payload after the prefix: 16 digits of nanosecond
// timestamp then 16 of randomness.
const auditIDLen = 32

const auditIDPrefix = "aud_"

func newAuditID() string {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UTC().UnixNano()))
	// A failed read would give every id in that instant the same tail, which
	// only costs tie-break determinism; the timestamp half still orders them.
	_, _ = rand.Read(buf[8:])
	return auditIDPrefix + hex.EncodeToString(buf[:])
}

// stampAudit gives an event the id and time it is stored under, where prev is
// the newest id already committed for that tenant ("" for the first row).
//
// Callers must hold the tenant's append lock until they commit — the Postgres
// advisory lock insertAuditTx takes, or the recorder mutex in the fakes. That
// is what makes the id ORDER the COMMIT order: the next appender cannot stamp
// until this one's row is committed and visible, so it reads this id as prev
// and mints a larger one. A row committing later therefore always sorts newer
// than every row a reader could already have seen, which is exactly the
// condition under which a descending cursor cannot skip it.
//
// nextAuditID is what makes that true across replicas as well as within one.
// Wall clocks disagree between machines by more than the gap between two
// commits, so a replica whose clock runs behind would otherwise mint an id that
// lands INSIDE a page a reader has already walked past.
func stampAudit(ev *AuditEvent, prev string) {
	ev.ID = nextAuditID(prev)
	ev.At = time.Now().UTC()
}

// nextAuditID mints an id that is strictly greater than prev. The clock is the
// preferred source — it keeps ids meaningfully time-ordered — and prev is the
// floor it may not fall below.
func nextAuditID(prev string) string {
	id := newAuditID()
	if prev == "" || id > prev {
		return id
	}
	return bumpAuditID(prev)
}

// bumpAuditID returns the id one increment above prev, treating the hex payload
// as a 128-bit big-endian counter. The low half is random, so incrementing it
// stays unguessable; the high half is a nanosecond timestamp, which cannot
// overflow into the carry this century.
func bumpAuditID(prev string) string {
	if !ValidAuditCursor(prev) {
		// Not an id this store minted, so there is nothing to order against.
		return newAuditID()
	}
	raw, err := hex.DecodeString(prev[len(auditIDPrefix):])
	if err != nil {
		return newAuditID()
	}
	for i := len(raw) - 1; i >= 0; i-- {
		raw[i]++
		if raw[i] != 0 {
			break
		}
	}
	return auditIDPrefix + hex.EncodeToString(raw)
}

var auditIDPattern = regexp.MustCompile(`^aud_[0-9a-f]{32}$`)
var catalogPublisherAuditIDPattern = regexp.MustCompile(`^pub_[0-9a-f]{16}$`)

// ValidAuditCursor reports whether s is a well-formed audit id, which is what
// the read accepts as a cursor. Checked rather than trusted: a cursor that is
// not an id can only be a caller guessing at the ordering, and answering page
// one for it would look exactly like the page they asked for.
func ValidAuditCursor(s string) bool { return auditIDPattern.MatchString(s) }

// validateAudit refuses an event outside the closed sets before it reaches
// durable state, for the reason ValidRole exists: every later reader gives an
// unrecognised value no meaning, and the journal has no second chance to fix a
// row it is not allowed to update.
//
// The id is not among the checks: nothing outside the durable append sets one
// (see stampAudit), so there is no caller-supplied id here to be malformed.
func validateAudit(ev *AuditEvent) error {
	switch {
	case ev == nil:
		return fmt.Errorf("%w: nil audit event", ErrInvalidAudit)
	case ev.CustomerID == "":
		return fmt.Errorf("%w: audit event has no tenant", ErrInvalidAudit)
	case !validActorKinds[ev.Actor.Kind]:
		return fmt.Errorf("%w: actor kind %q", ErrInvalidAudit, ev.Actor.Kind)
	case ev.Actor.Kind == ActorCatalogPublisher &&
		(!catalogPublisherAuditIDPattern.MatchString(ev.Actor.PublisherID) || ev.Actor.AccountID != "" ||
			ev.Actor.CustomerID != ev.CustomerID || ev.Action != ActionTemplateCatalogSet):
		return fmt.Errorf("%w: invalid catalog publisher actor", ErrInvalidAudit)
	case ev.Actor.Kind != ActorCatalogPublisher && ev.Actor.PublisherID != "":
		return fmt.Errorf("%w: publisher id on actor kind %q", ErrInvalidAudit, ev.Actor.Kind)
	case !validAuditActions[ev.Action]:
		return fmt.Errorf("%w: action %q", ErrInvalidAudit, ev.Action)
	case !validAuditOutcomes[ev.Outcome]:
		return fmt.Errorf("%w: outcome %q", ErrInvalidAudit, ev.Outcome)
	case ev.TargetKind != "" && !validAuditTargets[ev.TargetKind]:
		return fmt.Errorf("%w: target kind %q", ErrInvalidAudit, ev.TargetKind)
	}
	return nil
}

// NamespaceRedacted stands in for a requested namespace that is not a valid
// DNS-1123 label. The request was refused anyway, so the exact bytes buy a
// reader nothing — and storing arbitrary submitter input in the journal is the
// one thing the closed detail shape exists to prevent.
const NamespaceRedacted = "<invalid>"

var auditNamespaceLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// SafeNamespace bounds a submitted namespace for the journal. Empty stays empty
// — an unqualified submission requested nothing, which is a fact worth
// recording as absence.
func SafeNamespace(ns string) string {
	if ns == "" {
		return ""
	}
	if len(ns) > 63 || !auditNamespaceLabel.MatchString(ns) {
		return NamespaceRedacted
	}
	return ns
}

// SafeTemplateID bounds a referenced template id for the journal and for the
// refusal that names it. A reference central could not resolve is submitter
// input arriving in a header, and it reaches both a durable row and an error
// body — so it is held to the catalog's own id grammar, and anything else is
// recorded as the redaction rather than stored.
func SafeTemplateID(id string) string {
	if id == "" {
		return ""
	}
	if !templateIDPattern.MatchString(id) {
		return NamespaceRedacted
	}
	return id
}

// Audit page bounds. The journal is unbounded in the data model — nothing caps
// how much history a tenant accumulates — so, exactly as the roster does, the
// READ is what bounds it and the bound lives in the store rather than in the
// handler.
const (
	DefaultAuditLimit = 50
	MaxAuditLimit     = 200
)

// AuditQuery bounds one page of a tenant's journal. The zero value is the most
// recent page at the default limit.
type AuditQuery struct {
	// Limit caps the rows in the page. Zero or negative means
	// DefaultAuditLimit; anything above MaxAuditLimit is clamped to it. The
	// handler rejects an out-of-range limit with 400 before it gets here.
	Limit int
	// After is an exclusive cursor: only rows ordered strictly after it — that
	// is, older than it, since the page is newest-first — are in the page.
	After string
}

// AuditPage is one page of a tenant's journal, newest first.
type AuditPage struct {
	Events []AuditEvent
	// NextAfter is the cursor for the next page and is set ONLY when another
	// row exists behind this one, so a caller can tell "the journal ends here"
	// from "there is more" without a second call that returns nothing.
	NextAfter string
}

// bounded normalises a query the way both the in-store clamp and the durable
// read need it.
func (q AuditQuery) bounded() AuditQuery {
	if q.Limit <= 0 {
		q.Limit = DefaultAuditLimit
	}
	if q.Limit > MaxAuditLimit {
		q.Limit = MaxAuditLimit
	}
	return q
}

// AppendAudit writes one journal row on its own, for a decision that has no
// state change to ride along with — a refusal, or an observation of something
// that already happened. It returns the durable error so an AUTHORIZING caller
// can fail closed; observation callers deliberately ignore it (see
// observeWorkload).
//
// A nil persister is a successful no-op: the in-memory store is the OSS/dev and
// unit-test shape, and it has no durable journal to append to.
func (s *Store) AppendAudit(ev *AuditEvent) error {
	if err := validateAudit(ev); err != nil {
		return err
	}
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return nil
	}
	if err := p.appendAudit(ev); err != nil {
		s.recordPersistenceFailure("audit", "append", err)
		// Named by what the row was FOR, not by its id: the id is stamped inside
		// the transaction, so a failed append may not have one, and quoting a
		// value no row carries would send a reader looking for it.
		return fmt.Errorf("%w: append audit %s for %s: %w", ErrPersistence, ev.Action, ev.CustomerID, err)
	}
	return nil
}

// observeWorkload appends a lifecycle observation and swallows the failure.
//
// This is the one place the journal is deliberately best-effort, and the reason
// is the requirement that terminal cleanup never depend on it: FinishWorkload
// and its callers are how a burst stops billing, and refusing to mark a
// workload finished because an audit row would not write is how an audit
// backend outage turns into a cloud bill. The authorization decisions — submit
// and cancel — take the opposite trade and fail closed.
//
// Callers must NOT hold s.mu: this makes a durable round trip.
func (s *Store) observeWorkload(w *Workload, action, status string) {
	if w == nil {
		return
	}
	ev := NewAuditEvent(AuditEvent{
		CustomerID: w.CustomerID,
		Actor:      SystemActor(),
		Action:     action,
		Outcome:    OutcomeObserved,
		TargetKind: TargetWorkload,
		TargetID:   w.ID,
		Detail:     AuditDetail{Status: status, BurstID: w.BurstID},
	})
	if err := s.AppendAudit(ev); err != nil {
		slog.Warn("state: audit observation dropped; cleanup continues",
			"workload", w.ID, "action", action, "error", err)
	}
}

// observeWorkloadTerminal is observeWorkload for a workload that has just
// reached a terminal status, which is the one case where the action has to be
// derived rather than named by the caller.
func (s *Store) observeWorkloadTerminal(w *Workload, status string) {
	if w == nil {
		return
	}
	action := auditActionForStatus(status)
	if action == "" {
		slog.Warn("state: terminal status has no audit action; observation dropped",
			"workload", w.ID, "status", status)
		return
	}
	s.observeWorkload(w, action, status)
}

// auditActionForStatus maps a terminal workload status to the closed
// observation action that describes it. An unknown status maps to "", and the
// row is not written at all.
//
// Refusing is the safe answer, and the reason is that the alternative was
// tried: folding everything that was not "cancelled" into workload.completed
// recorded every FAILED workload — a budget overrun, a dispatch failure, a Job
// that exited nonzero — as a completion. The journal then said the opposite of
// what happened, which is worse than saying nothing. A status this map does not
// know is a lifecycle state the closed action set has no word for yet, so the
// observation is dropped (observeWorkloadTerminal logs it) rather than filed
// under a word that means something else.
func auditActionForStatus(status string) string {
	switch status {
	case "succeeded":
		return ActionWorkloadCompleted
	case "failed":
		return ActionWorkloadFailed
	case "cancelled":
		return ActionWorkloadCancelled
	default:
		return ""
	}
}

// WorkloadAuthorization is the bounded decision record stamped onto a workload
// at submission: what namespace was asked for, what central granted, and which
// rule at which version said so. It is what lets a reader answer "why was this
// allowed" from the workload itself, without re-deriving a policy that may have
// changed since.
//
// nil on a workload means a record written before provenance existed. Those
// stay readable — see Workload.SubmittedBy.
type WorkloadAuthorization struct {
	RequestedNamespace string `json:",omitempty"`
	GrantedNamespace   string `json:",omitempty"`
	Rule               string `json:",omitempty"`
	RuleVersion        string `json:",omitempty"`
	// Role is the submitter's membership role at submission time, for a human.
	// Empty for a cluster credential, which holds no role.
	Role      string `json:",omitempty"`
	DecidedAt time.Time
}

// WorkloadPlacement records the cluster placement decision that admitted a
// workload. It is separate from Workload.ClusterID, which remains the actual
// routed cluster used by teardown and older clients.
type WorkloadPlacement struct {
	RequestedClusterID string `json:",omitempty"`
	GrantedClusterID   string `json:",omitempty"`
	Mode               string `json:",omitempty"`
	Rule               string `json:",omitempty"`
	RuleVersion        string `json:",omitempty"`
	DecidedAt          time.Time
	// Receipt is the full provider placement decision this submission launched
	// under — the same typed answer the preview showed, bound by its digest.
	// nil on every record written before receipts existed, and on any
	// submission whose decider produced none; the five fields above remain the
	// cluster-placement answer either way, so an older record stays exactly as
	// readable as it was.
	Receipt *PlacementReceipt `json:",omitempty"`
}

// SubmitWorkload records a newly submitted workload AND the authorization
// decision that admitted it, and publishes the workload to the working set only
// once both are durable.
//
// The order is the whole point. PutWorkload's write-through is best-effort — a
// dropped workload row costs a restart its record and nothing more — but a
// submission that central authorized and cannot prove it authorized is exactly
// the gap this slice exists to close. So the durable write goes first and the
// in-memory publish second: a caller that sees an error has a store that never
// held the workload, which is what lets Create take its existing reap-and-503
// path instead of leaving a live VM behind a workload nobody can account for.
//
// The two writes ride ONE transaction in the Postgres backend, so there is no
// window where the workload row exists without its audit row.
//
// A nil persister publishes straight to memory: the OSS/dev store has no
// journal, and refusing submissions there would break the single-binary path
// for a guarantee it cannot make either way.
func (s *Store) SubmitWorkload(w *Workload, ev *AuditEvent) error {
	if err := validateAudit(ev); err != nil {
		return err
	}
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p != nil {
		if err := p.submitWorkload(w, ev); err != nil {
			s.recordPersistenceFailure("workload", "submit", err)
			return fmt.Errorf("%w: submit workload %s: %w", ErrPersistence, w.ID, err)
		}
	}
	s.mu.Lock()
	if p == nil && terminalWorkloadBindingConflict(w, s.workloads[w.ID]) {
		s.mu.Unlock()
		return fmt.Errorf("%w: terminal workload binding conflict", ErrPersistence)
	}
	preserveWorkloadTransition(w, s.workloads[w.ID])
	s.workloads[w.ID] = w
	s.mu.Unlock()
	return nil
}

// WorkloadCancelDecision is the whole answer to "may this human cancel this
// workload", taken from one consistent read of the four things that decide it.
type WorkloadCancelDecision struct {
	Allowed bool
	// Reason is a closed Reason* code for both answers, so the journal records
	// why a cancel was allowed as well as why it was refused.
	Reason string
	// Role is the caller's membership role at decision time.
	Role string
	// WorkloadID and BurstID name what was decided about.
	WorkloadID string
	BurstID    string
	// Submitter is the workload's stored provenance, nil for a legacy record.
	// Copied, so the caller holds no pointer into the store.
	Submitter *Actor
}

// WorkloadRetryDecision is the authorization answer for using one terminal
// workload as the source of a new run.
type WorkloadRetryDecision struct {
	Allowed bool
	Reason  string
	Role    string

	WorkloadID string
	BurstID    string
	Source     Workload
	Submitter  *Actor
}

// WorkloadReadDecision is a tenant-scoped snapshot used by read operations
// that must route through the connector after authorization, such as logs.
// Every valid tenant role may read; the method still takes one lock so a
// membership revoke cannot race the workload and cluster lookup.
type WorkloadReadDecision struct {
	Role   string
	Source Workload
}

// AuthorizeWorkloadCancel is a legacy advisory policy read. Its workload comes
// from this replica's cache, so it is not permission to perform a durable
// mutation. Actual cancellation uses RequestWorkloadCancellation, which checks
// current authority and commits the recoverable action together.
//
// The matrix:
//   - owner and admin may cancel ANY workload of their tenant, including one a
//     cluster credential or a pre-provenance record submitted;
//   - a member may cancel only a workload whose stored submitter is that same
//     human account — so a legacy record, and one a cluster token submitted,
//     are never a member's to cancel, because nothing proves it was theirs;
//   - a viewer may cancel nothing.
//
// ErrNotFound covers every way the caller has no business here: unknown tenant,
// revoked tenant, no membership, unknown workload, and a workload belonging to
// another tenant. They are deliberately one answer — a caller who is not a
// member must not learn that a tenant or a workload id exists.
func (s *Store) AuthorizeWorkloadCancel(customerID, accountID, workloadID string) (WorkloadCancelDecision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	caller, err := s.membershipForLocked(accountID, customerID)
	if err != nil {
		return WorkloadCancelDecision{}, err
	}
	w, ok := s.workloads[workloadID]
	if !ok || w.CustomerID != customerID {
		return WorkloadCancelDecision{}, ErrNotFound
	}
	return humanWorkloadCancelDecision(w, accountID, caller.Role), nil
}

// AuthorizeWorkloadRetry mirrors the human cancel matrix for starting a new
// run from an existing workload: owners and admins may retry any workload in
// their tenant, a member may retry only their own, and a viewer may retry none.
// This legacy cached read is advisory, not permission to create capacity.
// Mutations use PrepareWorkloadRetry followed by ReserveWorkloadRetry so current
// authority and the accepted audit share the admission reservation commit.
func (s *Store) AuthorizeWorkloadRetry(customerID, accountID, workloadID string) (WorkloadRetryDecision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	caller, err := s.membershipForLocked(accountID, customerID)
	if err != nil {
		return WorkloadRetryDecision{}, err
	}
	w, ok := s.workloads[workloadID]
	if !ok || w.CustomerID != customerID {
		return WorkloadRetryDecision{}, ErrNotFound
	}
	source := cloneWorkload(w)
	decision := WorkloadRetryDecision{
		Role:       caller.Role,
		WorkloadID: w.ID,
		BurstID:    w.BurstID,
		Source:     *source,
	}
	if w.SubmittedBy != nil {
		submitter := *w.SubmittedBy
		decision.Submitter = &submitter
	}
	switch {
	case caller.Role == RoleOwner || caller.Role == RoleAdmin:
		decision.Allowed, decision.Reason = true, ReasonRoleAuthorized
	case caller.Role == RoleMember:
		if w.SubmittedBy.submittedByAccount(accountID) {
			decision.Allowed, decision.Reason = true, ReasonSubmitterMatch
		} else {
			decision.Reason = ReasonNotSubmitter
		}
	default:
		decision.Reason = ReasonRoleReadOnly
	}
	return decision, nil
}

// AuthorizeWorkloadRead resolves one human-visible workload from the same
// snapshot as tenant membership. Unknown, cross-tenant, and revoked access all
// collapse to ErrNotFound through membershipForLocked and the ownership check.
func (s *Store) AuthorizeWorkloadRead(customerID, accountID, workloadID string) (WorkloadReadDecision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	caller, err := s.membershipForLocked(accountID, customerID)
	if err != nil {
		return WorkloadReadDecision{}, err
	}
	w, ok := s.workloads[workloadID]
	if !ok || w.CustomerID != customerID {
		return WorkloadReadDecision{}, ErrNotFound
	}
	return WorkloadReadDecision{Role: caller.Role, Source: *cloneWorkload(w)}, nil
}

func cloneWorkload(w *Workload) *Workload {
	if w == nil {
		return nil
	}
	cp := *w
	cp.SpecYAML = slices.Clone(w.SpecYAML)
	if w.SubmittedBy != nil {
		submitter := *w.SubmittedBy
		cp.SubmittedBy = &submitter
	}
	if w.Authorization != nil {
		authz := *w.Authorization
		cp.Authorization = &authz
	}
	if w.Placement != nil {
		placement := *w.Placement
		cp.Placement = &placement
	}
	if w.Cost != nil {
		cost := *w.Cost
		cp.Cost = &cost
	}
	if w.TemplateRef != nil {
		ref := *w.TemplateRef
		cp.TemplateRef = &ref
	}
	cp.Outcome = cloneWorkloadOutcome(w.Outcome)
	if w.NodeObservation != nil {
		obs := *w.NodeObservation
		cp.NodeObservation = &obs
	}
	return &cp
}

// submittedByAccount reports whether this provenance names the given human
// account. Safe on a nil receiver, which is the legacy record: a workload with
// no recorded submitter was submitted by nobody this store can name, so it is
// nobody's to cancel as a member.
func (a *Actor) submittedByAccount(accountID string) bool {
	return a != nil && a.Human() && a.AccountID == accountID
}

// TenantAuditFor reads one page of a tenant's journal for an owner or admin of
// that tenant.
//
// Authorization is decided under the store's read lock and the ROWS are read
// after it is released, which is the opposite of TenantRosterFor's one-snapshot
// rule and is right for a different reason: the roster's rows live in the same
// maps as the authorization, so reading them together costs nothing, while
// these rows live in Postgres and holding s.mu across that round trip would
// block every reader on this central — including the auth lookup on each agent
// request — for as long as the database is slow. The journal is append-only, so
// the worst a released lock can cost this read is a row appended after the
// decision, which is not a row the caller was not allowed to see.
//
// Errors are MembershipFor's, so the tenant-scoped answers stay identical
// across the whole human surface: ErrNotFound for an unknown tenant, a revoked
// one and a non-member alike, ErrNotAuthorized for a member or viewer who holds
// a grant that does not manage the tenant. A durable read failure is
// ErrPersistence — never ErrNotFound, which would tell a caller their live
// tenant had vanished.
func (s *Store) TenantAuditFor(ctx context.Context, customerID, callerAccountID string, q AuditQuery) (AuditPage, error) {
	if q.After != "" && !ValidAuditCursor(q.After) {
		return AuditPage{}, fmt.Errorf("%w: malformed cursor", ErrInvalidAudit)
	}
	q = q.bounded()

	s.mu.RLock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.RUnlock()
		return AuditPage{}, err
	}
	role := caller.Role
	p := s.persist
	s.mu.RUnlock()

	if role != RoleOwner && role != RoleAdmin {
		return AuditPage{}, fmt.Errorf("%w: %s is %q", ErrNotAuthorized, customerID, role)
	}
	page := AuditPage{Events: []AuditEvent{}}
	if p == nil {
		// No durable backend, so no journal: an empty page is the truthful
		// answer for the OSS/dev store, not an error.
		return page, nil
	}
	// One row beyond the page: the ONLY way to set a next cursor when another
	// row exists and leave it unset when the page is exactly the tail.
	rows, err := p.listAudit(ctx, customerID, q.After, q.Limit+1)
	if err != nil {
		return AuditPage{}, fmt.Errorf("%w: read audit for %s: %w", ErrPersistence, customerID, err)
	}
	if len(rows) > q.Limit {
		rows = rows[:q.Limit]
		page.NextAfter = rows[len(rows)-1].ID
	}
	for _, r := range rows {
		row := *r
		row.Detail.Namespaces = slices.Clone(r.Detail.Namespaces)
		row.Detail.TemplateCatalog = slices.Clone(r.Detail.TemplateCatalog)
		row.Detail.GitOpsSources = slices.Clone(r.Detail.GitOpsSources)
		page.Events = append(page.Events, row)
	}
	return page, nil
}
