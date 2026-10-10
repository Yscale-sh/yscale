// Package state holds the central server's data: customers, connected
// agents, in-flight workloads, and burst-node tracking.
//
// Storage is in-memory maps with an optional durability backend (the
// `persister` interface, see persist.go). The working set lives in memory so
// the hot path stays plain map lookups; each mutation write-throughs the one
// changed record to the backend so a process restart never strands a running
// burst or forgets a customer. New() is pure in-memory (tests, OSS-local);
// NewPostgres() (postgres.go) is the durable backend — it replaced the original
// JSON-snapshot store. The backend stores each entity as a JSONB row, so it
// stays in lockstep with these structs with no per-column mapping.
//
// Connected agents are deliberately NOT durable: an Agent holds a live WS
// send channel and is meaningless without its connection, so agents
// re-register via AddAgent on reconnect rather than being restored from disk.
package state

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// ErrNotFound is returned when a lookup misses.
var ErrNotFound = errors.New("not found")

// ErrAmbiguousCluster is returned when a caller asked for "this customer's
// connector" and the customer has connectors for more than one cluster. There
// is no right answer to pick: one of them runs the job, and the caller is the
// only party that knows which. Every reader of it turns it into "name the
// cluster" rather than a choice made by map order.
var ErrAmbiguousCluster = errors.New("customer has connected agents for more than one cluster")

// ErrInvalidClusterPolicy rejects a tenant cluster-placement policy outside the
// closed policy grammar.
var ErrInvalidClusterPolicy = errors.New("invalid cluster policy")

// ErrInvalidTenantLimits rejects guardrails whose stored semantics would be
// ambiguous or unusable. Zero is valid and means unlimited.
var ErrInvalidTenantLimits = errors.New("invalid tenant limits")

// MeshEndpoint describes a customer's self-hosted coordination-server (mesh)
// configuration. nil on a Customer means the shared Tailscale SaaS path (the
// default); a non-nil value points the customer's bursts/agents/gateways at
// their own coordination server via LoginServer.
// MeshProviderFactory marks a tenant box provisioned and credentialed by the
// factory. Central stores no admin key for it; every mesh operation is a
// factory RPC keyed by tenant ID.
const MeshProviderFactory = "factory"

type MeshEndpoint struct {
	Provider         string // provider tag: "tailscale" | "box" | MeshProviderFactory
	LoginServer      string // https box URL ("" for SaaS)
	APIKey           string `json:"-"` // plaintext exists only in the live working set
	APIKeyCiphertext string `json:",omitempty"`
	User             string // coordination-server user/namespace for this customer
	BackendID        string // the self-hosted box's provider VM id, for teardown
}

// UnmarshalJSON accepts the historical plaintext APIKey long enough for the
// Postgres startup migration to encrypt and rewrite it. Marshal never emits it.
func (m *MeshEndpoint) UnmarshalJSON(data []byte) error {
	type persisted MeshEndpoint
	var wire struct {
		persisted
		LegacyAPIKey string `json:"APIKey"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*m = MeshEndpoint(wire.persisted)
	m.APIKey = wire.LegacyAPIKey
	return nil
}

// Customer is a tenant/auth subject. The plaintext bearer is retained only
// for the request that minted it and for in-memory dev stores. Durable rows
// carry its one-way verifier, and authentication hashes the presented token.
type Customer struct {
	// Exact durable document this working copy came from. Never serialized or
	// shared by pointer: a stale snapshot must retain its own write precondition.
	persistedData string

	ID        string // e.g. "cust_abc123"
	Token     string `json:"-"` // bearer token customers put in YSCALE_TOKEN
	TokenHash string `json:",omitempty"`
	Email     string
	Plan      string // "free" | "pro" | "enterprise" | "trial" (self-service)

	// LinodeCloudAccount is this tenant's one active BYOC Linode account. Its
	// credential is an authenticated ciphertext envelope, never plaintext.
	// nil preserves the legacy platform-funded backend path.
	LinodeCloudAccount *CloudAccount `json:",omitempty"`

	// RuntimeBindings are tenant-owned environment values encrypted by central
	// and synced to the fixed yscale-runtime-bindings Secret in authorized
	// workload namespaces. The ciphertext is never rendered by public handlers.
	RuntimeBindings []*RuntimeBinding `json:",omitempty"`
	// RuntimeBindingsRevision is the tenant-wide desired-state generation. It
	// survives deletion of the last binding so an older in-flight sync can
	// never restore data that was already removed.
	RuntimeBindingsRevision int64 `json:",omitempty"`

	// Name is the tenant's human-chosen display name. Additive: empty for every
	// tenant that predates it and for operator-provisioned tenants, whose id is
	// what the operator knows them by. Display only — nothing is keyed or
	// authorized on it.
	Name string `json:",omitempty"`

	// HostedCapacityRequestedAt is the durable queue marker for a tenant's
	// request to use Yscale Shared capacity. nil means no pending request. An
	// existing hosted RegisteredCluster is the authoritative assigned state,
	// even if an older snapshot still carries this additive field.
	HostedCapacityRequestedAt *time.Time `json:",omitempty"`

	// RevokedAt marks a customer whose access is gone but whose record is
	// deliberately still here. Offboard revokes durably FIRST and only then
	// tears down what the tenant owned, so a crash or a half-finished reap
	// leaves an operator a record to retry the cleanup against instead of cloud
	// resources with no admin path back. A revoked customer never resolves by
	// token — not in this process and not after a restart — and its row is
	// removed for good only once cleanup verifies clean. nil = active, which is
	// every customer that predates offboard.
	RevokedAt *time.Time `json:",omitempty"`

	// Mesh is the per-customer coordination-server endpoint. nil = shared
	// Tailscale SaaS (the default, backward-compatible path).
	Mesh *MeshEndpoint

	// The tenant-wide POLICY view: the canonical UNION of every cluster's
	// reported routes, and the last union successfully pushed to the
	// coordination server. Policy is a tenant-scoped document, so a union is
	// the right desired state for it — and for a customer whose by-cluster map
	// predates this field (or is still empty because no cluster has reported
	// since the upgrade), GatewayRoutes is the only route intent there is, so
	// boot can still re-assert a policy from it.
	GatewayRoutes       []string `json:",omitempty"`
	PushedGatewayRoutes []string `json:",omitempty"`

	// The per-cluster view, keyed by cluster id: the EXACT canonical set that
	// cluster's gateway advertises, and the exact set last approved on that
	// cluster's gateway node. A dedicated yscale-gateway-<cluster> node must
	// carry its own cluster's routes and nothing else, so the tenant-wide
	// union above is the wrong desired state for it — approving the union
	// would put cluster A's CIDRs on cluster B's gateway.
	//
	// Additive: nil for every customer persisted before these fields existed.
	// Nothing infers a by-cluster entry from GatewayRoutes, because which
	// cluster reported a legacy route is not recorded anywhere and guessing
	// picks a gateway. A live cluster's next non-empty report — the agent
	// sends one right after connecting — establishes its own entry.
	//
	// Treated as copy-on-write by every writer: CustomerByID hands out the
	// live record, so mutating a stored map in place would race a reader
	// ranging it.
	GatewayRoutesByCluster       map[string][]string `json:",omitempty"`
	PushedGatewayRoutesByCluster map[string][]string `json:",omitempty"`

	// Per-tenant spend guardrails enforced at burst admission. 0 = unlimited
	// (the default for existing/seeded customers, preserving prior behavior);
	// provisioned pilot tenants get non-zero defaults. MaxConcurrentBursts caps
	// how many bursts can run at once; MaxHourlyUSD caps their aggregate
	// upstream burn rate ($/hr), which — with each burst's deadline — bounds the
	// daily bill without a spend ledger.
	MaxConcurrentBursts int     `json:",omitempty"`
	MaxHourlyUSD        float64 `json:",omitempty"`

	// WorkloadNamespaces is the set of Kubernetes namespaces this tenant
	// may submit workloads into, and the first entry is the one an
	// unqualified submission lands in. It exists because a workload's
	// metadata.namespace decides which namespace the connector reads
	// bucket-credentials Secrets from: left as submitter input, one team
	// on a shared cluster could name another team's namespace. Empty
	// (every tenant that predates the field) means the fail-closed set
	// — "default" alone. Mirror the connector's rbac.allowedNamespaces
	// here; the connector's RBAC is the second, independent gate.
	WorkloadNamespaces []string `json:",omitempty"`

	// ClusterPolicy is the tenant-owned placement policy. nil preserves the
	// pre-policy routing contract exactly; a non-nil policy gates explicit
	// cluster pins and enables deterministic automode when Auto is "ordered".
	ClusterPolicy *ClusterPolicy `json:",omitempty"`

	// TemplateCatalog is the tenant-owned launch catalog. nil — every tenant
	// that predates the field — inherits the server's own defaults and keeps
	// tracking them as they move; a published catalog is the tenant's and moves
	// only when they replace it. See WorkloadTemplateCatalog for why the
	// distinction is a pointer.
	TemplateCatalog *WorkloadTemplateCatalog `json:",omitempty"`

	// CatalogPublishers are tenant-scoped automation principals permitted only
	// to read and replace TemplateCatalog. Rows persist only credential digests;
	// public handlers render CatalogPublisherSummary instead.
	CatalogPublishers []*CatalogPublisher `json:",omitempty"`

	// GitOpsSources is the tenant's durable registry of Git repositories its
	// clusters reconcile. Configuration coordinates only — no credential rides
	// here, and there is no field for one. Empty for every tenant that has not
	// registered a source, which is the same registry as one that removed its
	// last: there is no server-owned default to inherit, so the pointer
	// distinction TemplateCatalog needs does not exist here. See gitops.go.
	GitOpsSources []GitOpsSource `json:",omitempty"`

	// RegisteredClusters is the tenant's durable cluster registry: the fleet
	// the tenant OWNS, as distinct from the connectors this replica happens to
	// hold a socket for. Bounded at MaxRegisteredClusters; rows carry the
	// SHA-256 of each cluster's connector credential and never the credential
	// itself. Empty for every tenant that predates the registry — their
	// connectors self-announce and are claimed in on first connection. See
	// clusters.go.
	RegisteredClusters []*RegisteredCluster `json:",omitempty"`
}

// UnmarshalJSON accepts historical Customer.Token rows so startup can replace
// them with TokenHash before serving traffic. A row carrying both forms must
// agree; otherwise startup refuses a document whose authentication meaning is
// ambiguous.
func (c *Customer) UnmarshalJSON(data []byte) error {
	type persisted Customer
	var wire struct {
		persisted
		LegacyToken string `json:"Token"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*c = Customer(wire.persisted)
	if wire.LegacyToken == "" {
		return nil
	}
	hash := HashCustomerToken(wire.LegacyToken)
	if c.TokenHash != "" && c.TokenHash != hash {
		return errors.New("customer token and verifier do not match")
	}
	c.Token = wire.LegacyToken
	c.TokenHash = hash
	return nil
}

// HashCustomerToken is the persisted verifier for a high-entropy tenant bearer
// token. Domain separation keeps this index distinct from the other credential
// hashes stored in the same customer document.
func HashCustomerToken(token string) string {
	sum := sha256.Sum256([]byte("yscale-customer-token-v1\x00" + token))
	return hex.EncodeToString(sum[:])
}

// NewCustomerToken mints the one-time plaintext form of a tenant bearer. The
// store persists only HashCustomerToken(token); callers must return the token
// directly to the operator and then forget it.
func NewCustomerToken() string {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("state: crypto/rand read failed while minting customer token: " + err.Error())
	}
	return "ysk_" + hex.EncodeToString(b[:])
}

func ensureCustomerTokenHash(c *Customer) {
	if c != nil && c.TokenHash == "" && c.Token != "" {
		c.TokenHash = HashCustomerToken(c.Token)
	}
}

const (
	ClusterPolicyAutoRequirePin = "require_pin"
	ClusterPolicyAutoOrdered    = "ordered"

	ClusterPlacementModePinned = "pinned"
	ClusterPlacementModeAuto   = "auto"

	clusterPolicyRule        = "tenant.cluster_policy"
	clusterPolicyRuleVersion = "v1"

	tenantLimitsRule        = "tenant.limits_set"
	tenantLimitsRuleVersion = "v1"
)

const (
	DefaultOperatorTenantLimit = 50
	MaxOperatorTenantLimit     = 100
)

// Keep this exactly aligned with the same-origin proxy's pathSegmentRe. A
// policy must never accept an id the console cannot send in X-Cluster-ID, nor
// reject an existing id the proxy already accepts.
var clusterIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ClusterPolicy is the tenant's cluster placement rule. Allow is ordered:
// automode uses that order first, then falls back to cluster-id order for
// eligible clusters not named there. Deny always wins over allow.
type ClusterPolicy struct {
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
	Auto  string   `json:"auto,omitempty"`
}

// Effective returns a normalized copy with the default automode materialized.
func (p *ClusterPolicy) Effective() ClusterPolicy {
	if p == nil {
		return ClusterPolicy{Auto: ClusterPolicyAutoRequirePin}
	}
	out := ClusterPolicy{
		Allow: append([]string(nil), p.Allow...),
		Deny:  append([]string(nil), p.Deny...),
		Auto:  p.Auto,
	}
	if out.Auto == "" {
		out.Auto = ClusterPolicyAutoRequirePin
	}
	return out
}

func (p *ClusterPolicy) normalized() *ClusterPolicy {
	effective := p.Effective()
	if len(effective.Allow) == 0 && len(effective.Deny) == 0 && effective.Auto == ClusterPolicyAutoRequirePin {
		return nil
	}
	return &effective
}

// ValidClusterID reports whether id is acceptable in durable policy. It is
// intentionally stricter than "non-empty" but still accepts existing operator
// supplied ids used by tests and older connectors.
func ValidClusterID(id string) bool {
	return clusterIDPattern.MatchString(id)
}

// ValidateClusterPolicy normalizes and validates a replacement policy.
func ValidateClusterPolicy(p ClusterPolicy) (*ClusterPolicy, error) {
	effective := (&p).Effective()
	if effective.Auto != ClusterPolicyAutoRequirePin && effective.Auto != ClusterPolicyAutoOrdered {
		return nil, fmt.Errorf("%w: auto must be %q or %q", ErrInvalidClusterPolicy, ClusterPolicyAutoRequirePin, ClusterPolicyAutoOrdered)
	}
	if len(effective.Allow)+len(effective.Deny) > 64 {
		return nil, fmt.Errorf("%w: policy may contain at most 64 cluster ids", ErrInvalidClusterPolicy)
	}
	seenAllow := make(map[string]bool, len(effective.Allow))
	for _, id := range effective.Allow {
		if !ValidClusterID(id) {
			return nil, fmt.Errorf("%w: invalid allow cluster id %q", ErrInvalidClusterPolicy, id)
		}
		if seenAllow[id] {
			return nil, fmt.Errorf("%w: duplicate allow cluster id %q", ErrInvalidClusterPolicy, id)
		}
		seenAllow[id] = true
	}
	seenDeny := make(map[string]bool, len(effective.Deny))
	for _, id := range effective.Deny {
		if !ValidClusterID(id) {
			return nil, fmt.Errorf("%w: invalid deny cluster id %q", ErrInvalidClusterPolicy, id)
		}
		if seenDeny[id] {
			return nil, fmt.Errorf("%w: duplicate deny cluster id %q", ErrInvalidClusterPolicy, id)
		}
		if seenAllow[id] {
			return nil, fmt.Errorf("%w: cluster id %q appears in both allow and deny", ErrInvalidClusterPolicy, id)
		}
		seenDeny[id] = true
	}
	return effective.normalized(), nil
}

func sameClusterPolicy(a, b *ClusterPolicy) bool {
	an, bn := a.normalized(), b.normalized()
	if an == nil || bn == nil {
		return an == nil && bn == nil
	}
	return an.Auto == bn.Auto && slices.Equal(an.Allow, bn.Allow) && slices.Equal(an.Deny, bn.Deny)
}

func copyClusterPolicy(p *ClusterPolicy) *ClusterPolicy {
	if p == nil {
		return nil
	}
	out := p.Effective()
	return &out
}

// HasMeshBox reports whether the customer has opted into a self-hosted
// coordination server (rather than the shared Tailscale SaaS path).
// Safe on a nil receiver so callers can use it on a tolerant lookup result.
func (c *Customer) HasMeshBox() bool {
	return c != nil && c.Mesh != nil
}

// Revoked reports whether the customer's access has been revoked. Safe on a nil
// receiver for the same reason HasMeshBox is.
func (c *Customer) Revoked() bool {
	return c != nil && c.RevokedAt != nil
}

// Account is a HUMAN SaaS identity — the person who signs in to the dashboard,
// as distinct from Customer, which is the tenant subject an agent
// authenticates as with Customer.Token. The two are deliberately separate: a
// human never holds a cluster token, and a cluster token never identifies a
// human.
//
// Authentication is delegated to Yscale ID; central stores no password. The
// durable identity key is the (Issuer, Subject) pair, never Email — email is
// mutable, reassignable, and not unique across issuers, so keying on it would
// let a re-used address inherit another human's tenants. Email/Name/
// EmailVerified are profile fields refreshed from the issuer on each sign-in.
// They never replace the identity key or membership authorization; verified
// email is used for explicit member lookup and first-workspace eligibility.
type Account struct {
	ID      string // "acct_<24 hex>", derived from (Issuer, Subject) — see accountID
	Issuer  string // Yscale ID issuer URL (YSCALE_ID_ISSUER)
	Subject string // Yscale ID `sub` — stable and opaque per issuer

	Email         string `json:",omitempty"`
	EmailVerified bool   `json:",omitempty"`
	Name          string `json:",omitempty"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

// AccountProfile is the mutable, issuer-supplied part of an Account. Split out
// so UpsertAccount's signature keeps the identity key (issuer, subject)
// separate from data the caller is merely refreshing.
type AccountProfile struct {
	Email         string
	EmailVerified bool
	Name          string
}

// TenantMembership binds one Account to one Customer with a role. It is the
// only thing that grants a human any visibility into a tenant; absence of a
// membership is absence of access.
type TenantMembership struct {
	ID         string // "mbr_<24 hex>", derived from (AccountID, CustomerID)
	AccountID  string
	CustomerID string
	Role       string // one of the Role* constants below
	CreatedAt  time.Time
}

// Membership roles. The set is closed and validated on write — an unrecognised
// role must never reach durable state, because every later authorization
// decision reads it back and an unknown value has no defined meaning.
const (
	RoleOwner  = "owner"
	RoleAdmin  = "admin"
	RoleMember = "member"
	RoleViewer = "viewer"
)

var validRoles = map[string]bool{
	RoleOwner: true, RoleAdmin: true, RoleMember: true, RoleViewer: true,
}

// ValidRole reports whether role is one of the recognised membership roles.
func ValidRole(role string) bool { return validRoles[role] }

var (
	// ErrInvalidIdentity rejects an empty/blank (issuer, subject) pair. A blank
	// key would collapse every unauthenticated caller onto one account.
	ErrInvalidIdentity = errors.New("invalid external identity")
	// ErrInvalidRole rejects a role outside the closed set above.
	ErrInvalidRole = errors.New("invalid membership role")
	// ErrRoleConflict rejects re-binding an existing membership to a different
	// role. Re-adding the SAME role is idempotent; silently overwriting a
	// different one would let a re-provision quietly demote an owner.
	ErrRoleConflict = errors.New("membership already exists with a different role")
	// ErrNotAuthorized means the caller holds a membership on the tenant but the
	// role on it does not permit the operation. Distinct from ErrNotFound, which
	// is what a caller with no membership at all gets: telling the two apart is
	// the whole point — a member who may not manage the roster already knows the
	// tenant exists, and a caller with no grant must not learn that it does.
	ErrNotAuthorized = errors.New("membership does not authorize this operation")
	// ErrOwnerProtected is the one refusal where the CALLER may manage the
	// roster and the TARGET is what stops them: only an owner may remove an
	// owner. It wraps ErrNotAuthorized, so anything matching on that keeps
	// matching, and a handler that matches this FIRST can say something true to
	// an admin — telling one that the operation "requires owner or admin" when
	// they are an admin is a message that sends them looking for a permission
	// they already hold.
	ErrOwnerProtected = fmt.Errorf("%w: only an owner may remove an owner", ErrNotAuthorized)
	// ErrOwnerRestricted is ErrOwnerProtected's twin on the grant and role-change
	// paths: the caller manages the roster and the OWNER role is what stops them.
	// An admin may hand out and change admin/member/viewer, but may not mint an
	// owner, promote anyone to owner, or touch an existing one — otherwise the
	// role floor is decorative, since any admin could promote themselves and then
	// remove the owners. It wraps ErrNotAuthorized like ErrOwnerProtected, and is
	// separate from it because "only an owner may remove an owner" is false here.
	ErrOwnerRestricted = fmt.Errorf("%w: only an owner may grant or change the owner role", ErrNotAuthorized)
	// ErrTargetNotFound is the roster-management answer for a target human the
	// tenant cannot act on: an email with no unique VERIFIED account behind it,
	// or an account id holding no grant on this tenant. The three email misses —
	// no account, an unverified one, and more than one match — collapse into it
	// deliberately, so an authorized manager cannot use the route to probe which
	// addresses have accounts on this central. It wraps ErrNotFound, so a caller
	// matching only that still answers 404.
	ErrTargetNotFound = fmt.Errorf("%w: no such target account", ErrNotFound)
	// ErrLastOwner rejects removing a tenant's only owner. A tenant with no owner
	// is unrecoverable through this surface: nothing else can grant a role, so
	// the remaining members would be locked out of their own roster forever.
	// Like ErrRoleConflict it is a conflict, not a fault — handlers answer 409.
	ErrLastOwner = errors.New("tenant must keep at least one owner")
	// ErrPersistence means the durable backend refused a write the caller may
	// not proceed without, and the in-memory state was left as it was. It is
	// distinct from the identity/role errors above because the caller did
	// nothing wrong: handlers map it to 503 (retryable), never to a 4xx.
	ErrPersistence = errors.New("durable write failed")
	// ErrCustomerExists rejects creating a customer id that is already taken —
	// by a live tenant or by a revoked one still awaiting cleanup. Deliberately
	// NOT an ErrPersistence: the backend is healthy and the request conflicts,
	// so handlers answer 409, not 503.
	ErrCustomerExists = errors.New("customer already exists")
	// ErrAccountHasTenant rejects first-workspace creation once the account has
	// any membership. The durable create checks this in the same transaction as
	// the tenant and owner insert, so a concurrent roster grant cannot turn a
	// zero-tenant signup into a second workspace.
	ErrAccountHasTenant = errors.New("account already belongs to a tenant")
	// ErrAccountEmailUnverified is a first-workspace authorization refusal,
	// not a database outage. Operator-created tenants do not require it.
	ErrAccountEmailUnverified = errors.New("creating a workspace requires a verified email")
	// ErrCustomerActive rejects finalizing the deletion of a customer that was
	// never revoked. The final delete is the last step of an offboard, and
	// running it on a live tenant would drop a working tenant's row without
	// ever cutting its access first.
	ErrCustomerActive = errors.New("customer is not revoked")
	// ErrInvalidAudit rejects an audit row outside the closed action/outcome/
	// target/actor sets, or a malformed read cursor. Like ErrInvalidRole it is
	// the caller's mistake and never a backend fault: the journal cannot be
	// updated after the fact, so a row with an unreadable field is evidence
	// nobody can use.
	ErrInvalidAudit = errors.New("invalid audit event")
	// ErrCustomerTombstoned means the durable backend refused a customer write
	// because a finished offboard permanently retired that id. Like
	// ErrCustomerExists it is a conflict, not a backend fault — and it is the
	// state ErrCustomerExists cannot see, because the row whose primary key
	// would have refused the write is exactly what the offboard deleted.
	// Creates surface it AS ErrCustomerExists (the caller's answer is the same
	// 409: pick another id); the whole-document upsert path returns it so a
	// refusal is visible in the log instead of looking like a dropped write.
	// Nothing clears a tombstone: a tenant id is an identity and security
	// boundary, and ids are generated rather than chosen, so no legitimate
	// caller needs one back.
	ErrCustomerTombstoned = errors.New("customer id is permanently retired")
)

// accountID derives an Account's durable primary key from its identity pair,
// and membershipID a membership's from (account, customer). Both are
// deterministic on purpose: the key IS the uniqueness mechanism, exactly as
// pod_slots.slot is. Two central replicas upserting the same identity write the
// same row rather than minting two ids for one human and splitting their
// memberships across both.
func accountID(issuer, subject string) string {
	return "acct_" + hashKey(issuer, subject)
}

func membershipID(accountID, customerID string) string {
	return "mbr_" + hashKey(accountID, customerID)
}

// SelfServiceTenantID derives the one tenant id a self-service signup may
// create for an account. Deterministic (SHA-256, like accountID and
// membershipID) on purpose: two concurrent requests — or two replicas — creating
// a first workspace for the same human derive the same primary key, so the
// durable create's own uniqueness check collides them onto one tenant instead
// of quietly buying two. The fixed prefix separates this derivation's domain
// from membershipID's, which hashes the same account id.
func SelfServiceTenantID(accountID string) string {
	return "cust_" + hashKey("self-service-tenant", accountID)
}

// hashKey joins its parts with a separator that cannot appear in either (so
// ("ab","c") and ("a","bc") differ) and truncates to 96 bits — collision-proof
// for identity keys, and short enough to read in a log line.
func hashKey(a, b string) string {
	sum := sha256.Sum256([]byte(a + "\x00" + b))
	return hex.EncodeToString(sum[:12])
}

// Agent is one connected cluster agent. Created on WS handshake,
// removed when the connection closes.
type Agent struct {
	ID         string // e.g. "agent_xyz789"
	CustomerID string
	ClusterID  string
	// WorkloadNamespace is the connector's declared single namespace scope.
	// Empty means not namespace-pinned.
	WorkloadNamespace string

	// AgentVersion is the build string the connector sent in its Hello. Empty
	// for a connector too old to send one. Reported on the tenant's cluster
	// list, which is the only place an operator can see what is actually
	// running in a customer's cluster.
	AgentVersion string

	// AuthoritativeOccupancy is the capability this connector claimed in its
	// Hello: that it can read the whole cluster's pods and so can report a
	// nodeOnly burst's node idle. It is live-connection state, not durable — a
	// reconnect restates it, and a connector too old to send it is false.
	//
	// Admission is what reads it. A nodeOnly burst placed on a connector that
	// cannot make the claim has no teardown signal coming at all, so it is given
	// a finite deadline instead of an observation ceiling it can never satisfy.
	AuthoritativeOccupancy bool

	ConnectedAt time.Time

	// Send is the agent's outbound message queue. Server writes into this;
	// the WS write loop drains it. Buffered so slow agents don't block
	// the server's reconcile loop.
	Send chan protocol.Envelope

	// lastSeen is the liveness stamp, behind seenMu rather than an exported
	// field: the WS read pump writes it on every frame from the connection's own
	// goroutine, and the tenant cluster list reads it from a request goroutine
	// that holds the store lock — which the pump does not. Use MarkSeen/SeenAt.
	seenMu   sync.Mutex
	lastSeen time.Time

	inventoryMu       sync.Mutex
	inventory         *ClusterInventorySnapshot
	heartbeatWarnMu   sync.Mutex
	lastHeartbeatWarn time.Time

	// ackWaiters correlate asynchronous command acknowledgements without
	// exposing channels in persisted/public state. Teardown jobs remain durable
	// in Redis until their waiter receives a successful ACK; a central restart
	// loses only this in-memory waiter, not the teardown job.
	ackMu      sync.Mutex
	ackWaiters map[string]chan protocol.CommandAck
	// commandLeases binds a durable command claim to the exact envelope in Send.
	// The write pump consumes the token after the WebSocket write so it can
	// fence delivered/ambiguous state without changing the public envelope.
	commandLeaseMu      sync.Mutex
	commandLeases       map[string]string
	commandAckAuditMu   sync.Mutex
	lastCommandAckAudit time.Time

	// evict is the tear-down signal: closed exactly once when this agent is
	// removed from routing — a rotation or delete revoking its socket, or the
	// normal disconnect path. Send is deliberately NEVER closed: request
	// goroutines enqueue concurrently with an eviction, and a send on a closed
	// channel panics where a send to an abandoned one is just dropped. Lazily
	// created behind evictMu because Agents are built as struct literals.
	evictMu sync.Mutex
	evict   chan struct{}
}

// ClusterInventorySnapshot is the newest cluster inventory this live socket
// reported. It is replica-local and intentionally not persisted.
type ClusterInventorySnapshot struct {
	ObservedAt              time.Time
	NodeInventoryObserved   bool
	NodeInventoryObservedAt time.Time
	PodInventoryObserved    bool
	PodInventoryObservedAt  time.Time
	NodeCount               int
	BurstCount              int
	PendingPods             int
}

// ClusterInventoryUpdate applies both independently validated scopes under one
// lock. A Valid=false scope is left unchanged; Valid=true and Observed=false
// clears only that scope.
type ClusterInventoryUpdate struct {
	NodeValid      bool
	NodeObserved   bool
	NodeObservedAt time.Time
	NodeCount      int
	BurstCount     int
	PodValid       bool
	PodObserved    bool
	PodObservedAt  time.Time
	PendingPods    int
}

// evictCh returns the eviction channel, creating it on first use.
func (a *Agent) evictCh() chan struct{} {
	a.evictMu.Lock()
	defer a.evictMu.Unlock()
	if a.evict == nil {
		a.evict = make(chan struct{})
	}
	return a.evict
}

// Evict signals the agent's pumps to stop. Idempotent, non-blocking, and safe
// to call under the store lock — it only closes a channel.
func (a *Agent) Evict() {
	a.evictMu.Lock()
	defer a.evictMu.Unlock()
	if a.evict == nil {
		a.evict = make(chan struct{})
	}
	select {
	case <-a.evict:
	default:
		close(a.evict)
	}
}

// Evicted is the tear-down signal the WS write pump selects on. Closed once
// the agent has been removed from routing.
func (a *Agent) Evicted() <-chan struct{} {
	return a.evictCh()
}

// Enqueue queues one envelope for the write pump. It never blocks and it
// cannot panic: Send is never closed, and evictMu makes the eviction check and
// non-blocking send one operation with respect to Evict. Once Evict returns,
// no later enqueue can place work on the abandoned queue.
func (a *Agent) Enqueue(env protocol.Envelope) error {
	a.evictMu.Lock()
	defer a.evictMu.Unlock()
	if a.evict == nil {
		a.evict = make(chan struct{})
	}
	select {
	case <-a.evict:
		return fmt.Errorf("agent %s is no longer connected", a.ID)
	default:
	}
	select {
	case a.Send <- env:
		return nil
	default:
		return fmt.Errorf("agent %s send queue full", a.ID)
	}
}

// MarkSeen records that a frame arrived from this agent.
func (a *Agent) MarkSeen(t time.Time) {
	a.seenMu.Lock()
	a.lastSeen = t
	a.seenMu.Unlock()
}

// SeenAt reports the last frame's arrival. Zero for an agent that has sent
// nothing since its Hello.
func (a *Agent) SeenAt() time.Time {
	a.seenMu.Lock()
	defer a.seenMu.Unlock()
	return a.lastSeen
}

func (a *Agent) ApplyClusterInventory(update ClusterInventoryUpdate) {
	a.inventoryMu.Lock()
	if a.inventory == nil && ((update.NodeValid && update.NodeObserved) || (update.PodValid && update.PodObserved)) {
		a.inventory = &ClusterInventorySnapshot{}
	}
	if a.inventory != nil {
		if update.NodeValid {
			a.inventory.NodeInventoryObserved = update.NodeObserved
			if update.NodeObserved {
				a.inventory.NodeInventoryObservedAt = update.NodeObservedAt
				a.inventory.NodeCount = update.NodeCount
				a.inventory.BurstCount = update.BurstCount
			} else {
				a.inventory.NodeInventoryObservedAt = time.Time{}
				a.inventory.NodeCount = 0
				a.inventory.BurstCount = 0
			}
		}
		if update.PodValid {
			a.inventory.PodInventoryObserved = update.PodObserved
			if update.PodObserved {
				a.inventory.PodInventoryObservedAt = update.PodObservedAt
				a.inventory.PendingPods = update.PendingPods
			} else {
				a.inventory.PodInventoryObservedAt = time.Time{}
				a.inventory.PendingPods = 0
			}
		}
		normalizeClusterInventorySnapshot(a.inventory)
		if !a.inventory.NodeInventoryObserved && !a.inventory.PodInventoryObserved {
			a.inventory = nil
		}
	}
	a.inventoryMu.Unlock()
}

// AllowHeartbeatWarning rate-limits authenticated remote-driven validation
// warnings for this socket.
func (a *Agent) AllowHeartbeatWarning(now time.Time, every time.Duration) bool {
	a.heartbeatWarnMu.Lock()
	defer a.heartbeatWarnMu.Unlock()
	if !a.lastHeartbeatWarn.IsZero() && now.Sub(a.lastHeartbeatWarn) < every {
		return false
	}
	a.lastHeartbeatWarn = now
	return true
}

func (a *Agent) ClusterInventory() (ClusterInventorySnapshot, bool) {
	a.inventoryMu.Lock()
	defer a.inventoryMu.Unlock()
	if a.inventory == nil {
		return ClusterInventorySnapshot{}, false
	}
	return *a.inventory, true
}

func normalizeClusterInventorySnapshot(snapshot *ClusterInventorySnapshot) {
	if snapshot == nil {
		return
	}
	if snapshot.NodeInventoryObserved && snapshot.NodeInventoryObservedAt.IsZero() {
		snapshot.NodeInventoryObservedAt = snapshot.ObservedAt
	}
	if snapshot.PodInventoryObserved && snapshot.PodInventoryObservedAt.IsZero() {
		snapshot.PodInventoryObservedAt = snapshot.ObservedAt
	}
	snapshot.ObservedAt = time.Time{}
	if snapshot.NodeInventoryObserved {
		snapshot.ObservedAt = snapshot.NodeInventoryObservedAt
	}
	if snapshot.PodInventoryObserved && snapshot.PodInventoryObservedAt.After(snapshot.ObservedAt) {
		snapshot.ObservedAt = snapshot.PodInventoryObservedAt
	}
}

// RegisterCommandAck installs a one-shot waiter before a command is enqueued.
// The returned cleanup must always be called so a timed-out command cannot leak
// its waiter for the lifetime of the agent connection.
func (a *Agent) RegisterCommandAck(commandID string) (<-chan protocol.CommandAck, func()) {
	a.ackMu.Lock()
	if a.ackWaiters == nil {
		a.ackWaiters = make(map[string]chan protocol.CommandAck)
	}
	ch := make(chan protocol.CommandAck, 1)
	a.ackWaiters[commandID] = ch
	a.ackMu.Unlock()
	return ch, func() {
		a.ackMu.Lock()
		delete(a.ackWaiters, commandID)
		a.ackMu.Unlock()
	}
}

// DeliverCommandAck resolves a registered waiter. It is deliberately
// non-blocking: a duplicate/late ACK must never wedge the WebSocket read pump.
func (a *Agent) DeliverCommandAck(ack protocol.CommandAck) bool {
	a.ackMu.Lock()
	ch := a.ackWaiters[ack.CommandID]
	if ch != nil {
		delete(a.ackWaiters, ack.CommandID)
	}
	a.ackMu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- ack:
	default:
	}
	return true
}

// Workload is one user-submitted job. Lifecycle: Pending → Provisioning
// → Running → Succeeded/Failed/Cancelled.
type Workload struct {
	ID         string
	CustomerID string
	AgentID    string // resolved when scheduled
	// ClusterID is the stable cluster the workload was dispatched to. Unlike
	// AgentID it survives connector reconnects and is safe to show in history.
	// Empty on records written before multi-cluster routing was recorded.
	ClusterID  string `json:",omitempty"`
	Status     string
	BurstID    string
	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time

	// Spec is opaque to state; the API layer parses it.
	SpecYAML []byte

	// SubmittedBy is the principal that submitted this workload, stamped once
	// at submission and never rewritten — provenance that changed later would
	// not be provenance. It is what AuthorizeWorkloadCancel compares a member's
	// account against.
	//
	// nil is a LEGACY record: every workload that predates this field, and
	// every one a store with no journal wrote. Those stay fully readable and
	// fully cancellable by an owner, an admin and the cluster credential —
	// only the member-cancels-their-own rule needs the field, and it fails
	// closed without it.
	SubmittedBy *Actor `json:",omitempty"`
	// Authorization is the bounded decision record that admitted the
	// submission: the namespace asked for, the namespace granted, and the rule
	// and version that said so. nil on the same legacy records.
	Authorization *WorkloadAuthorization `json:",omitempty"`
	// Placement is the cluster policy/routing decision that admitted the
	// submission. nil on legacy records and on records written before policy
	// governance existed.
	Placement *WorkloadPlacement `json:",omitempty"`
	// Cost is the frozen observation of what this workload's burst cost
	// upstream, written once by the reap that tore it down. nil means NO
	// OBSERVATION — a legacy record, a workload still running, or a reap
	// attempt that failed and re-queued the burst. A pointer for exactly that
	// reason: a zero-valued cost object rendered on those records would read as
	// "this ran for free", which is a different claim from "nobody measured it".
	Cost *WorkloadCost `json:",omitempty"`
	// RetryOfWorkloadID names the terminal workload this one was cloned from.
	// Empty means this record was an original submit or predates the retry
	// feature.
	RetryOfWorkloadID string `json:",omitempty"`
	// TemplateRef is the tenant catalog entry this submission was launched
	// from, stamped once at submission and never rewritten — provenance that
	// changed later would not be provenance. nil is a submission that named no
	// template: the raw API, a connector, and every record that predates the
	// catalog. It is NOT a claim that the workload matches a template today,
	// only that this entry is what admitted it.
	TemplateRef *TemplateRef `json:",omitempty"`
	// Outcome is the agent's receipt for the terminal observation that finished
	// this workload: what the compute did, and separately what the artifact
	// export did. nil is NO RECEIPT — a legacy record, a run still going, or an
	// agent that reported the terminal phase and nothing else. See
	// WorkloadOutcome for why it is never fabricated from the status.
	Outcome *WorkloadOutcome `json:",omitempty"`
	// SubmissionOrigin is which submission path reported this workload, stamped
	// once at submission and never rewritten — provenance that changed later
	// would not be provenance. It is what the connector SAID about itself, held
	// to protocol's closed set at the handler; it is not an authorization input,
	// and nothing reads it back to decide anything.
	//
	// Empty is UNKNOWN: every record written before the field existed, and every
	// one a store restores from a document that has no key for it. Those stay
	// fully readable and are never backfilled — central has no way to learn after
	// the fact which controller asked, and guessing would invent the one thing
	// this field exists to state honestly.
	SubmissionOrigin protocol.SubmissionOrigin `json:",omitempty"`
	// NodeObservation is the last connector-observed Kubernetes Node snapshot for
	// the burst that backed this workload, stamped atomically with the burst's own
	// node-phase update. nil means no connector has reported a persistable phase
	// — a legacy record, a workload whose burst has not registered, or a nodeOnly
	// burst that was never associated with a workload. The snapshot is written to
	// the permanent workload record so it survives burst retirement.
	NodeObservation *NodeObservation `json:",omitempty"`
	// PodObservation is the connector-observed scheduling state of this
	// workload's pod. nil means no connector has reported scheduling — a legacy
	// record, an older connector, or a workload whose pod has not been scheduled.
	PodObservation *PodObservation `json:",omitempty"`
	// GPUObservation is the connector-observed GPU readiness of this workload's
	// burst node. nil means no GPU readiness was observed — CPU-only workloads,
	// legacy records, or a burst whose NVIDIA device plugin has not registered.
	GPUObservation *GPUObservation `json:",omitempty"`
	// SchedulingObservation is the latest connector-observed scheduling state
	// of this workload's pod. Tracks transitions between "Waiting" (with a
	// classified reason) and "Scheduled". nil means no scheduling observation
	// has been made — a legacy record, an older connector, or a pod that has
	// not been observed yet. A "Scheduled" observation is terminal and can
	// never be regressed by a stale negative frame.
	SchedulingObservation *SchedulingObservation `json:",omitempty"`
}

// NodeObservation is a bounded snapshot of the Kubernetes Node state a
// connector observed for a workload's burst. Written once per phase
// transition, never fabricated, and carried on the permanent Workload record
// so it survives process restart and burst retirement.
type NodeObservation struct {
	NodeName          string `json:",omitempty"`
	Phase             string `json:",omitempty"`
	Reason            string `json:",omitempty"`
	ObservedAt        time.Time
	SourceTimestamped bool `json:",omitempty"`
}

// PodObservation is the connector-observed scheduling state of a workload's
// pod. Written when the connector reports PodScheduled=True with a non-empty
// nodeName. Once set, never overwritten — scheduling is a one-shot event.
type PodObservation struct {
	PodName     string    `json:",omitempty"`
	NodeName    string    `json:",omitempty"`
	ScheduledAt time.Time `json:",omitempty"`
}

// GPUObservation is the connector-observed GPU readiness of a workload's burst
// node. Written when the connector reports positive nvidia.com/gpu allocatable
// on the node. Once set, never overwritten. CPU-only workloads never receive one.
type GPUObservation struct {
	AllocatableAt time.Time `json:",omitempty"`
}

// SchedulingObservation is the latest connector-observed scheduling state of a
// workload's pod. "Waiting" carries a classified reason from a bounded allow-list;
// "Scheduled" is terminal and can never be regressed.
type SchedulingObservation struct {
	State      string    `json:",omitempty"` // "Scheduled" | "Waiting"
	Reason     string    `json:",omitempty"` // PascalCase token from allow-list (Waiting only)
	Message    string    `json:",omitempty"` // safe fixed message (Waiting only)
	ObservedAt time.Time `json:",omitempty"`
	// PodName is the Kubernetes Pod the connector observed this scheduling
	// state on. Additive: older records omit it and central leaves it empty.
	// Carried so a replacement pod's observation is not suppressed by the
	// prior pod's stale timestamp.
	PodName string `json:",omitempty"`
}

// Burst tracks a server-provisioned burst node — the backend booking
// behind a Workload. The decider creates one per Plan; cleanup happens
// when the agent reports the burst node has stopped or the workload
// terminates (cleanup wiring lands with the real NodeEvent handlers).
type Burst struct {
	ID         string // matches Plan.BurstID, e.g. "burst_abc123"
	CustomerID string
	AgentID    string
	// ClusterID is the cluster this burst was booked for, recorded at submit so
	// teardown can route the drain to the SAME cluster rather than to whichever
	// of the tenant's connectors answers. Empty on records written before it was
	// recorded; those fall back to the single-connector lookup.
	ClusterID string
	Backend   string // "flyio" | "linode" | "aws"
	BackendID string // backend-assigned VM/pod id
	// Region is the immutable provider routing region used to create this burst.
	// Empty is retained only for legacy bookings that predate region capture.
	Region string `json:",omitempty"`
	// CloudAccountID is the stable tenant cloud-account id used to create this
	// burst. Empty means the legacy platform-funded backend.
	CloudAccountID string `json:",omitempty"`
	NodeName       string // k8s Node name the burst kubelet registers as ("ys-burst-...")
	TSHostname     string // burst's Tailscale device hostname
	PodCIDR        string // per-burst pod /24 (10.244.N.0/24); the allocator derives free slots from live bursts' values
	// MeshProvider/MeshLoginServer record the coordination server that minted
	// this burst. Empty MeshProvider means a legacy record predating these fields.
	MeshProvider    string // "tailscale" | "box" | "" legacy
	MeshLoginServer string // self-hosted coordination-server base URL; "" for shared Tailscale SaaS
	CreatedAt       time.Time
	Status          string  // v0: "provisioning"
	SKU             string  // backend SKU: Fly machine class / EC2 instance type / Linode plan
	HourlyUSD       float64 // upstream per-hour rate; cost meter accrues this x lifetime on reap
	// TerminalCost is a frozen terminal cost observation carried by records
	// written by older releases; the reap path reuses it instead of re-deriving
	// the cost from the burst's lifetime.
	TerminalCost *WorkloadCost `json:",omitempty"`
	// ReapPending marks a burst whose cleanup was durably requested but has not
	// finished. The watchdog retries these independently of workload budgets, so an
	// unbudgeted burst cannot be orphaned after cancellation or a transient
	// provider, receipt, or settlement failure. It is not a provider-delete claim:
	// the existing lifecycle/claim path still elects the cleanup worker.
	ReapPending bool `json:",omitempty"`
	// ReapPendingStatus is the workload terminal status the watchdog must apply
	// if it completes a reap after the original caller has gone away. Explicit
	// cancellation and verified idle teardown use "cancelled"; failures use
	// "failed". A late booking inherits its workload's terminal status.
	// Existing terminal workloads are never overwritten.
	ReapPendingStatus string `json:",omitempty"`

	// Deadline and MaxUSD mirror the workload's Budget so the reaper
	// watchdog can enforce them centrally without re-parsing the spec.
	// Zero means "unset" — no wall-clock / spend cap was declared, so the
	// watchdog leaves the burst alone (only an opt-in global safety net can
	// reap an unbudgeted burst).
	Deadline time.Duration // max wall-clock, from spec.budget.deadline
	MaxUSD   float64       // max accrued spend, from spec.budget.maxUSD

	// NodeOnly indicates the burst was provisioned for a nodeOnly workload.
	// NodeOnly bursts are safe from the global safety-net max lifetime fallback,
	// but are still subject to explicit per-workload budgets (Deadline/MaxUSD).
	NodeOnly bool `json:",omitempty"`

	// GPUUtilPercent is the latest reported GPU utilisation (0–100) from the
	// burst node's nvidia-smi heartbeat. Updated by UpdateBurstGPUTelemetry
	// from the connector's authenticated heartbeat, never from a direct burst
	// report. omitempty so a burst with no GPU telemetry round-trips unchanged.
	GPUUtilPercent float64 `json:",omitempty"`

	// LastHeartbeatAt is the timestamp of the most recent GPU telemetry
	// observation from the burst node. nil until the first sample arrives.
	// Updated atomically with GPUUtilPercent.
	LastHeartbeatAt *time.Time `json:",omitempty"`

	// NodePhase is the last lifecycle phase the customer's connector observed
	// for this burst's Kubernetes Node (protocol.NodePhase*). Empty means no
	// connector has reported one — a legacy record, or a burst whose node has
	// not registered yet — which is why Status still starts at "provisioning"
	// rather than being derived from this.
	//
	// omitempty on all three: a burst written before node reports existed
	// round-trips unchanged, and an absent phase stays absent rather than
	// decoding as a report of "".
	NodePhase string `json:",omitempty"`
	// NodePhaseReason is the connector's short note for that phase (the Ready
	// condition's reason, say). Bounded by the handler before it is stored.
	NodePhaseReason string `json:",omitempty"`
	// NodePhaseAt is the connector observation time for this phase. New connectors
	// supply stable Kubernetes timestamps (condition LastTransitionTime, node
	// CreationTimestamp) that do not vary across reconnect replays; legacy
	// connectors that omit it get central's receipt time. It is for ordering and
	// status, not cost accounting, and proves nothing about teardown — see
	// OccupancyObservedAt.
	NodePhaseAt *time.Time `json:",omitempty"`

	// NodePhaseSourceTimestamped records whether NodePhaseAt came from the
	// connector's source timestamp rather than central's receipt time. A
	// source-timestamped event always upgrades a legacy record, and a legacy
	// event can never regress source-ordered state — this is the ordering
	// contract that prevents a reconnect replay from suppressing a real
	// transition. Not exposed in the API.
	NodePhaseSourceTimestamped bool `json:",omitempty"`

	// OccupancyObservedAt is when a connector last proved it could still see
	// whether this burst's node is idle, and it is what the nodeOnly silence
	// ceiling measures from.
	//
	// Deliberately separate from NodePhaseAt. Node health is reported by every
	// connector, including one scoped to a namespace and one that has lost
	// cluster-wide pod LIST — neither of which can ever send the idle teardown a
	// nodeOnly burst needs. Reading health as liveness kept the ceiling open for
	// capacity nothing was watching. It is stamped only by an event that
	// explicitly carries protocol.NodeEvent.OccupancyObserved, so nil means no
	// connector has ever made that claim — and what happens then is decided by
	// OccupancyObservationExpected below, not guessed.
	OccupancyObservedAt *time.Time `json:",omitempty"`

	// OccupancyObservationExpected records what the connector this burst was
	// placed on PROMISED at admission: that it could read the whole cluster's
	// pods and would therefore keep stamping OccupancyObservedAt.
	//
	// It is what makes silence readable before the first observation. A promise
	// that was made and never kept is a connector that died between admission and
	// its first 5m sweep, so the silence ceiling may measure from CreatedAt —
	// there is a capability to have lost. When it is false nothing was ever
	// promised, so a missing observation is not silence and the ceiling does not
	// apply; those bursts are bounded by the deadline admission gave them.
	//
	// omitempty, and false is right for every record written before this field
	// existed: a legacy burst made no promise anyone can point to.
	OccupancyObservationExpected bool `json:",omitempty"`
}

// PersistentVolume tracks a backend volume yscale provisions on
// behalf of a tenant. Two flavors:
//   - cache: read-only mirror of a customer bucket, addressed by
//     CacheKey. Shared across the tenant's workloads.
//   - persistent: writable, addressed by Name (per-tenant unique).
//
// Both flavors live on a specific backend in a specific datacenter;
// the decider must route bursts to that DC for the volume to attach.
type PersistentVolume struct {
	ID           string // "pv_abc123"
	TenantID     string
	Type         string // "cache" | "persistent"
	CacheKey     string // for cache type — derived from (tenant, source URI)
	Name         string // for persistent type — workload-spec name
	Backend      string // "flyio" | "linode" | "aws" | "yscale-self"
	BackendVolID string // backend-assigned volume id (Linode block-storage id, Fly volume id, …)
	DCRegion     string // "us-east", "EU-RO-1", etc — for routing affinity
	SizeGB       int
	Retention    string // raw spec string; parsed via pkg/cache.ParseRetention
	SourceURI    string // for cache type — "s3://b/p/" or equivalent
	SourceETag   string // composite ETag of cached objects, for staleness check
	State        string // "active" | "stale" | "evicting" | "deleted"
	CreatedAt    time.Time
	LastUsedAt   time.Time
}

// Store is the central server's persistence layer.
type Store struct {
	mu sync.RWMutex

	// custMu serializes the customer LIFECYCLE operations — durable create,
	// revoke, final delete — against each other. It is a second, narrower lock
	// on purpose: those three each run a multi-statement Postgres transaction,
	// and the check they start with is only meaningful if no other lifecycle
	// call can slip between it and the write. Holding s.mu for that window
	// would block every reader on this central (including the auth lookup on
	// each agent request) for as long as the database is slow, so s.mu is taken
	// only around the in-memory index edits, never across a round trip. Lock
	// order where both are needed is custMu then mu, never the reverse.
	custMu sync.Mutex

	customers      map[string]*Customer // by ID (includes revoked, pending cleanup)
	customersByTok map[string]*Customer // by TokenHash (ACTIVE customers only)

	// tombstoned is the in-memory half of customer_tombstones: ids a finished
	// offboard retired for good. It exists because the durable tombstone is not
	// reachable from here — the customers row is gone by then, so nothing in the
	// maps above can say the id was ever used, and the store's own writers would
	// hand it back out until the next restart read the table. Every customer
	// writer consults it, entries are added by DeleteRevokedCustomer and by
	// applySnapshot at boot, and nothing removes one: retirement is permanent,
	// ids are generated rather than chosen, and nothing legitimate needs one
	// back. A store with no persister (OSS/dev, tests) gets the same permanence
	// for the lifetime of the process.
	tombstoned map[string]bool

	// Human SaaS identities, kept in their own indexes so a human lookup can
	// never fall through to the cluster-token index (or the reverse).
	accounts           map[string]*Account // by Account.ID
	accountsByIdentity map[string]*Account // by identityKey(issuer, subject)

	// Memberships are indexed only by the two directions anything asks for —
	// "which tenants does this human have" and "who is on this tenant". There is
	// deliberately no by-id index: a membership id is derived from the pair, so
	// nothing ever holds one without already holding both ends.
	membershipsByAccount map[string]map[string]*TenantMembership // [accountID][customerID]
	membershipsByTenant  map[string]map[string]*TenantMembership // [customerID][accountID]

	agents       map[string]*Agent            // by Agent.ID
	agentsByCust map[string]map[string]*Agent // [customerID][agentID] — usually 1 per cluster

	// Derived indexes over the durable cluster registry, rebuilt on load like
	// the token index. clusterOwners is what makes a cross-tenant cluster id
	// refusable with no connector live; clusterCreds is what lets a connector
	// credential authenticate by digest without scanning tenants.
	clusterOwners         map[string]string              // [clusterID] -> customerID
	clusterCreds          map[string]clusterCredRef      // [sha256 hex of credential]
	catalogPublisherCreds map[string]catalogPublisherRef // [sha256 hex of credential]
	// hostedNamespaces is the one-owner index over hosted rows'
	// HostedNamespace: the namespaces are carved out of ONE shared physical
	// cluster, so unlike WorkloadNamespaces — which name namespaces in each
	// tenant's own clusters — two tenants holding the same entry here is a
	// cross-tenant boundary collapse. Rebuilt on load like clusterOwners.
	hostedNamespaces map[string]string // [namespace] -> customerID

	// sharedTailnetPushed is the Src central last put on the SHARED tailnet's
	// managed kubelet rule — the one ACL document every tenant without a mesh
	// box rides. It is what proves, on the next write, which rule on that shared
	// document is central's own: the destination cannot, since an operator is
	// free to author a rule with the same action, proto and dst, and deleting
	// theirs on that evidence is not a mistake anything undoes.
	//
	// GLOBAL, and deliberately not a field on any customer. The value spans
	// tenants, and the moment it has to survive is exactly a tenant going away:
	// a record kept on customer rows would vanish with the row an offboard
	// removes, leaving central unable to prove the rule that still grants that
	// tenant's CIDRs is its own to take back. It has its own durable row for the
	// same reason (see meshState).
	sharedTailnetPushed []string

	// sharedTailnetClaims are the WRITE-AHEAD ownership claims: every Src
	// central recorded it was ABOUT to put on that rule and has not yet proven
	// it collapsed. sharedTailnetPushed alone cannot cover the gap between the
	// ACL POST and the record of it — a write that lands and a receipt that does
	// not leaves a rule on the shared document carrying a union nothing durable
	// names, and once the desired union moves again that rule is
	// indistinguishable from an operator's and over-grants forever.
	//
	// Written BEFORE the POST and collapsed to nothing after a successful one,
	// so the proof set is (pushed + claims) and is bounded by
	// maxSharedTailnetClaims rather than growing with history.
	sharedTailnetClaims [][]string

	// startupReapArmed names every cluster this process has already handed to a
	// boot reap lease, keyed customer+cluster. In-memory and never persisted: it
	// is about THIS process's startup, and a Store is exactly one process's view
	// of the durable record. See ConsumeUnheldRouteClusters.
	startupReapArmed map[string]struct{}

	workloads          map[string]*Workload          // by ID
	bursts             map[string]*Burst             // by ID
	cloudAccountLeases map[string]*CloudAccountLease // by burst id; in-memory stores only
	// burstReapReceipts is the in-memory/OSS proof that a tenant's provider
	// teardown was secured. Postgres-backed stores query the durable receipt
	// table instead so another replica and a restart see the same answer.
	burstReapReceipts map[string]string // [burstID] -> customerID

	// idempotency holds workload-submission claims by scope digest. It is read
	// ONLY when persist is nil — the single-process self-hosted and test shape,
	// where this map is the durable record and the store lock is the claim's
	// atomicity. With a durable backend the table is the only source of truth,
	// because a claim mirrored per-replica is a key that buys one node per
	// central. See ClaimIdempotent.
	idempotency map[string]*IdempotencyClaim // by scope digest
	// connectorCommands is the in-memory/OSS command ledger. Postgres-backed
	// stores keep this collection database-authoritative so leases and ACKs are
	// shared across replicas and survive restart.
	connectorCommands map[string]*ConnectorCommand

	pvs           map[string]*PersistentVolume            // by ID
	pvsByCacheKey map[string]map[string]*PersistentVolume // [tenantID][cacheKey]
	pvsByName     map[string]map[string]*PersistentVolume // [tenantID][name]

	// admissionReservations is the in-memory reservation map. For stores with
	// a Postgres persister, the database is authoritative and this map is unused;
	// for in-memory stores (tests, OSS-local) this IS the reservation set and
	// the store lock serializes admission.
	admissionReservations map[string]*AdmissionReservation

	// persist is the durability backend. nil means in-memory only (the New()
	// store used by tests + OSS-local): every write-through is then a no-op.
	persist persister

	// Cost is the metrics meter. Optional (nil = no agent gauge). Set by
	// production wiring in main.go; left nil in tests.
	Cost *cost.Meter
}

// emptyStore allocates a Store with all maps initialized and no data.
func emptyStore() *Store {
	return &Store{
		customers:            make(map[string]*Customer),
		customersByTok:       make(map[string]*Customer),
		tombstoned:           make(map[string]bool),
		accounts:             make(map[string]*Account),
		accountsByIdentity:   make(map[string]*Account),
		membershipsByAccount: make(map[string]map[string]*TenantMembership),
		membershipsByTenant:  make(map[string]map[string]*TenantMembership),

		agents:                make(map[string]*Agent),
		agentsByCust:          make(map[string]map[string]*Agent),
		clusterOwners:         make(map[string]string),
		clusterCreds:          make(map[string]clusterCredRef),
		catalogPublisherCreds: make(map[string]catalogPublisherRef),
		hostedNamespaces:      make(map[string]string),
		workloads:             make(map[string]*Workload),
		bursts:                make(map[string]*Burst),
		cloudAccountLeases:    make(map[string]*CloudAccountLease),
		burstReapReceipts:     make(map[string]string),
		idempotency:           make(map[string]*IdempotencyClaim),
		connectorCommands:     make(map[string]*ConnectorCommand),
		pvs:                   make(map[string]*PersistentVolume),
		pvsByCacheKey:         make(map[string]map[string]*PersistentVolume),
		pvsByName:             make(map[string]map[string]*PersistentVolume),
		admissionReservations: make(map[string]*AdmissionReservation),
	}
}

// DefaultDevToken is the deterministic seed-customer token used by New() — the
// tests' single source of truth. Production in-memory wiring (main.go) must NOT
// use this: it passes a per-boot random token via NewWithSeedToken so a
// publicly exposed OSS central ships no known-token backdoor.
const DefaultDevToken = "yscale_test_local_dev_token_change_me"

// DevCustomerID is the ID of the seeded local-dev customer. Stable regardless
// of the (possibly randomized) token, so callers key off it, not the token.
const DevCustomerID = "cust_test"

// seedTestCustomer adds the local-dev test customer with the given token. The
// caller owns credential delivery; bearer credentials must never be logged.
//
// It writes the indexes directly (it runs before the store is shared), so it
// carries the tombstone check itself: an operator who offboarded the dev tenant
// retired that id, and a first-boot seed is the one path that would otherwise
// hand it a working token again.
func (s *Store) seedTestCustomer(token string) {
	if s.tombstoned[DevCustomerID] {
		slog.Warn("state: not seeding the dev customer; its id is permanently retired",
			"customer", DevCustomerID)
		return
	}
	test := &Customer{
		ID:    DevCustomerID,
		Token: token,
		Email: "test@yscale.sh",
		Plan:  "pro",
	}
	ensureCustomerTokenHash(test)
	s.customers[test.ID] = test
	s.customersByTok[test.TokenHash] = test
}

// New builds an empty in-memory store (no durability) seeded with one test
// customer using the deterministic DefaultDevToken. Used by tests and by
// callers that don't want a snapshot file.
func New() *Store {
	return NewWithSeedToken(DefaultDevToken)
}

// NewWithSeedToken is New() but with a caller-chosen seed-customer token. The
// in-memory production path passes a freshly generated random token so an
// exposed central has no known-token backdoor.
func NewWithSeedToken(token string) *Store {
	s := emptyStore()
	s.seedTestCustomer(token)
	return s
}

// NewDevToken returns a random 192-bit hex bearer token for the dev seed
// customer (yscale_dev_<48 hex>). Both production seed paths (the in-memory
// store and the opt-in Postgres first-boot seed) use it so no build ever ships
// a predictable token — a guessable dev token is a direct auth bypass, so on
// the (practically impossible) crypto/rand failure we fail closed.
func NewDevToken() string {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("state: crypto/rand read failed: " + err.Error())
	}
	return "yscale_dev_" + hex.EncodeToString(b[:])
}

// snapshot is the load-all form of the durable store: just the primary entity
// collections (derived indexes — by token, by cache key, by name — are rebuilt
// on apply). Agents are omitted by design (they re-register on reconnect).
type snapshot struct {
	Customers []*Customer
	// Tombstones are the customer ids finished offboards retired, read from the
	// tombstone table rather than derived from any row: the rows they name are
	// gone, which is the whole reason they are recorded separately.
	Tombstones  []string
	Accounts    []*Account
	Memberships []*TenantMembership
	Workloads   []*Workload
	Bursts      []*Burst
	PVs         []*PersistentVolume
	// MeshState is the single global mesh-state row, nil when nothing has
	// written one yet (a database that predates it, or a central that has never
	// converged the shared tailnet).
	MeshState *meshState
}

// meshState is the durable, GLOBAL record the mesh reconcile keeps: state
// that belongs to a whole tailnet rather than to any one tenant, and so has no
// customer row to live on.
//
// One row, one document, for the reason the shared tailnet has one ACL: the
// value it holds is cross-tenant. Keeping it per customer would tie the record
// of what central wrote to the lifecycle of tenants it outlives — an offboard
// removes the row, and with it the only proof that the grant still standing on
// the tailnet is central's to revoke.
type meshState struct {
	// SharedKubeletRoutes is the Src of the kubelet :10250 rule central last
	// successfully wrote on the shared tailnet. Written only AFTER that API call
	// returned: a record claiming a rule central had not written is what would
	// let the next write delete an operator's.
	SharedKubeletRoutes []string `json:",omitempty"`

	// SharedKubeletClaims are the Srcs central recorded it was ABOUT to write
	// there, oldest first, and has not yet proven gone. Written BEFORE the API
	// call for the opposite reason SharedKubeletRoutes is written after it: the
	// rule a POST installs exists whether or not its receipt survives, so the
	// only way a later write can still recognise it is a claim that was durable
	// before the POST was made. Cleared by the write that proves the document
	// carries exactly SharedKubeletRoutes.
	//
	// Absent in rows written before this field existed, which reads back as no
	// claims — exactly what a central that only ever wrote receipts had.
	SharedKubeletClaims [][]string `json:",omitempty"`
}

// applySnapshot populates the store's maps from a loaded snapshot and rebuilds
// the derived indexes. Called once at construction (NewPostgres) before the
// store is shared, so it needs no lock.
func (s *Store) applySnapshot(snap *snapshot) {
	// Tombstones go in FIRST: they are what the customer writers consult, and a
	// restart that indexed customers before reading them would answer "free" for
	// a retired id in the window between.
	for _, id := range snap.Tombstones {
		s.tombstoned[id] = true
	}
	// A revoked customer is restored by id — an operator may still have cleanup
	// to retry against it — but never into the token index. Rebuilding that
	// index from the row alone is exactly how a revoked tenant's credential
	// comes back to life at the next restart.
	//
	// A row whose id is tombstoned is skipped entirely. No writer can produce
	// that pair — the finalize deletes the row in the tombstone's transaction,
	// and every writer after it is refused — so it means a row was restored
	// underneath the retirement, and indexing it would put a retired tenant's
	// token back in the auth index.
	for _, c := range snap.Customers {
		if s.tombstoned[c.ID] {
			slog.Warn("state: ignoring customer row for a permanently retired id", "customer", c.ID)
			continue
		}
		ensureCustomerTokenHash(c)
		s.customers[c.ID] = c
		if !c.Revoked() && c.TokenHash != "" {
			s.customersByTok[c.TokenHash] = c
		}
		// Cluster registry indexes are rebuilt for revoked customers too: the
		// ids stay HELD (a half-offboarded tenant's cluster id must not be
		// claimable by someone else), while AuthClusterCredential refuses the
		// credentials itself by checking Revoked() — the same split the id/token
		// indexes above make.
		s.indexClustersLocked(c)
		s.indexCatalogPublishersLocked(c)
	}
	for _, a := range snap.Accounts {
		s.accounts[a.ID] = a
		s.accountsByIdentity[identityKey(a.Issuer, a.Subject)] = a
	}
	// Memberships are indexed only when BOTH ends resolve to something live. A
	// row whose account never landed, or whose customer is missing or revoked,
	// is dangling: it names a tenant nobody can reach, so honoring it would
	// hand a human a membership summary for a tenant that is gone.
	//
	// The row is skipped, NOT deleted. Load is a read path: an unindexed row
	// grants nothing, so the problem is already contained, and deleting here
	// would turn a grant that is merely unreadable right now — a half-restored
	// database, a customer row still to be written — into a lost one. Removal
	// is the writers' job, and both of them do it transactionally: the revoke
	// deletes a tenant's grants alongside the tenant, and the durable create
	// clears any leftovers for an id before it reissues it, so a reused id can
	// never resurrect a stale grant.
	for _, m := range snap.Memberships {
		cust := s.customers[m.CustomerID]
		if s.accounts[m.AccountID] == nil || cust == nil || cust.Revoked() {
			slog.Warn("state: ignoring dangling tenant membership",
				"membership", m.ID, "account", m.AccountID, "customer", m.CustomerID)
			continue
		}
		s.indexMembership(m)
	}
	for _, w := range snap.Workloads {
		s.workloads[w.ID] = w
	}
	for _, b := range snap.Bursts {
		s.bursts[b.ID] = b
	}
	for _, v := range snap.PVs {
		s.pvs[v.ID] = v
		s.indexPV(v)
	}
	if snap.MeshState != nil {
		s.sharedTailnetPushed = slices.Clone(snap.MeshState.SharedKubeletRoutes)
		s.sharedTailnetClaims = cloneRouteUnions(snap.MeshState.SharedKubeletClaims)
	}
	// Load deliberately reaps NOTHING. A persisted by-cluster route entry whose
	// cluster has neither a registry row nor a live socket is one nothing else
	// would ever remove, and load is where it becomes visible — but it is the
	// worst possible moment to act on it, because a process that has just booted
	// holds no sockets at all. Every live-only connector in the fleet looks
	// unheld for as long as it takes to reconnect, and reaping there withdrew a
	// running cluster's routes on every central restart, healing only once that
	// connector came back and re-reported.
	//
	// A restart is a disconnect from the connector's side, so it gets the
	// disconnect lifecycle: ConsumeUnheldRouteClusters names the candidates, the
	// startup wiring arms the same grace lease a socket close does, a reconnect
	// inside the grace cancels it, and an expiry runs ReapUnheldClusterGatewayRoutes
	// — which re-checks ownership under the store lock before it drops anything.
	// Nothing is lost by waiting: the entry is durable either way, and the reap
	// that eventually runs is the same durable one, with the same withdrawal
	// enqueued behind it.
}

// indexPV (re)builds the cache-key and name indexes for one PV. Shared by
// applySnapshot and PutPersistentVolume.
func (s *Store) indexPV(v *PersistentVolume) {
	if v.CacheKey != "" {
		if s.pvsByCacheKey[v.TenantID] == nil {
			s.pvsByCacheKey[v.TenantID] = make(map[string]*PersistentVolume)
		}
		s.pvsByCacheKey[v.TenantID][v.CacheKey] = v
	}
	if v.Name != "" {
		if s.pvsByName[v.TenantID] == nil {
			s.pvsByName[v.TenantID] = make(map[string]*PersistentVolume)
		}
		s.pvsByName[v.TenantID][v.Name] = v
	}
}

// mapValues returns a map's values as a slice, for snapshotting.
func mapValues[K comparable, V any](m map[K]V) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

// AddCustomer registers an additional customer (beyond the seeded test
// customer) by ID+token. Idempotent: re-adding the same ID overwrites the
// record (so re-seeding from env on restart is safe). Used for the env-seeded
// multi-tenant dev path — production gets customers from a signup flow. Returns
// the stored Customer so the caller can attach a Mesh endpoint to it.
//
// Runtime-attached mesh is preserved: a customer onboarded to the coordination server
// factory at runtime (SetCustomerMesh) holds its endpoint only in the persisted
// record. The env-seed path re-adds the same ID on restart with a fresh
// Customer{Mesh:nil}; a blind overwrite would silently downgrade that customer
// to the shared SaaS mesh. So when the incoming record carries no mesh but an
// existing one does, the existing endpoint is carried over. An incoming record
// that DOES set a mesh (e.g. env-configured box vars) still wins.
//
// An id a finished offboard retired is REFUSED: nothing is indexed, no token is
// activated, and nothing is written durably. This is the one place the seeding
// path could bring a retired tenant back — the env config it re-drives itself
// from still names the id, and knows nothing about the offboard — and the
// durable layer would refuse the write anyway (ErrCustomerTombstoned), leaving a
// live customer in this process's memory and nowhere else. There is no error to
// return here, so the refusal is logged.
//
// The returned pointer is then the CALLER'S record, not a registered customer:
// it is not in any index, so every lookup by id or token misses it and a
// follow-on SetCustomerMesh (the enterprise env attach) gets ErrNotFound rather
// than reviving the tenant. Callers that need to know use CreateTenant, which
// answers ErrCustomerExists.
func (s *Store) AddCustomer(c *Customer) *Customer {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	if s.tombstoned[c.ID] {
		s.mu.Unlock()
		slog.Warn("state: refusing to add a customer on a permanently retired id; not indexed",
			"customer", c.ID)
		return c
	}
	if existing := s.customers[c.ID]; existing != nil && c.persistedData == "" {
		c.persistedData = existing.persistedData
	}
	s.indexCustomerLocked(c)
	s.mu.Unlock()
	s.pUpsertCustomer(c)
	return c
}

// CreateTenant is AddCustomer for the SaaS provisioning path, where the tenant
// is only real if its row landed: nothing is indexed until the durable create
// commits, so a refused write leaves no tenant, no token, no owner grant, and
// nothing for the caller to clean up — just ErrPersistence and a retry.
//
// AddCustomer keeps its best-effort behavior for the env-seeded dev path, which
// re-drives itself from config on every boot and would rather run than refuse to
// start. A provisioned tenant has no such source: the token is handed out once,
// so a customer that exists only in this process's memory is a partner whose
// cluster stops authenticating at the next restart.
//
// The tenant and its first owner are ONE operation, not two. A create followed
// by a separate membership write has a state between them — a tenant with a
// token already minted and no owner — and undoing it means a revoke plus a
// delete that can each fail on their own, so the caller's "provision failed"
// and the store's contents disagree exactly when the database is the thing
// going wrong. Here there is nothing to undo: the whole thing commits or none
// of it does. ownerAccountID is optional; empty provisions an operator-managed
// tenant with no human attached, which is the pre-SaaS shape.
//
// The id must be free both live and durably. ErrCustomerExists covers an active
// tenant, a revoked one still awaiting cleanup, a row this replica never held —
// the create is an INSERT, so the primary key decides it, not this process's map
// — and an id a finished offboard permanently retired, which no primary key can
// refuse because the row is gone (see ErrCustomerTombstoned); this store's own
// tombstone set refuses that one before the round trip, and the transaction
// refuses it again for a retirement another replica recorded. Taking an id that
// a tenant once held is also the one moment a stale grant on it could come back
// to life, so the same transaction clears any membership rows left over for that
// id before inserting: the new tenant starts with exactly the owner this
// provisioning gives it.
//
// ErrNotFound means the named owner account does not exist, and the contract is
// deliberately the in-memory one: an owner is resolved through this store
// (ResolveAccount) before provisioning, so by the time this runs the account is
// indexed here. An id that is not in the map is one this store never resolved.
// The transaction re-checks the row underneath, which is a narrower thing — it
// catches an account whose own durable write never landed — and does not widen
// the contract to accounts this replica has never seen.
func (s *Store) CreateTenant(c *Customer, ownerAccountID string) (*Customer, *TenantMembership, error) {
	return s.createTenant(c, ownerAccountID, false)
}

// CreateFirstTenant requires a currently verified owner with no memberships.
// Profile, contact email and grants are read under the same account-row lock
// as profile/membership writers (or under mu in memory), through the commit.
// Unlike operator creation, it may resolve its owner directly from durable
// state; a prior read need not have hydrated the local account index.
func (s *Store) CreateFirstTenant(c *Customer, ownerAccountID string) (*Customer, *TenantMembership, error) {
	return s.createTenant(c, ownerAccountID, true)
}

// RotateCustomerCredential replaces a tenant's bearer and returns the new
// plaintext once. The verifier and audit event commit together before the
// in-memory auth index changes, so a refused durable write leaves the old
// credential working and never publishes the new one.
func (s *Store) RotateCustomerCredential(customerID string, by Actor) (string, error) {
	token := NewCustomerToken()
	tokenHash := HashCustomerToken(token)

	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.RLock()
	c := s.customers[customerID]
	if c == nil || c.Revoked() {
		s.mu.RUnlock()
		return "", ErrNotFound
	}
	oldHash := c.TokenHash
	snapshot := *c
	snapshot.Token = ""
	snapshot.TokenHash = tokenHash
	s.mu.RUnlock()

	ev := NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionTenantCredentialRotate,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetTenant,
		TargetID:   customerID,
		Detail:     AuditDetail{Reason: ReasonTenantCredentialRotated},
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return "", fmt.Errorf("rotate tenant credential %s: %w", customerID, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.customersByTok, oldHash)
	c.Token = ""
	c.TokenHash = tokenHash
	s.customersByTok[tokenHash] = c
	return token, nil
}

func (s *Store) createTenant(c *Customer, ownerAccountID string, requireFirst bool) (*Customer, *TenantMembership, error) {
	ensureCustomerTokenHash(c)
	if requireFirst && ownerAccountID == "" {
		return nil, nil, ErrNotFound
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.RLock()
	_, taken := s.customers[c.ID]
	retired := s.tombstoned[c.ID]
	durableFirst := requireFirst && s.persist != nil
	ownerMissing := !durableFirst && ownerAccountID != "" && s.accounts[ownerAccountID] == nil
	s.mu.RUnlock()
	// A retired id is taken too, and answers the same conflict: its row is gone,
	// so the map above cannot see it and neither can the primary key underneath.
	if taken || retired {
		return nil, nil, fmt.Errorf("%w: %s", ErrCustomerExists, c.ID)
	}
	if ownerMissing {
		return nil, nil, fmt.Errorf("account %q: %w", ownerAccountID, ErrNotFound)
	}
	var owner *TenantMembership
	if ownerAccountID != "" {
		owner = &TenantMembership{
			ID:         membershipID(ownerAccountID, c.ID),
			AccountID:  ownerAccountID,
			CustomerID: c.ID,
			Role:       RoleOwner,
			CreatedAt:  time.Now().UTC(),
		}
	}
	if err := s.pCreateTenant(c, owner, requireFirst); err != nil {
		if errors.Is(err, ErrCustomerExists) || errors.Is(err, ErrNotFound) || errors.Is(err, ErrAccountHasTenant) || errors.Is(err, ErrAccountEmailUnverified) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("%w: persist tenant %s: %w", ErrPersistence, c.ID, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if requireFirst && !durableFirst {
		// Profile and membership writers also hold mu. Do not check here and
		// release it before indexing the tenant and its owner grant.
		account := s.accounts[ownerAccountID]
		if err := firstWorkspaceEligibility(account, len(s.membershipsByAccount[ownerAccountID]) != 0); err != nil {
			return nil, nil, err
		}
		c.Email = account.Email
	}
	// Same order as the transaction: any grant left over for this id goes
	// before the new tenant is indexed, so a reused id starts with exactly the
	// owner this call gives it in memory too.
	s.forgetMembershipsLocked(c.ID)
	s.indexCustomerLocked(c)
	if owner == nil {
		return c, nil, nil
	}
	s.indexMembership(owner)
	copied := *owner
	return c, &copied, nil
}

// indexCustomerLocked writes one customer into the id and token indexes,
// carrying over the runtime-attached fields documented on AddCustomer. Callers
// hold s.mu.
//
// A revoke is carried over too, and it is the one field that is never
// overwritten rather than merely defaulted: re-adding an id whose tenant has
// been revoked — the env-seed path re-driving itself on boot is the way this
// happens — must not put that token back in the auth index. Nothing here
// un-revokes a tenant; DeleteRevokedCustomer is what finally retires the id,
// and only once cleanup verified clean. pgPersister.upsertCustomer holds the
// same line durably, for the replica that never saw the revoke, and the
// tombstone DeleteRevokedCustomer writes holds it after the row is gone.
//
// The old token is unindexed before the new one goes in. Re-adding an id with a
// different token is a rotation, and the token index is keyed by token, not by
// id: leaving the previous entry there would keep the retired credential
// authenticating as this customer — a token the operator believes they replaced
// — and nothing later would remove it, because every remover looks up the token
// through the record, which now carries the new one.
func (s *Store) indexCustomerLocked(c *Customer) {
	ensureCustomerTokenHash(c)
	if existing, ok := s.customers[c.ID]; ok {
		if c.TokenHash == "" {
			c.TokenHash = existing.TokenHash
		}
		if existing.TokenHash != c.TokenHash {
			delete(s.customersByTok, existing.TokenHash)
		}
		if existing.Revoked() {
			c.RevokedAt = existing.RevokedAt
		}
		if c.Mesh == nil && existing.Mesh != nil {
			c.Mesh = existing.Mesh
		}
		if len(c.GatewayRoutes) == 0 && len(existing.GatewayRoutes) > 0 {
			c.GatewayRoutes = append([]string(nil), existing.GatewayRoutes...)
		}
		if len(c.PushedGatewayRoutes) == 0 && len(existing.PushedGatewayRoutes) > 0 {
			c.PushedGatewayRoutes = append([]string(nil), existing.PushedGatewayRoutes...)
		}
		// The by-cluster halves carry over for the same reason as the union:
		// the env-seed path re-adds its customers from config on every boot and
		// knows nothing about what the fleet has reported since. Dropping them
		// would leave every gateway's exact set unknown until its cluster
		// reported again, and the union recomputed from an empty map would
		// widen the policy back to the stale legacy set.
		if len(c.GatewayRoutesByCluster) == 0 && len(existing.GatewayRoutesByCluster) > 0 {
			c.GatewayRoutesByCluster = copyRouteSets(existing.GatewayRoutesByCluster)
		}
		if len(c.PushedGatewayRoutesByCluster) == 0 && len(existing.PushedGatewayRoutesByCluster) > 0 {
			c.PushedGatewayRoutesByCluster = copyRouteSets(existing.PushedGatewayRoutesByCluster)
		}
		// Carried over for the same reason as the mesh endpoint, with more at
		// stake: this is the tenant's authorization boundary. The env-seed path
		// re-adds its customers from config on every boot, knowing nothing about
		// an operator's later SetCustomerWorkloadNamespaces, so a blind overwrite
		// would silently revoke every namespace but the fail-closed default at
		// the next restart — and the connector's RBAC, configured to match, would
		// keep granting reads the tenant is no longer authorized to ask for.
		if len(c.WorkloadNamespaces) == 0 && len(existing.WorkloadNamespaces) > 0 {
			c.WorkloadNamespaces = append([]string(nil), existing.WorkloadNamespaces...)
		}
		if c.ClusterPolicy == nil && existing.ClusterPolicy != nil {
			c.ClusterPolicy = copyClusterPolicy(existing.ClusterPolicy)
		}
		if c.TemplateCatalog == nil && existing.TemplateCatalog != nil {
			c.TemplateCatalog = copyTemplateCatalog(existing.TemplateCatalog)
		}
		if c.CatalogPublishers == nil && existing.CatalogPublishers != nil {
			c.CatalogPublishers = copyCatalogPublishers(existing.CatalogPublishers)
		}
		if c.RuntimeBindings == nil && existing.RuntimeBindings != nil {
			c.RuntimeBindings = copyRuntimeBindings(existing.RuntimeBindings)
		}
		if c.RuntimeBindingsRevision == 0 && existing.RuntimeBindingsRevision != 0 {
			c.RuntimeBindingsRevision = existing.RuntimeBindingsRevision
		}
		if len(c.GitOpsSources) == 0 && len(existing.GitOpsSources) > 0 {
			c.GitOpsSources = copyGitOpsSources(existing.GitOpsSources)
		}
		// The cluster registry is carried over like the namespace set, and for
		// the same stakes: rows hold the ONLY copy of each connector credential
		// hash, so a re-seed that dropped them would cut every registered
		// connector's reconnect path at the next boot. The old record's derived
		// index entries come out before the new record's go in, so a carried-over
		// registry is re-keyed to the record that now owns it.
		if c.RegisteredClusters == nil && existing.RegisteredClusters != nil {
			c.RegisteredClusters = copyRegisteredClusters(existing.RegisteredClusters)
		}
		s.unindexClustersLocked(existing)
		s.unindexCatalogPublishersLocked(existing)
	}
	s.customers[c.ID] = c
	if !c.Revoked() && c.TokenHash != "" {
		s.customersByTok[c.TokenHash] = c
	}
	s.indexClustersLocked(c)
	s.indexCatalogPublishersLocked(c)
}

// RevokeCustomer cuts a tenant's access without erasing it. The customer row is
// stamped RevokedAt and every membership naming it is deleted — a grant on a
// tenant nobody can reach is not access, it is a trap for the next holder of
// that id — while the record itself stays reachable through CustomerByID.
// Accounts survive untouched: a human may still own other tenants.
//
// Keeping the record is the point. Offboard revokes first and tears down
// second, so between the two there is a window — a failed reap, a crashed
// process, a factory that would not answer — where cloud resources are still
// running under an id. Deleting the customer there would leave those resources
// with no admin path back; a revoked record leaves an operator something to
// retry against. DeleteRevokedCustomer is what finally removes it, once cleanup
// verifies clean.
//
// The durable half is ONE transaction covering the stamped customer row and all
// its membership rows, and it commits before anything in memory changes. Two
// separate writes have a state between them: a failed membership delete
// followed by a successful customer write leaves a durable grant naming a
// tenant nobody owns, invisible until the id is provisioned again — at which
// point the old human is a member of the new tenant. An error therefore means
// NOTHING changed, in memory or durably, and the caller must not have started
// any teardown it cannot undo.
//
// Idempotent: revoking a missing or already-revoked id is a no-op, so an
// offboard retry walks straight past this step to the cleanup it still owes.
func (s *Store) RevokeCustomer(id string) error {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.RLock()
	c, ok := s.customers[id]
	s.mu.RUnlock()
	if !ok || c.Revoked() {
		return nil
	}
	// Stamped on a copy: the store keeps the live record untouched until the
	// transaction commits, so a refused write leaves a customer that is still
	// exactly what it was rather than one memory calls revoked and Postgres
	// calls active.
	revoked := *c
	now := time.Now().UTC()
	revoked.RevokedAt = &now
	if err := s.pRevokeCustomer(&revoked); err != nil {
		return fmt.Errorf("%w: revoke customer %s: %w", ErrPersistence, id, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c.RevokedAt = &now
	delete(s.customersByTok, c.TokenHash)
	s.forgetMembershipsLocked(id)
	return nil
}

// DeleteRevokedCustomer is the last step of an offboard: it removes a REVOKED
// customer's row for good, once the caller has verified nothing the tenant
// owned is still tracked. Splitting it from RevokeCustomer is what makes a
// partial offboard retryable — access is already gone by the time this runs, so
// a failure here costs another attempt and nothing else.
//
// "For good" is durable, not just local: the same transaction records a
// permanent tombstone for the id, so the id is retired rather than merely
// unoccupied. Without it, deleting the row leaves nothing that says the id was
// ever used, and the next writer to touch it — a replica still holding the
// pre-delete record, or a create that draws the same id — finds a free primary
// key and takes it. A tombstoned id is refused by every durable customer write
// from then on, and nothing un-retires one.
//
// The retirement is recorded in memory in the same step, not left for the next
// restart to read back: this process is itself one of the writers that would
// otherwise reissue the id, and a store with no persister at all (OSS/dev,
// tests) has no table to read. Both halves go in only after the durable step
// commits, so a failed finalize leaves the id exactly as retryable as it was.
//
// ErrCustomerActive on a customer that was never revoked: the id would then be
// freed while its token still authenticates. Idempotent on a missing id, so a
// retry after a successful delete reports the tenant gone rather than failing.
func (s *Store) DeleteRevokedCustomer(id string) error {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.RLock()
	c, ok := s.customers[id]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	if !c.Revoked() {
		return fmt.Errorf("%w: %s", ErrCustomerActive, id)
	}
	if err := s.pFinalizeRevokedCustomer(id); err != nil {
		return fmt.Errorf("%w: delete revoked customer %s: %w", ErrPersistence, id, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tombstoned[id] = true
	s.forgetCustomerLocked(id)
	return nil
}

// DeleteCustomer is the pre-SaaS, best-effort form kept for OSS/dev callers:
// memory is cleared whether or not the durable delete lands, and a failure is
// only logged. Anything that revokes ACCESS — tenant offboard above all — uses
// RevokeCustomer plus DeleteRevokedCustomer instead, which fail closed and stay
// retryable.
func (s *Store) DeleteCustomer(id string) {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	_, ok := s.customers[id]
	s.forgetCustomerLocked(id)
	s.mu.Unlock()
	if !ok {
		return
	}
	if err := s.pDeleteCustomerAndMemberships(id); err != nil {
		slog.Error("state: persist customer delete", "id", id, "error", err)
	}
}

// forgetCustomerLocked drops one customer and its memberships from the in-memory
// indexes. Callers hold s.mu.
func (s *Store) forgetCustomerLocked(id string) {
	c, ok := s.customers[id]
	if !ok {
		return
	}
	delete(s.customers, id)
	delete(s.customersByTok, c.TokenHash)
	s.unindexClustersLocked(c)
	s.forgetMembershipsLocked(id)
}

// forgetMembershipsLocked drops one tenant's memberships from both membership
// indexes, leaving the customer itself alone — what a revoke does. Callers hold
// s.mu.
func (s *Store) forgetMembershipsLocked(id string) {
	for accountID := range s.membershipsByTenant[id] {
		if byAcct := s.membershipsByAccount[accountID]; byAcct != nil {
			delete(byAcct, id)
			if len(byAcct) == 0 {
				delete(s.membershipsByAccount, accountID)
			}
		}
	}
	delete(s.membershipsByTenant, id)
}

// identityKey is the in-memory index key for an external identity. The
// separator cannot appear in either half, so two different pairs can never
// collide onto one key.
func identityKey(issuer, subject string) string { return issuer + "\x00" + subject }

// normalizeIdentity trims an incoming (issuer, subject) pair and rejects a
// blank half. Fails closed: a resolver that returned an empty `sub` must not be
// able to mint (or log in as) a shared "" account.
func normalizeIdentity(issuer, subject string) (string, string, error) {
	issuer, subject = strings.TrimSpace(issuer), strings.TrimSpace(subject)
	if issuer == "" || subject == "" {
		return "", "", ErrInvalidIdentity
	}
	return issuer, subject, nil
}

// UpsertAccount creates or refreshes the human account for one external
// identity and returns a copy of the stored record. Idempotent by construction:
// the row id is derived from (issuer, subject), so repeated sign-ins — and
// concurrent ones on different replicas — converge on a single account instead
// of minting a new id each time.
//
// Profile fields are overwritten from the issuer (that is the point of a cached
// profile); the identity pair and CreatedAt are not. Only the issuer may call
// this: it is the sole writer of profile data, so a caller that merely needs the
// account for an identity uses ResolveAccount instead.
func (s *Store) UpsertAccount(issuer, subject string, p AccountProfile) (*Account, error) {
	return s.accountForIdentity(issuer, subject, &p)
}

// ResolveAccount returns the account for an external identity, creating an
// empty-profile one when the human has never signed in. It never writes profile
// fields, so an operator provisioning a tenant by `sub` cannot blank out the
// cached email/name of a human who has: the issuer is the only writer of that
// data, and a `sub` is all a provisioning caller actually knows.
func (s *Store) ResolveAccount(issuer, subject string) (*Account, error) {
	return s.accountForIdentity(issuer, subject, nil)
}

// accountForIdentity backs both account entry points. A nil profile resolves
// without touching profile data; a non-nil one refreshes it.
//
// The durable write happens BEFORE the store is mutated, from a candidate copy,
// so a backend failure leaves the in-memory account exactly as it was and the
// caller gets ErrPersistence. This one is not best-effort like the customer
// writes: an account created for an owner grant is the row the membership
// points at, and a logged-and-ignored failure would persist the membership on
// top of an account that never landed — the grant reads fine until a restart,
// then vanishes.
func (s *Store) accountForIdentity(issuer, subject string, p *AccountProfile) (*Account, error) {
	issuer, subject, err := normalizeIdentity(issuer, subject)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.persist != nil {
		return s.resolveDurableAccountLocked(issuer, subject, p)
	}
	key := identityKey(issuer, subject)
	a, ok := s.accountsByIdentity[key]
	candidate, changed := accountAfterRefresh(issuer, subject, a, p, time.Now())
	if !changed {
		return candidate, nil
	}
	if !ok {
		a = &Account{}
		s.accounts[candidate.ID] = a
		s.accountsByIdentity[key] = a
	}
	*a = *candidate
	// candidate is this call's own value, never the stored pointer, so it is
	// already the copy callers get.
	return candidate, nil
}

// AccountByIdentity resolves an external identity to its account.
// ErrInvalidIdentity on a blank pair, ErrNotFound when no account exists.
func (s *Store) AccountByIdentity(issuer, subject string) (*Account, error) {
	return s.AccountByIdentityContext(context.Background(), issuer, subject)
}

// AccountByID returns a detached current account by durable id. ErrNotFound on
// miss; a configured durable backend cannot fall back to a cached profile.
func (s *Store) AccountByID(id string) (*Account, error) {
	return s.AccountByIDContext(context.Background(), id)
}

// AddTenantMembership binds an existing account to an existing customer with a
// validated role. Both ends must already exist — a membership naming a missing
// account or a missing tenant is the dangling row applySnapshot has to throw
// away, so it is refused at the door instead.
//
// Re-adding the same (account, customer, role) is idempotent. Re-adding it with
// a DIFFERENT role is ErrRoleConflict rather than an overwrite: role changes are
// a deliberate operation, and a re-provision must not be able to silently demote
// an owner.
//
// The grant is attributed to SystemActor: this form is central wiring itself up
// (seeds, fixtures), not a credential asking for a grant. Every route that has
// a principal calls GrantTenantMembership directly with it.
func (s *Store) AddTenantMembership(accountID, customerID, role string) (*TenantMembership, error) {
	m, _, err := s.GrantTenantMembership(accountID, customerID, role, SystemActor())
	return m, err
}

// GrantTenantMembership is AddTenantMembership plus whether THIS call is what
// created the grant. Same lock, same durable write, same errors — created is
// read off the insert path itself rather than from a lookup the caller does
// first, so two identical grants racing cannot both report themselves as the
// creator. An operator route needs that to answer 201-vs-200 and to audit-log a
// new grant without claiming one on every retry.
//
// by is the principal the grant is attributed to, and it is a parameter rather
// than something the store infers because only the caller knows which
// credential asked. A REAL grant writes one audit row in the same durable
// transaction as the membership; an idempotent re-grant writes neither, because
// nothing changed and a journal that claims a grant on every retry cannot be
// used to answer when access was given.
func (s *Store) GrantTenantMembership(accountID, customerID, role string, by Actor) (*TenantMembership, bool, error) {
	if !ValidRole(role) {
		return nil, false, fmt.Errorf("%w: %q", ErrInvalidRole, role)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.persist != nil {
		result, err := s.mutateMembershipLocked(membershipMutation{operation: membershipGrant, customer: customerID, target: accountID, role: role, by: by})
		return result.member, result.created, err
	}
	return s.grantMembershipLocked(accountID, customerID, role, by)
}

// grantMembershipLocked is GrantTenantMembership's body without the role
// validation and the lock, for the human path that has already taken both —
// and has decided, under the SAME lock, that the caller may grant this role at
// all. Sharing it is the point: the idempotent re-grant, the conflict, the
// durable write and its rollback are the tenant's rules, not the credential's,
// so the operator route and the member route cannot drift apart on them.
// Callers hold s.mu.
func (s *Store) grantMembershipLocked(accountID, customerID, role string, by Actor) (*TenantMembership, bool, error) {
	if s.accounts[accountID] == nil {
		return nil, false, fmt.Errorf("account %q: %w", accountID, ErrNotFound)
	}
	if c := s.customers[customerID]; c == nil || c.Revoked() {
		// A revoked tenant is as good as absent here: granting access to one
		// would write back a membership row the revoke just deleted.
		return nil, false, fmt.Errorf("customer %q: %w", customerID, ErrNotFound)
	}
	if existing := s.membershipsByAccount[accountID][customerID]; existing != nil {
		if existing.Role != role {
			return nil, false, fmt.Errorf("%w: %s is %q, not %q",
				ErrRoleConflict, existing.ID, existing.Role, role)
		}
		copied := *existing
		return &copied, false, nil
	}
	m := &TenantMembership{
		ID:         membershipID(accountID, customerID),
		AccountID:  accountID,
		CustomerID: customerID,
		Role:       role,
		CreatedAt:  time.Now().UTC(),
	}
	s.indexMembership(m)
	// Unlike the account/customer writes, this one is NOT best-effort. Nothing
	// re-drives a membership: a human never re-creates their own grant, so a
	// silently dropped write loses the tenant binding at the next restart with
	// no path back. Roll the in-memory insert back too, so the caller's error
	// and the store agree and a provisioning caller can abort cleanly.
	//
	// The audit row rides the same write, so a refused journal takes the grant
	// with it: there is no ordering here that leaves access granted and
	// unrecorded.
	if err := s.pUpsertMembership(m, NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionMembershipGrant,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetMembership,
		TargetID:   accountID,
		Detail:     AuditDetail{Role: role},
	})); err != nil {
		s.unindexMembership(m)
		return nil, false, fmt.Errorf("%w: persist membership %s: %w", ErrPersistence, m.ID, err)
	}
	copied := *m
	return &copied, true, nil
}

// SetTenantMembershipRole changes an EXISTING grant's role on an operator's
// behalf, and is the other half of the seam GrantTenantMembership deliberately
// left open: a re-grant with a different role is ErrRoleConflict rather than a
// silent overwrite, so correcting a mistyped role needs a call that says it is
// changing one.
//
// There is no caller account here, and that is the whole difference from
// RemoveTenantMembership. An operator is not a member of the tenant they are
// fixing, so this method takes no callerAccountID and invents none: passing an
// owner's id to reuse the human path would make the operator route's authority
// depend on some member's role, and the AdminAuth credential is the authority.
// What it does NOT skip is the durable rules, which belong to the tenant rather
// than to whoever asked:
//
//   - An unknown role is ErrInvalidRole, and never reaches durable state.
//   - An unknown tenant, a revoked one and an account with no grant on it are
//     one ErrNotFound, exactly as membershipForLocked collapses them.
//   - The same role again writes nothing and is not a change.
//   - Demoting the LAST owner is ErrLastOwner, for the same reason removing them
//     is: an ownerless tenant is unrecoverable through the human surface.
//
// It returns the updated grant and the role it replaced, so a caller can tell a
// real change from a no-op — and audit only the former — without a pre-read
// that would be reading one lock earlier than it acts. The journal follows the
// same rule from inside the lock: the same role again writes no row.
func (s *Store) SetTenantMembershipRole(accountID, customerID, role string, by Actor) (*TenantMembership, string, error) {
	if !ValidRole(role) {
		return nil, "", fmt.Errorf("%w: %q", ErrInvalidRole, role)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.persist != nil {
		result, err := s.mutateMembershipLocked(membershipMutation{operation: membershipRole, customer: customerID, target: accountID, role: role, by: by})
		return result.member, result.previous, err
	}
	m, err := s.cachedMembershipForLocked(accountID, customerID)
	if err != nil {
		return nil, "", err
	}
	return s.setMembershipRoleLocked(m, role, by)
}

// setMembershipRoleLocked is SetTenantMembershipRole's body once the target
// grant is in hand, shared with the human path for grantMembershipLocked's
// reason: the no-op rule, the owner floor, the durable write and its rollback
// belong to the tenant, and two copies of them would drift. m is the STORED
// pointer, so callers hold s.mu and nothing retains it past the lock.
func (s *Store) setMembershipRoleLocked(m *TenantMembership, role string, by Actor) (*TenantMembership, string, error) {
	customerID := m.CustomerID
	previous := m.Role
	if previous == role {
		copied := *m
		return &copied, previous, nil
	}
	if previous == RoleOwner && s.ownerCountLocked(customerID) < 2 {
		return nil, "", fmt.Errorf("%w: %s", ErrLastOwner, customerID)
	}
	// Both indexes hold the same pointer, so the role changes in one write and
	// the rollback is the mirror of it — the same shape as the insert/unindex
	// pair above, and for the same reason: a role that only landed in memory is
	// authority the next restart quietly takes back.
	m.Role = role
	if err := s.pUpsertMembership(m, NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionMembershipRoleChange,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetMembership,
		TargetID:   m.AccountID,
		Detail:     AuditDetail{Role: role, PreviousRole: previous},
	})); err != nil {
		m.Role = previous
		return nil, "", fmt.Errorf("%w: persist membership %s: %w", ErrPersistence, m.ID, err)
	}
	copied := *m
	return &copied, previous, nil
}

// DeleteTenantMembership removes one grant on an operator's behalf. Like
// SetTenantMembershipRole it takes no caller account, because an operator holds
// no membership on the tenant they are correcting; unlike it, the target is
// allowed to be absent.
//
// The order is the contract:
//
//   - The tenant goes first, so an unknown or revoked one is ErrNotFound rather
//     than the idempotent success an absent grant gets. Answering 204 for a
//     tenant that does not exist would report a revocation on nothing.
//   - An absent grant on a LIVE tenant is a no-op success (nil, nil), so a
//     retried removal does not fail.
//   - The last owner is refused whoever asks, operator included. An operator can
//     grant a replacement owner first; nothing can un-strand a tenant whose only
//     owner was deleted by the route meant to fix it.
//
// It returns the grant it removed, or nil for the no-op, so the audit log
// records access that was actually revoked — and the journal row is written on
// the same rule, from inside the lock, so an idempotent retry claims nothing.
func (s *Store) DeleteTenantMembership(accountID, customerID string, by Actor) (*TenantMembership, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.persist != nil {
		result, err := s.mutateMembershipLocked(membershipMutation{operation: membershipRemove, customer: customerID, target: accountID, by: by})
		return result.member, err
	}
	if c := s.customers[customerID]; c == nil || c.Revoked() {
		return nil, ErrNotFound
	}
	target := s.membershipsByTenant[customerID][accountID]
	if target == nil {
		return nil, nil
	}
	if target.Role == RoleOwner && s.ownerCountLocked(customerID) < 2 {
		return nil, fmt.Errorf("%w: %s", ErrLastOwner, customerID)
	}
	s.unindexMembership(target)
	if err := s.pDeleteMembership(target.ID, NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionMembershipRemove,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetMembership,
		TargetID:   accountID,
		Detail:     AuditDetail{PreviousRole: target.Role},
	})); err != nil {
		s.indexMembership(target)
		return nil, fmt.Errorf("%w: delete membership %s: %w", ErrPersistence, target.ID, err)
	}
	removed := *target
	return &removed, nil
}

// MembershipsForAccount lists one account's tenant bindings, sorted by customer
// id so a response is stable across calls. Returns an empty slice for an unknown
// account — an account with no memberships and an account that does not exist
// are the same amount of access.
func (s *Store) MembershipsForAccount(accountID string) []*TenantMembership {
	// Legacy inspection API: unavailable authority returns no rows. Request
	// paths must use the error-returning context API (or AccountTenantsContext).
	out, _ := s.MembershipsForAccountContext(context.Background(), accountID)
	return out
}

// MembershipsForTenant lists one tenant's members, sorted by account id so a
// roster is byte-stable across calls and across replicas. Copies, like
// MembershipsForAccount: the caller renders these after the lock is released,
// and the stored pointers are what a concurrent removal unindexes.
//
// A tenant that does not exist, and one that is revoked, both return an empty
// slice — a revoke deletes the grants with the tenant, so there is nothing to
// list either way. Authorization is NOT this accessor's job; MembershipFor
// decides whether the caller may see a roster at all.
func (s *Store) MembershipsForTenant(customerID string) []*TenantMembership {
	// Use MembershipsForTenantContext to distinguish empty from unavailable.
	out, _ := s.MembershipsForTenantContext(context.Background(), customerID)
	return out
}

// Roster page bounds. A tenant's member list is unbounded in the data model —
// nothing caps how many humans a tenant may grant — so the READ is what bounds
// it, and it is bounded in the store rather than in the handler: an unbounded
// roster is a response the store built, whatever the caller asked for.
const (
	DefaultRosterLimit = 100
	MaxRosterLimit     = 500
)

// RosterQuery bounds one page of a tenant roster. The zero value is the first
// page at the default limit.
type RosterQuery struct {
	// Limit caps the rows in the page. Zero or negative means
	// DefaultRosterLimit; anything above MaxRosterLimit is clamped to it. The
	// handler rejects an out-of-range limit with 400 before it gets here — the
	// clamp is for every other caller, so no path can ask for the whole roster.
	Limit int
	// After is an exclusive account-id cursor: only members ordered after it
	// are in the page. Empty starts at the first member.
	After string
}

// TenantMember is one row of a roster snapshot: a membership joined to its
// account, copied out under the store's lock. Values rather than pointers, and
// a copy of every rendered field, because the whole point of the snapshot is
// that the caller renders it after the lock is gone.
type TenantMember struct {
	AccountID string
	Email     string
	Name      string
	Role      string
	CreatedAt time.Time
}

// TenantRoster is one page of a tenant's members.
type TenantRoster struct {
	Members []TenantMember
	// NextAfter is the cursor for the next page, and is empty when this page is
	// the last one — so a caller can tell "the roster ends here" from "there is
	// more" without a second call that returns nothing.
	NextAfter string
}

// TenantRosterFor answers tenant liveness, caller authority and the bounded
// member page in one snapshot: a PostgreSQL read-only repeatable-read
// transaction, or one lock for the intentionally in-memory Store. A concurrent
// revoke falls before or after that snapshot, never between its two halves.
// A grant with a missing account remains visible without profile fields so an
// operator can identify and remove it; a present but corrupt durable row fails
// the response. ErrNotFound conceals unknown/revoked tenants and non-members;
// ErrNotAuthorized denies non-managers; ErrPersistence reports unavailable data.
func (s *Store) TenantRosterFor(customerID, callerAccountID string, q RosterQuery) (TenantRoster, error) {
	return s.TenantRosterForContext(context.Background(), customerID, callerAccountID, q)
}

func (s *Store) cachedTenantRosterFor(customerID, callerAccountID string, q RosterQuery) (TenantRoster, error) {
	limit := rosterLimit(q.Limit)
	s.mu.RLock()
	defer s.mu.RUnlock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return TenantRoster{}, err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		return TenantRoster{}, fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, caller.Role)
	}
	grants := s.membershipsByTenant[customerID]
	ids := make([]string, 0, len(grants))
	for accountID := range grants {
		if accountID > q.After {
			ids = append(ids, accountID)
		}
	}
	slices.Sort(ids)
	roster := TenantRoster{Members: make([]TenantMember, 0, min(len(ids), limit))}
	for _, accountID := range ids {
		if len(roster.Members) == limit {
			// There is at least one more row, so hand back the cursor that
			// resumes at it. Set only here: an exactly-full last page reports no
			// next cursor because the loop simply ends.
			roster.NextAfter = roster.Members[limit-1].AccountID
			break
		}
		m := grants[accountID]
		member := TenantMember{AccountID: accountID, Role: m.Role, CreatedAt: m.CreatedAt}
		if a := s.accounts[accountID]; a != nil {
			member.Email, member.Name = a.Email, a.Name
		}
		roster.Members = append(roster.Members, member)
	}
	return roster, nil
}

// TenantUsage is what one tenant's own members may see of what it is running
// right now: the live bursts, and the ceilings those bursts are measured
// against. It is a SNAPSHOT of the working set — there is no ledger behind it,
// nothing is accrued into it, and a burst that ends leaves it — so nothing
// built on it may present itself as a bill or a history.
//
// Role is the grant that authorized the read, carried back so a caller can
// render the tenant without a second lookup that would be reading one lock
// later than it authorized.
type TenantUsage struct {
	Role                string
	MaxConcurrentBursts int
	MaxHourlyUSD        float64
	// Bursts are copies, for the reason every human-surface read hands out
	// copies: the reaper rewrites these records, and a caller summing fields off
	// the stored pointers would be reading a burst mid-teardown.
	Bursts []*Burst
}

// TenantUsageFor answers what a tenant is running in ONE read lock: is the
// tenant live, does this caller hold a grant on it, and what is running. It is
// TenantRosterFor's shape and for its reason — a revoke landing between the
// authorization and the rows would render another instant's usage — but not its
// authority: EVERY role sees the tenant's own usage, including a viewer, which
// is what a read-only seat is for. What no role gets is another tenant's, and
// the membership check is what makes that so.
//
// Errors are MembershipFor's, unchanged: ErrNotFound for an unknown tenant, a
// revoked one and a non-member alike. There is no ErrNotAuthorized here because
// there is no role bar to fail.
func (s *Store) TenantUsageFor(customerID, callerAccountID string) (TenantUsage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return TenantUsage{}, err
	}
	// membershipForLocked already refused an absent or revoked tenant, so this
	// is the live record; the limits come off it under the same lock the bursts
	// are counted under, or the answer mixes a new ceiling with old usage.
	c := s.customers[customerID]
	usage := TenantUsage{
		Role:                caller.Role,
		MaxConcurrentBursts: c.MaxConcurrentBursts,
		MaxHourlyUSD:        c.MaxHourlyUSD,
		Bursts:              make([]*Burst, 0),
	}
	for _, b := range s.bursts {
		if b.CustomerID != customerID {
			continue
		}
		copied := *b
		usage.Bursts = append(usage.Bursts, &copied)
	}
	return usage, nil
}

// TenantClusterPolicyFor returns the effective cluster policy visible to any
// member of a live tenant. The stored nil policy is rendered as the default
// require-pin policy, but remains nil in state so old routing stays unchanged
// until a non-default policy is saved.
func (s *Store) TenantClusterPolicyFor(customerID, callerAccountID string) (ClusterPolicy, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return ClusterPolicy{}, "", err
	}
	c := s.customers[customerID]
	return c.ClusterPolicy.Effective(), caller.Role, nil
}

// SetTenantClusterPolicy replaces a tenant's placement policy. Owner/admin may
// write it; all other roles get ErrNotAuthorized. A behaviorally-identical
// replacement is still written durably but produces no audit row, so a seeded
// in-memory record can reconcile a best-effort boot write that never landed. A
// default policy is stored as nil, preserving no-policy routing behavior.
func (s *Store) SetTenantClusterPolicy(customerID, callerAccountID string, policy ClusterPolicy, by Actor) (ClusterPolicy, bool, string, error) {
	normalized, err := ValidateClusterPolicy(policy)
	if err != nil {
		return ClusterPolicy{}, false, "", err
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return ClusterPolicy{}, false, "", err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		s.mu.Unlock()
		return ClusterPolicy{}, false, caller.Role, fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, caller.Role)
	}
	c := s.customers[customerID]
	unchanged := sameClusterPolicy(c.ClusterPolicy, normalized)
	snapshot := *c
	snapshot.ClusterPolicy = copyClusterPolicy(normalized)
	role := caller.Role
	s.mu.Unlock()

	var ev *AuditEvent
	if !unchanged {
		ev = NewAuditEvent(AuditEvent{
			CustomerID: customerID,
			Actor:      by,
			Action:     ActionClusterPolicySet,
			Outcome:    OutcomeAccepted,
			TargetKind: TargetTenant,
			TargetID:   customerID,
			Detail: AuditDetail{
				Reason:        ReasonClusterPolicyUpdated,
				Role:          role,
				Rule:          clusterPolicyRule,
				RuleVersion:   clusterPolicyRuleVersion,
				ClusterPolicy: copyClusterPolicy(normalized),
			},
		})
	}
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return ClusterPolicy{}, false, role, fmt.Errorf("%w: persist cluster policy %s: %w", ErrPersistence, customerID, err)
	}
	if unchanged {
		return normalized.Effective(), false, role, nil
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.ClusterPolicy = copyClusterPolicy(normalized)
	}
	s.mu.Unlock()
	return normalized.Effective(), true, role, nil
}

// MembershipFor returns a copy of one account's membership in one tenant, and
// is the authorization seam the tenant-scoped human routes ask first.
//
// ErrNotFound covers all three ways a caller has no business with a tenant: the
// tenant does not exist, it is revoked, or this account is not a member. They
// are deliberately indistinguishable. A caller who is not a member must not be
// able to tell a tenant id that exists from one that does not, or the route
// becomes an oracle for enumerating other people's tenants.
func (s *Store) MembershipFor(accountID, customerID string) (*TenantMembership, error) {
	return s.MembershipForContext(context.Background(), accountID, customerID)
}

// membershipForLocked decides human authority for callers already holding
// s.mu. PostgreSQL authorization is current and detached; local stores return
// the stored pointer. Callers may inspect but never mutate the result. The
// durable read is bounded even when this lock also protects a local decision.
func (s *Store) membershipForLocked(accountID, customerID string) (*TenantMembership, error) {
	if s.persist != nil {
		return readHumanMembership(context.Background(), s.persist, accountID, customerID)
	}
	return s.cachedMembershipForLocked(accountID, customerID)
}

func (s *Store) cachedMembershipForLocked(accountID, customerID string) (*TenantMembership, error) {
	if c := s.customers[customerID]; c == nil || c.Revoked() {
		return nil, ErrNotFound
	}
	m := s.membershipsByAccount[accountID][customerID]
	if m == nil {
		return nil, ErrNotFound
	}
	return m, nil
}

// RemoveTenantMembership revokes one human's access to one tenant on behalf of
// another. Every check and the durable delete happen under ONE write lock,
// because the thing being enforced is a property of the whole member set: two
// owners removing each other concurrently would each read "another owner
// exists" and both commit, leaving an ownerless tenant that nothing can fix.
// Holding the lock across the durable write is the same trade AddTenantMembership
// makes, and for the same reason — a grant that exists in memory and not on disk
// is access that disappears at the next restart.
//
// The order of the checks is the security contract, not a style choice:
//
//   - The tenant and the CALLER's own membership go first, and both answer
//     ErrNotFound, so an unknown tenant, a revoked one and a non-member are one
//     answer. Nothing about the tenant is disclosed to someone with no grant on it.
//   - The caller's authority goes next. A member or viewer is ErrNotAuthorized:
//     they already know the tenant exists, so there is nothing left to hide.
//   - Only then is the TARGET looked up. An absent target is a no-op success —
//     the operation is idempotent, so a retried removal does not fail — and
//     answering that before the two checks above would turn this route into the
//     probe MembershipFor exists to prevent.
//   - An admin may remove admin/member/viewer but never an owner (the distinct
//     ErrOwnerProtected); only an owner may remove an owner, including themselves.
//   - The last owner is refused (ErrLastOwner) whoever asks.
//
// It returns the grant it removed, or nil for the idempotent no-op, so a caller
// can tell a real removal from an already-absent target — and audit-log only
// the former — without a pre-read that would be reading one lock earlier than
// it acts.
//
// by names the principal the removal is attributed to. It is separate from
// callerAccountID, which is who the store CHECKED: the two agree on this route
// and deliberately do not on the operator's, where the credential taking the
// action is not a member of the tenant at all.
func (s *Store) RemoveTenantMembership(customerID, callerAccountID, targetAccountID string, by Actor) (*TenantMembership, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.persist != nil {
		result, err := s.mutateMembershipLocked(membershipMutation{operation: membershipRemove, human: true, customer: customerID, caller: callerAccountID, target: targetAccountID, by: by})
		return result.member, err
	}
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return nil, err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		return nil, fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, caller.Role)
	}
	target := s.membershipsByTenant[customerID][targetAccountID]
	if target == nil {
		return nil, nil
	}
	if target.Role == RoleOwner {
		if caller.Role != RoleOwner {
			return nil, fmt.Errorf("%w: %s is %q", ErrOwnerProtected, caller.ID, caller.Role)
		}
		if s.ownerCountLocked(customerID) < 2 {
			return nil, fmt.Errorf("%w: %s", ErrLastOwner, customerID)
		}
	}
	// Unindex first so the delete and its rollback are symmetric, exactly as
	// AddTenantMembership's insert is: nothing else can observe the gap while
	// s.mu is held, and a refused durable delete leaves the store as it was.
	s.unindexMembership(target)
	if err := s.pDeleteMembership(target.ID, NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionMembershipRemove,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetMembership,
		TargetID:   targetAccountID,
		Detail:     AuditDetail{Role: caller.Role, PreviousRole: target.Role},
	})); err != nil {
		s.indexMembership(target)
		return nil, fmt.Errorf("%w: delete membership %s: %w", ErrPersistence, target.ID, err)
	}
	// A copy: the stored record is out of both indexes but the caller is about
	// to render fields off it, and nothing may retain a store pointer.
	removed := *target
	return &removed, nil
}

// AddTenantMemberByEmail grants one human access to one tenant on behalf of
// another, naming the target by the email address on their account rather than
// by an account id no colleague could know. It is RemoveTenantMembership's
// mirror image, and every reason that method takes the whole decision under one
// write lock applies here unchanged: the caller's authority, the target's
// identity, the owner floor, the durable write and its audit row are one
// snapshot, or an admin racing their own demotion still gets to mint an owner.
//
// The order of the checks is the security contract:
//
//   - The tenant and the CALLER's own grant go first and answer ErrNotFound
//     together, so an unknown tenant, a revoked one and a non-member are one
//     answer. Nothing about the tenant is disclosed to someone with no grant.
//   - The caller's authority goes next: a member or viewer is ErrNotAuthorized.
//   - Then the ROLE being granted, which needs no knowledge of the target: an
//     admin asking for an owner is ErrOwnerRestricted before any email is looked
//     at, so the refusal cannot double as an existence check.
//   - Only then is the email resolved, and only ever to an account of THIS
//     issuer whose address the issuer says is verified. Unknown, unverified and
//     ambiguous are one ErrTargetNotFound: an unverified address is not proof of
//     who holds it, and a second account on the same address is a decision this
//     route must not make on a manager's behalf.
//   - An existing OWNER grant is out of an admin's reach (ErrOwnerRestricted),
//     for the reason an owner is out of their reach to remove.
//
// Idempotence and conflict are grantMembershipLocked's, shared with the
// operator route: the same role again writes nothing and is not a change, and a
// different role is ErrRoleConflict rather than a silent overwrite — changing a
// role is SetTenantMemberRole's job, and it says so.
//
// by names the principal the grant is attributed to, separate from
// callerAccountID for RemoveTenantMembership's reason: this route's two agree,
// and the operator route's deliberately do not.
func (s *Store) AddTenantMemberByEmail(customerID, callerAccountID, issuer, email, role string, by Actor) (*TenantMembership, bool, error) {
	if !ValidRole(role) {
		return nil, false, fmt.Errorf("%w: %q", ErrInvalidRole, role)
	}
	issuer, email, err := normalizeMemberEmail(issuer, email)
	if err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.persist != nil {
		result, err := s.mutateMembershipLocked(membershipMutation{operation: membershipGrant, human: true, customer: customerID, caller: callerAccountID, issuer: issuer, email: email, role: role, by: by})
		return result.member, result.created, err
	}
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return nil, false, err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		return nil, false, fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, caller.Role)
	}
	if role == RoleOwner && caller.Role != RoleOwner {
		return nil, false, fmt.Errorf("%w: %s is %q", ErrOwnerRestricted, caller.ID, caller.Role)
	}
	target, err := s.verifiedAccountByEmailLocked(issuer, email)
	if err != nil {
		return nil, false, err
	}
	if existing := s.membershipsByAccount[target.ID][customerID]; existing != nil &&
		existing.Role == RoleOwner && caller.Role != RoleOwner {
		return nil, false, fmt.Errorf("%w: %s is %q", ErrOwnerRestricted, caller.ID, caller.Role)
	}
	return s.grantMembershipLocked(target.ID, customerID, role, by)
}

// SetTenantMemberRole changes an existing member's role on behalf of another
// human. It is AddTenantMemberByEmail's other half — a re-grant with a
// different role is a conflict, so correcting one takes a call that says it is
// changing a role — and it takes the same decisions under the same lock, in the
// same order.
//
// What it adds is the pair of owner rules an admin must not be able to route
// around: they may not promote anyone to owner (ErrOwnerRestricted, decided
// from the requested role alone), and they may not change an existing owner's
// role (the same error, decided once the target is known). An owner may do
// both, subject to the floor every path shares — demoting the last owner is
// ErrLastOwner, because an ownerless tenant is unrecoverable through this
// surface.
//
// A target holding no grant on this tenant is ErrTargetNotFound, not the
// idempotent success an absent removal gets: there is no role to change, and
// reporting one changed would be a lie a roster read immediately contradicts.
// It returns the updated grant and the role it replaced, so a caller can tell a
// real change from a no-op without a pre-read.
func (s *Store) SetTenantMemberRole(customerID, callerAccountID, targetAccountID, role string, by Actor) (*TenantMembership, string, error) {
	if !ValidRole(role) {
		return nil, "", fmt.Errorf("%w: %q", ErrInvalidRole, role)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.persist != nil {
		result, err := s.mutateMembershipLocked(membershipMutation{operation: membershipRole, human: true, customer: customerID, caller: callerAccountID, target: targetAccountID, role: role, by: by})
		return result.member, result.previous, err
	}
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return nil, "", err
	}
	if caller.Role != RoleOwner && caller.Role != RoleAdmin {
		return nil, "", fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, caller.Role)
	}
	if role == RoleOwner && caller.Role != RoleOwner {
		return nil, "", fmt.Errorf("%w: %s is %q", ErrOwnerRestricted, caller.ID, caller.Role)
	}
	target := s.membershipsByTenant[customerID][targetAccountID]
	if target == nil {
		return nil, "", ErrTargetNotFound
	}
	if target.Role == RoleOwner && caller.Role != RoleOwner {
		return nil, "", fmt.Errorf("%w: %s is %q", ErrOwnerRestricted, caller.ID, caller.Role)
	}
	return s.setMembershipRoleLocked(target, role, by)
}

// normalizeMemberEmail puts a caller-typed address into the form the stored
// profile is compared in: trimmed, and matched case-insensitively below. A
// blank issuer or address is ErrInvalidIdentity, exactly as normalizeIdentity's
// blank pair is — an empty address would otherwise match every account whose
// issuer never supplied one.
func normalizeMemberEmail(issuer, email string) (string, string, error) {
	issuer, email = strings.TrimSpace(issuer), strings.TrimSpace(email)
	if issuer == "" || email == "" {
		return "", "", ErrInvalidIdentity
	}
	return issuer, email, nil
}

// verifiedAccountByEmailLocked resolves an email to the ONE account of this
// issuer that holds it verified. Callers hold s.mu.
//
// Email is not an identity key — the Account doc says why: it is mutable,
// reassignable and not unique across issuers — so this is a lookup for a human
// naming a colleague, never an authorization key. That is why the whole scan
// has to be unambiguous: two accounts on one address means the store cannot say
// which human a manager meant, and picking either would hand a tenant to the
// wrong one. Unverified rows are invisible for the same reason: an address the
// issuer has not confirmed is a claim, not a person.
//
// Linear scan over the accounts map, like BurstsForCustomer's: the signed-in
// population is small in v0 and this runs once per roster mutation.
func (s *Store) verifiedAccountByEmailLocked(issuer, email string) (*Account, error) {
	var found *Account
	for _, a := range s.accounts {
		if a.Issuer != issuer || !a.EmailVerified || !strings.EqualFold(strings.TrimSpace(a.Email), email) {
			continue
		}
		if found != nil {
			return nil, ErrTargetNotFound
		}
		found = a
	}
	if found == nil {
		return nil, ErrTargetNotFound
	}
	return found, nil
}

// ownerCountLocked counts a tenant's owners. Callers hold s.mu.
func (s *Store) ownerCountLocked(customerID string) int {
	n := 0
	for _, m := range s.membershipsByTenant[customerID] {
		if m.Role == RoleOwner {
			n++
		}
	}
	return n
}

// indexMembership writes one membership into both indexes. Shared by
// applySnapshot and AddTenantMembership; callers hold s.mu (or own the store).
func (s *Store) indexMembership(m *TenantMembership) {
	if s.membershipsByAccount[m.AccountID] == nil {
		s.membershipsByAccount[m.AccountID] = make(map[string]*TenantMembership)
	}
	s.membershipsByAccount[m.AccountID][m.CustomerID] = m
	if s.membershipsByTenant[m.CustomerID] == nil {
		s.membershipsByTenant[m.CustomerID] = make(map[string]*TenantMembership)
	}
	s.membershipsByTenant[m.CustomerID][m.AccountID] = m
}

// unindexMembership is indexMembership's inverse, used to undo an in-memory
// insert whose durable write failed.
func (s *Store) unindexMembership(m *TenantMembership) {
	if byAcct := s.membershipsByAccount[m.AccountID]; byAcct != nil {
		delete(byAcct, m.CustomerID)
		if len(byAcct) == 0 {
			delete(s.membershipsByAccount, m.AccountID)
		}
	}
	if byTenant := s.membershipsByTenant[m.CustomerID]; byTenant != nil {
		delete(byTenant, m.AccountID)
		if len(byTenant) == 0 {
			delete(s.membershipsByTenant, m.CustomerID)
		}
	}
}

// AuthCustomer is the context-free authentication API. Request handlers use
// AuthCustomerContext so database reads share the request cancellation.
func (s *Store) AuthCustomer(token string) (*Customer, error) {
	return s.AuthCustomerContext(context.Background(), token)
}

// CustomerByID resolves a customer ID to a Customer. ErrNotFound on miss.
// Used by the decider to look up a customer's mesh endpoint when only the
// ID is available (e.g. from PlanOptions / a stored Burst) — and by offboard,
// which needs a REVOKED customer to come back so a half-finished cleanup can be
// retried against it.
func (s *Store) CustomerByID(id string) (*Customer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.customers[id]
	if !ok {
		return nil, ErrNotFound
	}
	return c, nil
}

// The four setters below all read-modify-WRITE THE WHOLE customer row, which
// puts them in the same class as the lifecycle trio: two of them racing, or one
// racing a revoke, means the loser's field is silently absent from the durable
// document — and when the loser is the revoke, the field that goes missing is
// the stamp that stops an offboarded token from authenticating after the next
// restart. So they take custMu, exactly like AddCustomer and RevokeCustomer do,
// and s.mu is held only around the map read and the field write, never across
// the round trip to the database. Lock order is custMu then mu, never the
// reverse. The durable write goes out from a copy taken under s.mu, so nothing
// reads the live record while the backend is marshalling it.

// TenantSummary is the immutable snapshot of a customer that the human account
// surface renders. It is a detached VALUE from SQL (or under the in-memory
// store lock), because
// CustomerByID hands back the live record: a caller that reads Plan or
// RevokedAt off that pointer after the lock is released is racing every writer
// of the row — including the revoke, whose whole job is to stamp the field the
// account surface then checks. Copying at the seam is also narrower than
// copying in CustomerByID, whose callers (the decider, the mesh policy
// reconciler) want the live record.
//
// Token and Mesh are absent by construction, not by filtering: the surface that
// consumes this is reachable with a human's access token, and neither belongs
// anywhere near it.
type TenantSummary struct {
	ID                  string
	Name                string
	Plan                string
	MaxConcurrentBursts int
	MaxHourlyUSD        float64
	Revoked             bool
	// WorkloadNamespaces travels with the summary because the console's
	// workload routes rebuild a Customer from it before submitting. Drop
	// it here and every console submission is authorized against the
	// fail-closed set instead of the tenant's own.
	WorkloadNamespaces []string
	ClusterPolicy      *ClusterPolicy
	// TemplateCatalog travels for the same reason WorkloadNamespaces does: the
	// console's submit path rebuilds a Customer from this summary, and a
	// summary that dropped it would judge every console submission against the
	// server defaults instead of the tenant's own published catalog.
	TemplateCatalog *WorkloadTemplateCatalog
}

// TenantSummaryByID returns the account-surface view of one customer.
// ErrNotFound on miss. A revoked customer still resolves — the caller decides
// what a revoked tenant means to it — but says so in Revoked.
func (s *Store) TenantSummaryByID(id string) (TenantSummary, error) {
	return s.TenantSummaryByIDContext(context.Background(), id)
}

// SetCustomerMesh attaches (or clears, when ep is nil) a customer's
// per-customer coordination-server endpoint. This is the write-side of the
// coordination-box factory: registration provisions a box, then records its
// {LoginServer, APIKey, User, BackendID} here so the decider's meshFor()
// routes that customer's bursts/agents to their own coordination server.
// Clearing (ep=nil) reverts the customer to the shared Tailscale path.
func (s *Store) SetCustomerMesh(customerID string, ep *MeshEndpoint) error {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.RLock()
	c, ok := s.customers[customerID]
	if !ok {
		s.mu.RUnlock()
		return ErrNotFound
	}
	snapshot := *c
	s.mu.RUnlock()
	if ep != nil {
		mesh := *ep
		snapshot.Mesh = &mesh
	} else {
		snapshot.Mesh = nil
	}
	if err := s.pSetCustomer(&snapshot, nil); err != nil {
		return err
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil {
		live.Mesh = snapshot.Mesh
	}
	s.mu.Unlock()
	return nil
}

// AttachMeshIfUnset records ep for an active customer that has no mesh yet,
// atomically. It reports false (no write) when the customer already has an
// endpoint — for example an env-attached box — or is revoked, so a delayed
// factory poll can neither overwrite another attachment nor re-arm a tenant
// whose offboarding has begun.
func (s *Store) AttachMeshIfUnset(customerID string, ep *MeshEndpoint) (bool, error) {
	if ep == nil {
		return false, errors.New("state: attach nil mesh endpoint")
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.RLock()
	c, ok := s.customers[customerID]
	if !ok {
		s.mu.RUnlock()
		return false, ErrNotFound
	}
	if c.Mesh != nil || c.Revoked() {
		s.mu.RUnlock()
		return false, nil
	}
	snapshot := *c
	s.mu.RUnlock()
	mesh := *ep
	snapshot.Mesh = &mesh
	if err := s.pSetCustomer(&snapshot, nil); err != nil {
		return false, err
	}
	s.mu.Lock()
	if live := s.customers[customerID]; live != nil {
		live.Mesh = snapshot.Mesh
	}
	s.mu.Unlock()
	return true, nil
}

// FactoryMeshCustomerIDs names, deterministically, every active tenant whose
// mesh is a factory-provisioned box. It is the startup replay list.
func (s *Store) FactoryMeshCustomerIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for _, customerID := range slices.Sorted(maps.Keys(s.customers)) {
		if c := s.customers[customerID]; c.Mesh != nil && c.Mesh.Provider == MeshProviderFactory && !c.Revoked() {
			out = append(out, customerID)
		}
	}
	return out
}

// SetCustomerWorkloadNamespaces replaces the namespaces a tenant may submit
// workloads into and returns the stored set. The caller validates the list
// (handlers.validateWorkloadNamespaces); this is the durable half.
//
// Unlike the other Set* helpers on this type, a failed durable write is
// RETURNED and the in-memory change is never published. Those record operational
// state that re-derives itself — a mesh attach, a route set the agent reports
// again on its next connection. This is an authorization boundary: an operator
// told "namespaces updated" whose write never landed has a tenant that submits
// into team-b today and is refused after the next restart, with the connector's
// RBAC already widened to match the change central forgot. The durable write
// happens before the in-memory pointer changes, so workload readers cannot
// observe an authorization set the database may still refuse. custMu keeps
// another customer writer from racing that write and publication.
//
// A revoked tenant is ErrNotFound. Its token authenticates nothing, so there is
// no submission left to authorize, and widening a revoked tenant's namespaces is
// a change the operator would not see take effect.
//
// A set that omits a standing hosted assignment's reserved namespace is
// ErrHostedNamespaceRequired, with the namespace named. The assignment keeps
// that namespace authorized by construction (AssignHostedCluster merged it
// in), so accepting the omission would 403 every hosted submit while the
// console still offers the target; releasing the reservation is
// DeleteHostedCluster's job.
//
// by attributes the replacement, and the audit row rides the same durable write
// as the customer document — a widened authorization boundary that landed
// without a journal row is precisely the change nobody could later account for.
// Setting the set a tenant already has AUDITS nothing: it is not a change, and a
// journal that recorded one would put a boundary move in the evidence that never
// happened.
//
// It still WRITES, though, with a nil audit event. The two are different
// questions, and conflating them cost a live tenant its set: an in-memory record
// whose durable write failed once — an env-seeded customer, whose row is written
// best-effort at boot — matches every later request for the same list, so a
// version that skipped the write entirely made the operator's correcting call a
// no-op forever, with no error to show for it. Re-writing the document
// reconciles the durable row against the memory the answer is taken from, and
// costs one upsert on a route only an operator can reach. What it must not do is
// republish: the in-memory pointer is already this value, so there is no
// transient widening to make visible, and touching it would only add a window.
func (s *Store) SetCustomerWorkloadNamespaces(customerID string, namespaces []string, by Actor) ([]string, error) {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok || c.Revoked() {
		s.mu.Unlock()
		return nil, ErrNotFound
	}
	if ns, omitted := hostedNamespaceOmittedLocked(c, namespaces); omitted {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrHostedNamespaceRequired, ns)
	}
	stored := slices.Clone(namespaces)
	unchanged := slices.Equal(c.WorkloadNamespaces, stored)
	snapshot := *c
	snapshot.WorkloadNamespaces = slices.Clone(stored)
	s.mu.Unlock()

	var ev *AuditEvent
	if !unchanged {
		ev = NewAuditEvent(AuditEvent{
			CustomerID: customerID,
			Actor:      by,
			Action:     ActionWorkloadNamespacesSet,
			Outcome:    OutcomeAccepted,
			TargetKind: TargetTenant,
			TargetID:   customerID,
			Detail:     AuditDetail{Namespaces: slices.Clone(stored)},
		})
	}
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return nil, err
	}
	if unchanged {
		return stored, nil
	}
	s.mu.Lock()
	c.WorkloadNamespaces = slices.Clone(stored)
	s.mu.Unlock()
	return stored, nil
}

// AddAgent registers a connected agent.
func (s *Store) AddAgent(a *Agent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agents[a.ID] = a
	if s.agentsByCust[a.CustomerID] == nil {
		s.agentsByCust[a.CustomerID] = make(map[string]*Agent)
	}
	s.agentsByCust[a.CustomerID][a.ID] = a
	s.Cost.AgentConnected()
}

// RemoveAgent drops an agent (called on disconnect). Idempotent: an agent a
// rotation or delete already evicted is not in the maps, so the stream
// cleanup's call finds nothing and cannot double-decrement the gauge.
func (s *Store) RemoveAgent(agentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[agentID]
	if !ok {
		return
	}
	delete(s.agents, agentID)
	if m := s.agentsByCust[a.CustomerID]; m != nil {
		delete(m, agentID)
	}
	a.Evict()
	s.Cost.AgentDisconnected()
}

// evictClusterAgentsLocked tears every live agent for (customerID, clusterID)
// out of routing: removed from both agent indexes so no new lookup can reach
// it, gauge decremented exactly once per socket, pumps signalled to stop. It
// runs under s.mu — inside the same critical section that publishes a
// rotation or delete — so by the time that mutation returns, the old
// connector is not a routable target on this replica.
func (s *Store) evictClusterAgentsLocked(customerID, clusterID string) {
	for id, a := range s.agentsByCust[customerID] {
		if a.ClusterID != clusterID {
			continue
		}
		delete(s.agents, id)
		delete(s.agentsByCust[customerID], id)
		a.Evict()
		s.Cost.AgentDisconnected()
	}
}

// CustomerForClusterID returns the customer owning clusterID. Used to reject a
// tenant naming another tenant's cluster in the ts-auth-key mint and on
// workload submit. The durable registry answers first — which is what makes
// the refusal hold with no connector live — and a connected agent covers the
// legacy cluster that has not been claimed into the registry yet (agents are
// dropped on disconnect); ok=false when unknown.
func (s *Store) CustomerForClusterID(clusterID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if owner, ok := s.clusterOwners[clusterID]; ok {
		return owner, true
	}
	for _, a := range s.agents {
		if a.ClusterID == clusterID {
			return a.CustomerID, true
		}
	}
	return "", false
}

// AgentsForCustomer returns the connected agents this replica holds for a
// customer, in a stable order: ClusterID ascending, then ConnectedAt
// descending, then ID ascending. So the first entry for any cluster is its
// NEWEST connection — which is the one a reconnecting connector left behind
// while the dead socket has not been reaped yet.
//
// The order is the whole contribution. Ranging a map gave a different answer
// per call, so "the customer's agent" was whichever entry Go felt like, and a
// multi-cluster tenant's workload landed on a different cluster each submit.
//
// LIVE pointers, deliberately: callers enqueue commands on Agent.Send and
// register ack waiters on the same record, so a copy would be a queue nobody
// drains. The slice is the caller's; the records are not.
func (s *Store) AgentsForCustomer(customerID string) []*Agent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.agentsForCustomerLocked(customerID)
}

func (s *Store) agentsForCustomerLocked(customerID string) []*Agent {
	m := s.agentsByCust[customerID]
	out := make([]*Agent, 0, len(m))
	for _, a := range m {
		out = append(out, a)
	}
	slices.SortFunc(out, func(a, b *Agent) int {
		if byCluster := strings.Compare(a.ClusterID, b.ClusterID); byCluster != 0 {
			return byCluster
		}
		if byConnected := b.ConnectedAt.Compare(a.ConnectedAt); byConnected != 0 {
			return byConnected
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out
}

// AgentForCluster returns the connected agent for ONE of a customer's clusters:
// the newest connection registered under clusterID, so a connector that
// reconnected before its old socket was reaped is addressed on the socket it is
// actually reading. ErrNotFound when this customer has no agent for that
// cluster — including when another customer does, so the lookup can never route
// across tenants.
func (s *Store) AgentForCluster(customerID, clusterID string) (*Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, a := range s.agentsForCustomerLocked(customerID) {
		if a.ClusterID == clusterID {
			return a, nil
		}
	}
	return nil, ErrNotFound
}

// AgentForCustomer returns the one connected agent a customer's commands can be
// routed to without the caller naming a cluster.
//
// Only defined while the answer is unambiguous. Duplicate sockets for a single
// cluster are still one answer — the newest of them — because a reconnect is
// one connector, not two. Two distinct cluster ids are ErrAmbiguousCluster:
// the caller has to say which cluster, since guessing spends money on the
// wrong one.
func (s *Store) AgentForCustomer(customerID string) (*Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	agents := s.agentsForCustomerLocked(customerID)
	if len(agents) == 0 {
		return nil, ErrNotFound
	}
	for _, a := range agents[1:] {
		if a.ClusterID != agents[0].ClusterID {
			return nil, ErrAmbiguousCluster
		}
	}
	return agents[0], nil
}

// PutWorkload stores or updates a workload. A whole-record write, so it carries
// the same field guards upsertWorkloadStmt puts on the durable row. The write is
// the one lifecycle path that REPLACES the map entry rather than mutating it,
// and its caller may have built that record before a concurrent targeted writer
// recorded cost or connector observations.
func (s *Store) PutWorkload(w *Workload) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stored, ok := s.workloads[w.ID]; ok {
		if terminalWorkloadBindingConflict(w, stored) {
			s.recordPersistenceFailure("workload", "upsert", fmt.Errorf("%w: terminal workload binding conflict", ErrPersistence))
			return
		}
		preserveWorkloadTransition(w, stored)
		if w.Cost == nil && stored.Cost != nil {
			w.Cost = stored.Cost
		}
		if stored.NodeObservation != nil &&
			(w.NodeObservation == nil ||
				!nodePhaseIsFresh(&stored.NodeObservation.ObservedAt, stored.NodeObservation.SourceTimestamped,
					w.NodeObservation.ObservedAt, w.NodeObservation.SourceTimestamped)) {
			w.NodeObservation = stored.NodeObservation
		}
		if stored.PodObservation != nil {
			w.PodObservation = stored.PodObservation
		}
		if stored.GPUObservation != nil {
			w.GPUObservation = stored.GPUObservation
		}
		if stored.SchedulingObservation != nil &&
			(w.SchedulingObservation == nil || !schedulingObservationFresh(stored.SchedulingObservation, w.SchedulingObservation)) {
			w.SchedulingObservation = stored.SchedulingObservation
		}
	}
	s.workloads[w.ID] = w
	s.pUpsertWorkload(w)
}

// PutWorkloadDurable publishes a workload only after its configured durable
// backend accepts it. It is reserved for records that must survive a restart
// even though they are not an accepted submission and therefore have no
// authorization audit transaction.
func (s *Store) PutWorkloadDurable(w *Workload) error {
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p != nil {
		if err := p.upsertWorkload(w); err != nil {
			s.recordPersistenceFailure("workload", "upsert", err)
			return fmt.Errorf("%w: persist workload %s: %w", ErrPersistence, w.ID, err)
		}
	}
	s.mu.Lock()
	if stored, ok := s.workloads[w.ID]; ok {
		if p == nil && terminalWorkloadBindingConflict(w, stored) {
			s.mu.Unlock()
			return fmt.Errorf("%w: terminal workload binding conflict", ErrPersistence)
		}
		preserveWorkloadTransition(w, stored)
		if stored.NodeObservation != nil &&
			(w.NodeObservation == nil ||
				!nodePhaseIsFresh(&stored.NodeObservation.ObservedAt, stored.NodeObservation.SourceTimestamped,
					w.NodeObservation.ObservedAt, w.NodeObservation.SourceTimestamped)) {
			w.NodeObservation = stored.NodeObservation
		}
		if stored.PodObservation != nil {
			w.PodObservation = stored.PodObservation
		}
		if stored.GPUObservation != nil {
			w.GPUObservation = stored.GPUObservation
		}
		if stored.SchedulingObservation != nil &&
			(w.SchedulingObservation == nil || !schedulingObservationFresh(stored.SchedulingObservation, w.SchedulingObservation)) {
			w.SchedulingObservation = stored.SchedulingObservation
		}
	}
	s.workloads[w.ID] = w
	s.mu.Unlock()
	return nil
}

// GetWorkload looks up a workload by ID and returns a COPY. Callers must not
// hold a pointer into the store's map: handlers read a workload's fields while
// the reaper watchdog concurrently finishes it, so a shared pointer is a data
// race on Status/FinishedAt. Mutations go through FinishWorkload/PutWorkload,
// which take the lock. (Shallow copy is enough — StartedAt/FinishedAt are
// replaced, never mutated in place, so sharing those pointers is safe.)
func (s *Store) GetWorkload(id string) (*Workload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.workloads[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneWorkload(w), nil
}

// WorkloadsForCustomer returns at most limit workload snapshots for one
// customer, newest first with ID as the deterministic tie-break. The records
// and their spec bytes are copied while the store lock is held so callers never
// retain live map pointers or slices while lifecycle writers update records.
func (s *Store) WorkloadsForCustomer(customerID string, limit int) []*Workload {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		return []*Workload{}
	}
	out := make([]*Workload, 0)
	for _, w := range s.workloads {
		if w.CustomerID != customerID {
			continue
		}
		out = append(out, cloneWorkload(w))
	}
	slices.SortFunc(out, func(a, b *Workload) int {
		if byCreated := b.CreatedAt.Compare(a.CreatedAt); byCreated != 0 {
			return byCreated
		}
		return strings.Compare(a.ID, b.ID)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// FinishWorkload is the trusted internal bool wrapper for a durable transition.
// Unknown IDs and storage failures return false. Request handlers must use
// FinishWorkloadContext so an unavailable store cannot look like a no-op.
//
// The terminal observation is journaled AFTER the lock is released and is
// best-effort: this call is how a burst stops billing, so an audit backend that
// is down must not be able to hold a teardown open.
func (s *Store) FinishWorkload(id, status string, finishedAt time.Time, onlyIfUnfinished bool) bool {
	return s.FinishWorkloadWithOutcome(id, status, finishedAt, onlyIfUnfinished, nil)
}

// FinishWorkloadWithOutcome is FinishWorkload plus the optional receipt the
// observation carried, written in the SAME critical section as the status it
// describes: a reader can never see a terminal status without the receipt that
// explains it, or a receipt attached to a status that has not landed yet.
//
// A nil outcome is the plain finish — cancel, the watchdog, a legacy completion
// report — and stores nothing. Nothing here derives a receipt from the status;
// a fabricated "succeeded because the status says succeeded" would be central
// inventing an observation the agent never made.
//
// First observation to carry a receipt wins for a GIVEN terminal status,
// matching the first-terminal-observation-wins rule the status itself follows:
// a redelivered report cannot rewrite what was recorded the first time.
//
// A receipt does NOT outlive the observation it described. A later finish that
// lands a DIFFERENT terminal status — the cancel path catching a record up, the
// watchdog failing a run a completion had already marked succeeded — replaces
// the stored receipt with its own, or drops it when it carries none. Either way
// the record is left explaining the status it actually holds, never the event
// that no longer stands; a finish that observed something is not silenced by the
// stale receipt it displaced.
func (s *Store) FinishWorkloadWithOutcome(id, status string, finishedAt time.Time, onlyIfUnfinished bool, outcome *WorkloadOutcome) bool {
	_, applied, err := s.transitionWorkload(context.Background(), workloadTransition{
		WorkloadID: id, Status: status, At: finishedAt,
		OnlyIfUnfinished: onlyIfUnfinished, Outcome: cloneWorkloadOutcome(outcome),
	})
	return err == nil && applied
}

// StartWorkload atomically stamps a workload's StartedAt and flips Status to
// "running" under the store lock, mirroring FinishWorkload's locking. First
// report wins: a workload that already has StartedAt is left untouched (the
// agent may re-deliver on watch resync), and a finished workload is never
// resurrected — the terminal Status/FinishedAt stand (the guard is evaluated
// under the same lock the finish writers hold). Returns true when it applied
// the change; unknown id / already started / already finished → false.
func (s *Store) StartWorkload(id string, at time.Time) bool {
	_, applied, err := s.transitionWorkload(context.Background(), workloadTransition{WorkloadID: id, Start: true, At: at})
	return err == nil && applied
}

// PutBurst stores or updates a burst record. The in-memory record is written
// unconditionally so this process retains evidence for reconciliation. With a
// durable backend, however, only the database can elect a reaper; callers must
// not assume a failed write is claimable from this local map.
func (s *Store) PutBurst(b *Burst) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b == nil {
		return fmt.Errorf("%w: nil burst booking", ErrPersistence)
	}
	if s.persist == nil {
		var terminal *Workload
		for _, w := range s.workloads {
			if w.BurstID == b.ID && w.FinishedAt != nil {
				if terminal != nil {
					return fmt.Errorf("%w: ambiguous terminal burst binding", ErrPersistence)
				}
				terminal = w
			}
		}
		if err := preservePendingBurst(b, s.bursts[b.ID], terminal); err != nil {
			return fmt.Errorf("%w: %w", ErrPersistence, err)
		}
	} else {
		// Durable writers merge pending cleanup back into this record. Do not
		// mutate a pointer a caller obtained from an earlier cache read.
		b = cloneBurstSnapshot(b)
	}
	s.bursts[b.ID] = b
	return s.pUpsertBurst(b)
}

// GetBurst looks up a burst by ID.
func (s *Store) GetBurst(id string) (*Burst, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.bursts[id]
	if !ok {
		return nil, ErrNotFound
	}
	return b, nil
}

// DeleteBurst drops a burst record. Called after teardown so a
// repeated completion report is a no-op (GetBurst → ErrNotFound). A non-nil
// error means the durable row survived, so the record can come back on restart.
func (s *Store) DeleteBurst(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.bursts, id)
	return s.pDeleteBurst(id)
}

// ClaimBurst atomically removes and returns a burst, reporting whether it was
// present. It is the race-safe primitive every reap path uses: exactly one
// caller "wins" a given burst, so concurrent reapers (the HTTP complete/cancel
// handlers and the idle watchdog) never double-tear-down or double-accrue cost.
//
// With a durable backend the DATABASE elects the winner, and the returned record
// is the row it deleted. This process's map cannot decide it: every replica
// holds the burst in its own memory, so a map-only claim lets two centrals both
// win and tear down — and bill for — the same VM. Sourcing the record from the
// database is also what lets a replica reap a burst it has never seen.
//
// A failed claim returns won=FALSE alongside the error. We cannot tell whether
// the DELETE committed, and a caller that tears down on a maybe-claim risks the
// double teardown and double charge this whole mechanism exists to prevent. The
// row survives a failed claim, so a later reap retries it.
//
// The honest cost: while the durable backend is down, no burst can be reaped and
// every live VM bills for the outage. That is accepted — a Postgres outage is
// already a P1, and paying for VMs is recoverable where double-charging a
// customer is not. 2a7a0bd made the opposite call (won=true plus the error) when
// memory was authoritative and the in-memory claim it recorded WAS the decision;
// that reasoning ends here, where the database decides.
func (s *Store) ClaimBurst(id string) (*Burst, bool, error) {
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return s.claimBurstInMemory(id)
	}

	// Deliberately NOT holding s.mu across the round trip: the claim's atomicity
	// lives in Postgres, so the lock would buy nothing while blocking every other
	// store reader and writer on a database call.
	ctx, cancel := opCtx()
	defer cancel()
	b, won, err := p.claimBurst(ctx, id)
	if err != nil {
		s.recordPersistenceFailure("burst", "claim", err)
		// Leave the in-memory record alone: the outcome is unknown, so this
		// replica must keep tracking a burst that may well still exist. The
		// retry that makes won=false safe comes from the durable row the failed
		// claim left behind — the reaper watchdog sweeps that set, so any
		// replica can make it.
		return nil, false, err
	}
	// Won or lost, the durable row is gone, so the local copy is stale either
	// way — drop it so admission counts and the watchdog stop seeing a burst
	// that no longer exists.
	s.mu.Lock()
	delete(s.bursts, id)
	s.mu.Unlock()
	return b, won, nil
}

// claimBurstInMemory is the entire claim when there is no durable backend: one
// process, so the mutex IS the atomicity. Unchanged behaviour for the in-memory
// and OSS stores.
func (s *Store) claimBurstInMemory(id string) (*Burst, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bursts[id]
	if !ok {
		return nil, false, nil
	}
	delete(s.bursts, id)
	return b, true, nil
}

// DurableBursts returns every burst recorded in DURABLE state — the whole
// live-burst set across replicas, as opposed to ListBursts' per-process map.
//
// Any worker whose job is to act on bursts it did not create reads this: the
// in-memory map only ever holds bursts this process wrote, so a second
// central's burst is invisible to it. That is a destroyed customer job for the
// orphan sweep and an unenforced budget for the reaper watchdog.
//
// ok=false means there is no durable backend (the in-memory/OSS store). There
// the memory map IS the truth, because only one process exists; callers fall
// back to ListBursts. An error means the durable set could not be read at all
// — never a partial one — so callers decide what a missing view costs them
// rather than mistaking it for an empty set.
func (s *Store) DurableBursts(ctx context.Context) (bursts []*Burst, ok bool, err error) {
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return nil, false, nil
	}
	bursts, err = p.listBursts(ctx)
	if err != nil {
		return nil, true, err
	}
	return bursts, true, nil
}

// DurableBurstBackendIDs is the backend-ID projection of DurableBursts, for the
// orphan sweep's tracked set. Same three-way contract, including ok=false for
// "no durable backend"; see DurableBursts.
//
// It must not read the in-memory map. The sweep destroys any owned cloud VM
// absent from the set it is given, and this process's map only ever holds
// bursts this process created — so a second central's live burst would look
// untracked and be destroyed, killing a running customer workload. On error the
// caller MUST abort rather than sweep: an empty or partial tracked set reads as
// "everything is an orphan".
func (s *Store) DurableBurstBackendIDs(ctx context.Context) (ids map[string]bool, ok bool, err error) {
	bursts, ok, err := s.DurableBursts(ctx)
	if err != nil || !ok {
		return nil, ok, err
	}
	ids = make(map[string]bool, len(bursts))
	for _, b := range bursts {
		if b.BackendID != "" {
			ids[b.BackendID] = true
		}
	}
	return ids, true, nil
}

// ReservedPodSlots returns the pod /24 slots held in DURABLE state, omitting
// reservations older than ttl.
//
// It covers the window between a Plan picking a /24 and the handler persisting
// the Burst that records it. In one process a map sufficed; across replicas it
// cannot, because the /24 a second central is mid-way through handing out is
// invisible here until its burst row lands — and two live bursts on one /24
// break routing for both, since each advertises the prefix as a mesh subnet
// route.
//
// Same three-way contract as DurableBursts: ok=false is "no durable backend"
// (the in-memory/OSS store, where the allocator's own map is the truth because
// only one process exists), and an error is "there is a backend but it could
// not be read". The caller must never conflate them — allocating from a partial
// view is exactly how a /24 gets handed out twice.
func (s *Store) ReservedPodSlots(ctx context.Context, ttl time.Duration) (slots map[int]bool, ok bool, err error) {
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return nil, false, nil
	}
	slots, err = p.reservedPodSlots(ctx, ttl)
	if err != nil {
		return nil, true, err
	}
	return slots, true, nil
}

// ReservePodSlot takes one pod /24 slot for burstID, returning won=false when
// another caller already holds it. Losing is ordinary, not an error: it means a
// second replica took the slot between this one's read and its write, which is
// the race the durable table exists to decide. The caller walks on to the next
// free slot.
//
// ok follows the DurableBursts convention (false = no durable backend), so a
// caller that reached here after seeing ok=true cannot silently degrade to
// process-local arbitration.
//
// s.mu is deliberately not held across the round trip: the atomicity lives in
// the database, so the lock would only block every other store reader behind a
// network call.
func (s *Store) ReservePodSlot(ctx context.Context, slot int, burstID string, ttl time.Duration) (won bool, ok bool, err error) {
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return false, false, nil
	}
	won, err = p.reservePodSlot(ctx, slot, burstID, ttl)
	if err != nil {
		s.recordPersistenceFailure("pod_slot", "reserve", err)
		return false, true, err
	}
	return won, true, nil
}

// ReleasePodSlot frees the pod /24 reservation a burst holds. Idempotent, and a
// no-op with no durable backend — there the allocator's own map ages its
// reservations out, unchanged.
//
// Callers must not release until the burst's node is confirmed destroyed. The
// reservation is the only thing holding the slot once the burst row is claimed
// away, and a slot handed to a new burst while the old node still advertises
// the prefix is the duplicate-/24 outage in a different order.
func (s *Store) ReleasePodSlot(ctx context.Context, burstID string) error {
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return nil
	}
	return s.recordPersistenceFailure("pod_slot", "release", p.releasePodSlot(ctx, burstID))
}

// ListBursts returns a snapshot slice of all live burst records — used by the
// reaper watchdog so it can iterate without holding the store lock.
func (s *Store) ListBursts() []*Burst {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return mapValues(s.bursts)
}

// BurstsForCustomer returns a snapshot of the given customer's live bursts.
// Used by tenant offboard to reap everything a tenant owns. Linear scan: the
// live-burst set is small in v0.
func (s *Store) BurstsForCustomer(customerID string) []*Burst {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Burst, 0)
	for _, b := range s.bursts {
		if b.CustomerID == customerID {
			out = append(out, b)
		}
	}
	return out
}

// WorkloadByBurst returns a COPY of the workload backed by the given burst id,
// or ErrNotFound. Like GetWorkload, it never hands back the live map pointer so
// callers can't race the reap writers. Linear scan: the live-workload set is
// small in v0.
func (s *Store) WorkloadByBurst(burstID string) (*Workload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, w := range s.workloads {
		if w.BurstID == burstID {
			cp := *w
			return &cp, nil
		}
	}
	return nil, ErrNotFound
}

// FinishWorkloadForBurst marks the workload backed by burstID finished, for a
// burst this replica may never have seen. Reports whether it applied the change.
//
// The selector and onlyIfUnfinished guard are evaluated under the same durable
// row lock, including when this replica has a stale cached copy. An absent row
// is a no-op; storage errors and ambiguous burst bindings are reported.
func (s *Store) FinishWorkloadForBurst(ctx context.Context, burstID, status string, finishedAt time.Time) (bool, error) {
	_, applied, err := s.transitionWorkload(ctx, workloadTransition{BurstID: burstID, Status: status, At: finishedAt, OnlyIfUnfinished: true})
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return applied, err
}

// PutPersistentVolume stores or updates a PersistentVolume. Indexes
// are rebuilt to match — atomic w.r.t. the store-level lock.
func (s *Store) PutPersistentVolume(v *PersistentVolume) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pvs[v.ID] = v
	s.indexPV(v)
	s.pUpsertPV(v)
}

// GetPersistentVolume looks up a PV by ID.
func (s *Store) GetPersistentVolume(id string) (*PersistentVolume, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.pvs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

// GetCacheVolume looks up an active cache volume by tenant + cache key.
// Returns ErrNotFound when none exists or the existing one is in
// non-active state (the caller should treat that as a cache miss).
func (s *Store) GetCacheVolume(tenantID, cacheKey string) (*PersistentVolume, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.pvsByCacheKey[tenantID]
	if m == nil {
		return nil, ErrNotFound
	}
	v, ok := m[cacheKey]
	if !ok || v.State != "active" {
		return nil, ErrNotFound
	}
	return v, nil
}

// GetNamedVolume looks up an active persistent volume by tenant +
// name. ErrNotFound on miss or non-active state.
func (s *Store) GetNamedVolume(tenantID, name string) (*PersistentVolume, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.pvsByName[tenantID]
	if m == nil {
		return nil, ErrNotFound
	}
	v, ok := m[name]
	if !ok || v.State != "active" {
		return nil, ErrNotFound
	}
	return v, nil
}

// ListPersistentVolumes returns all PVs (across tenants) in a stable
// order — used by the GC daemon.
func (s *Store) ListPersistentVolumes() []*PersistentVolume {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*PersistentVolume, 0, len(s.pvs))
	for _, v := range s.pvs {
		out = append(out, v)
	}
	return out
}

// TouchPersistentVolume bumps LastUsedAt to now. Called whenever a
// burst attaches to a volume — keeps TTL retention from evicting
// active workloads.
func (s *Store) TouchPersistentVolume(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.pvs[id]; ok {
		v.LastUsedAt = time.Now().UTC()
		s.pUpsertPV(v)
	}
}

// MarkPersistentVolumeState updates State (e.g. "evicting" → "deleted"
// after the backend confirms the volume is gone).
func (s *Store) MarkPersistentVolumeState(id, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.pvs[id]; ok {
		v.State = state
		s.pUpsertPV(v)
	}
}

// DeletePersistentVolume removes a PV from the store. Called by the
// GC daemon after the backend deletion succeeds.
func (s *Store) DeletePersistentVolume(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.pvs[id]
	if !ok {
		return
	}
	delete(s.pvs, id)
	if m := s.pvsByCacheKey[v.TenantID]; m != nil {
		delete(m, v.CacheKey)
	}
	if m := s.pvsByName[v.TenantID]; m != nil {
		delete(m, v.Name)
	}
	s.pDeletePV(id)
}

// ListOperatorTenants returns one page of live (non-revoked) customers sorted by ID ascending.
// `after` is an exclusive cursor on ID. `limit` bounds the number of returned customers.
func (s *Store) ListOperatorTenants(after string, limit int) ([]*Customer, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var customers []*Customer
	for _, c := range s.customers {
		if !c.Revoked() {
			customers = append(customers, c)
		}
	}
	slices.SortFunc(customers, func(a, b *Customer) int {
		return strings.Compare(a.ID, b.ID)
	})

	if after != "" {
		idx := 0
		for idx < len(customers) && customers[idx].ID <= after {
			idx++
		}
		customers = customers[idx:]
	}

	if limit <= 0 {
		limit = DefaultOperatorTenantLimit
	}
	if limit > MaxOperatorTenantLimit {
		limit = MaxOperatorTenantLimit
	}

	var nextAfter string
	if len(customers) > limit {
		nextAfter = customers[limit-1].ID
		customers = customers[:limit]
	}

	result := make([]*Customer, len(customers))
	for i, c := range customers {
		cp := *c
		result[i] = &cp
	}
	return result, nextAfter
}

// SetCustomerLimits updates a tenant's per-tenant spend guardrails atomically.
// Missing or revoked tenant returns ErrNotFound.
// Persistence failure returns ErrPersistence without mutating live memory.
// A behaviorally identical update returns (customerSnapshot, false, nil) and
// appends no duplicate audit event.
func (s *Store) SetCustomerLimits(customerID string, maxConcurrentBursts int, maxHourlyUSD float64, by Actor) (*Customer, bool, error) {
	if maxConcurrentBursts < 0 || maxHourlyUSD < 0 || math.IsNaN(maxHourlyUSD) || math.IsInf(maxHourlyUSD, 0) {
		return nil, false, ErrInvalidTenantLimits
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()

	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok || c.Revoked() {
		s.mu.Unlock()
		return nil, false, ErrNotFound
	}

	oldBursts := c.MaxConcurrentBursts
	oldHourly := c.MaxHourlyUSD
	unchanged := (oldBursts == maxConcurrentBursts && oldHourly == maxHourlyUSD)

	snapshot := *c
	snapshot.MaxConcurrentBursts = maxConcurrentBursts
	snapshot.MaxHourlyUSD = maxHourlyUSD
	s.mu.Unlock()

	var ev *AuditEvent
	if !unchanged {
		newBursts, newHourly := maxConcurrentBursts, maxHourlyUSD
		ev = NewAuditEvent(AuditEvent{
			CustomerID: customerID,
			Actor:      by,
			Action:     ActionTenantLimitsSet,
			Outcome:    OutcomeAccepted,
			TargetKind: TargetTenant,
			TargetID:   customerID,
			Detail: AuditDetail{
				Reason:                      ReasonTenantLimitsUpdated,
				Rule:                        tenantLimitsRule,
				RuleVersion:                 tenantLimitsRuleVersion,
				PreviousMaxConcurrentBursts: &oldBursts,
				MaxConcurrentBursts:         &newBursts,
				PreviousMaxHourlyUSD:        &oldHourly,
				MaxHourlyUSD:                &newHourly,
			},
		})
	}

	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return nil, false, err
	}

	if unchanged {
		s.mu.Lock()
		live := s.customers[customerID]
		var copyCust Customer
		if live != nil {
			copyCust = *live
		} else {
			copyCust = snapshot
		}
		s.mu.Unlock()
		return &copyCust, false, nil
	}

	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.MaxConcurrentBursts = maxConcurrentBursts
		live.MaxHourlyUSD = maxHourlyUSD
		snapshot = *live
	}
	s.mu.Unlock()

	return &snapshot, true, nil
}
