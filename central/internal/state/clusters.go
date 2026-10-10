package state

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// The durable tenant cluster registry: which clusters a tenant OWNS, as
// opposed to which connectors happen to hold a websocket on this replica.
// Rows live on Customer.RegisteredClusters and persist through the customer
// JSONB document, so the fleet survives a restart and a disconnected cluster
// stays a member of it. Two derived indexes are rebuilt on load — cluster id →
// owner, and credential hash → (owner, cluster) — so cross-tenant cluster ids
// are refused even when no connector is live, and a connector credential
// authenticates without scanning every tenant.

// MaxRegisteredClusters bounds one tenant's registry. The customer row is a
// single JSONB document rewritten on every customer write, so the collection
// has to stay small enough that stamping one cluster's connect time never
// turns into a megabyte rewrite. 32 is far above any real fleet today.
const MaxRegisteredClusters = 32

// How a registry row came to exist. Registered rows were created through the
// tenant API and carry a credential hash; claimed rows were self-announced by
// a legacy tenant-token connector's Hello and carry none — the tenant token
// itself is their credential. Hosted rows are platform-managed shared-capacity
// assignments made by the operator (see hosted.go): they carry a credential
// hash like registered rows, but their lifecycle belongs to the operator, so
// the tenant rotate/delete paths refuse them.
const (
	ClusterSourceRegistered = "registered"
	ClusterSourceClaimed    = "claimed"
	ClusterSourceHosted     = "hosted"
)

// Stable cluster states the tenant surface reports. connected_here is
// deliberately replica-scoped: a websocket lives on exactly one central, so
// this store can attest a live socket it holds and nothing about any other
// replica — disconnected means "no socket HERE and it has connected before",
// never a claim of global liveness.
const (
	ClusterStateNeverConnected = "never_connected"
	ClusterStateConnectedHere  = "connected_here"
	ClusterStateDisconnected   = "disconnected"
)

var (
	// ErrInvalidCluster rejects a registration outside the closed grammar: a
	// malformed cluster id, or a name that is empty, control-laden or too long.
	ErrInvalidCluster = errors.New("invalid cluster registration")
	// ErrClusterExists rejects a cluster id that is already taken — by any
	// tenant's registry row or by a live connector. Cluster ids are GLOBAL:
	// they name tailnet hostnames and MagicDNS entries, so two tenants holding
	// one id is the cross-tenant device takeover the mint path exists to stop.
	ErrClusterExists = errors.New("cluster id is already in use")
	// ErrClusterLimit rejects growing a registry past MaxRegisteredClusters.
	ErrClusterLimit = fmt.Errorf("a tenant may register at most %d clusters", MaxRegisteredClusters)
	// ErrClusterNotFound is the rotate/delete answer for a cluster id the
	// tenant does not hold. It wraps ErrNotFound so callers matching only that
	// still answer 404, and is distinct so a handler can tell "no such cluster"
	// from "no such tenant" — the caller is a proven member by then, so naming
	// the cluster as the missing thing discloses nothing.
	ErrClusterNotFound = fmt.Errorf("%w: no such registered cluster", ErrNotFound)
	// ErrClusterNamedByPolicy refuses deleting a cluster the tenant's own
	// allow/deny policy still names. Deleting it would leave the policy
	// pointing at nothing — and silently rewriting the policy on a delete
	// would move an authorization boundary nobody asked to move.
	ErrClusterNamedByPolicy = errors.New("cluster is still named by the tenant cluster policy")
	// ErrClusterOwnedElsewhere refuses binding a connector to a cluster id
	// another tenant holds — durably registered or live on this replica.
	ErrClusterOwnedElsewhere = errors.New("cluster id is registered to another tenant")
	// ErrClusterPlatformManaged refuses the tenant rotate/delete paths on a
	// hosted row. The row's credential secures a connector release the platform
	// operates on shared hardware, so a tenant rotating it would cut a
	// connector only the operator can redeploy, and a tenant delete would leak
	// the namespace reservation the assignment holds. The operator routes in
	// hosted.go are the lifecycle for these rows.
	ErrClusterPlatformManaged = errors.New("cluster is platform-managed hosted capacity")
)

// RegisteredCluster is one durable row of a tenant's cluster registry. It is
// PERSISTED VERBATIM inside the Customer JSONB document — CredentialHash
// included, which is why no public handler DTO may ever serialize this struct
// directly (see TenantCluster, the outward view with no hash field).
type RegisteredCluster struct {
	ClusterID string
	// Name is the tenant's display name. Tenant-authored text: it renders on
	// the tenant's own surface and nowhere else — never in audit rows or logs.
	Name string `json:",omitempty"`
	// CredentialHash is the SHA-256 (hex) of the cluster-scoped connector
	// credential, and the ONLY form the credential survives in: the plaintext
	// is revealed once in the register/rotation response and never stored.
	// Empty on claimed rows, whose connectors authenticate with the tenant
	// token.
	CredentialHash string `json:",omitempty"`
	// Source is ClusterSourceRegistered, ClusterSourceClaimed or
	// ClusterSourceHosted.
	Source       string
	RegisteredAt time.Time

	// HostedNamespace is the one workload namespace this hosted assignment
	// reserved for the tenant on the shared physical cluster — the exact list
	// the per-tenant connector release's rbac.allowedNamespaces is rendered
	// from. Set only on hosted rows; its global one-owner index
	// (Store.hostedNamespaces) is rebuilt on load like clusterOwners.
	HostedNamespace string `json:",omitempty"`
	// HostedNamespaceAdded records whether assigning this hosted row added its
	// namespace to Customer.WorkloadNamespaces. A tenant may already authorize
	// the same namespace name in a cluster it owns; deleting hosted capacity
	// must not revoke that pre-existing grant.
	HostedNamespaceAdded bool `json:",omitempty"`

	// Socket lifecycle metadata, stamped at connect/disconnect boundaries
	// only — never per heartbeat, which would rewrite the customer document
	// on every ping. LastObservedAt is the last frame seen from the most
	// recently closed socket; while a socket is live, liveness is read off the
	// live Agent instead.
	FirstConnectedAt   *time.Time `json:",omitempty"`
	LastConnectedAt    *time.Time `json:",omitempty"`
	LastDisconnectedAt *time.Time `json:",omitempty"`
	LastObservedAt     *time.Time `json:",omitempty"`
}

// clusterCredRef is what the credential-hash index resolves to.
type clusterCredRef struct {
	customerID string
	clusterID  string
}

// copyRegisteredClusters deep-copies a registry so a snapshot never shares row
// pointers with the live record. Rows are treated as immutable once published
// (every stamp replaces the row), but the durable write marshals its snapshot
// outside the store lock, so sharing pointers would still be a race.
func copyRegisteredClusters(in []*RegisteredCluster) []*RegisteredCluster {
	if in == nil {
		return nil
	}
	out := make([]*RegisteredCluster, 0, len(in))
	for _, rc := range in {
		copied := *rc
		if rc.FirstConnectedAt != nil {
			t := *rc.FirstConnectedAt
			copied.FirstConnectedAt = &t
		}
		if rc.LastConnectedAt != nil {
			t := *rc.LastConnectedAt
			copied.LastConnectedAt = &t
		}
		if rc.LastDisconnectedAt != nil {
			t := *rc.LastDisconnectedAt
			copied.LastDisconnectedAt = &t
		}
		if rc.LastObservedAt != nil {
			t := *rc.LastObservedAt
			copied.LastObservedAt = &t
		}
		out = append(out, &copied)
	}
	return out
}

// NormalizeClusterName trims and validates a tenant-supplied display name:
// required, at most 64 runes, no control characters. It is display text, not
// an identifier, so anything printable is fine.
func NormalizeClusterName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("%w: name is required", ErrInvalidCluster)
	}
	if utf8.RuneCountInString(name) > 64 {
		return "", fmt.Errorf("%w: name must be at most 64 characters", ErrInvalidCluster)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("%w: name must not contain control characters", ErrInvalidCluster)
		}
	}
	return name, nil
}

// HashClusterCredential is the one derivation of a connector credential the
// store keeps: SHA-256, hex. Lookup is by digest, so authenticating never
// compares the secret byte-by-byte against stored values — the map key IS the
// hash of the attacker-controlled input, which is the constant-time-safe shape
// (an attacker cannot steer a digest toward a target entry without already
// holding the preimage).
func HashClusterCredential(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// newClusterCredential mints a 192-bit connector credential. Same fail-closed
// stance as NewDevToken: a predictable credential is an auth bypass, so the
// (practically impossible) crypto/rand failure panics rather than degrades.
func newClusterCredential() string {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("state: crypto/rand read failed: " + err.Error())
	}
	return "yscale_cluster_" + hex.EncodeToString(b[:])
}

// newClusterID mints a cluster id for a registration that did not choose one.
// Matches clusterIDPattern by construction.
func newClusterID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("state: crypto/rand read failed: " + err.Error())
	}
	return "cluster-" + hex.EncodeToString(b[:])
}

// indexClustersLocked writes one customer's registry rows into the derived
// indexes; unindexClustersLocked removes exactly the rows a customer record
// put there. Both are keyed off the RECORD passed in, so re-indexing a
// replaced customer removes the old record's entries before the new record's
// go in (indexCustomerLocked calls them in that order). Callers hold s.mu.
func (s *Store) indexClustersLocked(c *Customer) {
	for _, rc := range c.RegisteredClusters {
		s.clusterOwners[rc.ClusterID] = c.ID
		if rc.CredentialHash != "" {
			s.clusterCreds[rc.CredentialHash] = clusterCredRef{customerID: c.ID, clusterID: rc.ClusterID}
		}
		if rc.HostedNamespace != "" {
			s.hostedNamespaces[rc.HostedNamespace] = c.ID
		}
	}
}

func (s *Store) unindexClustersLocked(c *Customer) {
	for _, rc := range c.RegisteredClusters {
		if s.clusterOwners[rc.ClusterID] == c.ID {
			delete(s.clusterOwners, rc.ClusterID)
		}
		if rc.CredentialHash != "" {
			if ref, ok := s.clusterCreds[rc.CredentialHash]; ok && ref.customerID == c.ID {
				delete(s.clusterCreds, rc.CredentialHash)
			}
		}
		if rc.HostedNamespace != "" {
			if owner, ok := s.hostedNamespaces[rc.HostedNamespace]; ok && owner == c.ID {
				delete(s.hostedNamespaces, rc.HostedNamespace)
			}
		}
	}
}

// registeredClusterLocked finds one row of a customer's registry. Callers hold
// s.mu; the returned pointer is the stored row and must not outlive the lock.
func registeredClusterLocked(c *Customer, clusterID string) (int, *RegisteredCluster) {
	for i, rc := range c.RegisteredClusters {
		if rc.ClusterID == clusterID {
			return i, rc
		}
	}
	return -1, nil
}

// clusterIDTakenLocked reports whether clusterID is held by anyone other than
// customerID — a durable registry row, or a live connector on this replica.
// Callers hold s.mu.
func (s *Store) clusterIDTakenLocked(clusterID, customerID string) bool {
	if owner, ok := s.clusterOwners[clusterID]; ok {
		return owner != customerID
	}
	for _, a := range s.agents {
		if a.ClusterID == clusterID && a.CustomerID != customerID {
			return true
		}
	}
	return false
}

// clusterHeldByLocked reports whether clusterID is CURRENTLY one of
// customerID's clusters. It is the positive form of clusterIDTakenLocked, which
// only says nobody ELSE holds the id — a distinction a deleted cluster turns
// into a real difference, since its id is held by nobody at all.
//
// Two things count as holding it, and they are the two ClaimAgentCluster
// admits: a durable registry row, or a live socket on this replica. The second
// is what keeps a legacy connector working — one whose self-chosen id is
// outside the ValidClusterID grammar claims no row and connects live-only, so
// the socket is the whole of its claim. Callers hold s.mu.
func (s *Store) clusterHeldByLocked(c *Customer, clusterID, customerID string) bool {
	if _, row := registeredClusterLocked(c, clusterID); row != nil {
		return true
	}
	for _, a := range s.agentsByCust[customerID] {
		if a.ClusterID == clusterID {
			return true
		}
	}
	return false
}

// prunableClaimedRowLocked picks the row a full registry can safely shed to
// admit a new legacy claim: the oldest disconnected CLAIMED row — by last
// connection, cluster id as the tie-break — with no live socket on this
// replica, no minted credential, and no mention in the tenant's allow/deny
// policy. API-registered rows are never candidates, and neither is a claimed
// row a rotation upgraded: a human minted those credentials explicitly, and
// they would die with the row. Returns an index into rows, or -1 when
// nothing is safe to drop. Callers hold s.mu.
func (s *Store) prunableClaimedRowLocked(c *Customer, rows []*RegisteredCluster) int {
	policy := c.ClusterPolicy.Effective()
	live := make(map[string]bool)
	for _, a := range s.agentsByCust[c.ID] {
		live[a.ClusterID] = true
	}
	victim := -1
	var oldest time.Time
	for i, rc := range rows {
		if rc.Source != ClusterSourceClaimed || rc.CredentialHash != "" || live[rc.ClusterID] {
			continue
		}
		if slices.Contains(policy.Allow, rc.ClusterID) || slices.Contains(policy.Deny, rc.ClusterID) {
			continue
		}
		at := rc.RegisteredAt
		if rc.LastConnectedAt != nil {
			at = *rc.LastConnectedAt
		}
		if victim < 0 || at.Before(oldest) || (at.Equal(oldest) && rc.ClusterID < rows[victim].ClusterID) {
			victim, oldest = i, at
		}
	}
	return victim
}

// TenantCluster is one durable registry row joined with this replica's live
// sockets — the store-side view behind GET /v1/tenants/{id}/clusters. It has
// no credential-hash field by construction, so the handler cannot leak one.
//
// The live fields (Connections, ConnectedAt, LastSeen, AgentVersion) describe
// sockets THIS replica holds and nothing else; State says connected_here
// rather than connected for exactly that reason. The registry rows themselves
// are complete — a disconnected cluster is still a row.
type TenantCluster struct {
	ClusterID    string
	Name         string
	Source       string
	State        string
	RegisteredAt time.Time

	// HostedNamespace travels on hosted rows only: it is the namespace the
	// tenant's submissions to this cluster must name, so the surface that
	// shows the cluster id has to show it too.
	HostedNamespace string

	FirstConnectedAt   *time.Time
	LastConnectedAt    *time.Time
	LastDisconnectedAt *time.Time
	LastObservedAt     *time.Time

	// Live socket observation, this replica only. Zero values when no socket
	// is held here. Connections above 1 is a reconnect whose old socket has
	// not been reaped yet — or a copied credential, which is worth surfacing.
	Connections  int
	ConnectedAt  time.Time
	LastSeen     time.Time
	AgentVersion string

	InventoryObserved       bool
	NodeInventoryObserved   bool
	NodeInventoryObservedAt time.Time
	PodInventoryObserved    bool
	PodInventoryObservedAt  time.Time
	NodeCount               int
	BurstCount              int
	PendingPods             int
}

const clusterInventoryFreshFor = 2 * time.Minute

func clusterInventoryFresh(now, observedAt time.Time) bool {
	if observedAt.IsZero() || observedAt.After(now) {
		return false
	}
	return now.Sub(observedAt) <= clusterInventoryFreshFor
}

// TenantClustersFor returns a tenant's durable cluster registry joined with
// the live sockets THIS replica holds, authorized against the caller's own
// grant in the same read lock the rows are taken in — TenantUsageFor's shape,
// and for its reason.
//
// Every role sees it, viewers included: the ids are what their own
// submissions have to name. No role sees another tenant's, and the errors are
// MembershipFor's unchanged, so the route cannot enumerate tenant ids.
//
// The REGISTRY rows are complete — they are durable state, not a socket
// census — but the live join is replica-local, which is what the handler's
// live_partial flag says. Rows are ordered by cluster id and every field is
// copied out.
func (s *Store) TenantClustersFor(customerID, callerAccountID string) ([]TenantCluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.membershipForLocked(callerAccountID, customerID); err != nil {
		return nil, err
	}
	c := s.customers[customerID]

	// agentsForCustomerLocked orders by (ClusterID, ConnectedAt desc, ID), so
	// the first entry of each run is that cluster's newest connection and the
	// run length is its socket count.
	type liveRow struct {
		connections             int
		connectedAt             time.Time
		lastSeen                time.Time
		agentVersion            string
		inventoryObserved       bool
		nodeInventoryObserved   bool
		nodeInventoryObservedAt time.Time
		podInventoryObserved    bool
		podInventoryObservedAt  time.Time
		nodeCount               int
		burstCount              int
		pendingPods             int
	}
	live := make(map[string]liveRow)
	now := time.Now().UTC()
	for _, a := range s.agentsForCustomerLocked(customerID) {
		if row, ok := live[a.ClusterID]; ok {
			row.connections++
			live[a.ClusterID] = row
			continue
		}
		row := liveRow{
			connections:  1,
			connectedAt:  a.ConnectedAt,
			lastSeen:     a.SeenAt(),
			agentVersion: a.AgentVersion,
		}
		if inv, ok := a.ClusterInventory(); ok {
			row.nodeInventoryObserved = inv.NodeInventoryObserved && clusterInventoryFresh(now, inv.NodeInventoryObservedAt)
			row.nodeInventoryObservedAt = inv.NodeInventoryObservedAt
			row.podInventoryObserved = inv.PodInventoryObserved && clusterInventoryFresh(now, inv.PodInventoryObservedAt)
			row.podInventoryObservedAt = inv.PodInventoryObservedAt
			row.inventoryObserved = row.nodeInventoryObserved && row.podInventoryObserved
			if row.nodeInventoryObserved {
				row.nodeCount = inv.NodeCount
				row.burstCount = inv.BurstCount
			}
			if row.podInventoryObserved {
				row.pendingPods = inv.PendingPods
			}
		}
		live[a.ClusterID] = row
	}

	out := make([]TenantCluster, 0, len(c.RegisteredClusters))
	for _, rc := range copyRegisteredClusters(c.RegisteredClusters) {
		row := TenantCluster{
			ClusterID:          rc.ClusterID,
			Name:               rc.Name,
			Source:             rc.Source,
			HostedNamespace:    rc.HostedNamespace,
			RegisteredAt:       rc.RegisteredAt,
			FirstConnectedAt:   rc.FirstConnectedAt,
			LastConnectedAt:    rc.LastConnectedAt,
			LastDisconnectedAt: rc.LastDisconnectedAt,
			LastObservedAt:     rc.LastObservedAt,
			State:              ClusterStateNeverConnected,
		}
		if rc.FirstConnectedAt != nil {
			row.State = ClusterStateDisconnected
		}
		if lr, ok := live[rc.ClusterID]; ok {
			row.State = ClusterStateConnectedHere
			row.Connections = lr.connections
			row.ConnectedAt = lr.connectedAt
			row.LastSeen = lr.lastSeen
			row.AgentVersion = lr.agentVersion
			row.InventoryObserved = lr.inventoryObserved
			row.NodeInventoryObserved = lr.nodeInventoryObserved
			row.NodeInventoryObservedAt = lr.nodeInventoryObservedAt
			row.PodInventoryObserved = lr.podInventoryObserved
			row.PodInventoryObservedAt = lr.podInventoryObservedAt
			row.NodeCount = lr.nodeCount
			row.BurstCount = lr.burstCount
			row.PendingPods = lr.pendingPods
		}
		out = append(out, row)
	}
	slices.SortFunc(out, func(a, b TenantCluster) int {
		return strings.Compare(a.ClusterID, b.ClusterID)
	})
	return out, nil
}

// tenantClusterView renders one registry row as the outward view a write
// handler returns. Fresh rows have no live socket by definition.
func tenantClusterView(rc *RegisteredCluster) TenantCluster {
	row := TenantCluster{
		ClusterID:          rc.ClusterID,
		Name:               rc.Name,
		Source:             rc.Source,
		HostedNamespace:    rc.HostedNamespace,
		RegisteredAt:       rc.RegisteredAt,
		FirstConnectedAt:   rc.FirstConnectedAt,
		LastConnectedAt:    rc.LastConnectedAt,
		LastDisconnectedAt: rc.LastDisconnectedAt,
		LastObservedAt:     rc.LastObservedAt,
		State:              ClusterStateNeverConnected,
	}
	if rc.FirstConnectedAt != nil {
		row.State = ClusterStateDisconnected
	}
	return row
}

// RegisterTenantCluster adds one cluster to a tenant's durable registry and
// mints its connector credential. Owner/admin only; every other role gets
// ErrNotAuthorized. clusterID is optional — empty gets a generated id — and a
// supplied one must pass ValidClusterID and be globally free.
//
// The plaintext credential is RETURNED AND FORGOTTEN: the store keeps only
// its SHA-256, so this response (and a later rotation's) is the only time the
// secret exists outside the caller's hands. The durable write — customer
// document plus audit row, one transaction — lands before anything in memory
// changes, SetTenantClusterPolicy's shape: a credential the caller was handed
// must not evaporate at the next restart because the write silently failed.
func (s *Store) RegisterTenantCluster(customerID, callerAccountID, clusterID, name string, by Actor) (TenantCluster, string, string, error) {
	name, err := NormalizeClusterName(name)
	if err != nil {
		return TenantCluster{}, "", "", err
	}
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		clusterID = newClusterID()
	} else if !ValidClusterID(clusterID) {
		return TenantCluster{}, "", "", fmt.Errorf("%w: invalid cluster id %q", ErrInvalidCluster, clusterID)
	}
	token := newClusterCredential()

	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return TenantCluster{}, "", "", err
	}
	role := caller.Role
	if role != RoleOwner && role != RoleAdmin {
		s.mu.Unlock()
		return TenantCluster{}, "", "", fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, role)
	}
	c := s.customers[customerID]
	if len(c.RegisteredClusters) >= MaxRegisteredClusters {
		s.mu.Unlock()
		return TenantCluster{}, "", "", ErrClusterLimit
	}
	if _, taken := registeredClusterLocked(c, clusterID); taken != nil || s.clusterIDTakenLocked(clusterID, customerID) {
		s.mu.Unlock()
		return TenantCluster{}, "", "", fmt.Errorf("%w: %s", ErrClusterExists, clusterID)
	}
	row := &RegisteredCluster{
		ClusterID:      clusterID,
		Name:           name,
		CredentialHash: HashClusterCredential(token),
		Source:         ClusterSourceRegistered,
		RegisteredAt:   time.Now().UTC(),
	}
	snapshot := *c
	snapshot.RegisteredClusters = append(copyRegisteredClusters(c.RegisteredClusters), row)
	s.mu.Unlock()

	ev := NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionClusterRegister,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetCluster,
		TargetID:   clusterID,
		Detail:     AuditDetail{Reason: ReasonClusterRegistered, Role: role},
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return TenantCluster{}, "", "", fmt.Errorf("%w: persist cluster registration %s: %w", ErrPersistence, customerID, err)
	}

	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.RegisteredClusters = snapshot.RegisteredClusters
		s.clusterOwners[clusterID] = customerID
		s.clusterCreds[row.CredentialHash] = clusterCredRef{customerID: customerID, clusterID: clusterID}
	}
	s.mu.Unlock()
	return tenantClusterView(row), token, role, nil
}

// RotateTenantClusterCredential replaces one registered cluster's connector
// credential and returns the new plaintext once. Owner/admin only. The old
// credential stops authenticating the moment the rotation is published, and
// any socket it holds open on THIS replica is evicted in the same store
// mutation — which is the point: rotation is the recovery path for a leaked
// credential, and a leaked credential's live connection must die with it
// rather than coast until its next reconnect. Rotating a CLAIMED row upgrades
// it: it gains a hash and its connector can move off the shared tenant token.
func (s *Store) RotateTenantClusterCredential(customerID, callerAccountID, clusterID string, by Actor) (TenantCluster, string, string, error) {
	token := newClusterCredential()

	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return TenantCluster{}, "", "", err
	}
	role := caller.Role
	if role != RoleOwner && role != RoleAdmin {
		s.mu.Unlock()
		return TenantCluster{}, "", "", fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, role)
	}
	c := s.customers[customerID]
	idx, existing := registeredClusterLocked(c, clusterID)
	if existing == nil {
		s.mu.Unlock()
		return TenantCluster{}, "", "", ErrClusterNotFound
	}
	if existing.Source == ClusterSourceHosted {
		s.mu.Unlock()
		return TenantCluster{}, "", "", fmt.Errorf("%w: %s", ErrClusterPlatformManaged, clusterID)
	}
	oldHash := existing.CredentialHash
	rows := copyRegisteredClusters(c.RegisteredClusters)
	row := rows[idx]
	row.CredentialHash = HashClusterCredential(token)
	snapshot := *c
	snapshot.RegisteredClusters = rows
	s.mu.Unlock()

	ev := NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionClusterRotate,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetCluster,
		TargetID:   clusterID,
		Detail:     AuditDetail{Reason: ReasonClusterCredentialRotated, Role: role},
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return TenantCluster{}, "", "", fmt.Errorf("%w: persist credential rotation %s: %w", ErrPersistence, customerID, err)
	}

	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.RegisteredClusters = rows
		if oldHash != "" {
			if ref, ok := s.clusterCreds[oldHash]; ok && ref.customerID == customerID {
				delete(s.clusterCreds, oldHash)
			}
		}
		s.clusterCreds[row.CredentialHash] = clusterCredRef{customerID: customerID, clusterID: clusterID}
	}
	s.evictClusterAgentsLocked(customerID, clusterID)
	s.mu.Unlock()
	return tenantClusterView(row), token, role, nil
}

// DeleteTenantCluster removes one cluster from the tenant's registry and
// invalidates its credential. Owner/admin only. A cluster the tenant's own
// allow/deny policy still names is refused (ErrClusterNamedByPolicy): the
// policy is an authorization boundary, and a delete must not silently move
// it. A live socket on THIS replica is evicted in the same mutation that
// removes the row, so the fleet list and the routing state agree the moment
// the delete returns — a deleted cluster is not a dispatch target.
//
// The cluster's gateway ROUTE INTENT goes with the row, in the same durable
// write: its by-cluster set is dropped and the tenant-wide policy union
// recomputed from the clusters that remain, so a deleted cluster's CIDRs stop
// being approvable the moment central agrees it is gone. Its pushed-gateway
// entry survives on purpose — see clearClusterGatewayRoutesLocked — as the
// record the caller's reconcile withdraws the node approval from.
func (s *Store) DeleteTenantCluster(customerID, callerAccountID, clusterID string, by Actor) (string, error) {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return "", err
	}
	role := caller.Role
	if role != RoleOwner && role != RoleAdmin {
		s.mu.Unlock()
		return role, fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, role)
	}
	c := s.customers[customerID]
	idx, existing := registeredClusterLocked(c, clusterID)
	if existing == nil {
		s.mu.Unlock()
		return role, ErrClusterNotFound
	}
	if existing.Source == ClusterSourceHosted {
		s.mu.Unlock()
		return role, fmt.Errorf("%w: %s", ErrClusterPlatformManaged, clusterID)
	}
	if c.ClusterPolicy != nil {
		policy := c.ClusterPolicy.Effective()
		if slices.Contains(policy.Allow, clusterID) || slices.Contains(policy.Deny, clusterID) {
			s.mu.Unlock()
			return role, ErrClusterNamedByPolicy
		}
	}
	oldHash := existing.CredentialHash
	rows := copyRegisteredClusters(c.RegisteredClusters)
	rows = append(rows[:idx], rows[idx+1:]...)
	if len(rows) == 0 {
		rows = nil
	}
	byCluster, union := clearClusterGatewayRoutesLocked(c, clusterID)
	snapshot := *c
	snapshot.RegisteredClusters = rows
	snapshot.GatewayRoutesByCluster = byCluster
	snapshot.GatewayRoutes = union
	s.mu.Unlock()

	ev := NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionClusterDelete,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetCluster,
		TargetID:   clusterID,
		Detail:     AuditDetail{Reason: ReasonClusterDeleted, Role: role},
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return role, fmt.Errorf("%w: persist cluster delete %s: %w", ErrPersistence, customerID, err)
	}

	s.mu.Lock()
	if live := s.customers[customerID]; live != nil {
		live.RegisteredClusters = rows
		live.GatewayRoutesByCluster = byCluster
		live.GatewayRoutes = union
		if s.clusterOwners[clusterID] == customerID {
			delete(s.clusterOwners, clusterID)
		}
		if oldHash != "" {
			if ref, ok := s.clusterCreds[oldHash]; ok && ref.customerID == customerID {
				delete(s.clusterCreds, oldHash)
			}
		}
	}
	s.evictClusterAgentsLocked(customerID, clusterID)
	s.mu.Unlock()
	return role, nil
}

// AuthClusterCredential resolves a cluster-scoped connector credential to its
// customer and bound cluster id. ErrNotFound on a miss, on a revoked tenant,
// and on a legacy tenant token alike — the caller falls back to AuthCustomer
// for the last of those. The presented secret is hashed FIRST and looked up
// by digest (see HashClusterCredential), so no stored secret is ever compared
// byte-by-byte against caller input.
func (s *Store) AuthClusterCredential(token string) (*Customer, string, error) {
	return s.AuthClusterCredentialContext(context.Background(), token)
}

// ClaimAgentCluster binds one connector's Hello cluster id to its tenant's
// durable registry, called on the agent stream BEFORE AddAgent. It refuses a
// cluster id another tenant holds (ErrClusterOwnedElsewhere), stamps the
// row's connect metadata, and — for a LEGACY tenant-token connector, which is
// the requireRegistered=false path — creates the row on first connection with
// Source claimed, which is how a pre-registry fleet becomes durable without
// anyone re-deploying. claimed=true reports that a row was created, so the
// caller can journal the claim.
//
// requireRegistered=true is the cluster-credential path: the credential is
// bound to a row, so a missing one (deleted since the request authenticated)
// is ErrClusterNotFound rather than a fresh claim.
//
// The LEGACY path never turns registry bookkeeping into an outage. A
// pre-registry cluster id outside the ValidClusterID grammar connects the way
// it always did — live-only, claiming no row — and a registry already at
// MaxRegisteredClusters first sheds its oldest safely-prunable claimed row
// (see prunableClaimedRowLocked), falling back to the same live-only mode
// when nothing is safe to drop. Cross-tenant ownership is refused on every
// path, ephemeral included.
//
// prunedCluster names the row that was shed, if any. Its gateway ROUTE INTENT
// goes with it, in this same durable write: the tenant no longer holds that
// cluster, so leaving its set behind would keep its CIDRs in the policy union
// with nothing left that could ever take them out — the shed row's connector is
// disconnected by construction (prunableClaimedRowLocked skips live ones) and
// has no row to come back to. Its PUSHED entry survives as the node-withdrawal
// tombstone; the caller enqueues the withdrawal once the claim has succeeded.
//
// The durable write is BEST-EFFORT (pUpsertCustomer's logging path), unlike
// the tenant-API writes above: this runs on every reconnect, and refusing a
// fleet's connectors because the database blipped would turn durability
// bookkeeping into an outage. The in-memory claim still protects the id.
func (s *Store) ClaimAgentCluster(customerID, clusterID string, requireRegistered bool, at time.Time) (claimed bool, prunedCluster string, err error) {
	if !ValidClusterID(clusterID) {
		if requireRegistered {
			return false, "", fmt.Errorf("%w: invalid cluster id %q", ErrInvalidCluster, clusterID)
		}
		// A legacy connector's self-chosen id can predate the grammar, and a
		// registry refusal here would brick a running fleet on upgrade. It
		// keeps the old contract: connected while the socket lives, member of
		// no durable fleet.
		s.mu.Lock()
		defer s.mu.Unlock()
		c, ok := s.customers[customerID]
		if !ok || c.Revoked() {
			return false, "", ErrNotFound
		}
		if s.clusterIDTakenLocked(clusterID, customerID) {
			return false, "", fmt.Errorf("%w: %s", ErrClusterOwnedElsewhere, clusterID)
		}
		return false, "", nil
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok || c.Revoked() {
		s.mu.Unlock()
		return false, "", ErrNotFound
	}
	if s.clusterIDTakenLocked(clusterID, customerID) {
		s.mu.Unlock()
		return false, "", fmt.Errorf("%w: %s", ErrClusterOwnedElsewhere, clusterID)
	}
	idx, existing := registeredClusterLocked(c, clusterID)
	if existing != nil && existing.Source == ClusterSourceHosted && !requireRegistered {
		s.mu.Unlock()
		return false, "", fmt.Errorf("%w: %s requires its cluster-scoped credential", ErrClusterPlatformManaged, clusterID)
	}
	rows := copyRegisteredClusters(c.RegisteredClusters)
	var row *RegisteredCluster
	if existing == nil {
		if requireRegistered {
			s.mu.Unlock()
			return false, "", ErrClusterNotFound
		}
		if len(rows) >= MaxRegisteredClusters {
			victim := s.prunableClaimedRowLocked(c, rows)
			if victim < 0 {
				// Every row is one it would be unsafe to drop. The limit is
				// SaaS bookkeeping, not an admission gate: the connector
				// connects live-only instead of being refused.
				s.mu.Unlock()
				return false, "", nil
			}
			pruned := rows[victim]
			rows = append(rows[:victim], rows[victim+1:]...)
			if s.clusterOwners[pruned.ClusterID] == customerID {
				delete(s.clusterOwners, pruned.ClusterID)
			}
			prunedCluster = pruned.ClusterID
			c.GatewayRoutesByCluster, c.GatewayRoutes =
				dropClusterGatewayRoutesLocked(c, []string{prunedCluster})
		}
		claimed = true
		row = &RegisteredCluster{
			ClusterID:    clusterID,
			Source:       ClusterSourceClaimed,
			RegisteredAt: at,
		}
		rows = append(rows, row)
	} else {
		row = rows[idx]
	}
	stamp := at
	if row.FirstConnectedAt == nil {
		row.FirstConnectedAt = &stamp
	}
	last := at
	row.LastConnectedAt = &last
	c.RegisteredClusters = rows
	s.clusterOwners[clusterID] = customerID
	snapshot := *c
	s.mu.Unlock()
	if err := s.pUpsertCustomer(&snapshot); err != nil {
		return false, "", err
	}
	return claimed, prunedCluster, nil
}

// MarkClusterDisconnected stamps one cluster's disconnect metadata when its
// socket closes: when it dropped, and the last frame it was seen sending.
// Best-effort durable write for ClaimAgentCluster's reason, and a silent
// no-op for a cluster that is no longer in the registry — a delete racing a
// disconnect must not resurrect the row.
func (s *Store) MarkClusterDisconnected(customerID, clusterID string, at, observed time.Time) {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok {
		s.mu.Unlock()
		return
	}
	idx, existing := registeredClusterLocked(c, clusterID)
	if existing == nil {
		s.mu.Unlock()
		return
	}
	rows := copyRegisteredClusters(c.RegisteredClusters)
	row := rows[idx]
	dropped := at
	row.LastDisconnectedAt = &dropped
	if !observed.IsZero() {
		seen := observed
		row.LastObservedAt = &seen
	}
	c.RegisteredClusters = rows
	snapshot := *c
	s.mu.Unlock()
	s.pUpsertCustomer(&snapshot)
}
