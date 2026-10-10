package state

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Yscale-hosted shared capacity: the operator runs ONE physical cluster and
// assigns each participating tenant its own connector release on it. There is
// deliberately no house tenant and no shared connector — a hosted assignment
// is a row in the TENANT'S OWN registry (Source hosted) with a tenant-bound
// credential and a globally unique virtual cluster id, so placement, audit,
// spend, Get and the agent lifecycle all stay tenant-scoped through the exact
// paths registered clusters already use. What makes it hosted is who manages
// it (the operator, via AdminAuth routes) and the one namespace it reserves:
// each assignment holds exactly one workload namespace on the shared cluster,
// one owner globally, and the per-tenant release's rbac.allowedNamespaces is
// rendered from that same value — the two gates of SECURITY-REVIEW H2, agreeing
// by construction.

var (
	// ErrHostedClusterExists rejects a second hosted assignment for a tenant
	// that already holds one. An assignment is one connector release and one
	// namespace on the shared cluster; a tenant needing more capacity is an
	// operator conversation, not a second row.
	ErrHostedClusterExists = errors.New("tenant already has a hosted cluster assignment")
	// ErrHostedNamespaceTaken rejects reserving a hosted namespace any
	// assignment already holds. The namespaces share one physical cluster, so
	// two owners of one name is the cross-tenant Secret/pod exposure the
	// reservation exists to prevent.
	ErrHostedNamespaceTaken = errors.New("hosted namespace is already reserved")
	// ErrInvalidHostedNamespace rejects a namespace outside the closed
	// grammar: not a DNS-1123 label, or a name the shared cluster's platform
	// owns (default, kube-*, yscale*).
	ErrInvalidHostedNamespace = errors.New("invalid hosted namespace")
	// ErrHostedNamespaceRequired refuses replacing a tenant's workload
	// namespaces with a set that omits a standing hosted assignment's reserved
	// namespace. The assignment's release grants Secret RBAC in exactly that
	// namespace and the console offers the row as a target, so a set without
	// it fails every hosted submit while the assignment stays live — and it
	// would strand the HostedNamespaceAdded bookkeeping DeleteHostedCluster
	// releases the grant through. Freeing the namespace is the hosted delete
	// route's job, not a side effect of an operator namespace edit.
	ErrHostedNamespaceRequired = errors.New("workload namespaces must keep the hosted assignment's reserved namespace")
)

// hostedClusterIDPattern is deliberately stricter than clusterIDPattern: a
// hosted virtual cluster id also names the per-tenant helm release on the
// shared cluster ("yscale-agent-<id>"), so it is held to DNS-1123-safe
// lowercase and bounded so the release name fits helm's 53-character limit.
var hostedClusterIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,20}$`)

// hostedNamespacePattern is the DNS-1123 label grammar, the same closed set
// handlers.validateWorkloadNamespaces holds tenant namespace lists to — and
// for the same second reason: the value is interpolated into the rendered
// rbac.allowedNamespaces helm flag, and the label alphabet cannot carry shell
// syntax or a second namespace.
var hostedNamespacePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// reservedHostedNamespaces are names no tenant may hold on the shared
// cluster: the platform's own namespaces and Kubernetes' system ones. default
// is here twice over — it is the fail-closed namespace every unconfigured
// tenant resolves to, and one tenant owning it would put every such tenant's
// unqualified submissions inside another tenant's boundary.
var reservedHostedNamespaces = map[string]bool{
	"default": true, "kube-system": true, "kube-public": true,
	"kube-node-lease": true, "yscale": true, "yscale-system": true,
}

// fallbackWorkloadNamespace mirrors handlers.authorizedWorkloadNamespaces'
// fail-closed resolution of an empty WorkloadNamespaces set. The hosted merge
// materializes it before appending so granting the hosted namespace never
// changes which namespace an unqualified submission lands in.
const fallbackWorkloadNamespace = "default"

const (
	HostedCapacityNotRequested = "not_requested"
	HostedCapacityRequested    = "requested"
	HostedCapacityAssigned     = "assigned"
)

// TenantHostedCapacity is the bounded tenant-facing view of the shared
// capacity queue. RequestedAt is present only while the request is pending;
// an assignment wins over a stale timestamp left by an older snapshot.
type TenantHostedCapacity struct {
	Role        string
	Status      string
	RequestedAt *time.Time
}

func hostedCapacityLocked(c *Customer, role string) TenantHostedCapacity {
	for _, rc := range c.RegisteredClusters {
		if rc.Source == ClusterSourceHosted {
			return TenantHostedCapacity{Role: role, Status: HostedCapacityAssigned}
		}
	}
	if c.HostedCapacityRequestedAt == nil {
		return TenantHostedCapacity{Role: role, Status: HostedCapacityNotRequested}
	}
	requestedAt := c.HostedCapacityRequestedAt.UTC()
	return TenantHostedCapacity{Role: role, Status: HostedCapacityRequested, RequestedAt: &requestedAt}
}

// TenantHostedCapacityFor returns the queue state to any active member. Tenant
// liveness, membership and state are read under one lock, so a revoked grant
// cannot authorize a later snapshot.
func (s *Store) TenantHostedCapacityFor(customerID, callerAccountID string) (TenantHostedCapacity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		return TenantHostedCapacity{}, err
	}
	return hostedCapacityLocked(s.customers[customerID], caller.Role), nil
}

// PendingHostedCapacityRequest is one row of the operator's pending queue:
// exactly what deciding an assignment needs, and nothing from the tenant's
// private surface — no token, email, membership, connector credential or
// credential hash has a field to land in.
type PendingHostedCapacityRequest struct {
	TenantID string
	// Name is the tenant's optional display name; empty is a real value
	// (operator-provisioned and pre-field tenants), not an error.
	Name string
	Plan string
	// RequestedAt is the durable request marker in UTC — when the tenant
	// asked, not when this row was materialized.
	RequestedAt time.Time
}

// PendingHostedCapacityRequests returns the operator's view of the shared
// capacity queue: every active tenant whose request marker is set and who does
// not already hold a hosted assignment, oldest request first with the tenant
// id as the deterministic tie break. The slice is non-nil (empty means no
// pending requests) and every row is a copy — the caller holds no pointer
// into store state. A pure read under the store read lock: no write, no
// audit row.
func (s *Store) PendingHostedCapacityRequests() []PendingHostedCapacityRequest {
	s.mu.RLock()
	requests := make([]PendingHostedCapacityRequest, 0)
	for _, c := range s.customers {
		if c == nil || c.Revoked() || c.HostedCapacityRequestedAt == nil {
			continue
		}
		// Same precedence the tenant view holds: a standing assignment is
		// authoritative over a stale marker an older snapshot left behind.
		if hostedCapacityLocked(c, "").Status != HostedCapacityRequested {
			continue
		}
		requests = append(requests, PendingHostedCapacityRequest{
			TenantID:    c.ID,
			Name:        c.Name,
			Plan:        c.Plan,
			RequestedAt: c.HostedCapacityRequestedAt.UTC(),
		})
	}
	s.mu.RUnlock()

	// Everything below works on copied values. Keep ordering outside the
	// store lock so a large operator queue cannot delay tenant writes.
	slices.SortFunc(requests, func(a, b PendingHostedCapacityRequest) int {
		if cmp := a.RequestedAt.Compare(b.RequestedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.TenantID, b.TenantID)
	})
	return requests
}

// RequestTenantHostedCapacity durably queues a request for an owner or admin.
// A pending request and an existing assignment are idempotent reads: neither
// writes nor appends another audit row. created reports the first accepted
// request, which is the only response that uses HTTP 202.
func (s *Store) RequestTenantHostedCapacity(customerID, callerAccountID string, by Actor) (view TenantHostedCapacity, created bool, err error) {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	caller, err := s.membershipForLocked(callerAccountID, customerID)
	if err != nil {
		s.mu.Unlock()
		return TenantHostedCapacity{}, false, err
	}
	role := caller.Role
	if role != RoleOwner && role != RoleAdmin {
		s.mu.Unlock()
		return TenantHostedCapacity{}, false, fmt.Errorf("%w: %s is %q", ErrNotAuthorized, caller.ID, role)
	}
	c := s.customers[customerID]
	current := hostedCapacityLocked(c, role)
	if current.Status != HostedCapacityNotRequested {
		s.mu.Unlock()
		return current, false, nil
	}
	requestedAt := time.Now().UTC()
	snapshot := *c
	snapshot.HostedCapacityRequestedAt = &requestedAt
	s.mu.Unlock()

	ev := NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionHostedCapacityRequest,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetTenant,
		TargetID:   customerID,
		Detail:     AuditDetail{Reason: ReasonHostedCapacityRequested, Role: role},
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return TenantHostedCapacity{}, false, err
	}

	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.HostedCapacityRequestedAt = snapshot.HostedCapacityRequestedAt
	}
	s.mu.Unlock()
	return TenantHostedCapacity{Role: role, Status: HostedCapacityRequested, RequestedAt: &requestedAt}, true, nil
}

// HostedClusterAssignment is one row of the operator's hosted inventory: which
// live tenant holds the assignment and the same safe cluster view the assign
// and rotate responses return. It carries the tenant's id, optional display
// name and plan — what deciding about shared capacity needs — and nothing from
// the tenant's private surface: no customer token, email, membership,
// connector credential or credential hash has a field to land in.
type HostedClusterAssignment struct {
	TenantID string
	// Name is the tenant's optional display name; empty is a real value
	// (operator-provisioned and pre-field tenants), not an error.
	Name    string
	Plan    string
	Cluster TenantCluster
}

// HostedClusterAssignments returns every standing hosted assignment across
// live tenants, ordered by tenant id with the cluster id as the deterministic
// tie break. Revoked tenants are absent — a revoked grant must not keep
// publishing the tenant's inventory — and so is every registry row the TENANT
// owns: only ClusterSourceHosted rows are the operator's to see here.
//
// The slice is non-nil (empty means no assignments) and every row is a deep
// copy, timestamps included, so a caller holds no pointer into store state and
// cannot mutate a customer through the response. A pure read under the store
// read lock: no write, no audit row. The State join is replica-local, exactly
// as TenantClustersFor's is — a row this replica holds no socket for reads as
// disconnected, not as down.
func (s *Store) HostedClusterAssignments() []HostedClusterAssignment {
	s.mu.RLock()
	rows := make([]HostedClusterAssignment, 0)
	for _, c := range s.customers {
		if c == nil || c.Revoked() {
			continue
		}
		var live map[string]bool
		for _, rc := range c.RegisteredClusters {
			if rc == nil || rc.Source != ClusterSourceHosted {
				continue
			}
			if live == nil {
				live = make(map[string]bool)
				for _, a := range s.agentsForCustomerLocked(c.ID) {
					live[a.ClusterID] = true
				}
			}
			// copyRegisteredClusters is the deep copy the durable writes use;
			// going through it keeps the timestamp pointers off store state.
			view := tenantClusterView(copyRegisteredClusters([]*RegisteredCluster{rc})[0])
			if live[rc.ClusterID] {
				view.State = ClusterStateConnectedHere
			}
			rows = append(rows, HostedClusterAssignment{
				TenantID: c.ID,
				Name:     c.Name,
				Plan:     c.Plan,
				Cluster:  view,
			})
		}
	}
	s.mu.RUnlock()

	// Everything below works on copied values. Keep ordering outside the store
	// lock so a large hosted inventory cannot delay tenant writes.
	slices.SortFunc(rows, func(a, b HostedClusterAssignment) int {
		if cmp := strings.Compare(a.TenantID, b.TenantID); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Cluster.ClusterID, b.Cluster.ClusterID)
	})
	return rows
}

// newHostedClusterID mints a virtual cluster id for an assignment that did not
// choose one. Matches hostedClusterIDPattern by construction and is
// recognizably hosted in every surface that shows cluster ids.
func newHostedClusterID() string {
	var b [7]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("state: crypto/rand read failed: " + err.Error())
	}
	return "hosted-" + hex.EncodeToString(b[:])
}

// normalizeHostedNamespace resolves the namespace an assignment reserves:
// the operator's explicit choice, or one derived from the tenant id. Both
// leave through the same validation, so a derived name is held to exactly the
// grammar a supplied one is.
func normalizeHostedNamespace(customerID, namespace string) (string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = deriveHostedNamespace(customerID)
	}
	if len(namespace) > 63 {
		return "", fmt.Errorf("%w: %q is longer than 63 characters", ErrInvalidHostedNamespace, namespace)
	}
	if !hostedNamespacePattern.MatchString(namespace) {
		return "", fmt.Errorf("%w: %q is not a Kubernetes namespace name "+
			"(lowercase letters, digits and '-', starting and ending alphanumeric)", ErrInvalidHostedNamespace, namespace)
	}
	if reservedHostedNamespaces[namespace] {
		return "", fmt.Errorf("%w: %q is reserved for the platform", ErrInvalidHostedNamespace, namespace)
	}
	return namespace, nil
}

// deriveHostedNamespace maps a tenant id onto the label alphabet: "cust_x8f2"
// becomes "ys-cust-x8f2". Collisions between distinct tenant ids that sanitize
// alike are caught by the one-owner reservation, and the operator names an
// explicit namespace instead.
func deriveHostedNamespace(customerID string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(customerID) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	ns := "ys-" + strings.Trim(b.String(), "-")
	if len(ns) > 63 {
		ns = strings.TrimRight(ns[:63], "-")
	}
	return ns
}

// hostedAuthorizedNamespaces merges the reserved namespace into the tenant's
// authorized workload set. The FIRST entry decides where an unqualified
// submission lands, so an empty set materializes its fail-closed resolution
// before the hosted namespace is appended — granting hosted capacity must
// never silently move existing submissions into the shared cluster's
// namespace.
func hostedAuthorizedNamespaces(existing []string, namespace string) []string {
	if slices.Contains(existing, namespace) {
		return slices.Clone(existing)
	}
	base := slices.Clone(existing)
	if len(base) == 0 {
		base = []string{fallbackWorkloadNamespace}
	}
	return append(base, namespace)
}

// AssignHostedCluster is the operator's grant of shared capacity to one
// tenant: a hosted registry row with a fresh cluster-scoped credential, plus
// the reservation of one globally unique workload namespace, in ONE durable
// write. RegisterTenantCluster's contract otherwise — the plaintext credential
// is returned and forgotten, and the durable write lands before anything in
// memory changes, so a persistence failure publishes neither the cluster nor
// the namespace ownership.
//
// There is no caller membership: the authority is the AdminAuth credential,
// and the actor the change is attributed to is the operator. clusterID and
// namespace are optional — empty gets a generated id / a tenant-derived
// namespace — and supplied values must pass the hosted grammars, which are
// stricter than the tenant registry's because both are rendered into the
// per-tenant release's helm command.
func (s *Store) AssignHostedCluster(customerID, clusterID, name, namespace string, by Actor) (TenantCluster, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "Yscale hosted capacity"
	}
	name, err := NormalizeClusterName(name)
	if err != nil {
		return TenantCluster{}, "", err
	}
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		clusterID = newHostedClusterID()
	} else if !hostedClusterIDPattern.MatchString(clusterID) {
		return TenantCluster{}, "", fmt.Errorf("%w: invalid hosted cluster id %q "+
			"(lowercase letters, digits and '-', at most 21 characters)", ErrInvalidCluster, clusterID)
	}
	namespace, err = normalizeHostedNamespace(customerID, namespace)
	if err != nil {
		return TenantCluster{}, "", err
	}
	token := newClusterCredential()

	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok || c.Revoked() {
		s.mu.Unlock()
		return TenantCluster{}, "", ErrNotFound
	}
	for _, rc := range c.RegisteredClusters {
		if rc.Source == ClusterSourceHosted {
			s.mu.Unlock()
			return TenantCluster{}, "", fmt.Errorf("%w: %s", ErrHostedClusterExists, rc.ClusterID)
		}
	}
	if len(c.RegisteredClusters) >= MaxRegisteredClusters {
		s.mu.Unlock()
		return TenantCluster{}, "", ErrClusterLimit
	}
	if _, taken := registeredClusterLocked(c, clusterID); taken != nil || s.clusterIDTakenLocked(clusterID, customerID) {
		s.mu.Unlock()
		return TenantCluster{}, "", fmt.Errorf("%w: %s", ErrClusterExists, clusterID)
	}
	// Any standing reservation refuses, the same tenant's included: a
	// same-tenant hit means a hosted row this loop did not see, which is
	// exactly the inconsistency to fail closed on.
	if _, taken := s.hostedNamespaces[namespace]; taken {
		s.mu.Unlock()
		return TenantCluster{}, "", fmt.Errorf("%w: %s", ErrHostedNamespaceTaken, namespace)
	}
	namespaceAdded := !slices.Contains(c.WorkloadNamespaces, namespace)
	row := &RegisteredCluster{
		ClusterID:            clusterID,
		Name:                 name,
		CredentialHash:       HashClusterCredential(token),
		Source:               ClusterSourceHosted,
		HostedNamespace:      namespace,
		HostedNamespaceAdded: namespaceAdded,
		RegisteredAt:         time.Now().UTC(),
	}
	snapshot := *c
	snapshot.RegisteredClusters = append(copyRegisteredClusters(c.RegisteredClusters), row)
	snapshot.WorkloadNamespaces = hostedAuthorizedNamespaces(c.WorkloadNamespaces, namespace)
	snapshot.HostedCapacityRequestedAt = nil
	s.mu.Unlock()

	ev := NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionHostedClusterAssign,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetCluster,
		TargetID:   clusterID,
		Detail:     AuditDetail{Reason: ReasonHostedClusterAssigned, Namespaces: []string{namespace}},
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return TenantCluster{}, "", fmt.Errorf("%w: persist hosted cluster assignment %s: %w", ErrPersistence, customerID, err)
	}

	s.mu.Lock()
	if live := s.customers[customerID]; live != nil && !live.Revoked() {
		live.RegisteredClusters = snapshot.RegisteredClusters
		live.WorkloadNamespaces = snapshot.WorkloadNamespaces
		live.HostedCapacityRequestedAt = nil
		s.clusterOwners[clusterID] = customerID
		s.clusterCreds[row.CredentialHash] = clusterCredRef{customerID: customerID, clusterID: clusterID}
		s.hostedNamespaces[namespace] = customerID
	}
	s.mu.Unlock()
	return tenantClusterView(row), token, nil
}

// hostedNamespaceOmittedLocked returns the reserved namespace of the first
// hosted assignment that namespaces does not carry — the set a workload
// namespace replacement must refuse. Callers hold s.mu.
func hostedNamespaceOmittedLocked(c *Customer, namespaces []string) (string, bool) {
	for _, rc := range c.RegisteredClusters {
		if rc.Source == ClusterSourceHosted && rc.HostedNamespace != "" && !slices.Contains(namespaces, rc.HostedNamespace) {
			return rc.HostedNamespace, true
		}
	}
	return "", false
}

// hostedClusterLocked finds a tenant's hosted row by cluster id. A row of any
// other source answers ErrClusterNotFound: the operator hosted routes see only
// hosted rows, so they can never operate on a cluster the tenant owns.
// Callers hold s.mu.
func hostedClusterLocked(c *Customer, clusterID string) (int, *RegisteredCluster, error) {
	idx, existing := registeredClusterLocked(c, clusterID)
	if existing == nil || existing.Source != ClusterSourceHosted {
		return -1, nil, ErrClusterNotFound
	}
	return idx, existing, nil
}

// RotateHostedClusterCredential is the operator's recovery path for a hosted
// connector credential: RotateTenantClusterCredential's semantics — the old
// credential stops authenticating the moment the rotation is published, and
// any socket it holds here is evicted in the same mutation — minus the
// membership check the tenant surface applies, because the tenant surface
// refuses hosted rows outright.
func (s *Store) RotateHostedClusterCredential(customerID, clusterID string, by Actor) (TenantCluster, string, error) {
	token := newClusterCredential()

	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok || c.Revoked() {
		s.mu.Unlock()
		return TenantCluster{}, "", ErrNotFound
	}
	idx, existing, err := hostedClusterLocked(c, clusterID)
	if err != nil {
		s.mu.Unlock()
		return TenantCluster{}, "", err
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
		Action:     ActionHostedClusterRotate,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetCluster,
		TargetID:   clusterID,
		Detail:     AuditDetail{Reason: ReasonHostedClusterCredentialRotated},
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return TenantCluster{}, "", fmt.Errorf("%w: persist hosted credential rotation %s: %w", ErrPersistence, customerID, err)
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
	return tenantClusterView(row), token, nil
}

// DeleteHostedCluster removes one hosted assignment: the registry row, its
// credential, the namespace reservation, the namespace's entry in the tenant's
// authorized workload set, and the cluster's gateway route intent — the exact
// grants AssignHostedCluster made plus whatever the connector reported since,
// released in one durable write. Returns the namespace it freed.
//
// The route half matches the tenant delete exactly: the by-cluster set is
// dropped and the policy union recomputed, while the pushed-gateway entry is
// kept as the record the caller's reconcile withdraws the node approval from.
//
// A hosted cluster the tenant's own allow/deny policy still names is refused
// exactly as the tenant delete would refuse it: the policy is tenant-owned,
// and an operator unassignment must not silently move a boundary the tenant
// wrote. The tenant edits the policy first; the assignment lifecycle stays
// recoverable without the operator overriding tenant state.
func (s *Store) DeleteHostedCluster(customerID, clusterID string, by Actor) (string, error) {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok {
		s.mu.Unlock()
		return "", ErrNotFound
	}
	idx, existing, err := hostedClusterLocked(c, clusterID)
	if err != nil {
		s.mu.Unlock()
		return "", err
	}
	if c.ClusterPolicy != nil {
		policy := c.ClusterPolicy.Effective()
		if slices.Contains(policy.Allow, clusterID) || slices.Contains(policy.Deny, clusterID) {
			s.mu.Unlock()
			return "", ErrClusterNamedByPolicy
		}
	}
	oldHash := existing.CredentialHash
	namespace := existing.HostedNamespace
	rows := copyRegisteredClusters(c.RegisteredClusters)
	rows = append(rows[:idx], rows[idx+1:]...)
	if len(rows) == 0 {
		rows = nil
	}
	namespaces := slices.Clone(c.WorkloadNamespaces)
	if namespace != "" && existing.HostedNamespaceAdded {
		namespaces = slices.DeleteFunc(namespaces, func(ns string) bool { return ns == namespace })
		if len(namespaces) == 0 {
			namespaces = nil
		}
	}
	byCluster, union := clearClusterGatewayRoutesLocked(c, clusterID)
	snapshot := *c
	snapshot.RegisteredClusters = rows
	snapshot.WorkloadNamespaces = namespaces
	snapshot.GatewayRoutesByCluster = byCluster
	snapshot.GatewayRoutes = union
	s.mu.Unlock()

	detail := AuditDetail{Reason: ReasonHostedClusterDeleted}
	if namespace != "" {
		detail.Namespaces = []string{namespace}
	}
	ev := NewAuditEvent(AuditEvent{
		CustomerID: customerID,
		Actor:      by,
		Action:     ActionHostedClusterDelete,
		Outcome:    OutcomeAccepted,
		TargetKind: TargetCluster,
		TargetID:   clusterID,
		Detail:     detail,
	})
	if err := s.pSetCustomer(&snapshot, ev); err != nil {
		return "", fmt.Errorf("%w: persist hosted cluster delete %s: %w", ErrPersistence, customerID, err)
	}

	s.mu.Lock()
	if live := s.customers[customerID]; live != nil {
		live.RegisteredClusters = rows
		live.WorkloadNamespaces = namespaces
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
		if namespace != "" {
			if owner, ok := s.hostedNamespaces[namespace]; ok && owner == customerID {
				delete(s.hostedNamespaces, namespace)
			}
		}
	}
	s.evictClusterAgentsLocked(customerID, clusterID)
	s.mu.Unlock()
	return namespace, nil
}

// HostedClusterRequiresScopedCredential reports whether clusterID is a
// platform-managed row belonging to customerID. Legacy tenant tokens remain
// valid for claimed and tenant-registered connectors, but must never mint a
// tailnet identity or open an agent stream as an operator-managed release.
func (s *Store) HostedClusterRequiresScopedCredential(customerID, clusterID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.customers[customerID]
	if c == nil || c.Revoked() {
		return false
	}
	_, row := registeredClusterLocked(c, clusterID)
	return row != nil && row.Source == ClusterSourceHosted
}

// HostedNamespaceForCluster returns the reserved namespace for a hosted
// assignment owned by customerID. The boolean is false for tenant-owned,
// unknown, cross-tenant, and revoked rows so callers cannot use this lookup to
// learn another tenant's hosted inventory.
func (s *Store) HostedNamespaceForCluster(customerID, clusterID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.customers[customerID]
	if c == nil || c.Revoked() {
		return "", false
	}
	_, row := registeredClusterLocked(c, clusterID)
	if row == nil || row.Source != ClusterSourceHosted {
		return "", false
	}
	return row.HostedNamespace, true
}
