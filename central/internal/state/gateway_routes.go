package state

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/yscale-sh/yscale/central/internal/routecidr"
)

// Gateway route intent is stored twice, at two different scopes, because the
// two things central converges want different desired states:
//
//   - The coordination-server POLICY is a tenant-scoped document. Its desired
//     state is the UNION of every cluster's routes (Customer.GatewayRoutes),
//     tracked against PushedGatewayRoutes.
//   - A dedicated yscale-gateway-<cluster> NODE is cluster-scoped. Its desired
//     state is that ONE cluster's exact set (GatewayRoutesByCluster[id]),
//     tracked against PushedGatewayRoutesByCluster[id].
//
// Collapsing them — the pre-existing shape — meant a two-cluster tenant pushed
// the union onto both gateways, so cluster A routed cluster B's CIDRs, and a
// shrink on A could never be told apart from a shrink on B.

// GatewayRouteIntent is an immutable snapshot of one customer's route intent
// and convergence state. CustomerByID hands back the live record, and the
// by-cluster halves are maps: a reconciler ranging one while a report writes
// it is a fatal concurrent map access, not a stale read. Taking a copy under
// the store lock is what makes the reconcile safe to run off the WS goroutine.
type GatewayRouteIntent struct {
	// Union is the tenant-wide canonical policy desired state; PushedUnion is
	// the last union a policy push landed.
	Union       []string
	PushedUnion []string

	// ByCluster and PushedByCluster are the per-gateway halves, keyed by
	// cluster id. A cluster with no entry has not reported since these fields
	// existed, which is a different thing from a tenant with no routes — and
	// it is never filled in from Union, because Union cannot say which gateway
	// any of its CIDRs belong to.
	ByCluster       map[string][]string
	PushedByCluster map[string][]string
}

// RoutesFor returns the exact canonical set cluster clusterID's gateway must
// carry, and whether that cluster has an entry at all.
func (i GatewayRouteIntent) RoutesFor(clusterID string) ([]string, bool) {
	routes, ok := i.ByCluster[clusterID]
	return routes, ok && len(routes) > 0
}

// PushedRoutesFor returns the exact set last approved on cluster clusterID's
// gateway node.
func (i GatewayRouteIntent) PushedRoutesFor(clusterID string) []string {
	return i.PushedByCluster[clusterID]
}

// CustomerGatewayRouteIntent snapshots a customer's route intent. ErrNotFound
// for an unknown customer, so a reconcile of a deleted tenant is a clean skip
// rather than an empty-looking desired state that would withdraw routes.
func (s *Store) CustomerGatewayRouteIntent(customerID string) (GatewayRouteIntent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.customers[customerID]
	if !ok {
		return GatewayRouteIntent{}, ErrNotFound
	}
	return GatewayRouteIntent{
		Union:           slices.Clone(c.GatewayRoutes),
		PushedUnion:     slices.Clone(c.PushedGatewayRoutes),
		ByCluster:       copyRouteSets(c.GatewayRoutesByCluster),
		PushedByCluster: copyRouteSets(c.PushedGatewayRoutesByCluster),
	}, nil
}

// SetCustomerGatewayRoutes records the last non-empty validated route set ONE
// of a customer's clusters reported, and recomputes the tenant-wide policy
// union from every cluster's stored set. changed reports whether either moved,
// which is what the caller enqueues a reconcile on.
//
// clusterID is the identity the report's own connection was admitted under —
// never anything from the message body. It is required: a route set with no
// cluster attached cannot say which gateway must carry it, and writing it into
// the union alone would put CIDRs into policy that no gateway is ever approved
// for. The agent stream refuses a Hello without a cluster id, so every live
// report has one.
//
// The cluster must be CURRENTLY one of this customer's — a live registry row,
// or a connector this replica still holds a socket for. "Not owned by anyone
// else" is too weak: a deleted cluster is owned by nobody, so a report already
// read off a socket the delete evicted would pass it, RECREATE the by-cluster
// entry the delete just dropped, and widen the tenant's policy union back to
// include a cluster central has agreed is gone. Nothing would ever take it out
// again; the connector is not coming back to report a smaller set. The agent
// stream claims or registers its cluster before it will accept a single report
// from that connection, so requiring the claim to still stand is the same
// invariant the socket was admitted under, just re-read at write time.
//
// The GRAMMAR is deliberately not a hard gate. ClaimAgentCluster admits a
// pre-registry connector whose self-chosen id predates the grammar live-only —
// no registry row at all — rather than bricking a running fleet, so the live
// socket is what proves that cluster's claim, and route intent keyed by the
// same proven id inherits exactly the identity the socket was admitted under.
// Refusing it would instead freeze that tenant's policy at whatever it last
// held.
//
// An EMPTY report is ignored — not a withdrawal. An agent that has not
// resolved its CIDRs yet reports nothing, and taking that as "remove this
// cluster's routes" would cut the tenant's data path on a transient lookup
// failure. Withdrawal happens through a SMALLER non-empty report, which
// replaces this cluster's set exactly, or through the cluster's lifecycle
// delete (clearClusterGatewayRoutesLocked).
//
// MIGRATION. A tenant persisted before the by-cluster map has a union and no
// per-cluster entries. The union here is recomputed from the by-cluster map
// alone, so the FIRST report from any of that tenant's clusters supersedes the
// legacy union with just that cluster's set, and the tenant's other clusters
// widen it back as their own reports arrive — each connector sends one right
// after it connects, so the window is one reconnect wide. On a multi-cluster
// legacy tenant that is a narrowing, which is the pre-existing last-writer-wins
// behavior of a tenant-scoped union rather than something the by-cluster map
// introduced. It is also the only safe direction: which cluster advertised a
// legacy CIDR was never recorded, so carrying it forward would mean attributing
// it to whichever gateway reported first and approving one cluster's route on
// another cluster's node. Nothing here ever writes a legacy CIDR into a
// by-cluster entry.
func (s *Store) SetCustomerGatewayRoutes(customerID, clusterID string, routes []string) (changed bool, err error) {
	if clusterID == "" {
		return false, fmt.Errorf("%w: gateway routes require a cluster id", ErrInvalidCluster)
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok {
		s.mu.Unlock()
		return false, ErrNotFound
	}
	if s.clusterIDTakenLocked(clusterID, customerID) {
		s.mu.Unlock()
		return false, fmt.Errorf("%w: %s", ErrClusterOwnedElsewhere, clusterID)
	}
	if !s.clusterHeldByLocked(c, clusterID, customerID) {
		s.mu.Unlock()
		return false, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterID)
	}
	incoming, invalid := routecidr.CanonicalizeCIDRs(routes)
	if len(invalid) > 0 {
		s.mu.Unlock()
		return false, fmt.Errorf("invalid gateway route CIDR %q", invalid[0])
	}
	if len(incoming) == 0 {
		s.mu.Unlock()
		return false, nil
	}
	byCluster := copyRouteSets(c.GatewayRoutesByCluster)
	if byCluster == nil {
		byCluster = map[string][]string{}
	}
	byCluster[clusterID] = incoming
	union := unionRouteSets(byCluster)

	if slices.Equal(c.GatewayRoutesByCluster[clusterID], incoming) && slices.Equal(c.GatewayRoutes, union) {
		s.mu.Unlock()
		return false, nil
	}
	// Copy-on-write, both halves: the snapshot below and any CustomerByID
	// caller share this record's map header until it is replaced.
	c.GatewayRoutesByCluster = byCluster
	c.GatewayRoutes = union
	snapshot := *c
	s.mu.Unlock()
	if err := s.pUpsertCustomer(&snapshot); err != nil {
		return false, err
	}
	return true, nil
}

// SetCustomerPushedGatewayRoutes records the tenant-wide union after a
// successful coordination-server POLICY push. It is deliberately independent
// of the per-gateway state: a policy that landed really is converged, even
// while one cluster's gateway node is still missing, and holding it back would
// re-push an identical policy on every retry of that gateway.
//
// An already-current union is written AGAIN rather than short-circuited. The
// in-memory value being current says nothing about the durable one: a customer
// row whose write was refused, or torn by a crash mid-upsert, leaves the two
// disagreeing, and an equality check on the in-memory copy would decline to fix
// it for the life of the process — the one convergence loop with no other way
// back. Re-writing what is already there is idempotent, so self-heal is the
// only behavior the check was buying anything against.
//
// What it was actually protecting against was a WRITE LOOP: retries of one
// permanently-missing gateway used to drag the policy half along, so every
// backoff rewrote the tenant's whole row to store the bytes already in it. That
// is fixed at the source — the reconciler now gates the policy half on its own
// evidence, and a node-only retry carries none, so it never reaches here.
func (s *Store) SetCustomerPushedGatewayRoutes(customerID string, routes []string) error {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok {
		s.mu.Unlock()
		return ErrNotFound
	}
	incoming, invalid := routecidr.CanonicalizeCIDRs(routes)
	if len(invalid) > 0 {
		s.mu.Unlock()
		return fmt.Errorf("invalid pushed gateway route CIDR %q", invalid[0])
	}
	c.PushedGatewayRoutes = append([]string(nil), incoming...)
	snapshot := *c
	s.mu.Unlock()
	return s.pUpsertCustomer(&snapshot)
}

// SetCustomerPushedClusterGatewayRoutes records the exact set successfully
// approved on ONE cluster's gateway node. Only the named cluster's entry is
// written, so a gateway that could not be resolved leaves its own convergence
// state behind — the next reconcile still sees work to do — while its
// siblings keep theirs.
func (s *Store) SetCustomerPushedClusterGatewayRoutes(customerID, clusterID string, routes []string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: pushed gateway routes require a cluster id", ErrInvalidCluster)
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok {
		s.mu.Unlock()
		return ErrNotFound
	}
	incoming, invalid := routecidr.CanonicalizeCIDRs(routes)
	if len(invalid) > 0 {
		s.mu.Unlock()
		return fmt.Errorf("invalid pushed gateway route CIDR %q", invalid[0])
	}
	pushed := copyRouteSets(c.PushedGatewayRoutesByCluster)
	if pushed == nil {
		pushed = map[string][]string{}
	}
	pushed[clusterID] = incoming
	if slices.Equal(c.PushedGatewayRoutesByCluster[clusterID], incoming) {
		s.mu.Unlock()
		return nil
	}
	c.PushedGatewayRoutesByCluster = pushed
	snapshot := *c
	s.mu.Unlock()
	return s.pUpsertCustomer(&snapshot)
}

// ClearCustomerPushedClusterGatewayRoutes drops ONE cluster's pushed-gateway
// tombstone. It is the last step of a deleted cluster's withdrawal: once the
// empty-route replacement has landed on that gateway node (or the node is
// gone), the record of what central approved there has no reader left, and
// keeping it would grow PushedGatewayRoutesByCluster by one dead entry per
// cluster the tenant ever deletes.
//
// Absent is already clear, and answers without a durable write.
func (s *Store) ClearCustomerPushedClusterGatewayRoutes(customerID, clusterID string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: clearing pushed gateway routes requires a cluster id", ErrInvalidCluster)
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok {
		s.mu.Unlock()
		return ErrNotFound
	}
	if _, ok := c.PushedGatewayRoutesByCluster[clusterID]; !ok {
		s.mu.Unlock()
		return nil
	}
	pushed := copyRouteSets(c.PushedGatewayRoutesByCluster)
	delete(pushed, clusterID)
	if len(pushed) == 0 {
		pushed = nil
	}
	c.PushedGatewayRoutesByCluster = pushed
	snapshot := *c
	s.mu.Unlock()
	return s.pUpsertCustomer(&snapshot)
}

// ReapUnheldClusterGatewayRoutes reaps the DESIRED gateway routes of the named
// clusters this customer no longer holds — no durable registry row, and no live
// socket on this replica — and recomputes the tenant-wide policy union from the
// clusters that remain. It returns the cluster ids it dropped, which is the
// caller's list of policy/node withdrawals to enqueue. A nil clusterIDs means
// every unheld cluster.
//
// It exists for the one entry nothing else can remove. A pre-grammar legacy
// connector is admitted LIVE-ONLY (see ClaimAgentCluster): it claims no registry
// row, so its socket is the whole of its claim — and the route intent it reports
// is keyed by that same socket-proven id. There is no lifecycle delete to run
// against a cluster that was never in the registry, so without this its set, and
// the tenant policy union built from it, would outlive the connector for the
// life of the process and every restart after it. That is also what bounds the
// union: an entry may only belong to a cluster the tenant still holds, so the
// tenant's policy is at most (registry rows + live sockets) sets of at most
// routecidr.MaxReportedCIDRs each.
//
// The NARROWING to a candidate list is what makes a GRACE PERIOD possible. A
// socket closing is not by itself intent to give up a cluster's routes — a
// connector restarting, a node rolling, a network blip all close one — so the
// disconnect boundary starts a lease per cluster instead of reaping, and the reap
// that eventually runs may only take the cluster whose lease actually expired.
// Without it that one expiry would sweep a sibling that disconnected a moment ago
// and is still inside its own grace.
//
// Fail-closed by construction. The ownership re-check is made HERE, under the
// same hold of the store lock as the write, and not by the caller: a lease says a
// cluster was unheld when the timer was armed, which is exactly the fact a
// reconnect invalidates. So a socket that connects — or a row that is created —
// while this runs is either seen and skipped, or lands after the write and
// re-establishes the entry with its own report. Nothing is ever dropped on
// evidence that was true a moment ago, and a customer that is gone is
// ErrNotFound rather than an empty-looking answer.
//
// PushedGatewayRoutesByCluster survives for every reaped cluster, for the reason
// clearClusterGatewayRoutesLocked keeps it: that absent-desired / present-pushed
// pair is the durable node-withdrawal tombstone, and it is the only record that
// central ever approved routes on that gateway.
func (s *Store) ReapUnheldClusterGatewayRoutes(customerID string, clusterIDs []string) ([]string, error) {
	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	c, ok := s.customers[customerID]
	if !ok {
		s.mu.Unlock()
		return nil, ErrNotFound
	}
	pruned := s.unheldRouteClustersLocked(c)
	if clusterIDs != nil {
		candidates := make(map[string]bool, len(clusterIDs))
		for _, id := range clusterIDs {
			candidates[id] = true
		}
		pruned = slices.DeleteFunc(pruned, func(id string) bool { return !candidates[id] })
	}
	if len(pruned) == 0 {
		s.mu.Unlock()
		return nil, nil
	}
	byCluster, union := dropClusterGatewayRoutesLocked(c, pruned)
	c.GatewayRoutesByCluster = byCluster
	c.GatewayRoutes = union
	snapshot := *c
	s.mu.Unlock()
	if err := s.pUpsertCustomer(&snapshot); err != nil {
		return nil, err
	}
	return pruned, nil
}

// ConsumeUnheldRouteClusters names, per customer, every cluster holding stored
// gateway route intent the tenant does not currently hold, and CLAIMS each one it
// returns: no later call on this Store hands that cluster out again. It is what
// the boot reap arms from, and the only way to ask — a read-only half that named
// the same candidates without claiming them was a second door onto a decision
// that has exactly one caller.
//
// It exists for BOOT. A process that has just started holds no sockets, so a
// live-only connector's entry is indistinguishable from an abandoned one until
// that connector reconnects — which is why load must not reap (see
// applySnapshot). Constructing the agent stream takes this list, arms the same
// grace lease a disconnect arms, and lets ReapUnheldClusterGatewayRoutes make the
// actual decision under the store lock once the grace has run out.
//
// Customers with nothing fresh are left out entirely, so an ordinary boot returns
// an empty map and arms no leases at all.
//
// The arming is once per STORE, not once per handler, because a handler is not
// what a boot is. Two agent streams wired to the same store — a second entry
// point, a test harness, a wiring that constructs one per listener — each armed
// their own lease over the same clusters, so the second construction restarted
// every grace period the first had already started counting: a fleet that
// constructs streams periodically would defer its boot reap forever. The store is
// the one thing they share, so the claim lives here.
//
// Per cluster rather than one flag for the whole call, so that a store which has
// nothing unheld yet does not spend the claim on an empty list. Clusters that
// become unheld later are still the disconnect boundary's work, and that path
// arms its lease directly.
//
// In-memory only: this is the current process's arming, and nothing about it
// should survive into the next one — the next boot owes those clusters a lease of
// their own.
func (s *Store) ConsumeUnheldRouteClusters() map[string][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.startupReapArmed == nil {
		s.startupReapArmed = map[string]struct{}{}
	}
	out := map[string][]string{}
	for _, customerID := range slices.Sorted(maps.Keys(s.customers)) {
		var fresh []string
		for _, clusterID := range s.unheldRouteClustersLocked(s.customers[customerID]) {
			key := startupReapKey(customerID, clusterID)
			if _, armed := s.startupReapArmed[key]; armed {
				continue
			}
			s.startupReapArmed[key] = struct{}{}
			fresh = append(fresh, clusterID)
		}
		if len(fresh) > 0 {
			out[customerID] = fresh
		}
	}
	return out
}

// startupReapKey pairs a customer with one of its clusters. The NUL separator
// cannot occur in either id, so no two pairs collide.
func startupReapKey(customerID, clusterID string) string { return customerID + "\x00" + clusterID }

// unheldRouteClustersLocked names, in a deterministic order, every cluster with
// a stored desired route set the customer no longer holds. "Held" is exactly
// clusterHeldByLocked — the same predicate a report is admitted under — so a
// cluster can only be reaped on the very evidence that would have let it write
// the entry in the first place. Callers hold s.mu.
func (s *Store) unheldRouteClustersLocked(c *Customer) []string {
	var out []string
	for _, clusterID := range slices.Sorted(maps.Keys(c.GatewayRoutesByCluster)) {
		if s.clusterHeldByLocked(c, clusterID, c.ID) {
			continue
		}
		out = append(out, clusterID)
	}
	return out
}

// SharedTailnetGatewayRoutes is the canonical UNION of the gateway routes of
// every tenant that rides the SHARED tailnet — the desired source set of the one
// kubelet ACL rule central manages there.
//
// Cross-tenant by necessity, not by convenience. The shared tailnet holds ONE
// policy document for every customer without a mesh box, so that rule's Src is a
// shared value: a write built from the customer being reconciled alone would
// revoke every other tenant's CIDRs on it. A customer WITH a mesh box owns a
// whole policy namespace of its own and contributes nothing here, and neither
// does a revoked one — its access is gone, and an ACL is access.
func (s *Store) SharedTailnetGatewayRoutes() []string {
	return s.SharedTailnetRouteIntent().Desired
}

// SharedTailnetIntent is an immutable snapshot of the shared tailnet's ONE
// managed kubelet rule: the Src it must carry, the Src central last put there,
// and the per-tenant contributions the desired union was built from.
//
// All three come out of a single hold of the store lock because they are one
// decision. Reading the desired union and the pushed union separately would let
// a tenant change in between, and the pushed union is the value the write uses
// to prove which rule is central's — a pushed union that never matched the
// desired one it is compared against turns a converged tailnet into a rewrite.
type SharedTailnetIntent struct {
	// Desired is the canonical union every tenant currently on the shared
	// tailnet contributes; Pushed is the canonical union of what central
	// recorded after the last successful write there.
	Desired []string
	Pushed  []string

	// Claims are the write-ahead ownership claims outstanding when this snapshot
	// was taken, oldest first: unions central recorded it was about to write and
	// has not yet proven gone. Empty on a tailnet whose last write completed.
	Claims [][]string

	// ByCustomer is Desired split back into the contributions it was summed
	// from, keyed by customer id. Every tenant on the shared tailnet is in it,
	// the ones contributing NOTHING included: a tenant whose routes have gone
	// has converged to exactly that, and leaving it out would keep its own
	// reconcile asking for work the shared document has already done.
	ByCustomer map[string][]string
}

// Proof is every Src central may have put on the shared tailnet's managed rule
// and cannot yet prove is gone: the union it recorded after its last successful
// write, plus every outstanding write-ahead claim. It is the whole of the
// ownership evidence a replace matches a rule against before deleting it.
//
// Pushed alone was not enough, and that is the defect this exists for. A POST
// that lands and a receipt that does not leaves a rule carrying a union nothing
// durable names; the retry then arrives with the OLD pushed union, and — once
// the desired union has moved on again — with a new src that does not match it
// either. The just-written rule matches neither candidate, survives the replace
// that should have taken it back, and grants :10250 to CIDRs no tenant
// advertises for as long as the tailnet exists. The claim is written before the
// POST precisely so that this list can name it afterwards.
//
// Empty unions are dropped: proving nothing is what an empty union does, and a
// replace that treated it as evidence would match every rule shaped like
// central's, including an operator's.
func (i SharedTailnetIntent) Proof() [][]string {
	out := make([][]string, 0, len(i.Claims)+1)
	for _, union := range append([][]string{i.Pushed}, i.Claims...) {
		if len(union) == 0 || containsUnion(out, union) {
			continue
		}
		out = append(out, slices.Clone(union))
	}
	return out
}

// SharedTailnetRouteIntent snapshots the shared tailnet's managed-rule state.
//
// Cross-tenant by necessity, not by convenience. The shared tailnet holds ONE
// policy document for every customer without a mesh box, so that rule's Src is a
// shared value: a write built from the customer being reconciled alone would
// revoke every other tenant's CIDRs on it. A customer WITH a mesh box owns a
// whole policy namespace of its own and contributes nothing here, and neither
// does a revoked one — its access is gone, and an ACL is access.
//
// A tenant that STOPS contributing therefore shrinks Desired the moment it is
// revoked, deleted, or attached to its own box, while Pushed still carries its
// CIDRs — which is precisely what makes the revocation visible as work.
func (s *Store) SharedTailnetRouteIntent() SharedTailnetIntent {
	s.mu.RLock()
	var all []string
	byCustomer := map[string][]string{}
	for _, customerID := range slices.Sorted(maps.Keys(s.customers)) {
		c := s.customers[customerID]
		if c.HasMeshBox() || c.Revoked() {
			continue
		}
		byCustomer[customerID] = slices.Clone(c.GatewayRoutes)
		all = append(all, c.GatewayRoutes...)
	}
	pushed := slices.Clone(s.sharedTailnetPushed)
	claims := cloneRouteUnions(s.sharedTailnetClaims)
	s.mu.RUnlock()
	desiredUnion, _ := routecidr.CanonicalizeCIDRs(all)
	return SharedTailnetIntent{Desired: desiredUnion, Pushed: pushed, Claims: claims, ByCustomer: byCustomer}
}

// ErrSharedTailnetClaimsFull refuses a new distinct shared-tailnet ownership
// claim: the bounded proof set is full, so there is no way to record the union
// about to be written, and a caller that saw this must not write.
var ErrSharedTailnetClaimsFull = errors.New("shared tailnet ownership proof set is full")

// maxSharedTailnetClaims bounds the write-ahead proof set. A claim is only
// retired by a write that completes, so a tailnet whose ACL API is refusing
// writes while its tenants keep changing their routes would otherwise accumulate
// one entry per distinct union forever, in a row every boot reads back.
//
// At the cap the new claim is REFUSED and every existing one kept. Evicting the
// oldest to make room was the wrong trade: a claim is only in the set because
// its POST may have landed, so dropping one makes a rule central may have
// written permanently unprovable, and no later replace can match — that grant
// then outlives the tailnet. Refusing costs a failed reconcile attempt, and
// attempts retry: the set collapses to nothing the moment ONE write completes
// (RecordSharedTailnetPushed), which is what unblocks progress.
const maxSharedTailnetClaims = 8

// ClaimSharedTailnetPush records, DURABLY and BEFORE the ACL write, that central
// is about to put intent.Desired on the shared tailnet's managed rule. It returns
// the intent to write with, whose Proof now names that union.
//
// This is the write-ahead half of the two-phase write the shared document needs.
// The rule a POST installs exists from the moment the API accepts it, whether or
// not central lives long enough to record it, so ownership CANNOT be established
// afterwards: a crash — or a refused durable write — between the POST and
// RecordSharedTailnetPushed leaves a grant nothing durable can name. Claiming
// first inverts that. The worst a claim that was never written costs is one
// extra candidate Src on the next replace, and that Src is one central chose; the
// worst a missing claim costs is a permanent over-grant.
//
// A caller that gets an error here must NOT write: an unclaimed POST is exactly
// the orphan this prevents.
//
// Nothing is written when the desired union is already provable — it is empty
// (there is no new rule to own), it equals the recorded pushed union, or it is
// already claimed — so a retry ladder against a failing ACL API costs one durable
// write per DISTINCT union, not one per attempt. Re-claiming a union the set
// already holds stays idempotent at the cap for exactly that reason.
//
// A NEW distinct union at maxSharedTailnetClaims is refused with
// ErrSharedTailnetClaimsFull, before any write and with every existing proof
// intact. Fail-closed: the alternative is writing a rule central cannot take
// back, or forgetting one it may already have written.
func (s *Store) ClaimSharedTailnetPush(intent SharedTailnetIntent) (SharedTailnetIntent, error) {
	desired, invalid := routecidr.CanonicalizeCIDRs(intent.Desired)
	if len(invalid) > 0 {
		return intent, fmt.Errorf("invalid shared gateway route CIDR %q", invalid[0])
	}
	s.custMu.Lock()
	defer s.custMu.Unlock()
	// Re-read under the lock rather than trusting the caller's snapshot: the
	// durable row this writes has to carry the pushed union that is actually
	// current, or a claim would silently roll it back.
	s.mu.RLock()
	pushed := slices.Clone(s.sharedTailnetPushed)
	claims := cloneRouteUnions(s.sharedTailnetClaims)
	s.mu.RUnlock()

	claimed := intent
	claimed.Desired = desired
	claimed.Pushed = pushed
	claimed.Claims = claims
	if len(desired) == 0 || slices.Equal(desired, pushed) || containsUnion(claims, desired) {
		return claimed, nil
	}
	if len(claims) >= maxSharedTailnetClaims {
		return claimed, fmt.Errorf("%w: %d unions outstanding, none provably gone; a completed ACL write collapses them",
			ErrSharedTailnetClaimsFull, len(claims))
	}
	claims = append(claims, desired)
	if err := s.pSetMeshState(&meshState{SharedKubeletRoutes: pushed, SharedKubeletClaims: claims}); err != nil {
		return claimed, err
	}
	s.mu.Lock()
	s.sharedTailnetClaims = claims
	s.mu.Unlock()
	claimed.Claims = claims
	return claimed, nil
}

// RecordSharedTailnetPushed records the Src a shared-tailnet ACL write actually
// landed, plus the per-tenant halves it was summed from.
//
// Called only AFTER the ACL write succeeded. Recording it first would claim a
// rule central had not written, and the next write would then delete whatever
// rule happened to carry that Src — including an operator's.
//
// It also COLLAPSES the write-ahead proof. The write that just returned replaced
// every rule matching any claim with exactly one carrying written.Desired (or
// removed them all, when that union is empty), so the document provably holds one
// central-owned rule and the recorded union names it. Keeping the claims past
// that point is what would grow the proof set with history; dropping them in the
// SAME durable row as the new pushed union is what keeps a crash from splitting
// the two.
//
// Durable FIRST, and fail-closed: an error leaves the store exactly as it was,
// so nothing here reports a proof that no restart could read back — and the
// claims survive to cover the rule that write did land. The tenants' own pushed
// unions follow it, best-effort like every other operational customer write,
// because they are convergence bookkeeping rather than authorship.
func (s *Store) RecordSharedTailnetPushed(written SharedTailnetIntent) error {
	incoming, invalid := routecidr.CanonicalizeCIDRs(written.Desired)
	if len(invalid) > 0 {
		return fmt.Errorf("invalid shared gateway route CIDR %q", invalid[0])
	}
	contributions := canonicalContributions(written.ByCustomer)
	s.custMu.Lock()
	defer s.custMu.Unlock()
	if err := s.pSetMeshState(&meshState{SharedKubeletRoutes: incoming}); err != nil {
		return err
	}
	s.mu.Lock()
	s.sharedTailnetPushed = incoming
	s.sharedTailnetClaims = nil
	changed := advanceSharedTenantsLocked(s.customers, contributions)
	s.mu.Unlock()
	var writeErr error
	for i := range changed {
		writeErr = errors.Join(writeErr, s.pUpsertCustomer(&changed[i]))
	}
	return writeErr
}

// AdvanceSharedTailnetTenants moves the per-tenant pushed bookkeeping forward for
// a shared document that ALREADY carries what those tenants want, without writing
// the ACL or the global mesh-state row.
//
// The case it exists for is two shared tenants advertising the SAME CIDRs. The
// first one's write converges the cross-tenant union, so when the second reports
// there is no ACL work left to do at all — and the early return that recognises
// that used to skip the per-tenant record with it, leaving the second tenant's
// PushedGatewayRoutes stale for good. Its own reconcile then re-asks the shared
// worker on every change forever, and its withdrawal tombstones can never be
// dropped, because both are gated on bookkeeping no write will ever advance.
//
// Only correct where the caller has established that Desired and Pushed agree:
// this asserts a convergence rather than performing one.
//
// The read-locked pre-pass is not an optimisation for its own sake. This runs on
// EVERY enqueue against an already-converged shared tailnet, which is the steady
// state of the whole shared fleet, and without it a fleet with nothing to do
// still took the customer WRITE lock per enqueue. Skipping is only ever safe in
// the direction of doing nothing: a contribution that does not already match its
// tenant's bookkeeping falls through to the authoritative path below.
//
// Both sides of that comparison are canonical, and have to be. The stored half
// already is, so a pre-pass reading the caller's raw contribution would call a
// converged tenant stale on nothing but spelling — taking the write lock every
// enqueue for a set the authoritative comparison then finds unchanged. The
// canonical form is therefore computed ONCE, up front and outside both locks,
// and the same map is what the slow path advances from.
func (s *Store) AdvanceSharedTailnetTenants(intent SharedTailnetIntent) {
	canonical := canonicalContributions(intent.ByCustomer)
	s.mu.RLock()
	stale := false
	for customerID, routes := range canonical {
		if c, ok := s.customers[customerID]; ok && !slices.Equal(c.PushedGatewayRoutes, routes) {
			stale = true
			break
		}
	}
	s.mu.RUnlock()
	if !stale {
		return
	}

	s.custMu.Lock()
	defer s.custMu.Unlock()
	s.mu.Lock()
	changed := advanceSharedTenantsLocked(s.customers, canonical)
	s.mu.Unlock()
	for i := range changed {
		s.pUpsertCustomer(&changed[i])
	}
}

// advanceSharedTenantsLocked points each named tenant's PushedGatewayRoutes at
// the contribution the shared document carries for it, and returns snapshots of
// the ones that moved for the caller to persist. Callers hold s.mu and pass
// CANONICAL contributions (canonicalContributions), because the stored half is
// canonical and the comparison here is what decides whether a row is written.
//
// On this path the shared ACL IS that tenant's policy, so the two converge at the
// same instant; leaving the per-tenant half behind would keep its own reconcile
// asking for work that has already been done. A tenant that went away between the
// write and this record is skipped — there is nothing left to converge.
func advanceSharedTenantsLocked(customers map[string]*Customer, byCustomer map[string][]string) []Customer {
	var changed []Customer
	for _, customerID := range slices.Sorted(maps.Keys(byCustomer)) {
		c, ok := customers[customerID]
		if !ok {
			continue
		}
		routes := byCustomer[customerID]
		if slices.Equal(c.PushedGatewayRoutes, routes) {
			continue
		}
		c.PushedGatewayRoutes = slices.Clone(routes)
		changed = append(changed, *c)
	}
	return changed
}

// canonicalContributions is the per-tenant half of a shared-tailnet intent in the
// one spelling the stored bookkeeping uses. Invalid CIDRs are dropped rather than
// refused: this is convergence bookkeeping for a union that has already been
// written, and the write itself is what validates.
func canonicalContributions(byCustomer map[string][]string) map[string][]string {
	out := make(map[string][]string, len(byCustomer))
	for customerID, routes := range byCustomer {
		out[customerID], _ = routecidr.CanonicalizeCIDRs(routes)
	}
	return out
}

// SharedTailnetCustomerIDs names, deterministically, every tenant that currently
// rides the shared tailnet. It is the boot replay's work list: a restart has to
// re-drive the shared document itself and the per-tenant bookkeeping a delete
// left durable, and neither has a connector obliged to come back and ask.
func (s *Store) SharedTailnetCustomerIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for _, customerID := range slices.Sorted(maps.Keys(s.customers)) {
		if c := s.customers[customerID]; !c.HasMeshBox() && !c.Revoked() {
			out = append(out, customerID)
		}
	}
	return out
}

// clearClusterGatewayRoutesLocked computes a customer's route intent after one
// cluster's LIFECYCLE DELETE: that cluster's desired set is dropped and the
// tenant-wide policy union recomputed from the clusters that remain. Callers
// hold s.mu and fold the results into the same durable snapshot as the registry
// row removal, so a tenant cannot end up with a deleted cluster's CIDRs still
// in policy because the second of two writes was the one that failed.
//
// PushedGatewayRoutesByCluster[clusterID] is deliberately LEFT IN PLACE. It is
// the only record of what central approved on that gateway node, and the node
// can outlive the registry row — a chart-managed yscale-gateway-<cluster> pod
// keeps its coordination-server registration until someone removes it.
// Withdrawing that approval is the delete's reconcile, which clears the
// tombstone once it lands.
//
// That absent-desired / present-pushed pair is therefore a DURABLE tombstone,
// not a scratch note: it rides the same customer document as everything else,
// so a central that dies between the delete and the withdrawal finds it again
// on the next boot. Nothing else could — the connector is gone, so no reconnect
// will ever name that cluster again.
//
// A cluster with no entry changes nothing — the customer's current values come
// straight back: its routes were never attributed to it (a legacy tenant whose
// union predates the by-cluster map), and there is no way to tell which of the
// union's CIDRs were its, so nothing is guessed.
func clearClusterGatewayRoutesLocked(c *Customer, clusterID string) (byCluster map[string][]string, union []string) {
	return dropClusterGatewayRoutesLocked(c, []string{clusterID})
}

// dropClusterGatewayRoutesLocked is the same computation for a SET of clusters:
// each named cluster's desired entry removed and the union recomputed once from
// what is left. The registry-cap prune and the unheld-cluster reap fold their
// results into the same durable snapshot as whatever removed those clusters, for
// the reason above. Callers hold s.mu.
//
// A cluster with no entry changes nothing, and a call that finds none returns
// the customer's current values untouched — so a caller can hand it a
// speculative list without either copying the map or writing a row for nothing.
func dropClusterGatewayRoutesLocked(c *Customer, clusterIDs []string) (byCluster map[string][]string, union []string) {
	byCluster = c.GatewayRoutesByCluster
	dropped := false
	for _, clusterID := range clusterIDs {
		if _, ok := byCluster[clusterID]; !ok {
			continue
		}
		if !dropped {
			byCluster = copyRouteSets(byCluster)
			dropped = true
		}
		delete(byCluster, clusterID)
	}
	if !dropped {
		return c.GatewayRoutesByCluster, c.GatewayRoutes
	}
	if len(byCluster) == 0 {
		byCluster = nil
	}
	return byCluster, unionRouteSets(byCluster)
}

// unionRouteSets is the tenant-wide policy desired state: every cluster's
// routes, canonicalized, deduplicated and sorted into one set. Each stored set
// is already canonical, so nothing here can be invalid.
func unionRouteSets(byCluster map[string][]string) []string {
	var all []string
	for _, clusterID := range slices.Sorted(maps.Keys(byCluster)) {
		all = append(all, byCluster[clusterID]...)
	}
	union, _ := routecidr.CanonicalizeCIDRs(all)
	return union
}

// cloneRouteUnions deep-copies an ordered list of canonical unions. nil in, nil
// out, for the reason copyRouteSets keeps nil: an absent list and an empty one
// both mean "no outstanding claims", and keeping nil keeps the field out of the
// persisted JSON.
func cloneRouteUnions(in [][]string) [][]string {
	if len(in) == 0 {
		return nil
	}
	out := make([][]string, 0, len(in))
	for _, union := range in {
		out = append(out, slices.Clone(union))
	}
	return out
}

// containsUnion reports whether unions already holds one equal to want. The
// stored unions are canonical — sorted and deduplicated by routecidr — so
// element-wise equality is set equality here.
func containsUnion(unions [][]string, want []string) bool {
	return slices.ContainsFunc(unions, func(u []string) bool { return slices.Equal(u, want) })
}

// copyRouteSets deep-copies a by-cluster route map. nil in, nil out: an absent
// map and an empty one mean the same thing (nothing reported since the
// upgrade), and keeping nil keeps it out of the persisted JSON.
func copyRouteSets(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for clusterID, routes := range in {
		out[clusterID] = slices.Clone(routes)
	}
	return out
}
