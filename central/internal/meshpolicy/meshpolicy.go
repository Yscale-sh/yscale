// yscale:proprietary

// Package meshpolicy owns the per-customer Headscale reconcile loop: it builds
// the tagOwner/route-approver policy from the UNION of a customer's validated,
// agent-reported gateway routes, PUTs it to that customer's Headscale box, and
// converges each of that customer's gateway NODES to its OWN cluster's exact
// route set — serialized per customer and off the WebSocket read goroutine.
//
// The approver builder (formerly central/cmd/yscale-cloud's pushHeadscalePolicy)
// lives here so both boot (attachHeadscaleFromEnv) and the agent-stream handler
// reach it without importing package main. Handlers depend only on the
// handlers.PolicyReconciler interface, which *Reconciler satisfies.
package meshpolicy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/central/internal/decider"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/routecidr"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/central/internal/tailscale"
	"github.com/yscale-sh/yscale/factory/client"
)

// PolicyEnsurer is the subset of *client.Headscale the reconciler needs. It is an
// interface so tests can inject a mock that records the calls without a live
// Headscale box.
//
// The two node methods are here rather than behind a separate optional
// interface on purpose: a type assertion would let a future change to
// *client.Headscale's method set turn gateway route approval off silently,
// and a customer whose gateway routes are advertised but never approved has no
// cross-cluster data path at all.
type PolicyEnsurer interface {
	EnsurePolicy(ctx context.Context, tagOwners, routeApprovers map[string][]string, extraACLs []client.PolicyACL) error
	FindDeviceByHostname(ctx context.Context, hostname string) (string, error)
	ApproveNodeRoutes(ctx context.Context, nodeID string, routes []string) error
}

// tailnetEnsurer is the subset of *tailscale.Client the reconciler uses to
// converge the shared-tailnet (Tailscale SaaS) kubelet ACL for customers that
// ride the default shared tailnet instead of a per-customer Headscale box. It
// mirrors tailscale.Client.ReplaceManagedKubeletACL exactly so the concrete
// client satisfies it with no adapter; an interface (not the concrete type) lets
// tests inject a recorder without a live Tailscale API.
//
// It is an EXACT REPLACE, not the additive merge this used to call. The shared
// tailnet holds ONE policy document for every box-less tenant, so a merge could
// only ever grow the kubelet rule's Src: a tenant whose gateway CIDRs shrank, or
// whose last cluster was deleted, left the old CIDRs granted on :10250 with
// nothing able to take them back. Replace is what makes revocation possible; the
// union recomputed across ALL shared tenants on every call (see
// state.SharedTailnetRouteIntent) is what keeps one tenant's reconcile from
// erasing another's.
//
// owned is the durable ownership PROOF SET: every union central may have put on
// that rule and cannot yet prove is gone (state.SharedTailnetIntent.Proof). It
// travels with the desired one because the replace has to prove which rule on a
// shared document is central's before it deletes anything — a destination is not
// a signature — and one recorded union is not enough to cover a write whose
// receipt was lost.
//
// CollapseManagedKubeletACL is the exceptional-recovery half, for a proof set
// standing at its cap. It takes the pushed union and the claims SEPARATELY
// because it chooses which of them to leave on the document, and that choice has
// to be made against the document itself, under the same Etag as the write that
// installs it — a claim is durable before its POST, so choosing from the record
// alone could install a union the tailnet has never carried. It returns the
// union that actually landed, which is the only one this reconciler may then
// record as pushed.
type tailnetEnsurer interface {
	ReplaceManagedKubeletACL(ctx context.Context, dst string, owned [][]string, src []string) error
	CollapseManagedKubeletACL(ctx context.Context, dst string, pushed []string, claims [][]string) ([]string, error)
}

// EnsurerFactory builds a PolicyEnsurer from a customer's mesh endpoint.
type EnsurerFactory func(ep *state.MeshEndpoint) PolicyEnsurer

func defaultEnsurer(ep *state.MeshEndpoint) PolicyEnsurer {
	return client.NewHeadscale(ep.LoginServer, ep.APIKey, ep.User)
}

const (
	defaultDebounce = 250 * time.Millisecond
	defaultBackoff  = 5 * time.Second
	// defaultMaxBackoff caps the retry ladder. A gateway pod that is never
	// coming back would otherwise have central re-asking that tenant's box
	// every defaultBackoff for the life of the process.
	defaultMaxBackoff = 5 * time.Minute
)

// Reconciler runs the per-customer serialized Headscale policy reconcile.
type Reconciler struct {
	store   *state.Store
	log     *slog.Logger
	factory EnsurerFactory

	// fabric builds the ensurer for factory-provisioned boxes
	// (Mesh.Provider == state.MeshProviderFactory), which have no box key in
	// central. nil keeps those tenants a no-op; wire it with UseFabric.
	fabric func(customerID string, ep *state.MeshEndpoint) PolicyEnsurer

	// shared is the shared-tailnet (Tailscale SaaS) ensurer used for customers
	// that have no per-customer mesh box — i.e. they ride yscale's default
	// multi-tenant tailnet. nil (the default) preserves the legacy no-op for
	// that path; wire it with UseSharedTailnet.
	shared tailnetEnsurer

	debounce   time.Duration
	backoff    time.Duration
	maxBackoff time.Duration

	ctx context.Context

	mu      sync.Mutex
	workers map[string]*worker
}

// Option customizes a Reconciler (test injection of the ensurer factory and
// timing knobs).
type Option func(*Reconciler)

// WithEnsurerFactory overrides how PolicyEnsurers are built (tests inject a mock).
func WithEnsurerFactory(f EnsurerFactory) Option {
	return func(r *Reconciler) { r.factory = f }
}

// WithDebounce sets the coalescing window for the async worker.
func WithDebounce(d time.Duration) Option { return func(r *Reconciler) { r.debounce = d } }

// WithBackoff sets the FIRST retry delay after a failed push; consecutive
// failures double it up to WithMaxBackoff.
func WithBackoff(d time.Duration) Option { return func(r *Reconciler) { r.backoff = d } }

// WithMaxBackoff caps the doubling retry delay.
func WithMaxBackoff(d time.Duration) Option { return func(r *Reconciler) { r.maxBackoff = d } }

// New constructs a Reconciler. ctx bounds the lifetime of the per-customer
// worker goroutines.
func New(ctx context.Context, store *state.Store, log *slog.Logger, opts ...Option) *Reconciler {
	r := &Reconciler{
		store:      store,
		log:        log,
		factory:    defaultEnsurer,
		debounce:   defaultDebounce,
		backoff:    defaultBackoff,
		maxBackoff: defaultMaxBackoff,
		ctx:        ctx,
		workers:    map[string]*worker{},
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// UseSharedTailnet wires the shared Tailscale SaaS client used to push the
// kubelet-transparency ACL for customers that ride yscale's default shared
// tailnet (no per-customer mesh box). It accepts the concrete *tailscale.Client
// (which satisfies the unexported tailnetEnsurer interface) so the caller —
// the enterprise wiring in cmd/yscale-cloud — does not need to import package
// tailscale to pass dec.TS(). A nil client (or never calling this) keeps the
// legacy no-op for the shared-tailnet path, preserving OSS behavior when the
// shared client is unconfigured.
// UseFabric routes policy for factory-provisioned tenant boxes through the
// factory. Call it during startup wiring, before work is enqueued.
func (r *Reconciler) UseFabric(f func(customerID string, ep *state.MeshEndpoint) PolicyEnsurer) {
	r.fabric = f
}

func (r *Reconciler) UseSharedTailnet(c *tailscale.Client) {
	if c == nil {
		return
	}
	r.shared = c
}

// PolicyUser normalizes a Headscale user into the policy owner form ("user@").
func PolicyUser(user string) string {
	user = strings.TrimSpace(user)
	if strings.HasSuffix(user, "@") {
		return user
	}
	return user + "@"
}

// BuildPolicy assembles the tagOwners, route approvers, and kubelet-transparency
// ACLs for a customer whose gateway advertises the (validated, masked) desired
// set. The burst-pool approver (decider.BurstPodCIDRPool -> decider.BurstTailnetTag)
// is central-authored and unconditional; the gateway approvers are exactly the
// desired set under tag:yscale-gateway, with NO env fallback.
//
// The kubelet ACL lets traffic the gateway forwards from the customer's desired
// cluster route CIDRs reach THIS customer's burst/agent tag on the kubelet port
// ONLY (:10250) — the seam that makes `kubectl logs`/`exec` work transparently
// for stock-kubelet bursts. It never targets other ports, the bootstrap port,
// the gateway tag, or any other destination, and is omitted entirely when the
// desired set is empty so an empty Src can't create an invalid ACL.
func BuildPolicy(user string, desired []string) (tagOwners, routeApprovers map[string][]string, kubeletACLs []client.PolicyACL) {
	owner := PolicyUser(user)
	tagOwners = map[string][]string{
		handlers.AgentTailnetTag:   {owner},
		handlers.GatewayTailnetTag: {owner},
	}
	routeApprovers = map[string][]string{}
	for _, route := range desired {
		addRouteApprover(routeApprovers, route, handlers.GatewayTailnetTag)
	}
	addRouteApprover(routeApprovers, decider.BurstPodCIDRPool, decider.BurstTailnetTag)

	if len(desired) > 0 {
		kubeletACLs = []client.PolicyACL{{
			Action: "accept",
			Proto:  "tcp",
			Src:    slices.Clone(desired),
			Dst:    []string{kubeletACLDst},
		}}
	}
	return tagOwners, routeApprovers, kubeletACLs
}

// kubeletPort is the authenticated kubelet API port the apiserver dials for
// logs/exec/attach. Those operations expose a code-execution surface, so the
// transparency ACL pins the burst/agent destination to exactly this port.
const kubeletPort = "10250"

// kubeletACLDst is the only destination the kubelet-transparency rule ever
// names. On the shared tailnet it doubles as the IDENTITY of central's own rule:
// the exact-replace write finds what it manages by this destination, so the two
// have to be the same constant or a replace would leave the rule it meant to
// supersede standing next to the new one.
const kubeletACLDst = handlers.AgentTailnetTag + ":" + kubeletPort

func addRouteApprover(routeApprovers map[string][]string, route, tag string) {
	for _, existing := range routeApprovers[route] {
		if existing == tag {
			return
		}
	}
	routeApprovers[route] = append(routeApprovers[route], tag)
}

// Reconcile pushes the customer's desired gateway-route policy to its mesh
// destination. The destination is selected per customer:
//
//   - A customer with a per-customer mesh box (Headscale) gets the FULL policy
//     (tagOwners + routeApprovers + kubelet ACLs) pushed to their own box — the
//     unchanged legacy path. A box owns a whole policy namespace, so a full
//     replace is safe. Each NAMED gateway node is then converged to its own
//     cluster's exact route set (approveGatewayRoutes), because the policy's
//     autoApprovers alone leave an already-registered gateway's routes pending.
//   - A customer with NO mesh box rides yscale's DEFAULT shared Tailscale SaaS
//     tailnet. For them the kubelet-transparency ACL (the :10250 rule that makes
//     `kubectl logs`/`exec`/`port-forward` reach their burst pods) lives in ONE
//     document shared with every other box-less tenant, so this reconcile does
//     not write it: it hands the work to the shared worker
//     (reconcileSharedTailnet), which recomputes the cross-tenant union once and
//     writes once. Only the narrow kubelet ACL is central's there
//     (tagOwners/routeApprovers are managed out-of-band globally). When no
//     shared client is configured (OSS build / TS_OAUTH_* unset) this path stays
//     a no-op, preserving prior behavior.
//
// clusterIDs names the gateways this request must converge — the concrete
// cluster identities that connected or reported, carried down from the handler.
// It is never inferred: a tenant's cluster registry says which clusters exist,
// not which one this event came from, so deriving a gateway from it approves one
// cluster's CIDRs on another cluster's node after a rename or a second
// registration. An EMPTY clusterIDs is the deliberate policy-only mode
// (ReconcilePolicyOnly) — push the policy, touch no node.
//
// The two halves have DIFFERENT desired states and are gated separately:
//
//   - Policy desired state is the tenant-wide union (Customer.GatewayRoutes),
//     gated on (policy work was asked for) && (its own force || union !=
//     PushedGatewayRoutes). A policy that lands is converged even if a gateway
//     node is missing, so PushedGatewayRoutes advances on its own — otherwise
//     every retry of that one gateway would re-PUT an identical policy document.
//   - Each named gateway's desired state is THAT cluster's exact set
//     (GatewayRoutesByCluster[id]), gated on (its own force || set !=
//     PushedGatewayRoutesByCluster[id]) and advanced only for the cluster whose
//     node the box actually accepted. A cluster with no stored set has reported
//     nothing since the upgrade; it is skipped, never filled in from the union.
//
// This exported entry point asks for BOTH halves, because every synchronous
// caller (boot, tests) wants both. The worker builds its own request, which is
// how a retry of one wedged gateway avoids dragging the policy along with it.
//
// Safe to call synchronously (boot) or from the worker goroutine; it must NOT
// be called inline on the WS read goroutine.
func (r *Reconciler) Reconcile(ctx context.Context, customerID string, clusterIDs []string, force bool) error {
	req := request{policy: true, policyForce: force}
	for _, id := range clusterIDs {
		if id == "" {
			continue
		}
		req.clusters = append(req.clusters, clusterRequest{id: id, force: force})
	}
	return r.reconcile(ctx, customerID, req).err
}

// reconcileOutcome is Reconcile's result seen from the worker: the error the
// synchronous callers get, plus which halves are worth ATTEMPTING AGAIN. The
// two are not the same question. A gateway whose agent has disconnected is a
// real error — its routes are unapproved — but retrying it converges nothing
// until that connector comes back and forces a reconcile of its own.
type reconcileOutcome struct {
	err            error
	policyFailed   bool
	failedClusters []string
}

// gatewayWork is one cluster's node-level job: converge THIS gateway to THIS
// cluster's exact route set. The routes travel with the cluster id so no later
// step can pair a set with the wrong node.
//
// withdraw marks the one job whose desired set is EMPTY on purpose: the cluster
// has been deleted from the registry, so its chart-managed gateway must have the
// approvals central gave it replaced with nothing. It is a distinct flag rather
// than an empty routes slice because the two mean opposite things everywhere
// else in this package — an empty set is normally "this cluster has reported
// nothing", which is a skip, never a withdrawal.
type gatewayWork struct {
	clusterID string
	routes    []string
	withdraw  bool
}

// errGatewayMissing marks the one node failure a retry cannot heal by itself:
// the gateway pod has not registered on the customer's box. Whether retrying is
// worth anything depends on the agent, which is why it is distinguishable.
var errGatewayMissing = errors.New("gateway node is not registered on the customer's box yet")

func (r *Reconciler) reconcile(ctx context.Context, customerID string, req request) reconcileOutcome {
	if customerID == sharedTailnetKey {
		return r.reconcileSharedTailnet(ctx, req)
	}
	cust, err := r.store.CustomerByID(customerID)
	if err != nil || cust == nil {
		return reconcileOutcome{} // unknown customer → nothing to reconcile
	}

	// Select the policy destination: the customer's own mesh box, the shared
	// tailnet client, or neither (no-op).
	var boxEnsurer PolicyEnsurer
	if cust.HasMeshBox() && cust.Mesh.Provider == state.MeshProviderFactory {
		if r.fabric == nil {
			return reconcileOutcome{} // no factory wired → no-op; routes stay stored
		}
		boxEnsurer = r.fabric(customerID, cust.Mesh)
	} else if cust.HasMeshBox() {
		boxEnsurer = r.factory(cust.Mesh)
	} else if r.shared == nil {
		return reconcileOutcome{} // shared tailnet client not configured → no-op; routes stay stored
	}

	intent, err := r.store.CustomerGatewayRouteIntent(customerID)
	if err != nil {
		// ErrNotFound is the tenant going away between the two reads above —
		// a deleted tenant has nothing to converge, so it is a clean skip.
		// Anything else is a store failure, and swallowing it would report a
		// convergence that never happened: retry whatever this attempt was for.
		if errors.Is(err, state.ErrNotFound) {
			return reconcileOutcome{}
		}
		r.log.Warn("reading gateway route intent failed; requeueing the work this attempt carried",
			"customer", customerID, "error", err)
		return reconcileOutcome{err: err, policyFailed: req.policy, failedClusters: requestClusterIDs(req)}
	}
	desired, _ := routecidr.CanonicalizeCIDRs(intent.Union)
	pushed, _ := routecidr.CanonicalizeCIDRs(intent.PushedUnion)
	// The policy half runs only when it was ASKED for. A request carrying
	// nothing but a wedged gateway's retry has no policy evidence in it, and
	// running the policy anyway — with a force that belonged to some other
	// item — is how one permanently-missing node re-PUTs the current policy and
	// rewrites the same customer row for the life of the process.
	policyStale := req.policy && (req.policyForce || !slices.Equal(desired, pushed))

	// The node half is Headscale-only. A shared-tailnet tenant has no
	// per-customer gateway node, so its cluster list buys nothing once the
	// union policy is current — and it must never reach a Headscale method.
	//
	// It does still owe the BOOKKEEPING half of a withdrawal. A delete leaves a
	// pushed-by-cluster tombstone for the reconcile to withdraw an approval
	// from; on this path there is no node that ever held one, so nothing has to
	// be taken back — but the entry has no reader left either, and keeping it
	// grows the map by one dead entry per cluster the tenant ever deletes.
	// Dropping it is work in its own right, which is why it is named here and
	// not folded into the policy gate: a tenant whose union is already converged
	// still owes it.
	var gateways []gatewayWork
	var tombstones []string
	if boxEnsurer != nil {
		gateways = pendingGatewayWork(intent, req.clusters)
	} else {
		tombstones = withdrawnTombstones(intent, req.clusters)
	}
	if !policyStale && len(gateways) == 0 && len(tombstones) == 0 {
		return reconcileOutcome{} // gate: nothing changed since the last successful push
	}

	if boxEnsurer == nil {
		// Shared-tailnet path. The document this tenant's routes land in is
		// GLOBAL — one policy for every box-less tenant — so the write itself is
		// not this customer's work to do: it is handed to the one shared worker,
		// which recomputes the cross-tenant union from a fresh snapshot and
		// writes once for however many tenants asked. A tenant's own reconcile
		// would otherwise rewrite an identical ACL once per connector.
		converged := r.sharedTailnetConverged()
		if policyStale || !converged {
			// Asked for on the tenant's own evidence OR on the document's: a
			// tenant whose union is already current can still be the only one
			// left holding work the shared document owes, and a hold that did
			// not ask would be a wait with nothing on the other end.
			r.EnqueueSharedTailnet(req.policyForce)
		}
		// The tombstones are this tenant's own bookkeeping, but they may not be
		// dropped until the shared document has actually caught up: the entry is
		// the durable record that a cluster of this tenant once had routes in
		// play, and discarding it while the ACL still grants them would report a
		// convergence that has not happened. Not-yet-converged is a requeue, not
		// a failure — the shared write is in flight, not broken.
		if len(tombstones) > 0 && !converged {
			r.log.Info("deleted cluster's pushed gateway routes held until the shared tailnet ACL catches up",
				"customer", customerID, "clusters", tombstones)
			return reconcileOutcome{failedClusters: tombstones}
		}
		return r.dropTombstones(customerID, tombstones, reconcileOutcome{})
	}

	// Mesh-box path. BuildPolicy's user seeds tagOwners with the box's own user
	// as the tag owner, which is why it is read here and not above: the shared
	// path has no box and discards tagOwners entirely.
	tagOwners, routeApprovers, kubeletACLs := BuildPolicy(cust.Mesh.User, desired)

	var out reconcileOutcome
	var errs []error
	if policyStale {
		// Mesh-box path (unchanged): full policy replace into the customer's box.
		if err := boxEnsurer.EnsurePolicy(ctx, tagOwners, routeApprovers, kubeletACLs); err != nil {
			r.log.Warn("headscale policy push failed; will retry, PushedGatewayRoutes not advanced",
				"customer", customerID, "desired_routes", desired, "error", err)
			// The policy IS the route-approver document a node approval is
			// approved against, so there is nothing useful to do to a gateway
			// until it lands. Every named cluster retries with it.
			return reconcileOutcome{err: err, policyFailed: true, failedClusters: workClusterIDs(gateways)}
		}
		r.log.Info("headscale policy pushed",
			"customer", customerID,
			"gateway_routes", desired,
			"burst_route", decider.BurstPodCIDRPool,
			"forced", req.policyForce)
		if rec := r.recordPushedPolicy(customerID, desired); rec.err != nil {
			errs = append(errs, rec.err)
			out.policyFailed = true
		}
	}

	// A policy autoApprover only takes routes out of PENDING at the moment a
	// node advertises them while the policy already names them, so the gateway
	// that registered first — or whose advertised set changed — stays pending
	// until the node itself is approved. Doing that here shares the worker's
	// serialization and bounded retry. Every cluster is attempted before the
	// joined error returns, so one wedged gateway cannot stop a healthy sibling.
	for _, g := range gateways {
		if err := r.approveGatewayRoutes(ctx, boxEnsurer, customerID, g); err != nil {
			r.log.Warn("gateway route approval failed; this cluster's pushed state not advanced",
				"customer", customerID, "cluster", g.clusterID, "desired_routes", g.routes,
				"withdraw", g.withdraw, "error", err)
			errs = append(errs, err)
			if errors.Is(err, errGatewayMissing) && !r.agentConnected(customerID, g.clusterID) {
				// The connector is gone, so nothing will register that gateway
				// until it comes back — and its connect forces a reconcile
				// carrying this same cluster id. Retrying in the meantime is
				// load on the tenant's box that cannot converge. Store evidence,
				// not a guessed timeout: a still-connected agent whose gateway
				// pod is merely slow keeps its retry ladder.
				r.log.Info("gateway missing and its agent has disconnected; leaving it for the next connect",
					"customer", customerID, "cluster", g.clusterID)
				continue
			}
			out.failedClusters = append(out.failedClusters, g.clusterID)
			continue
		}
		if err := r.recordGatewayConverged(customerID, g); err != nil {
			r.log.Warn("recording pushed gateway routes for cluster failed",
				"customer", customerID, "cluster", g.clusterID, "withdraw", g.withdraw, "error", err)
			errs = append(errs, err)
			out.failedClusters = append(out.failedClusters, g.clusterID)
			continue
		}
		if moved, policyMoved := r.clusterIntentMoved(customerID, g); moved {
			// The node call runs OUTSIDE any store lock — it is a provider round
			// trip — so this cluster's desired set can move while it is in
			// flight. The sharp case is a WITHDRAWAL racing the same id being
			// re-registered and reporting again: the empty replacement lands
			// after the new intent exists, and with nothing else queued it is the
			// last word on a gateway that is live. Re-reading the snapshot is the
			// evidence; a forced enqueue is what makes the newer state win.
			//
			// Only the CLUSTER half is requeued unless the tenant's policy really
			// did move with it. Whatever changed the intent enqueued its own
			// policy work, so an unconditional re-ask here is a second full
			// document PUT for a policy the store already calls current.
			r.log.Info("gateway route intent moved during the node call; requeueing the cluster",
				"customer", customerID, "cluster", g.clusterID,
				"withdraw", g.withdraw, "policy_moved", policyMoved)
			if policyMoved {
				r.Enqueue(customerID, g.clusterID, true)
			} else {
				r.enqueueCluster(customerID, g.clusterID, true)
			}
		}
	}
	out.err = errors.Join(errs...)
	return out
}

// recordGatewayConverged advances one gateway's convergence state after the box
// accepted its replacement set. A WITHDRAWAL clears the entry instead of
// writing an empty one: the cluster is gone, so the tombstone that told this
// reconcile there was an approval to take back has no reader left, and keeping
// it would grow the map by one dead entry per cluster the tenant ever deletes.
func (r *Reconciler) recordGatewayConverged(customerID string, g gatewayWork) error {
	if g.withdraw {
		return r.store.ClearCustomerPushedClusterGatewayRoutes(customerID, g.clusterID)
	}
	return r.store.SetCustomerPushedClusterGatewayRoutes(customerID, g.clusterID, g.routes)
}

// clusterIntentMoved re-reads ONE cluster's desired set after its node mutation
// landed and reports whether the store still says what the attempt acted on, and
// separately whether the TENANT's policy is stale as of that same read. It is
// deliberately a fresh SNAPSHOT read rather than a comparison against the intent
// this reconcile started from: the whole question is what happened during the
// provider call.
//
// A read failure counts as moved, in both halves. An extra reconcile is
// idempotent — the gate drops it the moment the two halves agree — while a
// missed one leaves a stale approval, or an empty one, as a gateway's final
// state.
func (r *Reconciler) clusterIntentMoved(customerID string, g gatewayWork) (moved, policyMoved bool) {
	intent, err := r.store.CustomerGatewayRouteIntent(customerID)
	if err != nil {
		// A tenant that went away owes nothing; there is no gateway left to
		// converge and no policy to hold it in.
		if errors.Is(err, state.ErrNotFound) {
			return false, false
		}
		return true, true
	}
	union, _ := routecidr.CanonicalizeCIDRs(intent.Union)
	pushedUnion, _ := routecidr.CanonicalizeCIDRs(intent.PushedUnion)
	policyMoved = !slices.Equal(union, pushedUnion)
	routes, live := intent.RoutesFor(g.clusterID)
	if g.withdraw {
		return live, policyMoved
	}
	desired, _ := routecidr.CanonicalizeCIDRs(routes)
	return !live || !slices.Equal(desired, g.routes), policyMoved
}

// pendingGatewayWork picks, out of the clusters this request named, the ones
// whose gateway node is not already converged to their own exact set. Each item
// is gated on ITS OWN force, so a reconnect on A cannot re-assert B's node just
// because the two happened to drain in the same pass.
//
// A cluster with no stored set is SKIPPED, not defaulted to the union: the
// union is every cluster's routes, and putting it on one gateway is the
// cross-cluster mutation this whole seam exists to prevent. A legacy tenant
// upgraded mid-flight lands here — its union survived the restart but no
// cluster has reported since — and the agent's post-connect report is what
// gives its gateway an exact set to converge to.
//
// The one exception is a request item that says the cluster was DELETED. Its
// desired set is gone for the same reason its registry row is, and the pushed
// entry left behind says central once approved routes on that gateway. That
// pair — and only that pair — is a withdrawal. A cluster that has merely never
// reported has no pushed entry, so it still reads as a skip, and an agent's
// empty report never gets this far (the store ignores it) so a transient CIDR
// lookup failure can never masquerade as a delete.
func pendingGatewayWork(intent state.GatewayRouteIntent, clusters []clusterRequest) []gatewayWork {
	var out []gatewayWork
	for _, c := range clusters {
		if c.id == "" {
			continue
		}
		routes, ok := intent.RoutesFor(c.id)
		if !ok {
			if c.withdraw && len(intent.PushedRoutesFor(c.id)) > 0 {
				out = append(out, gatewayWork{clusterID: c.id, withdraw: true})
			}
			continue
		}
		desired, _ := routecidr.CanonicalizeCIDRs(routes)
		if len(desired) == 0 {
			continue
		}
		if !c.force {
			pushed, _ := routecidr.CanonicalizeCIDRs(intent.PushedRoutesFor(c.id))
			if slices.Equal(desired, pushed) {
				continue
			}
		}
		out = append(out, gatewayWork{clusterID: c.id, routes: desired})
	}
	return out
}

// withdrawnTombstones names the deleted clusters whose pushed entry a
// SHARED-tailnet tenant is still carrying. It reads the same decision
// pendingGatewayWork makes — so a live cluster, or one that never reported, can
// no more be torn down here than it can on a box — and keeps only the
// withdrawals, because the converge half of that answer needs a gateway node
// this path does not have.
func withdrawnTombstones(intent state.GatewayRouteIntent, clusters []clusterRequest) []string {
	var out []string
	for _, g := range pendingGatewayWork(intent, clusters) {
		if g.withdraw {
			out = append(out, g.clusterID)
		}
	}
	return out
}

// dropTombstones removes the pushed-by-cluster entries a shared-tailnet
// tenant's deletes left behind, folding any refusal into out as that cluster's
// failure so the worker retries it on its own ladder.
func (r *Reconciler) dropTombstones(customerID string, clusterIDs []string, out reconcileOutcome) reconcileOutcome {
	errs := []error{out.err}
	for _, id := range clusterIDs {
		if err := r.store.ClearCustomerPushedClusterGatewayRoutes(customerID, id); err != nil {
			r.log.Warn("clearing a deleted cluster's pushed gateway routes failed",
				"customer", customerID, "cluster", id, "error", err)
			errs = append(errs, err)
			out.failedClusters = append(out.failedClusters, id)
			continue
		}
		r.log.Info("deleted cluster's pushed gateway routes dropped; the shared tailnet has no node to withdraw from",
			"customer", customerID, "cluster", id)
	}
	out.err = errors.Join(errs...)
	return out
}

// sharedTailnetKey is the worker key the ONE shared-tailnet reconcile runs
// under. The workers map is otherwise keyed by customer id, and this is not a
// customer: the shared tailnet holds a single ACL document for every box-less
// tenant, so its desired state is cross-tenant and its writer has to be one
// serialized, coalescing goroutine rather than one per tenant. The NUL byte
// keeps it out of the customer id space (ids come from newID and from operator
// config; neither can produce one), so a tenant cannot collide with it.
const sharedTailnetKey = "\x00shared-tailnet"

// EnqueueSharedTailnet asks the shared-tailnet worker to converge the ONE
// kubelet ACL rule central manages on the shared tailnet. Every box-less
// tenant's change funnels here, which is what makes N connectors reporting
// inside one debounce window a single ACL write instead of N identical ones.
//
// force re-asserts the rule even when the recorded union already matches the
// desired one — boot, where the document may have drifted. It is not a licence
// to rewrite: an assert that would produce a byte-identical rule stops at the
// GET (see tailscale.ReplaceManagedKubeletACL).
func (r *Reconciler) EnqueueSharedTailnet(force bool) {
	if r == nil || r.shared == nil {
		return
	}
	r.enqueue(sharedTailnetKey, "", force, false)
}

// reconcileSharedTailnet converges the shared tailnet's ONE managed kubelet
// rule. Its desired Src is the CROSS-TENANT union, deliberately not any single
// customer's routes: the tailnet's policy is shared, so a rule written from one
// tenant's view revokes every other tenant riding it. Recomputing the union from
// a fresh store snapshot per attempt is also what makes a concurrent change
// converge — a 412 retry re-reads whatever the other writer landed.
//
// An EMPTY union still issues the write. That is the case the additive merge
// could not express at all: the last shared tenant's routes going away — the
// tenant revoked, deleted, or moved onto its own box — has to REMOVE the rule,
// not leave it granting :10250 to CIDRs nobody advertises.
//
// The write is TWO-PHASE, and the order is the whole point. The union about to
// be written is CLAIMED durably first, so a POST that lands and a receipt that
// does not still leaves the rule it installed provably central's; the pushed
// union is recorded — and the claims collapsed — ONLY after the API call
// succeeds. A refused claim means no write at all: an unclaimed POST is exactly
// the orphan the claim exists to prevent. A refused record is a failed attempt
// whose claim survives, so the retry can still take back what it wrote.
//
// A FULL proof set is the one refusal that does not clear itself, and
// cleanSharedTailnetProof is what unwedges it before the claim is retried.
func (r *Reconciler) reconcileSharedTailnet(ctx context.Context, req request) reconcileOutcome {
	if r.shared == nil || !req.policy {
		return reconcileOutcome{}
	}
	intent := r.store.SharedTailnetRouteIntent()
	if !req.policyForce && len(intent.Claims) == 0 && slices.Equal(intent.Desired, intent.Pushed) {
		// Gate: the tailnet already carries this union and central holds no
		// in-flight claim that could still be sitting on the document. The
		// per-tenant halves still advance — a tenant whose CIDRs another tenant
		// already put on the shared rule converged the moment THAT write landed,
		// and there is no further write to carry its bookkeeping.
		r.store.AdvanceSharedTailnetTenants(intent)
		return reconcileOutcome{}
	}
	claimed, err := r.store.ClaimSharedTailnetPush(intent)
	if errors.Is(err, state.ErrSharedTailnetClaimsFull) {
		// Forward recovery, never an eviction: the cleanup spends the proof set on
		// a write it is already entitled to make, and only a set that collapsed
		// has room for this union. A failure there keeps every proof and retries.
		if out := r.cleanSharedTailnetProof(ctx, claimed); out.err != nil {
			return out
		}
		// Re-read rather than reuse: the collapse moved the claims, and the desired
		// union is recomputed per attempt on this path anyway.
		intent = r.store.SharedTailnetRouteIntent()
		claimed, err = r.store.ClaimSharedTailnetPush(intent)
	}
	if err != nil {
		r.log.Warn("claiming the shared-tailnet union failed; NOT writing the ACL, an unclaimed write cannot be taken back",
			"desired_union", intent.Desired, "pushed_union", intent.Pushed, "error", err)
		return reconcileOutcome{err: err, policyFailed: true}
	}
	if err := r.shared.ReplaceManagedKubeletACL(ctx, kubeletACLDst, claimed.Proof(), claimed.Desired); err != nil {
		r.log.Warn("shared-tailnet kubelet ACL replace failed; will retry, the pushed union is not advanced",
			"desired_union", claimed.Desired, "pushed_union", claimed.Pushed,
			"claims", len(claimed.Claims), "error", err)
		return reconcileOutcome{err: err, policyFailed: true}
	}
	if err := r.store.RecordSharedTailnetPushed(claimed); err != nil {
		r.log.Warn("recording the pushed shared-tailnet union failed; the ACL landed, the claim covers it, the record retries",
			"desired_union", claimed.Desired, "error", err)
		return reconcileOutcome{err: err, policyFailed: true}
	}
	r.log.Info("shared-tailnet kubelet ACL converged",
		"desired_union", claimed.Desired,
		"previous_union", claimed.Pushed,
		"collapsed_claims", len(claimed.Claims),
		"tenants", len(claimed.ByCustomer),
		"forced", req.policyForce)
	return reconcileOutcome{}
}

// cleanSharedTailnetProof unwedges a shared tailnet whose bounded proof set is
// full, by collapsing the central-owned copies on the document down to ONE that
// is already there and then recording exactly that union, which retires the
// claims.
//
// The cap is fail-closed for a reason that does not go away: a union is in the
// set because its POST may have landed, so evicting one leaves a rule central may
// have written that no later replace can match. But the set is only retired by a
// write that COMPLETES, and a tailnet whose ACL API is refusing writes while its
// tenants keep changing routes arrives at the cap with no room to claim the next
// distinct union — and then refuses every write after it, for good. Recovery has
// to come from a write central is already entitled to make.
//
// This is that write, and it needs no new claim. Every rule it may REMOVE carries
// a union the proof set already names, and the union it INSTALLS was ON the
// document in the version it replaced — so a POST that lands and a receipt that
// does not still leaves a rule that is provably central's, which is the whole
// property the claim exists to buy. Nothing new is owned, and nothing new is
// GRANTED: the selection happens inside CollapseManagedKubeletACL, against the
// document, under the Etag the replace uses. Choosing from the durable record
// here instead would install the newest claim — a union whose POST may never have
// been accepted — and re-grant :10250 to CIDRs the tailnet has never carried.
//
// Ordering is the same two-phase discipline read the other way round. The write
// goes first and the RECORD of it only after it returns, because the claims are
// what cover the rule that write may have left; a crash between them finds the
// same full set on the next boot and repeats this, which is idempotent — the
// second pass selects the same live union and stops at the GET, because the
// document already says it.
//
// RECOVERY IS TWO WRITES, and the window between them is unavoidable. The desired
// union cannot go out first: it is the union that could not be claimed, and an
// unclaimed POST is the orphan the whole scheme exists to prevent. So the
// document carries the collapse target from the moment that write returns until
// the desired one lands — an attempt that may be a retry ladder away if the ACL
// API is still flaky, or a whole restart away if central dies in between. What
// bounds that window is that the target is a grant the document was ALREADY
// making: the shared fleet keeps the kubelet access it had, and the collapse can
// only ever take grants away. An empty target means the document held no rule
// central could prove was its own, and there removing them is all it can honestly
// do.
//
// No per-tenant bookkeeping rides along. The document is back at a union central
// already had on it, not the one the tenants are asking for, so nothing of theirs
// has converged and no tombstone may be dropped against this.
func (r *Reconciler) cleanSharedTailnetProof(ctx context.Context, full state.SharedTailnetIntent) reconcileOutcome {
	landed, err := r.shared.CollapseManagedKubeletACL(ctx, kubeletACLDst, full.Pushed, full.Claims)
	if err != nil {
		r.log.Warn("shared-tailnet collapse failed at the proof cap; every claim is kept and the attempt retries",
			"pushed_union", full.Pushed, "claims", len(full.Claims), "error", err)
		return reconcileOutcome{err: err, policyFailed: true}
	}
	if err := r.store.RecordSharedTailnetPushed(state.SharedTailnetIntent{Desired: landed}); err != nil {
		r.log.Warn("collapsing the shared-tailnet proof set failed; the cleanup landed, the claims still cover it, the record retries",
			"cleanup_union", landed, "pushed_union", full.Pushed, "claims", len(full.Claims), "error", err)
		return reconcileOutcome{err: err, policyFailed: true}
	}
	r.log.Info("shared-tailnet proof set was full; the document was collapsed onto a union it already carried and the claims retired",
		"cleanup_union", landed, "pushed_union", full.Pushed, "collapsed_claims", len(full.Claims))
	return reconcileOutcome{}
}

// sharedTailnetConverged reports whether the shared document already carries
// every CIDR the shared tenants want on it, and nothing they do not.
//
// An outstanding CLAIM is not converged. It says a write may have landed whose
// receipt did not, so the document may still carry a rule granting a union no
// tenant asked for — and a tombstone dropped against that is a convergence
// nobody proved. The claim is cleared by the write that proves otherwise.
func (r *Reconciler) sharedTailnetConverged() bool {
	if r.shared == nil {
		// Nothing writes that document in this build, so there is no write to
		// wait behind and the bookkeeping is owed nothing.
		return true
	}
	intent := r.store.SharedTailnetRouteIntent()
	return len(intent.Claims) == 0 && slices.Equal(intent.Desired, intent.Pushed)
}

// ReplaySharedTailnet is what the SHARED tailnet owes at startup, and the
// counterpart of ReconcileAttach for tenants that have no box to attach.
//
// Two things a restart would otherwise swallow live here. The shared ACL itself
// is one: its desired union is recomputed from live tenants while the union it
// last carried is durable, so a tenant revoked, deleted, or reaped while central
// was down shows up as a difference nothing else would ever ask about — the
// tenant is gone, and gone tenants do not reconnect. The other is the per-tenant
// withdrawal bookkeeping a cluster delete or a boot reap left behind, which is
// replayed through the same ReconcileAttach every box-backed tenant uses.
//
// Everything here is enqueue-shaped apart from that replay: the ACL write runs
// on the shared worker with the same bounded ladder as any other push, so a
// tailnet API that is down at boot delays convergence instead of failing start.
func (r *Reconciler) ReplaySharedTailnet(ctx context.Context) error {
	if r == nil || r.shared == nil {
		return nil
	}
	var errs []error
	for _, customerID := range r.store.SharedTailnetCustomerIDs() {
		if err := r.ReconcileAttach(ctx, customerID); err != nil {
			errs = append(errs, fmt.Errorf("shared tenant %s: %w", customerID, err))
		}
	}
	// Last, and forced: the per-tenant replays above may each have enqueued it,
	// but a tailnet whose only change was a tenant that no longer exists has no
	// tenant left to ask on its behalf.
	r.EnqueueSharedTailnet(true)
	return errors.Join(errs...)
}

func workClusterIDs(work []gatewayWork) []string {
	out := make([]string, 0, len(work))
	for _, g := range work {
		out = append(out, g.clusterID)
	}
	return out
}

// requestClusterIDs names every gateway an attempt was carrying, for the
// failure paths that could not get far enough to tell which of them had work.
func requestClusterIDs(req request) []string {
	out := make([]string, 0, len(req.clusters))
	for _, c := range req.clusters {
		out = append(out, c.id)
	}
	return out
}

// recordPushedPolicy advances the tenant-wide pushed union after a policy push
// landed. A refused durable write is a failure of the policy half only.
func (r *Reconciler) recordPushedPolicy(customerID string, desired []string) reconcileOutcome {
	if err := r.store.SetCustomerPushedGatewayRoutes(customerID, desired); err != nil {
		r.log.Warn("recording pushed gateway routes failed", "customer", customerID, "error", err)
		return reconcileOutcome{err: err, policyFailed: true}
	}
	return reconcileOutcome{}
}

// agentConnected reports whether this replica still holds a socket for one of a
// customer's clusters — the only current evidence that a missing gateway has
// anyone left to bring it up.
func (r *Reconciler) agentConnected(customerID, clusterID string) bool {
	_, err := r.store.AgentForCluster(customerID, clusterID)
	return err == nil
}

// ReconcilePolicyOnly converges the customer's POLICY and nothing else. It is
// the boot path (attachHeadscaleFromEnv): the box has just been attached, no
// agent has connected yet, so no cluster identity is in hand and the tenant's
// gateway pod does not exist. Passing no cluster is what keeps that absence
// from reading as an attach failure. The node half is done by the agent's
// connect-time force reconcile, which carries the cluster id that connected.
func (r *Reconciler) ReconcilePolicyOnly(ctx context.Context, customerID string) error {
	return r.Reconcile(ctx, customerID, nil, true)
}

// ReconcileAttach is what a tenant owes at attach/startup: its POLICY
// re-asserted, and then every withdrawal a cluster delete left durable REPLAYED
// against the exact gateway it was owed on.
//
// It is not box-only. A SHARED-tailnet tenant reaches the same durable
// tombstones — a boot reap of a live-only connector's route intent writes them
// on either path — and the shared half of the replay is the enqueue its policy
// re-assert puts on the shared worker. ReplaySharedTailnet is what drives this
// for tenants that have no attach event of their own.
//
// The replay is the half a restart would otherwise swallow. A delete drops
// GatewayRoutesByCluster[cluster] and keeps PushedGatewayRoutesByCluster
// [cluster] (see clearClusterGatewayRoutesLocked), so that absent-desired /
// present-pushed pair is a durable tombstone saying central approved routes on
// a gateway it has since agreed is gone. The delete enqueues the withdrawal,
// but the worker is in-process: a central that dies before it drains loses it,
// and NOTHING else brings it back. The connector is deleted, so no reconnect
// ever names that cluster again, and the gateway pod is chart-managed — it can
// outlive the registry row still holding the approvals central gave it.
//
// The work comes from the route-intent SNAPSHOT and nowhere else. The tenant's
// registry is precisely what a delete removed the cluster from, so deriving the
// gateway from it finds nothing, and deriving it from the tenant's OTHER
// clusters would withdraw a live gateway's routes. Every replay names the exact
// cluster id its own tombstone was written under.
//
// It is idempotent by construction: the tombstone IS the queue, and each
// withdrawal clears its own once the empty replacement lands or the node is
// proven absent, so a second boot finds nothing to do. A tombstone whose
// withdrawal fails is left standing and handed to the worker's bounded ladder,
// so an API blip retries instead of being lost a second time.
func (r *Reconciler) ReconcileAttach(ctx context.Context, customerID string) error {
	orphans, err := r.pendingWithdrawals(customerID)
	if err != nil {
		// Which gateways owe a withdrawal is unknown, and guessing is the one
		// thing this path must not do. Re-assert the policy — separate work,
		// separately gated — and surface the read failure so the attach does
		// not look clean.
		r.log.Warn("reading gateway route intent for the attach withdrawal replay failed",
			"customer", customerID, "error", err)
		return errors.Join(r.ReconcilePolicyOnly(ctx, customerID), err)
	}
	req := request{policy: true, policyForce: true}
	for _, id := range orphans {
		req.clusters = append(req.clusters, clusterRequest{id: id, withdraw: true})
	}
	// reconcile re-reads the snapshot under its own lock, so a cluster that
	// reported between the scan and here still has a desired set and reads as a
	// skip rather than a teardown — the safe direction, and the reason the
	// withdrawal decision is not made twice.
	out := r.reconcile(ctx, customerID, req)
	for _, id := range out.failedClusters {
		r.EnqueueClusterRemoval(customerID, id)
	}
	if out.policyFailed {
		r.Enqueue(customerID, "", true)
	}
	return out.err
}

// pendingWithdrawals names every cluster whose route intent is the durable
// withdrawal tombstone: no desired set left, and a pushed entry saying central
// once approved routes on that cluster's gateway. Sorted, so a boot replay is
// deterministic. An unknown customer owes nothing; any other read failure is
// the caller's to surface, because a snapshot that could not be read is not
// evidence that there is nothing to withdraw.
func (r *Reconciler) pendingWithdrawals(customerID string) ([]string, error) {
	intent, err := r.store.CustomerGatewayRouteIntent(customerID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, clusterID := range slices.Sorted(maps.Keys(intent.PushedByCluster)) {
		if _, live := intent.RoutesFor(clusterID); live {
			continue
		}
		if len(intent.PushedRoutesFor(clusterID)) == 0 {
			continue
		}
		out = append(out, clusterID)
	}
	return out, nil
}

// approveGatewayRoutes converges ONE cluster's gateway node to exactly that
// cluster's stored, validated, canonical set, and nothing else. The call is a
// replace, not a merge: a shrunk set withdraws the approvals it dropped, a
// repeat is a no-op on the box, and a route the node happens to advertise but
// the customer never reported is never approved. That is sound only because
// yscale-gateway-<cluster> is a chart-managed node dedicated to this one
// function — central owns its whole approved set, so there is no out-of-band
// approval on it to preserve. The unconditional burst pod pool is a POLICY
// approver only (BuildPolicy); it is not part of any cluster's set and is
// deliberately not approved on the gateway.
//
// The set travels inside gatewayWork rather than being read here, so there is
// no point in this call where a tenant-wide union could be substituted for one
// cluster's routes. A shrink on A therefore withdraws on A alone; B's node is
// never opened or touched by A's event.
//
// Resolution is by hostname, never a pinned node id: the gateway registers as
// handlers.GatewayHostname(clusterID) and gets a new id on every re-register.
//
// A gateway that has not registered yet is an ERROR, not a skip — the routes
// really are unapproved. errGatewayMissing lets the caller decide whether a
// retry can still converge it.
//
// For a WITHDRAWAL the same call sends an empty set, which is how the node's
// last approval is taken back, and a missing node is SUCCESS rather than that
// error: a gateway that is not registered holds no approval, which is the exact
// state the withdrawal is trying to reach.
func (r *Reconciler) approveGatewayRoutes(ctx context.Context, box PolicyEnsurer, customerID string, g gatewayWork) error {
	hostname := handlers.GatewayHostname(g.clusterID)
	nodeID, err := box.FindDeviceByHostname(ctx, hostname)
	if err != nil {
		return fmt.Errorf("resolve gateway node %q: %w", hostname, err)
	}
	if nodeID == "" {
		if g.withdraw {
			r.log.Info("deleted cluster's gateway node is already gone; nothing to withdraw",
				"customer", customerID, "cluster", g.clusterID)
			return nil
		}
		return fmt.Errorf("%w: %q", errGatewayMissing, hostname)
	}
	if err := box.ApproveNodeRoutes(ctx, nodeID, g.routes); err != nil {
		return fmt.Errorf("approve routes on gateway node %q: %w", hostname, err)
	}
	if g.withdraw {
		r.log.Info("deleted cluster's gateway node route approval withdrawn",
			"customer", customerID,
			"cluster", g.clusterID,
			"node", nodeID)
		return nil
	}
	r.log.Info("gateway node routes approved",
		"customer", customerID,
		"cluster", g.clusterID,
		"node", nodeID,
		"routes", g.routes)
	return nil
}

// worker is the per-customer serialization point: a single goroutine drains
// reconcile requests so the handler never PUTs inline on the WS read goroutine.
//
// clusters is a SET, not a single id: coalescing is what makes the debounce
// cheap, and a tenant whose A and B gateways report inside one window must
// still converge both. Dropping to "the last cluster wins" would silently leave
// the other one's routes pending.
//
// policy is queued alongside the clusters rather than derived from them,
// because a tenant's policy is work in its own right: boot names no cluster,
// and a report from a cluster whose gateway is already converged can still move
// the union.
//
// force is likewise PER item rather than one tenant-wide bit. A connect-time
// re-assert is evidence about the thing that connected; letting it sit on the
// worker meant a reconnect on A re-forced a policy PUT and B's node approval on
// the next drain, and a partial failure re-armed it for work that was never
// forced at all.
type worker struct {
	wake chan struct{}

	mu          sync.Mutex
	policy      bool
	policyForce bool
	clusters    map[string]clusterItem
	// retry holds the backoff ladder PER queued item — each cluster id, plus
	// policyRetryKey for the tenant-wide half. One ladder for the whole tenant
	// meant a reconnect on a healthy cluster reset a permanently-missing
	// gateway's climb back to the base delay, so the cap never bounded it, and
	// a healthy cluster's retry was paced by the wedged one. An item with no
	// entry here has not failed and is due immediately.
	retry map[string]*retryState
	// gen counts, per item, how many times new work has been enqueued for it.
	// take stamps each drained item with the generation it left, and settle
	// arms a ladder only if that stamp still matches.
	//
	// Without it the two are indistinguishable. An attempt runs OUTSIDE w.mu —
	// it is a provider call — so a reconnect, a delete, or a fresh report can
	// land while it is in flight. That Enqueue deliberately clears the item's
	// ladder, because it is new evidence and the gateway must be tried NOW.
	// Then the older attempt fails and settle re-arms the very ladder that
	// Enqueue cleared, putting the new work behind a backoff that had already
	// climbed to the cap — a gateway that just came up waits five minutes on
	// the strength of an attempt made before it existed.
	gen map[string]uint64
}

// clusterItem is one queued gateway's pending work: the force its queueing
// events carried, and whether one of them was that cluster's lifecycle DELETE.
// Both are sticky until the item is drained, so coalescing inside a debounce
// window cannot lose a re-assert or a withdrawal.
type clusterItem struct {
	force    bool
	withdraw bool
}

// retryState is one item's place in the ladder: how many consecutive failures
// it has had, and the earliest time it should be attempted again.
type retryState struct {
	attempts int
	due      time.Time
}

// policyRetryKey is the retry-ladder key for the tenant-wide policy half. The
// cluster ids it shares the map with are never empty (Enqueue drops the empty
// id, and the agent stream refuses a Hello without one), so it cannot collide.
const policyRetryKey = ""

// request is one drained attempt: the halves that were due, each carrying its
// OWN force and the generation it was drained at. policy false means this
// attempt is not about the tenant's policy at all — the reconcile must neither
// push it nor advance its state.
type request struct {
	policy      bool
	policyForce bool
	policyGen   uint64
	clusters    []clusterRequest
}

// clusterRequest is one gateway inside a drained attempt.
type clusterRequest struct {
	id       string
	force    bool
	withdraw bool
	gen      uint64
}

// Enqueue requests a reconcile of customerID's clusterID gateway, coalescing
// with any in-flight request: every cluster identity is retained, and force is
// sticky ON THE ITEM IT WAS ASKED FOR until that item is drained. It returns
// immediately; the actual push runs on the per-customer worker goroutine. This
// satisfies handlers.PolicyReconciler.
//
// An empty clusterID enqueues policy-only work. The gateway node's name is
// derived from the cluster id, so a caller that cannot name a cluster — boot,
// which runs before any agent has connected — has no node to converge, and
// guessing one from the tenant's other clusters is the cross-cluster mutation
// this seam exists to prevent.
//
// The event resets the ladder of exactly what it is evidence about: the
// tenant's policy, and the cluster it names. A report or a reconnect really is
// a reason to try that gateway again now rather than at the end of a five
// minute backoff — but it says nothing about the tenant's OTHER clusters, whose
// ladders are left alone.
func (r *Reconciler) Enqueue(customerID, clusterID string, force bool) {
	r.enqueue(customerID, clusterID, force, false)
}

// EnqueueClusterRemoval requests the cleanup a cluster's LIFECYCLE DELETE
// leaves behind: the tenant's policy re-derived from the clusters that remain,
// and — on a per-customer box — the deleted cluster's dedicated gateway node
// stripped of the approvals central gave it.
//
// It is a separate entry point rather than a flag on Enqueue because the
// withdrawal must be traceable to an explicit delete and nothing else. Every
// other caller here is an agent event, and an agent that reports no routes is
// an agent whose CIDR lookup has not resolved yet — never a customer asking for
// their data path to be torn down.
//
// No force: a delete moves the stored intent, so the ordinary gates already see
// the work. Satisfies handlers.PolicyReconciler.
func (r *Reconciler) EnqueueClusterRemoval(customerID, clusterID string) {
	r.enqueue(customerID, clusterID, false, true)
}

func (r *Reconciler) enqueue(customerID, clusterID string, force, withdraw bool) {
	r.queue(customerID, clusterID, force, withdraw, true)
}

// enqueueCluster queues ONE gateway and deliberately leaves the tenant's policy
// half alone. It is for the requeue that follows a node call whose intent moved
// underneath it: the change that moved it enqueued its own policy work, and
// re-asking for a policy the store already says is current is a full document
// PUT bought with nothing.
func (r *Reconciler) enqueueCluster(customerID, clusterID string, force bool) {
	if clusterID == "" {
		return
	}
	r.queue(customerID, clusterID, force, false, false)
}

func (r *Reconciler) queue(customerID, clusterID string, force, withdraw, policy bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	w, ok := r.workers[customerID]
	if !ok {
		w = &worker{
			wake:     make(chan struct{}, 1),
			clusters: map[string]clusterItem{},
			retry:    map[string]*retryState{},
			gen:      map[string]uint64{},
		}
		r.workers[customerID] = w
		go r.runWorker(customerID, w)
	}
	r.mu.Unlock()

	w.mu.Lock()
	if policy {
		w.policy = true
		if force {
			w.policyForce = true
		}
		// Bump and clear together: the generation is what stops an attempt that
		// was already in flight from undoing this clear when it settles.
		w.gen[policyRetryKey]++
		delete(w.retry, policyRetryKey)
	}
	if clusterID != "" {
		item := w.clusters[clusterID]
		item.force = item.force || force
		item.withdraw = item.withdraw || withdraw
		w.clusters[clusterID] = item
		w.gen[clusterID]++
		delete(w.retry, clusterID)
	}
	w.mu.Unlock()

	select {
	case w.wake <- struct{}{}:
	default: // a wake is already queued; the worker will see the queued work
	}
}

// retryDelay is how long to wait before retrying an item that has already
// failed attempts times: the base backoff doubled per consecutive failure and
// capped. The cap bounds a permanently-missing gateway — otherwise central
// re-asks that tenant's box every backoff forever — and the reset on success or
// new work keeps one transient failure from delaying the next real change.
func (r *Reconciler) retryDelay(attempts int) time.Duration {
	d := r.backoff
	if d <= 0 {
		d = defaultBackoff
	}
	limit := r.maxBackoff
	if limit < d {
		limit = d
	}
	for i := 0; i < attempts && d < limit; i++ {
		d *= 2
	}
	if d > limit {
		d = limit
	}
	return d
}

// queuedLocked reports whether anything is waiting. Callers hold w.mu.
func (w *worker) queuedLocked() bool {
	return w.policy || len(w.clusters) > 0
}

// dueLocked reports whether a queued item may be attempted now. Callers hold w.mu.
func (w *worker) dueLocked(key string, now time.Time) bool {
	st, ok := w.retry[key]
	return !ok || !now.Before(st.due)
}

// nextWait reports how long until the earliest queued item comes due. runnable
// is false when nothing is queued at all — the signal to block on wake instead
// of arming a timer.
func (w *worker) nextWait(now time.Time) (wait time.Duration, runnable bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.queuedLocked() {
		return 0, false
	}
	keys := slices.Sorted(maps.Keys(w.clusters))
	if w.policy {
		keys = append(keys, policyRetryKey)
	}
	first := true
	for _, key := range keys {
		st, ok := w.retry[key]
		if !ok {
			return 0, true // never failed: due now
		}
		if d := st.due.Sub(now); first || d < wait {
			wait, first = d, false
		}
	}
	return max(wait, 0), true
}

// take drains the queued work that is DUE, leaving items still inside their own
// backoff for a later pass. Returns false when nothing is due yet.
func (w *worker) take(now time.Time) (request, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var req request
	if w.policy && w.dueLocked(policyRetryKey, now) {
		req.policy = true
		req.policyForce = w.policyForce
		req.policyGen = w.gen[policyRetryKey]
		w.policy = false
		w.policyForce = false
	}
	// Each cluster's force and withdraw travel WITH it. An item left behind
	// because it is still inside its own backoff keeps its own flags, so a
	// partial drain neither consumes a re-assert the queue still owes nor
	// hands one to an item that was never forced.
	for _, id := range slices.Sorted(maps.Keys(w.clusters)) {
		if !w.dueLocked(id, now) {
			continue
		}
		item := w.clusters[id]
		req.clusters = append(req.clusters, clusterRequest{
			id: id, force: item.force, withdraw: item.withdraw, gen: w.gen[id],
		})
		delete(w.clusters, id)
	}
	if !req.policy && len(req.clusters) == 0 {
		return request{}, false
	}
	return req, true
}

// settle records one attempt's outcome. Whatever converged is dropped and its
// ladder cleared; whatever failed is requeued and climbs ITS OWN ladder. A
// cluster the outcome deliberately gave up on — a missing gateway whose agent
// has disconnected — is neither, so it simply stops being retried until its
// connector comes back and enqueues it again.
//
// An item that NEW WORK arrived for mid-attempt is requeued but NOT laddered.
// The failure is real and the work still has to happen, so the flags it carried
// go back — but the ladder belongs to the attempt, and a newer Enqueue already
// declared this item due now on evidence the attempt never saw. Stamping it
// with a backoff the attempt earned would make the fresh reconnect wait out a
// climb that had nothing to do with it. w.gen is what tells the two apart: the
// attempt drained at a generation, and only that generation may arm it.
func (r *Reconciler) settle(w *worker, req request, out reconcileOutcome, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// current reports whether this item is still the one the attempt drained —
	// false once any Enqueue has bumped it since.
	current := func(key string, gen uint64) bool { return w.gen[key] == gen }

	failed := make(map[string]struct{}, len(out.failedClusters)+1)
	if out.policyFailed {
		// The force this half carried goes back with it, and with it alone: a
		// connect-time re-assert whose policy PUT failed must still re-assert,
		// while a wedged gateway's retry inherits nothing.
		w.policy = true
		w.policyForce = w.policyForce || req.policyForce
		if current(policyRetryKey, req.policyGen) {
			failed[policyRetryKey] = struct{}{}
		}
	} else if req.policy {
		delete(w.retry, policyRetryKey)
	}
	attempted := make(map[string]clusterRequest, len(req.clusters))
	for _, c := range req.clusters {
		attempted[c.id] = c
	}
	for _, id := range out.failedClusters {
		// OR against whatever is queued now: an Enqueue that landed while this
		// attempt was running owns its own flags too.
		c, drained := attempted[id]
		item := w.clusters[id]
		item.force = item.force || c.force
		item.withdraw = item.withdraw || c.withdraw
		w.clusters[id] = item
		// A failure this attempt cannot place — no drained item to compare a
		// generation against — is laddered rather than skipped: backing off is
		// the safe direction, and an unladdered requeue would spin.
		if !drained || current(id, c.gen) {
			failed[id] = struct{}{}
		}
	}
	for _, c := range req.clusters {
		if _, bad := failed[c.id]; !bad {
			delete(w.retry, c.id)
		}
	}
	for key := range failed {
		st := w.retry[key]
		if st == nil {
			st = &retryState{}
			w.retry[key] = st
		}
		st.due = now.Add(r.retryDelay(st.attempts))
		st.attempts++
	}
	w.pruneGenerationsLocked()
}

// pruneGenerationsLocked drops the generation stamps nothing is relying on any
// more. A stamp only has to outlive an item with work in flight, work queued, or
// a ladder to climb: one with none of those is a dead key that would otherwise
// grow the map by one per cluster id the tenant ever used, for the life of the
// process.
//
// It is only safe from settle, and only because a worker drains, reconciles and
// settles on ONE goroutine: the attempt being settled here is the only attempt
// that can be in flight, so "not queued and not laddered" really is nobody's.
// Even a lost race would be safe in the direction that matters — a re-created
// key starts at 0, which no drained stamp can equal, so settle would read it as
// newer work and decline to arm the ladder. Callers hold w.mu.
func (w *worker) pruneGenerationsLocked() {
	for key := range w.gen {
		if w.retry[key] != nil {
			continue
		}
		if key == policyRetryKey {
			if w.policy {
				continue
			}
		} else if _, queued := w.clusters[key]; queued {
			continue
		}
		delete(w.gen, key)
	}
}

func (r *Reconciler) runWorker(customerID string, w *worker) {
	for {
		wait, runnable := w.nextWait(time.Now())
		switch {
		case !runnable:
			// Nothing queued: block until an Enqueue wakes us, then debounce so
			// a burst of reports and reconnects coalesces into one push.
			select {
			case <-r.ctx.Done():
				return
			case <-w.wake:
			}
			if !r.sleep(r.debounce) {
				return
			}
		case wait > 0:
			// A retry is queued but not due yet. This sleep is INTERRUPTIBLE on
			// purpose: a new report or a reconnect is fresh evidence, and making
			// it wait out a capped five-minute backoff is how a gateway that
			// just came up stays unapproved for five minutes. Enqueue already
			// cleared that item's ladder, so after the debounce it reads as due.
			timer := time.NewTimer(wait)
			select {
			case <-r.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			case <-w.wake:
				timer.Stop()
				if !r.sleep(r.debounce) {
					return
				}
			}
		default:
			// Enqueued while the previous attempt was running. Drain the wake it
			// left so the idle branch cannot spin on it, and debounce it like
			// any other burst.
			select {
			case <-w.wake:
			default:
			}
			if !r.sleep(r.debounce) {
				return
			}
		}

		req, ok := w.take(time.Now())
		if !ok {
			continue
		}
		r.settle(w, req, r.reconcile(r.ctx, customerID, req), time.Now())
	}
}

// sleep waits d, reporting false if the reconciler's context ended first.
func (r *Reconciler) sleep(d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-r.ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
