// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/central/internal/factoryclient"
	"github.com/yscale-sh/yscale/central/internal/state"
)

// Tenants provisions and offboards tenants — the lifecycle spine: one call
// onboards a tenant (token + install bundle), one call tears everything they
// own back down and verifies nothing is left. It defaults tenants to the shared Tailscale mesh, so there
// is no per-customer coordination box to manage; a customer that opted into a
// Headscale box is flagged for shell deprovision (the box is a Linode VM +
// firewall, outside central's reach).
// tenantStore is the slice of the state store the lifecycle touches. Named as
// an interface, like accountStore above it, so the fail-closed paths can be
// exercised against a store whose durable backend is refusing writes — an
// offboard that tears down a tenant it could not actually revoke is otherwise
// only observable in production.
type tenantStore interface {
	CustomerByID(id string) (*state.Customer, error)
	CreateTenant(c *state.Customer, ownerAccountID string) (*state.Customer, *state.TenantMembership, error)
	RotateCustomerCredential(customerID string, by state.Actor) (string, error)
	RevokeCustomer(id string) error
	DeleteRevokedCustomer(id string) error
	ResolveAccount(issuer, subject string) (*state.Account, error)
	// The membership and namespace mutators each take the actor the change is
	// attributed to. Every route in this file passes state.OperatorActor(): the
	// authority is the AdminAuth credential, which is neither a person nor a
	// cluster, and recording it as anything else would put a human's name on a
	// change no human made.
	GrantTenantMembership(accountID, customerID, role string, by state.Actor) (*state.TenantMembership, bool, error)
	// SetTenantMembershipRole and DeleteTenantMembership are the operator's
	// correction pair. Neither takes a caller account: an operator holds no
	// membership on the tenant they are fixing, so the store enforces the
	// tenant's own rules (liveness, the closed role set, the last owner) and
	// nothing pretends a member asked.
	SetTenantMembershipRole(accountID, customerID, role string, by state.Actor) (*state.TenantMembership, string, error)
	DeleteTenantMembership(accountID, customerID string, by state.Actor) (*state.TenantMembership, error)
	// SetCustomerWorkloadNamespaces replaces the tenant's authorized workload
	// namespaces and returns the stored set. It is here rather than on the
	// provisioning path alone because the set outlives provisioning: teams get
	// added, and a tenant seeded from env config never went through Provision
	// at all.
	SetCustomerWorkloadNamespaces(customerID string, namespaces []string, by state.Actor) ([]string, error)
	// The hosted-capacity lifecycle (tenants_hosted.go): assign one
	// platform-managed connector release + reserved namespace on the shared
	// physical cluster, rotate its credential, release the assignment. All
	// three attribute to whichever operator credential carried the request —
	// the static one on /v1/admin, the signed-in human on /v1/operator — and
	// the tenant surface can list and route to the row but never mutate it.
	AssignHostedCluster(customerID, clusterID, name, namespace string, by state.Actor) (state.TenantCluster, string, error)
	RotateHostedClusterCredential(customerID, clusterID string, by state.Actor) (state.TenantCluster, string, error)
	DeleteHostedCluster(customerID, clusterID string, by state.Actor) (string, error)
	// PendingHostedCapacityRequests is the operator's read of the pending
	// queue (tenants_hosted.go): the safe copy-out view the discovery route
	// renders, with no write and no audit row behind it.
	PendingHostedCapacityRequests() []state.PendingHostedCapacityRequest
	// HostedClusterAssignments is the same kind of read for the standing
	// inventory: every hosted assignment across live tenants, copied out under
	// the store read lock, with no write and no audit row behind it either.
	HostedClusterAssignments() []state.HostedClusterAssignment
	BurstsForCustomer(customerID string) []*state.Burst
	ListOperatorTenants(after string, limit int) ([]*state.Customer, string)
	SetCustomerLimits(customerID string, maxConcurrentBursts int, maxHourlyUSD float64, by state.Actor) (*state.Customer, bool, error)
}

type Tenants struct {
	Store tenantStore
	// Reap claims + tears down one burst (= Workloads.reapBurst). Injected so
	// offboard reuses the exact, durable reap path. It stays a bool because
	// offboard holds nothing that a richer ReapOutcome would change: it counts
	// what it reaped and leaves everything else to the later sweep the reap path
	// already arranged.
	Reap     func(ctx context.Context, burstID, reason string) bool
	Log      *slog.Logger
	Endpoint string // central's URL for the install bundle (e.g. ws://yscale-cloud.yscale:8443)
	// Factory is optional so lifecycle behavior remains unchanged without the
	// enterprise factory wiring.
	Factory factoryclient.FabricService
	// TrackFabric and ForgetFabric feed the enterprise onboarding poller. They
	// are nil in tests and whenever the factory is not configured.
	TrackFabric  func(customerID string)
	ForgetFabric func(customerID string)
	// IdentityIssuer is the Yscale ID issuer (YSCALE_ID_ISSUER) that an
	// owner_subject is keyed against. Empty when the SaaS identity provider
	// isn't configured, in which case owner assignment is refused rather than
	// guessed — an account keyed to the wrong issuer is a different human.
	IdentityIssuer string
	// Reconciler is the coordination-policy seam the hosted-cluster DELETE
	// drives, for the reason the tenant surface's is: a released assignment's
	// routes have to leave the tenant's policy and its gateway node has to give
	// the approvals back. nil skips the enqueue.
	Reconciler PolicyReconciler
	// Commands is the durable connector-command ledger the operator recovery
	// routes read and requeue through (tenants_connector_commands.go). It is a
	// separate, deliberately tiny seam rather than two more methods on
	// tenantStore: the tenant lifecycle store answers questions about tenants,
	// and this answers one about a delivery ledger. nil leaves the recovery
	// routes answering "nothing to see" and refusing every requeue.
	Commands connectorCommandRecoveryLedger
}

// errIdentityNotConfigured distinguishes "this deployment can't assign owners"
// from the provisioning conflicts Provision otherwise returns, so the handler
// can answer 503 instead of 409.
var errIdentityNotConfigured = errors.New("owner_subject requires YSCALE_ID_ISSUER to be configured")

// pilot-default spend guardrails applied to a provisioned tenant when the
// caller doesn't specify (0). Bounds the worst case to ~$240/day.
const (
	defaultMaxConcurrentBursts = 5
	defaultMaxHourlyUSD        = 10.0
)

// ProvisionOpts parameterises a new tenant. All fields optional; ID is
// generated when empty. For the limits: 0 = apply the pilot default; a negative
// value = unlimited (stored as 0); a positive value = that explicit cap.
type ProvisionOpts struct {
	ID                  string
	Email               string
	Plan                string
	MaxConcurrentBursts int
	MaxHourlyUSD        float64
	// OwnerSubject is an optional Yscale ID `sub` to install as the tenant's
	// first owner. Absent (the pre-existing behavior) means the tenant is
	// operator-managed and has no human account attached yet. Tagged because
	// Go's field matcher does not bridge the underscore.
	OwnerSubject string `json:"owner_subject"`
	// WorkloadNamespaces are the Kubernetes namespaces this tenant may submit
	// workloads into; the first is where an unqualified submission lands.
	// Omitted resolves to the fail-closed set ("default" alone) and stores
	// nothing, so a later change to what "fail-closed" means reaches these
	// tenants. Supplied, it is validated before anything is written — see
	// validateWorkloadNamespaces. These must match the connector's
	// rbac.allowedNamespaces; the rendered install command sets both from this
	// one list. HandleSetWorkloadNamespaces changes them afterwards.
	WorkloadNamespaces []string `json:"workload_namespaces"`
}

// ProvisionResult is everything the operator hands the new tenant.
type ProvisionResult struct {
	CustomerID string `json:"customer_id"`
	Token      string `json:"token"`
	Endpoint   string `json:"endpoint"`
	// AgentInstall is the ready-to-paste command the partner runs in THEIR
	// cluster to install the agent against this tenant.
	AgentInstall string       `json:"agent_install"`
	Limits       TenantLimits `json:"limits"`
	// WorkloadNamespaces echoes the namespaces this tenant may submit into,
	// resolved — so an operator who supplied none sees the fail-closed set
	// rather than an empty list they have to interpret.
	WorkloadNamespaces []string `json:"workload_namespaces"`
	// ConnectorRBACSet is the `--set` in AgentInstall that scopes the
	// connector's Secret reads, on its own. It is already inside the install
	// command; it is repeated here because the update route returns the same
	// field, and an operator scripting against both should read one key rather
	// than parse a command line.
	ConnectorRBACSet string `json:"connector_rbac_set"`
	// OwnerAccountID is the human account installed as the tenant's owner.
	// Omitted entirely when no owner_subject was supplied, so the response for
	// the pre-existing call shape is unchanged.
	OwnerAccountID string `json:"owner_account_id,omitempty"`
}

// resolveLimit maps a ProvisionOpts limit value to the stored value:
// 0 → default, negative → 0 (unlimited), positive → as-is.
func resolveLimit[T int | float64](v, def T) T {
	if v < 0 {
		return 0
	}
	if v == 0 {
		return def
	}
	return v
}

// Provision creates a tenant on the shared Tailscale mesh and returns its token
// + install bundle. Errors if the ID is already taken — by a live tenant or by a
// revoked one still being cleaned up — so re-running with a fixed id can't
// silently clobber either.
//
// The customer is created DURABLY, owner or no owner. The token in the result
// is handed to the partner exactly once and there is nothing to re-drive it
// from, so returning one whose row never landed gives them a credential that
// stops authenticating at the next restart — a tenant that works today and is
// gone tomorrow, with no error anyone saw.
//
// With opts.OwnerSubject set, the tenant also gets its first human owner, in
// the SAME durable operation as the customer. The caller therefore never
// observes a half-provisioned tenant, and there is no rollback to get wrong:
// the owner's account is resolved before anything is written (a bad subject
// creates nothing), and the create either lands both records or neither. The
// account survives a refused create — it is an identity record, not tenant
// state, and re-resolving it is idempotent.
//
// Resolved, not upserted: all an operator supplies is a `sub`, so writing a
// profile from here would blank the email/name Yscale ID cached for a human who
// has already signed in.
func (t *Tenants) Provision(opts ProvisionOpts) (*ProvisionResult, error) {
	id := opts.ID
	if id == "" {
		id = newID("cust")
	}
	// A courtesy check for the common case; CreateTenant is what actually
	// decides it, under the store's customer-mutation lock and against the
	// durable row, so two provisions racing one id cannot both win.
	if _, err := t.Store.CustomerByID(id); err == nil {
		return nil, fmt.Errorf("tenant %q already exists", id)
	}
	var owner *state.Account
	if opts.OwnerSubject != "" {
		if t.IdentityIssuer == "" {
			return nil, errIdentityNotConfigured
		}
		a, err := t.Store.ResolveAccount(t.IdentityIssuer, opts.OwnerSubject)
		if err != nil {
			return nil, fmt.Errorf("resolve owner account: %w", err)
		}
		owner = a
	}
	// Validated before the token is minted and before anything is written: an
	// entry that isn't a namespace name is bad input, and a tenant provisioned
	// around one would carry an authorization set the API server can never
	// match and an install command the operator pastes into a shell.
	var stored []string
	if len(opts.WorkloadNamespaces) > 0 {
		valid, err := validateWorkloadNamespaces(opts.WorkloadNamespaces)
		if err != nil {
			return nil, err
		}
		stored = valid
	}
	plan := opts.Plan
	if plan == "" {
		plan = "pro"
	}
	token := state.NewCustomerToken()
	limits := TenantLimits{
		MaxConcurrentBursts: resolveLimit(opts.MaxConcurrentBursts, defaultMaxConcurrentBursts),
		MaxHourlyUSD:        resolveLimit(opts.MaxHourlyUSD, defaultMaxHourlyUSD),
	}
	cust := &state.Customer{
		ID: id, Token: token, Email: opts.Email, Plan: plan,
		MaxConcurrentBursts: limits.MaxConcurrentBursts,
		MaxHourlyUSD:        limits.MaxHourlyUSD,
		WorkloadNamespaces:  stored,
	}
	namespaces := authorizedWorkloadNamespaces(cust)
	ownerAccountID := ""
	if owner != nil {
		ownerAccountID = owner.ID
	}
	if _, _, err := t.Store.CreateTenant(cust, ownerAccountID); err != nil {
		return nil, fmt.Errorf("create tenant %s: %w", id, err)
	}
	if t.Factory != nil {
		if t.TrackFabric != nil {
			t.TrackFabric(id)
		}
		go func() {
			if err := t.Factory.EnsureFabric(context.Background(), id, id); err != nil {
				t.Log.Error("ensure tenant fabric", "customer", id, "error", err)
			}
		}()
	}
	res := &ProvisionResult{
		CustomerID:         id,
		Token:              token,
		Endpoint:           t.Endpoint,
		AgentInstall:       agentInstallCommand(t.Endpoint, token, namespaces),
		Limits:             limits,
		WorkloadNamespaces: namespaces,
		ConnectorRBACSet:   allowedNamespacesFlag(namespaces),
	}
	if owner != nil {
		res.OwnerAccountID = owner.ID
	}
	t.Log.Info("tenant provisioned", "customer", id, "plan", plan,
		"max_concurrent", limits.MaxConcurrentBursts, "max_hourly_usd", limits.MaxHourlyUSD,
		"workload_namespaces", namespaces, "owner_account", res.OwnerAccountID)
	return res, nil
}

// OffboardReport records what an offboard cleaned and whether anything is left.
//
// customer_deleted does NOT mean a record was deleted. Read that sentence
// before reading the fields: the name is a legacy one this API is keeping, and
// taking it literally is the mistake this comment exists to prevent.
//
//   - CustomerRevoked: access is cut, durably. The tenant's token
//     authenticates nothing, in this process and after a restart, and every
//     human grant on it is gone. The record itself is still there, on purpose,
//     so the rest of the cleanup can be retried against it.
//   - CustomerDeleted: an ALIAS of CustomerRevoked. It is set from the same
//     step and is never true when CustomerRevoked is false, so a partial
//     offboard — revoked, residue left, record kept — reports it true with no
//     record deleted anywhere. The field predates the account lifecycle, when
//     the offboard API set it as soon as the customer stopped working and had
//     no residue check to gate it on; callers written against that API read it
//     as "this tenant is done, drop it from the console".
//     Keeping that meaning is why it is an alias and not a row flag: moving it
//     onto the row would silently leave those callers showing a revoked tenant
//     as live until an operator cleared its residue by hand. It stays for them,
//     and nothing new should read it.
//   - CustomerRecordDeleted: the additive, literal one. True when the physical
//     row is gone and the id is retired, and only after cleanup verified
//     nothing the tenant owned is still tracked. This is the field to read for
//     the state of the database.
//
// So a completed offboard reports all three true. One that hit residue reports
// customer_revoked true, customer_deleted true (the alias, still just the
// revoke), customer_record_deleted FALSE and clean false: nobody can use the
// tenant, callers on the old contract see it as offboarded, an operator still
// has a record to retry against, and the one field that names a physical
// deletion does not claim one that did not happen.
type OffboardReport struct {
	CustomerID            string   `json:"customer_id"`
	BurstsReaped          int      `json:"bursts_reaped"`
	CustomerRevoked       bool     `json:"customer_revoked"`
	CustomerDeleted       bool     `json:"customer_deleted"` // legacy alias of CustomerRevoked; never a deletion
	CustomerRecordDeleted bool     `json:"customer_record_deleted"`
	Warnings              []string `json:"warnings,omitempty"`
	Clean                 bool     `json:"clean"`
}

// Offboard tears down everything a tenant owns and verifies nothing remains:
// revokes the tenant's access, reaps all its bursts (durable teardown queue),
// tears down or flags its coordination server, re-checks the books, and only
// then removes the customer record. Returns a report; Clean is true only when
// no residue and no manual step remain.
//
// The revoke goes FIRST, and a failed revoke aborts the whole offboard. Every
// step after it destroys something that cannot be put back — a reaped burst is
// a torn-down VM, a deleted fabric is a deleted box — so running them against a
// tenant whose token still authenticates is the worst of both outcomes: the
// partner's resources are gone and their credential still works. Ordering the
// one recoverable, transactional step first means a durable failure costs a
// retry and nothing else.
//
// The record goes LAST, and only once the books are clean. Revoke is not
// delete: a revoked tenant authenticates nothing, but it is still there, so a
// reap that failed halfway — or a process that died between the revoke and the
// teardown — leaves an operator a tenant to run this against again rather than
// cloud resources with no id to name them by. Calling Offboard on a revoked
// tenant is exactly that retry: the revoke no-ops and cleanup picks up where it
// stopped.
//
// There is deliberately NO force-delete. Residue that never reaps leaves the
// tenant revoked forever, holding its id — a tombstone, not a leak. Dropping
// the burst records instead would free the id while the VMs and fabric behind
// them are still up, and the next tenant to be issued that id would inherit
// resources, mesh grants and orphan-sweep scope that are not theirs. A stuck
// tombstone costs one unusable id; a freed id with live resources behind it
// costs tenant isolation.
func (t *Tenants) Offboard(ctx context.Context, customerID string) (*OffboardReport, error) {
	cust, err := t.Store.CustomerByID(customerID)
	if err != nil {
		return nil, fmt.Errorf("tenant %q not found", customerID)
	}
	rep := &OffboardReport{CustomerID: customerID}

	// 1. Revoke access. The stamped customer record and every membership on it
	//    go in one transaction, so an offboard cannot half-succeed and leave a
	//    human holding a grant on a tenant nobody can reach. On success the
	//    token stops authenticating immediately, in this process and after a
	//    restart; on failure nothing has changed and nothing has been torn down.
	if err := t.Store.RevokeCustomer(customerID); err != nil {
		t.Log.Error("revoke offboarded customer; no teardown started", "customer", customerID, "error", err)
		return nil, fmt.Errorf("revoke tenant %s: %w", customerID, err)
	}
	// Access is gone, durably. The row is still around, and only so the rest of
	// this can be retried — customer_record_deleted stays false until it really
	// goes. customer_deleted is set from this same step because it is the legacy
	// ALIAS of the revoke (see OffboardReport), not a statement that a row was
	// removed.
	rep.CustomerRevoked = true
	rep.CustomerDeleted = true

	// A tenant that rode the SHARED tailnet had its gateway CIDRs granted
	// :10250 in a policy document it shares with every other box-less tenant.
	// Revoking access does not touch that document, and nothing else will ever
	// ask about this tenant again — it is the tenant that is going away. The
	// reconcile recomputes the cross-tenant union from what is left, so this is
	// the point at which the revoked tenant's CIDRs stop being in it. Enqueue
	// only: the ACL write runs on the reconciler's worker with its own retry.
	if !cust.HasMeshBox() && t.Reconciler != nil {
		t.Reconciler.EnqueueSharedTailnet(false)
	}

	// 2. Reap every burst the tenant owns. reapBurst claims the record (so it
	//    disappears from the books immediately) and hands cloud teardown to the
	//    durable queue. It works off the burst record, which outlives the
	//    revoked customer.
	for _, b := range t.Store.BurstsForCustomer(customerID) {
		if t.Reap != nil && t.Reap(ctx, b.ID, "tenant offboard") {
			rep.BurstsReaped++
		}
	}

	// 3. Tear down the tenant's dedicated fabric. When the factory is wired the
	// delete is dispatched UNCONDITIONALLY — even before a Mesh ref is attached —
	// because a tenant offboarded mid-provisioning still has a box being created
	// that would otherwise orphan. DeleteFabric is idempotent: the factory treats
	// an absent fabric as already-deleted. Without factory wiring, a customer on
	// a box falls back to the manual-shell deprovision warning, read off the
	// snapshot taken before the revoke.
	if t.Factory != nil {
		go func() {
			if err := t.Factory.DeleteFabric(context.Background(), customerID); err != nil {
				t.Log.Error("delete tenant fabric", "customer", customerID, "error", err)
			}
		}()
		if t.ForgetFabric != nil {
			t.ForgetFabric(customerID)
		}
	}
	if cust.HasMeshBox() && t.Factory == nil {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf(
			"customer was on a Headscale box (linode %s); run `deploy/headscale/deprovision.sh --user %s` to remove the VM + firewall",
			cust.Mesh.BackendID, cust.Mesh.User))
	}

	// 4. Verify nothing the tenant owned is still tracked. Residue is what
	//    central still holds and can still reap, and it is the only thing that
	//    holds the record open: the Headscale warning above is a shell command
	//    on a VM outside central's reach, so waiting for it would mean the
	//    tenant's record never goes away no matter how many times an operator
	//    retries.
	residue := t.Store.BurstsForCustomer(customerID)
	if len(residue) > 0 {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf(
			"%d burst(s) still tracked after reap — tenant is revoked; retry offboard to finish cleanup", len(residue)))
		rep.Clean = false
		t.Log.Warn("tenant offboard incomplete; record kept for retry",
			"customer", customerID, "bursts_reaped", rep.BurstsReaped, "bursts_left", len(residue))
		return rep, nil
	}

	// 5. Cleanup is clean, so the record can go. A failure here is retryable and
	//    costs nothing: the tenant stays revoked, so no access comes back while
	//    the operator tries again.
	if err := t.Store.DeleteRevokedCustomer(customerID); err != nil {
		t.Log.Error("delete revoked customer after clean teardown", "customer", customerID, "error", err)
		return nil, fmt.Errorf("delete revoked tenant %s: %w", customerID, err)
	}
	// The row is gone and the id is tombstoned, which is the only thing
	// customer_record_deleted claims.
	rep.CustomerRecordDeleted = true
	// Asked for again, because the row going is a second, separate shrink of the
	// shared union: the revoke above stopped the tenant CONTRIBUTING, and this
	// removes the record of what it contributed. Neither has anything left to
	// drive it — the tenant no longer exists to be reconciled.
	if !cust.HasMeshBox() && t.Reconciler != nil {
		t.Reconciler.EnqueueSharedTailnet(false)
	}

	rep.Clean = len(rep.Warnings) == 0
	t.Log.Info("tenant offboarded", "customer", customerID, "bursts_reaped", rep.BurstsReaped, "clean", rep.Clean)
	return rep, nil
}

// HandleProvision serves POST /v1/admin/tenants. The JSON body (all fields
// optional) is ProvisionOpts; the response is the ProvisionResult bundle.
func (t *Tenants) HandleProvision(w http.ResponseWriter, r *http.Request) {
	var opts ProvisionOpts
	if r.Body != nil {
		// Body is optional; ignore a decode error on an empty/absent body.
		_ = json.NewDecoder(r.Body).Decode(&opts)
	}
	res, err := t.Provision(opts)
	if err != nil {
		writeJSON(w, provisionStatus(err), map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// TenantCredentialResponse contains the newly minted tenant bearer exactly
// once. Neither the verifier nor any previous credential is exposed.
type TenantCredentialResponse struct {
	CustomerID string `json:"customer_id"`
	Token      string `json:"token"`
}

// HandleRotateCustomerCredential serves
// POST /v1/admin/tenants/{tenant_id}/credential. The AdminAuth wrapper is the
// authority; the state mutation records that operator actor durably with the
// new verifier before invalidating the old bearer.
func (t *Tenants) HandleRotateCustomerCredential(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	token, err := t.Store.RotateCustomerCredential(tenantID, state.OperatorActor())
	if err != nil {
		status := http.StatusNotFound
		if errors.Is(err, state.ErrPersistence) {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	t.Log.Info("tenant credential rotated", "tenant", tenantID)
	writeJSON(w, http.StatusOK, TenantCredentialResponse{CustomerID: tenantID, Token: token})
}

// provisionStatus maps a Provision failure to a status. Everything that isn't a
// new owner-assignment failure keeps the original 409, so the duplicate-id
// contract callers already depend on is unchanged.
//
// A store that could not persist the account or the membership is 503, not 409:
// the request was valid and nothing about it conflicts — the durable backend is
// down, and the operator should retry rather than go looking for the tenant
// that supposedly already exists.
// An owner the store cannot find is 400: the request named a human who does not
// exist, which is bad input, not a collision on the tenant id. A workload
// namespace that isn't a namespace name is 400 for the same reason.
func provisionStatus(err error) int {
	switch {
	case errors.Is(err, errIdentityNotConfigured), errors.Is(err, state.ErrPersistence):
		return http.StatusServiceUnavailable
	case errors.Is(err, errInvalidWorkloadNamespaces),
		errors.Is(err, state.ErrInvalidIdentity), errors.Is(err, state.ErrNotFound):
		return http.StatusBadRequest
	default:
		return http.StatusConflict
	}
}

// HandleOffboard serves DELETE /v1/admin/tenants/{id}. Returns the
// OffboardReport (200 even when Clean is false — the caller inspects warnings).
//
// There is no force parameter, and its PRESENCE is a 400 rather than a silently
// ignored one. An earlier revision took force=abandon, which dropped a stuck
// tenant's burst records to free its id; that is exactly the cross-tenant hole
// this lifecycle exists to close, so the parameter was removed. A caller or
// script still sending it is asking for teardown semantics this endpoint no
// longer has, and ignoring it would run an ordinary offboard while the operator
// believes the tenant was force-removed. Rejecting BEFORE Offboard runs is the
// point: an unrecognised request performs no revoke, no reap and no delete.
//
// The key, not its value: ?force= carries no value at all and ?force=&force=1
// carries two, and a caller that sent either meant the same thing as force=1.
// Reading only the first non-empty value would run the ordinary offboard for
// both — which is the silent-ignore this rejection exists to avoid — so the
// query is checked for the key itself and every value it carries is echoed back.
func (t *Tenants) HandleOffboard(w http.ResponseWriter, r *http.Request) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid query string"})
		return
	}
	if values, sent := query["force"]; sent {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("force=%s is not supported: an offboard that cannot finish "+
				"leaves the tenant revoked and retryable, and nothing frees its id while "+
				"resources it owned are still tracked", strings.Join(values, ",")),
		})
		return
	}
	rep, err := t.Offboard(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, offboardStatus(err), map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// offboardStatus maps an Offboard failure to a status. A durable step that
// could not run is 503 and worth retrying: the request was valid and the tenant
// is still there — either untouched, because the revoke runs first and nothing
// is torn down without it, or revoked and awaiting only its final delete.
// Everything else is the unknown-tenant 404 this route has always answered.
func offboardStatus(err error) int {
	if errors.Is(err, state.ErrPersistence) {
		return http.StatusServiceUnavailable
	}
	return http.StatusNotFound
}

// GrantMemberRequest is the body of POST /v1/admin/tenants/{tenant_id}/members:
// bind an existing human account to this tenant with a role.
type GrantMemberRequest struct {
	AccountID string `json:"account_id"`
	Role      string `json:"role"`
}

// UpdateMemberRequest is the body of
// PATCH /v1/admin/tenants/{tenant_id}/members/{account_id}: the role the named
// member should hold from now on. Only the role is settable — the account and
// the tenant are the path, so a body that disagreed with them would be a second
// way to say the same thing and a way to say it wrong.
type UpdateMemberRequest struct {
	Role string `json:"role"`
}

// GrantMemberResult is what the operator gets back from the grant route and
// from the correction route, which render the same member document so one
// shape covers "this is now true" either way. Deliberately the same narrow
// shape a co-member sees minus the profile: the account id they named, the role
// that is now durable, and when the grant was made — CreatedAt is the grant's
// birth, so a role change does not move it. The human's issuer/subject, the
// tenant's Customer.Token and anything about its mesh are absent by
// construction — an operator route is not a reason to render them.
type GrantMemberResult struct {
	CustomerID string    `json:"customer_id"`
	AccountID  string    `json:"account_id"`
	Role       string    `json:"role"`
	CreatedAt  time.Time `json:"created_at"`
}

// HandleGrantMember serves POST /v1/admin/tenants/{tenant_id}/members — the one
// way a membership is created outside provisioning, and the reason the roster
// routes are reachable in production at all: /v1/account and the roster surface
// can read and remove grants, and until this route existed only a tenant's
// FIRST owner (from provision's owner_subject) could ever be made.
//
// It is an operator route, wired behind the same AdminAuth as provision and
// offboard. That is the whole authorization model here and it is deliberately
// not the human one: no Yscale ID access token and no Customer.Token opens it,
// because a human granting themselves a role on a tenant is the escalation this
// surface must not have, and a cluster credential is not a person at all.
// AdminAuth with no YSCALE_ADMIN_TOKEN 404s, so an unconfigured deployment does
// not have the route rather than having an open one.
//
// Every durable rule belongs to the store: live tenant, existing account,
// closed role set, idempotent re-grant, and a conflicting role refused rather
// than overwritten so a re-run cannot silently demote an owner. Changing a role
// that is already granted is HandleUpdateMemberRole's job, precisely because it
// has to say so.
func (t *Tenants) HandleGrantMember(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	var req GrantMemberRequest
	if r.Body == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a JSON body with account_id and role is required"})
		return
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Unlike provision's optional body, this one carries the whole request:
		// a body that did not parse names nobody, so there is nothing to grant.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.AccountID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "account_id is required"})
		return
	}
	if !state.ValidRole(req.Role) {
		// Checked here as well as in the store so an unrecognised role is bad
		// input (400) rather than the 404 an empty account id would collide with.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": invalidRoleMessage})
		return
	}

	m, created, err := t.Store.GrantTenantMembership(req.AccountID, tenantID, req.Role, state.OperatorActor())
	if err != nil {
		t.refuseMember(w, "grant tenant membership", tenantID, req.AccountID, err, "role", req.Role)
		return
	}
	res := GrantMemberResult{
		CustomerID: m.CustomerID,
		AccountID:  m.AccountID,
		Role:       m.Role,
		CreatedAt:  m.CreatedAt,
	}
	if !created {
		// The same grant was already there, so nothing was written and nothing
		// is audited: 200 says the state the caller asked for holds.
		writeJSON(w, http.StatusOK, res)
		return
	}
	t.Log.Info("tenant member granted", "tenant", tenantID, "account", m.AccountID, "role", m.Role)
	writeJSON(w, http.StatusCreated, res)
}

// HandleUpdateMemberRole serves
// PATCH /v1/admin/tenants/{tenant_id}/members/{account_id} — the operator's
// correction for a grant that named the right human with the wrong role. It
// exists because the grant route refuses a conflicting re-grant (409) rather
// than overwriting it: without a route that says "change this", a mistyped role
// on a tenant whose only manager holds it is unfixable through this API.
//
// Same AdminAuth as the grant route and for the same reason — a human who could
// change their own role is the escalation the whole surface is built to
// prevent, and a cluster token is not a person. The store owns every rule: the
// tenant must be live, the membership must already exist (404 covers all three
// misses together), the role must be in the closed set (400), and the last
// owner may not be demoted (409). Setting the role a member already holds is an
// idempotent 200 that writes nothing and audits nothing.
func (t *Tenants) HandleUpdateMemberRole(w http.ResponseWriter, r *http.Request) {
	tenantID, accountID := r.PathValue("tenant_id"), r.PathValue("account_id")
	if tenantID == "" || accountID == "" {
		http.NotFound(w, r)
		return
	}
	var req UpdateMemberRequest
	if r.Body == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a JSON body with role is required"})
		return
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// The role is the whole request: a body that did not parse asks for
		// nothing, and guessing which role it meant is how a demotion happens.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if !state.ValidRole(req.Role) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": invalidRoleMessage})
		return
	}

	m, previous, err := t.Store.SetTenantMembershipRole(accountID, tenantID, req.Role, state.OperatorActor())
	if err != nil {
		t.refuseMember(w, "set tenant member role", tenantID, accountID, err)
		return
	}
	res := GrantMemberResult{
		CustomerID: m.CustomerID,
		AccountID:  m.AccountID,
		Role:       m.Role,
		CreatedAt:  m.CreatedAt,
	}
	if previous == m.Role {
		// The member already held this role, so nothing was written. 200 says
		// the state the caller asked for holds; claiming a change here would put
		// a role transition that never happened in the audit trail.
		writeJSON(w, http.StatusOK, res)
		return
	}
	t.Log.Info("tenant member role changed", "tenant", tenantID, "account", m.AccountID,
		"old_role", previous, "new_role", m.Role)
	writeJSON(w, http.StatusOK, res)
}

// HandleRevokeMember serves
// DELETE /v1/admin/tenants/{tenant_id}/members/{account_id} — the operator's
// revoke, and the recovery path for a grant made to the wrong human on a tenant
// whose remaining members cannot reach them. The human route
// (Accounts.HandleRemoveMember) is unchanged and still the normal way a tenant
// manages itself; this one answers to the operator credential instead, so a
// tenant with no willing owner is not stuck.
//
// Removing an already-absent member is 204, but only after the store has proved
// the tenant is live — 204 on a tenant that does not exist would report a
// revocation on nothing. The last owner is 409 even here: an operator who wants
// them gone grants a replacement owner first, because nothing can un-strand a
// tenant whose only owner was deleted by the route meant to fix it.
//
// Like the human removal, this does NOT rotate the tenant's Customer.Token:
// that credential belongs to the cluster, not to any member. See
// docs/internals/tenant-isolation.md §7a.
func (t *Tenants) HandleRevokeMember(w http.ResponseWriter, r *http.Request) {
	tenantID, accountID := r.PathValue("tenant_id"), r.PathValue("account_id")
	if tenantID == "" || accountID == "" {
		http.NotFound(w, r)
		return
	}
	removed, err := t.Store.DeleteTenantMembership(accountID, tenantID, state.OperatorActor())
	if err != nil {
		t.refuseMember(w, "delete tenant membership", tenantID, accountID, err)
		return
	}
	if removed != nil {
		// nil is the idempotent no-op: the member was already gone, so this call
		// revoked no access and the audit trail must not say it did. The store
		// reports which happened from inside its write lock, so the log cannot
		// claim a removal another caller made.
		t.Log.Info("tenant member revoked", "tenant", tenantID,
			"account", removed.AccountID, "role", removed.Role)
	}
	w.WriteHeader(http.StatusNoContent)
}

// UpdateWorkloadNamespacesRequest is the body of
// PUT /v1/admin/tenants/{tenant_id}/workload-namespaces: the complete set of
// namespaces the tenant may submit into from now on.
//
// A replacement, not a patch, and that is why it is PUT. The set is an
// authorization boundary read as a whole on every submission, so an add-one
// verb would leave an operator removing a team's access with no way to say so —
// and reading back what they just wrote is the only way to be sure of what a
// tenant can reach. There is no "clear it" spelling: an empty list is refused,
// because a tenant with none configured resolves to the fail-closed default,
// and an operator who typed `[]` cannot mean both "no namespaces" and "default".
type UpdateWorkloadNamespacesRequest struct {
	WorkloadNamespaces []string `json:"workload_namespaces"`
}

// WorkloadNamespacesResult is what the update route returns: the set that is
// now durable, plus the connector-side change the operator still owes.
//
// Central is only ONE of the two gates. The connector's ServiceAccount holds
// `get` on Secrets in exactly `rbac.allowedNamespaces`, and central cannot
// change that — it lives in the customer's own cluster, applied by their helm
// release. So a namespace added here and not there authorizes a submission
// whose bucket credentials the API server then refuses to hand over, and one
// removed here but left there keeps a grant nobody is authorized to use.
// Naming the exact flag and the exact command is the difference between an
// operator finishing the job and an operator finding out from a failed burst.
type WorkloadNamespacesResult struct {
	CustomerID         string   `json:"customer_id"`
	WorkloadNamespaces []string `json:"workload_namespaces"`
	// ConnectorRBACSet is the helm `--set` that scopes the connector's Secret
	// reads to this same list.
	ConnectorRBACSet string `json:"connector_rbac_set"`
	// ConnectorUpgrade is that flag inside a ready-to-paste upgrade of an
	// already-installed connector.
	ConnectorUpgrade string `json:"connector_upgrade"`
	// ConnectorActionRequired says, in one sentence, that central's half is
	// done and the customer's cluster still has to be told.
	ConnectorActionRequired string `json:"connector_action_required"`
}

// HandleSetWorkloadNamespaces serves
// PUT /v1/admin/tenants/{tenant_id}/workload-namespaces — the supported way a
// tenant's authorized workload namespaces change after provisioning.
//
// It exists because provisioning is not when a tenant learns which namespaces
// it needs. A pilot adds a second team, an operator gets the list wrong the
// first time, and a tenant seeded from env config (CUSTn_ID/CUSTn_TOKEN, and
// the dev seed) never went through Provision at all — every one of those was,
// until this route, an edit to a Postgres row or a restart away from the
// fail-closed default. It answers to the same AdminAuth as provision and
// offboard, for the same reason the member routes do: a tenant widening its own
// namespace set is the escalation this boundary exists to prevent, and the
// cluster's own token is not a person.
func (t *Tenants) HandleSetWorkloadNamespaces(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		http.NotFound(w, r)
		return
	}
	var req UpdateWorkloadNamespacesRequest
	if r.Body == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a JSON body with workload_namespaces is required"})
		return
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// The list is the whole request. A body that did not parse asks for no
		// set at all, and guessing one would be guessing an authorization
		// boundary.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	namespaces, err := validateWorkloadNamespaces(req.WorkloadNamespaces)
	if err != nil {
		// Before the store, so a malformed list never reaches the durable
		// record or the rendered command.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	stored, err := t.Store.SetCustomerWorkloadNamespaces(tenantID, namespaces, state.OperatorActor())
	if err != nil {
		status := http.StatusNotFound
		body := "tenant not found"
		switch {
		case errors.Is(err, state.ErrPersistence):
			status, body = http.StatusServiceUnavailable, "account store unavailable"
			t.Log.Error("set tenant workload namespaces", "tenant", tenantID, "error", err)
		case errors.Is(err, state.ErrHostedNamespaceRequired):
			// A well-formed list that drops a hosted assignment's reserved
			// namespace conflicts with the standing assignment rather than
			// failing validation. The store's message names the namespace the
			// set must keep; the operator keeps it or deletes the assignment.
			status, body = http.StatusConflict, err.Error()
			t.Log.Warn("set tenant workload namespaces refused", "tenant", tenantID, "error", err, "status", status)
		default:
			t.Log.Warn("set tenant workload namespaces refused", "tenant", tenantID, "error", err, "status", status)
		}
		writeJSON(w, status, map[string]string{"error": body})
		return
	}

	t.Log.Info("tenant workload namespaces updated", "tenant", tenantID, "workload_namespaces", stored)
	writeJSON(w, http.StatusOK, WorkloadNamespacesResult{
		CustomerID:         tenantID,
		WorkloadNamespaces: stored,
		ConnectorRBACSet:   allowedNamespacesFlag(stored),
		ConnectorUpgrade:   agentUpgradeCommand(stored),
		ConnectorActionRequired: "central now authorizes exactly these namespaces; the connector's Secret RBAC is " +
			"a separate gate in the customer's cluster — run connector_upgrade there so rbac.allowedNamespaces matches",
	})
}

// invalidRoleMessage is the one thing every route says about a role outside the
// closed set, so the answer does not depend on which handler rejected it.
const invalidRoleMessage = "role must be one of owner, admin, member, viewer"

// refuseMember answers a store failure on the operator member routes: a status
// and a FIXED body from memberRefusal, plus the store's own error in the log.
//
// The split is the point. The store's text names membership ids and stored
// roles ("mbr_… is \"owner\", not \"admin\""), which is exactly what an operator
// debugging a refusal wants and exactly what a response body should not carry —
// echoing it made every refusal a different string, so nothing could depend on
// one, and put internal identifiers in a caller's error path. A 503 logs at
// Error because the durable backend refused; the rest is the request being
// wrong, which is a Warn.
func (t *Tenants) refuseMember(w http.ResponseWriter, op, tenantID, accountID string, err error, attrs ...any) {
	status, body := memberRefusal(err)
	logAttrs := []any{"tenant", tenantID, "account", accountID}
	logAttrs = append(logAttrs, attrs...)
	logAttrs = append(logAttrs, "error", err)
	if status == http.StatusServiceUnavailable {
		t.Log.Error(op, logAttrs...)
	} else {
		logAttrs = append(logAttrs, "status", status)
		t.Log.Warn(op+" refused", logAttrs...)
	}
	writeJSON(w, status, map[string]string{"error": body})
}

// memberRefusal maps a membership write failure to a status and the fixed body
// that goes with it. A tenant that does not exist, one that is revoked, an
// account that does not exist and a membership that was never granted all
// answer 404: this is an operator route, so there is no enumeration oracle to
// protect, and all four mean the same thing — the request named something that
// is not there.
//
// The two 409s are distinct because they are different problems with different
// fixes: a role already granted is corrected with PATCH, and a last owner is
// resolved by granting another one first.
//
// The default is 503, not 500: everything reachable here is a durable write
// path, and an error this function does not recognise is one whose effect on
// the membership is unknown, so the honest answer is the retryable one.
func memberRefusal(err error) (int, string) {
	switch {
	case errors.Is(err, state.ErrPersistence):
		return http.StatusServiceUnavailable, "account store unavailable"
	case errors.Is(err, state.ErrInvalidRole):
		return http.StatusBadRequest, invalidRoleMessage
	case errors.Is(err, state.ErrNotFound):
		return http.StatusNotFound, "tenant, account or membership not found"
	case errors.Is(err, state.ErrRoleConflict):
		return http.StatusConflict, "a different role is already granted; change it with PATCH"
	case errors.Is(err, state.ErrLastOwner):
		return http.StatusConflict, "tenant must keep at least one owner"
	case errors.Is(err, state.ErrOwnerProtected), errors.Is(err, state.ErrNotAuthorized):
		return http.StatusForbidden, "membership does not authorize this operation"
	default:
		return http.StatusServiceUnavailable, "account store unavailable"
	}
}

// agentChartRef is the connector chart shipped in the source tree and release
// archive. Every rendered install/upgrade command uses it: no Yscale-operated
// chart registry is assumed. Run from the repository/archive root, or
// substitute the operator's own chart mirror.
const agentChartRef = "./deploy/helm/yscale-agent"

// agentInstallCommand renders the helm command the partner runs to install the
// agent against this tenant. Matches deploy/helm/yscale-agent values
// (token=YSCALE_TOKEN, endpoint=central URL).
//
// rbac.allowedNamespaces is rendered from the SAME list central authorizes
// submissions against. The two are independent gates on the same boundary —
// central refuses the submission, the connector's RBAC refuses the Secret read
// — and they only work as defence in depth if they agree.
func agentInstallCommand(endpoint, token string, namespaces []string) string {
	return fmt.Sprintf("helm install yscale-agent "+agentChartRef+" "+
		"--namespace yscale --create-namespace "+
		"--set token=%s --set endpoint=%s %s",
		shellQuote(token), shellQuote(endpoint), allowedNamespacesFlag(namespaces))
}

// agentUpgradeCommand is agentInstallCommand's counterpart for a connector that
// is already installed: change the one value and leave everything else the
// operator configured alone (--reuse-values).
func agentUpgradeCommand(namespaces []string) string {
	return "helm upgrade yscale-agent " + agentChartRef + " " +
		"--namespace yscale --reuse-values " + allowedNamespacesFlag(namespaces)
}

// allowedNamespacesFlag renders the single helm flag that scopes the
// connector's Secret RBAC, so the install and upgrade commands and the JSON
// field an operator scripts against cannot spell it three different ways.
func allowedNamespacesFlag(namespaces []string) string {
	return "--set " + shellQuote("rbac.allowedNamespaces={"+strings.Join(namespaces, ",")+"}")
}

// shellQuote wraps a value for a POSIX shell: single quotes, with an embedded
// quote spelled '\” — inside single quotes nothing else is special, so this
// holds for every byte.
//
// Belt to validateWorkloadNamespaces' braces. Namespaces are DNS-1123 labels by
// the time they get here and cannot carry shell syntax at all, but these
// commands are rendered to be pasted into a root shell against the customer's
// own cluster, and the endpoint is operator-supplied config
// (YSCALE_CENTRAL_ENDPOINT) that nothing validates. A render that is safe only
// because of a check in another file is one refactor from being a command
// injection, so the quoting is here too, unconditionally.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
