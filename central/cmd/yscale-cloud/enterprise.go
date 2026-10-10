// yscale:proprietary

// Default full-release wiring: operator-owned Headscale factory, per-tenant
// coordination boxes and managed mesh policy, including self-hosted installs.
// The historical split-edition exporter removes this file; the default build
// and release packager never use that exporter.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/central/internal/credentialcipher"
	"github.com/yscale-sh/yscale/central/internal/decider"
	"github.com/yscale-sh/yscale/central/internal/factoryclient"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/central/internal/meshpolicy"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/factory/client"
	linodebackend "github.com/yscale-sh/yscale/pkg/backends/linode"
)

var _ mesh.Provider = (*client.Headscale)(nil)

var (
	enterpriseFactory factoryclient.FabricService
	enterprisePoller  *fabricPoller
)

func init() {
	validateMeshConfig = validateManagedMeshConfig
	// Per-customer Headscale policy reconciler: builds the tagOwner/route-approver
	// policy from each customer's stored, validated gateway routes and PUTs it to
	// their box, serialized per customer and off the WS read goroutine. Workers
	// live for the process lifetime (background ctx); boot reconcile and the
	// agent-stream handler both drive it.
	newPolicyReconciler = func(store *state.Store, log *slog.Logger) handlers.PolicyReconciler {
		return meshpolicy.New(context.Background(), store, log)
	}

	attachMeshBoxes = func(store *state.Store, log *slog.Logger, rec handlers.PolicyReconciler, dec *decider.Decider, aa *handlers.AgentAuth, stream *handlers.AgentStream) {
		if cipher, err := credentialcipher.New(os.Getenv("YSCALE_CREDENTIAL_MASTER_KEY")); err == nil {
			dec.WithCredentialCipher(cipher)
			if stream != nil {
				stream.CredentialCipher = cipher
			}
		} else if store.HasCloudAccounts() {
			panic("durable cloud accounts require a valid YSCALE_CREDENTIAL_MASTER_KEY")
		}
		if dec.TS() != nil {
			panic("managed-mesh release must not have TS_OAUTH_* set — configure your own factory; there is no shared Tailscale fallback")
		}

		// boxProvider constructs a provider for a customer's self-hosted
		// coordination box. This is the enterprise wiring of the
		// mesh.BoxProviderFunc seam; the OSS build leaves the seam unset so
		// every customer uses Tailscale.
		boxProvider := func(loginServer, apiKey, user string) mesh.Provider {
			return client.NewHeadscale(loginServer, apiKey, user)
		}
		dec.WithBoxProvider(boxProvider)
		aa.BoxProvider = boxProvider

		if factoryURL := os.Getenv("FACTORY_URL"); factoryURL != "" {
			fc := factoryclient.New(factoryURL, os.Getenv("FACTORY_BEARER_TOKEN"))
			enterpriseFactory = fc
			// Factory-routed mesh: a factory-provisioned tenant has no box key
			// in central. Burst and agent mint, teardown and policy are all
			// factory RPCs keyed by tenant ID.
			fabricMesh := func(customerID, loginServer string) mesh.Provider {
				return factoryclient.NewTenantMesh(fc, customerID, loginServer)
			}
			dec.WithFabricProvider(fabricMesh)
			aa.FabricProvider = fabricMesh
			if mr, ok := rec.(*meshpolicy.Reconciler); ok {
				mr.UseFabric(func(customerID string, ep *state.MeshEndpoint) meshpolicy.PolicyEnsurer {
					return factoryclient.NewTenantMesh(fc, customerID, ep.LoginServer)
				})
			}
			enterprisePoller = newFabricPoller(store, enterpriseFactory, log, 20*time.Second)
			if mr, ok := rec.(*meshpolicy.Reconciler); ok {
				enterprisePoller.attach = mr.ReconcileAttach
			}
			enterprisePoller.Start(backgroundWorkersContext)
			go enterprisePoller.replayAttached(backgroundWorkersContext)
			dec.SetFabricProvisioning(enterprisePoller.Provisioning)
		}

		mrec, ok := rec.(*meshpolicy.Reconciler)
		if !ok {
			panic(fmt.Sprintf("enterprise activation invariant violation: expected *meshpolicy.Reconciler, got %T", rec))
		}

		// Wire the shared Tailscale SaaS client so customers WITHOUT a per-customer
		// Headscale box (the default shared multi-tenant tailnet) get their
		// kubelet-transparency ACL pushed too. dec.TS() is nil when
		// TS_OAUTH_* is unset, in which case UseSharedTailnet is a no-op and the
		// shared-tailnet path keeps its legacy no-op.
		mrec.UseSharedTailnet(dec.TS())

		if c, err := store.CustomerByID(state.DevCustomerID); err == nil {
			// Attach cust_test's Headscale box from the unprefixed HEADSCALE_* env
			// (the original single-customer activation path, unchanged). Keyed off
			// the stable customer ID, not the now-randomized dev token.
			attachHeadscaleFromEnv(store, log, mrec, c.ID, "")
		}

		// Multi-tenant dev seed: additional customers from CUSTN_ID + CUSTN_TOKEN
		// env (N=2,3,...), each with its OWN Headscale box from HEADSCALEN_* env.
		// This is the env-driven equivalent of a signup flow — lets us prove the
		// per-customer factory with 2+ tenants on one central. Absent env => no-op,
		// so the default single-customer behavior is unchanged.
		for n := 2; n <= 9; n++ {
			id := os.Getenv(fmt.Sprintf("CUST%d_ID", n))
			tok := os.Getenv(fmt.Sprintf("CUST%d_TOKEN", n))
			if id == "" || tok == "" {
				continue
			}
			c := store.AddCustomer(&state.Customer{ID: id, Token: tok, Plan: "pro"})
			if _, err := store.CustomerByID(id); err != nil {
				log.Warn("extra customer seed refused", "customer", id, "slot", n, "error", err)
				continue
			}
			log.Info("seeded extra customer (dev multi-tenant)", "customer", c.ID, "slot", n)
			attachHeadscaleFromEnv(store, log, mrec, c.ID, fmt.Sprintf("HEADSCALE%d_", n))
		}

		// The shared tailnet's own startup replay, and the counterpart of the
		// attach reconcile above for every tenant that has no box to attach.
		// Last, so the boxes claimed just now are already excluded from the
		// shared union rather than briefly counted into it.
		//
		// The work a restart would otherwise swallow is real on this path too: a
		// tenant offboarded, or a live-only connector's route intent reaped at
		// load, leaves the shared ACL granting :10250 to CIDRs no tenant
		// advertises any more — and nothing will ask about it, because what went
		// away IS the thing that would have asked.
		if rerr := mrec.ReplaySharedTailnet(context.Background()); rerr != nil {
			log.Error("shared-tailnet startup replay incomplete; the reconciler's worker retries", "error", rerr)
		}
	}

	// centralEndpoint is central's URL for every install bundle this build
	// renders, with the agent helm chart's default when
	// YSCALE_CENTRAL_ENDPOINT is unset — one helper so no two surfaces can
	// tell a customer two different centrals.
	centralEndpoint := func() string {
		if endpoint := os.Getenv("YSCALE_CENTRAL_ENDPOINT"); endpoint != "" {
			return endpoint
		}
		return "ws://yscale-cloud.yscale:8443" // matches the agent helm default
	}

	// Operator-only tenant lifecycle (provision a pilot tenant; offboard it +
	// everything it owns). Multi-tenancy is an enterprise feature, so these
	// routes exist only in this build. Gated on YSCALE_ADMIN_TOKEN — unset
	// disables them (AdminAuth 404s).
	registerAdminRoutes = func(mux *http.ServeMux, store *state.Store, wls *handlers.Workloads, rec handlers.PolicyReconciler, log *slog.Logger) {
		tenants := &handlers.Tenants{
			Store: store,
			// Offboard only ever asked "did this one get torn down"; the reap
			// path's other outcomes are for the caller that holds a cordon.
			Reap: func(ctx context.Context, burstID, reason string) bool {
				return wls.ReapBurst(ctx, burstID, reason).Reaped()
			},
			Log:            log,
			Endpoint:       centralEndpoint(),
			Factory:        enterpriseFactory,
			IdentityIssuer: os.Getenv("YSCALE_ID_ISSUER"),
			// The same reconciler the agent stream drives. Releasing a hosted
			// assignment is the one route that has to withdraw a tenant's
			// gateway routes, and the connector it would otherwise have heard
			// it from is being taken away by this very call.
			Reconciler: rec,
			// The durable connector-command ledger, for the recovery routes
			// below. The tenant surface reads this same ledger; only an
			// operator credential may requeue out of it.
			Commands: store,
		}
		if enterprisePoller != nil {
			tenants.TrackFabric = enterprisePoller.Track
			tenants.ForgetFabric = enterprisePoller.Forget
		}
		adminToken := os.Getenv("YSCALE_ADMIN_TOKEN")
		mux.Handle("POST /v1/admin/tenants",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleProvision)))
		mux.Handle("POST /v1/admin/tenants/{tenant_id}/credential",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleRotateCustomerCredential)))
		mux.Handle("DELETE /v1/admin/tenants/{id}",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleOffboard)))
		// The namespaces a tenant may submit workloads into are one half of the
		// cross-namespace boundary (SECURITY-REVIEW.md H2) and they change after
		// provisioning — a pilot adds a team, an operator gets the list wrong, a
		// tenant seeded from CUSTn_* env never had one. Same AdminAuth: a tenant
		// that could widen its own set is the escalation the boundary exists for.
		mux.Handle("PUT /v1/admin/tenants/{tenant_id}/workload-namespaces",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleSetWorkloadNamespaces)))
		// Membership grants ride the operator credential, not the human one:
		// this is the only way a role is created after a tenant's first owner,
		// and a human who could grant themselves one would be the escalation the
		// roster routes exist to prevent. Same AdminAuth, so an unset
		// YSCALE_ADMIN_TOKEN 404s this route with the other two.
		mux.Handle("POST /v1/admin/tenants/{tenant_id}/members",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleGrantMember)))
		// The correction pair. A grant with the wrong role is 409 rather than an
		// overwrite, and the human removal needs an owner or admin of the tenant
		// to run it, so without these two a mistyped grant on a tenant whose only
		// manager holds it has no fix. Same AdminAuth as the grant, so all three
		// appear and disappear together with YSCALE_ADMIN_TOKEN.
		mux.Handle("PATCH /v1/admin/tenants/{tenant_id}/members/{account_id}",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleUpdateMemberRole)))
		mux.Handle("DELETE /v1/admin/tenants/{tenant_id}/members/{account_id}",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleRevokeMember)))
		// Yscale-hosted shared capacity: assign one platform-managed connector
		// release + reserved namespace per tenant on the shared physical
		// cluster, rotate its credential, release the assignment. Operator
		// credential for the same reason as the namespace route above — the
		// assignment moves the tenant's namespace authorization, and the
		// release runs on hardware only the operator holds. The tenant lists
		// and routes to the row through their existing cluster surface, which
		// refuses to rotate or delete it. Same AdminAuth, so all three appear
		// and disappear with YSCALE_ADMIN_TOKEN.
		mux.Handle("POST /v1/admin/tenants/{tenant_id}/hosted-clusters",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleAssignHostedCluster)))
		// The queue the assignment route consumes: which tenants are waiting
		// on shared capacity, so an operator can discover them without
		// already knowing each tenant id. A pure read — the response is the
		// store's safe copy-out view, so the route writes nothing and appends
		// no audit row. Same AdminAuth as the three mutators, so the whole
		// hosted surface appears and disappears with YSCALE_ADMIN_TOKEN.
		mux.Handle("GET /v1/admin/hosted-capacity/requests",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleListHostedCapacityRequests)))
		mux.Handle("GET /v1/admin/hosted-clusters",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleListHostedClusters)))
		mux.Handle("POST /v1/admin/tenants/{tenant_id}/hosted-clusters/{cluster_id}/credential",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleRotateHostedClusterCredential)))
		mux.Handle("DELETE /v1/admin/tenants/{tenant_id}/hosted-clusters/{cluster_id}",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleDeleteHostedCluster)))
		// Connector-command recovery, operator half. The tenant keeps the read
		// of its OWN stalls (GET /v1/connector-commands); requeueing a dead
		// letter is here, on the operator credential, because on the OSS and
		// connector path a tenant token IS the cluster's own credential — and
		// a connector that could re-drive the drains and announces central is
		// sending it is exactly the reach the ledger exists to bound. The
		// tenant is the {tenant_id} path segment, never a body; the audit row
		// is attributed to state.OperatorActor() because this shared static
		// token holds no per-operator identity. Same AdminAuth, so both appear
		// and disappear with YSCALE_ADMIN_TOKEN.
		mux.Handle("GET /v1/admin/tenants/{tenant_id}/connector-commands",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleListTenantConnectorCommands)))
		mux.Handle("POST /v1/admin/tenants/{tenant_id}/connector-commands/{id}/requeue",
			handlers.AdminAuth(adminToken, http.HandlerFunc(tenants.HandleRequeueTenantConnectorCommand)))
	}

	// SaaS human accounts. Yscale ID is the identity provider — central stores
	// no password and issues no human session — so the route only exists when
	// YSCALE_ID_INTERNAL_URL names a Yscale ID to validate access tokens
	// against; without it the handler 404s, mirroring AdminAuth's unset-token
	// behavior. This is a separate hook from registerAdminRoutes because it is a
	// separate credential: /v1/account authenticates a human, /v1/admin/tenants
	// an operator, and neither is a cluster token.
	registerAccountRoutes = func(mux *http.ServeMux, store *state.Store, wls *handlers.Workloads, rec handlers.PolicyReconciler, log *slog.Logger) {
		credentialCipher, cipherErr := credentialcipher.New(os.Getenv("YSCALE_CREDENTIAL_MASTER_KEY"))
		if cipherErr != nil && store.HasCloudAccounts() {
			panic("durable cloud accounts require a valid YSCALE_CREDENTIAL_MASTER_KEY")
		}
		accounts := &handlers.Accounts{
			Store:     store,
			Issuer:    os.Getenv("YSCALE_ID_ISSUER"),
			Log:       log,
			Workloads: wls,
			// The cluster DELETE below withdraws that cluster's route intent;
			// this is what converges the coordination server to it.
			Reconciler: rec,
			// Same endpoint the admin provision path renders into its install
			// command, so the two surfaces never tell a customer two different
			// centrals.
			Endpoint: os.Getenv("YSCALE_CENTRAL_ENDPOINT"),
			// Self-service first-workspace signup is a deliberate opt-in on the
			// exact value "true" — any other spelling leaves the route absent,
			// so a deployment cannot start selling workspaces by typo.
			AllowSelfServiceTenants: os.Getenv("YSCALE_SELF_SERVICE_TENANTS") == "true",
			CredentialCipher:        credentialCipher,
			ValidateLinodeAccount: func(ctx context.Context, token, region string) (string, error) {
				return linodebackend.NewWithConfig(token, linodebackend.Config{Region: region}).ValidateAccount(ctx)
			},
		}
		if idURL := os.Getenv("YSCALE_ID_INTERNAL_URL"); idURL != "" {
			accounts.Resolver = &handlers.YscaleID{BaseURL: idURL}
		}
		mux.Handle("GET /v1/account", http.HandlerFunc(accounts.HandleGet))
		// First-run onboarding: a verified human with no tenant creates their own
		// workspace on the SAME human credential as /v1/account. Registered
		// unconditionally and gated inside the handler, so a deployment with
		// YSCALE_SELF_SERVICE_TENANTS unset answers the identical 404 a route
		// that does not exist would.
		mux.Handle("POST /v1/account/tenants", http.HandlerFunc(accounts.HandleCreateTenant))
		mux.Handle("GET /v1/tenants/{tenant_id}/workloads", http.HandlerFunc(accounts.HandleListWorkloads))
		mux.Handle("POST /v1/tenants/{tenant_id}/workloads", http.HandlerFunc(accounts.HandleCreateWorkload))
		// The placement a launch would make, without making it. Registered with
		// the human workload routes because it is the same credential, the same
		// tenant scope and the same decision path — it just stops before the
		// part that spends money.
		mux.Handle("POST /v1/tenants/{tenant_id}/placement-preview", http.HandlerFunc(accounts.HandlePreviewPlacement))
		mux.Handle("GET /v1/tenants/{tenant_id}/workloads/{id}", http.HandlerFunc(accounts.HandleGetWorkload))
		mux.Handle("GET /v1/tenants/{tenant_id}/workloads/{id}/logs", http.HandlerFunc(accounts.HandleGetWorkloadLogs))
		mux.Handle("POST /v1/tenants/{tenant_id}/workloads/{id}/retry", http.HandlerFunc(accounts.HandleRetryWorkload))
		mux.Handle("DELETE /v1/tenants/{tenant_id}/workloads/{id}", http.HandlerFunc(accounts.HandleCancelWorkload))
		// Tenant member management rides the SAME credential and the same hook:
		// a roster is only meaningful where humans exist, so these appear and
		// disappear with /v1/account rather than on a switch of their own. Both
		// 404 when Yscale ID is unconfigured, for the same reason it does.
		mux.Handle("GET /v1/tenants/{tenant_id}/members", http.HandlerFunc(accounts.HandleListMembers))
		mux.Handle("POST /v1/tenants/{tenant_id}/members", http.HandlerFunc(accounts.HandleAddMember))
		mux.Handle("PATCH /v1/tenants/{tenant_id}/members/{account_id}", http.HandlerFunc(accounts.HandleUpdateMemberRole))
		mux.Handle("DELETE /v1/tenants/{tenant_id}/members/{account_id}", http.HandlerFunc(accounts.HandleRemoveMember))
		// A tenant's live burst usage: what it is running now against its own
		// ceilings, readable by every member because a read-only seat that cannot
		// see what the tenant is running is not a seat. Same credential and same
		// hook as the roster — it is the tenant's own state, scoped to the tenant,
		// so it appears and disappears with the rest of the human surface.
		mux.Handle("GET /v1/tenants/{tenant_id}/usage", http.HandlerFunc(accounts.HandleGetUsage))
		mux.Handle("GET /v1/tenants/{tenant_id}/hosted-capacity", http.HandlerFunc(accounts.HandleGetHostedCapacity))
		mux.Handle("POST /v1/tenants/{tenant_id}/hosted-capacity", http.HandlerFunc(accounts.HandleRequestHostedCapacity))
		// Which clusters the tenant has connected here, and therefore which ids
		// their submissions may name in X-Cluster-ID. Same credential and same
		// hook as the usage read — it is the tenant's own live state, scoped to
		// the tenant, readable by every member. Replica-local by construction:
		// the response says so, and callers must not read a missing cluster as a
		// cluster that is down.
		mux.Handle("GET /v1/tenants/{tenant_id}/clusters", http.HandlerFunc(accounts.HandleListClusters))
		// The registry writes: register a cluster and mint its connector
		// credential, rotate that credential, remove the cluster. Same
		// credential and same hook as the policy pair below — the registry is
		// the tenant's own governed state, readable by every member above and
		// writable only by its managers, which the store decides under the
		// lock it writes in. The credential appears in the register/rotate
		// response and nowhere else.
		mux.Handle("POST /v1/tenants/{tenant_id}/clusters", http.HandlerFunc(accounts.HandleRegisterCluster))
		mux.Handle("POST /v1/tenants/{tenant_id}/clusters/{cluster_id}/credential", http.HandlerFunc(accounts.HandleRotateClusterCredential))
		mux.Handle("DELETE /v1/tenants/{tenant_id}/clusters/{cluster_id}", http.HandlerFunc(accounts.HandleDeleteCluster))
		mux.Handle("GET /v1/tenants/{tenant_id}/cluster-policy", http.HandlerFunc(accounts.HandleGetClusterPolicy))
		mux.Handle("PUT /v1/tenants/{tenant_id}/cluster-policy", http.HandlerFunc(accounts.HandlePutClusterPolicy))
		// The launch templates this tenant publishes, and what a submission's
		// X-Template-ID/X-Template-Version pair is verified against. Same
		// credential and same hook as the policy pair above — it is the tenant's
		// own governed state, readable by every member because the ids are what
		// their submissions name, and writable only by the managers who own the
		// rest of the tenant's configuration.
		mux.Handle("GET /v1/tenants/{tenant_id}/templates", http.HandlerFunc(accounts.HandleGetTemplates))
		mux.Handle("PUT /v1/tenants/{tenant_id}/templates", http.HandlerFunc(accounts.HandlePutTemplates))
		mux.Handle("GET /v1/tenants/{tenant_id}/catalog-publishers", http.HandlerFunc(accounts.HandleListCatalogPublishers))
		mux.Handle("POST /v1/tenants/{tenant_id}/catalog-publishers", http.HandlerFunc(accounts.HandleCreateCatalogPublisher))
		mux.Handle("POST /v1/tenants/{tenant_id}/catalog-publishers/{publisher_id}/credential", http.HandlerFunc(accounts.HandleRotateCatalogPublisherCredential))
		mux.Handle("DELETE /v1/tenants/{tenant_id}/catalog-publishers/{publisher_id}", http.HandlerFunc(accounts.HandleDeleteCatalogPublisher))
		mux.Handle("GET /v1/automation/tenants/{tenant_id}/templates", handlers.CatalogPublisherAuth(store, http.HandlerFunc(accounts.HandleAutomationGetTemplates)))
		mux.Handle("PUT /v1/automation/tenants/{tenant_id}/templates", handlers.CatalogPublisherAuth(store, http.HandlerFunc(accounts.HandleAutomationPutTemplates)))
		mux.Handle("GET /v1/tenants/{tenant_id}/cloud-accounts/linode", http.HandlerFunc(accounts.HandleGetLinodeCloudAccount))
		mux.Handle("PUT /v1/tenants/{tenant_id}/cloud-accounts/linode", http.HandlerFunc(accounts.HandlePutLinodeCloudAccount))
		mux.Handle("DELETE /v1/tenants/{tenant_id}/cloud-accounts/linode", http.HandlerFunc(accounts.HandleDeleteLinodeCloudAccount))
		mux.Handle("GET /v1/tenants/{tenant_id}/runtime-bindings", http.HandlerFunc(accounts.HandleListRuntimeBindings))
		mux.Handle("PUT /v1/tenants/{tenant_id}/runtime-bindings/{key}", http.HandlerFunc(accounts.HandlePutRuntimeBinding))
		mux.Handle("DELETE /v1/tenants/{tenant_id}/runtime-bindings/{key}", http.HandlerFunc(accounts.HandleDeleteRuntimeBinding))
		// The Git repositories this tenant's clusters reconcile. Same credential
		// and same hook as the templates above — it is the tenant's own governed
		// configuration, readable by every member because it is what says how the
		// tenant deploys, and writable only by the managers who own the rest of
		// it. Coordinates only: no credential is stored here, so nothing on this
		// route can hand one back.
		mux.Handle("GET /v1/tenants/{tenant_id}/gitops/sources", http.HandlerFunc(accounts.HandleGetGitOpsSources))
		mux.Handle("PUT /v1/tenants/{tenant_id}/gitops/sources", http.HandlerFunc(accounts.HandlePutGitOpsSources))
		// The tenant's governance journal: who submitted what, whose access
		// changed, and what central allowed. Same credential and same hook as
		// the roster — it is the evidence behind the roster and the workload
		// list, scoped to the same tenant and restricted to the same managers,
		// so it appears and disappears with them rather than on a switch of its
		// own.
		mux.Handle("GET /v1/tenants/{tenant_id}/audit", http.HandlerFunc(accounts.HandleListAudit))

		// The browser-operator console for hosted capacity: a human's ordinary
		// Yscale ID access token plus an exact-subject allowlist — a THIRD
		// credential from the account surface above and the AdminAuth family,
		// which is why it is registered here rather than there: it rides the
		// same resolver instance (and therefore the same outbound identity
		// budget) as /v1/account. YSCALE_OPERATOR_SUBJECTS lists the humans who
		// may fulfill the pending queue; with it, the resolver or the issuer
		// unset, OperatorAuth leaves the routes absent (404), exactly as an
		// unset YSCALE_ADMIN_TOKEN disables the admin family. The handlers are
		// the admin surface's safe ones: the queue read is a pure copy-out,
		// and the assignment attributes its audit row to the signed-in human.
		operatorAuth := handlers.OperatorAuth{
			Store:    store,
			Resolver: accounts.Resolver,
			Issuer:   accounts.Issuer,
			Subjects: handlers.ParseOperatorSubjects(os.Getenv("YSCALE_OPERATOR_SUBJECTS")),
			Log:      log,
		}
		// Reconciler for the same reason the account surface holds one: the
		// hosted DELETE below withdraws that cluster's route intent, and this
		// is what converges the coordination server to it.
		hosted := &handlers.Tenants{Store: store, Log: log, Endpoint: centralEndpoint(), Reconciler: rec, Commands: store}
		mux.Handle("GET /v1/operator/tenants",
			operatorAuth.Wrap(http.HandlerFunc(hosted.HandleListOperatorTenants)))
		mux.Handle("PATCH /v1/operator/tenants/{tenant_id}/limits",
			operatorAuth.Wrap(http.HandlerFunc(hosted.HandlePatchTenantLimits)))
		mux.Handle("GET /v1/operator/hosted-capacity/requests",
			operatorAuth.Wrap(http.HandlerFunc(hosted.HandleListHostedCapacityRequests)))
		// The standing inventory beside the queue: what the shared cluster is
		// already carrying. Operator-only and read-only — a new console read,
		// so the static /v1/admin family stays exactly the routes it had.
		mux.Handle("GET /v1/operator/hosted-clusters",
			operatorAuth.Wrap(http.HandlerFunc(hosted.HandleListHostedClusters)))
		mux.Handle("POST /v1/operator/tenants/{tenant_id}/hosted-clusters",
			operatorAuth.Wrap(http.HandlerFunc(hosted.HandleAssignHostedCluster)))
		// The rest of the lifecycle on the same credential: the SAME handlers
		// the admin family runs, so validation, the one-time credential
		// reveal, token eviction, cluster-removal reconciliation and every
		// error status are one contract on both surfaces. What differs is only
		// the audit actor, which operatorLifecycleActor takes from the account id
		// OperatorAuth verified into the context.
		mux.Handle("POST /v1/operator/tenants/{tenant_id}/hosted-clusters/{cluster_id}/credential",
			operatorAuth.Wrap(http.HandlerFunc(hosted.HandleRotateHostedClusterCredential)))
		mux.Handle("DELETE /v1/operator/tenants/{tenant_id}/hosted-clusters/{cluster_id}",
			operatorAuth.Wrap(http.HandlerFunc(hosted.HandleDeleteHostedCluster)))
		// The connector-command recovery pair the admin family also carries,
		// on the SAME handlers: an operator working a stalled tenant from the
		// browser reaches the identical read and the identical single-command
		// requeue, with identical statuses. What differs is only the audit
		// actor, which operatorLifecycleActor takes from the account id
		// OperatorAuth verified into the context — so a browser requeue names
		// the human who asked for it rather than the shared static token.
		mux.Handle("GET /v1/operator/tenants/{tenant_id}/connector-commands",
			operatorAuth.Wrap(http.HandlerFunc(hosted.HandleListTenantConnectorCommands)))
		mux.Handle("POST /v1/operator/tenants/{tenant_id}/connector-commands/{id}/requeue",
			operatorAuth.Wrap(http.HandlerFunc(hosted.HandleRequeueTenantConnectorCommand)))
	}
}

// fabricPoller tracks fabrics started through the tenant lifecycle so the
// workload path can return a typed, retryable 503 while a tenant onboards, and
// attaches the tenant's mesh once its fabric is ready. The attached
// MeshEndpoint carries no credential: Provider is state.MeshProviderFactory and
// every mesh operation is routed through the factory. Start re-discovers
// tenants that still have no mesh, so a fabric that became ready while central
// was down is attached after a restart.
type fabricPoller struct {
	store   *state.Store
	factory factoryclient.FabricService
	// attach is the durable attach-time reconcile (meshpolicy ReconcileAttach:
	// policy plus replay of withdrawals a cluster delete left durable).
	attach   func(ctx context.Context, customerID string) error
	log      *slog.Logger
	interval time.Duration

	mu       sync.RWMutex
	inFlight map[string]fabricTrack
}

// fabricTrack distinguishes a tenant whose fabric was just requested (it may
// not exist at the factory for a moment, so "absent" is retried for
// fabricAbsentGrace) from one discovered at startup (absent means no fabric).
type fabricTrack struct {
	since     time.Time
	requested bool
}

const fabricAbsentGrace = 2 * time.Minute

func newFabricPoller(store *state.Store, factory factoryclient.FabricService, log *slog.Logger, interval time.Duration) *fabricPoller {
	return &fabricPoller{
		store: store, factory: factory, log: log, interval: interval,
		inFlight: make(map[string]fabricTrack),
	}
}

// Track marks a tenant whose fabric has been requested. It is safe to call on
// provisioning retries and returns immediately with the signup response.
func (p *fabricPoller) Track(customerID string) {
	p.mu.Lock()
	p.inFlight[customerID] = fabricTrack{since: time.Now(), requested: true}
	p.mu.Unlock()
}

// discover tracks a tenant found without a mesh at startup. It never
// downgrades a tenant whose fabric was explicitly requested.
func (p *fabricPoller) discover(customerID string) {
	p.mu.Lock()
	if _, ok := p.inFlight[customerID]; !ok {
		p.inFlight[customerID] = fabricTrack{since: time.Now()}
	}
	p.mu.Unlock()
}

func (p *fabricPoller) tracked(customerID string) (fabricTrack, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	t, ok := p.inFlight[customerID]
	return t, ok
}

// Forget stops tracking a tenant after offboard or a confirmed absent fabric.
func (p *fabricPoller) Forget(customerID string) {
	p.mu.Lock()
	delete(p.inFlight, customerID)
	p.mu.Unlock()
}

// Provisioning reports whether a tenant's fabric has not reached ready yet.
func (p *fabricPoller) Provisioning(customerID string) bool {
	p.mu.RLock()
	_, ok := p.inFlight[customerID]
	p.mu.RUnlock()
	return ok
}

func (p *fabricPoller) Start(ctx context.Context) {
	// The in-flight set is in memory; re-track every tenant without a mesh.
	// Tenants with no fabric resolve to absent on the first poll and drop out.
	for _, customerID := range p.store.SharedTailnetCustomerIDs() {
		p.discover(customerID)
	}
	go func() {
		p.poll(ctx)
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.poll(ctx)
			}
		}
	}()
}

// pollConcurrency bounds how many tenant fabric checks run at once so one slow
// factory response can't stall the whole tick.
const pollConcurrency = 4

func (p *fabricPoller) poll(ctx context.Context) {
	p.mu.RLock()
	ids := make([]string, 0, len(p.inFlight))
	for customerID := range p.inFlight {
		ids = append(ids, customerID)
	}
	p.mu.RUnlock()

	sem := make(chan struct{}, pollConcurrency)
	var wg sync.WaitGroup
	for _, customerID := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func(customerID string) {
			defer wg.Done()
			defer func() { <-sem }()
			p.check(ctx, customerID)
		}(customerID)
	}
	wg.Wait()
}

// check resolves one tenant's fabric readiness. A ready fabric is attached as
// a keyless factory MeshEndpoint and stops being tracked; an absent one just
// stops being tracked.
func (p *fabricPoller) check(ctx context.Context, customerID string) {
	cust, err := p.store.CustomerByID(customerID)
	if err != nil {
		p.Forget(customerID) // customer gone (offboarded) — stop tracking
		return
	}
	if cust.Mesh != nil {
		p.Forget(customerID) // mesh attached (env path or factory-routed) — no longer onboarding
		return
	}
	fabric, err := p.factory.GetFabric(ctx, customerID)
	if errors.Is(err, factoryclient.ErrFabricAbsent) {
		// A just-requested fabric can be absent until the factory stages it;
		// dropping it here would leave the tenant unattached until restart.
		if t, ok := p.tracked(customerID); ok && t.requested && time.Since(t.since) < fabricAbsentGrace {
			return
		}
		p.Forget(customerID)
		return
	}
	if errors.Is(err, factoryclient.ErrFabricProvisioning) {
		return // still onboarding — keep the typed 503 signal live
	}
	if err != nil {
		p.log.Warn("poll tenant fabric", "customer", customerID, "error", err)
		return
	}
	if fabric.Status != "ready" {
		return
	}
	if !strings.HasPrefix(fabric.LoginServer, "https://") || fabric.User == "" {
		p.log.Warn("tenant fabric ready without a usable mesh endpoint; not attaching", "customer", customerID)
		return
	}
	ep := &state.MeshEndpoint{
		Provider:    state.MeshProviderFactory,
		LoginServer: fabric.LoginServer,
		User:        fabric.User,
		BackendID:   fabric.BackendID,
	}
	attached, err := p.store.AttachMeshIfUnset(customerID, ep)
	if err != nil {
		p.log.Warn("attach tenant fabric", "customer", customerID, "error", err)
		return // still tracked: the next poll retries the attach
	}
	p.Forget(customerID)
	if !attached {
		p.log.Info("tenant fabric ready but not attached: tenant already has a mesh or is revoked", "customer", customerID)
		return
	}
	p.log.Info("tenant fabric ready; mesh attached", "customer", customerID, "login_server", fabric.LoginServer, "backend_id", fabric.BackendID)
	p.reconcileAttached(ctx, customerID)
}

// reconcileAttached runs the durable attach-time reconcile for one tenant. A
// failure is logged; the tenant stays attached and the policy worker retries.
func (p *fabricPoller) reconcileAttached(ctx context.Context, customerID string) {
	if p.attach == nil {
		return
	}
	attachCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := p.attach(attachCtx, customerID); err != nil {
		p.log.Error("factory tenant attach reconcile failed; tenant remains attached, the worker retries",
			"customer", customerID, "error", err)
	}
}

// replayAttached re-runs the attach-time reconcile for every tenant already
// attached to a factory box. A restart otherwise drops durable gateway
// withdrawals whose deleted connector will never reconnect to trigger them.
func (p *fabricPoller) replayAttached(ctx context.Context) {
	for _, customerID := range p.store.FactoryMeshCustomerIDs() {
		if ctx.Err() != nil {
			return
		}
		p.reconcileAttached(ctx, customerID)
	}
}

// attachHeadscaleFromEnv reads <prefix>HOSTNAME / <prefix>API_KEY / <prefix>USER
// (prefix "" => the original HEADSCALE_*; "HEADSCALE2_" => the 2nd tenant's box)
// and, when all three are present, validates the box (RegisterHeadscale) and
// attaches it to customerID via SetCustomerMesh. Absent env is a clean no-op
// (customer stays on shared Tailscale SaaS). A bad box logs loudly but does not
// crash central. This is the dev/env-driven twin of the production onboarding
// flow's RegisterHeadscale -> SetCustomerMesh pair.
func attachHeadscaleFromEnv(store *state.Store, log *slog.Logger, reconciler *meshpolicy.Reconciler, customerID, prefix string) {
	base := prefix
	if base == "" {
		base = "HEADSCALE_"
	}
	hsHost := os.Getenv(base + "HOSTNAME")
	if hsHost == "" {
		hsHost = os.Getenv(base + "LOGIN_SERVER") // tolerated: may carry https:// scheme
	}
	apiKey := os.Getenv(base + "API_KEY")
	user := os.Getenv(base + "USER")
	if hsHost == "" || apiKey == "" || user == "" {
		return
	}
	regCtx, regCancel := context.WithTimeout(context.Background(), 20*time.Second)
	loginServer, key, usr, backendID, rerr := client.RegisterHeadscale(regCtx, client.BoxInfo{
		Hostname:  hsHost,
		APIKey:    apiKey,
		User:      user,
		BackendID: os.Getenv(base + "BACKEND_ID"),
	})
	regCancel()
	if rerr != nil {
		log.Error("headscale factory: register failed; customer stays on shared Tailscale SaaS",
			"customer", customerID, "hostname", hsHost, "error", rerr)
		return
	}
	if serr := store.SetCustomerMesh(customerID, &state.MeshEndpoint{
		Provider:    "headscale",
		LoginServer: loginServer,
		APIKey:      key,
		User:        usr,
		BackendID:   backendID,
	}); serr != nil {
		log.Error("headscale factory: SetCustomerMesh failed", "customer", customerID, "error", serr)
		return
	}
	// Reconcile reads the customer's STORED gateway routes (carried over across
	// restart), so boot honors whatever the agent last reported; a pre-first-report
	// customer gets the empty-gateway baseline (tagOwners + unconditional burst
	// pool, no gateway CIDRs). It re-asserts regardless of the push gate.
	//
	// No agent has connected yet, so central holds no cluster identity and the
	// tenant's gateway pod may not exist. Its absence is not an attach failure:
	// a live gateway is converged by the connect-time force reconcile, which
	// carries the cluster id that actually connected.
	//
	// The DELETED ones are the exception, and the reason this is not policy-only.
	// A cluster delete leaves a durable withdrawal tombstone whose only driver
	// is the in-process worker, so a restart between the two loses it — and no
	// reconnect will ever bring it back, because the connector is what was
	// deleted. ReconcileAttach replays those against the exact gateways they
	// name.
	policyCtx, policyCancel := context.WithTimeout(context.Background(), 20*time.Second)
	perr := reconciler.ReconcileAttach(policyCtx, customerID)
	policyCancel()
	if perr != nil {
		log.Error("headscale factory: attach reconcile failed; customer remains attached, the worker retries",
			"customer", customerID, "login_server", loginServer, "user", usr, "error", perr)
	}
	log.Info("headscale factory: customer attached to per-customer Headscale box",
		"customer", customerID, "login_server", loginServer, "user", usr)
}
