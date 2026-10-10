// Command yscale-cloud is the central server (api.yscale.sh).
//
// It accepts workload submissions from customers via REST, holds all
// backend credentials, decides where to run each workload, and pushes
// commands to the customer's Yscale Cluster Connector over a long-lived WebSocket.
//
// The Connector (compatibility binary: yscale-agent) stores the customer's
// YSCALE_TOKEN but no cloud-provider credentials. All scheduling, backend
// selection, cost, and quota logic lives here.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/credentialcipher"
	"github.com/yscale-sh/yscale/central/internal/decider"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/pricing"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/broker"
	"github.com/yscale-sh/yscale/pkg/logger"
)

// validateMeshConfig is installed by the full managed-mesh build. The no-op
// remains only for the historical split-edition export tooling.
var validateMeshConfig = func() error { return nil }

// newPolicyReconciler builds the per-customer coordination-policy reconciler.
// OSS default: NoopReconciler — the Tailscale ACL is operator-managed, so
// there is no per-customer policy to reconcile. The enterprise build overrides
// this hook in its own file's init().
var newPolicyReconciler = func(store *state.Store, log *slog.Logger) handlers.PolicyReconciler {
	return handlers.NoopReconciler{}
}

// attachMeshBoxes wires customers' self-hosted coordination boxes into the
// decider and agent-auth handler. OSS default: no-op — every customer uses the
// shared Tailscale SaaS mesh. The enterprise build overrides this hook in its
// own file's init().
var attachMeshBoxes = func(store *state.Store, log *slog.Logger, rec handlers.PolicyReconciler, dec *decider.Decider, aa *handlers.AgentAuth, stream *handlers.AgentStream) {
}

// backgroundWorkersContext is replaced with main's signal context before any
// enterprise wiring starts background work. Its default keeps hook unit tests
// independent of process signal setup.
var backgroundWorkersContext = context.Background()

// registerAdminRoutes wires the operator-only multi-tenant admin endpoints
// (/v1/admin/tenants). OSS default: no-op — multi-tenancy is an enterprise
// feature. The enterprise build overrides this hook in its own file's init().
//
// rec is the same coordination-policy reconciler the agent stream drives. The
// cluster lifecycle needs it for the one event no agent can report: a delete,
// after which the connector is gone and only these handlers know the tenant's
// routes have to be withdrawn.
var registerAdminRoutes = func(mux *http.ServeMux, store *state.Store, wls *handlers.Workloads, rec handlers.PolicyReconciler, log *slog.Logger) {
}

// registerAccountRoutes wires the SaaS human-account endpoints (/v1/account),
// authenticated by a Yscale ID access token rather than a cluster token. OSS
// default: no-op — a self-hosted central has one operator and no SaaS identity
// provider. The enterprise build overrides this hook in its own file's init().
// rec is the reconciler, for the reason registerAdminRoutes takes one.
var registerAccountRoutes = func(mux *http.ServeMux, store *state.Store, wls *handlers.Workloads, rec handlers.PolicyReconciler, log *slog.Logger) {
}

// registerControlPlaneRoutes initializes the managed-mesh dependencies before
// handlers capture them. Registering tenant/account routes first would leave
// their Factory and poller callbacks nil for the lifetime of the server.
func registerControlPlaneRoutes(mux *http.ServeMux, store *state.Store, wls *handlers.Workloads, rec handlers.PolicyReconciler, dec *decider.Decider, aa *handlers.AgentAuth, stream *handlers.AgentStream, log *slog.Logger) {
	attachMeshBoxes(store, log, rec, dec, aa, stream)
	registerAdminRoutes(mux, store, wls, rec, log)
	registerAccountRoutes(mux, store, wls, rec, log)
}

// envDuration parses a duration from env (e.g. "24h", "2m"). Returns def when
// the var is unset or unparseable, so a typo degrades to the default rather
// than crashing the server.
func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration configuration; using default",
			"component", "configuration",
			"operation", "parse_duration",
			"setting", key,
			"value", v,
			"default", def.String(),
		)
		return def
	}
	return d
}

// lifecycleAdmissionFromEnv reads the authoritative-admission gate. It is an
// explicit opt-in: turning durable admission on changes what a create refuses,
// so an operator has to ask for it rather than get it by upgrading.
//
// Enabling it REQUIRES the lifecycle store this process already opened from
// LIFECYCLE_DATABASE_URL, and there is deliberately no fallback to DATABASE_URL.
// Two reasons, both structural. A fallback would open a SECOND pool against the
// same database, so the two halves of one saga — admission and provider delete —
// would run on different connections, different pool limits, and a runtime role
// verified twice with no guarantee it was the same role. And DATABASE_URL is the
// state store's credential, which is not the least-privilege lifecycle runtime
// role that OpenStore/AssertRuntimeSchema exist to enforce: accepting it would
// silently run the authoritative machine as whatever can write central's state.
//
// storeOpen reports whether that verified store exists. Enabling admission
// without it is a configuration error, not a downgrade to the inline path: an
// operator who asked for durable intent must not be given a create that has none.
func lifecycleAdmissionFromEnv(storeOpen bool) (bool, error) {
	switch os.Getenv("LIFECYCLE_AUTHORITATIVE_ADMISSION") {
	case "", "false":
		return false, nil
	case "true":
	default:
		return false, fmt.Errorf("LIFECYCLE_AUTHORITATIVE_ADMISSION must be true or false")
	}
	if !storeOpen {
		return false, fmt.Errorf("LIFECYCLE_AUTHORITATIVE_ADMISSION=true requires LIFECYCLE_DATABASE_URL " +
			"(the migrated, least-privilege lifecycle database); DATABASE_URL is not a substitute")
	}
	return true, nil
}

// placementTokenSignerFromEnv builds the signer behind the placement preview's
// launch credential from YSCALE_PLACEMENT_TOKEN_KEY.
//
// The key is deliberately an OPERATOR-supplied secret rather than one this
// process generates. A generated key would be per-replica and per-restart, so
// every rollout would invalidate the previews customers are holding and a
// two-replica deployment would refuse half of them at random — which reads to a
// customer as an intermittently broken product and trains them to retry until a
// launch sticks.
//
// It is also its OWN key, not YSCALE_ADMIN_TOKEN or a provider credential. Those
// are bearer credentials for other trust boundaries; reusing one here would make
// a placement token forgeable by anyone who holds it and would make rotating
// either of them silently invalidate the other's purpose.
//
// Unset returns (nil, nil): preview binding is off, which the caller reports and
// fails closed on. A key that is SET but too weak is a hard configuration error
// — an operator who asked for authenticated previews must not be given
// unauthenticated ones.
func placementTokenSignerFromEnv() (*state.PlacementTokenSigner, error) {
	secret := strings.TrimSpace(os.Getenv("YSCALE_PLACEMENT_TOKEN_KEY"))
	if secret == "" {
		return nil, nil
	}
	return state.NewPlacementTokenSigner(secret)
}

// trackedBackendIDs snapshots the provider IDs of every live burst — the set an
// orphan sweep treats as "not a leak". Rebuilt per sweep, never cached: a stale
// snapshot is what turns a running burst into a delete candidate.
//
// It reads DURABLE state, not this process's in-memory map, because the sweep
// destroys VMs. The map holds only bursts this process created, so another
// central's live burst would look untracked and be destroyed along with the
// customer workload running on it. Durable state is the only view that spans
// replicas.
//
// Returns ok=false when the read failed. The caller MUST NOT sweep on false:
// a partial or empty tracked set reads as "every VM is an orphan", which is
// the one mistake that costs a customer their job rather than costing money.
// providerDeleteTracker is the nonterminal half of the tracked set: resources a
// delete operation already owns. Satisfied by *lifecycle.Store.
type providerDeleteTracker interface {
	NonterminalProviderResourceIDs(ctx context.Context) (map[string]bool, error)
}

func trackedBackendIDs(ctx context.Context, store *state.Store, deletes providerDeleteTracker, log *slog.Logger) (map[string]bool, bool) {
	durable, haveDurable, err := store.DurableBurstBackendIDs(ctx)
	if err != nil {
		log.Error("orphan sweep skipped: could not read durable burst set; "+
			"sweeping on a partial set would destroy live bursts", "error", err)
		return nil, false
	}
	tracked := durable
	if !haveDurable {
		// No durable backend: the in-memory store IS the truth, because it is the
		// only process there.
		tracked = make(map[string]bool)
		for _, b := range store.ListBursts() {
			if b.BackendID != "" {
				tracked[b.BackendID] = true
			}
		}
	}
	if deletes == nil {
		return tracked, true
	}
	// A burst whose provider delete is queued, leased, retrying or waiting on an
	// operator is NOT a leak — it is a resource the delete worker owns and may
	// be inside a provider call for right now. Its live burst row is retired the
	// moment that delete terminalizes, so without this union the sweep and the
	// worker race for the same VM, and the sweep is the one that destroys
	// without recording anything.
	//
	// Same fail-closed rule as the read above, for a stronger reason: the rows
	// that could not be read are exactly the ones mid-teardown.
	inFlight, err := deletes.NonterminalProviderResourceIDs(ctx)
	if err != nil {
		log.Error("orphan sweep skipped: could not read in-flight provider deletes; "+
			"sweeping without them would race the delete worker", "error", err)
		return nil, false
	}
	if tracked == nil {
		tracked = make(map[string]bool, len(inFlight))
	}
	for id := range inFlight {
		tracked[id] = true
	}
	return tracked, true
}

func main() {
	var (
		listenAddr string
	)
	flag.StringVar(&listenAddr, "listen", ":8443", "address to listen on")
	flag.Parse()

	log, closeLog := logger.New(logger.Options{Job: "yscale-cloud"})
	slog.SetDefault(log)
	if err := validateMeshConfig(); err != nil {
		log.Error("invalid managed-mesh configuration", "error", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = closeLog(ctx)
	}()

	// DATABASE_URL enables durable state in Postgres: customers, in-flight
	// workloads, bursts, and volumes survive a restart so central never strands
	// a running burst or forgets a customer. Unset => in-memory only (tests /
	// OSS-local). (Replaces the former YSCALE_STATE_FILE JSON snapshot.)
	var store *state.Store
	devCredSource := "configured out of band"
	stateDSN := os.Getenv("DATABASE_URL")
	if dsn := stateDSN; dsn != "" {
		var customerCipher state.CustomerCredentialCipher
		if rawKey := strings.TrimSpace(os.Getenv("YSCALE_CREDENTIAL_MASTER_KEY")); rawKey != "" {
			cipher, cipherErr := credentialcipher.New(rawKey)
			if cipherErr != nil {
				log.Error("init customer credential encryption", "error", cipherErr)
				os.Exit(1)
			}
			customerCipher = cipher
		}
		dbCtx, dbCancel := context.WithTimeout(context.Background(), 15*time.Second)
		s, perr := state.NewPostgresWithCredentialCipher(dbCtx, dsn, customerCipher)
		dbCancel()
		if perr != nil {
			log.Error("init postgres state store", "error", perr)
			os.Exit(1)
		}
		store = s
		defer store.Close()
		log.Info("state: postgres backend active")
	} else {
		// In-memory dev store. The single implicit OSS tenant's bearer token is
		// taken from YSCALE_TOKEN when set (so an operator can point the agent
		// and CLI at this central); otherwise it is randomized per boot so an
		// exposed central ships no known-token backdoor. The token value is
		// never written to logs.
		seedToken := os.Getenv("YSCALE_TOKEN")
		if seedToken != "" {
			devCredSource = "YSCALE_TOKEN (env)"
		} else {
			seedToken = state.NewDevToken()
			devCredSource = "generated per boot (set YSCALE_TOKEN to choose one)"
		}
		store = state.NewWithSeedToken(seedToken)
		log.Warn("state: in-memory only (DATABASE_URL unset) — state is lost on restart")
	}

	// Confirm the dev seed exists without exposing its bearer credential. A log
	// store is not a credential-recovery channel.
	if c, err := store.CustomerByID(state.DevCustomerID); err == nil {
		log.Info("dev customer active (local dev only)",
			"customer", c.ID,
			"credential_source", devCredSource,
			"hint", "configure YSCALE_TOKEN out of band for the Cluster Connector and CLI",
		)
	}

	// LIFECYCLE_DATABASE_URL turns on the authoritative provider-delete state
	// machine: a reap records durable delete intent and a worker performs the
	// provider call, the cleanup and the receipts. This is a separate
	// durability boundary with a migration/runtime split — this process
	// VERIFIES the migrated schema and holds no DDL, so a runtime role cannot
	// reshape the record that decides whether a paid resource still exists.
	//
	// Unset keeps the pre-existing inline/queue teardown path exactly as it is
	// (the OSS/dev compatibility seam).
	var lifecycleStore *lifecycle.Store
	if lifecycleDSN := os.Getenv("LIFECYCLE_DATABASE_URL"); lifecycleDSN != "" {
		lifecycleCtx, lifecycleCancel := context.WithTimeout(context.Background(), 15*time.Second)
		openedLifecycle, lifecycleErr := lifecycle.OpenStore(lifecycleCtx, lifecycleDSN)
		lifecycleCancel()
		if lifecycleErr != nil {
			log.Error("init lifecycle store", "error", lifecycleErr)
			os.Exit(1)
		}
		lifecycleStore = openedLifecycle
		defer lifecycleStore.Close()
		log.Info("lifecycle: authoritative provider-delete state machine active")
	} else {
		log.Info("lifecycle: disabled (LIFECYCLE_DATABASE_URL unset) — teardown runs on the inline/queue path")
	}
	// Buffered depth 1 and non-blocking on send: this is a wake-up cache in
	// front of the durable queue, never a work channel. A dropped nudge costs
	// one poll interval.
	deleteWake := make(chan struct{}, 1)

	// REDIS_URL enables the durable teardown queue: reaps hand cloud teardown to
	// the broker so retries survive a central restart and can run from a
	// separate worker (the OSS/enterprise decoupling seam). Unset => teardown
	// runs inline in-process (single-binary / OSS-local).
	var teardownBroker broker.Broker
	var teardownRedis *broker.Redis
	if redisURL := os.Getenv("REDIS_URL"); redisURL != "" {
		rbCtx, rbCancel := context.WithTimeout(context.Background(), 15*time.Second)
		rb, rerr := broker.NewRedis(rbCtx, redisURL, 30*time.Second)
		rbCancel()
		if rerr != nil {
			log.Error("init redis broker", "error", rerr)
			os.Exit(1)
		}
		defer rb.Close()
		teardownBroker = rb
		teardownRedis = rb
		log.Info("broker: redis streams active (durable teardown queue)")
	} else {
		log.Info("broker: none (teardown runs inline in-process)")
	}

	// Cost meter: accrues yscale's upstream burst spend (tagged by backend)
	// and serves it at /metrics for the Grafana cost dashboard. Initialised
	// before the decider so we can wire it in via WithCost for reliability
	// metrics (mesh mint counts, pod-CIDR pool free, reap totals).
	costMeter := cost.NewMeter()
	costMeter.SetFixedMonthly(pricing.FixedMonthlyUSD)
	if teardownRedis != nil {
		costMeter.SetTeardownStreamSource(teardownRedis, handlers.TeardownStream, handlers.TeardownGroup)
	}

	// Wire the meter into the store so agent gauges and every state
	// write-through failure share the existing /metrics scrape.
	store.Cost = costMeter

	// The meter derives the live provisioning series from a composite source
	// that combines state's authoritative burst/workload view with lifecycle's
	// durable provider-create operations. Lifecycle records cover the paid
	// interval before state books a burst (processing) and after a create
	// succeeds but before the booking lands (succeeded). State/workload truth
	// decides for any burst that state already knows about.
	//
	// When lifecycle is disabled (OSS), the composite falls through to state
	// alone — the in-memory map is the burst set.
	{
		var lcSource cost.LifecycleSource
		if lifecycleStore != nil {
			lcSource = &lifecycleProvisioningAdapter{store: lifecycleStore}
		}
		composite := cost.NewCompositeSource(store, lcSource)
		costMeter.SetProvisioningSource(composite)
	}

	dec := decider.New(decider.Config{
		FlyToken:            os.Getenv("FLYIO_TOKEN"),
		FlyOrg:              os.Getenv("FLY_ORG"),
		FlyRegion:           os.Getenv("FLY_REGION"),
		LinodeToken:         os.Getenv("LINODE_TOKEN"),
		LinodeRegion:        os.Getenv("LINODE_REGION"),
		LinodeSSHKey:        os.Getenv("LINODE_SSH_KEY"),
		AWSAccessKeyID:      os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey:  os.Getenv("AWS_SECRET_ACCESS_KEY"),
		AWSSessionToken:     os.Getenv("AWS_SESSION_TOKEN"),
		AWSRegion:           os.Getenv("AWS_REGION"),
		GCPCredentialsFile:  os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
		GCPProjectID:        os.Getenv("GCP_PROJECT_ID"),
		GCPZone:             os.Getenv("GCP_ZONE"),
		GCPRegion:           os.Getenv("GCP_REGION"),
		AzureTenantID:       os.Getenv("AZURE_TENANT_ID"),
		AzureClientID:       os.Getenv("AZURE_CLIENT_ID"),
		AzureClientSecret:   os.Getenv("AZURE_CLIENT_SECRET"),
		AzureSubscriptionID: os.Getenv("AZURE_SUBSCRIPTION_ID"),
		AzureLocation:       os.Getenv("AZURE_LOCATION"),
		AzureResourceGroup:  os.Getenv("AZURE_RESOURCE_GROUP"),
		TSOAuthClientID:     os.Getenv("TS_OAUTH_CLIENT_ID"),
		TSOAuthClientSecret: os.Getenv("TS_OAUTH_CLIENT_SECRET"),
		TSTailnet:           os.Getenv("TS_TAILNET"),
		// Per-customer self-hosted coordination endpoints are NOT global
		// env: they live on Customer.Mesh in state (login-server URL, API
		// key, user). The decider/agent-auth handler select Tailscale SaaS
		// (default) vs a customer's own box at request time via the
		// mesh.Provider seam, wired by attachMeshBoxes.
	}).WithStore(store).WithCost(costMeter)

	// Authoritative admission. When enabled, the create path commits workload
	// intent, the requested burst and a leased provider-create operation to
	// PostgreSQL in one transaction BEFORE the first paid provider call, and no
	// create happens if that transaction cannot commit. Unset leaves the
	// inline OSS/dev path exactly as it was — see decider.WithLifecycleAdmission.
	//
	// It REUSES the lifecycle store opened above rather than opening its own.
	// Create and delete are two halves of one lifecycle aggregate — admission
	// writes the burst row that the authoritative delete later projects and holds
	// itself to — and a second pool would be a second connection budget and a
	// second, independently verified runtime identity for rows that must agree.
	// The schema/role verification that store already passed is the one this path
	// depends on; it never runs DDL of its own.
	lifecycleEnabled, lifecycleConfigErr := lifecycleAdmissionFromEnv(lifecycleStore != nil)
	if lifecycleConfigErr != nil {
		log.Error("invalid lifecycle admission configuration", "error", lifecycleConfigErr)
		os.Exit(1)
	}
	if lifecycleEnabled {
		dec = dec.WithLifecycleAdmission(lifecycleStore)
		log.Info("admission: postgres authoritative (durable intent commits before any provider create)")
	} else {
		log.Warn("admission: inline only (LIFECYCLE_AUTHORITATIVE_ADMISSION unset) — a crash between a provider create and the burst record can strand a node")
	}
	if lifecycleStore != nil {
		dec = dec.WithLifecycleReconciliation(lifecycleStore)
	}

	// Coordination-policy reconciler. OSS: a no-op (operator-managed
	// coordination-server ACL); the managed platform substitutes a reconciler
	// that builds and applies the coordination-server tag/route policy.
	reconciler := newPolicyReconciler(store, log)

	// Read ONCE and used twice: as the admission deadline for an unobservable
	// nodeOnly submission, and as the observation-silence ceiling the reaper
	// watchdog enforces below. Two reads would let the two halves of the nodeOnly
	// policy drift apart under an operator who thought they were setting one thing.
	nodeOnlyMaxLifetime := envDuration("YSCALE_NODE_ONLY_MAX_LIFETIME", 6*time.Hour)

	// The key a placement preview's launch credential is signed with. Read
	// BEFORE the handler is built and fatal on a weak one, for the same reason
	// the lifecycle configuration above is: a deployment that asked to
	// authenticate previews and cannot must not start and serve them unverified.
	placementTokens, placementTokenErr := placementTokenSignerFromEnv()
	if placementTokenErr != nil {
		// The error names the key's length and shape, never its value.
		log.Error("invalid placement launch token signing key", "error", placementTokenErr)
		os.Exit(1)
	}
	if placementTokens != nil {
		log.Info("placement previews: launch binding enabled (a preview hands out a signed launch token; a launch presents it back)")
	} else {
		log.Warn("placement previews: launch binding disabled (YSCALE_PLACEMENT_TOKEN_KEY unset) — previews still answer the decision but hand out no launch token, and a launch presenting one is refused rather than honored unverified")
	}

	// Admission reservations live in the primary DATABASE_URL database alongside
	// customers and bursts. When DATABASE_URL is set, the state store's Postgres
	// backend handles durable, cross-replica admission atomically. No separate
	// pool or schema is needed.
	if stateDSN != "" {
		log.Info("admission reservations: postgres durable via state store (cross-replica coordination)")
	}

	// Reconciliation interval is read ONCE and used two ways: as the periodic
	// ticker's period and in the startup log line below. Two reads would let
	// them diverge under an operator who thought they were setting one thing.
	reconcileInterval := effectiveReconcileInterval()

	wls := &handlers.Workloads{
		Store:               store,
		Commands:            store,
		Decider:             dec,
		Reaper:              dec,
		Log:                 log,
		Cost:                costMeter,
		Teardowns:           teardownBroker,
		NodeOnlyMaxLifetime: nodeOnlyMaxLifetime,
		PlacementTokens:     placementTokens,
	}
	// Assigned after construction, not in the literal: a typed-nil
	// *lifecycle.Store in an interface field is non-nil, and that would put the
	// handler in lifecycle mode with nothing behind it.
	if lifecycleStore != nil {
		wls.Deletes = lifecycleStore
		wls.DeleteWake = deleteWake
		wls.Evidence = &burstEvidenceComposite{lifecycle: lifecycleStore, state: store}
	}
	// Constructed AFTER the reconciler: constructing the stream is what arms the
	// startup route reap, and the withdrawals a reap enqueues have nowhere to go
	// before the reconciler behind them exists.
	//
	// The lifecycle seam is the workloads handler itself, so a connector
	// reporting that a burst node disappeared reaches the SAME claim, durable
	// teardown queue and cost freeze that /complete and the watchdog go through.
	// Anything else would be a second reap path to keep in step with this one.
	stream := handlers.NewAgentStream(store, log, reconciler,
		handlers.WithBurstLifecycle(wls), handlers.WithConnectorCommandLedger(store))
	gpuPrices := &handlers.GPUPrices{Log: log}
	agentAuth := &handlers.AgentAuth{Store: store, TS: dec.TS(), Log: log, Cost: costMeter}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	backgroundWorkersContext = ctx

	mux := http.NewServeMux()
	// Submit takes ConnectorSubmitAuth: a cluster-scoped connector credential
	// submits without naming a cluster, because its binding IS the cluster and
	// is injected as X-Cluster-ID before Create routes. The legacy tenant token
	// keeps its existing header-and-policy routing.
	mux.Handle("POST /v1/workloads",
		handlers.ConnectorSubmitAuth(store, http.HandlerFunc(wls.Create)))
	mux.Handle("GET /v1/workloads/{id}",
		handlers.ConnectorWorkloadAuth(store, http.HandlerFunc(wls.Get)))
	// The routes a connector acts on one of its own existing workloads through:
	// status read, the two lifecycle callbacks and cancel. They take
	// ConnectorWorkloadAuth,
	// which accepts cluster-scoped connector credentials but holds each one to
	// the workloads its own cluster is running — a sibling cluster's workload is
	// the same 404 as one that does not exist.
	mux.Handle("POST /v1/workloads/{id}/complete",
		handlers.ConnectorWorkloadAuth(store, http.HandlerFunc(wls.Complete)))
	mux.Handle("POST /v1/workloads/{id}/started",
		handlers.ConnectorWorkloadAuth(store, http.HandlerFunc(wls.Started)))
	mux.Handle("DELETE /v1/workloads/{id}",
		handlers.ConnectorWorkloadAuth(store, http.HandlerFunc(wls.Cancel)))
	// The two agent routes take ConnectorAuth, which also accepts
	// cluster-scoped connector credentials. Everything outside these and the
	// workload routes above stays on Auth, so a connector credential can never
	// read prices or tenant-wide spend, or touch bursts.
	mux.Handle("GET /v1/agent/stream",
		handlers.ConnectorAuth(store, stream))
	// Read-only credential proof for release tooling. ConnectorAuth performs
	// the complete tenant/cluster credential validation; only that authenticated
	// path can produce 204, so a proxy's generic 404 can never look valid.
	mux.Handle("GET /v1/agent/auth-check",
		handlers.ConnectorAuth(store, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})))
	mux.Handle("POST /v1/agent/ts-auth-key",
		handlers.ConnectorAuth(store, http.HandlerFunc(agentAuth.MintKey)))
	mux.Handle("GET /v1/price/gpu",
		handlers.Auth(store, gpuPrices))
	mux.Handle("GET /v1/spend",
		handlers.Auth(store, http.HandlerFunc(wls.Spend)))
	mux.Handle("GET /v1/bursts",
		handlers.Auth(store, http.HandlerFunc(wls.Bursts)))
	if lifecycleStore != nil {
		deleteAdmin := &handlers.ProviderDeleteAdmin{Deletes: lifecycleStore, Wake: deleteWake}
		adminToken := os.Getenv("YSCALE_ADMIN_TOKEN")
		mux.Handle("GET /v1/admin/provider-deletes/manual-attention",
			handlers.AdminAuth(adminToken, http.HandlerFunc(deleteAdmin.ListManualAttention)))
		mux.Handle("POST /v1/admin/provider-deletes/retry",
			handlers.AdminAuth(adminToken, http.HandlerFunc(deleteAdmin.RetryManualAttention)))
	}
	// Connector-command recovery, tenant half: the READ only. Tenant
	// middleware, like every other read on this handler, so a connector
	// credential cannot list the commands central is trying to send it. The
	// requeue that acts on this list is NOT here — on the OSS and connector
	// path a tenant token is the cluster's own credential, so re-driving a
	// dead letter is an operator action on the enterprise admin/operator
	// surfaces (enterprise.go), attributed to an operator.
	mux.Handle("GET /v1/connector-commands",
		handlers.Auth(store, http.HandlerFunc(wls.ConnectorCommands)))

	// Factory/mesh wiring must precede the operator and optional human-account
	// handlers: they capture the factory and poller when registered.
	registerControlPlaneRoutes(mux, store, wls, reconciler, dec, agentAuth, stream, log)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	// Unauthenticated (like /healthz) so the in-cluster Prometheus scrapes it.
	mux.Handle("GET /metrics", costMeter.Handler())

	srv := &http.Server{
		Addr: listenAddr,
		// HTTPMiddleware wraps the mux so ALL routes (auth'd and unauthenticated)
		// are measured: yscale_http_requests_total + yscale_http_request_duration_seconds.
		// Route label = r.Pattern (Go 1.22+ ServeMux — low-cardinality by construction).
		Handler:           costMeter.HTTPMiddleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
		// No write timeout: WS streams are long-lived; Go's WS support
		// disables the deadline on hijacked connections, but we don't
		// want the server-level timeout interfering anyway.
	}

	// Cache GC daemon — evicts stale and expired volumes per the
	// retention policy. Logs to the shared logger; ignores errors
	// from the loop except final shutdown.
	go func() {
		if err := decider.NewCacheGC(store, log).Run(ctx); err != nil && err != context.Canceled {
			log.Error("cache GC exited", "error", err)
		}
	}()

	// Reaper watchdog: retry cleanup a prior reap left pending, and enforce each
	// workload's declared budget (deadline / spend) centrally so a burst whose
	// completion signal never arrives can't run forever. Pending cleanup is
	// retried even when every budget ceiling is disabled. YSCALE_BURST_MAX_LIFETIME
	// (e.g. "24h") adds an opt-in global cap for unbudgeted ordinary bursts;
	// unset/0 disables that cap, not pending cleanup retries.
	//
	// YSCALE_NODE_ONLY_MAX_LIFETIME is the nodeOnly ceiling and defaults FINITE.
	// nodeOnly capacity is ended by a signal only the customer's connector can
	// send — node removed, or node idle — so a connector that dies takes the
	// whole teardown path with it and the burst bills until something else stops
	// it. 6 h is the conservative default: long enough that an ordinary connector
	// restart or a long idle grace never trips it, short enough that a dead one
	// costs a bounded amount. Set it explicitly to "0" to disable and accept
	// unbounded nodeOnly lifetime.
	//
	// It is held open only by OCCUPANCY observations, never by node health, so it
	// covers exactly the connectors that can make that claim. The same duration is
	// handed to the workloads handler above, which applies it as an admission
	// DEADLINE to a nodeOnly submission whose connector cannot make the claim at
	// all and that declared no budget of its own — one env var, one number, both
	// halves of the nodeOnly policy.
	go wls.RunReaperWatchdog(ctx,
		envDuration("YSCALE_REAP_INTERVAL", 2*time.Minute),
		envDuration("YSCALE_BURST_MAX_LIFETIME", 0),
		nodeOnlyMaxLifetime)

	// Teardown worker: drains the durable teardown queue (cloud DeleteNode +
	// node drain) with redelivery-based retry. Only runs when REDIS_URL is set;
	// otherwise reaps tear down inline. Consumer id = pod name so each replica
	// is a distinct consumer in the shared group.
	if teardownBroker != nil {
		consumer := os.Getenv("HOSTNAME")
		if consumer == "" {
			consumer = "central"
		}
		worker := &handlers.TeardownWorker{
			Broker: teardownBroker, Reaper: dec, Store: store,
			Log: log, Consumer: consumer, Commands: store,
		}
		go worker.Run(ctx)
	}

	// Provider delete worker: the only thing that calls a provider on a delete
	// once lifecycle mode is on. It drains once at startup before it ever waits,
	// which is what makes an operation whose lease expired with a previous pod
	// resume WITHOUT another workload signal — no Complete, no Cancel, no
	// watchdog tick required. Every replica runs one; the lease decides which of
	// them owns a given operation.
	if lifecycleStore != nil {
		deleteWorker := &handlers.ProviderDeleteWorker{
			Deletes:  lifecycleStore,
			Reaper:   dec,
			Store:    store,
			Commands: store,
			Cost:     costMeter,
			Log:      log,
			Wake:     deleteWake,
		}
		go deleteWorker.Run(ctx)
	}

	go func() {
		<-ctx.Done()
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shutdown)
	}()

	// Boot-time provider reconciliation: inventory each configured provider,
	// persist any failures, correlate against lifecycle truth, and quarantine
	// uncorrelated resources before any deletion is possible. Runs before serving
	// so a restart observes inherited leaks immediately without using the legacy
	// destructive CleanupOrphans path.
	{
		reconcileCtx, cancelReconcile := context.WithTimeout(ctx, reconcileAttemptTimeout)
		if err := dec.RefreshPodCIDRGauge(reconcileCtx); err != nil {
			log.Warn("boot pod cidr gauge refresh incomplete", "error", err)
		}
		if lifecycleStore != nil {
			summary, err := dec.ReconcileProviders(reconcileCtx, decider.ProviderReconcileOptions{Log: log})
			if err != nil {
				log.Error("boot provider reconciliation failed closed", "error", err)
			} else if summary.Observed+summary.Quarantined+summary.Deleted+summary.Failed > 0 {
				log.Warn("boot provider reconciliation complete",
					"targets", summary.Targets, "observed", summary.Observed,
					"quarantined", summary.Quarantined, "deleted", summary.Deleted,
					"failed", summary.Failed)
			}
		} else if tracked, ok := trackedBackendIDs(reconcileCtx, store, nil, log); ok {
			destroyed, err := dec.SweepOrphans(reconcileCtx, tracked)
			if err != nil {
				log.Error("boot orphan sweep failed", "error", err)
			} else if destroyed > 0 {
				log.Warn("boot orphan sweep complete", "destroyed", destroyed)
			}
		}
		cancelReconcile()
	}

	// Periodic provider reconciliation: bounded inventory worker. It never
	// interprets a failed list as absence; list failures are persisted and the
	// pass performs no delete/adopt transition for that provider target.
	if lifecycleStore != nil {
		go func() {
			log.Info("provider reconciliation worker started", "interval", reconcileInterval.String(),
				"grace_period", backends.OrphanGracePeriod.String())
			t := time.NewTicker(reconcileInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					reconcileCtx, cancelReconcile := context.WithTimeout(ctx, reconcileAttemptTimeout)
					if err := dec.RefreshPodCIDRGauge(reconcileCtx); err != nil {
						log.Warn("periodic pod cidr gauge refresh incomplete", "error", err)
					}
					summary, err := dec.ReconcileProviders(reconcileCtx, decider.ProviderReconcileOptions{Log: log})
					cancelReconcile()
					if err != nil {
						log.Error("provider reconciliation failed closed", "error", err)
					} else if summary.Observed+summary.Quarantined+summary.Deleted+summary.Failed > 0 {
						log.Warn("provider reconciliation complete",
							"targets", summary.Targets, "observed", summary.Observed,
							"quarantined", summary.Quarantined, "deleted", summary.Deleted,
							"failed", summary.Failed)
					}
				}
			}
		}()
	} else {
		go func() {
			interval := envDuration("YSCALE_ORPHAN_SWEEP_INTERVAL", 10*time.Minute)
			if interval <= 0 {
				interval = 10 * time.Minute
			}
			log.Info("legacy orphan sweep worker started", "interval", interval.String(),
				"grace_period", backends.OrphanGracePeriod.String())
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					sweepCtx, cancelSweep := context.WithTimeout(ctx, 30*time.Second)
					if err := dec.RefreshPodCIDRGauge(sweepCtx); err != nil {
						log.Warn("periodic pod cidr gauge refresh incomplete", "error", err)
					}
					tracked, ok := trackedBackendIDs(sweepCtx, store, nil, log)
					if !ok {
						cancelSweep()
						continue
					}
					destroyed, err := dec.SweepOrphans(sweepCtx, tracked)
					cancelSweep()
					if err != nil {
						log.Error("orphan sweep failed", "error", err)
					} else if destroyed > 0 {
						log.Warn("orphan sweep complete", "destroyed", destroyed)
					}
				}
			}
		}()
	}

	log.Info("yscale-cloud listening", "addr", listenAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "error", err)
		os.Exit(1)
	}
}

// burstEvidenceComposite joins lifecycle.Store (provider-created timestamp) and
// state.Store (durable reap receipt) into the single BurstEvidenceReader the
// workload detail handler uses. Neither store alone satisfies the interface.
type burstEvidenceComposite struct {
	lifecycle *lifecycle.Store
	state     *state.Store
}

func (c *burstEvidenceComposite) GetBurstProviderCreatedAt(ctx context.Context, customerID, clusterID, burstID string) (*time.Time, error) {
	return c.lifecycle.GetBurstProviderCreatedAt(ctx, customerID, clusterID, burstID)
}

func (c *burstEvidenceComposite) BurstReapRecorded(ctx context.Context, burstID, customerID string) (bool, error) {
	return c.state.BurstReapRecorded(ctx, burstID, customerID)
}

// lifecycleProvisioningAdapter adapts *lifecycle.Store into cost.LifecycleSource.
type lifecycleProvisioningAdapter struct {
	store *lifecycle.Store
}

func (a *lifecycleProvisioningAdapter) LiveProvisioningOps(ctx context.Context) ([]cost.LifecycleProvisioningRecord, error) {
	ops, err := a.store.LiveProvisioningOps(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]cost.LifecycleProvisioningRecord, len(ops))
	for i, op := range ops {
		out[i] = cost.LifecycleProvisioningRecord{
			BurstID:   op.BurstID,
			CreatedAt: op.CreatedAt,
		}
	}
	return out, nil
}
