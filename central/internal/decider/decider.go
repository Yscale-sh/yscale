// Package decider is the central server's brain: given a Workload,
// pick a backend, mint a Tailscale ephemeral auth key for the burst,
// and produce a Plan the API layer turns into agent commands.
//
// This is where backend creds live in the running process. They are
// loaded from environment variables at startup (Fly token, Linode
// token, AWS creds, Tailscale OAuth) and never leave this package's
// boundary except through outbound calls to the respective backend APIs.
package decider

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
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/credentialcipher"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/central/internal/pricing"
	"github.com/yscale-sh/yscale/central/internal/routecidr"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/central/internal/tailscale"
	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/aws"
	"github.com/yscale-sh/yscale/pkg/backends/azure"
	"github.com/yscale-sh/yscale/pkg/backends/flyio"
	"github.com/yscale-sh/yscale/pkg/backends/gcp"
	"github.com/yscale-sh/yscale/pkg/backends/linode"
	"github.com/yscale-sh/yscale/pkg/workload"
)

var errMeshProviderUnavailable = errors.New("mesh provider unavailable")

// Config holds the central server's backend credentials. Loaded from
// env at startup; immutable for the process lifetime.
type Config struct {
	FlyToken     string // FLYIO_TOKEN
	FlyOrg       string // FLY_ORG
	FlyRegion    string // FLY_REGION (default "ord")
	LinodeToken  string // LINODE_TOKEN
	LinodeRegion string // LINODE_REGION (default "us-ord")
	LinodeSSHKey string // LINODE_SSH_KEY — SSH pubkey added to bursts for debugging
	// AWS credentials. Region alone is enough to enable the backend —
	// the SDK falls back to the default credential chain (IAM role,
	// shared config, env) when access keys are empty.
	AWSAccessKeyID     string // AWS_ACCESS_KEY_ID
	AWSSecretAccessKey string // AWS_SECRET_ACCESS_KEY
	AWSSessionToken    string // AWS_SESSION_TOKEN (optional)
	AWSRegion          string // AWS_REGION
	// GCP credentials. ProjectID alone is enough to enable the backend —
	// the SDK falls back to Application Default Credentials (gcloud
	// login, the GCE metadata server) when CredentialsFile is empty.
	GCPCredentialsFile string // GOOGLE_APPLICATION_CREDENTIALS
	GCPProjectID       string // GCP_PROJECT_ID
	GCPZone            string // GCP_ZONE
	GCPRegion          string // GCP_REGION
	// Azure credentials (service principal). SubscriptionID + ResourceGroup
	// alone don't enable the backend — a ClientSecretCredential also needs
	// TenantID/ClientID/ClientSecret; see pkg/backends/azure.
	AzureTenantID       string // AZURE_TENANT_ID
	AzureClientID       string // AZURE_CLIENT_ID
	AzureClientSecret   string // AZURE_CLIENT_SECRET
	AzureSubscriptionID string // AZURE_SUBSCRIPTION_ID
	AzureLocation       string // AZURE_LOCATION
	AzureResourceGroup  string // AZURE_RESOURCE_GROUP
	// Tailscale OAuth client for minting burst auth keys.
	TSOAuthClientID     string // TS_OAUTH_CLIENT_ID
	TSOAuthClientSecret string // TS_OAUTH_CLIENT_SECRET
	TSTailnet           string // TS_TAILNET (e.g. "yscale.github" or "-")
}

const (
	// BurstTailnetTag is the tag central applies to burst nodes. It is still
	// shared with the agent for backwards compatibility with the existing ACL.
	BurstTailnetTag = handlers.AgentTailnetTag
	// BurstPodCIDRPool is the aggregate route that covers every per-burst /24
	// allocated by allocatePodCIDR.
	BurstPodCIDRPool = routecidr.BurstPodCIDRPool
)

// ErrFabricProvisioning signals that a tenant's fabric is still onboarding.
// It is a retryable condition; HTTP handlers detect its marker without
// importing this package (which would create an import cycle).
var ErrFabricProvisioning error = fabricProvisioningError{}

type fabricProvisioningError struct{}

func (fabricProvisioningError) Error() string       { return "fabric provisioning" }
func (fabricProvisioningError) FabricProvisioning() {}

// Decider implements handlers.Decider. v0 keeps it simple: every
// workload goes through Provision (no warm pool yet), backend selection
// is "Fly for CPU, Linode for GPU."
type Decider struct {
	cfg Config

	// Any backend may be nil if its token is unset; Plan returns a
	// clear error in that case rather than panicking.
	fly    backends.Backend
	linode backends.Backend
	aws    backends.Backend
	gcp    backends.Backend
	azure  backends.Backend
	// gcpRegion is the region the backend will use when the workload does
	// not pin one. Pricing must validate the effective launch region, not
	// assume an empty workload region means the backend default.
	gcpRegion string

	// ts is the Tailscale API client for minting ephemeral auth keys.
	// nil when TS_OAUTH_* env vars aren't set — Plan returns an error
	// in that case (SaaS bursts require Tailscale).
	ts *tailscale.Client

	// boxProvider constructs a provider for a customer's self-hosted mesh
	// endpoint. nil in the OSS build (Tailscale-only); wired by an alternative
	// build so meshFor can route box customers to their own coordination server.
	boxProvider    mesh.BoxProviderFunc
	fabricProvider func(customerID, loginServer string) mesh.Provider

	// fabricProvisioning is optionally wired by enterprise onboarding. Nil
	// preserves the normal OSS behavior when no provider is available.
	fabricProvisioning func(customerID string) bool

	// Store is the state layer the decider reads + writes for storage
	// volume tracking. Optional — when nil, storage routing falls back
	// to "no existing volumes" (always allocate new). Tests pass nil;
	// production wires the real store.
	store            *state.Store
	credentialCipher interface {
		Decrypt(string, []byte) (string, error)
	}
	linodeForAccount func(string, linode.Config) backends.Backend

	// Cost is the metrics meter. Optional (nil = no metrics). Wired via
	// WithCost in production; left nil in tests.
	Cost *cost.Meter

	// admission is the authoritative PostgreSQL record of create intent. nil is
	// the explicit OSS/dev compatibility seam — see WithLifecycleAdmission.
	admission ProviderCreateAdmission
	// reconciliation is the durable provider inventory/quarantine store. nil is
	// the explicit OSS/dev compatibility seam; production wires lifecycle.Store.
	reconciliation ProviderReconciliationStore

	mu sync.Mutex
	// reservedSlots bridges the window between allocating a pod /24 and the
	// handler persisting the Burst that records it: a slot handed out but not
	// yet visible in ListBursts. Reconciled against ListBursts and aged out on
	// each allocation so a Plan that never stored its burst can't leak a slot.
	reservedSlots map[int]time.Time
}

const (
	podSlotMin        = 10
	podSlotMax        = 250
	podReservationTTL = 10 * time.Minute
	// totalPodSlots is the pool size the free-slot gauge is measured against
	// (241 = slots 10..250 inclusive).
	totalPodSlots = podSlotMax - podSlotMin + 1
	// ambiguousCreateLeaseTTL keeps tenant credentials available for operator
	// investigation when Linode may have accepted a create whose response was
	// lost. Seven days is bounded but comfortably exceeds queue/restart outages;
	// disconnect remains blocked until teardown/manual reconciliation or expiry.
	ambiguousCreateLeaseTTL    = 7 * 24 * time.Hour
	cloudAccountLeaseOpTimeout = 10 * time.Second
)

// New constructs a Decider with the given backend creds. Backends
// with missing tokens are left nil — Plan returns a typed error if a
// workload routes to an unconfigured backend.
//
// store is optional: pass nil in tests to disable storage-volume
// tracking; production wires the real *state.Store via WithStore().
func New(cfg Config) *Decider {
	d := &Decider{cfg: cfg, reservedSlots: make(map[int]time.Time)}
	if cfg.FlyToken != "" && cfg.FlyOrg != "" {
		region := cfg.FlyRegion
		if region == "" {
			region = "ord"
		}
		d.fly = flyio.New(cfg.FlyToken, cfg.FlyOrg, region)
	}
	if cfg.LinodeToken != "" {
		d.linode = linode.New(cfg.LinodeToken, cfg.LinodeRegion, cfg.LinodeSSHKey)
	}
	// AWS: region alone is enough — the SDK falls back to the default
	// credential chain (IAM role, shared config) when access keys are
	// empty. Useful for prod IAM-role auth.
	if cfg.AWSAccessKeyID != "" || cfg.AWSRegion != "" {
		d.aws = aws.New(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSSessionToken, cfg.AWSRegion)
	}
	// GCP: project ID alone is enough — the SDK falls back to Application
	// Default Credentials (gcloud login, the GCE metadata server) when
	// GCPCredentialsFile is empty. CPU only; see pkg/backends/gcp.
	if cfg.GCPProjectID != "" {
		d.gcp = gcp.New(cfg.GCPProjectID, cfg.GCPZone, cfg.GCPRegion, cfg.GCPCredentialsFile)
		d.gcpRegion = configuredGCPRegion(cfg.GCPZone, cfg.GCPRegion)
	}
	// Azure: subscription + resource group are both structurally required
	// (there is no default-project-style fallback); CPU only, see
	// pkg/backends/azure.
	if cfg.AzureSubscriptionID != "" && cfg.AzureResourceGroup != "" {
		d.azure = azure.New(cfg.AzureTenantID, cfg.AzureClientID, cfg.AzureClientSecret, cfg.AzureSubscriptionID, cfg.AzureLocation, cfg.AzureResourceGroup)
	}
	if cfg.TSOAuthClientID != "" && cfg.TSOAuthClientSecret != "" && cfg.TSTailnet != "" {
		// Errors here only surface configuration mistakes; ignore at
		// construction time and let Plan() return the real error when
		// the first burst is requested.
		if ts, err := tailscale.New(tailscale.Config{
			ClientID:     cfg.TSOAuthClientID,
			ClientSecret: cfg.TSOAuthClientSecret,
			Tailnet:      cfg.TSTailnet,
		}); err == nil {
			d.ts = ts
		}
	}
	return d
}

// WithStore wires the state.Store into the decider so it can resolve
// existing storage volumes and persist new ones. Returns the same
// decider for chaining.
func (d *Decider) WithStore(s *state.Store) *Decider {
	d.store = s
	return d
}

func (d *Decider) WithCredentialCipher(c interface {
	Decrypt(string, []byte) (string, error)
}) *Decider {
	d.credentialCipher = c
	return d
}

// WithCost wires the cost meter into the decider for reliability metrics
// (mesh mint counts, fallback counts, pod-CIDR pool free). Returns the same
// decider for chaining.
func (d *Decider) WithCost(m *cost.Meter) *Decider {
	d.Cost = m
	return d
}

// WithBoxProvider wires the constructor for self-hosted-coordination-server
// providers. Left unset in the OSS build (every customer uses Tailscale SaaS).
func (d *Decider) WithBoxProvider(f mesh.BoxProviderFunc) *Decider {
	d.boxProvider = f
	return d
}

// WithFabricProvider wires the constructor for factory-provisioned tenant
// boxes (MeshEndpoint.Provider == state.MeshProviderFactory). Those tenants
// have no box key in central; the provider routes every operation through the
// factory.
func (d *Decider) WithFabricProvider(f func(customerID, loginServer string) mesh.Provider) *Decider {
	d.fabricProvider = f
	return d
}

// SetFabricProvisioning installs the optional readiness lookup used to return
// a retryable error while a tenant's fabric is onboarding.
func (d *Decider) SetFabricProvisioning(f func(customerID string) bool) {
	d.fabricProvisioning = f
}

// TS exposes the Tailscale client so other handlers (notably the
// /v1/agent/ts-auth-key endpoint that mints fresh keys per agent
// boot) can reuse the same OAuth credentials. Nil if central wasn't
// started with TS_OAUTH_* configured.
func (d *Decider) TS() *tailscale.Client { return d.ts }

// Compile-time assertion that *tailscale.Client satisfies mesh.Provider.
// This lives here (not in package mesh) because decider is the package
// that already imports both, which keeps mesh free of any import cycle
// with tailscale.
var _ mesh.Provider = (*tailscale.Client)(nil)

// meshFor returns the coordination-server provider for a customer. The
// DEFAULT is the shared Tailscale SaaS client (d.ts); a customer with a
// self-hosted mesh endpoint gets a provider pointed at their own server, but
// ONLY when a box-provider constructor has been wired (d.boxProvider). The OSS
// build leaves it unset, so every customer uses Tailscale SaaS. Returning
// mesh.Provider keeps the concrete types interchangeable at the call site.
//
// Invariant: cust==nil, cust.Mesh==nil, or no box provider wired yields exactly
// the d.ts path — byte-for-byte identical to the Tailscale-only default.
func (d *Decider) meshFor(cust *state.Customer) mesh.Provider {
	if cust != nil && cust.Mesh != nil && cust.Mesh.Provider == state.MeshProviderFactory {
		// Factory-credentialed box: never hand its keyless endpoint to the
		// direct box client. No factory wired means no provider (fail closed).
		if d.fabricProvider == nil {
			return nil
		}
		return d.fabricProvider(cust.ID, cust.Mesh.LoginServer)
	}
	if cust != nil && cust.Mesh != nil && d.boxProvider != nil {
		return d.boxProvider(cust.Mesh.LoginServer, cust.Mesh.APIKey, cust.Mesh.User)
	}
	// Default: the shared Tailscale SaaS client. Return a true interface
	// nil (not a typed nil *tailscale.Client) when it's unconfigured, so
	// callers can do a plain `prov != nil` guard without a nil-pointer
	// panic — matching the prior `d.ts != nil` checks.
	if d.ts == nil {
		return nil
	}
	return d.ts
}

// customerByID resolves a customer from the store, tolerating a nil store
// (tests) and lookup misses by returning nil. A nil result routes
// meshFor() back to the default Tailscale path, preserving backward
// compatibility.
func (d *Decider) customerByID(id string) *state.Customer {
	if d.store == nil || id == "" {
		return nil
	}
	cust, err := d.store.CustomerByID(id)
	if err != nil {
		return nil
	}
	return cust
}

// hasMeshBox reports whether a customer has an opted-in self-hosted mesh
// endpoint. Used to relax the "tailscale not configured" guard for those
// customers.
func hasMeshBox(cust *state.Customer) bool {
	return cust.HasMeshBox()
}

// tsAuthKeyExpiry is the TTL for the ephemeral auth key. Devices
// must complete `tailscale up` within this window; the key is
// reusable=true (so the burst can retry `tailscale up` on transient
// failure with the same key value) but ephemeral=true so each
// joined device disappears from the tailnet on disconnect.
const tsAuthKeyExpiry = 30 * time.Minute

// meshMintTimeout bounds a single mesh auth-key mint. A dead/unreachable
// coordination box must fail the request fast (fail closed) rather than hang;
// there is no shared-tailnet fallback to absorb the delay.
const meshMintTimeout = 10 * time.Second

const prepaidQuoteMaxDuration = 24 * time.Hour

// Quote computes the exact provider shape and bounded customer charge without
// minting credentials, allocating network state, touching storage, or calling
// a provider API.
func (d *Decider) Quote(_ context.Context, wl *workload.Workload, opts handlers.PlanOptions) (*handlers.BurstQuote, error) {
	if wl == nil {
		return nil, fmt.Errorf("workload is nil")
	}
	if d.fabricProvisioning != nil && d.fabricProvisioning(opts.CustomerID) && d.meshFor(d.customerByID(opts.CustomerID)) == nil {
		return nil, ErrFabricProvisioning
	}
	if d.ts == nil && d.boxProvider == nil {
		if d.fabricProvisioning != nil && d.fabricProvisioning(opts.CustomerID) {
			return nil, ErrFabricProvisioning
		}
		return nil, errors.New("no mesh provider configured")
	}
	decision, err := d.decide(wl, opts)
	if err != nil {
		return nil, err
	}
	// One issuance of the decision. The receipt is sealed with this quote's own
	// identity and window, which the digest deliberately excludes — so the same
	// decision quoted twice binds to the same digest while still telling a
	// reader exactly which issuance they are holding.
	now := time.Now().UTC()
	quoteID := "quote_" + randHex(12)
	receipt := decision.receipt
	if err := receipt.Seal(quoteID, now, placementQuoteTTL); err != nil {
		return nil, err
	}
	if err := receipt.Validate(); err != nil {
		return nil, err
	}
	return &handlers.BurstQuote{
		Backend: decision.backend, CloudAccountID: decision.cloudAccountID, ShapeHash: decision.shapeHash,
		Region: decision.region, SKU: decision.sku, HourlyUSD: decision.hourly,
		Placement: &receipt,
		Price: billing.PriceQuote{
			QuoteID: quoteID, PricingVersion: state.PlacementPricingVersion, Currency: "USD",
			Provider: decision.backend, SKU: decision.sku, Region: decision.region,
			ProviderRateMicroUSDPerHour: decision.rateMicroUSD, CustomerRateMicroUSDPerHour: decision.rateMicroUSD,
			MaximumDurationSeconds: int64(decision.duration.Seconds()), MaximumChargeMicroUSD: decision.maxMicroUSD,
			IssuedAt: now, ValidUntil: now.Add(placementQuoteTTL),
		},
	}, nil
}

// Plan preserves the legacy interface while routing production through the
// same quote-then-provision split used by prepaid admission.
func (d *Decider) Plan(ctx context.Context, wl *workload.Workload, opts handlers.PlanOptions) (*handlers.Plan, error) {
	quote, err := d.Quote(ctx, wl, opts)
	if err != nil {
		return nil, err
	}
	return d.PlanQuoted(ctx, wl, opts, quote)
}

// PlanQuoted provisions only after recomputing and matching the trusted quote.
func (d *Decider) PlanQuoted(ctx context.Context, wl *workload.Workload, opts handlers.PlanOptions, quote *handlers.BurstQuote) (*handlers.Plan, error) {
	if wl == nil {
		return nil, fmt.Errorf("workload is nil")
	}
	if quote == nil {
		return nil, fmt.Errorf("provider quote is nil")
	}
	if d.fabricProvisioning != nil && d.fabricProvisioning(opts.CustomerID) && d.meshFor(d.customerByID(opts.CustomerID)) == nil {
		return nil, ErrFabricProvisioning
	}
	// At least one mesh-provider mechanism must be wired. In the SaaS edition,
	// the box provider is present while a newly provisioned customer's Mesh ref
	// may not be populated yet, so availability must not be gated on that ref.
	if d.ts == nil && d.boxProvider == nil {
		if d.fabricProvisioning != nil && d.fabricProvisioning(opts.CustomerID) {
			return nil, ErrFabricProvisioning
		}
		return nil, errors.New("no mesh provider configured")
	}

	// The SAME decision path the quote (and the customer's preview) came from.
	// Recomputing it here is what makes the match below meaningful: two
	// independent runs of one function over unchanged inputs, not a comparison
	// against values the caller handed back.
	//
	// GPU admission is deliberately ahead of storage RESOLUTION for the reason
	// it is inside decide(): resolving an existing volume updates its
	// LastUsedAt timestamp, and a workload that cannot satisfy its per-GPU
	// price ceiling must have no durable or paid side effects at all.
	decision, err := d.decide(wl, opts)
	if err != nil {
		return nil, err
	}
	backendName, chosen, cloudAccountID := decision.backend, decision.chosen, decision.cloudAccountID
	resources, effectiveRegion := decision.resources, decision.region
	hourly, sku, shapeHash := decision.hourly, decision.sku, decision.shapeHash
	if quote.Backend != backendName || quote.CloudAccountID != cloudAccountID ||
		quote.ShapeHash != shapeHash || quote.Region != effectiveRegion || quote.SKU != sku || quote.HourlyUSD != hourly {
		return nil, errors.New("provider quote no longer matches workload shape")
	}
	// The placement decision must match too, not only the provider shape: a
	// changed cluster grant, constraint, candidate set or catalog version is a
	// different answer to the customer even when it resolves to the same machine,
	// and the digest is what a preview bound itself to.
	placementDigest, err := decision.digest()
	if err != nil {
		return nil, err
	}
	if quote.Placement != nil {
		if quote.Placement.Digest != placementDigest {
			return nil, errors.New("placement decision no longer matches workload shape")
		}
		// A quote can sit in the submission path across a billing call and a
		// durable write. Its issuance window is enforced HERE, at the last point
		// before admission commits intent, so a decision nothing is holding any
		// more cannot become a machine.
		if quote.Placement.Expired(time.Now().UTC()) {
			return nil, errors.New("placement decision has expired")
		}
	}

	// Resolve the customer's mesh provider: a box tenant mints on its own
	// self-hosted coordination server; an OSS / shared tenant mints on the
	// Tailscale SaaS client.
	//
	// Fail closed (#58): each tenant mints ONLY on its own mesh. There is
	// deliberately NO fallback to the shared Tailscale tailnet on a box-mint
	// failure — demoting a paid-for isolated tenant onto shared multi-tenant
	// infrastructure would break the isolation guarantee. A dead/unreachable box
	// fails the burst (the caller retries); the mint is bounded so a hung box
	// fails fast, and the customer's mesh ref is left intact (teardown keys device
	// deletion on it; dead-box demotion is owned by startup box-validation).
	//
	// RESOLVED before admission, MINTED after it. Selecting the provider is a
	// read; minting is the credential side effect. Splitting them matters
	// because "this tenant's fabric is still onboarding" is the one refusal the
	// API layer answers RETRYABLY — it releases the submission's key and invites
	// the client back — and a retry arrives under a fresh durable identity. An
	// operation admitted and then abandoned by that retry is exactly the
	// executable intent this path must never leave behind, so the check that
	// raises it runs while there is still nothing to abandon.
	cust := d.customerByID(opts.CustomerID)
	primary := d.meshFor(cust)
	if primary == nil {
		if d.fabricProvisioning != nil && d.fabricProvisioning(opts.CustomerID) {
			return nil, ErrFabricProvisioning
		}
		d.Cost.RecordMeshMint("unknown", "fail")
		return nil, errors.New("mesh: no coordination provider configured")
	}

	// The burst id is minted before the /24 because the durable reservation is
	// keyed by it: a release must be able to name the burst whose slot it frees,
	// since slots are recycled and freeing one by number could free a different
	// burst's live reservation. It is minted before ADMISSION for the same class
	// of reason: the durable record of what will be bought has to name it.
	burstID := "burst_" + randHex(6)

	// Providers with cross-region capacity fallback can leave the actual region
	// unknown until create. Canonicalize that absence for the lifecycle row in
	// the same way as authoritative delete. Fixed provider regions stay intact;
	// neither a preferred fallback nor another provider's default is evidence.
	admissionRegion := lifecycle.CanonicalProviderDeleteRegion(effectiveRegion)

	// AUTHORITATIVE ADMISSION. Everything above this line is pure resolution:
	// routing, pricing, the quote match and the serialized provider request are
	// functions of the submission, and none of them mints a credential,
	// allocates a /24, takes a tenant lease or calls a provider. Everything
	// below it is a side effect that a crash could strand — so the durable
	// record of intent commits HERE, in between, and the attempt that proceeds
	// past this point holds the lease that fences it.
	//
	// The placement digest rides INTO that record: what admission commits is
	// then the whole decision the customer was shown, so a replay that resumes
	// the stored request cannot resume it under a different placement.
	admitted, err := d.admitProviderCreate(ctx, wl, opts, resolveProviderCreateRequest(
		wl, opts, burstID, backendName, cloudAccountID, admissionRegion, sku, shapeHash, placementDigest, resources))
	if err != nil {
		return nil, err
	}
	// On a replay this is the request the ORIGINAL attempt admitted, read back
	// out of the operation row, so a resumed create buys the shape that was
	// committed rather than the one this process just recomputed.
	request := admitted.request
	burstID = request.BurstID

	// Every refusal from here to the provider call answers the submitter
	// terminally while holding the create lease, so each one settles the durable
	// operation. Leaving it unsettled would leave an expiring lease behind on an
	// operation nothing has resolved — so when the settlement itself cannot
	// commit, the ambiguous settlement error is what the submitter gets, not the
	// clean refusal that would release their hold.
	refuse := func(err error) (*handlers.Plan, error) {
		return nil, d.settleFailedProviderCreate(ctx, admitted, err)
	}

	// Existing volumes constrain the selected backend+DC. This remains after
	// the price gate because resolveStorage touches reused volume timestamps.
	storageRes, err := d.resolveStorage(wl, opts.CustomerID)
	if err != nil {
		return refuse(fmt.Errorf("resolve storage: %w", err))
	}
	if storageRes != nil && storageRes.pinBackend != "" && storageRes.pinBackend != backendName {
		return refuse(fmt.Errorf("storage volume pinned to backend %q but workload routes to %q (set spec.backend explicitly to override)",
			storageRes.pinBackend, backendName))
	}
	// Defence in depth on the region half. decide() already refused this against
	// the read-only affinity lookup, before anything was reserved or held; this
	// repeats it against the resolution that also WRITES, so a volume that moved
	// between the two reads cannot be attached from another DC.
	if storageRes != nil && storageRes.pinDCRegion != "" && resources.Region != "" &&
		storageRes.pinDCRegion != resources.Region {
		return refuse(fmt.Errorf("%w: volume is in %q but workload requests %q (drop spec.region to launch where the data is)",
			errStorageRegionPin, storageRes.pinDCRegion, resources.Region))
	}

	podCIDR, err := d.allocatePodCIDR(ctx, burstID)
	if err != nil {
		return refuse(err)
	}

	job, err := workload.ToJob(wl)
	if err != nil {
		return refuse(fmt.Errorf("rendering job: %w", err))
	}
	if wl.Spec.Storage != nil && (len(wl.Spec.Storage.Cache) > 0 || len(wl.Spec.Storage.Artifacts) > 0) {
		job.Annotations[workload.AnnotationCacheBurstID] = burstID
		job.Spec.Template.Annotations[workload.AnnotationCacheBurstID] = burstID
		job.Annotations[workload.AnnotationCacheBootstrapEndpoint] = request.BootstrapEndpoint
		job.Spec.Template.Annotations[workload.AnnotationCacheBootstrapEndpoint] = request.BootstrapEndpoint
	}
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return refuse(fmt.Errorf("marshaling job: %w", err))
	}

	mintCtx, mintCancel := context.WithTimeout(ctx, meshMintTimeout)
	defer mintCancel()
	authKey, err := primary.MintAuthKey(mintCtx, request.TSTags, tsAuthKeyExpiry)
	if err != nil {
		d.Cost.RecordMeshMint("unknown", "fail")
		return refuse(fmt.Errorf("mint mesh auth key: %w", err))
	}
	usedProv := primary
	meshLoginServer := usedProv.LoginServer()
	meshProvider := "tailscale"
	if meshLoginServer != "" {
		meshProvider = "box"
	}
	d.Cost.RecordMeshMint(meshProvider, "ok")

	estUSD := estimateCostUSD(wl, backendName, resources)

	// The provider call is rendered from the ADMITTED request plus this
	// attempt's ephemera (auth key, /24, coordination server). The shape, the
	// name, the image and the tier therefore cannot drift from what admission
	// committed — including on a replay, which renders the original request.
	spec := request.nodeSpec(admitted.clusterID, authKey, podCIDR, meshLoginServer)
	if cloudAccountID != "" {
		if d.store == nil {
			return refuse(errors.New("tenant cloud-account lease store is unavailable"))
		}
		won, err := d.store.AcquireCloudAccountLease(ctx, state.CloudAccountLease{
			BurstID: burstID, CustomerID: opts.CustomerID, CloudAccountID: cloudAccountID,
			ExpiresAt: time.Now().UTC().Add(podReservationTTL),
		})
		if err != nil {
			return refuse(fmt.Errorf("acquire tenant cloud-account lease: %w", err))
		}
		if !won {
			return refuse(errors.New("tenant cloud-account lease conflict"))
		}
	}

	backendID, err := chosen.CreateNode(ctx, spec)
	if err != nil {
		// One ambiguous boolean derived from the shared provider-neutral
		// classifier drives durable settlement, billing behavior, and the
		// cloud-account lease. Every adapter marks failures that prove no
		// billable resource exists; anything unmarked is ambiguous by default.
		ambiguous := backends.CreateOutcomeAmbiguous(err)
		var createErr error = &providerCreateError{err: fmt.Errorf("creating burst on %s: %w", backendName, err), ambiguous: ambiguous}
		// Settled terminally either way, and the distinction is recorded rather
		// than inferred: a proven-failed create is a burst that never existed,
		// an ambiguous one may have bought a machine and must reach an operator
		// through the dead-letter/manual-attention path instead of a retry that
		// could buy a second.
		//
		// A settlement that cannot commit ESCALATES the answer to ambiguous even
		// when the provider proved the create never landed. The proof is real,
		// but nothing durable holds it, and an operation left in processing is
		// not a record anyone can bill or refund against — so the hold stays
		// until an operator resolves it.
		if settleErr := d.failProviderCreate(ctx, admitted, providerCreateSafeError(ambiguous, backendName, err)); settleErr != nil {
			createErr = settleErr
		}
		// The cloud-account lease follows the provider classification captured
		// above. A settlement-write failure may still escalate the error returned
		// to billing, but it cannot turn a provider-proven absence into a machine.
		if cloudAccountID != "" && d.store != nil {
			leaseCtx, leaseCancel := context.WithTimeout(context.Background(), cloudAccountLeaseOpTimeout)
			defer leaseCancel()
			if ambiguous {
				if extendErr := d.store.ExtendCloudAccountLease(leaseCtx, burstID, opts.CustomerID, cloudAccountID,
					time.Now().UTC().Add(ambiguousCreateLeaseTTL)); extendErr != nil {
					return nil, errors.Join(createErr,
						fmt.Errorf("retain ambiguous-create cloud-account lease: %w", extendErr))
				}
			} else {
				if releaseErr := d.store.ReleaseCloudAccountLease(leaseCtx, burstID); releaseErr != nil {
					return nil, errors.Join(createErr,
						fmt.Errorf("release failed-create cloud-account lease: %w", releaseErr))
				}
			}
		}
		return nil, createErr
	}

	// The machine exists. Commit that fact under this attempt's lease before
	// returning a Plan the caller will book, so authoritative state never
	// reports "create requested" for a resource that is already billing.
	if err := d.succeedProviderCreate(ctx, admitted, backendID); err != nil {
		return nil, err
	}

	// Storage volumes that didn't yet exist now get persisted with
	// their newly-known Backend + DC. Bindings already point at the
	// final paths so the agent sees a consistent view immediately.
	d.recordPending(storageRes, backendName, effectiveRegion)

	plan := &handlers.Plan{
		BurstID:         burstID,
		Backend:         backendName,
		BackendID:       backendID,
		Region:          effectiveRegion,
		CloudAccountID:  cloudAccountID,
		NodeName:        spec.Name,
		TSHostname:      spec.TSHostname,
		MeshProvider:    meshProvider,
		MeshLoginServer: meshLoginServer,
		PodCIDR:         podCIDR,
		JobSpec:         jobJSON,
		EstimatedUSD:    estUSD,
		HourlyUSD:       hourly,
		SKU:             sku,
		Tier:            request.Tier,
		Placement:       quote.Placement,
	}
	// Carry the workload's declared budget onto the plan so central can enforce
	// it on the burst (the reaper watchdog reads these off the stored Burst).
	if wl.Spec.Budget != nil {
		plan.Deadline = wl.Spec.Budget.Deadline
		plan.MaxUSD = wl.Spec.Budget.MaxUSD
	}
	if storageRes != nil {
		plan.StorageBindings = storageRes.bindings
	}
	return plan, nil
}

type providerCreateError struct {
	err       error
	ambiguous bool
}

func (e *providerCreateError) Error() string { return e.err.Error() }
func (e *providerCreateError) Unwrap() error { return e.err }

func (d *Decider) CreateOutcomeAmbiguous(err error) bool {
	var createErr *providerCreateError
	return errors.As(err, &createErr) && createErr.ambiguous
}

func providerShapeHash(backendName, cloudAccountID string, resources backends.ResourceRequirements) (string, error) {
	raw, err := json.Marshal(struct {
		Backend        string                        `json:"backend"`
		CloudAccountID string                        `json:"cloud_account_id,omitempty"`
		Resources      backends.ResourceRequirements `json:"resources"`
	}{backendName, cloudAccountID, resources})
	if err != nil {
		return "", fmt.Errorf("encode provider quote shape: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// resolveNetworkingTier is the authoritative central-side tier resolver.
// The full tier is the only supported one; workload.Validate refuses "lite"
// (and every other non-empty non-"full" value) before we ever reach here.
// This wrapper always returns "full" so a defensive path cannot re-open a
// disabled tier on a spec that somehow bypassed validation.
func resolveNetworkingTier(wl *workload.Workload) string {
	return workload.ResolveNetworkingTier(wl)
}

// Teardown destroys the backend node and tailnet device behind a burst. It is
// the legacy inline/broker seam. The authoritative lifecycle worker calls the
// two narrower methods separately so provider absence can be receipted before
// mesh cleanup is attempted durably.
//
// Best-effort: every step is attempted and the errors joined, so one
// failure (e.g. a backend node already gone) doesn't strand the rest.
// The k8s Node object is removed separately, by the agent. cleanup.sh
// remains the backstop for anything this misses.
func (d *Decider) Teardown(ctx context.Context, b *state.Burst) error {
	providerErr := d.DeleteProviderNode(ctx, b)
	meshErr := d.CleanupMesh(ctx, b)
	if errors.Is(meshErr, errMeshProviderUnavailable) {
		slog.Warn("burst mesh provider is unavailable; legacy teardown keeps its best-effort behavior",
			"customer", b.CustomerID, "burst", b.ID, "mesh_provider", b.MeshProvider)
		meshErr = nil
	}
	return errors.Join(providerErr, meshErr)
}

// DeleteProviderNode is the authoritative provider-absence hinge. It performs
// no mesh, ledger, lease, network or Kubernetes cleanup.
func (d *Decider) DeleteProviderNode(ctx context.Context, b *state.Burst) error {
	if b == nil {
		return fmt.Errorf("burst is nil")
	}
	if be, _, err := d.backendForBurst(b); err != nil {
		return err
	} else if err := be.DeleteNode(ctx, b.BackendID); err != nil {
		return fmt.Errorf("delete %s node %s: %w", b.Backend, b.BackendID, err)
	}
	return nil
}

// CleanupMesh removes the coordination-plane device after provider absence is
// durable. It is idempotent and safe to replay from the cleanup repair queue.
func (d *Decider) CleanupMesh(ctx context.Context, b *state.Burst) error {
	if b == nil {
		return fmt.Errorf("burst is nil")
	}
	// Tear down the mesh device through the provider that minted this burst.
	// Legacy records have no stamp, so they intentionally keep the old
	// current-customer lookup behavior.
	var prov mesh.Provider
	switch b.MeshProvider {
	case "":
		prov = d.meshFor(d.customerByID(b.CustomerID))
	case "tailscale":
		prov = d.meshFor(nil)
	case "box":
		cust := d.customerByID(b.CustomerID)
		if hasMeshBox(cust) && cust.Mesh.LoginServer == b.MeshLoginServer {
			prov = d.meshFor(cust)
		} else {
			return fmt.Errorf("%w: minting mesh box no longer attached for login server %s", errMeshProviderUnavailable, b.MeshLoginServer)
		}
	default:
		return fmt.Errorf("%w: unknown burst mesh provider %q", errMeshProviderUnavailable, b.MeshProvider)
	}
	if prov == nil && b.TSHostname != "" {
		return fmt.Errorf("%w: no mesh provider resolved for device %s", errMeshProviderUnavailable, b.TSHostname)
	}
	if prov != nil && b.TSHostname != "" {
		devID, err := prov.FindDeviceByHostname(ctx, b.TSHostname)
		switch {
		case err != nil:
			return fmt.Errorf("find mesh device %s: %w", b.TSHostname, err)
		case devID != "":
			if err := prov.DeleteDevice(ctx, devID); err != nil {
				return fmt.Errorf("delete mesh device %s: %w", devID, err)
			}
			afterID, err := prov.FindDeviceByHostname(ctx, b.TSHostname)
			if err != nil {
				return fmt.Errorf("verify mesh device %s absence: %w", b.TSHostname, err)
			}
			if afterID != "" {
				return fmt.Errorf("mesh device %s still present after delete", b.TSHostname)
			}
		}
	}
	return nil
}

func (d *Decider) backendForTenant(backendName, customerID string) (backends.Backend, string, error) {
	if backendName != backends.TypeLinode || d.store == nil || customerID == "" {
		be, err := d.backendFor(backendName)
		return be, "", err
	}
	account, err := d.store.LinodeCloudAccount(customerID)
	if errors.Is(err, state.ErrNotFound) {
		be, err := d.backendFor(backendName)
		return be, "", err
	}
	if err != nil {
		return nil, "", fmt.Errorf("resolve tenant Linode account: %w", err)
	}
	if d.credentialCipher == nil {
		return nil, "", errors.New("tenant Linode credentials are unavailable")
	}
	token, err := d.credentialCipher.Decrypt(account.CredentialCiphertext,
		credentialcipher.AdditionalData(customerID, account.ID, account.Provider))
	if err != nil {
		return nil, "", errors.New("tenant Linode credentials could not be resolved")
	}
	if d.linodeForAccount != nil {
		return d.linodeForAccount(token, linode.Config{Region: account.Region, CPUImage: account.CPUImage, GPUImage: account.GPUImage}), account.ID, nil
	}
	be := linode.NewWithConfig(token, linode.Config{Region: account.Region, CPUImage: account.CPUImage, GPUImage: account.GPUImage})
	return be, account.ID, nil
}

func (d *Decider) backendForBurst(b *state.Burst) (backends.Backend, string, error) {
	if b.CloudAccountID == "" {
		be, err := d.backendFor(b.Backend)
		return be, "", err
	}
	if b.Backend != backends.TypeLinode || d.store == nil {
		return nil, "", errors.New("burst cloud account is unavailable")
	}
	account, err := d.store.LinodeCloudAccount(b.CustomerID)
	if err != nil || account.ID != b.CloudAccountID {
		return nil, "", errors.New("burst cloud account is unavailable")
	}
	if d.credentialCipher == nil {
		return nil, "", errors.New("burst cloud credentials are unavailable")
	}
	token, err := d.credentialCipher.Decrypt(account.CredentialCiphertext,
		credentialcipher.AdditionalData(b.CustomerID, account.ID, account.Provider))
	if err != nil {
		return nil, "", errors.New("burst cloud credentials could not be resolved")
	}
	if d.linodeForAccount != nil {
		return d.linodeForAccount(token, linode.Config{Region: account.Region, CPUImage: account.CPUImage, GPUImage: account.GPUImage}), account.ID, nil
	}
	return linode.NewWithConfig(token, linode.Config{Region: account.Region, CPUImage: account.CPUImage, GPUImage: account.GPUImage}), account.ID, nil
}

// SweepOrphans destroys any burst VM tagged as ours that no live burst record
// tracks — a backstop for VMs leaked by a crash between CreateNode and the
// record write, or a teardown that dropped the record without deleting the VM.
// It runs each configured backend's CleanupOrphans with the tracked set and
// joins the results.
//
// Safe to run on a timer against a serving central. CreateNode still writes the
// VM before the handler persists the burst record, so an in-flight burst is
// genuinely absent from trackedBackendIDs for a moment — but every backend's
// CleanupOrphans now skips candidates younger than backends.OrphanGracePeriod,
// which comfortably outlasts that window. Older-and-still-untracked is a real
// leak by construction.
//
// The consequence is that reclamation is never immediate: a VM leaked right now
// is destroyed by a sweep at least one grace period from now, not this one. That
// is the trade the age gate buys — bounded overbilling instead of a sweep that
// can delete a live burst.
func (d *Decider) SweepOrphans(ctx context.Context, trackedBackendIDs map[string]bool) (int, error) {
	total := 0
	var errs []error
	// Every constructed backend must appear here. A backend that can create a
	// node but is absent from this list has no cross-restart backstop at all:
	// a crash between CreateNode and the burst-record write leaves a VM that
	// nothing in the system will ever find. gcp and azure were missing while
	// GCP was already live in the deployed central.
	for _, be := range []backends.Backend{d.fly, d.linode, d.aws, d.gcp, d.azure} {
		if be == nil {
			continue
		}
		n, err := be.CleanupOrphans(ctx, trackedBackendIDs)
		total += n
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", be.Name(), err))
		}
	}
	return total, errors.Join(errs...)
}

// tsHostnameFor returns the Tailscale device hostname for a burst.
// Must be DNS-safe (letters, digits, hyphens). burstID is "burst_<hex>";
// flatten the underscore.
func tsHostnameFor(burstID string) string {
	return "yscale-" + strings.ReplaceAll(burstID, "_", "-")
}

// tsTagsFor returns the ACL tags applied to a burst. v0 uses a single
// shared tag — per-customer isolation will land once the operator can
// PATCH tailnet ACL JSON dynamically. customerID is accepted for
// future use.
//
// The tag must be owned by the OAuth client central uses (i.e. listed
// in tailnet ACL `tagOwners` for the client's own tag). For v0 we use
// the same tag that owns the OAuth client itself — Tailscale allows
// minting keys with the owner tag without further configuration.
func tsTagsFor(_ string) []string {
	return []string{BurstTailnetTag}
}

// bootstrapEndpointFor returns the URL the burst hits to fetch its
// kubelet kubeconfig. Resolved via MagicDNS over the tailnet — the
// agent registers as `yscale-agent-<cluster-id>`.
func bootstrapEndpointFor(clusterID string) string {
	safe := strings.ReplaceAll(clusterID, "_", "-")
	return "http://yscale-agent-" + safe + ":8080"
}

// burstImageForBackend returns the burst-node OCI image URL for a
// given backend. Fly machines pull from registry.fly.io (private to
// Fly, fast over their internal network). The image is built from
// Dockerfile.kubelet — see the burst-image-push Makefile target.
func burstImageForBackend(backendName string) string {
	switch backendName {
	case backends.TypeFlyIO:
		return "registry.fly.io/yscale-burst-image:kubelet-latest"
	}
	return backends.AgentImage()
}

// backendFor returns the configured backend matching name, or a typed
// error mentioning the missing env var so the operator knows what to
// set.
func (d *Decider) backendFor(name string) (backends.Backend, error) {
	switch name {
	case backends.TypeFlyIO:
		if d.fly == nil {
			return nil, fmt.Errorf("flyio %w: set FLYIO_TOKEN and FLY_ORG", errBackendNotConfigured)
		}
		return d.fly, nil
	case backends.TypeLinode:
		if d.linode == nil {
			return nil, fmt.Errorf("linode %w: set LINODE_TOKEN", errBackendNotConfigured)
		}
		return d.linode, nil
	case backends.TypeAWS:
		if d.aws == nil {
			return nil, fmt.Errorf("aws %w: set AWS_REGION (and AWS_ACCESS_KEY_ID/SECRET or IAM role)", errBackendNotConfigured)
		}
		return d.aws, nil
	case backends.TypeGCP:
		if d.gcp == nil {
			return nil, fmt.Errorf("gcp %w: set GCP_PROJECT_ID (and GOOGLE_APPLICATION_CREDENTIALS or ADC)", errBackendNotConfigured)
		}
		return d.gcp, nil
	case backends.TypeAzure:
		if d.azure == nil {
			return nil, fmt.Errorf("azure %w: set AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP, AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET", errBackendNotConfigured)
		}
		return d.azure, nil
	default:
		return nil, fmt.Errorf("%w %q", errUnknownBackend, name)
	}
}

// route picks the backend for a workload. Honors explicit
// Spec.Backend; empty or "auto" falls through to: GPU → linode, else
// flyio. Defense-in-depth re-checks validation that pkg/workload also
// enforces.
func (d *Decider) route(wl *workload.Workload) (string, error) {
	switch wl.Spec.Backend {
	case backends.TypeFlyIO:
		if wl.Spec.GPU != nil {
			return "", fmt.Errorf("backend=flyio %w", errGPUUnsupportedByBackend)
		}
		return backends.TypeFlyIO, nil
	case backends.TypeLinode:
		// Linode is explicit-allowed (CPU and GPU); it's also the
		// auto-routing target for GPU.
		return backends.TypeLinode, nil
	case backends.TypeAWS:
		// AWS is explicit-allowed for both CPU and GPU (G/P-family). It's
		// also the auto-routing target for datacenter-class GPU kinds
		// Linode can't serve.
		return backends.TypeAWS, nil
	case backends.TypeGCP:
		// GCP is CPU-only today (GPU quota is unapproved — gcp-plan.md
		// §13, pkg/backends/gcp's mapGPUType seam), so it is never an
		// auto-routing target for GPU; reject an explicit GPU request
		// here rather than dead-ending in CreateNode after admission.
		if wl.Spec.GPU != nil {
			return "", fmt.Errorf("backend=gcp %w", errGPUUnsupportedByBackend)
		}
		return backends.TypeGCP, nil
	case backends.TypeAzure:
		// Azure is CPU-only today (GPU quota is unapproved and
		// unexercisable — azure-plan.md §9/§13, pkg/backends/azure's
		// mapGPUType seam), so it is never an auto-routing target for GPU;
		// reject an explicit GPU request here rather than dead-ending in
		// CreateNode after admission.
		if wl.Spec.GPU != nil {
			return "", fmt.Errorf("backend=azure %w", errGPUUnsupportedByBackend)
		}
		return backends.TypeAzure, nil
	case "", backends.BackendAuto:
		// fall through
	default:
		return "", fmt.Errorf("%w %q", errUnknownBackend, wl.Spec.Backend)
	}
	if wl.Spec.GPU != nil {
		return gpuBackendForKind(wl.Spec.GPU.Kind), nil
	}
	return backends.TypeFlyIO, nil
}

// linodeGPUKinds are the GPU kinds the Linode backend serves (RTX-class),
// plus the empty/"any" sentinel that means "cheapest GPU" — which is also
// Linode. Every other kind is a datacenter card (T4/A10G/L4/L40S/A100/
// H100/H200) only AWS reaches, so auto-routing sends those to AWS.
var linodeGPUKinds = map[string]bool{
	"":           true,
	"any":        true,
	"rtx4000ada": true,
	"rtx4000":    true,
	"rtx6000":    true,
}

// gpuBackendForKind picks the auto-routing backend for a GPU kind: Linode
// for the RTX-class cards it serves (and the cheapest-GPU default), AWS
// for the datacenter silicon Linode lacks. Keeping this kind-based — not
// "GPU always → Linode" — means a request for an L4/A100/H100 no longer
// dead-ends at Linode's catalog; it reaches the backend that can place it.
func gpuBackendForKind(kind string) string {
	if linodeGPUKinds[strings.ToLower(kind)] {
		return backends.TypeLinode
	}
	return backends.TypeAWS
}

// resourcesFor maps a Workload to backend ResourceRequirements. When
// spec.replicas > 1 the burst is sized to fit all replicas — N × per-pod
// requests. GPU kind/count resolution remains shared with the backend through
// resolveGPUPriceQuote so admission prices the shape CreateNode will launch.
func resourcesFor(wl *workload.Workload) (backends.ResourceRequirements, error) {
	r := backends.ResourceRequirements{CPUMillis: 1000, MemoryMB: 2048, Region: wl.Spec.Region}
	if size, ok := workload.LookupSize(wl.Spec.Size); ok {
		r.CPUMillis = size.CPUMillis
		r.MemoryMB = size.MemoryMB
	} else if wl.Spec.CPU != "" || wl.Spec.Memory != "" {
		cpu, err := resource.ParseQuantity(wl.Spec.CPU)
		if err != nil {
			return r, fmt.Errorf("parsing spec.cpu %q: %w", wl.Spec.CPU, err)
		}
		memory, err := resource.ParseQuantity(wl.Spec.Memory)
		if err != nil {
			return r, fmt.Errorf("parsing spec.memory %q: %w", wl.Spec.Memory, err)
		}
		r.CPUMillis = cpu.MilliValue()
		r.MemoryMB = memory.Value() / (1024 * 1024)
	}
	// Multi-pod burst: scale the burst node so all replicas fit.
	if replicas := int64(workload.EffectiveReplicas(wl)); replicas > 1 {
		r.CPUMillis *= replicas
		r.MemoryMB *= replicas
	}
	if wl.Spec.GPU == nil {
		return r, nil
	}
	if wl.Spec.GPU.Count < 0 {
		return r, fmt.Errorf("spec.gpu.count must be >= 0")
	}

	// Per-pod GPU count multiplied by replicas — so 2 replicas × 1 GPU
	// = a burst with 2 GPUs.
	perPodGPU := max(1, wl.Spec.GPU.Count)
	replicas := workload.EffectiveReplicas(wl)
	if perPodGPU > math.MaxInt/replicas {
		return r, fmt.Errorf("effective GPU count overflows int: %d GPUs per pod x %d replicas", perPodGPU, replicas)
	}
	gpu := &backends.GPUSpec{
		Kind:      wl.Spec.GPU.Kind,
		Count:     perPodGPU * replicas,
		CPUMillis: r.CPUMillis,
		MemoryMB:  r.MemoryMB,
	}

	// Carry a requested literal SKU into admission. Central currently rejects
	// it before provisioning because no backend guarantees exact pinned launch
	// and authoritative pricing yet.
	if wl.Spec.GPU.SKU != "" {
		gpu.SKUs = []string{wl.Spec.GPU.SKU}
	}
	r.GPU = gpu
	return r, nil
}

// podSlotArbiter is the durable half of pod-CIDR allocation: the reads and the
// reservation that must be decided ACROSS replicas rather than inside one
// process. *state.Store is the only implementation; it is named as an interface
// so the allocator's fail-closed behaviour is testable without a Postgres —
// state's persister seam is unexported, so no other package can fake it.
type podSlotArbiter interface {
	DurableBursts(ctx context.Context) ([]*state.Burst, bool, error)
	ReservedPodSlots(ctx context.Context, ttl time.Duration) (map[int]bool, bool, error)
	ReservePodSlot(ctx context.Context, slot int, burstID string, ttl time.Duration) (bool, bool, error)
}

// RefreshPodCIDRGauge recomputes the free pod-CIDR capacity from the same
// durable or in-memory state used by allocation. A failed durable read leaves
// the previous gauge value intact so an unavailable database is never reported
// as an empty pool.
func (d *Decider) RefreshPodCIDRGauge(ctx context.Context) error {
	if d.store != nil {
		durable, err := d.refreshPodCIDRGaugeDurable(ctx, d.store)
		if err != nil {
			return err
		}
		if durable {
			return nil
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	inUse := d.podCIDRInUseInMemoryLocked(nowUTC())
	d.Cost.SetPodCIDRFree(totalPodSlots - len(inUse))
	return nil
}

func (d *Decider) refreshPodCIDRGaugeDurable(ctx context.Context, a podSlotArbiter) (bool, error) {
	inUse, durable, err := d.podCIDRInUseDurable(ctx, a)
	if err != nil {
		return true, fmt.Errorf("pod cidr gauge refresh: %w", err)
	}
	if !durable {
		return false, nil
	}
	d.Cost.SetPodCIDRFree(totalPodSlots - len(inUse))
	return true, nil
}

// allocatePodCIDR returns the next /24 from podMeshCIDR. The burst's
// CNI uses this to assign per-pod IPs; cluster traffic to the /24
// rides over the tailnet (subnet route advertised by the burst).
//
// With a durable backend the database arbitrates, because the in-memory view
// two centrals each hold is not a shared view: both would find the same slot
// free and hand the same /24 to two live bursts. That is not stale bookkeeping
// — each burst advertises its /24 as a mesh subnet route, so two peers claim
// one prefix and routing breaks for both customers.
func (d *Decider) allocatePodCIDR(ctx context.Context, burstID string) (string, error) {
	if d.store != nil {
		cidr, durable, err := d.allocateDurablePodCIDR(ctx, d.store, burstID)
		if durable {
			return cidr, err
		}
	}
	return d.allocatePodCIDRInMemory()
}

// allocateDurablePodCIDR allocates against durable state. durable=false means
// there is no durable backend at all, and the caller belongs on the in-memory
// path; it is never returned for a read that merely failed.
//
// A failed read FAILS the allocation. Falling back to memory would allocate
// from a partial view, which is precisely how a duplicate /24 gets handed out.
// Failing rejects the workload and the customer retries; a duplicate /24
// silently breaks two customers' networking.
func (d *Decider) allocateDurablePodCIDR(ctx context.Context, a podSlotArbiter, burstID string) (string, bool, error) {
	inUse, durable, err := d.podCIDRInUseDurable(ctx, a)
	if err != nil {
		return "", true, err
	}
	if !durable {
		return "", false, nil
	}

	d.Cost.SetPodCIDRFree(totalPodSlots - len(inUse))

	for slot := podSlotMin; slot <= podSlotMax; slot++ {
		if inUse[slot] {
			continue
		}
		won, ok, err := a.ReservePodSlot(ctx, slot, burstID, podReservationTTL)
		if err != nil {
			return "", true, fmt.Errorf("pod cidr: reserve slot %d: %w", slot, err)
		}
		if !ok {
			return "", true, fmt.Errorf("pod cidr: reserve slot %d: durable backend went away mid-allocation", slot)
		}
		if !won {
			// Another replica took this slot between the read above and this
			// write — the very race the durable table decides. Keep walking
			// rather than failing: there is nothing wrong, just a busier pool.
			continue
		}
		return podCIDRForSlot(slot), true, nil
	}
	return "", true, errPodPoolExhausted(len(inUse))
}

func (d *Decider) podCIDRInUseDurable(ctx context.Context, a podSlotArbiter) (map[int]bool, bool, error) {
	bursts, durable, err := a.DurableBursts(ctx)
	if err != nil {
		return nil, true, fmt.Errorf("pod cidr: read durable bursts: %w", err)
	}
	if !durable {
		return nil, false, nil
	}
	reserved, _, err := a.ReservedPodSlots(ctx, podReservationTTL)
	if err != nil {
		return nil, true, fmt.Errorf("pod cidr: read durable reservations: %w", err)
	}

	inUse := make(map[int]bool, len(bursts)+len(reserved))
	// Live bursts anywhere in the fleet, not just this replica's: a burst row
	// holds its slot for as long as it exists, and the claim that reaps it frees
	// the slot for every replica at once.
	for _, b := range bursts {
		if slot, ok := podSlotOf(b.PodCIDR); ok {
			inUse[slot] = true
		}
	}
	// Reservations cover the allocate->persist window. Expired rows are already
	// filtered out by the read.
	maps.Copy(inUse, reserved)
	return inUse, true, nil
}

// allocatePodCIDRInMemory is the whole allocator when there is no durable
// backend: one process, so its own map IS the reservation set. Unchanged
// behaviour for the in-memory and OSS stores.
func (d *Decider) allocatePodCIDRInMemory() (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := nowUTC()
	inUse := d.podCIDRInUseInMemoryLocked(now)

	// Publish the free-slot count before allocating so the gauge reflects
	// capacity before this burst claims a slot (241 = slots 10..250 inclusive).
	d.Cost.SetPodCIDRFree(totalPodSlots - len(inUse))

	for slot := podSlotMin; slot <= podSlotMax; slot++ {
		if !inUse[slot] {
			d.reservedSlots[slot] = now
			return podCIDRForSlot(slot), nil
		}
	}
	return "", errPodPoolExhausted(len(inUse))
}

func (d *Decider) podCIDRInUseInMemoryLocked(now time.Time) map[int]bool {
	inUse := make(map[int]bool)

	// Persisted live bursts are authoritative: survives a central restart (the
	// old monotonic counter reset to 10 and re-handed live /24s) and reclaims
	// automatically when a burst is reaped (ClaimBurst removes it from the set).
	if d.store != nil {
		for _, b := range d.store.ListBursts() {
			if slot, ok := podSlotOf(b.PodCIDR); ok {
				inUse[slot] = true
			}
		}
	}
	// Reservations cover the allocate->persist window; drop any now confirmed in
	// the store or aged past the TTL (a Plan that failed before storing).
	for slot, at := range d.reservedSlots {
		if inUse[slot] || now.Sub(at) > podReservationTTL {
			delete(d.reservedSlots, slot)
			continue
		}
		inUse[slot] = true
	}
	return inUse
}

func podCIDRForSlot(slot int) string { return fmt.Sprintf("10.244.%d.0/24", slot) }

func errPodPoolExhausted(inUse int) error {
	return fmt.Errorf("pod cidr pool exhausted (%d live /24s in 10.244.%d-%d.0/24)",
		inUse, podSlotMin, podSlotMax)
}

// podSlotOf parses the slot N out of a "10.244.N.0/24" burst pod CIDR.
// Returns false for anything not in the burst pool shape.
func podSlotOf(cidr string) (int, bool) {
	var a, b, slot, host int
	if _, err := fmt.Sscanf(cidr, "%d.%d.%d.%d/24", &a, &b, &slot, &host); err != nil {
		return 0, false
	}
	if a != 10 || b != 244 || host != 0 || slot < 0 || slot > 255 {
		return 0, false
	}
	return slot, true
}

// estimateCostUSD is a coarse pre-execution estimate so we can return
// it in the API response. Reads through pkg/cloud/pricing so plan-time
// estimate and selection logic agree on what a SKU costs. Real billing
// accrues per-second on the running burst.
func estimateCostUSD(wl *workload.Workload, backend string, resources backends.ResourceRequirements) float64 {
	hourly, _ := hourlyAndSKU(wl, backend, resources)
	hours := 1.0
	if wl.Spec.Budget != nil && wl.Spec.Budget.Deadline > 0 {
		hours = wl.Spec.Budget.Deadline.Hours()
	}
	return hourly * hours
}

type gpuPriceQuote struct {
	upstreamHourly float64
	sku            string
	effectiveCount int
}

func enforceGCPRegionPricing(backend, region string) error {
	if backend != backends.TypeGCP {
		return nil
	}
	if pricing.GCPRegionPriced(region) {
		return nil
	}
	return fmt.Errorf("gcp region %q has no verified E2 price; priced regions: us-central1, us-east1, us-west1", region)
}

func configuredGCPRegion(zone, region string) string {
	if region != "" {
		return region
	}
	if zone != "" {
		if i := strings.LastIndex(zone, "-"); i > 0 {
			return zone[:i]
		}
		return ""
	}
	return "us-central1"
}

// enforceGPUPriceAdmission applies the workload's per-GPU customer-price cap
// before Plan performs any stateful action. A pinned SKU is rejected because
// the current Linode and AWS backends select from Kind+Count and do not launch
// GPUSpec.SKUs exactly; accepting it would price one shape and launch another.
func enforceGPUPriceAdmission(wl *workload.Workload, backend string, resources backends.ResourceRequirements) error {
	if wl.Spec.GPU == nil || resources.GPU == nil {
		return nil
	}

	capUSD := wl.Spec.GPU.MaxHourlyUSD
	if math.IsNaN(capUSD) || math.IsInf(capUSD, 0) || capUSD < 0 {
		return errGPUPriceCapNotFinite
	}

	// The AWS catalog is explicitly approximate and us-east-1-only, while the
	// launch region is configurable. It remains useful for uncapped estimates,
	// but cannot safely enforce a hard customer ceiling in another region.
	if capUSD > 0 && backend == backends.TypeAWS && len(resources.GPU.SKUs) == 0 {
		effCount := max(1, resources.GPU.Count)
		instanceType, err := aws.MapGPUType(resources.GPU)
		if err != nil {
			return fmt.Errorf("gpu price admission rejected: cap $%.4f/GPU/hour, resolved SKU %q, %d effective GPU(s): %w",
				capUSD, "unresolved", effCount, err)
		}
		return fmt.Errorf("gpu price admission rejected: cap $%.4f/GPU/hour, resolved SKU %q, %d effective GPU(s): %w; omit maxHourlyUSD only if uncapped AWS spend is acceptable, or select a backend with authoritative pricing (Linode)",
			capUSD, instanceType, effCount, errAWSGPUPriceUnverifiable)
	}

	quote, err := resolveGPUPriceQuote(backend, resources.GPU)
	if err != nil {
		sku := quote.sku
		if sku == "" {
			sku = wl.Spec.GPU.SKU
		}
		if sku == "" {
			sku = "unresolved"
		}
		return fmt.Errorf("gpu price admission rejected: cap $%.4f/GPU/hour, resolved SKU %q, %d effective GPU(s): %w",
			capUSD, sku, quote.effectiveCount, err)
	}
	if capUSD == 0 {
		return nil
	}

	customerPerGPU := pricing.CustomerPrice(quote.upstreamHourly) / float64(quote.effectiveCount)
	if customerPerGPU > capUSD {
		return fmt.Errorf("%w: resolved SKU %q costs $%.4f/GPU/hour for %d effective GPU(s), over cap $%.4f/GPU/hour; raise spec.gpu.maxHourlyUSD, request fewer GPUs, or choose a cheaper gpu.kind",
			errGPUPriceCapExceeded, quote.sku, customerPerGPU, quote.effectiveCount, capUSD)
	}
	return nil
}

// resolveGPUPriceQuote resolves the exact automatic GPU launch shape and its
// catalog hourly price. Linode admission and the plan's cost fields share this
// resolver so count/SKU pricing cannot drift. AWS catalog rates remain estimates
// for uncapped reporting only; enforceGPUPriceAdmission never treats them as a
// region-aware hard-cap source.
func resolveGPUPriceQuote(backend string, gpu *backends.GPUSpec) (gpuPriceQuote, error) {
	quote := gpuPriceQuote{effectiveCount: max(1, gpu.Count)}
	if len(gpu.SKUs) > 0 {
		quote.sku = gpu.SKUs[0]
		return quote, fmt.Errorf("backend %q does not honor explicitly pinned GPU SKUs exactly; drop spec.gpu.sku and select via gpu.kind + gpu.count", backend)
	}

	switch backend {
	case backends.TypeLinode:
		plan, err := linode.MapGPUType(gpu)
		if err != nil {
			return quote, err
		}
		quote.sku = plan
		hourly, ok := pricing.LinodePlan(plan)
		if !ok {
			return quote, fmt.Errorf("resolved GPU SKU %q has no exact catalog price", plan)
		}
		quote.upstreamHourly = hourly
		return quote, nil

	case backends.TypeAWS:
		instanceType, err := aws.MapGPUType(gpu)
		if err != nil {
			return quote, err
		}
		quote.sku = instanceType
		hourly, ok := pricing.AWSInstance(instanceType)
		if !ok {
			return quote, fmt.Errorf("resolved GPU SKU %q has no exact catalog price", instanceType)
		}
		quote.upstreamHourly = hourly
		return quote, nil
	}
	return quote, fmt.Errorf("backend %q does not support priced GPU admission", backend)
}

// hourlyAndSKU returns the per-hour upstream rate and the backend SKU
// (Fly machine class / AWS instance type / Linode plan or GPU kind) the
// burst will run as. Used both for the API estimate (estimateCostUSD) and
// for per-burst cost accrual (the cost meter multiplies the rate by the
// burst's actual lifetime). Every backend prices.
func hourlyAndSKU(wl *workload.Workload, backend string, resources backends.ResourceRequirements) (float64, string) {
	if resources.GPU != nil {
		quote, err := resolveGPUPriceQuote(backend, resources.GPU)
		if err != nil {
			return 0, ""
		}
		return quote.upstreamHourly, quote.sku
	}
	switch backend {
	case backends.TypeFlyIO:
		if wl.Spec.Machine != nil && wl.Spec.Machine.FlyType != "" {
			if p, ok := pricing.FlyMachine(wl.Spec.Machine.FlyType); ok {
				return p, wl.Spec.Machine.FlyType
			}
		}
		return pricing.EstimateFlyCPU(resources.CPUMillis, resources.MemoryMB), "fly-cpu"
	case backends.TypeLinode:
		return pricing.EstimateLinodeCPU(resources.MemoryMB)
	case backends.TypeAWS:
		return pricing.EstimateAWS(resources.MemoryMB)
	case backends.TypeAzure:
		return pricing.EstimateAzureCPU(resources.MemoryMB)
	case backends.TypeGCP:
		return pricing.EstimateGCPCPU(resources.MemoryMB)
	}
	return 0, ""
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is unrecoverable; a zero/partial buffer would
		// yield a predictable burst id — which is the bootstrap grant key.
		// Fail closed rather than mint a guessable secret.
		panic("decider: crypto/rand read failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// nowUTC is a thin indirection so storage_test.go can fake time.
var nowUTC = func() time.Time { return time.Now().UTC() }
