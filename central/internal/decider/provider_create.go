package decider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/pkg/backends"
	v1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/workload"
)

// ProviderCreateAdmission is the authoritative-admission seam: the durable
// record of intent that must commit BEFORE any paid Backend.CreateNode call,
// and the lease that fences the single attempt allowed to make it.
//
// It is satisfied by *lifecycle.Store and is deliberately the four calls of the
// one create saga (admit, claim, succeed, fail) and nothing else. Issue #8's
// command ledger extends the same rows and the same outbox through this seam;
// it does not get a second saga.
//
// Declared as an interface here rather than taking the concrete store so the
// decider can be driven across the process-death boundaries this path exists
// for — a store that commits admission and then refuses the claim, or accepts
// the create and then refuses the settlement — without a database.
type ProviderCreateAdmission interface {
	AdmitWorkload(ctx context.Context, req lifecycle.AdmissionRequest) (lifecycle.AdmissionResponse, error)
	ClaimProviderCreateForBurst(ctx context.Context, customerID, clusterID, burstID string, lease time.Duration) (lifecycle.ProviderCreateOperation, bool, error)
	MarkProviderCreateSucceeded(ctx context.Context, id int64, leaseToken, providerResourceID string) error
	MarkProviderCreateFailed(ctx context.Context, id int64, leaseToken, safeError string, retryAt time.Time, maxAttempts int) error
}

const (
	// providerCreateCanonicalVersion versions the admitted provider request.
	// It is the canonical_version of the workload row and the payload_version
	// of the operation, so a replay under a different encoding is an explicit
	// idempotency conflict rather than a silently different machine.
	providerCreateCanonicalVersion = 1

	// providerCreateLeaseTTL bounds one inline create attempt. It is long enough
	// to outlast the slowest provider create central waits on; after it expires,
	// the operation becomes visibly ambiguous for operator reconciliation and is
	// never automatically reclaimed for another paid create.
	providerCreateLeaseTTL = 10 * time.Minute

	// providerCreateTerminalAttempts forces MarkProviderCreateFailed down its
	// dead-letter branch. The inline path answers the customer terminally on
	// every failure, so it must not leave an operation a future leased worker
	// would execute after the submitter was told the burst failed. Phase 2
	// replaces this with a queued retry whose customer response is asynchronous.
	providerCreateTerminalAttempts = 1

	// providerCreateRetryBackoff only satisfies the store's "retry must be in
	// the future" precondition on a terminal transition; nothing reschedules a
	// dead-lettered operation.
	providerCreateRetryBackoff = time.Minute

	// providerCreateSettleTimeout bounds a settlement written on a context
	// detached from the request. Settlement is the write that stops a provider
	// resource being unaccounted for, and the client hanging up — which is
	// exactly what a slow create causes — must not be what prevents it.
	providerCreateSettleTimeout = 10 * time.Second
)

// ErrAdmissionIdentityMissing is returned when authoritative admission is
// enabled but the caller supplied no durable workload identity to admit under.
// It fails closed: an unkeyed submission cannot be replay-protected, and a
// provider create that cannot be replay-protected must not happen.
var ErrAdmissionIdentityMissing = errors.New("authoritative admission requires a durable workload identity (PlanOptions.WorkloadID)")

// ProviderCreateNotClaimableError reports that authoritative admission already
// holds this submission's provider-create operation in a state this attempt
// must not act on. It names the state so the refusal is legible in a log line
// without a database session.
//
// LeaseExpired separates the two kinds of `processing`, which an operator has to
// tell apart: a live attempt is in flight and will settle on its own, while an
// expired lease is an attempt that died with the request already dispatched.
// Neither is reclaimable, but only the second one needs a human.
type ProviderCreateNotClaimableError struct {
	WorkloadID   string
	BurstID      string
	State        string
	LeaseExpired bool
}

func (e *ProviderCreateNotClaimableError) Error() string {
	if e.LeaseExpired {
		return fmt.Sprintf("provider-create operation for burst %s is %s with an expired lease; "+
			"a prior attempt may already have dispatched the create, so it is ambiguous rather than retryable", e.BurstID, e.State)
	}
	return fmt.Sprintf("provider-create operation for burst %s is %s and cannot be claimed by this attempt", e.BurstID, e.State)
}

// providerCreateRequest is the exact provider mutation, resolved and serialized
// BEFORE admission and stored as the operation payload.
//
// Everything in it is a pure function of the workload, the tenant, the routed
// backend and the matched quote. Nothing here mints a credential, allocates a
// /24, takes a lease or calls a provider — that is the whole point: the durable
// record of what will be bought is written while nothing has been bought.
//
// The per-attempt ephemera CreateNode also needs — the mesh auth key, the pod
// CIDR, the coordination login server — are deliberately absent. They are not
// part of the request's identity: a reclaimed attempt mints fresh ones and is
// still executing the same admitted request.
type providerCreateRequest struct {
	Version           int      `json:"version"`
	BurstID           string   `json:"burst_id"`
	NodeName          string   `json:"node_name"`
	Backend           string   `json:"backend"`
	CloudAccountID    string   `json:"cloud_account_id,omitempty"`
	Region            string   `json:"region"`
	SKU               string   `json:"sku"`
	Tier              string   `json:"tier"`
	AgentImage        string   `json:"agent_image"`
	JoinMode          string   `json:"join_mode"`
	BootstrapEndpoint string   `json:"bootstrap_endpoint"`
	TSHostname        string   `json:"ts_hostname"`
	TSTags            []string `json:"ts_tags"`
	ModelVolume       string   `json:"model_volume,omitempty"`
	ShapeHash         string   `json:"shape_hash"`
	// PlacementDigest is the identity of the placement decision this create was
	// admitted under — the same digest a preview bound itself to. It is part of
	// the durable payload so a replay resumes the create the customer was shown,
	// not merely a machine of the same shape.
	//
	// Empty on a payload admitted before receipts existed. Those replay exactly
	// as they did, deliberately: refusing them would turn a live in-flight
	// create into an ambiguous one at deploy time, which is the outcome
	// authoritative admission exists to avoid.
	PlacementDigest string                        `json:"placement_digest,omitempty"`
	Resources       backends.ResourceRequirements `json:"resources"`
}

// resolveProviderCreateRequest builds the request for a freshly minted burst
// ID. Pure by construction: every helper it calls is a pure function.
func resolveProviderCreateRequest(wl *workload.Workload, opts handlers.PlanOptions, burstID, backendName, cloudAccountID, region, sku, shapeHash, placementDigest string, resources backends.ResourceRequirements) providerCreateRequest {
	return providerCreateRequest{
		Version:           providerCreateCanonicalVersion,
		BurstID:           burstID,
		NodeName:          backends.NodeNamePrefix + burstID[len("burst_"):],
		Backend:           backendName,
		CloudAccountID:    cloudAccountID,
		Region:            region,
		SKU:               sku,
		Tier:              resolveNetworkingTier(wl),
		AgentImage:        burstImageForBackend(backendName),
		JoinMode:          backends.JoinModeKubelet,
		BootstrapEndpoint: bootstrapEndpointFor(opts.ClusterID),
		TSHostname:        tsHostnameFor(burstID),
		TSTags:            tsTagsFor(opts.CustomerID),
		ModelVolume:       wl.Spec.ModelVolume,
		ShapeHash:         shapeHash,
		PlacementDigest:   placementDigest,
		Resources:         resources,
	}
}

func (r providerCreateRequest) nodeIdentityLabels(clusterID string) map[string]string {
	return buildNodeIdentityLabels(r.BurstID, clusterID, r.Backend, r.Resources.Region, r.Resources)
}

// buildNodeIdentityLabels produces the stable identity labels for a burst
// node. The label set is deterministic and pure: it depends on the durable
// provider request plus the cluster identity already fenced by the authoritative
// admission row. Keeping the cluster ID out of the v1 provider payload preserves
// replay compatibility while still rendering the identity that admission owns.
func buildNodeIdentityLabels(burstID, clusterID, backendName, region string, resources backends.ResourceRequirements) map[string]string {
	labels := map[string]string{
		v1.LabelBurstID:       burstID,
		v1.LabelProviderClass: backendFamily(backendName),
	}
	if clusterID != "" {
		labels[v1.LabelClusterID] = clusterID
	}
	if region != "" {
		labels[v1.LabelRegion] = region
	}
	if resources.GPU != nil && resources.GPU.Kind != "" {
		labels[v1.LabelGPUKind] = resources.GPU.Kind
		labels[v1.LabelGPUCount] = v1.GPUCountLabelValue(resources.GPU.Count)
	}
	return labels
}

// backendFamily maps a backend name to its stable provider family for the
// provider-class label. This is the customer-visible identity — never an
// account ID or internal alias.
func backendFamily(backendName string) string {
	switch backendName {
	case backends.TypeLinode:
		return "linode"
	case backends.TypeFlyIO:
		return "flyio"
	case backends.TypeAWS:
		return "aws"
	case backends.TypeGCP:
		return "gcp"
	case backends.TypeAzure:
		return "azure"
	default:
		return backendName
	}
}

// validateNodeIdentityLabels checks that every generated label key/value
// passes Kubernetes canonical validation. Invalid identity must fail closed
// before admission.
func validateNodeIdentityLabels(labels map[string]string) error {
	if len(labels) > 6 {
		return fmt.Errorf("node identity labels contain %d entries, want at most 6", len(labels))
	}
	for k, val := range labels {
		if err := v1.ValidateLabelKey(k); err != nil {
			return fmt.Errorf("node identity label: %w", err)
		}
		if err := v1.ValidateLabelValue(val); err != nil {
			return fmt.Errorf("node identity label %s=%q: %w", k, val, err)
		}
	}
	if gpuCount, ok := labels[v1.LabelGPUCount]; ok {
		if err := v1.ValidateGPUCountLabel(gpuCount); err != nil {
			return err
		}
	}
	return nil
}

// nodeSpec renders the admitted request into the provider call, adding only the
// per-attempt ephemera. Building the spec from the ADMITTED request — not from
// the caller's locals — is what makes "the create executed is the create
// admitted" an invariant rather than a comment.
func (r providerCreateRequest) nodeSpec(clusterID, authKey, podCIDR, loginServer string) *backends.NodeSpec {
	return &backends.NodeSpec{
		Name:              r.NodeName,
		BurstID:           r.BurstID,
		AgentImage:        r.AgentImage,
		JoinMode:          r.JoinMode,
		BootstrapEndpoint: r.BootstrapEndpoint,
		TSAuthKey:         authKey,
		TSHostname:        r.TSHostname,
		TSTags:            r.TSTags,
		LoginServer:       loginServer,
		PodCIDR:           podCIDR,
		Tier:              r.Tier,
		Region:            r.Resources.Region,
		Resources:         r.Resources,
		ModelVolume:       r.ModelVolume,
		NodeLabels:        r.nodeIdentityLabels(clusterID),
	}
}

// admittedProviderCreate is one authorized attempt at one provider create: the
// request to execute and, when authoritative admission is enabled, the fencing
// lease that authorizes exactly this attempt to execute it.
//
// admitted=false is the compatibility seam, not a lost lease: it means no
// lifecycle store is wired and there is nothing durable to settle. See
// WithLifecycleAdmission.
type admittedProviderCreate struct {
	request   providerCreateRequest
	clusterID string
	admitted  bool
	op        lifecycle.ProviderCreateOperation
}

// WithLifecycleAdmission makes PostgreSQL the authority for workload admission
// and provider-create intent. Returns the same decider for chaining.
//
// Leaving it unwired is the EXPLICIT compatibility seam for the OSS/dev
// single-binary build: the decider resolves and executes the identical provider
// request inline, exactly as it always has, with no durable record of intent
// and therefore no protection against a crash between the provider create and
// the burst record. That trade is acceptable for a local operator running their
// own machines and is not acceptable for a tenant being billed, which is why
// the managed path wires this and the OSS path does not.
func (d *Decider) WithLifecycleAdmission(a ProviderCreateAdmission) *Decider {
	d.admission = a
	return d
}

// admitProviderCreate commits durable intent and leases the single attempt
// allowed to act on it. It returns the request to EXECUTE, which on a replay is
// the originally admitted request read back out of the operation row rather
// than the one this attempt just resolved.
func (d *Decider) admitProviderCreate(ctx context.Context, wl *workload.Workload, opts handlers.PlanOptions, request providerCreateRequest) (*admittedProviderCreate, error) {
	if d.admission != nil && opts.ClusterID == "" {
		return nil, errors.New("authoritative admission requires a cluster identity for node ownership")
	}
	if err := validateNodeIdentityLabels(request.nodeIdentityLabels(opts.ClusterID)); err != nil {
		return nil, fmt.Errorf("node identity label validation: %w", err)
	}
	if d.admission == nil {
		return &admittedProviderCreate{request: request, clusterID: opts.ClusterID}, nil
	}
	if opts.WorkloadID == "" {
		return nil, ErrAdmissionIdentityMissing
	}
	if request.SKU == "" {
		return nil, errors.New("authoritative admission requires a trusted SKU for the routed shape")
	}

	workloadSpec, err := json.Marshal(wl)
	if err != nil {
		return nil, fmt.Errorf("serialize workload spec for admission: %w", err)
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("serialize provider-create request for admission: %w", err)
	}

	// The lifecycle idempotency key is the workload ID, not the client's
	// Idempotency-Key. The workload ID is minted WITH the durable claim that
	// key already elected, so it is exactly as stable across a client's retries
	// while keeping a client-chosen — and routinely secret-adjacent — value out
	// of a second table. See handlers/idempotency.go on why that value is never
	// stored or echoed.
	resp, err := d.admission.AdmitWorkload(ctx, lifecycle.AdmissionRequest{
		CustomerID:            opts.CustomerID,
		ClusterID:             opts.ClusterID,
		IdempotencyKey:        opts.WorkloadID,
		CanonicalVersion:      providerCreateCanonicalVersion,
		WorkloadID:            opts.WorkloadID,
		BurstID:               request.BurstID,
		WorkloadSpec:          workloadSpec,
		BurstSpec:             requestJSON,
		Provider:              request.Backend,
		Region:                request.Region,
		CloudAccountID:        request.CloudAccountID,
		SKU:                   request.SKU,
		ProviderCreatePayload: requestJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("admit workload: %w", err)
	}

	op, claimed, err := d.admission.ClaimProviderCreateForBurst(ctx, opts.CustomerID, opts.ClusterID, resp.BurstID, providerCreateLeaseTTL)
	if err != nil {
		return nil, fmt.Errorf("claim provider-create operation: %w", err)
	}
	if !claimed {
		// A replay that reaches here has authoritative state saying this
		// submission's create is already succeeded, already terminal, owned by a
		// live attempt, or owned by an attempt that died holding the lease. Every
		// one of those is a reason NOT to buy a second machine — and where one may
		// already exist, the refusal is an AMBIGUOUS create rather than a clean
		// failure, so the caller retains the admission reservation and routes it
		// to manual attention instead of quietly forgetting a machine that is
		// billing.
		refusal := &ProviderCreateNotClaimableError{
			WorkloadID: resp.WorkloadID, BurstID: resp.BurstID,
			State: op.State, LeaseExpired: op.LeaseExpired,
		}
		if providerCreateMayHoldAResource(op.State) {
			return nil, &providerCreateError{err: refusal, ambiguous: true}
		}
		return nil, refusal
	}

	admittedRequest, err := decodeAdmittedRequest(op)
	if err != nil {
		return nil, d.settleFailedProviderCreate(ctx, &admittedProviderCreate{admitted: true, op: op}, err)
	}
	if err := validateNodeIdentityLabels(admittedRequest.nodeIdentityLabels(opts.ClusterID)); err != nil {
		invalid := fmt.Errorf("admitted node identity label validation: %w", err)
		return nil, d.settleFailedProviderCreate(ctx, &admittedProviderCreate{admitted: true, op: op}, invalid)
	}
	// A replay resumes the ORIGINAL request, so the shape a crashed attempt
	// admitted is the shape this attempt buys. Refuse rather than reconcile if
	// the durable request disagrees with the quote this attempt matched: a
	// silently different machine is the failure mode admission exists to stop.
	if admittedRequest.Backend != request.Backend || admittedRequest.Region != request.Region ||
		admittedRequest.SKU != request.SKU || admittedRequest.ShapeHash != request.ShapeHash {
		mismatch := fmt.Errorf("admitted provider request (%s/%s/%s) does not match the matched quote (%s/%s/%s)",
			admittedRequest.Backend, admittedRequest.Region, admittedRequest.SKU,
			request.Backend, request.Region, request.SKU)
		return nil, d.settleFailedProviderCreate(ctx, &admittedProviderCreate{admitted: true, op: op}, mismatch)
	}
	// The placement decision is compared on the same terms, and only when the
	// admitted payload carries one: an empty stored digest is a pre-receipt
	// admission resuming, not a substituted placement.
	if admittedRequest.PlacementDigest != "" && admittedRequest.PlacementDigest != request.PlacementDigest {
		mismatch := errors.New("admitted provider request was admitted under a different placement decision")
		return nil, d.settleFailedProviderCreate(ctx, &admittedProviderCreate{admitted: true, op: op}, mismatch)
	}
	if !resp.Inserted {
		slog.Warn("resuming an already-admitted provider create; a prior attempt committed intent without settling it",
			"customer", opts.CustomerID, "cluster", opts.ClusterID,
			"workload", resp.WorkloadID, "burst", resp.BurstID, "attempts", op.Attempts)
	}
	return &admittedProviderCreate{request: admittedRequest, clusterID: opts.ClusterID, admitted: true, op: op}, nil
}

// providerCreateMayHoldAResource reports whether an unclaimable operation state
// can be sitting on a provider resource the tenant is already paying for.
// pending and failed are the two that cannot: nothing has been created under
// them, or a prior attempt proved its create did not land.
func providerCreateMayHoldAResource(state string) bool {
	switch state {
	case lifecycle.OperationSucceeded, lifecycle.OperationDeadLetter, lifecycle.OperationProcessing:
		return true
	}
	return false
}

func decodeAdmittedRequest(op lifecycle.ProviderCreateOperation) (providerCreateRequest, error) {
	if op.PayloadVersion != providerCreateCanonicalVersion {
		return providerCreateRequest{}, fmt.Errorf("admitted provider request has unsupported payload version %d", op.PayloadVersion)
	}
	var request providerCreateRequest
	if err := json.Unmarshal(op.Payload, &request); err != nil {
		return providerCreateRequest{}, fmt.Errorf("decode admitted provider request: %w", err)
	}
	if request.Version != providerCreateCanonicalVersion {
		return providerCreateRequest{}, fmt.Errorf("admitted provider request has unsupported version %d", request.Version)
	}
	if request.BurstID != op.BurstID || request.Backend != op.Provider || request.SKU != op.SKU {
		return providerCreateRequest{}, errors.New("admitted provider request disagrees with its own operation row")
	}
	return request, nil
}

// succeedProviderCreate commits the provider resource ID under the attempt's
// lease. A failure here means a machine exists that authoritative state does
// not record, so it is reported as an AMBIGUOUS create: the caller retains the
// admission reservation, flags manual attention, and the orphan sweep is the
// backstop that eventually reclaims the node.
func (d *Decider) succeedProviderCreate(ctx context.Context, adm *admittedProviderCreate, backendID string) error {
	if adm == nil || !adm.admitted {
		return nil
	}
	settleCtx, cancel := settleContext(ctx)
	defer cancel()
	if err := d.admission.MarkProviderCreateSucceeded(settleCtx, adm.op.ID, adm.op.LeaseToken, backendID); err != nil {
		slog.Error("provider create succeeded but its authoritative settlement failed; the node is running and unrecorded",
			"burst", adm.op.BurstID, "workload", adm.op.WorkloadID, "backend_id", backendID, "error", err)
		return &providerCreateError{
			err:       fmt.Errorf("settle provider create for burst %s: %w", adm.op.BurstID, err),
			ambiguous: true,
		}
	}
	return nil
}

// failProviderCreate settles the attempt terminally under its lease and reports
// whether that settlement is durable.
//
// It is NOT best effort. An unsettled operation keeps a lease that will expire,
// and an expired create lease is ambiguous by construction — nothing durable
// records how far the attempt got before it stopped writing. Reporting the
// caller's clean "the provider refused" upwards while that row survives is the
// one answer this path must never give: it releases the tenant's admission
// reservation for a burst whose create may in fact have landed.
//
// nil means the failure is recorded and the caller's own error is the whole
// truth. A non-nil return is an AMBIGUOUS provider-create error that REPLACES
// it, so the caller retains the reservation and routes the submission to
// manual attention.
func (d *Decider) failProviderCreate(ctx context.Context, adm *admittedProviderCreate, cause error) error {
	if adm == nil || !adm.admitted || cause == nil {
		return nil
	}
	settleCtx, cancel := settleContext(ctx)
	defer cancel()
	err := d.admission.MarkProviderCreateFailed(settleCtx, adm.op.ID, adm.op.LeaseToken,
		cause.Error(), time.Now().Add(providerCreateRetryBackoff), providerCreateTerminalAttempts)
	if err == nil {
		return nil
	}
	slog.Error("could not settle a failed provider create; the outcome of this create is now unresolved",
		"burst", adm.op.BurstID, "workload", adm.op.WorkloadID, "error", err)
	// The settlement error names the burst and the store's own failure, never
	// the provider's response: the caller already holds its own classified error
	// for that, and a raw provider payload has no business travelling further up
	// than the durable safe-error column.
	return &providerCreateError{
		err: fmt.Errorf("provider_create_ambiguous: settlement for burst %s did not commit, "+
			"so this create's outcome is unresolved: %w", adm.op.BurstID, err),
		ambiguous: true,
	}
}

// settleFailedProviderCreate settles a refusal raised inside the leased window
// and returns the error the caller must answer with: its own cause when the
// settlement committed, and the ambiguous settlement failure when it did not.
func (d *Decider) settleFailedProviderCreate(ctx context.Context, adm *admittedProviderCreate, cause error) error {
	if settleErr := d.failProviderCreate(ctx, adm, cause); settleErr != nil {
		return settleErr
	}
	return cause
}

// settleContext detaches a settlement write from the request's cancellation
// while keeping it bounded and keeping the request's values (trace, logging).
func settleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), providerCreateSettleTimeout)
}

// providerCreateSafeError renders the durable failure text for a create,
// recording the ambiguity classification EXPLICITLY. An operator reading a
// dead-lettered row has to learn whether a machine may exist from the row
// itself, not by re-deriving it from a provider's error prose.
func providerCreateSafeError(ambiguous bool, backendName string, err error) error {
	if ambiguous {
		return fmt.Errorf("provider_create_ambiguous: %s may hold a machine for this burst: %w", backendName, err)
	}
	return fmt.Errorf("provider_create_failed: %s refused the create: %w", backendName, err)
}
