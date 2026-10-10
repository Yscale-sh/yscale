package decider

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/aws"
	"github.com/yscale-sh/yscale/pkg/backends/linode"
	"github.com/yscale-sh/yscale/pkg/workload"
)

// placementCandidateProviders is the supported provider set, in the fixed order
// candidates are enumerated and rendered.
//
// The order is part of state.PlacementCandidateSetVersion and deliberately does
// NOT depend on the request: an order derived from the routed backend or from
// price would make two previews of one unchanged workload serialize
// differently, and the digest that binds preview to launch covers this list.
var placementCandidateProviders = []string{
	backends.TypeLinode,
	backends.TypeAWS,
	backends.TypeFlyIO,
	backends.TypeGCP,
	backends.TypeAzure,
}

// placementQuoteTTL is how long one issued decision may be presented back at
// launch. It matches the billing price quote's own validity window: a receipt
// that outlived the quote it was priced from would bind a launch to a number
// nothing is holding.
const placementQuoteTTL = 5 * time.Minute

// PlacementRefusal is a decision that resolved to no placement, carrying the
// stable customer-facing reason and — when the decision got far enough to have
// one — the bounded receipt that explains it.
//
// It WRAPS the original error and reports its message verbatim, so every
// existing caller, log line and test sees exactly the refusal it saw before.
// The stable code is read structurally through the PlacementRejection interface
// (see handlers), which is how the API layer stays free of a decider import.
type PlacementRefusal struct {
	Reason  string
	Receipt *state.PlacementReceipt
	err     error
}

func (e *PlacementRefusal) Error() string { return e.err.Error() }
func (e *PlacementRefusal) Unwrap() error { return e.err }

// PlacementRejectionReason exposes the stable code without exporting the type
// to the API layer. Matches the fabricProvisioningError seam.
func (e *PlacementRefusal) PlacementRejectionReason() string { return e.Reason }

// PlacementRejectionReceipt exposes the bounded explanation, or nil when the
// refusal happened before a receipt could be built.
func (e *PlacementRefusal) PlacementRejectionReceipt() *state.PlacementReceipt { return e.Receipt }

// Sentinels that let the placement decision classify a refusal into a stable
// customer code WITHOUT parsing an error message. Each one is wrapped into the
// existing text so no error string changes.
var (
	errBackendNotConfigured    = errors.New("backend not configured")
	errUnknownBackend          = errors.New("unknown backend")
	errGPUUnsupportedByBackend = errors.New("does not support GPU workloads")
	errGPUPriceCapExceeded     = errors.New("gpu price cap exceeded")
	errGPUPriceCapNotFinite    = errors.New("gpu price admission rejected: maxHourlyUSD must be a finite value >= 0")
	errAWSGPUPriceUnverifiable = errors.New("authoritative region-aware AWS GPU pricing is unavailable")
	errNoTrustedPrice          = errors.New("no trusted price")
	errStorageBackendPin       = errors.New("storage volume pinned to another backend")
	errStorageRegionPin        = errors.New("storage volume pinned to another region")
)

// placementDecision is one resolved placement: the values the quote, the
// provider-create request and the receipt are all built from, resolved exactly
// once so they cannot drift from each other.
//
// Producing it performs no side effect of any kind — no credential mint, no
// /24, no lease, no provider call, no volume timestamp. That is what lets the
// same function serve a preview a customer is only looking at and the launch
// that spends their money.
type placementDecision struct {
	backend        string
	chosen         backends.Backend
	cloudAccountID string
	resources      backends.ResourceRequirements
	region         string
	sku            string
	hourly         float64
	shapeHash      string
	rateMicroUSD   int64
	maxMicroUSD    int64
	duration       time.Duration

	// receipt is UNSEALED: it carries the decision content and no issuance
	// identity, so its digest is already the stable one. Quote seals a copy per
	// issuance; PlanQuoted only ever compares digests.
	receipt state.PlacementReceipt
}

// digest returns the decision's stable identity.
func (p *placementDecision) digest() (string, error) { return p.receipt.ComputeDigest() }

// decide is the single placement path: preview, Plan and PlanQuoted all reach
// the provider decision through here, so a customer can never be shown one
// answer and charged for another.
//
// The refusal ORDER is the order admission has always used — route, then the
// tenant's provider account, then the resolved shape, then GPU price admission,
// then region pricing, then a trusted price — because those refusals are the
// documented contract of this path and reordering them would change which one a
// misconfigured submission is told about.
func (d *Decider) decide(wl *workload.Workload, opts handlers.PlanOptions) (*placementDecision, error) {
	if wl == nil {
		return nil, fmt.Errorf("workload is nil")
	}
	backendName, err := d.route(wl)
	if err != nil {
		return nil, &PlacementRefusal{Reason: routeRefusalReason(err), err: err}
	}
	// Resolved before the account check but reported after it, so the shape is
	// available for the receipt while the refusal a caller sees stays the one
	// this path has always answered with first.
	resources, resourcesErr := resourcesFor(wl)
	chosen, cloudAccountID, accountErr := d.backendForTenant(backendName, opts.CustomerID)

	// One receipt skeleton for every outcome below. refuse() finishes it with
	// the routed provider's stable code and no selection; the success path
	// finishes it with the selected candidate. Building both from the same
	// skeleton is what keeps a refusal and an acceptance describing the same
	// request under the same constraints.
	receipt := d.baseReceipt(wl, opts, resources)
	refuse := func(reason string, err error) error {
		refusal := &PlacementRefusal{Reason: reason, err: err}
		if resourcesErr != nil {
			return refusal
		}
		explained := receipt
		explained.Candidates = d.candidates(wl, resources, backendName, nil, reason)
		if sealErr := explained.Seal("", time.Time{}, 0); sealErr != nil {
			return refusal
		}
		if explained.Validate() != nil {
			return refusal
		}
		refusal.Receipt = &explained
		return refusal
	}

	if accountErr != nil {
		reason := state.PlacementRejectProviderAccountUnavail
		if errors.Is(accountErr, errBackendNotConfigured) || errors.Is(accountErr, errUnknownBackend) {
			reason = state.PlacementRejectProviderNotConfigured
		}
		return nil, refuse(reason, accountErr)
	}
	if resourcesErr != nil {
		return nil, resourcesErr
	}

	receipt.Constraints.AccountMode = state.PlacementAccountPlatform
	if cloudAccountID != "" {
		receipt.Constraints.AccountMode = state.PlacementAccountTenant
	}
	receipt.Constraints.AccountEligible = true
	// WHICH of the tenant's accounts was selected, bound one-way. The mode alone
	// cannot carry this: two BYOC accounts on one provider serve the identical
	// region and shape, so a digest that stopped at "tenant" would let a preview
	// taken under one account launch under the other. Set here — before every
	// refusal below copies the skeleton — so a refusal describes the same
	// selection an acceptance would have.
	receipt.AccountBinding = state.PlacementAccountBinding(opts.CustomerID, cloudAccountID)

	// The routed provider's own ladder. Each rung is the existing admission
	// check, called — not reimplemented — and mapped onto a stable code.
	if err := enforceGPUPriceAdmission(wl, backendName, resources); err != nil {
		reason, stable := gpuAdmissionRefusalReason(err, backendName, resources.GPU)
		if !stable {
			return nil, err
		}
		return nil, refuse(reason, err)
	}
	region := resources.Region
	if region == "" {
		region = d.defaultPlacementRegion(backendName)
	}
	if err := enforceGCPRegionPricing(backendName, region); err != nil {
		return nil, refuse(state.PlacementRejectRegionNotAllowed, err)
	}
	hourly, sku := hourlyAndSKU(wl, backendName, resources)
	if hourly <= 0 || sku == "" {
		return nil, refuse(state.PlacementRejectSKUNotSupported,
			fmt.Errorf("%w for %s workload shape", errNoTrustedPrice, backendName))
	}

	// Existing volumes are a HARD placement constraint, and the read that
	// discovers them here touches nothing: a workload that cannot land where its
	// data already is must be refused while the decision is still free, not
	// after a hold, a /24 and a mesh key. PlanQuoted keeps its own check against
	// the resolution that also writes, as defence in depth.
	storagePinBackend, storagePinRegion, err := d.storageAffinity(wl, opts.CustomerID)
	if err != nil {
		return nil, err
	}
	receipt.Constraints.StorageBackendPin = storagePinBackend
	receipt.Constraints.StorageRegionPin = storagePinRegion
	if storagePinBackend != "" && storagePinBackend != backendName {
		return nil, refuse(state.PlacementRejectStorageProviderMismatch,
			fmt.Errorf("%w: volume is on %q but workload routes to %q (set spec.backend explicitly to override)",
				errStorageBackendPin, storagePinBackend, backendName))
	}
	// The volume's DC is the second half of that same constraint, and it is
	// compared against the region the REQUEST pinned rather than the resolved
	// one. A workload that pinned no region lands in the provider account's own
	// default, which central does not observe until the create returns — so the
	// only honest comparison is against a region the submitter actually named.
	// Refusing on the priced-region fallback instead would reject a correct
	// reuse on a value nothing looked up.
	if storagePinRegion != "" && resources.Region != "" && storagePinRegion != resources.Region {
		return nil, refuse(state.PlacementRejectStorageRegionMismatch,
			fmt.Errorf("%w: volume is in %q but workload requests %q (drop spec.region to launch where the data is)",
				errStorageRegionPin, storagePinRegion, resources.Region))
	}

	shapeHash, err := providerShapeHash(backendName, cloudAccountID, resources)
	if err != nil {
		return nil, err
	}

	duration := prepaidQuoteMaxDuration
	if wl.Spec.Budget != nil && wl.Spec.Budget.Deadline > 0 && wl.Spec.Budget.Deadline < duration {
		duration = wl.Spec.Budget.Deadline
	}
	rate := int64(math.Ceil(hourly * 1_000_000))
	maximum := int64(math.Ceil(float64(rate) * duration.Hours()))
	if wl.Spec.Budget != nil && wl.Spec.Budget.MaxUSD > 0 {
		budgetMaximum := int64(math.Ceil(wl.Spec.Budget.MaxUSD * 1_000_000))
		if budgetMaximum < maximum {
			maximum = budgetMaximum
		}
	}

	selected := state.PlacementSelection{
		Provider: backendName, Region: region, SKU: sku,
		CPUMillis: resources.CPUMillis, MemoryMB: resources.MemoryMB,
		AccountMode: receipt.Constraints.AccountMode, HourlyMicroUSD: rate,
		MaximumDurationSeconds: int64(duration.Seconds()), MaximumChargeMicroUSD: maximum,
	}
	if resources.GPU != nil {
		selected.GPUKind = resources.GPU.Kind
		selected.GPUCount = resources.GPU.Count
	}
	receipt.Selected = selected
	receipt.Candidates = d.candidates(wl, resources, backendName, &selected, "")
	return &placementDecision{
		backend: backendName, chosen: chosen, cloudAccountID: cloudAccountID,
		resources: resources, region: region, sku: sku, hourly: hourly, shapeHash: shapeHash,
		rateMicroUSD: rate, maxMicroUSD: maximum, duration: duration,
		receipt: receipt,
	}, nil
}

// defaultPlacementRegion reports only a region the selected provider is known
// to use. Linode can fall back across regions based on live capacity, so its
// preferred region is not a placement promise. Never inherit another provider's
// configuration: this value is signed into the receipt and persisted in billing
// and lifecycle records as well as exposed on the burst Node.
func (d *Decider) defaultPlacementRegion(backend string) string {
	switch backend {
	case backends.TypeFlyIO:
		if d.cfg.FlyRegion != "" {
			return d.cfg.FlyRegion
		}
		return "ord"
	case backends.TypeAWS:
		if d.cfg.AWSRegion != "" {
			return d.cfg.AWSRegion
		}
		return "us-east-1"
	case backends.TypeGCP:
		return d.gcpRegion
	case backends.TypeAzure:
		if d.cfg.AzureLocation != "" {
			return d.cfg.AzureLocation
		}
		return "eastus"
	default:
		return ""
	}
}

// baseReceipt fills everything that is a function of the submission and the
// tenant alone: who asked, for what shape, under which cluster decision, and
// bounded by which hard constraints.
func (d *Decider) baseReceipt(wl *workload.Workload, opts handlers.PlanOptions,
	resources backends.ResourceRequirements) state.PlacementReceipt {
	receipt := state.PlacementReceipt{
		Version:            state.PlacementReceiptVersion,
		Tenant:             opts.CustomerID,
		RequestedClusterID: opts.ClusterPlacement.RequestedClusterID,
		GrantedClusterID:   opts.ClusterPlacement.GrantedClusterID,
		ClusterMode:        opts.ClusterPlacement.Mode,
		ClusterRule:        opts.ClusterPlacement.Rule,
		ClusterRuleVersion: opts.ClusterPlacement.RuleVersion,
		Requested: state.PlacementRequest{
			CPUMillis: resources.CPUMillis,
			MemoryMB:  resources.MemoryMB,
		},
		Constraints: state.PlacementConstraints{
			BackendPin: wl.Spec.Backend,
			RegionPin:  wl.Spec.Region,
		},
		PricingVersion:      state.PlacementPricingVersion,
		CandidateSetVersion: state.PlacementCandidateSetVersion,
		// Central prices from a curated catalog but observes no provider
		// capacity before the create call, so availability is only discoverable
		// at create time. Saying "quoted" here would promise a machine nobody
		// checked for.
		AvailabilityConfidence: state.PlacementAvailabilityCreateTimeOnly,
	}
	// The granted cluster falls back to the routed cluster id so a submission
	// that reached the decider through a path carrying no placement decision
	// (the OSS single-binary path) still records where it landed.
	if receipt.GrantedClusterID == "" {
		receipt.GrantedClusterID = opts.ClusterID
	}
	if resources.GPU != nil {
		receipt.Requested.GPUKind = resources.GPU.Kind
		receipt.Requested.GPUCount = resources.GPU.Count
	}
	if wl.Spec.GPU != nil {
		receipt.Requested.GPUReliability = wl.Spec.GPU.Reliability
		if wl.Spec.GPU.MaxHourlyUSD > 0 {
			receipt.Constraints.MaxHourlyMicroUSDPerGPU = int64(math.Ceil(wl.Spec.GPU.MaxHourlyUSD * 1_000_000))
		}
	}
	if wl.Spec.Budget != nil {
		if wl.Spec.Budget.MaxUSD > 0 {
			receipt.Constraints.MaxChargeMicroUSD = int64(math.Ceil(wl.Spec.Budget.MaxUSD * 1_000_000))
		}
		if wl.Spec.Budget.Deadline > 0 {
			receipt.Constraints.DeadlineSeconds = int64(wl.Spec.Budget.Deadline.Seconds())
		}
	}
	return receipt
}

// candidates enumerates the supported provider set in fixed order. selected is
// the resolved shape for the routed provider, or nil when the routed provider
// itself was refused — in which case routedReason is the code that refused it.
//
// A rejected candidate carries a code and no price: publishing a rate for a
// provider that cannot serve the request would read as an offer central is not
// making.
func (d *Decider) candidates(wl *workload.Workload, resources backends.ResourceRequirements,
	routed string, selected *state.PlacementSelection, routedReason string) []state.PlacementCandidate {
	out := make([]state.PlacementCandidate, 0, len(placementCandidateProviders))
	for _, provider := range placementCandidateProviders {
		if provider == routed {
			if selected != nil {
				out = append(out, state.PlacementCandidate{
					Provider: provider, Region: selected.Region, SKU: selected.SKU,
					GPUKind: selected.GPUKind, GPUCount: selected.GPUCount,
					CPUMillis: selected.CPUMillis, MemoryMB: selected.MemoryMB,
					AccountMode: selected.AccountMode, HourlyMicroUSD: selected.HourlyMicroUSD,
					Selected: true,
				})
				continue
			}
			out = append(out, state.PlacementCandidate{Provider: provider, Reason: routedReason})
			continue
		}
		out = append(out, state.PlacementCandidate{
			Provider: provider,
			Reason:   d.unroutedReason(wl, resources, provider),
		})
	}
	return out
}

// unroutedReason is why the current routing policy did not consider a provider
// for this request. It answers from the same rules route() applies, so the
// receipt cannot claim an alternative routing would never have chosen.
func (d *Decider) unroutedReason(wl *workload.Workload, resources backends.ResourceRequirements, provider string) string {
	// An explicit spec.backend is the constraint that excluded every other
	// provider, whatever they could otherwise have served.
	switch wl.Spec.Backend {
	case "", backends.BackendAuto:
	default:
		return state.PlacementRejectProviderNotAllowed
	}
	if wl.Spec.GPU == nil {
		// Auto CPU routing has one target. The others are not refused by their
		// catalogs — they are refused by the routing policy.
		return state.PlacementRejectProviderNotAllowed
	}
	switch provider {
	case backends.TypeFlyIO, backends.TypeGCP, backends.TypeAzure:
		// These backends serve no GPU at all; route() refuses them outright.
		return state.PlacementRejectGPUKindNotSupported
	}
	if resources.GPU == nil {
		return state.PlacementRejectProviderNotAllowed
	}
	return gpuShapeRejectionReason(provider, resources.GPU)
}

// routeRefusalReason maps a routing refusal onto a stable code without reading
// its message: the refusals carry sentinels for exactly this.
func routeRefusalReason(err error) string {
	switch {
	case errors.Is(err, errGPUUnsupportedByBackend):
		return state.PlacementRejectGPUKindNotSupported
	case errors.Is(err, errUnknownBackend):
		return state.PlacementRejectProviderNotAllowed
	}
	return state.PlacementRejectProviderNotAllowed
}

// gpuAdmissionRefusalReason classifies a GPU admission refusal. stable=false
// means the refusal is not a placement rejection at all (an invalid spec value)
// and belongs to the caller unchanged.
func gpuAdmissionRefusalReason(err error, backend string, gpu *backends.GPUSpec) (string, bool) {
	switch {
	case errors.Is(err, errGPUPriceCapNotFinite):
		return "", false
	case errors.Is(err, errGPUPriceCapExceeded), errors.Is(err, errAWSGPUPriceUnverifiable):
		// Both are the per-GPU ceiling refusing the placement: one because the
		// resolved price is over it, the other because no price central trusts
		// can prove it is under it.
		return state.PlacementRejectPriceAboveMaxHourly, true
	}
	if gpu == nil {
		return state.PlacementRejectSKUNotSupported, true
	}
	return gpuShapeRejectionReason(backend, gpu), true
}

// gpuShapeRejectionReason re-derives WHY a backend's own GPU selector cannot
// place a shape, by asking that same selector narrower questions — never by
// reading its error text. A provider message is an operator diagnostic; the
// customer contract is the stable code, and deriving one from the other by
// string match is how a provider's copy edit becomes an API change.
func gpuShapeRejectionReason(backend string, gpu *backends.GPUSpec) string {
	mapGPU := gpuTypeMapperFor(backend)
	if mapGPU == nil {
		return state.PlacementRejectGPUKindNotSupported
	}
	if len(gpu.SKUs) > 0 {
		// A pinned SKU is refused by every current backend: none launches
		// GPUSpec.SKUs exactly, so pricing one and launching another is the
		// failure this refusal exists to prevent.
		return state.PlacementRejectSKUNotSupported
	}
	count := max(1, gpu.Count)
	if _, err := mapGPU(&backends.GPUSpec{Kind: gpu.Kind, Count: 1}); err != nil {
		return state.PlacementRejectGPUKindNotSupported
	}
	if _, err := mapGPU(&backends.GPUSpec{Kind: gpu.Kind, Count: count}); err != nil {
		return state.PlacementRejectGPUCountNotSupported
	}
	if _, err := mapGPU(&backends.GPUSpec{Kind: gpu.Kind, Count: count, CPUMillis: gpu.CPUMillis}); err != nil {
		return state.PlacementRejectInsufficientCPU
	}
	if _, err := mapGPU(&backends.GPUSpec{Kind: gpu.Kind, Count: count, MemoryMB: gpu.MemoryMB}); err != nil {
		return state.PlacementRejectInsufficientMemory
	}
	// The selector resolves the shape; what failed is the catalog's ability to
	// price the SKU it resolved.
	return state.PlacementRejectSKUNotSupported
}

// gpuTypeMapperFor returns the backend's own Kind+Count→SKU selector, the same
// one CreateNode uses. nil for a backend that serves no GPU.
func gpuTypeMapperFor(backend string) func(*backends.GPUSpec) (string, error) {
	switch backend {
	case backends.TypeLinode:
		return linode.MapGPUType
	case backends.TypeAWS:
		return aws.MapGPUType
	}
	return nil
}
