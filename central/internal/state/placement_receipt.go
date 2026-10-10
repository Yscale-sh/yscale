package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// PlacementReceipt is the one normalized placement decision central computes,
// shows a human before launch, and persists on the workload it launched.
//
// It lives in state because state is the durable boundary both the decider
// (which produces it) and the API layer (which previews, binds and renders it)
// already depend on, and because it is stored inside the workload document
// alongside WorkloadPlacement. It is deliberately NOT a second scheduler, price
// catalog or provider registry: every value on it is produced by the existing
// routing, resource, pricing and cloud-account code, normalized into one shape.
//
// WHAT IT MAY NOT CARRY, by construction rather than by review: there is no
// field on this type — or on anything reachable from it — that can hold a
// provider credential, an upstream payload, or a provider's own error text. The
// only free-form strings are ids and catalog values the tenant can already see
// (their own cluster ids, a provider name, a region, a SKU), and every rejection
// is one of the closed PlacementReject* codes. Validate enforces that closure
// before a receipt is persisted or rendered, so a future producer cannot quietly
// widen it.
type PlacementReceipt struct {
	Version int `json:"version"`
	// Tenant is the account the decision was made for. Empty only on the
	// single-binary path that has no tenant to name; it is never the tenancy
	// check — a receipt is reachable only through the workload document it is
	// stored inside, which is already tenant-scoped.
	Tenant string `json:"tenant,omitempty"`

	// Cluster placement, as decided by the tenant cluster policy / X-Cluster-ID
	// routing that admitted the submission. Rule and RuleVersion name the policy
	// that decided it, matching WorkloadPlacement's own fields.
	RequestedClusterID string `json:"requested_cluster_id,omitempty"`
	GrantedClusterID   string `json:"granted_cluster_id,omitempty"`
	ClusterMode        string `json:"cluster_mode,omitempty"`
	ClusterRule        string `json:"cluster_rule,omitempty"`
	ClusterRuleVersion string `json:"cluster_rule_version,omitempty"`

	Requested   PlacementRequest     `json:"requested"`
	Constraints PlacementConstraints `json:"constraints"`

	// AccountBinding binds the decision to the tenant cloud account it selected
	// WITHOUT naming it: a domain-separated digest over (tenant, account id),
	// empty on a platform-funded placement. It exists because two of one
	// tenant's BYOC accounts can serve the identical provider, region and shape
	// — so without it, switching between them leaves the digest unchanged and a
	// preview taken under one account would launch under the other.
	//
	// It is INTERNAL: it is persisted so a stored receipt still validates, and
	// it is covered by the digest, but it is not a member of
	// PublicPlacementReceipt and therefore never reaches a tenant response, an
	// audit row or a log line. Deriving it from the account id one way is what
	// lets a launch re-derive and compare it without anything reversible ever
	// leaving central.
	AccountBinding string `json:"account_binding,omitempty"`

	// Candidates is the bounded, deterministically ordered set of providers the
	// current routing and catalog code actually considered. Exactly one carries
	// Selected=true on a decision that resolved; none do on a refusal receipt.
	Candidates []PlacementCandidate `json:"candidates"`
	// Selected repeats the chosen candidate's shape together with the bounds
	// that were admitted with it. Zero on a refusal receipt.
	Selected PlacementSelection `json:"selected"`

	PricingVersion         int    `json:"pricing_version"`
	CandidateSetVersion    string `json:"candidate_set_version"`
	AvailabilityConfidence string `json:"availability_confidence"`

	// QuoteID, IssuedAt and ExpiresAt are the issuance identity of ONE quote and
	// are deliberately excluded from Digest: two previews of an unchanged
	// decision must agree, and they never share these.
	QuoteID   string    `json:"quote_id,omitempty"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`

	// Digest binds a preview to the launch that consumes it. It covers every
	// decision-bearing field above and nothing volatile.
	Digest string `json:"digest"`
}

// PlacementRequest is the normalized shape the submitter asked for, after size
// lookup, replica scaling and GPU count resolution — i.e. what the decider
// actually placed, not the raw spec strings.
type PlacementRequest struct {
	GPUKind        string `json:"gpu_kind,omitempty"`
	GPUCount       int    `json:"gpu_count,omitempty"`
	GPUReliability string `json:"gpu_reliability,omitempty"`
	CPUMillis      int64  `json:"cpu_millis"`
	MemoryMB       int64  `json:"memory_mb"`
}

// PlacementConstraints are the hard constraints the decision had to satisfy.
// Each one either came from the submitted spec or from durable tenant state; a
// value here is a bound that was ENFORCED, not a preference.
type PlacementConstraints struct {
	// BackendPin and RegionPin are spec.backend / spec.region, when pinned.
	BackendPin string `json:"backend_pin,omitempty"`
	RegionPin  string `json:"region_pin,omitempty"`
	// StorageBackendPin and StorageRegionPin are the affinity an existing volume
	// imposes: a workload reusing a volume can only land where that volume is.
	StorageBackendPin string `json:"storage_backend_pin,omitempty"`
	StorageRegionPin  string `json:"storage_region_pin,omitempty"`
	// AccountMode is which funding account the placement is eligible for:
	// "platform" for platform-funded capacity, "tenant" for the tenant's own
	// cloud account. AccountEligible is false when a tenant account exists but
	// could not be used.
	AccountMode     string `json:"account_mode,omitempty"`
	AccountEligible bool   `json:"account_eligible"`
	// MaxHourlyMicroUSDPerGPU is spec.gpu.maxHourlyUSD, the customer-price
	// per-GPU ceiling admission enforces. 0 means uncapped.
	MaxHourlyMicroUSDPerGPU int64 `json:"max_hourly_micro_usd_per_gpu,omitempty"`
	// MaxChargeMicroUSD is spec.budget.maxUSD and DeadlineSeconds is
	// spec.budget.deadline. 0 means the submitter declared neither.
	MaxChargeMicroUSD int64 `json:"max_charge_micro_usd,omitempty"`
	DeadlineSeconds   int64 `json:"deadline_seconds,omitempty"`
}

// PlacementCandidate is one provider the decision considered. A rejected
// candidate carries a stable code and no price: quoting a rate for a provider
// that cannot serve the request would read as an offer.
type PlacementCandidate struct {
	Provider       string `json:"provider"`
	Region         string `json:"region,omitempty"`
	SKU            string `json:"sku,omitempty"`
	GPUKind        string `json:"gpu_kind,omitempty"`
	GPUCount       int    `json:"gpu_count,omitempty"`
	CPUMillis      int64  `json:"cpu_millis,omitempty"`
	MemoryMB       int64  `json:"memory_mb,omitempty"`
	AccountMode    string `json:"account_mode,omitempty"`
	HourlyMicroUSD int64  `json:"hourly_micro_usd,omitempty"`
	Selected       bool   `json:"selected,omitempty"`
	// Reason is one of the PlacementReject* codes, and is set on exactly the
	// candidates that were not selected.
	Reason string `json:"reason,omitempty"`
}

// PlacementSelection is the selected candidate plus the bounds admitted with
// it: the exact rate, duration and maximum charge the launch is allowed.
type PlacementSelection struct {
	Provider               string `json:"provider,omitempty"`
	Region                 string `json:"region,omitempty"`
	SKU                    string `json:"sku,omitempty"`
	GPUKind                string `json:"gpu_kind,omitempty"`
	GPUCount               int    `json:"gpu_count,omitempty"`
	CPUMillis              int64  `json:"cpu_millis,omitempty"`
	MemoryMB               int64  `json:"memory_mb,omitempty"`
	AccountMode            string `json:"account_mode,omitempty"`
	HourlyMicroUSD         int64  `json:"hourly_micro_usd,omitempty"`
	MaximumDurationSeconds int64  `json:"maximum_duration_seconds,omitempty"`
	MaximumChargeMicroUSD  int64  `json:"maximum_charge_micro_usd,omitempty"`
}

const (
	// PlacementReceiptVersion versions the receipt SHAPE. A stored receipt at a
	// version this build does not know is rendered as absent rather than
	// reinterpreted.
	PlacementReceiptVersion = 1
	// PlacementCandidateSetVersion versions the candidate ENUMERATION policy —
	// which providers are considered and in what order. It is part of the
	// digest, so widening the candidate set invalidates outstanding previews
	// instead of silently changing what they meant.
	PlacementCandidateSetVersion = "v1"
	// PlacementPricingVersion versions the price catalog the rates came from.
	// It is the same number the billing price quote carries, deliberately: a
	// receipt and the hold taken against it must not claim different catalogs.
	PlacementPricingVersion = 1
	// MaxPlacementCandidates bounds the serialized candidate list. The current
	// supported provider set is five; the bound leaves room without ever letting
	// this grow into an unbounded diagnostic.
	MaxPlacementCandidates = 8
)

// Funding account modes.
const (
	PlacementAccountPlatform = "platform"
	PlacementAccountTenant   = "tenant"
)

// Availability confidence. Today every decision is create_time_only: central
// prices from a curated catalog but does not observe provider capacity before
// the create call, so "this SKU exists and costs this" is not "this SKU is
// obtainable right now". The other two values exist because the difference is
// the customer-visible one, and a capacity observation that lands later must
// report itself here rather than in a new field.
const (
	PlacementAvailabilityQuoted           = "quoted"
	PlacementAvailabilityRecentlyObserved = "recently_observed"
	PlacementAvailabilityCreateTimeOnly   = "create_time_only"
)

// Stable rejection reasons. These are the CUSTOMER contract: a provider's own
// message may explain a refusal in an operator log, but it never becomes one of
// these values.
const (
	PlacementRejectProviderNotConfigured     = "provider_not_configured"
	PlacementRejectProviderNotAllowed        = "provider_not_allowed"
	PlacementRejectProviderAccountUnavail    = "provider_account_unavailable"
	PlacementRejectRegionNotAllowed          = "region_not_allowed"
	PlacementRejectRegionImageUnavailable    = "region_image_unavailable"
	PlacementRejectSKUNotSupported           = "sku_not_supported"
	PlacementRejectGPUKindNotSupported       = "gpu_kind_not_supported"
	PlacementRejectGPUCountNotSupported      = "gpu_count_not_supported"
	PlacementRejectInsufficientCPU           = "insufficient_cpu"
	PlacementRejectInsufficientMemory        = "insufficient_memory"
	PlacementRejectPriceAboveMaxHourly       = "price_above_max_hourly"
	PlacementRejectMaximumChargeExceeded     = "maximum_charge_exceeded"
	PlacementRejectStorageProviderMismatch   = "storage_provider_mismatch"
	PlacementRejectStorageRegionMismatch     = "storage_region_mismatch"
	PlacementRejectClusterTargetNotValidated = "cluster_target_not_validated"
	PlacementRejectCapacityNotObserved       = "capacity_not_observed"
)

// validPlacementRejections is the closed set Validate admits. Codes central
// cannot produce today are still members: they are the contract a UI and a CLI
// are written against, and a producer that starts emitting one must not have to
// change the reader to be understood.
var validPlacementRejections = map[string]bool{
	PlacementRejectProviderNotConfigured:     true,
	PlacementRejectProviderNotAllowed:        true,
	PlacementRejectProviderAccountUnavail:    true,
	PlacementRejectRegionNotAllowed:          true,
	PlacementRejectRegionImageUnavailable:    true,
	PlacementRejectSKUNotSupported:           true,
	PlacementRejectGPUKindNotSupported:       true,
	PlacementRejectGPUCountNotSupported:      true,
	PlacementRejectInsufficientCPU:           true,
	PlacementRejectInsufficientMemory:        true,
	PlacementRejectPriceAboveMaxHourly:       true,
	PlacementRejectMaximumChargeExceeded:     true,
	PlacementRejectStorageProviderMismatch:   true,
	PlacementRejectStorageRegionMismatch:     true,
	PlacementRejectClusterTargetNotValidated: true,
	PlacementRejectCapacityNotObserved:       true,
}

var validPlacementAvailability = map[string]bool{
	PlacementAvailabilityQuoted:           true,
	PlacementAvailabilityRecentlyObserved: true,
	PlacementAvailabilityCreateTimeOnly:   true,
}

// ValidPlacementRejection reports whether code is a stable customer-facing
// rejection reason.
func ValidPlacementRejection(code string) bool { return validPlacementRejections[code] }

// ErrInvalidPlacementReceipt marks a receipt that must not be persisted,
// rendered or honored.
var ErrInvalidPlacementReceipt = errors.New("invalid placement receipt")

// placementAccountBindingDomain separates this digest from every other SHA-256
// central computes, so a binding can never be confused with — or replayed as —
// a value derived for another purpose.
const placementAccountBindingDomain = "yscale.placement.account.v1"

// PlacementAccountBinding derives the one-way binding for a selected tenant
// cloud account. It returns "" for a platform-funded placement, which has no
// tenant account to bind.
//
// The tenant is mixed in so the binding is scoped to the account's owner: the
// same account id under two tenants is not the same selection, and no binding
// is comparable across tenants.
func PlacementAccountBinding(tenant, cloudAccountID string) string {
	if cloudAccountID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(placementAccountBindingDomain + "|" + tenant + "|" + cloudAccountID))
	return hex.EncodeToString(sum[:])
}

// PublicPlacementReceipt is the TENANT-facing projection of a decision: every
// field a customer needs to read the answer and bind a launch to it, and
// nothing central holds only for its own comparison.
//
// It is an explicit allow-list rather than a set of exclusions on the durable
// type, so a field added to PlacementReceipt is private until someone decides
// otherwise here. The JSON tags match the durable ones exactly, so what a
// client reads is byte-identical to what it read before this projection
// existed.
type PublicPlacementReceipt struct {
	Version int    `json:"version"`
	Tenant  string `json:"tenant,omitempty"`

	RequestedClusterID string `json:"requested_cluster_id,omitempty"`
	GrantedClusterID   string `json:"granted_cluster_id,omitempty"`
	ClusterMode        string `json:"cluster_mode,omitempty"`
	ClusterRule        string `json:"cluster_rule,omitempty"`
	ClusterRuleVersion string `json:"cluster_rule_version,omitempty"`

	Requested   PlacementRequest     `json:"requested"`
	Constraints PlacementConstraints `json:"constraints"`

	Candidates []PlacementCandidate `json:"candidates"`
	Selected   PlacementSelection   `json:"selected"`

	PricingVersion         int    `json:"pricing_version"`
	CandidateSetVersion    string `json:"candidate_set_version"`
	AvailabilityConfidence string `json:"availability_confidence"`

	// QuoteID, IssuedAt and ExpiresAt name the issuance the customer is
	// holding. ExpiresAt is load-bearing on the client: it is the value a launch
	// presents back alongside the digest to prove which window it is consuming.
	QuoteID   string    `json:"quote_id,omitempty"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`

	Digest string `json:"digest"`
}

// Public projects the decision onto its tenant-facing shape.
func (r PlacementReceipt) Public() PublicPlacementReceipt {
	return PublicPlacementReceipt{
		Version:                r.Version,
		Tenant:                 r.Tenant,
		RequestedClusterID:     r.RequestedClusterID,
		GrantedClusterID:       r.GrantedClusterID,
		ClusterMode:            r.ClusterMode,
		ClusterRule:            r.ClusterRule,
		ClusterRuleVersion:     r.ClusterRuleVersion,
		Requested:              r.Requested,
		Constraints:            r.Constraints,
		Candidates:             append([]PlacementCandidate(nil), r.Candidates...),
		Selected:               r.Selected,
		PricingVersion:         r.PricingVersion,
		CandidateSetVersion:    r.CandidateSetVersion,
		AvailabilityConfidence: r.AvailabilityConfidence,
		QuoteID:                r.QuoteID,
		IssuedAt:               r.IssuedAt,
		ExpiresAt:              r.ExpiresAt,
		Digest:                 r.Digest,
	}
}

// PublicPlacementReceiptOf is the ONE way a decision reaches a tenant: it
// applies the same renderability gate SafePlacementReceipt does and then drops
// everything private. A receipt central cannot vouch for renders as absent.
func PublicPlacementReceiptOf(r *PlacementReceipt) *PublicPlacementReceipt {
	safe := SafePlacementReceipt(r)
	if safe == nil {
		return nil
	}
	public := safe.Public()
	return &public
}

// ComputeDigest returns the SHA-256 over everything on the receipt except its
// issuance identity — the quote id and the issued-at/expires-at window.
//
// That exclusion IS the contract: a preview and the launch that consumes it are
// two issuances of the same decision, and a digest that moved with the clock
// could never bind them. Everything else is covered, so a changed catalog,
// policy, candidate set, cluster, constraint or selected shape produces a
// different digest and the launch is refused rather than silently substituted.
func (r PlacementReceipt) ComputeDigest() (string, error) {
	c := r
	c.QuoteID = ""
	c.IssuedAt = time.Time{}
	c.ExpiresAt = time.Time{}
	c.Digest = ""
	raw, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode placement receipt for digest: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// Seal stamps one issuance onto a decision and binds its digest. The digest is
// computed AFTER the volatile fields are set precisely to prove they do not
// enter it.
func (r *PlacementReceipt) Seal(quoteID string, issuedAt time.Time, ttl time.Duration) error {
	r.QuoteID = quoteID
	r.IssuedAt = issuedAt
	r.ExpiresAt = issuedAt.Add(ttl)
	digest, err := r.ComputeDigest()
	if err != nil {
		return err
	}
	r.Digest = digest
	return nil
}

// Expired reports whether this issuance's validity window has closed.
func (r PlacementReceipt) Expired(now time.Time) bool {
	return !r.ExpiresAt.IsZero() && now.After(r.ExpiresAt)
}

// Validate enforces the receipt's own invariants: known version, bounded and
// ordered candidates, closed rejection codes, one selected candidate at most,
// and a digest that actually covers the content. It is the gate every producer
// passes through before a receipt is persisted, rendered or honored.
func (r PlacementReceipt) Validate() error {
	switch {
	case r.Version != PlacementReceiptVersion:
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidPlacementReceipt, r.Version)
	case len(r.Candidates) > MaxPlacementCandidates:
		return fmt.Errorf("%w: %d candidates exceeds the %d bound", ErrInvalidPlacementReceipt, len(r.Candidates), MaxPlacementCandidates)
	case !validPlacementAvailability[r.AvailabilityConfidence]:
		return fmt.Errorf("%w: availability confidence %q", ErrInvalidPlacementReceipt, r.AvailabilityConfidence)
	case r.CandidateSetVersion == "":
		return fmt.Errorf("%w: no candidate-set version", ErrInvalidPlacementReceipt)
	// The binding and the funding mode are one fact stated twice, so they may
	// not disagree. A tenant-funded decision that carries no binding is a
	// decision whose account could be swapped without moving the digest, and a
	// binding on a platform-funded one names an account nothing selected —
	// both fail closed rather than being rendered as a decision central stands
	// behind.
	case r.Constraints.AccountMode == PlacementAccountTenant && r.AccountBinding == "":
		return fmt.Errorf("%w: tenant-funded decision carries no account binding", ErrInvalidPlacementReceipt)
	case r.Constraints.AccountMode != PlacementAccountTenant && r.AccountBinding != "":
		return fmt.Errorf("%w: account binding on a %q decision", ErrInvalidPlacementReceipt, r.Constraints.AccountMode)
	}
	selected := 0
	for _, c := range r.Candidates {
		if c.Provider == "" {
			return fmt.Errorf("%w: candidate with no provider", ErrInvalidPlacementReceipt)
		}
		if c.Selected {
			selected++
			if c.Reason != "" {
				return fmt.Errorf("%w: selected candidate %s carries rejection %q", ErrInvalidPlacementReceipt, c.Provider, c.Reason)
			}
			continue
		}
		if !validPlacementRejections[c.Reason] {
			return fmt.Errorf("%w: candidate %s has unstable reason %q", ErrInvalidPlacementReceipt, c.Provider, c.Reason)
		}
	}
	if selected > 1 {
		return fmt.Errorf("%w: %d selected candidates", ErrInvalidPlacementReceipt, selected)
	}
	if selected == 1 && r.Selected.Provider == "" {
		return fmt.Errorf("%w: selected candidate with no selection shape", ErrInvalidPlacementReceipt)
	}
	digest, err := r.ComputeDigest()
	if err != nil {
		return err
	}
	if digest != r.Digest {
		return fmt.Errorf("%w: digest does not cover the decision", ErrInvalidPlacementReceipt)
	}
	return nil
}

// SafePlacementReceipt returns the receipt only when it is renderable: a valid
// one this build understands. A stored receipt written by a newer or broken
// producer is dropped rather than shown, for the reason WorkloadOutcome is a
// pointer — an unreadable decision must read as "not recorded", never as a
// decision central is standing behind.
func SafePlacementReceipt(r *PlacementReceipt) *PlacementReceipt {
	if r == nil || r.Validate() != nil {
		return nil
	}
	return r
}
