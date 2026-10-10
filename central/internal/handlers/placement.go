package handlers

import (
	"io"
	"net/http"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/workload"
)

// placementTokenHeader carries the ONE credential a launch may present to bind
// itself to a preview: the opaque launch token central signed when it issued
// that preview, copied verbatim out of the answer it gave.
//
// One opaque value rather than the decision's own fields, deliberately. The
// digest covers only the decision, so it is the same value for as long as the
// decision holds and cannot say WHEN it was issued; the expiry that supplies
// that freshness is therefore the load-bearing half — and a freshness claim the
// CLIENT types is not one. Both halves now travel inside a signature central
// can check, so presenting a stale decision as a fresh one means forging a
// token rather than editing a timestamp.
//
// A header rather than a body field, for the reason the body cannot take one:
// the request body of POST /v1/workloads is the customer's workload document in
// their own YAML, and the CLI writes it verbatim. Adding a central-owned key to
// it would make the document central acts on differ from the one the customer
// wrote — and every existing client would have to learn a field to keep sending
// a spec.
const placementTokenHeader = "X-Yscale-Placement-Token"

// Stable machine-readable placement codes. They are the contract a CLI and a
// console branch on; the human-readable message beside them may be reworded.
const (
	// PlacementChangedCode answers a launch whose supplied decision no longer
	// matches the one central computes now.
	PlacementChangedCode = "placement_changed"
	// PlacementUnavailableCode answers a request central can place nowhere
	// under the current policy, catalog and tenant state.
	PlacementUnavailableCode = "placement_unavailable"
	// PlacementDigestInvalidCode answers a preview credential that is malformed,
	// incomplete, or not one this deployment issued to this account. Every such
	// reason collapses to this single code on purpose: distinguishing "forged"
	// from "truncated" from "issued to someone else" would be an oracle, and the
	// client's action is the same in all three cases — preview again.
	PlacementDigestInvalidCode = "placement_digest_invalid"
	// PlacementExpiredCode answers a launch presenting a preview whose issuance
	// window has closed. It is deliberately NOT placement_changed: the decision
	// may be identical, and what the client must do — preview again — is the
	// same either way, but why differs and a console should be able to say so.
	PlacementExpiredCode = "placement_preview_expired"
)

// placementRejectionMessages turn the stable code into one bounded sentence.
// A provider's own message never appears here: this map is the whole customer
// vocabulary for a refused placement.
var placementRejectionMessages = map[string]string{
	state.PlacementRejectProviderNotConfigured:     "the provider this workload routes to is not configured on this deployment",
	state.PlacementRejectProviderNotAllowed:        "no configured provider is allowed for this workload's routing",
	state.PlacementRejectProviderAccountUnavail:    "this tenant's provider account could not be used for this placement",
	state.PlacementRejectRegionNotAllowed:          "the requested region has no verified price for this provider",
	state.PlacementRejectRegionImageUnavailable:    "the requested region has no burst image for this provider",
	state.PlacementRejectSKUNotSupported:           "no priced SKU matches this workload shape",
	state.PlacementRejectGPUKindNotSupported:       "no configured provider serves the requested GPU kind",
	state.PlacementRejectGPUCountNotSupported:      "no configured provider serves the requested GPU count",
	state.PlacementRejectInsufficientCPU:           "no priced SKU offers the requested CPU alongside the requested GPUs",
	state.PlacementRejectInsufficientMemory:        "no priced SKU offers the requested memory alongside the requested GPUs",
	state.PlacementRejectPriceAboveMaxHourly:       "the resolved price is above spec.gpu.maxHourlyUSD",
	state.PlacementRejectMaximumChargeExceeded:     "the resolved maximum charge is above spec.budget.maxUSD",
	state.PlacementRejectStorageProviderMismatch:   "this workload's existing storage is on another provider",
	state.PlacementRejectStorageRegionMismatch:     "this workload's existing storage is in another region",
	state.PlacementRejectClusterTargetNotValidated: "the requested cluster is not a validated target for this tenant",
	state.PlacementRejectCapacityNotObserved:       "no capacity has been observed for this shape",
}

// placementRejectionMessage is the customer-safe sentence for a refusal.
func placementRejectionMessage(code string) string {
	if msg, ok := placementRejectionMessages[code]; ok {
		return msg
	}
	return "this workload cannot be placed under the current policy and provider catalog"
}

// PlacementPreviewResponse is the observational answer: the decision central
// would make right now, or the stable reason it can make none.
type PlacementPreviewResponse struct {
	Status  string `json:"status"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	// GPUProduct is the model guaranteed by the exact selected SKU. It stays
	// outside the sealed receipt so older stored decisions remain valid.
	GPUProduct string `json:"gpu_product,omitempty"`
	// Placement is the tenant-facing projection of the decision. The customer
	// gets the whole answer — every candidate, constraint, price and the digest
	// they launch with — and none of the internal binding material central
	// keeps only to compare one decision against another.
	Placement *state.PublicPlacementReceipt `json:"placement,omitempty"`
	// LaunchToken is EXACTLY what a launch presents back, and the only thing it
	// presents: one opaque string for the X-Yscale-Placement-Token header. A
	// client never has to reassemble a credential out of fields it read off the
	// decision, so there is nothing for it to get subtly wrong and nothing for a
	// caller to substitute.
	//
	// It appears only on this route's own 200, never on a refusal and never on
	// the launch surface: a decision central is refusing is not an issuance, and
	// handing out a credential for one would be issuing a preview from a path
	// that decided nothing.
	//
	// Empty on a deployment with no signing key, which is the fail-closed
	// posture rather than a fallback: the preview still answers the decision, it
	// simply cannot vouch for it, and a launch is refused rather than trusted on
	// the client's word.
	LaunchToken string `json:"launch_token,omitempty"`
}

// verifyPlacementToken checks a launch that presented a preview: the token must
// be one THIS deployment signed for THIS account, the issuance it names must
// still be open, and the decision it names must still be the one central
// computes. It returns ok=true when there is nothing to verify — no header at
// all, which is the cluster-token and existing-CLI path and must behave exactly
// as it always has.
//
// The order is not incidental. Authenticity, then expiry, then recomputation:
// each check is cheaper than the one after it, and every one of them lands
// before the admission reservation, the mesh key, the /24 and the provider
// call. Everything this function does is a read — it recomputes
// the decision through the same Quote path the launch itself will use and
// compares identities — so a refusal here costs the submitter nothing and
// leaves nothing to compensate.
//
// On success it returns the VERIFIED quote, so the launch can be bound to the
// exact decision that was checked instead of a third one computed moments later.
func (h *Workloads) verifyPlacementToken(r *http.Request, wl *workload.Workload, opts PlanOptions) (*BurstQuote, int, CreateWorkloadResponse, bool) {
	presented := r.Header.Values(placementTokenHeader)
	if len(presented) == 0 {
		return nil, 0, CreateWorkloadResponse{}, true
	}
	invalid := func(message string) (*BurstQuote, int, CreateWorkloadResponse, bool) {
		return nil, http.StatusBadRequest, CreateWorkloadResponse{
			Status:  "rejected",
			Code:    PlacementDigestInvalidCode,
			Message: message,
		}, false
	}
	// PRESENCE, not content, is what says "this launch meant to present a
	// preview". A header that arrived empty, blank, or twice is a client bug
	// answered as one — never read as the no-preview path, which would admit a
	// submission that asked to be checked and silently was not.
	if len(presented) > 1 {
		return invalid(placementTokenHeader + " must be presented exactly once, copied from one placement preview")
	}
	supplied := strings.TrimSpace(presented[0])
	if supplied == "" {
		return invalid(placementTokenHeader + " was presented empty; send the launch_token from a placement preview or omit the header entirely")
	}
	// A presented credential can only be honored by a deployment that can
	// authenticate one. Answering on the client's word because no key is
	// configured is precisely the substitution this check exists to prevent, so
	// it fails closed — and it does so before the verification quote, so an
	// unauthenticatable launch never reaches the decision path at all.
	if h.PlacementTokens == nil {
		h.Log.Error("a launch presented a placement token but no placement signing key is configured; refusing rather than honoring it unverified",
			"customer", opts.CustomerID, "cluster", opts.ClusterID)
		return nil, http.StatusServiceUnavailable, CreateWorkloadResponse{
			Status:  "rejected",
			Message: "placement previews are unavailable on this deployment",
		}, false
	}
	claims, err := h.PlacementTokens.Verify(supplied)
	if err != nil {
		// Deliberately one sentence for every reason. The refusal never says
		// which check failed, and never echoes the token: a forged signature, a
		// truncated envelope and a token minted by another deployment must be
		// the same answer.
		h.Log.Info("placement launch token failed verification",
			"customer", opts.CustomerID, "cluster", opts.ClusterID)
		return invalid(placementTokenHeader + " is not a launch token this deployment issued; preview the placement again and resubmit")
	}
	// Tenant binding. The signature proves central minted the token; this proves
	// it minted it for the account now submitting. Without it one tenant's
	// genuine, unexpired preview would be a bearer credential any other tenant
	// could replay onto their own launch.
	if claims.Tenant != opts.CustomerID {
		h.Log.Info("placement launch token presented by an account it was not issued to",
			"customer", opts.CustomerID, "cluster", opts.ClusterID)
		return invalid(placementTokenHeader + " is not a launch token this deployment issued; preview the placement again and resubmit")
	}
	// EXPIRY IS DECIDED ON THE AUTHENTICATED WINDOW, and decided before the
	// verification quote, the admission reservation and anything the decider
	// would have to compensate. This is the value that used
	// to be a client-supplied timestamp: it is now central's own, so a window
	// that has closed cannot be reopened by re-typing it.
	now := time.Now().UTC()
	if !claims.ExpiresAt.After(now) {
		h.Log.Info("placement preview presented after its issuance window closed",
			"customer", opts.CustomerID, "cluster", opts.ClusterID)
		return nil, http.StatusConflict, CreateWorkloadResponse{
			Status:  "rejected",
			Code:    PlacementExpiredCode,
			Message: "this placement preview has expired; preview the placement again and resubmit",
		}, false
	}
	quoted, ok := h.Decider.(QuotedDecider)
	if !ok {
		// A token can only be honored by a decider that can recompute the
		// decision it names. Answering "unchanged" without recomputing would be
		// the silent substitution this check exists to prevent.
		return nil, http.StatusServiceUnavailable, CreateWorkloadResponse{
			Status:  "rejected",
			Message: "placement previews are unavailable on this deployment",
		}, false
	}
	quote, err := quoted.Quote(r.Context(), wl, opts)
	if err != nil {
		if isFabricProvisioning(err) {
			return nil, http.StatusServiceUnavailable, CreateWorkloadResponse{
				Status:  "rejected",
				Message: "this account's fabric is still provisioning; retry shortly",
			}, false
		}
		// The decision the preview named no longer resolves at all. That is a
		// changed placement, answered with the same stable code and with the
		// current — refused — decision, so a client can show what moved.
		code, receipt := placementRefusalOf(err)
		if code == "" {
			return nil, http.StatusServiceUnavailable, CreateWorkloadResponse{
				Status:  "rejected",
				Message: "provider quote unavailable",
			}, false
		}
		h.Log.Info("placement token presented for a decision that no longer resolves",
			"customer", opts.CustomerID, "cluster", opts.ClusterID, "reason", code)
		return nil, http.StatusConflict, CreateWorkloadResponse{
			Status:    "rejected",
			Code:      PlacementChangedCode,
			Message:   placementRejectionMessage(code),
			Placement: placementReceiptResponse(opts, receipt),
		}, false
	}
	current := quote.Placement
	if current == nil {
		return nil, http.StatusServiceUnavailable, CreateWorkloadResponse{
			Status:  "rejected",
			Message: "placement previews are unavailable on this deployment",
		}, false
	}
	// The AUTHENTICATED digest against a freshly recomputed decision. This is the
	// comparison the whole credential exists to make trustworthy: the token says
	// which decision central promised, and this says whether that is still the
	// decision central would make.
	if current.Digest != claims.Digest {
		h.Log.Info("placement digest no longer matches the current decision",
			"customer", opts.CustomerID, "cluster", opts.ClusterID)
		return nil, http.StatusConflict, CreateWorkloadResponse{
			Status:    "rejected",
			Code:      PlacementChangedCode,
			Message:   "the placement decision changed since it was previewed; review the current decision and resubmit",
			Placement: placementReceiptResponse(opts, state.SafePlacementReceipt(current)),
		}, false
	}
	// The launch is held to the AUTHENTICATED window, not the fresh one it was
	// verified against. Narrowing the verified issuance here is what carries the
	// preview's own deadline into PlanQuoted, which checks it again immediately
	// before admission commits intent — so the time spent reserving admission
	// cannot outlive the decision the customer was shown. Only ExpiresAt moves,
	// and the digest deliberately excludes it, so the receipt still validates
	// and still names the same decision.
	//
	// There is no bound to enforce on the window any more, and no clock-skew
	// allowance to tune: the value is central's own signature over its own TTL
	// rather than a number a client sent, so "a window this deployment would not
	// issue" is not a reachable state.
	if claims.ExpiresAt.Before(current.ExpiresAt) {
		current.ExpiresAt = claims.ExpiresAt
	}
	return quote, 0, CreateWorkloadResponse{}, true
}

// placementReceiptResponse renders a decision on the launch surface's own
// placement shape, so a 409 and a 202 answer in the same envelope.
func placementReceiptResponse(opts PlanOptions, receipt *state.PlacementReceipt) *PlacementResponse {
	if receipt == nil {
		return nil
	}
	public := state.PublicPlacementReceiptOf(receipt)
	return &PlacementResponse{
		RequestedClusterID: opts.ClusterPlacement.RequestedClusterID,
		GrantedClusterID:   opts.ClusterPlacement.GrantedClusterID,
		Mode:               opts.ClusterPlacement.Mode,
		GPUProduct:         placementGPUProduct(public),
		Receipt:            public,
	}
}

// PreviewPlacement answers "where would this run, at what price, and why there"
// WITHOUT running it.
//
// It reaches the decision through the same parse, namespace authorization,
// cluster routing and Quote path a launch does, because a preview computed any
// other way is a second implementation that will eventually disagree with the
// one that spends money.
//
// It is observational by construction: no idempotency claim, no admission
// reservation, no mesh credential, no storage write, no
// provider call, no workload or burst record, no lifecycle operation, and no
// audit row. The only thing it leaves behind is the answer.
func (h *Workloads) PreviewPlacement(w http.ResponseWriter, r *http.Request) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	quoted, ok := h.Decider.(QuotedDecider)
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, PlacementPreviewResponse{
			Status: "rejected", Message: "placement previews are unavailable on this deployment"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, PlacementPreviewResponse{
			Status: "rejected", Message: "reading body: " + err.Error()})
		return
	}
	defer r.Body.Close()

	var wl workload.Workload
	if err := yaml.Unmarshal(body, &wl); err != nil {
		writeJSON(w, http.StatusBadRequest, PlacementPreviewResponse{
			Status: "rejected", Message: "parsing workload yaml: " + err.Error()})
		return
	}
	if err := workload.Validate(&wl); err != nil {
		writeJSON(w, http.StatusBadRequest, PlacementPreviewResponse{
			Status: "rejected", Message: err.Error()})
		return
	}

	// Same namespace pin as launch. A preview that answered for a namespace the
	// tenant is not authorized for would be a decision they cannot act on — and
	// the pinned namespace is part of the document the digest covers.
	//
	// Unlike Create this refusal is NOT journaled: a preview is a read, and
	// writing an authorization row for one would put an unauthenticated-shaped
	// write behind a route that must have no side effects at all.
	ns, err := authorizeWorkloadNamespace(cust, wl.Metadata.Namespace)
	if err != nil {
		writeJSON(w, http.StatusForbidden, PlacementPreviewResponse{
			Status: "rejected", Message: err.Error()})
		return
	}
	wl.Metadata.Namespace = ns

	agent, status, msg, placement := h.routeToCluster(cust, r.Header.Get(clusterIDHeader))
	if status != 0 {
		writeJSON(w, status, PlacementPreviewResponse{Status: "rejected", Message: msg})
		return
	}
	if namespace, hosted := h.Store.HostedNamespaceForCluster(cust.ID, agent.ClusterID); hosted && wl.Metadata.Namespace != namespace {
		writeJSON(w, http.StatusConflict, PlacementPreviewResponse{
			Status:  "rejected",
			Message: "hosted cluster " + agent.ClusterID + " accepts workloads only in its reserved namespace " + namespace,
		})
		return
	}

	// The same bound launch applies, applied here for the same reason the
	// namespace is pinned here: it changes the deadline, the deadline changes
	// the maximum charge, and a preview that skipped it would digest a decision
	// the launch would never make.
	if needsNodeOnlyBound(&wl, agent) {
		if h.NodeOnlyMaxLifetime <= 0 {
			writeJSON(w, http.StatusBadRequest, PlacementPreviewResponse{
				Status: "rejected", Message: unboundableNodeOnlyMessage})
			return
		}
		wl.Spec.Budget = &workload.Budget{Deadline: h.NodeOnlyMaxLifetime}
	}

	// No WorkloadID: a preview is not a submission and has no durable identity
	// to admit under. Quote never needs one — only the create saga does.
	quote, err := quoted.Quote(r.Context(), &wl, PlanOptions{
		CustomerID:       cust.ID,
		ClusterID:        agent.ClusterID,
		ClusterPlacement: placement.decision(),
	})
	if err != nil {
		if isFabricProvisioning(err) {
			w.Header().Set("Retry-After", "15")
			writeJSON(w, http.StatusServiceUnavailable, PlacementPreviewResponse{
				Status: "rejected", Message: "this account's fabric is still provisioning; retry shortly"})
			return
		}
		code, receipt := placementRefusalOf(err)
		if code == "" {
			// An error the decision path does not classify is an operator
			// problem, not a customer contract. It is logged in full and
			// answered with nothing.
			h.Log.Error("placement preview failed", "customer", cust.ID, "cluster", agent.ClusterID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, PlacementPreviewResponse{
				Status: "rejected", Message: "placement preview is unavailable; please retry"})
			return
		}
		writeJSON(w, http.StatusConflict, PlacementPreviewResponse{
			Status:    "rejected",
			Code:      PlacementUnavailableCode,
			Message:   placementRejectionMessage(code),
			Placement: state.PublicPlacementReceiptOf(receipt),
		})
		return
	}
	receipt := state.PublicPlacementReceiptOf(quote.Placement)
	if receipt == nil {
		writeJSON(w, http.StatusServiceUnavailable, PlacementPreviewResponse{
			Status: "rejected", Message: "placement previews are unavailable on this deployment"})
		return
	}
	answer := PlacementPreviewResponse{
		Status: "ok", Placement: receipt, GPUProduct: placementGPUProduct(receipt),
	}
	// The credential is minted from the SEALED issuance, after the decision is
	// final — so what is signed is exactly what was shown, and there is no
	// window in which the token could describe a decision the customer never saw.
	//
	// Signing is still a pure computation over values already in hand: the
	// preview remains a read, and this adds no claim, row, hold or provider call
	// to it. The tenant is the AUTHENTICATED caller rather than the receipt's own
	// Tenant field, because the caller is what the launch will compare against.
	if h.PlacementTokens != nil {
		token, err := h.PlacementTokens.Sign(state.PlacementLaunchClaims{
			Version:   receipt.Version,
			Tenant:    cust.ID,
			QuoteID:   receipt.QuoteID,
			Digest:    receipt.Digest,
			IssuedAt:  receipt.IssuedAt,
			ExpiresAt: receipt.ExpiresAt,
		})
		if err != nil {
			// A decision that cannot be vouched for is not answered as one a
			// launch could bind to. The full error is an operator problem and is
			// logged; the customer gets the same retry sentence any other
			// unavailable preview gets, and never the key or its shape.
			h.Log.Error("issuing a placement launch token failed",
				"customer", cust.ID, "cluster", agent.ClusterID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, PlacementPreviewResponse{
				Status: "rejected", Message: "placement preview is unavailable; please retry"})
			return
		}
		answer.LaunchToken = token
	}
	writeJSON(w, http.StatusOK, answer)
}

// needsNodeOnlyBound reports whether this submission is the one shape nothing
// else can bound: nodeOnly capacity on a connector that cannot observe pod
// occupancy, with no budget the submitter declared themselves.
func needsNodeOnlyBound(wl *workload.Workload, agent *state.Agent) bool {
	return wl.Spec.NodeOnly && !agent.AuthoritativeOccupancy && !hasDeclaredBudget(wl)
}
