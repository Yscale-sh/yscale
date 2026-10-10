package decider

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/credentialcipher"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/workload"
)

func placementOptions() handlers.PlanOptions {
	return handlers.PlanOptions{
		CustomerID: "cust_placement",
		ClusterID:  "cluster_east",
		ClusterPlacement: handlers.ClusterPlacementDecision{
			RequestedClusterID: "cluster_east",
			GrantedClusterID:   "cluster_east",
			Mode:               state.ClusterPlacementModePinned,
			Rule:               "tenant.cluster_policy",
			RuleVersion:        "v1",
		},
	}
}

func TestPlacementRegionUsesSelectedProvider(t *testing.T) {
	for _, tc := range []struct {
		name, backend, requested, configured, want string
	}{
		{"fly default", backends.TypeFlyIO, "", "", "ord"},
		{"fly configured", backends.TypeFlyIO, "", "iad", "iad"},
		{"linode automatic remains unknown", backends.TypeLinode, "", "us-ord", ""},
		{"linode pinned", backends.TypeLinode, "us-sea", "us-ord", "us-sea"},
		{"aws default", backends.TypeAWS, "", "", "us-east-1"},
		{"aws configured", backends.TypeAWS, "", "us-west-2", "us-west-2"},
		{"gcp configured", backends.TypeGCP, "", "us-central1", "us-central1"},
		{"azure default", backends.TypeAzure, "", "", "eastus"},
		{"azure configured", backends.TypeAzure, "", "westus2", "westus2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _ := newRecordingPlanDecider(t, tc.backend)
			d.gcpRegion = "us-central1" // GCP may be enabled alongside any backend.
			d.cfg.FlyRegion, d.cfg.LinodeRegion = tc.configured, tc.configured
			d.cfg.AWSRegion, d.cfg.AzureLocation = tc.configured, tc.configured
			wl := minimalPlanWorkload()
			wl.Spec.Backend, wl.Spec.Region = tc.backend, tc.requested
			quote, err := d.Quote(context.Background(), wl, placementOptions())
			if err != nil {
				t.Fatal(err)
			}
			if got := quote.Placement.Selected.Region; got != tc.want {
				t.Fatalf("placement region = %q, want %q", got, tc.want)
			}
			plan, err := d.PlanQuoted(context.Background(), wl, placementOptions(), quote)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Region != tc.want {
				t.Fatalf("plan region = %q, want %q", plan.Region, tc.want)
			}
		})
	}
}

// The digest binds a preview to a launch, so it must be a function of the
// DECISION and nothing else. Two quotes of one unchanged workload are issued
// seconds apart under different quote ids; if either of those leaked into the
// digest, no preview could ever be presented back.
func TestPlacementDigestIgnoresIssuance(t *testing.T) {
	d, _, _ := newRecordingPlanDecider(t, backends.TypeFlyIO)
	wl := minimalPlanWorkload()

	first, err := d.Quote(context.Background(), wl, placementOptions())
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	second, err := d.Quote(context.Background(), wl, placementOptions())
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if first.Placement == nil || second.Placement == nil {
		t.Fatal("quote carries no placement receipt")
	}
	if first.Placement.QuoteID == second.Placement.QuoteID {
		t.Fatal("two issuances share a quote id; the digest comparison below proves nothing")
	}
	if first.Placement.Digest != second.Placement.Digest {
		t.Fatalf("digest moved between issuances: %s vs %s", first.Placement.Digest, second.Placement.Digest)
	}
	if first.Placement.Digest == "" || first.Placement.Digest != mustDigest(t, first.Placement) {
		t.Fatal("sealed digest does not cover the receipt content")
	}
	if first.Placement.ExpiresAt.Sub(first.Placement.IssuedAt) != placementQuoteTTL {
		t.Fatalf("issuance window = %s, want %s",
			first.Placement.ExpiresAt.Sub(first.Placement.IssuedAt), placementQuoteTTL)
	}
	if first.Placement.QuoteID != first.Price.QuoteID {
		t.Fatalf("receipt quote id %q disagrees with the billing quote %q — the two must name one issuance",
			first.Placement.QuoteID, first.Price.QuoteID)
	}
}

// A decision that changes must NOT keep its digest: that is the whole mechanism
// behind placement_changed. Each mutation below is one a customer would care
// about, and each must move the digest.
func TestPlacementDigestMovesWhenTheDecisionMoves(t *testing.T) {
	d, _, _ := newRecordingPlanDecider(t, backends.TypeFlyIO)
	base, err := d.Quote(context.Background(), minimalPlanWorkload(), placementOptions())
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}

	for _, tc := range []struct {
		name    string
		mutate  func(*workload.Workload)
		options func() handlers.PlanOptions
	}{
		{name: "a bigger shape", mutate: func(wl *workload.Workload) { wl.Spec.Size = "large" }},
		{name: "a declared deadline", mutate: func(wl *workload.Workload) {
			wl.Spec.Budget = &workload.Budget{Deadline: 90 * time.Minute}
		}},
		{name: "a declared spend cap", mutate: func(wl *workload.Workload) {
			wl.Spec.Budget = &workload.Budget{MaxUSD: 1}
		}},
		{name: "another granted cluster", options: func() handlers.PlanOptions {
			opts := placementOptions()
			opts.ClusterPlacement.GrantedClusterID = "cluster_west"
			return opts
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wl := minimalPlanWorkload()
			opts := placementOptions()
			if tc.mutate != nil {
				tc.mutate(wl)
			}
			if tc.options != nil {
				opts = tc.options()
			}
			changed, err := d.Quote(context.Background(), wl, opts)
			if err != nil {
				t.Fatalf("Quote: %v", err)
			}
			if changed.Placement.Digest == base.Placement.Digest {
				t.Fatal("the decision changed but its digest did not; a stale preview would be honored")
			}
		})
	}
}

// The candidate list is a customer-facing contract: bounded, in one fixed
// order, every rejection from the closed code set, and never a provider's own
// words.
func TestPlacementCandidatesAreBoundedOrderedAndStable(t *testing.T) {
	d, _, _ := newRecordingPlanDecider(t, backends.TypeLinode)
	wl := gpuPlanWorkload(backends.BackendAuto, "rtx4000ada", 1, 1, 0)

	quote, err := d.Quote(context.Background(), wl, placementOptions())
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	receipt := quote.Placement
	if got := len(receipt.Candidates); got == 0 || got > state.MaxPlacementCandidates {
		t.Fatalf("candidates = %d, want 1..%d", got, state.MaxPlacementCandidates)
	}
	if len(receipt.Candidates) != len(placementCandidateProviders) {
		t.Fatalf("candidates = %d, want one per supported provider (%d)",
			len(receipt.Candidates), len(placementCandidateProviders))
	}
	selected := 0
	for i, c := range receipt.Candidates {
		if c.Provider != placementCandidateProviders[i] {
			t.Fatalf("candidate %d = %q, want %q — the order is part of the digest",
				i, c.Provider, placementCandidateProviders[i])
		}
		if c.Selected {
			selected++
			if c.Reason != "" || c.SKU == "" || c.HourlyMicroUSD <= 0 {
				t.Fatalf("selected candidate is not a complete offer: %+v", c)
			}
			continue
		}
		if !state.ValidPlacementRejection(c.Reason) {
			t.Fatalf("candidate %s carries an unstable reason %q", c.Provider, c.Reason)
		}
		if c.HourlyMicroUSD != 0 || c.SKU != "" {
			t.Fatalf("rejected candidate %s quotes a price it cannot honor: %+v", c.Provider, c)
		}
	}
	if selected != 1 {
		t.Fatalf("selected candidates = %d, want exactly 1", selected)
	}
	if receipt.Selected.Provider != backends.TypeLinode {
		t.Fatalf("selected provider = %q, want the routed linode", receipt.Selected.Provider)
	}
	// The RTX kind is Linode's; AWS is refused by its own selector, and the
	// CPU-only backends by routing.
	byProvider := map[string]state.PlacementCandidate{}
	for _, c := range receipt.Candidates {
		byProvider[c.Provider] = c
	}
	if got := byProvider[backends.TypeAWS].Reason; got != state.PlacementRejectGPUKindNotSupported {
		t.Fatalf("aws reason = %q, want gpu_kind_not_supported", got)
	}
	if got := byProvider[backends.TypeGCP].Reason; got != state.PlacementRejectGPUKindNotSupported {
		t.Fatalf("gcp reason = %q, want gpu_kind_not_supported", got)
	}

	// Nothing in the serialized receipt may carry a credential, an upstream
	// payload or a provider's error text.
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatalf("marshal receipt: %v", err)
	}
	for _, forbidden := range []string{"linode:", "aws:", "token", "secret", "cloud_account_id", "ca_", "error"} {
		if strings.Contains(strings.ToLower(string(raw)), forbidden) {
			t.Fatalf("receipt leaks %q: %s", forbidden, raw)
		}
	}
}

// Every rung of the routed provider's ladder must answer with a stable code AND
// leave the message admission has always produced, because operators and
// existing tests read that message.
func TestPlacementRefusalsCarryStableCodesWithoutChangingMessages(t *testing.T) {
	linodeGPUCapped := func() *workload.Workload {
		return gpuPlanWorkload(backends.TypeLinode, "rtx6000", 1, 1, 0.10)
	}
	for _, tc := range []struct {
		name        string
		decider     func(t *testing.T) *Decider
		workload    func() *workload.Workload
		wantReason  string
		wantMsg     string
		wantReceipt bool
	}{
		{
			name:        "provider is not configured",
			decider:     func(t *testing.T) *Decider { d, _, _ := newRecordingPlanDecider(t, backends.TypeLinode); return d },
			workload:    minimalPlanWorkload, // pinned to flyio
			wantReason:  state.PlacementRejectProviderNotConfigured,
			wantMsg:     "flyio backend not configured",
			wantReceipt: true,
		},
		{
			name:       "the backend serves no GPU",
			decider:    func(t *testing.T) *Decider { d, _, _ := newRecordingPlanDecider(t, backends.TypeGCP); return d },
			workload:   func() *workload.Workload { return gpuPlanWorkload(backends.TypeGCP, "l4", 1, 1, 0) },
			wantReason: state.PlacementRejectGPUKindNotSupported,
			wantMsg:    "backend=gcp does not support GPU workloads",
		},
		{
			name:        "the price is over the per-GPU cap",
			decider:     func(t *testing.T) *Decider { d, _, _ := newRecordingPlanDecider(t, backends.TypeLinode); return d },
			workload:    linodeGPUCapped,
			wantReason:  state.PlacementRejectPriceAboveMaxHourly,
			wantMsg:     "gpu price cap exceeded",
			wantReceipt: true,
		},
		{
			name:    "the GPU count has no plan",
			decider: func(t *testing.T) *Decider { d, _, _ := newRecordingPlanDecider(t, backends.TypeLinode); return d },
			workload: func() *workload.Workload {
				return gpuPlanWorkload(backends.TypeLinode, "rtx6000", 7, 1, 1)
			},
			wantReason:  state.PlacementRejectGPUCountNotSupported,
			wantMsg:     "gpu price admission rejected",
			wantReceipt: true,
		},
		{
			name:    "the GPU kind is unknown to the backend",
			decider: func(t *testing.T) *Decider { d, _, _ := newRecordingPlanDecider(t, backends.TypeLinode); return d },
			workload: func() *workload.Workload {
				return gpuPlanWorkload(backends.TypeLinode, "h100", 1, 1, 1)
			},
			wantReason:  state.PlacementRejectGPUKindNotSupported,
			wantMsg:     "gpu price admission rejected",
			wantReceipt: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.decider(t)
			_, err := d.Quote(context.Background(), tc.workload(), placementOptions())
			if err == nil {
				t.Fatal("Quote succeeded; want a refusal")
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("refusal message = %q, want it to still contain %q", err, tc.wantMsg)
			}
			var refusal *PlacementRefusal
			if !errors.As(err, &refusal) {
				t.Fatalf("refusal %v carries no stable placement reason", err)
			}
			if refusal.Reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", refusal.Reason, tc.wantReason)
			}
			if !state.ValidPlacementRejection(refusal.Reason) {
				t.Fatalf("reason %q is not in the stable set", refusal.Reason)
			}
			if !tc.wantReceipt {
				return
			}
			if refusal.Receipt == nil {
				t.Fatal("refusal carries no explanation receipt")
			}
			if refusal.Receipt.Selected.Provider != "" {
				t.Fatalf("refusal receipt selected %q", refusal.Receipt.Selected.Provider)
			}
			if err := refusal.Receipt.Validate(); err != nil {
				t.Fatalf("refusal receipt is not renderable: %v", err)
			}
		})
	}
}

// An xlarge workload (8000m/32768Mi) on rtx6000 x1 exactly matches the plan's
// raw capacity. With GPU-node system headroom the effective need exceeds the
// plan, so Quote must reject BEFORE any provider or mesh mutation.
func TestPlacementRejectsExactCapacityGPUBeforeMutation(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeLinode)
	wl := gpuPlanWorkload(backends.TypeLinode, "rtx6000", 1, 1, 2.0)
	wl.Spec.Size = "xlarge" // 8000m CPU, 32768Mi memory — exactly rtx6000 x1 raw capacity
	_, err := d.Plan(context.Background(), wl, placementOptions())
	if err == nil {
		t.Fatal("Plan succeeded for an exact-capacity rtx6000 request; headroom should reject it")
	}
	if !strings.Contains(err.Error(), "system headroom") {
		t.Fatalf("rejection = %q, want it to mention system headroom", err)
	}
	if backend.createCalls != 0 {
		t.Fatal("provider was contacted before the placement refusal")
	}
	if meshCalls.mint.Load() != 0 {
		t.Fatal("mesh keys were minted before the placement refusal")
	}
	if len(d.reservedSlots) != 0 {
		t.Fatal("PodCIDR was reserved before the placement refusal")
	}
}

// The receipt must state the SAME bounds admission enforces — not a second
// opinion computed alongside them.
func TestPlacementReceiptRestatesAdmissionsOwnBounds(t *testing.T) {
	d, _, _ := newRecordingPlanDecider(t, backends.TypeLinode)
	wl := gpuPlanWorkload(backends.TypeLinode, "rtx4000ada", 1, 1, 2.0)
	wl.Spec.Region = "us-ord"
	wl.Spec.Budget = &workload.Budget{Deadline: 6 * time.Hour, MaxUSD: 3.12}

	quote, err := d.Quote(context.Background(), wl, placementOptions())
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	receipt := quote.Placement
	if receipt.Constraints.BackendPin != backends.TypeLinode || receipt.Constraints.RegionPin != "us-ord" {
		t.Fatalf("pins not recorded as hard constraints: %+v", receipt.Constraints)
	}
	if receipt.Constraints.MaxHourlyMicroUSDPerGPU != 2_000_000 {
		t.Fatalf("per-GPU cap = %d micro-USD, want 2000000", receipt.Constraints.MaxHourlyMicroUSDPerGPU)
	}
	if receipt.Constraints.MaxChargeMicroUSD != 3_120_000 || receipt.Constraints.DeadlineSeconds != 6*3600 {
		t.Fatalf("declared budget not recorded: %+v", receipt.Constraints)
	}
	// The selection must agree with the billing quote to the micro-USD: they are
	// one decision, and a hold taken against a number the receipt does not show
	// is the drift this slice exists to remove.
	if receipt.Selected.HourlyMicroUSD != quote.Price.CustomerRateMicroUSDPerHour {
		t.Fatalf("receipt rate %d != priced rate %d",
			receipt.Selected.HourlyMicroUSD, quote.Price.CustomerRateMicroUSDPerHour)
	}
	if receipt.Selected.MaximumChargeMicroUSD != quote.Price.MaximumChargeMicroUSD {
		t.Fatalf("receipt maximum %d != priced maximum %d",
			receipt.Selected.MaximumChargeMicroUSD, quote.Price.MaximumChargeMicroUSD)
	}
	if receipt.Selected.MaximumChargeMicroUSD != 3_120_000 {
		t.Fatalf("maximum charge = %d, want the declared 3.12 USD ceiling", receipt.Selected.MaximumChargeMicroUSD)
	}
	if receipt.Selected.MaximumDurationSeconds != quote.Price.MaximumDurationSeconds {
		t.Fatalf("receipt duration %d != priced duration %d",
			receipt.Selected.MaximumDurationSeconds, quote.Price.MaximumDurationSeconds)
	}
	if receipt.Selected.GPUCount != 1 || receipt.Selected.GPUKind != "rtx4000ada" {
		t.Fatalf("selected GPU shape = %dx%q", receipt.Selected.GPUCount, receipt.Selected.GPUKind)
	}
	if receipt.Selected.AccountMode != state.PlacementAccountPlatform {
		t.Fatalf("account mode = %q, want platform for a tenant with no cloud account", receipt.Selected.AccountMode)
	}
	if receipt.AvailabilityConfidence != state.PlacementAvailabilityCreateTimeOnly {
		t.Fatalf("availability = %q; central observes no capacity before create", receipt.AvailabilityConfidence)
	}
}

// A workload reusing an existing volume can only land where that volume is —
// and the refusal must arrive from the DECISION, before any timestamp, hold,
// key or /24 has been spent on it.
func TestPlacementRefusesStorageAffinityWithoutTouchingTheVolume(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeFlyIO)
	store := state.New()
	d.WithStore(store)
	volume := &state.PersistentVolume{
		ID: "pv_pinned", TenantID: "cust_placement", Type: "persistent", Name: "models",
		Backend: backends.TypeLinode, DCRegion: "us-ord", State: "active", SizeGB: 10,
	}
	store.PutPersistentVolume(volume)

	wl := minimalPlanWorkload() // pinned to flyio
	wl.Spec.Storage = &workload.Storage{Persistent: []workload.PersistentSpec{{
		Name: "models", Target: "/models", SizeGB: 10,
	}}}

	_, err := d.Quote(context.Background(), wl, placementOptions())
	if err == nil {
		t.Fatal("Quote placed a workload away from its own data")
	}
	var refusal *PlacementRefusal
	if !errors.As(err, &refusal) || refusal.Reason != state.PlacementRejectStorageProviderMismatch {
		t.Fatalf("refusal = %v, want storage_provider_mismatch", err)
	}
	if refusal.Receipt == nil || refusal.Receipt.Constraints.StorageBackendPin != backends.TypeLinode {
		t.Fatalf("storage affinity is not recorded as a hard constraint: %+v", refusal.Receipt)
	}
	if !volume.LastUsedAt.IsZero() {
		t.Fatal("deciding a placement stamped the volume; a preview must leave storage untouched")
	}
	if backend.createCalls != 0 || meshCalls.mint.Load() != 0 || len(d.reservedSlots) != 0 {
		t.Fatalf("refusal had side effects: creates=%d mints=%d CIDRs=%d",
			backend.createCalls, meshCalls.mint.Load(), len(d.reservedSlots))
	}
}

// Deciding a placement must remain as side-effect-free as Quote has always
// been, because a preview a customer is only looking at runs the same code.
func TestDecideHasNoSideEffects(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeFlyIO)
	store := state.New()
	d.WithStore(store)
	volume := &state.PersistentVolume{
		ID: "pv_reused", TenantID: "cust_placement", Type: "persistent", Name: "models",
		Backend: backends.TypeFlyIO, State: "active", SizeGB: 10,
	}
	store.PutPersistentVolume(volume)
	wl := minimalPlanWorkload()
	wl.Spec.Storage = &workload.Storage{Persistent: []workload.PersistentSpec{{
		Name: "models", Target: "/models", SizeGB: 10,
	}}}

	if _, err := d.Quote(context.Background(), wl, placementOptions()); err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if !volume.LastUsedAt.IsZero() {
		t.Fatal("quoting stamped a reused volume")
	}
	if backend.createCalls != 0 || meshCalls.mint.Load() != 0 || len(d.reservedSlots) != 0 {
		t.Fatalf("quote side effects: creates=%d mints=%d CIDRs=%d",
			backend.createCalls, meshCalls.mint.Load(), len(d.reservedSlots))
	}
	if len(store.BurstsForCustomer("cust_placement")) != 0 {
		t.Fatal("quoting wrote a burst")
	}
}

// The launch that consumes a quote must refuse it when the decision moved,
// before it admits anything — the placement half of the shape-drift guard.
func TestPlanQuotedRefusesADriftedPlacement(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeFlyIO)
	quote, err := d.Quote(context.Background(), minimalPlanWorkload(), placementOptions())
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	// The provider shape is untouched; only the cluster the decision granted
	// moved. The shape comparison cannot see that, and the digest must.
	moved := placementOptions()
	moved.ClusterPlacement.GrantedClusterID = "cluster_west"
	_, err = d.PlanQuoted(context.Background(), minimalPlanWorkload(), moved, quote)
	if err == nil || !strings.Contains(err.Error(), "placement decision no longer matches") {
		t.Fatalf("PlanQuoted error = %v, want a placement mismatch", err)
	}
	if backend.createCalls != 0 || meshCalls.mint.Load() != 0 {
		t.Fatalf("drifted placement reached side effects: creates=%d mints=%d", backend.createCalls, meshCalls.mint.Load())
	}

	expired := *quote
	stale := *quote.Placement
	stale.IssuedAt = time.Now().UTC().Add(-2 * placementQuoteTTL)
	stale.ExpiresAt = stale.IssuedAt.Add(placementQuoteTTL)
	expired.Placement = &stale
	if _, err := d.PlanQuoted(context.Background(), minimalPlanWorkload(), placementOptions(), &expired); err == nil ||
		!strings.Contains(err.Error(), "placement decision has expired") {
		t.Fatalf("PlanQuoted honored an expired decision: %v", err)
	}
	if backend.createCalls != 0 || meshCalls.mint.Load() != 0 {
		t.Fatalf("expired placement reached side effects: creates=%d mints=%d", backend.createCalls, meshCalls.mint.Load())
	}
}

// #99 admission is what a replay resumes from, so the placement has to be part
// of the record it commits — otherwise a resumed create can buy the same shape
// under a decision the customer never saw.
func TestAdmittedProviderCreateBindsThePlacementDigest(t *testing.T) {
	d, _, admission, _, _, _ := newAdmissionSeamDecider(t)

	opts := seamPlanOptions()
	opts.ClusterPlacement = handlers.ClusterPlacementDecision{
		GrantedClusterID: "cluster_seam", Mode: state.ClusterPlacementModeAuto,
	}
	plan, err := d.Plan(context.Background(), minimalPlanWorkload(), opts)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.Placement == nil || plan.Placement.Digest == "" {
		t.Fatal("the plan carries no placement receipt to persist")
	}
	admitted := decodeSeamRequest(t, admission.admitted.ProviderCreatePayload)
	if admitted.PlacementDigest != plan.Placement.Digest {
		t.Fatalf("admitted digest = %q, plan digest = %q — a replay could resume another placement",
			admitted.PlacementDigest, plan.Placement.Digest)
	}
}

// A replay whose stored request was admitted under a DIFFERENT placement must
// not execute. The provider shape matches exactly; only the decision differs,
// which is precisely the silent substitution admission exists to stop.
func TestReplayRefusesAnotherPlacementDigest(t *testing.T) {
	d, backend, admission, _, _, _ := newAdmissionSeamDecider(t)
	opts := seamPlanOptions()

	// What a predecessor committed: the same machine, a different decision.
	original := resolveProviderCreateRequest(
		minimalPlanWorkload(), opts, "burst_replay", backends.TypeFlyIO, "",
		"", "fly-cpu", "shape_hash", "0000000000000000000000000000000000000000000000000000000000000000",
		backends.ResourceRequirements{CPUMillis: 1000, MemoryMB: 2048},
	)
	payload, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal replay payload: %v", err)
	}
	admission.replayPayload = payload
	admission.replayBurstID = "burst_replay"

	if _, err := d.Plan(context.Background(), minimalPlanWorkload(), opts); err == nil {
		t.Fatal("a replay executed under a placement it was not admitted under")
	}
	if backend.createCalls != 0 {
		t.Fatalf("the refused replay still bought a machine: creates=%d", backend.createCalls)
	}
	if admission.failCalls == 0 {
		t.Fatal("the refused replay left its operation unsettled")
	}
}

func mustDigest(t *testing.T, receipt *state.PlacementReceipt) string {
	t.Helper()
	digest, err := receipt.ComputeDigest()
	if err != nil {
		t.Fatalf("ComputeDigest: %v", err)
	}
	return digest
}

// A tenant's own cloud account is the only one a decision may report, and its
// IDENTITY never reaches the receipt: the mode is what a customer needs, the
// account id is tenant-internal cloud plumbing.
func TestPlacementReportsOnlyTheCallersOwnAccount(t *testing.T) {
	d, store, _, _, _ := tenantCloudDecider(t)
	wl := minimalPlanWorkload()
	wl.Spec.Backend = backends.TypeLinode

	owner, err := d.Quote(t.Context(), wl, handlers.PlanOptions{CustomerID: state.DevCustomerID})
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if got := owner.Placement.Selected.AccountMode; got != state.PlacementAccountTenant {
		t.Fatalf("account mode = %q, want tenant for a BYOC placement", got)
	}
	if owner.CloudAccountID == "" {
		t.Fatal("the quote resolved no tenant account; the assertion below proves nothing")
	}
	raw, err := json.Marshal(owner.Placement)
	if err != nil {
		t.Fatalf("marshal receipt: %v", err)
	}
	if strings.Contains(string(raw), owner.CloudAccountID) {
		t.Fatalf("the receipt serializes the tenant's cloud-account id: %s", raw)
	}

	// Another tenant asking the same question gets the platform account. The
	// stranger's decision must never surface the account this store holds for
	// someone else.
	store.AddCustomer(&state.Customer{ID: "cust_stranger", Token: "tok_stranger"})
	stranger, err := d.Quote(t.Context(), wl, handlers.PlanOptions{CustomerID: "cust_stranger"})
	if err != nil {
		t.Fatalf("Quote for the other tenant: %v", err)
	}
	if got := stranger.Placement.Selected.AccountMode; got != state.PlacementAccountPlatform {
		t.Fatalf("stranger account mode = %q, want platform", got)
	}
	if stranger.CloudAccountID != "" {
		t.Fatalf("the stranger's decision resolved account %q", stranger.CloudAccountID)
	}
	if stranger.Placement.Digest == owner.Placement.Digest {
		t.Fatal("two tenants' decisions share a digest; one could present the other's preview")
	}
}

// connectTenantLinodeAccount installs accountID as the tenant's BYOC Linode
// account, replacing whatever is connected. Everything except the identity is
// held constant, so a decision taken before and after differs in exactly one
// thing: WHICH of the tenant's accounts was selected.
func connectTenantLinodeAccount(t *testing.T, store *state.Store, cipher *credentialcipher.Cipher, accountID string) {
	t.Helper()
	if current, err := store.LinodeCloudAccount(state.DevCustomerID); err == nil {
		if err := store.DisconnectLinodeCloudAccount(state.DevCustomerID, current.ID,
			time.Now().UTC(), state.OperatorActor()); err != nil {
			t.Fatalf("disconnect %q: %v", current.ID, err)
		}
	}
	encrypted, err := cipher.Encrypt("tenant-token-v1",
		credentialcipher.AdditionalData(state.DevCustomerID, accountID, state.CloudProviderLinode))
	if err != nil {
		t.Fatalf("encrypt credential for %q: %v", accountID, err)
	}
	if _, _, err := store.SetLinodeCloudAccount(state.DevCustomerID, state.CloudAccount{
		ID: accountID, Provider: state.CloudProviderLinode, ProviderIdentity: "provider-uuid-" + accountID,
		Region: "us-ord", GPUImage: "private/tenant-gpu", CredentialCiphertext: encrypted,
		UpdatedAt: time.Now().UTC(),
	}, state.OperatorActor()); err != nil {
		t.Fatalf("connect %q: %v", accountID, err)
	}
}

// WHICH of a tenant's own cloud accounts a decision selected is part of the
// decision. Two BYOC accounts on one provider can serve the identical region
// and shape, so a digest that stopped at the funding MODE would let a preview
// taken under one account launch under the other — a burst billed to, and
// running under the credentials of, an account the customer never saw quoted.
//
// The binding that fixes it must not become a way to read the account back: it
// is absent from the tenant-facing projection entirely, and the account id
// appears nowhere on the wire.
func TestPlacementDigestBindsTheSelectedCloudAccount(t *testing.T) {
	d, store, cipher, _, _ := tenantCloudDecider(t)
	wl := minimalPlanWorkload()
	wl.Spec.Backend = backends.TypeLinode
	opts := handlers.PlanOptions{CustomerID: state.DevCustomerID}

	first, err := d.Quote(t.Context(), wl, opts)
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if first.CloudAccountID == "" || first.Placement.Selected.AccountMode != state.PlacementAccountTenant {
		t.Fatalf("the first decision is not BYOC: account=%q mode=%q",
			first.CloudAccountID, first.Placement.Selected.AccountMode)
	}

	connectTenantLinodeAccount(t, store, cipher, "ca_tenant_second")
	second, err := d.Quote(t.Context(), wl, opts)
	if err != nil {
		t.Fatalf("Quote after the account swap: %v", err)
	}
	if second.CloudAccountID == first.CloudAccountID {
		t.Fatal("the swap did not change the selected account; the assertions below prove nothing")
	}
	// Everything a customer reads is identical — same provider, same region,
	// same SKU, same price, same funding mode. That is the point: nothing on the
	// visible answer could have moved the digest.
	if second.Placement.Selected != first.Placement.Selected {
		t.Fatalf("the swap changed the visible selection, so this is not the case under test:\n%+v\n%+v",
			first.Placement.Selected, second.Placement.Selected)
	}
	if second.Placement.Digest == first.Placement.Digest {
		t.Fatal("switching cloud accounts left the digest unchanged; a preview taken under one account would launch under the other")
	}
	if second.Placement.AccountBinding == "" || first.Placement.AccountBinding == "" {
		t.Fatal("a tenant-funded decision carries no account binding")
	}

	// Neither the account id nor the binding may reach a tenant. The projection
	// is the whole public surface, so checking it is checking every response.
	public := state.PublicPlacementReceiptOf(first.Placement)
	if public == nil {
		t.Fatal("a valid BYOC decision does not render")
	}
	raw, err := json.Marshal(public)
	if err != nil {
		t.Fatalf("marshal public receipt: %v", err)
	}
	for _, forbidden := range []string{
		first.CloudAccountID, second.CloudAccountID,
		first.Placement.AccountBinding, second.Placement.AccountBinding,
		"account_binding", "cloud_account", "provider-uuid",
	} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("the tenant projection carries %q: %s", forbidden, raw)
		}
	}
	// What a customer legitimately needs — the funding mode and the digest they
	// launch with — is still there.
	if public.Selected.AccountMode != state.PlacementAccountTenant || public.Digest != first.Placement.Digest {
		t.Fatalf("the projection dropped what a customer must read: %+v", public)
	}
}

// A workload reusing an existing volume can only land where its data is, and
// the DC is half of that constraint. The refusal must carry the stable code and
// arrive from the DECISION — before a hold, a key, a /24 or a timestamp.
func TestPlacementRefusesAStorageRegionMismatch(t *testing.T) {
	for _, tc := range []struct {
		name        string
		volumeDC    string
		specRegion  string
		wantRefusal bool
	}{
		{name: "the workload asks for the volume's own region", volumeDC: "ord", specRegion: "ord"},
		{name: "the workload asks for another region", volumeDC: "ord", specRegion: "lhr", wantRefusal: true},
		// An unpinned workload lands in the provider account's default, which
		// central does not observe until the create returns. Refusing on the
		// priced-region fallback would reject a correct reuse on a guess.
		{name: "the workload pins no region at all", volumeDC: "ord"},
		// Nothing recorded the volume's DC, so there is nothing to compare.
		{name: "the volume records no DC", volumeDC: "", specRegion: "lhr"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeFlyIO)
			store := state.New()
			d.WithStore(store)
			volume := &state.PersistentVolume{
				ID: "pv_region", TenantID: "cust_placement", Type: "persistent", Name: "models",
				Backend: backends.TypeFlyIO, DCRegion: tc.volumeDC, State: "active", SizeGB: 10,
			}
			store.PutPersistentVolume(volume)

			wl := minimalPlanWorkload() // pinned to flyio, the volume's own backend
			wl.Spec.Region = tc.specRegion
			wl.Spec.Storage = &workload.Storage{Persistent: []workload.PersistentSpec{{
				Name: "models", Target: "/models", SizeGB: 10,
			}}}

			quote, err := d.Quote(context.Background(), wl, placementOptions())
			if !tc.wantRefusal {
				if err != nil {
					t.Fatalf("Quote refused a placement its data can serve: %v", err)
				}
				if quote.Placement.Constraints.StorageRegionPin != tc.volumeDC {
					t.Fatalf("storage region pin = %q, want %q",
						quote.Placement.Constraints.StorageRegionPin, tc.volumeDC)
				}
				return
			}
			if err == nil {
				t.Fatal("Quote placed a workload in a region its data is not in")
			}
			var refusal *PlacementRefusal
			if !errors.As(err, &refusal) {
				t.Fatalf("refusal %v carries no stable placement reason", err)
			}
			if refusal.Reason != state.PlacementRejectStorageRegionMismatch {
				t.Fatalf("reason = %q, want %q", refusal.Reason, state.PlacementRejectStorageRegionMismatch)
			}
			if refusal.Receipt == nil || refusal.Receipt.Constraints.StorageRegionPin != tc.volumeDC {
				t.Fatalf("the region affinity is not recorded as a hard constraint: %+v", refusal.Receipt)
			}
			if err := refusal.Receipt.Validate(); err != nil {
				t.Fatalf("refusal receipt is not renderable: %v", err)
			}
			if !volume.LastUsedAt.IsZero() {
				t.Fatal("the refused decision stamped the volume")
			}
			if backend.createCalls != 0 || meshCalls.mint.Load() != 0 || len(d.reservedSlots) != 0 {
				t.Fatalf("refusal had side effects: creates=%d mints=%d CIDRs=%d",
					backend.createCalls, meshCalls.mint.Load(), len(d.reservedSlots))
			}
		})
	}
}

// The LAUNCH refuses a region mismatch too, and buys nothing when it does.
//
// Which of the two checks answers is deliberately not asserted: decide() gets
// there first on every normal mismatch, and the check behind it — against the
// resolution that also writes — exists for a volume that moved between the two
// reads, an interleaving no test can stage from outside the decider. Pinning
// the OUTCOME rather than the layer is what keeps this a regression test for
// both of them: removing either one alone must not make a launch place a
// workload away from its data.
func TestPlanQuotedRefusesAStorageRegionMismatch(t *testing.T) {
	d, backend, _ := newRecordingPlanDecider(t, backends.TypeFlyIO)
	store := state.New()
	d.WithStore(store)

	wl := minimalPlanWorkload()
	wl.Spec.Region = "ord"
	wl.Spec.Storage = &workload.Storage{Persistent: []workload.PersistentSpec{{
		Name: "models", Target: "/models", SizeGB: 10,
	}}}

	// Quoted while the volume is where the workload asked to run.
	volume := &state.PersistentVolume{
		ID: "pv_moved", TenantID: "cust_placement", Type: "persistent", Name: "models",
		Backend: backends.TypeFlyIO, DCRegion: "ord", State: "active", SizeGB: 10,
	}
	store.PutPersistentVolume(volume)
	quote, err := d.Quote(context.Background(), wl, placementOptions())
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}

	// The volume is now somewhere else. decide() catches this too — the check
	// under test is the one after it, so the refusal must arrive with no machine
	// bought either way.
	volume.DCRegion = "lhr"
	store.PutPersistentVolume(volume)
	if _, err := d.PlanQuoted(context.Background(), wl, placementOptions(), quote); err == nil {
		t.Fatal("PlanQuoted attached a volume from another DC")
	} else if !strings.Contains(err.Error(), "storage volume pinned to another region") {
		t.Fatalf("PlanQuoted error = %v, want a storage region refusal", err)
	}
	if backend.createCalls != 0 {
		t.Fatalf("the refused launch still bought a machine: creates=%d", backend.createCalls)
	}
}

// An uncapped (maxHourlyUSD=0) GPU request whose shape exceeds the plan's
// capacity with system headroom must be refused BEFORE any provider or mesh
// mutation — not silently CPU-priced.
func TestPlacementRefusesUncappedGPUShapeBeforeMutation(t *testing.T) {
	d, backend, meshCalls := newRecordingPlanDecider(t, backends.TypeLinode)
	wl := gpuPlanWorkload(backends.TypeLinode, "rtx6000", 1, 1, 0)
	wl.Spec.Size = "xlarge" // 8000m/32768Mi — exceeds rtx6000 x1 with system headroom
	_, err := d.Plan(context.Background(), wl, placementOptions())
	if err == nil {
		t.Fatal("Plan succeeded for an uncapped xlarge rtx6000 request; GPU shape validation should reject it")
	}
	var refusal *PlacementRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("refusal %v carries no stable placement reason", err)
	}
	if refusal.Reason != state.PlacementRejectInsufficientCPU {
		t.Fatalf("reason = %q, want %q", refusal.Reason, state.PlacementRejectInsufficientCPU)
	}
	if backend.createCalls != 0 {
		t.Fatal("provider was contacted before the placement refusal")
	}
	if meshCalls.mint.Load() != 0 {
		t.Fatal("mesh keys were minted before the placement refusal")
	}
	if len(d.reservedSlots) != 0 {
		t.Fatal("PodCIDR was reserved before the placement refusal")
	}
}

// A supported uncapped GPU request must still Plan with the GPU SKU and rate.
func TestPlacementAdmitsUncappedGPUWithResolvedSKU(t *testing.T) {
	d, _, _ := newRecordingPlanDecider(t, backends.TypeLinode)
	wl := gpuPlanWorkload(backends.TypeLinode, "rtx4000ada", 1, 1, 0)
	quote, err := d.Quote(context.Background(), wl, placementOptions())
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if quote.SKU == "" || quote.SKU == "fly-cpu" {
		t.Fatalf("SKU = %q; want a resolved GPU plan, not a CPU fallback", quote.SKU)
	}
	if !strings.HasPrefix(quote.SKU, "g2-gpu-rtx4000a") {
		t.Fatalf("SKU = %q; want a Linode rtx4000ada GPU plan", quote.SKU)
	}
	if quote.HourlyUSD <= 0 {
		t.Fatalf("HourlyUSD = %f; want a positive GPU rate", quote.HourlyUSD)
	}
}
