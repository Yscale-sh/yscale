package state

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func sampleReceipt() PlacementReceipt {
	return PlacementReceipt{
		Version:            PlacementReceiptVersion,
		Tenant:             "cust_receipt",
		RequestedClusterID: "cluster_a",
		GrantedClusterID:   "cluster_a",
		ClusterMode:        ClusterPlacementModePinned,
		ClusterRule:        "tenant.cluster_policy",
		ClusterRuleVersion: "v1",
		Requested: PlacementRequest{
			GPUKind: "rtx4000ada", GPUCount: 1, GPUReliability: "reliable",
			CPUMillis: 4000, MemoryMB: 16384,
		},
		Constraints: PlacementConstraints{
			BackendPin: "linode", RegionPin: "us-east",
			AccountMode: PlacementAccountPlatform, AccountEligible: true,
			MaxHourlyMicroUSDPerGPU: 600_000, MaxChargeMicroUSD: 3_120_000, DeadlineSeconds: 21600,
		},
		Candidates: []PlacementCandidate{
			{Provider: "linode", Region: "us-east", SKU: "g2-gpu-rtx4000a1-s", HourlyMicroUSD: 520_000, Selected: true},
			{Provider: "aws", Reason: PlacementRejectGPUKindNotSupported},
		},
		Selected: PlacementSelection{
			Provider: "linode", Region: "us-east", SKU: "g2-gpu-rtx4000a1-s",
			GPUKind: "rtx4000ada", GPUCount: 1, CPUMillis: 4000, MemoryMB: 16384,
			AccountMode: PlacementAccountPlatform, HourlyMicroUSD: 520_000,
			MaximumDurationSeconds: 21600, MaximumChargeMicroUSD: 3_120_000,
		},
		PricingVersion:         PlacementPricingVersion,
		CandidateSetVersion:    PlacementCandidateSetVersion,
		AvailabilityConfidence: PlacementAvailabilityCreateTimeOnly,
	}
}

// The digest exists to bind a preview to a launch, so it must cover the
// decision and exclude the issuance. Both halves are load-bearing: without the
// first a changed decision would be honored, without the second no decision
// could ever be presented back.
func TestPlacementDigestCoversDecisionNotIssuance(t *testing.T) {
	base := sampleReceipt()
	if err := base.Seal("quote_1", time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC), 5*time.Minute); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	reissued := sampleReceipt()
	if err := reissued.Seal("quote_2", time.Date(2026, 8, 20, 18, 30, 0, 0, time.UTC), time.Hour); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if base.Digest != reissued.Digest {
		t.Fatalf("a second issuance changed the digest: %s vs %s", base.Digest, reissued.Digest)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*PlacementReceipt)
	}{
		{"another granted cluster", func(r *PlacementReceipt) { r.GrantedClusterID = "cluster_b" }},
		{"another rate", func(r *PlacementReceipt) { r.Selected.HourlyMicroUSD++ }},
		{"another SKU", func(r *PlacementReceipt) { r.Selected.SKU = "g2-gpu-rtx4000a1-m" }},
		{"another maximum charge", func(r *PlacementReceipt) { r.Selected.MaximumChargeMicroUSD++ }},
		{"another constraint", func(r *PlacementReceipt) { r.Constraints.MaxHourlyMicroUSDPerGPU++ }},
		{"another candidate set version", func(r *PlacementReceipt) { r.CandidateSetVersion = "v2" }},
		{"another pricing version", func(r *PlacementReceipt) { r.PricingVersion++ }},
		{"another rejection reason", func(r *PlacementReceipt) {
			r.Candidates[1].Reason = PlacementRejectProviderNotConfigured
		}},
		{"a reordered candidate list", func(r *PlacementReceipt) {
			r.Candidates[0], r.Candidates[1] = r.Candidates[1], r.Candidates[0]
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := sampleReceipt()
			changed.Candidates = append([]PlacementCandidate(nil), changed.Candidates...)
			tc.mutate(&changed)
			if err := changed.Seal("quote_1", time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC), 5*time.Minute); err != nil {
				t.Fatalf("Seal: %v", err)
			}
			if changed.Digest == base.Digest {
				t.Fatal("the decision changed but the digest did not")
			}
		})
	}
}

// Validate is the gate a receipt passes before it is persisted, rendered or
// honored. Each case below is a receipt that must never reach a customer.
func TestPlacementReceiptValidateRejectsWhatMustNotBeRendered(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*PlacementReceipt)
		reseal bool
	}{
		{name: "an unknown shape version", mutate: func(r *PlacementReceipt) { r.Version = 99 }, reseal: true},
		{name: "an unknown availability confidence", mutate: func(r *PlacementReceipt) {
			r.AvailabilityConfidence = "probably"
		}, reseal: true},
		{name: "a free-text rejection reason", mutate: func(r *PlacementReceipt) {
			r.Candidates[1].Reason = "linode: 400 Bad Request from the provider"
		}, reseal: true},
		{name: "two selected candidates", mutate: func(r *PlacementReceipt) {
			r.Candidates[1].Reason, r.Candidates[1].Selected = "", true
		}, reseal: true},
		{name: "more candidates than the bound", mutate: func(r *PlacementReceipt) {
			for len(r.Candidates) <= MaxPlacementCandidates {
				r.Candidates = append(r.Candidates, PlacementCandidate{
					Provider: "linode", Reason: PlacementRejectProviderNotAllowed})
			}
		}, reseal: true},
		{name: "a digest that does not cover the content", mutate: func(r *PlacementReceipt) {
			r.Selected.HourlyMicroUSD = 1
		}},
		// A tenant-funded decision whose account is not bound is one whose
		// account could be swapped without moving the digest.
		{name: "a tenant-funded decision with no account binding", mutate: func(r *PlacementReceipt) {
			r.Constraints.AccountMode = PlacementAccountTenant
		}, reseal: true},
		// And a binding on a platform-funded decision names an account nothing
		// selected.
		{name: "an account binding on a platform-funded decision", mutate: func(r *PlacementReceipt) {
			r.AccountBinding = PlacementAccountBinding("cust_receipt", "ca_tenant")
		}, reseal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt := sampleReceipt()
			receipt.Candidates = append([]PlacementCandidate(nil), receipt.Candidates...)
			if !tc.reseal {
				if err := receipt.Seal("quote_1", time.Now().UTC(), time.Minute); err != nil {
					t.Fatalf("Seal: %v", err)
				}
			}
			tc.mutate(&receipt)
			if tc.reseal {
				if err := receipt.Seal("quote_1", time.Now().UTC(), time.Minute); err != nil {
					t.Fatalf("Seal: %v", err)
				}
			}
			err := receipt.Validate()
			if !errors.Is(err, ErrInvalidPlacementReceipt) {
				t.Fatalf("Validate = %v, want an invalid-receipt error", err)
			}
			if SafePlacementReceipt(&receipt) != nil {
				t.Fatal("an invalid receipt was rendered as a decision central stands behind")
			}
		})
	}

	valid := sampleReceipt()
	if err := valid.Seal("quote_1", time.Now().UTC(), time.Minute); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("a well-formed receipt was refused: %v", err)
	}
	if SafePlacementReceipt(&valid) == nil {
		t.Fatal("a well-formed receipt was dropped")
	}
	if SafePlacementReceipt(nil) != nil {
		t.Fatal("nil became a decision")
	}
}

// A receipt is stored inside the workload document and served straight to a
// console, so its serialized form must survive a round trip byte-identically —
// a field that does not is a digest that changes on restart.
func TestPlacementReceiptSurvivesDurableRoundTrip(t *testing.T) {
	receipt := sampleReceipt()
	if err := receipt.Seal("quote_round", time.Now().UTC().Truncate(time.Second), 5*time.Minute); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	stored := Workload{ID: "wl_round", CustomerID: "cust_receipt", Placement: &WorkloadPlacement{
		RequestedClusterID: "cluster_a", GrantedClusterID: "cluster_a",
		Mode: ClusterPlacementModePinned, DecidedAt: time.Now().UTC(), Receipt: &receipt,
	}}
	raw, err := json.Marshal(stored)
	if err != nil {
		t.Fatalf("marshal workload: %v", err)
	}
	var restored Workload
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("unmarshal workload: %v", err)
	}
	if restored.Placement == nil || restored.Placement.Receipt == nil {
		t.Fatal("the receipt did not survive the workload document")
	}
	if err := restored.Placement.Receipt.Validate(); err != nil {
		t.Fatalf("the restored receipt no longer validates: %v", err)
	}
	if restored.Placement.Receipt.Digest != receipt.Digest {
		t.Fatalf("digest moved across a round trip: %s vs %s",
			restored.Placement.Receipt.Digest, receipt.Digest)
	}
	// A record written before receipts existed stays exactly as readable.
	var legacy Workload
	if err := json.Unmarshal([]byte(`{"ID":"wl_legacy","Placement":{"GrantedClusterID":"cluster_a","DecidedAt":"2026-01-01T00:00:00Z"}}`), &legacy); err != nil {
		t.Fatalf("unmarshal legacy workload: %v", err)
	}
	if legacy.Placement == nil || legacy.Placement.GrantedClusterID != "cluster_a" || legacy.Placement.Receipt != nil {
		t.Fatalf("legacy placement did not survive: %+v", legacy.Placement)
	}
}

// The rejection vocabulary is a closed contract. Every code issue #94 requires
// must be a member, and nothing outside the set may pass.
func TestPlacementRejectionSetIsClosedAndComplete(t *testing.T) {
	required := []string{
		"provider_not_configured", "provider_not_allowed", "provider_account_unavailable",
		"region_not_allowed", "region_image_unavailable", "sku_not_supported",
		"gpu_kind_not_supported", "gpu_count_not_supported", "insufficient_cpu",
		"insufficient_memory", "price_above_max_hourly", "maximum_charge_exceeded",
		"storage_provider_mismatch", "storage_region_mismatch",
		"cluster_target_not_validated", "capacity_not_observed",
	}
	for _, code := range required {
		if !ValidPlacementRejection(code) {
			t.Errorf("required rejection reason %q is not in the stable set", code)
		}
	}
	for _, code := range []string{"", "unknown", "PROVIDER_NOT_ALLOWED", "linode: 400 from provider"} {
		if ValidPlacementRejection(code) {
			t.Errorf("%q was admitted into the stable set", code)
		}
	}
}

// Expiry is judged against the issuance window the receipt carries, so a
// decision nothing is holding any more cannot be presented as current.
func TestPlacementReceiptExpiry(t *testing.T) {
	issued := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	receipt := sampleReceipt()
	if err := receipt.Seal("quote_1", issued, 5*time.Minute); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if receipt.Expired(issued.Add(4 * time.Minute)) {
		t.Fatal("a live decision reported itself expired")
	}
	if !receipt.Expired(issued.Add(6 * time.Minute)) {
		t.Fatal("an expired decision reported itself live")
	}
	unsealed := sampleReceipt()
	if unsealed.Expired(issued) {
		t.Fatal("a decision with no issuance window expired")
	}
}

// Nothing on the wire may carry a credential, an upstream payload or a
// provider's own words — the shape has nowhere to put one, and this is the test
// that fails if a field is ever added that does.
func TestPlacementReceiptSerializesNothingSensitive(t *testing.T) {
	receipt := tenantFundedReceipt("ca_tenant")
	if err := receipt.Seal("quote_1", time.Now().UTC(), time.Minute); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	allowed := map[string]bool{
		"version": true, "tenant": true, "requested_cluster_id": true, "granted_cluster_id": true,
		"cluster_mode": true, "cluster_rule": true, "cluster_rule_version": true,
		"requested": true, "constraints": true, "candidates": true, "selected": true,
		"pricing_version": true, "candidate_set_version": true, "availability_confidence": true,
		"quote_id": true, "issued_at": true, "expires_at": true, "digest": true,
		// The durable form carries the binding; it is a one-way digest, and
		// PublicPlacementReceipt is where it stops.
		"account_binding": true,
	}
	for field := range fields {
		if !allowed[field] {
			t.Errorf("receipt serializes an unreviewed field %q", field)
		}
	}
	if _, bound := fields["account_binding"]; !bound {
		t.Fatal("the tenant-funded fixture carries no binding; the exclusions below prove nothing")
	}
	lowered := strings.ToLower(string(raw))
	for _, forbidden := range []string{"credential", "token", "secret", "ciphertext", "cloud_account_id", "payload", "stderr", "ca_tenant"} {
		if strings.Contains(lowered, forbidden) {
			t.Errorf("receipt serializes %q: %s", forbidden, raw)
		}
	}
}

// tenantFundedReceipt is the sample decision as a BYOC placement: identical in
// everything a customer reads, differing only in WHICH of the tenant's own
// accounts it selected.
func tenantFundedReceipt(cloudAccountID string) PlacementReceipt {
	r := sampleReceipt()
	r.Constraints.AccountMode = PlacementAccountTenant
	r.Selected.AccountMode = PlacementAccountTenant
	r.Candidates[0].AccountMode = PlacementAccountTenant
	r.AccountBinding = PlacementAccountBinding(r.Tenant, cloudAccountID)
	return r
}

// The binding is what makes the account part of the decision. It must be
// stable, scoped to the account's owner, distinct per account, and one-way.
func TestPlacementAccountBindingIsScopedStableAndOneWay(t *testing.T) {
	first := PlacementAccountBinding("cust_a", "ca_one")
	if first == "" {
		t.Fatal("a selected account produced no binding")
	}
	if first != PlacementAccountBinding("cust_a", "ca_one") {
		t.Fatal("the binding is not stable; a launch could never re-derive it")
	}
	if first == PlacementAccountBinding("cust_a", "ca_two") {
		t.Fatal("two of one tenant's accounts share a binding")
	}
	if first == PlacementAccountBinding("cust_b", "ca_one") {
		t.Fatal("one account id under two tenants shares a binding")
	}
	if PlacementAccountBinding("cust_a", "") != "" {
		t.Fatal("a platform-funded decision was given an account binding")
	}
	if strings.Contains(first, "ca_one") || strings.Contains(first, "cust_a") {
		t.Fatalf("the binding carries what it was derived from: %s", first)
	}
}

// Two BYOC accounts can serve one provider, region, SKU and price. The account
// is therefore the ONLY thing that differs between these two decisions, and the
// digest must still move — otherwise a preview taken under one would launch
// under the other.
func TestPlacementDigestCoversTheSelectedAccount(t *testing.T) {
	issued := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	first := tenantFundedReceipt("ca_one")
	second := tenantFundedReceipt("ca_two")
	if err := first.Seal("quote_1", issued, 5*time.Minute); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := second.Seal("quote_1", issued, 5*time.Minute); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if first.Selected != second.Selected || first.Constraints != second.Constraints {
		t.Fatal("the two decisions differ in something a customer can see; this is not the case under test")
	}
	if first.Digest == second.Digest {
		t.Fatal("switching cloud accounts left the digest unchanged")
	}
}

// The tenant projection is an allow-list, and the binding is the field it
// exists to stop. Everything a customer needs to read the answer and launch
// against it survives; nothing central keeps for its own comparison does.
func TestPublicPlacementReceiptCarriesNoPrivateBinding(t *testing.T) {
	receipt := tenantFundedReceipt("ca_tenant")
	if err := receipt.Seal("quote_public", time.Now().UTC(), 5*time.Minute); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	public := PublicPlacementReceiptOf(&receipt)
	if public == nil {
		t.Fatal("a valid decision did not project")
	}
	raw, err := json.Marshal(public)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	allowed := map[string]bool{
		"version": true, "tenant": true, "requested_cluster_id": true, "granted_cluster_id": true,
		"cluster_mode": true, "cluster_rule": true, "cluster_rule_version": true,
		"requested": true, "constraints": true, "candidates": true, "selected": true,
		"pricing_version": true, "candidate_set_version": true, "availability_confidence": true,
		"quote_id": true, "issued_at": true, "expires_at": true, "digest": true,
	}
	for field := range fields {
		if !allowed[field] {
			t.Errorf("the tenant projection serializes an unreviewed field %q", field)
		}
	}
	for _, forbidden := range []string{"account_binding", receipt.AccountBinding, "ca_tenant"} {
		if strings.Contains(string(raw), forbidden) {
			t.Errorf("the tenant projection carries %q: %s", forbidden, raw)
		}
	}

	// The customer still gets the whole answer, including the two values a
	// launch presents back.
	if public.Digest != receipt.Digest || !public.ExpiresAt.Equal(receipt.ExpiresAt) {
		t.Fatalf("the projection dropped what a launch must present: %+v", public)
	}
	if public.Selected != receipt.Selected || public.Constraints != receipt.Constraints ||
		len(public.Candidates) != len(receipt.Candidates) {
		t.Fatalf("the projection dropped part of the decision: %+v", public)
	}
	if public.Selected.AccountMode != PlacementAccountTenant {
		t.Fatal("the projection dropped the funding mode, which is the customer's half of the account answer")
	}

	// A decision central cannot vouch for projects to nothing, for the same
	// reason SafePlacementReceipt drops it.
	tampered := receipt
	tampered.Selected.HourlyMicroUSD++
	if PublicPlacementReceiptOf(&tampered) != nil {
		t.Fatal("a receipt whose digest does not cover it was projected to a tenant")
	}
	if PublicPlacementReceiptOf(nil) != nil {
		t.Fatal("nil became a decision")
	}
}
