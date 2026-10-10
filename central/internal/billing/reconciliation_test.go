// yscale:proprietary

package billing

import "testing"

func TestCompareExternalCashReportsBoundedDifferenceCategories(t *testing.T) {
	base := ExternalCashObject{
		Provider: "stripe", ProviderAccountID: "acct_test", PaymentObjectID: "pi_one",
		CustomerID: "tenant-one", CreditedMicroUSD: 5_000_000, ReversedMicroUSD: 1_000_000,
	}
	other := base
	other.PaymentObjectID = "pi_two"
	other.CustomerID = "tenant-two"

	summary, err := compareExternalCash([]ExternalCashObject{base}, []ExternalCashObject{base})
	if err != nil || summary.TotalDifferences() != 0 {
		t.Fatalf("matching summary=%+v err=%v", summary, err)
	}

	identityAndAmount := base
	identityAndAmount.CustomerID = "tenant-other"
	identityAndAmount.ReversedMicroUSD = 2_000_000
	providerOnly := other
	ledgerOnly := base
	ledgerOnly.PaymentObjectID = "pi_three"
	summary, err = compareExternalCash(
		[]ExternalCashObject{base, ledgerOnly},
		[]ExternalCashObject{identityAndAmount, providerOnly},
	)
	if err != nil || summary.ProviderOnly != 1 || summary.LedgerOnly != 1 ||
		summary.Identity != 1 || summary.Amount != 1 || summary.TotalDifferences() != 4 {
		t.Fatalf("difference summary=%+v err=%v", summary, err)
	}
}

func TestCompareExternalCashRejectsInvalidAndDuplicateObjects(t *testing.T) {
	object := ExternalCashObject{
		Provider: "stripe", ProviderAccountID: "acct_test", PaymentObjectID: "pi_one",
		CustomerID: "tenant-one", CreditedMicroUSD: 5_000_000,
	}
	if _, err := compareExternalCash([]ExternalCashObject{object, object}, nil); err == nil {
		t.Fatal("duplicate ledger objects accepted")
	}
	object.ReversedMicroUSD = object.CreditedMicroUSD + 1
	if _, err := compareExternalCash(nil, []ExternalCashObject{object}); err == nil {
		t.Fatal("over-reversed provider object accepted")
	}
}

func TestCompareUsageCapturesAcceptsAuthoritativeCaptureAndZeroRelease(t *testing.T) {
	receipt := UsageReceipt{
		CustomerID: "tenant-one", WorkloadID: "workload-one", WorkloadRef: "workload-one",
		HoldID: 1, ReservedMicroUSD: 1_000_000, BurstID: "burst-one", CostPresent: true,
		CostBurstID: "burst-one", Backend: "linode", Authoritative: true,
		EstimatedUSD: 0.5, HourlyUSD: 1,
	}
	capture := usageCapture{
		CustomerID: "tenant-one", HoldID: 1, WorkloadRef: "workload-one",
		ReservedMicroUSD: 1_000_000, State: HoldCaptured, Provider: "linode",
		ProviderRateMicroUSDPerHour: 1_000_000, CapturedMicroUSD: 500_000,
		CaptureLedgerID: 1, CaptureLedgerMicroUSD: 500_000, CaptureExternalRef: "burst-one",
	}
	summary, err := compareUsageCaptures([]UsageReceipt{receipt}, []usageCapture{capture})
	if err != nil || summary.TotalDifferences() != 0 {
		t.Fatalf("matching capture summary=%+v err=%v", summary, err)
	}

	receipt.EstimatedUSD = 0
	capture.State = HoldReleased
	capture.CapturedMicroUSD = 0
	capture.CaptureLedgerID = 0
	capture.CaptureLedgerMicroUSD = 0
	capture.CaptureExternalRef = ""
	summary, err = compareUsageCaptures([]UsageReceipt{receipt}, []usageCapture{capture})
	if err != nil || summary.TotalDifferences() != 0 {
		t.Fatalf("zero-cost release summary=%+v err=%v", summary, err)
	}
}

func TestCompareUsageCapturesReportsBoundedLifecycleDifferences(t *testing.T) {
	receipt := UsageReceipt{
		CustomerID: "tenant-one", WorkloadID: "workload-one", WorkloadRef: "workload-one",
		HoldID: 1, ReservedMicroUSD: 1_000_000, BurstID: "burst-one",
	}
	capture := usageCapture{
		CustomerID: "tenant-one", HoldID: 1, WorkloadRef: "workload-one",
		ReservedMicroUSD: 1_000_000, State: HoldPending, Provider: "linode",
		ProviderRateMicroUSDPerHour: 1_000_000,
	}
	summary, err := compareUsageCaptures([]UsageReceipt{receipt}, []usageCapture{capture})
	if err != nil || summary.TotalDifferences() != 0 {
		t.Fatalf("live pending hold summary=%+v err=%v", summary, err)
	}
	receipt.ManualAttention = true
	summary, err = compareUsageCaptures([]UsageReceipt{receipt}, []usageCapture{capture})
	if err != nil || summary.Unsettled != 1 || summary.TotalDifferences() != 1 {
		t.Fatalf("manual pending hold summary=%+v err=%v", summary, err)
	}
	capture.State = HoldReleased
	summary, err = compareUsageCaptures([]UsageReceipt{receipt}, []usageCapture{capture})
	if err != nil || summary.TotalDifferences() != 0 {
		t.Fatalf("historical manual flag blocked convergence summary=%+v err=%v", summary, err)
	}
	capture.State = HoldCaptured
	capture.CapturedMicroUSD = 1
	capture.CaptureLedgerID = 1
	capture.CaptureLedgerMicroUSD = 1
	summary, err = compareUsageCaptures([]UsageReceipt{receipt}, []usageCapture{capture})
	if err != nil || summary.UsageMissing != 1 {
		t.Fatalf("capture without usage summary=%+v err=%v", summary, err)
	}
}

func TestCompareUsageCapturesRejectsWrongSettlementAndMissingAssociations(t *testing.T) {
	receipt := UsageReceipt{
		CustomerID: "tenant-one", WorkloadID: "workload-one", WorkloadRef: "workload-one",
		HoldID: 1, ReservedMicroUSD: 1_000_000, BurstID: "burst-one", CostPresent: true,
		CostBurstID: "burst-other", Backend: "linode", EstimatedUSD: 0.5, HourlyUSD: 1,
		AuthoritativeUsageRequired: true,
	}
	capture := usageCapture{
		CustomerID: "tenant-one", HoldID: 1, WorkloadRef: "workload-one",
		ReservedMicroUSD: 1_000_000, State: HoldCaptured, Provider: "linode",
		ProviderRateMicroUSDPerHour: 2_000_000, CapturedMicroUSD: 400_000,
		CaptureLedgerID: 1, CaptureLedgerMicroUSD: 400_000, CaptureExternalRef: "burst-wrong",
	}
	summary, err := compareUsageCaptures([]UsageReceipt{receipt}, []usageCapture{capture})
	if err != nil || summary.NonAuthoritative != 1 || summary.Association != 1 || summary.CaptureAmount != 2 {
		t.Fatalf("bad settlement summary=%+v err=%v", summary, err)
	}

	summary, err = compareUsageCaptures([]UsageReceipt{receipt}, nil)
	if err != nil || summary.HoldMissing != 1 {
		t.Fatalf("missing hold summary=%+v err=%v", summary, err)
	}
	summary, err = compareUsageCaptures(nil, []usageCapture{capture})
	if err != nil || summary.UsageMissing != 1 {
		t.Fatalf("missing usage summary=%+v err=%v", summary, err)
	}
}

func TestCompareUsageCapturesGrandfathersOnlyTheLegacyAuthorityProof(t *testing.T) {
	receipt := UsageReceipt{
		CustomerID: "tenant-one", WorkloadID: "workload-one", WorkloadRef: "workload-one",
		HoldID: 1, ReservedMicroUSD: 1_000_000, BurstID: "burst-one", CostPresent: true,
		CostBurstID: "burst-one", Backend: "linode", EstimatedUSD: 0.5, HourlyUSD: 1,
	}
	capture := usageCapture{
		CustomerID: "tenant-one", HoldID: 1, WorkloadRef: "workload-one",
		ReservedMicroUSD: 1_000_000, State: HoldCaptured, Provider: "linode",
		ProviderRateMicroUSDPerHour: 1_000_000, CapturedMicroUSD: 500_000,
		CaptureLedgerID: 1, CaptureLedgerMicroUSD: 500_000, CaptureExternalRef: "burst-one",
	}
	summary, err := compareUsageCaptures([]UsageReceipt{receipt}, []usageCapture{capture})
	if err != nil || summary.TotalDifferences() != 0 || summary.LegacyNonAuthoritative != 1 {
		t.Fatalf("legacy capture summary=%+v err=%v", summary, err)
	}

	receipt.AuthoritativeUsageRequired = true
	summary, err = compareUsageCaptures([]UsageReceipt{receipt}, []usageCapture{capture})
	if err != nil || summary.NonAuthoritative != 1 || summary.TotalDifferences() != 1 {
		t.Fatalf("post-activation capture summary=%+v err=%v", summary, err)
	}

	receipt.AuthoritativeUsageRequired = false
	capture.WorkloadRef = "other-workload"
	summary, err = compareUsageCaptures([]UsageReceipt{receipt}, []usageCapture{capture})
	if err != nil || summary.Association != 1 || summary.TotalDifferences() != 1 {
		t.Fatalf("legacy association mismatch summary=%+v err=%v", summary, err)
	}
}

func TestCompareUsageCapturesRejectsDuplicateReceipts(t *testing.T) {
	receipt := UsageReceipt{
		CustomerID: "tenant-one", WorkloadID: "workload-one", WorkloadRef: "workload-one",
		HoldID: 1, ReservedMicroUSD: 1,
	}
	if _, err := compareUsageCaptures([]UsageReceipt{receipt, receipt}, nil); err == nil {
		t.Fatal("duplicate usage receipts accepted")
	}
}
