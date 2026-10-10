package v1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yscale-sh/yscale/pkg/workload"
)

// --- DeepCopy ---

func TestDeepCopyWorkloadPreservesNewStatusFields(t *testing.T) {
	now := metav1.Now()
	original := &Workload{
		Status: WorkloadStatus{
			Phase:              "Running",
			Reason:             "ProvisioningStarted",
			WorkloadID:         "wl_abc",
			BurstID:            "burst_xyz",
			Backend:            "linode",
			Region:             "us-ord",
			ProviderClass:      "reliable",
			GPU:                &GPUStatus{Kind: "l4", Count: 2, Product: "NVIDIA L4"},
			PodName:            "hello-abc",
			StartedAt:          &now,
			SubmittedAt:        &now,
			NodeObservedAt:     &now,
			CleanupRequestedAt: &now,
			CleanupUpdatedAt:   &now,
			ProviderCreatedAt:  &now,
			ProviderDeletedAt:  &now,
			CostSoFarUSD:       "0.42",
			Conditions: []metav1.Condition{{
				Type:   ConditionAccepted,
				Status: metav1.ConditionTrue,
				Reason: ReasonAccepted,
			}},
		},
	}

	copied := original.DeepCopyObject().(*Workload)

	if copied.Status.WorkloadID != "wl_abc" {
		t.Errorf("WorkloadID = %q, want %q", copied.Status.WorkloadID, "wl_abc")
	}
	if copied.Status.Region != "us-ord" {
		t.Errorf("Region = %q, want %q", copied.Status.Region, "us-ord")
	}
	if copied.Status.ProviderClass != "reliable" {
		t.Errorf("ProviderClass = %q, want %q", copied.Status.ProviderClass, "reliable")
	}
	if copied.Status.Reason != "ProvisioningStarted" {
		t.Errorf("Reason = %q, want %q", copied.Status.Reason, "ProvisioningStarted")
	}
	if copied.Status.GPU == nil {
		t.Fatal("GPU is nil after deep copy")
	}
	if copied.Status.GPU.Kind != "l4" || copied.Status.GPU.Count != 2 || copied.Status.GPU.Product != "NVIDIA L4" {
		t.Errorf("GPU = %+v, want {l4, 2}", copied.Status.GPU)
	}

	// Mutating the copy must not affect the original.
	copied.Status.GPU.Kind = "a100"
	copied.Status.GPU.Count = 8
	copied.Status.GPU.Product = "NVIDIA A100 40GB"
	if original.Status.GPU.Kind != "l4" || original.Status.GPU.Count != 2 || original.Status.GPU.Product != "NVIDIA L4" {
		t.Error("mutating copied GPU affected the original")
	}
	copied.Status.ProviderCreatedAt.Time = copied.Status.ProviderCreatedAt.Add(time.Hour)
	if !original.Status.ProviderCreatedAt.Equal(&now) {
		t.Error("mutating copied provider creation timestamp affected the original")
	}
	copied.Status.ProviderDeletedAt.Time = copied.Status.ProviderDeletedAt.Add(time.Hour)
	if !original.Status.ProviderDeletedAt.Equal(&now) {
		t.Error("mutating copied provider deletion timestamp affected the original")
	}
}

func TestDeepCopyWorkloadNilGPU(t *testing.T) {
	original := &Workload{
		Status: WorkloadStatus{Phase: "Pending"},
	}
	copied := original.DeepCopyObject().(*Workload)
	if copied.Status.GPU != nil {
		t.Errorf("GPU should be nil for non-GPU workload, got %+v", copied.Status.GPU)
	}
}

func TestDeepCopyWorkloadListIndependentItems(t *testing.T) {
	list := &WorkloadList{
		Items: []Workload{
			{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"source": "original"}},
				Spec:       WorkloadSpec{Spec: workload.Spec{GPU: &workload.GPURequest{Kind: "h100", Count: 4}}},
				Status:     WorkloadStatus{Phase: "Running", GPU: &GPUStatus{Kind: "h100", Count: 4}, Conditions: []metav1.Condition{{Type: ConditionAccepted}}},
			},
			{Status: WorkloadStatus{Phase: "Pending"}},
		},
	}
	copied := list.DeepCopyObject().(*WorkloadList)
	if len(copied.Items) != 2 {
		t.Fatalf("Items len = %d, want 2", len(copied.Items))
	}
	if copied.Items[0].Status.GPU == nil || copied.Items[0].Status.GPU.Kind != "h100" {
		t.Error("first item GPU not preserved")
	}
	copied.Items[0].Labels["source"] = "copy"
	copied.Items[0].Spec.GPU.Kind = "a100"
	copied.Items[0].Status.GPU.Kind = "l4"
	copied.Items[0].Status.Conditions[0].Type = ConditionComplete
	if list.Items[0].Labels["source"] != "original" || list.Items[0].Spec.GPU.Kind != "h100" ||
		list.Items[0].Status.GPU.Kind != "h100" || list.Items[0].Status.Conditions[0].Type != ConditionAccepted {
		t.Fatal("mutating a copied list item changed the original")
	}
}

func TestDeepCopyNilWorkload(t *testing.T) {
	var w *Workload
	if w.DeepCopyObject() != nil {
		t.Error("nil Workload DeepCopy should return nil")
	}
}

func TestDeepCopyNilWorkloadList(t *testing.T) {
	var l *WorkloadList
	if l.DeepCopyObject() != nil {
		t.Error("nil WorkloadList DeepCopy should return nil")
	}
}

// --- JSON roundtrip ---

// TestWorkloadStatusJSONRoundtripNodeName pins the wire shape of the
// central-authoritative node projection: camelCase nodeName, omitted when
// empty, preserved through decode.
func TestWorkloadStatusJSONRoundtripNodeName(t *testing.T) {
	in := &Workload{
		ObjectMeta: metav1.ObjectMeta{Name: "train", Namespace: "ml"},
		Status: WorkloadStatus{
			Phase:        "Running",
			WorkloadID:   "wl_abc",
			NodeName:     "ip-192-0-2-4",
			CostSoFarUSD: "1.25",
		},
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"nodeName":"ip-192-0-2-4"`) {
		t.Errorf("json missing nodeName: %s", data)
	}

	var back Workload
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Status.NodeName != "ip-192-0-2-4" {
		t.Errorf("NodeName roundtrip = %q, want %q", back.Status.NodeName, "ip-192-0-2-4")
	}
	if back.Status.CostSoFarUSD != "1.25" {
		t.Errorf("CostSoFarUSD roundtrip = %q, want %q", back.Status.CostSoFarUSD, "1.25")
	}

	// An empty NodeName must stay omitted — the field is optional so old
	// agents and central documents without it decode unchanged.
	empty, err := json.Marshal(&Workload{Status: WorkloadStatus{Phase: "Pending"}})
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	if strings.Contains(string(empty), "nodeName") {
		t.Errorf("empty nodeName should be omitted: %s", empty)
	}
}

// --- Conditions ---

func TestSetConditionAppends(t *testing.T) {
	var conditions []metav1.Condition
	SetCondition(&conditions, metav1.Condition{
		Type:    ConditionAccepted,
		Status:  metav1.ConditionTrue,
		Reason:  ReasonAccepted,
		Message: "workload accepted",
	})
	if len(conditions) != 1 {
		t.Fatalf("len = %d, want 1", len(conditions))
	}
	if conditions[0].Type != ConditionAccepted {
		t.Errorf("Type = %q", conditions[0].Type)
	}
	if conditions[0].LastTransitionTime.IsZero() {
		t.Error("LastTransitionTime should be set on append")
	}
}

func TestSetConditionUpdatesInPlace(t *testing.T) {
	originalTransition := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	conditions := []metav1.Condition{{
		Type:               ConditionAccepted,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonSubmitFailed,
		LastTransitionTime: originalTransition,
	}}

	// Update with same status — LastTransitionTime preserved.
	SetCondition(&conditions, metav1.Condition{
		Type:   ConditionAccepted,
		Status: metav1.ConditionFalse,
		Reason: ReasonGatewayRequired,
	})
	if len(conditions) != 1 {
		t.Fatalf("len = %d, want 1", len(conditions))
	}
	if conditions[0].Reason != ReasonGatewayRequired {
		t.Errorf("Reason = %q, want %q", conditions[0].Reason, ReasonGatewayRequired)
	}
	if !conditions[0].LastTransitionTime.Equal(&originalTransition) {
		t.Error("LastTransitionTime should be preserved when status unchanged")
	}

	// Update with changed status — LastTransitionTime updated.
	SetCondition(&conditions, metav1.Condition{
		Type:   ConditionAccepted,
		Status: metav1.ConditionTrue,
		Reason: ReasonAccepted,
	})
	if conditions[0].Status != metav1.ConditionTrue {
		t.Error("status should be True")
	}
	if !conditions[0].LastTransitionTime.After(originalTransition.Time) {
		t.Error("LastTransitionTime should be updated when status changes")
	}
}

func TestSetConditionNilSlicePointer(t *testing.T) {
	// Should not panic.
	SetCondition(nil, metav1.Condition{Type: ConditionAccepted, Status: metav1.ConditionTrue})
}

func TestFindCondition(t *testing.T) {
	conditions := []metav1.Condition{
		{Type: ConditionComplete, Status: metav1.ConditionFalse},
		{Type: ConditionAccepted, Status: metav1.ConditionTrue},
	}

	c := FindCondition(conditions, ConditionAccepted)
	if c == nil {
		t.Fatal("should find Accepted")
	}
	if c.Status != metav1.ConditionTrue {
		t.Errorf("Status = %q, want True", c.Status)
	}

	if FindCondition(conditions, ConditionNodeReady) != nil {
		t.Error("should not find NodeReady")
	}

	if FindCondition(nil, ConditionAccepted) != nil {
		t.Error("nil conditions should return nil")
	}
}

func TestIsConditionTrue(t *testing.T) {
	conditions := []metav1.Condition{
		{Type: ConditionComplete, Status: metav1.ConditionFalse},
		{Type: ConditionAccepted, Status: metav1.ConditionTrue},
	}

	if IsConditionTrue(conditions, ConditionComplete) {
		t.Error("Complete is False, should not be true")
	}
	if !IsConditionTrue(conditions, ConditionAccepted) {
		t.Error("Accepted is True")
	}
	if IsConditionTrue(conditions, ConditionNodeReady) {
		t.Error("absent condition should not be true")
	}
}

// --- Labels ---

func TestValidateLabelValue(t *testing.T) {
	tests := []struct {
		value   string
		wantErr bool
	}{
		{"", false},
		{"reliable", false},
		{"l4", false},
		{"us-ord", false},
		{"burst_abc123", false},
		{"a", false},
		{"A100-SXM4", false},
		{"1", false},
		{"this.is.fine", false},

		// Bad values.
		{"-starts-with-dash", true},
		{"ends-with-dash-", true},
		{".starts-with-dot", true},
		{"has spaces", true},
		{"has/slash", true},
		{"has:colon", true},
		{string(make([]byte, 64)), true}, // 64 chars, all zero bytes
	}
	for _, tt := range tests {
		err := ValidateLabelValue(tt.value)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateLabelValue(%q) error = %v, wantErr = %v", tt.value, err, tt.wantErr)
		}
	}
}

func TestValidateLabelValueTooLong(t *testing.T) {
	long := make([]byte, 64)
	for i := range long {
		long[i] = 'a'
	}
	if err := ValidateLabelValue(string(long)); err == nil {
		t.Error("64-char label value should be rejected")
	}
	// 63 chars should be fine.
	short := long[:63]
	if err := ValidateLabelValue(string(short)); err != nil {
		t.Errorf("63-char label value should be valid: %v", err)
	}
}

func TestValidateGPUCountLabel(t *testing.T) {
	tests := []struct {
		value   string
		wantErr bool
	}{
		{"1", false},
		{"2", false},
		{"4", false},
		{"8", false},
		{"64", false},

		{"0", true},
		{"-1", true},
		{"65", true},
		{"abc", true},
		{"", true},
		{"1.5", true},
	}
	for _, tt := range tests {
		err := ValidateGPUCountLabel(tt.value)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateGPUCountLabel(%q) error = %v, wantErr = %v", tt.value, err, tt.wantErr)
		}
	}
}

func TestGPUCountLabelValue(t *testing.T) {
	if v := GPUCountLabelValue(0); v != "1" {
		t.Errorf("count 0 should default to %q, got %q", "1", v)
	}
	if v := GPUCountLabelValue(4); v != "4" {
		t.Errorf("count 4 = %q, want %q", v, "4")
	}
	if v := GPUCountLabelValue(-1); v != "-1" {
		t.Errorf("negative count must remain invalid for downstream validation, got %q", v)
	}
}

// --- Label constants are valid K8s label values ---

func TestLabelConstantsAreValidKeys(t *testing.T) {
	keys := []string{
		LabelBurstID,
		LabelProviderClass,
		LabelRegion,
		LabelGPUKind,
		LabelGPUCount,
		LabelNvidiaGPUPresent,
		LabelNvidiaGPUProduct,
		LabelWorkloadName,
		LabelWorkloadID,
	}
	for _, k := range keys {
		if err := ValidateLabelKey(k); err != nil {
			t.Errorf("label key %q is invalid: %v", k, err)
		}
	}
}

func TestValidateLabelKeyRejectsMalformedKey(t *testing.T) {
	if err := ValidateLabelKey("bad prefix/has spaces"); err == nil {
		t.Fatal("expected malformed qualified label key to fail")
	}
}

// --- Condition constants are non-empty and distinct ---

func TestConditionTypesAreDistinct(t *testing.T) {
	types := []string{
		ConditionAccepted,
		ConditionCapacityReady,
		ConditionNodeReady,
		ConditionGPUReady,
		ConditionScheduled,
		ConditionComplete,
		ConditionCleanupComplete,
	}
	seen := map[string]bool{}
	for _, ct := range types {
		if ct == "" {
			t.Error("condition type must not be empty")
		}
		if seen[ct] {
			t.Errorf("duplicate condition type %q", ct)
		}
		seen[ct] = true
	}
}

func TestReasonConstantsAreNonEmpty(t *testing.T) {
	reasons := []string{
		ReasonSubmitFailed,
		ReasonAccepted,
		ReasonPlacementAccepted,
		ReasonProvisioningStarted,
		ReasonWorkloadRunning,
		ReasonWorkloadSucceeded,
		ReasonWorkloadFailed,
		ReasonWorkloadCancelled,
		ReasonBackoffLimitExceeded,
		ReasonDeadlineExceeded,
		ReasonGatewayRequired,
		ReasonNodeJoining,
		ReasonNodeReady,
		ReasonNodeNotReady,
		ReasonNodeRemoved,
	}
	for _, r := range reasons {
		if r == "" {
			t.Error("reason constant must not be empty")
		}
	}
}

// --- Lifecycle status fields ---

func TestWorkloadStatusLifecycleFieldsJSONRoundtrip(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC))
	in := &Workload{
		Status: WorkloadStatus{
			Phase:                  "Succeeded",
			FinalCostUSD:           "1.234000",
			SKU:                    "g6-standard-2",
			HourlyUSD:              "2.500000",
			MaximumChargeUSD:       "2.000000",
			MaximumDurationSeconds: 3600,
			ClusterID:              "cluster_abc",
			Outcome:                "Succeeded",
			CleanupState:           "terminated",
			SubmittedAt:            &now,
			NodeObservedAt:         &now,
			CleanupRequestedAt:     &now,
			CleanupUpdatedAt:       &now,
			ProviderCreatedAt:      &now,
			ProviderDeletedAt:      &now,
		},
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, field := range []string{"finalCostUSD", "sku", "hourlyUSD", "maximumChargeUSD", "maximumDurationSeconds", "clusterID", "outcome", "cleanupState", "submittedAt", "nodeObservedAt", "cleanupRequestedAt", "cleanupUpdatedAt", "providerCreatedAt", "providerDeletedAt"} {
		if !strings.Contains(string(data), field) {
			t.Errorf("json missing %s: %s", field, data)
		}
	}

	var back Workload
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Status.FinalCostUSD != "1.234000" {
		t.Errorf("FinalCostUSD = %q, want %q", back.Status.FinalCostUSD, "1.234000")
	}
	if back.Status.SKU != "g6-standard-2" {
		t.Errorf("SKU = %q, want %q", back.Status.SKU, "g6-standard-2")
	}
	if back.Status.HourlyUSD != "2.500000" {
		t.Errorf("HourlyUSD = %q, want %q", back.Status.HourlyUSD, "2.500000")
	}
	if back.Status.ClusterID != "cluster_abc" {
		t.Errorf("ClusterID = %q, want %q", back.Status.ClusterID, "cluster_abc")
	}
	if back.Status.Outcome != "Succeeded" {
		t.Errorf("Outcome = %q, want %q", back.Status.Outcome, "Succeeded")
	}
	if back.Status.CleanupState != "terminated" || back.Status.ProviderCreatedAt == nil || !back.Status.ProviderCreatedAt.Equal(&now) || back.Status.ProviderDeletedAt == nil || !back.Status.ProviderDeletedAt.Equal(&now) {
		t.Errorf("cleanup roundtrip = %q/%v/%v", back.Status.CleanupState, back.Status.ProviderCreatedAt, back.Status.ProviderDeletedAt)
	}

	// Empty lifecycle fields must be omitted.
	empty, err := json.Marshal(&Workload{Status: WorkloadStatus{Phase: "Pending"}})
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	for _, field := range []string{"finalCostUSD", "sku", "hourlyUSD", "maximumChargeUSD", "maximumDurationSeconds", "clusterID", "outcome", "cleanupState", "submittedAt", "nodeObservedAt", "cleanupRequestedAt", "cleanupUpdatedAt", "providerCreatedAt", "providerDeletedAt"} {
		if strings.Contains(string(empty), field) {
			t.Errorf("empty %s should be omitted: %s", field, empty)
		}
	}
}

func TestCleanupReasonConstantsAreNonEmpty(t *testing.T) {
	reasons := []string{
		ReasonCleanupProven,
		ReasonCleanupQueued,
		ReasonCleanupDeleting,
		ReasonCleanupRetrying,
		ReasonCleanupFailed,
		ReasonManualAttention,
	}
	for _, r := range reasons {
		if r == "" {
			t.Error("cleanup reason constant must not be empty")
		}
	}
}

// --- Live GPU telemetry (gpuUtilPercent + lastHeartbeatAt) ---

// TestWorkloadStatusGPUTelemetryJSONRoundtripPreservesObservedZero pins the
// wire shape of the projected GPU telemetry pair. An observed idle GPU (0%)
// must survive as `"gpuUtilPercent":0` — a JSON number, not omitted — because
// nil/omitted is how absent telemetry is expressed on this API.
func TestWorkloadStatusGPUTelemetryJSONRoundtripPreservesObservedZero(t *testing.T) {
	heartbeat := metav1.NewTime(time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC))
	zero := 0.0
	in := &Workload{
		Status: WorkloadStatus{
			Phase:           "Running",
			GPUUtilPercent:  &zero,
			LastHeartbeatAt: &heartbeat,
		},
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"gpuUtilPercent":0`) {
		t.Errorf("observed 0%% GPU util must render as JSON number 0, got: %s", data)
	}
	if !strings.Contains(string(data), `"lastHeartbeatAt":"2026-08-25T14:30:00Z"`) {
		t.Errorf("heartbeat missing from wire: %s", data)
	}

	var back Workload
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Status.GPUUtilPercent == nil || *back.Status.GPUUtilPercent != 0 {
		t.Errorf("GPUUtilPercent roundtrip = %v, want observed 0", back.Status.GPUUtilPercent)
	}
	if back.Status.LastHeartbeatAt == nil || !back.Status.LastHeartbeatAt.Equal(&heartbeat) {
		t.Errorf("LastHeartbeatAt roundtrip = %v, want %v", back.Status.LastHeartbeatAt, heartbeat)
	}

	// Absent telemetry stays absent (nil pointers omitted, no `"gpuUtilPercent":0` faked).
	empty, err := json.Marshal(&Workload{Status: WorkloadStatus{Phase: "Pending"}})
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	for _, field := range []string{"gpuUtilPercent", "lastHeartbeatAt"} {
		if strings.Contains(string(empty), field) {
			t.Errorf("absent %s must be omitted: %s", field, empty)
		}
	}
}

// TestDeepCopyWorkloadIndependentGPUTelemetry proves DeepCopyObject clones
// the two GPU telemetry pointers so mutating a copy cannot corrupt the
// original — the invariant every projection loop relies on.
func TestDeepCopyWorkloadIndependentGPUTelemetry(t *testing.T) {
	heartbeat := metav1.NewTime(time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC))
	util := 42.5
	original := &Workload{
		Status: WorkloadStatus{
			GPUUtilPercent:  &util,
			LastHeartbeatAt: &heartbeat,
		},
	}
	copied := original.DeepCopyObject().(*Workload)
	if copied.Status.GPUUtilPercent == nil || *copied.Status.GPUUtilPercent != 42.5 {
		t.Fatalf("GPUUtilPercent not copied: %v", copied.Status.GPUUtilPercent)
	}
	if copied.Status.LastHeartbeatAt == nil || !copied.Status.LastHeartbeatAt.Equal(&heartbeat) {
		t.Fatalf("LastHeartbeatAt not copied: %v", copied.Status.LastHeartbeatAt)
	}
	*copied.Status.GPUUtilPercent = 99
	copied.Status.LastHeartbeatAt.Time = heartbeat.Add(time.Hour)
	if *original.Status.GPUUtilPercent != 42.5 {
		t.Error("mutating copied GPUUtilPercent affected the original")
	}
	if !original.Status.LastHeartbeatAt.Equal(&heartbeat) {
		t.Error("mutating copied LastHeartbeatAt affected the original")
	}
}

// --- GPUStatus ---

func TestGPUStatusZeroValueIsNilSafe(t *testing.T) {
	s := WorkloadStatus{}
	if s.GPU != nil {
		t.Error("zero-value GPU should be nil")
	}
	copied := (&Workload{Status: s}).DeepCopyObject().(*Workload)
	if copied.Status.GPU != nil {
		t.Error("deep-copied zero-value GPU should be nil")
	}
}

// --- Additive receipt fields (compute + artifact) ---

// TestWorkloadStatusReceiptFieldsJSONRoundtripPreservesCountedZero pins the
// wire shape of the receipt projection. compute and artifact legs render
// under distinct camelCase keys; the legacy status.outcome string is
// preserved unchanged so existing tooling and the printcolumn keep working.
// A counted-zero upload must render as JSON number 0 (not omitted), because
// null/omitted on this API means "did not count".
func TestWorkloadStatusReceiptFieldsJSONRoundtripPreservesCountedZero(t *testing.T) {
	var (
		zero  int64 = 0
		bytes int64 = 4096
	)
	in := &Workload{
		Status: WorkloadStatus{
			Phase:                   "Succeeded",
			Outcome:                 "Succeeded",
			ComputeResult:           "succeeded",
			ComputeReason:           "",
			ArtifactResult:          "succeeded",
			ArtifactReason:          "",
			ArtifactObjectsUploaded: &zero,
			ArtifactBytesUploaded:   &bytes,
		},
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{
		`"outcome":"Succeeded"`,
		`"computeResult":"succeeded"`,
		`"artifactResult":"succeeded"`,
		`"artifactObjectsUploaded":0`,
		`"artifactBytesUploaded":4096`,
	} {
		if !strings.Contains(string(data), want) {
			t.Errorf("json missing %q: %s", want, data)
		}
	}
	// Empty reason strings must be omitted — omitempty means "unset", the
	// same contract every other reason field on this status carries.
	for _, absent := range []string{`"computeReason"`, `"artifactReason"`} {
		if strings.Contains(string(data), absent) {
			t.Errorf("empty %s should be omitted: %s", absent, data)
		}
	}

	var back Workload
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Status.ComputeResult != "succeeded" || back.Status.ArtifactResult != "succeeded" {
		t.Errorf("result roundtrip = %q/%q", back.Status.ComputeResult, back.Status.ArtifactResult)
	}
	if back.Status.ArtifactObjectsUploaded == nil || *back.Status.ArtifactObjectsUploaded != 0 {
		t.Errorf("ArtifactObjectsUploaded roundtrip = %v, want counted 0", back.Status.ArtifactObjectsUploaded)
	}
	if back.Status.ArtifactBytesUploaded == nil || *back.Status.ArtifactBytesUploaded != 4096 {
		t.Errorf("ArtifactBytesUploaded roundtrip = %v, want 4096", back.Status.ArtifactBytesUploaded)
	}

	// Absent counts stay absent — nil pointers must not fabricate a 0 on
	// the wire (that would claim an upload the exporter never measured).
	empty, err := json.Marshal(&Workload{Status: WorkloadStatus{Phase: "Pending"}})
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	for _, field := range []string{"computeResult", "computeReason", "artifactResult", "artifactReason", "artifactObjectsUploaded", "artifactBytesUploaded"} {
		if strings.Contains(string(empty), field) {
			t.Errorf("absent %s must be omitted: %s", field, empty)
		}
	}
}

// TestDeepCopyWorkloadIndependentArtifactCounts proves DeepCopyObject clones
// the two artifact count pointers so a mutation on the copy cannot bleed
// back into the original — the invariant every projection loop relies on
// when it hands a CR to a caller.
func TestDeepCopyWorkloadIndependentArtifactCounts(t *testing.T) {
	var (
		objects int64 = 7
		bytes   int64 = 4096
	)
	original := &Workload{
		Status: WorkloadStatus{
			ArtifactResult:          "succeeded",
			ArtifactObjectsUploaded: &objects,
			ArtifactBytesUploaded:   &bytes,
		},
	}
	copied := original.DeepCopyObject().(*Workload)
	if copied.Status.ArtifactObjectsUploaded == nil || *copied.Status.ArtifactObjectsUploaded != 7 {
		t.Fatalf("ArtifactObjectsUploaded not copied: %v", copied.Status.ArtifactObjectsUploaded)
	}
	if copied.Status.ArtifactBytesUploaded == nil || *copied.Status.ArtifactBytesUploaded != 4096 {
		t.Fatalf("ArtifactBytesUploaded not copied: %v", copied.Status.ArtifactBytesUploaded)
	}
	*copied.Status.ArtifactObjectsUploaded = 99
	*copied.Status.ArtifactBytesUploaded = 1
	if *original.Status.ArtifactObjectsUploaded != 7 {
		t.Error("mutating copied ArtifactObjectsUploaded affected the original")
	}
	if *original.Status.ArtifactBytesUploaded != 4096 {
		t.Error("mutating copied ArtifactBytesUploaded affected the original")
	}
}

// TestDeepCopyWorkloadNilArtifactCounts covers the no-export case: nil count
// pointers must stay nil after DeepCopy (otherwise a fabricated 0 would
// travel with every read).
func TestDeepCopyWorkloadNilArtifactCounts(t *testing.T) {
	original := &Workload{Status: WorkloadStatus{ArtifactResult: "failed"}}
	copied := original.DeepCopyObject().(*Workload)
	if copied.Status.ArtifactObjectsUploaded != nil || copied.Status.ArtifactBytesUploaded != nil {
		t.Errorf("nil counts survived as %v/%v", copied.Status.ArtifactObjectsUploaded, copied.Status.ArtifactBytesUploaded)
	}
}
