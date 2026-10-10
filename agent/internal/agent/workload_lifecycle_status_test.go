package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	v1 "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

func TestCentralHTTPClientDecodesLifecycleFields(t *testing.T) {
	created := time.Date(2026, 8, 20, 11, 50, 0, 0, time.UTC)
	started := time.Date(2026, 8, 20, 11, 55, 0, 0, time.UTC)
	finished := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	nodeObserved := time.Date(2026, 8, 20, 11, 54, 0, 0, time.UTC)
	cleanupRequested := time.Date(2026, 8, 20, 12, 1, 0, 0, time.UTC)
	cleanupUpdated := time.Date(2026, 8, 20, 12, 4, 0, 0, time.UTC)
	deleted := time.Date(2026, 8, 20, 12, 5, 0, 0, time.UTC)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/workloads/wl_lifecycle", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"id":          "wl_lifecycle",
			"status":      "succeeded",
			"burst_id":    "burst_abc123",
			"cluster_id":  "cluster_abc",
			"created_at":  created.Format(time.RFC3339),
			"started_at":  started.Format(time.RFC3339),
			"finished_at": finished.Format(time.RFC3339),
			"cost": map[string]any{
				"usd":        1.234,
				"hourly_usd": 2.50,
			},
			"placement": map[string]any{
				"receipt": map[string]any{
					"selected": map[string]any{
						"provider":                 "linode",
						"region":                   "us-east",
						"sku":                      "g6-standard-2",
						"hourly_micro_usd":         520000,
						"maximum_charge_micro_usd": 2000000,
						"maximum_duration_seconds": 3600,
					},
				},
			},
			"outcome": map[string]any{
				"compute": map[string]any{
					"result": "Succeeded",
				},
			},
			"cleanup": map[string]any{
				"state":        "terminated",
				"requested_at": cleanupRequested.Format(time.RFC3339),
				"updated_at":   cleanupUpdated.Format(time.RFC3339),
				"deleted_at":   deleted.Format(time.RFC3339),
			},
			"node_observation": map[string]any{
				"node_name":   "ip-192-0-2-4",
				"phase":       protocol.NodePhaseReady,
				"reason":      "KubeletReady",
				"observed_at": nodeObserved.Format(time.RFC3339),
			},
			"spent_usd": 1.5,
		}
		data, _ := json.Marshal(resp)
		_, _ = w.Write(data)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_lifecycle")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}

	if snap.Status != "succeeded" {
		t.Errorf("Status = %q, want %q", snap.Status, "succeeded")
	}
	if snap.ClusterID != "cluster_abc" {
		t.Errorf("ClusterID = %q, want %q", snap.ClusterID, "cluster_abc")
	}
	if snap.CreatedAt == nil || !snap.CreatedAt.Equal(created) || snap.StartedAt == nil || !snap.StartedAt.Equal(started) {
		t.Errorf("CreatedAt/StartedAt = %v/%v, want %v/%v", snap.CreatedAt, snap.StartedAt, created, started)
	}
	if snap.FinishedAt == nil || !snap.FinishedAt.Equal(finished) {
		t.Errorf("FinishedAt = %v, want %v", snap.FinishedAt, finished)
	}
	if snap.FinalCostUSD == nil || *snap.FinalCostUSD != 1.234 {
		t.Errorf("FinalCostUSD = %v, want 1.234", snap.FinalCostUSD)
	}
	if snap.HourlyUSD == nil || *snap.HourlyUSD != 2.50 {
		t.Errorf("HourlyUSD = %v, want 2.50", snap.HourlyUSD)
	}
	if snap.SKU != "g6-standard-2" {
		t.Errorf("SKU = %q, want %q", snap.SKU, "g6-standard-2")
	}
	if snap.Provider != "linode" || snap.Region != "us-east" {
		t.Errorf("Provider/Region = %q/%q, want linode/us-east", snap.Provider, snap.Region)
	}
	if snap.PlacementHourlyMicroUSD == nil || *snap.PlacementHourlyMicroUSD != 520000 ||
		snap.MaximumChargeMicroUSD == nil || *snap.MaximumChargeMicroUSD != 2000000 ||
		snap.MaximumDurationSeconds == nil || *snap.MaximumDurationSeconds != 3600 {
		t.Errorf("placement pricing = hourly %v max %v duration %v", snap.PlacementHourlyMicroUSD, snap.MaximumChargeMicroUSD, snap.MaximumDurationSeconds)
	}
	if snap.OutcomeResult != "Succeeded" {
		t.Errorf("OutcomeResult = %q, want %q", snap.OutcomeResult, "Succeeded")
	}
	if snap.CleanupState != "terminated" {
		t.Errorf("CleanupState = %q, want %q", snap.CleanupState, "terminated")
	}
	if snap.CleanupDeletedAt == nil || !snap.CleanupDeletedAt.Equal(deleted) {
		t.Errorf("CleanupDeletedAt = %v, want %v", snap.CleanupDeletedAt, deleted)
	}
	if snap.NodeName != "ip-192-0-2-4" {
		t.Errorf("NodeName = %q, want %q", snap.NodeName, "ip-192-0-2-4")
	}
	if snap.NodePhase != protocol.NodePhaseReady || snap.NodeReason != "KubeletReady" || snap.NodeObservedAt == nil || !snap.NodeObservedAt.Equal(nodeObserved) {
		t.Errorf("node observation = %q/%q/%v", snap.NodePhase, snap.NodeReason, snap.NodeObservedAt)
	}
	if snap.CleanupRequestedAt == nil || !snap.CleanupRequestedAt.Equal(cleanupRequested) ||
		snap.CleanupUpdatedAt == nil || !snap.CleanupUpdatedAt.Equal(cleanupUpdated) {
		t.Errorf("cleanup timestamps = %v/%v", snap.CleanupRequestedAt, snap.CleanupUpdatedAt)
	}
	if snap.SpentUSD == nil || *snap.SpentUSD != 1.5 {
		t.Errorf("SpentUSD = %v, want 1.5", snap.SpentUSD)
	}
	if snap.BurstID != "burst_abc123" {
		t.Errorf("BurstID = %q, want %q", snap.BurstID, "burst_abc123")
	}
}

func TestCentralHTTPClientOmittedLifecycleFieldsStayNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"wl_minimal","status":"running"}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_minimal")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.FinishedAt != nil {
		t.Errorf("FinishedAt = %v, want nil", snap.FinishedAt)
	}
	if snap.FinalCostUSD != nil {
		t.Errorf("FinalCostUSD = %v, want nil", snap.FinalCostUSD)
	}
	if snap.CleanupState != "" {
		t.Errorf("CleanupState = %q, want empty", snap.CleanupState)
	}
	if snap.CleanupDeletedAt != nil {
		t.Errorf("CleanupDeletedAt = %v, want nil", snap.CleanupDeletedAt)
	}
	if snap.SKU != "" {
		t.Errorf("SKU = %q, want empty", snap.SKU)
	}
	if snap.OutcomeResult != "" {
		t.Errorf("OutcomeResult = %q, want empty", snap.OutcomeResult)
	}
	if snap.BurstID != "" {
		t.Errorf("BurstID = %q, want empty", snap.BurstID)
	}
}

func lifecycleStatusSyncReconciler(t *testing.T, objs ...*unstructured.Unstructured) (*WorkloadReconciler, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	list := make([]runtime.Object, len(objs))
	for i, o := range objs {
		list[i] = o
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{workloadGVR: "WorkloadList"},
		list...)
	r := &WorkloadReconciler{
		Dyn: dyn,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return r, dyn
}

func TestProjectCentralStatusLifecycleFields(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata":   map[string]any{"name": "test-wl", "namespace": "default", "generation": int64(1)},
		"status":     map[string]any{"workloadID": "wl_abc"},
	}}

	r, dyn := lifecycleStatusSyncReconciler(t, obj)

	finished := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	created := finished.Add(-10 * time.Minute)
	started := finished.Add(-5 * time.Minute)
	nodeObserved := finished.Add(-6 * time.Minute)
	cleanupRequested := finished.Add(time.Minute)
	cleanupUpdated := finished.Add(4 * time.Minute)
	finalCost := 1.234
	hourlyUSD := 2.50
	deleted := time.Date(2026, 8, 20, 12, 5, 0, 0, time.UTC)

	snap := CentralWorkloadSnapshot{
		Status:                  "succeeded",
		CreatedAt:               &created,
		StartedAt:               &started,
		FinishedAt:              &finished,
		FinalCostUSD:            &finalCost,
		HourlyUSD:               &hourlyUSD,
		Provider:                "linode",
		Region:                  "us-east",
		SKU:                     "g6-standard-2",
		PlacementHourlyMicroUSD: ptrInt64(520000),
		MaximumChargeMicroUSD:   ptrInt64(2000000),
		MaximumDurationSeconds:  ptrInt64(3600),
		ClusterID:               "cluster_abc",
		OutcomeResult:           "Succeeded",
		NodePhase:               protocol.NodePhaseReady,
		NodeReason:              "KubeletReady",
		NodeObservedAt:          &nodeObserved,
		CleanupState:            "terminated",
		CleanupRequestedAt:      &cleanupRequested,
		CleanupUpdatedAt:        &cleanupUpdated,
		CleanupDeletedAt:        &deleted,
	}

	r.projectCentralStatus(context.Background(), obj, snap)

	patches := 0
	for _, a := range dyn.Actions() {
		if a.Matches("patch", "workloads") {
			patches++
		}
	}
	if patches != 1 {
		t.Fatalf("patches = %d, want 1", patches)
	}

	// Verify the patch contents
	for _, a := range dyn.Actions() {
		if a.Matches("patch", "workloads") {
			pa := a.(interface{ GetPatch() []byte })
			var patchBody map[string]any
			if err := json.Unmarshal(pa.GetPatch(), &patchBody); err != nil {
				t.Fatalf("unmarshal patch: %v", err)
			}
			status := patchBody["status"].(map[string]any)
			if status["finishedAt"] != "2026-08-20T12:00:00Z" {
				t.Errorf("finishedAt = %v, want 2026-08-20T12:00:00Z", status["finishedAt"])
			}
			for key, want := range map[string]any{
				"phase": "Succeeded", "reason": v1.ReasonWorkloadSucceeded,
				"backend": "linode", "region": "us-east",
				"hourlyUSD": "2.500000", "maximumChargeUSD": "2.000000",
				"maximumDurationSeconds": float64(3600), "cleanupState": "terminated",
				"submittedAt": "2026-08-20T11:50:00Z", "startedAt": "2026-08-20T11:55:00Z",
				"nodeObservedAt": "2026-08-20T11:54:00Z", "providerDeletedAt": "2026-08-20T12:05:00Z",
			} {
				if status[key] != want {
					t.Errorf("status.%s = %v, want %v", key, status[key], want)
				}
			}
			if status["finalCostUSD"] != "1.234000" {
				t.Errorf("finalCostUSD = %v, want 1.234000", status["finalCostUSD"])
			}
			if status["hourlyUSD"] != "2.500000" {
				t.Errorf("hourlyUSD = %v, want 2.500000", status["hourlyUSD"])
			}
			if status["sku"] != "g6-standard-2" {
				t.Errorf("sku = %v, want g6-standard-2", status["sku"])
			}
			if status["clusterID"] != "cluster_abc" {
				t.Errorf("clusterID = %v, want cluster_abc", status["clusterID"])
			}
			if status["outcome"] != "Succeeded" {
				t.Errorf("outcome = %v, want Succeeded", status["outcome"])
			}

			// Verify CleanupComplete condition
			conditions, ok := status["conditions"]
			if !ok {
				t.Fatal("conditions absent in patch")
			}
			condSlice := conditions.([]any)
			found := map[string]metav1.ConditionStatus{}
			for _, c := range condSlice {
				cond := c.(map[string]any)
				conditionType, _ := cond["type"].(string)
				conditionStatus, _ := cond["status"].(string)
				found[conditionType] = metav1.ConditionStatus(conditionStatus)
				if conditionType == v1.ConditionCleanupComplete {
					if cond["reason"] != v1.ReasonCleanupProven {
						t.Errorf("CleanupComplete reason = %v, want %s", cond["reason"], v1.ReasonCleanupProven)
					}
				}
			}
			for _, conditionType := range []string{v1.ConditionComplete, v1.ConditionNodeReady, v1.ConditionCleanupComplete} {
				if found[conditionType] != metav1.ConditionTrue {
					t.Errorf("%s = %s, want True", conditionType, found[conditionType])
				}
			}
		}
	}

	updated, err := dyn.Resource(workloadGVR).Namespace("default").Get(context.Background(), "test-wl", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get updated workload: %v", err)
	}
	r.projectCentralStatus(context.Background(), updated, snap)
	patches = 0
	for _, a := range dyn.Actions() {
		if a.Matches("patch", "workloads") {
			patches++
		}
	}
	if patches != 1 {
		var lastPatch []byte
		for _, a := range dyn.Actions() {
			if a.Matches("patch", "workloads") {
				lastPatch = a.(interface{ GetPatch() []byte }).GetPatch()
			}
		}
		t.Fatalf("idempotent second projection produced another patch: total patches = %d, last = %s", patches, lastPatch)
	}
}

func TestCleanupCompleteConditionFailsClosed(t *testing.T) {
	tests := []struct {
		name       string
		state      string
		deletedAt  *time.Time
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "proven deletion",
			state:      "terminated",
			deletedAt:  ptrTime(time.Now()),
			wantStatus: metav1.ConditionTrue,
			wantReason: v1.ReasonCleanupProven,
		},
		{
			name:       "terminated without deleted_at fails closed",
			state:      "terminated",
			deletedAt:  nil,
			wantStatus: metav1.ConditionFalse,
			wantReason: v1.ReasonCleanupFailed,
		},
		{
			name:       "queued",
			state:      "queued",
			deletedAt:  nil,
			wantStatus: metav1.ConditionFalse,
			wantReason: v1.ReasonCleanupQueued,
		},
		{
			name:       "deleting",
			state:      "deleting",
			deletedAt:  nil,
			wantStatus: metav1.ConditionFalse,
			wantReason: v1.ReasonCleanupDeleting,
		},
		{
			name:       "retrying",
			state:      "retrying",
			deletedAt:  nil,
			wantStatus: metav1.ConditionFalse,
			wantReason: v1.ReasonCleanupRetrying,
		},
		{
			name:       "manual_attention",
			state:      "manual_attention",
			deletedAt:  nil,
			wantStatus: metav1.ConditionFalse,
			wantReason: v1.ReasonManualAttention,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := CentralWorkloadSnapshot{
				CleanupState:     tt.state,
				CleanupDeletedAt: tt.deletedAt,
			}
			cond := cleanupCompleteCondition(snap, 1)
			if cond.Type != v1.ConditionCleanupComplete {
				t.Errorf("type = %q, want %q", cond.Type, v1.ConditionCleanupComplete)
			}
			if cond.Status != tt.wantStatus {
				t.Errorf("status = %v, want %v", cond.Status, tt.wantStatus)
			}
			if cond.Reason != tt.wantReason {
				t.Errorf("reason = %q, want %q", cond.Reason, tt.wantReason)
			}
		})
	}
}

func TestCentralCompletionProjection(t *testing.T) {
	tests := []struct {
		status     string
		wantPhase  string
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{"provisioning", "Provisioning", metav1.ConditionFalse, v1.ReasonProvisioningStarted},
		{"running", "Running", metav1.ConditionFalse, v1.ReasonWorkloadRunning},
		{"succeeded", "Succeeded", metav1.ConditionTrue, v1.ReasonWorkloadSucceeded},
		{"failed", "Failed", metav1.ConditionFalse, v1.ReasonWorkloadFailed},
		{"cancelled", "Cancelled", metav1.ConditionFalse, v1.ReasonWorkloadCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			phase, reason, condition := centralCompletionProjection(CentralWorkloadSnapshot{Status: tt.status}, 3)
			if phase != tt.wantPhase || reason != tt.wantReason || condition == nil || condition.Status != tt.wantStatus {
				t.Fatalf("projection = %q/%q/%+v", phase, reason, condition)
			}
		})
	}
	if phase, reason, condition := centralCompletionProjection(CentralWorkloadSnapshot{Status: "future"}, 1); phase != "" || reason != "" || condition != nil {
		t.Fatalf("unknown status was projected: %q/%q/%+v", phase, reason, condition)
	}
}

func TestCentralCompletionProjectionWaitingForPodUsesDurableSignals(t *testing.T) {
	gpuReadyAt := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	scheduledAt := gpuReadyAt.Add(time.Minute)

	phase, reason, condition := centralCompletionProjection(CentralWorkloadSnapshot{
		Status:     "provisioning",
		GPUReadyAt: &gpuReadyAt,
	}, 4)
	if phase != "WaitingForPod" || reason != v1.ReasonWaitingForPod || condition == nil {
		t.Fatalf("waiting projection = %q/%q/%+v", phase, reason, condition)
	}
	if condition.Status != metav1.ConditionFalse || !condition.LastTransitionTime.Time.Equal(gpuReadyAt) {
		t.Fatalf("waiting condition = %+v, want False at GPU readiness", condition)
	}

	tests := []struct {
		name string
		snap CentralWorkloadSnapshot
		want string
	}{
		{
			name: "no GPU observation",
			snap: CentralWorkloadSnapshot{Status: "provisioning"},
			want: "Provisioning",
		},
		{
			name: "pod already scheduled",
			snap: CentralWorkloadSnapshot{Status: "provisioning", GPUReadyAt: &gpuReadyAt, ScheduledAt: &scheduledAt},
			want: "Provisioning",
		},
		{
			name: "central reports running",
			snap: CentralWorkloadSnapshot{Status: "running", GPUReadyAt: &gpuReadyAt},
			want: "Running",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, _ := centralCompletionProjection(tt.snap, 4)
			if got != tt.want {
				t.Fatalf("phase = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestProjectCentralStatusShowsWaitingForPodAfterGPUReady(t *testing.T) {
	gpuReadyAt := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata":   map[string]any{"name": "waiting-wl", "namespace": "ml", "generation": int64(2)},
		"status":     map[string]any{"workloadID": "wl_waiting"},
	}}
	r, dyn := lifecycleStatusSyncReconciler(t, obj)

	r.projectCentralStatus(context.Background(), obj, CentralWorkloadSnapshot{
		Status:     "provisioning",
		GPUReadyAt: &gpuReadyAt,
	})

	for _, action := range dyn.Actions() {
		if !action.Matches("patch", "workloads") {
			continue
		}
		var body map[string]any
		if err := json.Unmarshal(action.(interface{ GetPatch() []byte }).GetPatch(), &body); err != nil {
			t.Fatalf("unmarshal patch: %v", err)
		}
		status := body["status"].(map[string]any)
		if status["phase"] != "WaitingForPod" || status["reason"] != v1.ReasonWaitingForPod {
			t.Fatalf("status phase/reason = %v/%v, want WaitingForPod", status["phase"], status["reason"])
		}
		return
	}
	t.Fatal("status patch not emitted")
}

func TestNodeReadyConditionUsesOnlyKnownObservedPhases(t *testing.T) {
	tests := []struct {
		phase      string
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{protocol.NodePhaseReady, metav1.ConditionTrue, v1.ReasonNodeReady},
		{protocol.NodePhaseJoining, metav1.ConditionFalse, v1.ReasonNodeJoining},
		{protocol.NodePhaseNotReady, metav1.ConditionFalse, v1.ReasonNodeNotReady},
		{protocol.NodePhaseRemoved, metav1.ConditionFalse, v1.ReasonNodeRemoved},
	}
	for _, tt := range tests {
		t.Run(tt.phase, func(t *testing.T) {
			condition := nodeReadyCondition(CentralWorkloadSnapshot{NodePhase: tt.phase}, 2)
			if condition == nil || condition.Status != tt.wantStatus || condition.Reason != tt.wantReason {
				t.Fatalf("condition = %+v", condition)
			}
		})
	}
	if condition := nodeReadyCondition(CentralWorkloadSnapshot{}, 1); condition != nil {
		t.Fatalf("absent observation produced condition %+v", condition)
	}
}

func TestProjectCentralStatusNoCleanupConditionWhenAbsent(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata":   map[string]any{"name": "test-wl", "namespace": "default"},
		"status":     map[string]any{"workloadID": "wl_no_cleanup"},
	}}

	r, dyn := lifecycleStatusSyncReconciler(t, obj)

	snap := CentralWorkloadSnapshot{
		NodeName: "burst-node",
	}

	r.projectCentralStatus(context.Background(), obj, snap)

	for _, a := range dyn.Actions() {
		if a.Matches("patch", "workloads") {
			pa := a.(interface{ GetPatch() []byte })
			var patchBody map[string]any
			if err := json.Unmarshal(pa.GetPatch(), &patchBody); err != nil {
				t.Fatalf("unmarshal patch: %v", err)
			}
			status := patchBody["status"].(map[string]any)
			if _, hasConditions := status["conditions"]; hasConditions {
				t.Error("conditions present when no cleanup state reported — absent signals should be preserved")
			}
		}
	}
}

func TestGPUReadyConditionProjectedFromCentral(t *testing.T) {
	gpuAt := time.Date(2026, 8, 21, 10, 30, 0, 0, time.UTC)
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata":   map[string]any{"name": "gpu-wl", "namespace": "ml", "generation": int64(1)},
		"status":     map[string]any{"workloadID": "wl_gpu"},
	}}

	r, dyn := lifecycleStatusSyncReconciler(t, obj)
	snap := CentralWorkloadSnapshot{GPUReadyAt: &gpuAt}
	r.projectCentralStatus(context.Background(), obj, snap)

	for _, a := range dyn.Actions() {
		if !a.Matches("patch", "workloads") {
			continue
		}
		pa := a.(interface{ GetPatch() []byte })
		var body map[string]any
		if err := json.Unmarshal(pa.GetPatch(), &body); err != nil {
			t.Fatalf("unmarshal patch: %v", err)
		}
		status := body["status"].(map[string]any)
		if status["gpuReadyAt"] != "2026-08-21T10:30:00Z" {
			t.Errorf("gpuReadyAt = %v, want 2026-08-21T10:30:00Z", status["gpuReadyAt"])
		}
		conditions, ok := status["conditions"].([]any)
		if !ok {
			t.Fatal("conditions absent in patch")
		}
		var foundGPU bool
		for _, c := range conditions {
			cond := c.(map[string]any)
			if cond["type"] == v1.ConditionGPUReady {
				foundGPU = true
				if cond["status"] != string(metav1.ConditionTrue) {
					t.Errorf("GPUReady status = %v, want True", cond["status"])
				}
				if cond["reason"] != v1.ReasonGPUReady {
					t.Errorf("GPUReady reason = %v, want %s", cond["reason"], v1.ReasonGPUReady)
				}
			}
		}
		if !foundGPU {
			t.Fatal("GPUReady condition not projected")
		}
	}
}

func TestCPUOnlyWorkloadNoGPUReadyCondition(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata":   map[string]any{"name": "cpu-wl", "namespace": "ml", "generation": int64(1)},
		"status":     map[string]any{"workloadID": "wl_cpu"},
	}}

	r, dyn := lifecycleStatusSyncReconciler(t, obj)
	snap := CentralWorkloadSnapshot{
		NodeName:  "burst-node",
		NodePhase: protocol.NodePhaseReady,
	}
	r.projectCentralStatus(context.Background(), obj, snap)

	for _, a := range dyn.Actions() {
		if !a.Matches("patch", "workloads") {
			continue
		}
		pa := a.(interface{ GetPatch() []byte })
		var body map[string]any
		if err := json.Unmarshal(pa.GetPatch(), &body); err != nil {
			t.Fatalf("unmarshal patch: %v", err)
		}
		status := body["status"].(map[string]any)
		if _, ok := status["gpuReadyAt"]; ok {
			t.Error("gpuReadyAt present on CPU-only workload")
		}
		conditions, ok := status["conditions"].([]any)
		if ok {
			for _, c := range conditions {
				cond := c.(map[string]any)
				if cond["type"] == v1.ConditionGPUReady {
					t.Error("GPUReady condition present on CPU-only workload — absent is honest")
				}
			}
		}
	}
}

func TestScheduledConditionProjectedFromCentral(t *testing.T) {
	scheduledAt := time.Date(2026, 8, 21, 10, 35, 0, 0, time.UTC)
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata":   map[string]any{"name": "sched-wl", "namespace": "ml", "generation": int64(1)},
		"status":     map[string]any{"workloadID": "wl_sched"},
	}}

	r, dyn := lifecycleStatusSyncReconciler(t, obj)
	snap := CentralWorkloadSnapshot{
		PodName:     "my-pod-abc",
		ScheduledAt: &scheduledAt,
	}
	r.projectCentralStatus(context.Background(), obj, snap)

	for _, a := range dyn.Actions() {
		if !a.Matches("patch", "workloads") {
			continue
		}
		pa := a.(interface{ GetPatch() []byte })
		var body map[string]any
		if err := json.Unmarshal(pa.GetPatch(), &body); err != nil {
			t.Fatalf("unmarshal patch: %v", err)
		}
		status := body["status"].(map[string]any)
		if status["podName"] != "my-pod-abc" {
			t.Errorf("podName = %v, want my-pod-abc", status["podName"])
		}
		if status["scheduledAt"] != "2026-08-21T10:35:00Z" {
			t.Errorf("scheduledAt = %v, want 2026-08-21T10:35:00Z", status["scheduledAt"])
		}
		conditions, ok := status["conditions"].([]any)
		if !ok {
			t.Fatal("conditions absent in patch")
		}
		var foundScheduled bool
		for _, c := range conditions {
			cond := c.(map[string]any)
			if cond["type"] == v1.ConditionScheduled {
				foundScheduled = true
				if cond["status"] != string(metav1.ConditionTrue) {
					t.Errorf("Scheduled status = %v, want True", cond["status"])
				}
				if cond["reason"] != v1.ReasonScheduled {
					t.Errorf("Scheduled reason = %v, want %s", cond["reason"], v1.ReasonScheduled)
				}
			}
		}
		if !foundScheduled {
			t.Fatal("Scheduled condition not projected")
		}
	}
}

func TestScheduledAndGPUReadyIdempotent(t *testing.T) {
	gpuAt := time.Date(2026, 8, 21, 10, 30, 0, 0, time.UTC)
	scheduledAt := time.Date(2026, 8, 21, 10, 35, 0, 0, time.UTC)

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata":   map[string]any{"name": "both-wl", "namespace": "ml", "generation": int64(1)},
		"status":     map[string]any{"workloadID": "wl_both"},
	}}

	r, dyn := lifecycleStatusSyncReconciler(t, obj)
	snap := CentralWorkloadSnapshot{
		PodName:     "pod-1",
		ScheduledAt: &scheduledAt,
		GPUReadyAt:  &gpuAt,
	}

	r.projectCentralStatus(context.Background(), obj, snap)
	if n := patchCount(dyn); n != 1 {
		t.Fatalf("first projection: patches = %d, want 1", n)
	}

	updated, err := dyn.Resource(workloadGVR).Namespace("ml").Get(context.Background(), "both-wl", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get updated: %v", err)
	}
	r.projectCentralStatus(context.Background(), updated, snap)
	if n := patchCount(dyn); n != 1 {
		t.Fatalf("idempotent second projection: patches = %d, want 1", n)
	}
}

func TestGetWorkloadStatusDecodesPodAndGPUObservation(t *testing.T) {
	gpuAt := time.Date(2026, 8, 21, 10, 30, 0, 0, time.UTC)
	scheduledAt := time.Date(2026, 8, 21, 10, 35, 0, 0, time.UTC)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/workloads/wl_signals", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"id":     "wl_signals",
			"status": "running",
			"pod_observation": map[string]any{
				"pod_name":     "train-abc",
				"node_name":    "ys-burst-deadbeef",
				"scheduled_at": scheduledAt.Format(time.RFC3339),
			},
			"gpu_observation": map[string]any{
				"allocatable_at": gpuAt.Format(time.RFC3339),
			},
		}
		data, _ := json.Marshal(resp)
		_, _ = w.Write(data)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_signals")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.PodName != "train-abc" {
		t.Errorf("PodName = %q, want %q", snap.PodName, "train-abc")
	}
	if snap.ScheduledAt == nil || !snap.ScheduledAt.Equal(scheduledAt) {
		t.Errorf("ScheduledAt = %v, want %v", snap.ScheduledAt, scheduledAt)
	}
	if snap.GPUReadyAt == nil || !snap.GPUReadyAt.Equal(gpuAt) {
		t.Errorf("GPUReadyAt = %v, want %v", snap.GPUReadyAt, gpuAt)
	}
}

func TestGetWorkloadStatusOmittedSignalsStayNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"wl_minimal","status":"running"}`))
	}))
	defer srv.Close()

	cli := NewCentralHTTPClient(srv.URL, "t", "cluster_a")
	snap, err := cli.GetWorkloadStatus(context.Background(), "wl_minimal")
	if err != nil {
		t.Fatalf("GetWorkloadStatus: %v", err)
	}
	if snap.PodName != "" {
		t.Errorf("PodName = %q, want empty", snap.PodName)
	}
	if snap.ScheduledAt != nil {
		t.Errorf("ScheduledAt = %v, want nil", snap.ScheduledAt)
	}
	if snap.GPUReadyAt != nil {
		t.Errorf("GPUReadyAt = %v, want nil", snap.GPUReadyAt)
	}
}

func TestAdmissionConditionsUseOnlyDurableSignals(t *testing.T) {
	created := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		build         func(CentralWorkloadSnapshot, int64) *metav1.Condition
		snap          CentralWorkloadSnapshot
		conditionType string
		reason        string
	}{
		{"accepted absent", acceptedCondition, CentralWorkloadSnapshot{}, "", ""},
		{"accepted", acceptedCondition, CentralWorkloadSnapshot{CreatedAt: &created}, v1.ConditionAccepted, v1.ReasonAccepted},
		{"capacity absent", capacityReadyCondition, CentralWorkloadSnapshot{}, "", ""},
		{"capacity", capacityReadyCondition, CentralWorkloadSnapshot{BurstID: "burst_abc", CreatedAt: &created}, v1.ConditionCapacityReady, v1.ReasonPlacementAccepted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition := tt.build(tt.snap, 2)
			if tt.conditionType == "" {
				if condition != nil {
					t.Fatalf("condition = %+v, want absent", condition)
				}
				return
			}
			if condition == nil || condition.Type != tt.conditionType || condition.Status != metav1.ConditionTrue ||
				condition.Reason != tt.reason || condition.ObservedGeneration != 2 || !condition.LastTransitionTime.Time.Equal(created) {
				t.Fatalf("condition = %+v, want %s=True reason %s at %v", condition, tt.conditionType, tt.reason, created)
			}
		})
	}
}

func TestProjectCentralStatusAdmissionConditionsCoexistAndDoNotChurn(t *testing.T) {
	created := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
	observed := created.Add(time.Minute)
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1", "kind": "Workload",
		"metadata": map[string]any{"name": "conditions", "namespace": "default", "generation": int64(2)},
		"status":   map[string]any{"workloadID": "wl_conditions"},
	}}
	r, dyn := lifecycleStatusSyncReconciler(t, obj)
	snap := CentralWorkloadSnapshot{
		CreatedAt: &created, BurstID: "burst_abc", NodePhase: protocol.NodePhaseReady,
		GPUReadyAt: &observed, ScheduledAt: &observed, PodName: "pod-abc",
	}
	r.projectCentralStatus(context.Background(), obj, snap)
	if n := patchCount(dyn); n != 1 {
		t.Fatalf("first projection patches = %d, want 1", n)
	}
	updated, err := dyn.Resource(workloadGVR).Namespace("default").Get(context.Background(), "conditions", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get projected workload: %v", err)
	}
	conditions := workloadStatusConditions(updated)
	for _, conditionType := range []string{
		v1.ConditionAccepted, v1.ConditionCapacityReady, v1.ConditionNodeReady,
		v1.ConditionGPUReady, v1.ConditionScheduled,
	} {
		if !v1.IsConditionTrue(conditions, conditionType) {
			t.Errorf("%s condition not True: %+v", conditionType, v1.FindCondition(conditions, conditionType))
		}
	}
	r.projectCentralStatus(context.Background(), updated, snap)
	if n := patchCount(dyn); n != 1 {
		t.Fatalf("repeat projection patches = %d, want 1", n)
	}
}

func TestProjectCentralStatusLeavesLegacyAdmissionConditionsAbsent(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1", "kind": "Workload",
		"metadata": map[string]any{"name": "legacy", "namespace": "default"},
		"status":   map[string]any{"workloadID": "wl_legacy"},
	}}
	r, dyn := lifecycleStatusSyncReconciler(t, obj)
	r.projectCentralStatus(context.Background(), obj, CentralWorkloadSnapshot{NodeName: "burst-node"})
	updated, err := dyn.Resource(workloadGVR).Namespace("default").Get(context.Background(), "legacy", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get projected workload: %v", err)
	}
	conditions := workloadStatusConditions(updated)
	if v1.FindCondition(conditions, v1.ConditionAccepted) != nil || v1.FindCondition(conditions, v1.ConditionCapacityReady) != nil {
		t.Fatalf("legacy admission conditions = %+v, want absent", conditions)
	}
}

func patchCount(dyn *dynamicfake.FakeDynamicClient) int {
	n := 0
	for _, a := range dyn.Actions() {
		if a.Matches("patch", "workloads") {
			n++
		}
	}
	return n
}

func ptrTime(t time.Time) *time.Time { return &t }
func ptrInt64(v int64) *int64        { return &v }
