package agent

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

// recordingCentral records SubmitWorkload + CancelWorkload calls so the resync
// tests can assert what the reconciler did (and with which id).
type recordingCentral struct {
	mu      sync.Mutex
	submits int
	keys    []string
	origins []protocol.SubmissionOrigin
	cancels []string
}

func (c *recordingCentral) SubmitWorkload(_ context.Context, _ string, _ []byte, key string, origin protocol.SubmissionOrigin) (string, string, string, float64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.submits++
	c.keys = append(c.keys, key)
	c.origins = append(c.origins, origin)
	return "wl_new", "burst_new", "linode", 1.0, nil
}

func (c *recordingCentral) CancelWorkload(_ context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancels = append(c.cancels, id)
	return nil
}

func crWithStatus(ns, name string, status map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec":       map[string]any{"image": "x"},
		"status":     status,
	}}
}

// crWithUID builds a CR the way the apiserver hands one over: with its
// immutable UID and current spec generation.
func crWithUID(ns, name, uid string, generation int64) *unstructured.Unstructured {
	obj := crWithStatus(ns, name, nil)
	obj.SetUID(types.UID(uid))
	obj.SetGeneration(generation)
	return obj
}

func discardReconciler(central CentralAPI) *WorkloadReconciler {
	return NewWorkloadReconciler(nil, nil, central, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// submittingReconciler is a reconciler whose submit() path can run end to end:
// a dynamic client for the status patch, and the gateway preflight satisfied so
// the default full tier isn't refused before it reaches central.
func submittingReconciler(central CentralAPI) *WorkloadReconciler {
	r := NewWorkloadReconciler(nil,
		dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
			runtime.NewScheme(),
			map[schema.GroupVersionResource]string{workloadGVR: "WorkloadList"},
		),
		central, slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.GatewayEnabled = true
	return r
}

// TestResyncCancelsWithWorkloadIDAfterRestart is the Phase-1.3 fix: a fresh
// agent (empty submitted map) that sees a CR a prior process already submitted
// must NOT re-submit, and on a later CR delete must cancel at central with the
// central workload-id — not the burst-id, which would 404 and leak the burst.
func TestResyncCancelsWithWorkloadIDAfterRestart(t *testing.T) {
	central := &recordingCentral{}
	r := discardReconciler(central)

	cr := crWithStatus("ml", "train", map[string]any{
		"phase": "Provisioning", "workloadID": "wl_prior", "burstID": "burst_prior", "backend": "linode",
	})

	// Initial ADDED (watch replay after restart): dedup, no re-submit.
	r.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: cr})
	if central.submits != 0 {
		t.Fatalf("restart dedup must not re-submit; got %d submits", central.submits)
	}
	r.mu.Lock()
	got := r.submitted["ml/train"]
	r.mu.Unlock()
	if got != "wl_prior" {
		t.Fatalf("submitted[ml/train] = %q; want central workload-id wl_prior", got)
	}

	// Customer deletes the CR after the restart.
	r.handleEvent(context.Background(), watch.Event{Type: watch.Deleted, Object: cr})
	central.mu.Lock()
	defer central.mu.Unlock()
	if len(central.cancels) != 1 || central.cancels[0] != "wl_prior" {
		t.Fatalf("expected exactly one cancel with wl_prior, got %v", central.cancels)
	}
}

// TestResyncLegacyBurstIDFallback documents that a CR written by a pre-upgrade
// agent (status.burstID only, no workloadID) still dedups on restart so no
// duplicate burst is provisioned.
func TestResyncLegacyBurstIDFallback(t *testing.T) {
	central := &recordingCentral{}
	r := discardReconciler(central)

	cr := crWithStatus("ml", "legacy", map[string]any{
		"phase": "Provisioning", "burstID": "burst_legacy", "backend": "linode",
	})
	r.handleEvent(context.Background(), watch.Event{Type: watch.Added, Object: cr})
	if central.submits != 0 {
		t.Fatalf("legacy burstID must still dedup; got %d submits", central.submits)
	}
	r.mu.Lock()
	got := r.submitted["ml/legacy"]
	r.mu.Unlock()
	if got != "burst_legacy" {
		t.Fatalf("submitted[ml/legacy] = %q; want burst_legacy fallback", got)
	}
}

// A CR the reconciler submits is reported as the Workload-CR path, so a console
// can separate a run somebody applied a Workload for from one a pod's
// nodeSelector asked for.
func TestReconcilerReportsWorkloadCROrigin(t *testing.T) {
	central := &recordingCentral{}
	cr := crWithUID("ml", "train", "0e0f6e8a-1c2d-4e3f-8a9b-0c1d2e3f4a5b", 1)
	if err := submittingReconciler(central).submit(context.Background(), cr); err != nil {
		t.Fatalf("submit: %v", err)
	}

	central.mu.Lock()
	defer central.mu.Unlock()
	if len(central.origins) != 1 || central.origins[0] != protocol.OriginWorkloadCR {
		t.Fatalf("submitted origins = %q, want one %q", central.origins, protocol.OriginWorkloadCR)
	}
}

// TestSubmitKeyIsStableAcrossAgentRestart covers the window rebuildSubmitted
// cannot: the process died AFTER central accepted the submit and BEFORE
// markStatus stamped the id, so the CR carries no status to dedup on and a
// fresh reconciler submits it again. The key must be byte-identical to the
// first attempt's so central replays one workload instead of provisioning two.
func TestSubmitKeyIsStableAcrossAgentRestart(t *testing.T) {
	cr := crWithUID("ml", "train", "0e0f6e8a-1c2d-4e3f-8a9b-0c1d2e3f4a5b", 4)

	first := &recordingCentral{}
	if err := submittingReconciler(first).submit(context.Background(), cr); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	// New process: empty submitted map, same CR, still no status stamp.
	second := &recordingCentral{}
	if err := submittingReconciler(second).submit(context.Background(), cr); err != nil {
		t.Fatalf("resubmit after restart: %v", err)
	}

	if len(first.keys) != 1 || len(second.keys) != 1 {
		t.Fatalf("expected one submit each, got %d and %d", len(first.keys), len(second.keys))
	}
	if first.keys[0] != second.keys[0] {
		t.Errorf("key changed across restart: %q then %q", first.keys[0], second.keys[0])
	}
}

// TestSubmitKeyTracksOneRunPerObject pins the reconciler's actual lifecycle:
// editing a submitted CR is not a new run, including during the crash window
// before status lands. Recreating it is a new object and therefore a new run.
func TestSubmitKeyTracksOneRunPerObject(t *testing.T) {
	const uid = "0e0f6e8a-1c2d-4e3f-8a9b-0c1d2e3f4a5b"

	keyFor := func(cr *unstructured.Unstructured) string {
		central := &recordingCentral{}
		if err := submittingReconciler(central).submit(context.Background(), cr); err != nil {
			t.Fatalf("submit: %v", err)
		}
		return central.keys[0]
	}

	baseline := keyFor(crWithUID("ml", "train", uid, 1))
	if edited := keyFor(crWithUID("ml", "train", uid, 2)); edited != baseline {
		t.Errorf("spec edit changed one CR's submission key: %q then %q", baseline, edited)
	}
	if recreated := keyFor(crWithUID("ml", "train", "9f8e7d6c-5b4a-3210-9876-543210fedcba", 1)); recreated == baseline {
		t.Errorf("recreated CR reused the old object's key %q", baseline)
	}
}

// TestSubmitKeyFallsBackWhenObjectHasNoUID: a CR without the apiserver's UID
// (hand-built objects, and any client that drops metadata) still gets a stable
// key rather than an unkeyed submission that no retry can dedup.
func TestSubmitKeyFallsBackWhenObjectHasNoUID(t *testing.T) {
	cr := crWithStatus("ml", "train", nil)

	first, second := &recordingCentral{}, &recordingCentral{}
	for _, central := range []*recordingCentral{first, second} {
		if err := submittingReconciler(central).submit(context.Background(), cr); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	if first.keys[0] == "" {
		t.Fatal("submitted with no idempotency key")
	}
	if first.keys[0] != second.keys[0] {
		t.Errorf("fallback key not stable: %q then %q", first.keys[0], second.keys[0])
	}

	other := &recordingCentral{}
	if err := submittingReconciler(other).submit(context.Background(), crWithStatus("ml", "eval", nil)); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if other.keys[0] == first.keys[0] {
		t.Errorf("different CRs share fallback key %q", first.keys[0])
	}
}
