package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/yscale-sh/yscale/internal/evidence"
)

// These tests exist to pin the fail-closed seams from #93/#24. Every one
// of them fails against the pre-hardening runner, which is the point: each
// former false pass gets a named regression test rather than a comment.

const (
	testBurstID         = "burst_abc123def456"
	testGPUProductLabel = "NVIDIA-RTX-4000-Ada-Generation"
	testCaseYAML        = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: probe
spec:
  image: busybox:latest
`
)

// testBurstNodeName is the node name the decider derives from testBurstID.
var testBurstNodeName = "ys-burst-" + burstNodeSuffix(testBurstID)

func burstNode(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{"yscale.sh/burst-node": "true"},
	}}
}

// nodeStep is one scripted answer to a burst-Node listing.
type nodeStep struct {
	names []string
	err   error
	delay time.Duration
}

// fakeK8sWithNodeScript serves steps one per Nodes().List call, repeating
// the last step once the script runs out. That makes "node appears, then
// goes away" and "the apiserver breaks mid-poll" both expressible without
// timing games.
func fakeK8sWithNodeScript(steps ...nodeStep) *k8sfake.Clientset {
	k8s := k8sfake.NewSimpleClientset()
	var mu sync.Mutex
	calls := 0
	k8s.PrependReactor("list", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		i := calls
		calls++
		mu.Unlock()
		if len(steps) == 0 {
			return true, &corev1.NodeList{}, nil
		}
		if i >= len(steps) {
			i = len(steps) - 1
		}
		if steps[i].delay > 0 {
			time.Sleep(steps[i].delay)
		}
		if steps[i].err != nil {
			return true, nil, steps[i].err
		}
		list := &corev1.NodeList{}
		for _, n := range steps[i].names {
			list.Items = append(list.Items, *burstNode(n))
		}
		return true, list, nil
	})
	return k8s
}

func newFakeDynamic() *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{workloadGVR: "WorkloadList"},
	)
}

func workloadObj(name, phase, backend, burstID string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "yscale.sh/v1",
		"kind":       "Workload",
		"metadata": map[string]any{
			"name":      name,
			"namespace": "default",
		},
		"status": map[string]any{
			"phase":   phase,
			"backend": backend,
			"burstID": burstID,
		},
	}}
}

// dynServingWorkload answers every Get with the same terminal status, and
// echoes Creates back, so a Run can be driven without a real apiserver.
func dynServingWorkload(phase, backend, burstID string) *dynamicfake.FakeDynamicClient {
	dyn := newFakeDynamic()
	dyn.PrependReactor("create", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, a.(ktesting.CreateAction).GetObject(), nil
	})
	dyn.PrependReactor("get", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, workloadObj(a.(ktesting.GetAction).GetName(), phase, backend, burstID), nil
	})
	dyn.PrependReactor("delete", "workloads", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	return dyn
}

func testRunner(k8s *k8sfake.Clientset, dyn *dynamicfake.FakeDynamicClient, prov ProviderInventory, budget time.Duration) *Runner {
	return &Runner{
		K8s:          k8s,
		Dyn:          dyn,
		Namespace:    "default",
		RunID:        "t1",
		Provider:     prov,
		TerminalPoll: time.Millisecond,
		ReapPoll:     time.Millisecond,
		ProviderPoll: time.Millisecond,
		ReapBudget:   budget,
	}
}

// fakeProvider serves scripted audit results, one per call, repeating the
// last once the script runs out.
type fakeProvider struct {
	name          string
	steps         []OwnedInstances
	err           error
	delay         time.Duration
	waitForCancel bool
	beforeCount   func()
	calls         int
	residue       *ProviderResidue
	residueErr    error
}

func (f *fakeProvider) Name() string { return f.name }

func (f *fakeProvider) CountBurstInstances(ctx context.Context, _ string) (OwnedInstances, error) {
	if f.beforeCount != nil {
		f.beforeCount()
	}
	f.calls++
	if f.waitForCancel {
		<-ctx.Done()
		return OwnedInstances{}, ctx.Err()
	}
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if f.err != nil {
		return OwnedInstances{}, f.err
	}
	if len(f.steps) == 0 {
		return OwnedInstances{}, nil
	}
	i := f.calls - 1
	if i >= len(f.steps) {
		i = len(f.steps) - 1
	}
	return f.steps[i], nil
}

func (f *fakeProvider) AuditBurstResidue(_ context.Context, _ string, _ int) (ProviderResidue, error) {
	if f.residueErr != nil {
		return ProviderResidue{}, f.residueErr
	}
	if f.residue != nil {
		return *f.residue, nil
	}
	return ProviderResidue{}, nil
}

// ── 1. A disappeared CR is never a synthesized pass ──────────────────────

func TestWaitForTerminalNeverSynthesizesSuccessFromDisappearedCR(t *testing.T) {
	// Provisioning and Running are precisely the two phases the old code
	// converted into "Succeeded" on a NotFound.
	for _, lastPhase := range []string{"Provisioning", "Running"} {
		t.Run(lastPhase, func(t *testing.T) {
			dyn := newFakeDynamic()
			calls := 0
			dyn.PrependReactor("get", "workloads", func(ktesting.Action) (bool, runtime.Object, error) {
				calls++
				if calls == 1 {
					return true, workloadObj("probe", lastPhase, backendLinode, testBurstID), nil
				}
				return true, nil, apierrors.NewNotFound(workloadGVR.GroupResource(), "probe")
			})
			r := testRunner(k8sfake.NewSimpleClientset(), dyn, nil, time.Second)

			phase, _, err := r.waitForTerminal(context.Background(), "probe")
			if err == nil {
				t.Fatalf("CR disappeared after %s but waitForTerminal returned phase %q with no error", lastPhase, phase)
			}
			if phase == "Succeeded" {
				t.Fatalf("CR disappeared after %s and was synthesized into Succeeded", lastPhase)
			}
			if !errors.Is(err, errCRDisappeared) {
				t.Fatalf("want errCRDisappeared, got %v", err)
			}
		})
	}
}

func TestRunReportsErrorNotSuccessWhenCRDisappears(t *testing.T) {
	dyn := newFakeDynamic()
	dyn.PrependReactor("create", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, a.(ktesting.CreateAction).GetObject(), nil
	})
	calls := 0
	dyn.PrependReactor("get", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return true, workloadObj(a.(ktesting.GetAction).GetName(), "Running", backendLinode, testBurstID), nil
		}
		return true, nil, apierrors.NewNotFound(workloadGVR.GroupResource(), "probe-t1")
	})
	dyn.PrependReactor("delete", "workloads", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})

	r := testRunner(fakeK8sWithNodeScript(nodeStep{}), dyn, nil, 50*time.Millisecond)
	res := r.Run(context.Background(), Case{Name: "linode-gpu", RawYAML: []byte(testCaseYAML)})

	if res.Phase == "Succeeded" {
		t.Fatalf("Run reported Succeeded for a CR that vanished before a terminal phase")
	}
	if res.Phase != "Error" {
		t.Fatalf("want Phase=Error for a vanished CR, got %q", res.Phase)
	}
	if res.Reaped {
		t.Fatal("Reaped=true for a case that never reached a terminal phase")
	}
}

// ── 2. Node inventory failures fail the case ─────────────────────────────

func TestRunRefusesToCreateCRWhenBaselineInventoryFails(t *testing.T) {
	k8s := fakeK8sWithNodeScript(nodeStep{err: errors.New("apiserver unreachable")})
	dyn := newFakeDynamic()
	created := false
	dyn.PrependReactor("create", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		created = true
		return true, a.(ktesting.CreateAction).GetObject(), nil
	})

	r := testRunner(k8s, dyn, nil, time.Second)
	res := r.Run(context.Background(), Case{Name: "linode-gpu", RawYAML: []byte(testCaseYAML)})

	if created {
		t.Fatal("Run created a Workload CR without a usable burst-node baseline — a burst it cannot audit")
	}
	if res.Phase != "Error" {
		t.Fatalf("want Phase=Error, got %q", res.Phase)
	}
	if res.Reaped {
		t.Fatal("Reaped=true after the baseline inventory failed")
	}
	if !strings.Contains(res.Err, "apiserver unreachable") {
		t.Fatalf("error should name the inventory failure, got %q", res.Err)
	}
}

// ── 3. Reap requires observation, then absence ───────────────────────────

func TestWaitForReapFailClosed(t *testing.T) {
	other := "ys-burst-000000000000"
	listErr := errors.New("etcd leader election in progress")

	tests := []struct {
		name     string
		burstID  string
		baseline map[string]struct{}
		steps    []nodeStep
		wantErr  error
	}{
		{
			// The happy path: we saw the node, then it went away.
			name:    "observed then absent",
			burstID: testBurstID,
			steps: []nodeStep{
				{names: []string{testBurstNodeName}},
				{names: []string{testBurstNodeName}},
				{},
			},
		},
		{
			// The headline false pass: the old loop returned true on the
			// very first empty listing, which is the normal state before
			// the burst kubelet registers — and the permanent state when
			// the burst never launched at all.
			name:    "never observed is not a reap",
			burstID: testBurstID,
			steps:   []nodeStep{{}},
			wantErr: errReapUnobserved,
		},
		{
			name:    "still present at deadline",
			burstID: testBurstID,
			steps:   []nodeStep{{names: []string{testBurstNodeName}}},
			wantErr: errReapTimeout,
		},
		{
			// The old loop `continue`d on a list error, so an unreachable
			// apiserver was indistinguishable from a clean cluster.
			name:    "inventory error before observation",
			burstID: testBurstID,
			steps:   []nodeStep{{err: listErr}},
			wantErr: errReapInventory,
		},
		{
			name:    "inventory error after observation",
			burstID: testBurstID,
			steps: []nodeStep{
				{names: []string{testBurstNodeName}},
				{err: listErr},
			},
			wantErr: errReapInventory,
		},
		{
			// A different burst's node must not stand in for ours.
			name:    "only a foreign burst node is present",
			burstID: testBurstID,
			steps:   []nodeStep{{names: []string{other}}},
			wantErr: errReapUnobserved,
		},
		{
			name:     "baseline diff: new node observed then absent",
			burstID:  "",
			baseline: map[string]struct{}{other: {}},
			steps: []nodeStep{
				{names: []string{other, testBurstNodeName}},
				{names: []string{other}},
			},
		},
		{
			name:     "baseline diff: no new node ever appeared",
			burstID:  "",
			baseline: map[string]struct{}{other: {}},
			steps:    []nodeStep{{names: []string{other}}},
			wantErr:  errReapUnobserved,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &Runner{K8s: fakeK8sWithNodeScript(tc.steps...), ReapPoll: time.Millisecond}
			err := r.waitForReap(context.Background(), tc.baseline, tc.burstID, time.Now().Add(150*time.Millisecond))
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("want clean reap, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want %v, got a clean reap", tc.wantErr)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestWaitForReapDistinguishesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &Runner{K8s: fakeK8sWithNodeScript(nodeStep{names: []string{testBurstNodeName}}), ReapPoll: time.Millisecond}
	err := r.waitForReap(ctx, nil, testBurstID, time.Now().Add(time.Second))

	if !errors.Is(err, errReapCancelled) {
		t.Fatalf("want errReapCancelled, got %v", err)
	}
	// Cancellation must not be reported as an inventory or timeout failure:
	// an operator reading the report has to know nobody finished looking.
	if errors.Is(err, errReapInventory) || errors.Is(err, errReapTimeout) {
		t.Fatalf("cancellation misreported as another failure mode: %v", err)
	}
}

func TestWaitForReapRejectsAbsenceObservedAfterTheSharedDeadline(t *testing.T) {
	r := &Runner{
		K8s: fakeK8sWithNodeScript(
			nodeStep{names: []string{testBurstNodeName}},
			nodeStep{delay: 30 * time.Millisecond},
		),
		ReapPoll: time.Millisecond,
	}
	err := r.waitForReap(context.Background(), nil, testBurstID, time.Now().Add(10*time.Millisecond))
	if !errors.Is(err, errReapTimeout) {
		t.Fatalf("late node absence = %v, want errReapTimeout rather than a reap pass", err)
	}
}

// ── 4. Provider audit is required, exact, and fail-closed ────────────────

func TestWaitForProviderAbsence(t *testing.T) {
	tests := []struct {
		name     string
		backend  string
		burstID  string
		provider ProviderInventory
		wantErr  error
		wantIn   []string
	}{
		{
			name:    "missing backend identity cannot skip audit",
			backend: "",
			burstID: testBurstID,
			wantErr: errProviderInconclusive,
		},
		{
			// Kubernetes evidence alone must not clear a Linode case.
			name:    "linode with no inventory configured",
			backend: backendLinode,
			burstID: testBurstID,
			wantErr: errProviderUnconfigured,
			wantIn:  []string{"LINODE_TOKEN"},
		},
		{
			name:     "inventory audits a different backend",
			backend:  backendLinode,
			burstID:  testBurstID,
			provider: &fakeProvider{name: "flyio"},
			wantErr:  errProviderUnconfigured,
		},
		{
			name:     "linode case with no burstID cannot be matched",
			backend:  backendLinode,
			burstID:  "",
			provider: &fakeProvider{name: backendLinode},
			wantErr:  errProviderInconclusive,
		},
		{
			name:     "account listing failed",
			backend:  backendLinode,
			burstID:  testBurstID,
			provider: &fakeProvider{name: backendLinode, err: errors.New("status 429 (body redacted)")},
			wantErr:  errProviderInconclusive,
		},
		{
			name:     "instance still tagged at the deadline",
			backend:  backendLinode,
			burstID:  testBurstID,
			provider: &fakeProvider{name: backendLinode, steps: []OwnedInstances{{Count: 1, IDs: []string{"9001"}}}},
			wantErr:  errProviderResidue,
			wantIn:   []string{"9001", yscaleBurstTag, testBurstID},
		},
		{
			name:     "instance disappears while polling",
			backend:  backendLinode,
			burstID:  testBurstID,
			provider: &fakeProvider{name: backendLinode, steps: []OwnedInstances{{Count: 1, IDs: []string{"9001"}}, {}}},
		},
		{
			name:     "account is already clean",
			backend:  backendLinode,
			burstID:  testBurstID,
			provider: &fakeProvider{name: backendLinode},
		},
		{
			// Non-Linode cases keep their existing provider-independent
			// behavior in this slice.
			name:    "non-linode backend needs no audit",
			backend: "flyio",
			burstID: "burst_ffffffffffff",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &Runner{Provider: tc.provider, ProviderPoll: time.Millisecond}
			err := r.waitForProviderAbsence(context.Background(), tc.backend, tc.burstID, time.Now().Add(100*time.Millisecond))
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("want clean audit, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want %v, got a clean audit", tc.wantErr)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
			for _, want := range tc.wantIn {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error should name %q, got %q", want, err.Error())
				}
			}
		})
	}
}

func TestProviderAuditUsesTheRemainingSharedDeadline(t *testing.T) {
	provider := &fakeProvider{name: backendLinode, waitForCancel: true}
	r := &Runner{Provider: provider, ProviderPoll: time.Millisecond}
	start := time.Now()
	err := r.waitForProviderAbsence(context.Background(), backendLinode, testBurstID, time.Now().Add(20*time.Millisecond))
	if !errors.Is(err, errProviderInconclusive) {
		t.Fatalf("provider deadline = %v, want an inconclusive audit", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("provider request ignored the shared reap deadline: elapsed=%s", elapsed)
	}
}

func TestProviderAuditRejectsAZeroResultReturnedAfterTheDeadline(t *testing.T) {
	provider := &fakeProvider{name: backendLinode, delay: 30 * time.Millisecond}
	r := &Runner{Provider: provider, ProviderPoll: time.Millisecond}
	err := r.waitForProviderAbsence(context.Background(), backendLinode, testBurstID, time.Now().Add(10*time.Millisecond))
	if !errors.Is(err, errProviderInconclusive) {
		t.Fatalf("late zero inventory = %v, want an inconclusive audit rather than a reap pass", err)
	}
}

// ── 5. Run wires the two halves together ─────────────────────────────────

func TestRunDoesNotReapWhileProviderResidueRemains(t *testing.T) {
	// Baseline empty, then the burst node appears, then it is gone — the
	// Kubernetes half passes cleanly. The account does not.
	k8s := fakeK8sWithNodeScript(
		nodeStep{},
		nodeStep{names: []string{testBurstNodeName}},
		nodeStep{},
	)
	prov := &fakeProvider{name: backendLinode, steps: []OwnedInstances{{Count: 2, IDs: []string{"9001", "9002"}}}}

	r := testRunner(k8s, dynServingWorkload("Succeeded", backendLinode, testBurstID), prov, 60*time.Millisecond)
	res := r.Run(context.Background(), Case{Name: "linode-gpu-rtx6000", RawYAML: []byte(testCaseYAML)})

	if res.Phase != "Succeeded" {
		t.Fatalf("want the workload phase preserved as Succeeded, got %q (%s)", res.Phase, res.Err)
	}
	if res.Reaped {
		t.Fatal("Reaped=true while two Linode instances still carry the burst's ownership tags")
	}
	if !strings.Contains(res.ReapErr, "9001") || !strings.Contains(res.ReapErr, "9002") {
		t.Fatalf("reap error should name the leaked instance IDs, got %q", res.ReapErr)
	}
	if got := res.errorText(); !strings.Contains(got, "reap:") {
		t.Fatalf("report ERROR cell should surface the reap failure, got %q", got)
	}
	if prov.calls == 0 {
		t.Fatal("provider audit never ran")
	}
}

func TestRunDoesNotReapWhenLinodeInventoryIsUnconfigured(t *testing.T) {
	k8s := fakeK8sWithNodeScript(
		nodeStep{},
		nodeStep{names: []string{testBurstNodeName}},
		nodeStep{},
	)

	r := testRunner(k8s, dynServingWorkload("Succeeded", backendLinode, testBurstID), nil, 60*time.Millisecond)
	res := r.Run(context.Background(), Case{Name: "linode-gpu-rtx6000", RawYAML: []byte(testCaseYAML)})

	if res.Reaped {
		t.Fatal("a Linode case passed reap on Kubernetes evidence alone, with no account audit configured")
	}
	if !strings.Contains(res.ReapErr, "LINODE_TOKEN") {
		t.Fatalf("failure should tell the operator how to configure the audit, got %q", res.ReapErr)
	}
}

func TestRunKeepsNonLinodeCasesProviderIndependent(t *testing.T) {
	k8s := fakeK8sWithNodeScript(
		nodeStep{},
		nodeStep{names: []string{testBurstNodeName}},
		nodeStep{},
	)

	r := testRunner(k8s, dynServingWorkload("Succeeded", "flyio", testBurstID), nil, time.Second)
	res := r.Run(context.Background(), Case{Name: "flyio-cpu-nano", RawYAML: []byte(testCaseYAML)})

	if !res.Reaped {
		t.Fatalf("a flyio case with a clean Kubernetes reap should still pass, got ReapErr=%q", res.ReapErr)
	}
	if res.ReapTime <= 0 {
		t.Fatal("ReapTime should be recorded on a successful reap")
	}
}

func TestRunDeletesWorkloadBeforePollingDurableEvidence(t *testing.T) {
	k8s := fakeK8sWithNodeScript(
		nodeStep{},
		nodeStep{names: []string{testBurstNodeName}},
		nodeStep{},
	)
	dyn := newFakeDynamic()
	dyn.PrependReactor("create", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, a.(ktesting.CreateAction).GetObject(), nil
	})
	dyn.PrependReactor("get", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		obj := workloadObj(a.(ktesting.GetAction).GetName(), "Succeeded", "flyio", testBurstID)
		if err := unstructured.SetNestedField(obj.Object, "wl_evidence", "status", "workloadID"); err != nil {
			t.Fatalf("set workloadID: %v", err)
		}
		return true, obj, nil
	})
	var mu sync.Mutex
	deleted := false
	evidenceBeforeDelete := false
	dyn.PrependReactor("delete", "workloads", func(ktesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		deleted = true
		mu.Unlock()
		return true, nil, nil
	})

	now := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		if !deleted {
			evidenceBeforeDelete = true
		}
		mu.Unlock()
		fmt.Fprintf(w, `{"status":"succeeded","cleanup":{"state":"terminated","provider_created_at":%q,"deleted_at":%q,"durable_reap_receipt":true},"outcome":{"compute":{"result":"succeeded"}}}`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	}))
	defer srv.Close()

	r := testRunner(k8s, dyn, nil, time.Second)
	r.CentralEvidence = &CentralEvidenceClient{BaseURL: srv.URL, Token: "scoped-test-token", Client: srv.Client()}
	r.EvidencePoll = time.Millisecond
	res := r.Run(context.Background(), Case{Name: "flyio-cpu-evidence", RawYAML: []byte(testCaseYAML)})
	if !res.Reaped {
		t.Fatalf("reap failed: %s", res.ReapErr)
	}
	mu.Lock()
	defer mu.Unlock()
	if evidenceBeforeDelete {
		t.Fatal("Central evidence was polled before the Workload CR was deleted")
	}
}

// ── 6. GPU verification ────────────────────────────────────────────────

func TestSpecGPUCount(t *testing.T) {
	tests := []struct {
		name string
		obj  *unstructured.Unstructured
		want int64
	}{
		{
			name: "no GPU spec",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{"image": "busybox"},
			}},
			want: 0,
		},
		{
			name: "GPU with explicit count",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{"gpu": map[string]any{"kind": "rtx4090", "count": int64(2)}},
			}},
			want: 2,
		},
		{
			name: "GPU with no count defaults to 1",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{"gpu": map[string]any{"kind": "rtx4090"}},
			}},
			want: 1,
		},
		{
			name: "GPU with count 0 defaults to 1",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"spec": map[string]any{"gpu": map[string]any{"kind": "any", "count": int64(0)}},
			}},
			want: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := specGPUCount(tc.obj); got != tc.want {
				t.Fatalf("specGPUCount = %d, want %d", got, tc.want)
			}
		})
	}
}

func nvidiaLogReader(_ context.Context, _ kubernetes.Interface, _, _, _ string, _ int64) ([]byte, error) {
	return []byte("NVIDIA-SMI 550.127.05   Driver Version: 550.127.05\n"), nil
}

func TestVerifyGPUFailsClosed(t *testing.T) {
	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testBurstNodeName,
			Labels: map[string]string{
				"nvidia.com/gpu.present": "true",
				"nvidia.com/gpu.product": testGPUProductLabel,
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
		},
	}

	goodPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-t1-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe-t1"},
		},
		Spec: corev1.PodSpec{
			NodeName: testBurstNodeName,
			Containers: []corev1.Container{{
				Name: "smoke",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
					},
				},
			}},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "smoke",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
				},
			}},
		},
	}

	tests := []struct {
		name    string
		node    *corev1.Node
		pod     *corev1.Pod
		burstID string
		wantErr string
	}{
		{
			name:    "empty burstID",
			node:    gpuNode,
			pod:     goodPod,
			burstID: "",
			wantErr: "burstID is empty",
		},
		{
			name:    "malformed burstID",
			node:    gpuNode,
			pod:     goodPod,
			burstID: "not-a-burst-id",
			wantErr: "does not derive a valid node name",
		},
		{
			name:    "no pods found",
			node:    gpuNode,
			pod:     nil,
			burstID: testBurstID,
			wantErr: "no pods found",
		},
		{
			name: "pod on wrong node",
			node: gpuNode,
			pod: func() *corev1.Pod {
				p := goodPod.DeepCopy()
				p.Spec.NodeName = "worker-99"
				return p
			}(),
			burstID: testBurstID,
			wantErr: "want exact burst node",
		},
		{
			name: "no GPU limit on container",
			node: gpuNode,
			pod: func() *corev1.Pod {
				p := goodPod.DeepCopy()
				p.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
				return p
			}(),
			burstID: testBurstID,
			wantErr: "nvidia.com/gpu request and limit",
		},
		{
			name: "explicit GPU request too low",
			node: gpuNode,
			pod: func() *corev1.Pod {
				p := goodPod.DeepCopy()
				p.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("0"),
				}
				return p
			}(),
			burstID: testBurstID,
			wantErr: "nvidia.com/gpu request and limit",
		},
		{
			name: "node GPU allocatable too low",
			node: func() *corev1.Node {
				n := gpuNode.DeepCopy()
				n.Status.Allocatable = corev1.ResourceList{
					corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("0"),
				}
				return n
			}(),
			pod:     goodPod,
			burstID: testBurstID,
			wantErr: "allocatable nvidia.com/gpu=0",
		},
		{
			name: "nvidia.com/gpu.present label missing",
			node: func() *corev1.Node {
				n := gpuNode.DeepCopy()
				delete(n.Labels, "nvidia.com/gpu.present")
				return n
			}(),
			pod:     goodPod,
			burstID: testBurstID,
			wantErr: "nvidia.com/gpu.present=true",
		},
		{
			name: "nvidia.com/gpu.present label false",
			node: func() *corev1.Node {
				n := gpuNode.DeepCopy()
				n.Labels["nvidia.com/gpu.present"] = "false"
				return n
			}(),
			pod:     goodPod,
			burstID: testBurstID,
			wantErr: "nvidia.com/gpu.present=true",
		},
		{
			name: "nvidia.com/gpu.product label missing",
			node: func() *corev1.Node {
				n := gpuNode.DeepCopy()
				delete(n.Labels, "nvidia.com/gpu.product")
				return n
			}(),
			pod:     goodPod,
			burstID: testBurstID,
			wantErr: "nvidia.com/gpu.product",
		},
		{
			name: "gpu-not-ready taint still present",
			node: func() *corev1.Node {
				n := gpuNode.DeepCopy()
				n.Spec.Taints = []corev1.Taint{{
					Key:    "nvidia.com/gpu-not-ready",
					Effect: corev1.TaintEffectNoSchedule,
				}}
				return n
			}(),
			pod:     goodPod,
			burstID: testBurstID,
			wantErr: "gpu-not-ready taint",
		},
		{
			name: "GPU container exit code nonzero",
			node: gpuNode,
			pod: func() *corev1.Pod {
				p := goodPod.DeepCopy()
				p.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
				return p
			}(),
			burstID: testBurstID,
			wantErr: "did not terminate with exit code 0",
		},
		{
			name: "sidecar exit 0 does not validate failed GPU container",
			node: gpuNode,
			pod: func() *corev1.Pod {
				p := goodPod.DeepCopy()
				p.Status.ContainerStatuses[0].State.Terminated.ExitCode = 137
				p.Spec.Containers = append(p.Spec.Containers, corev1.Container{
					Name: "sidecar",
				})
				p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, corev1.ContainerStatus{
					Name: "sidecar",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
					},
				})
				return p
			}(),
			burstID: testBurstID,
			wantErr: "did not terminate with exit code 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var objs []runtime.Object
			if tc.node != nil {
				objs = append(objs, tc.node)
			}
			if tc.pod != nil {
				objs = append(objs, tc.pod)
			}
			k8s := k8sfake.NewSimpleClientset(objs...)
			r := &Runner{K8s: k8s, Namespace: "default", ReadPodLog: nvidiaLogReader}
			err := r.verifyGPU(context.Background(), "probe-t1", tc.burstID, 1)
			if err == nil {
				t.Fatal("verifyGPU passed, want failure")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestGPUVerificationFailureStillRunsReap(t *testing.T) {
	// Workload Succeeds, but no pod exists for GPU verification to find.
	// Reap must still run so paid resources are not stranded.
	gpuCaseYAML := `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: gpu-probe
spec:
  image: nvidia/cuda:latest
  gpu:
    kind: rtx4090
    count: 1
`
	k8s := fakeK8sWithNodeScript(
		nodeStep{},
		nodeStep{names: []string{testBurstNodeName}},
		nodeStep{},
	)
	prov := &fakeProvider{name: backendLinode}
	r := testRunner(k8s, dynServingWorkload("Succeeded", backendLinode, testBurstID), prov, time.Second)
	prober := &fakeAccessProber{}
	r.AccessProber = prober // configured, but evidence mode remains disabled
	res := r.Run(context.Background(), Case{Name: "linode-gpu-rtx4090", RawYAML: []byte(gpuCaseYAML)})

	if res.Err == "" {
		t.Fatal("GPU verification should have failed (no pods), but Err is empty")
	}
	if !strings.Contains(res.Err, "GPU verification") {
		t.Fatalf("Err should mention GPU verification, got %q", res.Err)
	}
	if !res.Reaped {
		t.Fatalf("reap must still run after GPU verification failure; ReapErr=%q", res.ReapErr)
	}
	if len(prober.calls) != 0 {
		t.Fatalf("evidence-disabled GPU case called access probe %d times", len(prober.calls))
	}
}

func TestCPUCaseSkipsGPUVerification(t *testing.T) {
	k8s := fakeK8sWithNodeScript(
		nodeStep{},
		nodeStep{names: []string{testBurstNodeName}},
		nodeStep{},
	)
	r := testRunner(k8s, dynServingWorkload("Succeeded", "flyio", testBurstID), nil, time.Second)
	prober := &fakeAccessProber{}
	r.EvidenceEnabled = true
	r.AccessProber = prober
	res := r.Run(context.Background(), Case{Name: "flyio-cpu-nano", RawYAML: []byte(testCaseYAML)})

	if res.Err != "" {
		t.Fatalf("CPU case should not run GPU verification, got Err=%q", res.Err)
	}
	if len(prober.calls) != 0 {
		t.Fatalf("evidence-enabled CPU case called access probe %d times", len(prober.calls))
	}
}

// ── 7. Pod log injection tests ─────────────────────────────────────────

func TestVerifyGPUSucceedsWithNVIDIASMILog(t *testing.T) {
	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testBurstNodeName,
			Labels: map[string]string{
				"nvidia.com/gpu.present": "true",
				"nvidia.com/gpu.product": testGPUProductLabel,
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-t1-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe-t1"},
		},
		Spec: corev1.PodSpec{
			NodeName: testBurstNodeName,
			Containers: []corev1.Container{{
				Name: "smoke",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
					},
				},
			}},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "smoke",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
				},
			}},
		},
	}
	k8s := k8sfake.NewSimpleClientset(gpuNode, pod)
	r := &Runner{
		K8s:       k8s,
		Namespace: "default",
		ReadPodLog: func(_ context.Context, _ kubernetes.Interface, _, _, _ string, _ int64) ([]byte, error) {
			return []byte("NVIDIA-SMI 550.127.05   Driver Version: 550.127.05\nGPU  Name\n"), nil
		},
	}
	if err := r.verifyGPU(context.Background(), "probe-t1", testBurstID, 1); err != nil {
		t.Fatalf("verifyGPU should succeed with NVIDIA-SMI in logs, got %v", err)
	}
}

func TestVerifyGPUFailsWhenLogUnavailable(t *testing.T) {
	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testBurstNodeName,
			Labels: map[string]string{
				"nvidia.com/gpu.present": "true",
				"nvidia.com/gpu.product": testGPUProductLabel,
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-t1-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe-t1"},
		},
		Spec: corev1.PodSpec{
			NodeName: testBurstNodeName,
			Containers: []corev1.Container{{
				Name: "smoke",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
					},
				},
			}},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "smoke",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
				},
			}},
		},
	}
	k8s := k8sfake.NewSimpleClientset(gpuNode, pod)
	r := &Runner{
		K8s:       k8s,
		Namespace: "default",
		ReadPodLog: func(_ context.Context, _ kubernetes.Interface, _, _, _ string, _ int64) ([]byte, error) {
			return nil, fmt.Errorf("container %q not found", "smoke")
		},
	}
	err := r.verifyGPU(context.Background(), "probe-t1", testBurstID, 1)
	if err == nil {
		t.Fatal("verifyGPU should fail when pod logs are unavailable")
	}
	if !strings.Contains(err.Error(), "read pod logs") {
		t.Fatalf("error should mention log reading, got %q", err.Error())
	}
}

func TestVerifyGPUFailsWhenLogMissesNVIDIASMI(t *testing.T) {
	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testBurstNodeName,
			Labels: map[string]string{
				"nvidia.com/gpu.present": "true",
				"nvidia.com/gpu.product": testGPUProductLabel,
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-t1-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe-t1"},
		},
		Spec: corev1.PodSpec{
			NodeName: testBurstNodeName,
			Containers: []corev1.Container{{
				Name: "smoke",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
					},
				},
			}},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "smoke",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
				},
			}},
		},
	}
	k8s := k8sfake.NewSimpleClientset(gpuNode, pod)
	r := &Runner{
		K8s:       k8s,
		Namespace: "default",
		ReadPodLog: func(_ context.Context, _ kubernetes.Interface, _, _, _ string, _ int64) ([]byte, error) {
			return []byte("no GPU output here\n"), nil
		},
	}
	err := r.verifyGPU(context.Background(), "probe-t1", testBurstID, 1)
	if err == nil {
		t.Fatal("verifyGPU should fail when NVIDIA-SMI is missing from logs")
	}
	if !strings.Contains(err.Error(), "NVIDIA-SMI") {
		t.Fatalf("error should mention NVIDIA-SMI, got %q", err.Error())
	}
}

// ── 8. specGPUCount against YAML decoding ──────────────────────────────

// ── 8b. verifyGPU cached log fallback ──────────────────────────────────

func successfulGPURunner(readLogs PodLogReader) *Runner {
	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testBurstNodeName,
			Labels: map[string]string{
				"nvidia.com/gpu.present": "true",
				"nvidia.com/gpu.product": testGPUProductLabel,
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-t1-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe-t1"},
		},
		Spec: corev1.PodSpec{
			NodeName: testBurstNodeName,
			Containers: []corev1.Container{{
				Name: "smoke",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
					},
				},
			}},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "smoke",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
				},
			}},
		},
	}
	return &Runner{
		K8s:        k8sfake.NewSimpleClientset(gpuNode, pod),
		Namespace:  "default",
		ReadPodLog: readLogs,
	}
}

func TestVerifyGPUFallsBackToCachedLogs(t *testing.T) {
	r := successfulGPURunner(func(_ context.Context, _ kubernetes.Interface, _, _, _ string, _ int64) ([]byte, error) {
		return nil, fmt.Errorf("no route to host")
	})
	r.capturedLog.WriteString("NVIDIA-SMI 550.127.05   Driver Version: 550.127.05\n")

	if err := r.verifyGPU(context.Background(), "probe-t1", testBurstID, 1); err != nil {
		t.Fatalf("verifyGPU should succeed using cached stream logs, got %v", err)
	}
}

func TestVerifyGPUUsesRetainedPodAndNodeAfterFastTeardown(t *testing.T) {
	r := successfulGPURunner(func(_ context.Context, _ kubernetes.Interface, _, _, _ string, _ int64) ([]byte, error) {
		return nil, fmt.Errorf("pod and node already gone")
	})
	pods, err := r.K8s.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(pods.Items) != 1 {
		t.Fatalf("fixture pod: count=%d err=%v", len(pods.Items), err)
	}
	r.gpuPodSnapshot = pods.Items[0].DeepCopy()
	r.gpuNodeSnapshot = &GPUNodeSnapshot{
		NodeName: testBurstNodeName, GPUAllocatable: 1, GPUPresentLabel: true, GPUProductLabel: testGPUProductLabel,
	}
	r.capturedLog.WriteString("NVIDIA-SMI retained output\n")
	r.K8s = k8sfake.NewSimpleClientset()

	if err := r.verifyGPU(context.Background(), "probe-t1", testBurstID, 1); err != nil {
		t.Fatalf("verifyGPU should use retained exact evidence after teardown: %v", err)
	}
}

func TestVerifyGPURejectsRetainedSnapshotWithoutLabel(t *testing.T) {
	// The retained snapshot must not pass verifyGPU unless GPUPresentLabel is
	// true — an old/legacy snapshot that predates the label observation must
	// fail closed rather than passing on allocatable alone.
	r := successfulGPURunner(func(_ context.Context, _ kubernetes.Interface, _, _, _ string, _ int64) ([]byte, error) {
		return []byte("NVIDIA-SMI 550.127.05\n"), nil
	})
	pods, _ := r.K8s.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	r.gpuPodSnapshot = pods.Items[0].DeepCopy()
	r.gpuNodeSnapshot = &GPUNodeSnapshot{
		NodeName:        testBurstNodeName,
		GPUAllocatable:  1,
		GPUPresentLabel: false,
		GPUProductLabel: testGPUProductLabel,
	}
	r.K8s = k8sfake.NewSimpleClientset() // force snapshot path
	err := r.verifyGPU(context.Background(), "probe-t1", testBurstID, 1)
	if err == nil {
		t.Fatal("verifyGPU accepted a retained snapshot without the label observation")
	}
	if !strings.Contains(err.Error(), "nvidia.com/gpu.present=true") {
		t.Fatalf("error should mention the missing label, got %q", err.Error())
	}
}

func TestVerifyGPUFailsWhenBothAPIAndCacheEmpty(t *testing.T) {
	r := successfulGPURunner(func(_ context.Context, _ kubernetes.Interface, _, _, _ string, _ int64) ([]byte, error) {
		return nil, fmt.Errorf("pod gone after fast teardown")
	})
	err := r.verifyGPU(context.Background(), "probe-t1", testBurstID, 1)
	if err == nil {
		t.Fatal("verifyGPU should fail when both API and cache are empty")
	}
	if !strings.Contains(err.Error(), "no cached stream logs") {
		t.Fatalf("error should mention missing cache, got %q", err.Error())
	}
}

func TestVerifyGPUCachedLogsMissingNVIDIASMI(t *testing.T) {
	r := successfulGPURunner(func(_ context.Context, _ kubernetes.Interface, _, _, _ string, _ int64) ([]byte, error) {
		return nil, fmt.Errorf("pod gone")
	})
	r.capturedLog.WriteString("some other output without GPU evidence\n")

	err := r.verifyGPU(context.Background(), "probe-t1", testBurstID, 1)
	if err == nil {
		t.Fatal("verifyGPU should fail when cached logs lack NVIDIA-SMI")
	}
	if !strings.Contains(err.Error(), "NVIDIA-SMI") {
		t.Fatalf("error should mention NVIDIA-SMI, got %q", err.Error())
	}
}

func TestLogCollectionWindowWaitsForInFlightStream(t *testing.T) {
	r := &Runner{
		K8s:       k8sfake.NewSimpleClientset(),
		Namespace: "default",
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		r.mu.Lock()
		r.capturedLog.WriteString("NVIDIA-SMI retained output\n")
		r.mu.Unlock()
	}()

	start := time.Now()
	r.logCollectionWindow(context.Background(), "probe-t1", 250*time.Millisecond)
	if got := r.cachedLogs(); !strings.Contains(got, "NVIDIA-SMI") {
		t.Fatalf("collection window returned without in-flight log evidence: %q", got)
	}
	if elapsed := time.Since(start); elapsed >= 250*time.Millisecond {
		t.Fatalf("collection window exhausted its budget after evidence arrived: %v", elapsed)
	}
}

// ── 9. waitForReap GPU snapshot seeding ────────────────────────────────

func TestWaitForReapSeedsFromGPUSnapshot(t *testing.T) {
	r := &Runner{
		K8s:      fakeK8sWithNodeScript(nodeStep{}),
		ReapPoll: time.Millisecond,
		gpuNodeSnapshot: &GPUNodeSnapshot{
			NodeName:       testBurstNodeName,
			GPUAllocatable: 1,
		},
	}
	err := r.waitForReap(context.Background(), nil, testBurstID, time.Now().Add(150*time.Millisecond))
	if err != nil {
		t.Fatalf("want clean reap (node pre-observed via GPU snapshot, then absent), got %v", err)
	}
}

func TestWaitForReapSnapshotDoesNotSeedCPUCases(t *testing.T) {
	r := &Runner{
		K8s:      fakeK8sWithNodeScript(nodeStep{}),
		ReapPoll: time.Millisecond,
	}
	err := r.waitForReap(context.Background(), nil, testBurstID, time.Now().Add(10*time.Millisecond))
	if !errors.Is(err, errReapUnobserved) {
		t.Fatalf("CPU case with no GPU snapshot should get errReapUnobserved, got %v", err)
	}
}

func TestWaitForReapSnapshotWrongBurstDoesNotSeed(t *testing.T) {
	r := &Runner{
		K8s:      fakeK8sWithNodeScript(nodeStep{}),
		ReapPoll: time.Millisecond,
		gpuNodeSnapshot: &GPUNodeSnapshot{
			NodeName:       "ys-burst-differentid",
			GPUAllocatable: 1,
		},
	}
	err := r.waitForReap(context.Background(), nil, testBurstID, time.Now().Add(10*time.Millisecond))
	if !errors.Is(err, errReapUnobserved) {
		t.Fatalf("GPU snapshot for a different burst should not seed observation, got %v", err)
	}
}

// ── 10. Per-case state reset ──────────────────────────────────────────

func TestPerCaseStateReset(t *testing.T) {
	r := &Runner{
		K8s:       k8sfake.NewSimpleClientset(),
		Namespace: "default",
		RunID:     "t1",
	}
	r.mu.Lock()
	r.logsStreamed = true
	r.logStarting = true
	r.capturedLog.WriteString("stale logs from previous case\n")
	r.mu.Unlock()
	r.gpuNodeSnapshot = &GPUNodeSnapshot{NodeName: "old-node", GPUAllocatable: 1}
	r.gpuPodSnapshot = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod"}}

	dyn := newFakeDynamic()
	dyn.PrependReactor("create", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, a.(ktesting.CreateAction).GetObject(), nil
	})
	calls := 0
	dyn.PrependReactor("get", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls <= 1 {
			return true, workloadObj(a.(ktesting.GetAction).GetName(), "Running", "flyio", testBurstID), nil
		}
		return true, workloadObj(a.(ktesting.GetAction).GetName(), "Succeeded", "flyio", testBurstID), nil
	})
	dyn.PrependReactor("delete", "workloads", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	r.Dyn = dyn
	r.TerminalPoll = time.Millisecond
	r.ReapPoll = time.Millisecond
	r.ReapBudget = 50 * time.Millisecond

	r.Run(context.Background(), Case{Name: "reset-test", RawYAML: []byte(testCaseYAML)})

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gpuNodeSnapshot != nil {
		t.Fatal("gpuNodeSnapshot should be nil after Run()")
	}
	if r.gpuPodSnapshot != nil {
		t.Fatal("gpuPodSnapshot should be nil after Run()")
	}
	if r.capturedLog.Len() > 0 {
		t.Fatal("capturedLog should be reset at the start of Run()")
	}
}

// ── 11. specGPUCount against YAML decoding ────────────────────────────

func TestSpecGPUCountFromYAMLDecoding(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want int64
	}{
		{
			name: "count 1 from real case YAML",
			yaml: `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: linode-gpu-tiny
spec:
  image: nvcr.io/nvidia/cuda:12.4.0-base-ubuntu22.04
  backend: linode
  size: small
  command: ["nvidia-smi"]
  gpu:
    kind: rtx4000ada
    count: 1
    reliability: reliable
  retries: 0
  budget:
    maxUSD: 0.20
    deadline: 20m
`,
			want: 1,
		},
		{
			name: "count 2 proves float64 handling",
			yaml: `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: gpu-multi
spec:
  image: cuda:latest
  gpu:
    kind: rtx4090
    count: 2
`,
			want: 2,
		},
		{
			name: "no GPU spec",
			yaml: `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: cpu-only
spec:
  image: busybox
`,
			want: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{}
			if err := sigsyaml.Unmarshal([]byte(tc.yaml), &obj.Object); err != nil {
				t.Fatalf("YAML decode failed: %v", err)
			}
			got := specGPUCount(obj)
			if got != tc.want {
				t.Fatalf("specGPUCount = %d, want %d", got, tc.want)
			}
		})
	}
}

// ── 7. maybeCaptureGPUNode observation ────────────────────────────────

func TestMaybeCaptureGPUNodeRetainsObservation(t *testing.T) {
	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testBurstNodeName,
			Labels: map[string]string{
				"yscale.sh/burst-node":   "true",
				"nvidia.com/gpu.present": "true",
				"nvidia.com/gpu.product": testGPUProductLabel,
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
			Conditions: []corev1.NodeCondition{{
				Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe"},
		},
		Spec: corev1.PodSpec{NodeName: testBurstNodeName},
	}

	k8s := k8sfake.NewSimpleClientset(gpuNode, pod)
	r := &Runner{K8s: k8s, Namespace: "default"}
	r.maybeCaptureGPUNode(context.Background(), "probe", testBurstID)

	if r.nodeReadyAt == nil {
		t.Fatal("nodeReadyAt should be set after observing a valid GPU node")
	}
	if r.nodeReadyAt.IsZero() {
		t.Fatal("nodeReadyAt must be nonzero")
	}
	if r.gpuReadyAt == nil {
		t.Fatal("gpuReadyAt should be set after observing a valid GPU node")
	}
	if r.gpuReadyAt.IsZero() {
		t.Fatal("gpuReadyAt must be nonzero")
	}
	if r.gpuNodeSnapshot == nil {
		t.Fatal("gpuNodeSnapshot should be retained")
	}
	if r.gpuNodeSnapshot.GPUAllocatable != 1 {
		t.Fatalf("GPUAllocatable = %d, want 1", r.gpuNodeSnapshot.GPUAllocatable)
	}
	if !r.gpuNodeSnapshot.GPUPresentLabel {
		t.Fatal("GPUPresentLabel should be true when the node carries the exact label")
	}
	if r.gpuNodeSnapshot.GPUProductLabel != testGPUProductLabel {
		t.Fatalf("GPUProductLabel = %q, want %q", r.gpuNodeSnapshot.GPUProductLabel, testGPUProductLabel)
	}
	if r.gpuNodeSnapshot.NodeName != testBurstNodeName {
		t.Fatalf("NodeName = %q, want %q", r.gpuNodeSnapshot.NodeName, testBurstNodeName)
	}
}

func TestGPUNodeReadyObservationRejectsNonPositiveAllocatable(t *testing.T) {
	for _, value := range []string{"0", "-1"} {
		t.Run(value, func(t *testing.T) {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   testBurstNodeName,
					Labels: map[string]string{"nvidia.com/gpu.present": "true"},
				},
				Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
					corev1.ResourceName("nvidia.com/gpu"): resource.MustParse(value),
				}},
			}

			if snapshot, ok := gpuNodeReadyObservation(node); ok || snapshot != nil {
				t.Fatalf("gpuNodeReadyObservation() = (%v, %v), want (nil, false)", snapshot, ok)
			}
		})
	}
}

func TestMaybeCaptureGPUNodeRejectsMissingLabel(t *testing.T) {
	// Allocatable > 0 and no taint, but the agent's NodeWatcher has not yet
	// stamped nvidia.com/gpu.present. maybeCaptureGPUNode must fail closed —
	// the label is not inferable from allocatable and central's fast teardown
	// must not observe a snapshot the runner never earned.
	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   testBurstNodeName,
			Labels: map[string]string{"yscale.sh/burst-node": "true"},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe"},
		},
		Spec: corev1.PodSpec{NodeName: testBurstNodeName},
	}

	k8s := k8sfake.NewSimpleClientset(gpuNode, pod)
	r := &Runner{K8s: k8s, Namespace: "default"}
	r.maybeCaptureGPUNode(context.Background(), "probe", testBurstID)

	if r.gpuNodeSnapshot != nil {
		t.Fatal("gpuNodeSnapshot must be nil when nvidia.com/gpu.present is absent")
	}
	if r.nodeReadyAt != nil || r.gpuReadyAt != nil {
		t.Fatal("readiness timestamps must be nil when the label is absent")
	}
}

func TestMaybeCaptureGPUNodeRejectsFalseLabel(t *testing.T) {
	// Label is explicitly "false" — the agent's NodeWatcher never
	// reconciled it to true. Any variant other than exact "true" fails closed.
	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testBurstNodeName,
			Labels: map[string]string{
				"yscale.sh/burst-node":   "true",
				"nvidia.com/gpu.present": "false",
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe"},
		},
		Spec: corev1.PodSpec{NodeName: testBurstNodeName},
	}

	k8s := k8sfake.NewSimpleClientset(gpuNode, pod)
	r := &Runner{K8s: k8s, Namespace: "default"}
	r.maybeCaptureGPUNode(context.Background(), "probe", testBurstID)

	if r.gpuNodeSnapshot != nil {
		t.Fatal("gpuNodeSnapshot must be nil when nvidia.com/gpu.present=false")
	}
}

func TestMaybeCaptureGPUNodeWaitsForObservedProductLabel(t *testing.T) {
	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testBurstNodeName,
			Labels: map[string]string{
				"yscale.sh/burst-node":   "true",
				"nvidia.com/gpu.present": "true",
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
			Conditions: []corev1.NodeCondition{{
				Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "probe-xyz", Namespace: "default", Labels: map[string]string{"yscale.sh/workload": "probe"},
		},
		Spec: corev1.PodSpec{NodeName: testBurstNodeName},
	}
	r := &Runner{K8s: k8sfake.NewSimpleClientset(gpuNode, pod), Namespace: "default"}
	r.maybeCaptureGPUNode(context.Background(), "probe", testBurstID)

	if r.nodeReadyAt == nil {
		t.Fatal("NodeReady observation must remain independent of the product label")
	}
	if r.gpuReadyAt != nil || r.gpuNodeSnapshot != nil {
		t.Fatal("GPU evidence was retained before nvidia.com/gpu.product was observed")
	}
}

func TestMaybeCaptureGPUNodeRejectsNotReadyTaint(t *testing.T) {
	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   testBurstNodeName,
			Labels: map[string]string{"yscale.sh/burst-node": "true"},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{{
				Key:    "nvidia.com/gpu-not-ready",
				Effect: corev1.TaintEffectNoSchedule,
			}},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe"},
		},
		Spec: corev1.PodSpec{NodeName: testBurstNodeName},
	}

	k8s := k8sfake.NewSimpleClientset(gpuNode, pod)
	r := &Runner{K8s: k8s, Namespace: "default"}
	r.maybeCaptureGPUNode(context.Background(), "probe", testBurstID)

	if r.nodeReadyAt != nil {
		t.Fatal("nodeReadyAt should be nil when gpu-not-ready taint is present")
	}
	if r.gpuReadyAt != nil {
		t.Fatal("gpuReadyAt should be nil when gpu-not-ready taint is present")
	}
}

func TestMaybeCaptureGPUNodeRejectsNoBurstID(t *testing.T) {
	k8s := k8sfake.NewSimpleClientset()
	r := &Runner{K8s: k8s, Namespace: "default"}
	r.maybeCaptureGPUNode(context.Background(), "probe", "")
	if r.nodeReadyAt != nil {
		t.Fatal("nodeReadyAt should be nil with empty burstID")
	}
}

func TestMaybeCaptureGPUNodeCapturesProviderIDForCPUCase(t *testing.T) {
	cpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   testBurstNodeName,
			Labels: map[string]string{"yscale.sh/burst-node": "true"},
		},
		Spec: corev1.NodeSpec{
			ProviderID: "linode://54321",
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe"},
		},
		Spec: corev1.PodSpec{NodeName: testBurstNodeName},
	}

	k8s := k8sfake.NewSimpleClientset(cpuNode, pod)
	r := &Runner{K8s: k8s, Namespace: "default"}
	r.maybeCaptureGPUNode(context.Background(), "probe", testBurstID)

	if r.providerInstanceID != 54321 {
		t.Fatalf("providerInstanceID = %d, want 54321 (captured from CPU node)", r.providerInstanceID)
	}
	if r.gpuNodeSnapshot != nil {
		t.Fatal("gpuNodeSnapshot should be nil for CPU-only node")
	}
	if r.nodeReadyAt != nil {
		t.Fatal("nodeReadyAt should be nil for CPU-only node")
	}
}

func TestMaybeCaptureGPUNodeCapturesProviderIDBeforeGPUReady(t *testing.T) {
	gpuNotReadyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   testBurstNodeName,
			Labels: map[string]string{"yscale.sh/burst-node": "true"},
		},
		Spec: corev1.NodeSpec{
			ProviderID: "linode://99999",
			Taints: []corev1.Taint{{
				Key:    "nvidia.com/gpu-not-ready",
				Effect: corev1.TaintEffectNoSchedule,
			}},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe"},
		},
		Spec: corev1.PodSpec{NodeName: testBurstNodeName},
	}

	k8s := k8sfake.NewSimpleClientset(gpuNotReadyNode, pod)
	r := &Runner{K8s: k8s, Namespace: "default"}
	r.maybeCaptureGPUNode(context.Background(), "probe", testBurstID)

	if r.providerInstanceID != 99999 {
		t.Fatalf("providerInstanceID = %d, want 99999 (captured even with gpu-not-ready)", r.providerInstanceID)
	}
	if r.gpuNodeSnapshot != nil {
		t.Fatal("gpuNodeSnapshot should be nil when gpu-not-ready taint is present")
	}
}

// ── 12. Post-create failure paths still run reap proof (#93) ────────────

func dynTimeoutAfterSnapshot(backend, burstID string, onDelete func()) (context.Context, context.CancelFunc, *dynamicfake.FakeDynamicClient) {
	ctx, cancel := context.WithCancel(context.Background())
	dyn := newFakeDynamic()
	dyn.PrependReactor("create", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, a.(ktesting.CreateAction).GetObject(), nil
	})
	dyn.PrependReactor("get", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		obj := workloadObj(a.(ktesting.GetAction).GetName(), "Provisioning", backend, burstID)
		cancel() // timeout only after one authoritative status snapshot
		return true, obj, nil
	})
	dyn.PrependReactor("delete", "workloads", func(ktesting.Action) (bool, runtime.Object, error) {
		if onDelete != nil {
			onDelete()
		}
		return true, nil, nil
	})
	return ctx, cancel, dyn
}

func TestRunTimeoutStillRunsReapProof(t *testing.T) {
	// Extra steps: capture() on the error path may list nodes before
	// waitForReap runs. The burst-present step repeats so reap sees it
	// regardless of whether capture consumed one.
	k8s := fakeK8sWithNodeScript(
		nodeStep{}, // baseline: empty
		nodeStep{names: []string{testBurstNodeName}}, // capture or reap: burst present
		nodeStep{names: []string{testBurstNodeName}}, // reap: burst still present
		nodeStep{}, // reap: burst gone (repeats)
	)
	cleanupRequested := false
	prov := &fakeProvider{name: backendLinode, beforeCount: func() {
		if !cleanupRequested {
			t.Error("provider audit ran before scoped Workload cleanup was requested")
		}
	}}

	ctx, cancel, dyn := dynTimeoutAfterSnapshot(backendLinode, testBurstID, func() {
		cleanupRequested = true
	})
	defer cancel()

	r := testRunner(k8s, dyn, prov, time.Second)
	res := r.Run(ctx, Case{Name: "linode-gpu-timeout", RawYAML: []byte(testCaseYAML)})

	if res.Phase != "Timeout" {
		t.Fatalf("want Phase=Timeout, got %q", res.Phase)
	}
	if !strings.Contains(res.Err, "context canceled") && !strings.Contains(res.Err, "timed out") {
		t.Fatalf("Err should preserve the timeout/cancellation, got %q", res.Err)
	}
	if !res.Reaped {
		t.Fatalf("Reaped should be true after clean Node+provider reap; ReapErr=%q", res.ReapErr)
	}
	if res.ReapErr != "" {
		t.Fatalf("ReapErr should be empty on clean reap, got %q", res.ReapErr)
	}
	if prov.calls == 0 {
		t.Fatal("provider audit never ran after a terminal-wait timeout")
	}
	if !cleanupRequested {
		t.Fatal("terminal-wait timeout did not request scoped Workload cleanup")
	}
}

func TestRunTimeoutWithProviderResiduePreservesRootError(t *testing.T) {
	k8s := fakeK8sWithNodeScript(
		nodeStep{}, // baseline
		nodeStep{names: []string{testBurstNodeName}}, // capture or reap: burst present
		nodeStep{names: []string{testBurstNodeName}}, // reap: burst still present
		nodeStep{}, // reap: burst gone (Node half passes)
	)
	prov := &fakeProvider{
		name:  backendLinode,
		steps: []OwnedInstances{{Count: 1, IDs: []string{"9001"}}},
	}

	ctx, cancel, dyn := dynTimeoutAfterSnapshot(backendLinode, testBurstID, nil)
	defer cancel()

	r := testRunner(k8s, dyn, prov, time.Second)
	res := r.Run(ctx, Case{Name: "linode-gpu-residue", RawYAML: []byte(testCaseYAML)})

	if res.Phase != "Timeout" {
		t.Fatalf("want Phase=Timeout, got %q", res.Phase)
	}
	if !strings.Contains(res.Err, "context canceled") && !strings.Contains(res.Err, "timed out") {
		t.Fatalf("root timeout should be preserved in Err, got %q", res.Err)
	}
	if res.Reaped {
		t.Fatal("Reaped=true while a provider instance still carries burst tags")
	}
	if !strings.Contains(res.ReapErr, "9001") {
		t.Fatalf("ReapErr should name the leaked instance, got %q", res.ReapErr)
	}
}

func TestRunTimeoutNoBurstIDCannotFalsePass(t *testing.T) {
	k8s := fakeK8sWithNodeScript(
		nodeStep{}, // baseline
		nodeStep{}, // reap: empty
	)
	prov := &fakeProvider{name: backendLinode}

	ctx, cancel, dyn := dynTimeoutAfterSnapshot(backendLinode, "", nil)
	defer cancel()

	r := testRunner(k8s, dyn, prov, 50*time.Millisecond)
	res := r.Run(ctx, Case{Name: "linode-no-burst", RawYAML: []byte(testCaseYAML)})

	if res.Reaped {
		t.Fatal("Reaped=true with an empty burstID — missing burst identity cannot produce a pass")
	}
	if res.ReapErr == "" {
		t.Fatal("ReapErr should explain why reap failed")
	}
}

func TestRunCRDisappearedStillRunsReapProof(t *testing.T) {
	k8s := fakeK8sWithNodeScript(
		nodeStep{}, // baseline
		nodeStep{names: []string{testBurstNodeName}}, // capture or reap: burst present
		nodeStep{names: []string{testBurstNodeName}}, // reap: burst still present
		nodeStep{}, // reap: burst gone
	)
	prov := &fakeProvider{name: backendLinode}

	dyn := newFakeDynamic()
	dyn.PrependReactor("create", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, a.(ktesting.CreateAction).GetObject(), nil
	})
	calls := 0
	dyn.PrependReactor("get", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return true, workloadObj(a.(ktesting.GetAction).GetName(), "Running", backendLinode, testBurstID), nil
		}
		return true, nil, apierrors.NewNotFound(workloadGVR.GroupResource(), "probe-t1")
	})
	dyn.PrependReactor("delete", "workloads", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})

	r := testRunner(k8s, dyn, prov, time.Second)
	res := r.Run(context.Background(), Case{Name: "linode-gpu-disappeared", RawYAML: []byte(testCaseYAML)})

	if res.Phase != "Error" {
		t.Fatalf("want Phase=Error for a vanished CR, got %q", res.Phase)
	}
	if !strings.Contains(res.Err, "disappeared") {
		t.Fatalf("Err should mention disappearance, got %q", res.Err)
	}
	if !res.Reaped {
		t.Fatalf("reap proof should still run after CR disappearance; ReapErr=%q", res.ReapErr)
	}
	if prov.calls == 0 {
		t.Fatal("provider audit never ran after CR disappeared")
	}
}

// ── 13. Node-ready 600s clock: default/override, missing/not-ready ────
//        deadline, exact-node Ready disarming, inventory fail-closed, CR
//        deletion/cleanup path, GPU-timestamp non-regression.

// readyBurstNode returns a burst-labelled node named after testBurstID
// carrying NodeReady=True.
func readyBurstNode() *corev1.Node {
	n := burstNode(testBurstNodeName)
	n.Status.Conditions = []corev1.NodeCondition{{
		Type: corev1.NodeReady, Status: corev1.ConditionTrue,
	}}
	return n
}

// notReadyBurstNode returns a burst-labelled node named after testBurstID
// carrying NodeReady=False.
func notReadyBurstNode() *corev1.Node {
	n := burstNode(testBurstNodeName)
	n.Status.Conditions = []corev1.NodeCondition{{
		Type: corev1.NodeReady, Status: corev1.ConditionFalse,
	}}
	return n
}

func TestNodeReadyBudgetDefaultAndOverride(t *testing.T) {
	r := &Runner{}
	if got, want := r.nodeReadyBudget(), 10*time.Minute; got != want {
		t.Fatalf("default nodeReadyBudget = %s, want %s (must equal 600s per issue #93)", got, want)
	}
	if got, want := defaultNodeReadyTimeout, 10*time.Minute; got != want {
		t.Fatalf("defaultNodeReadyTimeout constant = %s, want %s (600s)", got, want)
	}
	r.NodeReadyTimeout = 750 * time.Millisecond
	if got, want := r.nodeReadyBudget(), 750*time.Millisecond; got != want {
		t.Fatalf("override nodeReadyBudget = %s, want %s", got, want)
	}
}

func TestNodeIsReady(t *testing.T) {
	tests := []struct {
		name string
		node *corev1.Node
		want bool
	}{
		{"nil", nil, false},
		{"no conditions", &corev1.Node{}, false},
		{"Ready=True", readyBurstNode(), true},
		{"Ready=False", notReadyBurstNode(), false},
		{"Ready=Unknown", &corev1.Node{Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}},
		}}, false},
		{"other condition true only", &corev1.Node{Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse}},
		}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := nodeIsReady(tc.node); got != tc.want {
				t.Fatalf("nodeIsReady = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestCheckNodeReadyDeadlineMissingNode fails closed with
// errNodeReadyDeadline when the exact burst node never appears within
// the budget.
func TestCheckNodeReadyDeadlineMissingNode(t *testing.T) {
	r := &Runner{K8s: k8sfake.NewSimpleClientset(), NodeReadyTimeout: 20 * time.Millisecond}
	r.nodeReadyDeadline = time.Now().Add(r.nodeReadyBudget())
	// Poll the deadline for up to 200ms so the assertion isn't racy against
	// a slow test host, but still stops the moment the clock fires.
	deadline := time.Now().Add(200 * time.Millisecond)
	var err error
	for time.Now().Before(deadline) {
		err = r.checkNodeReadyDeadline(context.Background(), testBurstID)
		if err != nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !errors.Is(err, errNodeReadyDeadline) {
		t.Fatalf("want errNodeReadyDeadline for a burst node that never appeared, got %v", err)
	}
	if !strings.Contains(err.Error(), testBurstNodeName) {
		t.Fatalf("error should name the expected node %q, got %q", testBurstNodeName, err.Error())
	}
	if r.nodeReadyAt != nil {
		t.Fatal("nodeReadyAt must not be stamped when the node never appeared")
	}
}

// TestCheckNodeReadyDeadlineNodeNotReady fails closed when the node
// exists but NodeReady is not True by the deadline.
func TestCheckNodeReadyDeadlineNodeNotReady(t *testing.T) {
	r := &Runner{K8s: k8sfake.NewSimpleClientset(notReadyBurstNode()), NodeReadyTimeout: 20 * time.Millisecond}
	r.nodeReadyDeadline = time.Now().Add(r.nodeReadyBudget())
	deadline := time.Now().Add(200 * time.Millisecond)
	var err error
	for time.Now().Before(deadline) {
		err = r.checkNodeReadyDeadline(context.Background(), testBurstID)
		if err != nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !errors.Is(err, errNodeReadyDeadline) {
		t.Fatalf("want errNodeReadyDeadline for a node that never went Ready, got %v", err)
	}
	if r.nodeReadyAt != nil {
		t.Fatal("nodeReadyAt must not be stamped when NodeReady is not True")
	}
}

// TestCheckNodeReadyDeadlineDisarmsOnExactReady proves that a single
// NodeReady=True observation on the exact burst node disarms the clock
// permanently, even when subsequent polling extends past the original
// deadline.
func TestCheckNodeReadyDeadlineDisarmsOnExactReady(t *testing.T) {
	r := &Runner{K8s: k8sfake.NewSimpleClientset(readyBurstNode()), NodeReadyTimeout: 20 * time.Millisecond}
	r.nodeReadyDeadline = time.Now().Add(r.nodeReadyBudget())

	if err := r.checkNodeReadyDeadline(context.Background(), testBurstID); err != nil {
		t.Fatalf("want clean check after observing exact Ready node, got %v", err)
	}
	if r.nodeReadyAt == nil {
		t.Fatal("nodeReadyAt must be stamped after NodeReady=True observation")
	}
	firstStamp := *r.nodeReadyAt

	// Sleep past the original deadline. The clock must not re-arm.
	time.Sleep(40 * time.Millisecond)
	if err := r.checkNodeReadyDeadline(context.Background(), testBurstID); err != nil {
		t.Fatalf("clock re-armed after Ready observation: %v", err)
	}
	if !r.nodeReadyAt.Equal(firstStamp) {
		t.Fatalf("nodeReadyAt overwritten: first=%s now=%s", firstStamp, *r.nodeReadyAt)
	}
}

func TestCheckNodeReadyDeadlineRejectsLateReadyTransition(t *testing.T) {
	deadline := time.Now().Add(-time.Minute).UTC()
	node := readyBurstNode()
	node.Status.Conditions[0].LastTransitionTime = metav1.NewTime(deadline.Add(time.Second))
	r := &Runner{K8s: k8sfake.NewSimpleClientset(node), NodeReadyTimeout: 10 * time.Minute}
	r.nodeReadyDeadline = deadline

	err := r.checkNodeReadyDeadline(context.Background(), testBurstID)
	if !errors.Is(err, errNodeReadyDeadline) {
		t.Fatalf("late Ready transition must not disarm the deadline, got %v", err)
	}
	if r.nodeReadyAt != nil {
		t.Fatal("late Ready transition must not stamp nodeReadyAt")
	}
}

func TestCheckNodeReadyDeadlineAcceptsOnTimeTransitionObservedAfterDeadline(t *testing.T) {
	deadline := time.Now().Add(-time.Minute).UTC()
	node := readyBurstNode()
	wantReadyAt := deadline.Add(-time.Second)
	node.Status.Conditions[0].LastTransitionTime = metav1.NewTime(wantReadyAt)
	r := &Runner{K8s: k8sfake.NewSimpleClientset(node), NodeReadyTimeout: 10 * time.Minute}
	r.nodeReadyDeadline = deadline

	if err := r.checkNodeReadyDeadline(context.Background(), testBurstID); err != nil {
		t.Fatalf("on-time Ready transition observed after the poll deadline = %v", err)
	}
	if r.nodeReadyAt == nil || !r.nodeReadyAt.Equal(wantReadyAt) {
		t.Fatalf("nodeReadyAt = %v, want condition transition %v", r.nodeReadyAt, wantReadyAt)
	}
}

// TestCheckNodeReadyDeadlineExactBinding proves the check binds to the
// burstID-derived name — a foreign burst node in the cluster does NOT
// disarm the clock for this case.
func TestCheckNodeReadyDeadlineExactBinding(t *testing.T) {
	foreign := burstNode("ys-burst-000000000000")
	foreign.Status.Conditions = []corev1.NodeCondition{{
		Type: corev1.NodeReady, Status: corev1.ConditionTrue,
	}}

	r := &Runner{K8s: k8sfake.NewSimpleClientset(foreign), NodeReadyTimeout: 20 * time.Millisecond}
	r.nodeReadyDeadline = time.Now().Add(r.nodeReadyBudget())

	deadline := time.Now().Add(200 * time.Millisecond)
	var err error
	for time.Now().Before(deadline) {
		err = r.checkNodeReadyDeadline(context.Background(), testBurstID)
		if err != nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !errors.Is(err, errNodeReadyDeadline) {
		t.Fatalf("foreign Ready node must not disarm the clock, got %v", err)
	}
	if r.nodeReadyAt != nil {
		t.Fatal("nodeReadyAt must not be stamped from a foreign burst node")
	}
}

// TestCheckNodeReadyDeadlineFailsClosedOnNonNotFoundError fails the case
// immediately when the Node Get returns a non-NotFound error — an
// unreachable apiserver must never downgrade to "the node isn't there
// yet".
func TestCheckNodeReadyDeadlineFailsClosedOnNonNotFoundError(t *testing.T) {
	k8s := k8sfake.NewSimpleClientset()
	k8s.PrependReactor("get", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("etcd leader election in progress")
	})
	r := &Runner{K8s: k8s, NodeReadyTimeout: 10 * time.Second}
	r.nodeReadyDeadline = time.Now().Add(r.nodeReadyBudget())

	err := r.checkNodeReadyDeadline(context.Background(), testBurstID)
	if !errors.Is(err, errNodeReadyInventory) {
		t.Fatalf("want errNodeReadyInventory on non-NotFound error, got %v", err)
	}
	// Distinct from the deadline error — the operator must know we could
	// not look, not that we looked and gave up.
	if errors.Is(err, errNodeReadyDeadline) {
		t.Fatalf("inventory failure was misreported as a deadline expiry: %v", err)
	}
	if r.nodeReadyAt != nil {
		t.Fatal("nodeReadyAt must not be stamped on inventory failure")
	}
}

// TestCheckNodeReadyDeadlineNoBurstIDStillEnforces proves the clock
// still fires when the agent never stamps burstID onto the CR — a
// silent-agent case must not pass just because we cannot derive the
// expected node name.
func TestCheckNodeReadyDeadlineNoBurstIDStillEnforces(t *testing.T) {
	r := &Runner{K8s: k8sfake.NewSimpleClientset(), NodeReadyTimeout: 20 * time.Millisecond}
	r.nodeReadyDeadline = time.Now().Add(r.nodeReadyBudget())
	deadline := time.Now().Add(200 * time.Millisecond)
	var err error
	for time.Now().Before(deadline) {
		err = r.checkNodeReadyDeadline(context.Background(), "")
		if err != nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !errors.Is(err, errNodeReadyDeadline) {
		t.Fatalf("want errNodeReadyDeadline with empty burstID, got %v", err)
	}
	if !strings.Contains(err.Error(), "no burstID") {
		t.Fatalf("error should explain that burstID was never stamped, got %q", err.Error())
	}
}

// TestCheckNodeReadyDeadlineZeroDeadlineIsNoOp proves that the internal
// state is safe when a test drives waitForTerminal directly without
// arming the clock (the deadline is Time{}). Existing tests rely on
// this — the new invariant must not regress them.
func TestCheckNodeReadyDeadlineZeroDeadlineIsNoOp(t *testing.T) {
	r := &Runner{K8s: k8sfake.NewSimpleClientset()}
	// nodeReadyDeadline is zero — never armed.
	if err := r.checkNodeReadyDeadline(context.Background(), testBurstID); err != nil {
		t.Fatalf("zero deadline must be a no-op, got %v", err)
	}
}

// runNodeReadyDeadlineCase drives a full Run with a short node-ready
// budget and a fake agent that stamps burstID but never presents the
// exact burst node in-cluster. The workload dyn keeps serving
// Provisioning so waitForTerminal cannot short-circuit on Succeeded.
// The check MUST fire inside waitForTerminal and Run's existing error
// path MUST delete only this Workload CR.
func runNodeReadyDeadlineCase(t *testing.T, nodesInCluster ...runtime.Object) (CaseResult, *fakeProvider, *int) {
	t.Helper()

	dyn := newFakeDynamic()
	dyn.PrependReactor("create", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, a.(ktesting.CreateAction).GetObject(), nil
	})
	dyn.PrependReactor("get", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		// Always Provisioning + a stamped burstID: forces the run into
		// the node-ready enforcement rather than a terminal-phase pass.
		return true, workloadObj(a.(ktesting.GetAction).GetName(), "Provisioning", backendLinode, testBurstID), nil
	})
	deletes := 0
	dyn.PrependReactor("delete", "workloads", func(a ktesting.Action) (bool, runtime.Object, error) {
		deletes++
		return true, nil, nil
	})

	k8s := k8sfake.NewSimpleClientset(nodesInCluster...)
	prov := &fakeProvider{name: backendLinode}
	r := &Runner{
		K8s:              k8s,
		Dyn:              dyn,
		Namespace:        "default",
		RunID:            "t1",
		Provider:         prov,
		TerminalPoll:     time.Millisecond,
		ReapPoll:         time.Millisecond,
		ProviderPoll:     time.Millisecond,
		ReapBudget:       100 * time.Millisecond,
		NodeReadyTimeout: 15 * time.Millisecond,
	}
	res := r.Run(context.Background(), Case{Name: "linode-node-ready", RawYAML: []byte(testCaseYAML)})
	return res, prov, &deletes
}

func TestRunFailsAndDeletesCROnNodeReadyDeadline(t *testing.T) {
	res, _, deletes := runNodeReadyDeadlineCase(t)
	if res.Phase != "Timeout" {
		t.Fatalf("want Phase=Timeout on node-ready deadline expiry, got %q", res.Phase)
	}
	if !strings.Contains(res.Err, "did not become Kubernetes Ready") {
		t.Fatalf("Err must explain the deadline failure, got %q", res.Err)
	}
	if !strings.Contains(res.Err, testBurstNodeName) {
		t.Fatalf("Err should name the expected burst node %q, got %q", testBurstNodeName, res.Err)
	}
	if *deletes == 0 {
		t.Fatal("Run must delete only that Workload CR after a node-ready deadline")
	}
	// A node that never became Ready never became visible in the cluster
	// either, so reap has nothing to observe — it must fail closed rather
	// than false-pass.
	if res.Reaped {
		t.Fatal("Reaped=true with no burst node ever observed — reap must not synthesize a pass")
	}
}

func TestRunFailsAndDeletesCROnNodeReadyDeadlineWhenNotReady(t *testing.T) {
	res, _, deletes := runNodeReadyDeadlineCase(t, notReadyBurstNode())
	if res.Phase != "Timeout" {
		t.Fatalf("want Phase=Timeout on node-ready deadline expiry (not-ready node), got %q", res.Phase)
	}
	if !errorContainsAny(res.Err, "did not become Kubernetes Ready") {
		t.Fatalf("Err must explain the deadline failure, got %q", res.Err)
	}
	if *deletes == 0 {
		t.Fatal("Run must delete only that Workload CR after a node-ready deadline")
	}
}

// errorContainsAny is a tiny helper so test intent reads cleanly.
func errorContainsAny(got string, wants ...string) bool {
	for _, w := range wants {
		if strings.Contains(got, w) {
			return true
		}
	}
	return false
}

// TestRunSucceedsWhenExactNodeReady proves that observing the exact
// burst node NodeReady=True inside the window disarms the clock — a
// Succeeded phase surfaces normally even with a tiny NodeReadyTimeout.
func TestRunSucceedsWhenExactNodeReady(t *testing.T) {
	dyn := dynServingWorkload("Succeeded", "flyio", testBurstID)

	readyNode := readyBurstNode()
	// Give the synthetic Ready condition an authoritative transition time.
	// Without it production code intentionally falls back to poll time, which
	// makes this 5ms deadline test flaky under the race detector.
	readyNode.Status.Conditions[0].LastTransitionTime = metav1.Now()
	k8s := k8sfake.NewSimpleClientset(readyNode)
	// Node listing is used by baseline + reap; script it to prove the
	// node was observed and later went away, so reap passes cleanly.
	var mu sync.Mutex
	calls := 0
	k8s.PrependReactor("list", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		i := calls
		calls++
		mu.Unlock()
		list := &corev1.NodeList{}
		switch {
		case i == 0: // baseline
			return true, list, nil
		case i <= 2:
			list.Items = []corev1.Node{*burstNode(testBurstNodeName)}
			return true, list, nil
		default:
			return true, list, nil
		}
	})

	r := &Runner{
		K8s:              k8s,
		Dyn:              dyn,
		Namespace:        "default",
		RunID:            "t1",
		TerminalPoll:     time.Millisecond,
		ReapPoll:         time.Millisecond,
		ProviderPoll:     time.Millisecond,
		ReapBudget:       200 * time.Millisecond,
		NodeReadyTimeout: 5 * time.Millisecond,
	}
	res := r.Run(context.Background(), Case{Name: "flyio-cpu-nano", RawYAML: []byte(testCaseYAML)})
	if res.Phase != "Succeeded" {
		t.Fatalf("want Phase=Succeeded after exact-node Ready observation, got %q (err=%q)", res.Phase, res.Err)
	}
	if r.nodeReadyAt == nil {
		t.Fatal("nodeReadyAt must be stamped after observing NodeReady=True on the exact burst node")
	}
	if !res.Reaped {
		t.Fatalf("Reaped should be true on a clean reap, got ReapErr=%q", res.ReapErr)
	}
}

// TestNodeReadyStampIsTruthfulSeparateFromGPU asserts that the two
// timestamps are independently sourced: a node that is Kubernetes
// Ready but not yet GPU-ready stamps only nodeReadyAt; observing
// GPU-ready later stamps only gpuReadyAt without disturbing the
// existing nodeReadyAt.
func TestNodeReadyStampIsTruthfulSeparateFromGPU(t *testing.T) {
	// First observation: Ready but no GPU allocatable — the node is
	// Kubernetes Ready but the device plugin has not registered yet.
	node := readyBurstNode()
	r := &Runner{K8s: k8sfake.NewSimpleClientset(node), NodeReadyTimeout: 10 * time.Second}
	r.nodeReadyDeadline = time.Now().Add(r.nodeReadyBudget())

	if err := r.checkNodeReadyDeadline(context.Background(), testBurstID); err != nil {
		t.Fatalf("Ready node should stamp nodeReadyAt without error, got %v", err)
	}
	if r.nodeReadyAt == nil {
		t.Fatal("nodeReadyAt must be stamped from NodeReady=True alone")
	}
	if r.gpuReadyAt != nil {
		t.Fatal("gpuReadyAt must NOT be stamped from a Kubernetes-Ready observation")
	}
	if r.gpuNodeSnapshot != nil {
		t.Fatal("gpuNodeSnapshot must NOT be retained from a Kubernetes-Ready-only observation")
	}
	firstNodeStamp := *r.nodeReadyAt

	// Later observation: same node comes back GPU-ready. gpuReadyAt
	// stamps; nodeReadyAt is idempotent (never overwritten).
	gpuNode := readyBurstNode()
	gpuNode.Labels["nvidia.com/gpu.present"] = "true"
	gpuNode.Labels["nvidia.com/gpu.product"] = testGPUProductLabel
	gpuNode.Status.Allocatable = corev1.ResourceList{
		corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "probe-xyz",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "probe"},
		},
		Spec: corev1.PodSpec{NodeName: testBurstNodeName},
	}
	r.K8s = k8sfake.NewSimpleClientset(gpuNode, pod)
	r.maybeCaptureGPUNode(context.Background(), "probe", testBurstID)
	if r.gpuReadyAt == nil {
		t.Fatal("gpuReadyAt must be stamped once GPU-ready criteria hold")
	}
	if r.nodeReadyAt == nil || !r.nodeReadyAt.Equal(firstNodeStamp) {
		t.Fatalf("nodeReadyAt overwritten by GPU-ready observation: first=%v now=%v", firstNodeStamp, r.nodeReadyAt)
	}
	if r.gpuNodeSnapshot == nil {
		t.Fatal("gpuNodeSnapshot should be retained on GPU-ready observation")
	}
}

type fakeAccessProber struct {
	receipt *evidence.AccessProbe
	err     error
	calls   []AccessProbeRequest
}

func (f *fakeAccessProber) Probe(_ context.Context, req AccessProbeRequest) (*evidence.AccessProbe, error) {
	f.calls = append(f.calls, req)
	return f.receipt, f.err
}

func TestAccessProbeRunsOnceAgainstBurstIDExactNode(t *testing.T) {
	now := time.Now().UTC()
	prober := &fakeAccessProber{receipt: &evidence.AccessProbe{
		NodeName:    testBurstNodeName,
		Logs:        true,
		Exec:        true,
		PortForward: true,
		ObservedAt:  now,
	}}
	r := &Runner{
		Namespace:           "paid-gpu",
		RunID:               "yt-access",
		currentCase:         "linode-gpu-rtx4000",
		nodeReadyAt:         &now,
		accessProbeRequired: true,
		AccessProber:        prober,
	}
	if err := r.maybeProbeAccess(context.Background(), testBurstID); err != nil {
		t.Fatal(err)
	}
	if err := r.maybeProbeAccess(context.Background(), testBurstID); err != nil {
		t.Fatalf("second at-most-once check returned error: %v", err)
	}
	if len(prober.calls) != 1 {
		t.Fatalf("access probe calls = %d, want 1", len(prober.calls))
	}
	want := AccessProbeRequest{
		Namespace:    "paid-gpu",
		RunID:        "yt-access",
		CaseName:     "linode-gpu-rtx4000",
		ExpectedNode: burstNodeExpectedName(testBurstID),
	}
	if prober.calls[0] != want {
		t.Fatalf("access request = %+v, want %+v", prober.calls[0], want)
	}
	if r.accessProbe == prober.receipt {
		t.Fatal("runner retained caller-owned receipt pointer")
	}
}

func TestAccessProbeSkippedOutsideEvidenceGPUCases(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name            string
		evidenceEnabled bool
		gpuCount        int64
	}{
		{name: "evidence disabled GPU", evidenceEnabled: false, gpuCount: 1},
		{name: "evidence enabled CPU", evidenceEnabled: true, gpuCount: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prober := &fakeAccessProber{}
			r := &Runner{
				AccessProber:        prober,
				nodeReadyAt:         &now,
				accessProbeRequired: shouldProbeAccess(tc.evidenceEnabled, tc.gpuCount),
			}
			if err := r.maybeProbeAccess(context.Background(), testBurstID); err != nil {
				t.Fatal(err)
			}
			if len(prober.calls) != 0 {
				t.Fatalf("access probe called %d times", len(prober.calls))
			}
		})
	}
}

func TestAccessProbeNodeMismatchFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	prober := &fakeAccessProber{receipt: &evidence.AccessProbe{
		NodeName:    "ys-burst-other",
		Logs:        true,
		Exec:        true,
		PortForward: true,
		ObservedAt:  now,
	}}
	r := &Runner{AccessProber: prober, nodeReadyAt: &now, accessProbeRequired: true}
	if err := r.maybeProbeAccess(context.Background(), testBurstID); err == nil {
		t.Fatal("node-mismatched access receipt passed")
	}
	if r.accessProbeFailureStage != "access_probe.node" {
		t.Fatalf("failure stage = %q, want access_probe.node", r.accessProbeFailureStage)
	}
}

func TestAccessProbeFailureFailsCaseAndStillReaps(t *testing.T) {
	gpuCaseYAML := `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: access-gpu
spec:
  image: nvidia/cuda:latest
  gpu:
    kind: rtx4090
    count: 1
`
	node := readyBurstNode()
	node.Labels["nvidia.com/gpu.present"] = "true"
	node.Labels["nvidia.com/gpu.product"] = testGPUProductLabel
	node.Status.Allocatable = corev1.ResourceList{
		corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "access-gpu-t1-pod",
			Namespace: "default",
			Labels:    map[string]string{"yscale.sh/workload": "access-gpu-t1"},
		},
		Spec: corev1.PodSpec{NodeName: testBurstNodeName},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "workload",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
			}},
		},
	}
	k8s := k8sfake.NewSimpleClientset(node, pod)
	var listMu sync.Mutex
	listCalls := 0
	k8s.PrependReactor("list", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		listMu.Lock()
		call := listCalls
		listCalls++
		listMu.Unlock()
		list := &corev1.NodeList{}
		if call > 0 && call <= 2 {
			list.Items = []corev1.Node{*burstNode(testBurstNodeName)}
		}
		return true, list, nil
	})
	partial := &evidence.AccessProbe{
		NodeName:   testBurstNodeName,
		Logs:       true,
		ObservedAt: time.Now().UTC(),
	}
	prober := &fakeAccessProber{
		receipt: partial,
		err:     accessProbeError("exec", errors.New("stream failed")),
	}
	r := testRunner(k8s, dynServingWorkload("Succeeded", "flyio", testBurstID), nil, time.Second)
	r.EvidenceEnabled = true
	r.AccessProber = prober
	r.ReadPodLog = nvidiaLogReader
	res := r.Run(context.Background(), Case{Name: "flyio-gpu-access", RawYAML: []byte(gpuCaseYAML)})

	if res.Err == "" || !strings.Contains(res.Err, "access probe") {
		t.Fatalf("case did not fail on access probe error: phase=%q err=%q", res.Phase, res.Err)
	}
	if !res.Reaped {
		t.Fatalf("reap path was not reached after access probe failure: %q", res.ReapErr)
	}
	if len(prober.calls) != 1 || prober.calls[0].ExpectedNode != testBurstNodeName {
		t.Fatalf("probe requests = %+v, want exact node %q", prober.calls, testBurstNodeName)
	}
	if r.accessProbe == nil || !r.accessProbe.Logs || r.accessProbe.Exec || r.accessProbe.PortForward {
		t.Fatalf("partial access receipt was not retained: %+v", r.accessProbe)
	}
	if r.accessProbeFailureStage != "access_probe.exec" {
		t.Fatalf("failure stage = %q, want access_probe.exec", r.accessProbeFailureStage)
	}
}
