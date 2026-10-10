package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"

	"github.com/yscale-sh/yscale/internal/evidence"
	yscalelabels "github.com/yscale-sh/yscale/pkg/k8s/api/v1"
)

var workloadGVR = schema.GroupVersionResource{
	Group:    "yscale.sh",
	Version:  "v1",
	Resource: "workloads",
}

const (
	labelRun  = "yscale.sh/yscaletest-run"
	labelCase = "yscale.sh/yscaletest-case"

	// clusterIPPlaceholder is substituted in a case's RawYAML (before
	// apply) with the LIVE ClusterIP of the central yscale-cloud Service,
	// resolved at runtime by main.go. Use it in any case that must reach
	// central by IP (e.g. the pod-to-pod test) so the fixture stays
	// portable across clusters instead of hard-coding a ClusterIP that
	// rots on every cluster migration.
	clusterIPPlaceholder = "__YSCALE_CLOUD_CLUSTERIP__"

	// burstNodeSelector is the label the controller stamps on every burst
	// Node it registers; listing by it is how a case takes its burst
	// inventory.
	burstNodeSelector = "yscale.sh/burst-node=true"
)

// Default cadences and budgets for the reap evidence path. Each has a
// Runner override so tests can drive the same code deterministically
// without sleeping for minutes.
const (
	defaultTerminalPoll = 2 * time.Second
	defaultReapPoll     = 3 * time.Second
	// defaultProviderPoll is slower than the Node poll on purpose: the
	// provider audit is a rate-limited billed API call and the transition
	// it watches (instance actually destroyed) is minutes-scale.
	defaultProviderPoll = 10 * time.Second
	// defaultReapBudget bounds the WHOLE reap proof — Kubernetes Node
	// absence and the provider audit share it, so hardening reap can
	// never extend a run's wall clock.
	defaultReapBudget = 5 * time.Minute
	// defaultNodeReadyTimeout bounds ONLY the exact burst node's
	// come-up/join/connect window: from successful Workload CR creation
	// until the burstID-derived node appears in-cluster with a Kubernetes
	// NodeReady=True condition. It is independent of the per-case
	// -timeout, the workload budget/deadline, defaultReapBudget, and the
	// legacy pkg/controller NodeReadyTimeout. Once Ready is observed the
	// budget disarms for the remainder of the case.
	defaultNodeReadyTimeout = 10 * time.Minute
)

// Errors returned by the wait loops. They exist so callers (and tests)
// can tell the failure modes apart: #93/#24 treat "we could not look" and
// "we looked and it is still there" as different, and neither as success.
var (
	// errWaitTimeout: the per-case budget expired with the CR still in a
	// non-terminal phase.
	errWaitTimeout = errors.New("timed out waiting for a terminal phase")
	// errCRDisappeared: the Workload CR was deleted before we ever read a
	// terminal phase off it.
	errCRDisappeared = errors.New("workload CR disappeared before a terminal phase")

	// errReapCancelled: the run was interrupted mid-proof.
	errReapCancelled = errors.New("reap check cancelled")
	// errReapInventory: the burst-Node listing failed, so cluster state is
	// unknown. Never downgrade this to absence.
	errReapInventory = errors.New("burst node inventory failed")
	// errReapTimeout: the burst Node was observed and was still there when
	// the budget ran out.
	errReapTimeout = errors.New("burst node still present at the reap deadline")
	// errReapUnobserved: this case's burst Node was never seen at all, so
	// its absence proves nothing.
	errReapUnobserved = errors.New("burst node never observed")

	// errEvidenceAuth: the Central evidence request was rejected.
	errEvidenceAuth = errors.New("central evidence auth failed")
	// errEvidenceTimeout: the evidence poll expired before complete
	// evidence was observed.
	errEvidenceTimeout = errors.New("central evidence poll timed out")
	// errEvidenceIncomplete: evidence was returned but is missing a
	// required field (provider_created_at, deleted_at, or
	// durable_reap_receipt).
	errEvidenceIncomplete = errors.New("central evidence incomplete")

	// errNodeReadyDeadline: the exact burstID-derived node did not appear
	// and become Kubernetes NodeReady=True within the node-ready budget
	// (default 10m). Callers use this to fail the case explicitly and
	// hand off to the existing central-cancellation + reap-proof path.
	errNodeReadyDeadline = errors.New("burst node did not become Kubernetes Ready within the node-ready timeout")
	// errNodeReadyInventory: a non-NotFound error occurred while trying to
	// observe the exact burst node during the node-ready wait. Fail closed
	// rather than downgrade an unreachable apiserver into "the node isn't
	// there yet". Distinct sentinel from errReapInventory even though the
	// symptom is similar; the operator needs to know which loop looked.
	errNodeReadyInventory = errors.New("node-ready inventory failed")
)

// Case is a single test loaded from disk.
type Case struct {
	Name    string // filename stem
	Path    string
	RawYAML []byte
}

// CaseResult is what the report prints for each case.
type CaseResult struct {
	Case     Case
	Phase    string        // terminal Workload.Status.Phase, or "Timeout"/"Error"
	Backend  string        // backend recorded on the CR (from agent)
	BurstID  string        // burstID recorded on the CR (from agent)
	WallTime time.Duration // apply → terminal phase
	// Reaped is true only when BOTH halves of the proof passed: this
	// case's burst Node was observed in the cluster and then went away,
	// AND (for a Linode case) an exact account audit found zero instances
	// still carrying the burst's ownership tags. It is never set from an
	// inventory we could not read.
	Reaped   bool
	ReapTime time.Duration // terminal phase → reap (0 if not reaped)
	// ReapErr explains why Reaped is false. Kept separate from Err so a
	// case that failed its workload AND left residue reports both; the
	// report merges them into the single ERROR column via errorText.
	ReapErr      string
	Err          string
	WorkloadName string // generated metadata.name on the cluster
	// Diagnostic carries pod stdout/stderr + container exit info + events
	// captured at terminal-phase time, BEFORE cleanup deletes the Job.
	// Populated only on Failed/Timeout/Error so the success path stays
	// fast. Shown in the report so debugging never requires kubectl
	// digging post-hoc — the test framework IS the trace.
	Diagnostic string
	// ArtifactResult is the evidence artifact result ("passed" or "failed").
	// Empty when evidence is disabled.
	ArtifactResult string
}

// loadCases walks dir and decodes every .yaml as a Workload CR
// candidate. Doesn't validate the apiVersion/kind here — that's the
// agent's job. We just need a parseable YAML to apply.
// loadCases walks dir and decodes every .yaml. filter is a CSV of
// substrings; a case is included if its filename contains ANY of the
// substrings (OR semantics). Empty filter matches everything. Whitespace
// around CSV entries is trimmed; empty entries are ignored — so
// `-filter "flyio, linode"` and `-filter flyio,linode` both work.
func loadCases(dir, filter string) ([]Case, error) {
	var terms []string
	for _, t := range strings.Split(filter, ",") {
		if t = strings.TrimSpace(t); t != "" {
			terms = append(terms, t)
		}
	}
	matches := func(name string) bool {
		if len(terms) == 0 {
			return true
		}
		for _, t := range terms {
			if strings.Contains(name, t) {
				return true
			}
		}
		return false
	}

	var out []Case
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml") {
			return nil
		}
		if !matches(filepath.Base(path)) {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		stem := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		out = append(out, Case{Name: stem, Path: path, RawYAML: raw})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// checkAgentConnected verifies a yscale-agent pod is Running in the
// cluster. Doesn't guarantee the WS is connected to central, but
// catches the most common "I forgot to install the chart" foot-gun.
func checkAgentConnected(ctx context.Context, k8s kubernetes.Interface) error {
	pods, err := k8s.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=yscale-agent",
	})
	if err != nil {
		return fmt.Errorf("list yscale-agent pods: %w", err)
	}
	for _, p := range pods.Items {
		if p.Status.Phase == "Running" {
			return nil
		}
	}
	return fmt.Errorf("no Running yscale-agent pod found (cluster has %d total)", len(pods.Items))
}

// GPUNodeSnapshot is the authoritative observation of a burst node's GPU
// readiness, captured once during waitForTerminal when the workload pod is
// first assigned to a node that has sufficient nvidia.com/gpu allocatable,
// no gpu-not-ready taint, AND the agent's NodeWatcher has stamped
// nvidia.com/gpu.present exactly "true". A fast teardown by central can
// delete the Node before verifyGPU runs; this snapshot proves the node was
// valid at the time the workload ran. Both labels are read from the live Node
// and never inferred from allocatable or placement, so the snapshot stays
// independent of the requested GPU shape.
type GPUNodeSnapshot struct {
	NodeName        string
	GPUAllocatable  int64
	GPUPresentLabel bool
	GPUProductLabel string
}

// PodLogReader reads container logs from a pod. The production default
// reads from the Kubernetes API; tests inject a fake.
type PodLogReader func(ctx context.Context, k8s kubernetes.Interface, namespace, podName, container string, tailLines int64) ([]byte, error)

func defaultPodLogReader(ctx context.Context, k8s kubernetes.Interface, namespace, podName, container string, tailLines int64) ([]byte, error) {
	return k8s.CoreV1().Pods(namespace).
		GetLogs(podName, &corev1.PodLogOptions{TailLines: &tailLines, Container: container}).
		DoRaw(ctx)
}

// Runner runs one Case end-to-end against the target cluster.
type Runner struct {
	K8s       kubernetes.Interface
	Dyn       dynamic.Interface
	Namespace string
	RunID     string
	Dash      *Dashboard // optional; nil when -watch not set

	// Central yscale-cloud Service coordinates, resolved once in main.go
	// and shared by every case. CentralClusterIP fills clusterIPPlaceholder
	// in case YAML; CentralRef + CentralResolveErr are only used to build a
	// clear error when a case needs the IP but it couldn't be resolved.
	CentralClusterIP  string
	CentralRef        string // "<namespace>/<service>" for error messages
	CentralResolveErr error

	// Provider is the account-level inventory used to prove a burst's
	// provider resources are actually gone. Wired from main.go when
	// LINODE_TOKEN is set; nil otherwise, in which case a Linode case
	// fails its audit closed rather than passing on cluster evidence
	// alone. Non-Linode cases never consult it.
	Provider ProviderInventory

	// Mesh is the read-only mesh inventory used to prove a burst's
	// Tailscale device is absent. Wired from main.go when TS_OAUTH
	// credentials are set.
	Mesh MeshInventory

	// CentralEvidence is the authenticated Central evidence client.
	// When non-nil, the runner polls the workload detail endpoint after
	// CR deletion and fails closed unless provider-created time,
	// cleanup termination, delete receipt, and durable reap receipt
	// are all present. nil skips the Central evidence check.
	CentralEvidence *CentralEvidenceClient
	EvidencePoll    time.Duration

	// EvidenceEnabled gates the paid exact-node Kubernetes access proof.
	// AccessProber is injectable so runner tests never need a live streaming
	// apiserver; main wires the real client-go implementation only in evidence
	// mode. Evidence-disabled and non-GPU cases never call this seam.
	EvidenceEnabled bool
	AccessProber    AccessProber

	// ReadPodLog reads container logs. Nil means the production default
	// (Kubernetes pod-log API).
	ReadPodLog PodLogReader

	// Poll cadences / budget. Zero means the package default; only tests
	// set these, to exercise the same loops without minutes of sleeping.
	TerminalPoll time.Duration
	ReapPoll     time.Duration
	ProviderPoll time.Duration
	ReapBudget   time.Duration
	// NodeReadyTimeout bounds ONLY the exact burst node come-up/join/
	// connect window (see defaultNodeReadyTimeout). Zero means the
	// package default (10m). Tests override this to exercise the same
	// enforcement without waiting 10 minutes.
	NodeReadyTimeout time.Duration

	lastTrace time.Time // throttles maybeLiveTrace to ~30s cadence
	logCtx    context.Context
	logCancel context.CancelFunc

	mu           sync.Mutex
	logsStreamed bool // stream opened successfully
	logStarting  bool // goroutine is attempting to open the stream
	capturedLog  strings.Builder

	// LogCollectionBudget bounds the post-terminal wait for GPU log
	// evidence; only tests override this.
	LogCollectionBudget time.Duration

	currentCase string    // case name for the running goroutine; key into Dashboard
	caseStart   time.Time // when Run() began; for "+MM:SS" elapsed prints

	gpuNodeSnapshot *GPUNodeSnapshot // retained during waitForTerminal
	gpuPodSnapshot  *corev1.Pod      // latest exact scheduled Pod seen before teardown

	nodeReadyAt     *time.Time        // single observation at which maybeCaptureGPUNode proves Node and GPU ready
	gpuReadyAt      *time.Time        // same observation; separate field for explicit wiring
	centralEvidence *WorkloadEvidence // retained after Central evidence poll for artifact writing
	accessProbe     *evidence.AccessProbe

	accessProbeRequired     bool
	accessProbeAttempted    bool
	accessProbeFailureStage string

	// providerInstanceID is the numeric provider ID parsed from
	// spec.providerID on the burst Node (e.g. "linode://12345" → 12345).
	// Captured during maybeCaptureGPUNode so it survives fast teardown.
	providerInstanceID int

	// kubernetesNodeAbsent is set to true only on a successful
	// waitForReap transition. Reset per case.
	kubernetesNodeAbsent bool

	// nodeReadyDeadline is armed in Run() right after successful CR
	// creation and enforced from inside waitForTerminal. It is disarmed
	// permanently for the case as soon as r.nodeReadyAt is stamped.
	// Zero value = no enforcement (test paths that call waitForTerminal
	// directly without going through Run keep their existing behavior).
	nodeReadyDeadline time.Time
}

func orDuration(v, fallback time.Duration) time.Duration {
	if v <= 0 {
		return fallback
	}
	return v
}

func (r *Runner) podLogReader() PodLogReader {
	if r.ReadPodLog != nil {
		return r.ReadPodLog
	}
	return defaultPodLogReader
}

func (r *Runner) terminalPoll() time.Duration { return orDuration(r.TerminalPoll, defaultTerminalPoll) }
func (r *Runner) reapPoll() time.Duration     { return orDuration(r.ReapPoll, defaultReapPoll) }
func (r *Runner) providerPoll() time.Duration { return orDuration(r.ProviderPoll, defaultProviderPoll) }
func (r *Runner) reapBudget() time.Duration   { return orDuration(r.ReapBudget, defaultReapBudget) }
func (r *Runner) evidencePoll() time.Duration { return orDuration(r.EvidencePoll, defaultReapPoll) }
func (r *Runner) nodeReadyBudget() time.Duration {
	return orDuration(r.NodeReadyTimeout, defaultNodeReadyTimeout)
}

// fmtMMSS turns a duration into MM:SS (no microseconds) so phase
// transition prints stay readable at a glance.
func fmtMMSS(d time.Duration) string {
	s := int(d.Seconds())
	return fmt.Sprintf("%02d:%02d", s/60, s%60)
}

// streamPodLogs spins up a goroutine that follows the pod's stdout/
// stderr live, prefixing each line with the workload name so multiple
// parallel cases interleave readably. Cancelled when the test ends.
// No-op if the pod doesn't exist yet — caller throttles.
//
// The stream open is retryable: on a transient kubelet error (e.g.
// no-route-to-host while the burst node is still registering) the
// goroutine clears logStarting so the next poll-tick attempt can try
// again. logsStreamed is set only after the stream is open, never
// before. Captured output is written to capturedLog so verifyGPU can
// use it as evidence even after a fast provider teardown deletes the pod.
func (r *Runner) streamPodLogs(parentCtx context.Context, workloadName string) {
	r.mu.Lock()
	if r.logsStreamed || r.logStarting {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	pods, err := r.K8s.CoreV1().Pods(r.Namespace).List(parentCtx, metav1.ListOptions{
		LabelSelector: "yscale.sh/workload=" + workloadName,
	})
	if err != nil || len(pods.Items) == 0 {
		return
	}
	pod := pods.Items[0]
	started := false
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Running != nil || cs.State.Terminated != nil {
			started = true
			break
		}
	}
	if !started {
		return
	}

	r.mu.Lock()
	if r.logsStreamed || r.logStarting {
		r.mu.Unlock()
		return
	}
	r.logStarting = true
	ctx, cancel := context.WithCancel(parentCtx)
	r.logCtx = ctx
	r.logCancel = cancel
	r.mu.Unlock()

	go func() {
		follow := true
		stream, err := r.K8s.CoreV1().Pods(r.Namespace).
			GetLogs(pod.Name, &corev1.PodLogOptions{Follow: follow, Container: "smoke"}).
			Stream(ctx)
		if err != nil {
			stream, err = r.K8s.CoreV1().Pods(r.Namespace).
				GetLogs(pod.Name, &corev1.PodLogOptions{Follow: follow}).
				Stream(ctx)
		}
		if err != nil {
			fmt.Printf("[%s]   log stream unavailable (will retry): %v\n", workloadName, err)
			cancel()
			r.mu.Lock()
			r.logStarting = false
			r.mu.Unlock()
			return
		}

		r.mu.Lock()
		r.logsStreamed = true
		r.mu.Unlock()

		defer cancel()
		defer stream.Close()
		buf := make([]byte, 4096)
		for {
			n, err := stream.Read(buf)
			if n > 0 {
				for _, line := range strings.Split(strings.TrimRight(string(buf[:n]), "\n"), "\n") {
					if line != "" {
						fmt.Printf("[%s]   pod ▸ %s\n", workloadName, line)
						r.Dash.AppendLog(r.currentCase, line)
						r.mu.Lock()
						r.capturedLog.WriteString(line)
						r.capturedLog.WriteByte('\n')
						r.mu.Unlock()
					}
				}
			}
			if err != nil {
				return
			}
		}
	}()
}

// cachedLogs returns the log output captured by the stream goroutine.
func (r *Runner) cachedLogs() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.capturedLog.String()
}

// logCollectionWindow gives the stream goroutine a bounded window to
// deliver output after a terminal phase, so verifyGPU has evidence even
// when the pod disappears shortly after completion.
func (r *Runner) logCollectionWindow(ctx context.Context, workloadName string, budget time.Duration) {
	collectionCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if r.cachedLogs() != "" {
			return
		}
		// waitForTerminal may observe Succeeded in the same poll that first
		// sees a started container. Keep opening/retrying during this bounded
		// window instead of racing that goroutine and returning immediately.
		r.streamPodLogs(collectionCtx, workloadName)
		select {
		case <-collectionCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Runner) logCollectionBudget() time.Duration {
	return orDuration(r.LogCollectionBudget, 3*time.Second)
}

// maybeLiveTrace prints a snapshot of pod + burst-node state every
// ~30s during the waitForTerminal loop. Cheap (two cluster reads) and
// hugely helps when a hang would otherwise be a silent black box. No
// output when Succeeded was already printed.
func (r *Runner) maybeLiveTrace(ctx context.Context, workloadName string) {
	if time.Since(r.lastTrace) < 30*time.Second {
		return
	}
	r.lastTrace = time.Now()
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Burst node summary — pick any node matching the run; we don't
	// have the burstID handy here without re-fetching the CR, so this
	// is "is any burst node up for this case".
	pods, err := r.K8s.CoreV1().Pods(r.Namespace).List(cctx, metav1.ListOptions{
		LabelSelector: "yscale.sh/workload=" + workloadName,
	})
	if err != nil || len(pods.Items) == 0 {
		// Most likely the agent hasn't created the Job yet — that's
		// useful information by itself.
		fmt.Printf("[%s]   trace: no pod yet (waiting on agent CreateJob)\n", workloadName)
		return
	}
	for _, p := range pods.Items {
		node := p.Spec.NodeName
		if node == "" {
			node = "(unscheduled)"
		}
		fmt.Printf("[%s]   trace: pod=%s phase=%s node=%s\n", workloadName, p.Name, p.Status.Phase, node)
		// Mirror into dashboard.
		podName := p.Name
		podPhase := string(p.Status.Phase)
		var note string
		// If a container is stuck, print its waiting reason — the
		// 90% case of "Provisioning forever" is image-pull-back-off
		// or schedule-block.
		for _, cs := range p.Status.ContainerStatuses {
			if w := cs.State.Waiting; w != nil && w.Reason != "" && w.Reason != "ContainerCreating" {
				fmt.Printf("[%s]     container %s waiting: %s — %s\n", workloadName, cs.Name, w.Reason, w.Message)
				note = fmt.Sprintf("%s waiting: %s — %s", cs.Name, w.Reason, w.Message)
			}
		}
		r.Dash.UpdateCase(r.currentCase, func(s *CaseState) {
			s.PodName = podName
			s.PodPhase = podPhase
			if node != "(unscheduled)" {
				s.NodeName = node
			}
			if note != "" {
				s.Note = note
			}
		})
	}
}

// maybeCaptureGPUNode retains the latest exact scheduled Pod and snapshots
// the burst node's valid GPU state once. Both survive fast teardown so
// verifyGPU can evaluate the terminal Pod status after the live objects vanish.
func (r *Runner) maybeCaptureGPUNode(ctx context.Context, workloadName, burstID string) {
	if burstID == "" {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	pods, err := r.K8s.CoreV1().Pods(r.Namespace).List(cctx, metav1.ListOptions{
		LabelSelector: "yscale.sh/workload=" + workloadName,
	})
	if err != nil || len(pods.Items) == 0 {
		return
	}
	pod := pods.Items[0]
	if pod.Spec.NodeName == "" {
		return
	}
	expectedName := burstNodeExpectedName(burstID)
	if expectedName == "" || pod.Spec.NodeName != expectedName {
		return
	}
	r.gpuPodSnapshot = pod.DeepCopy()

	// Capture provider instance ID from the Node's spec.providerID as
	// soon as the exact burst Node is observed, before GPU readiness
	// checks. This works for CPU-only and GPU-failure cases too.
	if r.providerInstanceID == 0 {
		node, err := r.K8s.CoreV1().Nodes().Get(cctx, pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil {
			return
		}
		if pid := parseLinodeProviderID(node.Spec.ProviderID); pid > 0 {
			r.providerInstanceID = pid
		}

		// Node-ready is a genuinely separate observation from GPU-ready:
		// nodeReadyAt is stamped only when NodeReady=True is observed on
		// the exact burst node (never inferred from allocatable GPU count
		// or from the nvidia.com/gpu.present label). gpuReadyAt is stamped
		// only when the GPU-ready criteria are met. In production the
		// former precedes the latter; here they are recorded independently
		// so a receipt cannot fabricate one from the other.
		r.stampNodeReadyIfReady(node)
		if r.gpuNodeSnapshot == nil {
			if snap, ok := gpuNodeReadyObservation(node); ok {
				gpuNow := time.Now().UTC()
				r.gpuReadyAt = &gpuNow
				r.gpuNodeSnapshot = snap
			}
		}
		return
	}

	if r.gpuNodeSnapshot != nil && r.nodeReadyAt != nil {
		return
	}
	node, err := r.K8s.CoreV1().Nodes().Get(cctx, pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return
	}
	r.stampNodeReadyIfReady(node)
	if r.gpuNodeSnapshot != nil {
		return
	}
	snap, ok := gpuNodeReadyObservation(node)
	if !ok {
		return
	}
	gpuNow := time.Now().UTC()
	r.gpuReadyAt = &gpuNow
	r.gpuNodeSnapshot = snap
}

// gpuNodeReadyObservation returns a snapshot only when the node has positive
// nvidia.com/gpu allocatable, no gpu-not-ready taint, nvidia.com/gpu.present
// exactly equal to "true", AND a valid nonempty nvidia.com/gpu.product label.
// Both labels are read directly and never inferred from placement. Any other
// outcome returns false so the caller retains nothing.
func gpuNodeReadyObservation(node *corev1.Node) (*GPUNodeSnapshot, bool) {
	if node == nil {
		return nil, false
	}
	gpuQ := node.Status.Allocatable[corev1.ResourceName("nvidia.com/gpu")]
	if gpuQ.Sign() <= 0 {
		return nil, false
	}
	for _, t := range node.Spec.Taints {
		if t.Key == "nvidia.com/gpu-not-ready" {
			return nil, false
		}
	}
	if node.Labels[yscalelabels.LabelNvidiaGPUPresent] != "true" {
		return nil, false
	}
	product := node.Labels[yscalelabels.LabelNvidiaGPUProduct]
	if product == "" || yscalelabels.ValidateLabelValue(product) != nil {
		return nil, false
	}
	return &GPUNodeSnapshot{
		NodeName:        node.Name,
		GPUAllocatable:  gpuQ.Value(),
		GPUPresentLabel: true,
		GPUProductLabel: product,
	}, true
}

// burstNodeExpectedName derives the exact burst node name from a burstID
// using the same convention as the decider.
func burstNodeExpectedName(burstID string) string {
	if !strings.HasPrefix(burstID, "burst_") {
		return ""
	}
	suffix := burstNodeSuffix(burstID)
	if suffix == "" {
		return ""
	}
	return "ys-burst-" + suffix
}

// nodeIsReady reports whether node carries a NodeReady condition with
// status True. Any other status (False, Unknown, missing) counts as not
// ready — this is the ONLY signal used to disarm the node-ready deadline,
// so it never derives readiness from allocatable GPU count or from labels.
func nodeReadyTransition(node *corev1.Node, observedAt time.Time) (time.Time, bool) {
	if node == nil {
		return time.Time{}, false
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			if c.Status != corev1.ConditionTrue {
				return time.Time{}, false
			}
			readyAt := c.LastTransitionTime.Time.UTC()
			if readyAt.IsZero() {
				readyAt = observedAt.UTC()
			}
			return readyAt, true
		}
	}
	return time.Time{}, false
}

func nodeIsReady(node *corev1.Node) bool {
	_, ready := nodeReadyTransition(node, time.Now())
	return ready
}

// observeExactBurstNode Gets the exact burstID-derived Node by name.
// NotFound returns (nil, nil) so the caller keeps polling until the
// node-ready deadline. Any other error is returned as-is so callers can
// fail closed rather than downgrade an unreachable apiserver to "not
// there yet". Returns (nil, nil) when burstID does not yet derive a
// valid name (agent has not stamped .status.burstID).
func (r *Runner) observeExactBurstNode(ctx context.Context, burstID string) (*corev1.Node, error) {
	name := burstNodeExpectedName(burstID)
	if name == "" {
		return nil, nil
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	node, err := r.K8s.CoreV1().Nodes().Get(cctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return node, nil
}

// stampNodeReadyIfReady is the single point that stamps r.nodeReadyAt.
// It is idempotent — never overwrites an existing stamp — and truthful:
// it is derived solely from the node's NodeReady condition. GPU
// allocatable, the nvidia.com/gpu.present label, and the gpu-not-ready
// taint never influence this stamp; that keeps nodeReadyAt a genuine
// K8s-Ready observation, distinct from the later gpuReadyAt observation.
func (r *Runner) stampNodeReadyIfReady(node *corev1.Node) {
	if r.nodeReadyAt != nil {
		return
	}
	readyAt, ready := nodeReadyTransition(node, time.Now())
	if !ready {
		return
	}
	r.nodeReadyAt = &readyAt
}

// checkNodeReadyDeadline enforces the 600s node come-up/join/connect
// budget. It runs on every waitForTerminal tick. Behavior:
//   - Once r.nodeReadyAt is stamped, the check is disarmed permanently
//     for the case, so the clock CAN NEVER expire while the workload
//     continues.
//   - A non-NotFound Node Get error fails the case closed as
//     errNodeReadyInventory — we looked and could not tell.
//   - A missing (NotFound) burst node keeps polling until r.nodeReadyDeadline.
//   - Expiry returns errNodeReadyDeadline so Run's existing error path
//     deletes only this Workload CR, letting central cancellation plus
//     the bounded reap proof run against the same case.
//   - A zero r.nodeReadyDeadline (test paths that call waitForTerminal
//     without going through Run) is a no-op — no enforcement.
func (r *Runner) checkNodeReadyDeadline(ctx context.Context, burstID string) error {
	if r.nodeReadyAt != nil {
		return nil
	}
	// Parent-context cancellation (per-case -timeout, Ctrl-C, etc.) is
	// waitForTerminal's outer select's job to surface as errWaitTimeout.
	// Returning nil here defers to that path instead of misreporting a
	// canceled Get as an inventory failure.
	if ctx.Err() != nil {
		return nil
	}
	if burstID != "" {
		node, err := r.observeExactBurstNode(ctx, burstID)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("%w: %s: %v",
				errNodeReadyInventory,
				burstNodeExpectedName(burstID),
				err)
		}
		if readyAt, ready := nodeReadyTransition(node, time.Now()); ready {
			// A poll may land just after the deadline even though kubelet
			// became Ready just before it. Use the Node condition's own
			// transition time when present so polling cadence neither grants
			// late nodes extra time nor rejects an on-time transition.
			if !r.nodeReadyDeadline.IsZero() && readyAt.After(r.nodeReadyDeadline) {
				return fmt.Errorf("%w: expected %s, became Ready at %s after waiting %s",
					errNodeReadyDeadline,
					burstNodeExpectedName(burstID),
					readyAt.Format(time.RFC3339Nano),
					r.nodeReadyBudget())
			}
			r.nodeReadyAt = &readyAt
			return nil
		}
	}
	if r.nodeReadyDeadline.IsZero() || time.Now().Before(r.nodeReadyDeadline) {
		return nil
	}
	expected := burstNodeExpectedName(burstID)
	if expected == "" {
		expected = "(no burstID stamped on CR yet)"
	}
	return fmt.Errorf("%w: expected %s, waited %s", errNodeReadyDeadline, expected, r.nodeReadyBudget())
}

func (r *Runner) Run(ctx context.Context, c Case) CaseResult {
	res := CaseResult{Case: c}
	start := time.Now()
	r.currentCase = c.Name
	r.caseStart = start
	r.gpuNodeSnapshot = nil
	r.gpuPodSnapshot = nil
	r.nodeReadyAt = nil
	r.gpuReadyAt = nil
	r.centralEvidence = nil
	r.accessProbe = nil
	r.accessProbeRequired = false
	r.accessProbeAttempted = false
	r.accessProbeFailureStage = ""
	r.providerInstanceID = 0
	r.kubernetesNodeAbsent = false
	r.nodeReadyDeadline = time.Time{}
	if r.logCancel != nil {
		r.logCancel()
	}
	r.mu.Lock()
	r.logsStreamed = false
	r.logStarting = false
	r.capturedLog.Reset()
	r.mu.Unlock()
	r.Dash.CaseStart(c.Name)
	defer func() {
		r.Dash.UpdateCase(c.Name, func(s *CaseState) {
			s.Phase = res.Phase
			s.Backend = res.Backend
			s.BurstID = res.BurstID
			s.Err = res.errorText()
			s.FinishedAt = time.Now()
		})
	}()

	// 1. Decode + stamp identifying labels so trap-cleanup is safe.
	obj, err := r.prepare(c)
	if err != nil {
		res.Phase, res.Err = "Error", "prepare: "+err.Error()
		return res
	}
	res.WorkloadName = obj.GetName()
	r.accessProbeRequired = shouldProbeAccess(r.EvidenceEnabled, specGPUCount(obj))

	// 2. Snapshot pre-existing burst nodes so we can later detect
	//    THIS test's burst even if the CR doesn't surface burstID.
	//
	//    A failed baseline is fatal HERE, before the CR exists. Swallowing
	//    it used to leave reap comparing against an empty set, which reads
	//    as "no new burst node — nothing to reap" and passes. Refusing to
	//    launch is the cheap failure; launching a burst we cannot audit is
	//    the expensive one (#93/#24).
	preBursts, err := r.listBurstNodes(ctx)
	if err != nil {
		res.Phase, res.Err = "Error", "burst node baseline: "+err.Error()
		return res
	}

	// 3. Apply the CR — the customer-facing action.
	wls := r.Dyn.Resource(workloadGVR).Namespace(r.Namespace)
	created, err := wls.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		res.Phase, res.Err = "Error", "create: "+err.Error()
		return res
	}
	defer r.cleanup(context.Background(), created.GetName())
	// Arm the node-ready clock immediately after successful CR creation.
	// This is the ONLY thing it clocks: the exact burst node coming up,
	// joining, and reaching Kubernetes NodeReady=True. It is disarmed
	// permanently for the case as soon as r.nodeReadyAt is stamped.
	r.nodeReadyDeadline = time.Now().Add(r.nodeReadyBudget())

	// 4. Poll the CR until a terminal phase or per-case timeout.
	terminal, lastObj, err := r.waitForTerminal(ctx, created.GetName())
	var workloadID string
	res.WallTime = time.Since(start)
	if err != nil {
		// Only a genuinely expired budget is a "Timeout"; a vanished CR or
		// a broken apiserver read is an "Error". Both are failures — the
		// distinction is purely so the report says something true.
		res.Phase = "Error"
		if errors.Is(err, errWaitTimeout) || errors.Is(err, errNodeReadyDeadline) {
			res.Phase = "Timeout"
		}
		res.Err = err.Error()
		if lastObj != nil {
			res.Backend, _, _ = unstructured.NestedString(lastObj.Object, "status", "backend")
			res.BurstID, _, _ = unstructured.NestedString(lastObj.Object, "status", "burstID")
			workloadID, _, _ = unstructured.NestedString(lastObj.Object, "status", "workloadID")
		}
		res.Diagnostic = r.capture(ctx, created.GetName(), res.BurstID)
		// Request cleanup so central initiates teardown of any paid
		// resources before the reap proof runs below. Idempotent with
		// the deferred cleanup.
		r.cleanup(context.Background(), created.GetName())
		r.proveReap(context.Background(), preBursts, created.GetName(), workloadID, &res)
		return res
	}
	res.Phase = terminal
	res.Backend, _, _ = unstructured.NestedString(lastObj.Object, "status", "backend")
	res.BurstID, _, _ = unstructured.NestedString(lastObj.Object, "status", "burstID")
	workloadID, _, _ = unstructured.NestedString(lastObj.Object, "status", "workloadID")

	// Failed / Cancelled: capture diagnostic before cleanup deletes the
	// Job and Pod. 5-min Job TTL means pod logs vanish soon after.
	if terminal != "Succeeded" {
		if errMsg, found, _ := unstructured.NestedString(lastObj.Object, "status", "error"); found && errMsg != "" {
			res.Err = errMsg
		}
		res.Diagnostic = r.capture(ctx, created.GetName(), res.BurstID)
	}

	// 4b. GPU verification — fail-closed. When the workload spec
	// requests GPU, verify the burst actually delivered one before
	// declaring the case successful. Verification failure never skips reap.
	gpuCount := specGPUCount(obj)
	if gpuCount > 0 && terminal == "Succeeded" {
		r.logCollectionWindow(ctx, created.GetName(), r.logCollectionBudget())
		if err := r.verifyGPU(ctx, created.GetName(), res.BurstID, gpuCount); err != nil {
			res.Err = "GPU verification: " + err.Error()
		}
	}

	r.proveReap(ctx, preBursts, created.GetName(), workloadID, &res)
	return res
}

// proveReap runs both halves of the bounded zero-leak proof. Callers retain
// workload failure in Err while this helper records cleanup failure separately.
func (r *Runner) proveReap(ctx context.Context, preBursts map[string]struct{}, workloadName, workloadID string, res *CaseResult) {
	reapStart := time.Now()
	reapDeadline := reapStart.Add(r.reapBudget())
	if err := r.deleteWorkloadCR(context.Background(), workloadName); err != nil {
		res.ReapErr = "delete workload before durable evidence: " + err.Error()
		return
	}
	if err := r.waitForReap(ctx, preBursts, res.BurstID, reapDeadline); err != nil {
		res.ReapErr = err.Error()
		return
	}
	r.kubernetesNodeAbsent = true
	if err := r.waitForProviderAbsence(ctx, res.Backend, res.BurstID, reapDeadline); err != nil {
		res.ReapErr = err.Error()
		return
	}
	if err := r.waitForCentralEvidence(ctx, workloadID, reapDeadline); err != nil {
		res.ReapErr = err.Error()
		return
	}
	res.Reaped = true
	res.ReapTime = time.Since(reapStart)
}

// prepare decodes the raw YAML into an unstructured obj and stamps
// run/case labels + a unique name suffix so parallel runs don't
// collide.
func (r *Runner) prepare(c Case) (*unstructured.Unstructured, error) {
	// Substitute the live central ClusterIP into the raw YAML before
	// decoding, so cases never hard-code a cluster-specific Service IP.
	raw := c.RawYAML
	if strings.Contains(string(raw), clusterIPPlaceholder) {
		if r.CentralClusterIP == "" {
			return nil, fmt.Errorf("case references %s but the central Service ClusterIP for %s could not be resolved: %v "+
				"(check that yscale-cloud is deployed, or override with -central-service / -central-namespace)",
				clusterIPPlaceholder, r.CentralRef, r.CentralResolveErr)
		}
		raw = []byte(strings.ReplaceAll(string(raw), clusterIPPlaceholder, r.CentralClusterIP))
	}

	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(raw, &obj.Object); err != nil {
		return nil, fmt.Errorf("yaml: %w", err)
	}
	if obj.GetKind() != "Workload" {
		return nil, fmt.Errorf("expected kind=Workload, got %q", obj.GetKind())
	}
	base := obj.GetName()
	if base == "" {
		base = c.Name
	}
	// Stable suffix per runID so two parallel cases never collide.
	obj.SetName(fmt.Sprintf("%s-%s", base, r.RunID))
	obj.SetNamespace(r.Namespace)
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[labelRun] = r.RunID
	labels[labelCase] = c.Name
	obj.SetLabels(labels)
	// strip any incoming status block — server will fill it
	unstructured.RemoveNestedField(obj.Object, "status")
	return obj, nil
}

// waitForTerminal polls the CR every 2s until .status.phase is one of
// the terminal values, or ctx fires.
func (r *Runner) waitForTerminal(ctx context.Context, name string) (string, *unstructured.Unstructured, error) {
	wls := r.Dyn.Resource(workloadGVR).Namespace(r.Namespace)
	t := time.NewTicker(r.terminalPoll())
	defer t.Stop()
	var last *unstructured.Unstructured
	var lastPhase string
	for {
		select {
		case <-ctx.Done():
			return "", last, fmt.Errorf("%w (last phase=%q)", errWaitTimeout, lastPhase)
		case <-t.C:
			obj, err := wls.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					// A vanished CR used to be synthesized into "Succeeded"
					// whenever we'd already seen Provisioning/Running. That
					// is exactly what a central-side reap-without-writeback,
					// a stray `kubectl delete`, and a crashed agent all look
					// like — none of which proves the workload ran. #93/#24:
					// no observed terminal phase, no pass.
					return "", last, fmt.Errorf("%w (last phase=%q)", errCRDisappeared, lastPhase)
				}
				return "", last, fmt.Errorf("get: %w", err)
			}
			last = obj
			phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
			if phase != lastPhase {
				// +MM:SS shows wall-clock since CR-apply at each
				// transition — the most useful single signal for
				// "where is time going". Matches the dashboard's
				// case elapsed counter.
				elapsed := time.Since(r.caseStart)
				fmt.Printf("[%s] +%s phase: %s → %s\n", name, fmtMMSS(elapsed), lastPhase, phase)
				lastPhase = phase
				// Mirror to dashboard if enabled. Stays in sync with stdout.
				burstID, _, _ := unstructured.NestedString(obj.Object, "status", "burstID")
				backend, _, _ := unstructured.NestedString(obj.Object, "status", "backend")
				r.Dash.UpdateCase(r.currentCase, func(s *CaseState) {
					s.Phase = phase
					if burstID != "" {
						s.BurstID = burstID
					}
					if backend != "" {
						s.Backend = backend
					}
				})
				if phase == "Provisioning" && burstID != "" {
					fmt.Printf("[%s]   burst=%s backend=%s\n", name, burstID, backend)
				}
			}
			// Live trace: every ~30s while Provisioning, log what the
			// pod + node look like so a hang is obvious without `kubectl`.
			r.maybeLiveTrace(ctx, name)
			// Once a container starts, follow its stdout/stderr live.
			r.streamPodLogs(ctx, name)
			// Retain GPU node snapshot once the pod is on a valid burst node.
			crBurstID, _, _ := unstructured.NestedString(obj.Object, "status", "burstID")
			r.maybeCaptureGPUNode(ctx, name, crBurstID)
			// Enforce the exact-node come-up/join/Ready budget (default 10m).
			// Runs BEFORE the terminal-phase switch so a Succeeded phase does
			// not paper over a burst that never actually became Ready inside
			// the window. Disarmed permanently once nodeReadyAt is stamped.
			if err := r.checkNodeReadyDeadline(ctx, crBurstID); err != nil {
				return "", obj, err
			}
			// Evidence-mode GPU cases exercise logs, exec, and a real HTTP
			// port-forward as soon as the exact burst node is observed Ready.
			// Doing this before the terminal-phase switch avoids racing Central's
			// fast provider teardown after the workload Job completes.
			if err := r.maybeProbeAccess(ctx, crBurstID); err != nil {
				return "", obj, fmt.Errorf("access probe: %w", err)
			}
			switch phase {
			case "Succeeded", "Failed", "Cancelled":
				return phase, obj, nil
			}
		}
	}
}

// listBurstNodes returns the set of `ys-burst-*` node names currently
// in the cluster. Used to baseline reap detection.
func (r *Runner) listBurstNodes(ctx context.Context) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	nodes, err := r.K8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: burstNodeSelector,
	})
	if err != nil {
		return nil, err
	}
	for _, n := range nodes.Items {
		out[n.Name] = struct{}{}
	}
	return out, nil
}

// burstNodeSuffix converts a burstID into the node-name fragment the
// decider derives from it (backends.NodeNamePrefix + burstID minus its
// "burst_" prefix), truncated to the tail burst node names carry. This is
// the pre-existing exact node-name contract; it is factored out, not
// changed.
func burstNodeSuffix(burstID string) string {
	s := strings.TrimPrefix(burstID, "burst_")
	if len(s) > 12 {
		s = s[:12]
	}
	return s
}

// matchBurstNode returns the name of the burst Node belonging to this case,
// or "" if none is present. With a suffix it uses the exact node-name
// contract; without one it falls back to the baseline diff. Names are
// scanned in sorted order so a multi-match picks deterministically.
func matchBurstNode(current, baseline map[string]struct{}, suffix string) string {
	names := make([]string, 0, len(current))
	for n := range current {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if suffix != "" {
			if strings.Contains(n, suffix) {
				return n
			}
			continue
		}
		if _, pre := baseline[n]; !pre {
			return n
		}
	}
	return ""
}

// burstDescriptor names what we were looking for, for error messages.
func burstDescriptor(burstID, suffix string) string {
	if suffix != "" {
		return fmt.Sprintf("burstID %s (node name containing %q)", burstID, suffix)
	}
	return "any burst node absent from the pre-test baseline"
}

// waitForReap proves the Kubernetes half of reap: this case's burst Node
// was OBSERVED in the cluster and then became absent.
//
// Absence on its own is not evidence, and that was the old false pass. The
// previous loop returned true the moment a listing contained no matching
// node — which is the normal state for the first seconds after a terminal
// phase, before the burst kubelet has even registered, and the permanent
// state when the burst never launched. It also skipped past Node-list
// errors, so an unreachable apiserver looked exactly like a clean cluster.
//
// Now: a list error stops the proof, cancellation is reported as such, and
// the deadline distinguishes "never saw it" from "still there".
func (r *Runner) waitForReap(ctx context.Context, baseline map[string]struct{}, burstID string, deadline time.Time) error {
	reapCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	t := time.NewTicker(r.reapPoll())
	defer t.Stop()
	suffix := burstNodeSuffix(burstID)

	var observed string
	if r.gpuNodeSnapshot != nil && suffix != "" {
		if expected := burstNodeExpectedName(burstID); expected != "" && r.gpuNodeSnapshot.NodeName == expected {
			observed = r.gpuNodeSnapshot.NodeName
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: %v (burst node observed: %s)", errReapCancelled, err, or(observed, "none"))
		}
		// The deadline is checked before every list and again before accepting
		// absence. Otherwise a slow apiserver response that arrives after the
		// shared budget can still turn into Reaped=true.
		if !time.Now().Before(deadline) {
			return reapDeadlineError(observed, burstID, suffix)
		}
		now, err := r.listBurstNodes(reapCtx)
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("%w: %v (burst node observed: %s)", errReapCancelled, ctx.Err(), or(observed, "none"))
			}
			if errors.Is(reapCtx.Err(), context.DeadlineExceeded) || !time.Now().Before(deadline) {
				return reapDeadlineError(observed, burstID, suffix)
			}
			return fmt.Errorf("%w: %v", errReapInventory, err)
		}
		if name := matchBurstNode(now, baseline, suffix); name != "" {
			observed = name
		} else if observed != "" {
			if !time.Now().Before(deadline) {
				return reapDeadlineError(observed, burstID, suffix)
			}
			return nil
		}
		if !time.Now().Before(deadline) {
			return reapDeadlineError(observed, burstID, suffix)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v (burst node observed: %s)", errReapCancelled, ctx.Err(), or(observed, "none"))
		case <-reapCtx.Done():
			return reapDeadlineError(observed, burstID, suffix)
		case <-t.C:
		}
	}
}

func reapDeadlineError(observed, burstID, suffix string) error {
	if observed == "" {
		return fmt.Errorf("%w: never saw %s within the reap budget, so its absence proves nothing",
			errReapUnobserved, burstDescriptor(burstID, suffix))
	}
	return fmt.Errorf("%w: %s", errReapTimeout, observed)
}

// waitForProviderAbsence proves the authoritative half of reap: no instance
// in the account still carries this burst's ownership tags.
//
// Only Linode is wired in this slice; every other backend keeps its
// existing provider-independent behavior. A Linode case that reaches this
// point without a configured inventory fails closed — "we could not check"
// must never render as "nothing is there" (#93/#24).
func (r *Runner) waitForProviderAbsence(ctx context.Context, backend, burstID string, deadline time.Time) error {
	if backend == "" {
		return fmt.Errorf("%w: missing authoritative backend identity from CR snapshot", errProviderInconclusive)
	}
	if burstID == "" {
		return fmt.Errorf("%w: missing authoritative burst identity from CR snapshot", errProviderInconclusive)
	}
	if backend != backendLinode {
		return nil
	}
	if r.Provider == nil {
		return fmt.Errorf("%w: a %s case can only be proven reaped by an account audit; set LINODE_TOKEN so yscaletest can list the account",
			errProviderUnconfigured, backendLinode)
	}
	if got := r.Provider.Name(); got != backend {
		return fmt.Errorf("%w: configured inventory audits %q but this case ran on %q",
			errProviderUnconfigured, got, backend)
	}

	auditCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	t := time.NewTicker(r.providerPoll())
	defer t.Stop()
	var last OwnedInstances
	listed := false
	for {
		if !time.Now().Before(deadline) {
			return providerDeadlineError(last, listed, burstID)
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: audit cancelled: %v", errProviderInconclusive, err)
		}
		owned, err := r.Provider.CountBurstInstances(auditCtx, burstID)
		if err != nil {
			if !time.Now().Before(deadline) || errors.Is(auditCtx.Err(), context.DeadlineExceeded) {
				return providerDeadlineError(last, listed, burstID)
			}
			if ctx.Err() != nil {
				return fmt.Errorf("%w: audit cancelled: %v", errProviderInconclusive, ctx.Err())
			}
			return fmt.Errorf("%w: could not list provider inventory for %s: %w", errProviderInconclusive, burstID, err)
		}
		last, listed = owned, true
		if owned.Count == 0 {
			if !time.Now().Before(deadline) {
				return providerDeadlineError(last, listed, burstID)
			}
			return nil
		}
		if !time.Now().Before(deadline) {
			return providerDeadlineError(last, listed, burstID)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: audit cancelled: %v", errProviderInconclusive, ctx.Err())
		case <-auditCtx.Done():
			return providerDeadlineError(last, listed, burstID)
		case <-t.C:
		}
	}
}

func providerDeadlineError(last OwnedInstances, listed bool, burstID string) error {
	if listed && last.Count > 0 {
		// Counts and provider IDs only — enough to go delete them by hand,
		// nothing that echoes an API payload into the report.
		return fmt.Errorf("%w: %d instance(s) still tagged %q+%q at the reap deadline (ids: %s)",
			errProviderResidue, last.Count, yscaleBurstTag, burstID, strings.Join(last.IDs, ","))
	}
	return fmt.Errorf("%w: the shared reap deadline elapsed before a zero-leak Linode inventory completed for %s",
		errProviderInconclusive, burstID)
}

// waitForCentralEvidence polls the Central workload detail endpoint after CR
// deletion and fails closed unless all evidence fields are present:
// provider_created_at, cleanup state terminated, deleted_at, and
// durable_reap_receipt. Skipped when CentralEvidence is nil.
//
// Every incomplete field keeps polling: provider-delete becomes terminated
// before post-delete cleanup writes the durable reap receipt, so there is a
// legitimate convergence window where state=terminated but the receipt (and
// sometimes deleted_at or provider_created_at) have not landed yet. Only
// auth errors, context cancellation, and deadline expiry are immediate
// failures.
func (r *Runner) waitForCentralEvidence(ctx context.Context, workloadID string, deadline time.Time) error {
	if r.CentralEvidence == nil {
		return nil
	}
	if workloadID == "" {
		return fmt.Errorf("%w: .status.workloadID was never stamped on the CR", errEvidenceIncomplete)
	}

	t := time.NewTicker(r.evidencePoll())
	defer t.Stop()
	lastObservation := "no response received"
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: %v (last observation: %s)", errEvidenceTimeout, err, lastObservation)
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%w: evidence not complete by the reap deadline (last observation: %s)", errEvidenceTimeout, lastObservation)
		}
		ev, err := r.CentralEvidence.GetWorkloadEvidence(ctx, workloadID)
		if err != nil {
			if errors.Is(err, errEvidenceAuth) {
				return err
			}
			if !time.Now().Before(deadline) {
				return fmt.Errorf("%w: last error: %v", errEvidenceTimeout, err)
			}
			lastObservation = "request failed: " + err.Error()
			select {
			case <-ctx.Done():
				return fmt.Errorf("%w: %v (last observation: %s)", errEvidenceTimeout, ctx.Err(), lastObservation)
			case <-t.C:
				continue
			}
		}
		r.centralEvidence = ev
		if evidenceComplete(ev) {
			return nil
		}
		lastObservation = incompleteEvidenceSummary(ev)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v (last observation: %s)", errEvidenceTimeout, ctx.Err(), lastObservation)
		case <-t.C:
		}
	}
}

// maxIncompleteResultBytes bounds a result string echoed into the incomplete
// summary. Central's stored strings are already short; this just refuses to
// let a malformed response spray a long value into the operator-visible log.
const maxIncompleteResultBytes = 32

func incompleteEvidenceSummary(ev *WorkloadEvidence) string {
	if ev == nil {
		return "no response received"
	}
	if ev.Cleanup == nil {
		return "cleanup missing"
	}
	missing := make([]string, 0, 6)
	if ev.Cleanup.ProviderCreatedAt == nil {
		missing = append(missing, "provider_created_at")
	}
	if ev.Cleanup.State != "terminated" {
		missing = append(missing, "state="+ev.Cleanup.State)
	}
	if ev.Cleanup.DeletedAt == nil {
		missing = append(missing, "deleted_at")
	}
	if !ev.Cleanup.DurableReapReceipt {
		missing = append(missing, "durable_reap_receipt")
	}
	if ev.Outcome == nil {
		missing = append(missing, "outcome")
	} else if ev.Status == "succeeded" && ev.Outcome.Compute.Result != "succeeded" {
		missing = append(missing,
			"outcome.compute.result="+boundedResult(ev.Outcome.Compute.Result))
	}
	return "incomplete: " + strings.Join(missing, ", ")
}

func boundedResult(value string) string {
	if len(value) <= maxIncompleteResultBytes {
		return value
	}
	return value[:maxIncompleteResultBytes] + "…"
}

// evidenceComplete returns true only when every required field has arrived,
// including the receipt: an outcome that disagrees with a succeeded terminal
// status is treated as incomplete rather than fabricated into a pass.
func evidenceComplete(ev *WorkloadEvidence) bool {
	if ev == nil || ev.Cleanup == nil {
		return false
	}
	if ev.Cleanup.ProviderCreatedAt == nil ||
		ev.Cleanup.State != "terminated" ||
		ev.Cleanup.DeletedAt == nil ||
		!ev.Cleanup.DurableReapReceipt {
		return false
	}
	if ev.Outcome == nil {
		return false
	}
	if ev.Status == "succeeded" && ev.Outcome.Compute.Result != "succeeded" {
		return false
	}
	return true
}

// capture grabs everything we'd want for post-mortem debugging:
// burst node status + events + pod status + last 80 lines of pod
// stdout/stderr. Called for non-Succeeded terminal phases (including
// Timeout) BEFORE the cleanup defer deletes the Job/CR. Best-effort:
// any individual fetch error is appended as a note so the operator
// knows why a section is missing rather than getting a blank line.
func (r *Runner) capture(ctx context.Context, workloadName string, burstID string) string {
	// Bound capture to 30s so a flaky API server can't extend the run.
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var b strings.Builder

	// 1. Burst node — labels, taints, conditions, recent events.
	if burstID != "" {
		suffix := burstNodeSuffix(burstID)
		nodes, _ := r.K8s.CoreV1().Nodes().List(cctx, metav1.ListOptions{
			LabelSelector: burstNodeSelector,
		})
		for _, n := range nodes.Items {
			if !strings.Contains(n.Name, suffix) {
				continue
			}
			b.WriteString(fmt.Sprintf("\n  node %s: ", n.Name))
			for _, c := range n.Status.Conditions {
				b.WriteString(fmt.Sprintf("%s=%s ", c.Type, c.Status))
			}
			if len(n.Spec.Taints) > 0 {
				b.WriteString("\n    taints:")
				for _, t := range n.Spec.Taints {
					b.WriteString(fmt.Sprintf(" %s=%s:%s", t.Key, t.Value, t.Effect))
				}
			}
			b.WriteString("\n")
		}
	}

	// 2. Pods owned by the workload's Job. Pods are labelled
	//    yscale.sh/workload=<CR name> by pkg/workload/translate.go.
	pods, err := r.K8s.CoreV1().Pods(r.Namespace).List(cctx, metav1.ListOptions{
		LabelSelector: "yscale.sh/workload=" + workloadName,
	})
	if err != nil {
		b.WriteString(fmt.Sprintf("  pod list failed: %v\n", err))
		return b.String()
	}
	if len(pods.Items) == 0 {
		b.WriteString("  no pods found for this workload\n")
		return b.String()
	}
	for _, p := range pods.Items {
		b.WriteString(fmt.Sprintf("\n  pod %s phase=%s node=%s\n", p.Name, p.Status.Phase, p.Spec.NodeName))
		for _, cs := range p.Status.ContainerStatuses {
			if t := cs.State.Terminated; t != nil {
				b.WriteString(fmt.Sprintf("    container %s: reason=%s exit=%d", cs.Name, t.Reason, t.ExitCode))
				if t.Message != "" {
					b.WriteString(fmt.Sprintf(" msg=%q", t.Message))
				}
				b.WriteString("\n")
			} else if w := cs.State.Waiting; w != nil {
				b.WriteString(fmt.Sprintf("    container %s waiting: reason=%s msg=%q\n", cs.Name, w.Reason, w.Message))
			}
		}
		// Logs — last 80 lines is usually enough for the failure root cause
		// without flooding the report.
		tail := int64(80)
		logBytes, err := r.K8s.CoreV1().Pods(r.Namespace).
			GetLogs(p.Name, &corev1.PodLogOptions{TailLines: &tail}).
			DoRaw(cctx)
		if err != nil {
			b.WriteString(fmt.Sprintf("    logs unavailable: %v\n", err))
		} else if len(logBytes) > 0 {
			b.WriteString("    --- pod logs (tail 80) ---\n")
			for _, line := range strings.Split(strings.TrimRight(string(logBytes), "\n"), "\n") {
				b.WriteString("    | " + line + "\n")
			}
		}

		// Pod events (FailedScheduling, ImagePullBackOff, etc).
		evs, _ := r.K8s.CoreV1().Events(r.Namespace).List(cctx, metav1.ListOptions{
			FieldSelector: "involvedObject.name=" + p.Name + ",involvedObject.namespace=" + r.Namespace,
		})
		if len(evs.Items) > 0 {
			b.WriteString("    --- pod events ---\n")
			for _, e := range evs.Items {
				b.WriteString(fmt.Sprintf("    | %s/%s: %s\n", e.Type, e.Reason, e.Message))
			}
		}
	}
	return b.String()
}

// specGPUCount reads the requested GPU count from a Workload CR's spec.
// Returns 0 for CPU-only cases.
//
// sigs.k8s.io/yaml decodes YAML integers as float64 in map[string]any,
// so NestedInt64 returns (0, false) for YAML-decoded specs. We check
// the raw value type to handle both representations.
func specGPUCount(obj *unstructured.Unstructured) int64 {
	gpu, found, _ := unstructured.NestedMap(obj.Object, "spec", "gpu")
	if !found || gpu == nil {
		return 0
	}
	count, found, _ := unstructured.NestedInt64(obj.Object, "spec", "gpu", "count")
	if found && count > 0 {
		return count
	}
	if v, ok := gpu["count"]; ok {
		switch n := v.(type) {
		case float64:
			if n > 0 {
				return int64(n)
			}
		case int:
			if n > 0 {
				return int64(n)
			}
		}
	}
	return 1
}

// verifyGPU checks that a GPU workload actually received and used GPU
// resources on its burst node. It fails closed on every check.
func (r *Runner) verifyGPU(ctx context.Context, workloadName, burstID string, wantGPU int64) error {
	if burstID == "" {
		return fmt.Errorf("burstID is empty; GPU verification requires a nonempty burstID")
	}
	vctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pods, err := r.K8s.CoreV1().Pods(r.Namespace).List(vctx, metav1.ListOptions{
		LabelSelector: "yscale.sh/workload=" + workloadName,
	})
	if err != nil {
		return fmt.Errorf("list pods: %w", err)
	}
	var pod corev1.Pod
	if len(pods.Items) > 0 {
		pod = pods.Items[0]
	} else if r.gpuPodSnapshot != nil &&
		r.gpuPodSnapshot.Labels["yscale.sh/workload"] == workloadName {
		pod = *r.gpuPodSnapshot.DeepCopy()
	} else {
		return fmt.Errorf("no pods found for workload %s (and no retained exact pod snapshot)", workloadName)
	}

	// 1. Pod must have run on the exact burst node.
	expectedName := burstNodeExpectedName(burstID)
	if expectedName == "" {
		return fmt.Errorf("burstID %q does not derive a valid node name", burstID)
	}
	if pod.Spec.NodeName == "" {
		return fmt.Errorf("pod was never scheduled to a node")
	}
	if pod.Spec.NodeName != expectedName {
		return fmt.Errorf("pod ran on %q, want exact burst node %q", pod.Spec.NodeName, expectedName)
	}

	// 2. Find the GPU container. Kubernetes defaults an omitted extended-
	//    resource request from its limit, so use that as the effective request;
	//    when both are explicit, require both to cover the requested count.
	gpuContainerName := ""
	for _, c := range pod.Spec.Containers {
		gpuResource := corev1.ResourceName("nvidia.com/gpu")
		limit, hasLimit := c.Resources.Limits[gpuResource]
		if !hasLimit || limit.Value() < wantGPU {
			continue
		}
		request, hasRequest := c.Resources.Requests[gpuResource]
		if !hasRequest {
			request = limit
		}
		if request.Value() >= wantGPU {
			gpuContainerName = c.Name
			break
		}
	}
	if gpuContainerName == "" {
		return fmt.Errorf("no container has effective nvidia.com/gpu request and limit >= %d", wantGPU)
	}

	// 3. Node GPU checks: use the retained snapshot if the node is already
	//    gone (fast teardown); otherwise read live state. Both paths enforce
	//    positive allocatable, no gpu-not-ready taint, AND
	//    nvidia.com/gpu.present exactly "true" — the last is what proves the
	//    agent's NodeWatcher reconciled the label atomically with removing
	//    the taint.
	if r.gpuNodeSnapshot != nil && r.gpuNodeSnapshot.NodeName == expectedName {
		if !r.gpuNodeSnapshot.GPUPresentLabel {
			return fmt.Errorf("retained node snapshot %s does not carry %s=true",
				expectedName, yscalelabels.LabelNvidiaGPUPresent)
		}
		if r.gpuNodeSnapshot.GPUAllocatable < wantGPU {
			return fmt.Errorf("retained node snapshot %s allocatable nvidia.com/gpu=%d, want >= %d",
				expectedName, r.gpuNodeSnapshot.GPUAllocatable, wantGPU)
		}
		if r.gpuNodeSnapshot.GPUProductLabel == "" {
			return fmt.Errorf("retained node snapshot %s does not carry %s",
				expectedName, yscalelabels.LabelNvidiaGPUProduct)
		}
	} else {
		node, err := r.K8s.CoreV1().Nodes().Get(vctx, pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get burst node %s: %w (and no retained snapshot)", pod.Spec.NodeName, err)
		}
		nodeGPU := node.Status.Allocatable[corev1.ResourceName("nvidia.com/gpu")]
		if nodeGPU.Value() < wantGPU {
			return fmt.Errorf("node %s allocatable nvidia.com/gpu=%d, want >= %d",
				pod.Spec.NodeName, nodeGPU.Value(), wantGPU)
		}
		for _, t := range node.Spec.Taints {
			if t.Key == "nvidia.com/gpu-not-ready" {
				return fmt.Errorf("node %s still carries nvidia.com/gpu-not-ready taint", pod.Spec.NodeName)
			}
		}
		if node.Labels[yscalelabels.LabelNvidiaGPUPresent] != "true" {
			return fmt.Errorf("node %s does not carry %s=true",
				pod.Spec.NodeName, yscalelabels.LabelNvidiaGPUPresent)
		}
		product := node.Labels[yscalelabels.LabelNvidiaGPUProduct]
		if product == "" || yscalelabels.ValidateLabelValue(product) != nil {
			return fmt.Errorf("node %s does not carry a valid %s label",
				pod.Spec.NodeName, yscalelabels.LabelNvidiaGPUProduct)
		}
	}

	// 4. The GPU container must have terminated with exit code 0.
	gpuExitOK := false
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == gpuContainerName {
			if t := cs.State.Terminated; t != nil && t.ExitCode == 0 {
				gpuExitOK = true
			}
			break
		}
	}
	if !gpuExitOK {
		return fmt.Errorf("GPU container %q did not terminate with exit code 0", gpuContainerName)
	}

	// 5. Pod logs from the GPU container must contain NVIDIA-SMI output.
	tail := int64(200)
	logBytes, err := r.podLogReader()(vctx, r.K8s, r.Namespace, pod.Name, gpuContainerName, tail)
	if err != nil {
		if cached := r.cachedLogs(); cached != "" {
			logBytes = []byte(cached)
		} else {
			return fmt.Errorf("read pod logs for container %q: %w (no cached stream logs available)", gpuContainerName, err)
		}
	}
	if !strings.Contains(string(logBytes), "NVIDIA-SMI") {
		return fmt.Errorf("pod logs do not contain NVIDIA-SMI output")
	}

	return nil
}

// parseLinodeProviderID extracts the numeric instance ID from a Kubernetes
// Node spec.providerID of the form "linode://12345".
func parseLinodeProviderID(providerID string) int {
	const prefix = "linode://"
	if !strings.HasPrefix(providerID, prefix) {
		return 0
	}
	id, err := strconv.Atoi(strings.TrimPrefix(providerID, prefix))
	if err != nil || id <= 0 {
		return 0
	}
	return id
}

// burstTailscaleHostname derives the exact Tailscale device hostname for
// a burst using the same convention as the decider's tsHostnameFor.
func burstTailscaleHostname(burstID string) string {
	if burstID == "" {
		return ""
	}
	return "yscale-" + strings.ReplaceAll(burstID, "_", "-")
}

// cleanup deletes only the Workload CR we created. Per
// cleanup_safety_directive: no broad sweeps. Uses background ctx so
// it runs even on interrupt.
func (r *Runner) cleanup(ctx context.Context, name string) {
	if err := r.deleteWorkloadCR(ctx, name); err != nil {
		fmt.Fprintf(os.Stderr, "WARN cleanup: delete %s: %v\n", name, err)
	}
}

func (r *Runner) deleteWorkloadCR(ctx context.Context, name string) error {
	delCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	bg := metav1.DeletePropagationBackground
	err := r.Dyn.Resource(workloadGVR).Namespace(r.Namespace).
		Delete(delCtx, name, metav1.DeleteOptions{PropagationPolicy: &bg})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
