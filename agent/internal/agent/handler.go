package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/yscale-sh/yscale/pkg/protocol"
)

// RealHandler is the production Handler: applies CreateJob /
// DeleteJob via the Kubernetes API, and on BurstAnnounce records the
// per-burst storage bindings + authorizes the burst with the
// BootstrapServer so its bootstrap call can mint a kubelet kubeconfig.
//
// With the Tailscale-based SaaS path, burst-side network bring-up is
// the burst's responsibility (it joins the tailnet via its ephemeral
// auth key); the agent no longer manages WG peers.
//
// DrainNode cordons the node and evicts non-DaemonSet pods using the
// Eviction API (respects PodDisruptionBudgets), falling back to a
// direct Pod delete if eviction is still blocked at deadline.
type RealHandler struct {
	K8s          kubernetes.Interface
	Bootstrap    *BootstrapServer // nil disables token authorization
	Log          *slog.Logger
	DrainTimeout time.Duration // 0 → 60s default
	// WorkloadNamespace scopes the drain path's pod list to one namespace.
	// Empty (default) drains every namespace, which needs a cluster-wide pod
	// read. A hosted connector's pod grant is a Role in its tenant namespace,
	// so an unscoped list there is forbidden at the cluster scope and the
	// drain fails before it evicts anything. Mirrors -workload-namespace.
	WorkloadNamespace string
	// PodInventoryNamespaces is the explicit namespaced-RBAC allowlist used
	// when WorkloadNamespace is empty but the connector cannot list Pods at
	// cluster scope. Empty means a cluster-wide list.
	PodInventoryNamespaces []string
	// logStream is a test seam around the pods/log streaming request. nil uses
	// the configured Kubernetes client.
	logStream func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error)
}

// ReadableNamespaces is where a connector may list and watch namespaced objects
// (Jobs, Pods): its pinned workload namespace, else the namespaced-RBAC
// allowlist, else "" for the whole cluster. The chart passes the allowlist
// exactly when its Roles are namespaced, so an unscoped call there is forbidden
// and every watcher built on one silently sees nothing.
func ReadableNamespaces(workloadNamespace string, allowlist []string) []string {
	if workloadNamespace != "" {
		return []string{workloadNamespace}
	}
	if len(allowlist) == 0 {
		return []string{""}
	}
	return allowlist
}

func (h *RealHandler) openLogStream(ctx context.Context, namespace, pod string, options *corev1.PodLogOptions) (io.ReadCloser, error) {
	if h.logStream != nil {
		return h.logStream(ctx, namespace, pod, options)
	}
	return h.K8s.CoreV1().Pods(namespace).GetLogs(pod, options).Stream(ctx)
}

// NewRealHandler wires a real K8s client and (optionally) a
// BootstrapServer. Any dependency may be nil for partial setups; the
// corresponding callback returns a clear error rather than panicking.
func NewRealHandler(k8s kubernetes.Interface, boot *BootstrapServer, log *slog.Logger) *RealHandler {
	if log == nil {
		log = slog.Default()
	}
	return &RealHandler{K8s: k8s, Bootstrap: boot, Log: log}
}

// GPUTelemetrySamples delegates to the BootstrapServer's GPU sample cache.
func (h *RealHandler) GPUTelemetrySamples() []protocol.GPUTelemetrySample {
	if h == nil || h.Bootstrap == nil {
		return nil
	}
	return h.Bootstrap.GPUTelemetrySamples()
}

// drainTimeout returns the configured drain timeout, defaulting to 60s.
func (h *RealHandler) drainTimeout() time.Duration {
	if h.DrainTimeout > 0 {
		return h.DrainTimeout
	}
	return 60 * time.Second
}

// evictionPollInterval is how often we retry an eviction blocked by
// a PDB before falling back to direct delete.
const evictionPollInterval = 2 * time.Second

const (
	defaultLogTailLines int64 = 200
	maxLogTailLines     int64 = 1000
	maxWorkloadLogBytes int64 = 256 << 10
	burstNodeLabel            = "yscale.sh/burst-node"
)

// ObserveClusterInventory returns this connector's current visible cluster
// inventory for the heartbeat. Node and pod observations are independent:
// callers should use any observed scope even when the other scope failed.
func (h *RealHandler) ObserveClusterInventory(ctx context.Context) (protocol.Heartbeat, error) {
	if h == nil || h.K8s == nil {
		return protocol.Heartbeat{}, errors.New("kubernetes client not configured")
	}

	var (
		hb   protocol.Heartbeat
		errs []error
	)
	nodes, err := h.K8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{ResourceVersion: "0"})
	if err != nil {
		errs = append(errs, fmt.Errorf("list nodes: %w", err))
	} else {
		hb.NodeInventoryObserved = true
		observedAt := time.Now().UTC()
		hb.NodeInventoryObservedAt = &observedAt
		hb.NodeCount = len(nodes.Items)
		for i := range nodes.Items {
			if nodes.Items[i].Labels[burstNodeLabel] == "true" {
				hb.BurstCount++
			}
		}
	}

	podNamespaces := ReadableNamespaces(h.WorkloadNamespace, h.PodInventoryNamespaces)
	pendingPods := 0
	podsObserved := true
	for _, namespace := range podNamespaces {
		pods, listErr := h.K8s.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			FieldSelector:   fields.OneTermEqualSelector("status.phase", string(corev1.PodPending)).String(),
			ResourceVersion: "0",
		})
		if listErr != nil {
			errs = append(errs, fmt.Errorf("list pending pods in namespace %q: %w", namespace, listErr))
			podsObserved = false
			continue
		}
		pendingPods += len(pods.Items)
	}
	if podsObserved {
		hb.PodInventoryObserved = true
		observedAt := time.Now().UTC()
		hb.PodInventoryObservedAt = &observedAt
		hb.PendingPods = pendingPods
	}
	hb.InventoryObserved = hb.NodeInventoryObserved && hb.PodInventoryObserved
	return hb, errors.Join(errs...)
}

func (h *RealHandler) OnBurstAnnounce(_ context.Context, c protocol.BurstAnnounce) error {
	if h.Bootstrap != nil {
		// Authorize the BurstID so the burst can later fetch its
		// kubelet bootstrap kubeconfig from the agent, and remember the
		// tier, the workload namespace storage credentials may be read
		// from, and the storage bindings the burst receives in that
		// response.
		h.Bootstrap.AuthorizeBurst(c.BurstID, c.Tier, c.Namespace, c.Storage)
	}
	if c.Namespace == "" && len(c.Storage) > 0 {
		h.Log.Warn("burst announced with storage bindings but no namespace; storage signing will be refused for it (central predates namespace-scoped signing)",
			"burst", c.BurstID)
	}
	h.Log.Info("announced burst", "burst", c.BurstID, "ts_hostname", c.TSHostname, "tier", c.Tier, "namespace", c.Namespace, "storage_bindings", len(c.Storage))
	return nil
}

func (h *RealHandler) OnCreateJob(ctx context.Context, c protocol.CreateJob) error {
	if h.K8s == nil {
		return fmt.Errorf("kubernetes client not configured")
	}
	var job batchv1.Job
	if err := json.Unmarshal(c.JobSpec, &job); err != nil {
		return fmt.Errorf("decode job spec: %w", err)
	}
	// The central server stamps Namespace separately; honor it over
	// whatever's embedded in the rendered Job (defense against drift).
	if c.Namespace != "" {
		job.Namespace = c.Namespace
	}
	// Tag the Job so we can find it later via the workload selector
	// without trusting metadata.name.
	if job.Labels == nil {
		job.Labels = map[string]string{}
	}
	job.Labels[labelWorkloadID] = c.WorkloadID
	if job.Spec.Template.Labels == nil {
		job.Spec.Template.Labels = map[string]string{}
	}
	job.Spec.Template.Labels[labelWorkloadID] = c.WorkloadID

	created, err := h.K8s.BatchV1().Jobs(job.Namespace).Create(ctx, &job, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			// A Job with this name already exists. Same workload-id =
			// legit re-delivery (central re-sends on agent reconnect) —
			// skip. Different id = stale Job from a prior workload of
			// the same name; replace it, or the completion watcher
			// reports the stale workload and the burst never reaps.
			existing, getErr := h.K8s.BatchV1().Jobs(job.Namespace).Get(ctx, job.Name, metav1.GetOptions{})
			if getErr == nil && existing.Labels[labelWorkloadID] == c.WorkloadID {
				h.Log.Info("create_job: already exists (idempotent)", "workload", c.WorkloadID, "namespace", job.Namespace)
				return nil
			}
			h.Log.Info("create_job: replacing stale job", "workload", c.WorkloadID, "namespace", job.Namespace, "name", job.Name)
			fg := metav1.DeletePropagationForeground
			if delErr := h.K8s.BatchV1().Jobs(job.Namespace).Delete(ctx, job.Name, metav1.DeleteOptions{PropagationPolicy: &fg}); delErr != nil && !apierrors.IsNotFound(delErr) {
				return fmt.Errorf("deleting stale job %s/%s: %w", job.Namespace, job.Name, delErr)
			}
			for i := 0; i < 30; i++ {
				if _, e := h.K8s.BatchV1().Jobs(job.Namespace).Get(ctx, job.Name, metav1.GetOptions{}); apierrors.IsNotFound(e) {
					break
				}
				time.Sleep(time.Second)
			}
			created, err = h.K8s.BatchV1().Jobs(job.Namespace).Create(ctx, &job, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("recreating job %s/%s: %w", job.Namespace, job.Name, err)
			}
			h.Log.Info("created job (replaced stale)", "workload", c.WorkloadID, "namespace", created.Namespace, "name", created.Name)
			return nil
		}
		return fmt.Errorf("create job %s/%s: %w", job.Namespace, job.Name, err)
	}
	h.Log.Info("created job", "workload", c.WorkloadID, "namespace", created.Namespace, "name", created.Name)
	return nil
}

func (h *RealHandler) OnSyncRuntimeBindings(ctx context.Context, c protocol.SyncRuntimeBindings) error {
	if h.K8s == nil {
		return fmt.Errorf("kubernetes client not configured")
	}
	if err := protocol.ValidateSyncRuntimeBindings(c); err != nil {
		return err
	}
	if h.WorkloadNamespace != "" && c.Namespace != h.WorkloadNamespace {
		return fmt.Errorf("runtime binding namespace is outside connector scope")
	}
	secretAPI := h.K8s.CoreV1().Secrets(c.Namespace)
	current, err := secretAPI.Get(ctx, protocol.RuntimeBindingsSecretName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// Helm and Flux do not guarantee that the pre-created Secret is
		// observed before the restarted Deployment connects. Give that
		// GitOps-owned prerequisite a bounded window to appear; the agent
		// intentionally has no create permission for Secrets.
		deadline := time.NewTimer(30 * time.Second)
		defer deadline.Stop()
		retry := time.NewTicker(250 * time.Millisecond)
		defer retry.Stop()
		for apierrors.IsNotFound(err) {
			select {
			case <-ctx.Done():
				return fmt.Errorf("wait for runtime bindings secret: %w", ctx.Err())
			case <-deadline.C:
				return fmt.Errorf("get runtime bindings secret: %w", err)
			case <-retry.C:
				current, err = secretAPI.Get(ctx, protocol.RuntimeBindingsSecretName, metav1.GetOptions{})
			}
		}
	}
	if err != nil {
		return fmt.Errorf("get runtime bindings secret: %w", err)
	}
	if raw := current.Annotations["yscale.sh/runtime-bindings-revision"]; raw != "" {
		applied, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil || applied < 0 || applied > protocol.MaxRuntimeBindingRevision {
			return fmt.Errorf("runtime bindings secret has invalid revision")
		}
		if c.Revision < applied {
			h.Log.Info("ignored stale runtime bindings", "namespace", c.Namespace, "revision", c.Revision, "applied_revision", applied)
			return nil
		}
	}
	next := current.DeepCopy()
	next.Data = make(map[string][]byte, len(c.Bindings))
	for key, value := range c.Bindings {
		next.Data[key] = []byte(value)
	}
	if next.Annotations == nil {
		next.Annotations = map[string]string{}
	}
	next.Annotations["yscale.sh/runtime-bindings-revision"] = fmt.Sprintf("%d", c.Revision)
	if _, err := secretAPI.Update(ctx, next, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update runtime bindings secret: %w", err)
	}
	h.Log.Info("synced runtime bindings", "namespace", c.Namespace, "revision", c.Revision, "keys", protocol.RuntimeBindingKeys(c.Bindings))
	return nil
}

func (h *RealHandler) OnDeleteJob(ctx context.Context, c protocol.DeleteJob) error {
	if h.K8s == nil {
		return fmt.Errorf("kubernetes client not configured")
	}
	// Foreground deletion so the Pods owned by the Job get cleaned
	// up alongside the Job itself.
	policy := metav1.DeletePropagationForeground
	jobs, err := h.K8s.BatchV1().Jobs(c.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "yscale.sh/workload-id=" + c.WorkloadID,
	})
	if err != nil {
		return fmt.Errorf("list jobs for workload %s: %w", c.WorkloadID, err)
	}
	if len(jobs.Items) == 0 {
		h.Log.Info("delete_job: no matching job (already gone?)", "workload", c.WorkloadID)
		return nil
	}
	for i := range jobs.Items {
		j := &jobs.Items[i]
		err := h.K8s.BatchV1().Jobs(j.Namespace).Delete(ctx, j.Name, metav1.DeleteOptions{
			PropagationPolicy: &policy,
		})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete job %s/%s: %w", j.Namespace, j.Name, err)
		}
		h.Log.Info("deleted job", "workload", c.WorkloadID, "namespace", j.Namespace, "name", j.Name)
	}
	return nil
}

// OnFetchWorkloadLogs returns a bounded snapshot for Jobs stamped with the
// server-minted workload id. The command names no pod or container, so a
// caller cannot turn this into a general Kubernetes log reader.
func (h *RealHandler) OnFetchWorkloadLogs(ctx context.Context, c protocol.FetchWorkloadLogs) (protocol.WorkloadLogs, error) {
	result := protocol.WorkloadLogs{ObservedAt: time.Now().UTC(), Streams: []protocol.WorkloadLogStream{}}
	if h.K8s == nil {
		return result, fmt.Errorf("kubernetes client not configured")
	}
	if c.WorkloadID == "" || c.Namespace == "" {
		return result, fmt.Errorf("workload id and namespace required")
	}
	tail := c.TailLines
	if tail <= 0 {
		tail = defaultLogTailLines
	}
	if tail > maxLogTailLines {
		tail = maxLogTailLines
	}
	limit := c.MaxBytes
	if limit <= 0 || limit > maxWorkloadLogBytes {
		limit = maxWorkloadLogBytes
	}

	jobs, err := h.K8s.BatchV1().Jobs(c.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{labelWorkloadID: c.WorkloadID}.AsSelector().String(),
	})
	if err != nil {
		return result, fmt.Errorf("list workload jobs: %w", err)
	}
	sort.Slice(jobs.Items, func(i, j int) bool {
		if jobs.Items[i].CreationTimestamp.Equal(&jobs.Items[j].CreationTimestamp) {
			return jobs.Items[i].Name < jobs.Items[j].Name
		}
		return jobs.Items[i].CreationTimestamp.Before(&jobs.Items[j].CreationTimestamp)
	})

	var used int64
	for i := range jobs.Items {
		job := &jobs.Items[i]
		pods, err := h.K8s.CoreV1().Pods(c.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels.Set{"batch.kubernetes.io/job-name": job.Name}.AsSelector().String(),
		})
		if err != nil {
			return result, fmt.Errorf("list workload pods: %w", err)
		}
		sort.Slice(pods.Items, func(i, j int) bool {
			if pods.Items[i].CreationTimestamp.Equal(&pods.Items[j].CreationTimestamp) {
				return pods.Items[i].Name < pods.Items[j].Name
			}
			return pods.Items[i].CreationTimestamp.Before(&pods.Items[j].CreationTimestamp)
		})
		for p := range pods.Items {
			pod := &pods.Items[p]
			for _, container := range pod.Spec.Containers {
				if used >= limit {
					result.Truncated = true
					return result, nil
				}
				stream, err := h.openLogStream(ctx, c.Namespace, pod.Name, &corev1.PodLogOptions{
					Container:  container.Name,
					TailLines:  &tail,
					Timestamps: true,
				})
				if err != nil {
					return result, fmt.Errorf("read workload logs: %w", err)
				}
				remaining := limit - used
				body, readErr := io.ReadAll(io.LimitReader(stream, remaining+1))
				closeErr := stream.Close()
				if readErr != nil {
					return result, fmt.Errorf("read workload logs: %w", readErr)
				}
				if closeErr != nil {
					return result, fmt.Errorf("close workload logs: %w", closeErr)
				}
				if int64(len(body)) > remaining {
					body = body[:remaining]
					result.Truncated = true
				}
				used += int64(len(body))
				result.Streams = append(result.Streams, protocol.WorkloadLogStream{
					Pod:       pod.Name,
					Container: container.Name,
					Output:    string(body),
				})
				if result.Truncated {
					return result, nil
				}
			}
		}
	}
	return result, nil
}

func (h *RealHandler) OnDrainNode(ctx context.Context, c protocol.DrainNode) error {
	// Delete=true means burst teardown: revoke the burst's bootstrap
	// grant first so its (now-orphaned) BurstID can't keep minting
	// kubelet tokens. Done up front — it must happen even when the
	// Node object is already gone, and it's idempotent on retry.
	if c.Delete && h.Bootstrap != nil {
		h.Bootstrap.ForgetNode(c.NodeName)
	}
	if h.K8s == nil {
		return fmt.Errorf("kubernetes client not configured")
	}
	return h.drainAndDeleteNode(ctx, c.NodeName, c.Delete)
}

// OnPrepareIdleTeardown answers central's last question before it destroys an
// idle burst node: is the node STILL empty, and can it stay that way? It is not
// a drain — it evicts nothing and deletes nothing.
//
// Cordon first, list second. A list taken before the cordon describes an instant
// the scheduler was still free to bind into, which is exactly the race this
// closes — the connector's idle sweep already took one of those, and central is
// acting on it minutes later.
//
// Cluster-wide visibility is required for IdleNodeWatcher's reason: a
// namespace-scoped connector cannot tell an empty node from one busy with
// another namespace's pods, and there is no partial version of that check.
//
// The node is left CORDONED on success. Central claims and reaps it next, and an
// uncordon in between would reopen the window this whole command exists to shut.
//
// That cordon therefore outlives this command, so the acknowledgement carries the
// resourceVersion it produced. It remains a safe compatibility token for an
// explicit ReleaseIdleTeardown, though current central keeps the cordon while a
// pending reap is retried. It is empty when this command placed no cordon at all.
func (h *RealHandler) OnPrepareIdleTeardown(ctx context.Context, c protocol.PrepareIdleTeardown) (protocol.IdleTeardownPreflight, error) {
	var result protocol.IdleTeardownPreflight
	if h.K8s == nil {
		return result, fmt.Errorf("kubernetes client not configured")
	}
	nodeName := c.NodeName
	if h.WorkloadNamespace != "" {
		return result, fmt.Errorf("connector is scoped to namespace %s and cannot confirm %s is idle cluster-wide",
			h.WorkloadNamespace, nodeName)
	}
	node, err := h.K8s.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return result, fmt.Errorf("get node %s: %w", nodeName, err)
	}
	// The version this command's OWN cordon produced. Rolling back is only ever
	// allowed against exactly that version — see rollBackPreflightCordon. Empty
	// means the node was already unschedulable and nothing here may undo it.
	cordonedRV := ""
	if !node.Spec.Unschedulable {
		node.Spec.Unschedulable = true
		updated, err := h.K8s.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
		if err != nil {
			return result, fmt.Errorf("cordon node %s: %w", nodeName, err)
		}
		cordonedRV = updated.ResourceVersion
	}
	pods, err := h.K8s.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		// Unknown is not empty, the same rule the idle sweep follows.
		h.rollBackPreflightCordon(ctx, nodeName, cordonedRV)
		return result, fmt.Errorf("list pods on %s: %w", nodeName, err)
	}
	for i := range pods.Items {
		if podOccupiesNode(&pods.Items[i]) {
			h.rollBackPreflightCordon(ctx, nodeName, cordonedRV)
			return result, fmt.Errorf("node %s is not idle: pod %s/%s is on it",
				nodeName, pods.Items[i].Namespace, pods.Items[i].Name)
		}
	}
	h.Log.Info("idle teardown preflight: node confirmed empty, left cordoned", "node", nodeName)
	result.CordonResourceVersion = cordonedRV
	return result, nil
}

// OnReleaseIdleTeardown conditionally gives back the cordon one preflight placed.
//
// The preflight deliberately leaves the node cordoned so central can claim and
// reap behind it. Current central keeps that cordon while a pending reap is
// retried; this compatibility command remains available only when the teardown
// has been cancelled or an operator has otherwise decided the node should stay.
// The idle watcher never uncordons on its own, because a watcher that uncordons
// what it finds would fight every operator in the cluster.
//
// It undoes exactly ONE cordon: the one whose resourceVersion central was handed
// in the preflight's acknowledgement. Anything else — a version that has moved
// on, a node somebody cordoned since, a node already schedulable — is not this
// request's to reverse. It evicts nothing.
func (h *RealHandler) OnReleaseIdleTeardown(ctx context.Context, c protocol.ReleaseIdleTeardown) error {
	if h.K8s == nil {
		return fmt.Errorf("kubernetes client not configured")
	}
	if c.CordonResourceVersion == "" {
		// Without the version there is no way to tell this request's cordon from an
		// operator's, and an uncordon that guesses puts work back on a node somebody
		// deliberately emptied.
		return fmt.Errorf("release for %s names no preflight cordon to undo", c.NodeName)
	}
	return h.releasePreflightCordon(ctx, c.NodeName, c.CordonResourceVersion)
}

// rollBackPreflightCordon undoes ONLY a cordon the preflight itself added, and
// only if the node has not been touched since.
//
// cordonedRV is the resourceVersion this command's own cordon update returned;
// empty means the node was already unschedulable and belongs to something else —
// an operator, a drain in flight — so uncordoning it would put work back on a
// node somebody deliberately emptied.
//
// The undo is one JSON Patch whose `test` ops ARE the precondition: the node is
// still at that exact version, and it is still unschedulable. A GET followed by
// an UPDATE cannot express that — between the two, another drain can cordon the
// node for its own reasons, and the rollback then silently reverses a decision
// that has nothing to do with this preflight. So a failed precondition is never
// retried and never written past: the node stays cordoned, which costs
// scheduling capacity, where the other mistake puts pods onto a node being
// drained.
func (h *RealHandler) rollBackPreflightCordon(ctx context.Context, nodeName, cordonedRV string) {
	if cordonedRV == "" {
		return
	}
	if err := h.releasePreflightCordon(ctx, nodeName, cordonedRV); err != nil {
		h.Log.Warn("idle teardown preflight: cordon not rolled back; the node stays unschedulable until something uncordons it",
			"node", nodeName, "resource_version", cordonedRV, "error", err)
	}
}

// releasePreflightCordon is the one conditional undo both paths share — the
// preflight's own rollback and central's later release of a cordon whose reap
// never happened.
//
// The `test` ops ARE the precondition, and they carry the whole ownership
// argument: exactly the version this cordon produced, and still unschedulable.
// The patch is not retried and its failure is not resolved by writing anyway.
//
// A failed patch is ambiguous — the node may have been changed by somebody else,
// or the write may simply not have landed — so it is resolved by LOOKING, never
// by a second write. Success means the node is no longer stranded by this cordon:
// released here, released already, or gone. An error means it may still be
// cordoned and this call did not take it back, which is what the caller reports.
func (h *RealHandler) releasePreflightCordon(ctx context.Context, nodeName, cordonedRV string) error {
	patch := []byte(fmt.Sprintf(
		`[{"op":"test","path":"/metadata/resourceVersion","value":%q},`+
			`{"op":"test","path":"/spec/unschedulable","value":true},`+
			`{"op":"replace","path":"/spec/unschedulable","value":false}]`, cordonedRV))
	_, err := h.K8s.CoreV1().Nodes().Patch(
		ctx, nodeName, types.JSONPatchType, patch, metav1.PatchOptions{},
	)
	if err == nil {
		h.Log.Info("idle teardown cordon released", "node", nodeName, "resource_version", cordonedRV)
		return nil
	}
	if apierrors.IsNotFound(err) {
		// The node is gone. Whatever destroyed it took the cordon with it.
		return nil
	}
	node, getErr := h.K8s.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(getErr):
		return nil
	case getErr != nil:
		return fmt.Errorf("release cordon on %s: %w (the node could not be re-read: %v)", nodeName, err, getErr)
	case !node.Spec.Unschedulable:
		// Already schedulable — this cordon is not what is holding the node, and a
		// repeat of this command lands here. Nothing to give back.
		return nil
	}
	// Still cordoned, and this call is not entitled to change that: either the node
	// moved on under a decision of somebody else's, or the write failed. Both leave
	// the node unschedulable, which costs scheduling capacity, where writing anyway
	// puts pods onto a node somebody is draining.
	return fmt.Errorf("cordon on %s not released and the node is still unschedulable: %w", nodeName, err)
}

// drainAndDeleteNode cordons the node, evicts non-DaemonSet pods
// (falling back to direct delete on PDB timeout), and — when
// deleteNode is true — removes the Node object itself. This shared
// helper is called by both OnDrainNode (WS-driven teardown) and
// NodeGC (periodic ghost-node cleanup).
func (h *RealHandler) drainAndDeleteNode(ctx context.Context, nodeName string, deleteNode bool) error {
	node, err := h.K8s.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			h.Log.Info("drain_node: node already gone", "node", nodeName)
			return nil
		}
		return fmt.Errorf("get node %s: %w", nodeName, err)
	}
	if !node.Spec.Unschedulable {
		node.Spec.Unschedulable = true
		if _, err := h.K8s.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("cordon node %s: %w", nodeName, err)
		}
	}
	// Only where this connector may list pods: an unscoped list under the
	// namespaced default is forbidden, and the drain then failed before it
	// evicted anything and left the Node object behind (seen live 2026-10-04).
	var onNode []corev1.Pod
	for _, namespace := range ReadableNamespaces(h.WorkloadNamespace, h.PodInventoryNamespaces) {
		pods, err := h.K8s.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=" + nodeName,
		})
		if err != nil {
			return fmt.Errorf("list pods on %s: %w", nodeName, err)
		}
		onNode = append(onNode, pods.Items...)
	}
	timeout := h.drainTimeout()
	for i := range onNode {
		pod := &onNode[i]
		if isDaemonSetPod(pod) {
			continue
		}
		deadline := time.Now().Add(timeout)
		evicted := false
		for time.Now().Before(deadline) {
			err := h.K8s.PolicyV1().Evictions(pod.Namespace).Evict(ctx, &policyv1.Eviction{
				ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
			})
			if err == nil || apierrors.IsNotFound(err) {
				evicted = true
				break
			}
			if apierrors.IsTooManyRequests(err) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(evictionPollInterval):
				}
				continue
			}
			h.Log.Warn("eviction failed", "pod", pod.Name, "namespace", pod.Namespace, "error", err)
			break
		}
		if !evicted {
			h.Log.Warn("eviction timeout, deleting", "pod", pod.Name, "namespace", pod.Namespace)
			if err := h.K8s.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete pod %s/%s after eviction failure: %w", pod.Namespace, pod.Name, err)
			}
		}
	}
	h.Log.Info("drained node", "node", nodeName)

	// Burst teardown: the backing VM is already destroyed, so remove
	// the Node object — k8s won't garbage-collect it on its own and it
	// would otherwise linger NotReady forever.
	if deleteNode {
		if err := h.K8s.CoreV1().Nodes().Delete(ctx, nodeName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete node %s: %w", nodeName, err)
		}
		h.Log.Info("deleted node object", "node", nodeName)
	}
	return nil
}

// isDaemonSetPod is true if the pod is owned by a DaemonSet — DS pods
// are exempt from eviction (the DS controller will recreate them on
// the cordoned node and we'd loop forever).
func isDaemonSetPod(pod *corev1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}
