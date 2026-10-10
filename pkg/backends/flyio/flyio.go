package flyio

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/internal/agentenv"
)

const machinesAPIBase = "https://api.machines.dev/v1"

type Backend struct {
	token  string
	org    string
	region string
	client *http.Client
	// appName is the single Fly app for all burst nodes.
	appName  string
	appMu    sync.Mutex
	appReady bool
	// poolScope scopes ListPooledNodes and CleanupOrphans to an exact metadata
	// match. Empty owns only legacy unscoped resources; it is not a wildcard.
	poolScopeMu sync.RWMutex
	poolScope   string
}

func New(token, org, region string) *Backend {
	return &Backend{
		token:   token,
		org:     org,
		region:  region,
		client:  &http.Client{Timeout: 30 * time.Second},
		appName: fmt.Sprintf("hs-%s-burst", org),
	}
}

func (b *Backend) Name() string { return "flyio" }

// SetPoolScope sets the exact pool scope owned by this backend instance. An
// empty scope owns only legacy unscoped machines; it never sees scoped ones.
func (b *Backend) SetPoolScope(hash string) {
	b.poolScopeMu.Lock()
	defer b.poolScopeMu.Unlock()
	b.poolScope = hash
}

func (b *Backend) currentPoolScope() string {
	b.poolScopeMu.RLock()
	defer b.poolScopeMu.RUnlock()
	return b.poolScope
}

func (b *Backend) validateNodeSpec(spec *backends.NodeSpec) error {
	if spec == nil {
		return fmt.Errorf("creating fly machine: node spec is nil")
	}
	if !strings.HasPrefix(spec.Name, backends.NodeNamePrefix) || len(spec.Name) == len(backends.NodeNamePrefix) {
		return fmt.Errorf("creating fly machine: node name %q must start with %q and include an identifier", spec.Name, backends.NodeNamePrefix)
	}
	if spec.AgentImage == "" {
		return fmt.Errorf("creating fly machine %s: agent image is required", spec.Name)
	}
	if spec.ScopeHash != b.currentPoolScope() {
		return fmt.Errorf("creating fly machine %s: scopeHash does not match backend pool scope", spec.Name)
	}
	switch spec.Lifecycle {
	case "", backends.LifecycleCold:
		return nil
	case backends.LifecyclePrewarm:
		if spec.ScopeHash == "" || spec.ConfigHash == "" {
			return fmt.Errorf("creating prewarm fly machine %s: scopeHash and configHash are required", spec.Name)
		}
		if !isDigestPinnedImage(spec.AgentImage) {
			return fmt.Errorf("creating prewarm fly machine %s: agent image must be pinned by sha256 digest", spec.Name)
		}
		return nil
	default:
		return fmt.Errorf("creating fly machine %s: unsupported lifecycle %q", spec.Name, spec.Lifecycle)
	}
}

func (b *Backend) CreateNode(ctx context.Context, spec *backends.NodeSpec) (string, error) {
	if err := b.validateNodeSpec(spec); err != nil {
		// Pre-dispatch validation: no machine can exist because we never
		// called the provider.
		return "", backends.MarkCreateProvenZeroResource(err)
	}
	if err := b.ensureApp(ctx); err != nil {
		// ensureApp may have created the reusable app, but no machine request
		// has been dispatched, so this burst has no billable node.
		return "", backends.MarkCreateProvenZeroResource(fmt.Errorf("ensuring fly app: %w", err))
	}

	// Use the shared env-var builder so every backend stays in sync.
	// agentenv handles K3s vs kubelet branching + label encoding +
	// TS_TAGS / POD_CIDR / BOOTSTRAP_ENDPOINT.
	env := agentenv.AsMap(agentenv.Build(spec))

	// volumeMountPath is backend-specific and doesn't ride in the env;
	// it's the host path we attach the persistent volume at.
	guest := mapResources(spec.Resources)

	// Metadata stamped on every Fly machine. Scope and config-hash are
	// used by the warm-pool controller for matching and scope isolation.
	// Lifecycle drives auto_destroy: prewarm machines are not auto-destroyed
	// so they can be suspended and claimed later; cold machines self-destruct.
	meta := map[string]string{
		"yscale-node":        spec.Name,
		"yscale-lifecycle":   string(spec.Lifecycle),
		"yscale-scope":       spec.ScopeHash,
		"yscale-config-hash": spec.ConfigHash,
	}
	if spec.BurstID != "" {
		meta["yscale-burst-id"] = spec.BurstID
	}

	// Prewarm machines need auto_destroy=false so they survive creation
	// and can be suspended. Cold machines auto-destroy on exit.
	autoDestroy := spec.Lifecycle != backends.LifecyclePrewarm

	req := CreateMachineRequest{
		Name:   spec.Name,
		Region: b.region,
		Config: MachineConfig{
			Image:    spec.AgentImage,
			Env:      env,
			Guest:    guest,
			Metadata: meta,
			Restart: RestartPolicy{
				Policy: "no",
			},
			AutoDestroy: autoDestroy,
		},
	}

	machine, err := b.createMachine(ctx, &req)
	if err != nil {
		// A modeled 4xx from the create endpoint proves the provider refused
		// the machine. Transport failures, 5xx, and decode errors on an
		// accepted response leave the outcome ambiguous by default — the
		// machine may already be running under the returned ID.
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && backends.HTTPCreateResponseProvesNoResource(httpErr.StatusCode) {
			return "", backends.MarkCreateProvenZeroResource(err)
		}
		return "", err
	}
	if machine == nil || machine.ID == "" {
		// A decoded response with no ID is ambiguous: the provider may have
		// accepted the create even though we cannot name the machine.
		return "", backends.MarkCreateAmbiguous(fmt.Errorf("fly create returned no machine id for %s", spec.Name))
	}
	slog.InfoContext(ctx, "fly machine created",
		"component", "flyio_backend", "operation", "create",
		"app", b.appName, "machine_id", machine.ID, "node", spec.Name,
		"region", b.region, "scope", spec.ScopeHash,
		"config_hash", spec.ConfigHash, "lifecycle", spec.Lifecycle)

	return machine.ID, nil
}

func (b *Backend) StartNode(ctx context.Context, backendID string) error {
	machine, err := b.getOwnedMachine(ctx, backendID)
	if err != nil {
		return fmt.Errorf("authorizing start of machine %s: %w", backendID, err)
	}
	lifecycle := backends.NodeLifecycle(machine.Config.Metadata["yscale-lifecycle"])
	if lifecycle == backends.LifecyclePrewarm && (b.currentPoolScope() == "" || machine.State != "suspended") {
		return fmt.Errorf("refusing to start prewarm machine %s without a non-empty scope and suspended state", backendID)
	}
	if err := b.startMachine(ctx, backendID); err != nil {
		return err
	}
	slog.InfoContext(ctx, "fly machine start accepted",
		"component", "flyio_backend", "operation", "start",
		"app", b.appName, "machine_id", backendID,
		"node", machine.Config.Metadata["yscale-node"],
		"scope", machine.Config.Metadata["yscale-scope"],
		"lifecycle", lifecycle, "provider_state", machine.State)
	return nil
}

func (b *Backend) StopNode(ctx context.Context, backendID string) error {
	machine, err := b.getOwnedMachine(ctx, backendID)
	if err != nil {
		return fmt.Errorf("authorizing stop of machine %s: %w", backendID, err)
	}
	if err := b.stopMachine(ctx, backendID); err != nil {
		return err
	}
	slog.InfoContext(ctx, "fly machine stop accepted",
		"component", "flyio_backend", "operation", "stop",
		"app", b.appName, "machine_id", backendID,
		"node", machine.Config.Metadata["yscale-node"],
		"scope", machine.Config.Metadata["yscale-scope"],
		"lifecycle", machine.Config.Metadata["yscale-lifecycle"],
		"provider_state", machine.State)
	return nil
}

// SuspendNode suspends a running Fly machine via the /suspend endpoint.
// A suspended machine retains its process state, Tailscale identity, and
// kubelet state, enabling fast resume. CPU/RAM are not billed while
// suspended, but rootfs storage is. Resume is best-effort; the caller must
// enforce TTL, ownership, provider-state waits, and cold-start fallback.
func (b *Backend) SuspendNode(ctx context.Context, backendID string) error {
	machine, err := b.getOwnedMachine(ctx, backendID)
	if err != nil {
		return fmt.Errorf("authorizing suspend of machine %s: %w", backendID, err)
	}
	if b.currentPoolScope() == "" ||
		machine.Config.Metadata["yscale-lifecycle"] != string(backends.LifecyclePrewarm) ||
		machine.Config.Metadata["yscale-config-hash"] == "" {
		return fmt.Errorf("refusing to suspend machine %s: prewarm lifecycle, non-empty scope, and config hash are required", backendID)
	}
	if err := b.suspendMachine(ctx, backendID); err != nil {
		return err
	}
	slog.InfoContext(ctx, "fly machine suspend accepted",
		"component", "flyio_backend", "operation", "suspend",
		"app", b.appName, "machine_id", backendID,
		"node", machine.Config.Metadata["yscale-node"],
		"scope", machine.Config.Metadata["yscale-scope"],
		"lifecycle", machine.Config.Metadata["yscale-lifecycle"],
		"provider_state", machine.State)
	return nil
}

// WaitNodeState confirms an exact Fly provider state. It intentionally does
// not infer whether a suspended start resumed memory or cold-booted; the caller
// must prove that through the node/mesh readiness handshake before scheduling.
func (b *Backend) WaitNodeState(ctx context.Context, backendID string, state backends.ProviderNodeState) error {
	switch state {
	case backends.ProviderStateStarted, backends.ProviderStateStopped, backends.ProviderStateSuspended, backends.ProviderStateDestroyed:
	default:
		return fmt.Errorf("waiting for machine %s: unsupported provider state %q", backendID, state)
	}
	machine, err := b.getOwnedMachine(ctx, backendID)
	if state == backends.ProviderStateDestroyed && isHTTPStatus(err, http.StatusNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("authorizing wait for machine %s: %w", backendID, err)
	}
	if state == backends.ProviderStateSuspended &&
		(b.currentPoolScope() == "" || machine.Config.Metadata["yscale-lifecycle"] != string(backends.LifecyclePrewarm)) {
		return fmt.Errorf("refusing suspended-state wait for machine %s outside an enabled prewarm scope", backendID)
	}
	if err := b.waitMachineState(ctx, backendID, state, machine.InstanceID); err != nil {
		if state == backends.ProviderStateDestroyed && isHTTPStatus(err, http.StatusNotFound) {
			return nil
		}
		return err
	}
	slog.InfoContext(ctx, "fly machine state confirmed",
		"component", "flyio_backend", "operation", "wait_state",
		"app", b.appName, "machine_id", backendID,
		"node", machine.Config.Metadata["yscale-node"],
		"scope", machine.Config.Metadata["yscale-scope"],
		"lifecycle", machine.Config.Metadata["yscale-lifecycle"],
		"provider_state", state)
	return nil
}

// DeleteNode destroys a Fly machine node. It is idempotent to honor the
// at-least-once teardown contract (which can redeliver teardown requests):
// if the machine is already absent (HTTP 404), it returns nil.
func (b *Backend) DeleteNode(ctx context.Context, backendID string) error {
	// Fail closed on unknown ownership. Provider IDs are persisted input, and a
	// corrupted or cross-tenant ID must not become an arbitrary force-delete in
	// a shared app. A 404 remains idempotent success.
	machine, err := b.getOwnedMachine(ctx, backendID)
	if isHTTPStatus(err, http.StatusNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("authorizing destroy of machine %s: %w", backendID, err)
	}

	destroyErr := b.destroyMachine(ctx, backendID)
	destroyedOrAbsent := destroyErr == nil || isHTTPStatus(destroyErr, http.StatusNotFound)
	if !destroyedOrAbsent {
		return destroyErr
	}

	var errs []error
	// An explicitly attached yscale volume is the only volume this method may
	// delete. Generic unattached-volume sweeping is too broad in a shared app.
	if destroyedOrAbsent && machine != nil {
		if volID := machine.Config.Metadata["yscale-volume"]; volID != "" {
			if err := b.deleteVolume(ctx, volID); err != nil {
				errs = append(errs, err)
				slog.WarnContext(ctx, "fly legacy volume cleanup needs remediation",
					"component", "flyio_backend", "operation", "delete_volume",
					"app", b.appName, "machine_id", backendID,
					"volume_id", volID, "scope", b.currentPoolScope(),
					"error_class", httpErrorClass(err),
					"manual_remediation", true, "error", err)
			}
		}
	}

	if destroyedOrAbsent {
		slog.InfoContext(ctx, "fly machine destroyed",
			"component", "flyio_backend", "operation", "destroy",
			"app", b.appName, "machine_id", backendID,
			"node", machine.Config.Metadata["yscale-node"],
			"scope", machine.Config.Metadata["yscale-scope"],
			"lifecycle", machine.Config.Metadata["yscale-lifecycle"],
			"provider_state", machine.State,
			"already_absent", isHTTPStatus(destroyErr, http.StatusNotFound))
	}
	return errors.Join(errs...)
}

func (b *Backend) ListPooledNodes(ctx context.Context) ([]backends.PooledNode, error) {
	machines, err := b.listMachines(ctx)
	if err != nil {
		return nil, err
	}
	scope := b.currentPoolScope()
	var pooled []backends.PooledNode
	for _, m := range machines {
		if !ownsMachine(m, scope) {
			continue
		}
		lifecycle := backends.NodeLifecycle(m.Config.Metadata["yscale-lifecycle"])
		switch lifecycle {
		case backends.LifecyclePrewarm:
			// Suspended prewarm snapshots are visible only after an explicit,
			// non-empty scope is configured. This makes the default unscoped
			// controller incapable of discovering or resuming them.
			if scope == "" || m.State != "suspended" {
				continue
			}
		case "", backends.LifecycleCold:
			// Preserve the existing cold-stopped reuse path. Starting one is a
			// clean boot; it is not the Fly memory-snapshot feature.
			if m.State != "stopped" {
				continue
			}
		default:
			continue
		}
		pooled = append(pooled, backends.PooledNode{
			BackendID:  m.ID,
			Name:       m.Config.Metadata["yscale-node"],
			Lifecycle:  lifecycle,
			ScopeHash:  m.Config.Metadata["yscale-scope"],
			ConfigHash: m.Config.Metadata["yscale-config-hash"],
			CreatedAt:  m.CreatedAt,
		})
	}
	return pooled, nil
}

// ListOwnedNodes returns only Fly Machines carrying exact Yscale ownership
// metadata in this backend's current scope. It is read-only provider inventory
// for central reconciliation.
func (b *Backend) ListOwnedNodes(ctx context.Context) ([]backends.OwnedNode, error) {
	machines, err := b.listMachines(ctx)
	if err != nil {
		return nil, err
	}
	scope := b.currentPoolScope()
	owned := make([]backends.OwnedNode, 0)
	for _, m := range machines {
		if !ownsMachine(m, scope) {
			continue
		}
		owned = append(owned, backends.OwnedNode{
			BackendID: m.ID,
			Name:      m.Config.Metadata["yscale-node"],
			BurstID:   m.Config.Metadata["yscale-burst-id"],
			CreatedAt: m.CreatedAt,
		})
	}
	return owned, nil
}

func (b *Backend) CleanupOrphans(ctx context.Context, trackedIDs map[string]bool) (int, error) {
	machines, err := b.listMachines(ctx)
	if err != nil {
		return 0, err
	}

	scope := b.currentPoolScope()
	now := time.Now()
	destroyed := 0
	var errs []error
	for _, m := range machines {
		if !ownsMachine(m, scope) {
			continue
		}
		if trackedIDs[m.ID] {
			continue // tracked by controller
		}
		if backends.OrphanTooYoung(m.CreatedAt, now) {
			continue // may be mid-create; the caller's record write can still be in flight
		}

		// Force-destroy the untracked owned machine. A 404 is idempotent success;
		// any other error is returned so the controller can alert and retry.
		if err := b.destroyMachine(ctx, m.ID); err != nil && !isHTTPStatus(err, http.StatusNotFound) {
			errs = append(errs, err)
			slog.ErrorContext(ctx, "fly orphan destroy failed",
				"component", "flyio_backend", "operation", "cleanup_orphans",
				"app", b.appName, "machine_id", m.ID,
				"node", m.Config.Metadata["yscale-node"], "scope", scope,
				"error_class", httpErrorClass(err), "error", err)
			continue
		}
		destroyed++
		if volID := m.Config.Metadata["yscale-volume"]; volID != "" {
			if err := b.deleteVolume(ctx, volID); err != nil {
				errs = append(errs, err)
				slog.WarnContext(ctx, "fly orphan volume cleanup needs remediation",
					"component", "flyio_backend", "operation", "cleanup_orphans",
					"app", b.appName, "machine_id", m.ID,
					"volume_id", volID, "scope", scope,
					"error_class", httpErrorClass(err),
					"manual_remediation", true, "error", err)
			}
		}
	}

	if len(errs) > 0 {
		slog.WarnContext(ctx, "fly orphan cleanup completed with errors",
			"component", "flyio_backend", "operation", "cleanup_orphans",
			"app", b.appName, "scope", scope,
			"destroyed", destroyed, "errors", len(errs))
	} else if destroyed > 0 {
		slog.InfoContext(ctx, "fly orphan cleanup complete",
			"component", "flyio_backend", "operation", "cleanup_orphans",
			"app", b.appName, "scope", scope,
			"destroyed", destroyed, "errors", len(errs))
	}
	return destroyed, errors.Join(errs...)
}

func ownsMachine(m Machine, scope string) bool {
	name := m.Config.Metadata["yscale-node"]
	return strings.HasPrefix(name, backends.NodeNamePrefix) &&
		len(name) > len(backends.NodeNamePrefix) &&
		m.Config.Metadata["yscale-scope"] == scope
}

func isDigestPinnedImage(image string) bool {
	const marker = "@sha256:"
	markerIndex := strings.LastIndex(image, marker)
	if markerIndex <= 0 {
		return false
	}
	digest := image[markerIndex+len(marker):]
	if len(digest) != 64 {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func (b *Backend) getOwnedMachine(ctx context.Context, machineID string) (*Machine, error) {
	machine, err := b.getMachine(ctx, machineID)
	if err != nil {
		return nil, err
	}
	scope := b.currentPoolScope()
	if !ownsMachine(*machine, scope) {
		return nil, fmt.Errorf("machine is outside owned node prefix or scope %q", scope)
	}
	return machine, nil
}

func (b *Backend) GetNodeStatus(ctx context.Context, backendID string) (*backends.NodeStatus, error) {
	machine, err := b.getMachine(ctx, backendID)
	if err != nil {
		return nil, err
	}

	return &backends.NodeStatus{
		Phase:     mapState(machine.State),
		IP:        machine.PrivateIP,
		StartedAt: machine.CreatedAt,
	}, nil
}

func mapState(state string) backends.NodePhase {
	switch state {
	case "started":
		return backends.NodeRunning
	case "created", "starting":
		return backends.NodeStarting
	case "stopped", "suspended", "destroyed":
		return backends.NodeStopped
	case "failed":
		return backends.NodeFailed
	default:
		return backends.NodeUnknown
	}
}

// headroomFactor ensures the Fly machine has enough allocatable memory
// for the pod request after kubelet/system reserved (~10-15%) and the
// MiB→MB conversion (1 MiB = 1.048576 MB). ~30% covers both effects.
const headroomFactor = 1.30

// maxMemPerCPU is the Fly constraint: shared-cpu machines allow
// 256 MB .. 2048 MB per shared vCPU.
const maxMemPerCPU = 2048

func mapResources(r backends.ResourceRequirements) GuestConfig {
	// Start with the pod's request, scaled up for headroom.
	mem := int(r.MemoryMB)
	if mem < 1024 {
		mem = 1024 // enforce floor
	}
	mem = int(float64(mem) * headroomFactor)

	// Round up to Fly's required 256 MB boundary.
	if mem%256 != 0 {
		mem = ((mem / 256) + 1) * 256
	}

	// Compute cpus from the pod request (same ratio as before).
	cpus := max(int(r.CPUMillis/1000), 1)

	// If the headroomed memory exceeds the valid range for the chosen
	// cpu count, bump cpus until the machine is Fly-valid.
	for mem > cpus*maxMemPerCPU {
		cpus++
	}

	return GuestConfig{
		CPUs:     cpus,
		MemoryMB: mem,
		CPUKind:  "shared",
	}
}

// HTTP helpers for the Fly Machines API.

func (b *Backend) ensureApp(ctx context.Context) error {
	// Serialize the first successful check/create, but do not memoize transient
	// errors forever. A later CreateNode call must be able to recover after a
	// network outage or provider 5xx without restarting the controller.
	b.appMu.Lock()
	defer b.appMu.Unlock()
	if b.appReady {
		return nil
	}

	appURL := fmt.Sprintf("%s/apps/%s", machinesAPIBase, url.PathEscape(b.appName))
	resp, err := b.doRequest(ctx, http.MethodGet, appURL, nil)
	if err != nil {
		return fmt.Errorf("checking fly app: %w", err)
	}
	if isHTTPSuccess(resp.StatusCode) {
		resp.Body.Close()
		b.appReady = true
		return nil
	}
	if resp.StatusCode != http.StatusNotFound {
		defer resp.Body.Close()
		return apiError("checking fly app", resp)
	}
	resp.Body.Close()

	body := CreateAppRequest{AppName: b.appName, OrgSlug: b.org}
	resp, err = b.doRequest(ctx, http.MethodPost, machinesAPIBase+"/apps", body)
	if err != nil {
		return fmt.Errorf("creating fly app: %w", err)
	}
	defer resp.Body.Close()
	if !isHTTPSuccess(resp.StatusCode) {
		return apiError("creating fly app", resp)
	}

	b.appReady = true
	slog.InfoContext(ctx, "fly app ready",
		"component", "flyio_backend", "operation", "ensure_app", "app", b.appName)
	return nil
}

func (b *Backend) createMachine(ctx context.Context, req *CreateMachineRequest) (*Machine, error) {
	endpoint := fmt.Sprintf("%s/apps/%s/machines", machinesAPIBase, url.PathEscape(b.appName))
	resp, err := b.doRequest(ctx, http.MethodPost, endpoint, req)
	if err != nil {
		return nil, fmt.Errorf("creating machine: %w", err)
	}
	defer resp.Body.Close()

	if !isHTTPSuccess(resp.StatusCode) {
		return nil, apiError("creating machine", resp)
	}

	var machine Machine
	if err := json.NewDecoder(resp.Body).Decode(&machine); err != nil {
		return nil, fmt.Errorf("decoding machine response: %w", err)
	}
	return &machine, nil
}

func (b *Backend) getMachine(ctx context.Context, machineID string) (*Machine, error) {
	endpoint := fmt.Sprintf("%s/apps/%s/machines/%s", machinesAPIBase, url.PathEscape(b.appName), url.PathEscape(machineID))
	resp, err := b.doRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("getting machine %s: %w", machineID, err)
	}
	defer resp.Body.Close()

	if !isHTTPSuccess(resp.StatusCode) {
		return nil, apiError("getting machine "+machineID, resp)
	}

	var machine Machine
	if err := json.NewDecoder(resp.Body).Decode(&machine); err != nil {
		return nil, fmt.Errorf("decoding machine response: %w", err)
	}
	return &machine, nil
}

func (b *Backend) startMachine(ctx context.Context, machineID string) error {
	endpoint := fmt.Sprintf("%s/apps/%s/machines/%s/start", machinesAPIBase, url.PathEscape(b.appName), url.PathEscape(machineID))
	resp, err := b.doRequest(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return fmt.Errorf("starting machine %s: %w", machineID, err)
	}
	defer resp.Body.Close()
	if !isHTTPSuccess(resp.StatusCode) {
		return apiError(fmt.Sprintf("starting machine %s", machineID), resp)
	}
	return nil
}

func (b *Backend) listMachines(ctx context.Context) ([]Machine, error) {
	endpoint := fmt.Sprintf("%s/apps/%s/machines", machinesAPIBase, url.PathEscape(b.appName))
	resp, err := b.doRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("listing machines: %w", err)
	}
	defer resp.Body.Close()

	if !isHTTPSuccess(resp.StatusCode) {
		return nil, apiError("listing machines", resp)
	}

	var machines []Machine
	if err := json.NewDecoder(resp.Body).Decode(&machines); err != nil {
		return nil, fmt.Errorf("decoding machines response: %w", err)
	}
	return machines, nil
}

func (b *Backend) stopMachine(ctx context.Context, machineID string) error {
	endpoint := fmt.Sprintf("%s/apps/%s/machines/%s/stop", machinesAPIBase, url.PathEscape(b.appName), url.PathEscape(machineID))
	resp, err := b.doRequest(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return fmt.Errorf("stopping machine %s: %w", machineID, err)
	}
	defer resp.Body.Close()
	if !isHTTPSuccess(resp.StatusCode) {
		return apiError(fmt.Sprintf("stopping machine %s", machineID), resp)
	}
	return nil
}

func (b *Backend) suspendMachine(ctx context.Context, machineID string) error {
	endpoint := fmt.Sprintf("%s/apps/%s/machines/%s/suspend", machinesAPIBase, url.PathEscape(b.appName), url.PathEscape(machineID))
	resp, err := b.doRequest(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return fmt.Errorf("suspending machine %s: %w", machineID, err)
	}
	defer resp.Body.Close()
	if !isHTTPSuccess(resp.StatusCode) {
		return apiError(fmt.Sprintf("suspending machine %s", machineID), resp)
	}
	return nil
}

func (b *Backend) waitMachineState(ctx context.Context, machineID string, state backends.ProviderNodeState, instanceID string) error {
	query := url.Values{}
	query.Set("state", string(state))
	if state == backends.ProviderStateStopped {
		if instanceID == "" {
			return fmt.Errorf("waiting for machine %s state stopped: provider instance_id is required", machineID)
		}
		query.Set("instance_id", instanceID)
	}
	// The shared Fly client has a 30s transport timeout. Keep the provider
	// long-poll below it so a provider 408 remains typed instead of surfacing as
	// an ambiguous client timeout. Callers can retry under their own context.
	seconds := 25
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		deadlineSeconds := int((remaining + time.Second - 1) / time.Second)
		if deadlineSeconds < seconds {
			seconds = deadlineSeconds
		}
	}
	query.Set("timeout", fmt.Sprintf("%d", seconds))
	endpoint := fmt.Sprintf("%s/apps/%s/machines/%s/wait?%s",
		machinesAPIBase, url.PathEscape(b.appName), url.PathEscape(machineID), query.Encode())
	resp, err := b.doRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("waiting for machine %s state %s: %w", machineID, state, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return apiError(fmt.Sprintf("waiting for machine %s state %s", machineID, state), resp)
	}
	return nil
}

func (b *Backend) destroyMachine(ctx context.Context, machineID string) error {
	endpoint := fmt.Sprintf("%s/apps/%s/machines/%s?force=true", machinesAPIBase, url.PathEscape(b.appName), url.PathEscape(machineID))
	resp, err := b.doRequest(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusAccepted {
		// Do not report terminal deletion or remove an attached volume until Fly
		// confirms the asynchronous destroy reached its terminal state.
		resp.Body.Close()
		err := b.waitMachineState(ctx, machineID, backends.ProviderStateDestroyed, "")
		if isHTTPStatus(err, http.StatusNotFound) {
			return nil
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return apiError(fmt.Sprintf("destroying machine %s", machineID), resp)
	}
	return nil
}

func (b *Backend) deleteVolume(ctx context.Context, volumeID string) error {
	endpoint := fmt.Sprintf("%s/apps/%s/volumes/%s", machinesAPIBase, url.PathEscape(b.appName), url.PathEscape(volumeID))
	resp, err := b.doRequest(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("deleting volume %s: %w", volumeID, err)
	}
	defer resp.Body.Close()
	if isHTTPSuccess(resp.StatusCode) || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return apiError(fmt.Sprintf("deleting volume %s", volumeID), resp)
}

// HTTPError represents an error returned by the Fly.io API.
type HTTPError struct {
	Op         string
	StatusCode int
	Body       []byte
}

func (e *HTTPError) Error() string {
	if len(e.Body) > 0 {
		return fmt.Sprintf("%s: status %d: %s", e.Op, e.StatusCode, string(e.Body))
	}
	return fmt.Sprintf("%s: status %d", e.Op, e.StatusCode)
}

func apiError(operation string, resp *http.Response) error {
	const maxErrorBody = 32 << 10
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	return &HTTPError{
		Op:         operation,
		StatusCode: resp.StatusCode,
		Body:       body,
	}
}

func isHTTPSuccess(statusCode int) bool {
	return statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices
}

func isHTTPStatus(err error, statusCode int) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && httpErr.StatusCode == statusCode
}

func httpErrorClass(err error) string {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return "transport"
	}
	switch {
	case httpErr.StatusCode >= 400 && httpErr.StatusCode < 500:
		return "4xx"
	case httpErr.StatusCode >= 500:
		return "5xx"
	default:
		return "unexpected"
	}
}

func (b *Backend) doRequest(ctx context.Context, method, url string, body any) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshaling request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+b.token)
	req.Header.Set("Content-Type", "application/json")

	return b.client.Do(req)
}
