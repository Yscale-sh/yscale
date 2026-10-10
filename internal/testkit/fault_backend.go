// Package testkit provides deterministic controls at production interfaces.
package testkit

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/pkg/backends"
)

var (
	ErrCreateKnownFailed      = errors.New("provider create known failed")
	ErrCreateAmbiguous        = errors.New("provider create result ambiguous")
	ErrDeleteTransient        = errors.New("provider delete transient failure")
	ErrProviderResourceAbsent = errors.New("provider resource absent")
	ErrInventoryUnavailable   = errors.New("provider inventory unavailable")
)

type CreateMode string

const (
	CreateAccepted          CreateMode = "accepted"
	CreateKnownFailed       CreateMode = "known_failed"
	CreateAmbiguousZero     CreateMode = "ambiguous_zero"
	CreateAmbiguousOne      CreateMode = "ambiguous_one"
	CreateAmbiguousMultiple CreateMode = "ambiguous_multiple"
)

type AmbiguityClass string

const (
	AmbiguityUnknown  AmbiguityClass = "unknown"
	AmbiguityZero     AmbiguityClass = "zero"
	AmbiguityOne      AmbiguityClass = "one"
	AmbiguityMultiple AmbiguityClass = "multiple"
)

type Operation string

const (
	OperationCreate         Operation = "create"
	OperationStart          Operation = "start"
	OperationStop           Operation = "stop"
	OperationDelete         Operation = "delete"
	OperationStatus         Operation = "status"
	OperationListPooled     Operation = "list_pooled"
	OperationInventoryOwned Operation = "inventory_owned"
	OperationCleanupOrphans Operation = "cleanup_orphans"
)

// FaultPlan selects deterministic results without adding lifecycle behavior.
// ServerAssignedIDs model identities returned by a provider and are never
// derived from NodeSpec.
type FaultPlan struct {
	CreateMode                 CreateMode
	ServerAssignedIDs          []string
	ResourceAbsentAfterCreate  bool
	ReadyAfterStatusCalls      int
	DeleteTransientFailures    int
	DeleteAlwaysFails          bool
	InventoryTransientFailures int
	InventoryExhausted         bool
	CreatedAt                  time.Time
	Now                        time.Time
}

type Ownership struct {
	Name      string
	BurstID   string
	ScopeHash string
	Labels    map[string]string
}

func OwnershipFromSpec(spec *backends.NodeSpec) Ownership {
	if spec == nil {
		return Ownership{}
	}
	return Ownership{
		Name:      spec.Name,
		BurstID:   spec.BurstID,
		ScopeHash: spec.ScopeHash,
		Labels:    maps.Clone(spec.NodeLabels),
	}
}

type Resource struct {
	ProviderID string
	Ownership  Ownership
	Phase      backends.NodePhase
	Lifecycle  backends.NodeLifecycle
	ConfigHash string
	CreatedAt  time.Time
}

type Call struct {
	Sequence   int
	Operation  Operation
	ProviderID string
	Spec       *backends.NodeSpec
	Outcome    string
}

// FaultBackend implements the same Backend interface as live providers. It is
// provider state only: production workers remain responsible for orchestration,
// retries, ambiguity reconciliation, and lifecycle persistence.
type FaultBackend struct {
	mu sync.Mutex

	name                       string
	plan                       FaultPlan
	resources                  map[string]Resource
	issuedIDs                  map[string]struct{}
	statusCalls                map[string]int
	history                    []Call
	nextGeneratedID            int
	nextConfiguredID           int
	deleteFailuresRemaining    int
	inventoryFailuresRemaining int
}

var _ backends.Backend = (*FaultBackend)(nil)

func NewFaultBackend(name string, plan FaultPlan) (*FaultBackend, error) {
	if name == "" {
		return nil, fmt.Errorf("provider name is required")
	}
	if plan.CreateMode == "" {
		plan.CreateMode = CreateAccepted
	}
	switch plan.CreateMode {
	case CreateAccepted, CreateKnownFailed, CreateAmbiguousZero, CreateAmbiguousOne, CreateAmbiguousMultiple:
	default:
		return nil, fmt.Errorf("unsupported create mode %q", plan.CreateMode)
	}
	if plan.ReadyAfterStatusCalls < 0 || plan.DeleteTransientFailures < 0 || plan.InventoryTransientFailures < 0 {
		return nil, fmt.Errorf("fault counts must not be negative")
	}
	if plan.ResourceAbsentAfterCreate && plan.CreateMode != CreateAccepted {
		return nil, fmt.Errorf("resource absence after create requires accepted create mode")
	}
	seenIDs := make(map[string]struct{}, len(plan.ServerAssignedIDs))
	for _, id := range plan.ServerAssignedIDs {
		if id == "" {
			return nil, fmt.Errorf("server-assigned ID must not be empty")
		}
		if _, exists := seenIDs[id]; exists {
			return nil, fmt.Errorf("duplicate server-assigned ID %q", id)
		}
		seenIDs[id] = struct{}{}
	}
	plan.ServerAssignedIDs = append([]string(nil), plan.ServerAssignedIDs...)
	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	}
	if plan.Now.IsZero() {
		plan.Now = plan.CreatedAt.Add(backends.OrphanGracePeriod + time.Minute)
	}
	return &FaultBackend{
		name:                       name,
		plan:                       plan,
		resources:                  make(map[string]Resource),
		issuedIDs:                  make(map[string]struct{}),
		statusCalls:                make(map[string]int),
		deleteFailuresRemaining:    plan.DeleteTransientFailures,
		inventoryFailuresRemaining: plan.InventoryTransientFailures,
	}, nil
}

func (b *FaultBackend) CreateNode(ctx context.Context, spec *backends.NodeSpec) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		b.recordLocked(OperationCreate, "", spec, err.Error())
		return "", err
	}
	if spec == nil {
		err := errors.New("node spec is required")
		b.recordLocked(OperationCreate, "", nil, err.Error())
		return "", err
	}
	count := 0
	switch b.plan.CreateMode {
	case CreateAccepted, CreateAmbiguousOne:
		count = 1
	case CreateAmbiguousMultiple:
		count = 2
	}
	ids := make([]string, 0, count)
	for range count {
		id := b.nextServerIDLocked()
		ids = append(ids, id)
		if !b.plan.ResourceAbsentAfterCreate {
			b.resources[id] = Resource{
				ProviderID: id,
				Ownership:  OwnershipFromSpec(spec),
				Phase:      backends.NodeStarting,
				Lifecycle:  spec.Lifecycle,
				ConfigHash: spec.ConfigHash,
				CreatedAt:  b.plan.CreatedAt,
			}
		}
	}
	switch b.plan.CreateMode {
	case CreateAccepted:
		outcome := string(CreateAccepted)
		if b.plan.ResourceAbsentAfterCreate {
			outcome = "accepted_then_absent"
		}
		b.recordLocked(OperationCreate, ids[0], spec, outcome)
		return ids[0], nil
	case CreateKnownFailed:
		b.recordLocked(OperationCreate, "", spec, string(CreateKnownFailed))
		return "", ErrCreateKnownFailed
	case CreateAmbiguousZero, CreateAmbiguousOne, CreateAmbiguousMultiple:
		b.recordLocked(OperationCreate, "", spec, string(b.plan.CreateMode))
		return "", ErrCreateAmbiguous
	default:
		panic("validated create mode became invalid")
	}
}

func (b *FaultBackend) StartNode(ctx context.Context, providerID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		b.recordLocked(OperationStart, providerID, nil, err.Error())
		return err
	}
	resource, ok := b.resources[providerID]
	if !ok {
		b.recordLocked(OperationStart, providerID, nil, "absent")
		return ErrProviderResourceAbsent
	}
	resource.Phase = backends.NodeStarting
	b.resources[providerID] = resource
	b.statusCalls[providerID] = 0
	b.recordLocked(OperationStart, providerID, nil, "started")
	return nil
}

func (b *FaultBackend) StopNode(ctx context.Context, providerID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		b.recordLocked(OperationStop, providerID, nil, err.Error())
		return err
	}
	resource, ok := b.resources[providerID]
	if !ok {
		b.recordLocked(OperationStop, providerID, nil, "absent")
		return ErrProviderResourceAbsent
	}
	resource.Phase = backends.NodeStopped
	b.resources[providerID] = resource
	b.recordLocked(OperationStop, providerID, nil, "stopped")
	return nil
}

func (b *FaultBackend) DeleteNode(ctx context.Context, providerID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		b.recordLocked(OperationDelete, providerID, nil, err.Error())
		return err
	}
	if _, ok := b.resources[providerID]; !ok {
		b.recordLocked(OperationDelete, providerID, nil, "absent")
		// Production backends treat an already-absent resource as idempotent
		// delete success. Keep the fake on that same contract so recovery tests
		// cannot depend on a test-only not-found sentinel.
		return nil
	}
	if b.plan.DeleteAlwaysFails {
		b.recordLocked(OperationDelete, providerID, nil, "persistent_failure")
		return ErrDeleteTransient
	}
	if b.deleteFailuresRemaining > 0 {
		b.deleteFailuresRemaining--
		b.recordLocked(OperationDelete, providerID, nil, "transient_failure")
		return ErrDeleteTransient
	}
	delete(b.resources, providerID)
	delete(b.statusCalls, providerID)
	b.recordLocked(OperationDelete, providerID, nil, "deleted")
	return nil
}

func (b *FaultBackend) GetNodeStatus(ctx context.Context, providerID string) (*backends.NodeStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		b.recordLocked(OperationStatus, providerID, nil, err.Error())
		return nil, err
	}
	resource, ok := b.resources[providerID]
	if !ok {
		b.recordLocked(OperationStatus, providerID, nil, "absent")
		return nil, ErrProviderResourceAbsent
	}
	if resource.Phase == backends.NodeStarting {
		b.statusCalls[providerID]++
		if b.statusCalls[providerID] > b.plan.ReadyAfterStatusCalls {
			resource.Phase = backends.NodeRunning
			b.resources[providerID] = resource
		}
	}
	b.recordLocked(OperationStatus, providerID, nil, string(resource.Phase))
	return &backends.NodeStatus{Phase: resource.Phase, StartedAt: resource.CreatedAt}, nil
}

func (b *FaultBackend) ListPooledNodes(ctx context.Context) ([]backends.PooledNode, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		b.recordLocked(OperationListPooled, "", nil, err.Error())
		return nil, err
	}
	if err := b.inventoryErrorLocked(); err != nil {
		b.recordLocked(OperationListPooled, "", nil, "inventory_unavailable")
		return nil, err
	}
	pooled := make([]backends.PooledNode, 0)
	for _, resource := range b.resources {
		if resource.Phase != backends.NodeStopped {
			continue
		}
		pooled = append(pooled, backends.PooledNode{
			BackendID:  resource.ProviderID,
			Name:       resource.Ownership.Name,
			Lifecycle:  resource.Lifecycle,
			ScopeHash:  resource.Ownership.ScopeHash,
			ConfigHash: resource.ConfigHash,
			CreatedAt:  resource.CreatedAt,
		})
	}
	sort.Slice(pooled, func(i, j int) bool { return pooled[i].BackendID < pooled[j].BackendID })
	b.recordLocked(OperationListPooled, "", nil, "listed")
	return pooled, nil
}

func (b *FaultBackend) CleanupOrphans(ctx context.Context, trackedBackendIDs map[string]bool) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		b.recordLocked(OperationCleanupOrphans, "", nil, err.Error())
		return 0, err
	}
	if err := b.inventoryErrorLocked(); err != nil {
		b.recordLocked(OperationCleanupOrphans, "", nil, "inventory_unavailable")
		return 0, err
	}
	destroyed := 0
	for id, resource := range b.resources {
		if trackedBackendIDs[id] || backends.OrphanTooYoung(resource.CreatedAt, b.plan.Now) {
			continue
		}
		delete(b.resources, id)
		delete(b.statusCalls, id)
		destroyed++
	}
	b.recordLocked(OperationCleanupOrphans, "", nil, fmt.Sprintf("destroyed_%d", destroyed))
	return destroyed, nil
}

func (b *FaultBackend) Name() string {
	return b.name
}

// InventoryOwned lists exact ownership matches. An inventory fault returns an
// error, never an empty successful result.
func (b *FaultBackend) InventoryOwned(ctx context.Context, ownership Ownership) ([]Resource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		b.recordLocked(OperationInventoryOwned, "", nil, err.Error())
		return nil, err
	}
	if err := b.inventoryErrorLocked(); err != nil {
		b.recordLocked(OperationInventoryOwned, "", nil, "inventory_unavailable")
		return nil, err
	}
	resources := b.resourcesMatchingLocked(ownership)
	b.recordLocked(OperationInventoryOwned, "", nil, fmt.Sprintf("matches_%d", len(resources)))
	return resources, nil
}

func (b *FaultBackend) ClassifyCreateAmbiguity(ctx context.Context, ownership Ownership) (AmbiguityClass, []Resource, error) {
	resources, err := b.InventoryOwned(ctx, ownership)
	if err != nil {
		return AmbiguityUnknown, nil, err
	}
	switch len(resources) {
	case 0:
		return AmbiguityZero, resources, nil
	case 1:
		return AmbiguityOne, resources, nil
	default:
		return AmbiguityMultiple, resources, nil
	}
}

// MarkAbsent simulates authoritative provider absence without treating a
// Kubernetes observation as provider truth.
func (b *FaultBackend) MarkAbsent(providerID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.resources[providerID]; !ok {
		return false
	}
	delete(b.resources, providerID)
	delete(b.statusCalls, providerID)
	return true
}

// Resources returns stable copies for assertions; it does not represent a
// successful provider inventory call and is unaffected by inventory faults.
func (b *FaultBackend) Resources() []Resource {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.resourcesMatchingLocked(Ownership{})
}

func (b *FaultBackend) CallHistory() []Call {
	b.mu.Lock()
	defer b.mu.Unlock()
	history := make([]Call, len(b.history))
	for i, call := range b.history {
		history[i] = call
		history[i].Spec = cloneNodeSpec(call.Spec)
	}
	return history
}

func (b *FaultBackend) resourcesMatchingLocked(ownership Ownership) []Resource {
	matchAll := ownership.Name == "" && ownership.BurstID == "" && ownership.ScopeHash == "" && ownership.Labels == nil
	resources := make([]Resource, 0, len(b.resources))
	for _, resource := range b.resources {
		if !matchAll && !ownershipEqual(resource.Ownership, ownership) {
			continue
		}
		clone := resource
		clone.Ownership.Labels = maps.Clone(resource.Ownership.Labels)
		resources = append(resources, clone)
	}
	sort.Slice(resources, func(i, j int) bool { return resources[i].ProviderID < resources[j].ProviderID })
	return resources
}

func (b *FaultBackend) nextServerIDLocked() string {
	for {
		var id string
		if b.nextConfiguredID < len(b.plan.ServerAssignedIDs) {
			id = b.plan.ServerAssignedIDs[b.nextConfiguredID]
			b.nextConfiguredID++
		} else {
			b.nextGeneratedID++
			id = fmt.Sprintf("srv-%06d", b.nextGeneratedID)
		}
		if _, exists := b.issuedIDs[id]; exists {
			continue
		}
		b.issuedIDs[id] = struct{}{}
		return id
	}
}

func (b *FaultBackend) inventoryErrorLocked() error {
	if b.plan.InventoryExhausted {
		return ErrInventoryUnavailable
	}
	if b.inventoryFailuresRemaining > 0 {
		b.inventoryFailuresRemaining--
		return ErrInventoryUnavailable
	}
	return nil
}

func (b *FaultBackend) recordLocked(operation Operation, providerID string, spec *backends.NodeSpec, outcome string) {
	b.history = append(b.history, Call{
		Sequence:   len(b.history) + 1,
		Operation:  operation,
		ProviderID: providerID,
		Spec:       cloneNodeSpec(spec),
		Outcome:    outcome,
	})
}

func ownershipEqual(left, right Ownership) bool {
	return left.Name == right.Name &&
		left.BurstID == right.BurstID &&
		left.ScopeHash == right.ScopeHash &&
		maps.Equal(left.Labels, right.Labels)
}

func cloneNodeSpec(spec *backends.NodeSpec) *backends.NodeSpec {
	if spec == nil {
		return nil
	}
	clone := *spec
	clone.TSTags = append([]string(nil), spec.TSTags...)
	clone.NodeLabels = maps.Clone(spec.NodeLabels)
	if spec.Resources.GPU != nil {
		gpu := *spec.Resources.GPU
		gpu.SKUs = append([]string(nil), spec.Resources.GPU.SKUs...)
		clone.Resources.GPU = &gpu
	}
	return &clone
}
