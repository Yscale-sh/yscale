package decider

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/pkg/backends"
)

type reconcileBackend struct {
	name      string
	owned     []backends.OwnedNode
	listErr   error
	deleteErr error
	deleted   []string
	listCalls int
	onDelete  func()
}

func (b *reconcileBackend) CreateNode(context.Context, *backends.NodeSpec) (string, error) {
	return "", nil
}
func (b *reconcileBackend) StartNode(context.Context, string) error { return nil }
func (b *reconcileBackend) StopNode(context.Context, string) error  { return nil }
func (b *reconcileBackend) DeleteNode(_ context.Context, id string) error {
	if b.onDelete != nil {
		b.onDelete()
	}
	b.deleted = append(b.deleted, id)
	return b.deleteErr
}
func (b *reconcileBackend) GetNodeStatus(context.Context, string) (*backends.NodeStatus, error) {
	return &backends.NodeStatus{Phase: backends.NodeRunning}, nil
}
func (b *reconcileBackend) ListPooledNodes(context.Context) ([]backends.PooledNode, error) {
	return nil, nil
}
func (b *reconcileBackend) CleanupOrphans(context.Context, map[string]bool) (int, error) {
	return 0, nil
}
func (b *reconcileBackend) Name() string { return b.name }
func (b *reconcileBackend) ListOwnedNodes(context.Context) ([]backends.OwnedNode, error) {
	b.listCalls++
	return b.owned, b.listErr
}

type fakeReconciliationStore struct {
	protected []lifecycle.ProviderResourceRef
	creates   []lifecycle.ProviderCreateReconciliation
	orphans   map[string]lifecycle.ProviderOrphanRecord

	reconciledCreateID int64
	reconciledResource string
	reconcileCalls     int
	createListErr      error
	observeErrors      map[string]error
	reconcileErr       error
	claimErr           error
	markErr            error
	absentErr          error
	failureErr         error
	successErr         error
	lastFailure        string
	clockTimes         []time.Time
	clockCalls         int
	clockErrAt         int
	clockErr           error
	absentAt           time.Time

	failures  int
	successes int
	deleteErr []string
	deleted   []string
}

func (s *fakeReconciliationStore) ProviderInventoryTime(context.Context) (time.Time, error) {
	s.clockCalls++
	if s.clockErrAt == s.clockCalls {
		return time.Time{}, s.clockErr
	}
	if s.clockCalls <= len(s.clockTimes) {
		return s.clockTimes[s.clockCalls-1], nil
	}
	return time.Now().UTC(), nil
}

func (s *fakeReconciliationStore) ProtectedProviderResources(context.Context) ([]lifecycle.ProviderResourceRef, error) {
	return append([]lifecycle.ProviderResourceRef(nil), s.protected...), nil
}

func (s *fakeReconciliationStore) ProviderCreatesForReconciliation(_ context.Context, provider, account string) ([]lifecycle.ProviderCreateReconciliation, error) {
	if s.createListErr != nil {
		return nil, s.createListErr
	}
	out := make([]lifecycle.ProviderCreateReconciliation, 0)
	for _, c := range s.creates {
		if c.Provider == provider && c.CloudAccountID == account {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s *fakeReconciliationStore) ReconcileExpiredProviderCreateSucceeded(_ context.Context, id int64, resourceID string) error {
	if s.reconcileErr != nil {
		return s.reconcileErr
	}
	for _, create := range s.creates {
		if create.ID == id {
			if orphan, ok := s.orphans[providerResourceKey(create.Provider, create.CloudAccountID, resourceID)]; ok &&
				(orphan.State != lifecycle.ProviderOrphanQuarantined || orphan.Attempts > 0) {
				return lifecycle.ErrInvariantViolation
			}
		}
	}
	s.reconciledCreateID = id
	s.reconciledResource = resourceID
	s.reconcileCalls++
	for i := range s.creates {
		if s.creates[i].ID == id {
			s.creates[i].ProviderResourceID = resourceID
			s.creates[i].State = lifecycle.OperationSucceeded
			s.creates[i].LeaseExpired = false
		}
	}
	return nil
}

func (s *fakeReconciliationStore) ObserveProviderOrphan(_ context.Context, obs lifecycle.ProviderResourceObservation) (lifecycle.ProviderOrphanRecord, error) {
	if err := s.observeErrors[obs.ProviderResourceID]; err != nil {
		return lifecycle.ProviderOrphanRecord{}, err
	}
	if s.orphans == nil {
		s.orphans = make(map[string]lifecycle.ProviderOrphanRecord)
	}
	key := providerResourceKey(obs.Provider, obs.CloudAccountID, obs.ProviderResourceID)
	rec, ok := s.orphans[key]
	if !ok {
		rec = lifecycle.ProviderOrphanRecord{
			Provider: obs.Provider, CloudAccountID: obs.CloudAccountID,
			ProviderResourceID: obs.ProviderResourceID, State: lifecycle.ProviderOrphanQuarantined,
			FirstObservedAt: obs.ObservedAt,
		}
	}
	if !obs.ObservedAt.Before(rec.LastObservedAt) {
		if rec.State == lifecycle.ProviderOrphanDeleted {
			rec.State = lifecycle.ProviderOrphanQuarantined
			rec.DeletedAt = nil
			rec.FirstObservedAt = obs.ObservedAt
		}
		rec.ResourceName = obs.ResourceName
		rec.LastObservedAt = obs.ObservedAt
	}
	if rec.ProviderCreatedAt.IsZero() {
		rec.ProviderCreatedAt = obs.ProviderCreatedAt
	}
	rec.Observations++
	s.orphans[key] = rec
	return rec, nil
}

func (s *fakeReconciliationStore) ClaimProviderOrphanDelete(_ context.Context, ref lifecycle.ProviderResourceRef, _ time.Duration) (lifecycle.ProviderOrphanRecord, bool, error) {
	if s.claimErr != nil {
		return lifecycle.ProviderOrphanRecord{}, false, s.claimErr
	}
	key := providerResourceKey(ref.Provider, ref.CloudAccountID, ref.ProviderResourceID)
	rec := s.orphans[key]
	if rec.State != lifecycle.ProviderOrphanQuarantined && rec.State != lifecycle.ProviderOrphanDeleting {
		return lifecycle.ProviderOrphanRecord{}, false, nil
	}
	rec.State = lifecycle.ProviderOrphanDeleting
	rec.Attempts++
	rec.LeaseToken = "lease-token"
	s.orphans[key] = rec
	return rec, true, nil
}

func (s *fakeReconciliationStore) MarkProviderOrphanDeleteFailed(_ context.Context, ref lifecycle.ProviderResourceRef, _ string, safeError string) error {
	if s.markErr != nil {
		return s.markErr
	}
	key := providerResourceKey(ref.Provider, ref.CloudAccountID, ref.ProviderResourceID)
	rec := s.orphans[key]
	rec.State = lifecycle.ProviderOrphanQuarantined
	rec.LeaseToken = ""
	rec.LastError = safeError
	s.orphans[key] = rec
	s.deleteErr = append(s.deleteErr, key)
	return nil
}

func (s *fakeReconciliationStore) MarkProviderOrphanDeleted(_ context.Context, ref lifecycle.ProviderResourceRef, _ string, deletedAt time.Time) error {
	if s.markErr != nil {
		return s.markErr
	}
	key := providerResourceKey(ref.Provider, ref.CloudAccountID, ref.ProviderResourceID)
	rec := s.orphans[key]
	rec.State = lifecycle.ProviderOrphanDeleted
	rec.LeaseToken = ""
	rec.LastError = ""
	rec.DeletedAt = &deletedAt
	if deletedAt.After(rec.LastObservedAt) {
		rec.LastObservedAt = deletedAt
	}
	s.orphans[key] = rec
	s.deleted = append(s.deleted, key)
	return nil
}

func (s *fakeReconciliationStore) ReconcileProviderOrphansAbsent(_ context.Context, provider, account string, observed []string, absentAt time.Time) (int, error) {
	s.absentAt = absentAt
	if s.absentErr != nil {
		return 0, s.absentErr
	}
	seen := make(map[string]bool, len(observed))
	for _, id := range observed {
		seen[id] = true
	}
	deleted := 0
	for key, rec := range s.orphans {
		if rec.Provider != provider || rec.CloudAccountID != account || seen[rec.ProviderResourceID] ||
			rec.State == lifecycle.ProviderOrphanDeleted {
			continue
		}
		if !rec.LastObservedAt.Before(absentAt) {
			continue
		}
		rec.State = lifecycle.ProviderOrphanDeleted
		rec.LeaseToken = ""
		rec.LockedUntil = nil
		rec.LastError = ""
		rec.DeletedAt = &absentAt
		rec.LastObservedAt = absentAt
		s.orphans[key] = rec
		s.deleted = append(s.deleted, key)
		deleted++
	}
	return deleted, nil
}

func TestProviderReconciliationUsesInventoryWindow(t *testing.T) {
	start := time.Date(2026, 9, 5, 22, 0, 0, 0, time.UTC)
	finish := start.Add(time.Minute)
	store := &fakeReconciliationStore{clockTimes: []time.Time{start, finish}}
	backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{BackendID: "present"}}}
	d := &Decider{fly: backend, reconciliation: store}
	if _, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Grace: time.Hour}); err != nil {
		t.Fatal(err)
	}
	rec := store.orphans[providerResourceKey(backends.TypeFlyIO, "", "present")]
	if store.clockCalls != 2 || !rec.LastObservedAt.Equal(finish) || !store.absentAt.Equal(start) || len(backend.deleted) != 0 {
		t.Fatalf("inventory window not preserved: clockCalls=%d presence=%s absence=%s deletes=%v", store.clockCalls, rec.LastObservedAt, store.absentAt, backend.deleted)
	}
}

func TestProviderReconciliationRejectsWholeInvalidInventoryBeforeMutations(t *testing.T) {
	now := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	for _, provider := range []string{backends.TypeFlyIO, backends.TypeLinode} {
		for _, mode := range []string{"orphan", "adopt"} {
			for _, bad := range []struct {
				name        string
				node        backends.OwnedNode
				storeReject bool
			}{
				{"missing_id", backends.OwnedNode{}, true},
				{"blank_id", backends.OwnedNode{BackendID: " \t"}, true},
				{"oversized_id", backends.OwnedNode{BackendID: strings.Repeat("x", 256)}, true},
				{"nul_id", backends.OwnedNode{BackendID: "bad\x00id"}, true},
				{"invalid_utf8_id", backends.OwnedNode{BackendID: "bad\xffid"}, true},
				{"noncanonical_id", backends.OwnedNode{BackendID: " candidate "}, false},
				{"duplicate_id", backends.OwnedNode{BackendID: "candidate", BurstID: "different-metadata"}, false},
				{"nul_name", backends.OwnedNode{BackendID: "bad", Name: "bad\x00name"}, true},
				{"invalid_utf8_name", backends.OwnedNode{BackendID: "bad", Name: "bad\xffname"}, true},
			} {
				for _, order := range []string{"first", "last"} {
					t.Run(provider+"/"+mode+"/"+bad.name+"/"+order, func(t *testing.T) {
						candidate := backends.OwnedNode{BackendID: "candidate", BurstID: "pending"}
						store := &fakeReconciliationStore{orphans: make(map[string]lifecycle.ProviderOrphanRecord)}
						for _, id := range []string{"candidate", "not-seen"} {
							store.orphans[providerResourceKey(provider, "", id)] = lifecycle.ProviderOrphanRecord{
								Provider: provider, ProviderResourceID: id, State: lifecycle.ProviderOrphanQuarantined,
								FirstObservedAt: now.Add(-time.Hour), LastObservedAt: now.Add(-time.Hour), Observations: 1,
							}
						}
						before := make(map[string]lifecycle.ProviderOrphanRecord, len(store.orphans))
						for key, rec := range store.orphans {
							before[key] = rec
						}
						if mode == "adopt" {
							store.creates = []lifecycle.ProviderCreateReconciliation{{ID: 42, Provider: provider,
								BurstID: "pending", State: lifecycle.OperationProcessing, LeaseExpired: true}}
						}
						// Model the store rejecting an unpersistable observation.
						// Duplicate/noncanonical identities need inventory-level checks.
						if bad.storeReject {
							store.observeErrors = map[string]error{bad.node.BackendID: lifecycle.ErrInvalidArgument}
						}
						backend := &reconcileBackend{name: provider, owned: []backends.OwnedNode{candidate, bad.node}}
						if order == "first" {
							backend.owned[0], backend.owned[1] = backend.owned[1], backend.owned[0]
						}
						d := &Decider{reconciliation: store}
						if provider == backends.TypeFlyIO {
							d.fly = backend
						} else {
							d.linode = backend
						}
						summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute})
						if !errors.Is(err, lifecycle.ErrInvalidArgument) || summary.Failed != 1 || summary.Deleted != 0 ||
							len(backend.deleted) != 0 || store.reconcileCalls != 0 || len(store.deleted) != 0 ||
							store.successes != 0 || store.failures != 1 || !store.absentAt.IsZero() || !reflect.DeepEqual(store.orphans, before) {
							t.Fatalf("invalid inventory caused partial work: summary=%+v providerDeletes=%d adoptions=%d err=%v", summary, len(backend.deleted), store.reconcileCalls, err)
						}
						if !strings.Contains(store.lastFailure, "stage=inventory_invalid") {
							t.Fatalf("validation failure was not recorded at the inventory boundary: %s", store.lastFailure)
						}
						backend.owned = []backends.OwnedNode{candidate}
						store.observeErrors = nil
						_, err = d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now.Add(time.Minute), Grace: time.Minute})
						if err != nil || store.successes != 1 || store.orphans[providerResourceKey(provider, "", "not-seen")].State != lifecycle.ProviderOrphanDeleted {
							t.Fatalf("fresh valid inventory did not recover: err=%v", err)
						}
						if (mode == "adopt" && (store.reconcileCalls != 1 || len(backend.deleted) != 0)) ||
							(mode == "orphan" && (store.reconcileCalls != 0 || len(backend.deleted) != 1)) {
							t.Fatalf("valid inventory lost intended work: deletes=%d adoptions=%d", len(backend.deleted), store.reconcileCalls)
						}
					})
				}
			}
		}
	}
}

func TestProviderReconciliationInvalidInventoryPreservesReceiptError(t *testing.T) {
	cause := errors.New("synthetic inventory receipt unavailable")
	store := &fakeReconciliationStore{failureErr: cause}
	backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{}}}
	d := &Decider{fly: backend, reconciliation: store}
	summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{})
	if !errors.Is(err, lifecycle.ErrInvalidArgument) || !errors.Is(err, cause) || summary.Failed != 1 ||
		store.failures != 1 || store.successes != 0 || len(store.orphans) != 0 || len(backend.deleted) != 0 || !store.absentAt.IsZero() {
		t.Fatalf("validation/receipt error was lost or target continued: summary=%+v err=%v", summary, err)
	}
}

func TestProviderReconciliationInventoryClockFailuresStopTarget(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			cause := errors.New("synthetic inventory clock unavailable")
			store := &fakeReconciliationStore{clockErrAt: failAt, clockErr: cause}
			backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{BackendID: "present"}}}
			d := &Decider{fly: backend, reconciliation: store}
			summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{})
			if !errors.Is(err, cause) || summary.Failed != 1 || store.successes != 0 || len(store.orphans) != 0 || len(backend.deleted) != 0 || backend.listCalls != failAt-1 {
				t.Fatalf("clock failure continued target: summary=%+v calls=%d err=%v", summary, backend.listCalls, err)
			}
		})
	}
}

func TestProviderReconciliationRefreshesPresenceBeforeAdopting(t *testing.T) {
	for _, attempted := range []bool{false, true} {
		t.Run(map[bool]string{false: "absence_only", true: "delete_attempted"}[attempted], func(t *testing.T) {
			now := time.Date(2026, 9, 5, 22, 0, 0, 0, time.UTC)
			prior := now.Add(-time.Hour)
			ref := providerResourceKey(backends.TypeFlyIO, "", "present")
			store := &fakeReconciliationStore{
				creates: []lifecycle.ProviderCreateReconciliation{{ID: 42, Provider: backends.TypeFlyIO, BurstID: "reappeared", State: lifecycle.OperationProcessing, LeaseExpired: true}},
				orphans: map[string]lifecycle.ProviderOrphanRecord{ref: {Provider: backends.TypeFlyIO, ProviderResourceID: "present",
					State: lifecycle.ProviderOrphanDeleted, FirstObservedAt: prior, LastObservedAt: prior, DeletedAt: &prior}},
			}
			if attempted {
				rec := store.orphans[ref]
				rec.Attempts = 1
				store.orphans[ref] = rec
			}
			backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{BackendID: "present", BurstID: "reappeared"}}}
			d := &Decider{fly: backend, reconciliation: store}
			_, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute})
			if attempted {
				if !errors.Is(err, lifecycle.ErrInvariantViolation) || store.reconcileCalls != 0 {
					t.Fatalf("prior delete authorization was ignored: adopted=%d err=%v", store.reconcileCalls, err)
				}
			} else if err != nil || store.reconcileCalls != 1 || store.reconciledResource != "present" {
				t.Fatalf("new presence could not recover from an older absence: adopted=%d err=%v", store.reconcileCalls, err)
			}
			if len(backend.deleted) != 0 {
				t.Fatalf("adoption reconciliation deleted inventory: %v", backend.deleted)
			}
		})
	}
}

func TestProviderReconciliationRejectsInvalidInventoryClocks(t *testing.T) {
	start := time.Date(2026, 9, 5, 22, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		clocks    []time.Time
		listCalls int
	}{
		{"missing_start", []time.Time{{}}, 0},
		{"missing_finish", []time.Time{start, {}}, 1},
		{"backwards_finish", []time.Time{start, start.Add(-time.Minute)}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeReconciliationStore{clockTimes: tc.clocks}
			backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{BackendID: "present"}}}
			d := &Decider{fly: backend, reconciliation: store}
			summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{})
			if err == nil || summary.Failed != 1 || store.failures != 1 || store.successes != 0 ||
				len(store.orphans) != 0 || len(backend.deleted) != 0 || !store.absentAt.IsZero() || backend.listCalls != tc.listCalls {
				t.Fatalf("invalid clock continued target: summary=%+v calls=%d err=%v", summary, backend.listCalls, err)
			}
		})
	}
}

func TestProviderReconciliationDeleteReceiptClockAndRecovery(t *testing.T) {
	start := time.Date(2026, 9, 5, 22, 0, 0, 0, time.UTC)
	finish := start.Add(time.Minute)
	prior := start.Add(-time.Hour)
	cause := errors.New("synthetic post-delete clock unavailable")
	for _, tc := range []struct {
		name      string
		deletedAt time.Time
		clockErr  error
		wantError bool
	}{
		{"success", finish.Add(time.Minute), nil, false},
		{"unavailable", time.Time{}, cause, true},
		{"missing", time.Time{}, nil, true},
		{"backwards", start, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := providerResourceKey(backends.TypeFlyIO, "", "present")
			store := &fakeReconciliationStore{
				clockTimes: []time.Time{start, finish, tc.deletedAt},
				orphans: map[string]lifecycle.ProviderOrphanRecord{key: {
					Provider: backends.TypeFlyIO, ProviderResourceID: "present", State: lifecycle.ProviderOrphanQuarantined,
					FirstObservedAt: prior, LastObservedAt: prior, Observations: 1,
				}},
			}
			if tc.clockErr != nil {
				store.clockErrAt, store.clockErr = 3, tc.clockErr
			}
			backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{BackendID: "present"}}, onDelete: func() {
				if store.clockCalls != 2 {
					t.Errorf("deletion receipt clock read before provider deletion: calls=%d", store.clockCalls)
				}
			}}
			d := &Decider{fly: backend, reconciliation: store}
			summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Grace: time.Minute})
			rec := store.orphans[key]
			if len(backend.deleted) != 1 || store.clockCalls != 3 {
				t.Fatalf("provider/clock call sequence differs: deletes=%v clocks=%d err=%v", backend.deleted, store.clockCalls, err)
			}
			if !tc.wantError {
				if err != nil || summary.Deleted != 1 || summary.Failed != 0 || store.successes != 1 ||
					rec.DeletedAt == nil || !rec.DeletedAt.Equal(tc.deletedAt) || !store.absentAt.Equal(start) {
					t.Fatalf("receipt did not use post-delete time: summary=%+v deletedAt=%v err=%v", summary, rec.DeletedAt, err)
				}
				return
			}
			if err == nil || (tc.clockErr != nil && !errors.Is(err, tc.clockErr)) || summary.Failed != 1 || summary.Deleted != 0 ||
				store.failures != 1 || store.successes != 0 || len(store.deleted) != 0 || !store.absentAt.IsZero() ||
				rec.State != lifecycle.ProviderOrphanDeleting || rec.DeletedAt != nil || rec.LeaseToken == "" {
				t.Fatalf("clock failure manufactured deletion evidence: summary=%+v state=%s err=%v", summary, rec.State, err)
			}
			// A later complete inventory can settle the outstanding claim even
			// though the provider already succeeded and the worker lost its clock.
			backend.owned = nil
			nextStart := start.Add(time.Hour)
			store.clockTimes = append(store.clockTimes, nextStart, nextStart.Add(time.Minute))
			summary, err = d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Grace: time.Minute})
			rec = store.orphans[key]
			if err != nil || summary.Deleted != 1 || summary.Failed != 0 || len(backend.deleted) != 1 || store.successes != 1 ||
				rec.State != lifecycle.ProviderOrphanDeleted || rec.LeaseToken != "" || rec.LockedUntil != nil ||
				rec.DeletedAt == nil || !rec.DeletedAt.Equal(nextStart) {
				t.Fatalf("fresh inventory did not recover the outstanding deletion: summary=%+v state=%s err=%v", summary, rec.State, err)
			}
		})
	}
}

func TestProviderReconciliationReappearanceRestartsGrace(t *testing.T) {
	now := time.Date(2026, 9, 5, 22, 0, 0, 0, time.UTC)
	prior := now.Add(-time.Hour)
	key := providerResourceKey(backends.TypeFlyIO, "", "present")
	store := &fakeReconciliationStore{orphans: map[string]lifecycle.ProviderOrphanRecord{key: {
		Provider: backends.TypeFlyIO, ProviderResourceID: "present", State: lifecycle.ProviderOrphanDeleted,
		FirstObservedAt: prior, LastObservedAt: prior, DeletedAt: &prior, Attempts: 1,
	}}}
	backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{BackendID: "present"}}}
	d := &Decider{fly: backend, reconciliation: store}
	summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute})
	rec := store.orphans[key]
	if err != nil || summary.Quarantined != 1 || len(backend.deleted) != 0 || rec.State != lifecycle.ProviderOrphanQuarantined ||
		rec.DeletedAt != nil || !rec.FirstObservedAt.Equal(now) || rec.Attempts != 1 {
		t.Fatalf("reappearance reused old grace or discarded attempts: summary=%+v state=%s attempts=%d err=%v", summary, rec.State, rec.Attempts, err)
	}
	summary, err = d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now.Add(2 * time.Minute), Grace: time.Minute})
	if err != nil || summary.Deleted != 1 || len(backend.deleted) != 1 || store.orphans[key].Attempts != 2 {
		t.Fatalf("reappeared orphan could not be cleaned after fresh grace: summary=%+v deletes=%v err=%v", summary, backend.deleted, err)
	}
}

func (s *fakeReconciliationStore) RecordProviderInventoryFailure(_ context.Context, _, _, safeError string) error {
	s.failures++
	s.lastFailure = safeError
	return s.failureErr
}

func (s *fakeReconciliationStore) RecordProviderInventorySuccess(context.Context, string, string, int, int, int) error {
	s.successes++
	return s.successErr
}

func TestProviderReconciliationProtectsDurableLifecycleOwners(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		protected []lifecycle.ProviderResourceRef
		creates   []lifecycle.ProviderCreateReconciliation
	}{
		{
			name: "active burst",
			protected: []lifecycle.ProviderResourceRef{{
				Provider: backends.TypeFlyIO, ProviderResourceID: "res-live",
			}},
		},
		{
			name: "nonterminal delete",
			protected: []lifecycle.ProviderResourceRef{{
				Provider: backends.TypeFlyIO, ProviderResourceID: "res-live",
			}},
		},
		{
			name: "in-flight create",
			creates: []lifecycle.ProviderCreateReconciliation{{
				ID: 1, Provider: backends.TypeFlyIO, BurstID: "burst_live",
				State: lifecycle.OperationProcessing,
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeReconciliationStore{protected: tc.protected, creates: tc.creates}
			backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{
				BackendID: "res-live", Name: "ys-burst-live", BurstID: "burst_live", CreatedAt: now.Add(-time.Hour),
			}}}
			d := &Decider{fly: backend, reconciliation: store}
			summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
			if err != nil {
				t.Fatal(err)
			}
			if len(backend.deleted) != 0 || summary.Quarantined != 0 {
				t.Fatalf("owned lifecycle resource treated as orphan: deleted=%v summary=%+v", backend.deleted, summary)
			}
		})
	}
}

func TestProviderReconciliationExpiredAmbiguousCreateMatching(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name            string
		owned           []backends.OwnedNode
		wantAdopt       string
		wantQuarantined int
		wantDelete      int
	}{
		{name: "zero exact matches stays ambiguous", owned: nil},
		{name: "one exact match reconciles", owned: []backends.OwnedNode{{
			BackendID: "res-one", Name: "ys-burst-create", BurstID: "burst_create", CreatedAt: now.Add(-time.Hour),
		}}, wantAdopt: "res-one"},
		{name: "multiple exact matches stay ambiguous", owned: []backends.OwnedNode{
			{BackendID: "res-one", Name: "ys-burst-create", BurstID: "burst_create", CreatedAt: now.Add(-time.Hour)},
			{BackendID: "res-two", Name: "ys-burst-create", BurstID: "burst_create", CreatedAt: now.Add(-time.Hour)},
		}, wantQuarantined: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeReconciliationStore{creates: []lifecycle.ProviderCreateReconciliation{{
				ID: 42, Provider: backends.TypeFlyIO, BurstID: "burst_create",
				State: lifecycle.OperationProcessing, LeaseExpired: true,
			}}}
			backend := &reconcileBackend{name: backends.TypeFlyIO, owned: tc.owned}
			d := &Decider{fly: backend, reconciliation: store}
			summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
			if err != nil {
				t.Fatal(err)
			}
			if store.reconciledResource != tc.wantAdopt {
				t.Fatalf("reconciled resource = %q, want %q", store.reconciledResource, tc.wantAdopt)
			}
			if len(backend.deleted) != tc.wantDelete {
				t.Fatalf("deleted = %v, want %d deletes", backend.deleted, tc.wantDelete)
			}
			if summary.Quarantined != tc.wantQuarantined {
				t.Fatalf("quarantined = %d, want %d", summary.Quarantined, tc.wantQuarantined)
			}
		})
	}
}

func TestProviderReconciliationAmbiguousCandidatesStayQuarantined(t *testing.T) {
	for _, provider := range []string{backends.TypeFlyIO, backends.TypeLinode} {
		t.Run(provider, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 9, 5, 22, 0, 0, 0, time.UTC)
			store := &fakeReconciliationStore{creates: []lifecycle.ProviderCreateReconciliation{{
				ID: 42, Provider: provider, BurstID: "ambiguous-create", State: lifecycle.OperationProcessing, LeaseExpired: true,
			}}}
			backend := &reconcileBackend{name: provider, owned: []backends.OwnedNode{
				{BackendID: "candidate-one", BurstID: "ambiguous-create", CreatedAt: now.Add(-time.Hour)},
				{BackendID: "candidate-two", BurstID: "ambiguous-create", CreatedAt: now.Add(-time.Hour)},
				{BackendID: "ordinary-orphan", CreatedAt: now.Add(-time.Hour)},
			}}
			reopen := func() *Decider {
				d := &Decider{reconciliation: store}
				if provider == backends.TypeFlyIO {
					d.fly = backend
				} else {
					d.linode = backend
				}
				return d
			}
			for scan := 0; scan < 3; scan++ {
				// The ordinary orphan has disappeared after the second scan's
				// successful delete. Both ambiguous candidates are still present.
				if scan == 2 {
					backend.owned = backend.owned[:2]
				}
				opts := ProviderReconcileOptions{Now: now.Add(time.Duration(scan) * 2 * time.Minute), Grace: time.Minute, Log: slog.Default()}
				// Rebuild the decider each time; only the persisted observations
				// and current provider inventory survive between scans.
				summary, err := reopen().ReconcileProviders(ctx, opts)
				if err != nil {
					t.Fatal(err)
				}
				wantQuarantined, wantDeleted, wantProviderDeletes := 3, 0, 0
				if scan == 1 {
					wantDeleted = 1
				}
				if scan > 0 {
					wantProviderDeletes = 1
				}
				if scan == 2 {
					wantQuarantined = 2
				}
				if summary.Failed != 0 || summary.Quarantined != wantQuarantined || summary.Deleted != wantDeleted {
					t.Errorf("scan %d lost ambiguity quarantine: summary=%+v", scan, summary)
				}
				if len(backend.deleted) != wantProviderDeletes || (wantProviderDeletes > 0 && backend.deleted[0] != "ordinary-orphan") {
					t.Errorf("scan %d deleted ambiguous resources: %v", scan, backend.deleted)
				}
				for _, id := range []string{"candidate-one", "candidate-two"} {
					record := store.orphans[providerResourceKey(provider, "", id)]
					if record.State != lifecycle.ProviderOrphanQuarantined || record.Attempts != 0 || record.LeaseToken != "" || record.DeletedAt != nil || !record.FirstObservedAt.Equal(now) || record.Observations != scan+1 {
						t.Errorf("scan %d did not retain unclaimed quarantine for %s: state=%s attempts=%d observations=%d", scan, id, record.State, record.Attempts, record.Observations)
					}
				}
				if store.reconcileCalls != 0 || store.creates[0].State != lifecycle.OperationProcessing || store.creates[0].ProviderResourceID != "" {
					t.Error("multiple matches were guessed into one provider identity")
				}
			}
			// A complete later inventory with exactly one candidate permits
			// adoption; positive absence, not a delete call, resolves the other.
			backend.owned = backend.owned[:1]
			summary, err := reopen().ReconcileProviders(ctx, ProviderReconcileOptions{Now: now.Add(10 * time.Minute), Grace: time.Minute, Log: slog.Default()})
			if err != nil || store.reconcileCalls != 1 || store.reconciledResource != "candidate-one" || summary.Quarantined != 0 || summary.Deleted != 1 || len(backend.deleted) != 1 {
				t.Errorf("unique-match recovery failed: summary=%+v adopted=%s deletes=%v err=%v", summary, store.reconciledResource, backend.deleted, err)
			}
			_, err = reopen().ReconcileProviders(ctx, ProviderReconcileOptions{Now: now.Add(12 * time.Minute), Grace: time.Minute, Log: slog.Default()})
			if err != nil || store.reconcileCalls != 1 || len(backend.deleted) != 1 {
				t.Errorf("known identity was adopted or deleted again: err=%v", err)
			}
		})
	}
}

func TestProviderReconciliationDeadLetterWithoutReceiptDoesNotProtectExactMatch(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	key := providerResourceKey(backends.TypeFlyIO, "", "res-dead")
	store := &fakeReconciliationStore{
		creates: []lifecycle.ProviderCreateReconciliation{{
			ID: 7, Provider: backends.TypeFlyIO, BurstID: "burst_dead",
			State: lifecycle.OperationDeadLetter,
		}},
		orphans: map[string]lifecycle.ProviderOrphanRecord{key: {
			Provider: backends.TypeFlyIO, ProviderResourceID: "res-dead",
			State: lifecycle.ProviderOrphanQuarantined, FirstObservedAt: now.Add(-time.Hour),
		}},
	}
	backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{
		BackendID: "res-dead", Name: "ys-burst-dead", BurstID: "burst_dead", CreatedAt: now.Add(-time.Hour),
	}}}
	d := &Decider{fly: backend, reconciliation: store}
	summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Deleted != 1 || len(backend.deleted) != 1 {
		t.Fatalf("dead-letter without receipt protected a matching resource: summary=%+v deleted=%v", summary, backend.deleted)
	}
}

func TestProviderReconciliationQuarantineGraceAndRestartPersistence(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	grace := 15 * time.Minute
	for _, tc := range []struct {
		name      string
		firstSeen time.Time
		wantDel   bool
	}{
		{name: "first quarantine", firstSeen: time.Time{}},
		{name: "grace not elapsed", firstSeen: now.Add(-grace / 2)},
		{name: "stale orphan deletion", firstSeen: now.Add(-2 * grace), wantDel: true},
		{name: "restart persistent quarantine", firstSeen: now.Add(-3 * grace), wantDel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeReconciliationStore{}
			key := providerResourceKey(backends.TypeFlyIO, "", "orphan-1")
			if !tc.firstSeen.IsZero() {
				store.orphans = map[string]lifecycle.ProviderOrphanRecord{key: {
					Provider: backends.TypeFlyIO, ProviderResourceID: "orphan-1",
					State: lifecycle.ProviderOrphanQuarantined, FirstObservedAt: tc.firstSeen,
				}}
			}
			backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{
				BackendID: "orphan-1", Name: "ys-burst-orphan", CreatedAt: now.Add(-time.Hour),
			}}}
			d := &Decider{fly: backend, reconciliation: store}
			_, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: grace, Log: slog.Default()})
			if err != nil {
				t.Fatal(err)
			}
			if got := len(backend.deleted) > 0; got != tc.wantDel {
				t.Fatalf("deleted=%v, want deleted=%v", backend.deleted, tc.wantDel)
			}
		})
	}
}

func TestProviderReconciliationFailurePathsAndOwnershipFiltering(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	t.Run("ownership mismatch never enters inventory", func(t *testing.T) {
		store := &fakeReconciliationStore{}
		backend := &reconcileBackend{name: backends.TypeFlyIO}
		d := &Decider{fly: backend, reconciliation: store}
		summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
		if err != nil {
			t.Fatal(err)
		}
		if summary.Observed != 0 || summary.Quarantined != 0 || len(backend.deleted) != 0 {
			t.Fatalf("non-owned resource was acted on: summary=%+v deleted=%v", summary, backend.deleted)
		}
	})

	t.Run("list failure persists failed reconciliation and does not delete", func(t *testing.T) {
		store := &fakeReconciliationStore{}
		backend := &reconcileBackend{name: backends.TypeFlyIO, listErr: errors.New("list failed")}
		d := &Decider{fly: backend, reconciliation: store}
		summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
		if err != nil {
			t.Fatal(err)
		}
		if store.failures != 1 || summary.Failed != 1 || len(backend.deleted) != 0 {
			t.Fatalf("list failure not fail-closed: failures=%d summary=%+v deleted=%v", store.failures, summary, backend.deleted)
		}
	})

	t.Run("delete failure is recorded and retried", func(t *testing.T) {
		store := &fakeReconciliationStore{orphans: map[string]lifecycle.ProviderOrphanRecord{
			providerResourceKey(backends.TypeFlyIO, "", "orphan-1"): {
				Provider: backends.TypeFlyIO, ProviderResourceID: "orphan-1",
				State: lifecycle.ProviderOrphanQuarantined, FirstObservedAt: now.Add(-time.Hour),
			},
		}}
		backend := &reconcileBackend{name: backends.TypeFlyIO, deleteErr: errors.New("delete failed"), owned: []backends.OwnedNode{{
			BackendID: "orphan-1", Name: "ys-burst-orphan", CreatedAt: now.Add(-time.Hour),
		}}}
		d := &Decider{fly: backend, reconciliation: store}
		_, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
		if err != nil {
			t.Fatal(err)
		}
		if len(store.deleteErr) != 1 || len(store.deleted) != 0 {
			t.Fatalf("delete failure not recorded as retryable: failed=%v deleted=%v", store.deleteErr, store.deleted)
		}
		backend.deleteErr = nil
		_, err = d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now.Add(time.Minute), Grace: time.Minute, Log: slog.Default()})
		if err != nil {
			t.Fatal(err)
		}
		if len(store.deleted) != 1 {
			t.Fatalf("retry did not delete stale orphan: deleted=%v", store.deleted)
		}
	})
}

func TestProviderReconciliationPersistenceFailuresPropagate(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	t.Run("inventory failure receipt", func(t *testing.T) {
		store := &fakeReconciliationStore{failureErr: errors.New("write failed")}
		backend := &reconcileBackend{name: backends.TypeFlyIO, listErr: errors.New("provider payload token=secret")}
		d := &Decider{fly: backend, reconciliation: store}
		_, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
		if err == nil {
			t.Fatal("expected failed inventory receipt write to propagate")
		}
		if store.lastFailure == "" || store.lastFailure == "provider payload token=secret" {
			t.Fatalf("persisted unsafe failure string: %q", store.lastFailure)
		}
	})

	t.Run("success receipt", func(t *testing.T) {
		store := &fakeReconciliationStore{successErr: errors.New("write failed")}
		backend := &reconcileBackend{name: backends.TypeFlyIO}
		d := &Decider{fly: backend, reconciliation: store}
		_, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
		if err == nil {
			t.Fatal("expected failed success receipt write to propagate")
		}
	})

	t.Run("claim receipt", func(t *testing.T) {
		key := providerResourceKey(backends.TypeFlyIO, "", "orphan-1")
		store := &fakeReconciliationStore{
			claimErr: errors.New("claim write failed"),
			orphans: map[string]lifecycle.ProviderOrphanRecord{key: {
				Provider: backends.TypeFlyIO, ProviderResourceID: "orphan-1",
				State: lifecycle.ProviderOrphanQuarantined, FirstObservedAt: now.Add(-time.Hour),
			}},
		}
		backend := &reconcileBackend{name: backends.TypeFlyIO, owned: []backends.OwnedNode{{
			BackendID: "orphan-1", Name: "ys-burst-orphan", CreatedAt: now.Add(-time.Hour),
		}}}
		d := &Decider{fly: backend, reconciliation: store}
		_, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
		if err == nil || len(backend.deleted) != 0 {
			t.Fatalf("claim failure did not fail before provider call: err=%v deleted=%v", err, backend.deleted)
		}
	})

	t.Run("delete failure receipt", func(t *testing.T) {
		key := providerResourceKey(backends.TypeFlyIO, "", "orphan-1")
		store := &fakeReconciliationStore{
			markErr: errors.New("mark failed write"),
			orphans: map[string]lifecycle.ProviderOrphanRecord{key: {
				Provider: backends.TypeFlyIO, ProviderResourceID: "orphan-1",
				State: lifecycle.ProviderOrphanQuarantined, FirstObservedAt: now.Add(-time.Hour),
			}},
		}
		backend := &reconcileBackend{name: backends.TypeFlyIO, deleteErr: errors.New("raw delete secret"), owned: []backends.OwnedNode{{
			BackendID: "orphan-1", Name: "ys-burst-orphan", CreatedAt: now.Add(-time.Hour),
		}}}
		d := &Decider{fly: backend, reconciliation: store}
		_, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
		if err == nil {
			t.Fatal("expected failed delete-failure receipt write to propagate")
		}
	})
}

func TestProviderReconciliationCreateLookupFailureStopsTarget(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 5, 21, 30, 0, 0, time.UTC)
	for _, inventory := range []string{"present", "absent"} {
		for _, receipt := range []string{"recorded", "unavailable"} {
			t.Run(inventory+"/"+receipt, func(t *testing.T) {
				lookupErr := errors.New("synthetic create lookup failure")
				receiptErr := errors.New("synthetic failure receipt outage")
				key := providerResourceKey(backends.TypeFlyIO, "", "ambiguous-resource")
				original := lifecycle.ProviderOrphanRecord{
					Provider: backends.TypeFlyIO, ProviderResourceID: "ambiguous-resource",
					State: lifecycle.ProviderOrphanQuarantined, FirstObservedAt: now.Add(-time.Hour),
				}
				store := &fakeReconciliationStore{
					createListErr: lookupErr,
					orphans:       map[string]lifecycle.ProviderOrphanRecord{key: original},
					// An earlier orphan observation cannot override a current
					// in-flight create whose resource identity is still unknown.
					creates: []lifecycle.ProviderCreateReconciliation{{
						ID: 42, Provider: backends.TypeFlyIO, BurstID: "ambiguous-burst",
						State: lifecycle.OperationProcessing,
					}},
				}
				if receipt == "unavailable" {
					store.failureErr = receiptErr
				}
				backend := &reconcileBackend{name: backends.TypeFlyIO}
				if inventory == "present" {
					backend.owned = []backends.OwnedNode{{BackendID: "ambiguous-resource", BurstID: "ambiguous-burst", CreatedAt: now.Add(-time.Hour)}}
				}
				d := &Decider{fly: backend, reconciliation: store}
				opts := ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()}
				summary, err := d.ReconcileProviders(ctx, opts)
				if !errors.Is(err, lookupErr) {
					t.Errorf("original lookup failure lost: %v", err)
				}
				if receipt == "unavailable" && !errors.Is(err, receiptErr) {
					t.Errorf("failure receipt error lost: %v", err)
				}
				if summary.Failed != 1 || summary.Deleted != 0 || summary.Quarantined != 0 || store.failures != 1 || store.successes != 0 {
					t.Errorf("incomplete correlation reported success: summary=%+v failures=%d successes=%d", summary, store.failures, store.successes)
				}
				current := store.orphans[key]
				if len(backend.deleted) != 0 || len(store.deleted) != 0 || current.State != original.State || current.DeletedAt != nil || current.Attempts != 0 || current.Observations != 0 {
					t.Errorf("lookup failure allowed orphan mutation: provider_deletes=%d recorded_deletes=%d state=%s", len(backend.deleted), len(store.deleted), current.State)
				}
				// A subsequent healthy read may reconcile normally; this is a
				// per-attempt failure, not a permanently disabled sweeper.
				store.createListErr, store.failureErr = nil, nil
				if inventory == "present" {
					summary, err = d.ReconcileProviders(ctx, opts)
					if err != nil || summary.Failed != 0 || len(backend.deleted) != 0 || store.successes != 1 {
						t.Errorf("recovery did not protect the live create: summary=%+v err=%v", summary, err)
					}
				}
			})
		}
	}
}

func TestProviderReconciliationAbsenceRecoveryOnlyAfterSuccessfulInventory(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	t.Run("deleting row absent from successful inventory gets terminal receipt", func(t *testing.T) {
		key := providerResourceKey(backends.TypeFlyIO, "", "missing-after-delete")
		store := &fakeReconciliationStore{orphans: map[string]lifecycle.ProviderOrphanRecord{key: {
			Provider: backends.TypeFlyIO, ProviderResourceID: "missing-after-delete",
			State: lifecycle.ProviderOrphanDeleting, FirstObservedAt: now.Add(-time.Hour),
		}}}
		backend := &reconcileBackend{name: backends.TypeFlyIO}
		d := &Decider{fly: backend, reconciliation: store}
		summary, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
		if err != nil {
			t.Fatal(err)
		}
		if summary.Deleted != 1 || store.orphans[key].State != lifecycle.ProviderOrphanDeleted {
			t.Fatalf("absence recovery failed: summary=%+v orphan=%+v", summary, store.orphans[key])
		}
	})

	t.Run("list failure makes no absence transition", func(t *testing.T) {
		key := providerResourceKey(backends.TypeFlyIO, "", "unknown-inventory")
		store := &fakeReconciliationStore{orphans: map[string]lifecycle.ProviderOrphanRecord{key: {
			Provider: backends.TypeFlyIO, ProviderResourceID: "unknown-inventory",
			State: lifecycle.ProviderOrphanDeleting, FirstObservedAt: now.Add(-time.Hour),
		}}}
		backend := &reconcileBackend{name: backends.TypeFlyIO, listErr: errors.New("list failed")}
		d := &Decider{fly: backend, reconciliation: store}
		_, err := d.ReconcileProviders(context.Background(), ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
		if err != nil {
			t.Fatal(err)
		}
		if store.orphans[key].State == lifecycle.ProviderOrphanDeleted {
			t.Fatal("absence transition ran after a failed inventory")
		}
	})
}

func TestProviderReconciliationProviderAccountIsolation(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	store := &fakeReconciliationStore{protected: []lifecycle.ProviderResourceRef{{
		Provider: backends.TypeLinode, CloudAccountID: "account-a", ProviderResourceID: "same-id",
	}}, orphans: map[string]lifecycle.ProviderOrphanRecord{
		providerResourceKey(backends.TypeLinode, "account-b", "same-id"): {
			Provider: backends.TypeLinode, CloudAccountID: "account-b", ProviderResourceID: "same-id",
			State: lifecycle.ProviderOrphanQuarantined, FirstObservedAt: now.Add(-time.Hour),
		},
	}}
	backend := &reconcileBackend{name: backends.TypeLinode, owned: []backends.OwnedNode{{
		BackendID: "same-id", Name: "ys-burst-other-account", CreatedAt: now.Add(-time.Hour),
	}}}
	d := &Decider{reconciliation: store}
	protected, err := d.protectedProviderResources(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	summary, err := d.reconcileProviderTarget(context.Background(),
		providerInventoryTarget{provider: backends.TypeLinode, cloudAccountID: "account-b", backend: backend},
		protected, ProviderReconcileOptions{Now: now, Grace: time.Minute, Log: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Deleted != 1 || len(backend.deleted) != 1 {
		t.Fatalf("provider/account isolation failed: summary=%+v deleted=%v", summary, backend.deleted)
	}
}
