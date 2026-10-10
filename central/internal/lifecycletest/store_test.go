package lifecycletest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/internal/testkit"
)

// methodCase names one wrapped store method and the checkpoint pair it must
// place. invoke discards results and returns only the error, which is all the
// wrapper is responsible for propagating.
type methodCase struct {
	name   string
	before testkit.Checkpoint
	after  testkit.Checkpoint
	invoke func(context.Context, *Store) error
}

func methodCases() []methodCase {
	retryAt := func() time.Time { return time.Now().Add(time.Minute) }
	return []methodCase{
		{
			name:   "AdmitWorkload",
			before: testkit.BeforeAdmission,
			after:  testkit.AfterAdmission,
			invoke: func(ctx context.Context, s *Store) error {
				_, err := s.AdmitWorkload(ctx, lifecycle.AdmissionRequest{})
				return err
			},
		},
		{
			name:   "ClaimProviderCreate",
			before: testkit.BeforeProviderCreateClaim,
			after:  testkit.AfterProviderCreateClaim,
			invoke: func(ctx context.Context, s *Store) error {
				_, _, err := s.ClaimProviderCreate(ctx, time.Minute)
				return err
			},
		},
		{
			name:   "MarkProviderCreateSucceeded",
			before: testkit.BeforeProviderCreateMark,
			after:  testkit.AfterProviderCreateMark,
			invoke: func(ctx context.Context, s *Store) error {
				return s.MarkProviderCreateSucceeded(ctx, 1, "lease", "linode_1")
			},
		},
		{
			name:   "MarkProviderCreateFailed",
			before: testkit.BeforeProviderCreateMark,
			after:  testkit.AfterProviderCreateMark,
			invoke: func(ctx context.Context, s *Store) error {
				return s.MarkProviderCreateFailed(ctx, 1, "lease", "boom", retryAt(), 3)
			},
		},
		{
			name:   "RequestProviderDelete",
			before: testkit.BeforeProviderDeleteRequest,
			after:  testkit.AfterProviderDeleteRequest,
			invoke: func(ctx context.Context, s *Store) error {
				_, err := s.RequestProviderDelete(ctx, lifecycle.ProviderDeleteRequest{})
				return err
			},
		},
		{
			name:   "ClaimProviderDelete",
			before: testkit.BeforeProviderDeleteClaim,
			after:  testkit.AfterProviderDeleteClaim,
			invoke: func(ctx context.Context, s *Store) error {
				_, _, err := s.ClaimProviderDelete(ctx, time.Minute)
				return err
			},
		},
		{
			name:   "MarkProviderDeleteSucceeded",
			before: testkit.BeforeProviderDeleteMark,
			after:  testkit.AfterProviderDeleteMark,
			invoke: func(ctx context.Context, s *Store) error {
				_, err := s.MarkProviderDeleteSucceeded(ctx, 1, "lease")
				return err
			},
		},
		{
			name:   "MarkProviderDeleteFailed",
			before: testkit.BeforeProviderDeleteMark,
			after:  testkit.AfterProviderDeleteMark,
			invoke: func(ctx context.Context, s *Store) error {
				return s.MarkProviderDeleteFailed(ctx, 1, "lease", "boom", retryAt(), 3)
			},
		},
	}
}

// unreadyStore is the real lifecycle.Store with no pool. Every method rejects
// on assertReady, which is a real store error and lets these tests prove
// checkpoint placement and error propagation without a database.
func unreadyStore() *lifecycle.Store {
	return lifecycle.NewStore(nil)
}

func TestWrappedMethodsPropagateStoreErrorsBeforeAfterCommitCheckpoint(t *testing.T) {
	ctx := context.Background()
	for _, test := range methodCases() {
		t.Run(test.name, func(t *testing.T) {
			controller := mustController(t, testkit.CrashPlan{})
			store := mustStore(t, controller)

			err := test.invoke(ctx, store)
			// Disarmed: the store's own error reaches the caller unchanged.
			if !errors.Is(err, lifecycle.ErrInvalidArgument) {
				t.Fatalf("err = %v, want the store's own error", err)
			}
			if errors.Is(err, testkit.ErrInjectedCrash) {
				t.Fatal("disarmed controller injected a crash")
			}
			order := controller.Order()
			if len(order) != 1 || order[0] != test.before {
				t.Fatalf("checkpoints = %v, want only [%s]", order, test.before)
			}
		})
	}
}

func TestCrashBeforeAStoreCallShortCircuitsIt(t *testing.T) {
	ctx := context.Background()
	for _, test := range methodCases() {
		t.Run(test.name, func(t *testing.T) {
			controller := mustController(t, testkit.CrashPlan{
				Checkpoint: test.before,
				Occurrence: 1,
			})
			store := mustStore(t, controller)

			err := test.invoke(ctx, store)
			if !errors.Is(err, testkit.ErrInjectedCrash) {
				t.Fatalf("err = %v, want ErrInjectedCrash", err)
			}
			if errors.Is(err, lifecycle.ErrInvalidArgument) {
				t.Fatal("store was called despite crashing before it")
			}
			if controller.Count(test.after) != 0 {
				t.Fatalf("after checkpoint ran %d times", controller.Count(test.after))
			}
		})
	}
}

func TestAfterCommitCrashDoesNotMaskStoreFailure(t *testing.T) {
	ctx := context.Background()
	for _, test := range methodCases() {
		t.Run(test.name, func(t *testing.T) {
			controller := mustController(t, testkit.CrashPlan{
				Checkpoint: test.after,
				Occurrence: 1,
			})
			store := mustStore(t, controller)

			err := test.invoke(ctx, store)
			if !errors.Is(err, lifecycle.ErrInvalidArgument) {
				t.Fatalf("err = %v, want the store failure", err)
			}
			if errors.Is(err, testkit.ErrInjectedCrash) {
				t.Fatal("after-commit checkpoint masked an operation that did not commit")
			}
			if controller.Count(test.before) != 1 || controller.Count(test.after) != 0 {
				t.Fatalf("checkpoints = %+v", controller.Hits())
			}
		})
	}
}

func TestMarkMethodsShareOneCheckpointPairPerOperation(t *testing.T) {
	byMethod := make(map[string]methodCase)
	for _, test := range methodCases() {
		byMethod[test.name] = test
	}
	for _, pair := range [][2]string{
		{"MarkProviderCreateSucceeded", "MarkProviderCreateFailed"},
		{"MarkProviderDeleteSucceeded", "MarkProviderDeleteFailed"},
	} {
		success, failure := byMethod[pair[0]], byMethod[pair[1]]
		if success.before == "" || success.after == "" {
			t.Fatalf("%s has no checkpoint pair", pair[0])
		}
		// A crash "after the mark" must be indistinguishable whichever
		// terminal outcome the worker was recording.
		if success.before != failure.before || success.after != failure.after {
			t.Fatalf("%s uses %s/%s but %s uses %s/%s",
				pair[0], success.before, success.after,
				pair[1], failure.before, failure.after)
		}
	}
}

func TestWrapperIsSafeUnderConcurrentUse(t *testing.T) {
	ctx := context.Background()
	controller := mustController(t, testkit.CrashPlan{})
	store := mustStore(t, controller)
	cases := methodCases()

	var wg sync.WaitGroup
	for _, test := range cases {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				if err := test.invoke(ctx, store); !errors.Is(err, lifecycle.ErrInvalidArgument) {
					t.Errorf("%s: %v", test.name, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	hits := controller.Hits()
	if want := len(cases) * 10; len(hits) != want {
		t.Fatalf("hits = %d, want %d", len(hits), want)
	}
	seen := make(map[testkit.Checkpoint]int)
	for i, hit := range hits {
		if hit.Sequence != i+1 {
			t.Fatalf("hit %d sequence = %d", i, hit.Sequence)
		}
		seen[hit.Checkpoint]++
		if hit.Occurrence != seen[hit.Checkpoint] {
			t.Fatalf("hit %d = %+v, want occurrence %d", i, hit, seen[hit.Checkpoint])
		}
	}
	if controller.Fired() {
		t.Fatal("disarmed controller fired")
	}
}

func TestNewStoreRejectsMissingCollaborators(t *testing.T) {
	if _, err := NewStore(nil, mustController(t, testkit.CrashPlan{})); err == nil {
		t.Fatal("nil lifecycle store was accepted")
	}
	if _, err := NewStore(unreadyStore(), nil); err == nil {
		t.Fatal("nil crash controller was accepted")
	}
}

func TestInnerExposesTheRealStore(t *testing.T) {
	inner := unreadyStore()
	controller := mustController(t, testkit.CrashPlan{})
	store, err := NewStore(inner, controller)
	if err != nil {
		t.Fatal(err)
	}
	if store.Inner() != inner {
		t.Fatal("Inner did not return the wrapped store")
	}
	if store.Crash() != controller {
		t.Fatal("Crash did not return the wrapped controller")
	}
	// Read-only store methods reached through Inner carry no checkpoints.
	if _, err := store.Inner().GetProviderDelete(context.Background(), "c", "k", "b"); err == nil {
		t.Fatal("unready store accepted a read")
	}
	if len(controller.Hits()) != 0 {
		t.Fatalf("Inner access recorded checkpoints: %+v", controller.Hits())
	}
}

func mustController(t *testing.T, plan testkit.CrashPlan) *testkit.CrashController {
	t.Helper()
	controller, err := testkit.NewCrashController(plan)
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func mustStore(t *testing.T, controller *testkit.CrashController) *Store {
	t.Helper()
	store, err := NewStore(unreadyStore(), controller)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
