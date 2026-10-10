package testkit

import (
	"errors"
	"fmt"
	"sync"
)

// ErrInjectedCrash marks a simulated process death at a named checkpoint. A
// caller that observes it has learned nothing about the step it attempted: the
// step may have committed, may have reached a provider, or may never have
// started. Recovery must be driven from durable state, never from this error.
var ErrInjectedCrash = errors.New("testkit: injected crash")

// Checkpoint names one exact position on a workload's real path. Every
// position has a before/after pair so a test can separate "never ran" from
// "ran, result withheld" — the two cases durable convergence must tell apart.
//
// Checkpoints are names only. This package attaches no lifecycle meaning to
// them: it does not know which store transition or provider call sits between
// a pair, and it never decides what a crash implies for that work.
type Checkpoint string

const (
	BeforeAdmission Checkpoint = "before_admission"
	AfterAdmission  Checkpoint = "after_admission"

	BeforeProviderCreateClaim Checkpoint = "before_provider_create_claim"
	AfterProviderCreateClaim  Checkpoint = "after_provider_create_claim"
	BeforeProviderCreateCall  Checkpoint = "before_provider_create_call"
	AfterProviderCreateCall   Checkpoint = "after_provider_create_call"
	BeforeProviderCreateMark  Checkpoint = "before_provider_create_mark"
	AfterProviderCreateMark   Checkpoint = "after_provider_create_mark"

	BeforeCommandDispatch Checkpoint = "before_command_dispatch"
	AfterCommandDispatch  Checkpoint = "after_command_dispatch"

	BeforeWorkloadCompletion Checkpoint = "before_workload_completion"
	AfterWorkloadCompletion  Checkpoint = "after_workload_completion"

	BeforeProviderDeleteRequest Checkpoint = "before_provider_delete_request"
	AfterProviderDeleteRequest  Checkpoint = "after_provider_delete_request"
	BeforeProviderDeleteClaim   Checkpoint = "before_provider_delete_claim"
	AfterProviderDeleteClaim    Checkpoint = "after_provider_delete_claim"
	BeforeProviderDeleteCall    Checkpoint = "before_provider_delete_call"
	AfterProviderDeleteCall     Checkpoint = "after_provider_delete_call"
	BeforeProviderDeleteMark    Checkpoint = "before_provider_delete_mark"
	AfterProviderDeleteMark     Checkpoint = "after_provider_delete_mark"

	BeforeSettlement Checkpoint = "before_settlement"
	AfterSettlement  Checkpoint = "after_settlement"
)

// checkpointOrder is the canonical path order, used for validation and for
// tests that assert a run visited positions in the expected sequence.
var checkpointOrder = []Checkpoint{
	BeforeAdmission, AfterAdmission,
	BeforeProviderCreateClaim, AfterProviderCreateClaim,
	BeforeProviderCreateCall, AfterProviderCreateCall,
	BeforeProviderCreateMark, AfterProviderCreateMark,
	BeforeCommandDispatch, AfterCommandDispatch,
	BeforeWorkloadCompletion, AfterWorkloadCompletion,
	BeforeProviderDeleteRequest, AfterProviderDeleteRequest,
	BeforeProviderDeleteClaim, AfterProviderDeleteClaim,
	BeforeProviderDeleteCall, AfterProviderDeleteCall,
	BeforeProviderDeleteMark, AfterProviderDeleteMark,
	BeforeSettlement, AfterSettlement,
}

var checkpointSet = func() map[Checkpoint]struct{} {
	set := make(map[Checkpoint]struct{}, len(checkpointOrder))
	for _, checkpoint := range checkpointOrder {
		set[checkpoint] = struct{}{}
	}
	return set
}()

// Checkpoints returns the canonical path order as a fresh slice.
func Checkpoints() []Checkpoint {
	return append([]Checkpoint(nil), checkpointOrder...)
}

// ValidCheckpoint reports whether name is one of the declared checkpoints.
func ValidCheckpoint(name Checkpoint) bool {
	_, ok := checkpointSet[name]
	return ok
}

// CrashPlan arms at most one crash. A zero plan records hits without ever
// crashing, which is what a restarted process starts from.
type CrashPlan struct {
	// Checkpoint is the position that crashes. Empty disarms the controller.
	Checkpoint Checkpoint
	// Occurrence is the 1-based hit at Checkpoint that crashes. A later hit at
	// the same checkpoint proceeds normally, so recovery is never blocked by a
	// crash the test already spent.
	Occurrence int
}

// Hit is one recorded checkpoint entry. Sequence is assigned under the
// controller lock, so the hit log is a total order even when concurrent
// goroutines enter checkpoints at the same time.
type Hit struct {
	Sequence   int
	Checkpoint Checkpoint
	Occurrence int
	Crashed    bool
}

// CrashController injects one crash at one named checkpoint and records every
// checkpoint the run passed through. It is safe for concurrent use.
//
// The controller owns no lifecycle state and performs no recovery. It answers
// exactly one question — "does this position crash now?" — and leaves what a
// crash means to the durable store the caller is exercising.
type CrashController struct {
	mu     sync.Mutex
	plan   CrashPlan
	fired  bool
	counts map[Checkpoint]int
	hits   []Hit
}

// NewCrashController validates the plan and returns a disarmed-or-armed
// controller. An unknown checkpoint is rejected rather than silently never
// firing, so a typo cannot turn a crash test into a no-op that passes.
func NewCrashController(plan CrashPlan) (*CrashController, error) {
	if plan.Checkpoint == "" {
		if plan.Occurrence != 0 {
			return nil, fmt.Errorf("occurrence %d requires a checkpoint", plan.Occurrence)
		}
	} else {
		if !ValidCheckpoint(plan.Checkpoint) {
			return nil, fmt.Errorf("unknown checkpoint %q", plan.Checkpoint)
		}
		if plan.Occurrence < 1 {
			return nil, fmt.Errorf("checkpoint %q requires a 1-based occurrence", plan.Checkpoint)
		}
	}
	return &CrashController{plan: plan, counts: make(map[Checkpoint]int)}, nil
}

// Enter records a checkpoint hit and returns ErrInjectedCrash when this is the
// exact configured occurrence. It fires at most once for the lifetime of the
// controller.
func (c *CrashController) Enter(checkpoint Checkpoint) error {
	if !ValidCheckpoint(checkpoint) {
		return fmt.Errorf("testkit: unknown checkpoint %q", checkpoint)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counts[checkpoint]++
	occurrence := c.counts[checkpoint]
	crashed := !c.fired &&
		checkpoint == c.plan.Checkpoint &&
		occurrence == c.plan.Occurrence
	if crashed {
		c.fired = true
	}
	c.hits = append(c.hits, Hit{
		Sequence:   len(c.hits) + 1,
		Checkpoint: checkpoint,
		Occurrence: occurrence,
		Crashed:    crashed,
	})
	if crashed {
		return fmt.Errorf("%w at %s occurrence %d", ErrInjectedCrash, checkpoint, occurrence)
	}
	return nil
}

// Restart returns the controller a restarted process would construct. It
// deliberately shares nothing with the receiver — no hit log, no occurrence
// counts, no spent one-shot — so a test cannot let in-memory state survive the
// crash it is trying to prove is survivable. The durable store and the
// provider account are what carry across; this does not.
func (*CrashController) Restart(plan CrashPlan) (*CrashController, error) {
	return NewCrashController(plan)
}

// Hits returns a copy of the ordered hit log.
func (c *CrashController) Hits() []Hit {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Hit(nil), c.hits...)
}

// Order returns the checkpoints in the order they were entered.
func (c *CrashController) Order() []Checkpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	order := make([]Checkpoint, len(c.hits))
	for i, hit := range c.hits {
		order[i] = hit.Checkpoint
	}
	return order
}

// Count returns how many times checkpoint was entered.
func (c *CrashController) Count(checkpoint Checkpoint) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[checkpoint]
}

// Fired reports whether the armed crash has been spent.
func (c *CrashController) Fired() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fired
}
