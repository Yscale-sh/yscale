// yscale:proprietary

package billingcanary

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ScenarioOutcome is the fixed vocabulary the canary emits per scenario. Any
// other free-form status is a bug: operators triage by these tokens alone.
type ScenarioOutcome string

const (
	OutcomePass    ScenarioOutcome = "pass"
	OutcomeFail    ScenarioOutcome = "fail"
	OutcomeSkipped ScenarioOutcome = "skipped"
)

// ScenarioResult is one row in the canary evidence file. Fields are the
// closed allowlist — no free-form payload — and every string is sanitised via
// SafeNote before it lands here.
type ScenarioResult struct {
	Name       string          `json:"name"`
	Outcome    ScenarioOutcome `json:"outcome"`
	DurationMS int64           `json:"duration_ms"`
	Attempts   int             `json:"attempts,omitempty"`
	// Note is a bounded human sentence with no provider identifiers. Anything
	// that would leak an ID becomes "REDACTED" at ingest time.
	Note string `json:"note,omitempty"`
	// Counts is a bounded map of small integer facts (e.g. webhook attempts,
	// ledger rows). Keys must be short lower-snake identifiers.
	Counts map[string]int64 `json:"counts,omitempty"`
	// Flags carries booleans the operator specifically needs (e.g. kill switch
	// engaged). Same key convention as Counts.
	Flags map[string]bool `json:"flags,omitempty"`
}

// Evidence is the full canary emission. It is the sole product of a run.
//
// Healthy means no scenario failed; skipped scenarios do not clear it, because
// a deterministic-only run is legitimate. Complete means every scenario ran
// and passed — only a --live run against real Stripe cash can produce it. An
// operator reading a report should require Healthy && Complete before treating
// the canary as evidence that live billing works end to end.
type Evidence struct {
	SchemaVersion int              `json:"schema_version"`
	StartedAt     time.Time        `json:"started_at"`
	FinishedAt    time.Time        `json:"finished_at"`
	Mode          string           `json:"mode"` // "test" always; livemode canaries are rejected by LoadConfig.
	Healthy       bool             `json:"healthy"`
	Complete      bool             `json:"complete"`
	Scenarios     []ScenarioResult `json:"scenarios"`
}

// EvidenceSink is a bounded, allowlist-validated collector. Callers append
// results as scenarios complete; the sink refuses any note or count key that
// fails redaction so a stray provider identifier cannot be committed.
type EvidenceSink struct {
	startedAt time.Time
	scenarios []ScenarioResult
}

func NewEvidenceSink(startedAt time.Time) *EvidenceSink {
	return &EvidenceSink{startedAt: startedAt.UTC()}
}

// Add validates every allowlist field of the result before appending. Any
// forbidden value is a hard error — the run must not accept a smuggled ID.
func (s *EvidenceSink) Add(result ScenarioResult) error {
	if s == nil {
		return errors.New("billingcanary: nil evidence sink")
	}
	if err := Sanitise(result.Name); err != nil {
		return fmt.Errorf("billingcanary: scenario name: %w", err)
	}
	if result.Name == "" {
		return errors.New("billingcanary: scenario name is required")
	}
	switch result.Outcome {
	case OutcomePass, OutcomeFail, OutcomeSkipped:
	default:
		return fmt.Errorf("billingcanary: scenario %q outcome must be pass|fail|skipped", result.Name)
	}
	if result.DurationMS < 0 {
		return fmt.Errorf("billingcanary: scenario %q duration must be non-negative", result.Name)
	}
	if err := Sanitise(result.Note); err != nil {
		return fmt.Errorf("billingcanary: scenario %q note: %w", result.Name, err)
	}
	for key := range result.Counts {
		if err := validateAllowlistKey(key); err != nil {
			return fmt.Errorf("billingcanary: scenario %q count key: %w", result.Name, err)
		}
	}
	for key := range result.Flags {
		if err := validateAllowlistKey(key); err != nil {
			return fmt.Errorf("billingcanary: scenario %q flag key: %w", result.Name, err)
		}
	}
	s.scenarios = append(s.scenarios, result)
	return nil
}

// Snapshot materialises the current evidence with results sorted by scenario
// name. Repeatable output is the whole point: an operator diffs two runs.
func (s *EvidenceSink) Snapshot(finishedAt time.Time) Evidence {
	sorted := append([]ScenarioResult(nil), s.scenarios...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	healthy, complete := true, true
	for _, r := range sorted {
		switch r.Outcome {
		case OutcomeFail:
			healthy = false
			complete = false
		case OutcomeSkipped:
			complete = false
		}
	}
	return Evidence{
		SchemaVersion: 1,
		StartedAt:     s.startedAt,
		FinishedAt:    finishedAt.UTC(),
		Mode:          "test",
		Healthy:       healthy,
		Complete:      complete,
		Scenarios:     sorted,
	}
}

// WriteFile serialises the evidence at the caller-supplied path. The bytes
// are size-capped at MaxEvidenceBytes; a snapshot that would exceed it is a
// bug and is refused (rather than silently truncated). The file is written
// with restrictive permissions inside the caller's temp directory.
func WriteFile(path string, evidence Evidence) error {
	body, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return fmt.Errorf("billingcanary: encode evidence: %w", err)
	}
	if len(body) > MaxEvidenceBytes {
		return fmt.Errorf("billingcanary: evidence payload %d bytes exceeds cap %d", len(body), MaxEvidenceBytes)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("billingcanary: prepare evidence dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("billingcanary: open evidence file: %w", err)
	}
	defer f.Close()
	if _, err := io.WriteString(f, string(body)+"\n"); err != nil {
		return fmt.Errorf("billingcanary: write evidence: %w", err)
	}
	return nil
}

func validateAllowlistKey(key string) error {
	if key == "" {
		return errors.New("empty key")
	}
	if len(key) > 40 {
		return errors.New("key too long")
	}
	for _, r := range key {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return fmt.Errorf("key %q must be lower-snake ASCII", key)
		}
	}
	if strings.HasPrefix(key, "_") || strings.HasSuffix(key, "_") {
		return fmt.Errorf("key %q must not begin or end with _", key)
	}
	return nil
}
