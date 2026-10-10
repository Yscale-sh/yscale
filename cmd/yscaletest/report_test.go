package main

import (
	"testing"
	"time"
)

func TestAnyFailedRejectsNonemptyErrEvenWhenPhaseSucceededAndReaped(t *testing.T) {
	tests := []struct {
		name string
		r    CaseResult
		want bool
	}{
		{
			name: "clean pass",
			r:    CaseResult{Phase: "Succeeded", Reaped: true},
			want: false,
		},
		{
			name: "failed phase",
			r:    CaseResult{Phase: "Failed", Reaped: true},
			want: true,
		},
		{
			name: "not reaped",
			r:    CaseResult{Phase: "Succeeded", Reaped: false, ReapErr: "node still present"},
			want: true,
		},
		{
			name: "succeeded and reaped but workload Err is set",
			r:    CaseResult{Phase: "Succeeded", Reaped: true, Err: "GPU verification: container ran without GPU"},
			want: true,
		},
		{
			name: "reaped true with nonempty ReapErr is inconsistent and fails",
			r:    CaseResult{Phase: "Succeeded", Reaped: true, ReapErr: "provider audit timed out"},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := anyFailed([]CaseResult{tc.r})
			if got != tc.want {
				t.Fatalf("anyFailed = %v, want %v for %+v", got, tc.want, tc.r)
			}
		})
	}
}

func TestCasePassedRequiresAllFourConditions(t *testing.T) {
	clean := CaseResult{Phase: "Succeeded", Reaped: true, Err: "", ReapErr: ""}
	if !casePassed(clean) {
		t.Fatal("a clean result should pass")
	}
	tests := []struct {
		name   string
		mutate func(*CaseResult)
	}{
		{"Phase not Succeeded", func(r *CaseResult) { r.Phase = "Failed" }},
		{"Reaped false", func(r *CaseResult) { r.Reaped = false }},
		{"Err nonempty", func(r *CaseResult) { r.Err = "GPU verification failure" }},
		{"ReapErr nonempty", func(r *CaseResult) { r.ReapErr = "provider residue" }},
		{"ArtifactResult failed", func(r *CaseResult) { r.ArtifactResult = "failed" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := clean
			tc.mutate(&r)
			if casePassed(r) {
				t.Fatalf("casePassed should be false when %s", tc.name)
			}
		})
	}
}

func TestErrorTextMergesWorkloadAndReapErrors(t *testing.T) {
	tests := []struct {
		name string
		r    CaseResult
		want string
	}{
		{
			name: "workload error only",
			r:    CaseResult{Err: "exit code 1"},
			want: "exit code 1",
		},
		{
			name: "reap error only",
			r:    CaseResult{ReapErr: "node still present"},
			want: "reap: node still present",
		},
		{
			name: "both errors",
			r:    CaseResult{Err: "exit code 1", ReapErr: "node still present"},
			want: "exit code 1 | reap: node still present",
		},
		{
			name: "no errors",
			r:    CaseResult{},
			want: "",
		},
		{
			name: "evidence failed only",
			r:    CaseResult{ArtifactResult: "failed"},
			want: "evidence: failed",
		},
		{
			name: "workload error with evidence failure",
			r:    CaseResult{Err: "exit code 1", ArtifactResult: "failed"},
			want: "exit code 1 | evidence: failed",
		},
		{
			name: "all three error sources",
			r:    CaseResult{Err: "exit code 1", ReapErr: "node still present", ArtifactResult: "failed"},
			want: "exit code 1 | reap: node still present | evidence: failed",
		},
		{
			name: "evidence passed is not an error",
			r:    CaseResult{ArtifactResult: "passed"},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.r.errorText(); got != tc.want {
				t.Fatalf("errorText = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestShortDurationPreservesMilliseconds(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want string
	}{
		{name: "sub-second reap", in: 431*time.Millisecond + 900*time.Microsecond, want: "431ms"},
		{name: "wall time", in: 3*time.Minute + 10*time.Second + 400*time.Millisecond, want: "3m10s"},
		{name: "zero", in: 0, want: "0s"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shortDuration(tc.in); got != tc.want {
				t.Fatalf("shortDuration(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
