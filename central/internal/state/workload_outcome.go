package state

// Closed results a run outcome may report. They are STABLE strings, stored on
// every receipt and rendered to the customer, so a consumer reading an old
// record can tell what was claimed without a version table.
const (
	WorkloadResultSucceeded = "succeeded"
	WorkloadResultFailed    = "failed"
	// WorkloadResultSkipped belongs to the artifact export alone: the export was
	// configured and deliberately not attempted — most often because the compute
	// failed and there was nothing worth uploading. Compute is never skipped;
	// a workload that ran has a result either way.
	WorkloadResultSkipped = "skipped"
)

// WorkloadOutcome is the agent's receipt for ONE terminal observation: what the
// compute Job did, and — separately — what the artifact export did. It is the
// difference between "this run is finished" (Status, FinishedAt) and "this is
// what finishing meant", which the terminal status alone cannot carry: a run
// whose container exited 0 and whose outputs never reached the customer's
// bucket is not the same event as one where both worked.
//
// nil means NO RECEIPT — a legacy record, a workload still running, or an agent
// that reported only the terminal phase. It is a pointer for that reason: a
// zero-valued receipt rendered on those records would read as "the compute
// reported an empty result", which is a different claim from "nobody reported".
//
// Written once, by the terminal observation that carried it, and never mutated
// in place afterwards — readers hold copies (cloneWorkload), and the store
// replaces the pointer rather than editing the record behind it.
//
// What it deliberately does NOT hold: bucket names, prefixes, endpoints, secret
// references, signed URLs, or raw logs. Every one of those is
// submitter-controlled or credential-bearing, and none of them is needed to say
// what happened.
type WorkloadOutcome struct {
	// Compute is the observed terminal result of the workload's Job. Always
	// present on a stored receipt: a receipt with nothing to say about the
	// compute is not an observation of this run.
	Compute WorkloadComputeOutcome
	// Artifacts is the observed result of the artifact export, present only when
	// export was configured or attempted. nil is "no export in play", which is
	// not the same as an export that ran and uploaded nothing.
	Artifacts *WorkloadArtifactOutcome `json:",omitempty"`
}

// WorkloadComputeOutcome is what the Job itself did — WorkloadResultSucceeded
// or WorkloadResultFailed.
type WorkloadComputeOutcome struct {
	Result string
	// Reason is a short, bounded, human-facing note the agent observed (the Job
	// condition's message, say). Empty when the agent offered none; the handler
	// caps its length before it is ever stored.
	Reason string `json:",omitempty"`
}

// WorkloadArtifactOutcome is what the artifact export did, recorded separately
// from the compute so the honest case — the Job succeeded, the upload did not —
// is representable rather than collapsed into one verdict.
type WorkloadArtifactOutcome struct {
	// Result is WorkloadResultSucceeded, WorkloadResultFailed, or
	// WorkloadResultSkipped.
	Result string
	Reason string `json:",omitempty"`
	// ObjectsUploaded and BytesUploaded are what the export actually moved,
	// never negative. Pointers because "the agent did not count" and "the agent
	// counted zero" are different facts: a succeeded export that uploaded
	// nothing is a real, useful observation, and a stored 0 standing in for an
	// absent count would erase the distinction.
	ObjectsUploaded *int64 `json:",omitempty"`
	BytesUploaded   *int64 `json:",omitempty"`
}

// cloneWorkloadOutcome deep-copies a receipt so no caller — inside the store or
// out — retains a pointer into a stored record. The counts are pointers, so a
// struct copy alone would hand back shared, writable ints.
func cloneWorkloadOutcome(o *WorkloadOutcome) *WorkloadOutcome {
	if o == nil {
		return nil
	}
	cp := *o
	if o.Artifacts != nil {
		artifacts := *o.Artifacts
		artifacts.ObjectsUploaded = cloneCount(o.Artifacts.ObjectsUploaded)
		artifacts.BytesUploaded = cloneCount(o.Artifacts.BytesUploaded)
		cp.Artifacts = &artifacts
	}
	return &cp
}

func cloneCount(n *int64) *int64 {
	if n == nil {
		return nil
	}
	v := *n
	return &v
}
