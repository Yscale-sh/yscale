package agent

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/yscale-sh/yscale/pkg/workload"
)

// Closed result strings a receipt may report. They match central's stored
// vocabulary exactly; anything else is a 400 that would strand the burst.
const (
	OutcomeResultSucceeded = "succeeded"
	OutcomeResultFailed    = "failed"
	// OutcomeResultSkipped belongs to the artifact export alone: export was
	// configured and deliberately not attempted. Compute is never skipped.
	OutcomeResultSkipped = "skipped"
)

// completeRequest is the body of POST /v1/workloads/{id}/complete. Central
// decodes it with unknown fields rejected at every object level, so these tags
// are the contract rather than a convention. Outcome is omitted entirely when
// the agent observed nothing it can honestly report — the body every older
// connector sends, and the one central still stores no receipt for.
type completeRequest struct {
	Phase   string           `json:"phase"`
	Outcome *WorkloadOutcome `json:"outcome,omitempty"`
}

// WorkloadOutcome is the agent's receipt for one terminal observation: what the
// compute Job did, and — separately — what the artifact export did. The two are
// never collapsed: a Job whose container exited 0 and whose outputs never
// reached the customer's bucket is not the same event as one where both worked,
// and the phase alone cannot say which happened.
type WorkloadOutcome struct {
	Compute ComputeOutcome `json:"compute"`
	// Artifacts is present only when export was configured. Absent means "no
	// export in play", which is not the same as an export that moved nothing.
	Artifacts *ArtifactOutcome `json:"artifacts,omitempty"`
}

// ComputeOutcome is the workload Job's own terminal result.
type ComputeOutcome struct {
	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`
}

// ArtifactOutcome is the artifact export's result. Nothing about the
// destination travels with it — bucket, prefix, endpoint, secret reference and
// signed URL are all submitter-controlled or credential-bearing, and none is
// needed to say whether the upload worked.
type ArtifactOutcome struct {
	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`
	// Counts are pointers so "did not count" stays distinct from "counted
	// zero". Only ever set from what an upload actually completed.
	ObjectsUploaded *int64 `json:"objects_uploaded,omitempty"`
	BytesUploaded   *int64 `json:"bytes_uploaded,omitempty"`
}

// Closed category tokens for a failed export. They are what the uploader puts
// in its termination summary and what the watcher maps to a stored reason; the
// tokens themselves never reach central.
const (
	artifactFailureConfiguration = "configuration"
	artifactFailureSigner        = "signer"
	artifactFailureUpload        = "upload"
	artifactFailureTimeout       = "timeout"
	artifactFailureInterrupted   = "interrupted"
	artifactFailureJob           = "job"
)

// Every human-facing note the agent can put on a receipt is one of these fixed
// phrases. Nothing is formatted from a bucket, endpoint, object key, signed
// URL, secret reference, server response, or log line, so no such value can
// reach a record that outlives the run. TestOutcomeReasonsAreSafe pins that.
var artifactFailureReasons = map[string]string{
	artifactFailureConfiguration: "the artifact export could not be prepared",
	artifactFailureSigner:        "the uploader could not obtain signed upload URLs",
	artifactFailureUpload:        "an artifact upload to the object store failed",
	artifactFailureTimeout:       "the artifact upload did not finish before its deadline",
	artifactFailureInterrupted:   "the agent stopped waiting for the artifact upload",
	artifactFailureJob:           "the artifact uploader job failed",
}

const (
	genericArtifactFailureReason = "the artifact export failed"
	missingArtifactSummaryReason = "the artifact uploader reported no usable summary"
	skippedArtifactReason        = "the artifact export was skipped because the workload failed"
)

// artifactFailureReason maps a category token to its stored phrase. A token
// outside the table — including whatever a termination message claims — falls
// back to the generic phrase, so an unrecognised value can never be the note.
func artifactFailureReason(category string) string {
	if reason, ok := artifactFailureReasons[category]; ok {
		return reason
	}
	return genericArtifactFailureReason
}

// failedExport is the receipt for an export that produced no trustworthy count:
// the category names what broke, and nothing else travels with it.
func failedExport(category string) *ArtifactOutcome {
	return &ArtifactOutcome{Result: OutcomeResultFailed, Reason: artifactFailureReason(category)}
}

// jobReasonDeadlineExceeded is the Job controller's condition reason for a run
// it killed at activeDeadlineSeconds. The uploader Job sets that deadline, so
// this is also how an export that ran out of time is told apart from one the
// uploader itself failed.
const jobReasonDeadlineExceeded = "DeadlineExceeded"

// computeFailureReasons maps the Job controller's own terminal condition
// reasons to a short phrase. It is an allowlist, not a formatter: the condition
// MESSAGE is free text a failing pod can steer, so nothing outside this table
// becomes a reason — an unrecognised one is reported as no reason at all,
// which is the honest answer when the agent cannot say why.
var computeFailureReasons = map[string]string{
	"BackoffLimitExceeded":     "the job exhausted its retry budget",
	jobReasonDeadlineExceeded:  "the job ran past its active deadline",
	"PodFailurePolicy":         "a pod failure matched the job's failure policy",
	"FailedIndexes":            "one or more job indexes failed",
	"MaxFailedIndexesExceeded": "too many job indexes failed",
}

// jobOutcome is the receipt for a Job the agent watched terminate without
// running an upload of its own: the compute result it actually observed, plus —
// when export was configured and the compute failed — the export it
// deliberately did not attempt. Counts stay absent there; nothing was moved,
// and a zero would claim an upload ran.
func jobOutcome(job *batchv1.Job, phase string) *WorkloadOutcome {
	out := &WorkloadOutcome{Compute: computeOutcome(job, phase)}
	if out.Compute.Result == OutcomeResultFailed && job.Annotations[workload.AnnotationArtifacts] != "" {
		out.Artifacts = &ArtifactOutcome{Result: OutcomeResultSkipped, Reason: skippedArtifactReason}
	}
	return out
}

func computeOutcome(job *batchv1.Job, phase string) ComputeOutcome {
	if phase == "Succeeded" {
		return ComputeOutcome{Result: OutcomeResultSucceeded}
	}
	return ComputeOutcome{
		Result: OutcomeResultFailed,
		Reason: computeFailureReasons[terminalConditionReason(job, batchv1.JobFailed)],
	}
}

func terminalConditionReason(job *batchv1.Job, want batchv1.JobConditionType) string {
	for _, c := range job.Status.Conditions {
		if c.Type == want && c.Status == corev1.ConditionTrue {
			return c.Reason
		}
	}
	return ""
}
