package protocol

// WorkloadOriginHeader carries the connector's report of WHICH of its
// submission paths produced a POST /v1/workloads.
//
// It is provenance and nothing else. Central takes the tenant and the cluster
// from the authenticated credential, exactly as it always has, and this header
// never widens or narrows what a caller may do — a forged value would buy
// nothing but a wrong label on the caller's own record.
const WorkloadOriginHeader = "X-Workload-Origin"

// SubmissionOrigin is the closed set of submission paths a workload record can
// name. The values are a wire contract shared by the connector that reports one
// and the central handler that stores it, so they live here once rather than as
// free strings in each controller.
type SubmissionOrigin string

const (
	// OriginAPI is a human-authenticated API submission. Central can derive this
	// from the authenticated actor without trusting a caller-supplied header.
	// Cluster-token requests that omit the header remain unlabelled because old
	// connectors and direct API clients are indistinguishable during rollout.
	OriginAPI SubmissionOrigin = "api"

	// OriginPendingPod is the connector's pending-pod watcher: a pod asked for
	// burst capacity through its nodeSelector and the watcher submitted for it.
	//
	// It names the PATH, not the controller behind it. KEDA, an HPA, Argo and a
	// hand-applied Job all reach central this way, and the connector sees only a
	// Pending pod — so calling this "keda" would be a claim nobody measured.
	OriginPendingPod SubmissionOrigin = "pending-pod"

	// OriginWorkloadCR is the connector's Workload CR reconciler: someone
	// applied a yscale.sh/v1 Workload and the reconciler submitted its spec.
	OriginWorkloadCR SubmissionOrigin = "workload-cr"
)

// ConnectorReported reports whether o is one of the two origins a connector may
// claim on the wire.
//
// OriginAPI is deliberately absent: it is derived from a human-authenticated
// request, so a connector sending it would be describing itself rather than
// reporting one of the paths central asked about.
func (o SubmissionOrigin) ConnectorReported() bool {
	return o == OriginPendingPod || o == OriginWorkloadCR
}
