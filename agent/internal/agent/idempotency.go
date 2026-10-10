package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// idempotencyHeader is central's header for the durable submission claim on
// POST /v1/workloads. Central scopes the key to (tenant, submitter, canonical
// spec) and holds a claim on it, so a resubmit carrying the same key is a
// replay of one node rather than a second one.
const idempotencyHeader = "Idempotency-Key"

// clusterIDHeader is central's header for WHICH of a tenant's clusters a
// request is about. The cluster token authenticates the tenant only, so on a
// tenant running more than one connector this is what keeps a submission from
// being routed to the other one.
const clusterIDHeader = "X-Cluster-ID"

// submissionKey builds the Idempotency-Key for one logical submission.
//
// The key's only job is to be the SAME string every time the agent re-derives
// it for the same logical request — across a watch reconnect, a crash between
// submit and status patch, or a process restart — and a different string
// otherwise. It is a digest over identity parts rather than the parts
// themselves, which makes the result printable ASCII inside central's 8..255
// contract whatever those parts hold: a Kubernetes UID is a UUID from the
// apiserver but an arbitrary string from a fake client or a hand-built object.
//
// The parts are identity only — never the token, never the spec body. The key
// rides in a header, through every proxy log between here and central.
func submissionKey(prefix string, parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		// Length-prefixed so ("a", "bc") and ("ab", "c") stay distinct keys.
		fmt.Fprintf(h, "%d:%s", len(p), p)
	}
	return prefix + "-" + hex.EncodeToString(h.Sum(nil))[:32]
}

// podSubmissionKey is the key for the burst a Pending pod asks for. Keyed on
// the pod UID alone: the apiserver mints it once and never mutates it, so the
// same pod seen again after a watch reconnect or an agent restart replays its
// first submission instead of provisioning a second burst. A recreated pod is
// a new UID, hence a new burst, which is what the scheduler is asking for.
func podSubmissionKey(pod *corev1.Pod) string {
	return submissionKey("burstpod", string(pod.UID))
}

// crSubmissionKey is the key for one Workload CR's single submission. The
// reconciler does not turn spec edits into new runs, so generation is
// deliberately absent: an edit during the crash window between central
// accepting the workload and status being patched must still replay the first
// request instead of provisioning a second paid burst. A deliberate new run is
// a new CR and therefore has a new immutable UID.
func crSubmissionKey(obj *unstructured.Unstructured) string {
	if uid := string(obj.GetUID()); uid != "" {
		return submissionKey("workload", uid)
	}
	// Objects built by hand — tests, and any client that drops metadata — carry
	// no UID. namespace/name is the next most stable identity available: weaker
	// than a UID across a delete+recreate of the same name, still stable across a
	// restart or edit, and far better than submitting unkeyed.
	return submissionKey("workload", obj.GetNamespace(), obj.GetName())
}
