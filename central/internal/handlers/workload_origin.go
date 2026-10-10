package handlers

import (
	"fmt"
	"net/http"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// maxWorkloadOriginBytes bounds what is compared at all. The accepted values
// are two short labels; anything longer is not one of them, and refusing it on
// length keeps an oversized header out of the refusal body.
const maxWorkloadOriginBytes = 64

// submissionOrigin reads the connector-reported submission path off a create
// request.
//
// It is PROVENANCE, never authority. The tenant comes from the authenticated
// credential and the cluster from X-Cluster-ID checked against that credential,
// and neither reads this header — so the worst a forged value could do is put a
// wrong label on the forger's own workload. It is validated anyway, because a
// label nobody constrains is a label a console cannot render.
//
// No header is not a refusal. For a human-authenticated request central knows
// it is the API path. For a cluster credential, silence is ambiguous: it may be
// a CLI, customer automation, or a connector built before this header existed,
// so legacy cluster submissions remain unlabelled.
//
// A HUMAN submission is refused the header outright rather than trusted with
// it. The console and the retry route reach Create with a person in the request
// context, and a person is not a connector reporting which of its controllers
// fired — accepting the claim would make the one field a console shows for
// "this burst came up on its own" the one field a browser can write.
func submissionOrigin(r *http.Request, by submitter) (protocol.SubmissionOrigin, error) {
	values := r.Header.Values(protocol.WorkloadOriginHeader)
	if len(values) == 0 {
		if by.Actor.Kind == state.ActorHuman {
			return protocol.OriginAPI, nil
		}
		return "", nil
	}
	if by.Actor.Kind == state.ActorHuman {
		return "", fmt.Errorf("%s is not accepted from a human submission", protocol.WorkloadOriginHeader)
	}
	if len(values) != 1 || len(values[0]) > maxWorkloadOriginBytes {
		return "", fmt.Errorf("%s must be sent at most once, as a short value", protocol.WorkloadOriginHeader)
	}
	origin := protocol.SubmissionOrigin(values[0])
	if !origin.ConnectorReported() {
		// The accepted set is spelled out rather than echoing what arrived: the
		// value is caller-controlled and this string is a response body.
		return "", fmt.Errorf("%s must be %q or %q, or omitted",
			protocol.WorkloadOriginHeader, protocol.OriginPendingPod, protocol.OriginWorkloadCR)
	}
	return origin, nil
}
