package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"net/http"
	"strconv"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
)

const (
	idempotencyHeader = "Idempotency-Key"

	// The key contract, kept deliberately narrow. It is an opaque token the
	// client picks, so the only things central has an opinion about are that it
	// fits in a header, survives a database round trip unchanged, and is long
	// enough to be the dedup token it claims to be — a three-character key
	// scoped to one submitter is a collision waiting to replay somebody's old
	// answer at them.
	minIdempotencyKeyBytes = 8
	maxIdempotencyKeyBytes = 255

	// idempotencyRetryAfter is what an in-progress claim tells the client to
	// wait. Short: the common case behind it is a provision this process is
	// actively running, not an outage.
	idempotencyRetryAfter = 5

	// idempotencyWriteTimeout bounds the two writes that run on a context
	// detached from the request. Detaching removes the request's deadline along
	// with its cancellation, and a claim write with no deadline at all would
	// hold the handler open for as long as a wedged pool cares to.
	idempotencyWriteTimeout = 5 * time.Second
)

// detached is the context the terminal writes run on: freed from the request's
// cancellation — the client has usually already gone, which is the entire
// scenario — but still bounded.
func detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), idempotencyWriteTimeout)
}

// The refusals name what is wrong with the key and never the key. An
// Idempotency-Key is client-chosen and routinely derived from something the
// client considers its own (a form nonce, a session id, a request id), and a
// value echoed into an error body or a log line is a value in every proxy log
// and error tracker between here and the browser.
var (
	errIdempotencyKeyLength = errors.New("must be 8..255 bytes")
	errIdempotencyKeyChars  = errors.New("must be printable ASCII with no spaces")
)

func validateIdempotencyKey(key string) error {
	if len(key) < minIdempotencyKeyBytes || len(key) > maxIdempotencyKeyBytes {
		return errIdempotencyKeyLength
	}
	for i := 0; i < len(key); i++ {
		// '!'..'~' — printable ASCII minus space. Excludes the control
		// characters that would smuggle a newline into a log line and the
		// non-ASCII bytes whose normalisation would make "the same key" a
		// question with more than one answer.
		if key[i] < '!' || key[i] > '~' {
			return errIdempotencyKeyChars
		}
	}
	return nil
}

// idempotencyScopeID is the claim's primary key: a digest over the tenant, the
// submitter, the authenticated cluster binding and the key.
//
// Scoped, because the key is client-chosen — two tenants both sending
// "retry-1" must not collide, and one of them must not be able to probe for the
// other's by guessing. Scoped to the SUBMITTER as well as the tenant for the
// same reason within a tenant: two humans on one roster picking the same key
// would otherwise replay each other's workloads.
//
// boundClusterID is the cluster a scoped connector credential authenticates as,
// and it is in the scope for that same reason once more: a multi-cluster
// tenant's connectors all submit as the one cluster actor, so without it two
// sibling clusters picking "retry-1" — which each picks alone, knowing nothing
// of the other — would replay each other's workloads, or learn of each other
// through a 409. It is safe to scope on precisely because it is NOT chosen by
// the caller: ConnectorAuth resolves it from the credential itself and refuses
// an X-Cluster-ID that disagrees, so it is as stable across a connector's
// retries as the credential the connector holds.
//
// It is empty for the two callers that hold no such binding — a human, and the
// legacy tenant token — and those scopes hash exactly as they always have, so
// no claim already in flight changes meaning on deploy. Their routing stays out
// of the scope deliberately: it is resolved from whichever connector is
// currently registered, or named by a caller header, so a retry that routed
// elsewhere would be a different scope, and a different scope is a second node.
//
// Fields are length-prefixed so no concatenation of one scope can be forged as
// another.
func idempotencyScopeID(customerID string, by submitter, boundClusterID, key string) string {
	h := sha256.New()
	hashField(h, customerID)
	hashField(h, by.Actor.Kind)
	hashField(h, by.Actor.AccountID)
	hashField(h, by.Actor.CustomerID)
	if boundClusterID != "" {
		hashField(h, "cluster")
		hashField(h, boundClusterID)
	}
	hashField(h, key)
	return "idem_" + hex.EncodeToString(h.Sum(nil))
}

// idempotencyKeyDigest is the key alone, so an operator holding a key can find
// its claim without the table holding anything that could hand them one.
func idempotencyKeyDigest(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// canonicalRequestHash binds a key to ONE request. It digests the canonical
// spec — re-marshalled from the validated struct with the namespace already
// pinned to the one this tenant is authorized for — rather than the submitted
// bytes, so the binding is to the document central actually acts on. Two
// submissions that differ only in formatting, or in a namespace that pins to
// the same grant, ARE the same request and replay; anything that would run a
// different workload does not. Retry scopes the same digest by its source
// workload id, so one key cannot replay a new run from a different source that
// happened to have the same canonical spec.
//
// The template reference scopes it for the same reason, and it is not
// cosmetic: the same YAML launched under two different references is two
// submissions with different provenance, and replaying one as the other would
// stamp a run with a template it did not come from — the exact claim the
// reference exists to make. A submission that names no template hashes exactly
// as it always has, so no in-flight claim changes meaning on deploy.
func canonicalRequestHash(specYAML []byte, retryOf string, ref *state.TemplateRef) string {
	if retryOf == "" && ref == nil {
		sum := sha256.Sum256(specYAML)
		return hex.EncodeToString(sum[:])
	}
	h := sha256.New()
	if retryOf != "" {
		hashField(h, "retry")
		hashField(h, retryOf)
	}
	if ref != nil {
		hashField(h, "template")
		hashField(h, ref.ID)
		hashField(h, strconv.Itoa(ref.Version))
		hashField(h, ref.CatalogRevision)
	}
	_, _ = h.Write(specYAML)
	return hex.EncodeToString(h.Sum(nil))
}

type retryOfContextKey struct{}

func withRetryOfWorkload(ctx context.Context, workloadID string) context.Context {
	if workloadID == "" {
		return ctx
	}
	return context.WithValue(ctx, retryOfContextKey{}, workloadID)
}

func retryOfWorkload(ctx context.Context) string {
	id, _ := ctx.Value(retryOfContextKey{}).(string)
	return id
}

func validateSingleIdempotencyKey(r *http.Request) (string, error) {
	values := r.Header.Values(idempotencyHeader)
	if len(values) != 1 {
		return "", errors.New("Idempotency-Key header is required exactly once")
	}
	if err := validateIdempotencyKey(values[0]); err != nil {
		return "", err
	}
	return values[0], nil
}

func hashField(h hash.Hash, v string) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(v)))
	_, _ = h.Write(n[:])
	_, _ = h.Write([]byte(v))
}

// idempotentSubmission is Create's handle on one submission's claim. A zero
// claimID is the unkeyed cluster-token path, which predates this contract and
// keeps its exact previous behaviour: it holds no claim, so it neither replays
// nor blocks — it just carries the workload id, which is minted up front either
// way.
type idempotentSubmission struct {
	h          *Workloads
	claimID    string
	workloadID string
}

// beginIdempotent resolves the Idempotency-Key on a submission and decides
// whether Create may go on. done=true means the response has already been
// written — a refused key, a conflict, a replay, or a wait — and the caller
// must return.
//
// It runs AFTER the spec is validated, the namespace is authorized and the
// canonical spec is rendered, so the hash it binds is the request central would
// act on, and every refusal above it (a malformed spec, an unauthorized
// namespace and the audit row that records it) still happens exactly as before.
// It runs BEFORE the connector lookup, the admission reservation and Plan, so
// no keyed retry can reach a provider behind another one's back.
func (h *Workloads) beginIdempotent(w http.ResponseWriter, r *http.Request, cust *state.Customer, by submitter, specYAML []byte, ref *state.TemplateRef) (*idempotentSubmission, bool) {
	key := r.Header.Get(idempotencyHeader)

	if key == "" {
		// A human submitting through the console or the API has a client that
		// can retry them into a second node, and nothing else stops it — so the
		// key is required, and its absence is a client bug worth naming.
		//
		// A cluster credential reaching here is the connector/OSS path, which
		// shipped without this header. Requiring it would break every deployed
		// connector at once, so an unkeyed submission keeps working exactly as
		// it did — once, with no claim — and a connector that sends a key gets
		// the durable contract with no other change.
		if by.Actor.Kind == state.ActorHuman {
			writeJSON(w, http.StatusBadRequest, CreateWorkloadResponse{
				Status:  "rejected",
				Message: "Idempotency-Key header is required",
			})
			return nil, true
		}
		return &idempotentSubmission{h: h, workloadID: newID("wl")}, false
	}
	if err := validateIdempotencyKey(key); err != nil {
		writeJSON(w, http.StatusBadRequest, CreateWorkloadResponse{
			Status:  "rejected",
			Message: "invalid Idempotency-Key: " + err.Error(),
		})
		return nil, true
	}

	claim := &state.IdempotencyClaim{
		// The binding, not the header: AgentClusterFromContext reports what
		// ConnectorAuth authenticated, and it is read here — before the connector
		// lookup, the admission reservation and Plan — so a sibling cluster's key
		// can never reach far enough to replay this one's answer.
		ID:          idempotencyScopeID(cust.ID, by, AgentClusterFromContext(r.Context()), key),
		CustomerID:  cust.ID,
		Actor:       by.Actor,
		KeyDigest:   idempotencyKeyDigest(key),
		RequestHash: canonicalRequestHash(specYAML, retryOfWorkload(r.Context()), ref),
		// Minted with the claim, not after Plan: a retry that arrives while the
		// first attempt is still provisioning — or after the process running it
		// died — has to be handed the id of the workload it is waiting on, and
		// an id minted later would not exist yet.
		WorkloadID: newID("wl"),
		CreatedAt:  time.Now().UTC(),
	}
	existing, won, err := h.Store.ClaimIdempotent(r.Context(), claim)
	if err != nil {
		// The claim's outcome is unknown, so provisioning now is the one thing
		// that cannot be walked back. Refuse instead, retryably.
		h.Log.Error("idempotency claim failed", "customer", cust.ID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, CreateWorkloadResponse{
			Status:  "rejected",
			Message: "could not record the submission claim; please retry",
		})
		return nil, true
	}
	if won {
		return &idempotentSubmission{h: h, claimID: claim.ID, workloadID: claim.WorkloadID}, false
	}

	// The key is spent. Which of the three answers it gets depends only on what
	// the durable claim says, so every replica gives the same one.
	if existing.RequestHash != claim.RequestHash {
		// Refused BEFORE the admission reservation and before any provider
		// call: the client asked for a different workload under an identifier
		// that already means something, and running it would put the wrong job
		// behind an id its own retry logic will later replay.
		writeJSON(w, http.StatusConflict, CreateWorkloadResponse{
			Status:  "rejected",
			Message: "Idempotency-Key was already used for a different workload request",
		})
		return nil, true
	}
	if existing.State == state.IdempotencyCompleted {
		replayIdempotent(w, existing)
		return nil, true
	}

	// Still in flight. Deliberately NOT taken over, however old it is: a claim
	// whose owner died may have died mid-CreateNode, and there is no way from
	// here to tell a provider call that never happened from one that returned a
	// billing machine to a process that no longer exists. Re-running Plan on
	// that guess is the double-bill. So the claim stays exactly as it is —
	// visible in the table, holding the workload id — and an operator (or a
	// reconciler reading the same rows) resolves it.
	w.Header().Set("Retry-After", strconv.Itoa(idempotencyRetryAfter))
	writeJSON(w, http.StatusServiceUnavailable, CreateWorkloadResponse{
		ID:      existing.WorkloadID,
		Status:  "in_progress",
		Message: "a submission with this Idempotency-Key is already being processed; retry, or poll this workload id",
	})
	return nil, true
}

// replayIdempotent writes the stored answer back verbatim.
func replayIdempotent(w http.ResponseWriter, claim *state.IdempotencyClaim) {
	w.Header().Set("Content-Type", "application/json")
	// So a caller can tell a replay from a fresh submission without diffing
	// bodies — the body itself is byte-identical by design.
	w.Header().Set("Idempotency-Replayed", "true")
	w.WriteHeader(claim.Status)
	_, _ = w.Write(claim.Response)
}

// answer is Create's only exit once the claim is held: it records the terminal
// outcome and then writes it, so a retry replays this answer rather than
// provisioning again.
//
// The record is written on a context detached from the request, and that is the
// entire point of the detachment: the failure this path exists for is a client
// that gave up and retried, and by then the request context is cancelled. A
// completion that inherited it would fail precisely in the case it is for.
func (s *idempotentSubmission) answer(ctx context.Context, w http.ResponseWriter, status int, resp CreateWorkloadResponse) {
	if s.claimID == "" {
		writeJSON(w, status, resp)
		return
	}
	body, err := renderCreateResponse(resp)
	if err != nil {
		// CreateWorkloadResponse has no field that encoding/json can reject, but
		// keep the failure explicit: a claim without replay bytes must stay
		// in-progress rather than publish an answer later retries cannot match.
		s.h.Log.Error("encoding the idempotent submission outcome failed; retries under this key will be refused as in-progress",
			"workload", s.workloadID, "status", status, "error", err)
		writeJSON(w, http.StatusInternalServerError, CreateWorkloadResponse{
			ID:      s.workloadID,
			Status:  "failed",
			Message: "could not record the submission outcome",
		})
		return
	}
	writeCtx, cancel := detached(ctx)
	defer cancel()
	err = s.h.Store.CompleteIdempotent(writeCtx, s.claimID, status, body, time.Now().UTC())
	if err != nil {
		// The client still gets the answer its request earned — the submission
		// really did reach this outcome. What is lost is the replay: the claim
		// stays in progress, so a retry is told to wait instead of re-running
		// Plan. That is the safe side of this failure, and the loud log is
		// because the recoverable half needs a human.
		s.h.Log.Error("recording the idempotent submission outcome failed; retries under this key will be refused as in-progress",
			"workload", s.workloadID, "status", status, "error", err)
	}
	// Write the exact bytes stored above. This matters for the bounded fallback:
	// the first caller and every replay must receive one response, not a large
	// live answer followed by a smaller approximation on retry.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// release frees the key of a submission that bailed out with nothing
// provisioned, so the client's retry is a real retry rather than a permanent
// refusal on a key it will keep sending.
//
// Only ever correct BEFORE Decider.Plan is entered, or on an error Plan is
// known to raise before it touches a backend. Once a provider call is
// ambiguous, the claim is what stands between a retry and a second machine.
func (s *idempotentSubmission) release(ctx context.Context) {
	if s.claimID == "" {
		return
	}
	releaseCtx, cancel := detached(ctx)
	defer cancel()
	if err := s.h.Store.ReleaseIdempotent(releaseCtx, s.claimID); err != nil {
		// A stranded claim refuses this key from here on. Nothing is billing
		// and nothing is inconsistent — the tenant resubmits under a new key —
		// but it is a wedge an operator should see.
		s.h.Log.Error("releasing the idempotency claim failed; this key will be refused as in-progress",
			"workload", s.workloadID, "error", err)
	}
}

// renderCreateResponse produces exactly the bytes writeJSON would write, so a
// replayed answer is byte-identical to the original rather than a re-encoding
// that happens to carry the same fields.
//
// Over the bound, both the stored copy and the first caller receive the same
// bounded response: the only field that can grow is a scheduling error's
// upstream text, and letting that decide how much of the claim table one
// submission occupies is how a bounded record stops being bounded.
func renderCreateResponse(resp CreateWorkloadResponse) ([]byte, error) {
	body, err := encodeJSON(resp)
	if err != nil {
		return nil, err
	}
	if len(body) <= state.MaxIdempotencyResponseBytes {
		return body, nil
	}
	resp.Message = "the original response was too large to store; poll the workload id"
	return encodeJSON(resp)
}

func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
