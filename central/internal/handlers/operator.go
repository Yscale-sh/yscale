// yscale:proprietary

package handlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/yscale-sh/yscale/central/internal/state"
)

// ParseOperatorSubjects parses the YSCALE_OPERATOR_SUBJECTS allowlist: exact
// Yscale ID subjects, comma-separated, whitespace-trimmed, blanks ignored. The
// result is the set Wrap authorizes against. Configuration whitespace is not
// part of a subject; after parsing, membership is an exact-string match with
// no case folding, wildcarding, or normalization of the provider's value.
func ParseOperatorSubjects(raw string) map[string]bool {
	set := make(map[string]bool)
	for _, subject := range strings.Split(raw, ",") {
		if subject = strings.TrimSpace(subject); subject != "" {
			set[subject] = true
		}
	}
	return set
}

// OperatorAccountFromContext reports the central account id of the human an
// operator route authenticated, or "" when the request did not ride
// OperatorAuth. A non-empty return is the signal the shared hosted handlers
// use to attribute a change to the signed-in human rather than the static
// operator.
func OperatorAccountFromContext(ctx context.Context) string {
	accountID, _ := ctx.Value(ctxOperatorAccount).(string)
	return accountID
}

// operatorStore is the slice of the state store the operator surface touches.
// It deliberately names AccountByIdentity and NOT UpsertAccount: an operator
// route is not a sign-in, and a configured subject whose human has never
// signed in must fail closed rather than mint the account the allowlist would
// then permanently authorize.
type operatorStore interface {
	AccountByIdentity(issuer, subject string) (*state.Account, error)
}

// OperatorAuth is the browser-operator credential: a human's ordinary Yscale
// ID access token, validated through the SAME IdentityResolver the account
// surface uses, authorized by nothing but exact membership of the configured
// subject allowlist. It is a third credential alongside AdminAuth (a shared
// static token) and the cluster token: never the admin token proxied, never an
// email or tenant role — an operator console seat is granted by listing the
// human's stable subject, and nothing else promotes a human to it.
//
// Fail-closed shape, in order: an unconfigured resolver, issuer or allowlist
// makes the routes ABSENT (404), the same way an unset YSCALE_ADMIN_TOKEN
// disables the admin family and an unset resolver the account family; a
// missing or refused credential is 401; a valid credential whose subject is
// not allowlisted, or whose account does not exist, is 403; a locally shed
// lookup is 429 + Retry-After; an identity provider that could not be
// consulted is 503. The resolver itself carries the outbound bounds (the
// concurrency semaphore and the per-credential and global rate buckets), so
// wiring this to the account surface's resolver instance keeps the identity
// provider's total load at the one budget that surface already has.
type OperatorAuth struct {
	Store    operatorStore
	Resolver IdentityResolver
	// Issuer is YSCALE_ID_ISSUER: half of the account identity key the
	// existing-account lookup is keyed with. Empty leaves the routes absent.
	Issuer string
	// Subjects is the parsed YSCALE_OPERATOR_SUBJECTS allowlist. Empty leaves
	// the routes absent — an operator console nobody can reach is safer than
	// one every signed-in human can.
	Subjects map[string]bool
	Log      *slog.Logger
}

// Wrap gates one operator route. The wrapped handler sees the request only
// with a verified, allowlisted, existing human account, whose account id is
// carried in the request context for audit attribution.
func (oa OperatorAuth) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if oa.Store == nil || oa.Resolver == nil || oa.Issuer == "" || len(oa.Subjects) == 0 {
			// Not a refusal — the surface does not exist on this deployment.
			http.NotFound(w, r)
			return
		}
		token, ok := bearerToken(r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized: missing Bearer token"})
			return
		}
		identity, err := oa.Resolver.Resolve(r.Context(), token)
		if err != nil {
			log := oa.Log
			if log == nil {
				log = slog.Default()
			}
			switch {
			case errors.Is(err, ErrRateLimited):
				// Same contract as the account surface: the token was never
				// presented upstream, so Retry-After rather than a re-login.
				log.Warn("operator: identity resolution shed", "error", err)
				w.Header().Set("Retry-After", retryAfterSeconds)
				writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "too many identity lookups; retry shortly"})
			case errors.Is(err, ErrIdentityUnavailable):
				log.Warn("operator: identity provider unavailable", "error", err)
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "identity provider unavailable"})
			default:
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized: invalid access token"})
			}
			return
		}
		if !oa.Subjects[identity.Subject] {
			// The credential is fine and the human is real; they simply hold no
			// operator seat. 403, not 401: re-authenticating changes nothing.
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: not an operator"})
			return
		}
		account, err := oa.Store.AccountByIdentity(oa.Issuer, identity.Subject)
		if err != nil {
			log := oa.Log
			if log == nil {
				log = slog.Default()
			}
			if errors.Is(err, state.ErrNotFound) {
				// A subject the deployment listed whose human has never signed
				// in. Fail closed — minting the account here would promote the
				// human to operator on the strength of the allowlist alone.
				log.Warn("operator: allowlisted subject has no account; refusing")
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: no operator account"})
				return
			}
			// An identity the store will not key — the resolver already
			// rejects a blank subject, so this is a refused credential, not a
			// server fault; same answer callerAccount gives.
			log.Warn("operator: rejected identity", "error", err)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized: invalid identity"})
			return
		}
		ctx := context.WithValue(r.Context(), ctxOperatorAccount, account.ID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
