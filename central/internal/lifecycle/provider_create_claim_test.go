package lifecycle

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// An unconfigured store refuses before it can pretend to have looked: claimed
// must be false, because a caller that reads only the bool would otherwise
// proceed to a paid create on a lease it never got.
func TestScopedProviderCreateClaimRefusesAnUnconfiguredStore(t *testing.T) {
	var nilStore *Store
	for name, store := range map[string]*Store{"nil store": nilStore, "no pool": {}} {
		t.Run(name, func(t *testing.T) {
			op, claimed, err := store.ClaimProviderCreateForBurst(context.Background(),
				"cust_1", "cluster_1", "burst_1", time.Minute)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
			if claimed || op.LeaseToken != "" {
				t.Fatalf("a refused claim reported claimed=%v lease=%q", claimed, op.LeaseToken)
			}
		})
	}
}

// The scoped claim must lease the SAME saga the generic claim does: the same
// row, the same states, the same fencing token, the same append-only claim
// event. What it must not do is skip past its own operation the way a
// work-queue drain does — "skipped" would read as "no such operation" to a
// caller that just admitted one, and the honest answer there is to wait.
func TestScopedProviderCreateClaimFencesWithoutSkipping(t *testing.T) {
	raw, err := os.ReadFile("provider_create_claim.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	if strings.Contains(source, "FOR UPDATE SKIP LOCKED") {
		t.Fatal("the scoped claim must not skip a locked row; it would report a concurrently-leased operation as absent")
	}
	for _, fragment := range []string{
		"operation_type='provider_create'",
		"WHERE customer_id=$1 AND cluster_id=$2 AND burst_id=$3",
		"FOR UPDATE",
		"lease_token=$2",
		"locked_until=now()+make_interval(secs => $1)",
		"WHERE id=$3 AND state IN ('pending','failed')",
		"provider_create.claimed",
	} {
		if !strings.Contains(source, fragment) {
			t.Fatalf("the scoped claim no longer contains %q", fragment)
		}
	}
}

// The single most expensive mistake this package can make is executing a create
// twice, and an expired `processing` lease is exactly the state that invites it:
// the request was already handed to an attempt that then stopped writing, so
// nothing durable says whether a provider accepted it. Neither claim path may
// treat that as due work — not the background drain, and not the request-inline
// scoped claim.
//
// Asserted against the SQL because that is where the rule lives; the PostgreSQL
// tests then prove the queries behave as the text says.
func TestNeitherProviderCreateClaimReclaimsAnExpiredLease(t *testing.T) {
	scoped := readSource(t, "provider_create_claim.go")
	// The outbox claim in store.go DOES reclaim expired leases and must keep
	// doing so — a publisher can safely publish twice — so only the
	// provider-create function is read out of that file.
	generic := functionSource(t, readSource(t, "store.go"), "func (s *Store) ClaimProviderCreate(")

	for name, source := range map[string]string{
		"scoped claim":  scoped,
		"generic claim": generic,
	} {
		if strings.Contains(source, "state='processing' AND locked_until <= now()) AS claimable") ||
			strings.Contains(source, "OR (state='processing' AND locked_until <= now())") {
			t.Fatalf("the %s still treats an expired processing lease as due work", name)
		}
		if !strings.Contains(source, "state IN ('pending','failed')") {
			t.Fatalf("the %s no longer restricts claimability to states that prove no resource exists", name)
		}
	}

	// A refusal that cannot say WHICH kind of processing refused it leaves an
	// operator unable to tell a live attempt from a dead one.
	for _, fragment := range []string{
		"(state='processing' AND locked_until <= now()) AS lease_expired",
		"&op.LeaseExpired",
	} {
		if !strings.Contains(scoped, fragment) {
			t.Fatalf("the scoped claim no longer reports the expired-lease refusal: missing %q", fragment)
		}
	}
}

func readSource(t *testing.T, file string) string {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// functionSource slices one top-level function out of a file so a rule that
// applies to it is not accidentally satisfied — or broken — by its neighbours.
func functionSource(t *testing.T, source, signature string) string {
	t.Helper()
	start := strings.Index(source, signature)
	if start < 0 {
		t.Fatalf("source no longer declares %q", signature)
	}
	body := source[start+len(signature):]
	if end := strings.Index(body, "\nfunc "); end >= 0 {
		body = body[:end]
	}
	return body
}
