package lifecycle

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func validProviderDeleteRequest() ProviderDeleteRequest {
	return ProviderDeleteRequest{
		CustomerID: "cust_1",
		ClusterID:  "cluster_1",
		BurstID:    "burst_1",
		Reason:     "workload completed",
		Payload:    []byte(`{"requested_by":"watchdog"}`),
		Actor:      "reaper",
		TraceID:    "trace_1",
	}
}

func TestNormalizeProviderDeleteRequest(t *testing.T) {
	req, err := normalizeProviderDeleteRequest(validProviderDeleteRequest())
	if err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	if req.Reason != "workload completed" || string(req.Payload) != `{"requested_by":"watchdog"}` {
		t.Fatalf("normalized request changed semantic fields: %+v", req)
	}

	defaults := validProviderDeleteRequest()
	defaults.Reason = "  "
	defaults.Payload = nil
	got, err := normalizeProviderDeleteRequest(defaults)
	if err != nil {
		t.Fatal(err)
	}
	if got.Reason != "unspecified" || string(got.Payload) != `{}` {
		t.Fatalf("defaults = reason %q payload %s", got.Reason, got.Payload)
	}
}

func TestNormalizeProviderDeleteRequestRejectsInvalidInput(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*ProviderDeleteRequest)
	}{
		{name: "empty customer", mutate: func(r *ProviderDeleteRequest) { r.CustomerID = "" }},
		{name: "invalid burst", mutate: func(r *ProviderDeleteRequest) { r.BurstID = "burst with spaces" }},
		{name: "oversized reason", mutate: func(r *ProviderDeleteRequest) { r.Reason = strings.Repeat("x", maxSafeErrorBytes+1) }},
		{name: "invalid reason utf8", mutate: func(r *ProviderDeleteRequest) { r.Reason = string([]byte{0xff}) }},
		{name: "invalid payload", mutate: func(r *ProviderDeleteRequest) { r.Payload = []byte(`{`) }},
		{name: "array payload", mutate: func(r *ProviderDeleteRequest) { r.Payload = []byte(`[]`) }},
		{name: "oversized payload", mutate: func(r *ProviderDeleteRequest) {
			r.Payload = []byte(`{"data":"` + strings.Repeat("x", maxPayloadBytes) + `"}`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := validProviderDeleteRequest()
			test.mutate(&req)
			if _, err := normalizeProviderDeleteRequest(req); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestProviderDeleteSchemaContainsAuthoritativeStateMachine(t *testing.T) {
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS lifecycle.provider_deletes",
		"ADD COLUMN IF NOT EXISTS cloud_account_id",
		"OLD.cloud_account_id IS DISTINCT FROM NEW.cloud_account_id",
		"UNIQUE (customer_id, cluster_id, burst_id)",
		"REFERENCES lifecycle.workloads(customer_id, cluster_id, id)",
		"REFERENCES lifecycle.bursts(customer_id, cluster_id, id)",
		"CHECK (state IN ('queued','deleting','retrying','terminated','manual_attention'))",
		"state <> 'deleting' AND locked_until IS NULL AND lease_token IS NULL",
		"CHECK ((state = 'terminated') = (deleted_at IS NOT NULL))",
		"lifecycle_provider_delete_transition",
		"provider-delete terminal state is absorbing",
		"provider-delete generation may advance only on operator requeue",
		"invalid provider-delete attempt mutation",
		"WHERE state = 'manual_attention'",
		"idx_lifecycle_provider_deletes_nonterminal_resources",
		"ON lifecycle.provider_deletes(provider_resource_id)",
		"WHERE state <> 'terminated'",
	} {
		if !strings.Contains(providerDeleteSchema, required) {
			t.Fatalf("provider-delete schema missing %q", required)
		}
	}
}

func TestProviderDeleteClaimUsesSkipLockedAndDoesNotTerminalizeOnFailure(t *testing.T) {
	parts := []string{
		"provider_delete_request.go",
		"provider_delete_claim.go",
		"provider_delete_success.go",
		"provider_delete_failure.go",
		"provider_delete_reconcile.go",
		"provider_delete_helpers.go",
	}
	var source []byte
	for _, part := range parts {
		body, err := os.ReadFile(part)
		if err != nil {
			t.Fatal(err)
		}
		source = append(source, body...)
	}
	text := string(source)
	for _, required := range []string{
		"FOR UPDATE SKIP LOCKED",
		"CASE WHEN state='deleting' THEN locked_until ELSE next_attempt_at END",
		"state='deleting'",
		"state='terminated'",
		"tag.RowsAffected() != 1",
		"ProviderDeleteManualAttention",
		"burstState != BurstProviderCreated",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("provider-delete implementation missing %q", required)
		}
	}
	failureStart := strings.Index(text, "func (s *Store) MarkProviderDeleteFailed")
	if failureStart < 0 {
		t.Fatal("could not find provider-delete failure transition")
	}
	failureEnd := strings.Index(text[failureStart:], "func (s *Store) RetryProviderDelete")
	if failureEnd < 0 {
		t.Fatal("could not isolate provider-delete failure transition")
	}
	failureBody := text[failureStart : failureStart+failureEnd]
	if strings.Contains(failureBody, "terminal_at=now()") || strings.Contains(failureBody, "deleted_at=now()") {
		t.Fatal("provider-delete failure path must not stamp deletion or terminalize the burst")
	}
}

// The issue-#9 ordering, enforced at the source. Provider absence and the
// cleanup repair are ONE transaction, and the repair carries the operation's
// stored payload — a caller-supplied copy would let the repair act on something
// the delete never did.
func TestProviderDeleteSuccessEnqueuesTheCleanupRepairInTheSameTransaction(t *testing.T) {
	body, err := os.ReadFile("provider_delete_success.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	insert := strings.Index(text, "INSERT INTO lifecycle.outbox")
	if insert < 0 {
		t.Fatal("a confirmed provider delete enqueues no cleanup repair")
	}
	if !strings.Contains(text, "ProviderDeleteCleanupEventType") {
		t.Error("the cleanup repair is not typed as a provider-delete cleanup, so both claim filters miss it")
	}
	if !strings.Contains(text, "operation.Payload") {
		t.Error("the cleanup repair does not carry the operation's own immutable payload")
	}
	// Exactly one commit, and it comes after the insert.
	if got := strings.Count(text, "commit(ctx, tx"); got != 1 {
		t.Fatalf("provider-delete success commits %d times; the receipt and its repair must be atomic", got)
	}
	if commitAt := strings.Index(text, "commit(ctx, tx"); commitAt < insert {
		t.Fatal("the cleanup repair is enqueued after the transaction commits")
	}
}

// The two outbox consumers must filter in opposite directions. One predicate,
// inverted by a parameter, is what makes that structurally true instead of two
// WHERE clauses that can drift apart.
func TestOutboxClaimsPartitionOnTheCleanupEventType(t *testing.T) {
	body, err := os.ReadFile("store.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, "AND (event_type = $3) = $4") {
		t.Fatal("the outbox claim carries no event-type partition")
	}
	for _, required := range []string{
		"return s.claimOutboxEvent(ctx, lease, ProviderDeleteCleanupEventType, false)",
		"return s.claimOutboxEvent(ctx, lease, ProviderDeleteCleanupEventType, true)",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("outbox claim partition missing %q", required)
		}
	}
}

// A projection that committed on its own would leave, for the width of a
// process death, a burst recorded as provider_created with no delete operation
// against it — a paid resource the authoritative machine believes exists and
// has been told nothing about.
func TestBookingProjectionAndDeleteIntentShareOneTransaction(t *testing.T) {
	body, err := os.ReadFile("provider_delete_booking.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	start := strings.Index(text, "func (s *Store) RequestProviderDeleteForBooking")
	if start < 0 {
		t.Fatal("could not find the booking entry point")
	}
	end := strings.Index(text[start:], "\nfunc ")
	if end < 0 {
		t.Fatal("could not isolate the booking entry point")
	}
	booking := text[start : start+end]
	if got := strings.Count(booking, "s.pool.Begin"); got != 1 {
		t.Fatalf("the booking opens %d transactions, want exactly 1", got)
	}
	if !strings.Contains(booking, "s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})") {
		t.Fatal("booking ownership must read current state after the resource lock, regardless of session defaults")
	}
	if got := strings.Count(booking, "commit(ctx, tx"); got != 1 {
		t.Fatalf("the booking commits %d times, want exactly 1", got)
	}
	// The intent write is shared, not cloned: the SQL that decides which
	// identity a delete may act on must exist once.
	if !strings.Contains(booking, "requestProviderDeleteTx(ctx, tx, request)") {
		t.Error("the booking does not reuse the shared delete-intent write")
	}
	if strings.Contains(text, "INSERT INTO lifecycle.provider_deletes") {
		t.Error("the booking clones the delete-intent SQL instead of composing it")
	}
	if strings.Contains(text, "s.RequestProviderDelete(ctx") {
		t.Error("the booking calls the self-transacting entry point, so the two halves commit separately")
	}
}

func TestProviderDeleteReadRedactsWorkerOnlyFields(t *testing.T) {
	locked := time.Now().Add(time.Minute)
	record := ProviderDeleteRecord{
		LeaseToken:  "secret-fencing-token",
		LockedUntil: &locked,
		Payload:     []byte(`{"worker_only":true}`),
	}
	got := redactProviderDeleteForRead(record)
	if got.LeaseToken != "" {
		t.Fatalf("lease token was exposed: %q", got.LeaseToken)
	}
	if got.Payload != nil {
		t.Fatalf("worker payload was exposed: %s", got.Payload)
	}
	if got.LockedUntil == nil || !got.LockedUntil.Equal(locked) {
		t.Fatal("redaction changed non-secret lifecycle timing")
	}
}

func TestGetProviderDeleteSummariesValidation(t *testing.T) {
	// nil store returns immediately.
	var nilStore *Store
	if _, err := nilStore.GetProviderDeleteSummaries(nil, "cust_1", []ProviderDeleteSummaryRef{{ClusterID: "cluster_1", BurstID: "burst_1"}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil store: error = %v, want ErrInvalidArgument", err)
	}

	// Cannot test with a real pool here (no Postgres in unit tests), but we can
	// verify the validation logic by constructing a Store with a nil pool.
	s := &Store{}
	if _, err := s.GetProviderDeleteSummaries(nil, "cust_1", []ProviderDeleteSummaryRef{{ClusterID: "cluster_1", BurstID: "burst_1"}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil pool: error = %v, want ErrInvalidArgument", err)
	}
}

func TestGetProviderDeleteSummariesEmptyInput(t *testing.T) {
	// Empty burstIDs returns nil without querying — the check runs before
	// assertReady so a nil-pool Store is fine.
	s := &Store{}
	result, err := s.GetProviderDeleteSummaries(nil, "cust_1", nil)
	if err != nil || result != nil {
		t.Fatalf("nil burstIDs: result=%v err=%v", result, err)
	}
	result, err = s.GetProviderDeleteSummaries(nil, "cust_1", []ProviderDeleteSummaryRef{})
	if err != nil || result != nil {
		t.Fatalf("empty burstIDs: result=%v err=%v", result, err)
	}
}

func TestGetProviderDeleteSummariesRejectsBadInput(t *testing.T) {
	// nil pool; validation runs before the query.
	s := &Store{}

	valid := []ProviderDeleteSummaryRef{{ClusterID: "cluster_1", BurstID: "burst_1"}}
	if _, err := s.GetProviderDeleteSummaries(nil, "", valid); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty customer: error = %v, want ErrInvalidArgument", err)
	}
	if _, err := s.GetProviderDeleteSummaries(nil, "cust_1", []ProviderDeleteSummaryRef{{ClusterID: "", BurstID: "burst_1"}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty cluster ID: error = %v, want ErrInvalidArgument", err)
	}
	if _, err := s.GetProviderDeleteSummaries(nil, "cust_1", []ProviderDeleteSummaryRef{{ClusterID: "cluster_1", BurstID: ""}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty burst ID: error = %v, want ErrInvalidArgument", err)
	}
	if _, err := s.GetProviderDeleteSummaries(nil, "cust_1", []ProviderDeleteSummaryRef{{ClusterID: "cluster_1", BurstID: "bad burst"}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("invalid burst ID: error = %v, want ErrInvalidArgument", err)
	}

	// >100 burst IDs should be rejected.
	big := make([]ProviderDeleteSummaryRef, 101)
	for i := range big {
		big[i] = ProviderDeleteSummaryRef{ClusterID: "cluster_1", BurstID: fmt.Sprintf("burst_%d", i)}
	}
	if _, err := s.GetProviderDeleteSummaries(nil, "cust_1", big); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf(">100 burst IDs: error = %v, want ErrInvalidArgument", err)
	}
}
