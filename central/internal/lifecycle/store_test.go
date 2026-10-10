package lifecycle

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func validAdmission() AdmissionRequest {
	return AdmissionRequest{
		CustomerID:            "cust_1",
		ClusterID:             "cluster_1",
		IdempotencyKey:        "kube:cluster_1:uid_1:1",
		CanonicalVersion:      1,
		WorkloadID:            "wl_1",
		BurstID:               "burst_1",
		WorkloadSpec:          []byte(`{"image":"busybox","command":["true"]}`),
		BurstSpec:             []byte(`{"cpu":1,"memory_mb":512}`),
		Provider:              "linode",
		Region:                "us-east",
		SKU:                   "g6-standard-1",
		ProviderCreatePayload: []byte(`{"burst_id":"burst_1"}`),
		OutboxPayload:         []byte(`{"workload_id":"wl_1","burst_id":"burst_1"}`),
		Actor:                 "admission",
		TraceID:               "trace_1",
	}
}

func TestAdmissionValidation(t *testing.T) {
	req := validAdmission()
	if _, err := normalizeAdmission(req); err != nil {
		t.Fatalf("valid admission rejected: %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*AdmissionRequest)
	}{
		{name: "empty customer", mutate: func(r *AdmissionRequest) { r.CustomerID = "" }},
		{name: "space in ID", mutate: func(r *AdmissionRequest) { r.WorkloadID = "wl 1" }},
		{name: "oversized ID", mutate: func(r *AdmissionRequest) { r.BurstID = strings.Repeat("x", maxIdentifierBytes+1) }},
		{name: "zero version", mutate: func(r *AdmissionRequest) { r.CanonicalVersion = 0 }},
		{name: "invalid workload JSON", mutate: func(r *AdmissionRequest) { r.WorkloadSpec = []byte(`{`) }},
		{name: "array payload", mutate: func(r *AdmissionRequest) { r.BurstSpec = []byte(`[]`) }},
		{name: "oversized payload", mutate: func(r *AdmissionRequest) {
			r.ProviderCreatePayload = []byte(`{"data":"` + strings.Repeat("x", maxPayloadBytes) + `"}`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := validAdmission()
			test.mutate(&changed)
			if _, err := normalizeAdmission(changed); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("normalizeAdmission error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	withoutOutbox := validAdmission()
	withoutOutbox.OutboxPayload = nil
	normalized, err := normalizeAdmission(withoutOutbox)
	if err != nil {
		t.Fatal(err)
	}
	if len(normalized.OutboxPayload) == 0 || !strings.Contains(string(normalized.OutboxPayload), "workload_id") {
		t.Fatalf("default outbox payload missing canonical IDs: %s", normalized.OutboxPayload)
	}
}

func TestAdmissionPayloadHash(t *testing.T) {
	req, err := normalizeAdmission(validAdmission())
	if err != nil {
		t.Fatal(err)
	}
	base := admissionPayloadHash(req, true)
	same := req
	same.IdempotencyKey = "kube:other-key"
	same.WorkloadID = "wl_other"
	same.BurstID = "burst_other"
	if got := admissionPayloadHash(same, true); got != base {
		t.Fatal("payload hash should not include caller-supplied canonical IDs or retry key")
	}
	for _, mutate := range []func(*AdmissionRequest){
		func(r *AdmissionRequest) { r.CanonicalVersion++ },
		func(r *AdmissionRequest) { r.WorkloadSpec = []byte(`{"image":"alpine"}`) },
		func(r *AdmissionRequest) { r.BurstSpec = []byte(`{"cpu":2}`) },
		func(r *AdmissionRequest) { r.Provider = "aws" },
		func(r *AdmissionRequest) { r.ProviderCreatePayload = []byte(`{"burst_id":"burst_1","changed":true}`) },
		func(r *AdmissionRequest) { r.OutboxPayload = []byte(`{"workload_id":"wl_1","changed":true}`) },
	} {
		changed := req
		mutate(&changed)
		if got := admissionPayloadHash(changed, true); got == base {
			t.Fatal("payload hash failed to change for semantic mutation")
		}
	}
	if got := admissionPayloadHash(req, false); got == base {
		t.Fatal("explicit and store-generated outbox payloads must have distinct identities")
	}
}

// Admission and the provider-delete projection must agree on how a burst's
// provider identity is spelled, because the projection re-reads the very row
// admission wrote and refuses a delete whose identity disagrees. "No region"
// is the case that has two plausible spellings and therefore the one that
// silently strands a paid machine.
func TestAdmissionCanonicalizesRegionExactlyAsTheDeleteProjectionDoes(t *testing.T) {
	for _, region := range []string{"", "   "} {
		req := validAdmission()
		req.Region = region
		normalized, err := normalizeAdmission(req)
		if err != nil {
			t.Fatalf("region %q rejected: %v", region, err)
		}
		if normalized.Region != CanonicalProviderDeleteRegion(region) {
			t.Fatalf("admitted region for %q = %q, want %q — a later delete would read an identity conflict",
				region, normalized.Region, CanonicalProviderDeleteRegion(region))
		}
	}
	// A pinned region is preserved verbatim; canonicalization only names an
	// absence, it never rewrites a real routing decision.
	pinned := validAdmission()
	pinned.Region = "us-east"
	normalized, err := normalizeAdmission(pinned)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Region != "us-east" {
		t.Fatalf("pinned region became %q", normalized.Region)
	}
}

// A BYOC account is part of the admitted identity. Empty is the platform-funded
// backend; present, it is held to the same bound the projection holds it to, so
// nothing admission accepts can fail the delete projection's own validation.
func TestAdmissionAcceptsAndBoundsTheCloudAccount(t *testing.T) {
	absent := validAdmission()
	absent.CloudAccountID = "  "
	normalized, err := normalizeAdmission(absent)
	if err != nil {
		t.Fatalf("platform-funded admission rejected: %v", err)
	}
	if normalized.CloudAccountID != "" {
		t.Fatalf("blank cloud account normalized to %q, want empty", normalized.CloudAccountID)
	}

	byoc := validAdmission()
	byoc.CloudAccountID = "ca_0123456789abcdef"
	normalized, err = normalizeAdmission(byoc)
	if err != nil {
		t.Fatalf("BYOC admission rejected: %v", err)
	}
	if normalized.CloudAccountID != "ca_0123456789abcdef" {
		t.Fatalf("BYOC account became %q", normalized.CloudAccountID)
	}
	if err := validateBookingField("cloud account ID", normalized.CloudAccountID); err != nil {
		t.Fatalf("an admitted cloud account the delete projection would reject: %v", err)
	}

	for name, value := range map[string]string{
		"oversized": strings.Repeat("c", maxIdentifierBytes+1),
		"NUL byte":  "ca_\x00",
	} {
		bad := validAdmission()
		bad.CloudAccountID = value
		if _, err := normalizeAdmission(bad); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s cloud account error = %v, want ErrInvalidArgument", name, err)
		}
	}
}

// The cloud account selects the credentials a create is routed through. A replay
// that keeps every payload byte and changes only the account is a create against
// a different tenant's cloud, so it has to be a conflict rather than a silent
// reuse of the first admission's answer.
func TestAdmissionPayloadHashCoversTheCloudAccount(t *testing.T) {
	req, err := normalizeAdmission(validAdmission())
	if err != nil {
		t.Fatal(err)
	}
	platform := admissionPayloadHash(req, true)

	byoc := req
	byoc.CloudAccountID = "ca_tenant"
	tenant := admissionPayloadHash(byoc, true)
	if tenant == platform {
		t.Fatal("payload hash ignores the cloud account; a BYOC replay would reuse a platform-funded admission")
	}

	other := req
	other.CloudAccountID = "ca_other_tenant"
	if admissionPayloadHash(other, true) == tenant {
		t.Fatal("payload hash does not distinguish two cloud accounts")
	}
}

func TestAdmissionRejectsInvalidUTF8(t *testing.T) {
	req := validAdmission()
	req.OutboxPayload = []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}
	if _, err := normalizeAdmission(req); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("normalizeAdmission error = %v, want ErrInvalidArgument", err)
	}
}

func TestLeaseValidationAndTruncation(t *testing.T) {
	if !errors.Is(validateLease(time.Second-time.Nanosecond), ErrInvalidArgument) {
		t.Fatal("short lease accepted")
	}
	if !errors.Is(validateLease(maxLeaseDuration+time.Nanosecond), ErrInvalidArgument) {
		t.Fatal("long lease accepted")
	}
	if err := validateLease(time.Second); err != nil {
		t.Fatal(err)
	}
	got := truncateUTF8("a\x00b€", 5)
	if got != "a�b" {
		t.Fatalf("truncateUTF8 = %q", got)
	}
}

func TestSchemaContainsLifecycleInvariants(t *testing.T) {
	for _, required := range []string{
		"CREATE SCHEMA IF NOT EXISTS lifecycle",
		"UNIQUE (customer_id, cluster_id, idempotency_key)",
		"UNIQUE (customer_id, cluster_id, operation_type, semantic_key)",
		"UNIQUE (customer_id, cluster_id, event_key)",
		"FOREIGN KEY (customer_id, cluster_id, workload_id)",
		"FOREIGN KEY (customer_id, cluster_id, burst_id)",
		"CHECK (state IN ('pending','processing','succeeded','failed','dead_letter'))",
		"CHECK (state IN ('pending','processing','acknowledged','failed','dead_letter'))",
		"octet_length(payload::text) BETWEEN 2 AND 1048576",
		"octet_length(spec::text) BETWEEN 2 AND 1048576",
		// Admission writes the BYOC account onto the burst row that the
		// authoritative delete projection later re-reads, and the burst identity
		// trigger is what keeps the two from drifting after the fact.
		"cloud_account_id",
		"OLD.cloud_account_id IS DISTINCT FROM NEW.cloud_account_id",
		"lifecycle_events_immutable",
		"lifecycle_workload_transition",
		"lifecycle_burst_transition",
		"lifecycle_operation_transition",
		"lifecycle_outbox_transition",
		"FOR EACH ROW EXECUTE FUNCTION lifecycle.reject_lifecycle_event_mutation()",
		// Issue #93: truthful provider-creation timestamp
		"provider_created_at",
	} {
		if !strings.Contains(lifecycleSchema, required) {
			t.Fatalf("lifecycle schema missing %q", required)
		}
	}
}

func TestMarkProviderCreateSucceededSQLPreservesFirstTimestamp(t *testing.T) {
	source, err := os.ReadFile("store.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "COALESCE(provider_created_at, now())") {
		t.Fatal("MarkProviderCreateSucceeded must use COALESCE to preserve the first provider_created_at on replay")
	}
}

func TestBookingProjectsSQLCarriesProviderCreatedAt(t *testing.T) {
	source, err := os.ReadFile("provider_delete_booking.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "provider_created_at") {
		t.Fatal("projectBookingTx must include provider_created_at in the INSERT")
	}
}

func TestStoreSQLContainsSkipLockedClaims(t *testing.T) {
	source, err := os.ReadFile("store.go")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(source), "FOR UPDATE SKIP LOCKED"); got < 2 {
		t.Fatalf("provider-create and outbox claims must use FOR UPDATE SKIP LOCKED, count=%d", got)
	}
}

func TestErrorSentinelsWrap(t *testing.T) {
	for _, sentinel := range []error{
		ErrIdempotencyConflict,
		ErrIdentityConflict,
		ErrInvalidArgument,
		ErrInvariantViolation,
		ErrLeaseLost,
		ErrNotFound,
	} {
		if !errors.Is(errors.Join(errors.New("context"), sentinel), sentinel) {
			t.Fatalf("errors.Is failed for %v", sentinel)
		}
	}
}
