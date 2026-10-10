package lifecycle

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
)

func validBooking() ProviderDeleteBooking {
	return ProviderDeleteBooking{
		CustomerID:         "cust_1",
		ClusterID:          "cluster_1",
		WorkloadID:         "wl_1",
		BurstID:            "burst_1",
		Provider:           "linode",
		Region:             "us-east",
		CloudAccountID:     "account_1",
		SKU:                "g6-standard-2",
		ProviderResourceID: "linode_987654",
		Spec:               []byte(`{"id":"burst_1"}`),
		Reason:             "workload completed",
		Payload:            []byte(`{"burst":{"ID":"burst_1"}}`),
	}
}

func TestNormalizeBookingKeepsTheBookedIdentity(t *testing.T) {
	got, workloadAsserted, err := normalizeProviderDeleteBooking(validBooking())
	if err != nil {
		t.Fatalf("valid booking rejected: %v", err)
	}
	want := validBooking()
	if got.Provider != want.Provider || got.Region != want.Region || got.CloudAccountID != want.CloudAccountID ||
		got.SKU != want.SKU || got.ProviderResourceID != want.ProviderResourceID {
		t.Fatalf("normalization changed the booked provider identity: %+v", got)
	}
	if !workloadAsserted {
		t.Fatal("a booking that named a workload must be held to it")
	}
}

// A legacy booking records neither a cluster nor a region, and the aggregate is
// cluster-keyed. The substitutes have to be deterministic or two reaps of one
// burst land on two different rows — which is two delete operations for one
// paid resource.
func TestNormalizeBookingDerivesMissingAggregateKeysDeterministically(t *testing.T) {
	booking := validBooking()
	booking.ClusterID = ""
	booking.WorkloadID = ""
	booking.Region = ""

	first, workloadAsserted, err := normalizeProviderDeleteBooking(booking)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := normalizeProviderDeleteBooking(booking)
	if err != nil {
		t.Fatal(err)
	}
	if first.ClusterID != second.ClusterID || first.WorkloadID != second.WorkloadID {
		t.Fatalf("derived keys are not stable: %+v vs %+v", first, second)
	}
	// A derived workload id is a stand-in for "this caller does not know", not
	// an assertion. Holding a projection to it would refuse every reap of a
	// burst that lifecycle admission created under its real workload id.
	if workloadAsserted {
		t.Fatal("a derived workload ID must not be reported as one the caller asserted")
	}
	if first.ClusterID != unassignedClusterID {
		t.Errorf("cluster ID = %q, want %q", first.ClusterID, unassignedClusterID)
	}
	if first.WorkloadID != booking.BurstID {
		t.Errorf("workload ID = %q, want the burst's own id %q", first.WorkloadID, booking.BurstID)
	}
	if first.Region != unspecifiedProviderRegion {
		t.Errorf("region = %q, want %q", first.Region, unspecifiedProviderRegion)
	}
}

func TestNormalizeBookingRejectsAnUnusableBooking(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*ProviderDeleteBooking)
	}{
		{name: "empty customer", mutate: func(b *ProviderDeleteBooking) { b.CustomerID = "" }},
		{name: "empty burst", mutate: func(b *ProviderDeleteBooking) { b.BurstID = "" }},
		{name: "burst with spaces", mutate: func(b *ProviderDeleteBooking) { b.BurstID = "burst 1" }},
		{name: "empty provider", mutate: func(b *ProviderDeleteBooking) { b.Provider = "" }},
		{name: "oversized cloud account", mutate: func(b *ProviderDeleteBooking) {
			b.CloudAccountID = strings.Repeat("x", maxIdentifierBytes+1)
		}},
		{name: "empty sku", mutate: func(b *ProviderDeleteBooking) { b.SKU = "" }},
		{name: "empty provider resource", mutate: func(b *ProviderDeleteBooking) { b.ProviderResourceID = "" }},
		{name: "oversized provider resource", mutate: func(b *ProviderDeleteBooking) {
			b.ProviderResourceID = strings.Repeat("x", maxIdentifierBytes+1)
		}},
		{name: "provider resource with NUL", mutate: func(b *ProviderDeleteBooking) {
			b.ProviderResourceID = "linode_\x00987654"
		}},
		{name: "missing spec", mutate: func(b *ProviderDeleteBooking) { b.Spec = nil }},
		{name: "array spec", mutate: func(b *ProviderDeleteBooking) { b.Spec = []byte(`[]`) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			booking := validBooking()
			test.mutate(&booking)
			if _, _, err := normalizeProviderDeleteBooking(booking); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

// A SKU or a resource ID is whatever the cloud hands back, and holding it to
// the identifier grammar would fail a delete closed over a punctuation
// character — stranding a paid node rather than deleting it.
func TestNormalizeBookingAcceptsRealProviderDescriptors(t *testing.T) {
	for _, descriptor := range []string{
		"g6-standard-2",
		"Standard_D2s_v3",
		"t3.micro",
		"shared-cpu-1x@1024MB",
		"n1-standard-4 (preemptible)",
	} {
		booking := validBooking()
		booking.SKU = descriptor
		if _, _, err := normalizeProviderDeleteBooking(booking); err != nil {
			t.Errorf("SKU %q rejected: %v", descriptor, err)
		}
	}
}

// payload_hash carries a CHECK of exactly this shape; a hash that does not
// match it turns the projection into a constraint violation at reap time.
func TestBookingPayloadHashMatchesTheSchemaConstraint(t *testing.T) {
	pattern := regexp.MustCompile(`^[0-9a-f]{64}$`)
	if got := bookingPayloadHash([]byte(`{"id":"burst_1"}`)); !pattern.MatchString(got) {
		t.Fatalf("payload hash %q does not match the schema's ^[0-9a-f]{64}$", got)
	}
}

// The projection must WRITE provider_created rather than transition into it,
// must never rewrite a stored identity, and must re-read the burst under lock
// so a replay carrying a different resource is refused rather than ignored.
func TestBookingProjectionWritesIdentityOnceAndRereadsItUnderLock(t *testing.T) {
	body, err := os.ReadFile("provider_delete_booking.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(body)
	for _, required := range []string{
		"ON CONFLICT DO NOTHING",
		"'provider_created'",
		"FOR UPDATE",
		"ErrIdentityConflict",
		"WHERE state <> 'terminated'",
	} {
		if !strings.Contains(source, required) {
			t.Errorf("booking projection is missing %q", required)
		}
	}
	// An UPDATE against the aggregate rows would let a later reap rewrite the
	// identity an in-flight delete is already acting on.
	if strings.Contains(source, "UPDATE lifecycle.bursts") ||
		strings.Contains(source, "UPDATE lifecycle.workloads") {
		t.Fatal("the booking projection must never update an existing aggregate row")
	}
}
