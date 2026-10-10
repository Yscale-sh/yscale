package state

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLinodeCloudAccountRotationAndDisconnectGuardrails(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cust_cloud", Token: "tenant-auth", Plan: "pro"})
	owner, err := s.UpsertAccount("issuer", "owner", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddTenantMembership(owner.ID, "cust_cloud", RoleOwner); err != nil {
		t.Fatal(err)
	}
	by := HumanActor(owner.ID, "cust_cloud")
	first := CloudAccount{ID: "ca_1", Provider: CloudProviderLinode, ProviderIdentity: "uuid-1", Region: "us-ord", CredentialCiphertext: "v1.ciphertext-one", UpdatedAt: time.Now().UTC()}
	member, err := s.UpsertAccount("issuer", "member", AccountProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddTenantMembership(member.ID, "cust_cloud", RoleMember); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SetLinodeCloudAccount("cust_cloud", first, HumanActor(member.ID, "cust_cloud")); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("member connect err = %v", err)
	}
	if _, rotated, err := s.SetLinodeCloudAccount("cust_cloud", first, by); err != nil || rotated {
		t.Fatalf("connect = %v, %v", rotated, err)
	}
	rotated := first
	rotated.CredentialCiphertext = "v1.ciphertext-two"
	rotated.UpdatedAt = rotated.UpdatedAt.Add(time.Second)
	if got, wasRotation, err := s.SetLinodeCloudAccount("cust_cloud", rotated, by); err != nil || !wasRotation || got.ID != first.ID {
		t.Fatalf("rotation = %+v, %v, %v", got, wasRotation, err)
	}
	different := rotated
	different.ProviderIdentity = "uuid-2"
	if _, _, err := s.SetLinodeCloudAccount("cust_cloud", different, by); !errors.Is(err, ErrCloudAccountMismatch) {
		t.Fatalf("different identity err = %v", err)
	}

	expires := time.Now().UTC().Add(time.Minute)
	if won, err := s.AcquireCloudAccountLease(t.Context(), CloudAccountLease{BurstID: "burst_1", CustomerID: "cust_cloud", CloudAccountID: first.ID, ExpiresAt: expires}); err != nil || !won {
		t.Fatalf("lease = %v, %v", won, err)
	}
	if err := s.DisconnectLinodeCloudAccount("cust_cloud", first.ID, time.Now().UTC(), by); !errors.Is(err, ErrCloudAccountInUse) {
		t.Fatalf("disconnect with lease = %v", err)
	}
	if err := s.ReleaseCloudAccountLease(t.Context(), "burst_1"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutBurst(&Burst{ID: "burst_1", CustomerID: "cust_cloud", CloudAccountID: first.ID}); err != nil {
		t.Fatal(err)
	}
	if err := s.DisconnectLinodeCloudAccount("cust_cloud", first.ID, time.Now().UTC(), by); !errors.Is(err, ErrCloudAccountInUse) {
		t.Fatalf("disconnect with burst = %v", err)
	}
	if err := s.DeleteBurst("burst_1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DisconnectLinodeCloudAccount("cust_cloud", first.ID, time.Now().UTC(), by); err != nil {
		t.Fatal(err)
	}
}

func TestCloudAccountSerializedStateContainsNoPlaintext(t *testing.T) {
	c := &Customer{ID: "cust_cloud", LinodeCloudAccount: &CloudAccount{ID: "ca_1", Provider: CloudProviderLinode, CredentialCiphertext: "v1.encrypted"}}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "raw-provider-token") {
		t.Fatal("serialized customer contains plaintext")
	}
	if !strings.Contains(string(b), "v1.encrypted") {
		t.Fatal("serialized customer omitted ciphertext")
	}
	if b, err := json.Marshal(c.LinodeCloudAccount.Summary()); err != nil || strings.Contains(string(b), "encrypted") {
		t.Fatalf("safe summary leaked ciphertext: %s, %v", b, err)
	}
}

func TestCloudAccountSummaryUsesReadinessNotImageIdentifiers(t *testing.T) {
	withoutGPU := (&CloudAccount{ID: "ca_1", Provider: CloudProviderLinode, ProviderIdentity: "uuid", Region: "us-ord", CPUImage: "", UpdatedAt: time.Now().UTC()}).Summary()
	if withoutGPU.ProviderAccountID != "uuid" || !withoutGPU.CPUImageReady || withoutGPU.GPUImageReady {
		t.Fatalf("summary = %+v", withoutGPU)
	}
	withGPU := (&CloudAccount{GPUImage: "private/7"}).Summary()
	if !withGPU.CPUImageReady || !withGPU.GPUImageReady {
		t.Fatalf("configured summary = %+v", withGPU)
	}
	b, _ := json.Marshal(withoutGPU)
	if strings.Contains(string(b), "cpu_image\"") || strings.Contains(string(b), "gpu_image\"") || strings.Contains(string(b), "provider_identity") {
		t.Fatalf("unsafe summary JSON: %s", b)
	}
}

func TestExtendCloudAccountLeaseRequiresExactExistingLeaseAndOnlyLengthens(t *testing.T) {
	s := New()
	s.AddCustomer(&Customer{ID: "cust_extend", Token: "tok", Plan: "pro"})
	account := CloudAccount{ID: "ca_extend", Provider: CloudProviderLinode, ProviderIdentity: "uuid", Region: "us-ord", CredentialCiphertext: "v1.ciphertext", UpdatedAt: time.Now().UTC()}
	if _, _, err := s.SetLinodeCloudAccount("cust_extend", account, OperatorActor()); err != nil {
		t.Fatal(err)
	}
	initial := time.Now().UTC().Add(time.Minute)
	if won, err := s.AcquireCloudAccountLease(t.Context(), CloudAccountLease{BurstID: "burst_extend", CustomerID: "cust_extend", CloudAccountID: account.ID, ExpiresAt: initial}); err != nil || !won {
		t.Fatalf("acquire = %v, %v", won, err)
	}
	extended := time.Now().UTC().Add(7 * 24 * time.Hour)
	if err := s.ExtendCloudAccountLease(t.Context(), "burst_extend", "cust_extend", account.ID, extended); err != nil {
		t.Fatal(err)
	}
	if got := s.cloudAccountLeases["burst_extend"].ExpiresAt; !got.Equal(extended) {
		t.Fatalf("expiry = %v, want %v", got, extended)
	}
	if err := s.ExtendCloudAccountLease(t.Context(), "burst_extend", "cust_extend", account.ID, initial); err != nil {
		t.Fatal(err)
	}
	if got := s.cloudAccountLeases["burst_extend"].ExpiresAt; !got.Equal(extended) {
		t.Fatalf("extension shortened lease to %v", got)
	}
	if err := s.ExtendCloudAccountLease(t.Context(), "burst_extend", "other", account.ID, extended.Add(time.Hour)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mismatched extension err = %v", err)
	}
}

func TestExtendCloudAccountLeaseSQLIsExactAndMonotonic(t *testing.T) {
	sql := extendCloudAccountLeaseStmt(tblCloudAccountLeases)
	for _, want := range []string{"GREATEST(expires_at, $4)", "burst_id = $1", "customer_id = $2", "cloud_account_id = $3"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("extension SQL missing %q: %s", want, sql)
		}
	}
}
