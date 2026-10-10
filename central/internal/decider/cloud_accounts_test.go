package decider

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/credentialcipher"
	"github.com/yscale-sh/yscale/central/internal/handlers"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/backends/linode"
)

func tenantCloudDecider(t *testing.T) (*Decider, *state.Store, *credentialcipher.Cipher, *recordingBackend, *string) {
	t.Helper()
	store := state.New()
	cipher, err := credentialcipher.New(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	accountID := "ca_tenant"
	encrypted, err := cipher.Encrypt("tenant-token-v1", credentialcipher.AdditionalData(state.DevCustomerID, accountID, state.CloudProviderLinode))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.SetLinodeCloudAccount(state.DevCustomerID, state.CloudAccount{ID: accountID, Provider: state.CloudProviderLinode, ProviderIdentity: "provider-uuid", Region: "us-ord", GPUImage: "private/tenant-gpu", CredentialCiphertext: encrypted, UpdatedAt: time.Now().UTC()}, state.OperatorActor())
	if err != nil {
		t.Fatal(err)
	}
	d, _, _ := newRecordingPlanDecider(t, backends.TypeLinode)
	d.WithStore(store).WithCredentialCipher(cipher)
	tenantBackend := &recordingBackend{name: backends.TypeLinode, createID: "tenant-node"}
	seenToken := ""
	d.linodeForAccount = func(token string, _ linode.Config) backends.Backend { seenToken = token; return tenantBackend }
	return d, store, cipher, tenantBackend, &seenToken
}

func TestPlanExplicitLinodeUsesTenantAccountAndStampsProvenance(t *testing.T) {
	d, store, _, backend, seenToken := tenantCloudDecider(t)
	wl := minimalPlanWorkload()
	wl.Spec.Backend = backends.TypeLinode
	plan, err := d.Plan(t.Context(), wl, handlers.PlanOptions{CustomerID: state.DevCustomerID})
	if err != nil {
		t.Fatal(err)
	}
	if *seenToken != "tenant-token-v1" || backend.createCalls != 1 || plan.CloudAccountID != "ca_tenant" {
		t.Fatalf("tenant route token=%q calls=%d account=%q", *seenToken, backend.createCalls, plan.CloudAccountID)
	}
	if err := store.ReleaseCloudAccountLease(t.Context(), plan.BurstID); err != nil {
		t.Fatal(err)
	}
}

func TestPlanAutoRTXUsesTenantAccount(t *testing.T) {
	d, store, _, backend, seenToken := tenantCloudDecider(t)
	wl := gpuPlanWorkload("", "rtx4000ada", 1, 1, 0)
	plan, err := d.Plan(t.Context(), wl, handlers.PlanOptions{CustomerID: state.DevCustomerID})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Backend != backends.TypeLinode || plan.CloudAccountID != "ca_tenant" || *seenToken != "tenant-token-v1" || backend.createCalls != 1 {
		t.Fatalf("auto plan = backend=%q account=%q token=%q calls=%d", plan.Backend, plan.CloudAccountID, *seenToken, backend.createCalls)
	}
	if err := store.ReleaseCloudAccountLease(t.Context(), plan.BurstID); err != nil {
		t.Fatal(err)
	}
}

func TestPlanTenantCredentialFailureNeverFallsBack(t *testing.T) {
	d, store, _, _, _ := tenantCloudDecider(t)
	account, _ := store.LinodeCloudAccount(state.DevCustomerID)
	account.CredentialCiphertext = "v1.invalid"
	_, _, err := store.SetLinodeCloudAccount(state.DevCustomerID, *account, state.OperatorActor())
	if err != nil {
		t.Fatal(err)
	}
	wl := minimalPlanWorkload()
	wl.Spec.Backend = backends.TypeLinode
	if _, err := d.Plan(t.Context(), wl, handlers.PlanOptions{CustomerID: state.DevCustomerID}); err == nil {
		t.Fatal("Plan succeeded with undecryptable tenant credential")
	}
}

func TestTeardownUsesRotatedTokenForExactAccount(t *testing.T) {
	d, store, cipher, backend, seenToken := tenantCloudDecider(t)
	account, _ := store.LinodeCloudAccount(state.DevCustomerID)
	account.CredentialCiphertext, _ = cipher.Encrypt("tenant-token-v2", credentialcipher.AdditionalData(state.DevCustomerID, account.ID, account.Provider))
	account.UpdatedAt = time.Now().UTC()
	if _, _, err := store.SetLinodeCloudAccount(state.DevCustomerID, *account, state.OperatorActor()); err != nil {
		t.Fatal(err)
	}
	err := d.Teardown(t.Context(), &state.Burst{ID: "burst_x", CustomerID: state.DevCustomerID, Backend: backends.TypeLinode, BackendID: "tenant-node", CloudAccountID: account.ID})
	if err != nil {
		t.Fatal(err)
	}
	if *seenToken != "tenant-token-v2" || backend.deleteCalls != 1 || backend.deleteID != "tenant-node" {
		t.Fatalf("teardown token=%q calls=%d id=%q", *seenToken, backend.deleteCalls, backend.deleteID)
	}
}

func TestTeardownStampedMissingAccountFailsClosed(t *testing.T) {
	d, store, _, backend, _ := tenantCloudDecider(t)
	account, _ := store.LinodeCloudAccount(state.DevCustomerID)
	if err := store.DisconnectLinodeCloudAccount(state.DevCustomerID, account.ID, time.Now().UTC(), state.OperatorActor()); err != nil {
		t.Fatal(err)
	}
	err := d.Teardown(t.Context(), &state.Burst{CustomerID: state.DevCustomerID, Backend: backends.TypeLinode, BackendID: "node", CloudAccountID: account.ID})
	if err == nil || backend.deleteCalls != 0 {
		t.Fatalf("teardown err=%v deletes=%d", err, backend.deleteCalls)
	}
}

func TestAmbiguousTenantCreateExtendsLeaseForManualAttention(t *testing.T) {
	d, store, _, backend, _ := tenantCloudDecider(t)
	// Unknown is fail-closed: an adapter omission must retain the lease.
	backend.createErr = errors.New("connection lost after request write")
	wl := minimalPlanWorkload()
	wl.Spec.Backend = backends.TypeLinode
	_, err := d.Plan(t.Context(), wl, handlers.PlanOptions{CustomerID: state.DevCustomerID})
	if err == nil {
		t.Fatal("ambiguous create unexpectedly succeeded")
	}
	account, _ := store.LinodeCloudAccount(state.DevCustomerID)
	if err := store.DisconnectLinodeCloudAccount(state.DevCustomerID, account.ID,
		time.Now().UTC().Add(24*time.Hour), state.OperatorActor()); !errors.Is(err, state.ErrCloudAccountInUse) {
		t.Fatalf("disconnect one day after ambiguous create = %v, want retained lease", err)
	}
}

func TestProvenTenantCreateFailureReleasesLease(t *testing.T) {
	d, store, _, backend, _ := tenantCloudDecider(t)
	backend.createErr = backends.MarkCreateProvenZeroResource(errors.New("request rejected before create"))
	wl := minimalPlanWorkload()
	wl.Spec.Backend = backends.TypeLinode
	_, err := d.Plan(t.Context(), wl, handlers.PlanOptions{CustomerID: state.DevCustomerID})
	if err == nil {
		t.Fatal("proven create failure unexpectedly succeeded")
	}
	account, _ := store.LinodeCloudAccount(state.DevCustomerID)
	if err := store.DisconnectLinodeCloudAccount(state.DevCustomerID, account.ID, time.Now().UTC(), state.OperatorActor()); err != nil {
		t.Fatalf("disconnect after proven create failure = %v", err)
	}
}
