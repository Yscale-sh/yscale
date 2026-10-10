package state

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/credentialcipher"
)

func testCustomerCredentialCipher(t *testing.T, fill byte) *credentialcipher.Cipher {
	t.Helper()
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{fill}, 32))
	cipher, err := credentialcipher.New(key)
	if err != nil {
		t.Fatal(err)
	}
	return cipher
}

func TestCustomerDataContainsOnlyTokenVerifierAndEncryptedMeshCredential(t *testing.T) {
	const token = "yscale_customer_plaintext_token"
	const apiKey = "mesh_plaintext_admin_key"
	cipher := testCustomerCredentialCipher(t, 0x41)
	p := &pgPersister{customerCredentialCipher: cipher}
	customer := &Customer{
		ID: "cust_secure", Token: token, Plan: "pro",
		Mesh: &MeshEndpoint{Provider: "box", LoginServer: "https://mesh.example", APIKey: apiKey, User: "tenant"},
	}

	data, err := p.customerData(customer)
	if err != nil {
		t.Fatal(err)
	}
	raw := string(data)
	for _, secret := range []string{token, apiKey} {
		if strings.Contains(raw, secret) {
			t.Fatalf("customer JSON contains plaintext credential %q: %s", secret, raw)
		}
	}
	if strings.Contains(raw, `"Token":`) || strings.Contains(raw, `"APIKey":`) {
		t.Fatalf("customer JSON retained a legacy plaintext field: %s", raw)
	}
	if !strings.Contains(raw, `"TokenHash":`) || !strings.Contains(raw, `"APIKeyCiphertext":`) {
		t.Fatalf("customer JSON lacks verifier or ciphertext: %s", raw)
	}

	var loaded Customer
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Token != "" || loaded.TokenHash != HashCustomerToken(token) {
		t.Fatalf("loaded bearer material = token %q hash %q", loaded.Token, loaded.TokenHash)
	}
	if loaded.Mesh == nil || loaded.Mesh.APIKey != "" || loaded.Mesh.APIKeyCiphertext == "" {
		t.Fatalf("loaded mesh material = %+v", loaded.Mesh)
	}
	if rewrite, err := p.prepareLoadedCustomerCredential(&loaded); err != nil || rewrite {
		t.Fatalf("prepare encrypted row = (rewrite %v, err %v)", rewrite, err)
	}
	if loaded.Mesh.APIKey != apiKey {
		t.Fatalf("decrypted mesh key = %q", loaded.Mesh.APIKey)
	}

	wrongTenant := loaded
	wrongTenant.ID = "cust_other"
	wrongTenant.Mesh = &MeshEndpoint{
		Provider: loaded.Mesh.Provider, APIKeyCiphertext: loaded.Mesh.APIKeyCiphertext,
	}
	if _, err := p.prepareLoadedCustomerCredential(&wrongTenant); err == nil {
		t.Fatal("credential envelope decrypted under another tenant identity")
	}
}

func TestLegacyCustomerCredentialsMigrateWithoutChangingAuthentication(t *testing.T) {
	const token = "legacy_customer_token"
	const apiKey = "legacy_mesh_admin_key"
	legacy := []byte(`{"ID":"cust_legacy","Token":"` + token + `","Plan":"pro","Mesh":{"Provider":"box","LoginServer":"https://mesh.example","APIKey":"` + apiKey + `","User":"tenant"}}`)
	var customer Customer
	if err := json.Unmarshal(legacy, &customer); err != nil {
		t.Fatal(err)
	}
	if customer.Token != token || customer.TokenHash != HashCustomerToken(token) || customer.Mesh.APIKey != apiKey {
		t.Fatalf("legacy decode = %+v", customer)
	}

	p := &pgPersister{customerCredentialCipher: testCustomerCredentialCipher(t, 0x42)}
	rewrite, err := p.prepareLoadedCustomerCredential(&customer)
	if err != nil || !rewrite {
		t.Fatalf("prepare legacy row = (rewrite %v, err %v)", rewrite, err)
	}
	if customer.Mesh.APIKeyCiphertext == "" {
		t.Fatal("legacy mesh key was not encrypted")
	}
	data, err := p.customerData(&customer)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), token) || strings.Contains(string(data), apiKey) {
		t.Fatalf("migrated row retained plaintext: %s", data)
	}

	// The same token still authenticates after the safe document is reloaded;
	// migration changes storage representation, not the customer credential.
	var reloaded Customer
	if err := json.Unmarshal(data, &reloaded); err != nil {
		t.Fatal(err)
	}
	store := emptyStore()
	store.applySnapshot(&snapshot{Customers: []*Customer{&reloaded}})
	if got, err := store.AuthCustomer(token); err != nil || got.ID != customer.ID {
		t.Fatalf("authentication after migration = (%v, %v)", got, err)
	}
	if _, err := store.AuthCustomer(token + "-wrong"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong token after migration = %v", err)
	}
}

func TestCustomerCredentialMigrationFailsClosed(t *testing.T) {
	legacyMesh := &Customer{
		ID: "cust_mesh", TokenHash: HashCustomerToken("token"),
		Mesh: &MeshEndpoint{Provider: "box", APIKey: "plaintext"},
	}
	if _, err := (&pgPersister{}).prepareLoadedCustomerCredential(legacyMesh); err == nil {
		t.Fatal("plaintext mesh credential migrated without an encryption key")
	}

	activeWithoutVerifier := &Customer{ID: "cust_no_verifier", Plan: "pro"}
	if _, err := (&pgPersister{}).prepareLoadedCustomerCredential(activeWithoutVerifier); err == nil {
		t.Fatal("active customer without a bearer verifier was accepted")
	}

	mismatch := []byte(`{"ID":"cust_mismatch","Token":"plaintext","TokenHash":"not-the-verifier"}`)
	var customer Customer
	if err := json.Unmarshal(mismatch, &customer); err == nil {
		t.Fatal("customer with contradictory plaintext token and verifier was accepted")
	}
}

func TestRotateCustomerCredentialCommitsVerifierAndInvalidatesOldToken(t *testing.T) {
	const customerID = "cust_rotate"
	const oldToken = "tok_cust_rotate"
	store, spy := storeWithSpy(customerID)

	newToken, err := store.RotateCustomerCredential(customerID, OperatorActor())
	if err != nil {
		t.Fatal(err)
	}
	if newToken == "" || newToken == oldToken || !strings.HasPrefix(newToken, "ysk_") {
		t.Fatalf("new token = %q", newToken)
	}
	if _, err := store.AuthCustomer(oldToken); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old token authentication = %v, want ErrNotFound", err)
	}
	if got, err := store.AuthCustomer(newToken); err != nil || got.ID != customerID {
		t.Fatalf("new token authentication = (%v, %v)", got, err)
	}

	raw := spy.customers[customerID]
	if bytes.Contains(raw, []byte(newToken)) || bytes.Contains(raw, []byte(oldToken)) || bytes.Contains(raw, []byte(`"Token"`)) {
		t.Fatalf("durable customer row contains plaintext credential material: %s", raw)
	}
	var persisted Customer
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Token != "" || persisted.TokenHash != HashCustomerToken(newToken) {
		t.Fatalf("persisted credential = token %q hash %q", persisted.Token, persisted.TokenHash)
	}
	events := spy.eventsWith(ActionTenantCredentialRotate)
	if len(events) != 1 || events[0].Actor.Kind != ActorOperator || events[0].TargetID != customerID ||
		events[0].Detail.Reason != ReasonTenantCredentialRotated {
		t.Fatalf("rotation audit events = %+v", events)
	}
	auditJSON, err := json.Marshal(events[0])
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(auditJSON, []byte(newToken)) || bytes.Contains(auditJSON, []byte(oldToken)) {
		t.Fatalf("audit row contains plaintext credential: %s", auditJSON)
	}

	restarted := emptyStore()
	restarted.applySnapshot(spy.recordedSnapshot(t))
	if _, err := restarted.AuthCustomer(oldToken); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old token after restart = %v, want ErrNotFound", err)
	}
	if got, err := restarted.AuthCustomer(newToken); err != nil || got.ID != customerID {
		t.Fatalf("new token after restart = (%v, %v)", got, err)
	}
}

func TestRotateCustomerCredentialFailsClosedOnPersistenceError(t *testing.T) {
	const oldToken = "old_customer_token"
	store := emptyStore()
	p := &failCustomerPersister{}
	store.persist = p
	store.AddCustomer(&Customer{ID: "cust_rotate", Token: oldToken, Plan: "pro"})
	p.err = errors.New("database unavailable")

	newToken, err := store.RotateCustomerCredential("cust_rotate", OperatorActor())
	if newToken != "" || !errors.Is(err, ErrPersistence) {
		t.Fatalf("rotation = (%q, %v), want empty token and ErrPersistence", newToken, err)
	}
	if got, err := store.AuthCustomer(oldToken); err != nil || got.ID != "cust_rotate" {
		t.Fatalf("old token after refused rotation = (%v, %v)", got, err)
	}
	if len(p.eventsWith(ActionTenantCredentialRotate)) != 0 {
		t.Fatal("refused credential rotation appended an audit row")
	}
}

func TestCustomerDataEncryptsReplacementMeshCredential(t *testing.T) {
	p := &pgPersister{customerCredentialCipher: testCustomerCredentialCipher(t, 0x43)}
	customer := &Customer{ID: "cust_mesh_rotate", TokenHash: HashCustomerToken("token")}
	var ciphertexts []string
	for _, apiKey := range []string{"old_mesh_key", "replacement_mesh_key"} {
		customer.Mesh = &MeshEndpoint{Provider: "box", APIKey: apiKey}
		data, err := p.customerData(customer)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(apiKey)) {
			t.Fatalf("replacement row contains plaintext mesh credential: %s", data)
		}
		var persisted Customer
		if err := json.Unmarshal(data, &persisted); err != nil {
			t.Fatal(err)
		}
		ciphertexts = append(ciphertexts, persisted.Mesh.APIKeyCiphertext)
	}
	if ciphertexts[0] == "" || ciphertexts[0] == ciphertexts[1] {
		t.Fatalf("mesh credential ciphertexts = %q", ciphertexts)
	}
}
