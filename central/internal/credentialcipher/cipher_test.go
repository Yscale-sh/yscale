package credentialcipher

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestCipherRoundTripAndAADBinding(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	c, err := New(key)
	if err != nil {
		t.Fatal(err)
	}
	aad := AdditionalData("cust_a", "ca_1", "linode")
	envelope, err := c.Encrypt("secret-token", aad)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(envelope, "secret-token") {
		t.Fatal("envelope contains plaintext")
	}
	got, err := c.Decrypt(envelope, aad)
	if err != nil || got != "secret-token" {
		t.Fatalf("round trip = %q, %v", got, err)
	}
	if _, err := c.Decrypt(envelope, AdditionalData("cust_b", "ca_1", "linode")); err == nil {
		t.Fatal("ciphertext decrypted with different tenant AAD")
	}
}

func TestNewRejectsInvalidKeys(t *testing.T) {
	for _, key := range []string{"", "not-base64", base64.StdEncoding.EncodeToString(make([]byte, 31))} {
		if _, err := New(key); err == nil {
			t.Fatalf("New(%q) succeeded", key)
		}
	}
}
