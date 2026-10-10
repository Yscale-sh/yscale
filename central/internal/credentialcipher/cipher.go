// Package credentialcipher encrypts tenant-owned provider credentials before
// they enter application state or persistence.
package credentialcipher

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

const envelopeVersion = "v1"

var ErrInvalidMasterKey = errors.New("credential master key must be base64-encoded 32 bytes")

type Cipher struct{ aead cipher.AEAD }

func New(masterKeyBase64 string) (*Cipher, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(masterKeyBase64))
	if err != nil || len(key) != 32 {
		return nil, ErrInvalidMasterKey
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("initialize credential cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("initialize credential AEAD: %w", err)
	}
	return &Cipher{aead: aead}, nil
}

// AdditionalData binds a credential to its tenant, stable account id and
// provider. A ciphertext copied to another record therefore cannot decrypt.
func AdditionalData(tenantID, accountID, provider string) []byte {
	return []byte(tenantID + "\x00" + accountID + "\x00" + provider)
}

func (c *Cipher) Encrypt(plaintext string, additionalData []byte) (string, error) {
	if c == nil || c.aead == nil {
		return "", errors.New("credential encryption is unavailable")
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate credential nonce: %w", err)
	}
	sealed := c.aead.Seal(nonce, nonce, []byte(plaintext), additionalData)
	return envelopeVersion + "." + base64.RawStdEncoding.EncodeToString(sealed), nil
}

func (c *Cipher) Decrypt(envelope string, additionalData []byte) (string, error) {
	if c == nil || c.aead == nil {
		return "", errors.New("credential decryption is unavailable")
	}
	version, encoded, ok := strings.Cut(envelope, ".")
	if !ok || version != envelopeVersion {
		return "", errors.New("unsupported credential envelope")
	}
	sealed, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil || len(sealed) < c.aead.NonceSize()+c.aead.Overhead() {
		return "", errors.New("invalid credential envelope")
	}
	nonce := sealed[:c.aead.NonceSize()]
	plain, err := c.aead.Open(nil, nonce, sealed[c.aead.NonceSize():], additionalData)
	if err != nil {
		return "", errors.New("credential authentication failed")
	}
	return string(plain), nil
}
