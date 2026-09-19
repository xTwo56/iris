// Package endpointsecret encrypts per-endpoint signing secrets at rest.
package endpointsecret

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/xTwo56/iris/internal/endpoint"
)

const Format = 1

var ErrInvalid = errors.New("invalid encrypted endpoint secret")

// Cipher holds the runtime encryption key through AES-GCM, independent of bearer
// authentication and endpoint signing keys. Errors never contain secret material.
type Cipher struct{ aead cipher.AEAD }

// ParseKey requires canonical padded standard base64 encoding of exactly 32 bytes.
// Missing keys never cause generation of a replacement; existing data needs the same key.
func ParseKey(encoded string) ([]byte, error) {
	b, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(b) != 32 || base64.StdEncoding.EncodeToString(b) != encoded {
		return nil, errors.New("encryption key must be canonical base64 of 32 bytes")
	}
	return b, nil
}
func New(key []byte) (*Cipher, error) {
	if len(key) != 32 {
		return nil, ErrInvalid
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalid
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrInvalid
	}
	return &Cipher{aead: aead}, nil
}

// Sealed contains ciphertext only. AES-GCM's tag authenticates the encrypted secret.
type Sealed struct {
	Format            int
	Nonce, Ciphertext []byte
}

func aad(id endpoint.ID) []byte {
	b := []byte("iris-endpoint-secret")
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], Format)
	b = append(b, n[:]...)
	binary.BigEndian.PutUint64(n[:], uint64(len(id)))
	b = append(b, n[:]...)
	return append(b, []byte(id)...)
}

// Encrypt creates a fresh random nonce for every encryption. Associated data
// binds both format and endpoint identity, preventing copied ciphertext from
// decrypting under another endpoint even with the correct runtime key.
func (c *Cipher) Encrypt(id endpoint.ID, secret []byte) (Sealed, error) {
	if c == nil || c.aead == nil || strings.TrimSpace(string(id)) == "" || len(secret) != 32 {
		return Sealed{}, ErrInvalid
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return Sealed{}, errors.New("secret encryption failed")
	}
	return Sealed{Format, nonce, c.aead.Seal(nil, nonce, secret, aad(id))}, nil
}

// Decrypt authenticates before returning plaintext; unknown formats, corruption
// and wrong keys all fail closed. The caller must never log the returned bytes.
func (c *Cipher) Decrypt(id endpoint.ID, s Sealed) ([]byte, error) {
	if c == nil || c.aead == nil || strings.TrimSpace(string(id)) == "" || s.Format != Format || len(s.Nonce) != c.aead.NonceSize() || len(s.Ciphertext) != 32+c.aead.Overhead() {
		return nil, ErrInvalid
	}
	plain, err := c.aead.Open(nil, s.Nonce, s.Ciphertext, aad(id))
	if err != nil {
		return nil, ErrInvalid
	}
	return plain, nil
}
