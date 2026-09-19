package endpointsecret

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func TestEncryption(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	secret := bytes.Repeat([]byte{2}, 32)
	c, err := New(key)
	if err != nil {
		t.Fatal(err)
	}
	a, err := c.Encrypt("ep|\n", secret)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Encrypt("ep|\n", secret)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a.Nonce, b.Nonce) || bytes.Equal(a.Ciphertext, b.Ciphertext) || bytes.Contains(a.Ciphertext, secret) {
		t.Fatal("encryption not randomized")
	}
	got, err := c.Decrypt("ep|\n", a)
	if err != nil || !bytes.Equal(got, secret) {
		t.Fatal("round trip failed")
	}
	other, _ := New(bytes.Repeat([]byte{3}, 32))
	if _, err := other.Decrypt("ep|\n", a); err == nil {
		t.Fatal("wrong key accepted")
	}
	if _, err := c.Decrypt("other", a); err == nil {
		t.Fatal("endpoint substitution accepted")
	}
	for _, kind := range []string{"ciphertext", "nonce", "format"} {
		t.Run(kind, func(t *testing.T) {
			s := Sealed{a.Format, bytes.Clone(a.Nonce), bytes.Clone(a.Ciphertext)}
			switch kind {
			case "ciphertext":
				s.Ciphertext[0] ^= 1
			case "nonce":
				s.Nonce[0] ^= 1
			case "format":
				s.Format++
			}
			if _, err := c.Decrypt("ep|\n", s); err == nil {
				t.Fatal("tamper accepted")
			}
		})
	}
}
func TestKeyEncoding(t *testing.T) {
	for _, s := range []string{"", "bad", base64.StdEncoding.EncodeToString(make([]byte, 31)), base64.StdEncoding.EncodeToString(make([]byte, 33)), base64.StdEncoding.EncodeToString(make([]byte, 32)) + "\n"} {
		if _, err := ParseKey(s); err == nil {
			t.Fatal("invalid key accepted")
		}
	}
	if _, err := ParseKey(base64.StdEncoding.EncodeToString(make([]byte, 32))); err != nil {
		t.Fatal(err)
	}
}
