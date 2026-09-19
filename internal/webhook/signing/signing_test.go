package signing

import (
	"bytes"
	"encoding/base64"
	"testing"
	"time"
)

func TestVectorAndTampering(t *testing.T) {
	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = byte(i)
	}
	body := []byte(" {\"n\":1.00}\n")
	at := time.Unix(1700000000, 0)
	h, err := Sign(secret, body, "event|one", "delivery\n:two", at)
	if err != nil {
		t.Fatal(err)
	}
	const expected = "0f5b6645b6a5a45bdfa0ad4c317b6dd2110636a56d73f708a52273b8d2ac2bce"
	if h.Signature != "v1="+expected {
		t.Fatalf("vector: %s", h.Signature)
	}
	if err := Verify(secret, body, h, at, time.Minute); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"body", "event", "delivery", "timestamp", "signature", "version"} {
		t.Run(field, func(t *testing.T) {
			changed := h
			b := bytes.Clone(body)
			switch field {
			case "body":
				b[0] = 'x'
			case "event":
				changed.EventID = base64.RawURLEncoding.EncodeToString([]byte("other"))
			case "delivery":
				changed.DeliveryID = base64.RawURLEncoding.EncodeToString([]byte("other"))
			case "timestamp":
				changed.Timestamp = "1700000001"
			case "signature":
				changed.Signature = "v1=" + string(bytes.Repeat([]byte("0"), 64))
			case "version":
				changed.Signature = "v2=" + expected
			}
			if Verify(secret, b, changed, at, time.Minute) == nil {
				t.Fatal("tampering accepted")
			}
		})
	}
	for _, offset := range []time.Duration{-61 * time.Second, 61 * time.Second} {
		if Verify(secret, body, h, at.Add(offset), time.Minute) == nil {
			t.Fatal("stale/future accepted")
		}
	}
	for _, offset := range []time.Duration{-60 * time.Second, 60 * time.Second} {
		if Verify(secret, body, h, at.Add(offset), time.Minute) != nil {
			t.Fatal("boundary rejected")
		}
	}
	if _, err := Sign(nil, body, "e", "d", at); err == nil {
		t.Fatal("missing credential")
	}
	if Verify(bytes.Repeat([]byte{1}, 32), body, h, at, time.Minute) == nil {
		t.Fatal("wrong key")
	}
	a, _ := Sign(secret, body, "a|b", "c", at)
	b, _ := Sign(secret, body, "a", "b|c", at)
	if a.Signature == b.Signature {
		t.Fatal("ambiguous field boundaries")
	}
}
