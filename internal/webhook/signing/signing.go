// Package signing authenticates webhook messages; it does not encrypt payloads.
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"strconv"
	"strings"
	"time"
)

const (
	TimestampHeader = "X-Iris-Timestamp"
	SignatureHeader = "X-Iris-Signature"
	EventHeader     = "X-Iris-Event-ID"
	DeliveryHeader  = "X-Iris-Delivery-ID"
)

var ErrInvalid = errors.New("invalid webhook signing input")

// Headers contains only wire-safe metadata, never the signing secret. IDs use
// unpadded base64url so arbitrary domain strings survive HTTP header transport.
type Headers struct{ Timestamp, Signature, EventID, DeliveryID string }

// Sign authenticates exact bytes. The format is the ASCII prefix "iris-webhook-v1"
// followed by four uint64 big-endian length-prefixed fields: decimal Unix seconds,
// raw event ID, raw delivery ID, and body. HMAC-SHA256 is lowercase hex after "v1=".
func Sign(secret, body []byte, eventID, deliveryID string, at time.Time) (Headers, error) {
	if len(secret) != 32 || strings.TrimSpace(eventID) == "" || strings.TrimSpace(deliveryID) == "" || at.IsZero() || at.Unix() < 0 {
		return Headers{}, ErrInvalid
	}
	stamp := strconv.FormatInt(at.Unix(), 10)
	mac := digest(secret, body, stamp, eventID, deliveryID)
	return Headers{stamp, "v1=" + hex.EncodeToString(mac), base64.RawURLEncoding.EncodeToString([]byte(eventID)), base64.RawURLEncoding.EncodeToString([]byte(deliveryID))}, nil
}
func field(h hash.Hash, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	h.Write(n[:])
	h.Write(b)
}
func digest(secret, body []byte, stamp, eventID, deliveryID string) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte("iris-webhook-v1"))
	field(h, []byte(stamp))
	field(h, []byte(eventID))
	field(h, []byte(deliveryID))
	field(h, body)
	return h.Sum(nil)
}

// Verify is a small receiver-side verifier. It checks timestamp age in both
// directions and uses hmac.Equal for authentication. Receivers must still dedupe
// delivery IDs; tolerance limits replay age but cannot prevent in-window replay.
func Verify(secret, body []byte, headers Headers, now time.Time, tolerance time.Duration) error {
	if len(secret) != 32 || tolerance < 0 || now.IsZero() {
		return ErrInvalid
	}
	seconds, err := strconv.ParseInt(headers.Timestamp, 10, 64)
	if err != nil || seconds < 0 || strconv.FormatInt(seconds, 10) != headers.Timestamp {
		return ErrInvalid
	}
	at := time.Unix(seconds, 0)
	if at.Before(now.Add(-tolerance)) || at.After(now.Add(tolerance)) {
		return ErrInvalid
	}
	eventID, err := base64.RawURLEncoding.Strict().DecodeString(headers.EventID)
	if err != nil {
		return ErrInvalid
	}
	deliveryID, err := base64.RawURLEncoding.Strict().DecodeString(headers.DeliveryID)
	if err != nil || strings.TrimSpace(string(eventID)) == "" || strings.TrimSpace(string(deliveryID)) == "" {
		return ErrInvalid
	}
	if !strings.HasPrefix(headers.Signature, "v1=") {
		return ErrInvalid
	}
	mac, err := hex.DecodeString(strings.TrimPrefix(headers.Signature, "v1="))
	if err != nil {
		return ErrInvalid
	}
	if !hmac.Equal(mac, digest(secret, body, headers.Timestamp, string(eventID), string(deliveryID))) {
		return ErrInvalid
	}
	return nil
}
