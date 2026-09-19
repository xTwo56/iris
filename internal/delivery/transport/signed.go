package transport

import (
	"bytes"
	"context"
	"errors"
	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/event"
	"github.com/xTwo56/iris/internal/webhook/signing"
	"time"
)

// SignedSender authenticates requests before using the same protected transport.
// Secret lookup/decryption belongs to the caller. It cannot fall back to unsigned
// delivery when credentials are absent or invalid. The clock runs per invocation.
type SignedSender struct {
	sender *Sender
	now    func() time.Time
}

func NewSigned(sender *Sender, now func() time.Time) (*SignedSender, error) {
	if sender == nil || now == nil {
		return nil, errors.New("missing signing dependency")
	}
	return &SignedSender{sender, now}, nil
}

// Send freezes the exact bytes, signs them, and supplies only protected headers
// to the existing sender. No caller can inject Host, proxy or TLS configuration.
// Stable IDs persist across retries; each invocation gets a current timestamp.
func (s *SignedSender) Send(ctx context.Context, destination string, payload []byte, eventID event.ID, deliveryID delivery.ID, secret []byte) Observation {
	if ctx.Err() != nil {
		return failed(ctx.Err(), false)
	}
	body := bytes.Clone(payload)
	headers, err := signing.Sign(secret, body, string(eventID), string(deliveryID), s.now())
	if err != nil {
		return failed(signing.ErrInvalid, true)
	}
	return s.sender.send(ctx, destination, body, eventID, deliveryID, &headers)
}
