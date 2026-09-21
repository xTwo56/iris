package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/application/acceptance"
	"github.com/xTwo56/iris/internal/event"
)

// EventAcceptor saves an event and its delivery work together. The application
// service owns the transaction and replay decision; HTTP owns neither.
type EventAcceptor interface {
	Accept(context.Context, string, event.Event) (acceptance.Result, error)
}

// acceptEvent preserves producer input and delegates the entire acceptance.
// RawMessage retains the JSON value's bytes (including internal whitespace),
// while nil distinguishes an omitted payload from the valid value null. Envelope
// whitespace outside that value is not payload. IDs and timestamps are never
// generated or rounded here, because they participate in replay comparison.
func (d Dependencies) acceptEvent(w http.ResponseWriter, r *http.Request) {
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || strings.TrimSpace(keys[0]) == "" {
		fail(w, http.StatusBadRequest, "invalid_input", "one nonblank Idempotency-Key is required")
		return
	}
	var body struct {
		ID        event.ID        `json:"id"`
		EventType event.Type      `json:"event_type"`
		CreatedAt time.Time       `json:"created_at"`
		Payload   json.RawMessage `json:"payload"`
	}
	if !decode(w, r, &body) {
		return
	}
	e, err := event.New(body.ID, body.EventType, body.CreatedAt, body.Payload)
	if err != nil {
		fail(w, http.StatusBadRequest, "invalid_input", "valid id, event_type, created_at and payload are required")
		return
	}
	// Only the service can decide whether this key was already accepted. Its
	// result is returned after commit, so errors never advertise acceptance.
	result, err := d.EventAcceptor.Accept(r.Context(), keys[0], e)
	if err != nil {
		if errors.Is(err, acceptance.ErrSubmissionConflict) || errors.Is(err, acceptance.ErrDuplicateEvent) {
			fail(w, http.StatusConflict, "conflict", "submission key or event ID conflicts with an accepted event")
		} else {
			fail(w, http.StatusInternalServerError, "internal_error", "internal server error")
		}
		return
	}
	status := http.StatusCreated
	if result.Replayed {
		status = http.StatusOK
	}
	write(w, status, struct {
		EventID       event.ID `json:"event_id"`
		DeliveryCount int      `json:"delivery_count"`
	}{result.EventID, result.DeliveryCount})
}
