package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/xTwo56/iris/internal/application/redelivery"
	"github.com/xTwo56/iris/internal/delivery"
)

// Redeliverer owns locking, request replay and the run/outbox transaction. HTTP
// only validates the request and translates its committed result.
type Redeliverer interface {
	Redeliver(context.Context, string, delivery.ID) (redelivery.Result, error)
}

// redeliver accepts no execution overrides. Requiring an empty JSON object uses
// the same bounded, strict decoding as other routes; generated metadata never
// becomes part of the request comparison. Authentication runs before this method.
func (d Dependencies) redeliver(w http.ResponseWriter, r *http.Request) {
	keys := r.Header.Values("Idempotency-Key")
	id := delivery.ID(r.PathValue("id"))
	if len(keys) != 1 || strings.TrimSpace(keys[0]) == "" || strings.TrimSpace(string(id)) == "" {
		fail(w, 400, "invalid_input", "a delivery ID and one nonblank Idempotency-Key are required")
		return
	}
	var body *struct{}
	if !decode(w, r, &body) {
		return
	}
	if body == nil {
		fail(w, 400, "invalid_input", "body must be an empty JSON object")
		return
	}
	result, err := d.Redeliverer.Redeliver(r.Context(), keys[0], id)
	if err != nil {
		switch {
		case errors.Is(err, redelivery.ErrInvalidInput):
			fail(w, 400, "invalid_input", "invalid redelivery request")
		case errors.Is(err, redelivery.ErrNotFound):
			fail(w, 404, "not_found", "delivery not found")
		case errors.Is(err, redelivery.ErrConflict):
			fail(w, 409, "conflict", "redelivery key belongs to another delivery")
		case errors.Is(err, redelivery.ErrUnresolvedRun):
			fail(w, 409, "conflict", "delivery has a run without a confirmed terminal observation")
		default:
			fail(w, 500, "internal_error", "internal server error")
		}
		return
	}
	status := http.StatusCreated
	if result.Replayed {
		status = http.StatusOK
	}
	write(w, status, struct {
		DeliveryID delivery.ID    `json:"delivery_id"`
		RunID      delivery.RunID `json:"run_id"`
	}{result.DeliveryID, result.RunID})
}
