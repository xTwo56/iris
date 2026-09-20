// Package webhookworker executes one existing delivery obligation per SDK invocation.
package webhookworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/delivery/transport"
	"github.com/xTwo56/iris/internal/endpoint"
	endpointpg "github.com/xTwo56/iris/internal/endpoint/postgres"
	"github.com/xTwo56/iris/internal/endpointsecret"
	secretpg "github.com/xTwo56/iris/internal/endpointsecret/postgres"
	"github.com/xTwo56/iris/internal/event"
	eventpg "github.com/xTwo56/iris/internal/event/postgres"
	"github.com/xtwo56/mercury/remoteworker"
)

// TaskType versions the dispatcher's payload: exactly delivery_id and run_id.
const TaskType = "webhook.deliver.v1"

// Safe errors are suitable for Mercury's persisted handler diagnostics. They
// deliberately exclude database errors, destination URLs and signing material.
var (
	ErrPayload           = errors.New("invalid webhook.deliver.v1 payload")
	ErrReferences        = errors.New("missing or inconsistent delivery references")
	ErrCredentials       = errors.New("endpoint signing credentials unavailable or invalid")
	ErrStorage           = errors.New("webhook reference storage unavailable")
	ErrStart             = errors.New("attempt start could not be saved")
	ErrOutcome           = errors.New("observed attempt outcome could not be saved")
	ErrObservation       = errors.New("sender returned an invalid observation")
	ErrRetryableDelivery = errors.New("webhook delivery observed a retryable failure")
	ErrPermanentDelivery = errors.New("webhook delivery observed a permanent failure")
)

// Repository interfaces expose reads and append-only history, never transaction
// ownership. Production wiring supplies Iris's pool, not an open transaction.
type Runs interface {
	GetByID(context.Context, delivery.RunID) (delivery.Run, error)
}
type Deliveries interface {
	GetByID(context.Context, delivery.ID) (delivery.Delivery, error)
}
type Events interface {
	GetByID(context.Context, event.ID) (event.Event, error)
}
type Endpoints interface {
	GetByID(context.Context, endpoint.ID) (endpoint.Endpoint, error)
}
type Attempts interface {
	AppendStart(context.Context, delivery.AttemptStart) error
	AppendOutcome(context.Context, delivery.AttemptOutcome) error
}

// Sender is implemented by transport.SignedSender. Test fakes observe calls;
// production still uses the unchanged SSRF-protected HTTPS transport.
type Sender interface {
	Send(context.Context, string, []byte, event.ID, delivery.ID, []byte) transport.Observation
}

// Dependencies separate durable history, credential access and network delivery.
// Credentials returns caller-owned plaintext to clear after this execution.
type Dependencies struct {
	Runs                           Runs
	Deliveries                     Deliveries
	Events                         Events
	Endpoints                      Endpoints
	Attempts                       Attempts
	Sender                         Sender
	Credentials                    func(context.Context, endpoint.ID) ([]byte, error)
	NewAttemptID                   func() (delivery.AttemptID, error)
	Now                            func() time.Time
	StorageTimeout, CleanupTimeout time.Duration
}

// Handler performs Iris delivery work once; the SDK surrounds it with Mercury
// lifecycle ownership and reporting. Dependencies must be safe for concurrent use.
type Handler struct{ deps Dependencies }

var _ remoteworker.Handler = (*Handler)(nil)

// New validates collaborators before registration with Mercury's runtime.
func New(deps Dependencies) (*Handler, error) {
	if deps.Runs == nil || deps.Deliveries == nil || deps.Events == nil || deps.Endpoints == nil || deps.Attempts == nil || deps.Sender == nil || deps.Credentials == nil || deps.NewAttemptID == nil || deps.Now == nil || deps.StorageTimeout <= 0 || deps.CleanupTimeout <= 0 {
		return nil, errors.New("invalid webhook worker dependencies or persistence bounds")
	}
	return &Handler{deps: deps}, nil
}

type payload struct {
	DeliveryID delivery.ID    `json:"delivery_id"`
	RunID      delivery.RunID `json:"run_id"`
}

func decode(raw json.RawMessage) (payload, error) {
	var p payload
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&p) != nil || strings.TrimSpace(string(p.DeliveryID)) == "" || strings.TrimSpace(string(p.RunID)) == "" {
		return p, ErrPayload
	}
	var extra any
	if !errors.Is(dec.Decode(&extra), io.EOF) {
		return p, ErrPayload
	}
	return p, nil
}

// Execute saves a start, sends once, then appends only what was observed. Mercury's
// SDK owns execution lifecycle and retries; this function never resends on error.
// Receiver processing may succeed before our outcome or Mercury completion is
// durable, so receivers still need deduplication by the stable delivery identity.
func (h *Handler) Execute(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := decode(raw)
	if err != nil {
		return nil, remoteworker.Permanent(ErrPayload)
	}
	// Read existing obligations, not current routing eligibility. Neither disabled
	// flags nor a not-yet-acknowledged outbox entry invalidates an existing run.
	read, cancel := context.WithTimeout(ctx, h.deps.StorageTimeout)
	value, e, ep, secret, err := h.load(read, p)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	defer clear(secret)
	id, err := h.deps.NewAttemptID()
	if err != nil {
		return nil, remoteworker.Retryable(ErrStart)
	}
	// Align with PostgreSQL precision so reconstructing the persisted start cannot
	// round it beyond a subsequently observed finish time.
	start, err := delivery.NewAttemptStart(id, p.RunID, h.deps.Now().UTC().Truncate(time.Microsecond))
	if err != nil {
		return nil, remoteworker.Retryable(ErrStart)
	}
	write, cancel := context.WithTimeout(ctx, h.deps.StorageTimeout)
	err = h.deps.Attempts.AppendStart(write, start)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, remoteworker.Retryable(ErrStart)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// No database transaction is open here. Preserve event bytes and stable IDs;
	// SignedSender supplies a fresh signature timestamp and enforces SSRF bounds.
	observed := h.deps.Sender.Send(ctx, ep.URL(), e.Payload(), e.ID(), value.ID(), secret)
	if observed.HTTPStatus == 0 && (ctx.Err() != nil || errors.Is(observed.Err, context.Canceled) || errors.Is(observed.Err, context.DeadlineExceeded)) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, observed.Err
	}
	// A real HTTP response remains an observation even when cancellation interrupts
	// optional body draining. Without a response, require an actual transport error.
	if observed.HTTPStatus == 0 && observed.Err == nil {
		return nil, remoteworker.Retryable(ErrObservation)
	}
	var status *int
	if observed.HTTPStatus != 0 {
		value := observed.HTTPStatus
		status = &value
	}
	outcome, err := delivery.NewAttemptOutcome(start, h.deps.Now().UTC().Truncate(time.Microsecond), status, observed.Classification)
	if err != nil {
		return nil, remoteworker.Retryable(ErrObservation)
	}
	persist := ctx
	limit := h.deps.StorageTimeout
	if ctx.Err() != nil {
		// Preserve only a genuinely observed result after cancellation. This bounded
		// context cannot send another request or report ownership back to Mercury.
		persist = context.WithoutCancel(ctx)
		limit = h.deps.CleanupTimeout
	}
	write, cancel = context.WithTimeout(persist, limit)
	err = h.deps.Attempts.AppendOutcome(write, outcome)
	cancel()
	if err != nil {
		return nil, remoteworker.Retryable(ErrOutcome)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	switch observed.Classification {
	case delivery.ClassificationSucceeded:
		result, _ := json.Marshal(struct {
			AttemptID delivery.AttemptID `json:"attempt_id"`
		}{id})
		return result, nil
	case delivery.ClassificationRetryableFailure:
		return nil, remoteworker.Retryable(ErrRetryableDelivery)
	case delivery.ClassificationPermanentFailure:
		return nil, remoteworker.Permanent(ErrPermanentDelivery)
	default:
		return nil, remoteworker.Retryable(ErrObservation)
	}
}

// load follows immutable references and obtains signing material before creating
// attempt history. It never consults current routing eligibility or job acknowledgment.
func (h *Handler) load(ctx context.Context, p payload) (delivery.Delivery, event.Event, endpoint.Endpoint, []byte, error) {
	var d delivery.Delivery
	var e event.Event
	var ep endpoint.Endpoint
	bad := func(err error) (delivery.Delivery, event.Event, endpoint.Endpoint, []byte, error) {
		return d, e, ep, nil, err
	}
	run, err := h.deps.Runs.GetByID(ctx, p.RunID)
	if err != nil {
		return bad(referenceError(err))
	}
	if run.ID() != p.RunID || run.DeliveryID() != p.DeliveryID {
		return bad(remoteworker.Permanent(ErrReferences))
	}
	d, err = h.deps.Deliveries.GetByID(ctx, p.DeliveryID)
	if err != nil {
		return bad(referenceError(err))
	}
	if d.ID() != p.DeliveryID {
		return bad(remoteworker.Permanent(ErrReferences))
	}
	e, err = h.deps.Events.GetByID(ctx, d.EventID())
	if err != nil {
		return bad(referenceError(err))
	}
	ep, err = h.deps.Endpoints.GetByID(ctx, d.EndpointID())
	if err != nil {
		return bad(referenceError(err))
	}
	if e.ID() != d.EventID() || ep.ID() != d.EndpointID() {
		return bad(remoteworker.Permanent(ErrReferences))
	}
	secret, err := h.deps.Credentials(ctx, ep.ID())
	if err != nil {
		clear(secret)
		if errors.Is(err, secretpg.ErrNotFound) || errors.Is(err, endpointsecret.ErrInvalid) {
			return bad(remoteworker.Permanent(ErrCredentials))
		}
		return bad(remoteworker.Retryable(ErrStorage))
	}
	if len(secret) != 32 {
		clear(secret)
		return bad(remoteworker.Permanent(ErrCredentials))
	}
	return d, e, ep, secret, nil
}
func referenceError(err error) error {
	if errors.Is(err, deliverypg.ErrRunNotFound) || errors.Is(err, deliverypg.ErrNotFound) || errors.Is(err, eventpg.ErrNotFound) || errors.Is(err, endpointpg.ErrNotFound) {
		return remoteworker.Permanent(ErrReferences)
	}
	return remoteworker.Retryable(ErrStorage)
}
