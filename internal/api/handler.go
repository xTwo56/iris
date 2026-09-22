// Package api exposes authenticated event acceptance, routing management and manual redelivery.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/endpoint"
	endpointpg "github.com/xTwo56/iris/internal/endpoint/postgres"
	"github.com/xTwo56/iris/internal/event"
	"github.com/xTwo56/iris/internal/subscription"
	subscriptionpg "github.com/xTwo56/iris/internal/subscription/postgres"
)

// Endpoints and Subscriptions accept request contexts and leave connection
// ownership with the bootstrap caller. Existing PostgreSQL repositories satisfy them.
type Endpoints interface {
	GetByID(context.Context, endpoint.ID) (endpoint.Endpoint, error)
	SetFanout(context.Context, endpoint.ID, bool) error
}
type Subscriptions interface {
	Create(context.Context, subscription.Subscription) error
	GetByID(context.Context, subscription.ID) (subscription.Subscription, error)
	SetEnabled(context.Context, subscription.ID, bool) error
}

// EndpointCreator atomically creates the endpoint and encrypted secret. Its return
// value is only exposed by the authenticated creation response.
type EndpointCreator interface {
	Create(context.Context, endpoint.Endpoint) (string, error)
}

// Dependencies supplies storage and application-generated metadata for testability.
type Dependencies struct {
	EventAcceptor   EventAcceptor
	Redeliverer     Redeliverer
	History         History
	EndpointCreator EndpointCreator
	Endpoints       Endpoints
	Subscriptions   Subscriptions
	EndpointID      func() (endpoint.ID, error)
	SubscriptionID  func() (subscription.ID, error)
	Now             func() time.Time
}

// New protects every route before decoding or database access. The management
// bearer token is independent of webhook signing secrets; deployment must supply
// HTTPS. Only a digest is retained for constant-time credential comparison.
func New(token string, d Dependencies) (http.Handler, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("management token is required")
	}
	if d.History == nil || d.Redeliverer == nil || d.EventAcceptor == nil || d.EndpointCreator == nil || d.Endpoints == nil || d.Subscriptions == nil || d.EndpointID == nil || d.SubscriptionID == nil || d.Now == nil {
		return nil, errors.New("missing API dependency")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", d.acceptEvent)
	mux.HandleFunc("GET /v1/events/{id}/deliveries", d.listDeliveries)
	mux.HandleFunc("GET /v1/deliveries/{id}", d.getDelivery)
	mux.HandleFunc("GET /v1/deliveries/{id}/runs", d.listRuns)
	mux.HandleFunc("GET /v1/runs/{id}/attempts", d.listAttempts)
	mux.HandleFunc("POST /v1/deliveries/{id}/redeliver", d.redeliver)
	mux.HandleFunc("POST /v1/endpoints", d.createEndpoint)
	mux.HandleFunc("GET /v1/endpoints/{id}", d.getEndpoint)
	mux.HandleFunc("PATCH /v1/endpoints/{id}", d.patchEndpoint)
	mux.HandleFunc("POST /v1/subscriptions", d.createSubscription)
	mux.HandleFunc("GET /v1/subscriptions/{id}", d.getSubscription)
	mux.HandleFunc("PATCH /v1/subscriptions/{id}", d.patchSubscription)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fail(w, 404, "not_found", "route not found") })
	expected := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		parts := strings.Fields(r.Header.Get("Authorization"))
		if len(values) != 1 || len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			unauthorized(w)
			return
		}
		actual := sha256.Sum256([]byte(parts[1]))
		if subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
			unauthorized(w)
			return
		}
		mux.ServeHTTP(w, r)
	}), nil
}
func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="iris"`)
	fail(w, 401, "unauthorized", "authentication required")
}
func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, status int, code, message string) {
	write(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

// decode accepts exactly one bounded JSON object. Unknown fields reject attempts
// to set immutable metadata; pointer booleans distinguish false from absent/null.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		fail(w, 400, "invalid_input", "invalid JSON body")
		return false
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		fail(w, 400, "invalid_input", "body must contain one JSON value")
		return false
	}
	return true
}

// storageError exposes stable API failures without leaking SQL, connection strings
// or internal details. Domain validation is handled before repository calls.
func storageError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, endpointpg.ErrNotFound), errors.Is(err, subscriptionpg.ErrNotFound):
		fail(w, 404, "not_found", "resource not found")
	case errors.Is(err, subscriptionpg.ErrEndpointNotFound):
		fail(w, 400, "invalid_input", "referenced endpoint does not exist")
	case errors.Is(err, endpointpg.ErrDuplicateID), errors.Is(err, subscriptionpg.ErrDuplicateID), errors.Is(err, subscriptionpg.ErrDuplicateEndpointType):
		fail(w, 409, "conflict", "resource already exists")
	default:
		fail(w, 500, "internal_error", "internal server error")
	}
}
func endpointJSON(e endpoint.Endpoint) any {
	return struct {
		ID        endpoint.ID `json:"id"`
		URL       string      `json:"url"`
		CreatedAt time.Time   `json:"created_at"`
		Fanout    bool        `json:"fanout"`
	}{e.ID(), e.URL(), e.CreatedAt(), e.Fanout()}
}
func subscriptionJSON(s subscription.Subscription) any {
	return struct {
		ID         subscription.ID `json:"id"`
		EndpointID endpoint.ID     `json:"endpoint_id"`
		EventType  event.Type      `json:"event_type"`
		CreatedAt  time.Time       `json:"created_at"`
		Enabled    bool            `json:"enabled"`
	}{s.ID(), s.EndpointID(), s.EventType(), s.CreatedAt(), s.Enabled()}
}

// createEndpoint generates identity and time in the application, validates the
// destination through the domain constructor, then persists using request context.
func (d Dependencies) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var body struct {
		URL string `json:"url"`
	}
	if !decode(w, r, &body) {
		return
	}
	id, err := d.EndpointID()
	if err != nil {
		storageError(w, err)
		return
	}
	at := d.Now().UTC().Truncate(time.Microsecond)
	if strings.TrimSpace(string(id)) == "" || at.IsZero() {
		storageError(w, errors.New("invalid generated metadata"))
		return
	}
	e, err := endpoint.New(id, body.URL, at)
	if err != nil {
		fail(w, 400, "invalid_input", "invalid endpoint URL")
		return
	}
	secret, err := d.EndpointCreator.Create(r.Context(), e)
	if err != nil {
		storageError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	write(w, 201, struct {
		ID            endpoint.ID `json:"id"`
		URL           string      `json:"url"`
		CreatedAt     time.Time   `json:"created_at"`
		Fanout        bool        `json:"fanout"`
		SigningSecret string      `json:"signing_secret"`
	}{e.ID(), e.URL(), e.CreatedAt(), e.Fanout(), secret})
}
func (d Dependencies) getEndpoint(w http.ResponseWriter, r *http.Request) {
	e, err := d.Endpoints.GetByID(r.Context(), endpoint.ID(r.PathValue("id")))
	if err != nil {
		storageError(w, err)
		return
	}
	write(w, 200, endpointJSON(e))
}

// patchEndpoint changes future delivery eligibility only. Existing obligations
// remain eligible; the repository has no metadata update API.
func (d Dependencies) patchEndpoint(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Fanout *bool `json:"fanout"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.Fanout == nil {
		fail(w, 400, "invalid_input", "fanout is required")
		return
	}
	if err := d.Endpoints.SetFanout(r.Context(), endpoint.ID(r.PathValue("id")), *body.Fanout); err != nil {
		storageError(w, err)
		return
	}
	d.getEndpoint(w, r)
}

// createSubscription validates an exact routing rule; the database verifies its
// endpoint reference and pair uniqueness, including disabled subscriptions.
func (d Dependencies) createSubscription(w http.ResponseWriter, r *http.Request) {
	var body struct {
		EndpointID endpoint.ID `json:"endpoint_id"`
		EventType  event.Type  `json:"event_type"`
	}
	if !decode(w, r, &body) {
		return
	}
	id, err := d.SubscriptionID()
	if err != nil {
		storageError(w, err)
		return
	}
	at := d.Now().UTC().Truncate(time.Microsecond)
	if strings.TrimSpace(string(id)) == "" || at.IsZero() {
		storageError(w, errors.New("invalid generated metadata"))
		return
	}
	s, err := subscription.New(id, body.EndpointID, body.EventType, at)
	if err != nil {
		fail(w, 400, "invalid_input", "endpoint_id and event_type must be nonblank")
		return
	}
	if err := d.Subscriptions.Create(r.Context(), s); err != nil {
		storageError(w, err)
		return
	}
	write(w, 201, subscriptionJSON(s))
}
func (d Dependencies) getSubscription(w http.ResponseWriter, r *http.Request) {
	s, err := d.Subscriptions.GetByID(r.Context(), subscription.ID(r.PathValue("id")))
	if err != nil {
		storageError(w, err)
		return
	}
	write(w, 200, subscriptionJSON(s))
}

// patchSubscription changes only future routing; false is a valid explicit value.
func (d Dependencies) patchSubscription(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.Enabled == nil {
		fail(w, 400, "invalid_input", "enabled is required")
		return
	}
	if err := d.Subscriptions.SetEnabled(r.Context(), subscription.ID(r.PathValue("id")), *body.Enabled); err != nil {
		storageError(w, err)
		return
	}
	d.getSubscription(w, r)
}
