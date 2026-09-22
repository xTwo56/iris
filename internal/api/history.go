package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/endpoint"
	"github.com/xTwo56/iris/internal/event"
)

// History reads only persisted Iris data. Page methods fetch at most limit+1
// records, allowing next-page detection without unbounded reads or item lookups.
type History interface {
	GetByID(context.Context, delivery.ID) (delivery.Delivery, error)
	DeliveryPage(context.Context, event.ID, int, deliverypg.HistoryCursor) ([]delivery.Delivery, error)
	RunPage(context.Context, delivery.ID, int, deliverypg.HistoryCursor) ([]deliverypg.RunHistory, error)
	AttemptPage(context.Context, delivery.RunID, int, deliverypg.HistoryCursor) ([]deliverypg.AttemptHistory, error)
}

// Explicit DTOs allow only public history fields. Neither persistence structs nor
// secrets/outbox records are serialized, and no overall delivery status is invented.
type deliveryDTO struct {
	ID         delivery.ID `json:"id"`
	EventID    event.ID    `json:"event_id"`
	EndpointID endpoint.ID `json:"endpoint_id"`
	CreatedAt  time.Time   `json:"created_at"`
}

func deliveryResponse(d delivery.Delivery) deliveryDTO {
	return deliveryDTO{d.ID(), d.EventID(), d.EndpointID(), d.CreatedAt()}
}

type terminalDTO struct {
	MercuryJobID string                 `json:"mercury_job_id"`
	State        delivery.TerminalState `json:"state"`
	ObservedAt   time.Time              `json:"observed_at"`
}
type runDTO struct {
	ID         delivery.RunID   `json:"id"`
	DeliveryID delivery.ID      `json:"delivery_id"`
	CreatedAt  time.Time        `json:"created_at"`
	Trigger    delivery.Trigger `json:"trigger"`
	Resolution string           `json:"resolution"`
	Terminal   *terminalDTO     `json:"terminal_observation"`
}
type outcomeDTO struct {
	FinishedAt     time.Time               `json:"finished_at"`
	HTTPStatus     *int                    `json:"http_status"`
	Classification delivery.Classification `json:"classification"`
}
type attemptDTO struct {
	ID          delivery.AttemptID `json:"id"`
	RunID       delivery.RunID     `json:"run_id"`
	StartedAt   time.Time          `json:"started_at"`
	Observation string             `json:"observation"`
	Outcome     *outcomeDTO        `json:"outcome"`
}
type historyPage[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"next_cursor"`
}

// Cursors bind a position to its collection and parent. They are opaque routing
// data, not credentials or snapshots. Arbitrary string IDs remain unambiguous in
// versioned JSON; strict decoding rejects malformed or cross-collection cursors.
type historyCursor struct {
	Version int       `json:"v"`
	Kind    string    `json:"kind"`
	Parent  string    `json:"parent"`
	At      time.Time `json:"at"`
	ID      string    `json:"id"`
}

func historyParams(r *http.Request, kind string) (int, deliverypg.HistoryCursor, error) {
	bad := errors.New("invalid history query")
	// ParseQuery errors must not be silently discarded by URL.Query.
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || strings.TrimSpace(r.PathValue("id")) == "" {
		return 0, deliverypg.HistoryCursor{}, bad
	}
	limit := 50
	for key, v := range values {
		if (key != "limit" && key != "cursor") || len(v) != 1 || v[0] == "" {
			return 0, deliverypg.HistoryCursor{}, bad
		}
	}
	if raw := values.Get("limit"); raw != "" {
		for _, digit := range raw {
			if digit < '0' || digit > '9' {
				return 0, deliverypg.HistoryCursor{}, bad
			}
		}
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > deliverypg.MaxHistoryLimit {
			return 0, deliverypg.HistoryCursor{}, bad
		}
	}
	if raw := values.Get("cursor"); raw != "" {
		if len(raw) > 16384 {
			return 0, deliverypg.HistoryCursor{}, bad
		}
		data, err := base64.RawURLEncoding.Strict().DecodeString(raw)
		if err != nil {
			return 0, deliverypg.HistoryCursor{}, bad
		}
		var c historyCursor
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if dec.Decode(&c) != nil || dec.Decode(new(any)) != io.EOF || c.Version != 1 || c.Kind != kind || c.Parent != r.PathValue("id") || c.At.IsZero() || c.At.Year() < 1 || c.At.Year() > 9999 || c.At.Nanosecond()%1000 != 0 || strings.TrimSpace(c.ID) == "" {
			return 0, deliverypg.HistoryCursor{}, bad
		}
		return limit, deliverypg.HistoryCursor{At: c.At, ID: c.ID}, nil
	}
	return limit, deliverypg.HistoryCursor{}, nil
}
func nextHistoryCursor(kind, parent string, at time.Time, id string) (*string, error) {
	data, err := json.Marshal(historyCursor{1, kind, parent, at.UTC(), id})
	if err != nil {
		return nil, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(data)
	if len(encoded) > 16384 {
		return nil, errors.New("history cursor exceeds size limit")
	}
	return &encoded, nil
}
func historyError(w http.ResponseWriter, err error) {
	if errors.Is(err, deliverypg.ErrNotFound) || errors.Is(err, deliverypg.ErrHistoryParentNotFound) {
		fail(w, 404, "not_found", "history resource not found")
		return
	}
	fail(w, 500, "internal_error", "internal server error")
}
func (d Dependencies) getDelivery(w http.ResponseWriter, r *http.Request) {
	if strings.TrimSpace(r.PathValue("id")) == "" {
		fail(w, 400, "invalid_input", "delivery ID is required")
		return
	}
	value, err := d.History.GetByID(r.Context(), delivery.ID(r.PathValue("id")))
	if err != nil {
		historyError(w, err)
		return
	}
	write(w, 200, deliveryResponse(value))
}

// listDeliveries validates the cursor before storage access. Parent existence is
// checked by storage even for pages beyond the end; empty histories return [].
func (d Dependencies) listDeliveries(w http.ResponseWriter, r *http.Request) {
	limit, c, err := historyParams(r, "deliveries")
	if err != nil {
		fail(w, 400, "invalid_input", "invalid limit or cursor")
		return
	}
	values, err := d.History.DeliveryPage(r.Context(), event.ID(r.PathValue("id")), limit, c)
	if err != nil {
		historyError(w, err)
		return
	}
	page := historyPage[deliveryDTO]{Items: make([]deliveryDTO, 0)}
	if len(values) > limit {
		last := values[limit-1]
		page.NextCursor, err = nextHistoryCursor("deliveries", r.PathValue("id"), last.CreatedAt(), string(last.ID()))
		if err != nil {
			historyError(w, err)
			return
		}
		values = values[:limit]
	}
	for _, v := range values {
		page.Items = append(page.Items, deliveryResponse(v))
	}
	write(w, 200, page)
}

// listRuns reports only confirmed Mercury observations. Absence remains unresolved;
// it says nothing about current execution or any individual HTTP request.
func (d Dependencies) listRuns(w http.ResponseWriter, r *http.Request) {
	limit, c, err := historyParams(r, "runs")
	if err != nil {
		fail(w, 400, "invalid_input", "invalid limit or cursor")
		return
	}
	values, err := d.History.RunPage(r.Context(), delivery.ID(r.PathValue("id")), limit, c)
	if err != nil {
		historyError(w, err)
		return
	}
	page := historyPage[runDTO]{Items: make([]runDTO, 0)}
	if len(values) > limit {
		last := values[limit-1].Run
		page.NextCursor, err = nextHistoryCursor("runs", r.PathValue("id"), last.CreatedAt(), string(last.ID()))
		if err != nil {
			historyError(w, err)
			return
		}
		values = values[:limit]
	}
	for _, v := range values {
		item := runDTO{ID: v.Run.ID(), DeliveryID: v.Run.DeliveryID(), CreatedAt: v.Run.CreatedAt(), Trigger: v.Run.Trigger(), Resolution: "unresolved"}
		if v.Terminal != nil {
			item.Resolution = "terminal"
			item.Terminal = &terminalDTO{v.Terminal.MercuryJobID(), v.Terminal.State(), v.Terminal.ObservedAt()}
		}
		page.Items = append(page.Items, item)
	}
	write(w, 200, page)
}

// listAttempts includes unknown results and preserves nullable status separately
// from outcome presence. It never fills an outcome from the run's terminal state.
func (d Dependencies) listAttempts(w http.ResponseWriter, r *http.Request) {
	limit, c, err := historyParams(r, "attempts")
	if err != nil {
		fail(w, 400, "invalid_input", "invalid limit or cursor")
		return
	}
	values, err := d.History.AttemptPage(r.Context(), delivery.RunID(r.PathValue("id")), limit, c)
	if err != nil {
		historyError(w, err)
		return
	}
	page := historyPage[attemptDTO]{Items: make([]attemptDTO, 0)}
	if len(values) > limit {
		last := values[limit-1].Start
		page.NextCursor, err = nextHistoryCursor("attempts", r.PathValue("id"), last.StartedAt(), string(last.ID()))
		if err != nil {
			historyError(w, err)
			return
		}
		values = values[:limit]
	}
	for _, v := range values {
		item := attemptDTO{ID: v.Start.ID(), RunID: v.Start.RunID(), StartedAt: v.Start.StartedAt(), Observation: "unknown"}
		if v.Outcome != nil {
			item.Observation = "observed"
			item.Outcome = &outcomeDTO{FinishedAt: v.Outcome.FinishedAt(), Classification: v.Outcome.Classification()}
			if status, present := v.Outcome.HTTPStatus(); present {
				item.Outcome.HTTPStatus = &status
			}
		}
		page.Items = append(page.Items, item)
	}
	write(w, 200, page)
}
