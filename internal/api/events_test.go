package api_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/application/acceptance"
	"github.com/xTwo56/iris/internal/event"
)

type acceptFunc func(context.Context, string, event.Event) (acceptance.Result, error)

func (f acceptFunc) Accept(ctx context.Context, key string, e event.Event) (acceptance.Result, error) {
	return f(ctx, key, e)
}

const eventTime = "2026-09-19T00:00:00.123456789Z"

// Insert raw payload text rather than marshaling it, so tests can detect changes
// to whitespace, escape spelling and numbers at the HTTP/application boundary.
func eventEnvelope(id, typ, at, payload string) string {
	return fmt.Sprintf(`{"id":%q,"event_type":%q,"created_at":%q,"payload":%s}`, id, typ, at, payload)
}

func eventRequest(ctx context.Context, h http.Handler, body string, keys ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer secret")
	r.Header.Set("Content-Type", "application/json")
	for _, key := range keys {
		r.Header.Add("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestEventAcceptanceResponseAndInput(t *testing.T) {
	for _, payload := range []string{"{ \n\"n\":1.00, \"escaped\":\"\\u0061\" }", "null", "[1, 2.00]", `"value"`, "false"} {
		for _, replay := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replay=%v", payload, replay), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				h, _, _ := setupWithAcceptor(t, acceptFunc(func(got context.Context, key string, e event.Event) (acceptance.Result, error) {
					calls++
					if got != ctx || key != "supplied-key" || e.ID() != "supplied-id" || e.Type() != "type" || string(e.Payload()) != payload || e.CreatedAt().Format(time.RFC3339Nano) != eventTime {
						t.Fatal("submission input or context changed")
					}
					return acceptance.Result{EventID: e.ID(), DeliveryCount: 2, Replayed: replay}, nil
				}))
				w := eventRequest(ctx, h, eventEnvelope("supplied-id", "type", eventTime, payload), "supplied-key")
				want := http.StatusCreated
				if replay {
					want = http.StatusOK
				}
				if calls != 1 || w.Code != want || w.Body.String() != "{\"event_id\":\"supplied-id\",\"delivery_count\":2}\n" || w.Header().Get("Content-Type") != "application/json" {
					t.Fatalf("calls=%d, response=%d %s", calls, w.Code, w.Body)
				}
			})
		}
	}
}

func TestEventInvalidRequests(t *testing.T) {
	valid := eventEnvelope("id", "type", eventTime, "null")
	for _, tt := range []struct {
		name, body string
		keys       []string
	}{
		{"missing key", valid, nil},
		{"blank key", valid, []string{" \t"}},
		{"duplicate key header", valid, []string{"key", "key"}},
		{"omitted payload", `{"id":"id","event_type":"type","created_at":"` + eventTime + `"}`, []string{"key"}},
		{"blank ID", eventEnvelope(" ", "type", eventTime, "null"), []string{"key"}},
		{"blank type", eventEnvelope("id", " ", eventTime, "null"), []string{"key"}},
		{"invalid timestamp", eventEnvelope("id", "type", "yesterday", "null"), []string{"key"}},
		{"zero timestamp", eventEnvelope("id", "type", "0001-01-01T00:00:00Z", "null"), []string{"key"}},
		{"missing timestamp", `{"id":"id","event_type":"type","payload":null}`, []string{"key"}},
		{"invalid payload", eventEnvelope("id", "type", eventTime, "{"), []string{"key"}},
		{"unknown envelope field", strings.TrimSuffix(valid, "}") + `,"extra":true}`, []string{"key"}},
		{"trailing value", valid + " null", []string{"key"}},
		{"trailing junk", valid + " !", []string{"key"}},
		{"null envelope", "null", []string{"key"}},
		{"array envelope", "[]", []string{"key"}},
		{"empty body", "", []string{"key"}},
		{"oversized payload", eventEnvelope("id", "type", eventTime, `"`+strings.Repeat("a", 64<<10)+`"`), []string{"key"}},
		{"oversized envelope whitespace", valid + strings.Repeat(" ", 64<<10), []string{"key"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h, _, _ := setup(t) // fails if malformed requests reach acceptance
			w := eventRequest(context.Background(), h, tt.body, tt.keys...)
			if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), `"code":"invalid_input"`) {
				t.Fatalf("response=%d %s", w.Code, w.Body)
			}
		})
	}
}

func TestEventAcceptanceErrors(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want int
	}{
		{fmt.Errorf("wrapped: %w", acceptance.ErrSubmissionConflict), 409},
		{fmt.Errorf("wrapped: %w", acceptance.ErrDuplicateEvent), 409},
		{errors.New("private database details"), 500},
	} {
		h, _, _ := setupWithAcceptor(t, acceptFunc(func(context.Context, string, event.Event) (acceptance.Result, error) {
			return acceptance.Result{}, tt.err
		}))
		w := eventRequest(context.Background(), h, eventEnvelope("id", "type", eventTime, "null"), "key")
		if w.Code != tt.want || strings.Contains(w.Body.String(), "private") || strings.Contains(w.Body.String(), "event_id") {
			t.Fatalf("response=%d %s", w.Code, w.Body)
		}
	}
}
