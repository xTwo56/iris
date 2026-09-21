package api_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xTwo56/iris/internal/api"
	"github.com/xTwo56/iris/internal/application/acceptance"
	"github.com/xTwo56/iris/internal/application/redelivery"
	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/event"
)

type redeliverFunc func(context.Context, string, delivery.ID) (redelivery.Result, error)

func (f redeliverFunc) Redeliver(ctx context.Context, key string, id delivery.ID) (redelivery.Result, error) {
	return f(ctx, key, id)
}
func redeliveryHandler(t *testing.T, svc api.Redeliverer) http.Handler {
	t.Helper()
	h, _, _ := setupWithServices(t, acceptFunc(func(context.Context, string, event.Event) (acceptance.Result, error) {
		t.Fatal("unexpected event acceptance")
		return acceptance.Result{}, nil
	}), svc)
	return h
}
func redeliveryRequest(ctx context.Context, h http.Handler, id, body string, keys ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/deliveries/"+id+"/redeliver", strings.NewReader(body)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer secret")
	for _, key := range keys {
		r.Header.Add("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestRedeliveryValidation(t *testing.T) {
	h := redeliveryHandler(t, redeliverFunc(func(context.Context, string, delivery.ID) (redelivery.Result, error) {
		t.Fatal("invalid input reached service")
		return redelivery.Result{}, nil
	}))
	for _, tt := range []struct {
		name, id, body string
		keys           []string
	}{
		{"missing key", "d", "{}", nil}, {"blank key", "d", "{}", []string{" "}}, {"multiple keys", "d", "{}", []string{"a", "b"}},
		{"blank ID", "%20", "{}", []string{"key"}}, {"empty body", "d", "", []string{"key"}}, {"null", "d", "null", []string{"key"}},
		{"array", "d", "[]", []string{"key"}}, {"override", "d", `{"run_id":"r"}`, []string{"key"}}, {"trailing", "d", "{} {}", []string{"key"}},
		{"oversized", "d", "{" + strings.Repeat(" ", 64<<10) + "}", []string{"key"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if w := redeliveryRequest(context.Background(), h, tt.id, tt.body, tt.keys...); w.Code != 400 {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
		})
	}
}
func TestRedeliveryResponses(t *testing.T) {
	for _, tt := range []struct {
		name   string
		err    error
		replay bool
		status int
	}{
		{"created", nil, false, 201}, {"replay", nil, true, 200}, {"missing", redelivery.ErrNotFound, false, 404},
		{"key conflict", redelivery.ErrConflict, false, 409}, {"unresolved", redelivery.ErrUnresolvedRun, false, 409},
		{"invalid", redelivery.ErrInvalidInput, false, 400}, {"storage", errors.New("private database detail"), false, 500},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := redeliveryHandler(t, redeliverFunc(func(got context.Context, key string, id delivery.ID) (redelivery.Result, error) {
				if got != ctx || key != "key" || id != "d" {
					t.Fatal("request identity/context changed")
				}
				return redelivery.Result{DeliveryID: id, RunID: "new-run", Replayed: tt.replay}, tt.err
			}))
			w := redeliveryRequest(ctx, h, "d", "{}", "key")
			if w.Code != tt.status || strings.Contains(w.Body.String(), "private database") {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
			if tt.err == nil && w.Body.String() != "{\"delivery_id\":\"d\",\"run_id\":\"new-run\"}\n" {
				t.Fatal(w.Body.String())
			}
		})
	}
}
