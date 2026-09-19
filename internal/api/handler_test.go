package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/xTwo56/iris/internal/api"
	"github.com/xTwo56/iris/internal/endpoint"
	endpointpg "github.com/xTwo56/iris/internal/endpoint/postgres"
	"github.com/xTwo56/iris/internal/subscription"
	subscriptionpg "github.com/xTwo56/iris/internal/subscription/postgres"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type endpoints struct {
	e   endpoint.Endpoint
	err error
	ctx context.Context
}

func (s *endpoints) Create(ctx context.Context, e endpoint.Endpoint) error {
	s.ctx = ctx
	if s.err != nil {
		return s.err
	}
	s.e = e
	return nil
}
func (s *endpoints) GetByID(ctx context.Context, id endpoint.ID) (endpoint.Endpoint, error) {
	s.ctx = ctx
	if s.err != nil {
		return endpoint.Endpoint{}, s.err
	}
	if s.e.ID() != id {
		return endpoint.Endpoint{}, endpointpg.ErrNotFound
	}
	return s.e, nil
}
func (s *endpoints) SetFanout(ctx context.Context, id endpoint.ID, v bool) error {
	if _, err := s.GetByID(ctx, id); err != nil {
		return err
	}
	if v {
		s.e.FanoutEnable()
	} else {
		s.e.FanoutDisable()
	}
	return nil
}

type subscriptions struct {
	s   subscription.Subscription
	err error
}

func (s *subscriptions) Create(_ context.Context, v subscription.Subscription) error {
	if s.err != nil {
		return s.err
	}
	s.s = v
	return nil
}
func (s *subscriptions) GetByID(_ context.Context, id subscription.ID) (subscription.Subscription, error) {
	if s.err != nil {
		return subscription.Subscription{}, s.err
	}
	if s.s.ID() != id {
		return subscription.Subscription{}, subscriptionpg.ErrNotFound
	}
	return s.s, nil
}
func (s *subscriptions) SetEnabled(ctx context.Context, id subscription.ID, v bool) error {
	if _, err := s.GetByID(ctx, id); err != nil {
		return err
	}
	if v {
		s.s.Enable()
	} else {
		s.s.Disable()
	}
	return nil
}
func setup(t *testing.T) (http.Handler, *endpoints, *subscriptions) {
	t.Helper()
	e, s := &endpoints{}, &subscriptions{}
	h, err := api.New("secret", api.Dependencies{Endpoints: e, Subscriptions: s, EndpointID: func() (endpoint.ID, error) { return "ep", nil }, SubscriptionID: func() (subscription.ID, error) { return "sub", nil }, Now: func() time.Time { return time.Date(2026, 9, 19, 0, 0, 0, 123456789, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	return h, e, s
}
func request(h http.Handler, method, path, body, auth string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestAuthentication(t *testing.T) {
	h, _, _ := setup(t)
	for _, route := range []struct{ m, p string }{{"POST", "/v1/endpoints"}, {"GET", "/v1/endpoints/ep"}, {"PATCH", "/v1/endpoints/ep"}, {"POST", "/v1/subscriptions"}, {"GET", "/v1/subscriptions/sub"}, {"PATCH", "/v1/subscriptions/sub"}} {
		for _, auth := range []string{"", "Bearer wrong", "Basic secret"} {
			w := request(h, route.m, route.p, `{}`, auth)
			if w.Code != 401 || w.Header().Get("WWW-Authenticate") == "" || strings.Contains(w.Body.String(), "secret") {
				t.Fatalf("auth response: %d %s", w.Code, w.Body)
			}
		}
	}
	if _, err := api.New("", api.Dependencies{}); err == nil {
		t.Fatal("missing token accepted")
	}
}
func TestOperations(t *testing.T) {
	h, e, s := setup(t)
	for _, tt := range []struct {
		m, p, b string
		status  int
	}{{"POST", "/v1/endpoints", `{"url":"HTTPS://Example.com/h%2f"}`, 201}, {"GET", "/v1/endpoints/ep", "", 200}, {"PATCH", "/v1/endpoints/ep", `{"fanout":false}`, 200}, {"PATCH", "/v1/endpoints/ep", `{"fanout":false}`, 200}, {"POST", "/v1/subscriptions", `{"endpoint_id":"ep","event_type":"order.created"}`, 201}, {"GET", "/v1/subscriptions/sub", "", 200}, {"PATCH", "/v1/subscriptions/sub", `{"enabled":false}`, 200}, {"PATCH", "/v1/subscriptions/sub", `{"enabled":false}`, 200}} {
		w := request(h, tt.m, tt.p, tt.b, "Bearer secret")
		if w.Code != tt.status {
			t.Fatalf("%s %s: %d %s", tt.m, tt.p, w.Code, w.Body)
		}
		var value map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		if value["id"] == nil || value["created_at"] == nil {
			t.Fatal("missing metadata")
		}
	}
	if e.e.Fanout() || s.s.Enabled() {
		t.Fatal("false not saved")
	}
	if e.e.URL() != "HTTPS://Example.com/h%2f" || e.e.CreatedAt().Nanosecond() != 123456000 {
		t.Fatal("metadata changed")
	}
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "context")
	r := httptest.NewRequest("GET", "/v1/endpoints/ep", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(httptest.NewRecorder(), r)
	if e.ctx.Value(key{}) != "context" {
		t.Fatal("request context lost")
	}
}
func TestInvalidBodies(t *testing.T) {
	h, _, _ := setup(t)
	for _, tt := range []struct{ m, p, b string }{{"POST", "/v1/endpoints", `{"url":"http://example.com"}`}, {"POST", "/v1/endpoints", `{"url":"https://example.com","id":"x"}`}, {"POST", "/v1/endpoints", `{} {}`}, {"POST", "/v1/endpoints", `null`}, {"POST", "/v1/endpoints", `[]`}, {"POST", "/v1/endpoints", ""}, {"POST", "/v1/endpoints", `{"url":"` + strings.Repeat("a", 65536) + `"}`}, {"PATCH", "/v1/endpoints/ep", `{}`}, {"PATCH", "/v1/endpoints/ep", `{"fanout":null}`}, {"PATCH", "/v1/endpoints/ep", `{"fanout":false,"url":"https://other.com"}`}, {"PATCH", "/v1/endpoints/ep", `{"fanout":"false"}`}, {"POST", "/v1/subscriptions", `{"endpoint_id":"ep","event_type":" "}`}, {"PATCH", "/v1/subscriptions/sub", `{}`}, {"PATCH", "/v1/subscriptions/sub", `{"enabled":null}`}, {"PATCH", "/v1/subscriptions/sub", `{"enabled":false,"endpoint_id":"other"}`}} {
		w := request(h, tt.m, tt.p, tt.b, "Bearer secret")
		if w.Code != 400 {
			t.Fatalf("%s: got %d", tt.p, w.Code)
		}
	}
}
func TestStorageErrors(t *testing.T) {
	for _, tt := range []struct {
		path, body string
		err        error
		want       int
	}{{"endpoints", `{"url":"https://example.com"}`, endpointpg.ErrDuplicateID, 409}, {"subscriptions", `{"endpoint_id":"ep","event_type":"t"}`, subscriptionpg.ErrDuplicateID, 409}, {"subscriptions", `{"endpoint_id":"ep","event_type":"t"}`, subscriptionpg.ErrDuplicateEndpointType, 409}, {"subscriptions", `{"endpoint_id":"ep","event_type":"t"}`, subscriptionpg.ErrEndpointNotFound, 400}, {"endpoints", `{"url":"https://example.com"}`, errors.New("database secret detail"), 500}} {
		h, e, s := setup(t)
		if tt.path == "endpoints" {
			e.err = tt.err
		} else {
			s.err = tt.err
		}
		w := request(h, "POST", "/v1/"+tt.path, tt.body, "Bearer secret")
		if w.Code != tt.want || strings.Contains(w.Body.String(), "secret") {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
	}
	h, _, _ := setup(t)
	for _, tt := range []struct{ m, p, b string }{{"GET", "/v1/endpoints/missing", ""}, {"PATCH", "/v1/endpoints/missing", `{"fanout":false}`}, {"GET", "/v1/subscriptions/missing", ""}, {"PATCH", "/v1/subscriptions/missing", `{"enabled":false}`}} {
		if w := request(h, tt.m, tt.p, tt.b, "Bearer secret"); w.Code != 404 {
			t.Fatal(w.Code)
		}
	}
}
