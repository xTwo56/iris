package api_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/api"
	"github.com/xTwo56/iris/internal/application/acceptance"
	"github.com/xTwo56/iris/internal/application/redelivery"
	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/event"
)

type historyStub struct {
	deliveries []delivery.Delivery
	runs       []deliverypg.RunHistory
	attempts   []deliverypg.AttemptHistory
	err        error
	calls      int
	ctx        context.Context
	limit      int
	cursor     deliverypg.HistoryCursor
}

func (s *historyStub) read(ctx context.Context, limit int, c deliverypg.HistoryCursor) {
	s.calls++
	s.ctx = ctx
	s.limit = limit
	s.cursor = c
}
func (s *historyStub) GetByID(ctx context.Context, _ delivery.ID) (delivery.Delivery, error) {
	s.read(ctx, 0, deliverypg.HistoryCursor{})
	if len(s.deliveries) > 0 {
		return s.deliveries[0], s.err
	}
	return delivery.Delivery{}, s.err
}
func (s *historyStub) DeliveryPage(ctx context.Context, _ event.ID, limit int, c deliverypg.HistoryCursor) ([]delivery.Delivery, error) {
	s.read(ctx, limit, c)
	return s.deliveries, s.err
}
func (s *historyStub) RunPage(ctx context.Context, _ delivery.ID, limit int, c deliverypg.HistoryCursor) ([]deliverypg.RunHistory, error) {
	s.read(ctx, limit, c)
	return s.runs, s.err
}
func (s *historyStub) AttemptPage(ctx context.Context, _ delivery.RunID, limit int, c deliverypg.HistoryCursor) ([]deliverypg.AttemptHistory, error) {
	s.read(ctx, limit, c)
	return s.attempts, s.err
}
func historyHandler(t *testing.T, history api.History) http.Handler {
	t.Helper()
	h, _, _ := setupWithHistory(t, acceptFunc(func(context.Context, string, event.Event) (acceptance.Result, error) {
		t.Fatal("history called acceptance")
		return acceptance.Result{}, nil
	}), redeliverFunc(func(context.Context, string, delivery.ID) (redelivery.Result, error) {
		t.Fatal("history called redelivery")
		return redelivery.Result{}, nil
	}), history)
	return h
}
func TestHistoryValidation(t *testing.T) {
	stub := &historyStub{}
	h := historyHandler(t, stub)
	cursor := func(changes map[string]any) string {
		value := map[string]any{"v": 1, "kind": "deliveries", "parent": "event", "at": "2026-09-22T00:00:00Z", "id": "d"}
		for k, v := range changes {
			value[k] = v
		}
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(data)
	}
	for _, q := range []string{"limit=0", "limit=201", "limit=-1", "limit=+1", "limit=1.5", "limit=9999999999999999999999", "limit=1&limit=2", "limit=", "cursor=", "cursor=!", "other=1", "limit=%xx", "cursor=a&cursor=b", "cursor=" + strings.Repeat("a", 16385), "cursor=" + cursor(map[string]any{"v": 2}), "cursor=" + cursor(map[string]any{"kind": "runs"}), "cursor=" + cursor(map[string]any{"parent": "another"}), "cursor=" + cursor(map[string]any{"at": "0001-01-01T00:00:00Z"}), "cursor=" + cursor(map[string]any{"at": "2026-09-22T00:00:00.000000001Z"}), "cursor=" + cursor(map[string]any{"id": " "}), "cursor=" + cursor(map[string]any{"extra": true})} {
		w := request(h, "GET", "/v1/events/event/deliveries?"+q, "", "Bearer secret")
		if w.Code != 400 {
			t.Fatalf("%s: %d %s", q, w.Code, w.Body)
		}
	}
	if stub.calls != 0 {
		t.Fatal("invalid request reached storage")
	}
	w := request(h, "GET", "/v1/events/event/deliveries?limit=2&cursor="+cursor(nil), "", "Bearer secret")
	if w.Code != 200 || stub.limit != 2 || stub.cursor.ID != "d" {
		t.Fatalf("valid cursor: %d %+v", w.Code, stub)
	}
	for _, path := range []string{"/v1/events/%20/deliveries", "/v1/deliveries/%20", "/v1/deliveries/%20/runs", "/v1/runs/%20/attempts"} {
		if w := request(h, "GET", path, "", "Bearer secret"); w.Code != 400 {
			t.Fatalf("blank parent: %d", w.Code)
		}
	}
}
func TestHistoryEmptyAndErrors(t *testing.T) {
	for _, path := range []string{"/v1/events/e/deliveries", "/v1/deliveries/d/runs", "/v1/runs/r/attempts"} {
		stub := &historyStub{}
		h := historyHandler(t, stub)
		if w := request(h, "GET", path, "", "Bearer secret"); w.Code != 200 || w.Body.String() != "{\"items\":[],\"next_cursor\":null}\n" {
			t.Fatalf("empty: %d %s", w.Code, w.Body)
		}
		for _, tt := range []struct {
			err    error
			status int
		}{{deliverypg.ErrHistoryParentNotFound, 404}, {errors.New("private SQL credentials"), 500}} {
			stub.err = tt.err
			w := request(h, "GET", path, "", "Bearer secret")
			if w.Code != tt.status || strings.Contains(w.Body.String(), "private") {
				t.Fatalf("error: %d %s", w.Code, w.Body)
			}
		}
	}
	h := historyHandler(t, &historyStub{err: deliverypg.ErrNotFound})
	if w := request(h, "GET", "/v1/deliveries/missing", "", "Bearer secret"); w.Code != 404 {
		t.Fatal(w.Code)
	}
}
func TestHistoryMappingAndPagination(t *testing.T) {
	at := time.Date(2026, 9, 22, 0, 0, 0, 123456000, time.UTC)
	d, err := delivery.New("d", "e", "ep", at)
	if err != nil {
		t.Fatal(err)
	}
	r, err := delivery.NewRun("r", "d", at, delivery.TriggerManualRedelivery)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := delivery.NewRunTerminal("r", "job", delivery.TerminalFailed, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	a, err := delivery.NewAttemptStart("a", "r", at)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := delivery.NewAttemptOutcome(a, at.Add(time.Second), nil, delivery.ClassificationRetryableFailure)
	if err != nil {
		t.Fatal(err)
	}
	stub := &historyStub{deliveries: []delivery.Delivery{d, d}, runs: []deliverypg.RunHistory{{Run: r}, {Run: r, Terminal: &terminal}}, attempts: []deliverypg.AttemptHistory{{Start: a}, {Start: a, Outcome: &outcome}}}
	h := historyHandler(t, stub)
	for _, tt := range []struct {
		path string
		want string
	}{
		{"/v1/deliveries/d", `{"id":"d","event_id":"e","endpoint_id":"ep","created_at":"2026-09-22T00:00:00.123456Z"}`},
		{"/v1/deliveries/d/runs", `{"items":[{"id":"r","delivery_id":"d","created_at":"2026-09-22T00:00:00.123456Z","trigger":"manual_redelivery","resolution":"unresolved","terminal_observation":null},{"id":"r","delivery_id":"d","created_at":"2026-09-22T00:00:00.123456Z","trigger":"manual_redelivery","resolution":"terminal","terminal_observation":{"mercury_job_id":"job","state":"failed","observed_at":"2026-09-22T00:00:01.123456Z"}}],"next_cursor":null}`},
		{"/v1/runs/r/attempts", `{"items":[{"id":"a","run_id":"r","started_at":"2026-09-22T00:00:00.123456Z","observation":"unknown","outcome":null},{"id":"a","run_id":"r","started_at":"2026-09-22T00:00:00.123456Z","observation":"observed","outcome":{"finished_at":"2026-09-22T00:00:01.123456Z","http_status":null,"classification":"retryable_failure"}}],"next_cursor":null}`},
	} {
		w := request(h, "GET", tt.path, "", "Bearer secret")
		if w.Code != 200 || strings.TrimSpace(w.Body.String()) != tt.want {
			t.Fatalf("mapping %s: %d %s", tt.path, w.Code, w.Body)
		}
	}
	for _, path := range []string{"/v1/events/e/deliveries", "/v1/deliveries/d/runs", "/v1/runs/r/attempts"} {
		w := request(h, "GET", path+"?limit=1", "", "Bearer secret")
		var page struct {
			Items []json.RawMessage `json:"items"`
			Next  string            `json:"next_cursor"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || len(page.Items) != 1 || page.Next == "" {
			t.Fatalf("page: %d %s", w.Code, w.Body)
		}
		w = request(h, "GET", path+"?cursor="+url.QueryEscape(page.Next), "", "Bearer secret")
		if w.Code != 200 || !stub.cursor.At.Equal(at) || stub.ctx.Err() != nil {
			t.Fatalf("cursor/context: %d %+v", w.Code, stub.cursor)
		}
	}
}

func TestHistoryAuthenticationAndContext(t *testing.T) {
	stub := &historyStub{err: errors.New("injected storage error")}
	h := historyHandler(t, stub)
	for _, path := range []string{"/v1/events/e/deliveries", "/v1/deliveries/d", "/v1/deliveries/d/runs", "/v1/runs/r/attempts"} {
		for _, auth := range []string{"", "Bearer wrong"} {
			if w := request(h, "GET", path, "", auth); w.Code != 401 {
				t.Fatal(w.Code)
			}
		}
	}
	if stub.calls != 0 {
		t.Fatal("unauthorized history read reached storage")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, path := range []string{"/v1/events/e/deliveries", "/v1/deliveries/d", "/v1/deliveries/d/runs", "/v1/runs/r/attempts"} {
		r := httptest.NewRequest("GET", path, nil).WithContext(ctx)
		r.Header.Set("Authorization", "Bearer secret")
		h.ServeHTTP(httptest.NewRecorder(), r)
		if stub.ctx != ctx {
			t.Fatal("handler did not propagate request cancellation")
		}
	}
}
