package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
)

func TestHistoryIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := eventAPIDatabase(t, ctx)
	at := time.Date(2026, 9, 22, 0, 0, 0, 123456000, time.UTC)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO events VALUES ('event','type',$1,decode('6e756c6c','hex')),('empty','type',$1,decode('6e756c6c','hex'))`, at)
	for i, id := range []string{"A", "a", "b"} {
		timestamp := at
		if i == 2 {
			timestamp = at.Add(time.Second)
		}
		exec(`INSERT INTO endpoints VALUES ($1,'https://example.com/private-endpoint-config',$2,false)`, id, timestamp)
		exec(`INSERT INTO deliveries VALUES ($1,'event',$1,$2)`, id, timestamp)
		trigger := "manual_redelivery"
		if i == 0 {
			trigger = "initial"
		}
		exec(`INSERT INTO delivery_runs VALUES ($1,'A',$2,$3)`, id, timestamp, trigger)
		exec(`INSERT INTO attempt_starts VALUES ($1,'A',$2)`, id, timestamp)
	}
	exec(`INSERT INTO attempt_outcomes VALUES ('a',$1,503,'retryable_failure'),('b',$1,NULL,'permanent_failure')`, at.Add(time.Second))
	exec(`INSERT INTO outbox VALUES ('A','sensitive-internal-submission-key',$1,'job-A',$1),('b','another-internal-key',$1,'job-b',$1)`, at)
	exec(`INSERT INTO run_terminals VALUES ('A','job-A','succeeded',$1),('b','job-b','failed',$1)`, at.Add(time.Second))
	// Verify the storage boundary itself returns only the requested row plus
	// lookahead, rather than relying on HTTP to trim an unbounded repository list.
	repo := deliverypg.NewHistoryRepository(pool)
	if rows, err := repo.DeliveryPage(ctx, "event", 1, deliverypg.HistoryCursor{}); err != nil || len(rows) != 2 {
		t.Fatalf("bounded deliveries: %d %v", len(rows), err)
	}
	if rows, err := repo.RunPage(ctx, "A", 1, deliverypg.HistoryCursor{}); err != nil || len(rows) != 2 {
		t.Fatalf("bounded runs: %d %v", len(rows), err)
	}
	if rows, err := repo.AttemptPage(ctx, "A", 1, deliverypg.HistoryCursor{}); err != nil || len(rows) != 2 {
		t.Fatalf("bounded attempts: %d %v", len(rows), err)
	}
	h := historyHandler(t, deliverypg.NewHistoryRepository(pool))
	type page struct {
		Items []map[string]any `json:"items"`
		Next  *string          `json:"next_cursor"`
	}
	get := func(path string) page {
		t.Helper()
		w := request(h, "GET", path, "", "Bearer secret")
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body)
		}
		for _, forbidden := range []string{"sensitive-internal", "another-internal", "private-endpoint-config", "submission_key", "ciphertext", "nonce", "lease_token", "bearer", "signing_secret", "payload"} {
			if strings.Contains(w.Body.String(), forbidden) {
				t.Fatalf("sensitive field leaked: %s", forbidden)
			}
		}
		var result page
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	for _, path := range []string{"/v1/events/missing/deliveries", "/v1/deliveries/missing", "/v1/deliveries/missing/runs", "/v1/runs/missing/attempts"} {
		if w := request(h, "GET", path, "", "Bearer secret"); w.Code != 404 {
			t.Fatalf("missing parent: %s %d", path, w.Code)
		}
	}
	for _, path := range []string{"/v1/events/empty/deliveries", "/v1/deliveries/a/runs", "/v1/runs/a/attempts"} {
		p := get(path)
		if len(p.Items) != 0 || p.Next != nil {
			t.Fatalf("empty: %+v", p)
		}
	}
	runs := get("/v1/deliveries/A/runs")
	if runs.Items[0]["resolution"] != "terminal" || runs.Items[0]["terminal_observation"].(map[string]any)["state"] != "succeeded" || runs.Items[1]["resolution"] != "unresolved" || runs.Items[1]["terminal_observation"] != nil || runs.Items[2]["terminal_observation"].(map[string]any)["state"] != "failed" {
		t.Fatalf("run observations: %+v", runs.Items)
	}
	attempts := get("/v1/runs/A/attempts")
	if attempts.Items[0]["observation"] != "unknown" || attempts.Items[0]["outcome"] != nil {
		t.Fatal("terminal run fabricated an attempt outcome")
	}
	if attempts.Items[1]["outcome"].(map[string]any)["http_status"] != float64(503) || attempts.Items[2]["outcome"].(map[string]any)["http_status"] != nil {
		t.Fatal("nullable status changed")
	}
	w := request(h, "GET", "/v1/deliveries/A", "", "Bearer secret")
	var single map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &single); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(single) != 4 || single["id"] != "A" || single["event_id"] != "event" || single["endpoint_id"] != "A" {
		t.Fatalf("single delivery: %s", w.Body)
	}
	// Equal timestamps must use C ordering, independently for every collection.
	// Add a later item between pages to exercise a growing, non-snapshot history.
	for _, path := range []string{"/v1/events/event/deliveries", "/v1/deliveries/A/runs", "/v1/runs/A/attempts"} {
		first := get(path + "?limit=1")
		if len(first.Items) != 1 || first.Items[0]["id"] != "A" || first.Next == nil {
			t.Fatalf("first page: %+v", first)
		}
		switch path {
		case "/v1/events/event/deliveries":
			exec(`INSERT INTO endpoints VALUES ('z','https://example.com',$1,true)`, at)
			exec(`INSERT INTO deliveries VALUES ('z','event','z',$1)`, at.Add(2*time.Second))
		case "/v1/deliveries/A/runs":
			exec(`INSERT INTO delivery_runs VALUES ('z','A',$1,'manual_redelivery')`, at.Add(2*time.Second))
		case "/v1/runs/A/attempts":
			exec(`INSERT INTO attempt_starts VALUES ('z','A',$1)`, at.Add(2*time.Second))
			exec(`INSERT INTO attempt_outcomes VALUES ('z',$1,204,'succeeded')`, at.Add(2*time.Second))
		}
		ids := []string{"A"}
		next := first.Next
		for pages := 0; next != nil; pages++ {
			if pages > 5 {
				t.Fatal("pagination did not end")
			}
			p := get(path + "?limit=1&cursor=" + url.QueryEscape(*next))
			if len(p.Items) > 1 {
				t.Fatal("limit ignored")
			}
			for _, item := range p.Items {
				ids = append(ids, item["id"].(string))
			}
			next = p.Next
		}
		if !reflect.DeepEqual(ids, []string{"A", "a", "b", "z"}) {
			t.Fatalf("pagination: %v", ids)
		}
		// The next cursor contains only a position, not a new parent existence rule.
		otherPath := strings.Replace(path, "event/deliveries", "empty/deliveries", 1)
		if otherPath != path {
			if w := request(h, "GET", otherPath+"?cursor="+url.QueryEscape(*first.Next), "", "Bearer secret"); w.Code != 400 {
				t.Fatal("cross-parent cursor accepted")
			}
		}
	}
	finalAttempts := get("/v1/runs/A/attempts")
	lastOutcome := finalAttempts.Items[len(finalAttempts.Items)-1]["outcome"].(map[string]any)
	if lastOutcome["classification"] != "succeeded" || lastOutcome["http_status"] != float64(204) {
		t.Fatalf("successful outcome: %+v", lastOutcome)
	}
	// The join is read-only even when a terminal run has an unknown attempt.
	var unknown int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM attempt_starts s LEFT JOIN attempt_outcomes o ON o.attempt_id=s.id WHERE s.id='A' AND o.attempt_id IS NULL`).Scan(&unknown); err != nil || unknown != 1 {
		t.Fatalf("unknown result mutated: %d %v", unknown, err)
	}
	// Borrowed transactions see their own uncommitted parent and remain caller-owned.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `INSERT INTO events VALUES ('tx-event','type',$1,decode('6e756c6c','hex'))`, at); err != nil {
		t.Fatal(err)
	}
	if values, err := deliverypg.NewHistoryRepository(tx).DeliveryPage(ctx, "tx-event", 1, deliverypg.HistoryCursor{}); err != nil || len(values) != 0 {
		t.Fatalf("transaction read: %v %v", values, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := deliverypg.NewHistoryRepository(pool).DeliveryPage(ctx, "tx-event", 1, deliverypg.HistoryCursor{}); !errors.Is(err, deliverypg.ErrHistoryParentNotFound) {
		t.Fatalf("caller rollback: %v", err)
	}
}
