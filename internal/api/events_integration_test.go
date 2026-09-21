package api_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/application/acceptance"
	"github.com/xTwo56/iris/internal/delivery"
)

// Follow the existing fixture convention: the supplied URL only creates/drops a
// uniquely named database. Requests use real acceptance and its real repositories.
// No network server, dispatcher or worker is started by this test.
func eventAPIDatabase(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("IRIS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("IRIS_TEST_DATABASE_URL unset; isolated PostgreSQL checks skipped")
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	var suffix [12]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	name := "iris_event_api_test_" + hex.EncodeToString(suffix[:])
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+quoted); err != nil {
			t.Errorf("cleanup database: %v", err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	paths, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil || len(paths) == 0 {
		t.Fatal("missing migrations")
	}
	for _, path := range paths {
		migration, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

func TestEventHTTPAcceptanceIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := eventAPIDatabase(t, ctx)
	at := time.Date(2026, 9, 19, 0, 0, 0, 123456000, time.UTC)
	for _, id := range []string{"endpoint-a", "endpoint-b"} {
		if _, err := pool.Exec(ctx, `INSERT INTO endpoints (id,url,created_at,fanout) VALUES ($1,'https://example.com/webhook',$2,true)`, id, at); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO subscriptions (id,endpoint_id,event_type,created_at,enabled) VALUES ($1,$1,'type',$2,true)`, id, at); err != nil {
			t.Fatal(err)
		}
	}
	var sequence atomic.Int64
	svc := acceptance.New(pool, acceptance.IDs{
		Delivery: func() (delivery.ID, error) { return delivery.ID(fmt.Sprintf("del-%d", sequence.Add(1))), nil },
		Run:      func() (delivery.RunID, error) { return delivery.RunID(fmt.Sprintf("run-%d", sequence.Add(1))), nil },
	}, func() time.Time { return at })
	h, _, _ := setupWithAcceptor(t, svc)
	assertCounts := func(want [5]int) {
		t.Helper()
		var got [5]int
		for i, table := range []string{"events", "deliveries", "delivery_runs", "outbox", "event_submissions"} {
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&got[i]); err != nil {
				t.Fatal(err)
			}
		}
		if got != want {
			t.Fatalf("counts=%v want=%v", got, want)
		}
	}
	payload := "{ \n\"n\":1.00, \"escaped\":\"\\u0061\" }"
	body := eventEnvelope("event", "type", at.Format(time.RFC3339Nano), payload)
	for _, auth := range []string{"", "Bearer wrong"} {
		if w := request(h, "POST", "/v1/events", body, auth); w.Code != http.StatusUnauthorized {
			t.Fatalf("authentication: %d %s", w.Code, w.Body)
		}
	}
	for _, bad := range []string{"{}", strings.TrimSuffix(body, "}") + `,"unknown":true}`, body + " {}"} {
		if w := eventRequest(ctx, h, bad, "key"); w.Code != http.StatusBadRequest {
			t.Fatalf("validation: %d %s", w.Code, w.Body)
		}
	}
	if w := eventRequest(ctx, h, body); w.Code != http.StatusBadRequest {
		t.Fatalf("missing key: %d", w.Code)
	}
	assertCounts([5]int{})
	first := eventRequest(ctx, h, body, "key")
	if first.Code != http.StatusCreated || first.Body.String() != "{\"event_id\":\"event\",\"delivery_count\":2}\n" {
		t.Fatalf("first: %d %s", first.Code, first.Body)
	}
	assertCounts([5]int{1, 2, 2, 2, 1})
	var stored []byte
	var created time.Time
	if err := pool.QueryRow(ctx, `SELECT payload,created_at FROM events WHERE id='event'`).Scan(&stored, &created); err != nil || string(stored) != payload || !created.Equal(at) {
		t.Fatalf("stored input changed: %v", err)
	}
	var linked int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deliveries d JOIN delivery_runs r ON r.delivery_id=d.id
 JOIN outbox o ON o.run_id=r.id WHERE d.event_id='event' AND r.trigger='initial'
 AND o.submission_key='iris:delivery-run:' || r.id AND o.mercury_job_id IS NULL`).Scan(&linked); err != nil || linked != 2 {
		t.Fatalf("fan-out relationships: %d %v", linked, err)
	}
	// Changed routing must not change a committed replay's count or generate work.
	if _, err := pool.Exec(ctx, `UPDATE endpoints SET fanout=false`); err != nil {
		t.Fatal(err)
	}
	before := sequence.Load()
	for _, replayBody := range []string{body, eventEnvelope("event", "type", "2026-09-19T05:30:00.123456+05:30", payload)} {
		w := eventRequest(ctx, h, replayBody, "key")
		if w.Code != http.StatusOK || w.Body.String() != first.Body.String() {
			t.Fatalf("replay: %d %s", w.Code, w.Body)
		}
	}
	for _, conflict := range []string{
		eventEnvelope("other", "type", at.Format(time.RFC3339Nano), payload),
		eventEnvelope("event", "other", at.Format(time.RFC3339Nano), payload),
		eventEnvelope("event", "type", at.Add(time.Nanosecond).Format(time.RFC3339Nano), payload),
		eventEnvelope("event", "type", at.Format(time.RFC3339Nano), strings.Replace(payload, "1.00", "1.0", 1)),
		eventEnvelope("event", "type", at.Format(time.RFC3339Nano), strings.Replace(payload, " \n", "", 1)),
	} {
		if w := eventRequest(ctx, h, conflict, "key"); w.Code != http.StatusConflict {
			t.Fatalf("conflict: %d %s", w.Code, w.Body)
		}
	}
	if w := eventRequest(ctx, h, body, "another-key"); w.Code != http.StatusConflict {
		t.Fatalf("duplicate event ID: %d %s", w.Code, w.Body)
	}
	assertCounts([5]int{1, 2, 2, 2, 1})
	if sequence.Load() != before {
		t.Fatal("replay or conflict generated work")
	}
	if _, err := pool.Exec(ctx, `UPDATE endpoints SET fanout=true`); err != nil {
		t.Fatal(err)
	}
	// HTTP callers race through the same service; database key acquisition must
	// choose exactly one creator and return the same acceptance to the other.
	codes := make(chan int, 2)
	for range 2 {
		go func() {
			codes <- eventRequest(ctx, h, eventEnvelope("race", "type", at.Format(time.RFC3339Nano), "null"), "race-key").Code
		}()
	}
	a, b := <-codes, <-codes
	if !((a == 201 && b == 200) || (a == 200 && b == 201)) {
		t.Fatalf("concurrent statuses: %d %d", a, b)
	}
	assertCounts([5]int{2, 4, 4, 4, 2})
	if err := pool.QueryRow(ctx, `SELECT payload FROM events WHERE id='race'`).Scan(&stored); err != nil || string(stored) != "null" || !json.Valid(stored) {
		t.Fatalf("null payload lost: %v", err)
	}
}
