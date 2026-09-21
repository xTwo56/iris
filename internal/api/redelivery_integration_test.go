package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/application/redelivery"
	"github.com/xTwo56/iris/internal/delivery"
)

// These tests reuse the isolated API database fixture. All HTTP calls are in
// process; neither Mercury nor a receiver is contacted.
func redeliveryFixture(t *testing.T) (context.Context, *pgxpool.Pool, time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	pool := eventAPIDatabase(t, ctx)
	at := time.Date(2026, 9, 22, 0, 0, 0, 123456000, time.UTC)
	statements := []string{
		`INSERT INTO events VALUES ('event','type',$1,decode('7b20226e223a20312e3030207d','hex'))`,
		`INSERT INTO endpoints VALUES ('endpoint','https://example.com/webhook',$1,false)`,
		`INSERT INTO subscriptions VALUES ('subscription','endpoint','type',$1,false)`,
		`INSERT INTO deliveries VALUES ('delivery','event','endpoint',$1)`,
		`INSERT INTO delivery_runs VALUES ('initial','delivery',$1,'initial')`,
		`INSERT INTO outbox (run_id,submission_key,created_at,mercury_job_id,submitted_at) VALUES ('initial','iris:delivery-run:initial',$1,'initial-job',$1)`,
		`INSERT INTO attempt_starts VALUES ('unknown-attempt','initial',$1)`,
	}
	for _, sql := range statements {
		if _, err := pool.Exec(ctx, sql, at); err != nil {
			t.Fatal(err)
		}
	}
	return ctx, pool, at
}
func terminal(t *testing.T, ctx context.Context, pool *pgxpool.Pool, run, state string, at time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE outbox SET mercury_job_id=COALESCE(mercury_job_id,$2),submitted_at=COALESCE(submitted_at,$3) WHERE run_id=$1`, run, "job-"+run, at); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO run_terminals SELECT run_id,mercury_job_id,$2,$3 FROM outbox WHERE run_id=$1`, run, state, at); err != nil {
		t.Fatal(err)
	}
}
func countRedelivery(t *testing.T, ctx context.Context, pool *pgxpool.Pool, wantRuns, wantRequests int) {
	t.Helper()
	var runs, requests, entries int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM delivery_runs),(SELECT count(*) FROM redelivery_requests),(SELECT count(*) FROM outbox)`).Scan(&runs, &requests, &entries); err != nil {
		t.Fatal(err)
	}
	if runs != wantRuns || entries != wantRuns || requests != wantRequests {
		t.Fatalf("runs/outbox/requests=%d/%d/%d", runs, entries, requests)
	}
}
func TestRedeliveryHistoryAndReplayIntegration(t *testing.T) {
	ctx, pool, at := redeliveryFixture(t)
	var sequence atomic.Int64
	svc := redelivery.New(pool, func() (delivery.RunID, error) { return delivery.RunID(fmt.Sprintf("manual-%d", sequence.Add(1))), nil }, func() time.Time { return at })
	h := redeliveryHandler(t, svc)
	if w := redeliveryRequest(ctx, h, "delivery", "{}", "key"); w.Code != 409 {
		t.Fatalf("unresolved initial: %d", w.Code)
	}
	countRedelivery(t, ctx, pool, 1, 0)
	terminal(t, ctx, pool, "initial", "succeeded", at)
	// Capture old records including the start with unknown outcome. New work must
	// not mutate any original event, destination, obligation or execution history.
	snapshot := func() string {
		var s string
		err := pool.QueryRow(ctx, `SELECT json_build_array(
   (SELECT row_to_json(e) FROM events e WHERE id='event'),
   (SELECT row_to_json(d) FROM deliveries d WHERE id='delivery'),
   (SELECT row_to_json(e) FROM endpoints e WHERE id='endpoint'),
   (SELECT row_to_json(s) FROM subscriptions s WHERE id='subscription'),
   (SELECT row_to_json(r) FROM delivery_runs r WHERE id='initial'),
   (SELECT row_to_json(o) FROM outbox o WHERE run_id='initial'),
   (SELECT row_to_json(t) FROM run_terminals t WHERE run_id='initial'),
   (SELECT row_to_json(a) FROM attempt_starts a WHERE id='unknown-attempt'),
   (SELECT count(*) FROM attempt_outcomes))::text`).Scan(&s)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	before := snapshot()
	first := redeliveryRequest(ctx, h, "delivery", "{}", "key")
	if first.Code != 201 {
		t.Fatalf("first: %d %s", first.Code, first.Body)
	}
	var result struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	replay := redeliveryRequest(ctx, h, "delivery", " {} ", "key")
	if replay.Code != 200 || replay.Body.String() != first.Body.String() || sequence.Load() != 1 {
		t.Fatalf("replay: %d %s", replay.Code, replay.Body)
	}
	if w := redeliveryRequest(ctx, h, "other", "{}", "key"); w.Code != 409 {
		t.Fatalf("conflicting input: %d", w.Code)
	}
	if w := redeliveryRequest(ctx, h, "absent", "{}", "absent-key"); w.Code != 404 {
		t.Fatalf("missing: %d", w.Code)
	}
	if w := redeliveryRequest(ctx, h, "delivery", "{}", "next-key"); w.Code != 409 {
		t.Fatalf("pending outbox must block: %d", w.Code)
	}
	countRedelivery(t, ctx, pool, 2, 1)
	var linked bool
	if err := pool.QueryRow(ctx, `SELECT r.trigger='manual_redelivery' AND r.delivery_id='delivery' AND o.submission_key='iris:delivery-run:'||r.id AND o.mercury_job_id IS NULL FROM delivery_runs r JOIN outbox o ON o.run_id=r.id WHERE r.id=$1`, result.RunID).Scan(&linked); err != nil || !linked {
		t.Fatalf("new relationships: %v %v", linked, err)
	}
	terminal(t, ctx, pool, result.RunID, "failed", at)
	if w := redeliveryRequest(ctx, h, "delivery", "{}", "next-key"); w.Code != 201 {
		t.Fatalf("redelivery after failure: %d %s", w.Code, w.Body)
	}
	if w := redeliveryRequest(ctx, h, "delivery", "{}", "key"); w.Code != 200 || w.Body.String() != first.Body.String() {
		t.Fatalf("older replay: %d %s", w.Code, w.Body)
	}
	countRedelivery(t, ctx, pool, 3, 2)
	if snapshot() != before {
		t.Fatal("original history changed")
	}
}

func TestRedeliveryConcurrentIntegration(t *testing.T) {
	for _, sameKey := range []bool{true, false} {
		t.Run(fmt.Sprint(sameKey), func(t *testing.T) {
			ctx, pool, at := redeliveryFixture(t)
			terminal(t, ctx, pool, "initial", "succeeded", at)
			var sequence atomic.Int64
			svc := redelivery.New(pool, func() (delivery.RunID, error) { return delivery.RunID(fmt.Sprintf("manual-%d", sequence.Add(1))), nil }, func() time.Time { return at })
			h := redeliveryHandler(t, svc)
			start := make(chan struct{})
			var wg sync.WaitGroup
			codes := make([]int, 2)
			bodies := make([]string, 2)
			for i := range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					key := "same"
					if !sameKey {
						key = fmt.Sprint(i)
					}
					w := redeliveryRequest(ctx, h, "delivery", "{}", key)
					codes[i] = w.Code
					bodies[i] = w.Body.String()
				}()
			}
			close(start)
			wg.Wait()
			if sameKey {
				if !((codes[0] == 201 && codes[1] == 200) || (codes[1] == 201 && codes[0] == 200)) || bodies[0] != bodies[1] {
					t.Fatalf("identical: %v %v", codes, bodies)
				}
			} else if !((codes[0] == 201 && codes[1] == 409) || (codes[1] == 201 && codes[0] == 409)) {
				t.Fatalf("different: %v %v", codes, bodies)
			}
			countRedelivery(t, ctx, pool, 2, 1)
		})
	}
}

func TestRedeliveryRollbackIntegration(t *testing.T) {
	ctx, pool, at := redeliveryFixture(t)
	terminal(t, ctx, pool, "initial", "succeeded", at)
	// Force failure after inserting the run: an older outbox already owns this
	// submission key. The failed transaction must discard its run and request key.
	if _, err := pool.Exec(ctx, `UPDATE outbox SET submission_key='iris:delivery-run:collision' WHERE run_id='initial'`); err != nil {
		t.Fatal(err)
	}
	svc := redelivery.New(pool, func() (delivery.RunID, error) { return "collision", nil }, func() time.Time { return at })
	if _, err := svc.Redeliver(ctx, "retry-key", "delivery"); err == nil {
		t.Fatal("expected outbox conflict")
	}
	countRedelivery(t, ctx, pool, 1, 0)
	svc = redelivery.New(pool, func() (delivery.RunID, error) { return "valid", nil }, func() time.Time { return at })
	if result, err := svc.Redeliver(ctx, "retry-key", "delivery"); err != nil || result.Replayed {
		t.Fatalf("retry: %+v %v", result, err)
	}
	countRedelivery(t, ctx, pool, 2, 1)
	for _, input := range []struct {
		key string
		id  delivery.ID
	}{{"", "delivery"}, {"key", " "}} {
		if _, err := svc.Redeliver(ctx, input.key, input.id); !errors.Is(err, redelivery.ErrInvalidInput) {
			t.Fatalf("invalid input: %v", err)
		}
	}
}

// A failed commit must never advertise acceptance. This injected transaction
// rolls back instead, allowing a subsequent invocation to use the same key.
type rejectedCommit struct{ pgx.Tx }

func (tx rejectedCommit) Commit(ctx context.Context) error {
	if err := tx.Tx.Rollback(ctx); err != nil {
		return err
	}
	return errors.New("injected commit failure")
}

type rejectCommitDB struct{ pool *pgxpool.Pool }

func (db rejectCommitDB) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return rejectedCommit{tx}, nil
}
func TestRedeliveryCommitFailureIntegration(t *testing.T) {
	ctx, pool, at := redeliveryFixture(t)
	terminal(t, ctx, pool, "initial", "failed", at)
	svc := redelivery.New(rejectCommitDB{pool}, func() (delivery.RunID, error) { return "new", nil }, func() time.Time { return at })
	result, err := svc.Redeliver(ctx, "key", "delivery")
	if err == nil || result.RunID != "" {
		t.Fatalf("commit: %+v %v", result, err)
	}
	countRedelivery(t, ctx, pool, 1, 0)
	svc = redelivery.New(pool, func() (delivery.RunID, error) { return "new", nil }, func() time.Time { return at })
	if _, err := svc.Redeliver(ctx, "key", "delivery"); err != nil {
		t.Fatal(err)
	}
}

func TestRedeliveryOlderUnresolvedRunIntegration(t *testing.T) {
	ctx, pool, at := redeliveryFixture(t)
	// Model historical work with a terminal newer run. Checking just the latest
	// run would incorrectly ignore the initial run's still-missing observation.
	if _, err := pool.Exec(ctx, `INSERT INTO delivery_runs VALUES ('newer','delivery',$1,'manual_redelivery')`, at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO outbox (run_id,submission_key,created_at) VALUES ('newer','iris:delivery-run:newer',$1)`, at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	terminal(t, ctx, pool, "newer", "succeeded", at.Add(time.Second))
	svc := redelivery.New(pool, func() (delivery.RunID, error) { t.Fatal("generated work despite unresolved run"); return "", nil }, func() time.Time { return at })
	if _, err := svc.Redeliver(ctx, "key", "delivery"); !errors.Is(err, redelivery.ErrUnresolvedRun) {
		t.Fatalf("older unresolved run: %v", err)
	}
	countRedelivery(t, ctx, pool, 2, 0)
}
