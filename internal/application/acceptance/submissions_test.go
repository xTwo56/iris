package acceptance_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/application/acceptance"
	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/event"
)

// Each invocation creates its own database and applies the real migration.
// The connection database is used only for CREATE/DROP of that generated name.
func TestSubmissionIntegration(t *testing.T) {
	dsn := os.Getenv("IRIS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("IRIS_TEST_DATABASE_URL unset; PostgreSQL integration checks skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	var suffix [12]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	name := "iris_submission_test_" + hex.EncodeToString(suffix[:])
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quoted); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+quoted); err != nil {
			t.Errorf("cleanup database %s: %v", name, err)
		}
	}()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migration, err := os.ReadFile("../../../migrations/000001_event_routing.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	migration, err = os.ReadFile("../../../migrations/000002_delivery_history.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	migration, err = os.ReadFile("../../../migrations/000003_outbox.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	migration, err = os.ReadFile("../../../migrations/000004_event_submissions.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 18, 0, 0, 0, 123456789, time.UTC)
	if _, err := pool.Exec(ctx, `INSERT INTO endpoints VALUES ('ep','https://example.com',$1,true)`, at); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO subscriptions VALUES ('s','ep','type',$1,true)`, at); err != nil {
		t.Fatal(err)
	}
	var sequence atomic.Int64
	ids := acceptance.IDs{Delivery: func() (delivery.ID, error) { return delivery.ID(fmt.Sprint("d", sequence.Add(1))), nil }, Run: func() (delivery.RunID, error) { return delivery.RunID(fmt.Sprint("r", sequence.Add(1))), nil }}
	svc := acceptance.New(pool, ids, func() time.Time { return at })
	makeEvent := func(id event.ID, typ event.Type, when time.Time, payload string) event.Event {
		t.Helper()
		e, err := event.New(id, typ, when, []byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	counts := func() string {
		t.Helper()
		var ns []int
		for _, table := range []string{"events", "deliveries", "delivery_runs", "outbox", "event_submissions"} {
			var n int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
				t.Fatal(err)
			}
			ns = append(ns, n)
		}
		return fmt.Sprint(ns)
	}
	t.Run("replay conflicts and routing changes", func(t *testing.T) {
		e := makeEvent("first", "type", at, " {\"n\":1.00} ")
		result, err := svc.Accept(ctx, "key", e)
		if err != nil || result.DeliveryCount != 1 || result.Replayed {
			t.Fatalf("%+v %v", result, err)
		}
		before := counts()
		generated := sequence.Load()
		if _, err := pool.Exec(ctx, `UPDATE endpoints SET fanout=false`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE subscriptions SET enabled=false`); err != nil {
			t.Fatal(err)
		}
		replay, err := svc.Accept(ctx, "key", e)
		if err != nil || replay != replayResult(result) {
			t.Fatalf("%+v %v", replay, err)
		}
		// Same instant in another timezone remains equivalent.
		zoned := makeEvent(e.ID(), e.Type(), at.In(time.FixedZone("other", 19800)), string(e.Payload()))
		if got, err := svc.Accept(ctx, "key", zoned); err != nil || got != replayResult(result) {
			t.Fatalf("zone equivalence: %+v %v", got, err)
		}
		for _, different := range []event.Event{
			makeEvent("other", e.Type(), at, string(e.Payload())), makeEvent(e.ID(), "other", at, string(e.Payload())),
			makeEvent(e.ID(), e.Type(), at.Add(time.Nanosecond), string(e.Payload())),
			makeEvent(e.ID(), e.Type(), at, `{"n":1.00}`), makeEvent(e.ID(), e.Type(), at, " {\"n\":1.0} "),
		} {
			if got, err := svc.Accept(ctx, "key", different); !errors.Is(err, acceptance.ErrSubmissionConflict) || got.EventID != "" {
				t.Fatalf("expected conflict: %+v %v", got, err)
			}
		}
		if counts() != before || sequence.Load() != generated {
			t.Fatal("replay/conflict changed storage or generated work")
		}
		if _, err := svc.Accept(ctx, "different-key", e); !errors.Is(err, acceptance.ErrDuplicateEvent) {
			t.Fatal(err)
		}
		if counts() != before {
			t.Fatal("duplicate consumed new key")
		}
		if _, err := pool.Exec(ctx, `UPDATE endpoints SET fanout=true`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE subscriptions SET enabled=true`); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("zero delivery replay and blank key", func(t *testing.T) {
		e := makeEvent("zero", "none", at, `null`)
		a, err := svc.Accept(ctx, "zero-key", e)
		if err != nil || a.DeliveryCount != 0 {
			t.Fatal(err)
		}
		b, err := svc.Accept(ctx, "zero-key", e)
		if err != nil || replayResult(a) != b {
			t.Fatalf("%+v %v", b, err)
		}
		if _, err := svc.Accept(ctx, " \t", e); err == nil {
			t.Fatal("blank key accepted")
		}
	})
	t.Run("rollback releases key", func(t *testing.T) {
		before := counts()
		calls := 0
		failing := acceptance.New(pool, acceptance.IDs{Delivery: ids.Delivery, Run: func() (delivery.RunID, error) { calls++; return "", nil }}, func() time.Time { return at })
		e := makeEvent("retry", "type", at, `{}`)
		if _, err := failing.Accept(ctx, "retry-key", e); err == nil || calls != 1 {
			t.Fatal("expected failure")
		}
		if counts() != before {
			t.Fatal("rollback consumed key or left data")
		}
		if result, err := svc.Accept(ctx, "retry-key", e); err != nil || result.DeliveryCount != 1 {
			t.Fatalf("retry failed: %+v %v", result, err)
		}
	})
	t.Run("concurrent identical and conflicting", func(t *testing.T) {
		for _, conflict := range []bool{false, true} {
			key := fmt.Sprint("race-", conflict)
			e := makeEvent(event.ID(key), "type", at, `{}`)
			other := e
			if conflict {
				other = makeEvent(event.ID(key+"-other"), "type", at, `{}`)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			owner := acceptance.New(pool, acceptance.IDs{Delivery: func() (delivery.ID, error) { close(entered); <-release; return ids.Delivery() }, Run: ids.Run}, func() time.Time { return at })
			type response struct {
				result acceptance.Result
				err    error
			}
			first, second := make(chan response, 1), make(chan response, 1)
			go func() { r, err := owner.Accept(ctx, key, e); first <- response{r, err} }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			started := make(chan struct{})
			go func() { close(started); r, err := svc.Accept(ctx, key, other); second <- response{r, err} }()
			<-started
			close(release)
			a, b := <-first, <-second
			if a.err != nil {
				t.Fatal(a.err)
			}
			if conflict {
				if !errors.Is(b.err, acceptance.ErrSubmissionConflict) {
					t.Fatalf("conflict: %v", b.err)
				}
			} else if b.err != nil || replayResult(a.result) != b.result {
				t.Fatalf("replay: %+v %v", b.result, b.err)
			}
			var n int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE event_id=$1`, string(e.ID())).Scan(&n); err != nil || n != 1 {
				t.Fatalf("duplicate work: %d %v", n, err)
			}
		}
	})
	t.Run("migration rollback and reapply", func(t *testing.T) {
		down, err := os.ReadFile("../../../migrations/000004_event_submissions.down.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(down)); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			t.Fatal(err)
		}
	})
}

// Replay metadata describes the call; the original acceptance fields stay equal.
func replayResult(r acceptance.Result) acceptance.Result {
	r.Replayed = true
	return r
}
