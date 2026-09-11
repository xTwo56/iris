package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/event"
	eventpg "github.com/xTwo56/iris/internal/event/postgres"
)

// Each invocation creates its own database and applies the real migration.
// The connection database is used only for CREATE/DROP of that generated name.
func TestRepositoryIntegration(t *testing.T) {
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
	name := "iris_event_test_" + hex.EncodeToString(suffix[:])
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
	repo := eventpg.New(pool)
	at := time.Date(2026, 9, 11, 12, 30, 0, 123456000, time.UTC)
	makeEvent := func(id event.ID, payload string) event.Event {
		t.Helper()
		e, err := event.New(id, "order.created", at, []byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	t.Run("round trips", func(t *testing.T) {
		for _, payload := range []string{" \n{\"z\":1.00, \"a\":\"\\u0061\", \"z\":2}\t", "1e3", "null"} {
			e := makeEvent(event.ID("id-'"+payload), payload)
			if err := repo.Create(ctx, e); err != nil {
				t.Fatal(err)
			}
			got, err := repo.GetByID(ctx, e.ID())
			if err != nil {
				t.Fatal(err)
			}
			if got.ID() != e.ID() || got.Type() != e.Type() || !got.CreatedAt().Equal(at) || string(got.Payload()) != payload {
				t.Fatalf("round trip mismatch: %+v", got)
			}
			copy := got.Payload()
			copy[0] = 'X'
			if string(got.Payload()) != payload {
				t.Fatal("retrieved event payload is mutable")
			}
		}
	})
	t.Run("duplicate preserves original", func(t *testing.T) {
		original := makeEvent("duplicate", `{"original":1.00}`)
		if err := repo.Create(ctx, original); err != nil {
			t.Fatal(err)
		}
		if err := repo.Create(ctx, makeEvent("duplicate", `null`)); !errors.Is(err, eventpg.ErrDuplicateID) {
			t.Fatalf("expected duplicate, got %v", err)
		}
		got, err := repo.GetByID(ctx, original.ID())
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Payload()) != string(original.Payload()) {
			t.Fatal("duplicate overwrote original")
		}
	})
	t.Run("missing", func(t *testing.T) {
		if _, err := repo.GetByID(ctx, "missing"); !errors.Is(err, eventpg.ErrNotFound) {
			t.Fatalf("expected missing, got %v", err)
		}
	})
	t.Run("invalid rejected before query", func(t *testing.T) {
		// A nil query dependency proves validation returns before any database call.
		if err := eventpg.New(nil).Create(ctx, event.Event{}); err == nil {
			t.Fatal("zero event accepted")
		}
		var before, after int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&before); err != nil {
			t.Fatal(err)
		}
		if err := repo.Create(ctx, event.Event{}); err == nil {
			t.Fatal("zero event accepted")
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&after); err != nil {
			t.Fatal(err)
		}
		if before != after {
			t.Fatal("invalid input wrote a row")
		}
	})
	t.Run("caller transaction", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		r := eventpg.New(tx)
		if err := r.Create(ctx, makeEvent("transaction", "{}")); err != nil {
			t.Fatal(err)
		}
		if _, err := r.GetByID(ctx, "transaction"); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.GetByID(ctx, "transaction"); !errors.Is(err, eventpg.ErrNotFound) {
			t.Fatalf("rollback not respected: %v", err)
		}
	})
	t.Run("invalid stored JSON", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "INSERT INTO events VALUES ($1,$2,$3,$4)", "corrupt", "type", at, []byte("invalid")); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.GetByID(ctx, "corrupt"); err == nil {
			t.Fatal("invalid stored event reconstructed")
		}
	})
	t.Run("cancellation retained", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := repo.Create(canceled, makeEvent("canceled", "{}")); !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
		if _, err := repo.GetByID(canceled, "missing"); !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
	})
}

func TestCreateRejectsZeroEventBeforeQuery(t *testing.T) {
	if err := eventpg.New(nil).Create(context.Background(), event.Event{}); err == nil {
		t.Fatal("zero event accepted")
	}
}
