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
	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/endpoint"
)

// Each invocation creates its own database and applies the real migration.
// The connection database is used only for CREATE/DROP of that generated name.
func TestRunRepositoryIntegration(t *testing.T) {
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
	name := "iris_run_test_" + hex.EncodeToString(suffix[:])
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
	repo := deliverypg.NewRunRepository(pool)
	at := time.Date(2026, 9, 11, 12, 30, 0, 123456000, time.UTC)
	if _, err := pool.Exec(ctx, `INSERT INTO events VALUES ('e','type',$1,$2)`, at, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"d", "empty"} {
		if _, err := pool.Exec(ctx, `INSERT INTO endpoints VALUES ($1,'https://example.com',$2,false)`, id, at); err != nil {
			t.Fatal(err)
		}
		d, err := delivery.New(delivery.ID(id), "e", endpoint.ID(id), at)
		if err != nil {
			t.Fatal(err)
		}
		if err := deliverypg.New(pool).Create(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	makeRun := func(id delivery.RunID, d delivery.ID, when time.Time, trigger delivery.Trigger) delivery.Run {
		t.Helper()
		r, err := delivery.NewRun(id, d, when, trigger)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	initial := makeRun("initial-'", "d", at, delivery.TriggerInitial)
	runs := []delivery.Run{makeRun("a-later", "d", at.Add(time.Second), delivery.TriggerManualRedelivery), makeRun("z", "d", at, delivery.TriggerManualRedelivery), initial, makeRun("A", "d", at, delivery.TriggerManualRedelivery)}
	t.Run("round trips and multiple manual runs", func(t *testing.T) {
		for _, r := range runs {
			if err := repo.Create(ctx, r); err != nil {
				t.Fatal(err)
			}
			got, err := repo.GetByID(ctx, r.ID())
			if err != nil {
				t.Fatal(err)
			}
			if got.ID() != r.ID() || got.DeliveryID() != r.DeliveryID() || got.Trigger() != r.Trigger() || !got.CreatedAt().Equal(r.CreatedAt()) {
				t.Fatalf("metadata mismatch: %+v", got)
			}
		}
	})
	t.Run("constraints", func(t *testing.T) {
		for _, tt := range []struct {
			r    delivery.Run
			want error
		}{
			{makeRun(initial.ID(), "empty", at, delivery.TriggerManualRedelivery), deliverypg.ErrDuplicateRunID},
			{makeRun("second", "d", at, delivery.TriggerInitial), deliverypg.ErrDuplicateInitialRun},
			{makeRun("missing", "absent", at, delivery.TriggerInitial), deliverypg.ErrDeliveryNotFound},
		} {
			if err := repo.Create(ctx, tt.r); !errors.Is(err, tt.want) {
				t.Fatalf("want %v, got %v", tt.want, err)
			}
		}
		got, err := repo.GetByID(ctx, initial.ID())
		if err != nil {
			t.Fatal(err)
		}
		if got.DeliveryID() != initial.DeliveryID() || got.Trigger() != initial.Trigger() || !got.CreatedAt().Equal(at) {
			t.Fatal("conflict changed original")
		}
	})
	t.Run("listing ordering and isolation", func(t *testing.T) {
		got, err := repo.ListByDeliveryID(ctx, "d")
		if err != nil {
			t.Fatal(err)
		}
		want := []delivery.RunID{"A", "initial-'", "z", "a-later"}
		if len(got) != len(want) {
			t.Fatalf("got %d runs", len(got))
		}
		for i, r := range got {
			if r.ID() != want[i] || r.DeliveryID() != "d" {
				t.Fatalf("unexpected run: %+v", r)
			}
		}
		for _, id := range []delivery.ID{"empty", "absent"} {
			rs, err := repo.ListByDeliveryID(ctx, id)
			if err != nil || len(rs) != 0 {
				t.Fatalf("expected empty: %+v %v", rs, err)
			}
		}
	})
	t.Run("missing and invalid", func(t *testing.T) {
		if _, err := repo.GetByID(ctx, "absent"); !errors.Is(err, deliverypg.ErrRunNotFound) {
			t.Fatalf("expected missing: %v", err)
		}
		if err := repo.Create(ctx, delivery.Run{}); err == nil {
			t.Fatal("zero run accepted")
		}
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM delivery_runs").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != len(runs) {
			t.Fatalf("unexpected writes: %d", n)
		}
	})
	t.Run("caller rollback", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		r := deliverypg.NewRunRepository(tx)
		if err := r.Create(ctx, makeRun("tx", "empty", at, delivery.TriggerInitial)); err != nil {
			t.Fatal(err)
		}
		if _, err := r.GetByID(ctx, "tx"); err != nil {
			t.Fatal(err)
		}
		rs, err := r.ListByDeliveryID(ctx, "empty")
		if err != nil || len(rs) != 1 {
			t.Fatalf("transaction read: %+v %v", rs, err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.GetByID(ctx, "tx"); !errors.Is(err, deliverypg.ErrRunNotFound) {
			t.Fatal(err)
		}
		rs, err = repo.ListByDeliveryID(ctx, "empty")
		if err != nil || len(rs) != 0 {
			t.Fatalf("rollback failed: %+v %v", rs, err)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := repo.Create(canceled, initial); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if _, err := repo.GetByID(canceled, initial.ID()); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if _, err := repo.ListByDeliveryID(canceled, "d"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}
func TestCreateRejectsZeroRunBeforeQuery(t *testing.T) {
	if err := deliverypg.NewRunRepository(nil).Create(context.Background(), delivery.Run{}); err == nil {
		t.Fatal("zero run accepted")
	}
}
