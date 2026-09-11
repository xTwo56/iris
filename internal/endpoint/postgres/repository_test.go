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
	"github.com/xTwo56/iris/internal/endpoint"
	endpointpg "github.com/xTwo56/iris/internal/endpoint/postgres"
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
	name := "iris_endpoint_test_" + hex.EncodeToString(suffix[:])
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
	repo := endpointpg.New(pool)
	at := time.Date(2026, 9, 11, 12, 30, 0, 123456000, time.UTC)
	const url = "HTTPS://Example.COM/hooks/%2f?q='value'"
	makeEndpoint := func(id endpoint.ID, enabled bool) endpoint.Endpoint {
		t.Helper()
		e, err := endpoint.New(id, url, at)
		if err != nil {
			t.Fatal(err)
		}
		if !enabled {
			e.FanoutDisable()
		}
		return e
	}
	check := func(t *testing.T, id endpoint.ID, enabled bool) {
		t.Helper()
		got, err := repo.GetByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID() != id || got.URL() != url || !got.CreatedAt().Equal(at) || got.Fanout() != enabled {
			t.Fatalf("round trip mismatch: %+v", got)
		}
	}
	t.Run("round trips and shared URL", func(t *testing.T) {
		for _, tt := range []struct {
			id      endpoint.ID
			enabled bool
		}{{"enabled-'", true}, {"disabled", false}} {
			if err := repo.Create(ctx, makeEndpoint(tt.id, tt.enabled)); err != nil {
				t.Fatal(err)
			}
			check(t, tt.id, tt.enabled)
		}
	})
	t.Run("duplicate preserves all fields", func(t *testing.T) {
		if err := repo.Create(ctx, makeEndpoint("duplicate", false)); err != nil {
			t.Fatal(err)
		}
		replacement, err := endpoint.New("duplicate", "https://other.example", at.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Create(ctx, replacement); !errors.Is(err, endpointpg.ErrDuplicateID) {
			t.Fatalf("expected duplicate: %v", err)
		}
		check(t, "duplicate", false)
	})
	t.Run("missing", func(t *testing.T) {
		if _, err := repo.GetByID(ctx, "missing"); !errors.Is(err, endpointpg.ErrNotFound) {
			t.Fatalf("expected not found: %v", err)
		}
		for _, enabled := range []bool{true, false} {
			if err := repo.SetFanout(ctx, "missing", enabled); !errors.Is(err, endpointpg.ErrNotFound) {
				t.Fatalf("expected not found: %v", err)
			}
		}
	})
	t.Run("invalid input", func(t *testing.T) {
		var before, after int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM endpoints").Scan(&before); err != nil {
			t.Fatal(err)
		}
		if err := repo.Create(ctx, endpoint.Endpoint{}); err == nil {
			t.Fatal("zero endpoint accepted")
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM endpoints").Scan(&after); err != nil {
			t.Fatal(err)
		}
		if before != after {
			t.Fatal("invalid input inserted")
		}
	})
	t.Run("repeated updates preserve metadata", func(t *testing.T) {
		if err := repo.Create(ctx, makeEndpoint("flags", true)); err != nil {
			t.Fatal(err)
		}
		for _, enabled := range []bool{true, true, false, false, true, true} {
			if err := repo.SetFanout(ctx, "flags", enabled); err != nil {
				t.Fatal(err)
			}
			check(t, "flags", enabled)
		}
	})
	t.Run("caller rollback undoes inserts and updates", func(t *testing.T) {
		if err := repo.Create(ctx, makeEndpoint("existing", true)); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		r := endpointpg.New(tx)
		if err := r.Create(ctx, makeEndpoint("transaction", false)); err != nil {
			t.Fatal(err)
		}
		if err := r.SetFanout(ctx, "existing", false); err != nil {
			t.Fatal(err)
		}
		got, err := r.GetByID(ctx, "existing")
		if err != nil || got.Fanout() {
			t.Fatalf("transaction update not visible: %v", err)
		}
		if _, err := r.GetByID(ctx, "transaction"); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.GetByID(ctx, "transaction"); !errors.Is(err, endpointpg.ErrNotFound) {
			t.Fatalf("insert survived rollback: %v", err)
		}
		check(t, "existing", true)
	})
	t.Run("invalid stored URL", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "INSERT INTO endpoints VALUES ($1,$2,$3,$4)", "corrupt", "http://example.com", at, true); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.GetByID(ctx, "corrupt"); err == nil {
			t.Fatal("invalid stored endpoint accepted")
		}
	})
	t.Run("cancellation retained", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := repo.Create(canceled, makeEndpoint("canceled", true)); !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
		if _, err := repo.GetByID(canceled, "missing"); !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
		if err := repo.SetFanout(canceled, "missing", false); !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
	})
}

func TestCreateRejectsZeroEndpointBeforeQuery(t *testing.T) {
	if err := endpointpg.New(nil).Create(context.Background(), endpoint.Endpoint{}); err == nil {
		t.Fatal("zero endpoint accepted")
	}
}
