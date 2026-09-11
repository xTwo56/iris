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
	endpointpg "github.com/xTwo56/iris/internal/endpoint/postgres"
	"github.com/xTwo56/iris/internal/event"
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
	name := "iris_delivery_test_" + hex.EncodeToString(suffix[:])
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
	repo := deliverypg.New(pool)
	at := time.Date(2026, 9, 11, 12, 30, 0, 123456000, time.UTC)
	for _, id := range []string{"event-'", "other-event"} {
		if _, err := pool.Exec(ctx, `INSERT INTO events VALUES ($1,$2,$3,$4)`, id, "type", at, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	epRepo := endpointpg.New(pool)
	for _, id := range []endpoint.ID{"endpoint-'", "other-endpoint"} {
		e, err := endpoint.New(id, "https://example.com", at)
		if err != nil {
			t.Fatal(err)
		}
		e.FanoutDisable()
		if err := epRepo.Create(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	makeDelivery := func(id delivery.ID, ev event.ID, ep endpoint.ID) delivery.Delivery {
		t.Helper()
		d, err := delivery.New(id, ev, ep, at)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	check := func(t *testing.T, want delivery.Delivery) {
		t.Helper()
		for _, lookup := range []func() (delivery.Delivery, error){
			func() (delivery.Delivery, error) { return repo.GetByID(ctx, want.ID()) },
			func() (delivery.Delivery, error) {
				return repo.GetByEventEndpoint(ctx, want.EventID(), want.EndpointID())
			},
		} {
			got, err := lookup()
			if err != nil {
				t.Fatal(err)
			}
			if got.ID() != want.ID() || got.EventID() != want.EventID() || got.EndpointID() != want.EndpointID() || !got.CreatedAt().Equal(want.CreatedAt()) {
				t.Fatalf("round trip mismatch: %+v", got)
			}
		}
	}
	original := makeDelivery(" delivery-' ", "event-'", "endpoint-'")
	t.Run("round trip without routing eligibility", func(t *testing.T) {
		// No subscription exists and endpoint fanout is already false.
		if err := repo.Create(ctx, original); err != nil {
			t.Fatal(err)
		}
		check(t, original)
		if err := epRepo.SetFanout(ctx, original.EndpointID(), true); err != nil {
			t.Fatal(err)
		}
		check(t, original)
		if err := epRepo.SetFanout(ctx, original.EndpointID(), false); err != nil {
			t.Fatal(err)
		}
		check(t, original)
	})
	t.Run("constraint errors preserve original", func(t *testing.T) {
		for _, tt := range []struct {
			name string
			d    delivery.Delivery
			want error
		}{
			{"ID", makeDelivery(original.ID(), "other-event", "other-endpoint"), deliverypg.ErrDuplicateID},
			{"pair", makeDelivery("new-id", original.EventID(), original.EndpointID()), deliverypg.ErrDuplicateEventEndpoint},
			{"missing event", makeDelivery("missing-event", "absent", "other-endpoint"), deliverypg.ErrEventNotFound},
			{"missing endpoint", makeDelivery("missing-endpoint", "other-event", "absent"), deliverypg.ErrEndpointNotFound},
		} {
			t.Run(tt.name, func(t *testing.T) {
				if err := repo.Create(ctx, tt.d); !errors.Is(err, tt.want) {
					t.Fatalf("want %v, got %v", tt.want, err)
				}
			})
		}
		check(t, original)
	})
	t.Run("missing lookups", func(t *testing.T) {
		if _, err := repo.GetByID(ctx, "missing"); !errors.Is(err, deliverypg.ErrNotFound) {
			t.Fatalf("expected missing: %v", err)
		}
		for _, pair := range []struct {
			ev event.ID
			ep endpoint.ID
		}{{"other-event", "other-endpoint"}, {"absent", "endpoint-'"}, {"event-'", "absent"}} {
			if _, err := repo.GetByEventEndpoint(ctx, pair.ev, pair.ep); !errors.Is(err, deliverypg.ErrNotFound) {
				t.Fatalf("expected missing: %v", err)
			}
		}
	})
	t.Run("invalid input", func(t *testing.T) {
		var before, after int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM deliveries").Scan(&before); err != nil {
			t.Fatal(err)
		}
		if err := repo.Create(ctx, delivery.Delivery{}); err == nil {
			t.Fatal("zero delivery accepted")
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM deliveries").Scan(&after); err != nil {
			t.Fatal(err)
		}
		if before != after {
			t.Fatal("invalid insertion changed row count")
		}
	})
	t.Run("caller rollback", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		r := deliverypg.New(tx)
		d := makeDelivery("transaction", "other-event", "other-endpoint")
		if err := r.Create(ctx, d); err != nil {
			t.Fatal(err)
		}
		if _, err := r.GetByID(ctx, d.ID()); err != nil {
			t.Fatal(err)
		}
		if _, err := r.GetByEventEndpoint(ctx, d.EventID(), d.EndpointID()); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.GetByID(ctx, d.ID()); !errors.Is(err, deliverypg.ErrNotFound) {
			t.Fatalf("insert survived: %v", err)
		}
		if _, err := repo.GetByEventEndpoint(ctx, d.EventID(), d.EndpointID()); !errors.Is(err, deliverypg.ErrNotFound) {
			t.Fatalf("pair survived: %v", err)
		}
	})
	t.Run("cancellation retained", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := repo.Create(canceled, original); !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
		if _, err := repo.GetByID(canceled, original.ID()); !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
		if _, err := repo.GetByEventEndpoint(canceled, original.EventID(), original.EndpointID()); !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
	})
}

func TestCreateRejectsZeroBeforeQuery(t *testing.T) {
	if err := deliverypg.New(nil).Create(context.Background(), delivery.Delivery{}); err == nil {
		t.Fatal("zero delivery accepted")
	}
}
