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
	"github.com/xTwo56/iris/internal/event"
	"github.com/xTwo56/iris/internal/subscription"
	subpg "github.com/xTwo56/iris/internal/subscription/postgres"
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
	name := "iris_subscription_test_" + hex.EncodeToString(suffix[:])
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
	repo := subpg.New(pool)
	at := time.Date(2026, 9, 11, 12, 30, 0, 123456000, time.UTC)
	epRepo := endpointpg.New(pool)
	for _, id := range []endpoint.ID{"ep", "off", "other"} {
		e, err := endpoint.New(id, "https://example.com", at)
		if err != nil {
			t.Fatal(err)
		}
		if id == "off" {
			e.FanoutDisable()
		}
		if err := epRepo.Create(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	makeSub := func(id subscription.ID, ep endpoint.ID, typ event.Type, enabled bool) subscription.Subscription {
		t.Helper()
		s, err := subscription.New(id, ep, typ, at)
		if err != nil {
			t.Fatal(err)
		}
		if !enabled {
			s.Disable()
		}
		return s
	}
	check := func(t *testing.T, want subscription.Subscription) {
		t.Helper()
		got, err := repo.GetByID(ctx, want.ID())
		if err != nil {
			t.Fatal(err)
		}
		if got.ID() != want.ID() || got.EndpointID() != want.EndpointID() || got.EventType() != want.EventType() || !got.CreatedAt().Equal(want.CreatedAt()) || got.Enabled() != want.Enabled() {
			t.Fatalf("metadata mismatch: %+v", got)
		}
	}
	t.Run("round trips", func(t *testing.T) {
		for _, enabled := range []bool{true, false} {
			id := subscription.ID("enabled")
			if !enabled {
				id = "disabled"
			}
			s := makeSub(id, "ep", event.Type(" type-'"+id+" "), enabled)
			if err := repo.Create(ctx, s); err != nil {
				t.Fatal(err)
			}
			check(t, s)
		}
	})
	t.Run("constraints", func(t *testing.T) {
		original := makeSub("original", "ep", "unique", false)
		if err := repo.Create(ctx, original); err != nil {
			t.Fatal(err)
		}
		for _, tt := range []struct {
			s    subscription.Subscription
			want error
		}{
			{makeSub("original", "other", "different", true), subpg.ErrDuplicateID},
			{makeSub("new-id", "ep", "unique", true), subpg.ErrDuplicateEndpointType},
			{makeSub("missing-ep", "absent", "type", true), subpg.ErrEndpointNotFound},
		} {
			if err := repo.Create(ctx, tt.s); !errors.Is(err, tt.want) {
				t.Fatalf("want %v, got %v", tt.want, err)
			}
		}
		check(t, original)
	})
	t.Run("missing invalid and repeated updates", func(t *testing.T) {
		if _, err := repo.GetByID(ctx, "missing"); !errors.Is(err, subpg.ErrNotFound) {
			t.Fatal(err)
		}
		for _, enabled := range []bool{true, false} {
			if err := repo.SetEnabled(ctx, "missing", enabled); !errors.Is(err, subpg.ErrNotFound) {
				t.Fatal(err)
			}
		}
		if err := repo.Create(ctx, subscription.Subscription{}); err == nil {
			t.Fatal("zero subscription accepted")
		}
		s := makeSub("flags", "ep", "flags", true)
		if err := repo.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
		for _, enabled := range []bool{true, true, false, false, true} {
			if err := repo.SetEnabled(ctx, s.ID(), enabled); err != nil {
				t.Fatal(err)
			}
			if enabled {
				s.Enable()
			} else {
				s.Disable()
			}
			check(t, s)
		}
	})
	t.Run("eligibility exact type flags and ordering", func(t *testing.T) {
		for _, s := range []subscription.Subscription{
			makeSub("z", "ep", "order.created", true), makeSub("a", "other", "order.created", true),
			makeSub("off", "off", "order.created", true), makeSub("disabled-rule", "ep", "Order.Created", false),
			makeSub("case", "other", "Order.Created", true), makeSub("space", "ep", " order.created ", true),
			makeSub("wildcard", "ep", "order.*", true), makeSub("both-off", "off", "Order.Created", false),
		} {
			if err := repo.Create(ctx, s); err != nil {
				t.Fatal(err)
			}
		}
		for _, tt := range []struct {
			typ event.Type
			ids []subscription.ID
		}{
			{"order.created", []subscription.ID{"a", "z"}}, {"Order.Created", []subscription.ID{"case"}},
			{" order.created ", []subscription.ID{"space"}}, {"order.*", []subscription.ID{"wildcard"}},
			{"order", nil}, {"none", nil},
		} {
			got, err := repo.ListEligible(ctx, tt.typ)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.ids) {
				t.Fatalf("%q: got %+v, want %v", tt.typ, got, tt.ids)
			}
			for i, s := range got {
				if s.ID() != tt.ids[i] || !s.Enabled() || s.EventType() != tt.typ {
					t.Fatalf("unexpected eligible rule: %+v", s)
				}
				check(t, s)
			}
		}
		if err := repo.SetEnabled(ctx, "z", false); err != nil {
			t.Fatal(err)
		}
		if err := epRepo.SetFanout(ctx, "other", false); err != nil {
			t.Fatal(err)
		}
		got, err := repo.ListEligible(ctx, "order.created")
		if err != nil || len(got) != 0 {
			t.Fatalf("flags not observed: %+v %v", got, err)
		}
	})
	t.Run("caller rollback", func(t *testing.T) {
		s := makeSub("rollback-existing", "ep", "rollback", true)
		if err := repo.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		r := subpg.New(tx)
		if err := r.SetEnabled(ctx, s.ID(), false); err != nil {
			t.Fatal(err)
		}
		if err := r.Create(ctx, makeSub("rollback-new", "ep", "tx-new", true)); err != nil {
			t.Fatal(err)
		}
		got, err := r.ListEligible(ctx, "tx-new")
		if err != nil || len(got) != 1 {
			t.Fatalf("transaction write invisible: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		check(t, s)
		if _, err := repo.GetByID(ctx, "rollback-new"); !errors.Is(err, subpg.ErrNotFound) {
			t.Fatal(err)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := repo.ListEligible(canceled, "type"); !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
	})
}

func TestCreateRejectsZeroBeforeQuery(t *testing.T) {
	if err := subpg.New(nil).Create(context.Background(), subscription.Subscription{}); err == nil {
		t.Fatal("zero subscription accepted")
	}
}
