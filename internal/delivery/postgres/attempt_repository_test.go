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
)

// Each invocation creates its own database and applies the real migration.
// The connection database is used only for CREATE/DROP of that generated name.
func TestAttemptRepositoryIntegration(t *testing.T) {
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
	name := "iris_attempt_test_" + hex.EncodeToString(suffix[:])
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
	repo := deliverypg.NewAttemptRepository(pool)
	at := time.Date(2026, 9, 14, 12, 30, 0, 123456000, time.UTC)
	for _, sql := range []string{`INSERT INTO events VALUES ('e','type',$1,'{}')`, `INSERT INTO endpoints VALUES ('ep','https://example.com',$1,false)`, `INSERT INTO deliveries VALUES ('d','e','ep',$1)`, `INSERT INTO delivery_runs VALUES ('r','d',$1,'initial'),('empty','d',$1,'manual_redelivery')`} {
		if _, err := pool.Exec(ctx, sql, at); err != nil {
			t.Fatal(err)
		}
	}
	start := func(id delivery.AttemptID, run delivery.RunID, when time.Time) delivery.AttemptStart {
		t.Helper()
		s, err := delivery.NewAttemptStart(id, run, when)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	outcome := func(s delivery.AttemptStart, when time.Time, status *int, c delivery.Classification) delivery.AttemptOutcome {
		t.Helper()
		o, err := delivery.NewAttemptOutcome(s, when, status, c)
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	ptr := func(n int) *int { return &n }
	t.Run("records and optional status", func(t *testing.T) {
		for _, tt := range []struct {
			id     delivery.AttemptID
			c      delivery.Classification
			status *int
		}{
			{"A", delivery.ClassificationSucceeded, ptr(204)}, {"b", delivery.ClassificationRetryableFailure, nil}, {"c", delivery.ClassificationPermanentFailure, nil}, {"d", delivery.ClassificationRetryableFailure, ptr(503)}, {"e", delivery.ClassificationPermanentFailure, ptr(400)},
		} {
			s := start(tt.id, "r", at)
			if err := repo.AppendStart(ctx, s); err != nil {
				t.Fatal(err)
			}
			h, err := repo.GetByID(ctx, s.ID())
			if err != nil || h.Outcome != nil {
				t.Fatalf("incomplete history: %+v %v", h, err)
			}
			o := outcome(s, at.Add(time.Second), tt.status, tt.c)
			if err := repo.AppendOutcome(ctx, o); err != nil {
				t.Fatal(err)
			}
			h, err = repo.GetByID(ctx, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			if h.Start.ID() != s.ID() || h.Start.RunID() != s.RunID() || !h.Start.StartedAt().Equal(at) || h.Outcome == nil {
				t.Fatalf("bad history %+v", h)
			}
			got := h.Outcome
			status, has := got.HTTPStatus()
			want := 0
			if tt.status != nil {
				want = *tt.status
			}
			if got.AttemptID() != s.ID() || !got.FinishedAt().Equal(o.FinishedAt()) || got.Classification() != tt.c || status != want || has != (tt.status != nil) {
				t.Fatalf("bad outcome %+v", got)
			}
		}
	})
	t.Run("duplicates preserve history", func(t *testing.T) {
		if err := repo.AppendStart(ctx, start("A", "empty", at.Add(time.Hour))); !errors.Is(err, deliverypg.ErrDuplicateAttemptID) {
			t.Fatal(err)
		}
		if err := repo.AppendOutcome(ctx, outcome(start("A", "r", at), at.Add(time.Hour), nil, delivery.ClassificationPermanentFailure)); !errors.Is(err, deliverypg.ErrDuplicateOutcome) {
			t.Fatal(err)
		}
		h, err := repo.GetByID(ctx, "A")
		if err != nil {
			t.Fatal(err)
		}
		if h.Start.RunID() != "r" || !h.Start.StartedAt().Equal(at) || h.Outcome.Classification() != delivery.ClassificationSucceeded || !h.Outcome.FinishedAt().Equal(at.Add(time.Second)) {
			t.Fatal("history overwritten")
		}
	})
	t.Run("missing invalid and persisted timing", func(t *testing.T) {
		if err := repo.AppendStart(ctx, start("missing-run", "absent", at)); !errors.Is(err, deliverypg.ErrRunNotFound) {
			t.Fatal(err)
		}
		if err := repo.AppendOutcome(ctx, outcome(start("missing-start", "r", at), at, nil, delivery.ClassificationRetryableFailure)); !errors.Is(err, deliverypg.ErrAttemptStartNotFound) {
			t.Fatal(err)
		}
		if _, err := repo.GetByID(ctx, "absent"); !errors.Is(err, deliverypg.ErrAttemptNotFound) {
			t.Fatal(err)
		}
		if err := repo.AppendStart(ctx, delivery.AttemptStart{}); err == nil {
			t.Fatal("zero start accepted")
		}
		if err := repo.AppendOutcome(ctx, delivery.AttemptOutcome{}); err == nil {
			t.Fatal("zero outcome accepted")
		}
		s := start("timing", "empty", at)
		if err := repo.AppendStart(ctx, s); err != nil {
			t.Fatal(err)
		}
		fake := start(s.ID(), s.RunID(), at.Add(-time.Hour))
		if err := repo.AppendOutcome(ctx, outcome(fake, at.Add(-time.Microsecond), nil, delivery.ClassificationRetryableFailure)); err == nil {
			t.Fatal("trusted caller start time")
		}
		h, err := repo.GetByID(ctx, s.ID())
		if err != nil || h.Outcome != nil {
			t.Fatalf("invalid outcome inserted: %+v %v", h, err)
		}
	})
	t.Run("listing with unknown results", func(t *testing.T) {
		for _, s := range []delivery.AttemptStart{start("z", "r", at), start("a-later", "r", at.Add(time.Second))} {
			if err := repo.AppendStart(ctx, s); err != nil {
				t.Fatal(err)
			}
		}
		hs, err := repo.ListByRunID(ctx, "r")
		if err != nil {
			t.Fatal(err)
		}
		want := []delivery.AttemptID{"A", "b", "c", "d", "e", "z", "a-later"}
		if len(hs) != len(want) {
			t.Fatalf("got %d histories", len(hs))
		}
		for i, h := range hs {
			if h.Start.ID() != want[i] || (h.Outcome == nil) != (i >= 5) {
				t.Fatalf("unexpected history %+v", h)
			}
		}
		hs, err = repo.ListByRunID(ctx, "absent")
		if err != nil || len(hs) != 0 {
			t.Fatalf("expected empty: %v", err)
		}
	})
	t.Run("caller rollback", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		r := deliverypg.NewAttemptRepository(tx)
		s := start("tx", "empty", at)
		if err := r.AppendStart(ctx, s); err != nil {
			t.Fatal(err)
		}
		if err := r.AppendOutcome(ctx, outcome(s, at, ptr(200), delivery.ClassificationSucceeded)); err != nil {
			t.Fatal(err)
		}
		// Also append to a previously committed start; rollback must remove only the outcome.
		if err := r.AppendOutcome(ctx, outcome(start("timing", "empty", at), at, nil, delivery.ClassificationRetryableFailure)); err != nil {
			t.Fatal(err)
		}
		h, err := r.GetByID(ctx, "tx")
		if err != nil || h.Outcome == nil {
			t.Fatalf("transaction observation: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.GetByID(ctx, "tx"); !errors.Is(err, deliverypg.ErrAttemptNotFound) {
			t.Fatal(err)
		}
		h, err = repo.GetByID(ctx, "timing")
		if err != nil || h.Outcome != nil {
			t.Fatalf("outcome survived rollback: %v", err)
		}
	})
}
func TestAttemptZeroValuesBeforeQuery(t *testing.T) {
	r := deliverypg.NewAttemptRepository(nil)
	if err := r.AppendStart(context.Background(), delivery.AttemptStart{}); err == nil {
		t.Fatal("zero start accepted")
	}
	if err := r.AppendOutcome(context.Background(), delivery.AttemptOutcome{}); err == nil {
		t.Fatal("zero outcome accepted")
	}
}
