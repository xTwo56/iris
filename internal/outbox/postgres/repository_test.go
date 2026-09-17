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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/outbox"
	outboxpg "github.com/xTwo56/iris/internal/outbox/postgres"
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
	name := "iris_outbox_test_" + hex.EncodeToString(suffix[:])
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
	repo := outboxpg.New(pool)
	at := time.Date(2026, 9, 15, 0, 0, 0, 123456000, time.UTC)
	for _, sql := range []string{`INSERT INTO events VALUES ('e','type',$1,'{}')`, `INSERT INTO endpoints VALUES ('ep','https://example.com',$1,true)`, `INSERT INTO deliveries VALUES ('d','e','ep',$1)`} {
		if _, err := pool.Exec(ctx, sql, at); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"A", "z", "later", "duplicate-key", "race", "same", "tx", "constraint"} {
		if _, err := pool.Exec(ctx, `INSERT INTO delivery_runs VALUES ($1,'d',$2,'manual_redelivery')`, id, at); err != nil {
			t.Fatal(err)
		}
	}
	entry := func(id delivery.RunID, key string, when time.Time) outbox.Entry {
		t.Helper()
		e, err := outbox.New(id, key, when)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	t.Run("round trip and pending order", func(t *testing.T) {
		for _, e := range []outbox.Entry{entry("later", "key-later", at.Add(time.Second)), entry("z", "key-z", at), entry("A", " key-'A ", at)} {
			if err := repo.Create(ctx, e); err != nil {
				t.Fatal(err)
			}
			got, err := repo.GetByRunID(ctx, e.RunID())
			if err != nil {
				t.Fatal(err)
			}
			if got.RunID() != e.RunID() || got.SubmissionKey() != e.SubmissionKey() || !got.CreatedAt().Equal(e.CreatedAt()) {
				t.Fatal("metadata changed")
			}
			if _, _, ok := got.Acknowledgment(); ok {
				t.Fatal("not pending")
			}
		}
		got, err := repo.ListPending(ctx, 2)
		if err != nil || len(got) != 2 || got[0].RunID() != "A" || got[1].RunID() != "z" {
			t.Fatalf("order/limit: %+v %v", got, err)
		}
		for _, limit := range []int{-1, 0, outboxpg.MaxPendingLimit + 1} {
			if _, err := repo.ListPending(ctx, limit); err == nil {
				t.Fatal("invalid limit accepted")
			}
		}
	})
	t.Run("duplicates missing invalid", func(t *testing.T) {
		for _, tt := range []struct {
			e    outbox.Entry
			want error
		}{{entry("A", "other", at), outboxpg.ErrDuplicateRunID}, {entry("duplicate-key", "key-z", at), outboxpg.ErrDuplicateSubmissionKey}, {entry("absent", "missing", at), outboxpg.ErrRunNotFound}} {
			if err := repo.Create(ctx, tt.e); !errors.Is(err, tt.want) {
				t.Fatalf("want %v got %v", tt.want, err)
			}
		}
		if _, err := repo.GetByRunID(ctx, "absent"); !errors.Is(err, outboxpg.ErrNotFound) {
			t.Fatal(err)
		}
		if err := repo.MarkSubmitted(ctx, "absent", "job", at); !errors.Is(err, outboxpg.ErrNotFound) {
			t.Fatal(err)
		}
		if err := repo.Create(ctx, outbox.Entry{}); err == nil {
			t.Fatal("zero accepted")
		}
		submitted, err := entry("duplicate-key", "fresh", at).WithAcknowledgment("job", at)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Create(ctx, submitted); err == nil {
			t.Fatal("submitted entry inserted")
		}
	})
	t.Run("acknowledgment immutable", func(t *testing.T) {
		if err := repo.MarkSubmitted(ctx, "A", "job-A", at); err != nil {
			t.Fatal(err)
		}
		if err := repo.MarkSubmitted(ctx, "A", "job-A", at.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := repo.MarkSubmitted(ctx, "A", "different", at.Add(time.Hour)); !errors.Is(err, outboxpg.ErrSubmissionConflict) {
			t.Fatal(err)
		}
		got, err := repo.GetByRunID(ctx, "A")
		if err != nil {
			t.Fatal(err)
		}
		job, when, ok := got.Acknowledgment()
		if !ok || job != "job-A" || !when.Equal(at) || got.SubmissionKey() != " key-'A " {
			t.Fatal("ack overwritten")
		}
		pending, err := repo.ListPending(ctx, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range pending {
			if e.RunID() == "A" {
				t.Fatal("submitted still pending")
			}
		}
	})
	t.Run("concurrent acknowledgments", func(t *testing.T) {
		for _, id := range []delivery.RunID{"race", "same"} {
			if err := repo.Create(ctx, entry(id, string(id), at)); err != nil {
				t.Fatal(err)
			}
			gate := make(chan struct{})
			results := make(chan struct {
				job string
				at  time.Time
				err error
			}, 2)
			for i := 0; i < 2; i++ {
				job := "job-1"
				if i == 1 && id == "race" {
					job = "job-2"
				}
				when := at.Add(time.Duration(i) * time.Second)
				go func() {
					<-gate
					results <- struct {
						job string
						at  time.Time
						err error
					}{job, when, repo.MarkSubmitted(ctx, id, job, when)}
				}()
			}
			close(gate)
			a, b := <-results, <-results
			if id == "race" {
				if a.err != nil {
					a, b = b, a
				}
				if a.err != nil || !errors.Is(b.err, outboxpg.ErrSubmissionConflict) {
					t.Fatalf("race results: %v %v", a.err, b.err)
				}
			} else if a.err != nil || b.err != nil {
				t.Fatalf("same job race: %v %v", a.err, b.err)
			}
			got, err := repo.GetByRunID(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			job, when, _ := got.Acknowledgment()
			if id == "race" && (job != a.job || !when.Equal(a.at)) {
				t.Fatal("loser overwrote identity")
			}
			if id == "same" && (job != "job-1" || (!when.Equal(a.at) && !when.Equal(b.at))) {
				t.Fatal("bad repeated ack")
			}
		}
	})
	t.Run("transaction rollback", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		r := outboxpg.New(tx)
		if _, err := tx.Exec(ctx, `INSERT INTO delivery_runs VALUES ('tx-created','d',$1,'manual_redelivery')`, at); err != nil {
			t.Fatal(err)
		}
		if err := r.Create(ctx, entry("tx-created", "tx-key", at)); err != nil {
			t.Fatal(err)
		}
		if err := r.MarkSubmitted(ctx, "tx-created", "tx-job", at); err != nil {
			t.Fatal(err)
		}
		if err := r.MarkSubmitted(ctx, "z", "z-job", at); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.GetByRunID(ctx, "tx-created"); !errors.Is(err, outboxpg.ErrNotFound) {
			t.Fatal(err)
		}
		z, err := repo.GetByRunID(ctx, "z")
		if err != nil {
			t.Fatal(err)
		}
		if _, _, ok := z.Acknowledgment(); ok {
			t.Fatal("ack survived rollback")
		}
	})
	t.Run("migration constraints", func(t *testing.T) {
		for _, sql := range []string{
			`INSERT INTO outbox VALUES ('constraint','c',now(),'job',NULL)`,
			`INSERT INTO outbox VALUES ('constraint','c',now(),NULL,now())`,
			`INSERT INTO outbox VALUES ('constraint','c',now(),' ',now())`,
			`INSERT INTO outbox VALUES ('constraint',' ',now(),NULL,NULL)`,
			`INSERT INTO outbox VALUES ('constraint','c','0001-01-01 00:00:00+00',NULL,NULL)`,
			`INSERT INTO outbox VALUES ('constraint','c',now(),'job','infinity')`,
		} {
			_, err := pool.Exec(ctx, sql)
			var pe *pgconn.PgError
			if !errors.As(err, &pe) || pe.Code != "23514" {
				t.Fatalf("constraint not rejected: %v", err)
			}
		}
		_, err := pool.Exec(ctx, `DELETE FROM delivery_runs WHERE id='A'`)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "23001" {
			t.Fatalf("delete not restricted: %v", err)
		}
	})
	t.Run("migration down and reapply", func(t *testing.T) {
		down, err := os.ReadFile("../../../migrations/000003_outbox.down.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(down)); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			t.Fatal(err)
		}
		pending, err := repo.ListPending(ctx, 1)
		if err != nil || len(pending) != 0 {
			t.Fatalf("expected empty: %v", err)
		}
	})
}
